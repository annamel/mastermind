from copy import copy, deepcopy
import heapq
from logging import getLogger
import storage
import time

import pymongo

import jobs
from jobs.job import Job
from jobs.job_factory import JobFactory
from mastermind_core.config import config
from mastermind_core.db.mongo.pool import Collection
from sync import sync_manager
from sync.error import LockFailedError, LockAlreadyAcquiredError
from timer import periodic_timer
from history import GroupHistoryFinder
from collections import defaultdict, Counter
import itertools

import timed_queue

logger = getLogger('mm.sched')


class Scheduler(object):

    def __init__(self, db, niu, job_processor, namespaces_settings):

        self.params = config.get('scheduler', {})

        logger.info('Scheduler initializing')
        self.candidates = []

        self.job_processor = job_processor
        self.__tq = timed_queue.TimedQueue()

        self.node_info_updater = niu
        self.namespaces_settings = namespaces_settings

        self.starters = []

        if config['metadata'].get('scheduler', {}).get('db'):
            self.collection = Collection(db[config['metadata']['scheduler']['db']], 'scheduler')

        self.res = defaultdict(list)
        self.history_data = {}

        # store job limits instead of regular updating it from the config
        self.res_limits = defaultdict(dict)
        for res_type in jobs.Job.RESOURCE_TYPES:
            for job_type in jobs.JobTypes.AVAILABLE_TYPES:
                self.res_limits[job_type][res_type] = config.get('jobs', {}).get(job_type, {}).get(
                            'resources_limits', {}).get(res_type, 1)
        logger.info("Obtained res limits are {}".format(self.res_limits))

    def _start_tq(self):
        self.__tq.start()

    def register_periodic_func(self, starter_func, period_default, starter_name=None, lock_name=None):
        """
        Guarantee that starter_func would be run within specified period under a zookeeper lock
        :param starter_func - a function to be run
        :param period_default - the default value of period to run the function with
            if the "{starter_name}_period" is not specified under {starter_name} sub-section with sched config section
        :param starter_name - a logical name of starter. Name of starter subsection within sched section in config.
                            Defaults to starter_func.__name__
        :param lock_name - a name of lock in zookeeper that protects starter from simultaneous execution).
                            Defaults to "scheduler/{starter_name}
        """

        starter_name = starter_name or starter_func.__name__
        lock_name = lock_name or "scheduler/{}".format(starter_name)
        period_param = "{}_period".format(starter_name)
        period_val = self.params.get(starter_name, {}).get(period_param, period_default)

        logger.info("Registering periodic starter func {} to be raised on {} period".format(starter_name, period_val))

        if not self.params.get(starter_name, {}).get('enabled', False):
            logger.info("Starter {} is disabled".format(starter_name))
            return

        timer = periodic_timer(seconds=period_val)

        def _starter_periodic_func():
            try:
                logger.info('Starting {}'.format(starter_name))

                with sync_manager.lock(lock_name, blocking=False):
                    starter_func()

            except LockFailedError:
                logger.info('Another {} is already running under {} lock'.format(starter_func.__name__, lock_name))
            except:
                logger.exception('Failed to {}'.format(starter_name))
            finally:
                logger.info('{} finished'.format(starter_name))
                self.__tq.add_task_at(
                    starter_name,
                    timer.next(),
                    _starter_periodic_func)

        self.__tq.add_task_at(
            starter_name,
            timer.next(),
            _starter_periodic_func
        )

        starter = {
            "starter_func": starter_func,
            "starter_name": starter_name,
            "starter_lock": lock_name,
            "starter_timer": timer,
        }

        self.starters.append(starter)

        logger.info("Registered starter function")

    def update_resource_stat(self):

        # update jobs list
        active_jobs = self.job_processor.job_finder.jobs(
            statuses=jobs.Job.ACTIVE_STATUSES,
            sort=False,
        )

        # update job count per each type
        self.job_count = Counter()

        # update host stat
        self.res = defaultdict(list)

        errors = []

        for job in active_jobs:
            self.job_count[job.type] += 1
            res_demand = self.convert_resource_representation(job.resources, job._involved_groups, job.type, errors)
            self.add_to_resource_stat(res_demand, job.id)

        if len(errors) != 0:
            logger.warn("Errors during resource aggregation are {}".format(set(errors)))

    def convert_resource_representation(self, resources, groups, job_type, errors=None):
        """
        The function takes old resources (equivalent to job.resources), "resource_limits" for each job type from config
        and produces a new resource representation
        :param resources:  Dict of res_type where each value is a list of node addr or tuples (node_addr, fs)
        :param groups: List of groups ids
        :param job_type: job_type
        :param errors: list of strings
        :return: a dict where keys are tuples (res_type, group) or (res_type, node_addr) or (res_type, node_addr, fs_id)
                and values are consumption level
        """
        res_demand = {}

        for g in groups:
            res_demand[("Group", g)] = 100

        for res_type in resources:
            for addr in resources[res_type]:
                if res_type != Job.RESOURCE_FS:
                    if addr not in storage.hosts and errors:
                        errors.append('Host with address {} is not found in storage'.format(addr))

                    consumption = 100 / min(self.res_limits[job_type][res_type], 1)
                    res_demand[(res_type, addr)] = consumption
                else:
                    if addr[0] not in storage.hosts and errors:
                        errors.append('Host with address {} is not found in storage'.format(addr[0]))

                    # Assume that FS is always 100% utilized
                    res_demand[(Job.RESOURCE_FS, addr[0], addr[1])] = 100

        return res_demand

    def add_to_resource_stat(self, res_demand, job_id):
        """
        Add to saved resource representation resource consumption for the specified job. We could skip this and call
        update_resource_stat after each iteration but this is rather expensive. So while the representation won't be
        actual, let's keep it
        :param res_demand: a dict where keys are tuples (res_type, node_addr\group_id, [fs_id])
                            and values are consumption level
        :param job_id: resource owner
        """
        for r in res_demand:
            self.res[r].append((res_demand[r], job_id))

    def get_resource_crossing(self, res_demand):
        """
        Uncovers inter-crossing of resources between running jobs and specified resources
        :param resources: See convert_resource_representation
        :return: list of (res_level, job_id)
        """
        crossing_jobs = []

        for r in res_demand:
            crossing_jobs.extend(self.res[r])

        return crossing_jobs

    def cancel_crossing_jobs(self, job_type, sched_params, crossing_jobs, demand):
        """
        Try to cancel jobs that are concurrent with a job we are trying to create
        :param job_type: the type of the job we are trying to create
        :param sched_params: scheduling params of the job we are trying to create
        :param crossing_jobs:  see return of get_resource_crossing
        :param demand: required resources (see convert_resource_representation)
        :return: True if cancellation was successful, False otherwise
        """
        force = sched_params.get('force', False)

        # XXX: these jobs should not be hard-coded. This should be present in config
        STOP_ALLOWED_TYPES = (jobs.JobTypes.TYPE_RECOVER_DC_JOB,
                              jobs.JobTypes.TYPE_COUPLE_DEFRAG_JOB,
                              jobs.JobTypes.TYPE_TTL_CLEANUP_JOB
                              )

        job_ids = [v[1] for v in crossing_jobs]

        existing_jobs = self.job_processor.job_finder.jobs(ids=job_ids, sort=False)
        cancellable_jobs = []
        for job in existing_jobs:

            if self.job_processor.JOB_PRIORITIES[job.type] >= self.job_processor.JOB_PRIORITIES[job_type] and not force:
                logger.info('Cannot stop job {} type {} since it has >= priority and no force'.format(job.id, job.type))
                continue

            if job.status in (jobs.Job.STATUS_NOT_APPROVED, jobs.Job.STATUS_NEW):
                cancellable_jobs.append(job.id)
                continue

            if job.type not in STOP_ALLOWED_TYPES:
                logger.info('Cannot stop job {} of type is {}'.format(job.id, job.type))
                continue

            cancellable_jobs.append(job.id)

        if len(cancellable_jobs) == 0:
            logger.info("No jobs to cancel while {} are blocking".format(len(job_ids)))
            return False

        logger.info("Analyzing demand {} while cancellable_jobs are {}".format(demand, cancellable_jobs))

        # Analyze whether stopping of cancelable_jobs would be enough
        for res in demand:
            res_demand = demand[res]  # how much of resource of this type and id, the job wants to consume
            usage_if_cancel = 0  # how much of resource would be still consumed when we'd cancel all cancellable jobs

            for j in self.res[res]:
                if j[1] not in cancellable_jobs:
                    usage_if_cancel += j[0]

            if res_demand > (100 - usage_if_cancel):
                logger.info("No sense to cancel cause of res={} (demand={} < usage_if_cancel={}, jobs are {})".format(
                    res, res_demand, usage_if_cancel, self.res[res]))
                return False

        # XXX: Now we cancel all jobs while we may be satisfied with less amount. Fix that

        try:
            self.job_processor.stop_jobs_list(filter(lambda x: x.id in cancellable_jobs, existing_jobs))
        except:
            logger.exception("Failed to cancel jobs {}".format(cancellable_jobs))
            return False

        logger.info("Successfully cancelled {}".format(cancellable_jobs))
        return True

    def process_jobs(self, job_type, jobs_param_list, sched_params, filter_func=None):
        """
        Scheduler creates jobs with specified params of specified type. Sched_params and filter_func
        :param job_type: job type
        :param jobs_param_list: a list of dictionaries with params for jobs created.
                                A number of job created <= len(jobs_param_list)
        :param sched_params: in a common case sched params are stored in config in "scheduler/{starter}" section.
                            But in certain cases the starter may want to redefine some parameters.
                            For example, one may want to increase priority or decrease max_deadline_time
        :param filter_func: Filter function are called before job creation to extra-check whether those jobs are needed
                            See more details on wiki
                            (https://wiki.yandex-team.ru/users/aniam/mm-scheduler/#generacijadzhobovstartdzhobov)
        :return: a number of generated jobs
        """

        self.update_resource_stat()

        max_jobs = sched_params.get('max_executing_jobs')
        if max_jobs:
            job_count = self.job_count[job_type]
            if job_count >= max_jobs:
                logger.info('Found {} unfinished jobs (>= {}) of {} type'.format(job_count, max_jobs, job_type))
                return 0
            max_jobs -= job_count

        created_job = 0

        # common param for all types of jobs
        need_approving = not sched_params.get('autoapprove', False)
        force = sched_params.get('force', False)

        job_class = JobFactory.make_job_type(job_type)

        if not hasattr(job_class, 'report_resources'):
            logger.error("Couldn't schedule the job {}. Add static report_resources function".format(job_type))
            return 0

        job_report_resources = job_class.report_resources

        for job_param in jobs_param_list:

            # Get resource demand and verify whether run is possible
            old_res = job_report_resources(job_param)
            res = self.convert_resource_representation(old_res['resources'], old_res['groups'], job_type)

            crossing_jobs = self.get_resource_crossing(res)

            if len(crossing_jobs) != 0:
                logger.info("Job is crossing with {}. job_resources are {}".format(crossing_jobs, res))
                if not self.cancel_crossing_jobs(job_type, sched_params, crossing_jobs, res):
                    continue

            job_param['need_approving'] = need_approving

            try:
                job = self.job_processor._create_job(
                        job_type=job_type,
                        params=job_param,
                        force=force)

            except LockAlreadyAcquiredError as e:
                logger.error("Failed to create a new job {} since couple/group are already locked {}".format(job_type, e))
                # update resource stat, something has changed in the cluster or there is an error in the scheduler
                self.update_resource_stat()
                continue

            except:
                logger.exception("Creating job {} with params {} has excepted".format(job_type, job_param))
                continue

            created_job += 1
            if max_jobs and created_job >= max_jobs:
                return created_job

            self.add_to_resource_stat(res, job.id)

        # do not update self.job_count, since they would be rewritten on next update & they don't influence calculations

        return created_job

    def get_history(self):

        if len(self.history_data) != len(storage.replicas_groupsets):
            self.sync_history()
        return self.history_data

    def sync_history(self):

        cursor = self.collection.find()

        logger.info('Sync recover data is required: {} records/{} couples, cursor {}'.format(
                len(self.history_data), len(storage.replicas_groupsets), cursor.count()))

        recover_data_couples = set()
        history = {}

        for data_record in cursor:
            couple_str = data_record['couple']
            history[couple_str] = {'recover_ts': data_record.get('recover_ts')}
            recover_data_couples.add(couple_str)

        ts = int(time.time())

        # XXX: rework, it is too expensive
        storage_couples = set(str(c) for c in storage.replicas_groupsets.keys())
        add_couples = list(storage_couples - recover_data_couples)
        remove_couples = list(recover_data_couples - storage_couples)

        logger.info('Couples to add {}, couple to remove {}'.format(add_couples, remove_couples))

        offset = 0
        OP_SIZE = 200
        while offset < len(add_couples):
            bulk_op = self.collection.initialize_unordered_bulk_op()
            bulk_add_couples = add_couples[offset:offset + OP_SIZE]
            for couple in bulk_add_couples:
                bulk_op.insert({'couple': couple, 'recover_ts': ts})
            res = bulk_op.execute()
            if res['nInserted'] != len(bulk_add_couples):
                raise ValueError('Failed to add couples recover data: {}/{} ({})'.format(
                    res['nInserted'], len(bulk_add_couples), res))
            offset += res['nInserted']

        offset = 0
        while offset < len(remove_couples):
            bulk_op = self.collection.initialize_unordered_bulk_op()
            bulk_remove_couples = remove_couples[offset:offset + OP_SIZE]
            bulk_op.find({'couple': {'$in': bulk_remove_couples}}).remove()
            res = bulk_op.execute()
            if res['nRemoved'] != len(bulk_remove_couples):
                raise ValueError('failed to remove couples recover data: {0}/{1} ({2})'.format(
                    res['nRemoved'], len(bulk_remove_couples), res))
            offset += res['nRemoved']

        self.history_data = history

    def update_recover_ts(self, couple_id, ts):
        ts = int(ts)
        res = self.collection.update(
            {'couple': couple_id},
            {'couple': couple_id, 'recover_ts': ts},
            upsert=True)
        if res['ok'] != 1:
            logger.error('Unexpected mongo response during recover ts update: {}'.format(res))
            raise RuntimeError('Mongo operation result: {}'.format(res['ok']))

        # update cached representation
        self.history_data[couple_id] = {'recover_ts': ts}