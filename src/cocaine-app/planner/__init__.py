from copy import copy, deepcopy
import heapq
from logging import getLogger
import socket
import storage
import time
import datetime

import pymongo

from errors import CacheUpstreamError
import helpers as h
from infrastructure import infrastructure, UncoupledGroupsSelector
from infrastructure_cache import cache
import inventory
import jobs
from manual_locks import manual_locker
from mastermind_core.config import config
from mastermind_core.db.mongo.pool import Collection
from sync import sync_manager
from sync.error import LockFailedError, LockAlreadyAcquiredError
from timer import periodic_timer
import os
import re
from history import GroupHistoryFinder
from yt_worker import YqlWrapper

import timed_queue

logger = getLogger('mm.planner')


class Planner(object):

    MOVE_CANDIDATES = 'move_candidates'
    RECOVER_DC = 'recover_dc'
    COUPLE_DEFRAG = 'couple_defrag'
    TTL_CLEANUP = 'ttl_cleanup'
    TTL_YT_CLEANUP = 'ttl_yt_cleanup'

    RECOVERY_OP_CHUNK = 200

    COUPLE_DEFRAG_LOCK = 'planner/couple_defrag'
    RECOVER_DC_LOCK = 'planner/recover_dc'
    TTL_CLEANUP_LOCK = 'planner/ttl_cleanup'

    def __init__(self, db, niu, job_processor, namespaces_settings):

        self.params = config.get('planner', {})

        logger.info('Planner initializing')
        self.candidates = []
        self.job_processor = job_processor
        self.__max_plan_length = self.params.get('move', {}).get('max_plan_length', 5)
        self.__tq = timed_queue.TimedQueue()

        self.node_info_updater = niu
        self.namespaces_settings = namespaces_settings

        self.recover_dc_timer = periodic_timer(
            seconds=self.params.get('recover_dc', {}).get(
                'recover_dc_period', 60 * 15))
        self.couple_defrag_timer = periodic_timer(
            seconds=self.params.get('couple_defrag', {}).get(
                'couple_defrag_period', 60 * 15))
        self.ttl_cleanup_timer = periodic_timer(
            seconds=self.params.get('ttl_cleanup', {}).get('ttl_cleanup_period', 60 * 15))
        self.ttl_cleanup_yt_timer = periodic_timer(
            seconds=self.params.get('ttl_cleanup', {}).get('yt_cleanup_period', 60 * 60 * 24 * 10)
        )

        if config['metadata'].get('planner', {}).get('db'):
            self.collection = Collection(db[config['metadata']['planner']['db']], 'planner')

            if self.params.get('recover_dc', {}).get('enabled', False):
                self.__tq.add_task_at(
                    self.RECOVER_DC,
                    self.recover_dc_timer.next(),
                    self._recover_dc
                )

            if self.params.get('couple_defrag', {}).get('enabled', False):
                self.__tq.add_task_at(
                    self.COUPLE_DEFRAG,
                    self.couple_defrag_timer.next(),
                    self._couple_defrag
                )

            if self.params.get('ttl_cleanup', {}).get('enabled', False):
                self.__tq.add_task_at(
                    self.TTL_CLEANUP,
                    self.ttl_cleanup_timer.next(),
                    self._ttl_cleanup_planner
                )

                # Running ttl_yt_cleaner should happen under the same lock as TTL_CLEANUP in order to
                # decrease concurrent access to YT cleanup
                self.__tq.add_task_at(
                    self.TTL_YT_CLEANUP,
                    self.ttl_cleanup_yt_timer.next(),
                    self._ttl_yt_cleaner
                )


    def _start_tq(self):
        self.__tq.start()

    def add_planner(self, planner):
        planner.schedule_tasks(self.__tq)

    def _get_yt_stat(self):
        """
        Extract statistics from YT logs
        :return: list of couples ids with expired records volume above specified
        """
        try:
            # Configure parameters for work with YT
            yt_cluster = self.params.get('ttl_cleanup', {}).get('yt_cluster', "")
            yt_token = self.params.get('ttl_cleanup', {}).get('yt_token', "")
            yt_attempts = self.params.get('ttl_cleanup', {}).get('yt_attempts', 3)
            yt_delay = self.params.get('ttl_cleanup', {}).get('yt_delay', 10)

            yt_wrapper = YqlWrapper(cluster=yt_cluster, token=yt_token, attempts=yt_attempts, delay=yt_delay)

            aggregation_table = self.params.get('ttl_cleanup', {}).get('aggregation_table', "")
            base_table = self.params.get('ttl_cleanup', {}).get('tskv_log_table', "")
            expired_threshold = self.params.get('ttl_cleanup', {}).get('ttl_threshold', 10 * float(1024 ** 3))  # 10GB

            yt_wrapper.prepare_aggregate_for_yesterday(base_table, aggregation_table)

            couple_list = yt_wrapper.request_expired_stat(aggregation_table, expired_threshold)
            logger.info("YT request has completed")
            return couple_list
        except:
            logger.exception("Work with YQL failed")
            return []

    def _ttl_cleanup_planner(self):
        try:
            logger.info('Starting ttl cleanup planner')

            with sync_manager.lock(Planner.TTL_CLEANUP_LOCK, blocking=False):
                self._do_ttl_cleanup()

        except LockFailedError:
            logger.info('TTl cleanup planner is already running')
        except Exception:
            logger.exception('Failed to plan ttl cleanup')
        finally:
            logger.info('TTL cleanup planner finished')
            self.__tq.add_task_at(
                self.TTL_CLEANUP,
                self.ttl_cleanup_timer.next(),
                self._ttl_cleanup_planner)

    def _get_idle_groups(self, days_of_idle):
        """
        Iterates all over couples. Find couples where ttl_cleanup hasn't run for more than 'days of idle'
        :param days_of_idle: how long the group could be idle
        :return: list of groups[0] from couples
        """

        idle_groups = []

        # the epoch time when executed jobs are considered meaningful
        idleness_threshold = time.time() - datetime.timedelta(days=days_of_idle).total_seconds()

        couples_data = self.collection.find().sort('ttl_cleanup_ts', pymongo.ASCENDING)
        if couples_data.count() < len(storage.replicas_groupsets):
            logger.info('Sync cleanup data is required: {0} records/{1} couples'.format(
                couples_data.count(), len(storage.replicas_groupsets)))
            self.sync_historic_data(recover_ts=0, cleanup_ts=int(time.time()))
            couples_data = self.collection.find().sort('ttl_cleanup_ts', pymongo.ASCENDING)

        for couple_data in couples_data:
            c = couple_data['couple']

            # if couple_data doesn't contain cleanup_ts field then cleanup_ts has never been run on this couple
            # and None < idleness_threshold
            ts = couple_data.get('ttl_cleanup_ts')
            if ts > idleness_threshold:
                # all couples after this one have more "fresh" job worked for them
                logger.debug("Found lazy couples {}".format(idle_groups))
                return idle_groups

            # couple has format "gr0:gr1:...:grN". We are interested only in the group #0
            idle_groups.append(int(c.split(":")[0]))

        logger.debug("Find lazy couples {}".format(idle_groups))
        return idle_groups

    def _get_ttl_cleanup_last_run(self):
        """
        Iterates over collection and extracts only couples within namespaces with enabled ttl
        :return: dict ['couple_id'] = last_ttl_run_time
        """
        result = {}

        couples_data = self.collection.find().sort('cleanup_ts', pymongo.ASCENDING)
        if couples_data.count() < len(storage.replicas_groupsets):
            logger.info('Sync cleanup data is required: {0} records/{1} couples'.format(
                couples_data.count(), len(storage.replicas_groupsets)))
            self.sync_historic_data(recover_ts=0, cleanup_ts=int(time.time()))
            couples_data = self.collection.find().sort('cleanup_ts', pymongo.ASCENDING)

        for couple_data in couples_data:
            c = couple_data['couple']

            if c == "None":
                logger.error("Damaged couple? {}".format(couple_data))
                continue

            group_id = int(c.split(":")[0])

            if group_id not in storage.groups:
                logger.error("Not valid group is within collection {}".format(group_id))
                continue
            group = storage.groups[group_id]
            if not group.couple:
                logger.error("Group in collection is uncoupled {}".format(str(group)))
                continue

            ns_settings = self.namespaces_settings.get(group.couple.namespace.id)
            if ns_settings and not ns_settings.attributes.ttl.enable:
                logger.debug("Skipping group {} cause ns '{}' no ttl".format(group_id, group.couple.namespace.id))
                continue

            # if couple_data doesn't contain cleanup_ts field then cleanup_ts has never been run on this couple
            # and None < idleness_threshold
            result[group_id] = couple_data.get('cleanup_ts')

        logger.info("History of last run is of {} len".format(len(result)))
        return result

    def _cleanup_aggregation_yt(self):
        """
        Cleanup YT aggregation table by removing outdated records (where outdated means their exp_date < ttl_last_run)
        :return:
        """

        logger.info("Going to cleanup aggregation YT table")

        # Configure parameters for work with YT
        # XXX: when ttl_cleanup would be a separate class and a separate worker, remove re-reading params
        # (the code is dup with get_yt_stat)
        yt_cluster = self.params.get('ttl_cleanup', {}).get('yt_cluster', "")
        yt_token = self.params.get('ttl_cleanup', {}).get('yt_token', "")
        yt_attempts = self.params.get('ttl_cleanup', {}).get('yt_attempts', 3)
        yt_delay = self.params.get('ttl_cleanup', {}).get('yt_delay', 10)

        yt_wrapper = YqlWrapper(cluster=yt_cluster, token=yt_token, attempts=yt_attempts, delay=yt_delay)

        aggregation_table = self.params.get('ttl_cleanup', {}).get('aggregation_table', "")

        ttl_hist = self._get_ttl_cleanup_last_run()

        yt_wrapper.cleanup_aggregaton_table(aggregate_table=aggregation_table, couples_hist=ttl_hist)

        logger.info("Cleaning up YT is complete")

    def _ttl_yt_cleaner(self):
        try:
            logger.info('Starting ttl yt cleaner')

            with sync_manager.lock(Planner.TTL_CLEANUP_LOCK, blocking=False):
                self._cleanup_aggregation_yt()

        except LockFailedError:
            logger.info('TTl cleanup planner or ttl yt cleaner is already running')
        except:
            logger.exception('Failed to clean YT aggregation table')
        finally:
            logger.info('TTL YT cleaner finished')
            self.__tq.add_task_at(
                self.TTL_CLEANUP,
                self.ttl_cleanup_yt_timer.next(),
                self._ttl_yt_cleaner)

    def _do_ttl_cleanup(self):
        logger.info('Run ttl cleanup')

        max_cleanup_jobs = config.get('jobs', {}).get('ttl_cleanup_job', {}).get(
            'max_executing_jobs', 100)

        allowed_idleness_period = config.get('jobs', {}).get('ttl_cleanup_job', {}).get(
            'max_idle_days', 270)

        count = self.job_processor.job_finder.jobs_count(
            types=jobs.JobTypes.TYPE_TTL_CLEANUP_JOB,
            statuses=jobs.Job.ACTIVE_STATUSES)
        if count >= max_cleanup_jobs:
            logger.info('Found {} unfinished ttl cleanup jobs (>= {})'.format(count, max_cleanup_jobs))
            return
        else:
            max_new_cleanup_jobs = max_cleanup_jobs - count

        # get information from mds-proxy Yt logs
        yt_group_list = self._get_yt_stat()

        # get couples where ttl_cleanup wasn't run for long time (or never)
        time_group_list = self._get_idle_groups(days_of_idle=allowed_idleness_period)

        # remove dups
        yt_group_list = set(yt_group_list + time_group_list)

        for iter_group in yt_group_list:
            if iter_group not in storage.groups:
                logger.error("Not valid group is extracted from aggregation log {}".format(iter_group))
                continue
            iter_group = storage.groups[iter_group]
            if not iter_group.couple:
                logger.error("Iter group is uncoupled {}".format(iter_group))
                continue

            ns_settings = self.namespaces_settings.get(iter_group.couple.namespace.id)
            if not ns_settings or not ns_settings.attributes.ttl.enable:
                logger.debug("Skipping group {} cause ns '{}' without ttl".format(
                              iter_group.group_id, iter_group.couple.namespace.id))
                continue

            try:
                self.job_processor._create_job(
                    job_type=jobs.JobTypes.TYPE_TTL_CLEANUP_JOB,
                    params={
                        'iter_group': iter_group.group_id,
                        'couple': str(iter_group.couple),
                        'namespace': iter_group.couple.namespace.id,
                        'batch_size': None,  # get from config
                        'attempts': None,  # get from config
                        'nproc': None,  # get from config
                        'wait_timeout': None,  # get from config
                        'dry_run': False,
                        'need_approving': not self.params.get('ttl_cleanup', {}).get('autoapprove', False),
                    },
                )
                max_new_cleanup_jobs -= 1
                if max_new_cleanup_jobs == 0:
                    logger.info("Stopping job creation due to upper limitation on job count {}", max_cleanup_jobs)
                    break
            except LockAlreadyAcquiredError as e:
                logger.info("Failed to create a new job since couple/group are already locked {}".format(e))
                continue
            except:
                logger.exception("Creating job for iter group {} has excepted".format(iter_group))
                continue

    @staticmethod
    def _prepare_candidates_by_dc(suitable_groups, unsuitable_dcs):
        by_dc = {}
        for candidates in suitable_groups:
            unc_group = candidates[0]
            dc = unc_group.node_backends[0].node.host.dc
            if dc in unsuitable_dcs:
                continue
            by_dc.setdefault(dc, []).append(candidates)
        return by_dc

    def _sample_groups_by_ts_and_couple_dcs(self, state, groups):
        """
        Sample groups by its total space and dcs of its neighbouring groups.

        This method splits groups by their approximate total space and then selects
        those that have a unique set of dcs of coupled groups. This helps to narrow
        a list of candidate groups to move from a dc.
        """

        groups_sample = []

        groups = sorted(groups, key=lambda x: state.stats(x).total_space, reverse=True)

        dcs_combinations = set()
        total_space = None

        for group in groups:

            group_total_space = state.stats(group).total_space
            try:
                dcs = tuple(sorted(
                    nb.node.host.dc
                    for g in group.couple.groups
                    for nb in g.node_backends
                ))
            except CacheUpstreamError:
                continue

            if total_space and group_total_space >= total_space * 0.95:
                if dcs in dcs_combinations:
                    continue
            else:
                # dcs_combinations are dropped because next total space group has begun
                dcs_combinations = set()

            groups_sample.append(group)
            total_space = group_total_space
            dcs_combinations.add(dcs)

        return groups_sample

    def _sample_dc_candidates(self, src_group, dc_candidates, storage_state):
        candidates_ts = []
        for candidates in dc_candidates:
            dst_total_space = sum(
                storage_state.stats(candidate).total_space
                for candidate in candidates
            )
            candidates_ts.append((candidates, dst_total_space))

        total_space = None
        src_total_space = storage_state.stats(src_group).total_space

        res_candidates = []
        for candidate, dst_total_space in candidates_ts:
            if total_space and dst_total_space >= total_space * 0.95:
                continue

            if dst_total_space <= src_total_space * 1.05:
                # NOTE: found good enough candidate, no point in further searching
                return [candidate]

            res_candidates.append(candidate)
            total_space = dst_total_space
        return res_candidates

    @staticmethod
    def _filter_groups_on_busy_hosts(groups, busy_hosts):
        for g in groups:
            if g.node_backends[0].node.host.addr in busy_hosts:
                continue
            yield g

    COUNTABLE_STATUSES = [jobs.Job.STATUS_NOT_APPROVED,
                          jobs.Job.STATUS_NEW,
                          jobs.Job.STATUS_EXECUTING]

    @staticmethod
    def _jobs_slots(active_jobs, job_type, max_jobs_count):
        jobs_count = 0
        for job in active_jobs:
            if (job.type == job_type and
                    job.status in Planner.COUNTABLE_STATUSES):
                jobs_count += 1

        return max_jobs_count - jobs_count

    @staticmethod
    def _busy_group_ids(active_jobs):
        busy_group_ids = set()
        for job in active_jobs:
            busy_group_ids.update(job._involved_groups)
        return busy_group_ids

    @staticmethod
    def _is_locked(couple, busy_group_ids):
        for group in couple.groups:
            if group.group_id in busy_group_ids:
                return True
        return False

    def _recover_dc(self):
        try:
            start_ts = time.time()
            logger.info('Starting recover dc planner')

            max_recover_jobs = config.get('jobs', {}).get('recover_dc_job', {}).get(
                'max_executing_jobs', 3)
            # prechecking for new or pending tasks
            count = self.job_processor.job_finder.jobs_count(
                types=jobs.JobTypes.TYPE_RECOVER_DC_JOB,
                statuses=[jobs.Job.STATUS_NOT_APPROVED,
                          jobs.Job.STATUS_NEW,
                          jobs.Job.STATUS_EXECUTING])
            if count >= max_recover_jobs:
                logger.info('Found {0} unfinished recover dc jobs (>= {1})'.format(
                    count, max_recover_jobs))
                return

            with sync_manager.lock(Planner.RECOVER_DC_LOCK, blocking=False):
                self._do_recover_dc()

        except LockFailedError:
            logger.info('Recover dc planner is already running')
        except Exception:
            logger.exception('Recover dc planner failed')
        finally:
            logger.info('Recover dc planner finished, time: {:.3f}'.format(
                time.time() - start_ts))
            self.__tq.add_task_at(
                self.RECOVER_DC,
                self.recover_dc_timer.next(),
                self._recover_dc)

    def _do_recover_dc(self):

        max_recover_jobs = config.get('jobs', {}).get('recover_dc_job', {}).get(
            'max_executing_jobs', 3)

        active_jobs = self.job_processor.job_finder.jobs(
            statuses=jobs.Job.ACTIVE_STATUSES,
            sort=False,
        )

        slots = self._jobs_slots(active_jobs,
                                 jobs.JobTypes.TYPE_RECOVER_DC_JOB,
                                 max_recover_jobs)
        if slots <= 0:
            logger.info('Found {0} unfinished recover dc jobs'.format(
                max_recover_jobs - slots))
            return

        created_jobs = 0
        logger.info('Trying to create {0} jobs'.format(slots))

        need_approving = not self.params.get('recover_dc', {}).get('autoapprove', False)

        couple_ids_to_recover = self._recover_top_weight_couples(
            slots, active_jobs)

        for couple_id in couple_ids_to_recover:

            logger.info('Creating recover job for couple {0}'.format(couple_id))

            couple = storage.replicas_groupsets[couple_id]

            if not _recovery_applicable_couple(couple):
                logger.info('Couple {0} is no more applicable for recovery job'.format(
                    couple_id))
                continue

            try:
                job = self.job_processor._create_job(
                    jobs.JobTypes.TYPE_RECOVER_DC_JOB,
                    {'couple': couple_id,
                     'need_approving': need_approving})
                logger.info('Created recover dc job for couple {0}, job id {1}'.format(
                    couple, job.id))
                created_jobs += 1
            except Exception as e:
                logger.error('Failed to create recover dc job for couple {0}: {1}'.format(
                    couple_id, e))
                continue

        logger.info('Successfully created {0} recover dc jobs'.format(created_jobs))

    def sync_historic_data(self, recover_ts, cleanup_ts):
        """
        Update mongo DB storing (couple, recore_ts, cleanup_ts)
        Remove legacy records, add fresh ones
        """

        hist_data_couples = set()

        offset = 0
        while True:
            cursor = self.collection.find(
                fields=['couple'],
                sort=[('couple', pymongo.ASCENDING)],
                skip=offset, limit=self.RECOVERY_OP_CHUNK)
            count = 0
            for rdc in cursor:
                hist_data_couples.add(rdc['couple'])
                count += 1
            offset += count

            if count < self.RECOVERY_OP_CHUNK:
                break

        storage_couples = set(str(c) for c in storage.replicas_groupsets.keys())

        add_couples = list(storage_couples - hist_data_couples)
        remove_couples = list(hist_data_couples - storage_couples)

        logger.info('Couples to add to recover data list: {0}'.format(add_couples))
        logger.info('Couples to remove from recover data list: {0}'.format(remove_couples))

        offset = 0
        while offset < len(add_couples):
            bulk_op = self.collection.initialize_unordered_bulk_op()
            bulk_add_couples = add_couples[offset:offset + self.RECOVERY_OP_CHUNK]
            for couple in bulk_add_couples:
                bulk_op.insert({'couple': couple,
                                'recover_ts': recover_ts,
                                'ttl_cleanup_ts': cleanup_ts})
            res = bulk_op.execute()
            if res['nInserted'] != len(bulk_add_couples):
                raise ValueError('failed to add couples historic data: {0}/{1} ({2})'.format(
                    res['nInserted'], len(bulk_add_couples), res))
            offset += res['nInserted']

        offset = 0
        while offset < len(remove_couples):
            bulk_op = self.collection.initialize_unordered_bulk_op()
            bulk_remove_couples = remove_couples[offset:offset + self.RECOVERY_OP_CHUNK]
            bulk_op.find({'couple': {'$in': bulk_remove_couples}}).remove()
            res = bulk_op.execute()
            if res['nRemoved'] != len(bulk_remove_couples):
                raise ValueError('failed to remove couples historic data: {0}/{1} ({2})'.format(
                    res['nRemoved'], len(bulk_remove_couples), res))
            offset += res['nRemoved']

    def _recover_top_weight_couples(self, count, active_jobs):
        keys_diffs_sorted = []
        keys_diffs = {}
        ts_diffs = {}

        busy_group_ids = self._busy_group_ids(active_jobs)

        for couple in storage.replicas_groupsets.keys():
            if self._is_locked(couple, busy_group_ids):
                continue
            if not _recovery_applicable_couple(couple):
                continue
            c_diff = couple.keys_diff
            keys_diffs_sorted.append((str(couple), c_diff))
            keys_diffs[str(couple)] = c_diff
        keys_diffs_sorted.sort(key=lambda x: x[1])

        cursor = self.collection.find().sort('recover_ts', pymongo.ASCENDING)
        if cursor.count() < len(storage.replicas_groupsets):
            logger.info('Sync recover data is required: {0} records/{1} couples'.format(
                cursor.count(), len(storage.replicas_groupsets)))
            self.sync_historic_data(recover_ts=int(time.time()), cleanup_ts=0)
            cursor = self.collection.find().sort('recover_ts',
                                                 pymongo.ASCENDING)

        ts = int(time.time())

        # by default one key loss is equal to one day without recovery
        keys_cf = self.params.get('recover_dc', {}).get('keys_cf', 86400)
        ts_cf = self.params.get('recover_dc', {}).get('timestamp_cf', 1)

        def weight(keys_diff, ts_diff):
            return keys_diff * keys_cf + ts_diff * ts_cf

        weights = {}
        candidates = []
        for i in xrange(cursor.count() / self.RECOVERY_OP_CHUNK + 1):
            couples_data = cursor[i * self.RECOVERY_OP_CHUNK:
                                  (i + 1) * self.RECOVERY_OP_CHUNK]
            max_recover_ts_diff = None
            for couple_data in couples_data:
                c = couple_data['couple']
                ts_diff = ts - couple_data['recover_ts']
                ts_diffs[c] = ts_diff
                if c not in keys_diffs:
                    # couple was not applicable for recovery, skip
                    continue
                weights[c] = weight(keys_diffs[c], ts_diffs[c])
                max_recover_ts_diff = ts_diff

            cursor.rewind()

            top_candidates_len = min(count, len(weights))

            if not top_candidates_len:
                continue

            # TODO: Optimize this
            candidates = sorted(weights.iteritems(), key=lambda x: x[1])
            min_weight_candidate = candidates[-top_candidates_len]
            min_keys_diff = min(
                keys_diffs[candidate[0]]
                for candidate in candidates[-top_candidates_len:])

            missed_candidate = None
            idx = len(keys_diffs_sorted) - 1
            while idx >= 0 and keys_diffs_sorted[idx] >= min_keys_diff:
                c, keys_diff = keys_diffs_sorted[idx]
                idx -= 1
                if c in ts_diffs:
                    continue
                if weight(keys_diff, max_recover_ts_diff) > min_weight_candidate[1]:
                    # found possible candidate
                    missed_candidate = c
                    break

            logger.debug(
                'Current round: {0}, current min weight candidate '
                '{1}, weight: {2}, possible missed candidate is couple '
                '{3}, keys diff: {4} (max recover ts diff = {5})'.format(
                    i, min_weight_candidate[0], min_weight_candidate[1],
                    missed_candidate, keys_diffs.get(missed_candidate),
                    max_recover_ts_diff))

            if missed_candidate is None:
                break

        logger.info('Top candidates: {0}'.format(candidates[-count:]))

        return [candidate[0] for candidate in candidates[-count:]]

    def update_historic_ts(self, couple_id, recover_ts=None, cleanup_ts=None):
        """
        Update records in mongo with corresponding times
        :param couple_id: couple_id
        :param recover_ts: epoch time if we need to update recover_ts time else None
        :param cleanup_ts: epoch time if we need to update ttl_cleanup time else None
        """

        updated_values = {}
        if recover_ts:
            updated_values['recover_ts'] = int(recover_ts)
        if cleanup_ts:
            updated_values['ttl_cleanup_ts'] = int(cleanup_ts)

        if len(updated_values) == 0:
            return

        res = self.collection.update(
            {'couple': couple_id},
            {"$set": updated_values},
            upsert=True)
        if res['ok'] != 1:
            logger.error('Unexpected mongo response during update of historic data: {0}'.format(res))
            raise RuntimeError('Mongo operation result: {0}'.format(res['ok']))

    def update_recover_ts(self, couple_id, ts):
        self.update_historic_ts(couple_id, recover_ts=ts)

    def update_cleanup_ts(self, couple_id, ts):
        self.update_historic_ts(couple_id, cleanup_ts=ts)

    def _couple_defrag(self):
        try:
            start_ts = time.time()
            logger.info('Starting couple defrag planner')

            max_defrag_jobs = config.get('jobs', {}).get(
                'couple_defrag_job', {}).get('max_executing_jobs', 3)
            # prechecking for new or pending tasks
            count = self.job_processor.job_finder.jobs_count(
                types=jobs.JobTypes.TYPE_COUPLE_DEFRAG_JOB,
                statuses=[jobs.Job.STATUS_NOT_APPROVED,
                          jobs.Job.STATUS_NEW,
                          jobs.Job.STATUS_EXECUTING])

            if count >= max_defrag_jobs:
                logger.info('Found {0} unfinished couple defrag jobs '
                            '(>= {1})'.format(count, max_defrag_jobs))
                return

            with sync_manager.lock(Planner.COUPLE_DEFRAG_LOCK, blocking=False):
                self._do_couple_defrag()

        except LockFailedError:
            logger.info('Couple defrag planner is already running')
        except Exception:
            logger.exception('Couple defrag planner failed')
        finally:
            logger.info('Couple defrag planner finished, time: {:.3f}'.format(
                time.time() - start_ts))
            self.__tq.add_task_at(
                self.COUPLE_DEFRAG,
                self.couple_defrag_timer.next(),
                self._couple_defrag)

    def _do_couple_defrag(self):

        max_defrag_jobs = config.get('jobs', {}).get(
            'couple_defrag_job', {}).get('max_executing_jobs', 3)

        active_jobs = self.job_processor.job_finder.jobs(
            statuses=jobs.Job.ACTIVE_STATUSES,
            sort=False,
        )
        slots = self._jobs_slots(active_jobs,
                                 jobs.JobTypes.TYPE_COUPLE_DEFRAG_JOB,
                                 max_defrag_jobs)

        if slots <= 0:
            logger.info('Found {0} unfinished couple defrag jobs'.format(
                max_defrag_jobs - slots))
            return

        busy_group_ids = self._busy_group_ids(active_jobs)

        couples_to_defrag = []
        for couple in storage.replicas_groupsets.keys():
            if self._is_locked(couple, busy_group_ids):
                continue
            if couple.status not in storage.GOOD_STATUSES:
                continue
            couple_stat = couple.get_stat()
            if couple_stat.files_removed_size == 0:
                continue

            insufficient_space_nb = None
            want_defrag = False

            for group in couple.groups:
                for nb in group.node_backends:
                    if nb.stat.vfs_free_space < nb.stat.max_blob_base_size * 2:
                        insufficient_space_nb = nb
                        break
                if insufficient_space_nb:
                    break
                want_defrag |= group.want_defrag

            if not want_defrag:
                continue

            if insufficient_space_nb:
                logger.warn(
                    'Couple {0}: node backend {1} has insufficient '
                    'free space for defragmentation, max_blob_size {2}, vfs free_space {3}'.format(
                        str(couple), str(nb), nb.stat.max_blob_base_size, nb.stat.vfs_free_space))
                continue

            logger.info('Couple defrag candidate: {}, max files_removed_size in groups: {}'.format(
                str(couple), couple_stat.files_removed_size))

            couples_to_defrag.append((str(couple), couple_stat.files_removed_size))

        couples_to_defrag.sort(key=lambda c: c[1])
        need_approving = not self.params.get('couple_defrag', {}).get('autoapprove', False)

        if not couples_to_defrag:
            logger.info('No couples to defrag found')
            return

        created_jobs = 0
        logger.info('Trying to create {0} jobs'.format(slots))

        while couples_to_defrag and created_jobs < slots:
            couple_tuple, fragmentation = couples_to_defrag.pop()

            try:
                job = self.job_processor._create_job(
                    jobs.JobTypes.TYPE_COUPLE_DEFRAG_JOB,
                    {'couple': couple_tuple,
                     'need_approving': need_approving,
                     'is_cache_couple': False})
                logger.info('Created couple defrag job for couple {0}, job id {1}'.format(
                    couple_tuple, job.id))
                created_jobs += 1
            except Exception as e:
                logger.error('Failed to create couple defrag job: {0}'.format(e))
                continue

        logger.info('Successfully created {0} couple defrag jobs'.format(created_jobs))

    @h.concurrent_handler
    def restore_group(self, request):
        logger.info('----------------------------------------')
        logger.info('New restore group request: ' + str(request))

        if len(request) < 1:
            raise ValueError('Group id is required')

        try:
            group_id = int(request[0])
            if group_id not in storage.groups:
                raise ValueError
        except (TypeError, ValueError):
            raise ValueError('Group {0} is not found'.format(request[0]))

        try:
            use_uncoupled_group = bool(request[1])
        except (TypeError, ValueError, IndexError):
            use_uncoupled_group = False

        try:
            options = request[2]
        except IndexError:
            options = {}

        try:
            force = bool(request[3])
        except IndexError:
            force = False

        return self.create_restore_job(group_id, use_uncoupled_group, options.get('src_group'), force, autoapprove=False)

    def create_restore_job(self, group_id, use_uncoupled_group, src_group, force, autoapprove=False):
        group = storage.groups[group_id]
        involved_groups = [group]
        involved_groups.extend(g for g in group.coupled_groups)
        self.node_info_updater.update_status(involved_groups)

        job_params = {'group': group.group_id}
        job_params['need_approving'] = not autoapprove

        if len(group.node_backends) > 1:
            raise ValueError('Group {0} has {1} node backends, should have at most one'.format(
                group.group_id, len(group.node_backends)))

        if group.couple is None:
            raise ValueError('Group {0} is uncoupled'.format(group.group_id))

        candidates = []
        if src_group is None:
            for g in group.coupled_groups:
                if len(g.node_backends) != 1:
                    continue
                if not all(nb.status == storage.Status.OK for nb in g.node_backends):
                    continue
                candidates.append(g)

            if not candidates:
                raise ValueError(
                    'Group {0} cannot be restored, no suitable coupled groups are found'.format(
                        group.group_id))

            candidates.sort(key=lambda g: g.get_stat().files, reverse=True)
            src_group = candidates[0]

        else:
            src_group = storage.groups[src_group]

            if src_group not in group.couple.groups:
                raise ValueError(
                    'Group {0} cannot be restored from group {1}, '
                    'it is not member of a couple'.format(group.group_id, src_group.group_id))

        job_params['src_group'] = src_group.group_id

        if use_uncoupled_group:
            old_hostname = None
            group_history = infrastructure.get_group_history(group.group_id)
            if len(group_history.nodes) and len(group_history.nodes[-1].set):
                old_hostname = group_history.nodes[-1].set[0].hostname
            if old_hostname:
                uncoupled_groups = self.find_uncoupled_groups(
                    group=group,
                    host_addr=cache.get_ip_address_by_host(old_hostname)
                )
            else:
                uncoupled_groups = self.select_uncoupled_groups(group)
            job_params['uncoupled_group'] = uncoupled_groups[0].group_id
            job_params['merged_groups'] = [g.group_id for g in uncoupled_groups[1:]]

        job = self.job_processor._create_job(
            jobs.JobTypes.TYPE_RESTORE_GROUP_JOB,
            job_params, force=force)

        return job.dump()

    CREATE_JOB_ATTEMPTS = 3
    RUNNING_STATUSES = [jobs.Job.STATUS_NOT_APPROVED,
                        jobs.Job.STATUS_NEW,
                        jobs.Job.STATUS_EXECUTING]
    ERROR_STATUSES = [jobs.Job.STATUS_PENDING,
                      jobs.Job.STATUS_BROKEN]
    RESTORE_TYPES = [jobs.JobTypes.TYPE_MOVE_JOB,
                     jobs.JobTypes.TYPE_RESTORE_GROUP_JOB]
    RESTORE_AND_MANAGER_TYPES = [jobs.JobTypes.TYPE_MOVE_JOB,
                                 jobs.JobTypes.TYPE_RESTORE_GROUP_JOB,
                                 jobs.JobTypes.TYPE_BACKEND_MANAGER_JOB]

    def _create_restore_job(self, group, use_uncoupled_group, src_group, force, autoapprove):
        last_error = None
        for _ in xrange(self.CREATE_JOB_ATTEMPTS):
            try:
                return self.create_restore_job(group, use_uncoupled_group, src_group, force, autoapprove)
            except Exception as e:
                last_error = e
                continue
        if last_error:
                raise last_error

    def _create_backend_cleanup_job(self, group, force, autoapprove):
        last_error = None
        for _ in xrange(self.CREATE_JOB_ATTEMPTS):
            try:
                couple = storage.groups[group].couple
                job = self.job_processor._create_job(
                    jobs.JobTypes.TYPE_BACKEND_CLEANUP_JOB,
                    params={
                        'group': group,
                        'couple': str(couple) if couple else None,
                        'need_approving': not autoapprove
                    },
                    force=force,
                )
            except Exception as e:
                last_error = e
                continue

            return job.dump()

        if last_error:
            raise last_error

    def _create_backend_manager_job(self, group, force, autoapprove, cmd_type, mark_backend=None, unmark_backend=None):
        couple = storage.groups[group].couple
        params = {
            'group': group,
            'couple': str(couple) if couple else None,
            'need_approving': not autoapprove,
            'cmd_type': cmd_type,
            'mark_backend': mark_backend,
            'unmark_backend': unmark_backend,
        }
        last_error = None
        for _ in xrange(self.CREATE_JOB_ATTEMPTS):
            try:
                job = self.job_processor._create_job(
                    jobs.JobTypes.TYPE_BACKEND_MANAGER_JOB,
                    params,
                    force=force,
                )
                return job.dump()
            except Exception as e:
                last_error = e
                continue
        if last_error:
            raise last_error

    def _cleanup_job_find(self, group_id, active_jobs, uncoupled_groups, failed):
        group_jobs = self.job_processor.job_finder.jobs(
            groups=group_id,
            types=jobs.JobTypes.TYPE_BACKEND_CLEANUP_JOB,
            statuses=jobs.Job.ACTIVE_STATUSES,
            sort=False,
        )
        if group_jobs:
            job = group_jobs[0]

            if job.status in self.RUNNING_STATUSES:
                active_jobs.append(job.id)
            else:
                failed[group_id] = 'Backend cleanup job failed, group: {}, status: {}'.format(group_id,
                                                                                              job.status
                                                                                              )
        else:
            uncoupled_groups.append(group_id)

        return active_jobs, uncoupled_groups

    def _restore_job_find(self,
                          group_id,
                          active_jobs,
                          groups_to_backup,
                          groups_to_restore,
                          cancelled_jobs,
                          pending_restore_jobs,
                          restore_only=False
                          ):

        group_jobs = self.job_processor.job_finder.jobs(
            groups=group_id,
            types=self.RESTORE_AND_MANAGER_TYPES,
            statuses=jobs.Job.ACTIVE_STATUSES,
            sort=False,
        )
        if group_jobs:
            job = group_jobs[0]

            if job.status in self.RUNNING_STATUSES:
                active_jobs.append(job.id)
            elif job.status in self.ERROR_STATUSES:
                cancel_job = False
                if job.type == jobs.JobTypes.TYPE_MOVE_JOB:
                    cancel_job = True
                elif job.type == jobs.JobTypes.TYPE_BACKEND_MANAGER_JOB:
                    cancel_job = False
                elif job.group == job.src_group:
                    cancel_job = True
                if cancel_job:
                    try:
                        self.job_processor._cancel_job(job)
                        cancelled_jobs.append(job.id)
                    except Exception as e:
                        logger.exception('Failed to cancel job {}'.format(job.id))
                        raise ValueError('Failed to cancel job {}: {}'.format(job.id, e))
                    groups_to_restore.append(group_id)
                else:
                    # Jobs in pending or broken states that were
                    # working with fallback can be restored only
                    # manually
                    pending_restore_jobs.append(job.id)
            elif job.status == jobs.Job.STATUS_CANCELLED:
                groups_to_restore.append(group_id)
            else:
                raise ValueError(
                    'Unknown job status: {}'.format(job.status)
                )
        else:
            if restore_only:
                groups_to_restore.append(group_id)
            else:
                groups_to_backup.append(group_id)
        return active_jobs, groups_to_backup, groups_to_restore, cancelled_jobs, pending_restore_jobs

    @h.concurrent_handler
    def restore_groups_from_path(self, request):
        logger.info('----------------------------------------')
        logger.info('New restore groups from path request: ' + str(request))

        if 'src_host' not in request:
            raise ValueError('Source host is required')

        if 'path' not in request:
            raise ValueError('Path is required')

        if 'uncoupled_group' not in request:
            raise ValueError('Uncoupled_groups is required')

        if 'force' not in request:
            raise ValueError('Force is required')

        host_or_ip = request['src_host']
        try:
            hostname = socket.getfqdn(host_or_ip)
        except Exception as e:
            raise ValueError('Failed to get hostname for {0}: {1}'.format(host_or_ip, e))

        try:
            ips = h.ips_set(hostname)
        except Exception as e:
            raise ValueError('Failed to get ip list for {0}: {1}'.format(hostname, e))

        if '*' in request['path']:
            path = os.path.normpath(request['path']) + '/'
        else:
            path = os.path.normpath(request['path']) + '/*/'
        path = GroupHistoryFinder.node_backend_path_to_regexp(path)
        path_re = re.compile(path)

        try:
            use_uncoupled_group = bool(request['uncoupled_group'])
        except (TypeError, ValueError):
            use_uncoupled_group = False

        force = bool(request.get('force', False))

        autoapprove = bool(request.get('autoapprove', False))

        groups = set()
        for group in storage.groups:
            for backend in group.node_backends:
                if backend.node.host.addr in ips and path_re.match(backend.base_path):
                    if len(group.node_backends) == 1:
                        groups.add(group.group_id)
                    else:
                        raise ValueError(
                            'Group {} has {} node backends, currently '
                            'only groups with 1 node backend can be used'.format(
                                group.group_id, len(group.node_backends)))

        groups_to_backup = []
        groups_to_restore = []
        active_jobs = []
        uncoupled_groups = []
        cancelled_jobs = []
        pending_restore_jobs = []
        failed = {}

        for group in groups:
            group = storage.groups[group]
            if group.type not in [storage.Group.TYPE_UNKNOWN,
                                  storage.Group.TYPE_DATA,
                                  storage.Group.TYPE_UNCOUPLED,
                                  storage.Group.TYPE_UNCOUPLED_LRC_8_2_2_V1,
                                  storage.Group.TYPE_RESERVED_LRC_8_2_2_V1]:
                failed[group.group_id] = 'Failed group type: {}'.format(group.type)
                continue
            if group.couple is None:
                self._cleanup_job_find(group.group_id, active_jobs, uncoupled_groups, failed)
            else:
                self._restore_job_find(group.group_id,
                                       active_jobs,
                                       groups_to_backup,
                                       groups_to_restore,
                                       cancelled_jobs,
                                       pending_restore_jobs)

        #
        group_histories = infrastructure.group_history_finder.search_by_node_backend(
            hostname=hostname,
            path=path
        )
        start_idx = -1
        history_groups = set()
        for group_history in group_histories:
            for node_backends_set in group_history.nodes[start_idx:]:
                for node_backend in node_backends_set.set:
                    if not node_backend.path:
                        continue

                    if path_re.match(node_backend.path) is not None and node_backend.hostname == hostname:
                        history_groups.add(group_history.group_id)
                        break

        history_groups.difference_update(groups)
        for group in history_groups:
            group = [x for x in group_histories if x.group_id == group][0]

            # [2017-01-24 21:33:22] (xxx.example.com:1025:10/307
            # /srv/storage/1/8/,xxx.example.com:1025:10/13458
            # /srv/storage/45/3/)
            if len(group.nodes[-1].set) > 1:
                failed[group.group_id] = 'Count nodes in history > 1'
                continue

            if group not in storage.groups and not group.couples:
                self._cleanup_job_find(group.group_id, active_jobs, uncoupled_groups, failed)
            else:
                if len(group.couples) > 1:
                    failed[group.group_id] = 'Count couples in history > 1'
                    continue
                else:
                    couple = group.couples[0].couple

                if len(couple) == storage.Lrc.Scheme822v1.STRIPE_SIZE:
                    failed[group.group_id] = 'Failed group type: {}'.format('lrc')
                else:
                    self._restore_job_find(group.group_id,
                                           active_jobs,
                                           groups_to_backup,
                                           groups_to_restore,
                                           cancelled_jobs,
                                           pending_restore_jobs,
                                           restore_only=True)

        for group in groups_to_backup:
            try:
                job = self._create_restore_job(group, use_uncoupled_group, group, force, autoapprove)
                active_jobs.append(job['id'])
            except Exception as e:
                failed[group] = str(e)

        for group in groups_to_restore:
            try:
                job = self._create_restore_job(group, use_uncoupled_group, None, force, autoapprove)
                active_jobs.append(job['id'])
            except Exception as e:
                failed[group] = str(e)

        for group in uncoupled_groups:
            try:
                job = self._create_backend_cleanup_job(group, force, autoapprove)
                active_jobs.append(job['id'])
            except Exception as e:
                failed[group] = str(e)

        return {'jobs': active_jobs, 'failed': failed, 'cancelled_jobs': cancelled_jobs, 'help': pending_restore_jobs}

    MOVE_GROUP_ATTEMPTS = 3

    @h.concurrent_handler
    def move_groups_from_host(self, request):
        try:
            host_or_ip = request[0]
        except IndexError:
            raise ValueError('Host is required')

        try:
            hostname = socket.getfqdn(host_or_ip)
        except Exception as e:
            raise ValueError('Failed to get hostname for {0}: {1}'.format(host_or_ip, e))

        try:
            ips = h.ips_set(hostname)
        except Exception as e:
            raise ValueError('Failed to get ip list for {0}: {1}'.format(hostname, e))

        res = {
            'jobs': [],
            'failed': {},
        }

        for couple in storage.replicas_groupsets.keys():
            for group in couple.groups:
                if len(group.node_backends) != 1:
                    # skip group if more than one backend
                    continue

                if group.node_backends[0].node.host.addr in ips:

                    attempts = self.MOVE_GROUP_ATTEMPTS
                    while attempts > 0:
                        try:
                            job_params = self._get_move_group_params(group)
                            job = self.job_processor._create_job(
                                jobs.JobTypes.TYPE_MOVE_JOB,
                                job_params, force=True)
                            res['jobs'].append(job.dump())
                        except LockAlreadyAcquiredError as e:
                            logger.error(
                                'Failed to create move job for group {}, attempt {}/{}'.format(
                                    group.group_id,
                                    self.MOVE_GROUP_ATTEMPTS - attempts + 1,
                                    self.MOVE_GROUP_ATTEMPTS))
                            res['failed'][group.group_id] = str(e)
                            pass
                        except Exception as e:
                            logger.exception(
                                'Failed to create move job for group {}, attempt {}/{}'.format(
                                    group.group_id,
                                    self.MOVE_GROUP_ATTEMPTS - attempts + 1,
                                    self.MOVE_GROUP_ATTEMPTS))
                            res['failed'][group.group_id] = str(e)
                            pass
                        else:
                            break
                        finally:
                            attempts -= 1

        return res

    @h.concurrent_handler
    def move_group(self, request):
        logger.info('----------------------------------------')
        logger.info('New move group request: ' + str(request))

        if len(request) < 1:
            raise ValueError('Group id is required')

        try:
            group_id = int(request[0])
            if group_id not in storage.groups:
                raise ValueError
        except (TypeError, ValueError):
            raise ValueError('Group {0} is not found'.format(request[0]))

        group = storage.groups[group_id]

        try:
            options = request[1]
        except IndexError:
            options = {}

        try:
            force = bool(request[2])
        except IndexError:
            force = False

        uncoupled_groups = []
        for gid in options.get('uncoupled_groups') or []:
            if gid not in storage.groups:
                raise ValueError('Uncoupled group {0} is not found'.format(gid))
            uncoupled_groups.append(storage.groups[gid])

        job_params = self._get_move_group_params(group, uncoupled_groups=uncoupled_groups)

        job = self.job_processor._create_job(
            jobs.JobTypes.TYPE_MOVE_JOB,
            job_params, force=force)

        return job.dump()

    def _get_move_group_params(self, group, uncoupled_groups=None):

        involved_groups = [group]
        involved_groups.extend(g for g in group.coupled_groups)
        self.node_info_updater.update_status(involved_groups)

        if len(group.node_backends) != 1:
            raise ValueError(
                'Group {0} has {1} node backends, currently '
                'only groups with 1 node backend can be used'.format(
                    group.group_id, len(group.node_backends)))

        src_backend = group.node_backends[0]

        if not uncoupled_groups:
            uncoupled_groups = self.select_uncoupled_groups(group)
            logger.info('Group {0} will be moved to uncoupled groups {1}'.format(
                group.group_id, [g.group_id for g in uncoupled_groups]))
        else:
            dc, fsid = None, None
            self.node_info_updater.update_status(uncoupled_groups)
            locked_hosts = manual_locker.get_locked_hosts()
            for unc_group in uncoupled_groups:
                if len(unc_group.node_backends) != 1:
                    raise ValueError(
                        'Group {0} has {1} node backends, currently '
                        'only groups with 1 node backend can be used'.format(
                            unc_group.group_id, len(unc_group.node_backends)))

                selector = UncoupledGroupsSelector(
                    groups=[unc_group],
                    max_node_backends=1,
                    locked_hosts=locked_hosts,
                )
                if not selector.select():
                    raise ValueError(
                        'Uncoupled group {} is not applicable'.format(unc_group.group_id)
                    )

                nb = unc_group.node_backends[0]
                try:
                    host_dc = nb.node.host.dc
                except CacheUpstreamError:
                    raise RuntimeError('Failed to get dc for host {}'.format(
                        nb.node.host))
                if not dc:
                    dc, fsid = host_dc, nb.fs.fsid
                elif dc != host_dc or fsid != nb.fs.fsid:
                    raise ValueError(
                        'All uncoupled groups should be located on a single hdd on the same host')

                if unc_group.couple or unc_group.status != storage.Status.INIT:
                    raise ValueError('Group {0} is not uncoupled'.format(unc_group.group_id))

        uncoupled_group, merged_groups = uncoupled_groups[0], uncoupled_groups[1:]

        dst_backend = uncoupled_group.node_backends[0]
        if dst_backend.status != storage.Status.OK:
            raise ValueError('Group {0} node backend {1} status is {2}, should be {3}'.format(
                uncoupled_group.group_id, dst_backend, dst_backend.status, storage.Status.OK))

        if config.get('forbidden_dc_sharing_among_groups', False):
            dcs = set()
            for g in group.coupled_groups:
                dcs.update(h.hosts_dcs(nb.node.host for nb in g.node_backends))
            try:
                dst_dc = dst_backend.node.host.dc
            except CacheUpstreamError:
                raise RuntimeError('Failed to get dc for host {}'.format(
                    dst_backend.node.host))
            if dst_dc in dcs:
                raise ValueError(
                    'Group {0} cannot be moved to uncoupled group {1}, '
                    'couple {2} already has groups in dc {3}'.format(
                        group.group_id, uncoupled_group.group_id,
                        group.couple, dst_dc))

        job_params = {'group': group.group_id,
                      'uncoupled_group': uncoupled_group.group_id,
                      'merged_groups': [g.group_id for g in merged_groups],
                      'src_host': src_backend.node.host.addr,
                      'src_port': src_backend.node.port,
                      'src_family': src_backend.node.family,
                      'src_backend_id': src_backend.backend_id,
                      'src_base_path': src_backend.base_path,
                      'dst_host': dst_backend.node.host.addr,
                      'dst_port': dst_backend.node.port,
                      'dst_family': dst_backend.node.family,
                      'dst_backend_id': dst_backend.backend_id,
                      'dst_base_path': dst_backend.base_path}

        return job_params

    def account_job(self, ns_current_state, types, job, ns):

        add_groups = []
        remove_groups = []
        uncoupled_group_in_job = False

        try:
            group = storage.groups[job.group]
        except KeyError:
            logger.error('Group {0} is not found in storage (job {1})'.format(
                job.group, job.id))
            return

        try:
            if group.couple.namespace != ns:
                return
        except Exception:
            return

        if getattr(job, 'uncoupled_group', None) is not None:
            uncoupled_group_in_job = True
            try:
                uncoupled_group = storage.groups[job.uncoupled_group]
            except KeyError:
                logger.warn('Uncoupled group {0} is not found in storage (job {1})'.format(
                    job.uncoupled_group, job.id))
                pass
            else:
                uncoupled_group_fsid = job.uncoupled_group_fsid
                if uncoupled_group_fsid is None:
                    uncoupled_group_fsid = (
                        uncoupled_group.node_backends and
                        uncoupled_group.node_backends[0].fs.fsid or None)
                if uncoupled_group_fsid:
                    add_groups.append((uncoupled_group, uncoupled_group_fsid))
                else:
                    logger.warn('Uncoupled group {0} has empty backend list (job {1})'.format(
                        job.uncoupled_group, job.id))

        # GOOD_STATUSES includes FULL status, but ns_current_state only accounts OK couples!
        # Change behaviour
        if group.couple.status not in storage.GOOD_STATUSES:
            for g in group.coupled_groups:
                for fsid in set(nb.fs.fsid for nb in g.node_backends):
                    add_groups.append((g, fsid))

        if uncoupled_group_in_job and group.couple.status in storage.GOOD_STATUSES:
            remove_groups.append((group, group.node_backends[0].fs.fsid))

        if (job.type == jobs.JobTypes.TYPE_RESTORE_GROUP_JOB and
                not uncoupled_group_in_job and
                group.couple.status not in storage.GOOD_STATUSES):

            # try to add restoring group backend if mastermind knows about it
            if group.node_backends:
                add_groups.append((group, group.node_backends[0].fs.fsid))

        # applying accounting
        for (group, fsid), diff in zip(
                remove_groups + add_groups,
                [-1] * len(remove_groups) + [1] * len(add_groups)):

            logger.debug('Accounting group {0}, fsid {1}, diff {2}'.format(
                group.group_id, fsid, diff))

            if not group.node_backends:
                logger.debug('No node backend for group {0}, job for group {1}'.format(
                    group.group_id, job.group))
                continue

            try:
                cur_node = group.node_backends[0].node.host.parents
            except CacheUpstreamError:
                logger.warn('Skipping {} because of cache failure'.format(
                    group.node_backends[0].node.host))
                continue

            parts = []
            parent = cur_node
            while parent:
                parts.insert(0, parent['name'])
                parent = parent.get('parent')

            while cur_node:
                if cur_node['type'] in types:
                    ns_current_type_state = ns_current_state[cur_node['type']]
                    full_path = '|'.join(parts)
                    ns_current_type_state['nodes'][full_path] += diff
                    ns_current_type_state['avg'] += float(diff) / len(
                        ns_current_type_state['nodes'])

                    if cur_node['type'] == 'host' and 'hdd' in types:
                        hdd_path = full_path + '|' + str(fsid)
                        ns_hdd_type_state = ns_current_state['hdd']
                        if hdd_path in ns_hdd_type_state['nodes']:
                            ns_hdd_type_state['nodes'][hdd_path] += diff
                            ns_hdd_type_state['avg'] += float(diff) / len(
                                ns_hdd_type_state['nodes'])
                        else:
                            logger.warn('Hdd path {0} is not found under host {1}'.format(
                                hdd_path, full_path))
                parts.pop()
                cur_node = cur_node.get('parent')

    def get_suitable_uncoupled_groups_list(self, group, uncoupled_groups, busy_group_ids=None):
        logger.debug('{0}, {1}'.format(group.group_id, [g.group_id for g in group.coupled_groups]))
        stats = [g.get_stat() for g in group.couple.groups if g.node_backends]
        if not stats:
            raise RuntimeError('Cannot determine group space requirements')
        required_ts = max(st.total_space for st in stats)
        groups_by_fs = {}

        busy_group_ids = busy_group_ids or set(
            self.job_processor.job_finder.get_uncoupled_groups_in_service())

        logger.info('Busy uncoupled groups: {0}'.format(busy_group_ids))

        forbidden_dcs = set()
        if config.get('forbidden_dc_sharing_among_groups', False):
            forbidden_dcs = set(h.hosts_dcs(nb.node.host
                                            for g in group.coupled_groups
                                            for nb in g.node_backends))

        for g in uncoupled_groups:
            try:
                dc = g.node_backends[0].node.host.dc
            except CacheUpstreamError:
                continue
            if g.group_id in busy_group_ids or dc in forbidden_dcs:
                continue
            fs = g.node_backends[0].fs
            groups_by_fs.setdefault(fs, []).append(g)

        candidates = []

        for fs, groups in groups_by_fs.iteritems():
            fs_candidates = []
            gs = sorted(groups, key=lambda g: g.group_id)

            candidate = []
            ts = 0
            for g in gs:
                candidate.append(g)
                ts += g.get_stat().total_space
                if ts >= required_ts:
                    fs_candidates.append((candidate, ts))
                    candidate = []
                    ts = 0

            fs_candidates = sorted(fs_candidates, key=lambda c: (len(c[0]), c[1]))
            if fs_candidates:
                candidates.append(fs_candidates[0][0])

        return candidates

    def find_uncoupled_groups(self, group, host_addr):
        try:
            old_host_tree = cache.get_host_tree(cache.get_hostname_by_addr(host_addr))
        except CacheUpstreamError as e:
            raise RuntimeError('Failed to resolve host {}: {}'.format(host_addr, e))
        logger.info('Old host tree: {0}'.format(old_host_tree))

        uncoupled_groups = infrastructure.get_good_uncoupled_groups(max_node_backends=1)
        groups_by_dc = {}
        for unc_group in uncoupled_groups:
            try:
                dc = unc_group.node_backends[0].node.host.dc
            except CacheUpstreamError:
                logger.warn('Skipping group {} because of cache failure'.format(
                    unc_group))
                continue
            groups_by_dc.setdefault(dc, []).append(unc_group)

        logger.info('Uncoupled groups by dcs: {0}'.format(
            [(g_dc, len(groups)) for g_dc, groups in groups_by_dc.iteritems()]))

        dc_node_type = inventory.get_dc_node_type()
        weights = {}

        candidates = self.get_suitable_uncoupled_groups_list(group, uncoupled_groups)
        for candidate in candidates:
            try:
                host_tree = candidate[0].node_backends[0].node.host.parents
            except CacheUpstreamError:
                logger.warn('Skipping {} because of cache failure'.format(
                    candidate[0]))
                continue

            diff = 0
            dc = None
            dc_change = False
            old_node = old_host_tree
            node = host_tree
            while old_node:
                if old_node['name'] != node['name']:
                    diff += 1
                if node['type'] == dc_node_type:
                    dc = node['name']
                    dc_change = old_node['name'] != node['name']
                old_node = old_node.get('parent')
                node = node.get('parent')

            weights[tuple(candidate)] = (dc_change, diff, -len(groups_by_dc[dc]))

        sorted_weights = sorted(weights.items(), key=lambda wi: wi[1])
        if not sorted_weights:
            raise RuntimeError('No suitable uncoupled groups found')

        top_candidate = sorted_weights[0][0]

        logger.info('Top candidate: {0}'.format(
            [g.group_id for g in top_candidate]))

        return top_candidate

    def select_uncoupled_groups(self, group):
        if len(group.node_backends) > 1:
            raise ValueError('Group {0} should have no more than 1 backend'.format(group.group_id))

        types = ['root'] + inventory.get_balancer_node_types() + ['hdd']
        tree, nodes = infrastructure.filtered_cluster_tree(types)

        couple = group.couple
        ns = couple.namespace.id

        infrastructure.account_ns_couples(tree, nodes, ns)
        infrastructure.update_groups_list(tree)

        uncoupled_groups = infrastructure.get_good_uncoupled_groups(max_node_backends=1)
        candidates = self.get_suitable_uncoupled_groups_list(group, uncoupled_groups)
        logger.debug('Candidate groups for group {0}: {1}'.format(
            group.group_id,
            [[g.group_id for g in c] for c in candidates]))

        units = infrastructure.groups_units(uncoupled_groups, types)

        ns_current_state = infrastructure.ns_current_state(nodes, types[1:])
        logger.debug('Current ns {0} dc state: {1}'.format(
            ns, ns_current_state[inventory.get_dc_node_type()]))

        active_jobs = self.job_processor.job_finder.jobs(
            types=(jobs.JobTypes.TYPE_MOVE_JOB,
                   jobs.JobTypes.TYPE_RESTORE_GROUP_JOB),
            statuses=(jobs.Job.STATUS_NOT_APPROVED,
                      jobs.Job.STATUS_NEW,
                      jobs.Job.STATUS_EXECUTING,
                      jobs.Job.STATUS_PENDING,
                      jobs.Job.STATUS_BROKEN),
            sort=False)

        def log_ns_current_state_diff(ns1, ns2, tpl):
            node_type = 'hdd'
            for k in ns1[node_type]['nodes']:
                if ns1[node_type]['nodes'][k] != ns2[node_type]['nodes'][k]:
                    logger.debug(tpl.format(
                        k, ns1[node_type]['nodes'][k], ns2[node_type]['nodes'][k]))

        for job in active_jobs:
            cmp_ns_current_state = deepcopy(ns_current_state)
            self.account_job(ns_current_state, types, job, ns)
            log_ns_current_state_diff(
                cmp_ns_current_state, ns_current_state,
                'Applying job {0}, path {{0}}, old value: {{1}}, new value: {{2}}'.format(job.id))

        if not candidates:
            raise RuntimeError('No suitable uncoupled groups found')

        for level_type in types[1:]:
            ns_current_type_state = ns_current_state[level_type]

            weights = []
            for candidate in candidates:
                weight_nodes = copy(ns_current_type_state['nodes'])
                units_set = set()
                for merged_unc_group in candidate:
                    for nb_unit in units[merged_unc_group.group_id]:
                        units_set.add(nb_unit[level_type])
                for used_nb_nodes in units_set:
                    weight_nodes[used_nb_nodes] += 1

                weight = sum((c - ns_current_type_state['avg']) ** 2
                             for c in weight_nodes.values())

                heapq.heappush(weights, (weight, candidate))

            max_weight = weights[0][0]
            selected_candidates = []
            while weights:
                c = heapq.heappop(weights)
                if c[0] != max_weight:
                    break
                selected_candidates.append(c[1])

            logger.debug('Level {0}, ns {1}, min weight candidates: {2}'.format(
                level_type, ns, [[g.group_id for g in c_] for c_ in selected_candidates]))

            candidates = selected_candidates

        return candidates[0]

    @h.concurrent_handler
    def ttl_cleanup(self, request):
        """
        TTL cleanup job. Remove all records with expired TTL
        """

        # The function requires some preliminary parameters parsing
        # since it may be there first validation and we would fail
        # with completely unexpected and unpredicted arguments
        if 'iter_group' not in request:
            raise ValueError('Request should contain iter_group field')

        if request['iter_group'] not in storage.groups:
            raise ValueError('A valid iter group is expected')

        try:
            # validate params: they must be either None or numbers
            batch_size = request.get('batch_size')
            if batch_size:
                batch_size = int(batch_size)
        except ValueError:
            raise ValueError('Parameter "batch_size" must be a number')

        try:
            attempts = request.get('attempts')
            if attempts:
                attempts = int(attempts)
        except ValueError:
            raise ValueError('Parameter "attempts" must be a number')

        try:
            nproc = request.get('nproc')
            if nproc:
                nproc = int(nproc)
        except ValueError:
            raise ValueError('Parameter "nproc" must be a number')

        try:
            wait_timeout = request.get('wait_timeout')
            if wait_timeout:
                wait_timeout = int(wait_timeout)
        except ValueError:
            raise ValueError('Parameter "wait_timeout" must be a number')

        try:
            remove_all_older = request.get('remove_all_older')
            if remove_all_older:
                remove_all_older = int(remove_all_older)
        except ValueError:
            raise ValueError('Parameter "remove_all_older" must be a number')

        try:
            remove_permanent_older = request.get('remove_permanent_older')
            if remove_permanent_older:
                remove_permanent_older = int(remove_permanent_older)
        except ValueError:
            raise ValueError('Parameter "remove_permanent_older" must be a number')

        if remove_permanent_older and remove_all_older:
            raise ValueError(
                'Parameters "remove_all_older"({}) and "remove_permanent_older"({}) are '
                'mutually exclusive'.format(
                    remove_all_older,
                    remove_permanent_older
                )
            )

        iter_group = storage.groups[request['iter_group']]
        if not iter_group.couple:
            raise RuntimeError('Group is uncoupled? {}'.format(str(iter_group)))

        job = self.job_processor._create_job(
            job_type=jobs.JobTypes.TYPE_TTL_CLEANUP_JOB,
            params={
                'iter_group': iter_group.group_id,
                'couple': str(iter_group.couple),
                'namespace': iter_group.couple.namespace.id,
                'batch_size': batch_size,
                'attempts': attempts,
                'nproc': nproc,
                'wait_timeout': wait_timeout,
                'dry_run': request.get('dry_run'),
                'remove_all_older': remove_all_older,
                'remove_permanent_older': remove_permanent_older,
                'need_approving': request.get('need_approving'),
            },
        )

        return job.dump()


def _recovery_applicable_couple(couple):
    if couple.status not in storage.GOOD_STATUSES:
        return False
    return True
