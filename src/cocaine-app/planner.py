from copy import copy, deepcopy
import heapq
from logging import getLogger
import socket
import storage
import time
import traceback


import pymongo

from config import config
from db.mongo.pool import Collection
from errors import CacheUpstreamError
import helpers as h
from infrastructure import infrastructure
from infrastructure_cache import cache
import inventory
import jobs
from manual_locks import manual_locker
from sync import sync_manager
from sync.error import LockFailedError, LockAlreadyAcquiredError
from timer import periodic_timer

import timed_queue

logger = getLogger('mm.planner')


class Planner(object):

    MOVE_CANDIDATES = 'move_candidates'
    RECOVER_DC = 'recover_dc'
    COUPLE_DEFRAG = 'couple_defrag'

    RECOVERY_OP_CHUNK = 200

    COUPLE_DEFRAG_LOCK = 'planner/couple_defrag'
    RECOVER_DC_LOCK = 'planner/recover_dc'
    MOVE_LOCK = 'planner/move'

    def __init__(self, meta_session, db, niu, job_processor):

        self.params = config.get('planner', {})

        logger.info('Planner initializing')
        self.candidates = []
        self.meta_session = meta_session
        self.job_processor = job_processor
        self.__max_plan_length = self.params.get('move', {}).get('max_plan_length', 5)
        self.__tq = timed_queue.TimedQueue()

        self.node_info_updater = niu

        self.recover_dc_timer = periodic_timer(
            seconds=self.params.get('recover_dc', {}).get(
                'recover_dc_period', 60 * 15))
        self.couple_defrag_timer = periodic_timer(
            seconds=self.params.get('couple_defrag', {}).get(
                'couple_defrag_period', 60 * 15))

        if config['metadata'].get('planner', {}).get('db'):
            self.collection = Collection(db[config['metadata']['planner']['db']], 'planner')

            if self.params.get('move', {}).get('enabled', False):
                self.__tq.add_task_in(
                    self.MOVE_CANDIDATES,
                    10,
                    self._move_candidates
                )

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

    def _start_tq(self):
        self.__tq.start()

    def _move_candidates(self):
        try:
            logger.info('Starting move candidates planner')

            max_move_jobs = config.get('jobs', {}).get('move_job', {}).get('max_executing_jobs', 3)

            # prechecking for new or pending tasks
            count = self.job_processor.job_finder.jobs_count(
                types=jobs.JobTypes.TYPE_MOVE_JOB,
                statuses=[jobs.Job.STATUS_NOT_APPROVED,
                          jobs.Job.STATUS_NEW,
                          jobs.Job.STATUS_EXECUTING,
                          jobs.Job.STATUS_PENDING])

            if count >= max_move_jobs:
                logger.info('Found {0} unfinished move jobs (>= {1})'.format(count, max_move_jobs))
                return

            with sync_manager.lock(Planner.MOVE_LOCK, blocking=False):
                self._do_move_candidates(max_move_jobs - count)

        except LockFailedError:
            logger.info('Move jobs dc planner is already running')
        except Exception as e:
            logger.error('{0}: {1}'.format(e, traceback.format_exc()))
        finally:
            logger.info('Move candidates planner finished')
            self.__tq.add_task_in(
                self.MOVE_CANDIDATES,
                self.params.get('move', {}).get('generate_plan_period', 1800),
                self._move_candidates)

    def __busy_hosts(self, job_type):
        not_finished_jobs = self.job_processor.job_finder.jobs(types=job_type, statuses=(
            jobs.Job.STATUS_NOT_APPROVED,
            jobs.Job.STATUS_NEW,
            jobs.Job.STATUS_EXECUTING,
            jobs.Job.STATUS_PENDING,
            jobs.Job.STATUS_BROKEN))

        hosts = set()
        # TODO: checking @*_host and @*_group is not universal!
        for job in not_finished_jobs:
            if hasattr(job, 'src_host'):
                hosts.add(job.src_host)
            elif hasattr(job, 'src_group'):
                src_group_id = job.src_group
                if src_group_id in storage.groups:
                    src_group = storage.groups[src_group_id]
                    if src_group.node_backends:
                        hosts.add(src_group.node_backends[0].node.host.addr)
            if hasattr(job, 'dst_host'):
                hosts.add(job.dst_host)
            elif hasattr(job, 'group'):
                dst_group_id = job.group
                if dst_group_id in storage.groups:
                    dst_group = storage.groups[dst_group_id]
                    if dst_group.node_backends:
                        hosts.add(dst_group.node_backends[0].node.host.addr)
        return hosts

    def __apply_plan(self, candidates):

        logger.info('Applying candidates: {0}'.format(candidates))

        try:

            max_move_jobs = config.get('jobs', {}).get(
                'move_job', {}).get('max_executing_jobs', 3)

            # prechecking for new or pending tasks
            jobs_count = self.job_processor.job_finder.jobs_count(
                types=jobs.JobTypes.TYPE_MOVE_JOB,
                statuses=[jobs.Job.STATUS_NOT_APPROVED,
                          jobs.Job.STATUS_NEW,
                          jobs.Job.STATUS_EXECUTING,
                          jobs.Job.STATUS_PENDING])

            if jobs_count >= max_move_jobs:
                raise ValueError('Found {0} unfinished move jobs'.format(
                    jobs_count))

            need_approving = not self.params.get('move', {}).get('autoapprove', False)

            for i, candidate in enumerate(candidates):
                logger.info('Candidate {0}: data {1}, ms_error delta {2}'.format(
                    i, gb(candidate.delta.data_move_size), candidate.delta.ms_error_delta))

                for src_group, src_dc, dst_group, merged_groups, dst_dc in \
                        candidate.moved_groups:

                    try:
                        assert len(src_group.node_backends) == 1, \
                            'Src group {0} should have only 1 node backend'.format(
                                src_group.group_id)
                        assert len(dst_group.node_backends) == 1, \
                            'Dst group {0} should have only 1 node backend'.format(
                                dst_group.group_id)

                        dcs = []
                        for host in (src_group.node_backends[0].node.host,
                                     dst_group.node_backends[0].node.host):
                            try:
                                dcs.append(host.dc)
                            except CacheUpstreamError:
                                raise RuntimeError('Failed to get dc for host {}'.format(
                                    host))

                        src_dc_cnt, dst_dc_cnt = dcs

                        assert src_dc_cnt == src_dc, \
                            'Dc for src group {0} has been changed: {1} != {2}'.format(
                                src_group.group_id, src_dc_cnt, src_dc)
                        assert dst_dc_cnt == dst_dc, \
                            'Dc for dst group {0} has been changed: {1} != {2}'.format(
                                dst_group.group_id, dst_dc_cnt, dst_dc)

                        logger.info('Group {0} ({1}) moves to {2} ({3})'.format(
                            src_group.group_id, src_dc,
                            dst_group.group_id, dst_dc))

                        job = self.job_processor._create_job(
                            jobs.JobTypes.TYPE_MOVE_JOB,
                            {'group': src_group.group_id,
                             'uncoupled_group': dst_group.group_id,
                             'merged_groups': [mg.group_id for mg in merged_groups],
                             'src_host': src_group.node_backends[0].node.host.addr,
                             'src_port': src_group.node_backends[0].node.port,
                             'src_family': src_group.node_backends[0].node.family,
                             'src_backend_id': src_group.node_backends[0].backend_id,
                             'src_base_path': src_group.node_backends[0].base_path,
                             'dst_host': dst_group.node_backends[0].node.host.addr,
                             'dst_port': dst_group.node_backends[0].node.port,
                             'dst_family': dst_group.node_backends[0].node.family,
                             'dst_backend_id': dst_group.node_backends[0].backend_id,
                             'dst_base_path': dst_group.node_backends[0].base_path,
                             'need_approving': need_approving,
                             },
                            force=True)
                        logger.info('Job successfully created: {0}'.format(job.id))
                    except LockFailedError as e:
                        logger.error(
                            'Failed to create move job for moving '
                            'group {0} ({1}) to {2} ({3}): {4}'.format(
                                src_group.group_id, src_dc,
                                dst_group.group_id, dst_dc, e))
                    except Exception as e:
                        logger.error(
                            'Failed to create move job for moving '
                            'group {0} ({1}) to {2} ({3}): {4}\n{5}'.format(
                                src_group.group_id, src_dc,
                                dst_group.group_id, dst_dc,
                                e, traceback.format_exc()))

        except ValueError as e:
            logger.error('Plan cannot be applied: {0}'.format(e))

    def _do_move_candidates(self, max_jobs_count):

        active_jobs = self.job_processor.job_finder.jobs(
            statuses=jobs.Job.ACTIVE_STATUSES
        )
        busy_group_ids = self._busy_group_ids(active_jobs)

        busy_hosts = self.__busy_hosts([jobs.JobTypes.TYPE_MOVE_JOB,
                                        jobs.JobTypes.TYPE_RESTORE_GROUP_JOB])
        logger.debug('Busy hosts from executing jobs: {0}'.format(list(busy_hosts)))

        candidates = []
        cur_candidate = StorageState.current(busy_group_ids=busy_group_ids)

        for _ in xrange(min(self.__max_plan_length, max_jobs_count)):

            new_candidates = self._generate_candidates(
                cur_candidate,
                busy_hosts=busy_hosts,
                busy_group_ids=busy_group_ids,
            )

            if not new_candidates:
                break

            max_error_candidate = max(new_candidates, key=lambda c: c.delta.weight)
            logger.info('Max error candidate: {}'.format(max_error_candidate))

            candidates.append(max_error_candidate)

            # updating busy hosts and busy groups
            for src_group, src_dc, dst_group, uncoupled_groups, dst_dc in max_error_candidate.moved_groups:
                busy_hosts.add(src_group.node_backends[0].node.host.addr)
                busy_hosts.add(dst_group.node_backends[0].node.host.addr)
                busy_group_ids.add(src_group.group_id)
                busy_group_ids.add(dst_group.group_id)
                busy_group_ids.update(g.group_id for g in uncoupled_groups)

        self.__apply_plan(candidates)

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

    def _sample_groups_by_ts(self, state, groups):

        groups_by_space = {}

        groups = sorted(groups, key=lambda x: state.stats(x).total_space, reverse=True)

        total_space = None
        for group in groups:

            group_total_space = state.stats(group).total_space
            if total_space and group_total_space >= total_space * 0.95:
                continue

            groups_by_space[group_total_space] = group
            total_space = group_total_space

        return groups_by_space

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

    def _generate_candidates(self, candidate, busy_hosts, busy_group_ids):
        _candidates = []

        avg = candidate.mean_unc_percentage

        base_ms = candidate.state_ms_error

        uncoupled_groups = infrastructure.get_good_uncoupled_groups(max_node_backends=1)
        uncoupled_groups = self._filter_groups_on_busy_hosts(uncoupled_groups, busy_hosts)

        for c in candidate.iteritems():
            src_dc, src_dc_state = c

            if src_dc_state.unc_percentage > avg:
                logger.debug('Src dc {0}: src_dc percentage > avg percentage'.format(
                    src_dc))
                continue

            full_groups = self._sample_groups_by_ts(candidate, src_dc_state.groups).values()
            full_groups = list(self._filter_groups_on_busy_hosts(full_groups, busy_hosts))
            logger.debug(
                'Source dc "{dc}" candidates: decreased number from {total_groups_count} to '
                '{filtered_groups_count}'.format(
                    dc=src_dc,
                    total_groups_count=len(src_dc_state.groups),
                    filtered_groups_count=len(full_groups),
                )
            )

            for src_group in full_groups:

                src_host = src_group.node_backends[0].node.host.addr
                assert src_host not in busy_hosts, 'Group src host is not expected to be in busy_hosts'

                try:
                    # uncoupled_groups should change on each iteration (to skip those that are
                    # already used on previous steps)
                    suitable_uncoupled_groups = self.get_suitable_uncoupled_groups_list(
                        src_group, uncoupled_groups, busy_group_ids=busy_group_ids)
                except RuntimeError:
                    logger.warn('Skipping group {}, failed to get suitable uncoupled groups'.format(
                        src_group))
                    continue

                try:
                    unsuitable_dcs = tuple(
                        nb.node.host.dc
                        for g in src_group.couple.groups
                        for nb in g.node_backends
                    )
                except CacheUpstreamError:
                    logger.error('Failed to get unsuitable dc for group {}'.format(src_group))
                    continue

                candidates_per_dc = self._prepare_candidates_by_dc(
                    suitable_uncoupled_groups,
                    unsuitable_dcs=unsuitable_dcs,
                )

                for dst_dc, dc_candidates in candidates_per_dc.iteritems():

                    sample_dc_candidates = self._sample_dc_candidates(
                        src_group,
                        dc_candidates,
                        storage_state=candidate,
                    )
                    logger.debug(
                        'Source dc "{src_dc}", destination "{dst_dc}" candidates: decreased number '
                        'from {total_candidates_count} to {filtered_candidates_count}'.format(
                            src_dc=src_dc,
                            dst_dc=dst_dc,
                            total_candidates_count=len(dc_candidates),
                            filtered_candidates_count=len(sample_dc_candidates),
                        )
                    )
                    for candidates in sample_dc_candidates:
                        logger.debug('checking src_group {} against candidate {} '
                                     '({} -> {})'.format(
                                         src_group.group_id, [g.group_id for g in candidates],
                                         src_dc, dst_dc))

                        unc_group, merged_groups = candidates[0], candidates[1:]

                        dst_host = unc_group.node_backends[0].node.host.addr
                        assert dst_host not in busy_hosts, 'Group dst host is not expected to be in busy_hosts'

                        dst_dc_state = candidate.state[dst_dc]
                        if src_group.couple in dst_dc_state.couples:
                            # can't move group to dst_dc because a group
                            # from the same couple is already located there.
                            # This check also skip the cases when several groups
                            # in the same couple are moved to the same dc.
                            continue

                        unc_group_stat = candidate.stats(unc_group)

                        # TODO: check merged groups for files
                        if unc_group_stat.files + unc_group_stat.files_removed > 0:
                            continue

                        break
                    else:
                        # no suitable uncoupled group found in dst_dc
                        continue

                    new_candidate = candidate.copy()
                    try:
                        new_candidate.move_group(src_dc, src_group, dst_dc, unc_group, merged_groups)
                    except RuntimeError as e:
                        logger.warn('Skipping candidate src group {}: {}'.format(src_group, e))
                        continue

                    if new_candidate.state_ms_error < base_ms:
                        logger.debug(
                            'good candidate found: {0} group from {1} to {2}, '
                            'deviation changed from {3} to {4} (weight: {5}, lost space {6}) '
                            '(swap with group {7})'.format(
                                src_group.group_id, src_dc, dst_dc, base_ms,
                                new_candidate.state_ms_error, new_candidate.delta.weight,
                                gb(new_candidate.delta.lost_space), unc_group.group_id))
                        _candidates.append(new_candidate)
                    else:
                        logger.debug(
                            'bad candidate: {0} group from {1} to {2}, '
                            'deviation changed from {3} to {4} (swap with group {5})'.format(
                                src_group.group_id, src_dc, dst_dc, base_ms,
                                new_candidate.state_ms_error, unc_group.group_id))
                        logger.debug('Base candidate:')
                        candidate._debug()
                        logger.debug('New candidate aftere moving:')
                        new_candidate._debug()

                    time.sleep(0.1)
            time.sleep(1)

        return _candidates

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
            statuses=jobs.Job.ACTIVE_STATUSES
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

    def sync_recover_data(self):

        recover_data_couples = set()

        offset = 0
        while True:
            cursor = self.collection.find(
                fields=['couple'],
                sort=[('couple', pymongo.ASCENDING)],
                skip=offset, limit=self.RECOVERY_OP_CHUNK)
            count = 0
            for rdc in cursor:
                recover_data_couples.add(rdc['couple'])
                count += 1
            offset += count

            if count < self.RECOVERY_OP_CHUNK:
                break

        ts = int(time.time())

        storage_couples = set(str(c) for c in storage.replicas_groupsets.keys())

        add_couples = list(storage_couples - recover_data_couples)
        remove_couples = list(recover_data_couples - storage_couples)

        logger.info('Couples to add to recover data list: {0}'.format(add_couples))
        logger.info('Couples to remove from recover data list: {0}'.format(remove_couples))

        offset = 0
        while offset < len(add_couples):
            bulk_op = self.collection.initialize_unordered_bulk_op()
            bulk_add_couples = add_couples[offset:offset + self.RECOVERY_OP_CHUNK]
            for couple in bulk_add_couples:
                bulk_op.insert({'couple': couple,
                                'recover_ts': ts})
            res = bulk_op.execute()
            if res['nInserted'] != len(bulk_add_couples):
                raise ValueError('failed to add couples recover data: {0}/{1} ({2})'.format(
                    res['nInserted'], len(bulk_add_couples), res))
            offset += res['nInserted']

        offset = 0
        while offset < len(remove_couples):
            bulk_op = self.collection.initialize_unordered_bulk_op()
            bulk_remove_couples = remove_couples[offset:offset + self.RECOVERY_OP_CHUNK]
            bulk_op.find({'couple': {'$in': bulk_remove_couples}}).remove()
            res = bulk_op.execute()
            if res['nRemoved'] != len(bulk_remove_couples):
                raise ValueError('failed to remove couples recover data: {0}/{1} ({2})'.format(
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
            self.sync_recover_data()
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

    def update_recover_ts(self, couple_id, ts):
        ts = int(ts)
        res = self.collection.update(
            {'couple': couple_id},
            {'couple': couple_id, 'recover_ts': ts},
            upsert=True)
        if res['ok'] != 1:
            logger.error('Unexpected mongo response during recover ts update: {0}'.format(res))
            raise RuntimeError('Mongo operation result: {0}'.format(res['ok']))

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
            statuses=jobs.Job.ACTIVE_STATUSES
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

        if group.status not in (storage.Status.BAD, storage.Status.INIT, storage.Status.RO):
            raise ValueError('Group {0} has {1} status, should have BAD or INIT'.format(
                group.group_id, group.status))

        if (group.node_backends and group.node_backends[0].status not in (
                storage.Status.STALLED, storage.Status.INIT, storage.Status.RO)):
            raise ValueError(
                'Group {0} has node backend {1} in status {2}, '
                'should have STALLED or INIT status'.format(
                    group.group_id, str(group.node_backends[0]), group.node_backends[0].status))

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

        path = request['path']

        try:
            use_uncoupled_group = bool(request['uncoupled_group'])
        except (TypeError, ValueError):
            use_uncoupled_group = False

        force = bool(request.get('force', False))

        autoapprove = bool(request.get('autoapprove', False))

        groups = []
        for group in storage.groups:
            for backend in group.node_backends:
                if backend.node.host.addr in ips and backend.base_path.startswith(path):
                    if len(group.node_backends) == 1:
                        groups.append(group)
                    else:
                        raise ValueError(
                            'Group {} has {} node backends, currently '
                            'only groups with 1 node backend can be used'.format(
                                group.group_id, len(group.node_backends)))

        groups_to_backup = []
        groups_to_restore = []
        active_jobs = []
        uncoupled_groups = {}
        cancelled_jobs = []
        for group in groups:
            if group.couple is None:
                backend_name = "{host}:{port}:{family}/{backend_id}".format(
                    host=hostname,
                    port=group.node_backends[0].node.port,
                    family=group.node_backends[0].node.family,
                    backend_id=group.node_backends[0].backend_id,
                )
                uncoupled_groups[group.group_id] = str(backend_name)
            else:
                job = group.active_job
                if job is None:
                    groups_to_backup.append(group.group_id)
                else:
                    group_jobs = self.job_processor.job_finder.jobs(ids=job['id'],
                                                                    types=self.RESTORE_TYPES)
                    if len(group_jobs) > 1:
                        raise ValueError('Id {} has {} jobs'.format(job['id'], len(jobs)))
                    elif len(group_jobs) == 1:
                        job = group_jobs[0]

                        if job.status in self.RUNNING_STATUSES:
                            active_jobs.append(job.id)
                        elif job.status in self.ERROR_STATUSES:
                            try:
                                self.job_processor.cancel_job(job.id)
                                cancelled_jobs.append(job.id)
                            except Exception as e:
                                logger.exception('Failed to cancel job {}'.format(job.id))
                                raise ValueError('Failed to cancel job {}: {}'.format(job.id, e))
                            groups_to_restore.append(group.group_id)
                        else:
                            raise ValueError(
                                'Unknown job status: {}'.format(job.status))
                    else:
                        groups_to_backup.append(group.group_id)

        def restore_job(group, use_uncoupled_group, src_group, force, autoapprove):
            last_error = None
            for _ in xrange(self.CREATE_JOB_ATTEMPTS):
                try:
                    return self.create_restore_job(group, use_uncoupled_group, src_group, force, autoapprove)
                except Exception as e:
                    last_error = e
                    continue
            if last_error:
                    raise last_error

        failed = {}
        for group in groups_to_backup:
            try:
                job = restore_job(group, use_uncoupled_group, group, force, autoapprove)
                active_jobs.append(job['id'])
            except Exception as e:
                failed[group] = str(e)

        for group in groups_to_restore:
            try:
                job = restore_job(group, use_uncoupled_group, None, force, autoapprove)
                active_jobs.append(job['id'])
            except Exception as e:
                failed[group] = str(e)

        return {'jobs': active_jobs, 'failed': failed, 'uncoupled': uncoupled_groups, 'cancelled_jobs': cancelled_jobs}

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

                is_good = infrastructure.is_uncoupled_group_good(
                    unc_group, locked_hosts, [storage.Group.TYPE_UNCOUPLED], max_node_backends=1)
                if not is_good:
                    raise ValueError('Uncoupled group {0} is not applicable'.format(
                        unc_group.group_id))

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
                      jobs.Job.STATUS_BROKEN))

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

    def __check_namespace(self, namespace):
        if namespace not in infrastructure.ns_settings:
            raise ValueError('Namespace "{}" does not exist'.format(namespace))
        else:
            if infrastructure.ns_settings[namespace]['__service'].get('is_deleted'):
                raise ValueError('Namespace "{}" is deleted'.format(namespace))

    @h.concurrent_handler
    def convert_external_storage_to_groupset(self, request):
        if not request.get('src_storage'):
            raise ValueError('Request should contain "src_storage" field')

        if 'src_storage_options' not in request:
            raise ValueError('Request should contain "src_storage_options" field')

        if 'type' not in request:
            raise ValueError('Request should contain groupset "type" field')

        if 'settings' not in request:
            raise ValueError('Request should contain "settings" field')

        if 'namespace' not in request:
            raise ValueError('Request should contain "namespace" field')

        settings = request['settings']
        Groupset = storage.groupsets.make_groupset_type(
            type=request['type'],
            settings=settings,
        )

        Groupset.check_settings(settings)

        determine_data_size = request.get('determine_data_size', False)

        if determine_data_size or request.get('groupsets', []):
            # job will use supplied groupsets and choose additional groupsets if
            # necessary
            groupsets = [
                [int(group_id) for group_id in groupset.split(':')]
                for groupset in request.get('groupsets', [])
            ]
        else:
            # choose a single groupset for job to use (and to lock before job starts)
            groupsets = [
                self.job_processor._select_groups_for_groupset(
                    type=request['type'],
                    mandatory_dcs=request.get('mandatory_dcs', []),
                )
            ]

        for groupset in groupsets:
            self.job_processor._check_groupset(groupset, type=request['type'])

        namespace_id = request['namespace']
        self.__check_namespace(namespace_id)

        if namespace_id not in storage.namespaces:
            ns = storage.namespaces.add(namespace_id)
        else:
            ns = storage.namespaces[namespace_id]

        if request['type'] == storage.GROUPSET_LRC:
            job_type = jobs.JobTypes.TYPE_CONVERT_TO_LRC_GROUPSET_JOB
        else:
            raise ValueError('Unsupported groupset type: {}'.format(request['type']))

        job = self.job_processor._create_job(
            job_type=job_type,
            params={
                'ns': ns.id,
                'groups': groupsets,
                'mandatory_dcs': request.get('mandatory_dcs', []),
                'part_size': settings['part_size'],
                'scheme': settings['scheme'],
                'determine_data_size': determine_data_size,
                'src_storage': request['src_storage'],
                'src_storage_options': request['src_storage_options'],
            },
        )

        return job.dump()

    def cleanup(self, request):
        """
        Langolier job. Remove all records with expired TTL
        """
        job = self.job_processor._create_job(
                    job_type=jobs.JobTypes.TYPE_CLEANUP,
                    params=request)

        return job.dump()

def _recovery_applicable_couple(couple):
    if couple.status not in storage.GOOD_STATUSES:
        return False
    return True


class Delta(object):
    def __init__(self):
        self.data_move_size = 0.0
        self.ms_error_delta = 0.0
        self.lost_space = 0.0

    def __add__(self, other):
        res = Delta()
        res.data_move_size = self.data_move_size + other.data_move_size
        res.ms_error_delta = self.ms_error_delta + other.ms_error_delta
        res.lost_space = self.lost_space + other.lost_space

    def copy(self):
        res = Delta()
        res.data_move_size = self.data_move_size
        res.ms_error_delta = self.ms_error_delta
        res.lost_space = self.lost_space
        return res

    @property
    def weight(self):
        return -self.ms_error_delta - (self.lost_space / (50 * 1024 * 1024 * 1024))


class DcState(object):
    def __init__(self, storage_state):
        self.groups = []
        self.total_space = 0.0
        self.uncoupled_space = 0.0
        self.storage_state = storage_state
        self.couples = set()
        self.uncoupled_keys = []

    def add_group(self, group):
        self.groups.append(group)
        assert group.couple
        self.couples.add(group.couple)

    def account_uncoupled_group(self, group):
        self.uncoupled_space += self.storage_state.stats(group).total_space

    def copy(self, storage_state):
        obj = DcState(storage_state)
        obj.groups = copy(self.groups)
        obj.total_space, obj.uncoupled_space = self.total_space, self.uncoupled_space
        obj.couples = copy(self.couples)
        return obj

    def apply_stat(self, stat):
        self.total_space += stat.total_space

    @property
    def unc_percentage(self):
        return float(self.uncoupled_space) / self.total_space


class StorageState(object):
    def __init__(self, delta=None, state=None, stats=None):
        self.delta = Delta()
        if delta is not None:
            self.delta.data_move_size = delta.data_move_size

        if stats is None:
            self._stats = {}
        else:
            self._stats = copy(stats)

        if state is None:
            # requires self._stats to be initialized
            self.state = {
                dc: DcState(self)
                for dc in self.__dcs()
            }
        else:
            # requires self._stats to be initialized
            self.state = {
                dc: dc_state.copy(self)
                for dc, dc_state in state.iteritems()
            }
        self.moved_groups = []

    @classmethod
    def current(cls, busy_group_ids):
        obj = cls()
        for couple in cls.__get_full_couples():
            if any(g.group_id in busy_group_ids for g in couple.groups):
                # couple is participating in some job, skipping it
                continue
            for group in couple.groups:
                try:
                    dc = group.node_backends[0].node.host.dc
                except CacheUpstreamError:
                    logger.warn('Skipping group {} because of cache failure'.format(
                        group))
                    continue
                obj._stats[group.group_id] = group.get_stat()
                obj.state[dc].add_group(group)

        for group in infrastructure.get_good_uncoupled_groups(max_node_backends=1):
            try:
                dc = group.node_backends[0].node.host.dc
            except CacheUpstreamError:
                logger.warn('Skipping group {} because of cache failure'.format(
                    group))
                continue
            obj._stats[group.group_id] = group.get_stat()
            obj.state[dc].account_uncoupled_group(group)

        # TODO: this iteration can be unified with __get_full_couples iteration
        # to save time
        for group in storage.groups:
            for nb in group.node_backends:
                if nb.stat is None:
                    continue
                try:
                    dc = nb.node.host.dc
                except CacheUpstreamError:
                    logger.warn('Skipping host {} because of cache failure'.format(
                        nb.node.host))
                    continue
                obj.state[dc].apply_stat(nb.stat)

        logger.debug('Current state:')
        for dc, dc_state in obj.state.iteritems():
            logger.debug('dc: {0}, total_space {1}, uncoupled_space {2}, unc_percentage {3}'.format(
                dc, dc_state.total_space, dc_state.uncoupled_space, dc_state.unc_percentage))
        return obj

    def copy(self):
        return StorageState(
            delta=self.delta,
            state=self.state,
            stats=self._stats,
        )

    @property
    def mean_unc_percentage(self):
        # TODO: maybe this should be calculated as an average?
        unc_space = sum(dc_state.uncoupled_space for dc_state in self.state.itervalues())
        total_space = sum(dc_state.total_space for dc_state in self.state.itervalues())

        return float(unc_space) / total_space

    def _debug(self):
        logger.debug('mean percentage: {0}'.format(self.mean_unc_percentage))
        for dc, dc_state in self.state.iteritems():
            logger.debug('dc: {0}, unc_percentage {1}, unc_space {2} total_space {3}'.format(
                dc, dc_state.unc_percentage, dc_state.uncoupled_space, dc_state.total_space))

    def stats(self, group):
        return self._stats[group.group_id]

    @property
    def state_ms_error(self):

        avg = self.mean_unc_percentage

        lms_error = 0.0
        for dc_state in self.state.itervalues():
            lms_error += ((dc_state.unc_percentage - avg) * 100) ** 2

        return lms_error

    def iteritems(self):
        return self.state.iteritems()

    @staticmethod
    def __get_full_couples():
        couples = []

        for couple in storage.replicas_groupsets:
            if couple.status != storage.Status.FULL:
                continue

            if couple.namespace.id == storage.Group.CACHE_NAMESPACE:
                # there is no point in moving cached keys
                continue

            for group in couple.groups:
                if len(group.node_backends) > 1:
                    break
            else:
                couples.append(couple)

        return couples

    @staticmethod
    def __dcs():
        dcs = set()
        for group in storage.groups:
            for nb in group.node_backends:
                try:
                    dcs.add(nb.node.host.dc)
                except CacheUpstreamError:
                    continue
        return dcs

    def move_group(self, src_dc, src_group, dst_dc, dst_group, merged_groups):

        old_ms_error = self.state_ms_error

        uncoupled_groups = [dst_group] + merged_groups

        # logger.info('{0}'.format(self.state))
        self.state[src_dc].groups.remove(src_group)
        self.state[dst_dc].groups.append(src_group)

        # do this only in case if no other group from this couple is located in src_dc
        for group in src_group.couple:
            try:
                dc = group.node_backends[0].node.host.dc
            except CacheUpstreamError:
                raise RuntimeError('Failed to get dc for host {}'.format(
                    group.node_backends[0].node.host))
            if group != src_group and dc == src_dc:
                break
        else:
            self.state[src_dc].couples.remove(src_group.couple)

        self.state[dst_dc].couples.add(src_group.couple)

        self.state[src_dc].uncoupled_space += self.stats(src_group).total_space
        for group in uncoupled_groups:
            self.state[dst_dc].uncoupled_space -= self.stats(group).total_space

        # updating delta object
        self.delta.data_move_size += self.stats(src_group).used_space
        self.delta.ms_error_delta += self.state_ms_error - old_ms_error

        for group in uncoupled_groups:
            self.delta.lost_space += self.stats(group).total_space
        self.delta.lost_space -= self.stats(src_group).used_space

        self.moved_groups.append((src_group, src_dc, dst_group, merged_groups, dst_dc))

    def __str__(self):
        moved_group_descs = []
        for groups in self.moved_groups:
            src_group, src_dc, dst_group, merged_groups, dst_dc = groups
            moved_group_desc = 'group {src_group} ({src_dc}) -> {dst_group} ({dst_dc})'
            if merged_groups:
                moved_group_desc += ', merging groups {merged_groups}'
            moved_group_descs.append(
                moved_group_desc.format(
                    src_group=src_group,
                    src_dc=src_dc,
                    dst_group=dst_group,
                    dst_dc=dst_dc,
                    merged_groups=merged_groups,
                )
            )
        return '<StorageState: [{}]>'.format('; '.join(moved_group_descs))


def gb(bytes):
    return (bytes / (1024.0 * 1024.0 * 1024))
