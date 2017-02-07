import logging

import jobs
from mastermind_core.config import config
import storage
import time


logger = logging.getLogger('mm.sched.recover')


class RecoveryStarter(object):

    RECOVERY_OP_CHUNK = 200

    def __init__(self, job_processor, planner):
        self.planner = planner
        self.job_processor = job_processor
        planner.register_periodic_func(self._recover_dc, 60*15, starter_name="recover_dc")
        self.params = config.get('scheduler', {})


    def _recover_dc(self):
        try:
            start_ts = time.time()

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

            self._do_recover_dc()

            logger.info('Recover dc planner finished, time: {:.3f}'.format(
                time.time() - start_ts))

        except:
            logger.exception('Recover dc planner failed')

    def _do_recover_dc(self):

        max_recover_jobs = config.get('jobs', {}).get('recover_dc_job', {}).get(
            'max_executing_jobs', 3)

        active_jobs = self.job_processor.job_finder.jobs(
            statuses=jobs.Job.ACTIVE_STATUSES,
            sort=False,
        )

        slots = self.planner.jobs_slots(active_jobs,
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

    def _recover_top_weight_couples(self, count, active_jobs):
        keys_diffs_sorted = []
        keys_diffs = {}
        ts_diffs = {}

        busy_group_ids = self.planner.busy_group_ids(active_jobs)

        for couple in storage.replicas_groupsets.keys():
            if self.planner.is_locked(couple, busy_group_ids):
                continue
            if not _recovery_applicable_couple(couple):
                continue
            c_diff = couple.keys_diff
            keys_diffs_sorted.append((str(couple), c_diff))
            keys_diffs[str(couple)] = c_diff
        keys_diffs_sorted.sort(key=lambda x: x[1])

        cursor = self.planner.get_history('recover_ts')

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


def _recovery_applicable_couple(couple):
    if couple.status not in storage.GOOD_STATUSES:
        return False
    return True