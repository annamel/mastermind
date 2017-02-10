import logging

import jobs
from mastermind_core.config import config
import storage
import time


logger = logging.getLogger('mm.sched.recover')


class RecoveryStarter(object):

    def __init__(self, job_processor, planner):
        self.planner = planner
        self.job_processor = job_processor
        planner.register_periodic_func(self._do_recover_dc, 60*15, starter_name="recover_dc")
        self.params = config.get('scheduler', {})

    def _do_recover_dc(self):

        couple_ids_to_recover = self._recover_top_weight_couples()

        jobs_param = []

        for couple_id in couple_ids_to_recover:
            jobs_param.append({'couple': couple_id})

        sched_params = self.params.get('recover_dc')
        created_jobs = self.planner.process_jobs(jobs.JobTypes.TYPE_RECOVER_DC_JOB, jobs_param, sched_params)

        logger.info('Successfully created {0} recover dc jobs'.format(created_jobs))

    def _recover_top_weight_couples(self):

        ts = int(time.time())
        weights = {}

        # by default one key loss is equal to one day without recovery
        keys_cf = self.params.get('recover_dc', {}).get('keys_cf', 86400)
        ts_cf = self.params.get('recover_dc', {}).get('timestamp_cf', 1)
        def weight(keys_diff, ts_diff):
            return keys_diff * keys_cf + ts_diff * ts_cf

        # this is more then actual number of job running, but let planner cut the sequence on its own
        count = config.get('jobs', {}).get('recover_dc_job', {}).get('max_executing_jobs', 3)

        history_data = self.planner.get_history()

        for couple in storage.replicas_groupsets.keys():

            if couple.status not in storage.GOOD_STATUSES:
                continue
            c_diff = couple.keys_diff
            couple_str = str(couple)
            if couple_str not in history_data:
                self.planner.sync_history()
                if couple_str not in history_data:
                    # some very strange couple
                    continue
            weights[couple_str] = weight(c_diff, ts - history_data[couple_str]['recover_ts'])

        # sort weights (descending)
        candidates = sorted(weights.iteritems(), key=lambda x: -x[1])

        # XXX: fix double-conversion of representation
        return [candidate[0] for candidate in candidates[:count]]
