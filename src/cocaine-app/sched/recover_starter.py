import logging
import time

import jobs
from mastermind_core.config import config
import storage



logger = logging.getLogger('mm.sched.recover')


class RecoveryStarter(object):

    def __init__(self, job_processor, scheduler):
        self.scheduler = scheduler
        self.job_processor = job_processor
        self.params = config.get('scheduler', {}).get('recover_dc', {})
        scheduler.register_periodic_func(self._do_recover_dc, 60*15, starter_name="recover_dc")

    def _do_recover_dc(self):

        weight_couples = self._get_top_weight_couples()

        jobs_param = []

        for weight_couple_pair in weight_couples:
            jobs_param.append({'couple': weight_couple_pair[1]})

        created_jobs = self.scheduler.create_jobs(jobs.JobTypes.TYPE_RECOVER_DC_JOB, jobs_param, self.params)

        logger.info('Successfully created {0} recover dc jobs'.format(len(created_jobs)))

    def _get_top_weight_couples(self):
        """
        Analyzing all couples in order to find ones that need recover more then others
        :return: a list of tuples(weight, couple_id)
        """

        ts = int(time.time())
        weights = []

        # by default one key loss is equal to one day without recovery
        keys_cf = self.params.get('keys_cf', 86400)
        ts_cf = self.params.get('timestamp_cf', 1)
        def weight(keys_diff, ts_diff):
            return keys_diff * keys_cf + ts_diff * ts_cf

        min_keys_loss = self.params.get('min_key_loss', 1)
        history_data = self.scheduler.get_history()

        # Actually we need groupsets, not all couples, but that would be filtered by history_data
        for couple in storage.couples.keys():

            if couple.status not in storage.GOOD_STATUSES:
                continue
            c_diff = couple.keys_diff
            if c_diff < min_keys_loss:
                continue
            couple_str = str(couple)
            if couple_str not in history_data:
                # a new couple?
                continue
            # store a list instead of dict, since then sorting by first value of tuple may be done without calling
            # extra function what is much more effective. See Schwartzian transform
            weights.append((weight(c_diff, ts - history_data[couple_str]['recover_ts']), couple_str))

        # sort weights (descending)
        return sorted(weights, reverse=True)
