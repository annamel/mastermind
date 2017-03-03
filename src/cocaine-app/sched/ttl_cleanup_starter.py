import logging

from infrastructure import infrastructure
import jobs
from mastermind_core.config import config
import storage
import time
import datetime
from yt_worker import YqlWrapper


logger = logging.getLogger('mm.sched.ttl_cleanup')

class TtlCleanupStarter(object):
    def __init__(self, scheduler):
        scheduler.register_periodic_func(self._do_ttl_cleanup, 60*15, starter_name="ttl_cleanup")
        self.params = config.get('scheduler', {})
        self.scheduler = scheduler

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

    def _get_idle_groups(self, days_of_idle):
        """
        Iterates all over couples. Find couples where ttl_cleanup hasn't run for more than 'days of idle'
        :param days_of_idle: how long the group could be idle
        :return: list of groups[0] from couples
        """

        idle_groups = []

        # the epoch time when executed jobs are considered meaningful
        idleness_threshold = time.time() - datetime.timedelta(days=days_of_idle).total_seconds()

        couples_data = self.scheduler.get_history()
        # couples data is not sorted, so we need to iterate through it all.
        # But it may be faster then creation of a sorted representation

        for couple_id, couple_data in couples_data.iteritems():

            # if couple_data doesn't contain cleanup_ts field then cleanup_ts has never been run on this couple
            # and None < idleness_threshold
            ts = couple_data.get('ttl_cleanup_ts')
            if ts > idleness_threshold:
                continue

            # couple has format "gr0:gr1:...:grN". We are interested only in the group #0
            idle_groups.append(int(couple_id.split(":")[0]))

        return idle_groups

    def _do_ttl_cleanup(self):
        logger.info('Run ttl cleanup')

        job_params = []

        allowed_idleness_period = config.get('jobs', {}).get('ttl_cleanup_job', {}).get(
            'max_idle_days', 270)

        # get couples where ttl_cleanup wasn't run for long time (or never)
        time_group_list = self._get_idle_groups(days_of_idle=allowed_idleness_period)

        # get information from mds-proxy Yt logs
        yt_group_list = self._get_yt_stat()

        # remove dups
        couple_list = set(yt_group_list + time_group_list)

        for couple in couple_list:

            iter_group = couple  # in tskv coupld id is actually group[0] from couple id
            if iter_group not in storage.groups:
                logger.error("Not valid group is extracted from aggregation log {}".format(iter_group))
                continue
            iter_group = storage.groups[iter_group]
            if not iter_group.couple:
                logger.error("Iter group is uncoupled {}".format(str(iter_group)))
                continue

            job_params.append(
                    {
                        'iter_group': iter_group.group_id,
                        'couple': str(iter_group.couple),
                        'namespace': iter_group.couple.namespace.id,
                        'batch_size': None,  # get from config
                        'attempts': None,  # get from config
                        'nproc': None,  # get from config
                        'wait_timeout': None,  # get from config
                        'dry_run': False
                    })

        sched_params = self.params.get('ttl_cleanup')
        self.scheduler.create_jobs(jobs.JobTypes.TYPE_TTL_CLEANUP_JOB, job_params, sched_params)
