import logging

from infrastructure import infrastructure
import jobs
from mastermind_core.config import config
import storage


logger = logging.getLogger('mm.sched.ttl_cleanup')

class TtlCleanupStarter(object):
    def __init__(self, planner):
        planner.register_periodic_func(self._do_ttl_cleanup, 60*15, starter_name="ttl_cleanup")
        self.params = config.get('scheduler', {})
        self.planner = planner

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

            from yt_worker import YqlWrapper
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

    def _do_ttl_cleanup(self):
        logger.info('Run ttl cleanup')

        job_params = []

        couple_list = self._get_yt_stat()
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
        self.planner.process_jobs(jobs.JobTypes.TYPE_TTL_CLEANUP_JOB, job_params, sched_params)
