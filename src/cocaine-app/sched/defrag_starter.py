import logging
import time

import jobs
from mastermind_core.config import config
import storage


logger = logging.getLogger('mm.sched.defrag')


class DefragStarter(object):
    def __init__(self, job_processor, scheduler):
        self.scheduler = scheduler
        self.job_processor = job_processor
        scheduler.register_periodic_func(self._do_couple_defrag, 60*15, starter_name="couple_defrag")
        self.params = config.get('scheduler', {})

    def _need_defrag(self, couple):
        """
        Checks whether this couple needs to defrag
        :param couple: couple
        :return: True if needs
        """
        couple_stat = couple.get_stat()
        if couple_stat.files_removed_size == 0:
            return False

        want_defrag = False

        for group in couple.groups:
            for nb in group.node_backends:
                if nb.stat.vfs_free_space < nb.stat.max_blob_base_size * 2:
                    logger.warn('Couple {}: node backend {} has insufficient '
                        'free space for defragmentation, max_blob_size {}, vfs free_space {}'.format(
                            str(couple), str(nb), nb.stat.max_blob_base_size, nb.stat.vfs_free_space))
                    return False
            want_defrag |= group.want_defrag

        if not want_defrag:
            return False

        logger.info('Couple defrag candidate: {}, max files_removed_size in groups: {}'.format(
            str(couple), couple_stat.files_removed_size))
        return True

    def _do_couple_defrag(self):

        job_params = []
        for couple in storage.couples.keys():
            if couple.status not in storage.GOOD_STATUSES:
                continue

            if not self._need_defrag(couple):
                continue

            job_params.append(
                    {'couple': str(couple),
                     'is_cache_couple': False})

        if len(job_params) == 0:
            logger.info('No couples to defrag are found')
            return

        sched_params = self.params.get('couple_defrag')
        created_jobs = self.scheduler.create_jobs(jobs.JobTypes.TYPE_COUPLE_DEFRAG_JOB, job_params, sched_params)

        logger.info('Successfully created {} couple defrag jobs'.format(len(created_jobs)))
