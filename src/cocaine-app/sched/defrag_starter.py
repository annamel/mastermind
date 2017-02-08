import logging

import jobs
from mastermind_core.config import config
import storage
import time


logger = logging.getLogger('mm.sched.defrag')


class DefragStarter(object):
    def __init__(self, job_processor, planner):
        self.planner = planner
        self.job_processor = job_processor
        planner.register_periodic_func(self._do_couple_defrag, 60*15, starter_name="couple_defrag")
        self.params = config.get('scheduler', {})

    def _do_couple_defrag(self):

        busy_group_ids = self.planner.busy_group_ids(active_jobs)

        couples_to_defrag = []
        for couple in storage.replicas_groupsets.keys():
            if self.planner.is_locked(couple, busy_group_ids):
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

        if not couples_to_defrag:
            logger.info('No couples to defrag found')
            return

        job_params = []

        while couples_to_defrag:
            couple_tuple, fragmentation = couples_to_defrag.pop()

            job_params.append(
                    {'couple': couple_tuple,
                     'is_cache_couple': False})

        sched_params = self.params.get('couple_defrag')
        created_jobs = self.planner.process_jobs(jobs.JobTypes.TYPE_COUPLE_DEFRAG_JOB, job_params, sched_params)

        logger.info('Successfully created {0} couple defrag jobs'.format(created_jobs))
