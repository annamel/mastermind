import logging

import jobs
from mastermind_core.config import config
import storage
import time


logger = logging.getLogger('mm.planner.recover')


class DefragStarter(object):
    def __init__(self, job_processor, planner):
        self.planner = planner
        self.job_processor = job_processor
        planner.register_periodic_func(self._couple_defrag, 60*15, starter_name="couple_defrag")
        self.params = config.get('planner', {})

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

            self._do_couple_defrag()

        except:
            logger.exception('Couple defrag planner failed')

    def _do_couple_defrag(self):

        max_defrag_jobs = config.get('jobs', {}).get(
            'couple_defrag_job', {}).get('max_executing_jobs', 3)

        active_jobs = self.job_processor.job_finder.jobs(
            statuses=jobs.Job.ACTIVE_STATUSES,
            sort=False,
        )
        slots = self.planner.jobs_slots(active_jobs,
                                 jobs.JobTypes.TYPE_COUPLE_DEFRAG_JOB,
                                 max_defrag_jobs)

        if slots <= 0:
            logger.info('Found {0} unfinished couple defrag jobs'.format(
                max_defrag_jobs - slots))
            return

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
