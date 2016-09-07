import tasks
from job import Job
from job_types import JobTypes
import logging

logger = logging.getLogger('mm.jobs')

class MdsCleanupJob(Job):

    PARAMS = ()

    def __init__(self, **kwargs):
        super(MdsCleanupJob, self).__init__(**kwargs)
        self.type = JobTypes.TYPE_CLEANUP
        logger.error('MDS cleanup job inited')

    def _set_resources(self):
        self.resources = {}

    def create_tasks(self):
        self.tasks = {}

    @property
    def _involved_groups(self):
        return []

    @property
    def _involved_groups(self):
        return []

    @property
    def _involved_couples(self):
        return []

