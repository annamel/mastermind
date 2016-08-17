import tasks
from job import Job
from job_types import JobTypes
import logging
import storage

logger = logging.getLogger('mm.jobs')

class MdsCleanupJob(Job):

    PARAMS = (
        'groups',
        'iter_group',
        'batch_size',
        'remotes',
        'attemps',
        'nproc',
        'wait_timeout',
        'safe'
    )

    def __init__(self, **kwargs):
        super(MdsCleanupJob, self).__init__(**kwargs)
        self.type = JobTypes.TYPE_CLEANUP
        logger.error('MDS cleanup job inited')

    def _set_resources(self):
        self.resources = {
            Job.RESOURCE_HOST_IN: [],
            Job.RESOURCE_HOST_OUT: [],
            Job.RESOURCE_FS: [],
            }

        if self.iter_group not in storage.groups:
            logger.error("Not valid iter group is specified %s", self.iter_group)
            return None

        # The nodes that are not iterated would be asked to perform remove
        # operation. No data is written. And no data is read.
        # While iteration group node is working heavily
        nb = storage.groups[self.iter_group].node_backends[0]
        self.resources[Job.RESOURCE_HOST_OUT].append(nb.node.host.addr)
        self.resources[Job.RESOURCE_FS].append((nb.node.host.addr, str(nb.fs.fsid)))

    def create_tasks(self):
        logger.error("Cleanup job: groups={} (iter={}), batch_size={}, remotes={}, attemps={}".format(
            self.groups, self.iter_group, self.batch_size, self.remotes, self.attemps))
        self.tasks = {}
        # trace_id = self.id[:16]
        # XXX: tmp_dir, log_path, log_level

    @property
    def _involved_groups(self):
        return self.groups

    @property
    def _involved_couples(self):
        return []

