import tasks
from job import Job
from job_types import JobTypes
import logging
import storage
from infrastructure import infrastructure

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
        'dry_run',
        'resources'
    )

    def __init__(self, **kwargs):
        super(MdsCleanupJob, self).__init__(**kwargs)
        self.type = JobTypes.TYPE_TTL_CLEANUP_JOB

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

        # Log, Log_level, temp to be taken from config on infrastructure side
        langolier_cmd = infrastructure.ttl_cleanup_cmd(
            remotes=self.remotes,
            groups=self.groups,
            iter_group=self.iter_group,
            wait_timeout=self.wait_timeout,
            batch_size=self.batch_size,
            attempts=self.attemps,
            nproc=self.nproc,
            trace_id=int(self.id[:16], 16),
            safe=self.dry_run)

        if self.iter_group not in storage.groups:
            logger.error("Not valid iter group is specified for create task %s", self.iter_group)
            return None

        logger.debug("Cleanup job: Set for execution task %s", langolier_cmd)

        # Run langolier on the storage node where we are going to iterate
        nb = storage.groups[self.iter_group].node_backends[0]
        host = nb.node.host.addr
        task = tasks.MinionCmdTask.new(self, host=host, group=self.iter_group, cmd=langolier_cmd, params={})
        self.tasks.append(task)

    @property
    def _involved_groups(self):
        return self.groups

    @property
    def _involved_couples(self):
        return []

