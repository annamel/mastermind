import logging
import bisect
from collections import Counter, defaultdict

import jobs
from mastermind_core.config import config
from mastermind_core import helpers
import storage


logger = logging.getLogger('mm.sched.move')


class MoveStarter(object):

    def __init__(self, scheduler):

        self.scheduler = scheduler
        self.params = config.get('scheduler', {}).get('move', {})
        scheduler.register_periodic_func(self._do_move,
                                         period_default=1800,  # one per half an hour
                                         starter_name="move")

    def _prepare_dc_stat(self):

        def default_dc():
            return {
                "total_space": 0,
                "uncoupled_space": 0,
                "valid_as_source": False,
                "unc_percentage": 0,  # percent of uncoupled space
                "full_groups": [],
                "uncoupled_groups": [],
                "uncoupled_space_per_fs": Counter()  # d[fs]=volume
            }

        dcs = defaultdict(default_dc)
        total_space = 0
        uncoupled_space = 0

        # enumerate all groups
        for group in storage.groups.keys():
            for nb in group.node_backends:
                if nb.stat is None:
                    continue

                dc = nb.node.host.dc

                dcs[dc]["total_space"] += nb.stat.total_space
                total_space += nb.stat.total_space

                if group.type == storage.Group.TYPE_UNCOUPLED:
                    dcs[dc]["uncoupled_space"] += nb.stat.total_space
                    uncoupled_space += nb.stat.total_space
                    dcs[dc]["uncoupled_space_per_fs"][nb.fs.fsid] += nb.stat.total_space
                    dcs[dc]["uncoupled_groups"].append(group)

                elif group.type == storage.Group.TYPE_DATA:

                    if not group.couple:
                        continue

                    if group.couple.status != storage.Status.FULL:
                        continue

                    if group.couple.namespace.id == storage.Group.CACHE_NAMESPACE:
                        # there is no point in moving cached keys
                        continue

                    if any(len(g.node_backends) > 1 for g in group.couple.groups):
                        continue

                    dcs[dc]["full_groups"].append(group)

        avg_unc_percentage = float(uncoupled_space) / total_space

        for dc_name, dc in dcs.iteritems():
            unc_percent = float(dc["uncoupled_space"])/dc["total_space"]
            dc["unc_percentage"] = unc_percent

            if unc_percent > avg_unc_percentage:
                logger.info(
                    'Source dc "{}": skipped, uncoupled percentage {} > {} (avg)'.format(
                        dc_name, helpers.percent(unc_percent), helpers.percent(avg_unc_percentage))
                )
                continue

            uncoupled_space_max = self.params.get('uncoupled_space_max_bytes', 0)
            unc = dc["uncoupled_space"]
            if uncoupled_space_max and unc > uncoupled_space_max:
                logger.info(
                    'Source dc "{}": skipped, uncoupled space {} > {}'.format(
                        dc_name, helpers.convert_bytes(unc), helpers.convert_bytes(uncoupled_space_max))
                )
                continue

            dc["valid_as_source"] = True

        return dcs

    def _do_move(self):
        """
        Source group of move operation must satisfy the following requirements:
        - the host must have enough res
        - the host must belong to DC that have uncoupled_percentage < avg and > max
        - the group must be FULL

        Destination group of move operation must meet the following expectations:
        - the group must be uncoupled
        - it should have enough uncoupled space on fs
        - the host must have enough res
        - the host must belong to DC that have uncoupled_percentage > min

        Pair of dst+src must satisfy the following conditions:
        - coupled groups of src should not belong to dst datacenter
        - dst DC uncoupled space < src DC uncoupled space
        :return:
        """

        move_jobs_limits = self.params.get(jobs.JobTypes.TYPE_MOVE_JOB, {}).get('resources_limits', {})

        host_out_demand = {}
        host_out_demand[jobs.Job.RESOURCE_HOST_OUT] = 100/max(move_jobs_limits.get(jobs.Job.RESOURCE_HOST_OUT, 1), 1)
        host_out_notcandidates, busy_groups = self.scheduler.get_busy_nodes_and_groups(host_out_demand)

        host_in_demand = {}
        host_in_demand[jobs.Job.RESOURCE_HOST_IN] = 100/max(move_jobs_limits.get(jobs.Job.RESOURCE_HOST_IN, 1), 1)
        host_in_notcandidates = self.scheduler.get_busy_nodes_and_groups(host_in_demand)

        dcs_stat = self._prepare_dc_stat()

        # list of dict params for jobs we are trying to create
        params = []

        # iterate over DCs
        for src_dc, src_dc_val in dcs_stat.iteritems():
            if not src_dc_val["valid_as_source"]:
                logger.info("Skip dc {} since it is not valid as src".format(src_dc))
                continue

            # find suitable groups within specified dc
            src_groups = []
            for group in src_dc_val["full_groups"]:
                # the group doesn't satisfy resource expectation
                host_addr = group.node_backends[0].node.host.addr
                if host_addr in host_out_notcandidates:
                    continue

                # if the group is busy, let's skip it
                if group in busy_groups:
                    continue

                src_groups.append(group)

            if len(src_groups) == 0:
                logger.info("No src groups within dc {} (full len = {})".format(src_dc, len(src_dc_val["full_groups"])))
                continue

            # uncoupled percentage within the DC
            src_unc_percentage = src_dc_val["unc_percentage"]

            # we need only those groups that doesn't have a coupled group in the target DC
            filter_coupled = lambda x: not any(int(gid) in storage.groups
                                               and storage.groups[int(gid)] in dst_dc_val["full_groups"]
                                               for gid in str(x.couple).split(':'))

            # suitable dst dc
            for dst_dc, dst_dc_val in dcs_stat.iteritems():

                if dst_dc == src_dc:
                    # they would be filtered anyway while creating filtered_src_groups but let's avoid extra work
                    continue

                filtered_src_groups = filter(filter_coupled, src_groups)
                if len(filtered_src_groups) == 0:
                    logger.info("After filtering src groups nothing has left(was {}). skip {} {} pair".format(
                        len(src_groups), src_dc, dst_dc))
                    continue

                logger.info("Filtered {} out of {} for dc {}".format(len(filtered_src_groups), len(src_groups), src_dc))

                # we are interested in more or less equal of uncoupled space among DC
                if dst_dc_val["unc_percentage"] > src_unc_percentage:
                    logger.info('Skip dst dc {} since its unc_percentage ({}) < src ({})'.format(
                        dst_dc, dst_dc_val["unc_percentage"], src_unc_percentage))
                    continue

                # maintain at least some of uncoupled space within DC
                uncoupled_space_min_bytes = self.params.get('uncoupled_space_min_bytes', 0)
                if uncoupled_space_min_bytes and dst_dc_val['uncoupled_space'] <= uncoupled_space_min_bytes:
                    logger.info(
                        'Dst dc {} is skipped, uncoupled space {} <= {} (limit)'.format(
                            dst_dc, helpers.convert_bytes(dst_dc_val['uncoupled_space']),
                            helpers.convert_bytes(uncoupled_space_min_bytes))
                    )
                    continue

                # dst groups candidates. list of tuples (space_avail, gid, dc)
                # not all candidates would suit all src params due to dc and space_avail criteria
                dst_candidates = []
                for group in dst_dc_val["uncoupled_groups"]:

                    # doesn't satisfy resources
                    if len(group.node_backends) == 0:
                        continue

                    nb = group.node_backends[0]
                    if nb.node.host.addr in host_in_notcandidates:
                        continue

                    avail = dst_dc_val["uncoupled_space_per_fs"][nb.fs.fsid]
                    dst_candidates.append((avail, group.group_id))

                dst_candidates = sorted(dst_candidates, reverse=True)  # sort by avail space

                if len(dst_candidates) == 0:
                    logger.info("No dst candidates for move within dc {} (uncoupled = {})".format(
                        dst_dc, len(dst_dc_val["uncoupled_groups"])))
                    continue

                for group in filtered_src_groups:
                    src_total_space = storage.groups[group].get_stat().total_space

                    # Find leftmost item greater than or equal to src_total_space
                    i = bisect.bisect_left(dst_candidates, src_total_space)
                    if i == len(dst_candidates):
                        # No suitable candidates are found
                        continue

                    dst_group = dst_candidates[i][1]

                    params.append({
                        'group': group.group_id,
                        'uncoupled_group': dst_group,
                        'merged_groups': [],  #Fixup
                        'src_host': group.node_backends[0].node.host.addr,
                        'src_port': group.node_backends[0].node.port,
                        'src_family': group.node_backends[0].node.family,
                        'src_backend_id': group.node_backends[0].backend_id,
                        'src_base_path': group.node_backends[0].base_path,
                        'dst_host': storage.groups[dst_group].node_backends[0].node.host.addr,
                        'dst_port': storage.groups[dst_group].node_backends[0].node.port,
                        'dst_family': storage.groups[dst_group].node_backends[0].node.family,
                        'dst_backend_id': storage.groups[dst_group].node_backends[0].backend_id,
                        'dst_base_path': storage.groups[dst_group].node_backends[0].base_path
                    })

        # sort params to select "best plans" (first ones are more possible to be executed)
        if len(params) == 0:
            logger.info("No jobs to create")
            return

        created_jobs = self.scheduler.create_jobs(jobs.JobTypes.TYPE_MOVE_JOB, params, self.params)
        logger.info("Created {} move jobs of {} possible".format(len(created_jobs), len(params)))
