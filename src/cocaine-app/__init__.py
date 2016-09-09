#!/usr/bin/python
# encoding: utf-8
from functools import wraps
import logging
import signal
import sys
from time import sleep, time
import traceback
import uuid

# NB: pool should be initialized before importing
# any of cocaine-framework-python modules to avoid
# tornado ioloop dispatcher issues
import monitor_pool

from cocaine.worker import Worker
from cocaine.futures import chain

sys.path.append('/usr/lib')

import msgpack

import elliptics

import log
log.setup_logger()
logger = logging.getLogger('mm.init')

# storage should be imported before balancer
# TODO: remove this dependency
import storage
import balancer
from db.mongo.pool import MongoReplicaSetClient
import external_storage
import helpers
import history
import infrastructure
import jobs
import couple_records
import minions
import node_info_updater
from planner import Planner
from config import config
from manual_locks import manual_locker


i = iter(xrange(100))
logger.info("trace %d" % (i.next()))


def term_handler(signo, frame):
    # required to guarantee execution of cleanup functions registered
    # with atexit.register
    sys.exit(0)


signal.signal(signal.SIGTERM, term_handler)

nodes = config.get('elliptics', {}).get('nodes', []) or config["elliptics_nodes"]
logger.debug("config: %s" % str(nodes))

logger.info("trace %d" % (i.next()))
log = elliptics.Logger(str(config["dnet_log"]), config["dnet_log_mask"])

node_config = elliptics.Config()
node_config.io_thread_num = config.get('io_thread_num', 1)
node_config.nonblocking_io_thread_num = config.get('nonblocking_io_thread_num', 1)
node_config.net_thread_num = config.get('net_thread_num', 1)

logger.info(
    'Node config: io_thread_num {0}, nonblocking_io_thread_num {1}, '
    'net_thread_num {2}'.format(
        node_config.io_thread_num,
        node_config.nonblocking_io_thread_num,
        node_config.net_thread_num
    )
)

n = elliptics.Node(log, node_config)

logger.info("trace %d" % (i.next()))

addresses = []
for node in nodes:
    try:
        addresses.append(elliptics.Address(
            host=str(node[0]), port=node[1], family=node[2]))
    except Exception as e:
        logger.error('Failed to connect to storage node: {0}:{1}:{2}'.format(
            node[0], node[1], node[2]))
        pass

try:
    n.add_remotes(addresses)
except Exception as e:
    logger.error('Failed to connect to any elliptics storage node: {0}'.format(
        e))
    raise ValueError('Failed to connect to any elliptics storage node')

logger.info("trace %d" % (i.next()))
meta_node = elliptics.Node(log, node_config)

addresses = []
for node in config["metadata"]["nodes"]:
    try:
        addresses.append(elliptics.Address(
            host=str(node[0]), port=node[1], family=node[2]))
    except Exception as e:
        logger.error('Failed to connect to meta node: {0}:{1}:{2}'.format(
            node[0], node[1], node[2]))
        pass

logger.info('Connecting to meta nodes: {0}'.format(config["metadata"]["nodes"]))

try:
    meta_node.add_remotes(addresses)
except Exception as e:
    logger.error('Failed to connect to any elliptics meta storage node: {0}'.format(
        e))
    raise ValueError('Failed to connect to any elliptics storage META node')


wait_timeout = config.get('wait_timeout', 5)
logger.info('sleeping for wait_timeout for nodes to collect data ({0} sec)'.format(wait_timeout))
sleep(wait_timeout)

meta_wait_timeout = config['metadata'].get('wait_timeout', 5)

meta_session = elliptics.Session(meta_node)
meta_session.set_timeout(meta_wait_timeout)
meta_session.add_groups(list(config["metadata"]["groups"]))
logger.info("trace %d" % (i.next()))
n.meta_session = meta_session

mrsc_options = config['metadata'].get('options', {})

meta_db = None
if config['metadata'].get('url'):
    meta_db = MongoReplicaSetClient(config['metadata']['url'], **mrsc_options)


logger.info("trace %d" % (i.next()))
logger.info("before creating worker")
W = Worker(disown_timeout=config.get('disown_timeout', 2))
logger.info("after creating worker")


b = balancer.Balancer(n, meta_db)


def register_handle(h):
    @wraps(h)
    def wrapper(request, response):
        start_ts = time()
        req_uid = uuid.uuid4().hex
        try:
            data = yield request.read()
            data = msgpack.unpackb(data)
            logger.info(
                ':{req_uid}: Running handler for event {0}, '
                'data={1}'.format(
                    h.__name__,
                    str(data),
                    req_uid=req_uid
                )
            )
            res = h(data)
            if isinstance(res, chain.Chain):
                res = yield res
            else:
                logger.error('Synchronous handler for {0} handle'.format(h.__name__))
            response.write(res)
        except Exception as e:
            logger.error(
                ':{req_uid}: handler for event {0}, data={1}: Balancer error: {2}\n{3}'.format(
                    h.__name__, str(data), e,
                    traceback.format_exc().replace('\n', '    '),
                    req_uid=req_uid
                )
            )
            response.write({"Balancer error": str(e)})
        finally:
            logger.info(':{req_uid}: Finished handler for event {0}, time: {1:.3f}'.format(
                h.__name__,
                time() - start_ts,
                req_uid=req_uid)
            )
        response.close()

    W.on(h.__name__, wrapper)
    logger.info("Registering handler for event %s" % h.__name__)
    return wrapper


def init_infrastructure(jf, ghf):
    infstruct = infrastructure.infrastructure
    infstruct.init(n, jf, ghf)
    register_handle(infstruct.shutdown_node_cmd)
    register_handle(infstruct.start_node_cmd)
    register_handle(infstruct.disable_node_backend_cmd)
    register_handle(infstruct.enable_node_backend_cmd)
    register_handle(infstruct.reconfigure_node_cmd)
    register_handle(infstruct.recover_group_cmd)
    register_handle(infstruct.defrag_node_backend_cmd)
    register_handle(infstruct.search_history_by_path)
    b._set_infrastructure(infstruct)
    return infstruct


def init_node_info_updater(jf, crf, statistics):
    logger.info("trace node info updater %d" % (i.next()))
    niu = node_info_updater.NodeInfoUpdater(
        node=n,
        job_finder=jf,
        couple_record_finder=crf,
        prepare_namespaces_states=True,
        prepare_flow_stats=True,
        statistics=statistics)
    niu.start()
    register_handle(niu.force_nodes_update)
    register_handle(niu.force_update_namespaces_states)
    register_handle(niu.force_update_flow_stats)

    return niu


def init_statistics():
    register_handle(b.statistics.get_groups_tree)
    register_handle(b.statistics.get_couple_statistics)
    return b.statistics


def init_minions():
    m = minions.Minions(n)
    register_handle(m.get_command)
    register_handle(m.get_commands)
    register_handle(m.execute_cmd)
    register_handle(m.terminate_cmd)
    return m


def init_planner(job_processor, niu):
    planner = Planner(n.meta_session, meta_db, niu, job_processor)
    register_handle(planner.restore_group)
    register_handle(planner.move_group)
    register_handle(planner.move_groups_from_host)
    register_handle(planner.convert_external_storage_to_groupset)
    return planner


def init_job_finder():
    if not config['metadata'].get('jobs', {}).get('db'):
        logger.error(
            'Job finder metadb is not set up '
            '("metadata.jobs.db" key), will not be initialized'
        )
        return None
    jf = jobs.JobFinder(meta_db)
    register_handle(jf.get_job_list)
    register_handle(jf.get_job_status)
    register_handle(jf.get_jobs_status)
    return jf


def init_external_storage_meta():
    if not config['metadata'].get('external_storage', {}).get('db'):
        logger.error(
            'External storage metadb is not set up '
            '("metadata.external_storage.db" key), will not be initialized'
        )
    external_storage_meta = external_storage.ExternalStorageMeta(meta_db)
    register_handle(external_storage_meta.get_external_storage_mapping)
    return external_storage_meta


def init_couple_record_finder():
    if not config['metadata'].get('couples', {}).get('db'):
        msg = (
            'Couple finder metadb is not set up '
            '("metadata.couples.db" key), will not be initialized'
        )
        logger.error(msg)
        raise RuntimeError(msg)
    crf = couple_records.CoupleRecordFinder(meta_db)
    return crf


def init_group_history_finder():
    if not config['metadata'].get('history', {}).get('db'):
        logger.error(
            'History finder metadb is not set up '
            '("metadata.history.db" key), will not be initialized'
        )
        return None
    ghf = history.GroupHistoryFinder(meta_db)
    return ghf


def init_job_processor(jf, minions, niu, external_storage_meta, couple_record_finder):
    if jf is None:
        logger.error(
            'Job processor will not be initialized because '
            'job finder is not initialized'
        )
        return None
    j = jobs.JobProcessor(
        jf,
        n,
        meta_db,
        niu,
        minions,
        external_storage_meta=external_storage_meta,
        couple_record_finder=couple_record_finder,
    )
    register_handle(j.create_job)
    register_handle(j.cancel_job)
    register_handle(j.approve_job)
    register_handle(j.stop_jobs)
    register_handle(j.retry_failed_job_task)
    register_handle(j.skip_failed_job_task)
    register_handle(j.restart_failed_to_start_job)
    register_handle(j.build_lrc_groups)
    register_handle(j.add_groupset_to_couple)
    return j


def init_manual_locker(manual_locker):
    register_handle(manual_locker.host_acquire_lock)
    register_handle(manual_locker.host_release_lock)
    return manual_locker


jf = init_job_finder()

external_storage_meta = init_external_storage_meta()
crf = init_couple_record_finder()
ghf = init_group_history_finder()
io = init_infrastructure(jf, ghf)
niu = init_node_info_updater(jf, crf, b.statistics)
b.niu = niu
b.start()
init_statistics()
m = init_minions()
j = init_job_processor(jf, m, niu, external_storage_meta, crf)
if j:
    po = init_planner(j, niu)
    j.planner = po
else:
    po = None
ml = init_manual_locker(manual_locker)


for handler in balancer.handlers(b):
    logger.info("registering bounded function %s" % handler)
    if getattr(handler, '__wne', False):
        helpers.register_handle_wne(W, handler)
    else:
        register_handle(handler)

logger.info('activating timed queues')
try:
    tq_to_activate = [io, b.niu, m, j, po, b]
    for tqo in tq_to_activate:
        if tqo is None:
            continue
        tqo._start_tq()
except Exception as e:
    logger.error('failed to activate timed queue: {0}'.format(e))
    raise
logger.info('finished activating timed queues')

logger.info("Starting worker")
W.run()
logger.info("Initialized")
