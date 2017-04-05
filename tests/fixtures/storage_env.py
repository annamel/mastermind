import pytest
import json
import os
import traceback
import sys

import storage


STORAGE_FILLER_MARKER_NAME = 'storage_filler_marker'


def _find_marker(request, marker_name):
    if not hasattr(request.node.obj, 'pytestmark'):
        if not hasattr(request.node.obj, marker_name):
            print 'No pytest mark at all {}, {}'.format(request.node.obj, dir(request.node.obj))
            return None
        else:
            return getattr(request.node.obj, marker_name)

    marker = request.node.obj.pytestmark
    if isinstance(marker, list):
        marker = filter(lambda m: m.name == marker_name, marker)
        if not len(marker):
            print 'Marker is empty list'
            return None
        marker = marker[0]
    request.applymarker(marker)
    return marker


def apply_config_diff(json_original, diff):
    return json_original


def _create_backend_from_json(backend_addr, backend_json):
    """
    Modify storage in accordance with the content of json's "backend" section
    :param backend_addr: string "{ip:port/backend_id}"
    :param backend_json: dict of content of json
    """
    try:
        backend_id = backend_json['backend_id']

        node_addr = ':'.join(x for x in backend_addr.split(':')[:-1])
        node = storage.nodes[node_addr]

        node_backend = storage.node_backends.add(node, backend_id)

        node_backend.enable()

        gid = backend_json['group']
        if gid not in storage.groups:
            group = storage.groups.add(gid)
        else:
            group = storage.groups[gid]

        fsid = backend_json['fsid']
        fsid_key = '{host}:{fsid}'.format(host=node.host, fsid=fsid)

        fs = storage.fs.add(node.host, fsid)

        if node_backend not in fs.node_backends:
            fs.add_node_backend(node_backend)

        if node_backend.group is not group:
            group.add_node_backend(node_backend)

    except KeyError as e:
        print "Incorrect json for backend {} ({})".format(backend_json, e)
        traceback.print_exc(file=sys.stdout)
    except Exception as e:
        print "Error while creating backend from json {} ({})".format(backend_json, e)
        traceback.print_exc(file=sys.stdout)


def _create_node_from_json(node_json):
    """
    Modify storage in accordance with the content of the node section
    :param node_json: dict with the content of the node section
    """
    try:
        host = storage.hosts.add(node_json['host_id'])
        storage.nodes.add(host, node_json['port'], node_json['family'])

    except KeyError as e:
        print "Incorrect json for node {} ({})".format(node_json, e)
        traceback.print_exc(file=sys.stdout)
    except Exception as e:
        print "Error while creating node from json {} ({})".format(node_json, e)
        traceback.print_exc(file=sys.stdout)


def _create_group_from_json(group_id, group_json):
    """
    Modify storage in accordance with the 'group' config section
    :param group_id: int gid
    :param group_json: dict with the content of json
    :return:
    """
    try:
        if group_id not in storage.groups:
            storage.groups.add(group_id)

        if group_json['type'] != storage.Group.TYPE_DATA:
            raise RuntimeError('Support for group type {} is nor implemented'.format(group_json['type']))

        if group_json['groupset'] not in storage.groupsets:
            c = storage.groupsets.add(
                groups=(storage.groups[gid] for gid in group_json['metadata']['couple']),
                group_type=group_json['type']
            )

            ns_id = group_json['metadata']['namespace']
            if group_json['metadata']['namespace'] not in storage.namespaces:
                ns = storage.namespaces.add(ns_id)
            else:
                ns = storage.namespaces[ns_id]

            ns.add_couple(c)
    except KeyError as e:
        print "Incorrect json for group {} ({})".format(group_json, e)
        traceback.print_exc(file=sys.stdout)
    except Exception as e:
        print "Error while creating group from json {} ({})".format(group_json, e)
        traceback.print_exc(file=sys.stdout)


@pytest.fixture(scope='function')
def storage_filler(request):
    """
    Storage fixture fills in storage.* data structures with data from signature. Node_info_updater should be turned off
    This approach is supposed to be simpler then an alternative one with changing of MonitorStatParseWorker
    :param request
    :return json_config
    """

    # search for the nearest marker
    marker = _find_marker(request, STORAGE_FILLER_MARKER_NAME)
    if not marker:
        pytest.xfail('Failed to find pytest marker {}'.format(request))

    storage_config_desc = request.node.get_marker(STORAGE_FILLER_MARKER_NAME)
    fn = storage_config_desc.kwargs.get('storage_env_file') if storage_config_desc else ''
    diff = storage_config_desc.kwargs.get('storage_diff') if storage_config_desc else {}

    js = {}
    try:
        # Search for the file in the tests directory
        fn = '{}/{}'.format(os.path.dirname(os.path.dirname(__file__)), fn)
        with open(fn) as f:
            js = json.loads(f.read())
    except IOError as e:
        print 'Failed to open file with json "{}" due to {}'.format(fn, e)
        return {}
    except ValueError as e:
        print 'Failed to parse json {}'.format(e)
        return {}

    storage_json = apply_config_diff(js, diff)

    # Creating node at first. Backends depends on storage.nodes
    for _, node_json in storage_json['nodes'].iteritems():
        _create_node_from_json(node_json)

    for backend_addr, backend_json in storage_json['backends'].iteritems():
        _create_backend_from_json(backend_addr, backend_json)

    for group_id, group_json in storage_json['groups'].iteritems():
        _create_group_from_json(int(group_id), group_json)

    def storage_env_cleanup():
        """
        Clean everything from storage
        """
        storage.groups = []
        storage.node_backends = []
        storage.groupsets = []
        storage.namespaces = []
        storage.hosts = []
        storage.fs = []

    request.addfinalizer(storage_env_cleanup)

    return storage_json
