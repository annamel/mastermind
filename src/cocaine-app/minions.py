import datetime
import json
import threading
import time
import traceback
import urllib

from cocaine.logging import Logger
import elliptics
import msgpack
from tornado.httpclient import AsyncHTTPClient, HTTPClient
from tornado.ioloop import IOLoop

from config import config
import errors
import keys
import timed_queue
import storage


logging = Logger()


class Minions(object):

    STATE_FETCH = 'state_fetch'
    STATE_FETCH_ACTIVE = 'state_fetch_active'
    HISTORY_FETCH = 'history_fetch'
    HISTORY_ENTRY_FETCH = 'history_entry_%s_fetch'

    MAKE_IOLOOP = 'make_ioloop'
    STATE_URL_TPL = 'http://{host}:{port}/rsync/list/'
    START_URL_TPL = 'http://{host}:{port}/rsync/start/'

    def __init__(self, node):
        self.node = node
        self.meta_session = self.node.meta_session

        self.commands = {}
        self.history = {}

        self.__tq = timed_queue.TimedQueue()
        self.__tq.start()

        self.__tq.add_task_in(self.MAKE_IOLOOP, 0,
            self._make_tq_thread_ioloop)

        self.__tq.add_task_in(self.HISTORY_FETCH,
            5, self._fetch_history)

        self.__tq.add_task_in(self.STATE_FETCH,
            5, self._fetch_states)

        self.__commands_lock = threading.Lock()

    def _make_tq_thread_ioloop(self):
        logging.debug('Minion states, creating thread ioloop')
        io_loop = IOLoop()
        io_loop.make_current()

    def _fetch_states(self, hosts=None):
        logging.info('Fetching minion states task started')
        try:
            states = {}

            if hosts is None:
                hosts = storage.hosts
            for host in hosts:
                url = self.STATE_URL_TPL.format(host=host.addr, port='8080')
                states[url] = host
            logging.debug('Starting async batch')
            responses = AsyncHTTPBatch(states.keys()).get()
            for url, response in responses.iteritems():
                host = states[url]

                data = self._get_response(host, response)
                if not data:
                    continue
                try:
                    self._process_state(host.addr, data)
                except errors.MinionApiError:
                    continue
            logging.info('Finished fetching minion states task')
        except Exception as e:
            logging.error('Failed to sync minions state: %s\n%s' %
                          (e, traceback.format_exc()))
        finally:
            self.__tq.add_task_in(self.STATE_FETCH,
                config.get('minions_fetch_period', 50),
                self._fetch_states)

    def _process_state(self, addr, response):

        response_data = self._get_wrapped_response(json.loads(response))

        hostname = storage.hosts[addr].hostname
        for uid, state in response_data.iteritems():
            state['uid'] = uid
            state['host'] = addr
            state['hostname'] = hostname

        with self.__commands_lock:
            self.commands.update(response_data)

        for uid, state in response_data.iteritems():
            if (self.history.get(uid) is None or
                int(self.history[uid]) != int(state['progress'])):

                logging.debug('Adding new task {0}'.format(self.HISTORY_ENTRY_FETCH % uid))
                self.__tq.add_task_in(self.HISTORY_ENTRY_FETCH % uid,
                    config.get('minions_history_entry_update_delay', 1),
                    self._history_entry_update, state)

        for uid, state in response_data.iteritems():
            if state['progress'] < 1.0:
                self.__tq.add_task_in(self.STATE_FETCH_ACTIVE,
                    config.get('minions_active_fetch_period', 5),
                    self._fetch_states,
                    hosts=[storage.hosts[addr]])
                break

        return response_data

    def _get_response(self, host, response):
        if response.error:
            code = response.error.code
            if code == 599:
                logging.debug('Failed to connect to minion '
                              'on host {0} ({1})'.format(host, response.error.message))
            else:
                logging.debug('Minion http error on host {0}, '
                              'code {1} ({2})'.format(host, code, response.error.message))
            return None

        return response.body

    def _get_wrapped_response(self, response):
        if response['status'] != 'success':
            logging.warn('Host: {0}, minion returned error: {1}'.format(
                host, response['error']))
            raise errors.MinionApiError('Minion error: {0}'.format(response['error']))
        return response['response']

    def get_command(self, request):
        try:
            uid = request[0]
        except ValueError:
            raise ValueError('Invalid parameters')
        return self.commands[uid]

    def get_commands(self, request):
        return sorted(self.commands.itervalues(), key=lambda c: c['start_ts'])

    def execute_cmd(self, request):
        try:
            host, command, params = request[0:3]
        except ValueError:
            raise ValueError('Invalid parameters')

        if not host in storage.hosts:
            raise ValueError('Host {0} is not present in cluster'.format(host))

        url = self.START_URL_TPL.format(host=host, port='8080')
        data = {'command': command}
        for k, v in params.iteritems():
            if k == 'command':
                raise ValueError('Parameter "command" is not accepted as command parameter')
            if not isinstance(v, basestring):
                logging.warn('Failed parameter: %s' % (v,))
                raise ValueError('Only strings are accepted as command parameters')
            data[k] = v

        headers = {'X-Auth': 'kLrgtdt}?#U}zi~WJul3NmJ}Ey3kT0xJH'}
        response = HTTPClient().fetch(url, method='POST',
                                           headers=headers,
                                           body=urllib.urlencode(data))

        data = self._process_state(host, self._get_response(host, response))
        return data

    # history processing

    def _fetch_history(self):
        try:
            logging.info('Fetching minion commands history')
            now = datetime.datetime.now()

            for entry in self._history_entries(now):
                logging.debug('Fetched history entry for command {0}'.format(entry['uid']))
                self.history[entry['uid']] = entry['progress']

            logging.info('Finished fetching minion commands history')
        except Exception as e:
            logging.info('Failed to fetch minion history')
            logging.exception(e)
        finally:
            self.__tq.add_task_in(self.HISTORY_FETCH,
                config.get('minions_history_fetch_period', 120),
                self._fetch_history)

    def _history_entries(self, dt):
        key = keys.MINION_HISTORY_KEY % dt.strftime('%Y-%m')
        idxs = self.meta_session.find_all_indexes([key])
        for idx in idxs:
            data = idx.indexes[0].data
            entry = self._unserialize(data)
            yield entry

    def _history_entry_update(self, state):
        try:
            uid = state['uid']
            logging.debug('Started updating minion history entry '
                          'for command {0}'.format(uid))
            update_history_entry = False
            try:
                eid = elliptics.Id(keys.MINION_HISTORY_ENTRY_KEY % uid.encode('utf-8'))
                # set to read_latest when it raises NotFoundError on non-existent keys
                r = self.meta_session.read_data(eid).get()[0]
                history_state = self._unserialize(r.data)
                self.history[uid] = history_state['progress']
                if int(self.history[uid]) != int(state['progress']):
                    update_history_entry = True
            except elliptics.NotFoundError:
                logging.debug('History state is not found')
                update_history_entry = True

            if update_history_entry:
                logging.info('Updating minion history entry for command {0}'.format(uid))
                start = datetime.datetime.fromtimestamp(state['start_ts'])
                key = keys.MINION_HISTORY_KEY % start.strftime('%Y-%m')
                self.meta_session.update_indexes(eid, [key], [self._serialize(state)])
                self.history[uid] = state['progress']
            else:
                logging.debug('Update for minion history entry '
                              'of command {0} is not required'.format(uid))
        except Exception as e:
            logging.error('Failed to update minion history entry for '
                          'uid {0}: {1}\n{2}'.format(uid, str(e), traceback.format_exc()))

    @staticmethod
    def _unserialize(data):
        return msgpack.unpackb(data)

    @staticmethod
    def _serialize(data):
        return msgpack.packb(data)

    def minion_history_log(self, request):
        try:
            year, month = [int(_) for _ in request[0:2]]
        except ValueError:
            raise ValueError('Invalid parameters')

        dt = datetime.datetime(year, month, 1)

        entries = self._history_entries(dt)
        return sorted(entries, key=lambda e: e['start_ts'])


class AsyncHTTPBatch(object):

    HEADERS = {'X-Auth': 'kLrgtdt}?#U}zi~WJul3NmJ}Ey3kT0xJH'}

    def __init__(self, urls, timeout=3):
        self.left = len(urls)
        self.urls = urls
        self.timeout = timeout
        self.ioloop = IOLoop.current()
        logging.debug('Minion states, ioloop fetched: {0}'.format(self.ioloop))
        self.responses = {}

    def get(self, emergency_timeout=None):
        self.emergency_timeout = self.ioloop.add_timeout(
            time.time() + (emergency_timeout or self.timeout * 2),
            self._emergency_halt)
        logging.debug('Minion states, creating async http clients')
        [AsyncHTTPClient().fetch(url, callback=self._process,
            request_timeout=self.timeout, headers=self.HEADERS) for url in self.urls]
        logging.debug('Minion states, starting ioloop')
        self.ioloop.start()
        return self.responses

    def _process(self, response):
        logging.debug('Minion states, received response from {0} '
                      '({1:.4f}s)'.format(response.request.url, response.request_time))
        self.responses[response.request.url] = response
        self.left -= 1
        if not self.left:
            logging.debug('Minion states, stopping loop')
            self.ioloop.remove_timeout(self.emergency_timeout)
            self.ioloop.stop()

    def _emergency_halt(self):
        logging.warn('Minion states, emergency exit. '
                     'Unprocessed requests: {0}'.format(self.left))
        self.ioloop.stop()

