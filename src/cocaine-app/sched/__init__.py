from copy import copy, deepcopy
import heapq
from logging import getLogger
import socket
import storage
import time

import pymongo

from errors import CacheUpstreamError
import helpers as h
from infrastructure import infrastructure, UncoupledGroupsSelector
from infrastructure_cache import cache
import inventory
import jobs
from manual_locks import manual_locker
from mastermind_core.config import config
from mastermind_core.db.mongo.pool import Collection
from sync import sync_manager
from sync.error import LockFailedError, LockAlreadyAcquiredError
from timer import periodic_timer
import os
import re
from history import GroupHistoryFinder

import timed_queue

logger = getLogger('mm.sched')


class Scheduler(object):

    def __init__(self, db, niu, job_processor, namespaces_settings):

        self.params = config.get('scheduler', {})

        logger.info('Scheduler initializing')
        self.candidates = []

        self.job_processor = job_processor
        self.__tq = timed_queue.TimedQueue()

        self.node_info_updater = niu
        self.namespaces_settings = namespaces_settings

        starters = []

        if config['metadata'].get('scheduler', {}).get('db'):
            self.collection = Collection(db[config['metadata']['scheduler']['db']], 'scheduler')

    def _start_tq(self):
        self.__tq.start()

    def register_periodic_func(self, starter_func, period_default, starter_name=None, lock_name=None):
        """
        Guarantee that starter_func would be run within specified period under a zookeeper lock
        :param starter_func - a function to be run
        :param period_default - the default value of period to run the function with
            if the "{starter_name}_period" is not specified under {starter_name} sub-section with sched config section
        :param starter_name - a logical name of starter. Name of starter subsection within sched section in config.
                            Defaults to starter_func.__name__
        :param lock_name - a name of lock in zookeeper that protects starter from simultaneous execution).
                            Defaults to "scheduler/{starter_name}
        """

        starter_name = starter_name or starter_func.__name__
        lock_name = lock_name or "scheduler/{}".format(starter_name)
        period_param = "{}_period".format(starter_name)
        period_val = self.params.get(starter_name, {}).get(period_param, period_default)

        logger.info("Registering periodic starter func {} to be raised on {} period".format(starter_name, period_val))

        if not self.params.get(starter_name, {}).get('enabled', False):
            logger.info("Starter {} is disabled".format(starter_name))
            return

        timer = periodic_timer(period_val)

        def _starter_periodic_func(self):
            try:
                logger.info('Starting {}'.format(starter_name))

                with sync_manager.lock(lock_name, blocking=False):
                    starter_func()

            except LockFailedError:
                logger.info('Another {} is already running under {} lock'.format(starter_func.__name__, lock_name))
            except:
                logger.exception('Failed to {}'.format(starter_name))
            finally:
                logger.info('{} finished'.format(starter_name))
                self.__tq.add_task_at(
                    starter_name,
                    timer.next(),
                    _starter_periodic_func)

        self.__tq.add_task_at(
            starter_name,
            timer.next(),
            _starter_periodic_func
        )

        starter = {
            "starter_func": starter_func,
            "starter_name": starter_name,
            "starter_lock": lock_name,
            "starter_timer": timer,
        }

        self.starters.append(starter)

        logger.info("Registered starter function")

    COUNTABLE_STATUSES = [jobs.Job.STATUS_NOT_APPROVED,
                          jobs.Job.STATUS_NEW,
                          jobs.Job.STATUS_EXECUTING]

    @staticmethod
    def jobs_slots(active_jobs, job_type, max_jobs_count):
        jobs_count = 0
        for job in active_jobs:
            if (job.type == job_type and
                    job.status in Scheduler.COUNTABLE_STATUSES):
                jobs_count += 1

        return max_jobs_count - jobs_count

    @staticmethod
    def busy_group_ids(active_jobs):
        busy_group_ids = set()
        for job in active_jobs:
            busy_group_ids.update(job._involved_groups)
        return busy_group_ids

    @staticmethod
    def is_locked(couple, busy_group_ids):
        for group in couple.groups:
            if group.group_id in busy_group_ids:
                return True
        return False

    def get_history(self, sort_field):
        cursor = self.collection.find().sort(sort_field, pymongo.ASCENDING)
        if cursor.count() < len(storage.replicas_groupsets):
            logger.info('Sync recover data is required: {0} records/{1} couples'.format(
                cursor.count(), len(storage.replicas_groupsets)))
            self.sync_recover_data()
            cursor = self.collection.find().sort(sort_field, pymongo.ASCENDING)

        return cursor

    def sync_recover_data(self, lim=200):

        recover_data_couples = set()

        offset = 0
        while True:
            cursor = self.collection.find(
                fields=['couple'],
                sort=[('couple', pymongo.ASCENDING)],
                skip=offset, limit=lim)
            count = 0
            for rdc in cursor:
                recover_data_couples.add(rdc['couple'])
                count += 1
            offset += count

            if count < lim:
                break

        ts = int(time.time())

        storage_couples = set(str(c) for c in storage.replicas_groupsets.keys())

        add_couples = list(storage_couples - recover_data_couples)
        remove_couples = list(recover_data_couples - storage_couples)

        logger.info('Couples to add to recover data list: {0}'.format(add_couples))
        logger.info('Couples to remove from recover data list: {0}'.format(remove_couples))

        offset = 0
        while offset < len(add_couples):
            bulk_op = self.collection.initialize_unordered_bulk_op()
            bulk_add_couples = add_couples[offset:offset + self.RECOVERY_OP_CHUNK]
            for couple in bulk_add_couples:
                bulk_op.insert({'couple': couple,
                                'recover_ts': ts})
            res = bulk_op.execute()
            if res['nInserted'] != len(bulk_add_couples):
                raise ValueError('failed to add couples recover data: {0}/{1} ({2})'.format(
                    res['nInserted'], len(bulk_add_couples), res))
            offset += res['nInserted']

        offset = 0
        while offset < len(remove_couples):
            bulk_op = self.collection.initialize_unordered_bulk_op()
            bulk_remove_couples = remove_couples[offset:offset + self.RECOVERY_OP_CHUNK]
            bulk_op.find({'couple': {'$in': bulk_remove_couples}}).remove()
            res = bulk_op.execute()
            if res['nRemoved'] != len(bulk_remove_couples):
                raise ValueError('failed to remove couples recover data: {0}/{1} ({2})'.format(
                    res['nRemoved'], len(bulk_remove_couples), res))
            offset += res['nRemoved']

    def update_recover_ts(self, couple_id, ts):
        ts = int(ts)
        res = self.collection.update(
            {'couple': couple_id},
            {'couple': couple_id, 'recover_ts': ts},
            upsert=True)
        if res['ok'] != 1:
            logger.error('Unexpected mongo response during recover ts update: {0}'.format(res))
            raise RuntimeError('Mongo operation result: {0}'.format(res['ok']))
