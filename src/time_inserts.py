# -*- encoding: utf-8 -*-
#
# Copyright © 2013 Rackspace Hosting
#
# Author: Thomas Maddox <thomas.maddox@rackspace.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
"""Ceilometer load testing application.
"""

import argparse
import time

from ceilometer import storage
from oslo.config import cfg
from pymongo import errors

import efile
import plugins
import pools
import rando
import test_setup

cfg.CONF.set_override("connection", test_setup.db_conn, group='database')


def before_test(event_generator, plugins, conn, settings):
    if test_setup.db_conn.startswith('mongodb'):
        conn.write_concern = {'w': settings.write_concern,
                              'j': settings.journaling}
        if settings.sharding:
            try:
                conn.conn.admin.command('enablesharding', 'ceilometer')
                conn.conn.admin.command('shardcollection', 'ceilometer.event',
                                        key={'_id': "hashed"})
            except errors.OperationFailure:
                pass


def run_test(event_generator, plugin_list, conn, settings):
    total_seconds = 0
    total_failed = 0
    delta_history = []
    revs = settings.events / settings.batch
    publish_frequency = settings.publish
    for x in range(1, revs + 1):
        events = event_generator.get_events(settings.batch)
        start = time.time()
        failed = conn.record_events(events)
        end = time.time()
        total_seconds += end - start
        total_failed += len(failed)

        if x % publish_frequency == 0:
            delta_history.append(total_seconds)
            stats = {'stored': publish_frequency * settings.batch,
                     'frequency': settings.publish,
                     'seconds': total_seconds,
                     'total_stored': x * settings.batch,
                     'failed': total_failed}
            plugins.invoke('publish', plugin_list, stats)
            total_seconds = 0
            total_failed = 0
            time.sleep(settings.rest)

    totals = {'total_seconds': sum(delta_history),
              'total_events': settings.events}
    plugins.invoke('after_test', plugin_list, totals)


def after_test(event_generator, plugins, conn, settings):
    pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Time Inserting Events")

    parser.add_argument('--name', '-n', type=str, required=True,
                        help="Name of the test; used for publishing stats.")
    parser.add_argument('--events', '-e', type=int, default=1000,
                        help=("Number of events to insert during test. "
                              "Default: 1000"))
    parser.add_argument('--input', '-i', type=str, default=None,
                        help=("File to read pregenerated events from"))
    parser.add_argument('--batch', '-b', type=int, default=100,
                        help=("Number of events to generate before sending to "
                              "the database. Default: 100"))
    parser.add_argument('--publish', '-p', type=int, default=2,
                        help=("Number of batches to accumulate before"
                              "publishing stats. Default: 2"))
    parser.add_argument('--rest', '-r', type=int, default=0,
                        help="Seconds to rest between batches. Default: 0")
    parser.add_argument('--store', '-s', type=str, default=None,
                        help="Filename to store pool dump with.")
    parser.add_argument('--pool', '-f', type=str, default=None,
                        help="Input filename for a randomizer pool dump file.")
    parser.add_argument('--journaling', '-j', action='store_true',
                        help=("Enable journaling, if the datastore supports"
                              " it."))
    parser.add_argument('--write_concern', '-w', type=int, default=1,
                        help=("Write concern level, if the datastore supports"
                              " it. Default: 1"))
    parser.add_argument('--sharding', action='store_true',
                        help="Enforce a sharded datastore, if supported.")

    args = parser.parse_args()
    if args.input is not None:
        pool = pools.Pool.from_snapshot(args.pool) if args.pool else \
            pools.Pool(args.events, test_setup, store=args.store)
        generator = rando.RandomEventGenerator(pool, test_setup)
    else:
        generator = efile.EventsFromFile(args.input, test_setup)
    plugin_list = plugins.initialize_plugins(args.name, test_setup.plugins)
    conn = storage.get_connection(cfg.CONF)

    before_test(generator, plugin_list, conn, args)
    run_test(generator, plugin_list, conn, args)
