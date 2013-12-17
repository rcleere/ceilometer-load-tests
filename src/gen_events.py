# -*- encoding: utf-8 -*-
#
# Copyright © 2013 Rackspace Hosting
#
# Author: Ryan Cleere <ryan.cleere@rackspace.com>
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
"""Generate random events and pickle them to a file
"""

import argparse
import pickle
import time
from datetime import timedelta

import pools
import rando
import test_setup

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Events File")

    parser.add_argument('--events', '-e', type=int, default=1000,
                        help=("Number of events to insert during test. "
                              "Default: 1000"))
    parser.add_argument('--batch', '-b', type=int, default=100,
                        help=("Number of events to generate before sending to "
                              "the file. Default: 100"))
    parser.add_argument('--store', '-s', type=str, default=None, required=True,
                        help="Filename to store events to.")
    parser.add_argument('--pool', '-f', type=str, default=None,
                        help="Input filename for a randomizer pool dump file.")
    args = parser.parse_args()

    outfile = open(args.store, "w")

    pool = pools.Pool.from_snapshot(args.pool) if args.pool else \
        pools.Pool(args.events, test_setup, None)
    rand = rando.RandomEventGenerator(pool, test_setup)
    revs = args.events / args.batch
    total_events = 0
    all_start = time.time()
    for x in range(1, revs + 1):
        gen_start = time.time()
        events = rand.get_events(args.batch)
        gen_end = time.time()
        p_start = time.time()
        for event in events:
            pickle.dump(event, outfile)
        p_end = time.time()
        total_events += args.batch
        gen_time = gen_end - gen_start
        p_time = p_end - p_start
        print "Generated %i (%i/%i) events in %s and pickle.dumped in %s" % (args.batch, total_events, args.events, str(timedelta(seconds=gen_time)), str(timedelta(seconds=p_time)))
    all_end = time.time()
    total_time = all_end - all_start
    print "Wrote %i events to \"%s\", took %s" % (total_events, args.store, str(timedelta(seconds=total_time)))

    outfile.close()
