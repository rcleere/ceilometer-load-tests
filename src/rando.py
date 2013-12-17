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
"""Random event generator for Ceilometer storage layer.
"""

import datetime
import itertools
import random
import time
import uuid

from ceilometer.storage import models


class RandomEventGenerator(object):

    def _get_next_generated(self):
        """Gets the next generated event timestamp.

        :Note: Given some potential, it may also have a negative delta to
        simulate messages out of order.
        """
        negative_boundary = -float(self.rand_generated_potential) / \
            self.message_order_integrity
        delta = random.randint(negative_boundary,
                               self.rand_generated_potential)
        self.last_generated = self.last_generated + \
            datetime.timedelta(seconds=delta)
        return self.last_generated

    def _generate_random_timestamp(self):
        """Returns a random datetime from from_time to to_time (UNIX ts)
        """
        return datetime.datetime.utcfromtimestamp(
            random.randint(self.timestamp_start, self.timestamp_end))

    def _generate_random_integer(self):
        """Generates a random integer from pre-set constraints
        """
        return random.randint(self.rand_int_start,
                              self.rand_int_start +
                              self.rand_int_range)

    def _generate_random_float(self):
        """Generates a random float.
        """
        return random.random()

    def _generate_random_text(self):
        """Chooses a random string from a pre-made string pool.
        """
        return random.choice(self.pool.strings_pool)

    def _make_traits(self, traits_list):
        """Given a list of (key, dtype, pool=None) tuples, build a Traits list.
        """
        traits = []
        for key, dtype, pool in itertools.chain(*traits_list):
            if not pool:
                pool = [self._rand_type_map[dtype]()]
            traits.append(
                models.Trait(key, dtype, random.choice(pool))
            )
        return traits

    def _create_random_event(self):
        """Randomizes event attributes and traits.
        """
        required_traits = None
        event_type = self.pool.events_pool[random.randrange(0,
                                           len(self.pool.events_pool))]
        if event_type.startswith('compute'):
            required_traits = self.pool.compute_keys
        elif event_type.startswith('image'):
            required_traits = self.pool.glance_keys

        extra_traits = random.sample(self.pool.extra_traits_pool,
                                     self.extra_traits_per_event)
        traits_list = [required_traits, self.pool.required_keys,
                       extra_traits]
        trait_models = self._make_traits(traits_list)
        return models.Event(str(uuid.uuid4()),
                            event_type,
                            self._get_next_generated(), trait_models)

    def get_events(self, quantity):
        """Generates N random events.
        """
        event_models = []
        for x in range(quantity):
            event_models.append(self._create_random_event())
        return event_models

    def generate_random_event(self):
        """Generates a single random event.
        """
        return self._create_random_event()

    def __init__(self, pool, settings):
        self.pool = pool
        self.last_generated = datetime.datetime.utcnow()
        self.rand_int_start = settings.rand_int_start
        self.rand_int_range = settings.rand_int_range
        self.extra_traits_per_event = settings.extra_traits_per_event
        self.rand_generated_potential = settings.rand_generated_potential
        self.message_order_integrity = settings.message_order_integrity
        self.timestamp_start = settings.timestamp_start or int(time.time())
        self.timestamp_end = settings.timestamp_end or (self.ts_start + 86400)
        self._rand_type_map = {
            models.Trait.INT_TYPE: self._generate_random_integer,
            models.Trait.FLOAT_TYPE: self._generate_random_float,
            models.Trait.DATETIME_TYPE: self._generate_random_timestamp,
            models.Trait.TEXT_TYPE: self._generate_random_text
        }
