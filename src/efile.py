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
"""Input pre-generated events for Ceilometer storage layer.
"""

import time
import pickle

from ceilometer.storage import models

class EventsFromFile(object):

    def get_events(self, quantity):
        """Get X events from file
        """
        event_models = []
        for x in range(quantity):
            event_models.append(pickle.load(self.fd))
        return event_models

    def __init__(self, event_file, settings):
        self.fd = open(event_file, "r")
