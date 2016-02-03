#!/usr/bin/env python

#
# (c) Copyright 2015 Cloudera, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import sys

from cm_api.api_client import ApiException

import cluster
import cm
import config
import mgmt
import util

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

LOG = logging.getLogger(__name__)


def provision(config):
    try:
        # Open a connection to CM and get a CM object
        api = util.get_api_handle(config)

        # Get dictionary of hostname to host ids
        host_id_map = util.host_id_map(config, api)

        # Update CM config
        cm.update_config(config, api)

        # Update CM license
        cm.update_license(config, api)

        # Update all hosts configuration
        cm.update_hosts_config(config, api)

        # Create Cloudera Management Services
        mgmt.add_or_update(config, api, host_id_map)

        # Add cluster and services
        cluster.add_or_update(config, api, host_id_map)

    except ApiException, e:
        logging.error("Error: %s" % e)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.fatal("No configuration file specified")
        sys.exit(-1)

    config = config.read_config(sys.argv[1])

    provision(config)
