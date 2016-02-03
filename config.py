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

import json
import logging
import sys

LOG = logging.getLogger(__name__)


def read_json_config(configfile):
    LOG.debug("Reading JSON config from %s" % configfile)
    with open(configfile, 'r') as f:
        config = json.load(f)
        LOG.debug("JSON Config: %s" % json.dumps(config, indent=2))
        return config


def read_config(configfile, fmt="json"):
    if fmt == "json":
        return read_json_config(configfile)
    else:
        LOG.error("Config files in %s format not supported" % fmt)


def get_cluster_service_by_type(config, type):
    svc = None
    if 'cluster' in config and 'services' in config['cluster']:
        for s in config['cluster']['services']:
            if 'type' in s and s['type'] == type.upper():
                svc = s
    return svc


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    if len(sys.argv) > 1:
        read_config(sys.argv[1])
    else:
        LOG.error("No config file supplied")
