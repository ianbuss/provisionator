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

import parcels
import service
import util

LOG = logging.getLogger(__name__)


def check_clusters(api, name, version, host_ids):
    existing_clusters = api.get_all_clusters()
    for c in existing_clusters:
        LOG.debug("Existing cluster: %s" % c)
        # Matching cluster already exists?
        if c.name == name:
            if c.fullVersion == version:
                # Should also check host membership...
                LOG.info("Cluster %s, version %s already exists" % (name, version))
            else:
                LOG.warn(
                        "Cluster %s already exists but has a different version: %s" % (
                            name, version))
            return c
    return None


def _add(conf, api, hosts):
    name = conf['cluster']['name']
    version = conf['cluster']['version']
    hostnames = conf['cluster']['hosts']

    host_ids = []
    for hostname in hostnames:
        host_ids.append(hosts[hostname])

    cluster = check_clusters(api, name, version, host_ids)
    if not cluster:
        LOG.info(
                "Creating cluster: name => %s, version => %s, hosts => %s" % (
                    name, version, hostnames))
        cluster = api.create_cluster(name, fullVersion=version)
        LOG.debug("Adding host IDs: %s" % host_ids)
        cluster.add_hosts(host_ids)

        # Get parcels
        parcel = parcels.get_parcel_by_short_version(cluster, version)

        # Download, distribute and activate parcel
        LOG.info("Downloading %s, %s" % (parcel.product, parcel.version))
        parcels.download_parcel(cluster, parcel)
        LOG.info("Distributing %s" % parcel.version)
        parcels.distribute_parcel(cluster, parcel)
        LOG.info("Activating %s" % parcel.version)
        parcels.activate_parcel(cluster, parcel)

    return cluster


def add_or_update(conf, api, hosts):
    cl = _add(conf, api, hosts)

    # Add services
    for svc in conf['cluster']['services']:
        service.add_or_update(svc, cl, hosts, False)
        restart(cl, True)
        service.start(svc, cl)


def restart(cluster, exception_on_fail=True):
    cmd = cluster.restart(restart_only_stale_services=True, redeploy_client_configuration=True)
    util.wait_for_command(cmd, exception_on_fail)
