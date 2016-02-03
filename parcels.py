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
import time

from errors import ProvisionatorException

LOG = logging.getLogger(__name__)


def set_parcel_repo(api, repo_urls):
    """
    Appends given repository to 'REMOTE_PARCEL_REPO_URLS'
    api        -- API client handle
    repo_urls  -- Comma separated list of parcel repository URLs
    """
    cm_config = api.get_cloudera_manager().get_config('full')
    repo_config = cm_config['REMOTE_PARCEL_REPO_URLS']
    value = repo_config.value or repo_config.default
    value += ',' + repo_urls
    api.get_cloudera_manager().update_config({'REMOTE_PARCEL_REPO_URLS': value})
    time.sleep(10)


def get_parcel_by_short_version(cluster, version):
    parcels = [p for p in cluster.get_all_parcels() if p.version.startswith(version)]
    matching_parcels = sorted(parcels, key=lambda p: p.version, reverse=True)
    if len(matching_parcels) < 1:
        LOG.error("No matching parcels found for %s" % version)
        raise ProvisionatorException("No matching parcels found for %s" % version)
    return matching_parcels[0]


def get_parcel(api, cluster_name, parcel_version):
    """
    Get ApiParcel object
    api             -- API client handle
    cluster_name    -- Name of cluster
    parcel_version  -- New parcel version (e.g. '5.5.2-1.cdh5.5.2.p0.118')
    """
    cluster = api.get_cluster(cluster_name)
    return cluster.get_parcel('CDH', parcel_version)


def download_parcel(cluster, parcel):
    """
    Downloads given parcel version
    cluster -- ApiCluster object
    parcel  -- ApiParcel object
    """
    parcel.start_download()
    while True:
        parcel = cluster.get_parcel('CDH', parcel.version)
        if parcel.stage == 'DOWNLOADED':
            break
        if parcel.state.errors:
            raise ProvisionatorException(str(parcel.state.errors))
        LOG.info("Parcel download progress: %s/%s",
                 parcel.state.progress, parcel.state.totalProgress)
        time.sleep(5)

    LOG.info("Downloaded CDH parcel version '%s' on cluster '%s'", parcel, cluster.name)


def distribute_parcel(cluster, parcel):
    """
    Distribute given parcel version
    cluster -- ApiCluster object
    parcel  -- ApiParcel object
    """
    parcel.start_distribution()
    while True:
        parcel = cluster.get_parcel('CDH', parcel.version)
        if parcel.stage == 'DISTRIBUTED':
            break
        if parcel.state.errors:
            raise ProvisionatorException(str(parcel.state.errors))
        LOG.info("Parcel distribution progress: %s/%s",
                 parcel.state.progress, parcel.state.totalProgress)
        time.sleep(5)

    LOG.info("Distributed CDH parcel version '%s' on cluster '%s'",
             parcel.version, cluster.name)


def activate_parcel(cluster, parcel):
    """
    Activate given parcel version
    cluster -- ApiCluster object
    parcel  -- ApiParcel object
    """
    parcel.activate()
    while True:
        parcel = cluster.get_parcel('CDH', parcel.version)
        if parcel.stage == 'ACTIVATED':
            break
        if parcel.state.errors:
            raise ProvisionatorException(str(parcel.state.errors))
        LOG.info("Parcel activation progress: %s/%s",
                 parcel.state.progress, parcel.state.totalProgress)
        time.sleep(5)

    LOG.info("Activated CDH parcel version '%s' on cluster '%s'",
             parcel.version, cluster.name)
