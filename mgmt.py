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

import util
from cm_api.api_client import ApiException
from cm_api.endpoints.services import ApiServiceSetupInfo

LOG = logging.getLogger(__name__)


def add_or_update(conf, api, hosts, remove=False):
    cm = api.get_cloudera_manager()

    default_host = hosts[conf['cm']['host']]

    mgmt_cfg = {}
    if conf['mgmt']['config']:
        mgmt_cfg = conf['mgmt']['config']

    # Check for an existing MGMT service
    mgmt_svc = None
    new = False
    try:
        mgmt_svc = cm.get_service()
        LOG.info("Cloudera Management Service already exists, skipping creation")
    except ApiException:
        pass

    if not mgmt_svc:
        new = True
        mgmt_setup_info = ApiServiceSetupInfo(name="mgmt", type="MGMT", config=mgmt_cfg)
        mgmt_svc = cm.create_mgmt_service(mgmt_setup_info)
    else:
        mgmt_svc.update_config(mgmt_cfg)

    for svc in conf['mgmt']['services']:
        # Use CM host as default for MGMT hosts
        svc_host = default_host
        if 'host' in svc:
            svc_host = hosts[svc['host']]
        rolename = "mgmt-%s" % svc['name']
        try:
            mgmt_svc.get_role(rolename)
        except ApiException:
            mgmt_svc.create_role(rolename, svc['name'], svc_host)
        svc_grp = mgmt_svc.get_role_config_group("mgmt-%s-BASE" % svc['name'])
        svc_grp.update_config(svc['config'])

    stale, client_stale = get_staleness(cm)
    if new:
        start(cm)
    elif stale:
        restart(cm)


def start(cm, exception_on_fail=True):
    LOG.debug("Starting management services")
    cmd = cm.get_service().start()
    util.wait_for_command(cmd, exception_on_fail)


def restart(cm, exception_on_fail=True):
    LOG.debug("Restarting management services")
    cmd = cm.get_service().restart()
    util.wait_for_command(cmd, exception_on_fail)


def get_staleness(cm):
    svc = cm.get_service()
    return svc.configStale == 'true', svc.clientConfigStalenessStatus == 'true'
