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

import copy
import logging

from cm_api.api_client import ApiException

import util
from errors import ProvisionatorException

LOG = logging.getLogger(__name__)
SVC_INTERPOLATE = ['HUE']


def _interpolate_roles(config, cluster, host_id_map):
    # Take a deep copy since we will potentially change the configuration
    _conf = copy.deepcopy(config)
    _poplist = []

    for k in _conf:
        if _conf[k].startswith('@@') and _conf[k].endswith('@@'):
            # Strip @@s
            _value = _conf[k][2:-2]
            _svstring, _rlstring, _hoststring = _value.split('-', 2)
            if not _svstring or not _rlstring or not _hoststring:
                # This is an invalid interpolation string - remove it
                LOG.warn("Could not parse [%s] as service-role-hostname, removing" % _value)
                _poplist.append(k)
                continue

            try:
                _sv = cluster.get_service(_svstring)
                _roles = _sv.get_roles_by_type(_rlstring, view='full')
                _hostid = host_id_map[_hoststring]
                for r in _roles:
                    if r.hostRef.hostId == _hostid:
                        LOG.info("Replacing [%s => %s] with [%s]" % (k, _conf[k], r.name))
                        _conf[k] = r.name
            except ApiException, e:
                LOG.warn("No such service [%s] in cluster. Removing [%s => %s] from configuration" %
                         (_svstring, k, _conf[k]))
                _poplist.append(k)

    # Remove invalid configuration variables
    for p in _poplist:
        _conf.pop(p)

    return _conf


def rename(service, display_name):
    service._get_resource_root().put(service._path(),
                                     data="{\"displayName\":\"%s\"}" % display_name)


def add_or_update(svc_config, cluster, host_id_map, start_service=True):
    LOG.debug("Service configuration: %s" % svc_config)

    to_interpolate = svc_config['type'] in SVC_INTERPOLATE
    new = False

    try:
        service = cluster.get_service(svc_config['name'])
        LOG.info("Service %s already exists on cluster %s, skipping creation" % (
            svc_config['name'], cluster.name))
    except ApiException, e:
        new = True
        logging.info("Creating service %s, type %s" % (svc_config['name'], svc_config['type']))
        service = cluster.create_service(svc_config['name'], svc_config['type'])
        LOG.debug("Service created: %s", service)

    # Service-wide config
    if 'config' in svc_config:
        _conf = svc_config['config']
        if to_interpolate:
            _conf = _interpolate_roles(_conf, cluster, host_id_map)
        service.update_config(_conf)

    # Rename
    if 'displayname' in svc_config:
        rename(service, svc_config['displayname'])

    # Add roles
    if 'roles' in svc_config:
        for role in svc_config['roles']:
            if 'hosts' in role:
                for host in role['hosts']:
                    role_name = "%s-%s-%s" % (svc_config['name'], role['type'], util.id(host))
                    if len(role_name) > 63:
                        svc_short_name = util.short_service_name(svc_config['name'])
                        role_name = "%s-%s-%s" % (svc_short_name, role['type'], util.id(host))
                        if len(role_name) > 63:
                            role_name = role_name[:64]
                    try:
                        service.get_role(role_name)
                    except ApiException:
                        LOG.info("Creating new role: %s" % role_name)
                        service.create_role(role_name, role['type'], host_id_map[host])

            # Update roleConfigGroup for role
            if 'config' in role:
                role_config = service.get_role_config_group(
                        "%s-%s-BASE" % (svc_config['name'], role['type']))
                _conf = role['config']
                if to_interpolate:
                    _conf = _interpolate_roles(_conf, cluster, host_id_map)
                role_config.update_config(_conf)

    # Get updated service object
    service = cluster.get_service(svc_config['name'])
    LOG.debug("Service %s stale: %s, %s" % (service.name,
                                            service.configStale,
                                            service.clientConfigStalenessStatus))

    if new:
        # First run - this might fail for valid reasons for some services so add an additional start
        LOG.info("Performing first run actions for service %s" % svc_config['name'])
        cmd = service._cmd('firstRun', None, api_version=7)
        util.wait_for_command(cmd, True)
        if start_service:
            start(svc_config, cluster, True)
    elif start_service and service.configStale == 'true':
        LOG.info("Configuration stale for %s, restarting" % svc_config['name'])
        restart(svc_config, cluster, True)
    if service.clientConfigStalenessStatus == 'STALE':
        LOG.info("Client config stale for %s, redeploying" % svc_config['name'])
        cmd = service.deploy_client_config()
        util.wait_for_command(cmd, True)


def start(svc_config, cluster, exception_on_fail=False):
    service = cluster.get_service(svc_config['name'])
    cmd = service.start()
    return util.wait_for_command(cmd, exception_on_fail)


def restart(svc_config, cluster, exception_on_fail=False):
    service = cluster.get_service(svc_config['name'])
    cmd = service.restart()
    return util.wait_for_command(cmd, exception_on_fail)


def is_service_stale(name, cluster):
    svc = cluster.get_service(name)
    return svc.configStale == 'true' and svc.clientConfigStaleness == 'STALE'
