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

import codecs
import hashlib
import logging

from cm_api.api_client import ApiResource

from errors import ProvisionatorException

LOG = logging.getLogger(__name__)


def get_api_handle(conf):
    host = conf['cm']['host']
    user = conf['cm']['user']
    password = conf['cm']['password']
    use_tls = conf['cm']['ssl']
    api = ApiResource(host, username=user, password=password, use_tls=use_tls)
    return api


def host_id_map(conf, api):
    hosts = api.get_all_hosts()
    host_id_map = {}
    for host in hosts:
        host_id_map[host.hostname] = host.hostId
    return host_id_map


def id(*args):
    m = hashlib.md5()
    for a in args:
        m.update(codecs.encode(a))
    return m.hexdigest()


def short_service_name(svcname):
    m = hashlib.md5()
    m.update(codecs.encode(svcname))
    return "%s%s" % (svcname[:4], m.hexdigest())


def wait_for_command(cmd, exception_on_fail=False, timeout=300):
    status = cmd.wait(timeout)
    if status.active == 'true':
        if exception_on_fail:
            raise ProvisionatorException(
                "Command %s (%s) timed out after %d seconds" % (status.name, status.id, timeout))
        else:
            LOG.warn(
                "Command %s (%s) timed out after %d seconds" % (status.name, status.id, timeout))
    if status.active == 'false' and status.success == 'false':
        if exception_on_fail:
            raise ProvisionatorException(
                "Command %s (%s) failed with: %s" % (status.name, status.id, status.resultMessage))
        else:
            LOG.warn(
                "Command %s (%s) failed with: %s" % (status.name, status.id, status.resultMessage))

    return status.active == 'false' and status.success == 'true'
