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
import time

from cm_api.api_client import ApiException

import cluster
import config
import mgmt
import util
from errors import ProvisionatorException

LOG = logging.getLogger(__name__)
logging.basicConfig()
LOG.setLevel(logging.INFO)
DATANODE_TRANSCEIVER_PORT = 1004
DATANODE_WEB_PORT = 1006


def check_creds(user, passwd):
    # Sanity check KDC credentials
    if not '@' in user:
        raise ProvisionatorException("Supplied KDC account username contains '@")
    elif len(passwd) == 0:
        raise ProvisionatorException("Supplied KDC account password is empty")


def enable_kerberos(conf):
    try:
        # Open a connection to CM and get a CM object
        api = util.get_api_handle(conf)
        cm = api.get_cloudera_manager()
        cl = None
        if 'cluster' in conf and 'name' in conf['cluster']:
            cl = api.get_cluster(conf['cluster']['name'])
        else:
            raise ProvisionatorException("No cluster specified")

        # Check the HDFS service to see if Kerberos is already enabled
        hdfs_svc = config.get_cluster_service_by_type(conf, 'HDFS')
        hdfs = cl.get_service(hdfs_svc['name'])
        hdfs_cfg, hdfs_roletype_cfg = hdfs.get_config(view='full')

        if not hdfs_cfg['hadoop_security_authentication'].value == 'kerberos':
            # Kerberos has not been enabled - add the KDC creds
            if 'kdc_user' in conf['cm'] and 'kdc_pass' in conf['cm']:
                check_creds(conf['cm']['kdc_user'], conf['cm']['kdc_pass'])
                LOG.info("Importing credentials for administrative account")
                cmd = cm.import_admin_credentials(conf['cm']['kdc_user'], conf['cm']['kdc_pass'])
                util.wait_for_command(cmd, True)

            # OK let's do this
            datanode_transceiver_port = DATANODE_TRANSCEIVER_PORT
            datanode_web_port = DATANODE_WEB_PORT

            cm_cfg = cm.get_config()
            if 'SINGLE_USER_ENABLED' in cm_cfg:
                # TODO: don't hardcode this
                datanode_transceiver_port=4004
                datanode_web_port=4006

            LOG.info("Running Kerberos configuration now...")
            cmd = cl.configure_for_kerberos(datanode_transceiver_port=datanode_transceiver_port,
                                            datanode_web_port=datanode_web_port)

            util.wait_for_command(cmd, True)
            #TODO: in some cases time.sleep(x) is needed to avoid race conditions
            try:
                mgmt_svc = cm.get_service()
                LOG.info("Restarting management services")
                mgmt.restart(cm)
            except ApiException:
                pass
            LOG.info("Restarting cluster")
            cluster.restart(cl)
        else:
            LOG.info("Kerberos already enabled")
    except ApiException, e:
        raise ProvisionatorException(e)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.fatal("No configuration file specified")
        sys.exit(-1)

    conf = config.read_config(sys.argv[1])

    enable_kerberos(conf)
