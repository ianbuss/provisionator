#!/usr/bin/env python

import logging
import util
from cm_api.endpoints.services import ApiServiceSetupInfo
from cm_api.api_client import ApiException

def create(conf, cm, hosts, remove=False):
  default_host = hosts[conf['cm']['host']]

  # Check for an existing MGMT service
  try:
    cm.get_service()
    logging.info("Cloudera Management Service already exists, skipping creation")
    return
  except ApiException, e:
    pass

  mgmt_cfg = {}
  if conf['mgmt']['config']:
    mgmt_cfg = conf['mgmt']['config']

  mgmt_setup_info = ApiServiceSetupInfo(name="mgmt", type="MGMT", config=mgmt_cfg)
  mgmt_svc = cm.create_mgmt_service(mgmt_setup_info)

  for svc in conf['mgmt']['services']:
    # Use CM host as default for MGMT hosts
    svc_host = hosts[conf['cm']['host']]
    if 'host' in svc:
      svc_host = hosts[svc['host']]
    mgmt_svc.create_role("mgmt-" + svc['name'], svc['name'], svc_host)
    svc_grp = mgmt_svc.get_role_config_group("mgmt-%s-BASE" % svc['name'])
    svc_grp.update_config(svc['config'])

def start(cm, wait=True):
  logging.debug("Starting management services with wait=%s" % str(wait))
  cmd = cm.get_service().start()
  if wait:
    cmd.wait(timeout=300)

