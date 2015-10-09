#!/usr/bin/env python

import logging
import util
from cm_api.api_client import ApiException
from cm_api.endpoints.services import ApiServiceSetupInfo

def add_or_update(svc_config, cluster, host_id_map, start_service=True):
  logging.debug(svc_config)
  new = False
  service = None
  try:
    service = cluster.get_service(svc_config['name'])
    logging.info("Service %s already exists on cluster %s" % (svc_config['name'], cluster.name))
  except ApiException, e:
    new = True
    logging.info("Creating service %s, type %s" % (svc_config['name'], svc_config['type']))
    service = cluster.create_service(svc_config['name'], svc_config['type'])
    logging.debug(service)

  # Service-wide config
  if 'config' in svc_config:
    service.update_config(svc_config['config'])

  # Add roles
  for role in svc_config['roles']:
    for host in role['hosts']:
      role_name = "%s-%s-%s" % (svc_config['name'], role['type'], util.id(host))
      try:
        service.get_role(role_name)
      except ApiException:
        logging.info("Creating new role: %s" % role_name)
        service.create_role(role_name, role['type'], host_id_map[host])
    # Update roleConfigGroup for role
    if 'config' in role:
      role_config = service.get_role_config_group("%s-%s-BASE" % (svc_config['name'], role['type']))
      role_config.update_config(role['config'])

  # Get updated service object
  service = cluster.get_service(svc_config['name'])
  logging.debug(service.configStale)
  logging.debug(service.clientConfigStalenessStatus)
  if new and start_service:
    # First run - this might fail for valid reasons for some services so add an additional start
    cmd = service._cmd('firstRun', None, api_version=7)
    cmd.wait(300)
    start(svc_config, cluster)
  elif start_service and service.configStale == 'true':
    restart(svc_config, cluster)
  
  if service.clientConfigStalenessStatus == 'STALE':
    logging.info("Client config stale for %s, redeploying" % svc_config['name'])
    cmd = service.deploy_client_config()
    cmd.wait(300)

def start(svc_config, cluster):
  service = cluster.get_service(svc_config['name'])
  cmd = service.start()
  cmd.wait(300)

def restart(svc_config, cluster):
  service = cluster.get_service(svc_config['name'])
  cmd = service.restart()
  cmd.wait(300)