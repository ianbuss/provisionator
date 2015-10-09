#!/usr/bin/env python

import sys
import logging
import cm_api
import config, mgmt, util, cluster, service
from cm_api.api_client import ApiException

if __name__ == "__main__":
  logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
  if len(sys.argv) < 2:
    logging.fatal("No configuration file specified")
    sys.exit(-1)

  config = config.read_config(sys.argv[1])

  try:
    # Open a connection to CM and get a CM object
    api = util.get_api_handle(config)
    host_id_map = util.host_id_map(config, api)
    cm = api.get_cloudera_manager()

    # Update CM config
    if 'config' in config['cm']:
      cm.update_config(config['cm']['config'])

    # Create Cloudera Management Services
    mgmt.create(config, cm, host_id_map)
    mgmt.start(cm)

    # Add cluster
    cl = cluster.add(config, api, host_id_map)

    # Add services
    for svc in config['cluster']['services']:
      service.add_or_update(svc, cl, host_id_map)
      # service.start(svc, cl)

  except ApiException, e:
    logging.error("Error: %s" % e)
