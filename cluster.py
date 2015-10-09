#!/usr/bin/env python

import logging
import time

def wait_for_parcel_stage(cluster, parcel, stage):
  parcel = cluster.get_parcel(parcel.product, parcel.version)
  while not parcel.stage == stage:
    logging.debug(parcel)
    time.sleep(2)
    parcel = cluster.get_parcel(parcel.product, parcel.version)

def check_clusters(api, name, version, host_ids):
  existing_clusters = api.get_all_clusters()
  for c in existing_clusters:
    logging.debug("Existing cluster: %s" % c)
    # Matching cluster already exists?
    if c.name == name:
      if c.fullVersion == version:
        # Should also check host membership...
        logging.info("Cluster %s, version %s already exists" % (name, version))
      else:
        logging.warn("Cluster %s already exists but has a different version: %s" % (name, version))
      return c
  return None

def add(conf, api, hosts):
  name = conf['cluster']['name']
  version = conf['cluster']['version']
  hostnames = conf['cluster']['hosts']

  host_ids = []
  for hostname in hostnames:
    host_ids.append(hosts[hostname])

  cluster = check_clusters(api, name, version, host_ids)
  if not cluster:
    logging.info("Creating cluster: name => %s, version => %s, hosts => %s" % (name, version, hostnames))
    cluster = api.create_cluster(name, fullVersion=version)
    logging.debug("Adding host IDs: %s" % host_ids)
    cluster.add_hosts(host_ids)
  
    # Get parcels
    parcels = [p for p in cluster.get_all_parcels() if p.version.startswith(version)]
    matching_parcels = sorted(parcels, key=lambda p: p.version, reverse=True)
    if len(matching_parcels) < 1:
      logging.error("No matching parcels found for %s" % version)
      return None
  
    # Download, distribute and activate parcel
    parcel = matching_parcels[0]
    logging.info("Downloading %s, %s" % (parcel.product, parcel.version))
    parcel.start_download()
    wait_for_parcel_stage(cluster, parcel, 'DOWNLOADED')
    logging.info("Distributing %s" % parcel.version)
    parcel.start_distribution()
    wait_for_parcel_stage(cluster, parcel, 'DISTRIBUTED')
    logging.info("Activating %s" % parcel.version)
    parcel.activate()
    wait_for_parcel_stage(cluster, parcel, 'ACTIVATED')

  return cluster
