#!/usr/bin/env python

import codecs
import logging
import md5
from cm_api.api_client import ApiResource, ApiException
from cm_api.endpoints.types import ApiConfig

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

#def create_config(conf):
#  api_config = {}
#  for c in conf:
#    api_config[c['name']] = ApiConfig(c['name'], c['value'])
#  return api_config

def id(*args):
  m = md5.new()
  for a in args:
    m.update(codecs.encode(a))
  return m.hexdigest()