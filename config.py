#!/usr/bin/env python

import json
import logging
import sys

def read_json_config(configfile):
  logging.debug("Reading JSON config from %s" % configfile)
  with open(configfile, 'r') as f:
    config = json.load(f)
    logging.debug("JSON Config: %s" % json.dumps(config, indent=2))
    return config

def read_config(configfile, fmt="json"):
  if fmt == "json":
    return read_json_config(configfile)
  else:
    logging.error("Config files in %s format not supported" % fmt)

if __name__ == "__main__":
  logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
  if len(sys.argv) > 1:
    read_config(sys.argv[1])
  else:
    logging.error("No config file supplied")

