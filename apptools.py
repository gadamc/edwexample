import ConfigParser
import random
import re
import time
import random
import json
import requests
import copy
import os


#a little class to hold the configuration and make it easier to pass around the info
class appConfig:

  def __init__ (self, configfile):
    self.readconfig(configfile)

  def readconfig(self, configfile):
    #read credentials configuration
    config = ConfigParser.RawConfigParser()
    config.read(configfile)

    self.server = config.get('Cloudant', 'server')
    self.dbname = config.get('Cloudant', 'dbname')
    self.username = config.get('Cloudant', 'username')
    self.password = config.get('Cloudant', 'password')
    self.viewname = config.get('Cloudant', 'viewname')

    self.headers = {'content-type': 'application/json'}
    self.auth = (self.username, self.password)


# some helper functions to generate Edelweiss-like metadata docs
def generate_doc(counter, run): 
  runname = '%s_%03d' % (run,counter)
  
  doc = {
    '_id': runname,
    'run': run,
    'conditions': {
      'temperature':0.015 + 0.001*random.random(),  #generate a random temperature near 15 mK
      'volts': {
        'detector_a':[8.0, -8.0, 2.0, -2.0],
        'detector_b':[4.0, -4.0, 1.5, -1.5]
      } 
    },
    'original_file':'/mnt/data/%s.root' % runname,
    'md5sum': 12345,
    'datecreated': time.time(),
    'status': 'good',
    'process': []
  }

  return doc


def getnextrun(anAppConfig):

  url = '%s/%s/_all_docs?limit=1&descending=true' % (anAppConfig.server, anAppConfig.dbname)
  
  r = requests.get(url, auth=anAppConfig.auth,  headers=anAppConfig.headers)

  if r.status_code == 200:
    
    try:
      match = re.search('[0-9]{3,}_[0-9]{3,}', r.json()['rows'][0]['id'])
      if match:
        return int( match.group(0).split('_')[0]) + 1
    except IndexError: #assume this is because 'rows' is an empty array
      return 0

  else:
    raise Exception('bad request: status: %s, text: %s' % (r.status_code, r.text) )











