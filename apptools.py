import ConfigParser
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
def generate_doc(counter, runname): 
  filename = '%s_%03d' % (runname, counter)
  
  doc = {
    '_id': filename,
    'run': runname,
    'conditions': {
      'temperature':0.015 + 0.001*random.random(),  #generate a random temperature near 15 mK
      'volts': {
        'detector_a':[8.0, -8.0, 2.0, -2.0],
        'detector_b':[4.0, -4.0, 1.5, -1.5]
        } 
      },
    'original_file':'/mnt/data/%s.root' % filename,
    'md5sum': '%032x' % random.getrandbits(128),
    'datecreated': time.time(),
    'status': 'good',
    'process': []
    }

  return doc

def getnextrun(anAppConfig):
  '''
    This function gets the last document put on the database (indexed by the primary key, _id)
    and determines the next edelweiss run name. 
    In this example, doc._id has a form like 'ma22a000_000', which is <runname>_<filenumber>. 
    So this function gets the largest doc._id value in the database (ex. ma22a003_005) and 
    determines that the edelweiss run is 003 and so the next run number will be 4. This function
    doesn't increment the ma22a part of the run name, however. 
  '''
  
  #note: the endkey="a" prevents finding any _design documents, which would happen if the only doc on the 
  #database is the _design doc

  r = requests.get(
    '%s/%s/_all_docs?limit=1&descending=true&endkey="a"' % (anAppConfig.server, anAppConfig.dbname), 
    auth=anAppConfig.auth,  
    headers=anAppConfig.headers
  )

  if r.status_code == 200:
    data = r.json()['rows']

    if len(data) == 1:   #since I called limit=1, just check for the proper data length.
      match = re.search('[0-9]{3,}_[0-9]{3,}', data[0]['id'])
      if match:
        return 'ma22a%03d' % (int( match.group(0).split('_')[0]) + 1)
      else:
        message = 'doc._id (%s) does not match as expected. Response: %s' % (data[0]['id'], r.text) 
        raise Exception(message)
    else:
      return 'ma22a000'

  else:
    raise Exception('bad response: status: %s, text: %s' % (r.status_code, r.text) )
