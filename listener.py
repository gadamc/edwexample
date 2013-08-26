import ConfigParser
import requests 
import json
import apptools
import copy
import os

def run(filter, callback, configfile='edw.ini'):

  (c_server, c_dbname, c_username, c_password, c_viewname) = apptools.readconfig(configfile)

  headers = {'content-type': 'application/json'}
  auth = (c_username, c_password)


  #..  set up continuous polling from _changes.
  #      for simplicity, only look for any changes after this
  #      script has started. To do this, first get the 
  #      latest sequence value from the database
  initial_seq = apptools.getcurrentsequence()
  print 'listening for new files starting from sequence:', initial_seq


  #..  use requests library to GET _changes feed with these parameters
  #
  params = {
    'feed': 'continuous',
    'heartbeat': 60000, #60 second heartbeat (default)
    'since': initial_seq
  }

  changes = requests.get(
    '%s/%s/_changes?filter=%s' % (c_server, c_dbname, filter),
    params=params,
    stream=True,
    auth=auth, headers=headers
  )

  #..  User iter_lines to get new data returned by _changes
  #
  for line in changes.iter_lines(chunk_size=2):
    if line:  # filter out keep-alive new lines
      print 'caught line'
      print line
      callback(line)

    else:
      #print 'Received heartbeat'
      pass

  print 'Hey! Why did I die!'