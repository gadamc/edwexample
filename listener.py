import requests 
import json
import datetime

def run(changes_filter, callback, anAppConfig):

  # set up continuous polling from _changes.
  # for simplicity, only look for any changes after this
  # script has started. To do this, first get the 
  # latest sequence value from the database

  initial_seq = getcurrentsequence( anAppConfig )
  print 'listening for new files starting from sequence:', initial_seq


  # Use requests library to GET _changes feed with these parameters
  
  params = {
    'feed':'continuous',
    'heartbeat':30000, #30 second heartbeat 
    'since':initial_seq
  }

  changes = requests.get(
    '%s/%s/_changes?filter=%s' % (anAppConfig.server, anAppConfig.dbname, changes_filter),  
    params=params,
    stream=True,
    auth=anAppConfig.auth, 
    headers=anAppConfig.headers
  )

  print 'GET', changes.url

  # User iter_lines to get new data returned by _changes
  
  for line in changes.iter_lines(chunk_size=1):
    if line:  # filter out keep-alive new lines
      print datetime.datetime.now(), 'caught line:', line
      callback(line)

    else:
      #print 'Received heartbeat'
      pass

  print 'Hey! Why did I die!'


def getcurrentsequence(anAppConfig):

  r = requests.get(
    '%s/%s' % (anAppConfig.server, anAppConfig.dbname), 
    auth=anAppConfig.auth, 
    headers=anAppConfig.headers
  )
  
  return r.json()['update_seq']
