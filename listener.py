import requests 
import json
import datetime

def run(filter, callback, anAppConfig):


  #..  set up continuous polling from _changes.
  #      for simplicity, only look for any changes after this
  #      script has started. To do this, first get the 
  #      latest sequence value from the database
  initial_seq = getcurrentsequence( anAppConfig )
  print 'listening for new files starting from sequence:', initial_seq


  #..  use requests library to GET _changes feed with these parameters
  #
  params = {
    'feed': 'continuous',
    'heartbeat': 60000, #60 second heartbeat (default)
    'since': initial_seq
  }

  changes = requests.get(
    '%s/%s/_changes?filter=%s' % (anAppConfig.server, anAppConfig.dbname, filter),  #filter is specified here
    params = params,
    stream = True,
    auth = anAppConfig.auth, 
    headers = anAppConfig.headers
  )

  #..  User iter_lines to get new data returned by _changes
  #
  for line in changes.iter_lines(chunk_size=1):
    if line:  # filter out keep-alive new lines
      print datetime.datetime.now(), 'caught line:', line
      callback(line)

    else:
      #print 'Received heartbeat'
      pass

  print 'Hey! Why did I die!'


def getcurrentsequence(anAppConfig):
  
  url = '%s/%s' % (anAppConfig.server, anAppConfig.dbname)

  r = requests.get(url, auth=anAppConfig.auth, headers=anAppConfig.headers)
  
  return r.json()['update_seq']