import ConfigParser
import random
import re
import time
import random
import json
import requests
import copy
import os


c_server = None
c_dbname = None
c_username = None
c_password = None
c_viewname = None
auth = None
headers = {'content-type': 'application/json'}


def readconfig(configfile = 'edw.ini'):
  global c_server, c_dbname, c_username, c_password, c_viewname, auth

  #read credentials configuration
  config = ConfigParser.RawConfigParser()
  config.read(configfile)

  c_server = config.get('Cloudant', 'server')
  c_dbname = config.get('Cloudant', 'dbname')
  c_username = config.get('Cloudant', 'username')
  c_password = config.get('Cloudant', 'password')
  c_viewname = config.get('Cloudant', 'viewname')
  auth=(c_username, c_password)

  return (c_server, c_dbname, c_username, c_password, c_viewname)



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


def getnextrun():
  url = '%s/%s/_all_docs?limit=1&descending=true' % (c_server, c_dbname)
  
  r = requests.get(url, auth=auth,  headers=headers)

  if r.status_code == 200:
    
    try:
      match = re.search('[0-9]{3,}_[0-9]{3,}', r.json()['rows'][0]['id'] )
      if match:
        return int( match.group(0).split('_')[0]) + 1
    except IndexError: #assume this is because 'rows' is an empty array
      return 0

  else:
    raise Exception('bad request: status: %s, text: %s' % (r.status_code, r.text) )



def getcurrentsequence():

	r = requests.get('%s/%s' % (c_server, c_dbname), auth=auth, headers=headers)
	return r.json()['update_seq']



# "Data Processing" routines... well, the simulation of those routines. 
#
#  these functions are called by the listener.py / listen_<routine>.py scripts.
#
def handle_newfiles(line):
  #In EDELWEISS, I didn't use the line returned by requests.iter_lines (and passed to this function), 
  #but rather, I used a Mapreduce View to tell me what data need processing. 
  #I use the view specified in the .ini file to search for new files to process.
  #This view returns keys = ['name', doc._id], where 'name' is the value doc.process[x].name 
  #for the last element of the doc.process list. If doc.process is an empty list, then this mapreduce
  #view returns keys = [0, doc._id], which indicate that these are new files. 
  #So, in this function, I sort the view results to grab only these keys (see the startkey, endkey
  # options specificed in the requests.get line below).
  #
  #Using the Mapreduce View to find documents that need action has some advantages over using
  #the line returned directly by the _change feed.  
  #The _change feed simply acts as a trigger for me to start looking for jobs to do and the 
  #Mapreduce views tell me exactly which documents
  #need attention. This means that if the script that calls this function is not running when new documents are
  #added to the database, those documents will still be handled because they'll still
  #show up in the mapreduce view. Additionally, this pattern allows us to start over the processing steps for 
  #a particular document by simply deleting the values in that document.process list.  
  #After the values are removed from
  #doc.process, these documents show up next time the appropriate view is queried. 
  #Of course, processing of a file in EDELWEISS is completely serial, so if a particular action needs to 
  #be done a second time, we have re-run all subsequent processes after that action (which is acceptable 
  #for us, but maybe not for you.)
  
  r = requests.get('%s/%s/%s?startkey=[0,""]&endkey=[1,""]&reduce=false&include_docs=true' % (c_server, c_dbname, c_viewname), headers=headers, auth=auth)
  viewreturn = r.json()['rows']
  
  #with the list of documents that correspond to new files that need attention
  #I perform the action (in this case, I just simulate the results and add them
  #to doc.process). In reality, if something bad happens, then I can set doc.status="bad"
  #or something more instructive, and then write a map function to show all doc.status != "good"
  #so that I can fix the problems. 

  for row in viewreturn:
    doc = row['doc']

    # code here grabs the doc['original_file'] and uses sftp to 
    # move the file to our batch processing system in Lyon. 
    # 

    procresults = {'name':'move_to_sps',  #moving the data file
        'hostname':'sps.in2p3.fr',
        'file':'/edelweiss/data/%s' % os.path.basename(doc['original_file']),
        'md5sum': 12345,
        'log': 'results of sftp job' 
        }      

    doc['process'].append(procresults)

    print 'appending process results to doc', row['id']      
    print json.dumps(procresults, indent=1)

    rr = requests.put('%s/%s/%s' % (c_server, c_dbname, row['id']), data = json.dumps(doc), auth=auth, headers=headers)
    if rr.status_code not in (200, 201):
      print 'bad status', rr.status_code, rr, row['id']


def handle_signalprocessing(line):
  #Like handle_newfile, but does a different job.
  
  r = requests.get('%s/%s/%s?startkey=["move_to_sps",""]&endkey=["move_to_sps\ufff0",""]&reduce=false&include_docs=true' % (c_server, c_dbname, c_viewname), headers=headers, auth=auth)
  viewreturn = r.json()['rows']


  for row in viewreturn:
    doc = row['doc']

    # code here would grab the doc['process'][0]['file'] and use physics codes to 
    # analyze the raw data and save the results of that analysis to an output file. 
    # (I'm assuming process[0] tells me where the file is that I want - you can probably come up with 
    # something better.)
    newfile = doc['process'][0]['file'].strip('.root') + '.amp.root'

    procresults = {'name':'signal_processing',  #moving the data file
        'hostname':'sps.in2p3.fr',
        'file': newfile,
        'md5sum': 123,
        'log': 'results of calculation' 
        }

    doc['process'].append(procresults)

    print 'appending process results to doc', row['id']      
    print json.dumps(procresults, indent=1)

    rr = requests.put('%s/%s/%s' % (c_server, c_dbname, row['id']), data = json.dumps(doc), auth=auth, headers=headers)
    if rr.status_code not in (200, 201):
      print 'bad status', rr.status_code, rr, row['id']



