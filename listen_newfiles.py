import listener
import apptools
import os
import json
import requests
import datetime

myconfig = apptools.appConfig('edw.ini')  

def handle_newfiles(line):
  '''
    This callback handles new files that are added to the database. It finds all new files via a call
    to the Mapreduce function (specified in the .ini configuration file) with appropriate options. The 
    'line' passed to this function from the _changes feed is not used here. New files, in EDELWEISS, are
    moved from the data acquisition machine to our batch processing farm for analysis. This function 
    simulates that process. 

    The Mapreduce View returns keys = ['name', doc._id], where 'name' is the value doc['process'][x]['name'] 
    for the last element of the doc['process'] list. If doc['process'] is an empty list, which is true
    for a new file, then this Mapreduce View returns keys = [0, doc._id].  
    So, in this function, I sort the View results to grab only these keys of the form [0, doc._id] 
    (see the startkey, endkey options specificed in the requests.get line below).
    
    Using the Mapreduce View to find documents that need action has some advantages over using the line 
    returned directly by the _change feed.  The _change feed simply acts as a trigger to start looking 
    for jobs to do and the Mapreduce Views tell me exactly which documents need attention. This means 
    that if the script that calls this function is not running when new documents are added to the database, 
    those documents will still be handled because they'll show up in the Mapreduce View. The Mapreduce
    View also only emits documents that have a doc['status'] == 'good', which prevents problematic data
    from propagating down the processing chain. Additionally, this pattern allows us to start the processing 
    steps for a particular document over by simply deleting the values in the doc['process'] list. When the 
    values are removed from doc['process'], these documents show up next time the View is queried.  
    The processing of a physics ROOT file in EDELWEISS is completely serial, so if a particular step needs 
    to be redone, we have re-run all subsequent processes after that action (which is acceptable for our 
    particular use).

  '''    

  #Call the Mapreduce View with the startkey/endkey combination that gets me a list of all new files
  #in the database.

  r = requests.get(
    '%s/%s/%s?startkey=[0,""]&endkey=[1,""]&reduce=false&include_docs=true' % (myconfig.server, myconfig.dbname, myconfig.viewname), 
    auth=myconfig.auth,
    headers=myconfig.headers
  )

  
  # With the list of documents that correspond to new files that need attention,
  # perform the action (in this case, I just simulate the results and add them
  # to doc['process']). If something bad happens, then I can set doc.status to something
  # != "good", which lets me find these documents later (via another Mapreduce View that indexes
  # the status)

  for row in r.json()['rows']:
    doc = row['doc']

    try:
      # Here we would put the code that grabs the doc['original_file'] and uses sftp to 
      # move the file to our batch processing system in Lyon. 
      # We raise exceptions as appropriate and catch those exceptions so that we
      # update the document in the database with an error message (in doc['status']).

      procresults = {
        'name':'move_to_sps',  #moving the data file
        'hostname':'sps.in2p3.fr',
        'file':'/edelweiss/data/%s' % os.path.basename(doc['original_file']),
        'md5sum':doc['md5sum'], #obviously, we wouldn't do this. We check for the correct file size. 
        'log':'results of sftp job' 
      }      
         
      doc['process'].append(procresults)

    except:
      doc['status'] = 'failed move to sps'


    # We update the document in the database

    print datetime.datetime.now(), 'appending process results to doc', row['id']      
    print json.dumps(procresults, indent=1)

    rr = requests.put(
      '%s/%s/%s' % (myconfig.server, myconfig.dbname, row['id']), 
      data=json.dumps(doc), 
      auth=myconfig.auth, 
      headers=myconfig.headers
    )

    if rr.status_code not in (200, 201):
      print 'bad status', rr.status_code, rr, row['id']
    else:
      print datetime.datetime.now(), 'updated', row['id']

#.........................

# Call the listener.run function, which will run in a loop indefinitely and listen for 
# data from the _changes feed. It will use the filter specified to return only
# lines from the _changes feed for docs that return true from the filter function. 
# The filter function is defined in the _design document on the database. (See the
# design.process.json file or the couchapp.)
# When a new line is returned, the callback, above, is executed. 

try:
  listener.run( 
    changes_filter='process/with_process&last=0', 
    callback=handle_newfiles , 
    anAppConfig=myconfig
  )
except KeyboardInterrupt: #don't print the traceback
  pass


