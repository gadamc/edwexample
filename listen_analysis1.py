import listener
import apptools
import json
import random
import requests
import datetime

myconfig = apptools.appConfig('edw.ini')  

def handle_signalprocessing(line):
  '''
    Like handle_newfile in the listen_newfile.py script, but does a different job. I query the view
    looking for docs that have completed the "move_to_sps" process and then perform some action 
    on those docs. In the real production code, this function would perform signal processing of the
    raw ROOT files. 
  '''

  r = requests.get(
    '%s/%s/%s?startkey=["move_to_sps",""]&endkey=["move_to_sps\ufff0",""]&reduce=false&include_docs=true' % (myconfig.server, myconfig.dbname, myconfig.viewname), 
    auth=myconfig.auth,
    headers=myconfig.headers
  )

  for row in r.json()['rows']:
    doc = row['doc']

    try:
      # Here is where the meat of the digital signal processing is done. In this space
      # we find the raw ROOT file on our batch processing system and would pass that
      # file path to programs to analyze the digitized signals. 
      # The doc['process'] list in the database contains the object metadata for each process.
      # We know that the first element in that list contains the metadata for the move of the 
      # file from the experiment site to the batch system and that doc['process'][0]['file']
      # is the path to the raw ROOT file. 
     
      # This code is wrapped in a try-except block to catch any errors or exceptions raised
      # by the physics analysis code. When we get an error or exception, we do not append a new
      # object to the doc['process'] list and we set the doc['status'] value to an appropriate
      # message to indicate the problem. 

      newfile = doc['process'][0]['file'].strip('.root') + '.amp.root'

      procresults = {
        'name':'signal_processing',  #moving the data file
        'hostname':'sps.in2p3.fr',
        'file':newfile,
        'md5sum':'%032x' % random.getrandbits(128),
        'log':'results of calculation' 
      }

      doc['process'].append(procresults)

    except:
      doc['status'] = 'signal processing failure'


    # Regardless of the outcome of the signal processing, we update the metatdata doc on 
    # the database. 

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
# data from the _changes feed. It will use the filter specified below to return only
# lines from the _changes feed for docs that return true from that filter function. 
# The filter function is defined in the _design document on the database. (See the
# design.process.json file or the couchapp.)
# When a new line is returned, the callback, above, is executed. 

try:
  listener.run( 
    changes_filter='process/with_process&last=move_to_sps',  
    callback=handle_signalprocessing, 
    anAppConfig=myconfig
  )
except KeyboardInterrupt: #don't print the traceback
  pass




