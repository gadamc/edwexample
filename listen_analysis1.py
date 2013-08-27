import listener
import apptools
import json
import random
import requests
import datetime

myconfig = apptools.appConfig('edw.ini')  #reads the .ini file and holds the info


def handle_signalprocessing(line):
  #Like handle_newfile in the listen_newfile.py script, but does a different job. I query the view
  #looking for docs that have completed the "move_to_sps" process and then perform some action 
  #on those docs.
  
  r = requests.get('%s/%s/%s?startkey=["move_to_sps",""]&endkey=["move_to_sps\ufff0",""]&reduce=false&include_docs=true' % (myconfig.server, myconfig.dbname, myconfig.viewname), headers=myconfig.headers, auth=myconfig.auth)
  viewreturn = r.json()['rows']


  for row in viewreturn:
    doc = row['doc']

    try:
      # code here would grab the doc['process'][0]['file'] and use physics codes to 
      # analyze the raw data and save the results of that analysis to an output file. 
      # (I'm assuming process[0] tells me where the file is that I want - you can probably come up with 
      # something better.)
      newfile = doc['process'][0]['file'].strip('.root') + '.amp.root'

      procresults = {'name':'signal_processing',  #moving the data file
          'hostname':'sps.in2p3.fr',
          'file': newfile,
          'md5sum': int(123*random.random()),
          'log': 'results of calculation' 
          }

      doc['process'].append(procresults)

    except:
      doc['status'] = 'signal processing failure'

    print datetime.datetime.now(), 'appending process results to doc', row['id']      
    print json.dumps(procresults, indent=1)

    rr = requests.put('%s/%s/%s' % (myconfig.server, myconfig.dbname, row['id']), data = json.dumps(doc), auth=myconfig.auth, headers=myconfig.headers)
    if rr.status_code not in (200, 201):
      print 'bad status', rr.status_code, rr, row['id']
    else:
      print datetime.datetime.now(), 'updated', row['id']



#.........................

#call the listener.run function and use the function above for the callback

listener.run( filter = 'process/with_process&last=move_to_sps',  
              callback = handle_signalprocessing, 
              anAppConfig = myconfig)




