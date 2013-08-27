import listener
import apptools
import os
import json
import requests
import datetime

myconfig = apptools.appConfig('edw.ini')  #reads the .ini file and holds the info


# "Data Processing" new file routine... well, the simulation of the routine. 
#
def handle_newfiles(line):
  #In EDELWEISS, I didn't use the line returned by the _changes feed (and passed to this function), 
  #but rather, I used a Mapreduce View to tell me what data needs processing. 
  #I use the view specified in the .ini file to search for new files to process.
  #This view returns keys = ['name', doc._id], where 'name' is the value doc.process[x].name 
  #for the last element of the doc.process list. If doc.process is an empty list, then this mapreduce
  #view returns keys = [0, doc._id], which indicates a new file.  
  #So, in this function, I sort the view results to grab only these keys of the form [0, doc._id] 
  #(see the startkey, endkey options specificed in the requests.get line below).
  #
  #Using the Mapreduce View to find documents that need action has some advantages over using
  #the line returned directly by the _change feed.  
  #The _change feed simply acts as a trigger for me to start looking for jobs to do and the 
  #Mapreduce views tell me exactly which documents
  #need attention. This means that if the script that calls this function is not running when new documents are
  #added to the database, those documents will still be handled because they'll still
  #show up in the mapreduce view. Additionally, this pattern allows us to start the processing steps for 
  #a particular document over by simply deleting the values in the doc.process list.  
  #When the values are removed from doc.process, these documents show up next time the view is queried. 
  #Of course, the processing of a physics ROOT file in EDELWEISS is completely serial, so if a particular step needs to 
  #be redone, we have re-run all subsequent processes after that action (which is acceptable 
  #for us, but maybe not for you.)
  

  #So, I call the mapreduce view with the startkey/endkey combination that gets me a list of all new files
  #in the database. 

  r = requests.get('%s/%s/%s?startkey=[0,""]&endkey=[1,""]&reduce=false&include_docs=true' % (myconfig.server, myconfig.dbname, myconfig.viewname), headers=myconfig.headers, auth=myconfig.auth)
  viewreturn = r.json()['rows']
  
  #with the list of documents that correspond to new files that need attention,
  #I perform the action (in this case, I just simulate the results and add them
  #to doc.process). If something bad happens, then I can set doc.status to something
  #!= "good", which lets me find these documents later (via another map-reduce view that indexes
  # the status)

  for row in viewreturn:
    doc = row['doc']

    try:
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

    except:
      doc['status'] = 'failed move to sps'


    print datetime.datetime.now(), 'appending process results to doc', row['id']      
    print json.dumps(procresults, indent=1)

    rr = requests.put('%s/%s/%s' % (myconfig.server, myconfig.dbname, row['id']), data = json.dumps(doc), auth=myconfig.auth, headers=myconfig.headers)
    if rr.status_code not in (200, 201):
      print 'bad status', rr.status_code, rr, row['id']
    else:
      print datetime.datetime.now(), 'updated', row['id']

#.........................

#call the listener.run function and use the function above for the callback

listener.run( filter = 'process/with_process&last=0', 
              callback = handle_newfiles , 
              anAppConfig = myconfig)



