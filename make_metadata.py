import requests
import json
import apptools  #some script used to simulate the Edelweiss use-case
import sys

'''
You can run this script without arguments, or with a single argument that is an integer which 
specifies the number of new documents to put on your cloudant database. 
note: this can break if you try to do something extreme. there are no checks in this code.

'''

#read credentials configuration
myconfig =  apptools.appConfig('edw.ini')

#a list of docs to upload 
docs = []

#can specify on command line how many new docs to create, otherwise just create one new doc
numdocs = 1
try:
  numdocs = int(sys.argv[1])  #check for command line number of docs
except:
  pass

#In Edelweiss, we generate a unique run name for each new run of the 
#data acquisition system. To simulate that behavior for this example, 
#this function determines the next run name every time you run this script. 
runname = apptools.getnextrun( myconfig ) 

#now generate the fake edelweiss docs for upload to the db
for i in range(numdocs):
  docs.append( apptools.generate_doc(i, runname) )

#use bulk_docs insert to upload docs to the database
url = '%s/%s/_bulk_docs' % (myconfig.server, myconfig.dbname)
print runname
print 'uploading', len(docs), 'to', url

r = requests.post(
  url, 
  data=json.dumps({'docs':docs}),
  auth=myconfig.auth, 
  headers=myconfig.headers
)

#verify that the bulk upload was successful. should return a 201. 
print 'HTTP GET return code:', r.status_code

#show the current database sequnce number so you can see the sequence number iterate through this example
print 'current database sequence number:'
r = requests.get(
  '%s/%s' % (myconfig.server, myconfig.dbname), 
  auth=myconfig.auth, 
  headers=myconfig.headers
)

print r.json()['update_seq']
