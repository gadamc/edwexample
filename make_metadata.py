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
(c_server, c_dbname, c_username, c_password, c_viewname) = apptools.readconfig('edw.ini')


#create an array of docs to upload 
docs = []

#can specify on command line how many new docs to create
numdocs = 30
try:
  numdocs = int(sys.argv[1])  #check for command line number of docs
except:
  pass

run = 'ma22a%03d' % apptools.getnextrun() #this is just to change the name of the 'run' every time you call this script, which is more realistic

for i in range(numdocs):
  docs.append( apptools.generate_doc(i, run) )


#use bulk_docs insert to upload docs to the database
url = '%s/%s/_bulk_docs' % (c_server, c_dbname)
print run
print 'uploading', len(docs), 'to', url

headers = {'content-type': 'application/json'}
auth=(c_username, c_password)

r = requests.post(url, auth=auth, data=json.dumps({'docs':docs}), headers=headers)

print 'HTTP GET return code:', r.status_code

#show the current database sequnce number
print 'current database sequence number:'
r = requests.get('%s/%s' % (c_server, c_dbname), auth=auth, headers=headers)
print r.json()['update_seq']
