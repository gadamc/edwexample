import requests
import json
import apptools

'''
Run this script to upload the _design/process document.

'''

#read credentials configuration
myconfig =  apptools.appConfig('edw.ini')

f = open('design.process.json')

doc = json.load(f)
url = '%s/%s/%s' % (myconfig.server, myconfig.dbname, doc['_id'])

#if this _design doc already exists on the database, then replace it with the data in the file. must get _rev
r = requests.head(url, auth = myconfig.auth, headers = myconfig.headers)
if r.status_code == 200:
  doc['_rev'] = r.headers['etag'].strip('"')

r = requests.put(url, auth = myconfig.auth, data = json.dumps(doc), headers = myconfig.headers)

print 'HTTP GET return code:', r.status_code
