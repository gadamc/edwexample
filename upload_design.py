import requests
import json
import apptools

'''
Run this script to upload the _design/process document.

'''

#read credentials configuration
(c_server, c_dbname, c_username, c_password, c_viewname) = apptools.readconfig('edw.ini')

f = open('design.process.json')
doc = json.load(f)

url = '%s/%s/%s' % (c_server, c_dbname, doc['_id'])

r = requests.put(url, auth=apptools.auth, data=json.dumps(doc), headers= apptools.headers)

print 'HTTP GET return code:', r.status_code
