#Example Process Management tool using CouchDB/Cloudant

0. pip install requests
1. edit edw.ini file
2. python upload_design.py
3. Open three terminal windows (a, b, c). In each terminal run the following commands
	a. python make_metadata.py 10  # make 10 new documents for the database
	b. python listen_newfiles.py   # turn on the listeners
	c. python listen_analysis1.py
4. repeatedly call: python make_metadata.py 1  # or some number

You should see something like this


