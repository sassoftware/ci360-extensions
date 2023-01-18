"""
Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os, argparse
from datetime import date
from datetime import timedelta

#Setup: Mounting Cloud Storage
print("Setup: binding docker folders with s3...")
os.system("sh mount.sh")
print("Setup: binding completed.")

print("Setup: healthcheck...")
os.system("sh log.sh")
print("Setup: healthcheck: OK")

print("Setup: cleaning *.csv from dscwh/ \nSetup: cleaning *.sql from sql/")
os.system("sh clean.sh")
print("Setup: completed.")

print("Starting the SAS UDM Loader")
#Step 0: Check Arguement and Set Default Value
yesterday = str(date.today() - timedelta(days = 1))

parser = argparse.ArgumentParser()
parser.add_argument('-m', action='store', dest='mart', type=str, 
	help='enter dataMart: detail, dbtReport or snapshot', required=True)
parser.add_argument('-st', action='store', dest='start', type=str, 
	help='enter start time: ie. 2017-11-07T10', required=False, default=yesterday+"T00")
parser.add_argument('-et', action='store', dest='end', type=str, 
	help='enter end time: ie. 2017-11-07T12', required=False, default=yesterday+"T23")
parser.add_argument('-ct', action='store', dest='category', type=str, default='discover',
	help='category to download : e.g. discover,engagedirect .. - default discover', required=False)
parser.add_argument('-svn', action='store', dest='schemaversion', type=str, default="1",
	help='enter schemaVersion: ie. 3 - default 1', required=False)
parser.add_argument('-cd', action='store', dest='delimiter', type=str, 
	help='enter a csv delimiter - default | (pipe)', required=False, default="|")
args = parser.parse_args()

print('Step 0: ')
print(args)
print('Step 0 - Done.')


#Step 1: Donwload CI360 Data
import subprocess
os.chdir('/app/ci360-download-client-python')
cmd = ['python', 'discover.py','-m', args.mart, '-st', args.start, '-et', args.end, '-cf', 'yes', '-ct', args.category, '-svn', str(args.schemaversion), '-ch', 'yes', '-cl', 'yes', '-cd', args.delimiter, '-a', 'no']
subprocess.call(cmd)

print('Step 1: ')
print(cmd)
print('Step 1 - Done.')


#Step 2: Display dscwh folder 
import listing_files
_pipe = listing_files.main()

print('Step 2: ')
print(_pipe)
print('Step 2 - Done.')

#Step 3: Notify snowflake to dwonload data via API
import snowflake_python_jwt, uuid, json, requests

_requestId = str(uuid.uuid4())

_accountname = os.getenv('SF_ACCOUNT')
_database = os.getenv('SF_DATABASE')
_schema = os.getenv('SF_SCHEMA')
_warehouse = os.getenv('SF_WAREHOUSE')
_role = os.getenv('SF_ROLE')
_stage = os.getenv('SF_STAGE')
_format = os.getenv('SF_FORMAT')
_prefix = os.getenv('SF_PREFIX')
_mart = args.mart
_header = {
	'Content-Type': 'application/json',
	'Accept': 'application/json',
	'Authorization': 'Bearer '+ str(snowflake_python_jwt.main())
}

print(_mart)

for p in _pipe:
	if p != 'dscwh':
		if _mart == 'snapshot':
			pipe = p
			url = "https://"+_accountname+".snowflakecomputing.com/api/v2/statements"
			payload = json.dumps(
				{
				"statement": "truncate table "+_prefix+pipe+"; COPY INTO "+_database+"."+_schema+"."+_prefix+pipe+" FROM @"+_database+"."+_schema+"."+_stage+" FILE_FORMAT = ( FORMAT_NAME = "+_database+"."+_schema+"."+_format+") files=('"+pipe+".csv')",
				"timeout": 60,
				"parameters": {"MULTI_STATEMENT_COUNT": "2"},
				"database": _database,
				"schema": _schema,
				"warehouse": _warehouse,
				"role": _role
				})
			_header.update({
				'User-Agent': 'myApplicationName/1.0',
				'X-Snowflake-Authorization-Token-Type': 'KEYPAIR_JWT'
			})
		else:
			pipe = p.split('_',1)[1]
			url = "https://"+_accountname+".snowflakecomputing.com/v1/data/pipes/"+_database+"."+_schema+"."+_prefix+pipe+"/insertFiles?requestId="+_requestId
			payload = json.dumps({
				"files":[{"path":p+'.csv'}]})
		headers = _header
		print(url)
		print(payload)

		response = requests.request("POST", url, headers=headers, data=payload)
		print(response.text)

print('Step 3 - Done.')
print("the SAS UDM Loader completed.")

