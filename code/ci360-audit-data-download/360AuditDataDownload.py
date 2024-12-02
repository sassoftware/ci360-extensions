#*************************************************************************************************************#
# Program Name: 360AuditDataDownload.py                                                                       #
# Program Description: This program helps download Audit data from 360.                                       #
# Authors: SAS Global CI Practice                                                                             #
# Date: 11-October-2024                                                                                       #
# Version: 1.0.2                                                                                              #
# Copyright  2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.                                   #
# SPDX-License-Identifier: Apache-2.0                                                                         #
# ============================================================================================================#

# Import Python Libraries
import json
import jwt
import base64
import os
import time
import csv
import configparser
import pandas as pd
import fastparquet
import urllib.request
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError
import logging # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL

def Download360AuditData():
  try:  
    # Initialize config and credential files
    currdate = time.strftime("%Y%m%d")   
    currdatetime = time.strftime("%m%d%Y%H%M%S") 
    path_current_directory = os.path.dirname(__file__)
    path_config_file = os.path.join(path_current_directory, 'conf', 'config.ini') # Locate App Config File
    path_creds_file = os.path.join(path_current_directory, 'conf', 'credentials.ini') # Locate App Credentials File
   
    # Read variables from the Credentials file
    credsfile = configparser.ConfigParser()
    credsfile.read(path_creds_file) 
    apiuser = credsfile.get('CREDENTIALS','apiuser')
    apisecret = credsfile.get('CREDENTIALS','apisecret')   
    staticjwtoken =  credsfile.get('CREDENTIALS','staticjwt')   

    # Read variables from the Config file
    configfile = configparser.ConfigParser()
    configfile.read(path_config_file)
    gatewayhost = configfile.get('TENANT','gatewayhost')
    dataRangeStartTimeStamp = configfile.get('API','dataRangeStartTimeStamp')
    dataRangeEndTimeStamp = configfile.get('API','dataRangeEndTimeStamp')
    downloadparquet = configfile.get('PROGRAM','downloadparquet')
    outputformat = configfile.get('PROGRAM','outputformat')
    loglevel=configfile.get('PROGRAM','loglevel')    
    loglevel = str.upper(loglevel)
    
    # Initialize the log file
    logfile = os.path.join(path_current_directory, 'logs\\')
    logfile = logfile + 'Log_' + currdate + '.log'
    logging.basicConfig(filename=logfile, filemode='a+', level=logging.getLevelName(loglevel), encoding='utf-8', force=True, format='%(asctime)s - %(process)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    #logging.getLogger().setLevel(loglevel)

    logging.info('----------------------------------------------------------------------------------')
    logging.info('Begin code execution')   

    logging.info('Configuration file procesisng complete')  

    # Build the API URLs
    tokenapiurl = 'https://' + gatewayhost + '/token?grant_type=password&username=' + apiuser + '&password=' + apisecret
    if len(dataRangeStartTimeStamp) != 0 and len(dataRangeEndTimeStamp) != 0:
      auditapiurl = 'https://' + gatewayhost + '/marketingAdmin/auditRecords?dataRangeStartTimeStamp=' + dataRangeStartTimeStamp + '&dataRangeEndTimeStamp=' + dataRangeEndTimeStamp
    else:
      auditapiurl = 'https://' + gatewayhost + '/marketingAdmin/auditRecords'

    logging.debug('Token API URL - %s' % tokenapiurl) 
    logging.debug('Audit API URL - %s' % auditapiurl) 

    # Generate a temporary JWT for authorizing the 360 Audit Data API      
    # Define a Session object for the HTTP call.  
    sessionadapter = HTTPAdapter(max_retries=3)
    session = requests.Session()
    session.mount(tokenapiurl, sessionadapter)
    # Make the HTTP call with a timeout of 30 seconds. If call fails, retry for a max three times
    apiheaders =  {
       'Content-Type': 'application/x-www-form-urlencoded',
       'Authorization': 'Bearer ' + staticjwtoken
    }  
    response = session.post(tokenapiurl, headers=apiheaders, timeout=(30,30))
    logging.debug('Token API Response - %s' % str(response.json())) 
    if response.status_code==200:
      respstr=json.dumps(response.json())   
      jsonstr = json.loads(respstr)
      if 'access_token' in jsonstr:
        tempjwt=jsonstr['access_token']
    else:
      raise Exception (response.json())
    
    logging.debug('Access Token - %s' % tempjwt)  

    # Call the 360 Audit Data API
    # Define a Session object for the HTTP call.  
    session.mount(auditapiurl, sessionadapter)
    # Make the HTTP call with a timeout of 30 seconds. If call fails, retry for a max three times
    apiheaders =  {
        'Authorization': 'Bearer ' + tempjwt
    }  
    response = session.get(auditapiurl, headers=apiheaders, timeout=(30,30))
    logging.debug('Audit Data API Response - %s' % str(response.json())) 
    if response.status_code==200:
      respstr=json.dumps(response.json())   
      jsonstr = json.loads(respstr) 
      if 'items' in jsonstr:
        count=jsonstr['count']
        items=jsonstr['items']
    else:
      raise Exception (response.json())

    logging.debug('Number of Audit Data Files - %s' % count) 
    logging.debug('Audit File URL - %s' % str(items)) 

    # Download audit files from AWS
    for i in range(count):
      filename = '.\output\AuditDataFile' + str(i) + '_' + currdatetime + '.parquet'
      logging.debug('Audit File Name - %s' % filename) 
      urllib.request.urlretrieve(items[i], filename)

    # Convert parquet files to csv
    import glob
    from fastparquet import ParquetFile
    prqtfiles = glob.glob(".\output\*.parquet")
    for p in prqtfiles:
      pr = ParquetFile(p, sep='\t')
      df = pr.to_pandas()
      df.to_csv(p[:-8] + '.csv', index=False)
    
    logging.info('End code execution')   
    logging.info('----------------------------------------------------------------------------------')

  except Exception as ex:    
    logging.debug('Exception - %s' % str(ex))  
    print(str(ex))

if __name__ == "__main__":
  Download360AuditData()
