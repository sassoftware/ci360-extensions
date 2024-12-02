#*************************************************************************************************************#
# Program Name: 360CopyItem.py                                                                                #
# Program Description: This program helps copy/promote objects from one 360 tenant to the other.              #
# Author: SAS Global Customer Intelligence Practice                                                           #
# Date: 16-October-2024                                                                                       #
# Copyright  2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.                                   #
# SPDX-License-Identifier: Apache-2.0                                                                         #
#*************************************************************************************************************#

# Import Python Libraries
import json
import jwt
import base64
import os
import time
import csv
import configparser
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError
import logging # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL

# Function that returns a JSON Web token
def getjwt(tenantid,cliensecret):
  encodedSecret = base64.b64encode(bytes(cliensecret, 'utf-8'))
  jwtoken = jwt.encode({'clientID': tenantid}, encodedSecret, algorithm='HS256')
  return jwtoken

# Main function of the program.
def init():
  try:  
    global sourceextgwurl, sourcetenantid, sourceclientsecret, sourcecsvfilepath
    global targetextgwurl, targettenantid, targetclientsecret, targetbusinesscontextid
    global sourcejwtoken, targetjwtoken
    global promotionpkgapiurl, promotionjobapiurl, promotionjobstatusapiurl

    # Read configuration and credential files
    currdate = time.strftime("%Y%m%d")    
    path_current_directory = os.path.dirname(__file__)
    path_config_file = os.path.join(path_current_directory, 'conf/config.ini') # Locate App Config File
    configfile = configparser.ConfigParser()
    configfile.read(path_config_file) # Read App Config File
    path_creds_file = os.path.join(path_current_directory, 'conf/credentials.ini') # Locate App Credentials File
    credsfile = configparser.ConfigParser()
    credsfile.read(path_creds_file) # Read App Credentials File
    
    sourceextgwurl = configfile.get('DEFAULT','source360extgwhost')    
    targetextgwurl = configfile.get('DEFAULT','target360extgwhost')  
    targetbusinesscontextid = configfile.get('DEFAULT','targetbusinesscontextid')
    sourcecsvfilepath = configfile.get('DEFAULT','sourcecsvfilepath')
    loglevel=configfile.get('DEFAULT','loglevel')    

    sourcetenantid = credsfile.get('CREDENTIALS','sourcetenantid')
    sourceclientsecret = credsfile.get('CREDENTIALS','sourceclientsecret') 
    targettenantid = credsfile.get('CREDENTIALS','targettenantid')
    targetclientsecret = credsfile.get('CREDENTIALS','targetclientsecret')
    
    # Configure logging
    logfile = os.path.join(path_current_directory, 'logs\\')
    logfile = logfile + 'Log_' + currdate + '.log'
    logging.basicConfig(filename=logfile, filemode='a+', format='%(asctime)s - %(process)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

    # Get JSON web tokens for both source and target 360 tenants
    sourcejwtoken = getjwt(sourcetenantid, sourceclientsecret)
    targetjwtoken = getjwt(targettenantid, targetclientsecret)

    # Define 360 promotion URLs
    promotionpkgapiurl = sourceextgwurl + '/marketingPromotion/promotionPackageJob'
    promotionjobapiurl = targetextgwurl + '/marketingPromotion/promotionJob'
    promotionjobstatusapiurl = targetextgwurl + '/marketingPromotion/promotionJob'

    # Check if source and target tenant IDs are same
    if sourcetenantid==targettenantid:
       raise Exception('Source and Target tenants should be different. You cannot copy to the same Tenant!')  
  except Exception as ex:
    print(ex)
    raise Exception(ex)  

# Read input CSV file containing list of object data to be copied
def readcsv():
  global sessionadapter, session
  try: 
    sessionadapter = HTTPAdapter(max_retries=3)
    session = requests.Session()
    with open(sourcecsvfilepath) as file_obj:             
            reader_obj = csv.DictReader(file_obj)  
            for row in reader_obj:   # Iterate through CSV rows              
                record=row                         
                data_str=json.dumps(record)
                jsonstr = json.loads(data_str)
                objectID = jsonstr['objectID']
                objectType = jsonstr['objectType']
                objectDependency = jsonstr['objectDependency']
                copy360item(objectID, objectType, objectDependency)
    sessionadapter.close()
    session.close()
  except Exception as ex:
    print(ex)
    raise Exception(ex)      

# Copy object data from one tenant to the other based on input data
def copy360item(objectID, objectType, objDependencies): 
  try:    
    pkgpayload = '{"objectId":"' + objectID + '","objectType":"' + objectType + '","dependencies":"' + objDependencies  + '"}'
    srcheaders =  {'Content-Type': 'application/json','Authorization': 'Bearer ' + sourcejwtoken}    
    session.mount(promotionpkgapiurl, sessionadapter)
   
    # Make the HTTP call with a timeout of 30 seconds. If call fails, retry for a max three times
    response = session.post(promotionpkgapiurl, headers=srcheaders, data=pkgpayload, timeout=(30,30))
    if response.status_code not in (200, 201):
      logging.error(response.text)
      raise Exception(response.text) 
    # Read Response from HTTP call in JSON format  
    respstr=json.dumps(response.json())     
    logging.debug('Response JSON: %s' % respstr)  
    jsonstr = json.loads(respstr)
    if 'promotionPackageId' in jsonstr:
       promotionPackageId=jsonstr['promotionPackageId']
    
    prmtrgtheaders =  {'Content-Type': 'application/json','Authorization': 'Bearer ' + targetjwtoken}    
    if str(objectType).casefold() == 'Message'.casefold() or str(objectType).casefold() == 'Creative'.casefold():
      prmpayload =  '{"promotionPackageId":"' +  promotionPackageId + '"}'
    else:
      prmpayload =  '{"promotionPackageId":"' +  promotionPackageId + '","businessContextId":"' + targetbusinesscontextid + '"}' 
    session.mount(promotionjobapiurl, sessionadapter)
    response = session.post(promotionjobapiurl, headers=prmtrgtheaders, data=prmpayload, timeout=(30,30))
    if response.status_code not in (200, 201):
      logging.error(response.text)
      raise Exception(response.text)    
    respstr=json.dumps(response.json())       
    logging.debug('Response JSON: %s' % respstr)  
    jsonstr = json.loads(respstr)
    if 'promoteJobId' in jsonstr:
       promotionJobId=jsonstr['promoteJobId']
    prmjbstsapiurl = promotionjobstatusapiurl + "/" + promotionJobId    
    prmjbtrgtheaders =  {'Authorization': 'Bearer ' + targetjwtoken}
    status = 'QUEUED'
    while status.casefold() == 'QUEUED'.casefold():
      session.mount(prmjbstsapiurl, sessionadapter)
      response = session.get(prmjbstsapiurl, headers=prmjbtrgtheaders, timeout=(30,30))
      if response.status_code not in (200, 201):
        logging.error(response.text)
        raise Exception(response.text) 
      respstr=json.dumps(response.json())  
      jsonstr = json.loads(respstr)     
      if 'status' in jsonstr:
        status=jsonstr['status']
      if status.casefold() == 'FAILURE'.casefold():
        logging.error(response.text)
    
    print(f"Promotion status of {objectType} Object# {objectID} is: {status}!")
    logging.info(f"Promotion status of {objectType} Object# {objectID} is: {status}!")
  except Exception as ex:
    print(ex)
    raise Exception(ex)
  return (respstr)

# Set entry point of the program
if __name__ == "__main__":
  init()
  resp = readcsv()