#*************************************************************************************************************#
# Program Name: ConnectorProxy.py                                                                             #
# Program Description: This python program hosts a flask web service that is invoked from a 360 connector.    #
#                      It encodes and converts the json request payload to xml and does a post request to an  #
#                      on-prem SAS Stored Process hosted as a SAS BI Web Service with the new xml payload.    #
#                                                                                                             #
# Author: Global Customer Intelligence Practice                                                               #
# Date: 11-October-2024                                                                                       #
#                                                                                                             #
# Copyright  2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.                                   #
# SPDX-License-Identifier: Apache-2.0                                                                         #
#*************************************************************************************************************#

# Import Python Libraries
from flask import Flask, request, jsonify, json
from conf.serviceConfig import svcConfig
import os
import time
import configparser
import requests
import html
from io import StringIO
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError
import logging # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL

# Custom Class for implementing String Builder type functionality
class StringBuilder:
  _file_str = None

  def __init__(self):
    self._file_str = StringIO()

  def Append(self, str):
    self._file_str.write(str)

  def __str__(self):
    return self._file_str.getvalue()
  
app = Flask(__name__)
app.config.from_object(svcConfig)
@app.route('/ExecuteSTP', methods=['POST']) # REST API URL

def init():
  global currdatetime, configfile
  try:
    currdatetime = time.strftime("%Y%m%d%H%M%S")    
    path_current_directory = os.path.dirname(__file__)
     # Locate App Config File
    path_config_file = os.path.join(path_current_directory, 'conf', 'serviceConfig.ini')
    # Locate App Credentials File    
    path_creds_file = os.path.join(path_current_directory, 'conf', 'credentials.ini') 
    configfile = configparser.ConfigParser()
    # Read App Config File
    configfile.read(path_config_file) 
    credsfile = configparser.ConfigParser()
    # Read App Credentials File
    credsfile.read(path_creds_file) 
  except:
    print('Cannot read Configuration File!')    
    raise Exception('Cannot read Configuration File!')



def ExecuteSTP(): # REST API Function
  
  

  try: # Read values from App Config Files
    url = configfile.get('DEFAULT','stpurl')
    #stpusername = credsfile.get('DEFAULT','stpusername')
    #stppassword = credsfile.get('DEFAULT','stppassword')
    proglog = configfile.get('DEFAULT','proglog')
    # Log data only if config parameter is set to true
    if (proglog.casefold == 'true'):
      loglevel=configfile.get('DEFAULT','loglevel')    
      logfile = configfile.get('DEFAULT','logfilepath')
      logfile = logfile + 'ServiceLog_' + currdatetime + '.log'
      
      logging.basicConfig(filename=logfile, filemode='a+', format='%(asctime)s - %(process)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
      if (loglevel.casefold=='debug'): # Set apt log level based on config value
        logging.getLogger().setLevel(logging.DEBUG)
      elif (loglevel.casefold=='info'):
        logging.getLogger().setLevel(logging.INFO)
      elif (loglevel.casefold=='warning'):
        logging.getLogger().setLevel(logging.WARNING)
      elif (loglevel.casefold=='error'):
        logging.getLogger().setLevel(logging.ERROR)
      elif (loglevel.casefold=='critical'):
        logging.getLogger().setLevel(logging.CRITICAL)
        
      logging.info('----------------------------------------------------------------------------------')
      logging.info('Code Execution Started!!!')    

    # Read request payload from 360
    payload = request.data
    decstr = payload.decode('utf-8')
    jsonstr = json.loads(decstr)
    stpName = jsonstr['sendParameters']['stpName']
    stpUrl = url.replace("{{stpName}}",stpName)

    if (proglog == 'true'):
      logging.debug("STP Name: " + str(stpName))
      logging.debug("STP URL: " + str(stpUrl))
      logging.debug("360 Payload: " + str(payload))

    # Build a XML string from the json payload
    sbxmldata = StringBuilder()
    sbxmldata.Append('<' + stpName + '>\r\n')
    sbxmldata.Append('<streams>\r\n')
    sbxmldata.Append('<instream>\r\n')
    sbxmldata.Append('<Value>\r\n')
    sbxmldata.Append('<Table>\r\n')
    sbxmldata.Append('<InData>\r\n')

    def convertJsonToXml(json_obj):
      for key, value in json_obj.items():
        if isinstance(value, dict):
          convertJsonToXml(value)
        else:    
          sbxmldata.Append('<' + key + '>' + html.escape(str(value)) + '</' + key + '>\r\n')
      return(str(sbxmldata))
    
    tempstr=convertJsonToXml(jsonstr)    
    sbxmldata.Append('</InData>\r\n')
    sbxmldata.Append('</Table>\r\n')
    sbxmldata.Append('</Value>\r\n')
    sbxmldata.Append('</instream>\r\n')
    sbxmldata.Append('</streams>\r\n')
    sbxmldata.Append('</' + stpName + '>\r\n')  

    if (proglog.casefold == 'true'):
      logging.debug("XML Data: " + str(sbxmldata))      
      f = open( 'logs\\360TaskPayload_' + currdatetime + '.xml', 'w')
      f.write(str(sbxmldata))
      f.close()

    # Define a Session object for the HTTP call.  
    sessionadapter = HTTPAdapter(max_retries=3)
    session = requests.Session()
    session.mount(stpUrl, sessionadapter)
    # Make the HTTP call to the STP with a timeout of 30 seconds. If call fails, retry for a max three times
    stpheaders =  {'Content-Type':'application/xml'}  
    stpdata = str(sbxmldata)
    response = session.post(stpUrl, headers=stpheaders, data=stpdata, timeout=(30,30))
    
    if (proglog.casefold == 'true'):
      logging.debug("STP Response: " + str(response.text))  
      f = open( 'logs\STPResponse_' + currdatetime + '.txt', 'w' )
      f.write( str(response) )
      f.close()
    print("STP Response:" + str(response.text))
  except Exception as err:
    print(str(err))
    if (proglog == 'true'):
      logging.error(str(err))

  if (proglog == 'true'):
    logging.info('Code Execution Ended!!!')
    logging.info('----------------------------------------------------------------------------------')
  # Return STP response to 360
  return (jsonify(response.text))

init()

# The REST API will be published on this location on the Server
if __name__ == '__main__':
  with app.app_context():    
    app.run(host = app.config['host'], port = app.config['port'], debug=app.config['debug']) 

