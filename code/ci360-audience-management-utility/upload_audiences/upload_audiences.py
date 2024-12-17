# **********************************************************************************************************************
# PROGRAM: SAS Customer Intelligence 360 Audience Management Utility
# DESCRIPTION: The SAS Customer Intelligence 360 Audience Management Utility package is a helper tool that help App Devs
#            to create and manage Audiences from data source to which SAS Customer Intelligence 360 can not connect
#            directly. The audience definition and the corresponding audience will be created in 360 tenant by using
#            Marketing Audience API. The created audience will have only the schema and no audience data. The Audience
#            data can be uploaded to created audience by again using Marketing Audience API.
#            The configuration file will includes details necessary to authenticate and connect to 360 tenant.
# VERSION: 0.0
# DATE CREATED: 21-AUGUST-2024
# DATE CREATED: 21-AUGUST-2024
#
# #Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# #SPDX-License-Identifier: Apache-2.0
# **********************************************************************************************************************

import requests
import json
import time
import base64
import jwt
import os
import configparser
import logging

# Global variables
external_gateway_host = ""
tenant_id = ""
client_secret = ""
api_user_name = ""
api_user_password = ""
audience_file_name = ""
audience_name = ""
audience_id = ""
bearer_token = ""
temp_token = ""
signed_url = ""


# START: Initialize Working Directory and Logging for this package.
# Set the working directory to the python script's directory.
script_dir = os.path.dirname(os.path.realpath(__file__))
os.chdir(script_dir)
# Set up logging
logging.basicConfig(
    filename='audience_upload.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logging.info('Script started.')
# END: Initialize Working Directory and Logging for this package.


# START: Initialize the parser and read the config file.
config = configparser.ConfigParser()
config.read('config.ini')
# Read properties from the DEFAULT section.
try:
    external_gateway_host = config['DEFAULT']['external_gateway_host']
    tenant_id = config['DEFAULT']['tenant_id']
    client_secret = config['DEFAULT']['client_secret']
    api_user_name = config['DEFAULT']['UID_360']
    api_user_password = config['DEFAULT']['UID_PSWD']
    logging.info('Successfully read DEFAULT section from config file.')
except KeyError as e:
    logging.error(f"Missing configuration key in DEFAULT section: {e}")
    raise KeyError(f"Missing configuration key: {e}")
# Read properties from the Audience_Configuration section
try:
    audience_file_name = config['Audience_Configuration']['audience_file_name']
    audience_name = config['Audience_Configuration']['audience_name']
    audience_id = config['Audience_Configuration']['audience_id']
    logging.info('Successfully read Audience_Configuration section from config file.')
except KeyError as e:
    logging.error(f"Missing configuration key in Audience_Configuration section: {e}")
    raise KeyError(f"Missing configuration key: {e}")
# END: Initialize the parser and read the config file


# This method is used to obtain a signed URL for file upload.
def get_signed_url():
    url = external_gateway_host + "/marketingData/fileTransferLocation"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + bearer_token
    }
    payload = ""
    
    try:
        response = requests.post(url, headers=headers, data=payload)
        response.raise_for_status()
        response_data = response.json()
        logging.info("Signed URL obtained.")
        print("Signed URL obtained.")
        return response_data['signedURL']
    except requests.RequestException as e:
        logging.error(f"Error obtaining signed URL: {e}")
        raise Exception(f"Error obtaining signed URL: {e}")


# This method is used to upload the audience file to the signed URL.
def upload_file_to_signed_location(signed_url, file_name):
    headers = {'Content-type': 'text/csv'}
    
    try:
        with open(file_name, 'rb') as file:
            response = requests.put(signed_url, data=file, headers=headers)
            response.raise_for_status()
            logging.info("File has been uploaded to signed location.")
            print("File has been uploaded to signed location.")
    except FileNotFoundError as e:
        logging.error(f"File not found: {e}")
        raise FileNotFoundError(f"File not found: {e}")
    except requests.RequestException as e:
        logging.error(f"Error uploading file: {e}")
        raise Exception(f"Error uploading file: {e}")


# This method start the data upload job.
def start_data_upload_job():
    url = external_gateway_host + "/marketingAudience/audiences/" + audience_id + "/data"
    payload = json.dumps({
        "name": audience_name,
        "audienceId": audience_id,
        "fileLocation": signed_url,
        "headerRowIncluded": False
    })
    headers = {
        'Authorization': 'Bearer ' + temp_token,
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.put(url, headers=headers, data=payload)
        response.raise_for_status()
        logging.info("Data upload job started.")
        print("Data upload job started.")
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Error starting data upload job: {e}")
        raise Exception(f"Error starting data upload job: {e}")


# This method provides a temporary access token.
def obtain_temp_access_token():
    url = external_gateway_host + '/token'
    headers = {
        "Authorization": "Bearer " + bearer_token,
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive"
    }
    body = {
        "grant_type": "password",
        "username": api_user_name,
        "password": api_user_password
    }

    try:
        response = requests.post(url, data=body, headers=headers)
        response.raise_for_status()
        response_data = response.json()
        logging.info("Temporary access token obtained.")
        return response_data['access_token']
    except requests.RequestException as e:
        logging.error(f"Error obtaining temporary access token: {e}")
        raise Exception(f"Error obtaining temporary access token: {e}")


# This method generates a bearer token.
def get_bearer_token(tenant_id, secret):
    try:
        encoded_secret = base64.b64encode(bytes(secret, 'utf-8'))
        token = jwt.encode({'clientID': tenant_id}, encoded_secret, algorithm='HS256')
        logging.info("Bearer token generated.")
        return token
    except Exception as e:
        logging.error(f"Error generating token: {e}")
        raise Exception(f"Error generating token: {e}")


# This call to method 'get_bearer_token' generate bearer token
bearer_token = get_bearer_token(tenant_id, client_secret)

# This call to method 'obtain_temp_access_token' get temporary token
temp_token = obtain_temp_access_token()

# This call to method 'get_signed_url' to get signed url for CSV file upload
signed_url = get_signed_url()
time.sleep(2)

# This call to method 'upload_file_to_signed_location' to upload the audience file to signed url
upload_file_to_signed_location(signed_url, audience_file_name)
time.sleep(5)

# This call to method 'start_data_upload_job' to start the Audience data upload job
start_data_job_response = start_data_upload_job()
time.sleep(10)

logging.info('Script completed.')
print("Script completed. Please check logs for more details.")
