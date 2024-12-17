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
bearer_token = ""
temp_token = ""
json_string = ""

# START: Initialize Working Directory and Logging for this package.
# Set the working directory to the python script's directory.
script_dir = os.path.dirname(os.path.realpath(__file__))
os.chdir(script_dir)
# Set up logging
logging.basicConfig(
    filename='create_audience_definition.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logging.info('Script started.')
# END: Initialize Working Directory and Logging for this package.


# START: Read JSON data from file and convert into string.
try:
    with open('audience_definition.json', 'r', encoding='utf-8') as file:
        json_data = json.load(file)
        logging.info("JSON data successfully read from file.")
except FileNotFoundError as e:
    logging.error(f"JSON file not found: {e}")
    raise FileNotFoundError(f"JSON file not found: {e}")
except json.JSONDecodeError as e:
    logging.error(f"Error decoding JSON file: {e}")
    raise json.JSONDecodeError(f"Error decoding JSON file: {e}")

# Convert JSON object to string without special characters.
json_string = json.dumps(json_data, ensure_ascii=False)
# END: Read JSON data from file and convert into string.


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
# END: Initialize the parser and read the config file


# This method call 360 API to create an Audience definition and corresponding Audience in 360.
def create_audience_definition():
    """Create audience definition by uploading JSON data."""
    temp_token = obtain_temp_access_token()
    body = json_string
    url = external_gateway_host + '/marketingAudience/audiences'
    headers = {
        "Authorization": "Bearer " + temp_token,
        'Content-Type': 'application/vnd.sas.marketing.audience.definition.upload+json',
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive"
    }

    try:
        response = requests.post(url, data=body, headers=headers)
        response.raise_for_status()
        response_data = response.json()
        logging.info(response_data)
        print(response_data)
        print("AudienceID:- ", response_data['audienceId'])
        print("Please save Audience ID, as it will be needed while uploading the audience. ")
        logging.info("Audience definition created successfully. With AudienceID:- " + response_data['audienceId'])
        print("Audience definition created successfully.")
        return response_data
    except requests.RequestException as e:
        logging.error(f"Error creating audience definition: {e}")
        raise Exception(f"Error creating audience definition: {e}")


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

# This call to method 'create_audience_definition' to create an Audience Definition, and it's corresponding Audience.
response_data = create_audience_definition()
logging.info('Script completed.')
print("Script completed. Please check logs for additional information.")
