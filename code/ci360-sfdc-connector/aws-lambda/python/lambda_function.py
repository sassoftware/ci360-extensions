"""
Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import json
import datetime
import urllib3
import boto3
from botocore.exceptions import ClientError

print("Initializing function")

# Get the service resource
http = urllib3.PoolManager()

# Initialize global variables
event_ttl = int(os.environ['event_ttl']) * 1000
cached_token = None
cached_token_exp_ts = 0
token_ttl = int(os.environ['sf_auth_token_ttl'])
sm_secret_id_prefix = os.environ['sm_secret_id_prefix']
sf_owner_id = os.environ['sf_owner_id']
secret_cache = {}


"""
lambda_handler:
Main event handler entry point for lamdba request
"""
def lambda_handler(event, context):
    if event is not None and event["body"] is not None:
        body = json.loads(event["body"])
        print("Received event:", body["eventName"], "event type:", body["eventType"])
        event_type = body["eventType"]
        if event_type == "outboundSystem":
            if process_event(body):
                return {
                    'statusCode': 200,
                    'body': json.dumps('OK')
                }                
            else:
                return {
                    'statusCode': 500,
                    'body': json.dumps('ERROR')
                }
        else:
            print("event type other than outboundSystem")
    return {
        'statusCode': 200,
        'body': json.dumps('OK')
    }


"""
process_event:
Top level of business logic, event processing is here
"""
def process_event(event_body):
    # determine if event is "fresh"
    event_age = int(datetime.datetime.utcnow().timestamp() * 1000) - int(event_body["date"]["generatedTimestamp"])
    if event_age > event_ttl:
        print("event too old, age:", event_age)
        return True
    print("identityId:", event_body["identityId"], "tenant_id:", event_body["externalTenantId"])
    # parse creative
    creative_content = event_body["impression"]["creativeContent"]
    try:
        message = json.loads(creative_content)
    except json.JSONDecodeError:
        print("Invalid JSON in creative")
        return False
    contact_data = message["contact"]
    case_data = message["case"]
    contact_data["OwnerId"] = sf_owner_id
    case_data["OwnerId"] = sf_owner_id
    print("Contact:", contact_data, "Case:", case_data)
    # get API URL
    secret = get_secret(event_body["externalTenantId"])
    api_url = secret["sf_api_url"]
    # make API calls
    print("Creating contact")
    contact_resp = call_sf_api(api_url + "/Contact", json.dumps(contact_data), event_body["externalTenantId"])
    if contact_resp is not None:
        case_data["ContactId"] = contact_resp["id"]
        print("Creating case")
        case_resp = call_sf_api(api_url + "/Case", json.dumps(case_data), event_body["externalTenantId"])
        return True
    else:
        return False

"""
call_sf_api
Salesforce API call function
"""
def call_sf_api(sf_api_url, data_string, tenant_id):
    # get OAuth token    
    token = get_sf_oauth_token(tenant_id)
    if token is not None:
        # set headers
        req_headers = { "Authorization": "Bearer " + token, 'Content-Type': 'application/json' }
        print("Calling API:", sf_api_url)
        r = http.request('POST', sf_api_url, body = data_string, headers=req_headers)
        print("Response Status:", r.status, "Body:", r.data)
        response = json.loads(r.data)
        return response
    else:
        print("Failed to get token")


def get_sf_oauth_token(tenant_id):
    global cached_token, cached_token_exp_ts
    now_ts = int(datetime.datetime.utcnow().timestamp())    
    if cached_token is not None and cached_token_exp_ts > now_ts:
        return cached_token
    else:
        # get API credentials
        secret = get_secret(tenant_id)
        # build query string
        querystring = "?grant_type=password&client_id=" + secret["sf_client_id"] + "&client_secret=" + secret["sf_client_secret"] + "&username=" + secret["sf_username"] + "&password=" + secret["sf_password"]
        token_url = secret["sf_auth_url"] + querystring
        print("Auth POST URL:", secret["sf_auth_url"])
        r = http.request('POST', token_url)
        print("Response Status:", r.status, "Body:", r.data)
        if r.status == 200:
            response = json.loads(r.data)
            # store token
            cached_token = response["access_token"]
            cached_token_exp_ts = now_ts + token_ttl
            return response["access_token"]
        return None
    

"""
get_secret
Retrieve Secret from SecretsManager containing API 
"""
def get_secret(tenant_id):
    try:
        secret = secret_cache[tenant_id]
    except KeyError:
        print(f"Secret not found in cache, fetching for tenant: {tenant_id}")
        secret = fetch_secret(tenant_id)
        secret_cache[tenant_id] = secret
    return secret
    
def fetch_secret(tenant_id):
    # Get secrets
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name="us-east-1")
    try:
        secret_id = sm_secret_id_prefix + tenant_id
        get_secret_value_response = client.get_secret_value(SecretId=secret_id)
    except ClientError as e:
        print("Failed to get secrets: ", e.response)
        #raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = json.loads(get_secret_value_response['SecretString'])
            return secret
        else:
            print("No SecretString found")
    return None
    