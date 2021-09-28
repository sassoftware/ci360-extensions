"""
Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import logging
import os
import json
import urllib3
from datetime import datetime
from uuid import uuid4
from re import split

import azure.functions as func
from azure.cosmos import exceptions, CosmosClient, PartitionKey

logging.info("Initializing function")

# Initialize the Cosmos client
cosmos_endpoint = os.environ['db_cosmosdb_endpoint']
cosmos_key = os.environ['db_cosmosdb_key']

# Get the service resource
cosmos_client = CosmosClient(cosmos_endpoint, cosmos_key)
cosmos_db = cosmos_client.get_database_client(os.environ['db_database_name'])
http = urllib3.PoolManager()

# Initialize global variables
subscriber_key_attr = os.environ['subscriber_key_attr']
db_cache_requests_table = os.environ['db_cache_requests_table']
db_failed_requests_table = os.environ['db_failed_requests_table']
event_ttl = int(os.environ['event_ttl']) * 1000

# Initialize global "cache" variables
secret_cache = json.loads(os.environ['AppSecrets'])
oauth_token = ''
oauth_expires = 0

# Set global constants
oauth_req_headers = { 'Content-Type': 'application/json' }
api_req_headers = { 'Authorization': '', 'Content-Type': 'application/json' }



"""
main:
Main event handler entry point for function request
"""
def main(req: func.HttpRequest) -> func.HttpResponse:
    body = req.get_json()
    logging.info("Received event: %s", body["eventName"])
    event_type = body["eventType"]
    stub_send = False
    try:
        if req.params["stubSend"].upper() == "TRUE":
            stub_send = True
    except KeyError:
        pass
    logging.debug("stub_send: %s", stub_send)
    # Initialize response body
    http_resp_json = { 'status': '' }
    if event_type == "outboundSystem":
        try: 
            request_id = process_event(body, stub_send)
            http_resp_json = { 'status': 'OK', 'requestId': request_id }
            return func.HttpResponse( json.dumps(http_resp_json), status_code=200 )
        except AssertionError as error:
            logging.warn(str(error))
            http_resp_json = { 'status': 'BadRequest', 'message': str(error) }
            return func.HttpResponse( json.dumps(http_resp_json), status_code=400 )
        except KeyError as error:
            logging.warn("Missing configuration key: %s", str(error))
            http_resp_json = { 'status': 'Error', 'message': 'Missing key ' + str(error) }
            return func.HttpResponse( json.dumps(http_resp_json), status_code=500 )
        except:
            logging.warn("Unknown error occured: %s", str(error))
            http_resp_json = { 'status': 'Error', 'message': 'Unknown error occured' }
            return func.HttpResponse( json.dumps(http_resp_json), status_code=500 )
    else:
        logging.info("Event type other than outboundSystem")
    http_resp_json["status"] = "Accepted"
    return func.HttpResponse( json.dumps(http_resp_json), status_code=202 )


"""
process_event:
Top level of business logic, event processing is here
"""
def process_event(event_body, stub_send):
    # determine if event is "fresh"
    event_age = int(datetime.utcnow().timestamp() * 1000) - int(event_body["date"]["generatedTimestamp"])
    assert event_age < event_ttl, "Event too old, age: " + str(event_age)
    logging.info("identityId: %s, tenant_id: %s RTC: %s", event_body["identityId"], event_body["externalTenantId"], event_body["contactResponse"]["responseTrackingCode"])
    # parse creative
    msg_req, definition_key = parse_creative(event_body["impression"]["creativeContent"])
    # validate
    assert validate_request(msg_req, definition_key), "Invalid request envelope"
    # get auth token
    token = get_oauth_token()
    # now call send API
    http_status, request_id = call_sfmc_sms_api(msg_req, definition_key, token, stub_send)
    if http_status == 200 or http_status == 201 or http_status == 202:
        try:
            cache_msg_request(request_id, event_body["identityId"], event_body["externalTenantId"], event_body["contactResponse"]["responseTrackingCode"])
        except exceptions.CosmosHttpResponseError as error:
            logging.warn("CosmosDB request failed: %s", str(error))
        return request_id
    else:
        save_failed_request(event_body, http_status)
    return ""
    


"""
parse_creative:
Creative content parsing logic, returns message request object (envelope)
"""
def parse_creative(creative_content):
    # initialize envelope
    msg_req = {
            "Subscribers": [
                {
                    "MobileNumber": "",
                    "SubscriberKey": "",
                    "Attributes": {
                        "SMS_EN": ""
                    }
                }
            ],
            "Subscribe": "true",
            "Resubscribe": "true",
            "keyword": "UAT_JOIN",
            "Override": "false"
        }
    definition_key = ""
    creative_parts = split(";|\n", creative_content)
    # body of message to be sent is last part of creative
    msg_req["Subscribers"][0]["Attributes"]["SMS_EN"] = creative_parts[len(creative_parts)-1].strip()
    # parse out the rest of the creative (key:value format)
    for i in range(len(creative_parts)-1):
        if creative_parts[i].strip():
            msg_attr = creative_parts[i].split(":", 1)
            if len(msg_attr) == 2:
                attr_name = msg_attr[0].strip()
                attr_value = msg_attr[1].strip()
                if attr_name.upper() == "DEFINITIONKEY":
                    definition_key = attr_value
                elif attr_name.upper() == "TO":
                    msg_req["Subscribers"][0]["MobileNumber"] = attr_value
                elif attr_name.upper() == "SUBSCRIBERKEY":
                    msg_req["Subscribers"][0]["SubscriberKey"] = attr_value
                else:
                    if attr_name.upper() == subscriber_key_attr.upper():
                        msg_req["Subscribers"][0]["SubscriberKey"] = attr_value
            else:
                logging.warn("Invalid attribute: %s", creative_parts[i])
    return msg_req, definition_key



    
"""
validate_request:
Validate message request object, check that it's populated correctly
"""
def validate_request(msg_req, definition_key):
    if definition_key is None or not definition_key.strip():
        return False
    return True

    
"""
call_sfmc_api
Salesforce API call function
"""
def call_sfmc_sms_api(msg_req, definition_key, oauth_token, stub_send):
    logging.info("Call API - def_key: %s", definition_key)    
    secret = secret_cache
    encoded_data = json.dumps(msg_req).encode('utf-8')
    api_req_headers["Authorization"] = "Bearer " + oauth_token
    api_url = secret["sfmc_sms_api_url"] + definition_key + "/send"
    logging.debug("Calling API: %s", api_url)
    if stub_send:
        logging.info("Stubbing send API call")
        request_id = "TEST_" + str(uuid4())
        return 202, request_id
    else:
        r = http.request('POST', api_url, body = encoded_data, headers = api_req_headers)
        logging.info("Response Status: %d, Body: %s", r.status, r.data)
        request_id = ""
        if r.status == 202:
            sfmc_resp = json.loads(r.data)
            request_id = sfmc_resp["tokenId"]
        return r.status, request_id    


"""
get_oauth_token
Get access token from cache or call OAuth API
"""
def get_oauth_token():
    global oauth_token, oauth_expires
    current_time = int(datetime.utcnow().timestamp())
    if oauth_token.strip() and oauth_expires > current_time:
        logging.debug("Valid token in cache")
    else:
        logging.info("Expired token, fetching new token")
        oauth_token, expires_in = call_sfmc_oauth()
        logging.debug("New token received, expires_in: %s", expires_in)
        oauth_expires = current_time + expires_in
    return oauth_token


"""
call_sfmc_auth_api
Salesforce OAuth call function
"""
def call_sfmc_oauth():
    secret = secret_cache
    auth_payload = { 
                    "grant_type": "client_credentials", 
                    "client_id": secret["sfmc_client_id"], 
                    "client_secret": secret["sfmc_client_secret"], 
                    "scope": "sms_read sms_send sms_write", 
                    "account_id": secret["sfmc_account_id"] 
                }
    encoded_data = json.dumps(auth_payload).encode('utf-8')
    r = http.request('POST', secret["sfmc_auth_url"], body = encoded_data, headers = oauth_req_headers)
    logging.debug("Response Status: %d", r.status)
    auth_resp = json.loads(r.data)
    return auth_resp["access_token"], auth_resp["expires_in"]


"""
cache_msg_request
Stores identity and requestId association for status update
"""
def cache_msg_request(request_id, identity_id, tenant_id, rtc):
    logging.info("Caching request to table: %s", db_cache_requests_table)
    container = cosmos_db.get_container_client(db_cache_requests_table)
    cache_item = { 
                'id': request_id, 
                'request_id': request_id, 
                'datahub_id': identity_id, 
                'tenant_id': tenant_id, 
                'rtc': rtc 
            }
    logging.info("Caching request: %s", cache_item)
    # upsert_item will upsert the record based on request_id (key)
    container.upsert_item(cache_item)



"""
save_failed_request
Saves data for a failed request
"""
def save_failed_request(event_body, http_status):
    logging.info("Saving failed request: %s", event_body["guid"])
    container = cosmos_db.get_container_client(db_failed_requests_table)
    failed_item={
                'id': event_body["guid"],
                'guid': event_body["guid"],
                'generatedTimestamp': event_body["date"]["generatedTimestamp"],
                'event_body': json.dumps(event_body),
                'http_status': http_status, 
                'insert_ts': datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            }
    # upsert_item will upsert the record based on guid (key)
    container.upsert_item(failed_item)

