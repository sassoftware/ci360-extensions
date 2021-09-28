"""
Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import logging
import os
import json
import datetime
import urllib3

import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.cosmos import exceptions, CosmosClient, PartitionKey

logging.info("Initializing function")

# Initialize the Cosmos client
cosmos_endpoint = os.environ['CosmosDBEndpoint']
cosmos_key = os.environ['CosmosDBKey']

# Get the service resource
cosmos_client = CosmosClient(cosmos_endpoint, cosmos_key)
cosmos_db = cosmos_client.get_database_client('ConnectorDataCache')
http = urllib3.PoolManager()

# Initialize global variables
db_cache_identities_table = os.environ['db_cache_identities_table']
db_failed_requests_table = os.environ['db_failed_requests_table']
scg_default_sender = os.environ['scg_default_sender']
cache_ttl = int(os.environ['cache_ttl'])
event_ttl = int(os.environ['event_ttl']) * 1000
identity_field = ''
try:
    identity_field = os.environ['identity_field']
except KeyError:
    print("identity_field not set")
secret_cache = {}
scg_demo_senders = {}
try:
    scg_demo_senders = json.loads(os.environ['scg_demo_senders'])
except KeyError:
    print("scg_demo_senders not set")
logging.info("scg_demo_senders:", scg_demo_senders)


"""
main:
Main event handler entry point for function request
"""
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    body = req.get_json()

    print("Received event:", body["eventName"])
    event_type = body["eventType"]
    if event_type == "outboundSystem":
        if process_event(body):
            return func.HttpResponse( "OK", status_code=200 )         
        else:
            return func.HttpResponse( "ERROR", status_code=500 )
    else:
        logging.info("event type other than outboundSystem")

    return func.HttpResponse( "OK", status_code=200 )


"""
process_event:
Top level of business logic, event processing is here
"""
def process_event(event_body):
    print(event_body)
    # determine if event is "fresh"
    event_age = int(datetime.datetime.utcnow().timestamp() * 1000) - int(event_body["date"]["generatedTimestamp"])
    if event_age > event_ttl:
        logging.warn("event too old, age: %s", event_age)
        return True
    logging.info("identityId: %s, tenant_id: %s", event_body["identityId"], event_body["externalTenantId"])
    # parse creative
    creative_content = event_body["impression"]["creativeContent"]
    logging.info("creative: %s", creative_content)
    msg_req = parse_creative(creative_content)
    # set external Id
    msg_req["external_id"] = event_body["identityId"] + "|" + event_body["properties"]["externalCode"] + "|" + event_body["externalTenantId"]
    # validate
    if validate_request(msg_req):
        # now call API
        http_status = call_scg_api(msg_req, event_body["externalTenantId"])
        if http_status == 200:
            if identity_field.strip() == "":
                cache_identity(msg_req["to"][0], event_body["identityId"], event_body["externalTenantId"])
            return True
        else:
            save_failed_request(event_body, http_status)
    return False
    
    
"""
parse_creative:
Creative content parsing logic, returns message request object
"""
def parse_creative(creative_content):
    msg_req = {}
    from_channel = None
    creative_parts = creative_content.split(";")
    # body of message to be sent is last part of creative
    msg_req["body"] = creative_parts[len(creative_parts)-1].strip()
    # parse out the rest of the creative (key:value format)
    for i in range(len(creative_parts)-1):
        msg_attr = creative_parts[i].split(":", 1)
        if msg_attr[0].upper() == "TO":
            to_address = msg_attr[1]
            msg_req["to"] = [ msg_attr[1] ]
        elif msg_attr[0].upper() == "FROM":
            from_channel = msg_attr[1]
        elif msg_attr[0].upper() == "MEDIA_URLS":
            msg_req["media_urls"] = msg_attr[1]
    # set default sender first, override below if provided
    msg_req["from"] = get_sender(to_address, scg_default_sender)
    if from_channel is not None:
        msg_req["from"] = from_channel
    return msg_req


"""
get_sender:
Retrieve sender channel or ID based on country code for SMS
"""
def get_sender(to_address, default_sender):
    sender_channel = default_sender
    for country_code in scg_demo_senders:
        country_code_with_prefix = "+" + country_code
        if to_address.startswith(country_code) or to_address.startswith(country_code_with_prefix):
            sender_channel = scg_demo_senders[country_code]
    return sender_channel

    
"""
validate_request:
Validate message request object, check that it's populated correctly
"""
def validate_request(msg_req):
    if msg_req["body"] is None or not msg_req["body"].strip():
        return False
    elif msg_req["to"] is None or len(msg_req["to"]) == 0 or not msg_req["to"][0].strip():
        return False
    elif msg_req["from"] is None or not msg_req["from"].strip():
        return False
    return True

    
"""
call_scg_api
Syniverse API call function
"""
def call_scg_api(msg_req, tenant_id):
    secret = get_secret(tenant_id)
    logging.info("msg_req: %s", msg_req)
    encoded_data = json.dumps(msg_req).encode('utf-8')
    req_headers = { "Authorization": "Bearer " + secret["scg_access_token"], 'Content-Type': 'application/json' }
    r = http.request('POST', secret["scg_api_url"], body = encoded_data, headers = req_headers)
    logging.info("Response Status: %d, Body: %s", r.status, r.data)
    return r.status

"""
cache_identity
Stores identity and phone association for 2-way SMS purposes
"""
def cache_identity(phone, identity_id, tenant_id):
    # calculate TTL expire time
    ttl_ts = int(datetime.datetime.now().timestamp()) + cache_ttl
    container = cosmos_db.get_container_client(db_cache_identities_table)
    # prefix phone with + if needed
    if not phone.startswith("+"):
        phone = "+" + str(phone)
    cache_item = { 'id': phone, 'phone': phone, 'datahub_id': identity_id, 'tenant_id': tenant_id, 'expire_ts': ttl_ts }
    print("Caching identity: ", cache_item)
    # put_item will upsert the record based on phone (key)
    container.upsert_item(cache_item)

"""
save_failed_request
Saves data for a failed request
"""
def save_failed_request(event_body, http_status):
    print("Saving failed request: ", event_body)
    container = cosmos_db.get_container_client(db_failed_requests_table)
    # put_item will upsert the record based on guid
    failed_item={
                'id': event_body["guid"],
                'guid': event_body["guid"],
                'generatedTimestamp': event_body["date"]["generatedTimestamp"],
                'event_body': json.dumps(event_body),
                'http_status': http_status, 
                'insert_ts': datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            }
    container.upsert_item(failed_item)   

"""
get_secret
Retrieve Secret from SecretsManager containing API 
"""
def get_secret(tenant_id):    
    try:
        secret = secret_cache[tenant_id]
        logging.info("secret found in cache")
    except KeyError:
        logging.info("secret not found in cache, fetching for tenant: %s", tenant_id)
        secret = fetch_secret(tenant_id)
        secret_cache[tenant_id] = secret
    return secret
    

def fetch_secret(tenant_id):
    app_secrets = os.environ['AppSecrets']
    #logging.debug("AppSecret: %s", app_secrets)
    secret = json.loads(app_secrets)
    return secret
