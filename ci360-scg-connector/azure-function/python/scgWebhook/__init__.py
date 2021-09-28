"""
Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import logging
import os
import json
import urllib3

import azure.functions as func
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.cosmos import exceptions, CosmosClient, PartitionKey

logging.info("Initializing function")

# Initialize the Cosmos client
cosmos_endpoint = os.environ['CosmosDBEndpoint']
cosmos_key = os.environ['CosmosDBKey']

# Get the service resource
cosmos_client = CosmosClient(cosmos_endpoint, cosmos_key)
cosmos_db = cosmos_client.get_database_client('ConnectorDataCache')

# Initialize global variables
db_cache_identities_table = os.environ['db_cache_identities_table']
scg_mo_event = os.environ['scg_mo_event']
sm_secret_id_prefix = os.environ['sm_secret_id_prefix']
scg_mo_body_uppercase = os.environ['scg_mo_body_uppercase'].lower() == 'true'
identity_field = os.environ['identity_field']
default_tenant_id = os.environ['tenant_id']
secret_cache = {}


"""
main:
Main event handler entry point for function request
"""
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    event_body = req.get_json()
    event_type = event_body["event"]["evt-tp"]
    logging.info("event_type: %s", event_type)
    # check if MO message or status update
    if event_type == "mo_message_received":
        process_mo_message(event_body)
    else:
        process_status_message(event_body)
    
    return func.HttpResponse("OK")


"""
process_status_message:
Message status update processing logic
"""
def process_status_message(event_body):
    msg_data = {}
    fld_list = event_body["event"]["fld-val-list"]
    # parse out required values
    msg_id = fld_list["message_id"]
    to_address = fld_list["to_address"]
    from_address = fld_list["from_address"]
    new_state = fld_list["new_state"]
    logging.info("msg_id: %s, new_state: %s", msg_id, new_state)
    # parse out external message id
    ext_msg_id = fld_list["external_message_request_id"]
    id_parts = ext_msg_id.split("|")
    tenant_id = None
    if len(id_parts) >= 2:
        msg_data["datahub_id"] = id_parts[0]
        msg_data["externalCode"] = id_parts[1]
    if len(id_parts) >= 3:
        tenant_id = id_parts[2]
    if tenant_id is None:
        logging.info("No tenant_id in external_id, using default tenant_id: %s", default_tenant_id)
        tenant_id = default_tenant_id
    # translate new_state to external 360 event
    try:
        msg_data["eventName"] = os.environ["scg_response_event_" + new_state]
        call_ci360_api(msg_data, tenant_id)
    except KeyError as e:
        logging.info("No event defined for state %s", new_state)

"""
process_mo_message:
Inbound (MO) message processing logic
"""
def process_mo_message(event_body):
    fld_list = event_body["event"]["fld-val-list"]
    # parse out required values
    #msg_id = fld_list["message_id"]
    #to_address = fld_list["to_address"]
    from_address = fld_list["from_address"]
    msg_body = fld_list["message_body"]
    # check if message_body is JSON for RCS reply
    try:
        msg_json = json.loads(msg_body)
    except ValueError as e:
        logging.info("simple message body - not JSON")
        # uppercase message body if configured
        if scg_mo_body_uppercase:
            msg_body = fld_list["message_body"].upper()
    else:
        logging.info("JSON message body: %s", msg_json)
        msg_body = msg_json["postbackData"]
    # determine inbound channel (default = SMS)
    channel = "SMS"
    sender_id = fld_list["sender_id_id"]
    # hardcoded sender_id for WhatsApp demo
    if sender_id == "LhvA6oXM43cXBlFiozlmG4":
        channel = "WhatsApp"
    # get datahub_id and tenant from cache based on phone
    datahub_id, tenant_id = get_cached_identity(from_address)
    if tenant_id is None:
        logging.info("No tenant_id in cache, using default tenant_id: %s", default_tenant_id)
        tenant_id = default_tenant_id
    logging.info("datahub_id: %s, tenant_id: ", datahub_id, tenant_id)
    if identity_field.strip() == "":
        if datahub_id is not None:
            msg_data = { "eventName": scg_mo_event, "datahub_id": datahub_id, "message_body": msg_body }
            call_ci360_api(msg_data, tenant_id)
        else:
            logging.info("no cached identity found and no identity field")
    else:
        msg_data = { "eventName": scg_mo_event, identity_field: from_address, "message_body": msg_body }
        call_ci360_api(msg_data, tenant_id)
    

"""
call_ci360_api:
Method calls CI360 Event API to inject external event
"""
def call_ci360_api(msg_data, tenant_id):
    secret = get_secret(tenant_id)
    if secret is not None:
        # call api
        logging.info("CI360 event msg_data: %s", msg_data)
        encoded_data = json.dumps(msg_data).encode('utf-8')
        req_headers = { "Authorization": "Bearer " + secret['ci360_token'], 'Content-Type': 'application/json' }
        http = urllib3.PoolManager()
        r = http.request('POST', secret['ci360_api_url'], body = encoded_data, headers = req_headers)
        logging.info("Response Status: %s, Body: %s", r.status, r.data)
    else:
        logging.info("Could not get secret (API keys)")
        
"""
get_cached_identity:
Retrieve identity from cache (DynamoDB table) based on message recipient/sender
"""
def get_cached_identity(from_addr):
    print("Reading identity data from cache")
    # read data from cache
    container = cosmos_db.get_container_client(db_cache_identities_table)
    try:
        db_item = container.read_item(item=from_addr, partition_key=from_addr)
    except exceptions.CosmosResourceNotFoundError as e:
        logging.info(e.response['Error']['Message'])
        return None
    else:
        #print(response)
        datahub_id = None
        tenant_id = None
        try:
            datahub_id = db_item['Item']['datahub_id']
            tenant_id = db_item['Item']['tenant_id']
        except KeyError:
            logging.info("no item returned from cache")
        return datahub_id, tenant_id

"""
get_secret
Retrieve Secret from cache or SecretsManager containing API tokens
"""
def get_secret(tenant_id):
    try:
        secret = secret_cache[tenant_id]
        print("secret found in cache")
        #print("secret:", secret)
    except KeyError:
        print(f"secret not found in cache, fetching for tenant: {tenant_id}")
        #secret = fetch_secret(tenant_id)
        secret_cache[tenant_id] = secret
    return secret
    
"""
fetch_secret
Retrieve Secret from SecretsManager containing API tokens (used when secret not found in cache)
"""
def fetch_secret(tenant_id):
    # Get secrets
    app_secrets = os.environ['AppSecrets']
    logging.info("AppSecret: %s", app_secrets)
    secret = json.loads(app_secrets)
    return secret