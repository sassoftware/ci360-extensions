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
dynamodb = boto3.resource('dynamodb')
http = urllib3.PoolManager()

# Initialize global variables
db_cache_identities_table = os.environ['db_cache_identities_table']
db_failed_requests_table = os.environ['db_failed_requests_table']
scg_default_sender = os.environ['scg_default_sender']
cache_ttl = int(os.environ['cache_ttl'])
event_ttl = int(os.environ['event_ttl']) * 1000
sm_secret_id_prefix = os.environ['sm_secret_id_prefix']
identity_field = os.environ['identity_field']
default_tenant_id = os.environ['tenant_id']
secret_cache = {}
scg_demo_senders = {}
try:
    scg_demo_senders = json.loads(os.environ['scg_demo_senders'])
except KeyError:
    print("scg_demo_senders not set")
print("scg_demo_senders:", scg_demo_senders)


"""
lambda_handler:
Main event handler entry point for lamdba request
"""
def lambda_handler(event, context):
    if event is not None and event["body"] is not None:
        body = json.loads(event["body"])
        print("Received event:", body["eventName"])
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
    print(event_body)
    # determine if event is "fresh"
    event_age = int(datetime.datetime.utcnow().timestamp() * 1000) - int(event_body["date"]["generatedTimestamp"])
    #print("timestamp:", event_body["date"]["generatedTimestamp"], "current_ts:", int(datetime.datetime.utcnow().timestamp() * 1000), "age:", event_age)
    if event_age > event_ttl:
        print("event too old, age:", event_age)
        return True
    print("identityId:", event_body["identityId"], "tenant_id:", event_body["externalTenantId"])
    # parse creative
    creative_content = event_body["impression"]["creativeContent"]
    print("creative:", creative_content)
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
        #print("attr:", msg_attr[0], "value:", msg_attr[1])
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
        #print(country_code, "->", scg_demo_senders[country_code])
        country_code_with_prefix = "+" + country_code
        if to_address.startswith(country_code) or to_address.startswith(country_code_with_prefix):
            #print("found demo sender for:", to_address)
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
    print("msg_req:", msg_req)
    encoded_data = json.dumps(msg_req).encode('utf-8')
    req_headers = { "Authorization": "Bearer " + secret["scg_access_token"], 'Content-Type': 'application/json' }
    #print("req_headers:", req_headers)
    r = http.request('POST', secret["scg_api_url"], body = encoded_data, headers = req_headers)
    print("Response Status:", r.status, "Body:", r.data)
    return r.status

"""
cache_identity
Stores identity and phone association for 2-way SMS purposes
"""
def cache_identity(phone, identity_id, tenant_id):
    # calculate TTL expire time
    ttl_ts = int(datetime.datetime.now().timestamp()) + cache_ttl
    table = dynamodb.Table(db_cache_identities_table)
    # prefix phone with + if needed
    if not phone.startswith("+"):
        phone = "+" + str(phone)
    cache_item = { 'phone': phone, 'datahub_id': identity_id, 'tenant_id': tenant_id, 'expire_ts': ttl_ts }
    print("Caching identity: ", cache_item)
    # put_item will upsert the record based on phone (key)
    table.put_item(Item=cache_item)
    
"""
save_failed_request
Saves data for a failed request
"""
def save_failed_request(event_body, http_status):
    print("Saving failed request: ", event_body)
    table = dynamodb.Table(db_failed_requests_table)
    # put_item will upsert the record based on guid
    table.put_item(
            Item={
                'guid': event_body["guid"],
                'generatedTimestamp': event_body["date"]["generatedTimestamp"],
                'event_body': json.dumps(event_body),
                'http_status': http_status, 
                'insert_ts': datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            }
        )   
    

"""
get_secret
Retrieve Secret from SecretsManager containing API 
"""
def get_secret(tenant_id):
    try:
        secret = secret_cache[tenant_id]
        print("secret found in cache")
        #print("secret:", secret)
    except KeyError:
        print(f"secret not found in cache, fetching for tenant: {tenant_id}")
        secret = fetch_secret(tenant_id)
        secret_cache[tenant_id] = secret
    return secret
    
def fetch_secret(tenant_id):
    # Get secrets
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name="us-west-2")
    try:
        secret_id = sm_secret_id_prefix + tenant_id
        get_secret_value_response = client.get_secret_value(SecretId=secret_id)
    except ClientError as e:
        print("Failed to get secrets: ", e.response)
        raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = json.loads(get_secret_value_response['SecretString'])
            #print(secret)
            return secret
        else:
            print("No SecretString found")
    return None
    