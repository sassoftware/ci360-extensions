"""
Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import json
import datetime
import urllib3

print("Initializing function")

# Get the service resource
http = urllib3.PoolManager()

# Initialize global variables
default_sender = os.environ['default_sender']
event_ttl = int(os.environ['event_ttl']) * 1000
sm_secret_id_prefix = os.environ['sm_secret_id_prefix']
secret_cache = {}


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
    # determine if event is "fresh"
    event_age = int(datetime.datetime.utcnow().timestamp() * 1000) - int(event_body["date"]["generatedTimestamp"])
    if event_age > event_ttl:
        print("event too old, age:", event_age)
        return True
    print("identityId:", event_body["identityId"], "tenant_id:", event_body["externalTenantId"])
    # parse creative
    creative_content = event_body["impression"]["creativeContent"]
    #print("creative:", creative_content)
    msg_req = parse_creative(creative_content)
    # validate
    if validate_request(msg_req):
        # now call API
        http_status = call_twilio_api(msg_req, event_body["externalTenantId"])
        if http_status == 200 or http_status == 201:
            return True
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
    msg_req["Body"] = creative_parts[len(creative_parts)-1].strip()
    # parse out the rest of the creative (key:value format)
    for i in range(len(creative_parts)-1):
        msg_attr = creative_parts[i].split(":", 1)
        if msg_attr[0].upper() == "TO":
            to_address = msg_attr[1]
            msg_req["To"] = msg_attr[1]
        elif msg_attr[0].upper() == "FROM":
            from_channel = msg_attr[1]
        elif msg_attr[0].upper() == "MEDIA_URLS":
            msg_req["MediaUrl"] = msg_attr[1]
    # set default sender first, override below if provided
    msg_req["From"] = default_sender
    if from_channel is not None:
        msg_req["From"] = from_channel
    return msg_req

    
"""
validate_request:
Validate message request object, check that it's populated correctly
"""
def validate_request(msg_req):
    if msg_req["Body"] is None or not msg_req["Body"].strip():
        return False
    elif msg_req["To"] is None or not msg_req["To"].strip():
        return False
    elif msg_req["From"] is None or not msg_req["From"].strip():
        return False
    return True

    
"""
call_twilio_api
Twilio API call function
"""
def call_twilio_api(msg_req, tenant_id):
    secret = get_secret(tenant_id)
    #print("msg_req:", msg_req)
    auth_string = secret["twilio_account_sid"] + ":" + secret["twilio_auth_token"]
    req_headers = urllib3.util.make_headers(basic_auth=auth_string)
    r = http.request('POST', secret["twilio_api_url"], fields = msg_req, headers = req_headers)
    print("Response Status:", r.status, "Body:", r.data)
    return r.status

"""
get_secret
Retrieve Secret from SecretsManager containing API 
"""
def get_secret(tenant_id):
    try:
        secret = secret_cache[tenant_id]
        #print("secret found in cache")
    except KeyError:
        print(f"secret not found in cache, fetching for tenant: {tenant_id}")
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
        raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = json.loads(get_secret_value_response['SecretString'])
            return secret
        else:
            print("No SecretString found")
    return None
    