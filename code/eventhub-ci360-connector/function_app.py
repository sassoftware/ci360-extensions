"""
Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import azure.functions as func
import logging
import requests
import os

app = func.FunctionApp()

@app.event_hub_message_trigger(arg_name="azeventhub", event_hub_name="ci360_events",
                               connection="CI360EventHubNM_RootManageSharedAccessKey_EVENTHUB") 
def eventhub_to_CI(azeventhub: func.EventHubEvent):
    logging.info('Python EventHub trigger processed an event: %s',
                azeventhub.get_body().decode('utf-8'))

    logging.info("Sending Event to CI360.")
    
    # Retrieve URL and token from environment variables with error handling
    try:
        url = os.environ["ci360_url"]
        token = os.environ["token"]
        logging.info(f"url: {url}")
    except KeyError as e:
        logging.error(f"Environment variable {e} not found")
        raise
    
    eventbody = azeventhub.get_body().decode('utf-8')


    payload = eventbody
    headers = {
    'Content-type': 'application/json',
    'Authorization': 'Bearer ' + token 
    }

    try:
        # Make the POST request to the API endpoint
        response = requests.post(url, headers=headers, data=payload)

        # Log and print the response text
        logging.info(f"Response: {response.text}")
        print(response.text)

        # Check if the request was successful
        response.raise_for_status()

    except requests.exceptions.RequestException as e:
        # Log any errors that occur
        logging.error(f"Request failed: {e}")
        print(f"Request failed: {e}")