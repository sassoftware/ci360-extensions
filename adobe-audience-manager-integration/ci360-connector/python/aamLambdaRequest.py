"""
Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import json

from botocore.vendored import requests

def lambda_handler(event, context):

	#replace the following values
	client = 'DEMDEX_CLIENT_VALUE' #Specific to Website (check the network tab for *.demdex.com calls)
	host = 'DCS_REGION_HOST' #https://docs.adobe.com/content/help/en/audience-manager/user-guide/api-and-sdk-code/dcs/dcs-api-reference/dcs-regions.html
	ci360_token = 'CI360_TOKEN'  #Replace with actual CI360 gateway token (can be managed with variables or secrets)

	#Recieves the JSON body from CI360
	body = json.loads(event['body'])

	# Print jsonincoming json body for debugging
	print("Printing incoming payload:\n", body)

	#extract variables from CI360 Payload
	s_ecid = body['properties']['s_ecid'].strip('"') #Extract Adobe Experience Cloud ID
	eventName = body['customName'].strip('"') #Extract event name
	eventGroupName = body['customGroupName'].strip('"') #Extract event group name

	#Print extracted properties for debugging
	print("Event Name: ", eventName)
	print("Event Group Name: ", eventGroupName)
	print("Adobe Experience Cloud ID: ", s_ecid)

	#setup the URL and headers for requests post
	url = 'https://{host}.demdex.net/event?c_caller=ci360sts&d_rtbd=json&d_dst=1&c_ci360event={eventName}&c_ci360eventcategory={eventGroupName}&mid={s_ecid}'.format(host=host, eventName=eventName, eventGroupName=eventGroupName, s_ecid=s_ecid)
	headers= {'Host': client}

	#Send request to Adobe AAM DCS API
	r = requests.post(url, headers = headers)
	print(r.text)

	#Store and print response in object for External Event
	adobeResponse = r.json()
	print(response)

	### Code for external event into CI360

	#extract datahub id to send back with event
	datahubId = body['identityId'].strip('"')

	#URL and headers for the marketing gateway (note it might be best to use AWS Gateway for this)
	externalEventUrl = 'extapigwservice-training.ci360.sas.com/marketingGateway/events'

	externalEventHeaders = {
	  'Content-Type': 'application/json',
	  'Authorization': 'Bearer ' + ci360_token
	  }

	#This is an example payload, please change according to needs
	externalEventParams = {
	  "eventName": "External Event - Test",
	  "datahub_id": datahubId,
	  "applicationId": "123123",
	  "attributes": {
		  "test": "testingyo",
		  "group": adobeResponse
	  	}
	  }

	#Send request to external event gateway
	externalEventRequest = requests.post(externalEventUrl, json = externalEventParams, headers = externalEventHeaders)
	print(externalEventRequest.json())

	return {
		'statusCode': 200,
		'body': json.dumps(r.text)
	}
