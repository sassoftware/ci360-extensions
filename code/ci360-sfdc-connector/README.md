# CI360 Salesforce CRM Connector

## Overview

CI360 integration to Salesforce CRM (SFDC) using CI360 Connector Framework. CI360 SFDC Connector is a sample connector that supports creating new contacts and cases in SFDC using Salesforce APIs. Minor modifications of the connector function are needed to support additional use cases (for example, creating leads). This connector supports a more complex use case of creating a new contact and attaching a case to the contact, by orchestrating multiple Salesforce API calls.

## Connector Architecture

Connector uses AWS Lambda function to support SFDC features. 

Lambda function uses the following AWS features:
- Lambda function Environment Variables: for basic configuration that is applicable across tenants (multiple tenants are supported by a single Lambda in order to serve multi-tenant environments)
- IAM: definition of role under which Lambda functions are being executed, and which gives access to other AWS components (like Secrets Manager)
- Secrets Manager: stores API keys and endpoint URLs for Salesforce API and CI360 API gateway (secrets are one pre tenant)
- API Gateway: exposes Lambda function as API endpoint, also secures it using API Keys

## Prerequisites

This connector has been developed for AWS platform. Account needs to be set up for the AWS platform.

## Installation

### AWS Deployment

Steps required to install connector functions to AWS:
- Create Lambda function for outbound (sfdcEventHandler)
- Add Environment Variables for the function
    - event_ttl, sf_auth_token_ttl, sm_secret_id_prefix, sf_owner_id
- Create role in IAM (sfdcConnectorLamdba-role)
    - Grant AWSOpsWorksCloudWatchLogs, SecretsManagerReadWrite permission policies
- Associate role with the Lamdba function
- Create SecretsManager secret(s)
    - one secret per tenant, with ID demo/SFDCConnector/APIs/tenant_id
    - prefix is demo/SFDCConnector/APIs/ and set in environment variable sm_secret_id_prefix
    - secret store should contain the following keys: sf_auth_url, sf_client_id, sf_client_secret, sf_username, sf_password, sf_api_url
- Create API gateway (sfdcApi)
    - Add resources to API gateway
    - Configure POST methods, proxy lambda, enable CORS if desired
    - Configure API Keys and Plans and associate with resource/stage (for authentication)

## Using the Connector

### Configuration

The following environment variables are used to configure connector behavior for outbound function:
-	event_ttl: age, in seconds, after which is incoming event from CI360 considered stale and will not be processed (timestamp in event payload is compared with current time)
-   sf_auth_token_ttl: age, in seconds, after which SF OAuth token needs to be refreshed
-	sm_secret_id_prefix: prefix used to construct Secret Name when retrieving secret from Secret Manager (prefix will be appended with tenant ID to get the full name of the store)
-	sf_owner_id: user ID of SFDC user to be set as owner of new objects created by the connector


### Register your connector in CI360

In order to use the connector, you need to register the connector and endpoint with these details into the CI360 system. Documentation sections are referenced below for eacy access.

**Add and Register a Connector**
Please refer to [`Add and Register a Connector`](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/ext-connectors-add.htm) in SAS Customer Intelligence 360 admin guide.

**Add an Endpoint**
Please refer to [`Add an Endpoint`](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/ext-connectors-add-endpoint.htm) in SAS Customer Intelligence 360 admin guide.

### CI360 Setup

Steps to set up new connector for Salesforce CRM:
-   In CI360, go to General Settings -> External Access -> Connectors
-   Create “New Connector”
-   Name it "SFDC Connector” 
    -   This can be anything but should be logical, as it will be used every time new External System Task for SFDC contact creation is required
-   Create “New Endpoint”
-   Name it “SFDC API”
    -   URL is your deployed Lambda function (behind API Gateway in AWS)
    -   Method is POST
    -   Add x-api-key header if API Keys are configured

### Using Connector with External System Task

Contacts and cases are created in SFDC using External System Task. External System Task can be triggered individually (by configuring Trigger event on the Orchestration tab), or as part of an Activity Map. When you are configuring a new External System Task, you will need to associate it with SFDC connector on the Orchestration tab of the task. You will also need to use a Plain Text creative, which will contain JSON object that includes contact and case information that will be sent to Salesforce.

This is a sample structure of the Plain Text creative that needs to be used with External System Task:
```
{
	"contact": {
	    "LastName": "{{LastName}}",
	    "FirstName": "{{FirstName}}",
	    "Phone": "{{Phone}}",
	    "MobilePhone": "{{Phone}}",	    
	    "Email": "{{Email}}",
	    "LeadSource": "Website",
	    "MailingCountry": "United States", 
	    "Job_Role__c": "Other" 
	}, 
	"case": {
	    "Status": "New",
	    "Subject": "Customer is interested in investments",
	    "Priority": "Medium",
	    "Description": "Customer registered for an investment seminar, plans to invest {{InvestAmount}}"
	}
}
```

## Modifying the Connector

For this connector, we are creating a contact in SFDC and then attaching a new case to the contact we created. Slight modification to the JSON objects passed within Plain Text creative in CI360 and to the logic within the Lambda function would be needed to support different use cases, such as creating leads for example, with or without attached objects. Additionally, note that various SFDC environments will likely have different configuration of specific objects, such as contacts or leads, and as such, will have different fields associated with those objects (or required). This requirement itself does not warrant a code change though, since we are using JSON structure inside the Plain Text creative to supply the fields needed by SFDC and all required fields should be included in the creative itself with appropriate names.

### Processing CI360 Event

Most of the modification is most likely to be around process_event function, and specifically around code where SFDC API is called. In general, we are simply calling the SFDC API endpoints specific to the object we are trying to create. For example, to create a new contact, we'll invoke the contact REST API endpoint and pass in the contact object from our JSON creative:
```
contact_resp = call_sf_api(api_url + "/Contact", json.dumps(contact_data), event_body["externalTenantId"])
```

If a new lead is to be created, we would simply change the endpoint being called and pass in appropriate information for that object. For linked objects, like in this example, where a case is being attached to the contact we just created, reponse infromation from the first call to SFDC REST API is being used to update the object before the second call: 
```
case_data["ContactId"] = contact_resp["id"]
case_resp = call_sf_api(api_url + "/Case", json.dumps(case_data), event_body["externalTenantId"])
```

