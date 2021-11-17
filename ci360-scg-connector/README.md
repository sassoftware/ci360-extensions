# CI360 Syniverse Connector

## Overview

CI360 SCG Connector provides integration to Syniverse Communication Gateway using CI360 Connector Framework. CI360 Syniverse Connector supports outbound SMS/MMS, WhatsApp and WeChat messages using SCG APIs. It enables marketing end-users to send SMS, MMS, WhatsApp and WeChat messages to their customers using CI360 External System Task.

### Connector Architecture

Syniverse connector has been implemented for both AWS Lambda and Azure Functions. High level application architecture is the same:

Connector uses two functions (Azure or AWS) to support all the features. Webhook function is used for message status updates and two-way messaging (SMS replies). If only simple "fire and forget" one way messaging is required, Webhook function is not required. Webhook function is not configured as part of connector setup in CI360, it simply injects external events using CI360 API gateway.


## Prerequisites

This connector has been developed for AWS and Azure platforms. Accounts need to be set up for the target platform.
Additionally, command line tools are required for deployment to Azure.

## Installation

### AWS Deployment

Lambda functions use the following AWS features:
- Lambda function Environment Variables: for basic configuration that is applicable across tenants (multiple tenants are supported by a single Lambda in order to serve our demo environments)
- IAM: definition of role under which Lambda functions are being executed, and which gives access to other AWS components (like Dynamo and Secrets Manager)
- Secrets Manager: stores API keys and endpoint URLs for Syniverse API and CI360 API gateway
- DynamoDB: used as short term storage for "cached" identities (relationship between CI360 identity ID and recepient phone)
- API Gateway: exposes Lambda function as API endpoint, also secures it using API Keys

Steps required to install connector functions to AWS:
- Create Lambda function for outbound (scgMessageRequest)
- Create Lamdba function for inbound/webhook (scgWebhook)
- Add Environment Variables for both functions
    - scgMessageRequest: cache_ttl, event_ttl, db_cache_identities_table, db_failed_requests_table, identity_field, scg_default_sender, sm_secret_id_prefix, tenant_id
    - scgWebhook: db_cache_identities_table, identity_field, scg_mo_body_uppercase, scg_mo_event, scg_response_event_DELIVERED, scg_response_event_FAILED, scg_response_event_CLICKED, sm_secret_id_prefix, tenant_id
- Create role in IAM (scgConnectorLamdba)
    - Grant AWSOpsWorksCloudWatchLogs, AmazonDynamoDBFullAccess and SecretsManagerReadWrite permission policies
- Associate role with two Lamdba functions
- Create SecretsManager secret(s)
    - one secret per tenant, with ID demo/SyniverseConnector/APIs/tenant_id
    - prefix is demo/SyniverseConnector/APIs/ and set in environment variable sm_secret_id_prefix
    - secret store should contain the following keys: ci360_api_url, ci360_token, scg_api_url, scg_access_token
- Create DynamoDB table “Identities” (or whatever name is configured in db_cache_identities_table variable)
    - Partition key is “phone” (String)
    - Enable TTL if desired (field “expire_ts”)
- Create DynamoDB table “FailedRequests” (or whatever name is configured in db_failed_requests_table variable)
    - Partition key is “guid” (String)
- Create API gateway (scgApi)
    - Add resources (one for scgMessageRequest, one for scgWebhook)
    - Configure POST methods, proxy Lambda, enable CORS if desired
    - Configure API Keys and Plans and associate with resource/stage (for authentication)


### Azure Deployment

Create necessary resources:
- Create a new Function App
- Turn on Managed Identity: Function App -> Overview -> Features
    - We will assign roles from various other services, but you can see all assigned roles here too
- Create environment variables at Function App level 
    - Configuration -> Environment variables
- Create new CosmosDB
     - Granting access to Cosmos DB for Azure Function: Cosmos DB (ci360-conn) -> Access Control (IAM)
        - Role Assignment -> Add Role Assignment, Select a Role (Cosmos DB Account Reader and DocumentDB Account Contributor), Assign Access to: Function App, then select our app (ci360-scg-conn)
- Create new Key Vaule
    - Granting access to Key Vault for Azure Function: Key Vault -> Access Control (IAM)
        - Role Assignment -> Add Role Assignment, Select a Role (Cosmos DB Account Reader and DocumentDB Account Contributor), Assign Access to: Function App, then select our app (ci360-scg-conn)

Alternatively, function app and resources can be created using command line options. For example, to create and initialize Syniverse connector function app:
```
func init ci360-scg-conn --python
func new --name HttpExample --template "HTTP trigger" --authlevel "anonymous"
```

Securing Function:
- Functions -> sfmcMessage -> Integration
- Click on HTTP Trigger, see Authorization level: Anonymous or Function
- Cannot be changed in portal/UI for Linux consumption apps (most likely we are using this), has to be changed in function.json file, then deployed
    - “authLevel”: “anonymous” to “authLevel”: “function”

Function App can be deployed using Visual Studio Code, if Azure extension is installed. Using Azure CLI, from root level folder of your application, execute:
```
func azure functionapp publish ci360-scg-conn
```


## Using the Connector

### Configuration

The following environment variables are used to configure connector behavior for outbound function:
-	cache_ttl: time, in seconds, after which cached entry (in identity cache table) will be purged
-	event_ttl: age, in seconds, after which is incoming event from CI360 considered stale and will not be processed (timestamp in event payload is compared with current time)
-	db_cache_identities_table: table name in DynamoDB used to store/cache identity-recipient mapping
-	db_failed_requests_table: table name in DynamoDB used to store failed requests (for possible future processing)
-	identity_field: Identity field to be used if datahub ID is not cached (e.g. subject_id), and it will be populated with recipient/sender address (optional)
-	scg_default_sender: Default sender channel or ID to be used for outgoing messages, if sender is not provided in message payload
-	scg_demo_senders: mapping of international country codes to Syniverse public channels (used primarily to support SAS demo purposes)
-	sm_secret_id_prefix: prefix used to construct Secret Name when retrieving secret from Secret Manager (prefix will be appended with tenant ID to get the full name of the store)
-	tenant_id: default tenant ID, to be used for processing inbound (Syniverse MO messages) when tenant ID is not found in identity cache

The following environment variables are used to configure connector behavior for webhook function:
-	db_cache_identities_table: table name in DynamoDB used to store/cache identity-recipient mapping
-	identity_field: Identity field to be used if datahub ID is not cached (e.g. subject_id), and it will be populated with recipient/sender address (optional)
-	scg_mo_body_uppercase: Indicates whether the whole incoming message should be upper-cased before being injected into CI360 (allows for case-insensitive attribute based criteria in CI360)
-	scg_mo_event: Name of external event to be injected into CI360 when inbound (MO) message has been received by the webhook
-	scg_response_event variables: Each variable specified the name of external event to be injected into CI360 when specific message state update has been received
    -	scg_response_event_DELIVERED
    -	scg_response_event_FAILED
    -	scg_response_event_CLICKED
    -	scg_response_event_READ
-	sm_secret_id_prefix: prefix used to construct Secret Name when retrieving secret from Secret Manager (prefix will be appended with tenant ID to get the full name of the store)
-	tenant_id: default tenant ID, to be used for processing inbound (Syniverse MO messages) when tenant ID is not found in identity cache

NOTE: While AWS Lambda variables are defined for each function separately, they are shared for Azure Functions under the parent Function App.

### Register your connector in CI360

In order to use the connector, you need to register the connector and endpoint with these details into the CI360 system. Documentation sections are referenced below for eacy access.

**Add and Register a Connector**
Please refer to [`Add and Register a Connector`](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/ext-connectors-add.htm) in SAS Customer Intelligence 360 admin guide.

**Add an Endpoint**
Please refer to [`Add an Endpoint`](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/ext-connectors-add-endpoint.htm) in SAS Customer Intelligence 360 admin guide.


### CI360 Setup

Steps to set up new connector for Syniverse:
-   In CI360, go to General Settings -> External Access -> Connectors
-   Create “New Connector”
-   Name it “Syniverse Connector” 
    -   This can be anything but should be logical, as it will be used every time new External System Task for message delivery is created
-   Create “New Endpoint”
-   Name it “SCG Message API”
    -   URL is your deployed Lambda function (behind API Gateway in AWS)
    -   Method is POST
    -   Add x-api-key header if API Keys are configured

### Using Connector with External System Task

Messages are sent to customers using External System Task. External System Task can be triggered individually (by configuring Trigger event on the Orchestration tab), or as part of an Activity Map. When you are configuring a new External System Task, you will need to associate it with Syniverse on the Orchestration tab of the task. You will also need to use a Plain Text creative, which will contain all the personalization variables that will be sent to Syniverse for execution.

This is a sample structure of the Plain Text creative that needs to be used with External System Task:
```
TO:{{phone}};
FROM:channel:aaaweFWFAaefAEFaefaw;
CHANNEL:MMS;
MEDIAL_URLS:https://www.sas.com/images/logo.jpg;
{{firstname}}, thank you for registering for our seminar!
```

Last line of the creative contains the actual (and personalized) text message to be sent. TO field contains the email address of the recipient. FROM and CHANNEL fields are optional - FROM will replace a default sender channel from configuration if provided, and CHANNEL defaults to SMS/MMS if not specified. MEDIA_URLS is only required if media (such as an image) is attached to the message. Applicable to MMS and WHATSAPP.

