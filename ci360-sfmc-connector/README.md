# CI360 Salesforce Marketing Cloud Connector

## Overview

CI360 integration to Salesforce Marketing Cloud using CI360 Connector Framework. CI360 SFMC Connector supports outbound email and SMS messages using SFMC APIs.

### Connector Architecture

Connector uses three Azure Functions to support all the features. Two outbound Azure functions implement Email and SMS connector endpoints.

Callback Azure Function is used for message status updates. If only simple "fire and forget" one-way messaging is required, Callback Function is not required. Callback Function is not configured as part of connector setup in CI360, it simply injects external events using CI360 API gateway and is triggered by a call from SFMC and its Event Notification service.

Azure Functions use the following Azure features:
- Azure Function App Configuration: for basic configuration that is applicable across all three functions, and does not contain sensitive information such as API keys or tokens
- IAM: definition of role under which Lambda functions are being executed, and which gives access to other AWS components (like Dynamo and Secrets Manager)
- Azure Key Vault: stores API keys and endpoint URLs for SMFC API and CI360 API gateway
- CosmosDB: used as short-term storage for "cached" identities (relationship between CI360 identity ID and SFMC request IDs) needed when injecting callback/response events back into CI360

## Prerequisites

This connector has been developed for Microsoft Azure platform. Account need to be set up for the Azure platform.
Command line tools are required for deployment to Azure.

## Installation

### Azure Deployment

Create necessary resources:
- Create Azure Function App (ci360-sfmc-conn)
- Add Configuration Variables:
    - event_ttl, db_cosmosdb_endpoint, db_cosmosdb_key, db_database_name, db_cache_requests_table, db_failed_requests_table, subscriber_key_attr, subscriber_key_attr_contact
    - specific values for the configuration variables and descriptions can be found below
- Turn on Managed Identity (under Function App -> Overview -> Features)
    - Grant policies (policies can be granted for all the services from this page, or individually for each service as described below)
- Create CosmosDB table “Identities” (or whatever name we configure in db_cache_identities_table) 
    - Partition key is “datahub_id” (String)
    - Enable TTL if desired
- Grant access to CosmosDB for the Function App (CosmosDB -> Access Control (IAM) -> Role Assignment -> Add Role Assignment)
    - Select roles: Cosmos DB Account Reader and DocumentDB Account Contributor
    - Assign Access to: Function App, then select our app (ci360-sfmc-conn)
- Create Key Vault secret(s)
    - Name secret SalestorceConnector-API-Keys
    - Secret store should contain the following keys: ci360_api_url, ci360_token, sfmc_email_api_url, sfmc_sms_api_url, sfmc_auth_url
- Grant access to Key Vault for the Function App (Key Vault -> Access Control (IAM) -> Role Assignment -> Add Role Assignment)
    - Select roles: Key Vault Reader and Key Vault Secrets User
    - Assign Access to: Function App, then select our app (ci360-sfmc-conn)

Alternatively, function app and resources can be created using command line options. For example, to create and initialize Syniverse connector function app:
```
func init ci360-sfmc-conn --python
func new --name HttpExample --template "HTTP trigger" --authlevel "anonymous"
```

### Using Key Vault reference in Function App Configuration

Values stored in Key Vault Secrets can be referenced directly in application configuration by using a Key Vault reference URL. When configured this way, secret access is controlled by IAM, and secret values are accessed by application code as environment variables, in the same way as non-secured configuration settings.
Example for configuring Key Value reference in application configuration, for Cosmos DB key:
- @Microsoft.KeyVault(SecretUri=https://mykeyvault.vault.azure.net/secrets/CosmosDBKey/6e5cceb9xxxxxxxxxxxc82d53b6e1)
- Get the URL from Key Vault -> Secrets -> your_secret  -> click on current version, then copy Secret Identifier
Once set up, Configuration will show “Key vault Reference” with green checkbox next to application setting which indicates reference is used and confirms it is valid and accessible by the application. 


### Securing Function API Access

Securing Function:
- Functions -> sfmcMessage -> Integration
- Click on HTTP Trigger, see Authorization level: Anonymous or Function
- Cannot be changed in portal/UI for Linux consumption apps (most likely we are using this), has to be changed in function.json file, then deployed
    - “authLevel”: “anonymous” to “authLevel”: “function”

Function App can be deployed using Visual Studio Code, if Azure extension is installed. Using Azure CLI, from root level folder of your application, execute:
```
func azure functionapp publish ci360-sfmc-conn
```


## Using the Connector

### Configuration

The following Azure Function App environment variables are used to configure connector behavior (Settings  Configuration  Application settings):
- __event_ttl__: age, in seconds, after which is incoming event from CI360 considered stale and will not be processed (timestamp in event payload is compared with current time)
- __db_cosmosdb_endpoint__: endpoint URL for Azure CosmosDB instance to be used (e.g. https://ci360-conn.documents.azure.com)
- __db_cosmosdb_key__: CosmosDB access key, using Key Vault secret reference URL (see previous section for description)
- __db_cache_requests_table__: table name in CosmosDB used to store/cache identity-recipient mapping
- __db_database_name__: database name in CosmosDB instance where tables used by the connector are located
- __db_failed_requests_table__: table name in CosmosDB used to store failed requests (for possible future processing)
- __subscriber_key_attr__: personalization field/attribute passed to the connector from CI360 to be used to populate SubscriberKey required by SFMC API (e.g. CONTACT_ID)
- __subscriber_key_attr_contact__: include subscriber key attribute configured in previous setting as content personalization field as well (true/false)


### Register your connector in CI360

In order to use the connector, you need to register the connector and endpoint with these details into the CI360 system. Documentation sections are referenced below for eacy access.

**Add and Register a Connector**
Please refer to [`Add and Register a Connector`](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/ext-connectors-add.htm) in SAS Customer Intelligence 360 admin guide.

**Add an Endpoint**
Please refer to [`Add an Endpoint`](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/ext-connectors-add-endpoint.htm) in SAS Customer Intelligence 360 admin guide.


### CI360 Setup

Steps to set up new connector for Syniverse:
-   In CI360, go to General Settings -> External Access -> Connectors
-   Create "New Connector"
-   Name it "Salesforce Marketing Cloud Connector"
    -   This can be anything but should be logical, as it will be used every time new External System Task for message delivery is created
-   Create “New Endpoint”
-   Name it "Send Email"
    -   URL is your deployed Azure function
    -   Method is POST
    -   Add x-function-key header if your function is secured (recommended)
    -   Repeat these steps for "Send SMS" endpoint


### Using Connector with External System Task

Messages are sent to customers using External System Task. External System Task can be triggered individually (by configuring Trigger event on the Orchestration tab), or as part of an Activity Map. When you are configuring a new External System Task, you will need to associate it with Salesforce Marketing Cloud Connector on the Orchestration tab of the task. You will also need to use a Plain Text creative, which will contain all the personalization variables that will be sent to SFMC for execution.

This is a sample structure of the Plain Text creative that needs to be used with External System Task:
```
DEFINITIONKEY:SASCI360_UAT
TO:{{email}}
CONTACT_ID:{{contact_id}}
FirstName:{{first_name}}
```

DEFINITIONKEY is a value for SFMC send defintion key, and it identifies the template (or send defintion) configured in SFMC. TO and CONTACT_ID are required - TO field contains the email address of the recipient, and CONTACT_ID is a customer specific key which is mapped to the Subscriber Key in SFMC.
All other fields are optional and correspond to personalization fields (or merge tags) within SFMC email creative.

