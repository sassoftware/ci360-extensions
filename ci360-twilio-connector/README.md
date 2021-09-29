# CI360 Twilio Connector

## Overview

CI360 integration to Twilio using CI360 Connector Framework. CI360 Twilio Connector supports outbound SMS messages using Twilio APIs. Only one-way (outbound) SMS is supported by this connector at the moment.

## Connector Architecture

Connector uses AWS Lambda function to support Twilio SMS features. Currently, only one-way "fire and forget" SMS messaging is supported.

Lambda function uses the following AWS features:
- Lambda function Environment Variables: for basic configuration that is applicable across tenants (multiple tenants are supported by a single Lambda in order to serve multi-tenant environments)
- IAM: definition of role under which Lambda functions are being executed, and which gives access to other AWS components (like Secrets Manager)
- Secrets Manager: stores API keys and endpoint URLs for Twilio API and CI360 API gateway (secrets are one pre tenant)
- API Gateway: exposes Lambda function as API endpoint, also secures it using API Keys

## Prerequisites

This connector has been developed for AWS platform. Account needs to be set up for the AWS platform.

## Installation

### AWS Deployment

Steps required to install connector functions to AWS:
- Create Lambda function for outbound (twilioMessageRequest)
- Add Environment Variables for the function
    - event_ttl, default_sender, sm_secret_id_prefix
- Create role in IAM (twilioConnectorLamdba-role)
    - Grant AWSOpsWorksCloudWatchLogs, SecretsManagerReadWrite permission policies
- Associate role with the Lamdba function
- Create SecretsManager secret(s)
    - one secret per tenant, with ID demo/TwilioConnector/APIs/tenant_id
    - prefix is demo/TwilioConnector/APIs/ and set in environment variable sm_secret_id_prefix
    - secret store should contain the following keys: twilio_api_url, twilio_account_sid, twilio_auth_token
- Create API gateway (twilioApi)
    - Add resources to API gateway
    - Configure POST methods, proxy lambda, enable CORS if desired
    - Configure API Keys and Plans and associate with resource/stage (for authentication)

## Using the Connector

### Configuration

The following environment variables are used to configure connector behavior for outbound function:
-	event_ttl: age, in seconds, after which is incoming event from CI360 considered stale and will not be processed (timestamp in event payload is compared with current time)
-   default_sender: phone number, in internation format, to be used as sender (From number) if sender is not specified in the request
-	sm_secret_id_prefix: prefix used to construct Secret Name when retrieving secret from Secret Manager (prefix will be appended with tenant ID to get the full name of the store)


### Register your connector in CI360

In order to use the connector, you need to register the connector and endpoint with these details into the CI360 system. Documentation sections are referenced below for eacy access.

**Add and Register a Connector**
Please refer to [`Add and Register a Connector`](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/ext-connectors-add.htm) in SAS Customer Intelligence 360 admin guide.

**Add an Endpoint**
Please refer to [`Add an Endpoint`](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/ext-connectors-add-endpoint.htm) in SAS Customer Intelligence 360 admin guide.

### CI360 Setup

Steps to set up new connector for Twilio:
-   In CI360, go to General Settings -> External Access -> Connectors
-   Create “New Connector”
-   Name it “Twilio Connector” 
    -   This can be anything but should be logical, as it will be used every time new External System Task for message delivery is created
-   Create “New Endpoint”
-   Name it “Twilio SMS API”
    -   URL is your deployed Lambda function (behind API Gateway in AWS)
    -   Method is POST
    -   Add x-api-key header if API Keys are configured

### Using Connector with External System Task

Messages are sent to customers using External System Task. External System Task can be triggered individually (by configuring Trigger event on the Orchestration tab), or as part of an Activity Map. When you are configuring a new External System Task, you will need to associate it with Twilio SMS on the Orchestration tab of the task. You will also need to use a Plain Text creative, which will contain all the personalization variables that will be sent to Twilio for execution.

This is a sample structure of the Plain Text creative that needs to be used with External System Task:
```
TO:{{phone}};
FROM:+19085551212;
MEDIAL_URLS:https://www.sas.com/images/logo.jpg;
{{firstname}}, thank you for registering for our seminar!
```

Last line of the creative contains the actual (and personalized) text message to be sent. TO field contains the email address of the recipient. FROM field is optional - FROM will replace a default sender channel from configuration if provided. MEDIA_URLS is only required if media (such as an image) is attached to the message, applicable to MMS.


