# CI360 MailChimp Bulk Connector

## Overview

CI360 integration to MailChimp email service using CI360 Connector Framework. MailChimp Bulk Connector supports sending audiences of customers generated in CI360 from various sources to MailChimp lists. Attributes of an audience are set as MailChimp customer tags, which can be used as personalization fields in emails.

## Connector Architecture

Connector uses AWS Lambda function to support accept CI360 bulk connector webhook call and transfer data to MailChimp.

Lambda function uses the following AWS features:
- Lambda function Environment Variables: for basic configuration, such as storing MailChimp API URL and other parameters
- API Gateway: exposes Lambda function as API endpoint, also secures it using API Keys
- Python code for Lambda function was written with expectation that function is integrated with API Gateway using Lambda proxy integration (if not using proxy integration, slight change to code will be required)

## Prerequisites

This connector has been developed for AWS platform. Account needs to be set up for the AWS platform. Lambda function ephemeral storage size may need to be adjusted to accomodate large uploads, although default 512MB should be sufficient for most uses. For more inforamtion, see [`Configure ephemeral storage for Lambda functions`](https://docs.aws.amazon.com/lambda/latest/dg/configuration-ephemeral-storage.html)

MailChimp account is required with an API key created for the account. For more information about MailChimp API keys, see [`About API Keys`](https://mailchimp.com/help/about-api-keys/). For more information about MailChimp APIs used for this integration, see [`Batch Operations`](https://mailchimp.com/developer/marketing/api/batch-operations/start-batch-operation/) and [`List/Audiences Batch Subscribe`](https://mailchimp.com/developer/marketing/api/lists/batch-subscribe-or-unsubscribe/)

## Installation

### AWS Deployment

Steps required to install connector functions to AWS:
- Create Lambda function (mailchimpListUpload)
- Adjust ephemeral storage for Lambda function as needed for your use case: needed for temporary file storage, Lambda ephemeral storage is mounted as /tmp and can be configured for 512MB to 10GB
- Add Environment Variables for the function
    - MC_BASE_URL, AUTH_KEY, EMAIL_FIELD_NAME, CONTACTS_PER_OPERATION, OPERATION_BATCH_SIZE
- Create API gateway (mailchimpApi)
    - Add resources to API gateway
    - Configure POST methods, proxy lambda integration
    - Configure API Keys and Plans and associate with resource/stage (for authentication)

## Using the Connector

### Configuration

The following environment variables are used to configure connector behavior:
-	MC_BASE_URL: Base URL for MailChimp API instance (protocol and hostname only, e.g. "https://us16.api.mailchimp.com/3.0")
-   AUTH_KEY: MailChimp API key
-	EMAIL_FIELD_NAME: Field name containing email address in CI360 output (this will be configured as required output attribute for the custom task type)
-   CONTACTS_PER_OPERATION: Number of contacts to be uploaded as part of single operation (connector uses batch subscribe API operation POST /lists/{list_id}), maximum 500
-   OPERATION_BATCH_SIZE: Number of operations to be submitted in a single API call (connector users POST /batches), total number of contacts uploaded in a single API call will be CONTACTS_PER_OPERATION x OPERATION_BATCH_SIZE

### Register your connector in CI360

In order to use the connector, you need to register the bulk connector and webhook endpoint with these details into the CI360 system. Bulk connector will be used by a Custom Task Type that also needs to be created to allow users to leverage this function and activate their audiences through MailChimp email platform. Documentation sections are referenced below for easy access.

**Create a Connector**
Please refer to [`Create a Connector`](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/custom-task-create-connector.htm) in SAS Customer Intelligence 360 admin guide.

**Create a Custom Task Type**
Please refer to [`Create a Custom Task Type`](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/custom-tasks-create-triggered.htm) in SAS Customer Intelligence 360 admin guide.

### CI360 Setup

Steps to set up new connector for MailChimp:
-   In CI360, go to General Settings -> External Access -> Connectors
-   Create “New Connector”
-   Name it MailChimp Connector” 
-   Select "Bulk" as Connection Type
-   Create “New Webhook Endpoint”
-   Name it “MailChimp List Upload Webhook”
    -   URL is your deployed Lambda function (behind API Gateway in AWS)
    -   Method is POST
    -   Add x-api-key header if API Keys are configured

After the connector has been created and configured, create a new Custom Task Type using the webhook endpoint as connection. You can name the task type "MailChimp", "MailChimp List" or something similar and appropriate.

MailChimp custom task type needs two required attributes to operate correctly with this connector:
- Send Parameters: configure a "Customer List" attribute, with attribute name being "list_id" (display name can be adjusted, but connector code expects list_id), and set it up as dropdown list containing various audiences/lists created in MailChimp
- Outbound Data: one required outbound data element, containing customer email address, with configured attribute name matching EMAIL_FIELD_NAME configuration parameter for the connector Lambda function
- Additional outbound data elements can be added if needed, or users can simply be allowed to add them in the task

### Using Connector with Custom Task Types

Customer data is uploaded to MailChimp based on audience configured in CI360 (sourced from on-prem data source, uploaded cloud data or cloud audience). Columns uploaded to MailChimp will depend on data fields selected in Outbound Data section of Delivery tab in new Custom Task Type.

