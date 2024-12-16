# CI360 Match Bulk Connector

## Overview

CI360 integration to CI360 Match using CI360 Connector Framework. Match Bulk Connector supports sending audiences generated in CI360 from various sources to CI360 Match. Attributes of an audience are set as Match tags. This is a bulk equivalent of Match SetSV function and uses Match bulk user registration API.

## Connector Architecture

Connector uses AWS Lambda function to support accept CI360 bulk connector webhook call and transfer data to Match.

Lambda function uses the following AWS features:
- Lambda function Environment Variables: for basic configuration, such as storing Match API URL and filename prefix
- API Gateway: exposes Lambda function as API endpoint, also secures it using API Keys
- Python code for Lambda function was written with expectation that function is integrated with API Gateway using Lambda proxy integration (if not using proxy integration, slight change to code will be required)

## Prerequisites

This connector has been developed for AWS platform. Account needs to be set up for the AWS platform.

## Installation

### AWS Deployment

Steps required to install connector functions to AWS:
- Create Lambda function (matchBulkUpload)
- Add Environment Variables for the function
    - match_base_url, match_auth_key, upload_file_prefix, match_id_field_name
- Create API gateway (matchBulkUploadApi)
    - Add resources to API gateway
    - Configure POST methods, proxy lambda integration
    - Configure API Keys and Plans and associate with resource/stage (for authentication)

## Using the Connector

### Configuration

The following environment variables are used to configure connector behavior:
-	match_base_url: Base URL for Match API instance (protocol and hostname only, e.g. "https://demo.aimatch.com")
-   match_auth_key: API key
-	upload_file_prefix: prefix used for generated upload file names (e.g. "userreg-")
-   match_id_field_name: name used for Match ID for the particular tenant (e.g. CUSTOMERID or MID)


### Register your connector in CI360

In order to use the connector, you need to register the bulk connector and webhook endpoint with these details into the CI360 system. Bulk connector will be used by a Custom Task Type that also needs to be created to allow users to leverage this function and activate their audiences through Match. Documentation sections are referenced below for easy access.

**Create a Connector**
Please refer to [`Create a Connector`](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/custom-task-create-connector.htm) in SAS Customer Intelligence 360 admin guide.

**Create a Custom Task Type**
Please refer to [`Create a Custom Task Type`](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/custom-tasks-create-triggered.htm) in SAS Customer Intelligence 360 admin guide.

### CI360 Setup

Steps to set up new connector for Match:
-   In CI360, go to General Settings -> External Access -> Connectors
-   Create “New Connector”
-   Name it "Match Connector” 
-   Select "Bulk" as Connection Type
-   Create “New Webhook Endpoint”
-   Name it “Match Bulk Upload Webhook”
    -   URL is your deployed Lambda function (behind API Gateway in AWS)
    -   Method is POST
    -   Add x-api-key header if API Keys are configured

After the connector has been created and configured, create a new Custom Task Type using the webhook endpoint as connection. You can name the task type "Match", "CI360 Match" or something similar and appropriate.

### Using Connector with Custom Task Types

Customer data is uploaded to Match based on audience configured in CI360 (sourced from on-prem data source, uploaded cloud data or cloud audience). Columns uploaded to Match will depend on data fields selected in Outbound Data section of Delivery tab in new Custom Task Type. When configuring bulk Custom Task Type, for this implementation, Send Parameters are not needed, and there should be no required Outbound Data elements, users will be able to add individual data elements to Delivery tab to match the individual scenarios.
