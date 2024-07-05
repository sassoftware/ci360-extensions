# CI360 Braze User Import Bulk Connector

## Overview

CI360 integration to Braze using CI360 Connector Framework. Braze User Import Bulk Connector supports sending audiences generated in CI360 from various sources to Braze. Attributes of an audience are set as user attributes. Connector will process CI360 output files to comply with Braze specifications, then upload to the configured S3 bucket. Once CSV file is in the target S3 bucket, it will be picked up with second Lambda, publiched by Braze, which will process the CSV file and post user attributes from file to Braze API using User Track endpoint.

## Connector Architecture

Connector uses AWS Lambda function to support accept CI360 bulk connector webhook call and transfer data to Braze.

Lambda function uses the following AWS features:
- Lambda function Environment Variables: for basic configuration, such as storing Braze S3 bucket and filename prefix
- API Gateway: exposes Lambda function as API endpoint, also secures it using API Keys
- Python code for Lambda function was written with expectation that function is integrated with API Gateway using Lambda proxy integration (if not using proxy integration, slight change to code will be required)

## Prerequisites

This connector has been developed for AWS platform. Account needs to be set up for the AWS platform. Additionally, Braze account is required, and Braze User CSV Import Lambda needs to be deployed. S3 bucket created by Braze Lambda needs to be configured as environment variable for this connector. 

Braze Lambda application can be deployed directly from AWS Serverless Application Directory: [`braze-user-attribute-import`](https://console.aws.amazon.com/lambda/home?region=us-east-1#/create/app?applicationId=arn:aws:serverlessrepo:us-east-1:585170621372:applications/braze-user-attribute-import)

For more infomation on Braze User CSV Import Lambda, see: [`User attribute CSV to Braze import`](https://www.braze.com/docs/user_csv_lambda) and https://github.com/braze-inc/growth-shares-lambda-user-csv-import


## Installation

### AWS Deployment

Steps required to install connector functions to AWS:
- Create Lambda function (brazeUserImport)
- Add Environment Variables for the function
    - braze_s3_bucket_name, upload_file_prefix, id_column
- Modify the role assigned to Lambda function and add AmazonS3FullAccess policy
- Adjust Lambda general configuration settings:
    - Increase timeout (suggested 10 minutes)
    - Increase ephemeral storage to accomodate output file sizes (default/minimum is 512MB, suggested 2-5GB)
    - Increase memory (Lambda uses minimal memory, but additional allocated memory also increses CPU allocation, suggested 1024MB)
- Create new API gateway using REST API type (brazeUserImportApi)
    - Add resource to API gateway (e.g. brazeUserApi)
    - Configure POST method for new resource, use Lambda proxy integration
    - Configure API Keys and Plans and associate with resource/stage (for authentication)

## Using the Connector

### Configuration

The following environment variables are used to configure connector behavior:
-	braze_s3_bucket_name: Name of S3 bucket created by Braze Lambda
-	upload_file_prefix: prefix used for generated upload file names (e.g. "userfile-")
-	id_column: name of column in CSV file that contains the user ID (e.g. "external_id")


### Register your connector in CI360

In order to use the connector, you need to register the bulk connector and webhook endpoint with these details into the CI360 system. Bulk connector will be used by a Custom Task Type that also needs to be created to allow users to leverage this function and activate their audiences through Braze. Documentation sections are referenced below for easy access.

**Create a Connector**
Please refer to [`Create a Connector`](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/custom-task-create-connector.htm) in SAS Customer Intelligence 360 admin guide.

**Create a Custom Task Type**
Please refer to [`Create a Custom Task Type`](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/custom-tasks-create-triggered.htm) in SAS Customer Intelligence 360 admin guide.

### CI360 Setup

Steps to set up new connector for Braze:
-   In CI360, go to General Settings -> External Access -> Connectors
-   Create “New Connector”
-   Name it Braze Connector” 
-   Select "Bulk" as Connection Type
-   Create “New Webhook Endpoint”
-   Name it Braze User Import Webhook”
    -   URL is your deployed Lambda function (behind API Gateway in AWS)
    -   Method is POST
    -   Add x-api-key header if API Keys are configured

After the connector has been created and configured, create a new Custom Task Type using the webhook endpoint as connection. You can name the task type "Braze", "Braze User Import" or something similar and appropriate.

### Using Connector with Custom Task Types

Customer data is uploaded to Braze based on audience configured in CI360 (sourced from on-prem data source, uploaded cloud data or cloud audience). Columns uploaded to Braze will depend on data fields selected in Outbound Data section of Delivery tab in new Custom Task Type.

When configuring new Custom Task Type, the following should be set:
- only one required Outbound Data attribute, with attribute name matching the value set for "id_column" Lambda environment variable (e.g. "external_id")
- no other predefined attributes should be set
- make sure to allow users to add additional data attributes in their tasks
- no Send Parameters are necessary
