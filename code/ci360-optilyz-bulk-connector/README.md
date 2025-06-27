# CI360 Optilyz Bulk Connector



# Optilyz Integration for SAS CI360

CI360 integration to Optilyz direct mail service using the CI360 Connector Framework. The Optilyz Bulk Connector supports sending customer audiences generated in CI360 to Optilyz campaigns. Audience attributes are used to personalize printed communications like letters, postcards, and flyers sent via Optilyz’s print fulfillment system.

## Connector Architecture

The connector uses an AWS Lambda function to handle webhook calls from CI360 and transfer audience data to Optilyz.

Lambda function utilizes the following AWS services:

- Lambda Environment Variables: Store configuration like API keys, endpoint URLs, and other Optilyz parameters
- API Gateway: Exposes the Lambda function as a secure HTTP endpoint; API Keys can be enabled for security

## Prerequisites

This integration is designed to run on AWS. An active AWS account is required. For large audiences, you may need to increase the storage for Lambda (default is 512MB, can go up to 10GB).

You also need:

- An active Optilyz account
- An API key from Optilyz
- Pre-created print templates or campaigns within your Optilyz account
- Familiarity with SAS CI360 configuration

## Installation

### AWS Deployment

To deploy the connector backend:

1. Create a Lambda function

2. Adjust Storage:
   - Lambda stores temporary files in `/tmp`; adjust storage if processing large audiences

3. Add Environment Variables:

   - `optilyz_base_url`: Optilyz API URL (e.g. https://api.optilyz.com/v2)
   - `optilyz_auth_url`: Optilyz Auth URL (e.g. https://www.optilyz.com/api/v1/authenticate)
   - `api_key`: Optilyz API key
   - `auth_key`: Authentication key

4. Create an API Gateway

5. Add POST method with integration to the Lambda function

6. Deploy the API and save the endpoint URL

### CI360 Setup

To connect CI360 with your AWS Lambda endpoint:

1. In CI360:
   - Navigate to General Settings → External Access → Connectors
   - Click New Connector
   - Click Custom Connector
   - Name it "Optilyz Connector"
   - Select Bulk as the connection type
   - Click Save and Add Webhook Now

2. Webhook Setup:
   - Create a new Webhook Endpoint
   - Name it "Optilyz Upload Customer List"
   - Use your deployed Lambda API Gateway URL
   - Set HTTP method to POST

3. Custom Task Type:
   - Create a new Custom Task Type using the webhook connection
   - Name it "Optilyz" or similar

   Define the following:

   - Send Parameters: Add a field for `automation_id` (given to you within each Optilyz campaign)

   - Outbound Data:
     - Required: Last Name, Address, Zip Code, City (matches env variables in Optilyz: `lastName`, `street`, `zipcode`, `city_`)
     - Note: City needs to be hardcoded in Lambda as it is a reserved keyword in CI360
     - Optional: First name, country, gender, etc., to be mapped in Optilyz

   - Connection: If needed, set connection to "Optilyz Connector"

## Configuration

The following environment variables are required in Lambda for correct operation:

| Variable             | Description                                                                 |
|----------------------|-----------------------------------------------------------------------------|
| `optilyz_base_url`   | Base URL for Optilyz API (e.g., https://api.optilyz.com/v1)                |
| `api_key`            | API key to authenticate with Optilyz                                       |
| `optilyz_auth_url`   | Optilyz Authentication URL (https://www.optilyz.com/api/v1/authenticate)   |
| `auth_key`           | Authentication Key                                                         |

## Using the Connector with Custom Task Types

Audience data is uploaded to Optilyz via batch API operations. The fields exported depend on the outbound data selected in the custom task type within CI360. Once sent, Optilyz will handle generation and delivery of printed communication based on the selected template.

### Sample Workflow

1. A marketing team builds a journey in CI360 with a split condition.
2. Customers entering the branch are sent to the "Optilyz" task.
3. The task sends audience data and template ID to the Lambda webhook.
4. The Lambda function formats and sends the batch to Optilyz via API.
5. Optilyz handles print and delivery automatically.

## References

- Optilyz Developer API Docs: https://www.optilyz.com/doc/api/
- SAS CI360 Guide – Create a Connector and Custom Task Type: https://communities.sas.com/t5/SAS-Customer-Intelligence/How-To-Pushing-customer-data-to-AWS-S3-using-CI360-Custom-Task/m-p/951835

For additional questions or support, contact the project lead or integration team.
