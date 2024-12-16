# CI360 Azure Blob Connector

## Overview

The **CI360 Azure Blob Connector** provides seamless integration between SAS Customer Intelligence 360 (CI360) and Microsoft Azure Blob Storage using the CI360 Bulk Connector Framework. This connector enables CI360-generated customer data files to be uploaded to a specified Azure Blob Storage container. The connector is designed to work with a CI360 Custom Task Type for automated data transfers to Azure Blob Storage, which can then be utilized for downstream processes or analytics.

## Connector Architecture

The connector uses an Azure Function to handle files staged by CI360. The Azure Function is triggered via a webhook call from CI360. The connector relies on the Azure Blob Storage connection string for authentication and data transfer.

### Azure Function Features

- **Environment Variables**: Configurable through the Azure portal or deployment pipeline for storing connection strings and other parameters.
- **Azure Blob Storage SDK**: Facilitates secure and efficient file uploads to Blob Storage.
- **Azure API Management (Optional)**: Provides an additional layer of security and control if required.

## Prerequisites

1. An Azure subscription with permissions to create and manage Blob Storage and Function Apps.
2. A Blob Storage account and a designated container for storing files.
3. CI360 Bulk Connector setup permissions.

## Installation

### Azure Deployment Steps

1. **Create an Azure Function App**:
   - Use the Azure Portal, CLI, or ARM templates to set up the Function App.
   - Choose the appropriate runtime stack (e.g., Python).

2. **Add Environment Variables**:
   - Navigate to the Function App settings and add the following variables:
     - `AZURE_STORAGE_CONNECTION_STRING`: The connection string for your Azure Blob Storage account.
     - `AZURE_BLOB_CONTAINER_NAME`: The name of the container where files will be uploaded.

3. **Deploy the Azure Function Code**:
   - Deploy the provided function code to the Function App using tools such as Azure CLI, VS Code, or GitHub Actions.
   - Ensure the function reads the connection string and container name from the environment variables.

4. **Set Up API Management (Optional)**:
   - If using API Management, expose the Azure Function endpoint as an API.
   - Configure security settings like API keys for authentication.

## Using the Connector

### Configuration

The Azure Function reads the following environment variables for operation:
- `AZURE_STORAGE_CONNECTION_STRING`: The connection string for the Azure Blob Storage account.
- `AZURE_BLOB_CONTAINER_NAME`: The name of the Blob Storage container where files will be uploaded.

### CI360 Setup

1. **Create a New Bulk Connector**:
   - Go to **General Settings -> External Access -> Connectors** in CI360.
   - Create a "New Connector" and select "Bulk" as the Connector Type.
   - Name the connector (e.g., "Azure Blob Connector").

2. **Set Up a Webhook Endpoint**:
   - Create a "New Webhook Endpoint".
   - Provide a descriptive name like "Blob Upload".
   - Set the **URL** to the Azure Function's HTTP trigger endpoint.
   - Use the **POST** HTTP method.
   - Add an `x-api-key` header if using API keys for authentication.

3. **Configure a Custom Task Type**:
   - Use the webhook endpoint as the connection for the Custom Task Type.
   - Name it appropriately (e.g., "Azure Blob").
   - Define the following **Send Parameters**:
     - **`blob_filename`**: Specifies the name of the file to be uploaded.

### Uploading Data with Custom Task Types

The CI360 platform uploads audience data to Azure Blob Storage based on the configured audience. File uploads include the columns specified in the Outbound Data section of the Delivery tab for the Custom Task Type. Users specify the file name in the Send Parameters tab.

