"""
Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import azure.functions as func
import logging
import json
import requests
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os



# Read configuration from environment variables
AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
AZURE_BLOB_CONTAINER_NAME = os.getenv('AZURE_BLOB_CONTAINER_NAME')

# Validate the environment variables
if not AZURE_STORAGE_CONNECTION_STRING:
    raise ValueError("Environment variable 'AZURE_STORAGE_CONNECTION_STRING' is not set.")
if not AZURE_BLOB_CONTAINER_NAME:
    raise ValueError("Environment variable 'AZURE_BLOB_CONTAINER_NAME' is not set.")

# Set up the connection to Azure Blob Storage
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_name = AZURE_BLOB_CONTAINER_NAME

print(f"Successfully connected to Azure Blob Storage container: {container_name}")


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="bulkconnectorv1")
def bulkconnectorv1(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    req_body = req.get_json()
    temp_s3_url = req_body['presignedUrls']['dataFile']
    #Convert the request body to a string for printing
    req_body_str = json.dumps(req_body, indent=4)

    
    try:
        response = requests.get(temp_s3_url)
        response.raise_for_status()
        
        # Save the file to Azure Blob Storage
        blob_name = req_body['sendParameters']['file_name']  
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        
        # Upload content to the blob
        blob_client.upload_blob(response.content, overwrite=True)
        
        return func.HttpResponse(f"File downloaded from S3 and uploaded to Azure Blob Storage successfully.", status_code=200)
    
    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading file: {e}")
        return func.HttpResponse(f"Failed to download file from S3: {str(e)}", status_code=400)

    except Exception as e:
        logging.error(f"Error uploading file to Blob Storage: {e}")
        return func.HttpResponse(f"Failed to upload file to Blob Storage: {str(e)}", status_code=500)