"""
Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import json
import urllib3
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

s3_bucket_name = os.environ["s3_bucket_name"]

http = urllib3.PoolManager()
s3 = boto3.client('s3')

def lambda_handler(event, context):
    print("Received webhook data")
    event_body = json.loads(event["body"])

    try:
        presigned_urls = event_body["presignedUrls"]
        print("dataFile:", presigned_urls["dataFile"])
        print("metadataFile:", presigned_urls["metadataFile"])
        
        # get filenames
        upload_filename = event_body["sendParameters"]["s3_filename"]
        local_tmp_file = "/tmp/" + upload_filename
        assert upload_filename, "S3 filename cannot be blank"
        
        # download files
        print("Downloading output data files")   
        
        print("downloading data file:", local_tmp_file)
        dl_http_code = download_file(presigned_urls['dataFile'], local_tmp_file)
        #print_file_contents(local_tmp_file, 500)
        assert dl_http_code == 200, "File not available from CI360"

        print("Uploading file to S3 bucket", s3_bucket_name)
        s3_url = upload_to_s3(local_tmp_file, upload_filename)

        response_body = { "status": "OK", "upload_filename": upload_filename, "bucket_name": s3_bucket_name, "s3_url": s3_url }    
        return {
            "statusCode": 200,
            "body": json.dumps(response_body)
        }
    except AssertionError as error:
        print("Assertion error:", str(error))
        http_resp_json = { 'status': 'Error', 'message': str(error) }
        return { 'statusCode': 500, 'body': json.dumps(http_resp_json) }
    except KeyError as error:
        print("Missing configuration key:", str(error))
        http_resp_json = { 'status': 'BadRequest', 'message': 'Missing key ' + str(error) }
        return { 'statusCode': 400, 'body': json.dumps(http_resp_json) }
    except ClientError as error:
        print("Failed to upload file:", str(error))
        http_resp_json = { 'status': 'Error', 'message': 'Failed to upload file (invalid AWS configuration): ' + str(error) }
        return { 'statusCode': 500, 'body': json.dumps(http_resp_json) }
    except urllib3.exceptions.RequestError as error:
        print("Failed to upload file:", str(error))
        http_resp_json = { 'status': 'Error', 'message': 'Failed to upload file (AWS error): ' + str(error) }
        return { 'statusCode': 500, 'body': json.dumps(http_resp_json) }
    except NoCredentialsError as error:
        print("Failed to upload file:", str(error))
        http_resp_json = { 'status': 'Error', 'message': 'Failed to upload file (invalid AWS credentials): ' + str(error) }
        return { 'statusCode': 500, 'body': json.dumps(http_resp_json) }
    except:
        print("Unknown error occured")
        http_resp_json = { 'status': 'Error', 'message': 'Unknown error occured' }
        return { 'statusCode': 500, 'body': json.dumps(http_resp_json) }


def download_file(url, local_filename):
    with http.request('GET', url, preload_content=False, decode_content=False) as response:
        if response.status == 200:
            with open(local_filename, 'wb') as file:
                for chunk in response.stream(8192):
                    file.write(chunk)
            print("Download complete. File saved as", local_filename)
        else:
            print("ERROR: Unable to download file. Status Code:", response.status)
        return response.status
    


def upload_to_s3(local_file, s3_file):
    try:
        s3.upload_file(local_file, s3_bucket_name, s3_file)
        url = s3.generate_presigned_url(ClientMethod='get_object', Params={'Bucket': s3_bucket_name, 'Key': s3_file}, ExpiresIn=24 * 3600)

        print("Upload Successful:", url)
        return url
    except FileNotFoundError:
        print("ERROR: The file was not found")
        return None

    
def print_file_contents(filename, maxsize=-1):
    file_stats = os.stat(filename)
    print("File", filename, "(", file_stats.st_size, " bytes ):")
    with open(filename, 'r') as f:
        print(f.read(maxsize))
