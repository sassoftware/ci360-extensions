"""
Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import json
import urllib3
import time
import csv
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from boto3.exceptions import S3UploadFailedError

s3_bucket_name = os.environ["braze_s3_bucket_name"]
upload_file_prefix = os.environ['upload_file_prefix']
id_column = os.environ["id_column"]

http = urllib3.PoolManager()
s3 = boto3.client('s3')

def lambda_handler(event, context):
    print("Received webhook data")
    event_body = json.loads(event["body"])

    try:
        presigned_urls = event_body["presignedUrls"]
        print("dataFile:", presigned_urls["dataFile"])
        print("metadataFile:", presigned_urls["metadataFile"])
        
        # generate filenames
        ts_milli = round(time.time() * 1000)
        base_filename = upload_file_prefix + str(ts_milli)
        upload_filename = base_filename + '.csv'
        uploadfile = '/tmp/' + base_filename
        datafile = uploadfile + '_dat'
        metafile = uploadfile + '_md'
        
        # download files
        print("Downloading output data files")
        
        print("downloading data file:", datafile)
        dl_http_code = download_file(presigned_urls['dataFile'], datafile)
        assert dl_http_code == 200, "Data file not available from CI360"
        print_file_contents(datafile, 500)

        print("downloading meta file:", metafile)
        dl_http_code = download_file(presigned_urls['metadataFile'], metafile)
        assert dl_http_code == 200, "Metadata file not available from CI360"
        print_file_contents(metafile)

        print("formatting braze file:", uploadfile)
        format_file_for_braze(metafile, datafile, uploadfile)

        print_file_contents(uploadfile, 500)

        print("Uploading file to S3 bucket", s3_bucket_name)
        s3_url = upload_to_s3(uploadfile, upload_filename)

        # clean up storage
        print("Removing temporary files")
        os.remove(datafile)
        os.remove(metafile)
        os.remove(uploadfile)

        #response_body = { "status": "OK", "upload_filename": upload_filename, "bucket_name": s3_bucket_name, "s3_url": s3_url }    
        response_body = { "status": "OK", "upload_filename": upload_filename, "bucket_name": s3_bucket_name }    
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
    except S3UploadFailedError as error:
        print("Failed to upload file:", str(error))
        http_resp_json = { 'status': 'Error', 'message': 'Failed to upload file (S3 upload failed): ' + str(error) }
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
    

def format_file_for_braze(metafile, datafile, uploadfile):
    # process metadata (header) file: 
    # we need to remove identity_id column
    # and also make sure external_id is the first column
    print("processing meta file:", metafile)
    column_index = -1
    with open(metafile, 'r') as mf:
        #metastr = mf.read()
        mf_reader = csv.reader(mf)
        header_row = next(mf_reader)
        assert id_column in header_row, "ID column not found in CSV file"
        # Get the index of the column to move
        column_index = header_row.index(id_column)
        print(id_column, "at index", column_index)

    # Create the new header order (id_column to front, skip first column)
    new_header_row = [header_row[column_index]] + header_row[1:column_index] + header_row[column_index+1:]

    # process file = transform as needed:
    # remove first column (identity_id)
    # and move external_id column to be the first
    print("processing data file:", datafile)
    with open(datafile, mode='r', newline='') as in_file, open(uploadfile, mode='w', newline='') as out_file:
        writer = csv.writer(out_file)
        # first write header row
        print("writing header row")
        writer.writerow(new_header_row)
        # now process data file, read in row by row, swap columns and write
        csv_reader = csv.reader(in_file, delimiter=',')
        print("writing data rows")
        line_count = 0
        for row in csv_reader:
            #row.pop(0)
            new_row = [row[column_index]] + row[1:column_index] + row[column_index+1:]
            writer.writerow(new_row)
            line_count += 1
        print(f"Processed {line_count} lines.")


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
