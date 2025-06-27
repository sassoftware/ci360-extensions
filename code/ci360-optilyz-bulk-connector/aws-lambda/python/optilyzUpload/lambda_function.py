"""
Copyright © 2024, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import json
import urllib3
import time
import csv

upload_file_prefix = os.environ['upload_file_prefix']
BASE_URL = os.environ['optilyz_api_url']
AUTH_URL = os.environ['optilyz_auth_url']
API_KEY = os.environ["optilyz_api_key"]

TEMP_DIR = "/tmp"
# get temp directory (for local execution of code, /tmp will exist in Lambda env)
TEMP_DIR = os.environ.get("TEMP", "tmp") if os.name == "nt" else "/tmp"
if not os.path.exists(TEMP_DIR):
    os.makedirs(TEMP_DIR)
if os.access(TEMP_DIR, os.W_OK):
    print(f"{TEMP_DIR} is writable")
else:
    print(f"{TEMP_DIR} is NOT writable")
    raise PermissionError(f"Cannot write to {TEMP_DIR}")    


TOKEN_CACHE = {"token": None, "expires_at": 0}

http = urllib3.PoolManager()

def lambda_handler(event, context):
    print("Received webhook data")
    event_body = json.loads(event["body"])

    try:
        presigned_urls = event_body["presignedUrls"]
        print("dataFile:", presigned_urls["dataFile"])
        print("metadataFile:", presigned_urls["metadataFile"])
        automation_id = event_body["sendParameters"]["automation_id"]
        
        ts_milli = round(time.time() * 1000)
        base_filename = upload_file_prefix + str(ts_milli)
        upload_filename = base_filename + '.csv'

        uploadfile = os.path.join(TEMP_DIR, base_filename)
        datafile = uploadfile + '_dat'
        metafile = uploadfile + '_md'
        metafile_csv = uploadfile + '_md.csv'

        print("Downloading output data files")
        
        print("downloading data file:", datafile)
        dl_http_code = download_file(presigned_urls['dataFile'], datafile)
        assert dl_http_code == 200, "Data file not available from CI360"
        print_file_contents(datafile, 500)

        print("downloading meta file:", metafile)
        dl_http_code = download_file(presigned_urls['metadataFile'], metafile)
        assert dl_http_code == 200, "Metadata file not available from CI360"
        print_file_contents(metafile)

        convert_meta_json_to_csv(metafile, metafile_csv)

        print('Assembling CSV file')
        filenames = [metafile_csv, datafile]
        concatenate_files(filenames, uploadfile)
        print('Final CSV file sample:')
        print_file_contents(uploadfile, 300)

        process_contact_file(uploadfile, automation_id)

        print("Removing temporary files")
        os.remove(datafile)
        os.remove(metafile)
        os.remove(metafile_csv)
        os.remove(uploadfile)

        response_body = { "status": "OK" }    
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
    except Exception as e:
        import traceback
        error_message = traceback.format_exc()
        print("Unknown error occurred:\n", error_message)
        http_resp_json = { 'status': 'Error', 'message': error_message }
        return { 'statusCode': 500, 'body': json.dumps(http_resp_json) }


def download_file(url, local_filename):
    print(f"Downloading from: {url}")

    with http.request('GET', url, preload_content=False, decode_content=False) as response:
        print(f"HTTP Response Status: {response.status}")
        if response.status == 200:
            with open(local_filename, 'wb') as file:
                for chunk in response.stream(8192):
                    #print(f"Writing chunk of size {len(chunk)} bytes")
                    file.write(chunk)
            print("Download complete. File saved as", local_filename)
            if os.path.exists(local_filename):
                    print(f"File successfully downloaded: {local_filename}")
            else:
                print(f"File does not exist after download: {local_filename}")
                raise FileNotFoundError(f"File not found: {local_filename}")
        else:
            print("ERROR: Unable to download file. Status Code:", response.status)
        return response.status   


def concatenate_files(filenames, output_file):
    with open(output_file, 'w') as outfile:
        for fname in filenames:
            with open(fname) as infile:
                for line in infile:
                    outfile.write(line)


def convert_meta_json_to_csv(input_file, output_file):
    with open(input_file, 'r') as f:
        json_array = json.load(f)
    
    column_names = [item['columnName'] for item in json_array]
    
    csv_string = ','.join(column_names) + '\n'
    
    with open(output_file, 'w') as f:
        f.write(csv_string)


def process_contact_file(csv_file_path, automation_id):
    api_url = BASE_URL + "/automations/" + automation_id + "/recipients"
    with open(csv_file_path, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)

        contact_list = []
        for row in csv_reader:
            address_fields = {}
            for key, value in row.items():
                if key == "city_":
                    address_fields["city"] = value
                elif key == "country_":
                    address_fields["country"] = value
                elif key != "identity_id":
                    address_fields[key] = value

            contact_row = { "address": address_fields, "variation": 1 }
            contact_list.append(contact_row)

    payload = {"addresses": contact_list}
    print(payload)

    print("Sending data to Optilyz")
    result = send_post_request(api_url, payload)
    print("Completed Optilyz calls")
    print(f"Status: {result['status']}")
    print(f"Response data: {result['data']}")


def send_post_request(url, json_body):
    """
    Sends an HTTP POST request with a Bearer token and a JSON body.

    Returns:
        An object containing the status code and response data.
    """

    token = get_auth_token() 
    print("Got token calling API")

    encoded_body = json.dumps(json_body).encode('utf-8')
    
    headers = urllib3.make_headers(basic_auth = token+':')
    headers["Content-Type"] = "application/json"

    print(f"Request URL: {url}")
    print(f"Headers: {headers}")

    response = http.request("POST", url, body=encoded_body, headers=headers)
    
    print(f"Response Code: {response.status}")
    print(f"Response Data: {response.data.decode('utf-8')}")

    if response.status == 401: 
        print("Token expired, fetching a new one...")
        token = get_auth_token() 
        headers["Authorization"] = f"Bearer {token}"
        response = http.request("POST", url, body=encoded_body, headers=headers)
        print(f"Retry Response Code: {response.status}")

    return {
        "status": response.status,
        "data": response.data.decode("utf-8")
    }


def get_auth_token():
    """
    Retrieves a fresh authentication token from Optilyz.
    Uses API Key in a POST request to get a temporary token that expires in 1 hour.
    """
    global TOKEN_CACHE

    if time.time() < TOKEN_CACHE["expires_at"]:
        print(f"Using cached token (expires in {TOKEN_CACHE['expires_at'] - time.time()} seconds)")
        return TOKEN_CACHE["token"]

    print("Fetching new token...")
    headers = {"Content-Type": "application/json"}
    body = json.dumps({"key": API_KEY})

    response = http.request("POST", AUTH_URL, body=body, headers=headers)

    print(f"Auth Response Code: {response.status}")
    print(f"Auth Response Data: {response.data.decode('utf-8')}")  
    print(f" Response Headers: {response.headers}")  

    if response.status == 200:
        token_data = json.loads(response.data.decode("utf-8"))
        TOKEN_CACHE["token"] = token_data["token"]  
        TOKEN_CACHE["expires_at"] = time.time() + 3600 - 60  
        print(f"New token obtained, expires in {3600 - 60} seconds")

        return TOKEN_CACHE["token"]
    else:
        raise Exception(f"Failed to fetch token: {response.status} - {response.data.decode('utf-8')}")


def print_file_contents(filename, maxsize=-1):
    file_stats = os.stat(filename)
    print("File", filename, "(", file_stats.st_size, " bytes ):")
    with open(filename, 'r') as f:
        print(f.read(maxsize))
