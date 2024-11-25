"""
Copyright © 2024, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import json
import csv
import time
import urllib3

# Initialize HTTP connection pool
http = urllib3.PoolManager()

# Configuration
BASE_URL = os.environ['MC_BASE_URL']
TEMP_PATH = '/tmp/'
CSV_FILE_PREFIX = 'memb_'
CONTACTS_PER_OP = int(os.environ['CONTACTS_PER_OPERATION'])    # this can be up to 500 contacts (contacts per single operation)
OP_BATCH_SIZE = int(os.environ['OPERATION_BATCH_SIZE'])        # number of operations in a batch
AUTH_TOKEN = os.environ['AUTH_TOKEN']
EMAIL_FIELD = os.environ['EMAIL_FIELD_NAME']
STANDARD_FIELDS = [EMAIL_FIELD, 'identity_id']

print('Configuration: contacts per op:', CONTACTS_PER_OP, ', batch size:', OP_BATCH_SIZE)

def lambda_handler(event, context):
    print("Received webhook data")
    event_body = json.loads(event["body"])

    try:
        # process input data from webhook
        presigned_urls = event_body["presignedUrls"]
        print("dataFile:", presigned_urls['dataFile'])
        print("metadataFile:", presigned_urls['metadataFile'])
        send_parameters = event_body["sendParameters"]
        print("sendParameters:", event_body["sendParameters"])

        # generate filenames
        ts_milli = round(time.time() * 1000)
        csv_filename = CSV_FILE_PREFIX + str(ts_milli)
        csv_file_path = TEMP_PATH + csv_filename
        datafile = csv_file_path + '_dat'
        metafile = csv_file_path + '_md'
        metafile_csv = csv_file_path + '_md.csv'

        # download files from CI360
        print("Downloading output data files from CI360")

        print("downloading meta file:", metafile)
        download_file(presigned_urls['metadataFile'], metafile)
        print_file_contents(metafile)

        print("downloading data file:", datafile)
        download_file(presigned_urls['dataFile'], datafile)
        print_file_contents(datafile, 300)

        # convert JSON meta to CSV header
        convert_meta_json_to_csv(metafile, metafile_csv)

        # concatenate header and data into one file to be uploaded
        print('Assembling CSV file')
        filenames = [metafile_csv, datafile]
        concatenate_files(filenames, csv_file_path)
        print('Final CSV file sample:')
        print_file_contents(csv_file_path, 300)

        # check if CSV file exists, in case there were issues with download from CI360
        if not os.path.exists(csv_file_path):
            print(f"{csv_file_path} does not exist.")
            http_resp_json = { 'status': 'Error', 'message': 'Error downloading or processing output files' }
            return { 'statusCode': 500, 'body': json.dumps(http_resp_json) }

        batch_ids = process_contact_file(csv_file_path, send_parameters['list_id'])
        print('batches:', batch_ids)
        response_body = { 'status': 'OK', 'batches': batch_ids }
        return {
            'statusCode': 200,
            'body': json.dumps(response_body)
        }
    except AssertionError as error:
        print("Assertion error:", str(error))
        http_resp_json = { 'status': 'Error', 'message': str(error) }
        return { 'statusCode': 500, 'body': json.dumps(http_resp_json) }
    except KeyError as error:
        print("Missing configuration key:", str(error))
        http_resp_json = { 'status': 'BadRequest', 'message': 'Missing key ' + str(error) }
        return { 'statusCode': 400, 'body': json.dumps(http_resp_json) }
    except urllib3.exceptions.RequestError as error:
        print("Failed to upload file:", str(error))
        http_resp_json = { 'status': 'Error', 'message': 'Failed to upload file (AWS error): ' + str(error) }
        return { 'statusCode': 500, 'body': json.dumps(http_resp_json) }
    except:
        print("Unknown error occured")
        http_resp_json = { 'status': 'Error', 'message': 'Unknown error occured' }
        return { 'statusCode': 500, 'body': json.dumps(http_resp_json) }


def print_file_contents(filename, maxsize=-1):
    file_stats = os.stat(filename)
    print("File", filename, "(", file_stats.st_size, "bytes ):")
    with open(filename, 'r') as f:
        print(f.read(maxsize))


"""
Download content from URL and write to local_filename file
"""
def download_file(url, local_filename):
    try:
        with http.request('GET', url, preload_content=False, decode_content=False) as response:
            if response.status == 200:
                with open(local_filename, 'wb') as file:
                    for chunk in response.stream(8192):
                        file.write(chunk)
                print(f"Download complete. File saved as {local_filename}")
            else:
                print(f"Error: Unable to download file. Status Code: {response.status}")

    except urllib3.exceptions.RequestError as e:
        print(f"Network Error: {e}")
    except Exception as e:
        print(f"Error: {e}")


"""
Concatenate files contained in filenames list into a single output_file
"""
def concatenate_files(filenames, output_file):
    with open(output_file, 'w') as outfile:
        for fname in filenames:
            with open(fname) as infile:
                for line in infile:
                    outfile.write(line)


"""
Read a JSON array of strings from an input file,
convert it to a comma-delimited string, and write to an output file.

:param input_file: Path to the input JSON file
:param output_file: Path to the output CSV file
"""
def convert_meta_json_to_csv(input_file, output_file):
    with open(input_file, 'r') as f:
        json_array = json.load(f)
    
    # Extract only the columnName values
    column_names = [item['columnName'] for item in json_array]
    
    # Convert the column names to a comma-delimited string
    csv_string = ','.join(column_names) + '\n'
    
    # Write the comma-delimited string to the output file
    with open(output_file, 'w') as f:
        f.write(csv_string)



"""
Sends an HTTP POST request with a Bearer token and a JSON body.

Returns:
    An object containing the status code and response data.
"""
def send_post_request(url, token, json_body):
    encoded_body = json.dumps(json_body)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    response = http.request("POST", url, body=encoded_body, headers=headers)

    return {
        "status": response.status,
        "data": response.data.decode("utf-8")
    }


"""
Adds contact list members to an operation for a specific list_id
"""
def add_members_to_operation_batch(contact_list, operations, list_id):
    contact_list_len = len(contact_list)
    print(f"Adding operation with {contact_list_len} records")
    # Members add payload, update sync_tags and update_exisiting options if needed
    mc_add_request = { "members": contact_list, "sync_tags": True, "update_existing": True }
    req_string = json.dumps(mc_add_request)
    print("Request size (characters):", len(req_string))

    operation = {
        "method": "POST",
        "path": f"/lists/{list_id}",
        "body": json.dumps(mc_add_request)
    }
    operations.append(operation)


"""
Read CI360 output CSV file and process into MailChimp API batches

Returns:
    List of submitted batch IDs
"""
def process_contact_file(csv_file_path, list_id):
    batch_ids = []
    # Open the CSV file and read it
    with open(csv_file_path, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)

        operations = []
        contact_list = []
        for row in csv_reader:
            # build merge_fields child object (exclude email and datahub_id)
            merge_fields = {}
            for key, value in row.items():
                if key not in STANDARD_FIELDS:
                    merge_fields[key] = value

            mc_row = { "email_address": row[EMAIL_FIELD], "status": "subscribed", "merge_fields": merge_fields }
            contact_list.append(mc_row)

            # complete the batch
            if len(contact_list) >= CONTACTS_PER_OP:
                print('Batch full')
                # add members to operation
                add_members_to_operation_batch(contact_list, operations, list_id)
                # clear member list
                contact_list = []

            if len(operations) >= OP_BATCH_SIZE:
                print('Operation batch full')
                batch_id = submit_operation_batch(operations)
                print('batch id:', batch_id)
                batch_ids.append(batch_id)
                # clear batch
                operations = []


        # no more row in CSV file, check if we have any records in list
        if len(contact_list) > 0:
            print('Last batch')
            add_members_to_operation_batch(contact_list, operations, list_id)

        if len(operations) >= 0:
            batch_id = submit_operation_batch(operations)
            print('batch id:', batch_id)
            batch_ids.append(batch_id)

    return batch_ids


"""
Submit single operations batch to MailChimp API

Returns:
    batch ID if successful, empty string otherwise
"""
def submit_operation_batch(operations):
    batches_api_url = BASE_URL + "/batches"

    print('Submitting batch... operation count:', len(operations))
    # submit operation batch
    batch_payload = { "operations": operations }

    result = send_post_request(batches_api_url, AUTH_TOKEN, batch_payload)
    # Print the result
    print(f"Status: {result['status']}")
    print(f"Response data: {result['data']}")

    if result['status'] == 200:
        resp_obj = json.loads(result['data'])
        batch_id = resp_obj['id']
        return batch_id
    return ""

    

