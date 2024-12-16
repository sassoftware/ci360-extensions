"""
Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import json
import urllib3
import time
import xml.etree.ElementTree as ET
import csv

match_auth_key = os.environ['match_auth_key']
match_base_url = os.environ['match_base_url']
upload_file_prefix = os.environ['upload_file_prefix']
match_id_field_name = os.environ['match_id_field_name']

# Create a PoolManager instance for managing HTTP connections with connection pooling
http = urllib3.PoolManager()


def lambda_handler(event, context):
    print("Received webhook data")
    event_body = json.loads(event['body'])
    print(event_body)
    presigned_urls = event_body["presignedUrls"]
    print("dataFile:", presigned_urls['dataFile'])
    print("metadataFile:", presigned_urls['metadataFile'])
    
    # generate filenames
    ts_milli = round(time.time() * 1000)
    upload_filename = upload_file_prefix + str(ts_milli)
    matchfile = '/tmp/' + upload_filename
    sourcefile = matchfile + '_src'
    metafile = matchfile + '_md'
    
    # download files
    print("Downloading output data files from CI360")
    
    print("downloading meta file:", metafile)
    download_file(presigned_urls['metadataFile'], metafile)
    #print_file_contents(metafile)
    
    print("downloading data file:", sourcefile)
    download_file(presigned_urls['dataFile'], sourcefile)
    #print_file_contents(sourcefile, 500)


    # process file = transform as needed
    print("processing:", sourcefile)
    with open(sourcefile, mode='r') as in_file, open(matchfile, mode='w') as out_file:
        csv_reader = csv.reader(in_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            row.pop(0)
            # Add file processing logic here as needed (if data transformation is needed)
            #print(",".join(row))
            line_count += 1
        print(f"Processed {line_count} lines.")
    
    # process metadata (header) file - we will rename identity_id column that's included in the output file as default with Match ID column name
    metastr = ""
    with open(metafile, 'r') as f:
        metastr = f.read()
    metastr = metastr.replace("identity_id", match_id_field_name)
    with open(metafile, 'w') as file1:
        file1.write(metastr + "\n")
    
    # concatenate header and data into one file to be uploaded
    # Match requires the header row with column names
    filenames = [metafile, sourcefile]
    concatenate_files(filenames, matchfile)
    #print_file_contents(matchfile, 500)
    

    print("Uploading data to Match")
    
    # get upload URL
    print("getting upload URL for:", upload_filename)
    upload_url = get_match_upload_url(upload_filename)
    print("upload URL:", upload_url)
    
    # upload file to Match
    print("uploading file to match:", matchfile)
    file_stats = os.stat(matchfile)
    print(f"File size is {file_stats.st_size} bytes")
    upload_file(upload_url, matchfile)
    
    # check upload status
    print("check upload status:", upload_filename)
    upload_status = get_match_upload_status(upload_filename)
    print("upload status:", upload_status)
    
    response_body = { "status": upload_status, "upload_name": upload_filename }
    return {
        'statusCode': 200,
        'body': json.dumps(response_body)
    }


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


def match_api_call(api_url):
    basic_auth_str = match_auth_key + ':'
    headers = urllib3.make_headers(basic_auth=basic_auth_str)
    headers['Accept'] = 'application/xml'
    r = http.request('GET', api_url, headers=headers)
    return r


def get_value_from_xml(xml_data, tag):
    # parse response XML
    try:
        root = ET.fromstring(xml_data)
        for child in root:
            if child.tag == tag:
                return child.text
    except Exception as e:
        print(f"Error parsing response XML: {e}")
        print(f"response XML: {xml_data}")
    return None


def get_match_upload_url(upload_filename):   
    url = match_base_url + '/ads/userRegistrationUrl?name=' + upload_filename + '.csv'
    print("request url:", url)
    r = match_api_call(url)
    if r.status == 200:
        return get_value_from_xml(r.data, 'url')
    else:
        print(f"status: {r.status}")
    return None
    
    
def get_match_upload_status(upload_filename):   
    url = match_base_url + '/ads/userRegistrations/' + upload_filename + '.csv'
    print("request url:", url)
    r = match_api_call(url)
    if r.status == 200:
        return get_value_from_xml(r.data, 'status')
    else:
        print(f"status: {r.status}")
    return None
    
    
def upload_file(url, file_path):
    try:
        with open(file_path, 'rb') as file:
            file_content = file.read()

            response = http.request('PUT', url, body=file_content, headers={'Content-Type': 'text/plain'})

            if response.status == 200:
                print("File upload successful:")
                print(response.data.decode('utf-8'))
            else:
                print(f"Error: Unable to upload file. Status Code: {response.status}")

    except Exception as e:
        print(f"Error: {e}")

        
def print_file_contents(filename, maxsize=-1):
    file_stats = os.stat(filename)
    print("File", filename, "(", file_stats.st_size, " bytes ):")
    with open(filename, 'r') as f:
        print(f.read(maxsize))


def concatenate_files(filenames, output_file):
    with open(output_file, 'w') as outfile:
        for fname in filenames:
            with open(fname) as infile:
                for line in infile:
                    outfile.write(line)
