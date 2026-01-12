# Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import argparse
import requests
import json
import time
import os
import configparser
import logging
import logging.config
import shutil
import sys
from tqdm import tqdm
from tqdm.utils import CallbackIOWrapper
import re

from modules.ci360tenant import Ci360Tenant


logging.config.fileConfig(os.path.abspath(os.getcwd() + "/logger.ini"))
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def directory_check(directory):
    """Check if directory exists."""
    if not os.path.isdir(directory):
        logger.error('"%s" is not an existing directory', directory)
        sys.exit(1)
    return os.path.abspath(directory)

def file_check(file_path):
    """Check if file exists."""
    if not os.path.isfile(file_path):
        logger.error('"%s" is not an existing file', file_path)
        sys.exit(1)
    return os.path.abspath(file_path)

def read_config(config_path='config.ini'):
    """Read config and return a dict of values."""
    config = configparser.ConfigParser()
    config.read(config_path)
    try:
        tenant_information = config['tenant_information']
    except KeyError:
        logger.error("Configuration file is missing 'tenant_information' section.")
        sys.exit(1)
    try:
        return {
            'tenantID': tenant_information['tenantID'],
            'client_secret': tenant_information['client_secret'],
            'baseURL': tenant_information['ci360_url']
        }
    except Exception:
        logger.error("Error parsing configuration file.")
        sys.exit(1)

def parse_cli_args():
    """Parse command line arguments to override config values."""
    # Create the top-level
    description = """\n
    Export or Upload contact preference data for Email and SMS from CI360.
    Requires a configuration ini file with tenant information.
    Default config file path is 'config.ini' in the current directory.
    """
    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter)

    ## Config file path
    parser.add_argument('-c', '--config', default='config.ini',
        help='Path to config ini file. default: config.ini')
    
    ## Verbose logging
    parser.add_argument(
        '-v', '--verbose',
        action="store_const",
        dest="loglevel",
        const=logging.DEBUG, # -v sets level to INFO
        help="Increase log verbosity (e.g., show INFO messages)")
    
    # Create subparsers for "upload" and "download" commands
    subparsers = parser.add_subparsers(dest="command", help="Sub-command help")

    upload_description = """\n
    Upload contact preference data for Email and SMS to CI360.

    Example:
        py contact_preference.py upload email path/to/contact_preference_file.csv -H
        py contact_preference.py upload sms path/to/contact_preference_file.csv -k -d uploaded_files/
    """
    # Create the parser for the "upload" command
    parser_upload = subparsers.add_parser("upload",
        description=upload_description,
        help="Upload a contact preference file.",
        formatter_class=argparse.RawDescriptionHelpFormatter)

    parser_upload.add_argument("contact_preference_type", choices=['email','sms'],
        help='Type of contact preference to upload. Options are "email" or "sms".')
    
    parser_upload.add_argument("filename", type=str, 
        help="Path to the contact preference file to upload")

    parser_upload.add_argument('-H', '--headers', action='store_true', dest='headers_included',
        help="Headers included in the contact preference file.")
    
    parser_upload.add_argument('-k', '--keep-file-uploaded', action='store_true', dest='keep_file_uploaded', default=False,
        help='Copy the file uploaded in CI360 to Folder. By default, file is not copied. Files are stored with the (date)_(Export_Table_Job_ID).csv')
    
    parser_upload.add_argument('-d', '--keep-destination', dest='keep_destination', default='uploaded_files/',
        help='Destination folder to keep uploaded file. default: uploaded_files/ in the base directory')
    
    parser_upload.add_argument('-C', '--validate-file', action='store_true', dest='validate_file', default=False,
        help='File is validated before upload to prevent long waiting times. By Default this check is not made.')

    download_description = """\n
    Download contact preference data for Email and SMS from CI360.

    Example:
        py contact_preference.py download email -O downloads/
        py contact_preference.py download sms -O downloads/ -f sms_preferences -e .txt -nt -s -H -w 15 -m 20
    """
    # Create the parser for the "download" command
    parser_download = subparsers.add_parser("download",
        description=download_description,
        help="Download contact preferences data",
        formatter_class=argparse.RawDescriptionHelpFormatter)

    parser_download.add_argument("contact_preference_type", choices=['email','sms'],
        help='Type of contact preference to download. Options are "email" or "sms".')

    parser_download.add_argument('-O', '--download-dir', default='downloads/', dest='download_dir',
        help='Output download directory. default: downloads/')
    
    parser_download.add_argument('-f', '--file-name', default='contact_preference_data', dest='file_name',
        help='Base output file name (without extension). default: contact_preference_data')

    parser_download.add_argument('-e', '--file-extension', default='.csv', dest='file_extension',
        help='File extension for the output file, default: .csv')
    
    parser_download.add_argument('-nt', '--no-file-name-timestamp', action='store_false', dest='file_name_timestamp',
        help='Do not append timestamp to file name. By default, timestamp is appended.')

    parser_download.add_argument('-s', '--include-source-and-timestamp', action='store_true',  dest='include_source_and_timestamp',
        help='Include source and timestamp in export file. Source and timestamp fields are NOT added by default.')
    
    parser_download.add_argument('-H', '--include-export-headers', action='store_true',  dest='include_export_headers',
        help='Include headers in export file. Headers are NOT included by default.')

    parser_download.add_argument('-w', '--wait-time', type=int, default=10, dest='wait_time',
        help='Seconds to wait between status checks. default: 10')
    
    parser_download.add_argument('-m', '--max-tries', type=int, default=10, dest='max_tries',
        help='Maximum number of status check retries. default: 10')

    validate_description = """\n
    Basic Verification of contact preference data for Email and SMS from CI360.

    Example:
        py contact_preference.py validate email contact_preferences_email.csv
        py contact_preference.py validate sms contact_preferences_sms.csv 
    """
    # Create the parser for the "validate" command
    parser_validate = subparsers.add_parser("validate",
        description=validate_description,
        help="validate contact preferences data",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    
    parser_validate.add_argument("contact_preference_type", choices=['email','sms'],
        help='Type of contact preference to validate. Options are "email" or "sms".')
    
    parser_validate.add_argument("filename", type=str, 
        help="Path to the contact preference file to validate")
    
    parser_validate.add_argument('-H', '--headers', action='store_true', dest='headers_included',
    help="Headers included in the contact preference file.")
    
    return parser.parse_args()

def request_contact_preference_export(baseURL, jwt_token, contact_preference_type, include_source_and_timestamp=False):
    """Request Export Table Job ID for Contact Preference Export."""
    url = baseURL.rstrip('/') + "/marketingData/tableJobs/"
    headers = {'Content-Type': 'application/json', 'authorization': "Bearer " + jwt_token}
    logger.debug("Sending POST Request to get Export Table Job ID")
    payload={"jobType": "CONTACT_PREFERENCE_EXPORT", "includeSourceAndTimestamp": include_source_and_timestamp}
    if contact_preference_type == "sms":
        payload["jobType"] = "CONTACT_PREFERENCE_SMS_EXPORT"
    logger.debug("Making a request for Export Table Job ID for %s and includeSourceAndTimestamp=%s", payload["jobType"], payload["includeSourceAndTimestamp"])
    response = requests.post(url.strip(), headers=headers, data=json.dumps(payload))
    try:
        post_response = response.json()
    except Exception:
        logger.error("Failed to parse POST response: %s", response.text)
        response.raise_for_status()
    if 'id' in post_response and post_response['id']:
        logger.debug("Export Table Job ID Generated %s", post_response['id'])
        logger.debug("Retrieved Export Table Job ID")
        return post_response['id']
    else:
        logger.error("Error while making POST Request. %s", response.reason)
        logger.error("HTTP Status: %s", response.status_code)
        if response.text:
            logger.error("Message: %s", response.text)
        sys.exit(1)

def get_export_job_status(baseURL, table_job_id, jwt_token):
    """Get Export Job Status using Table Job ID."""
    url = baseURL.rstrip('/') + "/marketingData/tableJobs/" + table_job_id
    headers = {'Content-Type': 'application/json', 'authorization': "Bearer " + jwt_token}
    logger.debug("Making request for status of Export Table Job ID %s", table_job_id)
    response = requests.get(url, headers=headers)
    try:
        data = response.json()
    except Exception:
        logger.error("Failed to parse GET response: %s", response.text)
        response.raise_for_status()
    logger.debug("Successful Response from Get Request")
    if data:
        status = data.get('status', " ")
        logger.debug("Status of the data is %s", status)
        return data, status
    else:
        logger.error("Error while GET Request. %s", response.reason)
        logger.error("HTTP Status: %s", response.status_code)
        if response.text:
            logger.error("Message: %s", response.text)
        return None, " "

def download_to_csv(job_status_response, download_dir, file_name, file_name_timestamp, include_header, file_extension=".csv"):
    """Download contact preference data to CSV file."""
    if not job_status_response or 'downloadItemList' not in job_status_response:
        logger.error("No download items found in response.")
        return None
    
    # normalize & ensure extension
    if not file_extension.startswith('.'):
        file_extension = '.' + file_extension

    # add timestamp if required
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    if file_name_timestamp:
        file_output_name = f"{file_name}_{timestamp}{file_extension}"
        logger.debug("File name with timestamp: %s", file_output_name)
    else:
        file_output_name = f"{file_name}{file_extension}"
    
    # Error if downloadItemList is empty
    data_items = job_status_response.get('downloadItemList', [])
    if not data_items:
        logger.error("downloadItemList is empty.")
        return None
    
    output_file = os.path.join(download_dir, file_output_name)

    try:
        with open(output_file, 'wb') as f:
            # write header first
            header_written = False
            if include_header:
                for row in data_items:
                    url = row.get('url', '') or ''
                    path = row.get('path', '') or ''
                    if not url:
                        logger.warning("Empty URL in downloadItemList entry, skipping.")
                        break
                    if "_header" in path.lower():
                        logger.debug("Fetching header from %s", url)
                        response = requests.get(url, allow_redirects=True, stream=True, timeout=30)
                        response.raise_for_status()
                        total_size = int(response.headers.get("content-length", 0))
                        with tqdm(total=total_size, unit="B", unit_scale=True, unit_divisor=1024) as t:
                            for chunk in response.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)
                                    t.update(len(chunk))
                        header_written = True
                        logger.debug("Header written to file.")
                        break

            # write data parts
            for row in data_items:
                url = row.get('url', '') or ''
                path = row.get('path', '') or ''
                if not url:
                    logger.warning("Empty URL in downloadItemList entry, skipping.")
                    break
                if "header" not in path.lower():
                    logger.debug("Fetching data part from %s", url)
                    response = requests.get(url, allow_redirects=True, stream=True, timeout=30)
                    response.raise_for_status()
                    total_size = int(response.headers.get("content-length", 0))
                    with tqdm(total=total_size, unit="B", unit_scale=True, unit_divisor=1024) as t:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                t.update(len(chunk))

        logger.info("**** Data successfully downloaded to %s with headers %s****", output_file, "written" if header_written else "not written")

    except requests.RequestException as e:
        logger.error("Network error while downloading parts: %s", e)
        if os.path.exists(output_file):
            try:
                os.remove(output_file)
            except Exception:
                pass
        return None
    except Exception as e:
        logger.error("Unexpected error writing file: %s", e)
        return None

def download_contact_preferences(ci360_tenant, file_params, retry_params):
    """Download contact preferences data from CI 360."""
    logger.info("Initiating Contact Preference Export for type: %s", file_params['contact_preference_type'])
    table_job_id = request_contact_preference_export(ci360_tenant.base_url, ci360_tenant.jwt_token, file_params['contact_preference_type'], file_params['include_source_and_timestamp'])
    export_job_status_response, status = get_export_job_status(ci360_tenant.base_url, table_job_id, ci360_tenant.jwt_token)
    retry_counter = 1
    while retry_counter <= retry_params['max_tries']:
        if status == "PROCESSING":
            logger.debug("Status of the Data is PROCESSING")
            logger.debug("Wait until the status changes from PROCESSING to EXPORTED")
            time.sleep(retry_params['wait_time'])
            logger.debug("Status is PROCESSING so retrying to get the data after waiting %s seconds", retry_params['wait_time'])
            export_job_status_response, status = get_export_job_status(ci360_tenant.base_url, table_job_id, ci360_tenant.jwt_token)
            if retry_counter == retry_params['max_tries']:
                logger.info("Reached max retries")
                sys.exit(1)
        elif status == "EXPORTED":
            logger.debug("Export Job Status Response: %s", export_job_status_response)
            download_to_csv(export_job_status_response, file_params['download_dir'], file_params['file_name'], file_params['file_name_timestamp'], file_params['include_export_header'], file_params['file_extension'])
            break
        else:
            logger.debug("Response status undefined. Exiting the process")
            sys.exit(1)
        retry_counter += 1

def request_upload_signed_url(baseURL, jwt_token):
    """Request Upload Signed URL to upload a file to CI 360."""
    url = baseURL.rstrip('/') + "/marketingData/fileTransferLocation"
    logger.debug("Preparing to send POST request for Upload File Transfer Location %s", url)
    headers = {'authorization': "Bearer " + jwt_token}
    response = requests.post(url, headers=headers)
    logger.debug("Response received from POST request for Upload File Transfer Location: %s", response.text)
    try:
        data = response.json()
    except Exception:
        logger.error("Failed to parse POST response: %s", response.text)
        response.raise_for_status()
    if data:
        signedURL = data.get('signedURL', " ")
        logger.debug("Signed URL of the data is %s", signedURL)
        return signedURL
    else:
        logger.error("Error while GET Request. %s", response.reason)
        logger.error("HTTP Status: %s", response.status_code)
        if response.text:
            logger.error("Message: %s", response.text)
        return None, " "

def upload_file_to_ci360(signedURL, upload_file):
    """Upload contact preference file to CI 360 using Signed URL."""
    if not signedURL or signedURL.strip() == "":
        logger.error("Failed to get signed URL for upload")
        sys.exit(1)

    if not os.path.isfile(upload_file):
        logger.error("The specified file to upload does not exist: %s, please check your file path and that the file exists", upload_file)
        sys.exit(1)

    try:
        file_size = os.path.getsize(upload_file)
        logger.debug("Uploading file %s (size: %d bytes) to signed URL", upload_file, file_size)
    except Exception as e:
        logger.error("Failed to get file size: %s", e)
        sys.exit(1)

    headers = {'Content-Type': 'text/csv'}
    try:
        with open(upload_file, 'rb') as f:
            with tqdm(total=file_size, unit="B", unit_scale=True, unit_divisor=1024) as t:
                wrapped_file = CallbackIOWrapper(t.update, f, "read")
                response = requests.put(signedURL, data=wrapped_file, headers=headers)
                response.raise_for_status()
        if response.status_code != 200:
            logger.error("Failed to upload file. Status code: %s", response.status_code)
            sys.exit(1)
        if response.status_code == 200:
            logger.info("**** File successfully uploaded ****")
            return True
    except requests.RequestException as e:
        logger.error("Network error during file upload: %s", e)
        sys.exit(1)
    except Exception as e:
        logger.error("Unexpected error during file upload: %s", e)
        sys.exit(1)

def contact_preferences_import_request(ci360_tenant, signedURL, contact_preference_type, headers_included=False):
    """Request Contact Preference Import Job in CI 360."""
    url = ci360_tenant.base_url.rstrip('/') + "/marketingData/tableJobs/"
    logger.debug("Preparing to send POST request for Contact Preference Import Job %s", url)
    headers = {'Content-Type': 'application/json', 'authorization': "Bearer " + ci360_tenant.jwt_token}
    payload={"jobType": "CONTACT_PREFERENCE_IMPORT", "fileLocation": signedURL, "headerRowIncluded": headers_included}
    if contact_preference_type == "sms":
        payload["jobType"] = "CONTACT_PREFERENCE_SMS_IMPORT"
    logger.debug("Making a request for Contact Preference Import Job for %s with headers_included=%s", payload["jobType"], payload["headerRowIncluded"])
    logger.debug("Payload for Contact Preference Import Request: %s", payload)
    logger.info("**** Making Import Request for uploaded file ****")
    response = requests.post(url.strip(), headers=headers, data=json.dumps(payload))
    try:
        data = response.json()
    except Exception:
        logger.error("Failed to parse POST response: %s", response.text)
        response.raise_for_status()
    if data:
        logger.debug("Successful Response from Contact Preference Import Request")
        logger.debug("Response from Contact Preference Import Request: %s", data)
        job_id =data.get('id', " ")
        return job_id
    else:
        logger.error("Error while POST Request. %s", response.reason)
        logger.error("HTTP Status: %s", response.status_code)
        if response.text:
            logger.error("Message: %s", response.text)
        sys.exit(1)

def get_import_job_status(baseURL, table_job_id, jwt_token):
    """Get Import Job Status using Table Job ID."""
    url = baseURL.rstrip('/') + "/marketingData/tableJobs/" + table_job_id
    headers = {'Content-Type': 'application/json', 'authorization': "Bearer " + jwt_token}
    logger.debug("Making request for status of Export Table Job ID %s", table_job_id)
    response = requests.get(url, headers=headers)
    try:
        data = response.json()
    except Exception:
        logger.error("Failed to parse GET response: %s", response.text)
        response.raise_for_status()
    logger.debug("Successful Response from Get Request")
    if data:
        status = data.get('status', " ")
        logger.debug("Status of the data is %s", status)
        return data, status
    else:
        logger.error("Error while GET Request. %s", response.reason)
        logger.error("HTTP Status: %s", response.status_code)
        if response.text:
            logger.error("Message: %s", response.text)
        return None, " "

def copy_file(source_file, destination, file_name):
    """Copy uploaded file to specified destination."""
    logger.debug("Copying uploaded file from %s to %s", source_file, destination)
    
    try:
        # Ensure the destination directory exists if the destination is a file path in another folder
        # or if the destination is a directory itself.
        destination_dir = os.path.dirname(destination)
        if destination_dir and not os.path.exists(destination_dir):
            os.makedirs(destination_dir)
        destination = os.path.join(destination_dir, file_name)
        # Copy the file
        shutil.copy2(source_file, destination)
        logger.info("Successfully copied %s to %s", source_file, destination)

    except FileNotFoundError:
        print(f"Error: The source file '{source_file}' was not found.")
    except PermissionError:
        print(f"Error: Permission denied when trying to copy the file.")
    except shutil.SameFileError:
        print(f"Error: Source and destination represent the same file.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def upload_contact_preferences(ci360_tenant, file_params):
    """Upload contact preference file to CI 360."""
    signedURL = request_upload_signed_url(ci360_tenant.base_url, ci360_tenant.jwt_token)
    upload_file_to_ci360(signedURL, file_params['file_name'])
    table_job_id = contact_preferences_import_request(ci360_tenant, signedURL, file_params['contact_preference_type'], file_params['headers_included'])
    
    import_job_status_response, status = get_import_job_status(ci360_tenant.base_url, table_job_id, ci360_tenant.jwt_token)
    logger.debug("Import Job Status Response: %s", import_job_status_response)
    retry_counter = 1
    while retry_counter <= 10:
        if status == "Queued":
            logger.debug("Status of the Data is QUEUED")
            logger.debug("Wait until the status changes from QUEUED to PROCESSING")
            time.sleep(15)
            logger.debug("Status is QUEUED so retrying to get the data after waiting 15 seconds")
            import_job_status_response, status = get_import_job_status(ci360_tenant.base_url, table_job_id, ci360_tenant.jwt_token)

        elif status == "Processing Identities" or status == "Processing Preferences" or status == "Processing":
            logger.debug("Status of the Data is PROCESSING")
            logger.debug("Wait until the status changes from PROCESSING to IMPORTED")
            time.sleep(30)
            logger.debug("Status is PROCESSING so retrying to get the data after waiting 30 seconds")
            import_job_status_response, status = get_import_job_status(ci360_tenant.base_url, table_job_id, ci360_tenant.jwt_token)
            if retry_counter == 10:
                logger.warning("Processing took too long max retries")
                sys.exit(1)

        elif status == "Failed Validation":
            logger.error("Import Job Failed with status: %s", status)
            logger.error("Status Description: %s", import_job_status_response.get('statusDescription', "'No description provided'"))
            sys.exit(1)

        elif status == "Imported":
            logger.info("**** Contact Preference file successfully imported ****")
            logger.info("**** Import Job ID: %s ****", table_job_id)
            logger.debug("Job Details: %s", import_job_status_response)
            logger.debug("keep_file_uploaded parameter is %s", file_params['keep_file_uploaded'])
            if file_params['keep_file_uploaded']:
                file_name = time.strftime("%Y%m%d-%H%M%S") + "_" + table_job_id + ".csv"
                logger.debug("File name to copy the uploaded file: %s", file_params['keep_destination'] + file_name)
                copy_file(file_params['file_name'], file_params['keep_destination'], file_name)
            break
                
        else:
            logger.debug("import_job_status_response: %s", import_job_status_response)
            logger.debug("Status undefined. Exiting the process")
            sys.exit(1)
        retry_counter += 1

def check_csv_file_headers(file_path, expected_headers):
    """Check if CSV file contains expected headers. Supports regex patterns."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            header = f.readline().strip().split(',')
            # Strip whitespace from header values
            header = [col.strip() for col in header]
            
            logger.debug("CSV Headers found: %s", header)
            
            for i, expected in enumerate(expected_headers):
                logger.debug("Checking header index %d for pattern: %s", i, expected)
                # Check if header at position i matches the regex pattern
                if i < len(header) and re.match(expected, header[i]):
                    logger.debug("Header match found: %s matches pattern %s", header[i], expected)
                    continue
                else:
                    logger.error("Header at index %d: '%s' does not match pattern: %s", i, header[i] if i < len(header) else "MISSING", expected)
                    return False
            return True
    except FileNotFoundError:
        logger.error("The specified file does not exist: %s", file_path)
        sys.exit(1)
    except Exception as e:
        logger.error("An error occurred while reading the file: %s", e)
        sys.exit(1)

def check_csv_column_values(file_path, has_headers, column_index, expected_values_regex):
    """Check if CSV file's specified column contains only expected values (in regex form). Not including header row if it exists"""
    logger.debug("validating column %d of the CSV file: %s", column_index, file_path)
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            errors = 0
            for i, line in enumerate(f):
                if i == 0 and has_headers:  # Skip header row
                    continue
                value = line.split(',')[column_index].strip()
                if not any(re.match(pattern, value) for pattern in expected_values_regex):
                    errors += 1
            logger.debug("Errors found: %s", errors)
            if errors > 0:
                return False, errors
            else:
                return True, errors
    except Exception as e:
        logger.error("An error occurred while reading the file: %s", e)
        sys.exit(1)

def validate_contact_preferences_file(file_params):
    """Basic verification of contact preference file is in correct format to upload."""
    logger.info("Validating Contact Preference file: %s", file_params['file_name'])
    if file_params['headers_included']:
        expected_headers = [r'(?i)^identity_type$', r'(?i)^identity_value$', r'(?i)^preference_type(?:_cd)?$', r'(?i)^preference_value$']
        headers_valid = check_csv_file_headers(file_params['file_name'], expected_headers)
    else:
        headers_valid = True
    # Check if the csv file's first column contains all "email_id" as the value
    if file_params['contact_preference_type'] == 'email':
        identity_column_check_value = [r"(?i)^email_id$"]
    elif file_params['contact_preference_type'] == 'sms':
        identity_column_check_value = [r"(?i)^sms_id$"]
    else:
        logger.error("No contact preference type set")
        sys.exit(1)
    identity_type_column_valid, identity_type_errors = check_csv_column_values(file_params['file_name'], True, 0, identity_column_check_value)
    preference_type_column_valid, preference_type_errors = check_csv_column_values(file_params['file_name'], True, 2, [r"(?i).*OPT-OUT",r"(?i).*SPAM",r"(?i).*HARDBOUNCE"])
    preference_value_column_valid, preference_value_errors = check_csv_column_values(file_params['file_name'], True, 3, [r"(?i)^true$", r"(?i)^false$"])
    
    if file_params['headers_included']:
        if not headers_valid:
            logger.error("File were said to be included but were invalid per the standards or weren't included")
    if not identity_type_column_valid:
        logger.error("validation of identity_type column came up %s, there were %s rows with invalid values.", identity_type_column_valid, identity_type_errors)
    if not preference_type_column_valid:
        logger.error("preference_type column validity: %s, there were %s rows with invalid values, Preference value does not conform to standards (OPT-OUT, PROGRAMID1@OPT-OUT)", preference_type_column_valid, preference_type_errors)
    if not preference_value_column_valid:
        logger.error("preference_value column validity: %s, there were %s rows with invalid values. All values in this column should be either True or False", preference_value_column_valid, preference_value_errors)
    
    if not (headers_valid and identity_type_column_valid and preference_type_column_valid and preference_value_column_valid):
        logger.info("\u274c Validation Failure. Note: Validate only checks file structure and standard values, %s values are not checked for formatting or validated.", file_params["contact_preference_type"])
        sys.exit(1)

    if headers_valid and identity_type_column_valid and preference_type_column_valid and preference_value_column_valid:
        logger.info("\u2714 Validation Successful. Note: Validate only checks file structure and standard values, %s values are not checked for formatting or validated.", file_params["contact_preference_type"])

def main(cli_args=None):
    """Main function to handle contact preference upload/download."""
    args = cli_args or parse_cli_args()
    if args.loglevel:
        logger.setLevel(args.loglevel)
    if not args.command == "validate":
        if args.config:
            config_path = args.config
        tenant_information=read_config(config_path) # Initializing the variables and getting data from Configuration file
    file_parameters = {}
    retry_parameters = {}
    
    # Handle upload command
    if args.command == "upload":
        if args.filename:
            file_parameters['file_name'] = file_check(args.filename)
            logger.debug("File to upload: %s", file_parameters['file_name'])
        if args.contact_preference_type:
            file_parameters['contact_preference_type'] = args.contact_preference_type
        if args.headers_included is not None:
            file_parameters['headers_included'] = args.headers_included
        if args.keep_file_uploaded is not None:
            file_parameters['keep_file_uploaded'] = args.keep_file_uploaded
        if args.keep_destination is not None:
            directory_check(args.keep_destination)
            file_parameters['keep_destination'] = args.keep_destination
        if args.validate_file:
            validate_contact_preferences_file(file_parameters)
            

        logger.debug("File parameters for upload: %s", file_parameters)
        ci360_tenant = Ci360Tenant(tenant_information['tenantID'], tenant_information['client_secret'], tenant_information['baseURL'])
        upload_contact_preferences(ci360_tenant, file_parameters)

    # Handle download command
    if args.command == "download":
        if args.download_dir:
            directory_check(args.download_dir)
            file_parameters['download_dir'] = args.download_dir
        if args.file_name:
            file_parameters['file_name'] = args.file_name
        if args.file_extension:
            file_parameters['file_extension'] = args.file_extension
        if args.file_name_timestamp is not None:
            file_parameters['file_name_timestamp'] = args.file_name_timestamp
        if args.contact_preference_type:
            file_parameters['contact_preference_type'] = args.contact_preference_type
        if args.include_source_and_timestamp is not None:
            file_parameters['include_source_and_timestamp'] = args.include_source_and_timestamp
        if args.include_export_headers is not None:
            file_parameters['include_export_header'] = args.include_export_headers
        if args.wait_time is not None:
            retry_parameters['wait_time'] = args.wait_time
        if args.max_tries is not None:
            retry_parameters['max_tries'] = args.max_tries
        
        logger.debug("File parameters for upload: %s", file_parameters)
        ci360_tenant = Ci360Tenant(tenant_information['tenantID'], tenant_information['client_secret'], tenant_information['baseURL'])
        download_contact_preferences(ci360_tenant, file_parameters, retry_parameters)

        # Handle validate command
    if args.command == "validate":
        if args.contact_preference_type:
            file_parameters['contact_preference_type'] = args.contact_preference_type
        if args.filename:
            file_parameters['file_name'] = file_check(args.filename)
        if args.headers_included is not None:
            file_parameters['headers_included'] = args.headers_included

        logger.debug("File to validate: %s", file_parameters['file_name'])
        validate_contact_preferences_file(file_parameters)

if __name__ == "__main__":
    main()
