# SAS Customer Intelligence 360 Audience Management Utility
## Table of contents
Welcome to SAS Customer Intelligence 360 Audience Management Utility. This utility is build for managing Audiences using Marketing Audience API.
- [Overview](#Overview)
  - [Prerequisites](#Prerequisites)
  - [Setup](#Setup)
- [Create_Audience_Definition](#Create_Audience_Definition)
  - [Configuration](#Configuration)
  - [Running the Script](#running-the-script)
  - [End Result](#end-result)
- [Upload_Audiences](#Upload_Audiences)
  - [Configuration](#configuration-1)
  - [Update CSV file](#update-csv-file)
  - [Running the Script](#running-the-script-1)
  - [End Result](#end-result-1)
- [Logging](#Logging)
## Overview
This utility will make use of python scripts for creating audience definitions, its corresponding audience and for uploading audiences data into 360 tenant. Below are the python scripts that are embedded within this utility:
1. `Create_Audience_Definition`: This can be used to create an audience definition and corresponding audiences within 360 solution by uploading JSON data.
2. `Upload_Audiences`: This can be used to upload audience data in an audience created by previous python script "Create_Audience_Definition" 360 tenant.

### Prerequisites

- Python 3.12 or higher.
- Pip (Python package installer).
- Access to a SAS Customer Intelligence 360 tenant.
- Connection details of a 360 Access Point and a 360 API User.
- Awareness of 360 Audiences.

### Setup
Download the repository on your local machine. Using below command prompt navigate to project directory 'audience-management-utility':
```bash
cd audience-management-utility
```
Setup virtual environment for each python scripts (optional):
```bash
python -m venv upload_audiences_env
source upload_audiences_env/bin/activate   # On Windows: .\upload_audiences_env\Scripts\activate
```
Install dependencies using below command.
```bash
pip install -r requirements.txt
```
## Create_Audience_Definition
### Configuration
Update the config.ini file under create_audience_definition directory that must contains below required parameters necessary for the code to initialize, authenticate and connect to 360 tenant. 

```
[DEFAULT]
external_gateway_host = *****    /* 360 tenant External gateway host url */
tenant_name = *****      /* Tenant name from 360 UI */
tenant_id = *****    /* Tenant ID from 360 UI */
client_secret = *****     /* Client secret associated with the general access point  */
UID_360 = API-*****     /* API user name defined in 360 UI */
UID_PSWD = *****      /* API user secret defined in 360 UI */
```

Ensure you have a audience_definition.json file under the create_audience_definition directory. This JSON file will include JSON schema for creating an audience definition and its corresponding audience.

### Running the Script
```bash
python create_audience_definition.py
```
### End Result
Navigate to 360 tenant and confirm the Audience definition as well as its corresponding Audience that should get created in 360 tenant.
The created Audience will have only the schema and no audience data. To upload audience data, follow the next python script i.e. 'upload_audiences'.

## Upload_Audiences
```
cd upload_audiences
```
### Configuration

Update the config.ini file under create_audience_definition directory that must contains below required parameters necessary for the code to initialize, authenticate and connect to 360 tenant.
```
[DEFAULT]
external_gateway_host = *****    /* 360 tenant External gateway host url */
tenant_name = *****      /* Tenant name from 360 UI */
tenant_id = *****    /* Tenant ID from 360 UI */
client_secret = *****     /* Client secret associated with the general access point  */
UID_360 = API-*****     /* API user name defined in 360 UI */
UID_PSWD = *****      /* API user secret defined in 360 UI */

[Audience_Configuration]
audience_file_name = *****    /* Name of the CSV file that contains Audience data to upload */
audience_name = *****     /* Name of the Audience OR Audience Definition created using audience API */
audience_id = *****      /* ID of the Audience definition */
```

### Update CSV file
Before running the python script you have to update the `audience.csv` file.
Open `audience.csv` file in a CSV editor or a text editor. Ensure that the data and column in the file match with the audience definition.
The CSV file should not contain headers. Any discrepancy in the column data types and the number of columns will result in an error.

**EXAMPLE**:
    If your audience definition specifies three columns: `user_id` (numeric), `email` (character), and `signup_date` (Date), your `audience.csv` should look like this:

    ```plaintext
    1,john.doe@example.com,2023-01-15
    2,jane.doe@example.com,2023-01-16
    3,bob.smith@example.com,2023-01-17
    ```
Once you have verified the `audience.csv` file, you can proceed with the audience data upload process by running the python script.

### Running the Script
```bash
python upload_audiences.py
```
### End Result
Navigate to 360 tenant and confirm the Audience data that should get created in 360 tenant with respect to the `audience.csv` file.
This way one can upload Audience data in an audience that was created using Marketing Audience APIs.


## Logging
Both the scripts generate below mentioned log files in their respective directories:

•	audience_upload.log

•	create_audience_definition.log

These logs provide detailed information about the script execution and help in troubleshooting issues.

This `README.md` file provides comprehensive instructions for setting up and running both scripts, as well as details on dependencies, configuration, and logging.
