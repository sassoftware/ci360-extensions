# SAS CI360 VELOXPY REST API Client

**SAS CI 360 VELOXPY** is a Automation-ready, secure, and extensible Python library for interacting with RESTful APIs using asynchronous I/O.  
Key features include:

- Token-based authentication
- Background token refresh
- Configuration-driven support for multiple services
- PEP-compliant code style
- Full pip packaging support
- Easy extensibility for CI 360 APIs

Supported APIs:

- Marketing Audience (`/marketingAudience`)
- Marketing Data (`/marketingData`)
- Marketing Execution (`/marketingExecution`)
- Marketing Gateway (`/marketingGateway`)


### Features
- Token management
- Simplified client interface for REST APIs
- Logging

 ##### Planned features
 - API Throttling
 - Addition of Examples
 - Publish to PyPI(twine)

## Setup

Download the repository on your local machine. using the command prompt below, navigate to project directory 'sas_ci360_veloxpy':
```bash
cd sas_ci360_veloxpy
```
Setup virtual environment for each Python scripts (optional):
```bash
python -m venv venv_sas_ci360_veloxpy
source venv_sas_ci360_veloxpy/bin/activate   # On Windows: .\venv_sas_ci360_veloxpy\Scripts\activate
```
Install dependencies using the below command.
```bash
pip install -r requirements.txt
```
### Configuration (Gitlab - Internal)

Before running the examples, create a `sasci360veloxpy.ini` (or refer `./sasci360veloxpy.ini`) file at any place with the following content:
Please give the path for this file in `init` method. Please refer Usage.py for more info

```bash
initApp("<FullPathToFile>\\sasci360veloxpy.ini")
```

The Content for `sasci360veloxpy.ini` as below

```ini

[tenant]
extapigateway_url=extapigwservice-<your url>
client_id=<your tenant Id>
client_secret=<your tenant secret>
protocol=https://
api_user_name=<your api user name>
api_user_password=<your api user password>

## Share and Test
```
To share this package with others for local testing, follow these steps:

1. **Package the Project**  
   Build the wheel and source distribution:
   ```bash
   python -m build
   ```

2. **Share the Package Files (Already generated)**

   Send the generated files from the `dist/` directory (e.g., `.whl` and `.tar.gz`) to your collaborators.

3. **Install Locally for Testing**  
   On the recipient’s machine:
   - Place the received files in a folder.
   - (Optional) Create and activate a virtual environment:
     ```bash
     python -m venv venv_sas_ci360_veloxpy
     # On Windows:
     .\venv_sas_ci360_veloxpy\Scripts\activate
     # On macOS/Linux:
     source venv_sas_ci360_veloxpy/bin/activate
     ```
   - Install the package using pip:
     ```bash
     pip install path\to\your\package.whl
     # Or, for source distribution:
     pip install path\to\your\package.tar.gz
     ```
   - Install dependencies if needed:
     ```bash
     pip install -r requirements.txt
     ```

4. **Test the Installation**  
   Import and use the package in Python to verify installation.

**Note:**  
You don't need to upload to PyPI for local testing. Sharing the built files is sufficient.
## Installation

```bash
pip install sas-ci360-veloxpy
```

---

## Usage Example

```python
from sas_ci360_veloxpy import initApp, APIClient

# Initialize the application
initApp("<<path>>\\sasci360veloxpy.ini")

# Create an API client
apiClient = APIClient()

# Example: Get all audiences
audiences = apiClient.get_audiences()

# Example: Create a bulk task job
job = apiClient.create_job_to_execute_bulk_task(task_id="your-task-id")
```

---
## APIClient Methods

### Audience Methods

#### `get_audiences()`
- **Description:** Returns a list of all audiences.
- **Query Params:** `sortBy`, `start`, `limit`, `status`, `source`, `audienceType`, `name`

#### `async_get_audiences()`
- **Description:** Returns audiences asynchronously.

#### `get_audience_by_id(audience_id)`
- **Description:** Returns details for a specific audience.
- **Path Param:** `audience_id`

#### `delete_audience_by_id(audience_id)`
- **Description:** Deletes a specific audience.
- **Path Param:** `audience_id`

#### `patch_audience(audience_id)`
- **Description:** Patch (update) a specific audience.
- **Path Param:** `audience_id`

#### `update_audience_by_id(file_path, audienceId)`
- **Description:** Updates an audience using a file.
- **Path Params:** `file_path`, `audienceId`

#### `upload_file_for_external_events(file_path)`
- **Description:** Uploads a file containing external events.
- **Param:** `file_path`

#### `create_audience_definition(file_path)`
- **Description:** Creates an audience definition using a CSV file.
- **Param:** `file_path`

#### `get_signed_url()`
- **Description:** Returns a signed URL for file upload.

#### `upload_audience(file_path)`
- **Description:** Uploads audience data from a file.
- **Param:** `file_path`

#### `get_audience_upload_history(audienceId)`
- **Description:** Returns upload history for a specific audience.
- **Path Param:** `audienceId`

#### `get_file_history_by_upload_id(audienceId, historyId)`
- **Description:** Returns file upload history for a specific audience and history.
- **Path Params:** `audienceId`, `historyId`

---

### Marketing Execution Methods

#### `create_job_to_execute_bulk_task(task_id)`
- **Description:** Initiates a job to execute a bulk task.
- **Body Param:** `task_id`

#### `get_a_task_job(task_job_id)`
- **Description:** Retrieves details for a specific task job.
- **Path Param:** `task_job_id`

#### `create_job_to_execute_segment_map(segment_map_id=None, segment_map_name=None, folder_path=None, version=1, override_schedule=False)`
- **Description:** Initiates a job to execute a segment map.
- **Body Params:** `segment_map_id`, `segment_map_name`, `folder_path`, `version`, `override_schedule`

#### `get_a_segment_map_job(segment_map_job_id)`
- **Description:** Retrieves details for a specific segment map job.
- **Path Param:** `segment_map_job_id`

#### `retrieve_response_tracking_codes(task_id=None, occurrence_id=None, task_version_id=None, from_time=None, to_time=None, limit=None, start=None, to_file=False, delimiter_param="comma", include_header_row_param=False)`
- **Description:** Retrieves response tracking codes based on criteria.
- **Query Params:** `task_id`, `occurrence_id`, `task_version_id`, `from_time`, `to_time`, `limit`, `start`, `to_file`, `delimiter_param`, `include_header_row_param`

#### `retrieve_response_tracking_code_by_id(response_tracking_code_id)`
- **Description:** Retrieves a response tracking code by its ID.
- **Path Param:** `response_tracking_code_id`

#### `retrieve_execution_occurrences(task_id=None, segment_map_id=None, type=None, status=None, from_time=None, to_time=None, start_time_from=None, start_time_to=None, limit=None, start=None, to_file=False, delimiter_param="comma", include_header_row_param=False)`
- **Description:** Retrieves execution occurrences based on criteria.
- **Query Params:** `task_id`, `segment_map_id`, `type`, `status`, `from_time`, `to_time`, `start_time_from`, `start_time_to`, `limit`, `start`, `to_file`, `delimiter_param`, `include_header_row_param`

#### `retrieve_execution_occurrences_by_id(occurrence_id)`
- **Description:** Retrieves execution occurrence details by occurrence ID.
- **Path Param:** `occurrence_id`

---

### Marketing Data Methods

#### `access_analytic_services()`
- **Description:** Get links to analytic services or items.

#### `get_collection_of_transfer_items()`
- **Description:** Returns a collection of transfer items.

#### `create_transfer_location_to_upload_analytic_data(columns, listType)`
- **Description:** Creates a transfer location for uploading analytic data.
- **Body Params:** `columns`, `listType`

#### `get_transfer_result_by_ID(transferId)`
- **Description:** Returns a transfer result by ID.
- **Path Param:** `transferId`

#### `create_and_run_customer_job(jobType, identityType, identityList)`
- **Description:** Creates and runs a customer job.
- **Body Params:** `jobType`, `identityType`, `identityList`

#### `get_customer_job_details_by_ID(customerJobId)`
- **Description:** Returns a customer job by ID.
- **Path Param:** `customerJobId`

#### `create_signed_URL_to_upload_files()`
- **Description:** Creates a signed URL for secure file upload.

#### `get_identity_record_by_ID_filter(filterType, value)`
- **Description:** Returns an identity record by type and value.
- **Query Params:** `filterType`, `value`

#### `get_identity_record(identityRecord)`
- **Description:** Returns an identity record by ID.
- **Path Param:** `identityRecord`

#### `get_summary_of_import_requests(dataDescriptorId=None, start=0, limit=10)`
- **Description:** Returns a summary of import requests.
- **Query Params:** `dataDescriptorId`, `start`, `limit`

#### `create_and_run_import_request(name, dataDescriptorId, fieldDelimiter, fileLocation, fileType, headerRowIncluded, recordLimit, updateMode)`
- **Description:** Creates and runs an import request for uploaded data.
- **Body Params:** `name`, `dataDescriptorId`, `fieldDelimiter`, `fileLocation`, `fileType`, `headerRowIncluded`, `recordLimit`, `updateMode`

#### `get_details_of_import_request_by_job_ID(importRequestJobId)`
- **Description:** Returns details of an import request by job ID.
- **Path Param:** `importRequestJobId`

#### `get_summary_of_all_tables(start=0, limit=10, name=None, type=None)`
- **Description:** Returns a summary of all tables in the system.
- **Query Params:** `start`, `limit`, `name`, `type`

#### `create_customer_table(name, description, makeAvailableForTargeting, dataItems, customProperties=None)`
- **Description:** Creates a customer table.
- **Body Params:** `name`, `description`, `makeAvailableForTargeting`, `dataItems`, `customProperties`

#### `get_table_object_by_ID(tableId)`
- **Description:** Returns metadata for a table by ID.
- **Path Param:** `tableId`

#### `update_table_by_ID_PATCH(tableId, name, description, makeAvailableForTargeting, dataItems, customProperties=None)`
- **Description:** Updates a table by ID using PATCH.
- **Body Params:** `tableId`, `name`, `description`, `makeAvailableForTargeting`, `dataItems`, `customProperties`

#### `update_table_by_ID_POST(tableId, name, description, makeAvailableForTargeting, dataItems, customProperties=None)`
- **Description:** Updates a table by ID using POST with method override.
- **Body Params:** `tableId`, `name`, `description`, `makeAvailableForTargeting`, `dataItems`, `customProperties`

#### `delete_table_by_ID(tableId)`
- **Description:** Deletes a table by ID.
- **Path Param:** `tableId`

#### `create_table_job(tableId, jobType, dataDescriptorId, fileLocation=None, headerRowIncluded=None, includeSourceAndTimestamp=None)`
- **Description:** Creates and runs a table job.
- **Body Params:** `tableId`, `jobType`, `dataDescriptorId`, `fileLocation`, `headerRowIncluded`, `includeSourceAndTimestamp`

#### `get_specific_table_job_details_by_ID(tableJobId)`
- **Description:** Returns details of a table job by ID.
- **Path Param:** `tableJobId`

---

## Notes

- All methods return Python objects (usually `dict` or `list`) representing the API response.
- For authentication and configuration, ensure you call `initApp()` before using `APIClient`.
- For more details on each parameter, see the docstrings in the source code.

## Usage
 This section explains how to use this library.
 In Phase 1 we have provided 6 examples files like below

### Example: Generate a Static JWT Token

This example demonstrates how to generate a static JWT token using the provided utility.

### How to run:

From root directory 
```
python .\examples\file_name.py

e.g.

python .\examples\get_static_token.py
```

**get_static_token.py**
- This example shows how a user can generate a static JWT token.


```bash

from sas_ci360_veloxpy.io.loader import getConfigDetails
from sas_ci360_veloxpy.auth.bearer import SASCI360VeloxPyBearerTokenManager

def getToken():
    configData=getConfigDetails()

    bearerTokenMgr = SASCI360VeloxPyBearerTokenManager(configData['extapigateway_url'],configData['client_id'],configData['client_secret'], configData['api_user_name'],configData['api_user_password'])
    bearerToken = bearerTokenMgr.generateStaticJWT()
    token = bearerToken
    return token    

staticToken = getToken()
print("StaticToken",staticToken)
```


**run_app_async.py**
This example shows how a user can generate a bearer token, using async functionality
```bash
from sas_ci360_veloxpy.io.loader import getConfigDetails
from sas_ci360_veloxpy.auth.bearer import SASCI360VeloxPyBearerTokenManager


async def getToken():
        configData=getConfigDetails()
        bearerTokenMgr = SASCI360VeloxPyBearerTokenManager(configData['extapigateway_url'],configData['client_id'],configData['client_secret'], configData['api_user_name'],configData['api_user_password'])
        bearerToken = await bearerTokenMgr.get_token()
        token = bearerToken
        return token

async def main():
    initApp("<FullPathToFile>\\sasci360veloxpy.ini")
  
    try:
        tokenValue = await getToken()
        print("tokenValue",tokenValue)
    except Exception as exc:
      print("Unexpected error:", exc)

      
if __name__ == "__main__":
    asyncio.run(main()) 
```
**usage_marketing_audience.py**

- This example help a user to get existing audiences, creating new audience definitions and uploading audiences data to its corresponding audience definantion into 360 tenant. Below are the audience defination that are embedded within this client:

1. `Get_Audiences`: This can be used to  get all the existing audiences from CI360 tenant.
```bash
from sas_ci360_veloxpy import APIClient

  # Create an API client
apiClient = APIClient()

# Example: Get all audiences
audiences = apiClient.get_audiences()

```
2. `Create_Audience_Definition`: This can be used to create an audience definition and corresponding audiences within 360 solution by uploading JSON data.
```bash
from sas_ci360_veloxpy import APIClient

  # Create an API client
apiClient = APIClient()

# Example: Get all audiences
audiences = apiClient.get_audiences()

createAudience = "C:\\<mypath>\\audience_definition.json"
res_data = audiences.create_audience_definition(createAudience)

```
3. `Upload_Audiences`: This can be used to upload audience data in an existing Audience on CI360 Tenant
```bash
from sas_ci360_veloxpy import APIClient

  # Create an API client
apiClient = APIClient()

# Example: Get all audiences
audiences = apiClient.get_audiences()
uploadAudience = "C:\\<mypath>\\audience_config.ini"
res_data = audiences.upload_audiences(uploadAudience)

```

**marketing_gateway.py**
- This example shows how Marketing Gateway APIs are used to send external events.
This file contains examples for sending single event and Bulk upload events
```bash
from sas_ci360_veloxpy import APIClient

  # Create an API client
apiClient = APIClient()


  upload_file_for_external_eventsFilePath = "<FullPathToFile>\\BulkUploadEvents.csv"
  res_data = bulkUploadEvents.upload_file_for_external_events(upload_file_for_external_eventsFilePath)

```
**send_single_event.py** 
- Example to showcase Sending a single external event 
```bash
from sas_ci360_veloxpy import APIClient

  # Create an API client
apiClient = APIClient()

#Send Single Event
event_data = {
"eventName": "External_Event_Example",//Evenet name should exist in CI360.
"subject_id": "267756",
"testattr1": "test",
"testattr2": "test@sas.com"
}
res_data_event=apiClient.send_external_events(event_data)
```
    
**usage.py**
- This example shows App initialization and a never-ending loop (except for keyboard interrupt) that runs every 60 seconds to call an audience API and get the details.
 ```bash
from sas_ci360_veloxpy import initApp, ApiClient


def main():
    initApp("<FullPathToFile>\\sasci360veloxpy.ini")
    print("App Initiated")
    
    apiClient = ApiClient()
    res_data = apiClient.sync_get_audiences()


if __name__ == "__main__":
    main()
 ```

## FAQ

**Q1: What Python versions are supported?**  
A: This package supports Python 3.10 and above.

**Q2: How do I install the package?**  
A: You can install using pip and the provided wheel or source distribution:
```bash
pip install path\to\your\package.whl
```

**Q3: Do I need to upload the package to PyPI for testing?**  
A: No, you can share the built files directly for local installation.

**Q4: How do I activate the virtual environment?**  
A:  
- On Windows:
  ```bash
  .\venv_sas_ci360_veloxpy\Scripts\activate
  ```
- On macOS/Linux:
  ```bash
  source venv_sas_ci360_veloxpy/bin/activate
  ```

**Q5: Where can I find usage examples?**  
A: See the [Usage](#usage) section above for sample code.


**Q6: Which API this API client supports?**  
A: 

 **API support in Phase 1**
- Marketing Audience (`/marketingAudience`)
- Marketing Gateway (`/marketingGateway`)

 **Planned API support in Phase 2**

- Marketing Data (`/marketingData`)
- Marketing Execution (`/marketingExecution`)

 **Planned API support in next Phases**

- Container Image Management (`/marketingContainerImage`)
- Copy Item (`/marketingPromotion`)
- Digital Assets (`/marketingDigitalAssets`)
- Marketing Administration (`/marketingAdmin`)
- Marketing Design (`/marketingDesign`)
- Plan (`/marketingPlan`)
- System for Cross-Domain Identity Management (SCIM) (`/scim`)

**Q7: How to support logging?**  
A: Refer to examples marketing_gateway.py
    Add this snippet

```bash
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("Logs_Client_API.log"),
        logging.StreamHandler()
    ]
)


logger = logging.getLogger("ClientRootLogger")
```
and then use this in method or file

```bash
    logger.info("res send_external_events %s", data)
```
Sample LoggerFile Generated
```bash
2025-08-01 14:13:58,511 [INFO] Client_Module: Init App Data None
2025-08-01 14:13:58,512 [INFO] sas_ci_360_veloxpy.MarketingGatewayApi: Calling upload bulk file: path\examples\BulkUploadEvents.csv
2025-08-01 14:13:58,513 [DEBUG] sas_ci_360_veloxpy.MarketingGatewayApi: File path for bulk upload: path\examples\BulkUploadEvents.csv
2025-08-01 14:13:58,513 [DEBUG] sas_ci_360_veloxpy.MarketingGatewayApi: Getting S3 URL for batch upload external event
2025-08-01 14:13:58,513 [DEBUG] sas_ci_360_veloxpy.MarketingGatewayApi: Calling get_signed_url_for_batch_upload_external_event to get S3 URL
2025-08-01 14:13:58,513 [INFO] sas_ci_360_veloxpy.MarketingGatewayApi: Calling batch upload external event
2025-08-01 14:13:58,514 [DEBUG] sas_ci_360_veloxpy.MarketingGatewayApi: Arguments details for batch upload external event: {'authType': 'static_jwt', 'path': '/marketingGateway/bulkEventsFileLocation', 'method': 'POST', 'headers': {'Authorization': 'Bearer {token}', 'Content-Type': 'application/json'}, 'data': {'applicationId': 'eventGenerator', 'version': '1'}}
2025-08-01 14:13:58,514 [DEBUG] sas_ci_360_veloxpy.MarketingGatewayApi: Converting data to JSON format for batch upload external event
2025-08-01 14:13:58,515 [DEBUG] sas_ci_360_veloxpy.MarketingGatewayApi: Making API request to get S3 URL for batch upload external event
2025-08-01 14:13:58,515 [DEBUG] asyncio: Using proactor: IocpProactor
2025-08-01 14:13:58,518 [DEBUG] sas_ci_360_veloxpy.SASCI360VeloxPyBaseService: Token inside base service for calling an API
2025-08-01 14:13:58,518 [INFO] sas_ci_360_veloxpy.SASCI360VeloxPyBaseService: Token inside base service for calling an API
2025-08-01 14:13:58,519 [DEBUG] sas_ci_360_veloxpy.SASCI360VeloxPyBaseService: No token provided, fetching token based on authType
2025-08-01 14:13:58,520 [INFO] sas_ci_360_veloxpy.SASCI360VeloxPyBaseService: Fetching static JWT token
2025-08-01 14:14:00,036 [INFO] ClientModuleLogger: Starting single event upload
2025-08-01 14:14:00,037 [INFO] sas_ci_360_veloxpy.MarketingGatewayApi: Calling send external single event: {'eventName': 'External_Event_Example', 'subject_id': '267756', 'testattr1': 'test', 'testattr2': 'test@test.com'}
2025-08-01 14:14:00,038 [DEBUG] sas_ci_360_veloxpy.MarketingGatewayApi: Arguments details for sending external single event: {'authType': 'static_jwt', 'path': '/marketingGateway/events', 'method': 'POST', 'headers': {'Authorization': 'Bearer {token}', 'Content-Type': 'application/json'}, 'data': '{"eventName": "External_Event_Example", "subject_id": "267756", "testattr1": "test", "testattr2": "test@test.com"}'}
2025-08-01 14:14:00,038 [DEBUG] sas_ci_360_veloxpy.MarketingGatewayApi: Making API request to send external single event

```

Note: This is sample example, please configure according to your requirements

**Q8: How to use the Logging utility?**  
A: Please refer below snippet how to use it.

```bash
from sas_ci360_veloxpy.utils.logging import SASCI360VeloxPyLogging
import configparser
import sys 

loggingFramework =  SASCI360VeloxPyLogging()

loggingFramework.startLogger()
loggingFramework.writeLogMessage("This is a test log message at TRACE level for default log.", "TRACE")
loggingFramework.writeLogMessage("This is a test log message at DEBUG level for default log.", "DEBUG")
loggingFramework.writeLogMessage("This is a test log message at INFO level for default log.", "INFO")
loggingFramework.writeLogMessage("This is a test log message at WARN level for default log.", "WARN") 
loggingFramework.writeLogMessage("This is a test log message at ERROR level for default log.", "ERROR")
loggingFramework.writeLogMessage("This is a test log message at CRITICAL level for default log.", "CRITICAL")
loggingFramework.writeLogMessage("This is a test log message at undefined logging level for default log.", "SYS")
loggingFramework.stopLogger()

```

**Q9: How to call any API?**  
A: Please refer the  [Usage](#usage) `usage_marketing_audience.py` and `marketing_gateway.py`
