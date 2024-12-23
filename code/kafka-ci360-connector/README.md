# Kafka to CI360 Connector

## Overview

Using Kafka to CI360 connector, CI360 can receive events from a Kafka environment. With simple configuration, events from a Kafka topic can be ingested into CI360 in the form of external events. These events can be used for personalization and customer journey orchestration.

## Prerequisites
- Python 3.7 or above
- Access to SAS Customer Intelligence 360 tenant and a configured access point:
    1. From the user interface, navigate to **General Settings** > **External Access** > **Access Points**
    2. Create a new access point if one does not exist
    3. Get the following information from the access point:  
       ```
        External gateway address: e.g. https://extapigwservice-<server>/marketingGateway  
        Name: ci360_agent  
        Tenant ID: abc123-ci360-tenant-id-xyz  
        Client secret: ABC123ci360clientSecretXYZ  
       ```

## Setup

1. Clone the repository to your local machine.

2. Navigate to the project directory.
    
3. Set up virtual environments for script (optional but recommended):
    ```bash
    python -m venv kafka-to-ci360-connector
    source kafka-to-ci360-connector/bin/activate   # On Windows: .\kafka-to-ci360-connector\Scripts\activate
    ```

4. Install dependencies for script:
    ```bash
    pip install -r requirements.txt
    ```

### Notes:
- Ensure the directory structure in your repository matches the paths mentioned in the `README.md`.

## Configuration
### Config.ini file

Find the config.ini file in the same directory where your Python script (script_name.py) is located.

Edit config.ini file in a text editor:

```
[CI360]
url = your_external_gateway_host
tenantID = tenant ID value from your access point configuration
clientSecret = client secret value from your access point configuration
default_event_name = Update with the name of the default event to be used when the event name is not present in the event received from Kafka.

[Kafka]
bootstrap_servers = your_kafka_server_address
topic = your_topic_name
groupid = your_group_id

```

Note: 
- Please create the external event in CI360 prior to running this script with the required attributes. You need to create both the default event specified in config and any other events that are expected from Kafka, based on mapping below.
- Replace placeholders like `your_external_gateway_host`, `your_tenant_id`, etc., with actual values.

### Field Mapping Configuration

The field_mapping.ini file is used to define the mapping between the incoming event keys and the modified keys that will be sent to CI360. This file allows you to specify how fields in the incoming JSON events should be renamed or transformed.

Example field_mapping.ini
```
[FieldMapping]
eventName = eventName
customerIncome = custIncome
customerExpense = custExpense
loanTerm = loanTerm
maritalStatus = maritalStatus
subject_id = custDetails.custnum
customerName = custDetails.custname
customerZip = custDetails.custzip
```

In this file:

The keys on the left side are the names of the fields after mapping.
The values on the right side are the original field names from the incoming events.
Keys containing dots (.) indicate nested JSON structures.

Nested JSON Handling: For nested JSON fields, use dot notation to specify the path. For example, custDetails.custnum indicates that custnum is a nested field within the custDetails object in the incoming JSON.

## Run the Script
Ensure the script, config.ini, and field_mapping.ini are updated and in the same directory. Then, run the script with the following command:
```bash
python Kafka_to_CI360_connector.py
``` 

### Logging
The script sets up logging to a file (script_log.log) and the console. It uses a rotating file handler to manage log file size and backups. These logs provide detailed information about the script execution and help in troubleshooting issues.


