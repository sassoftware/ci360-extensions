# GDPR Remove Customer Cloud Data

## Overview
The GDPR Remove Customer Cloud Data provides a custom script that deletes records from the CI360 identity table via API (e.g customers that rejected gdpr approval or have been replaced in on-prem data). The code takes a list of customer identifiers created on-premise and sends them to the API for deletion. CI360 then removes customer records from CI360 cloud data.


### Pre-requisites
- Base SAS
- Database and libname credentials
- CI360 gateway host and access point credentials
- Table with records for deletion (i.e. **CI360_GDPR_DELETE** table) including rules on when to delete the records
- Scheduling software (optional) (i.e Cron, LSF) 

## Installation
When implementing this process, you will need to review and/or adjust the following:
1. Download the project files into the on-premise server.
2. Create an Access Point in the CI 360 UI and get the tenant ID and secret for your JWT authentication.
3. Edit the **initialize_parameters.sas** file and update tenant, database, and other relevant variables.
4. Edit the following codes to reflect the appropriate logic to identify data for GDPR processing:
    - **gdpr_delete_run.sas** - _lines 15 to 22_
    - **gdpr_delete.sas** - _lines 18 to 24, 31 to 38_
5. Run initial tests for **gdpr_delete_exec.sas**. Ensure that identities were previously uploaded to CI360 and that table pre-requisites have the proper data. Deletion of identity records may be checked via the CI360 UI through _Configuration > General Settings > Content Delivery > Diagnostics_.
6. Schedule **gdpr_delete_exec.sas** as needed. 

### File Overview
The following files are included in the main folder of this project:

- **initialize_parameters.sas**
  - Contains sensitive information like user names, passwords, and the database LIBNAME statement.
  - MUST be customized for all customers.

- **gdpr_delete_exec.sas**
    - Standard .sas file executed on the command line to run the process

- **gdpr_delete.sh**
    - shell script for the gdpr delete process
    - script can be scheduled to run regularly through the available scheduling software (i.e. Cron)

Code included in the macro folder:
- **check_tables_ddl.sas**
    - code to initialize required tables for the project (_TABLE_ADMIN, CI360_EVENT_LOG, CI360_GDPR_DELETE_)

- **gdpr_delete_run.sas**
    - performs the initial checks on availability of tables for the gdpr delete process
    - checks the readiness of the **CMDM.CI360_GDPR_DELETE** table. Also checks if the GDPR_delete process was ran for the day. Script will continue only if the script has not been executed for the day. 

- **gdpr_delete.sas**
    - main code for the gdpr_delete process
    - reads the corresponding data from the table **CMDM.CI360_GDPR_DELETE** and uses proc HTTP to send them in JSON format to the Cloud.

### Customer-Specific Variables

These variables should always be updated based on your customer environment.

| Variable | Default Value | Description |
| ------ | ------ | ------ |
| CI360_server | _custom_ | CI360 Server path |
| DSC_TENANT_ID | _custom_ | Tenant ID shown on Access Point in 360 UI |
| DSC_SECRET_KEY |  _custom_ |  Access Point secret generated in 360 UI |
| diBatchControlIn | TABLE_ADMIN | Administration table that contains status of table updates |
| gdprDeleteTable | CI360_GDPR_DELETE | Table containing subject_ids for deletion |
| eventLogTable | CI360_EVENT_LOG | Event log table where gdpr delete process logs will be written |
| IB_METADATALIB | CMDM | Inbound data metadata library |
| IB_DBOLIB | CMDM | Inbound data library |
| IB_STAGELIB | CMDM | Staging library source of the gdpr delete table |


## Additional Resources

[SAS Help Centre](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/ch-gdpr-support.htm)

