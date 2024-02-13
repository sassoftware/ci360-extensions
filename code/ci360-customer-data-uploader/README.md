# Upload Customer Data to CI360 Cloud Datahub table

## Overview
This asset contains processes to prepare on-premises data for upload of Customer and identity information to CI360. An auxiliary table is used to check on records that are to be updated and uploaded to CI360. Once the final data tables are ready, the script facilitates the upload of both customer and web identity data through the use of several HTTP procedures and defined data descriptors.

Note: table names, structures and transformation logic for on-premises Customer data and Customer datahub table are to be adjusted as needed. 

### Pre-requisites
- Base SAS
- Python for token generation
- Database and libname credentials
- CI360 gateway host and access point credentials
- Source tables for Customer Data Upload
- Scheduling software (optional) (i.e Cron, LSF) 

## Installation
When implementing this process, you will need to review and/or adjust the following:
1. Download the project files into the on-premise server.
2. Update the following files to include tables and aggregation rules for the customer data upload, auxiliary table (to identify record upload status) and web identity upload:
   - **upload_customer.sas** (_lines 24 to 78, 84 to 90_)
   - **upload_customer_run.sas** (_lines 15 to 40_)
3. Create an Access Point in the CI 360 UI and get the tenant ID and secret for your JWT authentication (if not yet defined).
4. Create data descriptor for the Customer Data upload and Web Identity upload. Note that data descriptor definitions must be aligned to the final structure of the customer and identity data for upload.
5. Edit the **initialize_parameters.sas** file and update tenant, database, data descriptor names and other relevant variables.
6. Current logic has dependencies with the identities uploader asset (through CI360_IDMAP_PENDING table) to ensure that subject_ids are properly uploaded prior to uploading additional data. Edit the **upload_customer_run.sas** (_lines 33 to 35_) to remove dependencies as needed. 
7. Web Identity upload is optional and maybe commented out as needed. (_see upload_customer.sas line 278_)
8. Run initial tests for **upload_customer_exec.sas**. Ensure that data descriptors and table pre-requisites are aligned and have the proper data. Check logs for execution details. Customer and Web Identity upload may be checked via the CI360 UI through _General Settings > Data Collection > Table Management > Descriptor Import History_.
9. Schedule the **upload_customer_exec.sas**. 

### File Overview
The following files are included in the main folder of this project:

- **initialize_parameters.sas**
  - Contains sensitive information like user names, passwords, and the database LIBNAME statement.
  - MUST be customized for all customers.

- **upload_customer_exec.sas**
    - Standard .sas file executed on the command line to run the process

- **upload_customer.sh**
    - shell script for the customer upload process
    - script can be scheduled to run regularly through the available scheduling software (i.e. Cron)

Codes included in the macro folder:
- **check_tables_ddl.sas**
    - code to initialize required tables for the project (EventLogTable, customer table)

- **upload_customer_run.sas**
   - performs the initial checks on the readiness of the customer data source tables (source tables may vary for each implementation). 
   - executes the upload_customer.sas code if all of the following are observed:
      - the source tables are available for upload for the day. 
      - the upload_customer script has not run for the day. 

- **upload_customer.sas**
   - performs data preparation for customer data and identity upload base table.
   - creates an auxiliary table to identify records that need to be added or updated to CI360. The DateEffectiveChange column (_last_updated_ts_) helps facilitate the identification of new data points compared to those already uploaded.
   - creates a csv file from final table for upload (_customer_upl.csv_) and then uploads the data to CI 360 through several HTTP procedures and the defined data descriptor. 
   - creates an web identity table source from the final customer upload table. Uploads the web identity table to CI360 through the same steps done for the customer upload.

### Customer-Specific Variables

These variables should always be updated based on your customer environment.

| Variable | Default Value | Description |
| ------ | ------ | ------ |
| CI360_server | _custom_ | CI360 Server path |
| DSC_TENANT_ID | _custom_ | Tenant ID shown on Access Point in 360 UI |
| DSC_SECRET_KEY |  _custom_ |  Access Point secret generated in 360 UI |
| descriptor_cust | CUSTOMER | Name of data descriptor for customer upload |
| descriptor_identity | WEB_IDENTITY | Name of data descriptor for identity upload |
| IB_DBOLIB | CMDM | Inbound data library for datahub table|
| IB_BATCHLIB | CMDM | Inbound data library for batch processes |
| eventLogTable | CI360_EVENT_LOG | Event log table where gdpr delete process logs will be written |
| semaphoresTable | TABLE_ADMIN| Administration table that contains status of table updates|
| idmapPendingTable | CI360_IDMAP_PENDING | Status table for identity upload (optional for dependencies with separate identity upload process) |
| dataHubTable | DATAHUB| Auxiliary table for customer data uploaded to CI360 |
| custTable | INDIVIDUAL | Customer table source. May vary per project |

## Additional Resources

[SAS Help Centre](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/ch-data-import.htm)



