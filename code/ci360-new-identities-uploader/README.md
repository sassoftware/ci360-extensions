# Upload New Identities to CI360

## Overview

This process automates a recurring load of identities from an on-premises database to CI360. It tracks what has already been sent and only new identities are uploaded each time this is run.

This process is implemented via standard .sas files executed on the command line (or scheduled to be run by the sas command)

## Installation
### Getting Started
When implementing this process, you will need to review and/or adjust the following:
* Create a Generic Agent in the CI 360 UI and get the tenant ID and secret for your JWT authentication.
* Get the latest ds2 utilities from code/ci360-api-ds2-utilities/CI360Utilities.sas, in this repository, and copy the .sas file to your deployment in the ci360-new-identities-uploader/util folder.
* Edit the initialize_parameters.sas file and update tenant, database, and other relevant variables.
* Ensure pre-requisites of CI360Utilities are installed. Add the following to /sas/sashome/SASFoundation/9.4/bin/sasenv_local:
```bash
export MAS_M2PATH=/sas/sashome/SASFoundation/9.4/misc/tkmas/mas2py.py
export MAS_PYPATH=/usr/bin/python
```
* Adjust data descriptor, table, and index/key definitions to include the identity types the customer uses. 
* Adjust table DDL for the database engine and identitiy types being used.
* Update the database query in the generateidmap.sas macro based on the customer's data structures and the identity types being mapped.
* Update parameters (db_upload_list parameter in initialize_parameter.sas)and file output (sendidmap.sas) based on identity types being mapped. Note that order of attributes must align to the descriptor definition.


### File Overview
The following files are included in the main folder of this project:
* IdentityMap.json
  - Contains the JSON used to create a descriptor in CI 360.
  - This is a prerequisite and the descriptor MUST be manually created before running the identity mapping process.
  - MUST be customized to reflect the identity types used by this customer (and the layout of the CI360_IDMAP_PENDING and CI360_IDMAP_PROCESSED tabes.

* initialize_parameters.sas
  - Contains sensitive information like user names, passwords, and the database LIBNAME statement.
  - MUST be customized for all customers.

* idmapping_exec.sas
  - Standard .sas file executed on the command line to run the process
  - calls loadidentities.sas

Code included in the macro folder:
* check_tables_ddl.sas
  - Contains the db agnostic DDL required to create the two tables used by this process (CI360_IDMAP_PENDING and CI360_IDMAP_PROCESSED). 
  - MUST be customized and run manually for customer's standards for schema name, tablespaces, etc.
  - MUST be customized to reflect the identity types used by this customer.
  - ONLY use identity type column names (SUBJECT_ID, LOGIN_ID, CUSTOMER_ID) not customer's column names.
  - V_DIM_INDIVIDUAL should be replaced with the name of the primary subject table (e.g Customer) that will be used as the subject table for CI360
  - Sample data in the data folder of the repo can be used to test this code (but may need be adjusted if changes are made to columns in the tables)

* generateidmap.sas
  - Contains the logic used to query the database and build the list of identities to be sent to 360.
  - Main query MUST be customized to reflect the the columns and tables in customer's data mart and the identity types used by this customer.  
  - Remainder of this program should not require modification.

* sendidmap.sas
  - Contains the logic used to export from the IDMAP tables to a file and upload it to CI 360.
  - All 'Customer' type files uploaded are automatically processed for identities
  - This program should not require modification.

* idmappingprocess.sas
  - Performs the upload of the file of identities to CI360
  - Remainder of this program should not require modification.

* loadidentities.sas
  - Controller that orchestrates the extraction of new customer identifier from the on_prem primary subject table
  - Variables MUST be customized for each customer's environment
  - call idmappingprocess.sas


### Customer-Specific Variables

These variables should always be updated based on your customer environment.

| Variable | Default Value | Description |
| -------- | ------------- | ----------- |
| api_agent | *Custom* | Access Point agent name defined in 360 UI |
| api_tenant | *Custom* | Tenant ID shown on Access Point in 360 UI |
| api_secret | *Custom* | Access Point secret generated in 360 UI |
| ci360_env | use.ci360.sas.com | Change this as necessary based on which realm the tenant is in |
| descriptor_name | *Custom* | Name of the descriptor created for identity mapping |
| import_wait_mins | 120 | How long to wait before import to compelte before timing out |


### Process Variables 

These variables normally do not need to be changed, but can be adjusted if necessary.

| Variable | Default Value | Description |
| -------- | ------------- | ----------- |
| sas_utility_library | CI360UTL | the location of the CI360Utilities DS2 package or where it will be created |
| sas_utility_path | &sas_include_path | where to create the CI360Utilities package - normally same as include path |
| sas_utility_version | 4 | The minimum version of the CI360Utilities DS2 package required |
| sas_process_idmap_code | idmappingProcess.sas | Main orchestration process file |
| sas_generate_idmap_code | generateidmap.sas | needs to contain the GenerateIDMap() macro and set rc (1 | success) |
| sas_send_idmap_code | sendidmap.sas | needs to contain the SendIDMap() macro and set rc (1 | success) |
| sas_output_file | idmap_output | the base name to be used with this process for output files |
| sas_upload_file | &sas_include_path/idmap_upload.csv | full path to the file to use for uploads |
| sas_upload_code | &sas_include_path/upload_idmap.sas | full path of the dynamic code file to write to execute the upload |
| sas_db_libref_cust | idmapsrc | libref to use throughout when referencing ID Map source |
| sas_db_libref_idmap | idmapdb | libref to use throughout when referencing ID Map tables |
| sas_log_level | 1 | Possible log levels: 0=Errors Only, 1=Info, 2=Debug, 3=Trace (sensitive info obfuscated), 4=Trace (sensitive info plain text) |


## Running
  - Execute idmapping_exec.sas from the command line
  - Process is typically run after the primary subject table has been updated in the on_prem db

## Additional Resources

[SAS Help Centre](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/ch-data-identities.htm)

