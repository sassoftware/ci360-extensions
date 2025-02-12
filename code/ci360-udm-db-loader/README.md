# CI360 UDM Database Loader

## Overview
The ci360-udm-db-loader utility loads data from the CI 360 Unified Data Model (UDM) into a relational database management system (RDBMS), The data can also include the CI360 Common Data Model (CDM) which is a part of the UDM. 
The utility is capable of automatically generating database-specific Data Definition Language (DDL) scripts and load code. The DDL scripts are used for creating UDM tables in the database, while the load code is responsible for populating these tables with data. This streamlines the process of setting up and managing data pipelines for UDM data.

## Supported target databases 
- Microsoft SQL Server

This tool will support multiple target databases. Next up are SQL Server on Azure and Oracle.

## Supported UDM Schema Version
This tool contains metadata for UDM schema version 16. 

## Pre-requisites

To set up the ci360-udm-db-loader, you need access to a supported database. Also note the ci360-udm-db-loader does not download the UDM data. It is built and tested to use data downloaded via the ci360-download-client-sas utility. 

So before you deploy this tool make sure you have
1.	https://github.com/sassoftware/ci360-download-client-sas deployed 
2.	access details for a the supported target database that allow to  create tables and load data. Foresee a separate schema for the UDM tables and optionally a second schema for temporary tables that get created and dropped during the load process.

> **WARNING:**
> Downloaded SAS datasets are deleted after each succesfull upload to prevents unnecessary reloading. The download tool will recreate tables as required.

> **TIP:** This tool should NOT run at the same time as the download tool. Schedule it to run after downloading. 

> **TIP:** A dataset that remains undeleted after upload indicates an upload error. Check the log.

## Configuration

Unzip and copy the tool to a location on you SAS server, where you also have the ci360-download-client-sas deployed. 
Edit these sections in the config.sas file, located in the config folder. 

%let slash=/; /* Set to / for Linux or \ for Windows */

#### Tenant Configuration
Define tenant details and schema version for which you want to use this utility. T
- %let DSC_TENANT_ID=%str(< Tenant ID Value>);
- %let DSC_SECRET_KEY=%str(< Secret Key Value >);
- %let External_gateway=https://< external gateway host >/marketingGateway;
- %let SCHEMA_VERSION=16;

### Path Configurations
These downloaded datasets will act as an input and will be stored under { UtilityLocation}/data folder or any location which can be set in config.sas file.

- %let utilityLocation=; /* path of the main folder of this tool, so the parent of the config folder */
- %let downloadutilitylocation=; /* path of the main folder of ci360-download-client-sas, so the parent of the data folder */


#### Database Details
Define the third-party database name and the credentials to access the database.

  > **NOTE:** These are database specific details its usage may differ according to the database you are working on.

- %let database=MSSQL; /* MSSQL */
- %let trglib=Target; /* Provide Target Library name */
- %let dbname=sqlsvr; /* Provide Database */
- %let dbsrc=mydbsrc; /* Specifies the Microsoft SQL Server data source to which you want to connect*/
- %let dbschema=myschema; /* provide database schema detail */
- %let dbuser=myuser;/* lets you connect to database with a user ID.*/
- %let dbpass=mypass; /* specifies the database password that is associated with your user ID.*/
- %let dbpath=; /* NOT USED with MSSQL - database path value */
- %let dbdns=mydbdns; /* NOT USED with MSSQL - database DNS name */

If you prefer to use different database schema for staging data then please provide staging schema details. By default it uses Target schema details. 

### Time zone
The UDM makes datetime values available in GMT. Use this configuration to adapt the time zone.

- %let timeZone_Value=AMERICA/NEW_YORK; /* Provide time zone specific value for convertion of datetime fields into target tables */

For more information on time zone and its values please see : [Time Zone Info and Time Zone Names](https://go.documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/lesysoptsref/n13ytdu4ohkwoln1gtu6byka5lpd.htm)


## Configuration in udmlaunch.sas
In udmloader.sas file from udmloader_launch folder provide the location of the config.sas in this line of code.
- %include "{utilityLocation}\config\config.sas";

## File Overview
The **udmloader_launch** folder of this project includes below file:

- **udmloader.sas**
  - This the main macro which will launch the utility. See the usage section below

The **config** folder contains below content:

- **config.sas**
  - This file contains the environment specific configurations.

- **METADATA_TABLE.csv**
    - This csv is UDM schema specific as it identifies which columns are a part of the primary key or each table. This CSV will be updated with every release of a new UDM schema version.

- **datatypes.sas7bdat**
    - This data set contains datatype mapping for supported databases. This will map the schema datatypes to the database specific datatypes. This table will be updated as new database platforms are supported.

The **macros** folder contains all the .sas code files.

The **code** folder will have the generated DDL and load code files which will be used for execution process for each supported database. The code files for the latest supported schema version are provided.

# Running the ci360-udm-db-loader

The ci360-udm-db-loader should be scheduled to run after the ci360-download-client-sas utility has downloaded new data. You can create one command-line or shell script that runs both utilities in sequence. 

However, before you can load the data, the target tables need to be created. Run **EXECUTEDDL** from  the **Running utility for Interactive Execution** topic below.

Next go to **Running utility for Batch Execution** to load the tool in batch 

If you want to upload the lates supported schema version of the UDM tables you can  directly use the default generated ddl and load code from code folder. If not use the parameters **CREATEDDL** and **CREATEETLCODE** as described below.

### Running utility for Interactive Execution
To run the utility interactively (e.g. via SAS Studio or SAS Enterprise Guide), edit **udmloader.sas** from udmloader_launch folder, update **%Let sysparameter=XXXXX ;** variable with one of the below parameters and run it.

- **GENERATEMETADATA** : If you want to load an older UDM Schema version, use this parameter to generate metadata from UDM Schema details. Next, use this metadata to generate the DDL and the load codes. 
 
- **CREATEDDL/CREATEETLCODE** : This will  generates DDL or load code.

- **EXECUTEDDL**:This will execute the generated database specific DDL. This will pick the generated DDL code from previous step and create the tables in target Database .

   > **NOTE:** Run EXECUTEDDL only once before loading data for the first time. If a correction is needed, drop all tables before re-executing

- **LOADDATA** : This will execute the generated database specific ETL code file. It will Insert/update downloaded data into database specific target tables (you can schedule this periodically depending on the frequency of data download by using batch process). 

### Running utility for Batch Execution

To run this utility in batch mode you need to execute below command through command promt:

**{SASHOME}/sas –sysin {UDMLoader_Location}/UDMLoader.sas -sysparm {PARAMETER} -log {UDMLoader_Location}/UDMLoader.log**

**PARAMETER** value would be LOADDATA to load data into target database.

## Additional Resources
To get a better understanding of the inner workings of this utility please check out the **ci360-udm-db-loader_design_document.pdf**  in this package.

