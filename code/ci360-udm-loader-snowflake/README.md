# CI360 UDM Loader for Snowflake

## Overview

The Customer Intelligence 360 Loader for Snowflake is a utility that can automate the download of UDM data from Customer Intelligence 360 and then seemlessly load this into Snowflake.  This utility includes scripts to create the UDM tables, pipes and file formats in your Snowflake environment.  The loader utilises the SAS provided python download client within a docker container.  The download client stores the compressed CSV (.zip) files in an S3 bucket and then triggers Snowflake to load them from S3 using the configured Snowflake 'Stage'.  

These instructions are only detailing the steps required for AWS S3 but could easily be extended to Microsoft Azure or Google Cloud providers.

## Prerequisites

It is advisable that you have a basic understanding of the following topics before using the utility:
- Ability to create STAGES, PIPES, TABLES in Snowflake via UI or Snowflake’s worksheet.
- Basic knowledge of AWS S3 and IAM Roles
- Experience of using the Customer Intelligence 360 python download client (ci360-download-client-python) - documenation can be found here on [SAS Github][1]
- Understanding Linux as well as Docker commands

### Docker Environment
Make sure you have the ability to:
- Create images
- Create and run containers

### Customer Intelligence 360
Make sure you have the following information at hand:
- Download URL ``` https://extapigwservice-<server>/marketingGateway/discoverService/dataDownload/eventData/ ```
- Access Point Tenant ID ( can be found in CI360 UI – General Settings – Access Point – click on your Access Point)
- Access Point Secret ( can be found in CI360 UI – General Settings – Access Point – click on your Access Point)

### Snowflake
Make sure you have the following information at hand:
- Key pair for authentication
- ACCOUNT name
- WAREHOUSE name
- DATABASE name
- SCHEMA name
- prefix name
- STAGE name
- FILE FORMAT name
- TABLE names
- PIPE names
- ROLE name


### AWS 
Make sure you have the following information at hand:
- REGION name
- S3 bucket
- IAM user with secret 
	

## Configuring the Customer Intelligence UDM Loader for Snowflake

Before you can start using the data loader you'll need to prepare your Snowflake environment and update the configuration and environment files to customise them for your site.  Below is a high level list of the steps that are required.

### AWS Configuration

1. 	Create or identify an existing AWS IAM user that will be used to access the S3 bucket.
2. 	Create or identity an existing S3 bucket to download the UDM data to.  Update the bucket policy to allow the above IAM user access (ListBucket, GetBucketLocation, ListBucketMultipartUploads, DeleteObject, GetObject, PutObject, PutObjectAcl)
3. 	Create the following folders in your S3 bucket
    - dsccnfg
	- dscdonl
	- dscextr
	- dscwh
	- log
	- sql

### Snowflake Configuration

1.	Create the UDM tables in Snowflake that will be used to load the UDM data.  You can find sample UDM scripts in the /sql folder of the loader utility (e.g. detail_snapshot_schema_v9_create_table.sql) It's recommended that you prefix these tables so that you can more easily filter on them in Snowflake.
2.	Create STAGE in Snowflake that connect to your S3 bucket. (e.g. snowflake_create_stage.sql)
3.	Create FILE FORMAT that match the delimiter used when downloading the UDM data.  Default for the provided scripts is a tilde '~'. (e.g. snowflake_create_file_format.sql)
3.	Create PIPES for each of the tables created in step 1 referencing your STAGE and FILE FORMAT. (e.g.detail_snapshot_schema_v9_create_pipes.sql)

### Docker Configuration

1.	Copy or clone this repository to a host that can access your docker environment.
2.	Update env.list to customise the settings for your environment (AWS bucket and Snowflake connection details).
3.	Update .passwd-s3fs in the config folder to specify your AWS IAM key and secret in the following format KEY:SECRET  e.g. AKuplkVTR4U75Hfdgtyu:tryplbBdggF8GC5OOFEhkcPnZPYt32tXABStyujk
4.	Update .passwd-snowflake in the config folder to specify your iss key, sub key, username and password.  Keep it in single line and separate by colon :  e.g. <ras_public_fp>:<username>:<password>
5.	Add your Snowflake RSA private key to .snowflake_rsa_private_key.pem
6.	Update config.txt in the config folder with your CI360 agentName, tenantId, secret and baseURL e.g. https://extapigwservice-eu-prod.ci360.sas.com/marketingGateway/discoverService/dataDownload/eventData/ for the EU production environment.
7.	Use the following docker command to build the image: 
	```sh
	docker build . --tag <prefix>-ci360-snowflake:1.0
    ```

## Running the Customer Intelligence UDM Loader

You run the Customer Intelligence UDM Loader by spinning up a docker container with a number of invocation options.  The following options are available on the command line:

-m {detail | dbtReport | snapshot}
-st {start_date}
-et {end_date}
-ct {cdm | discover | engagedigital | engagedirect | engagemobile | engageweb | engageemail | optoutdata | plan }
-svn {schema_version}
-cd {delimiter}

For example if you wanted download the 'detail' mart to your Snowflake environment using schema 9 for 9th January 2022 using the tilde as a delimiter you would run this command.
```sh
docker run -it --privileged --rm --env-file config/env.list <prefix>-ci360-snowflake:1.0 "python ci360_udm_loader.py -m detail -st 2022-01-09T00 -et 2022-01-09T23 -ct discover -svn 9 -cd '~'"
```

[1]: https://github.com/sassoftware/ci360-download-client-python "SAS Customer Intelligence 360 Download Client: Python"