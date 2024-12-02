# SAS CI360 Audit Data Download using Python

## Overview
The 360 Audit Data Records download utility is very useful for those looking to download Audit data from 360. THe user would not need to know any API or worry about the parquet file format, as this utility will help download the data in both parquet as well as csv formats. These csv files can then be imported in any database for further processing and analysis.

## Table of Contents
Welcome to the CI360 Audit Data Records download utility. This program helps download the Audit data from 360 in parquet and csv formats.

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
	- [Credentials.ini](#credentials.ini)
	- [Config.ini](#config.ini)        
- [Deployment](#deployment)
	- [Program Deployment](#program-deployment)	
- [Execution](#execution)
	- [How to execute?](#how-to-execute)    		 
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)
- [Additional Resources](#additional-resources)
	- [360 Documentation](#360-documentation)
	- [Apache Parquet Documentation](#apache-parquet-documentation)
		
## Prerequisites 

 - Appropriate 360 license
 - 360 General Access Point (Agent)
 - 360 API User
 - Python 3, preferably latest available version 
 - Python libraries (Run the "Install Python Libraries.bat" file in the repository)
	- configparser
 	- pandas
	- fastparquet
	- requests
	- jsonpath

## Configuration

In this section, we will have a look at the two configuration files that are required to run this utility, namely 'credentials.ini' and 'config.ini'.

### Credentials.ini

This file contains the credentials that are used in this program.
 - apiuser = API-<tenant_moniker>-<name of apiuser>.
   360 API user created for accessing the APIs. Create one in 360 -> General Settings -> API Credentials. 	
 - apisecret = This is the secret key/password for the API User as obtained from 360.
 - staticjwt = Use an Access Token created inside your General Access Point. Set the Access Token expiry to be as long as desired, knowing that it will need to be recreated and updated here after expiry.

### Configuration.ini

The parameters efined in this file are required for the program to run correctly. Any mistakes in configuring these parameters will lead to unexpected results or failure to run. Lets look at each of the parameters in this file.
 - gatewayhost = This is the External gateway host for your tenant viewable in General Settings -> Access Points.
 - dataRangeStartTimeStamp = Enter the start datetime from which you want to download the Audit data, in UTC/Zulu format. e.g.:2024-10-07T01:00Z
 - dataRangeEndTimeStamp = Enter the end datetime until which you want to download the Audit data, in UTC/Zulu format. e.g.:2024-10-07T10:00Z
 - downloadparquet = Set to "Yes" if you want to download the parquet files, else set to "No".
 - outputformat = As for now this is defaulted to "CSV" only.
 - logsdir = Directory for saving logs. Defaulted to ".\logs\". 
 - outputdir = Directory for saving parquet files that are downloaded from 360. Defaulted to ".\output\".
 - logging = Set to "true" if you want the program to write logs, else set to "false".
 - loglevel = Set the log level for the porgram. Possible values are: DEBUG, INFO, WARNING, ERROR, CRITICAL.
 

## Deployment

In this section, we will learn how to deploy this utility to your server. 

### Program Deployment

- Download the 360 Audit Data Download repository from gitlab/github.
- Copy the folder to any specific drive or directory as required.
- Configure the program by following steps as described in the previous section.
- Make sure the directory has sufficient authorization for the program to write files in it.

## Execution

In this section, we will learn how to execute this utility to download Audit data from 360. 

### How to execute?

- Once, you have deployed this program to a specific directory on the server and finished configuring as per the instructions given here, you are ready to execute the program to download Audit data from 360.
- Open command prompt with administrator privileges and change directory to this program's directory on the server.
-  Once in the program directory, run the following command to execute the program:
		python 360AuditDataDownload.py
- You will notice the program exit after execution completes. Do check the logs directory for logs.
- The 360 Audit Data in csv format will be saved to the output directory as defined in the configuration file.
- You can configure the program to run on a schedule, say daily at a given time, by using the Windows scheduler.
- You may have to write code to automatically import the CSVs to a database of your choice. 

	
## Troubleshooting

In this section, we will see how to fix some common issues related to this utility. 

- Utility not downloading data
	- Check configuration file, specifically tenant gateway host.
	- Check credentials file and ensure the values are correct and in sync with whats on the 360 tenant.
	- Check log files and look for any errors in program execution.
	- Make sure general agent is active and the access token has not expired.

## Additional Resources

### 360 Documentation
- 360: https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintwlcm/home.htm 
- About Audit Records in 360: https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/ch-audit-records.htm 
- Downloading Audit records in 360: https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/n0gnofoerdhdmtn107smymifowh9.htm 
- Schema for 360 Audit Records: https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/n1kap54o69x9nyn1s150fo7m7z3c.htm 
- SAS BI Web Services: https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/audit-records-parquet.htm 

### Apache Parquet Documentation
- Apache Parquet: https://parquet.apache.org/docs/ 







