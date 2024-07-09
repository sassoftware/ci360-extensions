# SAS CI360 REST API Utilities DS2 Package

## Table of Contents
Welcome to the SAS Customer Intelligence 360 (CI360) REST API Utilities DS2 Package. This is a collection of utility functions for interacting with the CI360 REST API from SAS code.

- [360 Utilities Package](#overview)
    - [Prerequisites](#prerequisites)
- [Configuration - SASCI360ParamFile.sas](#configuration)
    - [Initialization - Use in SAS Code](#initialization)
- [Examples - Sample Code](#examples)
- [External Methods](#external-methods)


## Overview

The SAS CI360 Utilities package is a helper tool that will aid App Devs to integrate with SAS CI360 without needing to implement any of the CI360 REST API. The utilities package will make calling the REST API as easy as just calling a function/method with necessary parameters. The configuration file for the package will include all necessary details required to authenticate and connect to the desired CI360 tenant. 

### Prerequisites

- Base SAS
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

## Configuration

The SASCI360ParamFile.sas contains the required parameters necessary for the code to initialize, authenticate and connect to the 360 tenant. Make sure you update this file before using the utilities package in code. 

The following parameters are available in this file:
```
/* 360 Tenant Details */
CI360_ENV = abcd.ci360.sas.com;			/* SAS AWS hostname suffix */
TENANT_ID = xxxx;				        /* Tenant ID from 360 UI */
APPLICATION_ID = ENGAGEDIRECT;			/* Application ID from 360 UI */
ACCESS_POINT = abcd;			        /* The name of the general access point defined in 360 UI */
ACCESS_POINT_SECRET = xxxx; 		    /* The client secret associated with the general access point from 360 UI */
AGENT = &ACCESS_POINT.;                 /* Set older version macrovar values */
AGENT_SECRET = &ACCESS_POINT_SECRET.;   /* Set older version macrovar values */
API_USER = 'APIxxx';	 			    /* API User name defined in 360 UI */
API_SECRET = 'xxxx';				    /* API User secret from 360 UI */

/* You can optionally set proxy server values if you are behind a proxy */
CUST_PROXY_URL = http://abcd.pqr.sas.com:8080; /* PROXY URL should be in the form http://host:port without quotes */
CUST_PROXY_USER = abcdpqrsas;                  /* Proxy Username */
CUST_PROXY_PWD = ********;                     /* Proxy Password */

/* SAS CI360Utilities Details */       
SAS_UTILITY_LIBRARY = CI_LIB;	   			             /* The location of the CI360Utilities DS2 package or where it will be created */
SAS_UTILITY_PATH = D:\SAS\Contexts\Banking\Jobs\common\; /* Where to create the CI360Utilities package - normally same as include path */
SAS_UTILITY_FILENAME = CI360Utilities.sas;
```

### Initialization
In this section you will see how to embed the utilities package in your SAS code.
```
/* Set variables */
%let sas_utility_library = CI360UTL;	    /* the location of the CI360Utilities DS2 package or where it will be created */
%let sas_utility_path = &sas_include_path;	/* where to create the CI360Utilities package - normally same as include path */
%let sas_utility_version = 4;				/* The minimum version of the CI360Utilities DS2 package required */

/* Include utilities code file */
%include "&sas_utility_path/CI360Utilities.sas";
/* Call create package method */
%CreatePackage();
```

## Examples
Here are a couple of examples on how to call the methods in the utilities package.
```
/* Sample method calls */
/* Check version method */
	PROC DS2 NOLIBS CONN="((DRIVER=BASE;CATALOG=&sas_utility_library;SCHEMA=(NAME=&sas_utility_library;PRIMARYPATH={&sas_utility_path}));(DRIVER=BASE;CATALOG=WORK;SCHEMA=(NAME=WORK;PRIMARYPATH={%sysfunc(pathname(work))}));)";
		data work.VersionCheck (OVERWRITE=YES);
			declare package &sas_utility_library..CI360Utilities ci360(%tslit(&ci360_env), &api_tenant, &api_agent, &api_secret,sas_log_level);
			declare int version;

			method run();
				version = ci360.MajorVersion(); /* This way, here you can call any external method from the utilities package */
			end;
		enddata;
		run;
	QUIT;

    /* Request upload path method */
    PROC DS2 NOLIBS CONN="((DRIVER=BASE;CATALOG=&sas_utility_library;SCHEMA=(NAME=&sas_utility_library;PRIMARYPATH={&sas_utility_path}));(DRIVER=BASE;CATALOG=WORK;SCHEMA=(NAME=WORK;PRIMARYPATH={%sysfunc(pathname(work))}));)";
			data WORK.UploadPathResult (OVERWRITE=YES);
				declare package &sas_utility_library..CI360Utilities ci360(%tslit(&ci360_env), &api_tenant, &api_agent, &api_secret, &sas_log_level);
				declare varchar(2048) url;

				method run();
					url = ci360.RequestUploadPath('fileTransferLocation');
				end;
			enddata;
			run;
		QUIT;    
```

## External Methods

### CreateDescriptor
This method creates a data table in 360.
- Parameters:
	- strDescriptorJSON varchar(32767)  /* Should contain the table structure in CI360 table json format */
- Return Type:
	- varchar(2048)

### RequestUploadPath
Ask CI360 for a location to upload the specified type of file.
- Parameters: 
	- strMethod varchar(100) /* Values could be: bulkEventsFileLocation or fileTransferLocation */
- Return Type: 
	- varchar(2048)

### RequestUploadPath
Ask CI360 for a location to upload the specified type of file.
- Parameters: 
	- strMethod varchar(100) /* Values could be: bulkEventsFileLocation or fileTransferLocation */
	- strApplicationID varchar(100) /* Name of an External Application created in CI360 */
- Return Type: 
	- varchar(2048)

### ImportData
Method to validate completing of a file import in CI360.
- Parameters: 
	- varchar(256) strUploadName
	- varchar(2048) strDescriptor 
	- varchar(2048) strLocation 
	- varchar(36) strUpdateMode 
	- int blnHeaderRow 
	- int intWaitMins
- Returns: 
	- int

## Additional Resources

* [CI360 REST API DS2 Utilities Documentation](CI360DS2Utilities_Documentation.pdf)
