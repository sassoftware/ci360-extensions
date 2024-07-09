/**********************************************************************************************************************
PROGRAM: SAS CUSTOMER INTELLIGENCE 360 REST API Utilities DS2 Package
DESCRIPTION: The SAS CI360 Utilities package is a helper tool that will aid App Devs to integrate with 
			 360 without needing to implement any of the 360 REST API. The utilities package will make 
			 calling the 360 REST API as easy as just calling a function/method with necessary parameters. 
			 The configuration file for the package will include all necessary details required to authenticate 
			 and connect to the desired 360 tenant.  
VERSION: 4.3
DATE MODIFIED: 25-APRIL-2024
AUTHOR: GLOBAL CUSTOMER INTELLIGENCE ENABLEMENT TEAM

#Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0
**********************************************************************************************************************/

/*** #PARAMETER CONFIGURATION FILE ***/

%global CI360_ENV TENANT_ID APPLICATION_ID 
        ACCESS_POINT ACCESS_POINT_SECRET AGENT AGENT_SECRET
        API_USER API_SECRET
        CUST_PROXY_URL CUST_PROXY_USER CUST_PROXY_PWD        
        SAS_UTILITY_LIBRARY SAS_UTILITY_LIBPATH SAS_UTILITY_FILENAME
;
 
/* CI360-specific variables */
%let CI360_ENV = training.ci360.sas.com;						 /* SAS AWS hostname suffix */
%let TENANT_ID = xxx;								 			 /* Tenant ID from 360 UI */
%let APPLICATION_ID = ENGAGEDIRECT;								 /* Application ID from 360 UI */
%let ACCESS_POINT = ExternalEvents;							 	 /* The name of the general access point defined in 360 UI */
%let ACCESS_POINT_SECRET = xxxx; 		                         /* The client secret associated with the general access point from 360 UI */
%let AGENT = &ACCESS_POINT.;                                     /* Set older version macrovar values */
%let AGENT_SECRET = &ACCESS_POINT_SECRET.;                       /* Set older version macrovar values */
%let API_USER = 'APIxxx';	 							 		 /* API User name defined in 360 UI */
%let API_SECRET = 'xxx';								 		 /* API User secret from 360 UI */

/* Optionally set Proxy Server Values */
/* PROXY URL should be in the form http://host:port without quotes */
%let CUST_PROXY_URL = http://inetgw.unx.sas.com:80;
%let CUST_PROXY_USER = ;
%let CUST_PROXY_PWD = ;


/* SAS CI360Utilities Details */
%LET SAS_UTILITY_LIBRARY = CI_LIB;									/* the location of the CI360Utilities DS2 package or where it will be created */
%LET SAS_UTILITY_PATH = D:\SAS\Contexts\Banking\Jobs\common\;		/* where to create the CI360Utilities package - normally same as include path */
%LET SAS_UTILITY_FILENAME = CI360Utilities.sas;
