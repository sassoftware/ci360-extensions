/*-----------------------------------------------------------------------------
 Copyright © 2024, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 SPDX-License-Identifier: Apache-2.0
-----------------------------------------------------------------------------*/

/*****************************/ 
/* CI Migration Preparation Tool
/*****************************/ 
/* Program: 0_main.sas
/* Adjust the macro variables to your environment 
/*****************************/ 

%let metaserver=10.123.123.123; /* replace with host name or IP address of SAS Metadata Server */
%let metaport=8561; /* typically kept at 8561 */
%let uid=myuser; /* replace by user defined in SAS9 metadata that can access all campaigns */
%let pass={SAS005}ABCDE1234567890; /* Encode password via proc pwencode e.g., proc pwencode in="mypassword" method=SAS005; run; */
%let SASWebAppServer=&metaserver.; /* host name or IP address of SAS Mid tier Server */
%let davroot=http://&SASWebAppServer.:8080/SASContentServer/repository/default/sasdav/Customer%20Intelligence; /* Port may be 7980, but try 8080 first */

%let utillityFolder=C:\sas\ci-modernization-preparation-tool;  /* where you installed this tool */
%let dataFolder=&utillityFolder.\data; /* where the data and CSVs will land */
%let xmlFolder =&utillityFolder.\data\xml; /* where the extracted campaign xml files will land */
%let xml_file_replace=0; /* set to 1 to overwrite xml files 0 saves time in reruns */

/* Output data library */
libname madata "&datafolder."; /* no change needed */


/* For Section for 2_get_campaign_data.sas  */
%let metadata_root_folder=/CI/Financial Services/; /* Metadata root folder is the folder containing all compaigns of ONE business context. */
%let OS_specific_options =;                             /* use this for linux servers and comment out the next line */
%let OS_specific_options = options noxwait xsync xmin;  /* use this for windows servers */
%let JAVACMD=java; /* If you need to add a path, don't use Quotes in this  java command */
%let JAVACMD=C:\PROGRA~1\SASHome\SASPRI~1\9.4\jre\bin\java; /* example if JAVA_HOME is not set - no quotes - comment out to use the above */
%let classpath=&utillityFolder./mapo2xml/lib%str(/)*;
%let xmlmap=&utillityFolder./config/mapo.map; 


/* Section for 3_imap_dataitems.sas */
/* Information Map */
%let mappath=/CI/Financial Services/Information Maps; /* IMAP must correspond to business context of campaigns in metadata_root_folder variable above. */
%let mapname=MAInformationMap; /* ADAPT as needed */
%*let mapname='Marketing Automation Sample'n; /* Note: quotes and n at the end are needed in case the IMAP name contains spaces */


/* Section for 4_campaign_analysis.sas */
%let export_key_tables_to_csv=1; /* 1=yes, 0=no */


/* Section for 5_calculated_items_analysis.sas */
%let calculated_Items_json=&dataFolder./CI66CalcCols.json; /* !!! Detailed instructions see 5_calculated_items_analysis.sas or readme.md !!! */

/* batch execution */
options linesize=120; /* For text output created in batch mode */
/* In case of batch execution, uncomment lines below - I would do them one by one. */
/* %include "&utillityFolder/scripts/1_get_metadata_campaigns.sas"; */
/* %include "&utillityFolder/scripts/2_get_campign_data.sas"; */
/* %include "&utillityFolder/scripts/3_get_imap_dataitems.sas"; */
/* %include "&utillityFolder/scripts/4_campaign_analysis.sas";*/
/* %include "&utillityFolder/scripts/5_calculated_items_analysis.sas"; */
/* %include "&utillityFolder/scripts/6_link_node_analysis.sas"; */
