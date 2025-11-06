/**********************************************************************************************************************
PROGRAM: SAS CUSTOMER INTELLIGENCE 360 Custom On-Prem Scheduler for Direct Marketing Tasks, Bulk Email Tasks and Segment Maps
DESCRIPTION: This application helps the CI 360 User to create deployed jobs from the CI 360 tenant 
             for Direct Marketing Tasks, Bulk Email Tasks and Direct Segment Maps for a SAS Administrator 
             to schedule on-prem in the SAS LSF scheduler.
FILE NAME: autoexec.sas
DESCRIPTION: This is a configuration file for this program. Specify all important 360 and on-prem configurations here.
VERSION: 2.0
DATE MODIFIED: 25-APRIL-2024
AUTHOR: GLOBAL CUSTOMER INTELLIGENCE ENABLEMENT TEAM

#Copyright � 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0
**********************************************************************************************************************/

/*********************************************************************/
/*  Begin Site Specific Edits                                        */
/*********************************************************************/
/* Tenant information */
%let ENDPOINT       = <<GATEWAYADDRESS>>; 				              /* External gateway address for Tenant from 360 UI - e.g., https://extapigwservice-training.ci360.sas.com*/
%let TENANT_ID      = <<TENANTID>>; 		 	      	              /* Tenant ID from 360 UI */
%let SECRET_KEY     = <<AGENTSECRET>>;   				                /* Tenant Secret */
%let TENANT_NAME    = <<TENANTNAME>>;					                  /* Tenant Name for Reporting Label */
%let ENVIRONMENT_NM = '<<ENVIRONMENTNAMEFORREPORTING>>';        /* Environment Name for Reporting Label - use single quotes */

/* SAS Metadata information */
%let METAPARENT = <<YOURTOPLEVELMETADATAPATHANDFOLDER>>;    		/* Parent folder for Process Metadata - e.g, /Shared Data/Customer Intelligence/CI360Jobs */
%let META_JOB_FOLDER    = &METAPARENT./Jobs;                    /* Metadata folder for user submitted 360 jobs */
%let META_DEPLOY_FOLDER = &METAPARENT./Deployed Jobs;			      /* Metadata folder for deployed 360 jobs  */
%let META_COMPUTE_LOC   = <<COMPUTELOC>>;                       /* SAS Compute Tier for scheduling - e.g., SASApp */
%let META_BATCH_SERVER  = <<BATCHSERVER>>;        			        /* SAS Batch Server Name in Metadata - e.g., SASApp - SAS DATA Step Batch Server  */
%let META_JOB_DIR       = 360 Jobs;					                    /* Directory for the jobs on SAS Batch Server in Metadata */
%let META_DEPLOY_DIR    = 360 Deployed Jobs;				            /* Directory for the deployed jobs on SAS Batch Server in Metadata */
%let META_RESPONSIBLE   = <<YOURUSER>>;             			      /* Metadata user creating, scheduling jobs  */

/* Jobs Locations */
%let SEPARATOR = /;                                             /* Depends on your OS. Could be / or \ */
%let CI360_FOLDER 	= <<OSFOLDERCI360JOBS>>; 				            /* Full OS path for the CI360Jobs Folder -- e.g., D:\SAS\Software\CI360Jobs */
%let JOB_DIR          	= Jobs;							                    /* Folder name for the jobs created with the createJob stored process  */
%let DEPLOY_DIR         = DeployedJobs;						              /* Folder name for the deployed jobs created with the createJob stored process  */
%let JOB_FOLDER 	= &CI360_FOLDER.&SEPARATOR.&JOB_DIR.;			    /* Full path for the jobs created with the createJob stored process  */
%let DEPLOY_JOB_FOLDER  = &CI360_FOLDER.&SEPARATOR.&DEPLOY_DIR.;/* Full path for the deployed jobs created with the createJob stored process  */
%let MACRO_FOLDER       = &CI360_FOLDER.&SEPARATOR.Macros;			/* Full path for the macros folder */
%let AUTOEXEC           = &CI360_FOLDER.&SEPARATOR.Config&SEPARATOR.autoexec.sas;  /* Full path and file name for the autoexec file  */
%let SAS_LOG_REDIRECT =  Y;							                        /* Indicate Y to redirect the log to &CI360_FOLDER.Logs */
/*********************************************************************/
/*  End Site Specific Edits                                          */
/*********************************************************************/

/* Load macros */
data _null_;
  macro_folder = strip(symget("MACRO_FOLDER"));
  if 0=find(getoption("sasautos"), strip(macro_folder)) then do;
    call execute('options append=sasautos="' || strip(macro_folder) || '";');
  end;
run;

/* functions to generate job name, sasjob and sasdeploy from task name */ 
%if %sysfunc(exist(work._360jobs)) %then %do;
  proc sql noprint;
    drop table work._360jobs;
  quit;
%end;
proc fcmp outlib=work._360jobs.util;
  function timestamp() $;
    length timestamp $23;
    timestamp = translate(put(datetime(), e8601dt23.3), " ", "T");
    return(timestamp);
  endsub;
  function jobname($objectType, $objectName) $;
    length s1 s2 s3 jobname $256;
    s1 = upcase(ktranslate(strip(objectName), "_", "ÆØÅæøå"));            /* Replace Danish characters with underscores */
    s2 = compress(strip(s1), "ABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890_- ", "k"); /* Keep filename-safe characters   */
    s3 = translate(strip(s2), "_", " -");                                      /* Replace spaces and dashes with underscores */
    if objectType="DMTask"     then jobname = "DM_" || s3;
    if objectType="SegmentMap" then jobname = "SEG_" || s3;
    if objectType="EmailTask" then jobname = "EMAIL_" || s3;
    return(strip(jobname));
  endsub;
  function jobpath($objectType, $objectName) $;
    length folder path $512;
    /* removing trailing "/" from folder */
    folder = prxchange('s/\/+$//', 1, strip(symget("META_JOB_FOLDER"))); 
    path = cats(folder, "/", jobname(objectType, objectName));
    return(strip(path)); 
  endsub;
  function deploypath($objectType, $objectName) $;
    length folder path $512;
    /* removing trailing "/" from folder */
    folder = prxchange('s/\/+$//', 1, strip(symget("META_DEPLOY_FOLDER"))); 
    path = cats(folder, "/", jobname(objectType, objectName));
    return(strip(path)); 
  endsub;
run;
options cmplib=work._360jobs;

%put NOTE: --- %sysfunc(timestamp()) ---;

/* Generate token */
%let TOKEN =;
data _null_;
  header     = '{"alg":"HS256","typ":"JWT"}';
  payload    = '{"clientID":"' || strip(symget("TENANT_ID")) || '"}';
  encHeader  = translate(put(strip(header),$base64x64.), "-_ ", "+/=");
  encPayload = translate(put(strip(payload),$base64x64.), "-_ ", "+/=");
  key        = put(strip(symget("SECRET_KEY")),$base64x72.);
  digest     = sha256hmachex(strip(key),catx(".",encHeader,encPayload), 0);
  encDigest  = translate(put(input(digest,$hex64.),$base64x64.), "-_ ", "+/=");
  token      = catx(".", encHeader,encPayload,encDigest);
  call symputx("TOKEN",token);
run;

%put &=token;

