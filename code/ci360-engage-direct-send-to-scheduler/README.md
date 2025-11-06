# SAS Customer Intelligence 360 Custom On-Prem Scheduler for Direct Marketing, Bulk Tasks and Segment Maps
		
## Overview
This application helps the CI 360 User to create deployed jobs from the CI 360 tenant for Direct Marketing Tasks, Bulk Email Tasks and Direct Segment Maps for a SAS Administrator to schedule on-prem in the SAS LSF scheduler.

## Table of Contents
Welcome to the "SAS CI 360 - Custom On-Prem Scheduler for Direct Marketing, Bulk Tasks and Segment Maps" repository. This is a utility that will help you leverage the on-prem SAS LSF Scheduler for complex scheduling of tasks and segment maps, which is not possible in CI 360. Here we will help you to configure this utility to run with your tenant and on-prem configuration.

- [Overview](#description)
- [Stored Process](#stored-process)
	- [Prerequisites](#prerequisites)
	- [Included Files](#included-files)
- [General Setup](#general-setup)
	- [Extract Content](#extract-content)
	- [General Access Point](#general-access-point)
- [Schedule Manager Configuration](#schedule-manager-configuration)	
	- [360 Jobs Directory](#360-jobs-directory)
	- [360 Deployed Jobs Directory](#360-deployedjobs-directory)
- [STP Implementation](#stp-implementation)
	- [Authentication](#authentication)
	- [STP Import](#stp-import)
		- [Metadata Folders](#metadata-folders)
		- [Import SPK](#import-spk)
		- [Remove STP Macros](#remove-stp-macros)
	- [Configure Autoexe.sas](#configure-autoexe.sas)
		- [Tenant Information](#tenant-information)
		- [Metadata Information](#metadata-information)	
		- [Jobs Location](#jobs-location)	
	- [Deploy STP](#deploy-stp)
- [Connector Implementation](#connector-implementation)		
	- [Download Agent Framework](#download-agent-framework)
	- [Configure Agent](#configure-agent)
		- [Agent Endpoint](#aganet-endpoint)
		- [Event Streaming](#event-streaming)
	- [Agent Startup](#aganet-startup)
	- [Connector Configuration](#connector-configuration)
- [Custom Properties](#custom-properties)
	- [Edit Custom Properties](#edit-custom-properties)
	- [Upload Custom Properties](#upload-custom-properties)
- [Validation](#validation)
- [Troubleshooting](#troubleshooting)
- [Additional Resources](#additional-resources)
	- [360 Documentation](#360-documentation)
	- [SAS STP Web Application Documentation](#sas-stp-web-application-documentation)

## Description
This application helps the CI 360 User to create deployed jobs from the CI 360 tenant for Direct Marketing Tasks, Bulk Email Tasks and Direct Segment Maps for a SAS Administrator to schedule on-prem in the SAS LSF scheduler.

Marketers click ‘Create Job’ from within the Custom Properties tab of a Segment Map or Direct Marketing Task to create an On-Premises job that is ready to be scheduled.
(This mimics the CI6.x “Send to Admin to Schedule” functionality.) 

Users with scheduling capabilities can use the SAS Schedule Manager to:
- Create a time triggered schedule
- Create flows with dependencies between 360 deployed jobs
- Set up notifications for flow start, success or failed end
- Monitor execution status of flow

Additional capabilities when SAS Schedule Manager leverages Platform LSF:
- File, multiple event triggers for flow
- Monitor job execution status within flow 
- Pause, End, Start Flows from Platform Manager User Interface

## Stored Process
The createjob STP is the main file of this program. This STP is called from within CI 360 connector configuration to create an on-prem job.

### Prerequisites 
There are a few pre-requisites for this program to run. Please ensure you have them all.

 - Appropriate 360 license
 - 360 General Access Point (Agent)
 - 360 Connector
 - Java 1.8+
 -	To deploy a SAS 9.4 stored process as a web service, the SAS administrator needs to install one of the following:
	-	SAS BI Web Services for .NET, which is part of SAS Integration Technologies
	-	SAS Web Infrastructure Platform (WIP) and its associated components, which is included in the Engage: Direct on-premises installation
 -	Scheduling Server configured in SAS Management Console (Platform Process Manager or Operating System Scheduling Scheduler)
	See About SAS 9.4 Scheduling Servers for more information (https://go.documentation.sas.com/?cdcId=bicdc&cdcVersion=9.4&docsetId=scheduleug&docsetTarget=p0e2328k5bi682n0zmbxhga41qq5.htm&locale=en)
 -	User/PW for Scheduling Administrator
 -	SAS Users Group(s) defined that will act as Schedulers with appropriate capabilities and authorization
	-	Schedule Manager capability assigned under Management Console 9.4 capabilities
	-	Authorized for access to the Scheduling Administrator account

### Included Files
All files/folders are found within the parent repository folder.

| Name | Description |
| ---- | ----------- |
| ../Config/autoexec.sas | Autoexec file for the SAS Stored Process containing all environment variables. |
| ../Config/createJob.spk |	SAS Stored Process Import Package; Contains createJob.SAS |
| ../Config/createJobCustomProperties.xlsx | Custom Property Worksheet to add the connector to the 360 User Interface. |
| ../Macros/check_syscc.sas	| Checks the system return code. |
| ../Macros/check_value.sas	| Checks the validity of required macrovars. |
| ../Macros/execute360object.sas | Uses the public scheduling API to execute the designated Segment Map or DM task. |
| ../Macros/parseJobMetadata.sas | Reads SAS scheduling metadata for required inputs into the code to deploy the SAS jobs. |
| ../Macros/prochttp_check_return.sas | Checks the return code of proc http for success. |

## General Setup

### Extract Content
Extract the contents (including subfolders) of this repository into a new folder “CI360Jobs” within a directory on your compute tier where your other SAS configurations and code are stored. 

A location for a standard CI implementation may be /SAS/software/CI360Jobs (at the same folder level of your CI360DirectAgent).	
For example: 
D:\SAS\Software\CI360DirectAgent  <-Direct Agent
D:\SAS\Software\CI360Jobs         <-CI360 Jobs code and related files

Ensure that your SAS Service account (e.g., sassrv) that will run SAS 9.4 stored processes and your scheduling user has full permissions to this parent CI360Jobs folder and all subfolders.

- ../CI360Jobs
- ../CI360Jobs/Config
- ../Config/autoexec.sas
- ../createJobCustomProperties.xls
- ../createJob.spk 
- ../CI360Jobs/ConnectorPY/logs
- ../CI360Jobs/DeployedJobs
- ../CI360Jobs/Jobs
- ../CI360Jobs/Macros 
- ../Macros/check_syscc.sas
- ../Macros/check_value.sas
- ../Macros/execute360object.sas
- ../Macros/parseExecutionStatus.sas
- ../Macros/parseJobMetadata.sas
- ../Macros/prochttp_check_return.sas
- ../CI360Jobs/StoredProcesses

### General Access Point
For the connector to be able to call an on-prem STP, you must first create an Access Point in CI 360, which you will need to host on the server. 

- Log into 360 and navigate to General Settings->External Access-> Access Points. 
- If you do not already have a suitable access point, create one. 
- Add the access point credentials and mark it as 'Active'. 
- Note the Tenant ID, Access Point Name, Client Secret and the External Gateway Address displayed at the top of the page as you will provide them 
  as environment parameters in the configuration file. 

## Schedule Manager Configuration

### 360 Jobs Directory
Create a new CI 360 Jobs Directory 

- Log into SAS Management Console as a scheduling administrator. 
- Right Click on Scheduler Manager to select to manage Deployment Directories. 
- Click New to add a new directory. 
- Navigate to the target directory created in step one of these instructions.:
	- Name: CI360 Jobs
	- Directory:  /SAS/software/CI360Jobs/Jobs 

### 360 DeployedJobs Directory
Create a new 360 DeployedJobs Directory 

- Repeat the above steps to create a new directory for the CI360 Deployed Jobs.
- Name: CI360 Deployed Jobs
	- Directory:  /SAS/software/CI360Jobs/DeployedJobs
	- Click OK to exit managing deployed directories.

## STP Implementation

### Authentication
Enable Basic authentication for the STP Web App.

- Log into SAS Management Console as an administrator. 
- Navigate to Application Management\Configuration Manager\SAS Application Infrastructure\Stored Process Web App 9.4 
- Right-click to select Properties and select the Advanced tab. 
- Ensure that AllowBasicAuthentication is set to true

### STP Import
Import the CreateJob Stored Process

#### Metadata Folders
Create Metadata Folders

- Log into SAS Management Console as an administrator and create the following metadata folders:
	- Shared Data\Customer Intelligence\CI360Jobs\Stored Processes
	- Shared Data\Customer Intelligence\CI360Jobs\Jobs
	- Shared Data\Customer Intelligence\CI360Jobs\Deployed Jobs 
- Ensure that the user groups that will be granted scheduling capabilities have read/write metadata permissions to these folders.

#### Import SPK
Import createJobv2.0.spk to SAS Management Console.

- Copy this file if needed to a location accessible to your SAS Management Console client.
- Save the CreateJob SAS stored process in the new ..\CI360Jobs\Stored Processes folder.
- Save the source createJob.sas file on your compute tier in the new ../CI360Jobs/StoredProcesses folder.

#### Remove STP Macros
As written, the stored process code does not contain %stpbegin / %stpend statements. If you will be editing or reviewing this code from SAS Enterprise Guide, explicitly remove these macro inclusions from the editor so they will not be inadvertently added into the code.

- In Enterprise Guide, navigate to ../CI360Jobs/StoredProcesses and open the createJob stored process.
- Navigate to the SAS Code window and deselect the option to Include code for Stored process macros. 
- Click Save to save these changes and exit.

### Configure Autoexe.sas
Configure autoexec.sas configuration file. This SAS program contains macrovar assignment statements to assign site and tenant specific details and credentials for authorization. Values <<IN BRACKETS>> must be edited for each site. All values should be confirmed for any deviation from the set step names/paths provided.

#### Tenant Information
| Name	| Description | Sample Value |
| ----- | ----------- | ------------ |
| ENDPOINT | External gateway address for Tenant from 360 | https://extapigwservice-training.ci360.sas.com |
| TENANT_ID	| Tenant ID from 360 UI |	3e***678 |
| SECRET_KEY | Tenant Secret |	ABCDE12345FGHIJK |
| TENANT_NAME | Label for Logs – Tenant Name |	GCIE Sample Tenant |
| ENVIRONMENT_NM |	Label for Logs – Tenant Environment | 'DEV' (include single quotes) |

#### Metadata Information
| Name	| Description | Sample Value |
| ----- | ----------- | ------------ |
| METAPARENT |	SAS Metadata Folder where all Metadata for CI360Jobs lives | /Shared Data/Customer Intelligence/CI360Jobs |
| META_JOB_FOLDER |	Metadata folder for Jobs | (inherits from METAPARENT) |
| META_DEPLOY_FOLDER | Metadata folder for Deployed Jobs | (inherits from METAPARENT) |
| META_COMPUTE_LOC | SAS Compute Tier for scheduling | SASApp |
| META_BATCH_SERVER	| SAS Batch Server for scheduling |	SASApp – SAS DATA Step Batch Server |
| META_JOB_DIR | Directory for jobs on the SAS Batch Server | 360 Jobs (set up step 3) |
| META_DEPLOY_DIR |	Directory for deployed jobs on the SAS Batch Server | 360 Deployed Jobs (set up step 4) |
| META_RESPONSIBLE | Metadata User who will be creating, deploying jobs | sasdemo |

#### Jobs Location
| Name | Description | Sample Value |
| ----- | ----------- | ------------ |
| CI360_FOLDER | Full path of the CI360Jobs folder created in Step 1 |	D:\SAS\Contexts\CI360Jobs |
| JOB_DIR |	Folder name for the jobs created with the createJob stored process.	| Jobs (set up step 1) |
| DEPLOY_DIR | Folder name for the deployed jobs created with the createJob stored process. | Deployed Jobs (set up step 1) |
| JOB_FOLDER | Full path of the CI360Jobs/Jobs folder for .sas jobs created by the stored process. | (Inherited) |
| DEPLOY_JOB_FOLDER | Full path of the CI360Jobs/DeployedJobs folder for .sas jobs created by the stored process. | (Inherited) |
| MACRO_FOLDER | Full path of the CI360Jobs/macros folder | (Inherited) |
| AUTOEXEC | Full path of the CI360Jobs/Config/autoexec.sas file	| (Inherited) |
| SAS_LOG_REDIRECT | Indicate Y to redirect the log to &CI360_FOLDER.Logs | Y |
 
### Deploy STP
Deploy the Stored Process as a Web Service.

- Within SAS Management Console, navigate to the folder where the metadata for the stored process lives. 
- Right-click on createJob and select Deploy as a Web Service.
- Select the Web Service Maker URL that has been configured for the environment. 
- Check with your SAS Administrator if no values are available, or you are unsure which to select when one or more present.
- Provide a name for the Web Service: create360Job
- Leave the default 'Use my current credentials to deploy' checked.
- Click Next.
- Note the Namespace. Click Next.
- Review the values for the deployment.                                                         
- Click Finish and review the location of your new web service.  
- Click OK.                                                        

## Connector Implementation
Following are the guidelines to configure a Connector in CI 360.

### Download Agent Framework
Download the general framework for the General Access Point found within General Settings->External Access->Access Points>Download Framework.

It is recommended to unzip and save the framework in a location accessible for common use, preferably near the Direct Agent.	
For Example:
- D:\SAS\Software\CI360DirectAgent  <-Direct Agent
- D:\SAS\Software\CI360GeneralAgent <-New General Agent

### Configure Agent
Follow the instructions contained within the agent framework zip file 'On Premise Agent Installation Guide.docx'.

#### Agent Endpoint
- For the on-premises agent to process events from SAS Customer Intelligence 360 an endpoint must be configured. 
- When the on-premises agent starts up it will use this endpoint to connect to the SAS Customer Intelligence 360 marketing gateway.
- Edit the ..\config\agent-endpoints.properties file and set the following property with the external gateway address gathered above.
		- event.streaming.endPointNodeName = <<external api gateway address>>:443
- The port number should always be 443.

#### Event Streaming
- For the on-premises agent to authenticate with the endpoint, the tenant ID and client secret must be added to the agent configuration. 
- Edit the ..\config\event-streaming-configuration.properties file within the on-premises agent config folder and set the following properties with 
  the TenantID and Secret gathered above:
	- event.streaming.tenantID=<<360 Tenant ID>> 
	- event.streaming.clientSecret=<<360 Client Secret>>

### Agent Startup
- From a command line navigate to the 'bin' folder and run the respective script for Windows or UNIX. 
- For Windows, the 'mkt-agent-sdk.bat' file would be executed. 
- This will start the agent shell.
- Meanwhile, in the background the on-premises agent will have automatically connected to the SAS Customer Intelligence 360 endpoint configured  
  earlier and will start sending those events to any enabled plug-in that needs to process it.
- Type “help” from the on-premises agent console for assistance and type “agent stop” to stop the agent.
- Additional information about running the agent as a service is available in the 'On Premise Agent Installation Guide.docx' found within the 
  downloaded agent framework.

### Connector Configuration
- In the 360 UI, navigate to General Settings->External Access->Connectors. 
- Click on the new icon to create a new Connector.
- Enter a name and description in the Details Section. 
- Enter your solution Admin for support contact.
- Click New Endpoint and enter the details of the on-premises web service. 
- The URL should be as shown below: 
     http://##/SASStoredProcess/do1?_username=**&_password=^^&_program=/Shared+Data/Customer+Intelligence/CI360Jobs/Stored+Processes/createJob&OBJECT_TYPE=DMTask&Description=&OBJECT_ID={{$objectid}}&OBJECT_NAME={{$objectname}}
     where
	 '## - SAS Server where the STP is hosted
	 ** - SAS MA Username
	 ^^ - SAS MA User password
- For example: 
	 http://sasbap.demo.sas.com/SASStoredProcess/do1?_username=sasdemo&_password=12345&_program=/Shared+Data/Customer+Intelligence/CI360Jobs/Stored+Processes/createJob&OBJECT_TYPE=DMTask&Description=&OBJECT_ID={{$objectid}}&OBJECT_NAME={{$objectname}}
- Once done, click on the ‘Define variable’ link on the upper right corner in the same window.
- Key in details as below:
	  objectid	: id
	  objectname: name
- Leave the Authorization, Headers and Parameters sections unchanged.
- Save the configuration by hitting the ‘Save’ button and you will come back to the Connector info screen. 
- Observe the Endpoints section and you will see the endpoint you just created. 
- Note the Connector ‘ID’ here. CON**-** This will be required to configure Custom Properties later.
- Click on ‘Apply’ to save your changes to this Connector. 
- Important: Once you are on the Connector List page, you may need to associate the Access Point you created in Step 9 with this Connector. To do 
  so: 
  - Scroll right on the Connector List until you see the ‘Access Point’ column. 
  - Click on the ‘Add’ link and select the apt Access Point from the dropdown list. 
  - Click Save and validate on the Connector list if the Access Point has been associated with your Connector. 
  - You will see a ‘View’ instead of ‘Add’ under the ‘Access Point’ column now.

## Custom Properties
Configure Custom Properties for the Direct Marketing Tasks, Bulk Email Tasks and Segment Maps in CI 360.

### Edit Custom Properties
- For the on-premises agent to process events from SAS Customer Intelligence 360 an endpoint must be configured. 
- When the on-premises agent starts up it will use this endpoint to connect to the SAS Customer Intelligence Upload Custom Properties Template: 
  CustomProperties.xlsx
- Update Column Q-Data Values Source values in the Properties Tab to be the Connector ID for your implementation from the step above.
- Once updated, the custom properties update can be applied to the Tenant.

### Upload Custom Properties
- Go to General Settings > Data Collection > Data Sources and click on Upload in the top right of the screen.
- Choose the updated CreateJobCustomProperties.xls and select Upload Data.
- Check the job status and confirm that the Custom Properties were updated successfully.
- Open a new Direct Marketing Task, Bulk Email Task or Segment Map to confirm the new Create On-Prem Job Custom Properties are present. 

## Validation
- Create a new Direct Marketing Task or Segment Map in 360.
- Configure and save the DM Task and/or Segment Map as per your requirements.
- On the Properties tab, locate the ‘Create On-Prem Job’ custom property. 
- Click on the ‘Create Job’ button.
- Observe the Agent console window for logs.
- If the Integration is correctly setup, you will see a return response populates the custom property fields on the 360 UI as shown above.
- If not, revisit and affirm all steps in this document and perform this test use case again.

## Troubleshooting
- Confirm that the stored process was deployed correctly and that the web services are running by navigating to http://sasbap.demo.sas.com/SASStoredProcess/do1 in your SAS 9.4 environment.

- If the agent fails to start, review the ..\logs\sas.mkt.apigw.sdk.log file. A common reason will be that the port is already in use. 
  Edit the port in ..\CI360GeneralAgent\config\agent-runtime.properties

	      ***************************
	      APPLICATION FAILED TO START
	      ***************************
	      Description:
	      Web server failed to start. Port 8080 was already in use.
	      Action:
	      Identify and stop the process that's listening on port 8080 or configure this application to listen on another port.


- If you are experiencing file in use errors, confirm you have correctly completed 6.3 to deselect the stored process macros from your code.

	      ERROR: File is in use, _WEBOUT.
	      ERROR: Due to the previous error, the JSON output file is incomplete and invalid, or in some cases, unable to be created. If created, the JSON output file is retained so that you can review it to help determine the cause of the error.

## Additional Resources
Here are a few documentation links that will help you get started with the CI 360 objects.

### 360 Documentation
- 360: https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintwlcm/home.htm 
- General Access Point: https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/ext-access-pts-general.htm 
- Custom Properties: https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/p1hr64t3g3qalun18hhbnmfm3n07.htm 
- Connectors: https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/integrating-about-connectors.htm 
- Direct Marketing Task: https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintug/ch-dirmkt-working.htm

### SAS STP Web Application Documentation
- SAS STP Web Application: https://support.sas.com/rnd/itech/doc9/dev_guide/stprocess/stpwebapp.html
