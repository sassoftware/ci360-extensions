# SAS CI 360 Tenant Promotion Utility

## Overview

The 360 Tenant Promotion utility helps in copying objects from one tenant to the other. This Python based utility implements the 360 Copy Item API and supports copying of the following objects across tenants:
 - Calculated data items
 - Creatives 
 - Direct marketing tasks
 - Export templates
 - Messages
 - On-premises segment maps 
A simple configuration and list of objects to be copied is all you need to get started. As long as you provide the right information, copying objects from one tenant to the other is going to be a walk-in-the-park task.

## Table of Contents
Welcome to the 360 Tenant Promotion utility. This program helps copy objects from one tenant to the other.

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
	- [Credentials.ini](#credentials.ini)
	- [Config.ini](#config.ini) 
	- [Preparing Input](#preparing-input)       
	- [Usage Guidelines](#usage-guidelines)	
- [Deployment](#deployment)
	- [Program Deployment](#program-deployment)		
- [Execution](#execution)
	- [How to execute?](#how-to-execute?)    		 
- [Troubleshooting](#troubleshooting)
- [Additional Resources](#additional-resources)
	- [360 Documentation](#360-documentation)
	- [360 Copy Item API Documentation](#360-copy-item-api-documentation)
		


## Prerequisites 

 - Appropriate 360 license
 - 360 General Access Point (Agent)
 - 360 API User
 - Python 3, preferably latest available version 
 - Python libraries (Run the "Install Python Libraries.bat" file in the repository)
	- configparser
 	- jwt	
	- requests

## Configuration

In this section, we will have a look at the two configuration files that are required to run this utility, namely 'credentials.ini' and 'config.ini'.

### Credentials.ini

This file contains the credentials that are used in this program.
 - sourcetenantid = <Tenant ID of Source 360 Tenant>
 - sourceclientsecret = <Client Secret of Source 360 Tenant>
 - targettenantid = <Tenant ID of Target 360 Tenant>
 - targetclientsecret = <Client Secret of Target 360 Tenant>

### Configuration.ini

The parameters defined in this file are required for the program to run correctly. Any mistakes in configuring these parameters will lead to unexpected results or failure to run. Lets look at each of the parameters in this file.
 - source360extgwhost = <External API Gateway Host of Source 360 Tenant>
 - target360extgwhost = <External API Gateway Host of Target 360 Tenant>
 - targetbusinesscontextid = <Business Context ID of Target 360 Tenant> ***
 - sourcecsvfilepath = <Location of input csv file>
 - logging = <Set to "true" if you want the program to write logs, else set to "false">
 - loglevel = <Set the log level for the porgram. Possible values are: DEBUG, INFO, WARNING, ERROR, CRITICAL>
 
 *** Business Context ID is not available in the UI of 360. This value needs to be picked from the browser developer tools window. Follow these instructions to obtain the Business Context ID:
  - In the browser window/tab where you have logged in to the 360 tenant, open developer tools. In Chrome, you can press Fn+F12 to open developer tools window.
  - Now on the 360 tenant, navigate to General settings->System configuration->Manage Busines Contexts.
  - In the developer tools window, click on the Network tab. You will notice a small text box for filtering the results. Type in 'businesscontextid' without the single quotes in this filter box.
  - In the 360 tenant window, now click on the business context you wish to select for the promotion.
  - In the developer tools window, under network tab, you will see a filtered result with name field in this format: "https://design-training.ci360.sas.com/SASWebMarketingMid/rest/dataItems/stratifiedSampling?businessContextId=27fc9fb8-a182-4e86-97fe-e086ff992c94". Note the query string parameter here i.e. businessContextId. The value after the '=' sign is the business context Id for the business context you selected in 360. '27fc9fb8-a182-4e86-97fe-e086ff992c94' in this example. Copy this value and paste it in the config.ini file against the 'targetbusinesscontextid' parameter.

### Preparing Input

You would need to prepare a list of objects to be promoted to the target 360 tenant. The input file named "Input.csv" should be saved in the "input" directory of the utility. Please follow the below steps to create this input file:
- The first row of the input file has the name of the columns separated by a comma ','. 
- The column names should not be changed i.e. objectID,objectType,objectDependency.
- Now, type in the objectID, objectType and objectDependency separated by ',' in the subsequent rows.
- To locate the objectID, open the object in 360 UI. For example, if you want to copy a creative, open the desired creative in 360. Notice the URL in the browser address bar. It should be something like this: https://design-training.ci360.sas.com/SASWebMarketing/#/creatives/035da01d-2e2f-47a7-9643-2f709380b54d
- The objectID of this creative is the value after creatives/ i.e. '035da01d-2e2f-47a7-9643-2f709380b54d'.
- The objectType will be 'creative'.
- The dependency will be 'true' if you want all dependent objects on this creative to be copied to the target 360 tenant as well, or if not, then set it 'false'.
- You can only use one of these objectTypes:
	- calculatedDataItem
	- creative
	- exportTemplates
	- message
	- segment
	- task
- Here is an example of a sample Input.csv file:
		objectID,objectType,objectDependency
		13fca32f-f355-46fb-ab98-e133c44f8366,creative,false
		dd7591c6-d70c-418a-922f-3df8b8a605b0,message,true
		328d763d-b13c-4898-9821-28360051e4fd,calculatedDataItem,false

### Usage Guidelines

When you are creating the Input.csv file, be aware of these usage guidelines:

- You cannot copy a top-level item if it already exists on the target tenant. To copy an item that already exists on the target tenant, manually delete the item before copying from the source tenant.
- When you choose to create dependencies, dependent items are created if they do not exist on the target tenant. Dependent items on the target tenant are not updated or overwritten if they already exist.
- The cloud segments and events that are used in a direct marketing task must already exist on the target tenant before you can successfully copy the direct marketing task.
- When you copy a direct marketing task, the export template that is used in the task is copied also. However, the export template is available only from within the direct marketing task that is copied. It is not copied as a stand-alone template.
- Items that are copied to the target tenant are set to the Designing state.
- The target tenant must be configured to support the copied item. The configuration of the target tenant must be identical to the configuration for the source tenant, including the general settings, business context, data items, custom properties, and assets.
F- olders are created on the target tenant if required and the folder settings for the item type are enabled in General Settings > System Configuration > Folders.
- Permissions are dropped for items if a user or group does not exist on the target tenant.
- After a mobile creative is copied to the target tenant, you must regenerate the design preview. Click Edit in the Content tab and click Done to close the editor.

**Copying Direct Marketing Tasks**

When you copy a direct marketing task, be aware of these usage guidelines:
- The cloud segments and events that are used in a direct marketing task must already exist on the target tenant before you can successfully copy the direct marketing task.
- The export template that is used in the direct marketing task that you copy is copied also. However, the export template is available only from within the direct marketing task that is copied. It is not copied as a stand-alone template.

## Deployment

In this section, we will learn how to deploy this utility to your server. 

### Program Deployment

- Download the 360 Tenant Promotion repository from gitlab/github.
- Copy the folder to any specific drive or directory as required.
- Configure the program by following steps as described in the previous section.
- Make sure the directory has sufficient authorization for the program to write files in it.

## Execution

In this section, we will learn how to execute this utility to copy objects from the source 360 tenant to the target 360 tenant. 

### How to execute?

- Once, you have deployed this program to a specific directory on the server and finished configuring as per the instructions given here, you are ready to execute the program to copy objects from one 360 tenant to the other.
- Open command prompt with administrator privileges and change directory to this program's directory on the server.
-  Once in the program directory, run the following command to execute the program:
		python 360CopyItem.py
- You will notice the program exit after execution completes. Do check the logs directory for logs.
- Check the target 360 tenant to validate if the objects listed in the Input.csv file have been copied over from the source 360 tenant.
- You can also configure the program to run on a schedule depending on your requirements.


## Troubleshooting

In this section, we will see how to fix some common issues related to this utility. 

- Utility not promoting data that is specified in the input file
	- Check configuration file, specifically source and target tenant gateway host urls.
	- Check credentials file and ensure the values are correct and in sync with whats on the 360 tenant.	
	- Check Input file for errors in objectID or objectType values.
	- Check log files and look for any errors in program execution.
	- Make sure general agent is active and the credentials match with the configuration in this utility.

## Additional Resources

### 360 Documentation
- 360: https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintwlcm/home.htm 

### 360 Copy Item API Documentation
- Copy Item API: https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintapis/copy-item-api.htm?requestorId=a4f6444c-0f49-482d-b1ef-df917b948aa6 
- Copy Item API Developer Documentation: https://support.sas.com/documentation/onlinedoc/ci/ci360-apis/marketingPromotion/v1/redoc.html

