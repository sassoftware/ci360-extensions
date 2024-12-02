# SAS CI360 Custom Task Type (Connector) with onprem SAS STP (SASBIWS) Integration using Python Flask
		
## Overview
The "SAS CI 360 Custom Task Type (Connector) with onprem SAS STP (SASBIWS) Integration using Python Flask" utility helps you integrate a 360 custom task type using a connector with an onprem SAS STP. The 360 CTT sends the task payload through the associated Connector endpoint to the configured Flask service. The Flask service then reads the JSON payload, encodes it, converts it to XML and sends to the on-prem SASBI web service STP. The STP reads the incoming XML, downloads the metadata and data files from AWS and stores in a SAS Dataset.

## Table of Contents
Welcome to the "SAS CI 360 Custom Task Type (Connector) with onprem SAS STP (SASBIWS) Integration using Python Flask" repository. This is a utility that will help you integrate a 360 custom task type using a connector with an onprem SAS STP. The STP will download the datafile that is received from the Custom Task Type payload.

- [Overview](#overview)
- [360 Custom Task Type](#360-custom-task-type)
	- [CTT Prerequisites](#ctt-prerequisites)
	- [Tenant Configuration](#tenant-configuration)
	- [Connector Configuration](#connector-configuration)
- [360 General Access Point](#360-general-access-point)
	- [Agent Configuration](#agent-configuration)	
- [Python Flask](#pythonflask)
	- [Flask Prerequisites](#flask-prerequisites)
	- [Flask Configuration](#flask-configuration)
	- [Flask Service Deployment](#flask-service-deployment)       		
- [SAS Stored Process](#sas-stored-process)
	- [STP Prerequisites](#stp-prerequisites)
	- [STP Configuration](#stp-configuration)
	- [STP Deployment](#stp-deployment)
		- [Create a STP](#create-a-stp)
		- [Deploy the STP as a Web Service](#deploy-the-stp-as-a-web-service)   
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)
- [Additional Resources](#additional-resources)
	- [360 Documentation](#360-documentation)
	- [SAS BI Web Services Documentation](#sas-bi-web-services-documentation)
	- [Python Flask Documentation](#python-flask-documentation)

## 360 Custom Task Type

Create a 360 Custom Task Type (Bulk) that will be used for this integration.

### CTT Prerequisites 

 - Appropriate 360 license
 - 360 General Access Point (Agent)
 - 360 Connector

### Tenant Configuration

For how to create a Custom Task Type, you can refer to the 360 documentation here: https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/custom-task-create.htm?requestorId=a4f6444c-0f49-482d-b1ef-df917b948aa6

 - Create a new Custom Task Type (Bulk)
 - Create a new Connector and add an Endpoint
 - Define Send Parameters that you wish to send out from the Task
 - Define Data Attributes (Outbound Data) that you wish to export in the data file 
	- Add a Data Attribute for "STP Name" as described here: 
	  - Display Name: STP Name
	  - Attribute Name: stpName
	  - Type: Character
	  - Default Value: <<Leave blank>>
	  - Required: On (Set to True)
 - Define any metrics for measuring task performance

### Connector Configuration

 - Define a Bulk Outbound Connector for the Custom Task Type. You can create it from the CTT screen itself.
 - Associate a General Access Point with this Connector
 - Add a webhook endpoint
	 - Define the endpoint URL as: http://<host:port>/ExecuteSTP. Host and Port are where the Flask service is hosted. For example, the URL will be: http://127.0.0.10:8100/ExecuteSTP if the service is hosted on 127.0.0.10 and port number where the Flask is running is 8100. 
	 - Define the method to be: "POST"
	 - Add a Header with Key as "Content-Type" and Value as "application/json"

## 360 General Access Point 

### Agent Configuration
- For more details on setting up and running a General Access Point refer to the 360 documentation here: https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/ext-access-pts-general.htm 

## Python Flask

In this section, we will define a Python Flask service and host/deploy it on the server.

### Flask Prerequisites
 
- Recommended to have the latest Python (Python 3.13.0) installed on the Server. 
  Note: This service has been tested on 3.13.0, yet earlier 3.x versions of Python may work fine too.

- Copy the "Python Flask" folder from this repository to the Server.  

- List of required Python libraries: 
  Do a pip install <library name> to install these libraries. For example: pip install flask. 
  Alternatively, run the "Install Python Libraries.bat" file in the "Python Flask" folder to install these libraries on the server.

 - flask
 - jsonify
 - requests
 - html
 - configparser
 - logging

### Flask Configuration

In the "Python Flask" folder, you will notice the "conf" directory. This is the configuration folder for this program. In this folder, there are three config files for this Flask service.

 - credentials.ini
   This file contains the credentials required to connect to the SAS STP. This is optional and only required if the STP requires authorization.
	- stpusername - Username to connect to the STP
	- stppassword - Password to connect to the STP

 - serviceConfig.ini
   This file contains some crucial parameters required for execution of the program. 
    - stpurl - This is the URL of the onprem SASBIWS Stored Process. Do not specify the name of the STP in this URL. use the following format: "http://sas-aap.demo.sas.com/SASBIWS/rest/storedProcesses/Shared Data/Customer Intelligence/CI360Jobs/Stored Processes/{{stpName}}". The STP Name will be picked up from the CTT Data Attribute "STP Name". 
	- Note: If you have deployed the STP as a web service, the URL will contain the name of the web service here. So, do not mention the name of the web service in this parameter. For example, if this is your Web Service URL: "http://sas-aap.demo.sas.com:80/SASBIWS/services/DownloadFile", just specify "http://sas-aap.demo.sas.com:80/SASBIWS/services/{{stpName}}" in the "stpurl" parameter.
	- proglog - Set this parameter value to "true" if you want the program to log execution info, else set it to "false".
	- loglevel - Set the desired logging level for the program. Values could be DEBUG, INFO, WARNING, ERROR, CRITICAL.
	- logfilepath - Defines the log directory path. Do not change the default value i.e. ".\logs\".
   
 - serviceConfig.py
   This file contains parameters that define where i.e. what location/url on the Server to host this Flask service.
	- host - This is the IP where this Flask Service will be hosted on the Server. Defaulted to '127.0.0.10'.
	- port - This is the port number where this Flask Service will be run on the Server. Defaulted to 8100.
	- debug - Set to 'True' or 'False' if you would like to debug this code. Defaulted to 'False'.

### Flask Service Deployment

After you have installed Python, dependent Python libraries and updated the 2 program configuration files, you would need to run/deploy the Flask Service on the Server. Here is how you would host the Flask service (on a Windows machine):

 - Open command prompt as administrator.
 - Change directory to the location of the Python Flask program, say "cd c:\Python Flask".
 - Type this command and hit enter: "python ConnectorProxy.py"
 - The Flask service will be hosted on the host:port as defined in the serviceConfig.py file. For example, http://127.0.0.10:8100/ExecuteSTP if host is 127.0.0.10 and port is 8100.
 - Keep the command window open.
 - You can also opt to deploy the Flask program on Windows IIS. This link should be helpful: https://learn.microsoft.com/en-us/visualstudio/python/configure-web-apps-for-iis-windows?view=vs-2022 


## SAS Stored Process

In this section, we will see how to configure a SAS Stored Process and optionally deploy it as a SAS BI Web Service.

### STP Prerequisites
 - SAS 9.4 M7+
 - SAS Management Console
 - SAS Enterprise Guide
 - Necessary access to configure and deploy a STP in SMC 

### STP Configuration

Copy the "SAS Stored Process" folder from this repository to the Server. In this folder, you will notice the "conf" directory. This is the configuration folder for this program. In this "conf" folder, there are 4 config files that are required for this STP. You need to edit just the "config.sas" file and leave the other three as they are.

- config.sas - This file stores librefs, filerefs, variables, macros and pre-assigned values required for the execution of the Stored Process. The parameters you can change in this file are:

	- saslogredirect - Set this flag to "true" if you want the STP log to be written to a file, else set it to "false".
	- debug - Set this flag to "true" if you want all files generated by the process to be logged in a folder. If set to "false" no process files will be created. For example, API payloads, API responses, etc. 
	- liblocation - This will ideally be the location of the folder where the STP code resides. For example, if you copied the "SAS Stored Process folder to C drive, the liblocation would be C:\SAS Stored Process".

- stpDownloadFile.sas - You will find this file in the "SAS Stored Process/code" directory. This is the main code file of the STP so be very careful while making any changes. Please do not update any code other than the change mentioned here. 
	- Look for this line in the code (Line No.12): %let projdir = %sysfunc(dlgcdir('C:\Ron')); 
	- Change the value in between the single quotes i.e. "C:\Ron" to the location of the SAS Stored Process folder as defined in the config.sas file. So, if your STP code is at "C:\SAS Stored Process" the updated code will look like this:
		%let projdir = %sysfunc(dlgcdir('C:\SAS Stored Process'));
	- Save the file after making this change and exit. 
	- Note: Do not change any thing else in the code or the program may not function as expected.

### STP Deployment

#### Create a STP 

Once you have saved the changes to the STP configuration files, you can now create a Stored Process (STP) in SAS Management Console. Let's look at the steps required to create a STP in SMC. 
- Open SAS Management Console on the Server.
- Click on the "Folders" tab.
- Navigate to the folder location where you would want to host this STP.
- Right click on the desired root folder and click "New -> Stored Process".

- In the new "General" window, specify a "Name" and "Description" for the STP. For example, "Download File". Click "Next" to move to the next screen.

- In the "Execution" window, select the appropriate Application Server. 
- Select the "Server Type" as "Default Server".
- Select "Source code location and execution" as "Allow execution on selected application server only" -> "Store source code on application server only" -> "Source code repository", then click on "Manage".
- Click "Add" and then give the path of the STP source code. For example, "C:\SAS Stored Process\code". 
- Select this new path you added for the "Source code repository".
- In the "Source File" text box, type the name of the code file i.e. "stpDownloadFile.sas".  
- For the "Result capabilities" parameter, check the "Stream" option. Click "Next".

- Make no changes in the "Parameters" screen. Click "Next".

- In the "Data" window, click on "New" add the following parameters:
	- Type: XML Data Source
	- Label: instream
	- Description: instream
	- Fileref: instream
	- Expected content type: text/xml
	- Check "Allow rewinding stream"
	- Click "Ok" to close the window.

- Click on "Finish" to complete setting up the STP.

#### Deploy the STP as a Web Service

You might want to deploy the STP as a web service. Note this step is optional. 
	
- Once you have created the STP and it appears in the STP list, right click on it and select "Deploy as Web Service".
- In the new window, specify the "Web Service Maker URL". For example: "http://sas-aap.demo.sas.com/SASBIWS/services/WebServiceMaker".
- Specify a name for the web service in the "New Web Service Name" text box. For example: "DownloadFile".    
- Use your current credentials to deploy the web service or specify custom credentials. Click "Next".
- Click "Next" on the "Web Service Keywords and Namespace" window.
- Click "Finish" to complete the deployment process. Note the Web Service URL on the "Web service successfully deployed to the following location:" window. For example: "http://sas-aap.demo.sas.com:80/SASBIWS/services DownloadFile".
- You can test this URL by doing a POST request to it in Postman. 
	
## Troubleshooting

In this section, we will see how to fix some common issues related to this utility. 

### Custom Task Type Connector

- Connector not calling the Flask Web Service.
	- Check if the connector is associated to a general agent.
	- Check if the general agent is up and running.
	- Check if the Python Flask service url is correct.
	- Check if the Flask service is running.
	- Restart flask service if required.
	- Restart general agent if required. 

### Flask Service

- Flask Service is not responding.
	- Check if general agent is running. 
	- Check if flask service is running. 
	- Check if flask configuration is correct.
	- Restart flask service if required.

### Stored Process Web Service

- SASBIWS is not responding.
	- Check if STP url is correct.
	- Check if STP server is up and running.
	- Check if STP configuration is corect.
	- Restart STP server if required.

## Additional Resources

### 360 Documentation
- 360: https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintwlcm/home.htm 
- General Access Point: https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/ext-access-pts-general.htm 
- Custom Task Type: https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/custom-task-create.htm
- Connectors: https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/integrating-about-connectors.htm 

### SAS BI Web Services Documentation
- SAS BI Web Services: https://go.documentation.sas.com/doc/en/itechcdc/9.4/wbsvcdg/titlepage.htm 

### Python Flask Documentation
- Python Flask: https://pypi.org/project/Flask/
