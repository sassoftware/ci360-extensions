# SAS Customer Intelligence 360 - Extensions


## Overview
SAS CI360 Connector Framework and Agent SDK provide infrastructure to integrate SAS Customer Intelligence 360 (CI360) with other applications. This repository contains a number of connectors and agents that are ready to be used to enhance the capabilities of CI360 and connect to 3rd party services. In addition, there are other integration assets included as well. All integration assets included in this repository are listed below, with short description. Details about each are included within specific sub-folders.

## Table of Contents

This topic contains the following sections:
* <a href="#prerequisites">Prerequisites</a>
* <a href="#installation">Installation</a>
* <a href="#list-of-extensions">List of Extensions</a>
* <a href="#getting-started">Getting Started</a>
* <a href="#contributing">Contributing</a>
* <a href="#license">License</a>
* <a href="#resources">Additional Resources</a>

## Prerequisites

Prerequisites vary between integration assets included here, and are listed in detail for each in the respecitive sub-folders. But some of the prerequisites include:
- Amazon Web Service account with access to Lambda and API Gateway service
- Microsoft Azure account with access to Functions and other Azure services
- Java 8 or newer for 23.07 release or earlier, Java 11 or newer for 23.08 release or later
- Python 3.7 or newer

## Installation

Installation instructions for every extension are included in the project specific sub-folder README file. Installation and deployment instructions are platform specific.

## List of Extensions

This is a list of connectors, agents and other related content and utilities included in this repository.

### Integrations

- [__SMS/MMS (via Syniverse)__](code/ci360-scg-connector): Syniverse Communication Gateway (SCG) connector, enables SMS and MMS communication through Syniverse
- [__WhatsApp (via Syniverse)__](code/ci360-scg-connector): Syniverse Communication Gateway (SCG) connector, enables WhatsApp communication through Syniverse
- [__SMS (via SFMC)__](code/ci360-sfmc-connector): Salesforce Marketing Cloud (SFMC) connector, enables SMS through Salesforce Marketing Cloud
- [__Email (via SFMC)__](code/ci360-sfmc-connector): Salesforce Marketing Cloud (SFMC) connector, enables Email communication through Salesforce Marketing Cloud
- [__SMS (via Twilio)__](code/ci360-twilio-connector): Twilio connector, enables SMS through Twilio
- [__SMS (via Twilio)__](code/ci360-twilio-custom-task-connector): Twilio connector, enables SMS through Twilio, no-code integration using CI360 custom task types
- [__Google Analytics__](code/google-analytics-integration): Implementation of connection with Google Analytics (GA)
- [__Facebook Event Manager__](code/facebook-event-manager-integration): Implementation of connection with Facebook Event Manager
- [__Adobe Audience Manager__](code/adobe-audience-manager-integration): Implementation of connection with Adobe Audience Manager (AAM)
- [__SAS Event Stream Processing__](code/ci360-esp-agent): SAS Event Stream Processing (ESP) agent enables streaming of CI360 events into ESP
- [__SAS ESP CI360 Adapter__](code/esp-ci360-adapter): SAS Event Stream Processing adapter that allows streaming of events from an ESP window to CI360
- [__SAS Cloud Analytic Services__](code/ci360-cas-agent): SAS Cloud Analyic Services (CAS) Agent streams CI360 events into a CAS table
- [__Snowflake Streaming Agent__](code/ci360-snowflake-agent): CI360 Snowflake Agent streams all received events to the configured Snowflake instance
- [__CRM (via Salesforce)__](code/ci360-sfdc-connector): Salesforce CRM (SFDC) connector, enables creating contacts and cases in SFDC
- [__CI360 Debug Agent__](code/ci360-debug-agent): CI360 Debug agent streams events into log files, console output, local database or Elastic search API
- [__CI360 Event to DB Agent__](code/ci360-events-to-db-agent): Stream CI360 events to DB Table
- [__AWS S3__](code/ci360-s3-bulk-connector): Upload customer data to AWS S3 bucket
- [__Google BigQuery Streaming Agent__](code/ci360-gbq-event-streaming-agent): Google BigQuery agent enables streaming of CI360 events into Google BigQuery table
- [__Kafka CI360 Connector__](code/kafka-ci360-connector): Send events from Kafka to CI360
- [__CI360 Braze Bulk User Import Connector__](code/ci360-braze-bulk-user-import-connector): Send customer audiences and attributes generated in CI360 from various sources to Braze 
- [__MailChimp List Upload__](code/ci360-mailchimp-bulk-connector): Send customer lists and attributes from CI360 to MailChimp for use in email campaign activation
- [__Azure Eventhub CI360 Connector__](code/eventhub-ci360-connector): Stream events from Azure Event Hub to CI360
- [__Azure Blob Storage__](code/ci360-azure-blob-storage-bulk-connector): Upload CI360 generated customer data to Azure Blob storage
- [__CI360 Match__](code/ci360-match-bulk-connector): Sync CI360 targeted audiences to CI360 Match
- [__Optilyz Connector__](code/ci360-optilyz-bulk-connector): Send customer audiences and attributes generated in CI360 from various sources to Optilyz for direct mail automation
- [__CI360 Direct Send to Scheduler__](code/ci360-direct-send-to-scheduler/): Allows a CI 360 User to create deployed jobs from the CI 360 tenant for Direct Marketing Tasks, Bulk Email Tasks and Direct Segment Maps for a SAS Administrator to schedule on-prem in the SAS LSF scheduler.

### Utilities

- [__Google Tag Manager__](https://github.com/sassoftware/sas-ci360-template-google-tag-manager): Employ the use of Google Tag Manager's Community Templates in order to easily deploy javascript actions for Customer Intelligence 360
- [__Snowy CI360__](code/snowy): Browser extension as an easy way to monitor the network traffic (POST) to SAS CI 360, with the ability to search the form data
- [__API Helper for CI360__](code/ci360-api-helper): Interact with CI360 APIs using easy to use web based UI
- [__CI360 API DS2 Utilities__](code/ci360-api-ds2-utilities): Utilities package for CI360 API interaction using DS2
- [__Customer Data Upload for CI360__](code/ci360-customer-data-uploader): Upload customer data to CI360 cloud datahub table
- [__Snowflake UDM Loader__](code/ci360-udm-loader-snowflake): CI360 UDM Loader for Snowflake automates the download of UDM data from Customer Intelligence 360 and then seemlessly loads this into Snowflake, utilizing SAS provided Python download client within a Docker container
- [__CI360 GDPR Delete__](code/ci360-gdpr-delete): GDPR remove customer cloud data from CI360
- [__CI360 Identity Uploader__](code/ci360-new-identities-uploader): Upload new identities to CI360
- [__CI360 Audience from Viya (custom step)__](code/ci360-audience-from-viya): Upload analytically defined customer audience from Viya into CI360 using Viya Custom Step
- [__CI Modernization Preparation Tool__](code/ci-modernization-preparation-tool): Customer-facing tool intended to support the modernization from CI 6.6 to SAS CI 360
- [__CI360 Direct SQL Extraction Utility__](code/ci360-engage-direct-sql-extraction-utility): Utility for extracting generated SQL statements from CI360 Direct logs
- [__CI360 Direct Data Item Extraction Utility__](code/ci360-engage-direct-dataitem-log-extraction-utility): Utility for creating a report on used data items in CI360 Direct Marketing tasks and segment maps
- [__CI360 Tenant Copy Utility__](code/ci360-tenant-copy-utility): CI360 Copy Utility helps in copying objects from one tenant to another
- [__CI360 Audit Data Download__](code/ci360-audit-data-download): Download the Audit data from CI360 in parquet and CSV formats
- [__CI360 Custom Task Connector Integration with On-prem SAS STP__](code/ci360-connector-sas-stp-integration-using-python-flask): Integrate a CI360 Custom Task Type using a connector with an onprem SAS stored process (STP)
- [__CI360 Audience Management Utility__](code/ci360-audience-management-utility): Create, manage and populate CI360 audiences using Python scripts
- [__CI360 Custom Code Nodes__](code/ci360-custom-code-nodes): Stored process that extracts SAS code from a specific location and makes them available to run in CI360 as part of a stored process
- [__JavaScript Event API Support__](code/ci360-js-event-api-support-doc): Code snippets for for SPA applications, delivering a spot/creative using SAS CI360
- [__CI360 UDM Database Loader__](code/ci360-udm-db-loader): Load data from the CI 360 Unified Data Model (UDM) into a relational database management system (RDBMS)
- [__CI360 Direct Agent Monitor__](code/ci360-direct-agent-monitor): Assits with automating Direct Agent health checks
- [__CI360 General Agent Log Analyzer__](code/ci360-general-agent-log-analyzer): Provides insights on performance metrics, when integrating a CI360 custom connector with an on-premises endpoint
- [__SAS CI 360 VeloxPy__](code/sas-ci360-veloxpy): Automation-ready, secure, and extensible Python library for interacting with RESTful APIs of CI 360 using asynchronous I/O. 

- [__CI360 Direct Merge Variables__](code/ci360-direct-merge-variables): This stored process is intended to be used as a post process in the CI360 Engage Direct DM Task.   It updates any SAS datasets exported in that task, with corresponding dataitems from other SAS datasets.

## Getting Started

To set up and use the provided extension code you need to perform the following steps :

### Download sample code
1. Download a `ci360-extensions` project source code into zip/tar.gz/tar.bz2/tar file format on your local machine.<br/>
   `Note:` You can also clone the project on your local machine

2. The project will be downloaded on your local machine in your specified file format. You need to unzip/untar the downloaded project.  

3. You can see the folder `ci360-extensions` after unzip/untar the project.

4. Open the `ci360-extensions` folder. It will contain multiple sub-folders for various connector and agent integration resources.

### Build or Deploy
Build and deployment instructions are included for each of the integration assets in their respoective sub-folders.


### Deploying CI360 Connectors
Please refer [Set up a Custom Connector](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/ext-connectors-custom.htm) section in SAS Customer Intelligence 360 admin guide.

##### Register your connector into CI360

For most connector or agent integration assets, you need to register the connector and endpoint with these details into the CI360 system to use the connector. Details are included for each connector or agent in their sub-folders. Documentation sections are referenced below for eacy access.

**Add and Register a Connector**
Please refer to [`Add and Register a Connector`](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/ext-connectors-add.htm) in SAS Customer Intelligence 360 admin guide.

**Add an Endpoint**
Please refer to [`Add an Endpoint`](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/ext-connectors-add-endpoint.htm) in SAS Customer Intelligence 360 admin guide.

### Deploying CI360 Agents

For CI360 agent development, agent SDK needs to be downloaded and installed into the local Maven repository. See [`Download the General Agent`](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/gen-access-download.htm) in SAS Customer Intelligence 360 admin guide. General Agent includes the SDK.

To install SDK JAR into local Maven repository:
```
mvn install:install-file -Dfile=<path where CI360 agent was downloaded>/sdk/mkt-agent-sdk-jar-1.current release.jar -DpomFile=path where CI360 agent was downloaded/sdk/pom.xml
```

**Create an Access Point Definition**
Please refer to [`Create an Access Point Definition`](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/extapi-config-agentdefa.htm) in SAS Customer Intelligence 360 admin guide.

### Extension Best Practices

##### Logging and PII

Many agents and connectors included here log information for debugging purposes, either in local log files or cloud based logging services. While connectors and agents generally won't explicitly log any PII data, some include mechanism for logging complete CI360 event objects, based on logging levels configured, usually controlled using logging specific configuration files. In order to avoid logging or storage of PII data, it is a best practice to either disable logging by setting the logging level, or exclude PII data from being sent by CI360.

## Contributing

> We welcome your contributions! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to submit contributions to this project. 


## License

> This project is licensed under the [Apache 2.0 License](LICENSE).


## Additional Resources

For more information, see [External Data Integration with Connectors](http://documentation.sas.com/?cdcId=cintcdc&cdcVersion=production.a&docsetId=cintag&docsetTarget=ext-connectors-manage.htm&locale=en#p0uwf5nm4rrkn1n1gwrm03rh911r).
