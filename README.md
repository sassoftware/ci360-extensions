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
- Java 1.8 or newer
- Python 3.7 or newer

## Installation

Installation instructions for every extension are included in the project specific sub-folder README file. Installation and deployment instructions are platform specific.

## List of Extensions

This is a list of connectors and agents included in this repository:
- [__SMS/MMS (via Syniverse)__](code/ci360-scg-connector): Syniverse Communication Gateway (SCG) connector, enables SMS and MMS communication through Syniverse
- [__WhatsApp (via Syniverse)__](code/ci360-scg-connector): Syniverse Communication Gateway (SCG) connector, enables WhatsApp communication through Syniverse
- [__WeChat (via Syniverse)__](code/ci360-scg-connector): Syniverse Communication Gateway (SCG) connector, enables WeChat communication through Syniverse
- [__SMS (via SFMC)__](code/ci360-sfmc-connector): Salesforce Marketing Cloud (SFMC) connector, enables SMS through Salesforce Marketing Cloud
- [__Email (via SFMC)__](code/ci360-sfmc-connector): Salesforce Marketing Cloud (SFMC) connector, enables Email communication through Salesforce Marketing Cloud
- [__SMS (via Twilio)__](code/ci360-twilio-connector): Twilio connector, enables SMS through Twilio
- [__Google Analytics__](code/google-analytics-integration): Implementation of connection with Google Analytics (GA)
- [__Facebook Event Manager__](code/facebook-event-manager-integration): Implementation of connection with Facebook Event Manager
- [__Adobe Audience Manager__](code/adobe-audience-manager-integration): Implementation of connection with Adobe Audience Manager (AAM)
- [__Google Tag Manager__](https://github.com/sassoftware/sas-ci360-template-google-tag-manager): Employ the use of Google Tag Manager's Community Templates in order to easily deploy javascript actions for Customer Intelligence 360
- [__SAS Event Stream Processing__](code/ci360-esp-agent): SAS Event Stream Processing (ESP) agent enables streaming of CI360 events into ESP
- [__SAS ESP CI360 Adapter__](code/esp-ci360-adapter): SAS Event Stream Processing adapter that allows streaming of events from an ESP window to CI360
- [__CI360 Debug Agent__](code/ci360-debug-agent): CI360 Debug agent streams events into log files, console output, local database or Elastic search API
- [__SAS Cloud Analytic Services__](code/ci360-cas-agent): SAS Cloud Analyic Services (CAS) Agent streams CI360 events into a CAS table
- [__Snowflake Streaming Agent__](code/ci360-snowflake-agent): CI360 Snowflake Agent streams all received events to the configured Snowflake instance
- [__Snowflake UDM Loader__](code/ci360-udm-loader-snowflake): CI360 UDM Loader for Snowflake automates the download of UDM data from Customer Intelligence 360 and then seemlessly loads this into Snowflake, utilizing SAS provided Python download client within a Docker container
- [__CRM (via Salesforce)__](code/ci360-sfdc-connector): Salesforce CRM (SFDC) connector, enables creating contacts and cases in SFDC

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

For CI360 agent development, agent SDK needs to be downloaded and installed into the local Maven repository. See [`Download an Access Point SDK`](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/extapi-config-downloadsdk.htm) in SAS Customer Intelligence 360 admin guide.

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