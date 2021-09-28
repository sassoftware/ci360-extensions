# SAS Customer Intelligence 360 - Extensions


## Overview
SAS CI360 Connector Framework and Agent SDK provide infrastructure to integrate CI360 applications with other applications. This repository contains a number of connectors and agents that are ready to be used to enhance the capabilities of CI360 and connect to 3rd party services. In addition, there are other integration assets included as well. All integration assets included in this repository are listed below, with short description. Details about each are included within specific sub-folders.

## Table of Contents

This topic contains the following sections:
* <a href="#prerequisites">Prerequisites</a>
* <a href="#installation">Installation</a>
* <a href="#list">List of Extensions</a>
* <a href="#getstart">Getting Started</a>
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
- [__ci360-scg-agent__](ci360-scg-agent): Syniverse Communication Gateway (SCG) agent, enables SMS/MMS/WhatsApp through Syniverse
- [__ci360-scg-connector__](ci360-scg-connector): Syniverse Communication Gateway (SCG) connector, enables SMS/MMS/WhatsApp through Syniverse
- [__ci360-sfmc-connector__](ci360-sfmc-connector): Salesforce Marketing Cloud (SFMC) connector, enables Email and SMS through Salesforce

## Getting Started

To set up and use the sample connector codes you need to perform the following steps :

### Download sample connector code
1. Download a `ci360-extensions` project source code into zip/tar.gz/tar.bz2/tar file format on your local machine.<br/>
   `Note:` You can also clone the project on your local machine

2. The project will be downloaded on your local machine in your specified file format. You need to unzip/untar the downloaded project.  

3. You can see the folder `ci360-extensions` after unzip/untar the project.

4. Open the `ci360-extensions` folder. It will contain multiple sub-folders for various connector and agent integration resources.

### Build or Deploy
Build and deployment instructions are included for each of the integration assets in their respoective sub-folders.

### Deploying CI360 Connectors
Please refer [Set up a Custom Connector](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/ext-connectors-custom.htm) section in SAS Customer Intelligence 360 admin guide.

### Register your connector into CI360

For most connector or agent integration assets, you need to register the connector and endpoint with these details into the CI360 system to use the connector. Details are included for each connector or agent in their sub-folders. Documentation sections are referenced below for eacy access.

**Add and Register a Connector**
Please refer to [`Add and Register a Connector`](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/ext-connectors-add.htm) in SAS Customer Intelligence 360 admin guide.

**Add an Endpoint**
Please refer to [`Add an Endpoint`](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/ext-connectors-add-endpoint.htm) in SAS Customer Intelligence 360 admin guide.


## Contributing

> We welcome your contributions! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to submit contributions to this project. 


## License

> This project is licensed under the [Apache 2.0 License](LICENSE).


## Additional Resources

For more information, see [External Data Integration with Connectors](http://documentation.sas.com/?cdcId=cintcdc&cdcVersion=production.a&docsetId=cintag&docsetTarget=ext-connectors-manage.htm&locale=en#p0uwf5nm4rrkn1n1gwrm03rh911r).