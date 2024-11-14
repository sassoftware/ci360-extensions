# CI360 Google BigQuery Event Streaming Agent

## Index

1. [Overview](#overview)
2. [Design](#design)
    1. [Authentication](#authentication)
    2. [Connectivity](#connectivity)
    3. [Logging](#logging)
3. [Prerequisites](#prerequisites)
4. [Using the Agent](#using-the-agent)
    1. [Configuration](#configuration)
    2. [Running the Agent](#running-the-agent)
    3. [CI360 Configuration](#ci360-configuration)
5. [Building the Agent](#building-the-agent)
6. [Updates to Agents and SDKs](#updates-to-agents-and-sdks)
7. [View Agent Status](#view-agent-status)

## Overview

CI360 Google BigQuery Event Streaming Agent writes all received events to the configured Google Big Query Table, with selected list of event attributes.
 
## Design

This Agent uses the [Event Streaming](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/integrating-stream-about.htm) framework provided by CI360 and the [Storage Write API](https://cloud.google.com/bigquery/docs/write-api-stream) of Google BigQuery.

The [Storage Write API](https://cloud.google.com/bigquery/docs/write-api#default_stream) provides a default stream, designed for streaming scenarios where you have continuously arriving data.

### Authentication

1. Between Agent and CI360 Tenant

Authentication from Agent to CI360 is controlled using the Tenant ID and Client Secret obtained from the user interface of CI360. Please refer to the online [documentation](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/gen-access-local-config.htm) to know more about how to get the Tenant ID and create client secret. 

2. Between Agent and Google

Authentication for BigQuery Storage API Client Libraries can be done in different ways as explained [here](https://cloud.google.com/bigquery/docs/reference/storage/libraries#authentication).

We suggest to use the Service Account mechanism for authentication, details of the same are available [here](https://cloud.google.com/docs/authentication/provide-credentials-adc).

### Connectivity

1. Between Agent and CI360 Tenant

The Agent establishes a secure WebSocket connection to the CI360 Tenant using the credentials provided in the config file. This is an outbound connection initiated by the agent to the CI360 Tenant on the cloud. For this the server where agent is running should have connectivity to CI360 Tenant.

If the connection is closed, the agent will automatically attempt to connect to the tenant again.

2. Between Agent and Google BigQuery

This connection is established automatically by the [BigQuery Storage API Client Libraries](https://cloud.google.com/bigquery/docs/reference/storage/libraries)  based on the Authentication configured on the server.

### Logging

Agents is capable of writing logs at INFO, DEBUG and TRACE levels. For production it is recommended to use INFO level, and for test servers it is recommended to use DEBUG level. 

> **WARNING!**: If you enable "TRACE" level logging, then all the event attributes will be written to the log file. This may open an issue if there are PII information inside the event payload (for example, mobile number or email address.). You will have to limit the use of trace level on production and promptly clean up the log file in order to avoid PII data leak.


Various aspects of the logging can be controlled using the `logback.xml` file including the location, name and the file rolling controls. 

For general information about how to configure logback, please refer to this [link](https://logback.qos.ch/manual/configuration.html).

## Prerequisites
- Java 1.8 or newer
- Maven 3.5.3 or newer (For compiling and building the project.)
- open the agent.config file to enter connection details to CI360 and Google Big Query.
- Authentication configured for Google BigQuery Client Libraries.
- Tables created in BigQuery for inserting the event data.

## Using the Agent

If everything is packaged, you can copy the created ZIP file to any location, extract it and apply needed changes to agent.config file. 

### Configuration 

#### CI360 gateway settings

You need to provide correct values for

- __ci360.gatewayHost__: Hostname for CI360 API gateway (can be obtained from Settings page, Access in CI360 user interface.)
- __ci360.tenantID__: tenant ID for access point
- __ci360.clientSecret__: client secret for access point

#### Standard agent config

You need to provide correct values for

- __agent.keepaliveInterval__: interval at which keep-alive process runs and send a ping to CI360 gateway (in milliseconds) - used as a workaround for aggressive firewall timeouts (0 = disabled)
- __agent.runInteractiveConsole__: allow interactive console (true or false, should be false for production)
- __agent.monitorOutputInterval__: interval at which agent monitor process runs and prints out current event stats (in milliseconds)
- __agent.failedEventFileLocation__: Folder location where the failed events needs to be written. Events will be written into individual files.
-__agent.max_error_count__: Specify how many back-to-back errors should the agent manage before stop acknowledging events to CI360.   

#### Google BigQuery Dataset and Table configurations

You need to provide correct values for

- __db.writeToDb__: This must be set to `true` for the agent to write the data to GBQ. If set to any other value, events will not be written to DB.

- __db.projectId__: This is the project ID from Google BigQuery.

- __db.datasetName__: Dataset name in Google Big Query.

- __db.tableName__: The table name in the Google BigQuery.

- __db.columns__: Columns that needs to be written to the table.
Here is a list that we have agreed upon for now: `eventname,generatedTimestamp,subject_id,datahub_id,event_uid,event_json`

> If you provide a column name that is not there in the GBQ table, then the agent will fail inserting the data to GBQ. Hence please ensure to validate the column name.

- __db.convert_to_date_time_columns__: The timestamp provided by CI360 are in the epoch long format. This needs to be converted to a Timestamp format that GBQ understands. 
For now the only column that needs this conversion is `generatedTimestamp`

- __db.max_retry_count__: This controls how many times the agent will try to insert data into GBQ before failing and writing it to a file. Set this to `3` for ideal performance. 

> For database output, agent will match CI360 event attributes by name (not case sensitive) to database table columns. Database table name is specified using __db.eventTable__ configuration parameter. If CI360 event contains an attribute matching the name of a database table column (mentioned using __db.columns__ ), it will be written to the table, otherwise NULL will be inserted. For that reason, it is recommended that any columns for the destination table be created as nullable. 

> TIP: If you create a column named `event_json` in the table and list that in __db.columns__ the agent will automatically write the entire event json as a text into this column. This is not advisable for production as it will affect the size of the table. This is useful for validating the data received by the agent.

### Running the Agent

Please update the content of the respective files before running the agent. Here are the startup scripts that you can use.

on Windows:
```
gbq-event-stream-agent.cmd

```

on Unix/Linux:
```
./gbq-event-stream-agent

```

Based on your folder structure you may want to update the location of the JAR file on the script.

### CI360 Configuration

In order to stream events from CI360 to the agent, access point needs to be created in CI360. To create an access point in SAS Customer Intelligence 360:
1. From the user interface, navigate to **General Settings** > **External Access** > **Access Points**
2. Create a new access point if one does not exist
3. Get the following information from the access point:  
```
External gateway address: e.g. https://extapigwservice-<server>/marketingGateway  
Name: ci360_agent  
Tenant ID: abc123-ci360-tenant-id-xyz  
Client secret: ABC123ci360clientSecretXYZ  
```

Tenant ID and Client secret are needed for agent.config above. 

Once access point is created, you can associate events you'd like to stream to the agent. This can be either done on Access Point configuration page under Associations, or for individual events on their respective Orchestration pages (under External Availability). 

For more information about configuring a CI360 General Agent and associating events for streaming, please refer to the product documentation [here]( https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/ext-access-pts-general.htm)

## Building the Agent
Prior to compiling the agent, you need to install agent SDK. Download the SDK from CI360 and follow instructions included in SDK to install into local Maven repository. For example:
```
mvn install:install-file -Dfile=<path where CI360 agent was downloaded>/sdk/mkt-agent-sdk-jar-1.current release.jar -DpomFile=path where CI360 agent was downloaded/sdk/pom.xml
```

To compile project:
```
mvn compile
```

To package the agent (package will compile, test and package the agent):
```
mvn package
```

Distribution file will be created in target folder, in .zip format.

To test run the agent:
```
mvn exec:java -Dlogback.configurationFile=logback.xml -DconfigFile=agent.config -Dexec.mainClass=com.sas.ci360.agent.CustomAgent
```
Detailed steps for how to create your own agent is given [here](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/gen-access-create.htm)

## Updates to Agents and SDKs

Although agents and SDKs are typically supported for at least three versions with backward compatibility. While an agent that is a few months old would still be usable, you would not be provided with any new functionality until you upgrade to the newest version of the agent. Therefore, it is recommended to upgrade to the latest version every month. The Agent Version column of the access points table displays warnings if the version of the agent that is being used is either expiring soon or is already at the point of no longer being supported. The appropriate message is also displayed at the top of each page after a specific access point has been selected from the access points table.

The warnings read as follows:

```
"This version of the agent is about to expire. Updating the agent to the latest version is recommended."
```

```
"This version of the agent is no longer supported. It must be updated to a supported version to be able to connect to SAS Customer Intelligence 360."
```
```
A list of supported and soon-to-be-unsupported SDK versions is available in the log.
```
Sample log file entry:
```
2023-02-13 14:00:56,560 INFO  [t@1334618867-34] com.sas.mkt.agent.sdk.StreamWebSocket    - Gateway version: v2301
2023-02-13 14:00:56,560 INFO  [t@1334618867-34] com.sas.mkt.agent.sdk.StreamWebSocket    - Gateway supported versions: v2301,v2212,v2211,v2210,v2209,v2208,v2207,v2206
2023-02-13 14:00:56,561 INFO  [t@1334618867-34] com.sas.mkt.agent.sdk.StreamWebSocket    - Gateway warning versions: v2205,v2204,v2203
```
### Updating the agent with latest CI360 libraries

You will have to [download](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/ext-access-about.htm#n02294z7ejrl6an1ekr3w29exdxp) the latest Agent SDK from SAS CI360 and then follow the steps mentioned under [Building the Agent](#building-the-agent).

We recommend to test the new build on your development or test environment before deploying them on the production.

## View Agent Status
There two options:

1. `Through the UI`: To view the status of an existing agent, click Details View. The Agent Status window notifies you if the agent is active.

2. `Log file`: Look for Ping Status printed in the agent log file every 5 minutes. 

Sample log entry:
```
2023-02-13 15:28:41,916 INFO  [KeepaliveThread] com.sas.mkt.agent.sdk.CI360Agent         - Calling GET on https://extapigwservice-training.ci360.sas.com/marketingGateway/commons/healthcheck
2023-02-13 15:28:42,126 INFO  [KeepaliveThread] com.sas.ci360.agent.CustomAgent          - Ping response: {"status":"UP"}
```

For more details related to this agent, please contact your SAS representative. 