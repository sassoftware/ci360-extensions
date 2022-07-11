 # CI360 Snowflake Agent

## Overview

CI360 Snowflake Agent writes all received events to the configured Snowflake instance. Only the configured columns/attributes will be written to the Snowflake table.
 

## Prerequisites
- Java 1.8 or newer
- Maven 3.5.3 or newer
- open the agent.config file to enter connection details to CI360 
- A valid Snowflake account and access credentials

## Using the Agent
If everything is packaged, you can copy the created ZIP file to any location, extract it and apply needed changes to agent.config file. 

### Configuration 

#### CI360 gateway settings

You need to provide correct values for
```
ci360.gatewayHost=extapigwservice-xxxxxx 
ci360.tenantID=xxxxxx 
ci360.clientSecret=xxxxxx
```

- __ci360.gatewayHost__: Hostname for CI360 API gateway (can be obtained from Settings page, Access)
- __ci360.tenantID__: tenant ID for access point
- __ci360.clientSecret__: client secret for access point

#### Standard agent config
- __agent.keepaliveInterval__: interval at which keepalive process runs and send a ping to CI360 gateway (in milliseconds) - used as a workaround for aggressive firewall timeouts (0 = disabled)
- __agent.runInteractiveConsole__: allow interactive console (true or false, should be false for production)
- __agent.monitorOutputInterval__: interval at which agent monitor process runs and prints out current event stats (in milliseconds)
- __agent.lastEventOutput__: filename where last event payload (JSON) should be written, if omitted, the file will not be written (should be disabled in production)
- __agent.write_failed_events_folder__: If you wish to write the event JSON to a folder, in case of an error writing it to Snowflake, configure the folder name. Remember to consider the PII implications on production.

Set or update Snowflake database-connection information.
```
sf.connction_url=jdbc:snowflake://<provide your snowflake account>.snowflakecomputing.com
sf.PRIVATE_KEY_FILE=<<Location and file name of your RSA private key.>>
sf.user=<<Snowflake user name>>
sf.database=<<Snowflake database>>
sf.schema=<<Snowflake schema>>
sf.warehouse=<<Snowflake warehouse>>
sf.role=<<Snowflake role of the user>>
sf.table_name=<<Snowflake table name where you want the events to be written>>
sf.columns=eventname,sessionId,channelType,timestamp,datahub_id,rowKey,visitor_state,page_title,searchTerm,page_name,customer_id,subject_id,login_id,EVENT_JSON

```
```
Snowflake column names can be configured based on your requirement. The column names are matched with the event attributes of the 360 events. If not found an empty string will be added for that column.
The column name 'EVENT_JSON' acts as a keyword for inserting the entire event JSON to a column named EVENT_JSON in the Snowflake table you configured.
```
For generating and configuring the key pair for Snowflake, please refer to the documentation here: https://docs.snowflake.com/en/user-guide/key-pair-auth.html

Plain text/logging output of raw event data is controlled by logback.xml configuration file.

### Running the Agent

Then you can run the start-up script:

Ensure that you have the private key on the server you are running the agent. Refer to documentation for more details: https://docs.snowflake.com/en/user-guide/key-pair-auth.html

on Windows:
```
run_agent.cmd
```

on Unix/Linux:
```
./run_agent.sh
```

### CI360 Configuration

In order to stream events from CI360 to the agent, an access point needs to be created in CI360. To create an access point in SAS Customer Intelligence 360:
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

Once the access point is created, you can associate events you'd like to stream to the agent. This can be either done on the Access Point configuration page under Associations or for individual events on their respective Orchestration pages (under External Availability). 

## Building the Agent
Prior to compiling the agent, you need to install agent SDK. Download the SDK from CI360 and follow the instructions included in SDK to install it into the local Maven repository. For example:
```
mvn install:install-file -Dfile=<path where CI360 agent was downloaded>/sdk/mkt-agent-sdk-jar-1.current release.jar -DpomFile=path where CI360 agent was downloaded/sdk/pom.xml
```

To compile the project:
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