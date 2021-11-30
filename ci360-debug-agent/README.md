# CI360 Debug Agent

## Overview

CI360 Debug Agent writes all received events with and their JSON parameters into a file and into the console and be used to debug events. It can also write all events into a database table, and/or send to Elastic API (for search and streaming dashboard).
This agent also provides an example of steps and sample code that are needed to develop your own agent to stream events.
 
Events which are received by CI360 can be tracked into 3 files: 
- summary of events: per event one line with timestamp and event name
- all events with complete JSON parameters
- last event with JSON parameters

Events can also be stored/forwarded to:
- Elastic search (using Elastic HTTP API)
- Database table (local using JDBC connection) 

## Prerequisites
- Java 1.8 or newer
- Maven 3.5.3 or newer
- open the agent.config file to enter connection details to CI360 

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
- __agent.runInteractiveConsole__: allow interacive console (true or false, should be false for production)
- __agent.monitorOutputInterval__: interval at which agent monitor process runs and prints out current event stats (in milliseconds)
- __agent.lastEventOutput__: filename where last event payload (JSON) should be written, if omitted, file will not be written (should be disabled in production)

Also, set or update the values for Elastic search, if applicable
```
elastic.callElastic=true 
elastic.host=
elastic.port=9200 
elastic.index=ci360events
```

Set or update database connection information, if applicable
```
db.writeToDb=true 
db.url=jdbc:postgresql://127.0.0.1:5432/postgres 
db.user= 
db.password= 
db.minIdle=5 
db.maxIdle=10 
db.maxOpenPreparedStatements=100 
db.eventTable=events
```

For database output, agent will match CI360 event attributes by name (not case sensitive) to database table columns. Database table name is specified using __db.eventTable__ configuration parameter. If CI360 event contains an attribute matching the name of a database table column, it will be written to the table, otherwise NULL will be inserted. For that reason, it is recommeneded that any columns for the destination table be created as nullable. 

Plain text/logging output of raw event data is controlled by logback.xml configuration file.

### Running the Agent

Then you can run the startup script:

on Windows:
```
run_agent.cmd
```

on Unix/Linux:
```
./run_agent.sh
```

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

