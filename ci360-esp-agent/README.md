# CI360 ESP Agent

## Overview

CI360 ESP Agent streams events from SAS CI360 (Customer Intelligence 360) to SAS ESP (Event Stream Processing). Events associated with an access point in CI360 are streamed to the agent and injected as new events (insert operation) into a source window in running ESP server.

## Prerequisites

- Java version 1.8 or later
- Apache Maven version 3.6 or later
- SAS Event Stream Processing version 6.1 or later
- Access point in SAS Customer Intelligence 360
    1. From the user interface, navigate to **General Settings** > **External Access** > **Access Points**
    2. Create a new access point if one does not exist
    3. Get the following information from the access point:  
       ```
        External gateway address: e.g. https://extapigwservice-<server>/marketingGateway  
        Name: ci360_agent  
        Tenant ID: abc123-ci360-tenant-id-xyz  
        Client secret: ABC123ci360clientSecretXYZ  
       ```

## Using the Agent

You need to change the connection settings in the "agent.config" file to connect to your access point in CI360
- ci360.gatewayHost 
- ci360.tenantID 
- ci360.clientSecret

You also need to provide ESP connection settings and project/window names. See Configuration section for details.

#### Windows
You can run the agent on windows by executing follwing cmd:
```
run_esp_agent.cmd
```

#### Unix
You can run the agent on linux/unix by calling:
```
./run_esp_agent.sh
```

If you want to start ESP server, load the prepared ESP project and also connect the CAS adapter packaged with SAS ESP, then you need to run:
```
./1_start_esp_server.sh
sleep 3
./2_load_esp_project.sh
sleep 2
./3_run_cas_adapter.sh
sleep 1
./4_start_ci360_esp_agent.sh
./status.sh
```

Sample CI360_Event_Stream.xml project has been included.

The script "status.sh" prints the status of your running scripts.

## Configuration

Configuration file agent.config has the following parameters:

### Standard agent config - CI360 gateway settings
- __ci360.gatewayHost__: Hostname for CI360 API gateway (can be obtained from Settings page, Access)
- __ci360.tenantID__: tenant ID for access point
- __ci360.clientSecret__: client secret for access point

### Standard agent config
- __agent.keepaliveInterval__: interval at which keepalive process runs and send a ping to CI360 gateway (in milliseconds) - used as a workaround for aggressive firewall timeouts (0 = disabled)

### ESP settings
- __esp.host__: hostname for ESP server
- __esp.port__: port for ESP server
- __esp.urlParameters__: URL parameters to use when injecting an event using HTTP
- __esp.Project__: ESP project where event will be injected
- __esp.Query__: ESP query where event will be injected
- __esp.Window__: ESP window where event will be injected


### CI360 Configuration

In order to stream events from CI360 to the agent, access point needs to be created in CI360 (see Prerequisites).
 
Once access point is created, you can associate events you'd like to stream to the agent. This can be either done on Access Point configuration page under Associations, or for individual events on their respective Orchestration pages (under External Availability). 

## Build Process

Prerequisites: 
- Java 1.8 or newer
- Maven 3.6 or newer
- CI360 Agent SDK installed in the local Maven repository (needs to match version in pom.xml)

Prior to compiling the agent, you need to install agent SDK. Download the SDK from CI360 and follow instructions included in SDK to install into local Maven repository. For example:
```
mvn install:install-file -Dfile=<path where CI360 agent was downloaded>/sdk/mkt-agent-sdk-jar-1.current release.jar -DpomFile=path where CI360 agent was downloaded/sdk/pom.xml
```

You can build and package the agent by executing:
 
```
mvn package
```

Distribution archive (ZIP) will be created in target folder. Pre-built distribution is available under Releases.
