# ESP CI360 Adapter

## Overview

SAS ESP (SAS Event Stream Processing) adapter that subscribes to a window in ESP and streams events to CI360 external event API. It enables users to stream events from an active ESP window in real time to CI360.

### Quick Start

To use the ESP-CI360 adapter, follow these steps after the project has been built and packaged (or if you already have a distribution archive):
1. Unzip/copy the adapter to a directory where it will run
2. Edit configuration file (__config.properties__) and specify ESP and CI360 connection information
3. Run the adapter using the included script __start_esp_subscriber.sh__  

## Build Process

Prerequisites: 
- Java 1.8 or newer
- Maven 3.5.3 or newer
- dfx-esp-api installed in the local Maven repository (needs to match version in pom.xml)

You can build the agent by executing:
 
```
mvn package
```

Distribution archive (ZIP) will be created in target folder. 

Required dfx-esp-api library JAR can be found in your local installation of SAS ESP, in $DFESP_HOME/lib directory. It is a best practice to always use the JAR included with your current installation to ensure version compatibility. You can reference the included command line to install required dfx-esp-api into local Maven:
 
```
mvn install:install-file -Dfile=dfx-esp-api.jar -DgroupId=com.sas.esp -DartifactId=dfx-esp-api -Dversion=6.2 -Dpackaging=jar -DgeneratePom=true
```

## Using the Adapter

When adapter is running, it will subscribe to an active ESP window. Every event from the subscribed window will be streamed to CI360 as an external event. In order to use the adapter, the following steps need to be compeled:
- ESP server needs to be running
- ESP project needs to be loaded into ESP and running
- Adapter configuration file updated with ESP configuration (host, port and subscribed window information) and CI360 configuration (API gateway URL, token and External event name)
- CI360 external event needs to be created in CI360 and active (published)

### Configuration

You need to change the connection settings in the "config.properties" file to specify ESP server and project, as well as information needed connect to your access point in CI360 (gateway URL and token).

###### Windows
You can run the adapter on Windows by executing follwing command:
```
start_esp_subscriber.bat
```

###### Unix
You can run the adapter on Linux/Unix by calling:
```
./start_esp_subscriber.sh config.properties
```

Configuration file config.properties has the following parameters:

### Configuration parameters

###### CI360 settings
- __ci360.gateway__: URL for CI360 API gateway (can be obtained from Settings page, Access)
- __ci360.token__: CI360 API token
- __ci360.identity__: identity field to be used for CI360 external events
- __ci360.exteventname__: name of CI360 external event to be injected (External event has to be active in CI360 tenant - created and published)

###### ESP settings
- __esp.host__: hostname for ESP server
- __esp.port__: port for ESP server
- __esp.urlParameters__: URL parameters to use when injecting an event using HTTP
- __esp.Project__: ESP project we are subscribing to
- __esp.Query__: ESP query we are subscribing to
- __esp.Window__: ESP window we are subscribing to

### CI360 Setup

In order to inject external events from ESP into CI360, external event needs to be created and published. In addition, external event attributes need to be defined, and need to match ESP window attributes that we want to capture as part of the event. Once event is active and events are injected into CI360 by the adapter, it can be used to trigger tasks, activities or as targeting criteria.

For more information on creating External Events, see [`Creating External Events`](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintug/events-external.htm) in SAS Customer Intelligence 360 user guide.
