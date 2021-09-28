# CI360 SCG Agent

## Overview

Syniverse Communication Gateway (SCG) Messaging agent. Provides connectivity between SAS CI360 and Syniverse API. Supports SMS, MMS, Facebook Messenger and WeChat.

## Prerequisites

1. Install Java (version 1.8 or later)
   
2. Install Apache Maven (version 3.6 or later)

3. Create an access point in SAS Customer Intelligence 360.
    1. From the user interface, navigate to **General Settings** > **External Access** > **Access Points**.
    2. Create a new access point if one does not exist.
    3. Get the following information from the access point:  
       ```
        External gateway address: e.g. https://extapigwservice-<server>/marketingGateway  
        Name: ci360_agent  
        Tenant ID: abc123-ci360-tenant-id-xyz  
        Client secret: ABC123ci360clientSecretXYZ  
       ```

## Installation

1. Build the project using maven package - this will produce a ZIP file in target directory:
    ```
    mvn package
    ```
2. Copy ZIP file to a location where agent will be running and unzip
3. Edit agent.config (see Configuration section below)

## Building a container

Dockerfile is included with agent distribution. It can be used to build an image and run a container. The build and deployment can be customized to suite the environment needs, but the simple approach is documented below.

### Build a Docker image

From top directory of agent distribution (where distribution archive has been unzipped), run the following command:

```
docker build -t ci360-scg-agent .
```

### Run a container

Since we can run multiple containers based on image we built, it is recommended that agent configuration file be maintained separately, on the host machine. Once the agent file has been created, we can run the container using:

```
docker run -d -p 8081:8080 --mount type=bind,source=/opt/install/syniverse/scg_agent_prd1.config,target=/opt/ci360-scg-agent/agent.config --name ci360-scg-prd1 ci360-scg-agent:latest
```



## Using the Agent

### Configuration

Configuration file agent.config has the following parameters:

**Standard agent config - CI360 gateway settings**
- __ci360.gatewayHost__: Hostname for CI360 API gateway (can be obtained from Settings page, Access)
- __ci360.tenantID__: tenant ID for access point
- __ci360.clientSecret__: client secret for access point

**Standard agent config**
- __agent.keepaliveInterval__: interval at which keepalive process runs and send a ping to CI360 gateway (in seconds) - used as a workaround for aggressive firewall timeouts (0 = disabled)
- __agent.runInteractiveConsole__: allow interacive console (true or false, should be false for production)
- __agent.monitorOutputInterval__: interval at which agent monitor process runs and prints out current event stats (in seconds)
- __agent.maxRetries__: maximum number of times a failed event will be retried (retries are disabled if set to 0)
- __agent.retryInterval__: interval at which retry process runs for events that failed to process (in seconds)
- __agent.lastEventOutput__: filename where last event payload (JSON) should be written, if omitted, file will not be written (should be disabled in production)

**Syniverse API settings**
- __scg.consumerKey__: Syniverse consumer key
- __scg.consumerSecret__: Syniverse consumer secret
- __scg.accessToken__: Syniverse API access token
- __scg.apiUrl__: Syniverse API endpoint URL (https://api.syniverse.com)
- __scg.defaultSender__: default sender channel (or sender ID)
- __scg.defaultChannel__: default channel (SMS, MMS, RCS etc.)
- __scg.demo.senders.sms__: map of country codes to senders/channels, overrides default sender when matched on country code for SMS/MMS messages (example: 1,channel:1KJPMkuHQkair_o15etpmg|34,channel:zm8lO9Y9QKGKTeS-BoHCKA|44,channel:DJm-vHcnSBKbeK4b2FAOLQ|49,channel:qSOdzTqaSfO0bmwLEQGNdw)

**Custom agent settings**
- __agent.event.statusMethod__: obtain message status (delivered/failed/clicked) via webhook call from Syniverse or by polling Synivers API, options are WEBHOOK or POLL (if empty, status won't be obtained by agent) 
- __agent.http.port__: port number on which HTTP server should run (if property is not present, HTTP server will not be started)
- __agent.http.webhookContextRoot__: context root for webhook endpoint
- __agent.messageStatusInterval__: interval at which status of sent messages should be checked (in seconds)
- __agent.twoWay.enabled__: enable two-way messaging, if enabled, internal HTTP endpoint will be started, values: true/false
- __agent.twoWay.identityCacheEnabled__: cache relationship between recipient address (e.g. phone number) and identity (datahub_id), values: true/false
- __agent.cache.messageCacheName__: logical name of cache used for outgoing message metadata
- __agent.cache.identityCacheName__: logical name of cache used for caching relationship between recipient address (e.g. phone number) and identity (datahub_id)
- __agent.cache.cacheDirectory__: name of directory in which ehcache data should be stored (for disk persistence, relative to agent deployment directory)
- __agent.cache.messageCacheHeap__: maximum number of entries to be stored in heap
- __agent.cache.messageCacheOffHeapMB__: maximum off heap memory to be used by ehcache for message cache persistence (in MB)
- __agent.cache.messageCacheDiskMB__: maximum disk storage to be used by ehcache for message cache persistence (in MB)
- __agent.cache.messageCacheTTLMin__: message cache time to live, sent message state is discarded after this period (in minutes)
- __agent.event.responseEventNames__: map of Syniverse message dispositions to names of CI360 external events to be injected (example: DELIVERED:SMS Delivered,FAILED:SMS Failed,CLICKED:SMS Clicked)
- __agent.event.moEventName__: name of external event to be injected when new message (MO = mobile originated message) is received (example: Inbound SMS)
- __agent.event.moIdentityField__: name of identity field to be used to inject sender address (e.g. phone number), if left blank or ommited, only datahub_id will be populated if known (example: subject_id or mobile_id)
- __agent.event.moFromField__: name of external event attribute that will contain sender (e.g. phone number) of received message (example: from_address)
- __agent.event.moMessageBodyField__: name of external event attribute that will contain body of received message (example: message_body)
- __agent.event.moMessageBodyUpperCase__: upper-case entire content of received text message when injecting into CI360 (example: true, default is false)
- __agent.creative.format__: name of creative format used for messages (Plain Text object in 360), options are JSON or PLAIN
- __agent.creative.encoding__: encoding to use when executing POST requests to API (example: UTF-8, default is sytem default, usually plain text)
- __agent.event.TTL__: Time to live or expiration period for an event received by the agent - if event is older than this time, it will be discarded by the agent (in seconds)

### Running the Agent

- Run agent using run_agent.sh script (or .bat for Windows environments)
- All output from the agent will be in logs directory
- If running as container, simply start the container as described above

### Using the Agent with External System Task

Messages are sent to customers using External System Task. External System Task can be triggered individually (by configuring Trigger event on the Orchestration tab), or as part of an Activity Map. When you are configuring a new External System Task, you will need to associate it with Syniverse Agent on the Orchestration tab of the task. You will also need to use a Plain Text creative, which will contain all the personalization variables that will be sent to Syniverse for execution.

This is a sample structure of the Plain Text creative that needs to be used with External System Task:
```
TO:{{phone}};
FROM:channel:aaaweFWFAaefAEFaefaw;
CHANNEL:MMS;
MEDIAL_URLS:https://www.sas.com/images/logo.jpg;
{{firstname}}, thank you for registering for our seminar!
```

Last line of the creative contains the actual (and personalized) text message to be sent. TO field contains the email address of the recipient. FROM and CHANNEL fields are optional - FROM will replace a default sender channel from configuration if provided, and CHANNEL defaults to SMS/MMS if not specified. MEDIA_URLS is only required if media (such as an image) is attached to the message. Applicable to MMS and WHATSAPP.

