# Stream Events to DB Table

## Overview
The **Contact History (CH) Streaming Agent** enables users to stream CI360 events into an on-premise database in real time enabling users to immediately sync both the online and offline customer contact history. CH events are received by the agent, then messages are parsed and then written into the target table in the on-premise database. 


### Prerequisites

- Java 17 or newer _(java-17-openjdk)_
- Apache Maven 3.9.6 or newer _(apache-maven-3.9.6-bin.tar.gz)_
- mkt-agent-sdk 
- Connectivity between the on-premise database, agent server and CI360
- Access Point & gateway Host for SAS Customer Intelligence 360
    1. From the user interface, navigate to _General Settings > External Access > Access Points_
    2. Create a new access point if one does not exist
	3. Get the following information from the access point:
       **External gateway host:** extapigwservice-<server>/marketingGateway  
       **Access Point Name**: CH Agent  
       **Tenant ID**: abc123-ci360-tenant-id-xyz  (sample)
       **Client secret**: ABC123ci360clientSecretXYZ (sample)   

- Database connection details
	- **url**=jdbc:postgresql://localhost:5432/postgres
	- **username**=postgres
	- **password**=pw

- Pre-defined ContactHistory on-premise table with the following attributes:
	- _datahub_id varchar(36)_
	- _subject_id varchar(18)_
	- _contact_id varchar(36)_
	- _contact_dttm_utc timestamp(6) without time zone_
	- _task_id varchar(36)_
	- _channel_type varchar(36)_
	- _insert_ts timestamp WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP_


    Should there be changes in the attributes of the tables, updates should be made in the project code to accommodate the change in attributes.

## Installation

### Getting Started 
1. Define the access point in the CI360 UI. Take note of the credentials in the access point since these will be used in the agent configuration.
2. Ensure **maven** and **mkt-agent-sdk** are installed in the on-premise server. You can install Maven using the following:
```bash
curl -O https://dlcdn.apache.org/maven/maven-3/3.9.6/binaries/apache-maven-3.9.6-bin.tar.gz
tar xzvf apache-maven-3.9.6-bin.tar.gz
```
For reference on downloading and installing SAS CI360 Agent SDK, please refer to __Deploying CI360 Agents__ section of repository README.

3. Download the project files to the on-premise server. Update the /src/main/resources/application-properties file with the credentials obtained in step 1. Depending on the project approach, other properties files may also be used which are available under /src/main/resources/. Recompile and repackage the agent files as needed.

4. Run the agent in the on-premise server using the following command:

    Gradle:
`gradlew bootRun`

    _CH Agent may also be packaged and run directly with java._

5. Check and verify the agent logs if agent is running.
6. Once agent is running, events in CI360 may be associated to the defined access point to stream these events to the agent. Configuration for this can either be done through the Access Point configuration page under Associations (i.e. _Access Point Properties > Associations > Associated Events > add Contact Standard Event as a selected Event_), or through individual events on their respective Orchestration pages (under _External Availability_). API calls may be posted to trigger the associated events and test the agent.

7. Updates may be applied to reflect project specific requirements in the asset. This can include but are not limited to the following:
	- Additional attributes for the ContactHistory table. The following files must be updated to reflect the updated attributes of the **ContactHistory** table: 
		- /src/main/resources/schema.sql
		- /src/main/java/com/sas/incubation/ci/agent/entities/ContactHistory.java
		- /src/main/java/com/sas/incubation/ci/agent/repository/BatchContactHistoryRepository.java
		- /src/main/java/com/sas/incubation/ci/agent/impl/EventProcessor.java
	
	- Additional filtering logic for event processing. Logic may be added to the following file: /src/main/java/com/sas/incubation/ci/agent/impl/EventProcessor.java by uncommenting and updating the IF block in line 60.
	
	Once done, the package needs to be recompiled and downloaded to the on-premise server with the proper configuration details.

## Additional Resources
[SAS Help Centre](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/ch-integrating-about.htm)
