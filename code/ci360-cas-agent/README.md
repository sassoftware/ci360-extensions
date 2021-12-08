# CI360 CAS Agent

## Overview
SAS Cloud Analyic Services (CAS) Agent streams CI360 events into a CAS table. Event attributes to be written to CAS table are based on field names, exact match between event attribute and field name is required.

## Prerequisites

- Java 1.8 or newer
- Maven 3.5.3 or newer
- CI360 Agent SDK installed in the local Maven repository (needs to match version in _pom.xml_)
- CAS Client library installed in the local Maven repository
- Access point in SAS Customer Intelligence 360
    1. From the user interface, navigate to **General Settings** > **External Access** > **Access Points**
    2. Create a new access point if one does not exist
    3. Get the following information from the access point:  
       ```
        External gateway address: e.g. https://extapigwservice-<server>/marketingGateway  
        Name: CAS Agent  
        Tenant ID: abc123-ci360-tenant-id-xyz  
        Client secret: ABC123ci360clientSecretXYZ  
       ```


## Using the Agent

You need to change the connection settings in the "agent.config" file to connect to your access point in CI360
- ci360.gatewayHost 
- ci360.tenantID 
- ci360.clientSecret

You also need to provide CAS connection settings and table name and columns (_cas.tableName_ and _cas.tableColumns_). See <a href="#configuration">Configuration</a> section for details.

#### Windows
You can run the agent on Windows by executing follwing cmd:
```
run_agent.cmd
```

#### Unix
You can run the agent on Linux/Unix by calling:
```
./run_agent.sh
```

The script _status.sh_ prints the status of your running scripts.

## Configuration

Configuration file agent.config has the following parameters:

#### CI360 specific settings
- __ci360.gatewayHost__: Hostname for CI360 API gateway (can be obtained from Settings page, Access)
- __ci360.tenantID__: tenant ID for access point
- __ci360.clientSecret__: client secret for access point
- __agent.keepaliveInterval__: interval at which keepalive process runs and send a ping to CI360 gateway (in milliseconds) - used as a workaround for aggressive firewall timeouts (0 = disabled)
- __agent.runInteractiveConsole__: allow interacive console (true or false, should be false for production)
- __agent.monitorOutputInterval__: interval at which agent monitor process runs and prints out current event stats (in milliseconds)
- __agent.batchInterval__: interval at which agent writes events to CAS table (in milliseconds)
- __agent.lastEventOutput__: filename where last event payload (JSON) should be written, if omitted, file will not be written (should be disabled in production)

#### CAS settings
- __cas.tableName__: name of CAS table to be used to write CI360 events 
- __cas.tableColumns__: list of table columns and data types for CAS table (in JSON array format, every object has name and type attribute, valid data types are VARCHAR, INT32, INT64)
- __cas.createTable__: specifies if the CAS table should be created if it does not already exist (true/false)
- __cas.host__: host name of CAS server
- __cas.port__: port to be used for CAS server connection
- __cas.username__: CAS/Viya username
- __cas.password__: CAS/Viya password
- __cas.caslib__: name of CASLIB to be used, if ommited, CASLIB will not be specified and default will be used (usually CASUSER)
- __cas.commitRowCount__: when writing a batch of events to CAS table, commit will be issued every X rows (only matters if batch is larger than X rows, all writes are commited at the end of batch)
- __cas.maxBatchSize__: maximum number of rows to process in one batch (if ommited or 0, there is no maximum size for a batch)

Example of __cas.tableColumns__ configuration:
```
cas.tableColumns=[{"name":"timestamp", "type":"INT64"},{"name":"event", "type":"VARCHAR"},{"name":"channelType", "type":"VARCHAR"},\
{"name":"eventName", "type":"VARCHAR"},{"name":"identityId", "type":"VARCHAR"},{"name":"spot_id", "type":"VARCHAR"},{"name":"email_id", "type":"VARCHAR"},\
{"name":"subject_id", "type":"VARCHAR"},\
{"name":"domain", "type":"VARCHAR"},{"name":"uri", "type":"VARCHAR"},{"name":"referrer", "type":"VARCHAR"},{"name":"session", "type":"VARCHAR"},\
{"name":"mobile_appid", "type":"VARCHAR"},{"name":"visitor_state", "type":"VARCHAR"},{"name":"page_host", "type":"VARCHAR"},\
{"name":"page_path", "type":"VARCHAR"},{"name":"page_title", "type":"VARCHAR"},{"name":"browser_platform", "type":"VARCHAR"},\
{"name":"geo_country", "type":"VARCHAR"},{"name":"geo_latitude", "type":"DOUBLE"},{"name":"geo_longitude", "type":"DOUBLE"},{"name":"geo_ip", "type":"VARCHAR"},\
{"name":"PageTitle", "type":"VARCHAR"},{"name":"PageCategory", "type":"VARCHAR"},\
{"name":"customEventName", "type":"VARCHAR"},{"name":"customEventGroupName", "type":"VARCHAR"},{"name":"searchTerm", "type":"VARCHAR"}]
```

CAS Agent expects that the table with the configured column already exists, and the list of column matches the configuration, or the agent will create the table on startup (this behavior can be configured through _cas.createTable_).

### CI360 Configuration

In order to stream events from CI360 to the agent, access point needs to be created in CI360 (see <a href="#prerequisites">Prerequisites</a>).
 
Once access point is created, you can associate events you'd like to stream to the agent. This can be either done on Access Point configuration page under Associations, or for individual events on their respective Orchestration pages (under External Availability). 

## Build Process

Agent SDK can be downloaded from CI360 environment. In the ZIP archive, SDK can be found in sdk folder. To add Agent SDK to your local Maven repository, you can execute:
```
mvn install:install-file -Dfile=mkt-agent-sdk-jar-1.<current release>.jar -Djavadoc=mkt-agent-sdk-jar-1.<current release>-docs.jar -DpomFile=pom.xml
```

Download _cas-client-3.15.13.jar_ and _commons-crypto-1.0.14.jar_ from:
https://tds.sas.com/downloads/package.htm?pid=1976

Install these jars into your local Maven repository using the example command below (change path to jar)
```
mvn install:install-file -Dfile=cas-client-3.15.13.jar -DgroupId=com.sas.cas -DartifactId=cas-client -Dversion=3.15.13 -Dpackaging=jar -DgeneratePom=true
mvn install:install-file -Dfile=commons-crypto-1.0.14.jar -DgroupId=com.sas.commons -DartifactId=commons-crypto -Dversion=1.0.14 -Dpackaging=jar -DgeneratePom=true
```

You can build the agent by executing:
 
```
mvn package
```

Distribution archive (ZIP) will be created in target folder. Pre-built distribution is available under Releases.


## Running the Agent in a Container

Dockerfile is included with agent distribution. It can be used to build an image and run a container. The build and deployment can be customized to suite the environment needs, but the simple approach is documented below.

### Build a Docker image

From top directory of agent distribution (where distribution archive has been unzipped), run the following command:

```
docker build -t ci360-cas-agent .
```

### Run a container

Since we can run multiple containers based on image we built, it is recommended that agent configuration file be maintained separately, on the host machine. All agent files, including _agent.config_ file, are located in _/opt/ci360-cas-agent_ within the image. Once the agent image has been created, we can run the container using:

```
docker run -d -p 8081:8080 --mount type=bind,source=/opt/install/cas/cas_agent_prd1.config,target=/opt/ci360-cas-agent/agent.config --name ci360-cas-prd1 ci360-cas-agent:latest
```

### Connecting to Viya 4 environment

In order to connect to CAS on Viya 4 environment, trust store needs to be provided and packaged with the agent. Trust store is already referenced in the included _Dockerfile_:

```
-Djavax.net.ssl.trustStore=viya4_trustedcerts.jks
```

### Deploying the Agent on Kubernetes

Follow these steps to deploy CAS Agent into Viya 4 Kubernetes environment (or any other Kubernetes environmnet, but primary goal is to deploy alongside Viya4).

#### Build and Register the Image

Take the distribution ZIP file created in the target directory and unzip this to a new location.
Copy the trustedcerts.jks file from your Viya deployment to the root directory of the agent as this will be referenced in the _Dockerfile_.
Edit the _Dockerfile_ and change the _ENTRYPOINT_.

Old: 
```
ENTRYPOINT ["java", "-Dlogback.configurationFile=logback.xml", "-DconfigFile=agent.config", "-Djavax.net.ssl.trustStore=viya4_trustedcerts.jks", "-Xms32m", "-Xmx2048m", "-jar", "ci360-cas-agent-21.09.1.jar"]
```
New:
```
ENTRYPOINT ["java", "-Dlogback.configurationFile=/ci360-cas-agent-config/logback.xml", "-DconfigFile=/ci360-cas-agent-config/agent.config", "-Djavax.net.ssl.trustStore=viya4_trustedcerts.jks", "-Xms32m", "-Xmx2048m", "-jar", "ci360-cas-agent-21.09.1.jar"]
```

Build the Docker image and push to your chosen container registry. In the example below we are pushing it to a private AWS container registry (_XXXXXXXXXXX.dkr.ecr.us-east-1.amazonaws.com_).

Build the image:  
```
docker build -t ci360-cas-agent .
```
Tag the image:  
```
docker tag ci360-cas-agent:latest XXXXXXXXXXX.dkr.ecr.us-east-1.amazonaws.com/ci360-cas-agent:latest
```
Push the image:  
```
docker push XXXXXXXXXXX.dkr.ecr.us-east-1.amazonaws.com/ci360-cas-agent:latest
```

#### Create K8s Artifacts

Take a copy of __ci360-cas-agent.yaml__ manifest from the _k8_ directory of the agent distribution as this will be used to create the K8s artifacts.  The yaml file contain definitions for creating a PersistentVolumeClaim, ConfigMap, Deployment and a HorizontalPodAutoscaler.
You will need to edit this yaml file to provide details of your _ci360.gatewayHost, ci360.tenantID, ci360.clientSecret, cas.username_ and _cas.password_ which can be found in the ConfigMap section:

```
   ci360.gatewayHost=extapigwservice-demo.cidemo.sas.com
   ci360.tenantID=0bf01xxxxxxxa3a2cacaa
   ci360.clientSecret=MTAxOTIxxxxxxxxxxxYjc0ZW5nN2ZpaWpp
   cas.username=sasdemo
   cas.password=xxxxxxxx
```

You will also need to update the Deployment section to provide details of your containers – image and imagePullSecrets – name (if deployed to a private container registry)

```
     imagePullSecrets:
     - name: aws-pull-v2
     containers:
     - image: XXXXXXXXXXX.dkr.ecr.us-east-1.amazonaws.com/ci360-cas-agent:latest
```

You should now be ready to apply this manifest your K8s cluster:
```
kubectl apply -f ./ci360-cas-agent.yaml –namespace <YOUR NAMESPACE>
```

