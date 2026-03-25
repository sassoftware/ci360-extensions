# Spring Boot Sample --- CI 360 Connector Endpoint

This repository contains a **minimal Spring Boot sample application**
that implements a connector endpoint for receiving outbound events from
**SAS Customer Intelligence 360 (CI 360)**.

The connector endpoint writes the connector payload to a PostgreSQL
database.

The primary goal of this project is to demonstrate:

-   How to structure a small Spring Boot service
-   How to expose a simple REST endpoint
-   How to parse raw JSON payloads
-   How to extract selected fields
-   How to persist inbound events using Spring JDBC
-   How to handle time consistently

PostgreSQL, Docker, and the agent container exist **only to make the
sample runnable end-to-end**.\
They are not the focus of the example.

------------------------------------------------------------------------

## Running the Connector sample

The project includes a docker compose application with 3 containers (see docker-compose.yml):
-   General agent
-   Connector app
-   PostgreSQL database

This setup provides a convient way for running the connector app incl dependencies (agent and postgres).

Alternatively you can build the maven project (see app/src), and deploy using your faviorite deployment method for Java apps.

### How-to deploy using Docker Compose (e.g Docker Desktop with wsl2)

Create `360.env`:

    CI360_GATEWAY_HOST=***
    CI360_TENANT_ID=***
    CI360_CLIENT_SECRET=***

Then run:

``` bash
docker compose up
```

This starts three containers:

-   General agent
-   Connector app
-   PostgreSQL database

In CI 360 configure the connector endpoint as:

    http://spring.local:8080/connector


------------------------------------------------------------------------

## What This Sample Is (and Is Not)

### ✔ This sample is

-   A reference implementation of a **connector-style HTTP endpoint**
-   Focused on **Spring Boot code structure and clarity**
-   Explicit rather than abstract
-   Easy to read, debug, and extend
-   Suitable for learning and discussion

### ✘ This sample is not

-   A production-ready connector

------------------------------------------------------------------------

## Project Structure

    src/main/java/com/example/connectorservice
    ├── ConnectorApplication.java
    ├── config
    │   └── TimeZoneConfig.java
    ├── controller
    │   └── ConnectorController.java
    ├── model
    │   └── ConnectorPayload.java
    ├── repository
    │   └── ConnectorPayloadRepository.java
    └── service
        └── ConnectorService.java

Each layer has a single, clear responsibility.

------------------------------------------------------------------------

## Application Entry Point

### `ConnectorApplication`

``` java
@SpringBootApplication
public class ConnectorApplication {
  public static void main(String[] args) {
    SpringApplication.run(ConnectorApplication.class, args);
  }
}
```

Standard Spring Boot bootstrap using auto-configuration and component
scanning.

------------------------------------------------------------------------

## Time Handling

### `TimeZoneConfig`

``` java
@PostConstruct
public void setTimeZone() {
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
}
```

Why this exists:

-   Prevents accidental dependence on host or container timezone.
-   Makes timestamp behavior deterministic across environments.
-   Avoids DST and locale-related bugs.

------------------------------------------------------------------------

## REST Endpoint

### `ConnectorController`

``` java
@PostMapping(consumes = {
  MediaType.APPLICATION_JSON_VALUE,
  MediaType.TEXT_PLAIN_VALUE
})
public ResponseEntity<String> receive(@RequestBody String payload) {
  Instant now = Instant.now();
  boolean isJson = connectorService.process(payload, now);

  if (isJson)
    return ResponseEntity.accepted().body("ok");
  else
    return ResponseEntity.badRequest().body("Malformed JSON input.");
}
```

Design choices:

-   Accepts the raw request body as `String`.
-   Keeps the endpoint independent of payload schema.
-   Delegates all logic to the service layer.

------------------------------------------------------------------------

## Service Layer

### `ConnectorService`

``` java
public boolean process(String payloadString, Instant receivedDttm) {
  ConnectorPayload payload =
      new ConnectorPayload(payloadString, receivedDttm);

  repository.save(payload);
  return payload.isJson();
}
```

------------------------------------------------------------------------

## Domain Model

### `ConnectorPayload`

``` java
public ConnectorPayload(String body, Instant receivedDttm) {
  this.body = body;
  this.receivedDttm = receivedDttm;
  try {
    this.json = mapper.readTree(body);
    this.guid = json.path("guid").textValue();
    JsonNode tsNode = json.path("date").path("generatedTimestamp");
    if (tsNode.isNumber())
      this.eventDttm = Instant.ofEpochMilli(tsNode.longValue());
  } catch (Exception e) {
    logger.error("Malformed JSON input", e);
  }
}
```

------------------------------------------------------------------------

## Persistence Layer

### `ConnectorPayloadRepository`

``` java
private static final String INSERT_SQL = """
  INSERT INTO connector_payloads
    (received_dttm, event_dttm, guid, payload)
  VALUES (?, ?, ?, ?)
""";
```

Uses `JdbcTemplate` directly with explicit SQL.

------------------------------------------------------------------------

## Payload Example (CI 360)

``` json
{
  "guid": "18a06b23-390f-4435-9a0b-bc0dd63da938",
  "eventName": "SandboxOutbound",
  "date": {
    "generatedTimestamp": 1751625227523
  },
  "properties": {
    "externalCode": "TSK_202",
    "name": "hello"
  }
}
```

Only a minimal subset is extracted.\
The full payload is persisted unchanged.

------------------------------------------------------------------------

## How to Build Your Own Connector Logic

Partners will typically extend this sample by adding **custom business
logic** inside the service layer --- usually in:

    ConnectorService.process(...)

This keeps the controller stable while allowing integration-specific
behavior to evolve independently.

Typical extensions include:

-   Sending messages to a third-party API (SMS, email, SaaS platforms)
-   Publishing events to a message queue or event bus (Kafka, SQS,
    Pub/Sub, etc.)
-   Enriching the payload with external lookups
-   Filtering or routing events based on payload content
-   Transforming payloads into downstream formats

### Recommended Pattern

1.  Keep the controller unchanged.
2.  Parse once into `ConnectorPayload`.
3.  Add custom logic in the service layer.
4.  Isolate external integrations behind small helper services or
    clients.

Example conceptual flow:

``` text
Controller
   → ConnectorService.process(...)
       → Parse payload
       → Custom logic (send, publish, transform, enrich)
       → Persist or acknowledge
```

### Example Extension (Conceptual)

``` java
public boolean process(String payloadString, Instant receivedDttm) {
  ConnectorPayload payload =
      new ConnectorPayload(payloadString, receivedDttm);

  if (!payload.isJson()) {
    return false;
  }

  // Example: route based on payload content
  if ("SMS".equals(payload.json().path("channelType").asText())) {
    smsClient.sendMessage(payload);
  }

  // Example: publish to a queue
  eventPublisher.publish(payload);

  repository.save(payload);
  return true;
}
```

This sample keeps the default behavior intentionally simple so partners
can layer their own logic without fighting framework complexity.

------------------------------------------------------------------------

## Running the Sample (Optional)

``` bash
docker compose up --build
```

------------------------------------------------------------------------

## Design Principles Illustrated

-   Thin controllers
-   Explicit data flow
-   Minimal dependencies
-   Deterministic time handling
-   Simple JDBC over ORM
