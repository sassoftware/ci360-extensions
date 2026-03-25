package com.example.connectorservice.model;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

public class ConnectorPayload {
  private String guid; // event guid
  private String body;
  private JsonNode json;
  private Instant receivedDttm;  // from system clock
  private Instant eventDttm;     // generated timestamp from payload

  private static final ObjectMapper mapper = JsonMapper.builder().build();
  private static final Logger logger = LoggerFactory.getLogger(ConnectorPayload.class);

  public ConnectorPayload(String body, Instant receivedDttm) {
    this.body = body;
    this.receivedDttm = receivedDttm;
    try {
      // Parse JSON
      this.json = mapper.readTree(body);

      // Fetch event guid + generated timestamp
      this.guid = json.path("guid").textValue();
      JsonNode tsNode = json.path("date").path("generatedTimestamp");
      if (tsNode.isNumber())
        this.eventDttm = Instant.ofEpochMilli(tsNode.longValue());
    } catch (Exception e) {
      logger.error("Malformed JSON input: {}", body, e);
    }
  }

  public JsonNode json() {
    return json;
  }

  public String body() {
    return body;
  }

  public String guid() {
    return guid;
  }

  public Instant receivedDttm() {
    return receivedDttm;
  }

  public Instant eventDttm() {
    return eventDttm;
  }

  public boolean isJson() {
    return (json != null);
  }
}
