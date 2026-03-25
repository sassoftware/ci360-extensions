package com.example.connectorservice.service;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.example.connectorservice.model.ConnectorPayload;
import com.example.connectorservice.repository.ConnectorPayloadRepository;

@Service
public class ConnectorService {
  private static final Logger logger = LoggerFactory.getLogger(ConnectorService.class);
  private final ConnectorPayloadRepository repository;

  public ConnectorService(ConnectorPayloadRepository repository) {
    this.repository = repository;
  }

  public boolean process(String payloadString, Instant receivedDttm) {
    logger.debug("Received payloadString: {}", payloadString);
    ConnectorPayload payload = new ConnectorPayload(payloadString, receivedDttm);
    logger.debug("guid: {}, received: {}, generated: {}", payload.guid(), payload.receivedDttm(), payload.eventDttm());
    repository.save(payload);
    return payload.isJson();
  }

}
