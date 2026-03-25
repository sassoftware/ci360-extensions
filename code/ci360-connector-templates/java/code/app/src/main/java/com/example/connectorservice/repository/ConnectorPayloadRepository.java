package com.example.connectorservice.repository;

import java.sql.Timestamp;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.example.connectorservice.model.ConnectorPayload;

@Repository
public class ConnectorPayloadRepository {

  private static final Logger logger = LoggerFactory.getLogger(ConnectorPayloadRepository.class);

  private final JdbcTemplate jdbc;
  private static final String INSERT_SQL = """
        INSERT INTO connector_payloads (received_dttm, event_dttm, guid, payload)
        VALUES (?, ?, ?, ?)
      """;

  public ConnectorPayloadRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  private Timestamp toTimestamp(Instant dttm) {
    if (dttm == null)
      return null;
    return Timestamp.from(dttm);
  }

  public void save(ConnectorPayload payload) {

    jdbc.update(
        INSERT_SQL,
        toTimestamp(payload.receivedDttm()),
        toTimestamp(payload.eventDttm()),
        payload.guid(),
        payload.body());

    logger.debug("Saved connector payload with guid={}", payload.guid());
  }

}
