CREATE TABLE IF NOT EXISTS connector_payloads (
  received_dttm TIMESTAMPTZ(3),
  event_dttm TIMESTAMPTZ(3),
  guid VARCHAR(36),
  payload TEXT
);
