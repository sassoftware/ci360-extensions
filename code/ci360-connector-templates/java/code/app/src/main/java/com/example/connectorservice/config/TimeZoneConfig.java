package com.example.connectorservice.config;

import jakarta.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import java.util.TimeZone;

@Configuration
public class TimeZoneConfig {
  private static final Logger logger = LoggerFactory.getLogger(TimeZoneConfig.class);

  @PostConstruct
  public void setTimeZone() {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    logger.info("JVM default time zone set to {}", TimeZone.getDefault());
  }
}
