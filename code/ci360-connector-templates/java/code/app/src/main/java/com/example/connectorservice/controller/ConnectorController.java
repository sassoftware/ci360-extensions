package com.example.connectorservice.controller;

import com.example.connectorservice.service.ConnectorService;

import java.time.Instant;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/connector")
public class ConnectorController {

  private final ConnectorService connectorService;

  public ConnectorController(ConnectorService connectorService) {
    this.connectorService = connectorService;
  }

  @PostMapping(consumes = { MediaType.APPLICATION_JSON_VALUE, MediaType.TEXT_PLAIN_VALUE })
  public ResponseEntity<String> receive(@RequestBody String payload) {
    Instant now = Instant.now();
    boolean isJson = connectorService.process(payload, now);
    if (isJson)
      return ResponseEntity.accepted().body("ok");
    else
      return ResponseEntity.badRequest().body("Malformed JSON input.");
  }
}
