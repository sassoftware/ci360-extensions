/* Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.sas.ci360.agent.errors;

import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sas.ci360.agent.EventHandler;

public class ErrorHandler {
	private static final Logger logger = LoggerFactory.getLogger(ErrorHandler.class);
	
	private int maxRetries = 0;
	private EventHandler eventHandler;
	private Queue<FailedEvent> retryQueue = new ConcurrentLinkedQueue<FailedEvent>();
	
	public ErrorHandler(EventHandler eventHandler) {
		this.eventHandler = eventHandler;
		
	}
	
	public void initialize(Properties config) {
		if (config.getProperty("agent.maxRetries") != null) {
			try {
				this.maxRetries = Integer.parseInt(config.getProperty("agent.maxRetries"));
			} catch (NumberFormatException ex) {
				logger.warn("Cannot parse agent.maxRetries configuration property, invalid number: " + config.getProperty("agent.maxRetries"));
			}
		}
	}
	
	public void enqueueEvent(JSONObject jsonEvent) {
		if (this.maxRetries > 0) {
			logger.debug("Adding failed event to queue");
			
			try {
				FailedEvent failedEvent = new FailedEvent(jsonEvent);
				retryQueue.add(failedEvent);
			} catch (Exception ex) {
				logger.error("Failed to add event to retry queue, error: " + ex.getMessage());
			}
		}
	}
	
	public void processQueue() {
		logger.info("Processing retry queue");
		
		final int queueSize = retryQueue.size();
		logger.info("Retry queue size: " + queueSize);
		
		for (int i=0; i < queueSize; i++) {
			FailedEvent failedEvent = retryQueue.remove();
			
			try {
				eventHandler.processEvent(failedEvent.getEvent());
			} catch (Exception e) {
				logger.error("Failed to re-process event, error: " + e.getMessage());
				
				failedEvent.incrementRetryCount();
				if (failedEvent.getRetryCount() < this.maxRetries) {
					try {
						retryQueue.add(failedEvent);
					} catch (Exception ex) {
						logger.error("Failed to re-post event to retry queue, error: " + ex.getMessage());
					}
				}
				else {
					logger.debug("Max retries exceeded for event");
				}
			}
		}
	}
}
