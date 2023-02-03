/* Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.sas.ci360.agent;

import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

import com.sas.ci360.agent.exceptions.ConfigurationException;
import com.sas.ci360.agent.exceptions.EventHandlerException;
import com.sas.ci360.agent.impl.SnowflakeHandler;
import com.sas.ci360.agent.util.AgentUtils;
import com.sas.mkt.agent.sdk.CI360Agent;
import com.sas.mkt.agent.sdk.CI360AgentException;
import com.sas.mkt.agent.sdk.CI360StreamInterface;
import com.sas.mkt.agent.sdk.ErrorCode;
import com.sas.ci360.agent.util.Constants;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;



public class CustomAgent {
	private static final Logger logger = LoggerFactory.getLogger(CustomAgent.class);
	private static final Logger statsLogger = LoggerFactory.getLogger("CustomAgent.stats");
	private static final Logger eventLogger = LoggerFactory.getLogger("CustomAgent.events");
	private static Map<String, Integer> statsHashMap = new ConcurrentHashMap<String, Integer>();
	

	static boolean exiting = false;
	
	private static EventHandler eventHandler = new SnowflakeHandler();

	public static void main(String[] args) {
		Properties props = System.getProperties();
		
		final Properties config = AgentUtils.readConfig(props.getProperty("configFile"));
		

		

		props.setProperty("ci360.gatewayHost", config.getProperty("ci360.gatewayHost"));
		props.setProperty("ci360.tenantID", config.getProperty("ci360.tenantID"));
		props.setProperty("ci360.clientSecret", config.getProperty("ci360.clientSecret"));
		logger.info("Connecting to Gateway: " + config.getProperty("ci360.gatewayHost"));
		
		final String lastEventOutputFile = config.getProperty("agent.lastEventOutput");
		final boolean outputLastEvent = lastEventOutputFile != null;
		
		

		// initialize event handler
		try {
			logger.debug("Initialize event handler");
			eventHandler.initialize(config);
		} catch (ConfigurationException ex) {
			logger.error(ex.getMessage());
			System.exit(1);
		}
		
		try {
			final CI360Agent agent = new CI360Agent();
			CI360StreamInterface streamListener = new CI360StreamInterface() {
				public boolean processEvent(final String event) {
					Thread eventThread = new Thread() {
						public void run() {
							logger.trace("Event received");
							if (event.startsWith("CFG")) {
								logger.debug("Config event received");
							} else {
								JSONObject jsonEvent = new JSONObject(event);
								JSONObject eventAttr = jsonEvent.getJSONObject(Constants.JSON_ATTRIBUTES);
								String eventName = eventAttr.getString(Constants.JSON_EVENTNAME);
								logger.info("(rowKey:"+jsonEvent.getString(Constants.JSON_ROWKEY)+"), "+"Event received: eventName={}", eventName);
								
								AgentUtils.logEvent(eventAttr, eventLogger);
								// pretty print last json event for debug purpose into a file
								if (outputLastEvent) {
									AgentUtils.writeToFile(lastEventOutputFile, eventAttr.toString(4), false);
								}

								// update stats
								statsHashMap.compute(Agent.STATS_EVENT_COUNT, (k, v) -> v == null ? 1 : v + 1);
								statsHashMap.compute(eventName, (k, v) -> v == null ? 1 : v + 1);

								try {
									eventHandler.processEvent(jsonEvent);
								} catch (EventHandlerException e) {
									logger.error("(rowKey:"+jsonEvent.getString(Constants.JSON_ROWKEY)+"), "+"Failed to process event, error: {}", e.getMessage());
									if (e.isRetryable()) {
										// TODO: add error/retry handler
										//errorHandler.enqueueEvent(jsonEvent);
									}
								} catch (Exception e) {
									logger.error("(rowKey:"+jsonEvent.getString(Constants.JSON_ROWKEY)+"), "+"Failed to process event, unhandled error: {} ({})", e.getMessage(), e.getClass().getName());
								}

							}
						}
					};
					eventThread.start();
					return true;
				}

				public void streamClosed(ErrorCode errorCode, String message) {
					if (exiting) {
						logger.info("Stream closed");
					} else {
						logger.info("Stream closed " + errorCode + ": " + message);
						try {
							Thread.sleep(15000);
						} catch (InterruptedException e) {

						}
						try {
							// Try to reconnect to the event stream.
							agent.startStream(this, true);
						} catch (CI360AgentException e) {
							logger.error("ERROR " + e.getErrorCode() + ": " + e.getMessage());
						}
					}
				}
			};
			agent.startStream(streamListener, true);
			logger.info("Agent started");

			final int monitorPrintInterval = Integer.parseInt(config.getProperty("agent.monitorOutputInterval"));
			Thread monitorThread = new Thread("MonitorThread") {
				public void run() {
					logger.debug("Starting monitor thread");
					while (true) {
						logger.trace("Monitor thread sleeping " + monitorPrintInterval + " ms");
						try {
							Thread.sleep(monitorPrintInterval);
						} catch (InterruptedException e) {
							logger.error("Thread interrupted");
						}
						
						logger.info("Event count: " + statsHashMap.getOrDefault(Constants.STATS_EVENT_COUNT, 0));
						statsLogger.debug("Event stats (full): " + statsHashMap.toString());
					}
				}
			};
			monitorThread.start();
			logger.info("Monitor thread started");
			
			final int keepaliveInterval = Integer.parseInt(config.getProperty("agent.keepaliveInterval"));
			Thread keepaliveThread = new Thread("KeepaliveThread") {
				public void run() {
					logger.debug("Starting keepalive thread");
					while (true) {
						logger.trace("Keepalive thread sleeping " + keepaliveInterval + " ms");
						try {
							Thread.sleep(keepaliveInterval);
						} catch (InterruptedException e) {
							logger.error("Thread interrupted");
						}
						
						try {
							logger.debug("Pinging gateway");
							logger.debug("Ping response: " + agent.healthcheck());
						} catch (CI360AgentException e) {
							logger.error(e.getMessage());
							e.printStackTrace();
						}
					}
				}
			};
			keepaliveThread.start();
			logger.info("Keepalive thread started");

			if (config.getProperty("agent.runInteractiveConsole").equalsIgnoreCase("true")) {
				logger.info("Interactive commands are accepted");
				runInteractiveConsole(agent);
			} else {
				logger.warn("No interactive commands are accepted");
			}

		} catch (CI360AgentException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
			System.exit(-1);
		}

	}

	private static void runInteractiveConsole(CI360Agent agent) {
		// Continue until user enters "exit" to standard input.
		Scanner in = new Scanner(System.in);
		while (true) {
			String input = in.nextLine();
			if (input.equalsIgnoreCase("exit")) {
				exiting = true;
				agent.stopStream();
				in.close();
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {

				}
				System.exit(0);
				;
			} else if (input.startsWith("send ")) {
				try {
					String message = agent.injectEvent(input.substring(5));
					System.out.println("SUCCESS: " + message);
				} catch (CI360AgentException e) {
					System.err.println("ERROR: " + e.getMessage());
				}
			} else if (input.startsWith("ping")) {
				try {
					String message = agent.ping();
					System.out.println("SUCCESS: " + message);
				} catch (CI360AgentException e) {
					System.err.println("ERROR: " + e.getMessage());
				}
			} else if (input.startsWith("config")) {
				try {
					String message = agent.getAgentConfig();
					System.out.println("SUCCESS: " + message);
				} catch (CI360AgentException e) {
					System.err.println("ERROR: " + e.getMessage());
				}
			} else if (input.startsWith("healthcheck")) {
				try {
					String message = agent.healthcheck();
					System.out.println("SUCCESS: " + message);
				} catch (CI360AgentException e) {
					System.err.println("ERROR: " + e.getMessage());
				}
			} else if (input.startsWith("connection")) {
				boolean status = agent.isConnected();
				System.out.println("Connection Status: " + (status ? "UP" : "DOWN"));
			} else if (input.startsWith("diag")) {
				try {
					String message = agent.diagnostics();
					System.out.println("SUCCESS: " + message);
				} catch (CI360AgentException e) {
					System.err.println("ERROR: " + e.getMessage());
				}
			} else if (input.startsWith("bulk ")) {
				try {
					String message = agent.requestBulkEventURL(input.substring(5));
					System.out.println("SUCCESS  URL: " + message);
				} catch (CI360AgentException e) {
					System.err.println("ERROR: " + e.getMessage());
				}
			} else if (input.startsWith("sendmessage ")) {
				try {
					agent.sendWebSocketMessage(input.substring(12).trim());
					System.out.println("SUCCESS: " + input.substring(12).trim());
				} catch (CI360AgentException e) {
					System.err.println("ERROR: " + e.getMessage());
				}
			}
		}
	}


}
