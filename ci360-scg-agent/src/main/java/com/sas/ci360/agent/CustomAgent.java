/* Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.sas.ci360.agent;

import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

import com.sas.ci360.agent.cache.IdentityCache;
import com.sas.ci360.agent.cache.MessageCache;
import com.sas.ci360.agent.errors.ErrorHandler;
import com.sas.ci360.agent.exceptions.ConfigurationException;
import com.sas.ci360.agent.exceptions.EventHandlerException;
import com.sas.ci360.agent.impl.MessageEventHandler;
import com.sas.ci360.agent.impl.PostEventCacheCallback;
import com.sas.ci360.agent.status.MessageStatusService;
import com.sas.ci360.agent.util.AgentUtils;
import com.sas.ci360.http.ScgWebhookHandler;
import com.sas.ci360.http.RootHandler;
import com.sas.mkt.agent.sdk.CI360Agent;
import com.sas.mkt.agent.sdk.CI360AgentException;
import com.sas.mkt.agent.sdk.CI360StreamInterface;
import com.sas.mkt.agent.sdk.ErrorCode;
import com.sun.net.httpserver.HttpServer;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Properties;

/**
 * This class contains sample code used to demonstrate the usage of the CI360
 * Agent SDK {@link CI360Agent} to interact with CI360. The sample will connect
 * to the CI360 event stream and will print out all events that arrive from
 * CI360. It also accepts a few command from standard input. <br>
 * <br>
 * exit - exits the sample agent <br>
 * <br>
 * send - sends an external event to CI360. following the send command is the
 * event to be injected. The event is in JSON. See
 * {@link CI360Agent#injectEvent(String)}. <br>
 * <br>
 * bulk - requests a Signed S3 URL be returned for uploaded events into CI360.
 * Following the "bulk" command is the application ID to use. See
 * {@link CI360Agent#requestBulkEventURL(String)}.
 * 
 * @author magibs
 *
 */
public class CustomAgent {
	private static final Logger logger = LoggerFactory.getLogger(CustomAgent.class);
	private static final Logger statsLogger = LoggerFactory.getLogger("CustomAgent.stats");
	private static final Logger eventLogger = LoggerFactory.getLogger("CustomAgent.events");

	private static Map<String, Integer> statsHashMap = new ConcurrentHashMap<String, Integer>();
	private static final String STATS_EVENT_COUNT = "TotalEventCount";
	private static final String CFG_EVENT_PREFIX = "CFG";
	private static final String JSON_EVENTNAME = "eventName";
	private static final String JSON_ATTRIBUTES = "attributes";

	static boolean exiting = false;
	
	private static EventHandler eventHandler = new MessageEventHandler();
	private static ErrorHandler errorHandler = new ErrorHandler(eventHandler);
	private static HttpServer httpServer;
	private static MessageCache msgCache;
	private static IdentityCache identityCache;
	
	private static Thread monitorThread;
	private static Thread keepaliveThread;
	private static Thread retryThread;
	private static Thread statusThread;

	public static void main(String[] args) {
		Properties props = System.getProperties();
		final Properties config = AgentUtils.readConfig(props.getProperty("configFile"));

		if (config.getProperty("ci360.gatewayHost") == null) {
			logger.error("Missing required configuration property ci360.gatewayHost");
			System.exit(1);
		}
		if (config.getProperty("ci360.tenantID") == null) {
			logger.error("Missing required configuration property ci360.tenantID");
			System.exit(1);
		}
		if (config.getProperty("ci360.clientSecret") == null) {
			logger.error("Missing required configuration property ci360.clientSecret");
			System.exit(1);
		}
		
		props.setProperty("ci360.gatewayHost", config.getProperty("ci360.gatewayHost"));
		props.setProperty("ci360.tenantID", config.getProperty("ci360.tenantID"));
		props.setProperty("ci360.clientSecret", config.getProperty("ci360.clientSecret"));
		logger.info("Connecting to Gateway: {}, tenantID: {}", config.getProperty("ci360.gatewayHost"), config.getProperty("ci360.tenantID"));
		
		final String lastEventOutputFile = config.getProperty("agent.lastEventOutput");
		final boolean outputLastEvent = lastEventOutputFile != null && !lastEventOutputFile.trim().equals("");

		
		// initialize all objects
		try {
			// initialize cache
			logger.debug("Initialize cache");
			msgCache = new MessageCache(config);
			identityCache = new IdentityCache(config);
			
			// initialize post process callback
			logger.debug("Initialize callback");
			PostEventCacheCallback callback = new PostEventCacheCallback(msgCache, identityCache, config);
			
			// initialize error handler
			logger.debug("Initialize error handler");
			errorHandler.initialize(config);
			
			// initialize event handler
			logger.debug("Initialize event handler");
			eventHandler.initialize(config);
			eventHandler.registerCallback(callback);
		} catch (ConfigurationException ex) {
			logger.error(ex.getMessage());
			System.exit(1);
		}

		logger.info("Starting agent");
		
		try {
			final CI360Agent agent = new CI360Agent();
			CI360StreamInterface streamListener = new CI360StreamInterface() {
				public boolean processEvent(final String event) {
					Thread eventThread = new Thread() {
						public void run() {
							logger.trace("Event received");
							if (event.startsWith(CFG_EVENT_PREFIX)) {
								logger.debug("Config event received");
							} else {
								JSONObject jsonEvent = new JSONObject(event);
								JSONObject eventAttr = jsonEvent.getJSONObject(JSON_ATTRIBUTES);
								String eventName = eventAttr.getString(JSON_EVENTNAME);
								logger.info("Event received: eventName={}", eventName);
								
								AgentUtils.logEvent(eventAttr, eventLogger);
								// pretty print last json event for debug purpose into a file
								if (outputLastEvent) {
									AgentUtils.writeToFile(lastEventOutputFile, eventAttr.toString(4), false);
								}

								// update stats
								statsHashMap.put(STATS_EVENT_COUNT, statsHashMap.getOrDefault(STATS_EVENT_COUNT, 0) + 1);
								statsHashMap.put(eventName, statsHashMap.getOrDefault(eventName, 0) + 1);

								try {
									eventHandler.processEvent(jsonEvent);
								} catch (EventHandlerException e) {
									logger.error("Failed to process event, error: {}", e.getMessage());
									if (e.isRetryable()) {
										errorHandler.enqueueEvent(jsonEvent);
									}
								} catch (Exception e) {
									logger.error("Failed to process event, unhandled error: {} ({})", e.getMessage(), e.getClass().getName());
								}

							}
						}
					};
					eventThread.start();
					return true;
				}

				public void streamClosed(ErrorCode errorCode, String message) {
					if (exiting) {
						System.out.println("Stream closed");
					} else {
						System.out.println("Stream closed " + errorCode + ": " + message);
						try {
							Thread.sleep(15000);
						} catch (InterruptedException e) {

						}
						try {
							// Try to reconnect to the event stream.
							agent.startStream(this, true);
						} catch (CI360AgentException e) {
							System.err.println("ERROR " + e.getErrorCode() + ": " + e.getMessage());
						}
					}
				}
			};
			agent.startStream(streamListener, true);
			logger.info("Agent started");

			// event monitor thread
			final int monitorPrintInterval = Integer.parseInt(config.getProperty("agent.monitorOutputInterval")) * 1000;
			monitorThread = new Thread("MonitorThread") {
				public void run() {
					logger.debug("Starting monitor thread");
					while (!exiting) {
						logger.trace("Monitor thread sleeping {} ms", monitorPrintInterval);
						try {
							Thread.sleep(monitorPrintInterval);
						} catch (InterruptedException e) {
							logger.error("Thread interrupted");
						}
						
						logger.info("Event count: {}", statsHashMap.getOrDefault(STATS_EVENT_COUNT, 0));
						statsLogger.debug("Event stats (full): {}", statsHashMap.toString());
					}
					logger.info("Monitor thread exiting");
				}
			};
			monitorThread.start();
			logger.info("Monitor thread started");
			
			// keepalive thread
			final int keepaliveInterval = Integer.parseInt(config.getProperty("agent.keepaliveInterval", "0")) * 1000;
			if (keepaliveInterval > 0) {
				keepaliveThread = new Thread("KeepaliveThread") {
					public void run() {
						logger.debug("Starting keepalive thread");
						while (!exiting) {
							logger.trace("Keepalive thread sleeping {} ms", keepaliveInterval);
							try {
								Thread.sleep(keepaliveInterval);
							} catch (InterruptedException e) {
								logger.error("Thread interrupted");
							}
							
							try {
								logger.debug("Pinging gateway");
								logger.debug("Ping response: {}", agent.healthcheck());
							} catch (CI360AgentException e) {
								logger.error(e.getMessage());
								e.printStackTrace();
							}
						}
						logger.info("Keepalive thread exiting");
					}
				};
				keepaliveThread.start();
				logger.info("Keepalive thread started");
			}
			else {
				logger.info("Keepalive thread disabled");
			}
			
			// event retry thread
			final int retryThreadInterval = Integer.parseInt(config.getProperty("agent.retryInterval", "0")) * 1000;
			if (retryThreadInterval > 0) {
				retryThread = new Thread("RetryThread") {
					public void run() {
						logger.debug("Starting retry thread");
						while (!exiting) {
							logger.trace("Retry thread sleeping {} ms", retryThreadInterval);
							try {
								Thread.sleep(retryThreadInterval);
							} catch (InterruptedException e) {
								logger.error("Thread interrupted");
							}
							
							errorHandler.processQueue();
						}
						logger.info("Retry thread exiting");
					}
				};
				retryThread.start();
				logger.info("Retry thread started");
			}
			else {
				logger.info("Retry thread disabled");
			}
			
			// message status thread
			final int statusThreadInterval = Integer.parseInt(config.getProperty("agent.messageStatusInterval", "0")) * 1000;
			if (config.getProperty("agent.event.statusMethod", "").equalsIgnoreCase("POLL") && statusThreadInterval > 0) {
				statusThread = new Thread("StatusThread") {
					public void run() {
						logger.debug("Starting message status thread");
						while (!exiting) {
							logger.trace("Message status thread sleeping {} ms", statusThreadInterval);
							try {
								Thread.sleep(statusThreadInterval);
							} catch (InterruptedException e) {
								logger.error("Thread interrupted");
							}
							
							try {
								MessageStatusService msgStatusService = new MessageStatusService(config, agent, msgCache);
								msgStatusService.processCache();
							} catch (ConfigurationException e) {
								logger.error(e.getMessage());
							}
						}
						logger.info("Message status thread exiting");
					}
				};
				statusThread.start();
				logger.info("Message status thread started");
			}
			else {
				logger.info("Message status thread disabled");
			}

			// HTTP server
			if ((config.getProperty("agent.event.statusMethod", "").equalsIgnoreCase("WEBHOOK") || config.getProperty("agent.twoWay.enabled").equalsIgnoreCase("TRUE")) 
					&& config.getProperty("agent.http.port") != null) {
				logger.info("Initialize HTTP server");
				
				try {
					final int httpServerPort = Integer.parseInt(config.getProperty("agent.http.port"));
					final String httpContextRoot = config.getProperty("agent.http.webhookContextRoot");
					if (httpContextRoot == null) {
						logger.error("Missing required configuration property agent.http.webhookContextRoot");
						System.exit(1);
					}
					
					logger.debug("Server starting on port " + httpServerPort);
					logger.debug("Using context root: " + httpContextRoot);
					httpServer = HttpServer.create(new InetSocketAddress(httpServerPort), 0);
					httpServer.createContext("/", new RootHandler());
					httpServer.createContext(httpContextRoot, new ScgWebhookHandler(config, agent, msgCache, identityCache));
					httpServer.setExecutor(null);
					httpServer.start();
					
					logger.info("HTTP Server started");
				} catch (NumberFormatException e) {
					logger.error("Invalid HTTP port number, NumberFormatException: {}", e.getMessage());
					System.exit(1);
				} catch (IOException e) {
					logger.error("ERROR Starting HTTP server, IOException: {}", e.getMessage());
					System.exit(1);
				} catch (Exception e) {
					logger.error("ERROR Starting HTTP server, Exception: {}", e.getMessage());
					System.exit(1);
				}
			}
			else {
				logger.info("HTTP/webhook disabled");
			}
			
			// interactive console
			if (config.getProperty("agent.runInteractiveConsole") != null && config.getProperty("agent.runInteractiveConsole").equalsIgnoreCase("true")) {
				logger.info("Interactive commands are accepted");
				runInteractiveConsole(agent);
			} else {
				logger.info("No interactive commands are accepted");
			}

			
			// shutdown hook
			Runtime.getRuntime().addShutdownHook(new Thread("ShutdownThread") {
				public void run() {
					System.out.println("Shutdown hook activated");
					logger.warn("Starting agent shutdown");
					exiting = true;
					
					logger.info("Closing message cache");
					msgCache.close();
					
					logger.info("Stopping agent stream");
					agent.stopStream();
					
					if (httpServer != null) {
						logger.info("Stopping HTTP server");
						httpServer.stop(2);
					}
					
					logger.info("Stopping threads");
					if (monitorThread != null) monitorThread.interrupt();
					if (keepaliveThread != null) keepaliveThread.interrupt();
					if (retryThread != null) retryThread.interrupt();
					
					logger.info("Waiting 2 seconds...");
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e) {

					}
					logger.info("Shutdown process completed.");
					System.out.println("Exiting...");
				}
			});
			
		} catch (CI360AgentException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
			System.exit(-1);
		}

	}

	private static void runInteractiveConsole(CI360Agent agent) {
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
