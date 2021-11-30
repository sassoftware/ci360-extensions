/* Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.sas.ci360.agent;

import com.sas.ci360.agent.exceptions.ConfigurationException;
import com.sas.ci360.agent.exceptions.EventHandlerException;
import com.sas.ci360.agent.impl.ESPEventHandler;
import com.sas.ci360.agent.util.AgentUtils;
import com.sas.mkt.agent.sdk.CI360Agent;
import com.sas.mkt.agent.sdk.CI360AgentException;
import com.sas.mkt.agent.sdk.CI360StreamInterface;
import com.sas.mkt.agent.sdk.ErrorCode;

import java.util.Properties;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ESPAgent {
	private static final Logger logger = LoggerFactory.getLogger(ESPAgent.class);
	private static final Logger eventLogger = LoggerFactory.getLogger("CustomAgent.events");
	
	private static final String CFG_EVENT_PREFIX = "CFG";
	private static final int DEFAULT_KEEPALIVE_INTERVAL = 600000;

	static boolean exiting=false;
	static boolean run=true;
	
	private static EventHandler eventHandler = new ESPEventHandler();
	
	private static Thread keepaliveThread;
	
	
	public static void main(String[] args) {
		Properties props = System.getProperties();
		final Properties config = AgentUtils.readConfig(props.getProperty("configFile"));	
		
		props.setProperty("ci360.gatewayHost",         config.getProperty("ci360.gatewayHost"));
		props.setProperty("ci360.tenantID",            config.getProperty("ci360.tenantID"));
		props.setProperty("ci360.clientSecret",        config.getProperty("ci360.clientSecret"));
		
		final String espWindowUrl = 
					  config.getProperty("esp.Project") + "/" 
					+ config.getProperty("esp.Query") + "/" 
					+ config.getProperty("esp.Window");
		
		if ( config.getProperty("esp.callEsp", "").equalsIgnoreCase("true") ) {
			logger.info("ESP is enabled, forwarding events to window: {}", espWindowUrl);
		} else {
			logger.warn("ESP is disabled");
		}
		
		if ( config.getProperty("sid.callSid", "").equalsIgnoreCase("true") ) {
			logger.info("SID is enabled, forwarding events containing string: {}", config.getProperty("ci360.eventRequest") );
		} else {
			logger.warn("SID is disabled");
		}
		
		// initialize event handler
		try {
			logger.debug("Initialize event handler");
			eventHandler.initialize(config);
		} catch (ConfigurationException ex) {
			logger.error(ex.getMessage());
			System.exit(1);
		}
		
		try {
			final CI360Agent agent=new CI360Agent();
			eventHandler.setAgent(agent);
			CI360StreamInterface streamListener=new CI360StreamInterface() {
				public boolean processEvent(String event) {
					logger.debug("Event: " + event);
					
					if (event.startsWith(CFG_EVENT_PREFIX)) {
						logger.debug("Config event received");
						return true;
					}
					
					//for form submits delete the prefix of form.f. from all form fields, then you can use it in ESP
					String event_manipulated = event.replaceAll("form.f.", "");
					boolean manipulation_error = false;
					JSONObject jsonEvent = new JSONObject();
					
					try {
						JSONObject jsonEvent_manipulated = new JSONObject(event_manipulated);						
		            } catch (JSONException e) {
		            	manipulation_error = true;
		            	logger.warn("Event manipulation error: {}", e.toString());
		            }
					
					if (manipulation_error) {
						jsonEvent = new JSONObject(event); 
					} else {
						jsonEvent = new JSONObject(event_manipulated);
					}
					JSONObject eventAttr = jsonEvent.getJSONObject("attributes");
												
					AgentUtils.logEvent(eventAttr, eventLogger);
					logger.debug("MANIPULATED EVENT: {} - {}", eventAttr.getString("eventname"), event_manipulated);						
					
					logger.info("Process event");
					try {
						eventHandler.processEvent(jsonEvent);
					} catch (EventHandlerException e) {
						logger.error("Failed to process event, error: {}", e.getMessage());
						if (e.isRetryable()) {
							// TODO: add error/retry handler
						}
					} catch (Exception e) {
						logger.error("Failed to process event, unhandled error: {} ({})", e.getMessage(), e.getClass().getName());
					}
					
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
							//Try to reconnect to the event stream.
							agent.startStream(this, true);
						} catch (CI360AgentException e) {
							System.err.println("ERROR " + e.getErrorCode() + ": " + e.getMessage());
						}
					}
				}
			};
			agent.startStream(streamListener, true);
			
			int keepaliveIntervalConf = Integer.parseInt(config.getProperty("agent.keepaliveInterval", "0"));
			final boolean enableHealthcheck = keepaliveIntervalConf > 0;
			final int keepaliveInterval = keepaliveIntervalConf > 0 ? keepaliveIntervalConf : DEFAULT_KEEPALIVE_INTERVAL;
			if (keepaliveInterval > 0) {
				keepaliveThread = new Thread("KeepaliveThread") {
					public void run() {
						logger.debug("Starting keepalive thread");
						while (!exiting) {
							logger.trace("Keepalive thread sleeping " + keepaliveInterval + " ms");
							try {
								Thread.sleep(keepaliveInterval);
							} catch (InterruptedException e) {
								logger.error("Thread interrupted");
							}
							
							if (enableHealthcheck) {
								try {
									logger.debug("Pinging gateway");
									logger.debug("Ping response: " + agent.healthcheck());
								} catch (CI360AgentException e) {
									logger.error(e.getMessage());
									e.printStackTrace();
								}
							}
						}
					}
				};
				keepaliveThread.start();
				logger.info("Keepalive thread started");	
			}

			System.out.println("No interactive commands are accepted  (Hit CTRL+C to exit) \n");
			//Exit for CTRL+C
			Runtime.getRuntime().addShutdownHook(new Thread() {
				public void run() {
					System.out.println("Shutdown hook activated");
					logger.warn("Starting agent shutdown");
					run = false;
				    exiting=true;
				    logger.info("Stopping agent stream");
					agent.stopStream();
					
					logger.info("Stopping threads");
					if (keepaliveThread != null) keepaliveThread.interrupt();
					
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

}
