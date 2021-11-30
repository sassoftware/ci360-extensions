/* Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.sas.ci360.agent.status;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sas.ci360.agent.cache.MessageCache;
import com.sas.ci360.agent.exceptions.ConfigurationException;
import com.sas.ci360.agent.scg.AuthInfo;
import com.sas.ci360.agent.scg.MessageRequest;
import com.sas.ci360.agent.scg.Session;
import com.sas.mkt.agent.sdk.CI360Agent;
import com.sas.mkt.agent.sdk.CI360AgentException;

public class MessageStatusService {
	private static final Logger logger = LoggerFactory.getLogger(MessageStatusService.class);
	
	private static final String JSON_RETRY_CNT = "retryCnt";
	private static final int MAX_RETRIES = 2;
	
	private CI360Agent agent;
	private MessageCache mc;
	
	private final String consumerKey;
	private final String consumerSecret;
	private final String accessToken;
	private final String apiUrl;
	private final String messageEncoding;
	
	private Map<String, String> responseEvents = new HashMap<String, String>();
	
	public MessageStatusService(Properties config, CI360Agent agent, MessageCache mc) throws ConfigurationException {
		this.agent = agent;
		this.mc = mc;
		
		String responseEventNames = config.getProperty("agent.event.statusEventNames");
		if (responseEventNames != null) {
			String dispositions[] = responseEventNames.split(",");
			for (int i=0; i < dispositions.length; i++) {
				String disposition[] = dispositions[i].split(":");
				String status = disposition[0];
				String eventName = disposition[1];
				logger.debug("Mapping status {} to event {}", status, eventName);
				responseEvents.put(status, eventName);
			}
		}
		
		this.consumerKey = config.getProperty("scg.consumerKey");
		this.consumerSecret = config.getProperty("scg.consumerSecret");
		if (config.getProperty("scg.accessToken") == null) throw new ConfigurationException("Missing required configuration property scg.accessToken");
		this.accessToken = config.getProperty("scg.accessToken");
		if (config.getProperty("scg.apiUrl") == null) throw new ConfigurationException("Missing required configuration property scg.apiUrl");
		this.apiUrl = config.getProperty("scg.apiUrl");
		this.messageEncoding = config.getProperty("agent.creative.encoding") == null || config.getProperty("agent.creative.encoding").trim().isEmpty() ? null : config.getProperty("agent.creative.encoding");
	}
	
	public void processCache() {
		logger.info("Processing cached entries");
		
		List<String> msgIds = mc.getKeys();
		logger.debug("Got list of message keys: " + msgIds.size());
		
		// Construct an instance of the authentication object
	    AuthInfo auth = new AuthInfo(this.consumerKey, this.consumerSecret, this.accessToken);

	    // Prepare a session to the server
	    Session session = new Session(this.apiUrl, auth, this.messageEncoding);
		
		for (String msgId : msgIds) {
		    try {
		    	JSONObject msgData = mc.get(msgId);
		    	if (msgData != null) {
		    		int retryCnt = 0;
		    		if (msgData.has(JSON_RETRY_CNT)) {
		    			retryCnt = msgData.getInt(JSON_RETRY_CNT);
		    		}
		    		
		    		MessageRequest result = session.get(msgId);
				    logger.debug("Message state: {}, msgId: {}", result.getState(), msgId);
				    String responseEvent = responseEvents.get(result.getState());
				    if (responseEvent != null) {
						// add event name to retrieved object with attributes
				    	msgData.put("eventName", responseEvent);
				    	logger.debug("CI360 Event object: " + msgData.toString());
				    	
						// inject delivered event into 360					
						try {
							String message = agent.injectEvent(msgData.toString());
							logger.debug("SUCCESS: " + message);
							mc.remove(msgId);
						} catch (CI360AgentException e) {
							logger.error("ERROR: " + e.getMessage());
							if (retryCnt < MAX_RETRIES) {
								msgData.put(JSON_RETRY_CNT, retryCnt + 1);
								mc.put(msgId, msgData);
							}
							else {
								logger.debug("Retry count exceeded for message, removing");
								mc.remove(msgId);
							}
						}
					}
					else {
						logger.debug("Event not found for state: {}", result.getState());
					}
		    	}
		    	
		    } catch (Exception ex) {
		    	logger.warn("Status call failed: " + ex.getMessage());
		    }
		}
		

	}
}
