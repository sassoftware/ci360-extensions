/* Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.sas.ci360.agent.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sas.ci360.agent.EventHandler;
import com.sas.ci360.agent.PostEventCallback;
import com.sas.ci360.agent.exceptions.ConfigurationException;
import com.sas.ci360.agent.exceptions.EventHandlerException;
import com.sas.ci360.agent.scg.AuthInfo;
import com.sas.ci360.agent.scg.MessageRequest;
import com.sas.ci360.agent.scg.Session;
import com.sas.ci360.agent.util.AgentUtils;

public class MessageEventHandler implements EventHandler {
	private static final Logger logger = LoggerFactory.getLogger(MessageEventHandler.class);
	
	private PostEventCallback callback;
	
	private static final String EXT_ID_DELIM = "|";
	private static final String DEMO_SENDER_DELIM = "\\|";
	
	private static final String JSON_ATTRIBUTES = "attributes";
	private static final String JSON_CREATIVE_CONTENT = "creative_content";
	private static final String JSON_TIMESTAMP = "timestamp";
	private static final String JSON_FROM = "from";
	private static final String JSON_TO = "to";
	private static final String JSON_BODY = "body";
	private static final String JSON_MEDIA_URLS = "media_urls";
	private static final String JSON_CHANNEL = "channel";
	private static final String CREATIVE_JSON = "JSON";
	private static final String CREATIVE_PLAIN = "PLAIN";
	private static final String CREATIVE_PLAIN_DELIM = ";";
	private static final String CREATIVE_PLAIN_FIELD_DELIM = ":";
	private static final String CHANNEL_SMS = "SMS";
	private static final String CHANNEL_RCS = "RCS";
	private static final String CHANNEL_FACEBOOK = "Facebook";
	private static final String CHANNEL_WECHAT = "WeChat";
	private static final String CHANNEL_WHATSAPP = "WhatsApp";
	private static final String PREFIX_FACEBOOK = "fb:@";
	private static final String PREFIX_WECHAT = "we:@";
	private static final String PREFIX_WHATSAPP = "wa:";
	private static final String RCS_CONTENT_TYPE = "application/vnd.scg.grcs.raw-message";
	private static final String JSON_MSGID = "msgId";
	private static final String JSON_DATAHUB_ID = "datahub_id";
	private static final String JSON_EXTERNALCODE = "externalCode";
	private static final String JSON_RECIPIENT = "recipient";
	
	private String consumerKey;
	private String consumerSecret;
	private String accessToken;
	private String apiUrl;
	private String defaultSender;
	private String defaultChannel;
	private String creativeFormat;
	private String messageEncoding;
	private Map<String, String> senderMapSMS = new HashMap<String, String>();
	private long eventTTL = 0;

	@Override
	public void initialize() {

	}

	@Override
	public void initialize(Properties config) throws ConfigurationException {	
		initialize();
		
		this.consumerKey = config.getProperty("scg.consumerKey");
		this.consumerSecret = config.getProperty("scg.consumerSecret");
		if (config.getProperty("scg.accessToken") == null) throw new ConfigurationException("Missing required configuration property scg.accessToken");
		this.accessToken = config.getProperty("scg.accessToken");
		if (config.getProperty("scg.apiUrl") == null) throw new ConfigurationException("Missing required configuration property scg.apiUrl");
		this.apiUrl = config.getProperty("scg.apiUrl");
		this.defaultSender = config.getProperty("scg.defaultSender");
		this.defaultChannel = config.getProperty("scg.defaultChannel");
		if (config.getProperty("agent.creative.format") == null) throw new ConfigurationException("Missing required configuration property agent.creative.format");
		this.creativeFormat = config.getProperty("agent.creative.format").toUpperCase();
		this.messageEncoding = config.getProperty("agent.creative.encoding") == null || config.getProperty("agent.creative.encoding").trim().isEmpty() ? null : config.getProperty("agent.creative.encoding");
		
		String demoSenders = config.getProperty("scg.demo.senders.sms");
		if (demoSenders != null && !demoSenders.trim().isEmpty()) {
			String senders[] = demoSenders.split(DEMO_SENDER_DELIM);
			for (int i=0; i < senders.length; i++) {
				String sender[] = senders[i].split(",");
				String countryCode = sender[0];
				String channel = sender[1];
				logger.debug("Mapping country code {} to {}", countryCode, channel);
				this.senderMapSMS.put(countryCode, channel);
			}
		}
		
		if (config.getProperty("agent.event.TTL") != null) {
			this.eventTTL = Long.parseLong(config.getProperty("agent.event.TTL")) * 1000;
		}
	}
	
	@Override
	public void registerCallback(PostEventCallback callback) {
		this.callback = callback;
	}
	
	@Override
	public void processEvent(JSONObject jsonEvent) throws EventHandlerException {
		logger.trace("Event: {}", jsonEvent.toString());
		// parse event
		JSONObject eventAttr = jsonEvent.getJSONObject(JSON_ATTRIBUTES);
		
		if (this.eventTTL > 0) {
			long eventTimestamp = eventAttr.getLong(JSON_TIMESTAMP);
			if (System.currentTimeMillis() > eventTimestamp + this.eventTTL) {
				long age = System.currentTimeMillis() - eventTimestamp;
				logger.warn("RESULT:MSG_STALE:" + eventTimestamp + ":" + age);
				throw new EventHandlerException("Rejected stale event: event timestamp=" + eventTimestamp + ", age=" + age, EventHandlerException.NOT_RETRYABLE);
			}
		}
		
		if (!eventAttr.has(JSON_CREATIVE_CONTENT)) {
			throw new EventHandlerException("Event missing creative", EventHandlerException.NOT_RETRYABLE);
		}
		
		String creative = eventAttr.getString(JSON_CREATIVE_CONTENT);
		logger.debug("creative_content: " + creative);
		
		String msgId = null;
		MessageRequest mrq = null;
		try {			
			// parse input and build message
			if (this.creativeFormat.contentEquals(CREATIVE_JSON)) {
				mrq = buildMessageRequestJSON(creative);
			} else if (this.creativeFormat.contentEquals(CREATIVE_PLAIN)) {
				mrq = buildMessageRequestPlain(creative);
			} else {
				throw new Exception("No creative format specified in configuration");
			}
			mrq.setExternal_id(createExternalId(eventAttr.getString(JSON_DATAHUB_ID), eventAttr.getString(JSON_EXTERNALCODE)));
		} catch (Exception ex) {
			logger.error("RESULT:MSG_PARSE_FAILED:" + creative + ":" + ex.getMessage());
			throw new EventHandlerException("Event creative parsing error", EventHandlerException.NOT_RETRYABLE, ex);
		}

		try {
			// Construct an instance of the authentication object
		    AuthInfo auth = new AuthInfo(this.consumerKey, this.consumerSecret, this.accessToken);
	
		    // Prepare a session to the server
		    Session session = new Session(this.apiUrl, auth, this.messageEncoding);
		    
		    // Send message
		    msgId = session.create(mrq);
		    logger.info("RESULT:API_DELIVERED:" + msgId + ":" + AgentUtils.maskRecipients(mrq.getTo()));
		} catch (Exception ex) {
			logger.error("RESULT:API_CALL_FAILED:" + creative + ":" + ex.getMessage());
			throw new EventHandlerException("Syniverse API call error", EventHandlerException.IS_RETRYABLE, ex);
		}
		
	    if (this.callback != null && msgId != null) {	    	
	    	try {
		    	JSONObject eventData = new JSONObject();
		    	eventData.put(JSON_MSGID, msgId);
		    	eventData.put(JSON_DATAHUB_ID, eventAttr.getString(JSON_DATAHUB_ID));
		    	eventData.put(JSON_EXTERNALCODE, eventAttr.getString(JSON_EXTERNALCODE));
		    	eventData.put(JSON_RECIPIENT, mrq.getTo().get(0));
		    	
	    		logger.debug("Adding message to status cache");
	    		this.callback.postEvent(eventData);
	    	} catch (Exception ex) {
	    		logger.error("Event callback post failed: msgId=" + msgId);
	    	}
	    }
	}

	private MessageRequest buildMessageRequestJSON(String creative) throws Exception {
		JSONObject message = new JSONObject(creative);
		
		String channel = this.defaultChannel;
		
		String from = message.has(JSON_FROM) ? message.getString(JSON_FROM) : null;
		String to = message.getString(JSON_TO);
		String body = message.getString(JSON_BODY);
		String mediaUrls = message.has(JSON_MEDIA_URLS) ? message.getString(JSON_MEDIA_URLS) : null;
		
		if (to == null || to.trim().equals("")) {
			throw new Exception("Missing recipient");
		}
		if (body == null || body.trim().equals("")) {
			throw new Exception("Missing message text");
		}
		
		if (from == null) {
			from = getSender(to, channel);
		}
		
	    MessageRequest mrq = new MessageRequest();
	    mrq.setFrom(from);
	    mrq.setTo(to);
	    mrq.setBody(body);
	    if (mediaUrls != null) mrq.setMedia_urls(mediaUrls);
	    
	    return mrq;
	}
	
	private MessageRequest buildMessageRequestPlain(String creative) throws Exception {
		try {
			String from = null;
			String to = null;
			String mediaUrls = null;
			String channel = this.defaultChannel;
			
			String creativeParts[] = creative.split(CREATIVE_PLAIN_DELIM);
			String msgBody = creativeParts[creativeParts.length-1].trim();
			logger.trace("msgBody: " + msgBody);
			
			for (int i=0; i < creativeParts.length-1; i++) {
				String headerField[] = creativeParts[i].split(CREATIVE_PLAIN_FIELD_DELIM, 2);
				String key = headerField[0].trim();
				String value = headerField[1].trim();
				logger.trace("key: " + key + ", val: " + value);
				
				if (key.toLowerCase().equals(JSON_TO)) {
					to = value;
				} else if (key.toLowerCase().equals(JSON_FROM)) {
					from = value;
				} else if (key.toLowerCase().equals(JSON_MEDIA_URLS)) {
					mediaUrls = value;
				} else if (key.toLowerCase().equals(JSON_CHANNEL)) {
					channel = value;
				}
			}
			
			if (to == null || to.trim().equals("")) {
				throw new Exception("Missing recipient");
			}
			if (msgBody == null || msgBody.trim().equals("")) {
				throw new Exception("Missing message text");
			}
			
			if (from == null) {
				from = getSender(to, channel);
			}
			
			logger.debug("Channel: " + channel);
			
		    MessageRequest mrq = new MessageRequest();
		    mrq.setFrom(from);
		    mrq.setTo(constructRecipient(to, channel));
		    mrq.setBody(msgBody);
		    if (mediaUrls != null) mrq.setMedia_urls(mediaUrls);
		    
		    // channel specific fields
		    if (channel.equalsIgnoreCase(CHANNEL_RCS)) {
		    	if (msgBody.startsWith("{") && msgBody.endsWith("}")) {
			    	logger.debug("Setting RCS specific attributes");
			    	mrq.setContent_type(RCS_CONTENT_TYPE);
		    	}
		    }
		    
		    return mrq;
		}
		catch (Exception ex) {
			throw new Exception("Error parsing message: " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}
	
	private String constructRecipient(String to, String channel) {
		if (channel.equalsIgnoreCase(CHANNEL_FACEBOOK)) {
			return PREFIX_FACEBOOK + to;
		} else if (channel.equalsIgnoreCase(CHANNEL_WECHAT)) {
			return PREFIX_WECHAT + to;
		} else if (channel.equalsIgnoreCase(CHANNEL_WHATSAPP)) {
			return PREFIX_WHATSAPP + to;
		}
		return to;
	}
	
	private String getSender(String to, String channel) {
		if (CHANNEL_SMS.equals(channel) && this.senderMapSMS.size() > 0) {
			for (Map.Entry<String,String> entry : this.senderMapSMS.entrySet()) {				
				if (to.startsWith(entry.getKey()) || to.startsWith("+" + entry.getKey())) {
					logger.debug("Specific sender found for prefix " + entry.getKey() + ": " + entry.getValue());
					return entry.getValue();
				}
			}
		}
		return this.defaultSender;		
	}
	
	private String createExternalId(String datahubId, String externalCode) {
		return datahubId + EXT_ID_DELIM + externalCode;
	}
}
