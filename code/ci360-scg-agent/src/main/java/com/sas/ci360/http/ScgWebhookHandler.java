package com.sas.ci360.http;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sas.ci360.agent.cache.IdentityCache;
import com.sas.ci360.agent.cache.MessageCache;
import com.sas.ci360.agent.exceptions.WebhookHandlerException;
import com.sas.ci360.agent.scg.SCGMessage;
import com.sas.ci360.agent.util.AgentUtils;
import com.sas.mkt.agent.sdk.CI360Agent;
import com.sas.mkt.agent.sdk.CI360AgentException;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class ScgWebhookHandler implements HttpHandler {
	private static final Logger logger = LoggerFactory.getLogger(ScgWebhookHandler.class);
	
	private static final String CONTENT_TYPE = "Content-Type";
	private static final String APPLICATION_JSON = "application/json";
	private static final String RESPONSE_OK = "OK";
	private static final String EXT_ID_DELIM = "\\|";
	
	private static final String JSON_EVENT_NAME = "eventName";
	private static final String JSON_DATAHUB_ID = "datahub_id";
	private static final String JSON_EXTERNALCODE = "externalCode";
	
	private CI360Agent agent;
	private MessageCache msgCache;
	private IdentityCache idCache;
	
	private Map<String, String> responseEvents = new HashMap<String, String>();
	private String moEvent;
	private String moIdentityField;
	private String moFromField;
	private String moMessageBodyField;
	private boolean moMessageBodyUpperCase = false;
	
	public ScgWebhookHandler() {
		super();
	}

	public ScgWebhookHandler(Properties config, CI360Agent agent, MessageCache mc, IdentityCache idCache) {
		super();
		this.agent = agent;
		this.msgCache = mc;
		this.idCache = idCache;
		
		moEvent = config.getProperty("agent.event.moEventName");
		moIdentityField = config.getProperty("agent.event.moIdentityField");
		moFromField = config.getProperty("agent.event.moFromField");
		moMessageBodyField = config.getProperty("agent.event.moMessageBodyField");
		moMessageBodyUpperCase = config.getProperty("agent.event.moMessageBodyUpperCase", "false").toLowerCase().equals("true");
		
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
	}

	@Override
	public void handle(HttpExchange he) throws IOException {
		String httpResponse = RESPONSE_OK;
		int httpStatus = HttpStatus.SC_OK;
		
		try {
			// read request
			String reqBody = IOUtils.toString(he.getRequestBody(), StandardCharsets.UTF_8.name());
			logger.trace("Webhook payload: {}", reqBody);
			
			// parse webhook request
			SCGMessage scgMsg = new SCGMessage(reqBody);
			if (scgMsg.isMoMessage()) {
				logger.info("MO Callback received: evtId: {}, msgId: {}, body: {}", scgMsg.getEventId(), scgMsg.getMsgId(), scgMsg.getMessageBody());
				processMOEvent(scgMsg);
			}
			else {
				logger.info("MT Callback received: evtId: {}, msgId: {}, new_state: {}", scgMsg.getEventId(), scgMsg.getMsgId(), scgMsg.getNewState());
				processMTEvent(scgMsg);
			}

		} catch (IOException ex) {
			logger.error("Error reading request: {}", ex.getMessage());
			httpStatus = HttpStatus.SC_INTERNAL_SERVER_ERROR;
			httpResponse = "Error reading request: " + ex.getMessage();
		} catch (JSONException ex) {
			logger.error("Error parsing JSON: {}", ex.getMessage());
			httpStatus = HttpStatus.SC_BAD_REQUEST;
			httpResponse = "Error parsing JSON: " + ex.getMessage();
		} catch (WebhookHandlerException ex) {
			logger.error("Error processing request: {}", ex.getMessage());
			httpStatus = HttpStatus.SC_INTERNAL_SERVER_ERROR;
			httpResponse = "Error processing request: " + ex.getMessage();
		}
		
		logger.trace("Sending HTTP response");
		Headers headers = he.getResponseHeaders();
		headers.add(CONTENT_TYPE, APPLICATION_JSON);
		he.sendResponseHeaders(httpStatus, httpResponse.getBytes().length);
		OutputStream os = he.getResponseBody();
		os.write(httpResponse.getBytes());
		os.close();
	}
	
	private void processMTEvent(SCGMessage scgMsg) throws WebhookHandlerException {
		String externalId = scgMsg.getExternalMessageId();
		
		JSONObject msgData = null;
		if (externalId != null) {
			logger.debug("Status externalId: {}", externalId);
			String idComponents[] = externalId.split(EXT_ID_DELIM);
			if (idComponents.length == 2) {
				msgData = new JSONObject();
				msgData.put(JSON_DATAHUB_ID, idComponents[0]);
				msgData.put(JSON_EXTERNALCODE, idComponents[1]);
			}
		}
				
		if (msgData == null) {
			msgData = msgCache.get(scgMsg.getMsgId());
		}
		
		if (msgData != null) {
			logger.debug("msgId: {}, data: {}", scgMsg.getMsgId(), msgData.toString());
			String responseEvent = responseEvents.get(scgMsg.getNewState());
			if (responseEvent != null) {
				// add event name to retrieved object with attributes
		    	msgData.put(JSON_EVENT_NAME, responseEvent);
				logger.debug("CI360 Event object: {}", msgData.toString());
				
				// inject delivered event into 360
				try {
					String message = agent.injectEvent(msgData.toString());
					logger.debug("SUCCESS: " + message);
					msgCache.remove(scgMsg.getMsgId());
				} catch (CI360AgentException e) {
					throw new WebhookHandlerException("CI360 Gateway Exception: " + e.getMessage(), e);
				}
			}
			else {
				logger.debug("Event not found for state: {}", scgMsg.getNewState());
			}
		}
		else {
			logger.warn("Message data not found in cache or received in status, msgId={}", scgMsg.getMsgId());
		}
	}
	
	private void processMOEvent(SCGMessage scgMsg) throws WebhookHandlerException {
		if (moEvent == null) {
			logger.warn("No event name defined for MO event");
			return;
		}
		JSONObject msgData = new JSONObject();
    	msgData.put(JSON_EVENT_NAME, moEvent);
    	if (moIdentityField != null) msgData.put(moIdentityField, scgMsg.getFromAddress());
    	if (moFromField != null) msgData.put(moFromField, scgMsg.getFromAddress());
    	if (moMessageBodyField != null) {
    		if (scgMsg.getMessageBody() != null && moMessageBodyUpperCase) {
    			msgData.put(moMessageBodyField, scgMsg.getMessageBody().toUpperCase());
    		} else {
    			msgData.put(moMessageBodyField, scgMsg.getMessageBody());
    		}
    	}
    	
    	String datahubId = idCache.get(scgMsg.getFromAddress());
    	if (datahubId != null) {
    		msgData.put(JSON_DATAHUB_ID, datahubId);
    	}
    	else {
    		logger.info("Identity not found in cache for {}", AgentUtils.maskRecipient(scgMsg.getFromAddress()));
    	}
    	
		logger.debug("CI360 Event object: {}", msgData.toString());
		
		// inject MO event into 360
		try {
			String message = agent.injectEvent(msgData.toString());
			logger.debug("SUCCESS: " + message);
		} catch (CI360AgentException e) {
			throw new WebhookHandlerException("CI360 Gateway Exception: " + e.getMessage(), e);
		}

	}
}
