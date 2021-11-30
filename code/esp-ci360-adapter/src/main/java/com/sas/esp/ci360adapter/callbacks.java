/* Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.sas.esp.ci360adapter;

/* These import files are specific to the ESP objects being used here. */
import com.sas.esp.api.server.event.EventOpcodes;
import com.sas.esp.api.server.ReferenceIMPL.dfESPevent;
import com.sas.esp.api.server.ReferenceIMPL.dfESPeventblock;
import com.sas.esp.api.server.ReferenceIMPL.dfESPschema;
import com.sas.util.HttpUtils;
/* These import files are needed for all subscribing code. */
import com.sas.esp.api.pubsub.clientCallbacks;
import com.sas.esp.api.pubsub.clientFailures;
import com.sas.esp.api.pubsub.clientFailureCodes;
import com.sas.esp.api.pubsub.clientGDStatus;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.sas.esp.api.dfESPException;

public class callbacks implements clientCallbacks {
	private static final Logger logger = LoggerFactory.getLogger(callbacks.class);
	
	private static final String HEADER_AUTH = "Authorization";
	private static final String HEADER_BEARER = "Bearer ";
	private static final String CONTENTTYPE_JSON = "application/json";
	private static final String DUMMY_ARRAYEND = ",XX";
	private static final String COMMA_DELIM = ",";
	private static final String DATATYPE_INT32 = "INT32";
	private static final String DATATYPE_INT64 = "INT64";
	private static final String DATATYPE_DOUBLE = "DOUBLE";
	private static final String DATATYPE_UTF8STR = "UTF8STR";
	
	/* Flag indicating whether to keep main in a non-busy wait while the subscribeFunction does its thing.
    */
	private static boolean nonBusyWait = true;
	
	private String ci360identity;
	private String espIdentity;
	private String ci360token;
	private String ci360gateway;
	private String ci360externalEvent;
	private String ci360applicationId;
	private Map<String, String> ci360Headers;

	// Constructor
	callbacks(Properties config) {
		ci360identity = config.getProperty("ci360.identity");
		espIdentity = config.getProperty("esp.identityAttr");
		ci360token = config.getProperty("ci360.token");
		ci360gateway = config.getProperty("ci360.gateway");
		ci360externalEvent = config.getProperty("ci360.exteventname");
		ci360applicationId = config.getProperty("ci360.applicationId");
		
		/* create HTTP headers object for CI360 API */
		ci360Headers = new HashMap<String, String>();
		ci360Headers.put(HEADER_AUTH, HEADER_BEARER + ci360token);
	}

	public void processEvent(String eventStr, String schemaStr, String formatStr) {
		//String identityValue = "";

		// added one attribute just to make sure to create an array with the full length of attributes
		// if the last attribute is null or an empty string, then the attribute doesn't make it into the array
		logger.debug("Event Attributes..: {}", schemaStr);
		logger.debug("Event Formats.....: {}", formatStr);
		logger.debug("Event Values......: {}", eventStr);
		eventStr = eventStr + DUMMY_ARRAYEND;
		schemaStr = schemaStr + DUMMY_ARRAYEND;
		formatStr = formatStr + DUMMY_ARRAYEND;

		String[] keys = schemaStr.split(COMMA_DELIM);
		String[] values = eventStr.split(COMMA_DELIM);
		String[] formats = formatStr.split(COMMA_DELIM);

		JSONObject ci360Payload = new JSONObject();
		ci360Payload.put("eventName", ci360externalEvent);
		if (ci360applicationId != null) {
			ci360Payload.put("applicationId", ci360applicationId);			
		}
		
		logger.debug("Populate CI360 event object");
		for (int i = 0; i < keys.length - 1; i++) {
//			if (keys[i].equalsIgnoreCase(espIdentity)) {
//				identityValue = values[i];
//			}

			if (formats[i].equalsIgnoreCase(DATATYPE_INT32) || formats[i].equalsIgnoreCase(DATATYPE_INT64)) {
				if (values[i] != null && values[i].length() > 0) {
					ci360Payload.put(keys[i], Long.valueOf(values[i]));
				}
			} else if (formats[i].equalsIgnoreCase(DATATYPE_DOUBLE)) {
				try {
					ci360Payload.put(keys[i], Double.valueOf(values[i]));
				} catch (NumberFormatException e) {
					// value format doesn't fit
					ci360Payload.put(keys[i], values[i]);
				}

			} else if (formats[i].equalsIgnoreCase(DATATYPE_UTF8STR)) {
				ci360Payload.put(keys[i], values[i]);
			} else {
				ci360Payload.put(keys[i], values[i]);
			}
		}
//		ci360Payload.put(ci360identity, identityValue);
		logger.debug("CI360 Event: {}", ci360Payload.toString());
		
		logger.debug("Call CI360 Event API");		
		try {
			String ci360response = HttpUtils.httpPost(ci360gateway, ci360Payload.toString(), CONTENTTYPE_JSON, ci360Headers);
//			JSONObject ci360responseJson = new JSONObject(ci360response);
			logger.info("CI360 Event injected: {}", ci360response);
		} catch (Exception e) {
			logger.error("Failed to call CI360 Gateway: {}", e.getMessage());
		}

	}

	
	/* We need to define a subscribe method which will get called when new events
	   are published from the server via the pub/sub API. This method gets an
	   eventblock, the schema of the event block for processing purposes, and an optional
	   user context pointer supplied by the call to start().
	   For this example we are just going to write the event as CSV to System.err.
	*/
	public void dfESPsubscriberCB_func(dfESPeventblock eventBlock, dfESPschema schema, Object ctx) {		
		String schemaStr = schema.getNames().toString().replace("[","").replace("]","").replace(", ",",");
		String formatStr = schema.getTypes().toString().replace("[","").replace("]","").replace(", ",",");
		
		dfESPevent event;
		int eventCnt = eventBlock.getSize();

		for (int eventIndx = 0; eventIndx < eventCnt; eventIndx++) {
	        /* Get the event out of the event block. */
			event = eventBlock.getEvent(eventIndx);
			try {
		        /* Convert from binary to CSV using the schema and print to System.err. */
			    String eventStr = event.toStringCSV(schema, true, false).substring(4);

			    logger.info("Event detected, processing event...");
				processEvent(eventStr, schemaStr, formatStr);				
			} catch (dfESPException e) {
				logger.error("event.toString() failed");
				return;
			}
			if (event.getOpcode() == EventOpcodes.eo_UPDATEBLOCK) {
				++eventIndx;  /* skip the old record in the update block */
			}
		}
	}
	/* We also define a callback function for subscription failures given
	   we may want to try to reconnect/recover, but in this example we will just print
	   out some error information and release the non-busy wait set below so the 
	   main in program can end. The cbf has an optional context pointer for sharing state
	   across calls or passing state into calls.
	*/
	public void dfESPpubsubErrorCB_func(clientFailures failure, clientFailureCodes code, Object ctx) {
	    switch (failure) {
	    case pubsubFail_APIFAIL:
	    	logger.error("Client subscription API error with code {}", code);
	        break;
	    case pubsubFail_THREADFAIL:
	    	logger.error("Client subscription thread error with code {}", code);
	        break;
	    case pubsubFail_SERVERDISCONNECT:
	    	logger.error("Server disconnect");
		case pubsubFail_NONE:
			break;
		default:
			break;
	    }
	    /* Release the busy wait which will end the program. */
		nonBusyWait = false;
	}
	/* We need to define a dummy publish method which is only used when implementing
	   guaranteed delivery.
	*/
        public void dfESPGDpublisherCB_func(clientGDStatus eventBlockStatus, long eventBlockID, Object ctx) {
	}
	public boolean getNonBusyWait() {
		return nonBusyWait;
	}
}
