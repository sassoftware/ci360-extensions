/* Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.sas.ci360.agent.impl;

import java.util.Properties;
import java.util.Random;

import org.apache.http.entity.ContentType;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sas.ci360.agent.EventHandler;
import com.sas.ci360.agent.PostEventCallback;
import com.sas.ci360.agent.exceptions.ConfigurationException;
import com.sas.ci360.agent.exceptions.EventHandlerException;
import com.sas.ci360.agent.util.HttpUtils;
import com.sas.mkt.agent.sdk.CI360Agent;

public class ESPEventHandler implements EventHandler {
	private static final Logger logger = LoggerFactory.getLogger(ESPEventHandler.class);
	
	private final Random rand = new Random();
	
	private String espProject;
	private String espQuery;
	private String espWindow;
	private String urlParameters;
	private String espServer;
	private String espWindowUrl;
	private String urlContextRoot;

	@Override
	public void initialize() {
		
	}
	
	@Override
	public void initialize(Properties config) throws ConfigurationException {
		this.espProject    = config.getProperty("esp.Project");
		this.espQuery      = config.getProperty("esp.Query");
		this.espWindow     = config.getProperty("esp.Window");
		this.urlParameters = config.getProperty("esp.urlParameters");
		this.espServer     = "http://"+config.getProperty("esp.host") +":"+ config.getProperty("esp.port");
		this.espWindowUrl  = espProject + "/" + espQuery + "/" + espWindow;
		// TODO: will change in SAS ESP version 6.2 - example: .../eventStreamProcessing/windows/...
		this.urlContextRoot = "/SASESP";

	}
	
	@Override
	public void processEvent(JSONObject jsonEvent) throws EventHandlerException  {
		JSONObject eventAttr = jsonEvent.getJSONObject("attributes");
		logger.info("Call ESP");
		callESP(eventAttr);
	}
	
	
	
	private void callESP (JSONObject eventAttr)  {
		JSONArray espEventPayload = new JSONArray();
		JSONArray espEventBlock = new JSONArray();

		String eventid_str = eventAttr.getString("timestamp") + getRandomNumberInRange(1,10000);
		eventAttr.put("eventid", eventid_str);
		espEventBlock.put(eventAttr);
		
		espEventPayload.put(espEventBlock);

		// will change in SAS ESP version 6.2 - example: .../eventStreamProcessing/windows/...
		String url = espServer + urlContextRoot + "/windows/"+ espWindowUrl +"/state"+ urlParameters;
		logger.info("ESP window URL: " + url);
		logger.debug("ESP event payload: " + espEventPayload.toString());
		
		try {
			String response = HttpUtils.httpPut(url, espEventPayload.toString(), ContentType.APPLICATION_JSON.toString());
			logger.info("ESP response: " + response);
		} catch (Exception e) {
			logger.error("Exception: " + e.getMessage());
			e.printStackTrace();
		}	
	}
	
	
	private int getRandomNumberInRange(int min, int max) {
		if (min >= max) {
			throw new IllegalArgumentException("max must be greater than min");
		}

		return rand.nextInt((max - min) + 1) + min;
	}


	@Override
	public void registerCallback(PostEventCallback callback) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setAgent(CI360Agent agent) {
		// TODO Auto-generated method stub		
	}
}
