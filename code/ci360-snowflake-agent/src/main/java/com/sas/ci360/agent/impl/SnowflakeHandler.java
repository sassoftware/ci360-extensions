/* Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.sas.ci360.agent.impl;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import javax.swing.LookAndFeel;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sas.ci360.agent.EventHandler;
import com.sas.ci360.agent.PostEventCallback;
import com.sas.ci360.agent.exceptions.ConfigurationException;
import com.sas.ci360.agent.exceptions.EventHandlerException;

import com.sas.ci360.agent.util.SnowflakeUtils;
import com.sas.ci360.agent.util.Constants;

public class SnowflakeHandler implements EventHandler {
	private static final Logger logger = LoggerFactory.getLogger(SnowflakeHandler.class);
	private String write_failed_events_folder;
	private boolean writeFailedEvents;

	@Override
	public void initialize() {
		// default properties

	}

	@Override
	public void initialize(Properties config) throws ConfigurationException {
		initialize();
		write_failed_events_folder = config.getProperty("agent.write_failed_events_folder");
		writeFailedEvents = write_failed_events_folder != null;
		SnowflakeUtils.createConnection(config);
	}

	@Override
	public void registerCallback(PostEventCallback callback) {

	}

	@Override
	public void processEvent(JSONObject jsonEvent) throws EventHandlerException {
		logger.trace("(rowKey:"+jsonEvent.getString(Constants.JSON_ROWKEY)+"), "+"Event: " + jsonEvent.toString());
		boolean result = SnowflakeUtils.insertEvent(jsonEvent);
		if (!result) {
			if (writeFailedEvents)
				writeFailedEventsToFolder(jsonEvent, write_failed_events_folder);
		}
	}

	public void writeFailedEventsToFolder(JSONObject jsonEvent, String folderPath) {
		try {
			JSONObject eventAttr = jsonEvent.getJSONObject(Constants.JSON_ATTRIBUTES);
			String eventName = eventAttr.getString(Constants.JSON_EVENTNAME);
			String currentRowKey = jsonEvent.getString(Constants.JSON_ROWKEY);
			String filePath = folderPath + "/" + eventName + "_" + currentRowKey + ".err";
			logger.info("Error file location = {}", filePath);
			FileWriter errorFile = new FileWriter(filePath);
			errorFile.write(jsonEvent.toString());
			errorFile.close();

		} catch (IOException ioe) {
			logger.error("(rowKey:"+jsonEvent.getString(Constants.JSON_ROWKEY)+"), "+"An exception occured while writig to event to error folder log: {}", ioe.toString());
			logger.error("(rowKey:"+jsonEvent.getString(Constants.JSON_ROWKEY)+"), "+"Unable to write to error file for Event = {}", jsonEvent.toString());
		}

	}
}
