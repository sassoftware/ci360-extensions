/* Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.sas.ci360.agent.impl;

/*import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;*/
import java.util.ArrayList;
import java.util.Arrays;

import java.util.List;
import java.util.Properties;

/*import org.apache.commons.dbcp2.BasicDataSource;*/
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*import com.google.protobuf.Descriptors.DescriptorValidationException;*/
import com.sas.ci360.agent.EventHandler;
import com.sas.ci360.agent.PostEventCallback;
import com.sas.ci360.agent.exceptions.ConfigurationException;
import com.sas.ci360.agent.exceptions.EventHandlerException;

import com.sas.ci360.agent.util.AgentUtils;
import com.sas.ci360.agent.util.GoogleBQUtil;

public class GoogleBQStorageAPIHandler implements EventHandler {
	private static final Logger logger = LoggerFactory.getLogger(GoogleBQStorageAPIHandler.class);

	private List<String> columns = new ArrayList<String>();

	private boolean writeToDb;

	private String projectId = "";
	private String datasetName = "";
	private String tableName = "";

	@Override
	public void initialize() {

	}

	@Override
	public void initialize(Properties config) throws ConfigurationException {
		initialize();

		this.writeToDb = config.getProperty("db.writeToDb") != null
				&& config.getProperty("db.writeToDb").equalsIgnoreCase("true");

		if (this.writeToDb) {

			if (config.getProperty("db.projectId") == null)
				throw new ConfigurationException("Missing required configuration property db.projectId");
			if (config.getProperty("db.datasetName") == null)
				throw new ConfigurationException("Missing required configuration property db.datasetName");
			if (config.getProperty("db.tableName") == null)
				throw new ConfigurationException("Missing required configuration property db.tableName");
			if (config.getProperty("db.columns") == null)
				throw new ConfigurationException("Missing required configuration property db.columns");
			if (config.getProperty("agent.failedEventFileLocation") == null)
				throw new ConfigurationException(
						"Missing required configuration property agent.failedEventFileLocation");

			this.projectId = config.getProperty("db.projectId");
			this.datasetName = config.getProperty("db.datasetName");
			this.tableName = config.getProperty("db.tableName");
			AgentUtils.failedEventFileLocation = config.getProperty("agent.failedEventFileLocation");

			// This is the retry count while insterting data to GBQ. Default is 3.
			if (config.getProperty("db.max_retry_count") != null)
				AgentUtils.MAX_RETRY_COUNT = Integer.parseInt(config.getProperty("db.max_retry_count"));

			// This is what decides how many back to back errors the agent will handle
			// before acknowledging the events back to 360. Default is 2500.
			if (config.getProperty("agent.max_error_count") != null)
				AgentUtils.MAX_ERROR_COUNT = Integer.parseInt(config.getProperty("agent.max_error_count"));

			String columns_temp = config.getProperty("db.columns");
			if (columns_temp != null && columns_temp.trim().length() > 0) {
				this.columns = Arrays.asList(columns_temp.split(","));
				AgentUtils.columns = this.columns;
			} else {
				throw new ConfigurationException("Invalid configuration property db.columns");
			}

			// check if there is a need to convert certain columns to date time
			if (config.getProperty("db.convert_to_date_time_columns") != null) {
				String convert_columns_temp = config.getProperty("db.convert_to_date_time_columns");
				if (convert_columns_temp != null && convert_columns_temp.trim().length() > 0) {
					AgentUtils.date_time_convert_columns = Arrays.asList(convert_columns_temp.split(","));
				}
			}

			logger.info("Event will be written to DB.");
			logger.debug("Google: projectId=" + this.projectId + ", datasetName=" + this.datasetName + ", tableName="
					+ this.tableName);
			logger.debug("Columns to be written: " + columns_temp);

			try {
				GoogleBQUtil.initialize(this.projectId, this.datasetName, this.tableName);
			} catch (Exception ex) {
				logger.error("Unable to initialize the GBQ table.", ex);
				throw new ConfigurationException("Unable to initialize the GBQ table. " + ex.getMessage());
			}
		} else {
			logger.info("Event won't be written to DB.");
		}

	}

	@Override
	public void registerCallback(PostEventCallback callback) {

	}

	@Override
	public void processEvent(JSONObject jsonEvent) throws EventHandlerException {
		
		if (writeToDb) {
			String rowKey = AgentUtils.getFormattedLogPrefix(jsonEvent);
			logger.debug(rowKey + "Start processing event.");
			
			try {
				logger.trace("Attempting to get the values for columns configured.");
				JSONArray jsonArr = AgentUtils.getSelectedColumns(jsonEvent);
				logger.trace("Got the columns. Final list: "+jsonArr.toString());
				GoogleBQUtil.writeToDefaultStream(jsonArr, rowKey);
				logger.debug(rowKey + "End processing event.");
			} //catch (DescriptorValidationException | InterruptedException | IOException | Exception e) {
				catch (Exception e){
				AgentUtils.Event_Error_Count++;
				logger.error(rowKey + "Unable to write the event to table.");
				AgentUtils.writeFiledEventToFile(jsonEvent, logger);
			}
		}
	}

	@Override
	public void cleanup() {
		GoogleBQUtil.cleanup();
	}
/*
	private Object getValue(JSONObject jsonEvent, String key) {
		key = key.trim();
		String value = null;

		JSONObject eventAttr = jsonEvent.getJSONObject(AgentConstants.JSON_ATTRIBUTES);

		// Check if the key is there in the event attribute
		value = getStringIgnoreCase(eventAttr, key);

		// if key is not there in the attribute, then check at the parent level.
		if (value != null) {
			return value;
		} else {
			value = getStringIgnoreCase(jsonEvent, key);
		}

		if (value != null)
			return value;

		// if not there anywhere, then is that a spacial key?
		if (key.equalsIgnoreCase("event_json")) {
			return jsonEvent.toString();
		}

		return null;
	}

	private String getStringIgnoreCase(JSONObject jobj, String key) {
		Iterator<String> iter = jobj.keySet().iterator();
		while (iter.hasNext()) {
			String objKey = iter.next();
			if (objKey.equalsIgnoreCase(key)) {
				return jobj.getString(objKey);
			}
		}
		return null;
	}
	*/
}
