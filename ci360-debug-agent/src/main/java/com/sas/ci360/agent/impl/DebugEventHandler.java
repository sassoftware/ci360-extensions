/* Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.sas.ci360.agent.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.dbcp2.BasicDataSource;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sas.ci360.agent.EventHandler;
import com.sas.ci360.agent.PostEventCallback;
import com.sas.ci360.agent.exceptions.ConfigurationException;
import com.sas.ci360.agent.exceptions.EventHandlerException;
import com.sas.ci360.agent.util.AgentUtils;
import com.sas.ci360.agent.util.HttpUtils;

public class DebugEventHandler implements EventHandler {
	private static final Logger logger = LoggerFactory.getLogger(DebugEventHandler.class);
		
	private static final String CONTENT_TYPE_JSON = "application/json";
	private static final String JSON_ATTRIBUTES = "attributes";
	private static final String JSON_TIMESTAMP = "timestamp";
	private static final String JSON_DATE = "date";
	
	private static final String ENCODING_UTF8 = "UTF-8";
	
	private boolean callElastic;
	private String elasticUrl;
	
	private BasicDataSource ds = new BasicDataSource();
	private List<String> columns = new ArrayList<String>();
	private String sqlEventInsert;
	private String sqlSelectAll;
	private String eventTable;
	private boolean writeToDb;
	

	@Override
	public void initialize() {
		// default properties
		ds.setMinIdle(5);
		ds.setMaxIdle(10);
		ds.setMaxOpenPreparedStatements(100);
	}

	@Override
	public void initialize(Properties config) throws ConfigurationException {	
		initialize();
		
		this.callElastic = config.getProperty("elastic.callElastic") != null && config.getProperty("elastic.callElastic").equalsIgnoreCase("true");

		if (callElastic) {
			if (config.getProperty("elastic.host") == null) throw new ConfigurationException("Missing required configuration property elastic.host");
			if (config.getProperty("elastic.port") == null) throw new ConfigurationException("Missing required configuration property elastic.port");
			if (config.getProperty("elastic.index") == null) throw new ConfigurationException("Missing required configuration property elastic.index");
			
			this.elasticUrl = config.getProperty("elastic.host") + ":" + config.getProperty("elastic.port") + "/"
					+ config.getProperty("elastic.index") + "/_doc";
			logger.info("Send Events to Elasticsearch using url: " + elasticUrl);
			
		}
		
		this.writeToDb = config.getProperty("db.writeToDb") != null && config.getProperty("db.writeToDb").equalsIgnoreCase("true");
		
		if (writeToDb) {
			if (config.getProperty("db.url") == null) throw new ConfigurationException("Missing required configuration property db.url");
			if (config.getProperty("db.user") == null) throw new ConfigurationException("Missing required configuration property db.user");
			if (config.getProperty("db.password") == null) throw new ConfigurationException("Missing required configuration property db.password");
			if (config.getProperty("db.eventTable") == null) throw new ConfigurationException("Missing required configuration property db.eventTable");
			
			logger.info("Setting DB connection: url:" + config.getProperty("db.url"));
			ds.setUrl(config.getProperty("db.url"));
			ds.setUsername(config.getProperty("db.user"));
			ds.setPassword(config.getProperty("db.password"));
			if (config.getProperty("db.minIdle") != null) ds.setMinIdle(Integer.parseInt(config.getProperty("db.minIdle")));
			if (config.getProperty("db.maxIdle") != null) ds.setMaxIdle(Integer.parseInt(config.getProperty("db.maxIdle")));
			if (config.getProperty("db.maxOpenPreparedStatements") != null) ds.setMaxOpenPreparedStatements(Integer.parseInt(config.getProperty("db.maxOpenPreparedStatements")));
			
			logger.info("Writting event data to table: " + config.getProperty("db.eventTable"));
			this.eventTable = config.getProperty("db.eventTable");
		    this.sqlSelectAll = "SELECT * FROM " + this.eventTable;
		    logger.debug("Select SQL: " + this.sqlSelectAll);
			
			try {
				getMetadata();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public void registerCallback(PostEventCallback callback) {
		
	}
	
	@Override
	public void processEvent(JSONObject jsonEvent) throws EventHandlerException {
		logger.trace("Event: " + jsonEvent.toString());
		
		if (callElastic) {
			callElastic(jsonEvent);
		}
		
		if (writeToDb) {
			writeToDb(jsonEvent);
		}
	}

	private void callElastic(JSONObject jsonEvent) throws EventHandlerException {
		// prepare JSON object
		JSONObject newJsonEvent = new JSONObject();

		JSONObject eventAttr = jsonEvent.getJSONObject(JSON_ATTRIBUTES);
		Iterator<String> keys = eventAttr.keys();
		while(keys.hasNext()) {
			String key = keys.next();
			String value = eventAttr.getString(key);

			if (key.indexOf(".") != -1) {	
				//removing the dot (.) in json property names			
				newJsonEvent.put(key.replace(".","_"), value );
			} else {
				newJsonEvent.put(key, value);
			}
		}

		// adding a date property
		newJsonEvent.put(JSON_DATE, AgentUtils.formatTimestamp(eventAttr.getString(JSON_TIMESTAMP)));		
		
		// call Elastic
        String httpResponse;
		try {
			httpResponse = HttpUtils.httpPost(this.elasticUrl, newJsonEvent.toString(), CONTENT_TYPE_JSON, ENCODING_UTF8);
			logger.debug("Elastic response: " + httpResponse);
		} catch (Exception ex) {
			throw new EventHandlerException("Elastic API call error", EventHandlerException.IS_RETRYABLE, ex);
		}
	}
	
	
	
	
	private void writeToDb(JSONObject jsonEvent) throws EventHandlerException {
		JSONObject eventAttr = jsonEvent.getJSONObject("attributes");
		
		if (logger.isTraceEnabled()) logger.trace("Getting connection");
		try (Connection con = getConnection(); 
				PreparedStatement stmt = con.prepareStatement(sqlEventInsert);) {
			if (logger.isTraceEnabled()) logger.trace("Prepare statement");
			for (int i=0; i < this.columns.size(); i++) {
				stmt.setString(i+1, getStringIgnoreCase(eventAttr, this.columns.get(i)));
			}

			if (logger.isTraceEnabled()) logger.trace("Execute statement");
			try {
				stmt.executeUpdate();
			}
			catch (SQLException ex) {
				throw new EventHandlerException("Error while writing data: SQL State: " + ex.getSQLState() + " " + ex.getMessage());
			}
			finally {
				stmt.close();
			}
		}
		catch (SQLException ex) {
			throw new EventHandlerException("Connection or statement error: SQL State: " + ex.getSQLState() + " " + ex.getMessage());
		}
		
	}

	private Connection getConnection() throws SQLException {
		return ds.getConnection();
	}
	
	private void getMetadata() throws SQLException {
		Connection con = getConnection();
		Statement stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery(this.sqlSelectAll);

		ResultSetMetaData rsMetaData = rs.getMetaData();
		int numberOfColumns = rsMetaData.getColumnCount();

		List<String> values = new ArrayList<String>();

		// get the column names; column indexes start from 1
		for (int i = 1; i < numberOfColumns + 1; i++) {
			String columnName = rsMetaData.getColumnName(i);
			// String tableName = rsMetaData.getTableName(i);
			int columnType = rsMetaData.getColumnType(i);
			logger.debug("Column: " + columnName + ", type: " + columnType);
			this.columns.add(columnName);
			values.add("?");
		}

		sqlEventInsert = "INSERT INTO " + this.eventTable + " (" + String.join(",", this.columns) + ") VALUES (" + String.join(",", values) + ")";
		logger.debug("Insert SQL: " + sqlEventInsert);

		rs.close();
		stmt.close();
	}
	
	private Object getIgnoreCase(JSONObject jobj, String key) {
	    Iterator<String> iter = jobj.keySet().iterator();
	    while (iter.hasNext()) {
	        String objKey = iter.next();
	        if (objKey.equalsIgnoreCase(key)) {
	            return jobj.get(objKey);
	        }
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
}
