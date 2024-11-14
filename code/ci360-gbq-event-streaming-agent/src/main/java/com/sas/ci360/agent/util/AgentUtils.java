/* Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.sas.ci360.agent.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;

public class AgentUtils {
	public static int Event_Error_Count = 0;
	public static String failedEventFileLocation;
	public static int MAX_RETRY_COUNT = 3;
	public static int MAX_ERROR_COUNT = 2500;
	public static List<String> columns = new ArrayList<String>();
	public static List<String> date_time_convert_columns = new ArrayList<String>();

	public static Properties readConfig(String fileName) {
		Properties prop = new Properties();
		InputStream is = null;
		try {
			is = new FileInputStream(fileName);
		} catch (FileNotFoundException ex) {
			ex.printStackTrace();
		}
		try {
			prop.load(is);
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return prop;
	}

	public static String getFormattedLogPrefix(JSONObject eventJson) {
		// try {
		// JSONObject eventAttr =
		// eventJson.getJSONObject(AgentConstants.JSON_ATTRIBUTES);
		String eventName;
		try {
			eventName = getValue(eventJson, AgentConstants.JSON_EVENTNAME).toString();

			String id = getValue(eventJson, AgentConstants.EVENT_JSON_IDENTIFIER).toString();
			// String id = eventAttr.getString(AgentConstants.EVENT_JSON_IDENTIFIER);
			return "$$(EventName=" + eventName + "; " + AgentConstants.EVENT_JSON_IDENTIFIER + "=" + id + ")$$ ";
		} catch (Exception e) {
			return e.toString();
		}
	}

	public static void logEvent(JSONObject eventAttr, Logger logger) {
		String timestamp = eventAttr.getString("timestamp");
		String event_channel = eventAttr.getString("event_channel");
		String eventname = eventAttr.getString("eventname");
		String eventtype = eventAttr.getString("event");

		Timestamp ts = new Timestamp(Long.parseLong(timestamp));
		String time = ts.toString();
		if (time.length() == 22) {
			time = ts.toString() + "0";
		} else if (ts.toString().length() == 21) {
			time = ts.toString() + "00";
		}

		String content = time + ": " + event_channel + " - " + eventtype + " (" + eventname + "): "
				+ eventAttr.toString();
		logger.debug(content);
	}

	public static void printEvent(JSONObject eventAttr, boolean outputToFile, boolean outputToConsole,
			String outputFile) {
		String timestamp = eventAttr.getString("timestamp");
		String event_channel = eventAttr.getString("event_channel");
		String eventname = eventAttr.getString("eventname");
		String eventtype = eventAttr.getString("event");

		Timestamp ts = new Timestamp(Long.parseLong(timestamp));
		String time = ts.toString();
		if (time.length() == 22) {
			time = ts.toString() + "0";
		} else if (ts.toString().length() == 21) {
			time = ts.toString() + "00";
		}

		String content = time + ": " + event_channel + " - " + eventtype + " (" + eventname + ")";
		if (outputToFile) {
			writeToFile(outputFile, content, true);
		}
		if (outputToConsole) {
			System.out.println(content);
		}

	}

	public static void writeToFile(String filename, String content, boolean append) {
		try (FileWriter fw = new FileWriter(filename, append)) {
			fw.write(content + "\n");
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public static void writeToFile(String filename, String content) {
		writeToFile(filename, content, true);
	}

	public static void writeFiledEventToFile(JSONObject event, Logger logger) {
		try {
			logger.debug("Trying to write the failed event json to a file.");
			if (event != null) {
				String fileName = createFileName(event);
				File file = new File(failedEventFileLocation + fileName);
				JSONArray content = getSelectedColumns(event);
				if (!file.exists()) {
					writeToFile(Paths.get(failedEventFileLocation, fileName).toString(), content.toString());
					logger.info("Failed event is written to file, file name: " + fileName);
				} else {
					// This might happen when CI360 sends the same event to agent more than ones.
					// Because the first time agent might not have acknowledged it.
					logger.debug("JSON file is already written.");
				}
			}
		} catch (Exception ex) {
			logger.error("Unable to write the failed event to file. Writing to log instead.\n" + event.toString(), ex);
		}

	}

	public static void writeFiledEventToFile(JSONArray data, Logger logger) {
		try {
			logger.debug("Trying to write the failed event json array to a file.");
			if (data != null && data.length() > 0) {
				String fileName = createFileName(data.getJSONObject(0));
				File file = new File(failedEventFileLocation + fileName);
				if (!file.exists()) {
					writeToFile(Paths.get(failedEventFileLocation, fileName).toString(), data.toString());
					logger.info("Failed event is written to file, file name: " + fileName);
				}
			} else {

			}
		} catch (Exception ex) {
			logger.error("Unable to write the failed event to file. Writing to log instead.\n" + data.toString(), ex);
		}
	}

	public static String createFileName(JSONObject event) {
		String fileName = "";
		try {
			Object rowKey = getValue(event, AgentConstants.EVENT_JSON_IDENTIFIER);
			if (null != rowKey) {
				fileName = rowKey.toString();
			}
		} catch (Exception e) {
		}

		if (fileName == null || fileName.length() < 1) {
			UUID uuid = UUID.randomUUID();
			fileName = "uuid-" + uuid.toString();
		}

		fileName = fileName + ".json";
		return fileName;
	}

	public static JSONArray getSelectedColumns(JSONObject jsonEvent) throws Exception {
		JSONArray jsonArr = new JSONArray();
		JSONObject record = new JSONObject();
		for (int i = 0; i < columns.size(); i++) {
			String col = columns.get(i);
			col = col.trim();
			Object val = getValue(jsonEvent, col);
			record.put(col, val);
		}
		jsonArr.put(record);
		return jsonArr;
	}

	public static Object getIgnoreCase(JSONObject jobj, String key) {
		Iterator<String> iter = jobj.keySet().iterator();
		while (iter.hasNext()) {
			String key1 = iter.next();
			if (key1.equalsIgnoreCase(key)) {
				return jobj.get(key1);
			}
		}
		return -999;
	}

	public static String formatTimestamp(String timestamp) {
		Timestamp ts = new Timestamp(Long.parseLong(timestamp));
		String time = ts.toString();

		if (time.length() == 22) {
			time = ts.toString() + "0";
		} else if (ts.toString().length() == 21) {
			time = ts.toString() + "00";
		}

		return time;
	}

	public static Object getValue(JSONObject jsonEvent, String key) throws Exception {
		key = key.trim();
		String value = null;

		JSONObject eventAttr = jsonEvent.getJSONObject(AgentConstants.JSON_ATTRIBUTES);

		// Check if the key is there in the event attribute
		value = getStringIgnoreCase(eventAttr, key);

		// if key is not there in the attribute, then check at the parent level.
		if (value != null) {
			return getDateifDateCol(key, value);
		} else {
			value = getStringIgnoreCase(jsonEvent, key);
		}

		if (value != null) {
			return getDateifDateCol(key, value);
		}

		// if not there anywhere, then is that a spacial key?
		if (key.equalsIgnoreCase("event_json")) {
			return jsonEvent.toString();
		}

		return null;
	}

	public static String getDateifDateCol(String key, String value) throws Exception {
		// check if the field is a timestamp field or not
		if (null != AgentUtils.date_time_convert_columns && AgentUtils.date_time_convert_columns.size() > 0) {
			for (String col : AgentUtils.date_time_convert_columns) {
				if (key.equalsIgnoreCase(col.trim().toLowerCase())) {
					value = getCurrentEpochTimeStamp(value);
					return value;
				}
			}

		}
		return value;
	}

	public static String getCurrentEpochTimeStamp(String timeStamp) throws Exception {
		return Instant.ofEpochMilli(Long.parseLong(timeStamp)).atOffset(ZoneOffset.UTC).toString();
	}

	public static String getStringIgnoreCase(JSONObject jobj, String key) {
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
