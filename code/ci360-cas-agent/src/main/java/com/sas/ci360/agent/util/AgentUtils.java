package com.sas.ci360.agent.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Properties;

import org.json.JSONObject;
import org.slf4j.Logger;

public class AgentUtils {
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
		
		String content = time +": " + event_channel + " - " + eventtype + " (" + eventname + "): " + eventAttr.toString();
		logger.debug(content);
	}
	
	public static void printEvent(JSONObject eventAttr, boolean outputToFile, boolean outputToConsole, String outputFile) {
		String timestamp = eventAttr.getString("timestamp");
		String event_channel = eventAttr.getString("event_channel");
		String eventname = eventAttr.getString("eventname");
		String eventtype = eventAttr.getString("event");
		
		Timestamp ts=new Timestamp(Long.parseLong(timestamp)); 
		String time = ts.toString();
		if (time.length() == 22) {
			time = ts.toString() + "0";
		} else if (ts.toString().length() == 21) {
			time = ts.toString() + "00";
		}
		
		String content = time +": " + event_channel + " - " + eventtype + " (" + eventname + ")";
		if (outputToFile) {
			writeToFile(outputFile, content, true);
		} 
		if (outputToConsole) {
			System.out.println(content);
		}
		
	}
	
	public static void writeToFile(String filename, String content, boolean append) {
		try (FileWriter fw = new FileWriter(filename, append)) {
			fw.write(content+"\n");
		} catch (Exception e) {
			System.out.println(e);
		}
	}
	
	public static void writeToFile(String filename, String content) {
		writeToFile(filename, content, true);
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
}
