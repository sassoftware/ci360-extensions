/* Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.sas.esp.ci360adapter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
/* Standard Java imports */
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.Level;
/* These import files are needed for all subscribing code. */
import com.sas.esp.api.pubsub.dfESPclient;
import com.sas.esp.api.pubsub.dfESPclientHandler;

public class subscribe_client {
	/**
	 * @param args
	 */
	/* This is a subscribe client example for using the ESP pub/sub API.  One could test this
	   against the subscribeServer example provided in the ESP server distributions 
	   This is actually a generic subscribe tool to subscribe to any window's events for any
	   instance of an ESP application, which could also be a server.
	*/
	public static void main(String[] args) throws IOException {
	    /* Check command line arguments. */
		if (args.length != 1) {
			System.err.println("Usage: subscribe_client <config.txt>");
	        System.exit(1);
		}
		
		/* Read configuration file. */
		String config_file = args[0];
		File f = new File(config_file);
		
		if(f.exists() && !f.isDirectory()) { 
			System.out.println("Starting ESP - CI360 adapter using config file: " + config_file );
		}
		else {
			System.out.println("Trying to start adapter, but config file " + config_file + " does not exist!");
			System.exit(1); 
		}
		
		Properties config = readConfig(config_file);
		String espProject    = config.getProperty("esp.Project");
		String espQuery      = config.getProperty("esp.Query");
		String espWindow     = config.getProperty("esp.Window");
		String urlParameters = config.getProperty("esp.urlParameters");
		String espServer     = "dfESP://"+config.getProperty("esp.host") +":"+ config.getProperty("esp.port");
		String espWindowUrl  = espProject + "/" + espQuery + "/" + espWindow;
		String espUrl        = espServer + "/" + espWindowUrl + urlParameters;
		System.out.println("espUrl: " + espUrl);
		
	    /*  Initialize subscribing capabilities.  This is the first pub/sub API call that must
        be made, and it only needs to be called once. The parameter is the log level,
        so we are turning logging off for this example.
        */
		dfESPclientHandler handler = new dfESPclientHandler();
		if (!handler.init(Level.WARNING)) {
			System.err.println("init() failed");
			System.exit(1);
		}
	    /* Get the schema and write it to System.err.
	       The URL is as follows:
	           dfESP://host:port/project/contquery/window?get=schema
	    */
		String schemaUrl = espUrl.substring(0, espUrl.indexOf("?snapshot=")) + "?get=schema";
		System.out.println("schemaUrl: " + schemaUrl);
		ArrayList<String> schemaVector = handler.queryMeta(schemaUrl);
		if (schemaVector == null) {
			System.err.println("Schema query (" + schemaUrl + ") failed");
			System.exit(1);
		}
		System.err.println(schemaVector.get(0));
		
	    /* Start this subscribe session.  This validates subscribe connection parameters, 
	       but does not make the actual connection.
	       The parameters for this call are URL, user defined subscriber callback function,
	       an optional user defined subscribe services error callback function, and an
	       optional user defined context pointer (NULL in this case).
	       The URL is as follows 
	       dfESP://host:port/project/contquery/window?snapshot=true/false
	       When a new window event arrives, the callback function is invoked to process it.
		*/
		
		callbacks myCallbacks = new callbacks(config);
		dfESPclient client = handler.subscriberStart(espUrl, myCallbacks, 0);
		if (client == null) {
			System.err.println("subscriberStart(" + espUrl + ", myCallbacks) failed");
			System.exit(1);
		}
	    /* Now make the actual connection to the ESP application or server. */
		if (!handler.connect(client)) {
			System.err.println("connect() failed");
			System.exit(1);
		}
	    /* Create a mostly non-busy wait loop. */
		while (myCallbacks.getNonBusyWait()) {
			try {
				Thread.sleep(1000);    //sleep for 1000 ms
			} catch(InterruptedException ie){
				// just continue sleeping
			}
		}
	    /* Stop pubsub, but block (i.e., true) to ensure that all queued events are first processed. */
		handler.stop(client, true);
	}
	
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
	
}
