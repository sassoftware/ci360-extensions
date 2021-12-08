package com.sas.ci360.agent.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sas.cas.CASActionResults;
import com.sas.cas.CASAuthenticatedUserInfo;
import com.sas.cas.CASClient;
import com.sas.cas.CASClientInterface;
import com.sas.cas.CASException;
import com.sas.cas.CASTable;
import com.sas.cas.CASValue;
import com.sas.cas.CASTable.OutputType;
import com.sas.cas.actions.sessionProp.SetSessOptOptions;
import com.sas.cas.actions.table.AddTableOptions;
import com.sas.cas.actions.table.Addtablevariable;
import com.sas.cas.actions.table.Addtablevariable.RTYPE;
import com.sas.cas.actions.table.Addtablevariable.TYPE;
import com.sas.cas.actions.table.TableInfoOptions;
import com.sas.cas.events.CASAuthenticatedUserEventListener;
import com.sas.cas.events.CASMessageTagEvent;
import com.sas.cas.events.CASMessageTagHandler;
import com.sas.cas.io.CASBaseTable;
import com.sas.cas.io.CASDataAppender;
import com.sas.cas.messages.CASMessageHeader;
import com.sas.ci360.agent.EventHandler;
import com.sas.ci360.agent.PostEventCallback;
import com.sas.ci360.agent.cas.TableColumn;
import com.sas.ci360.agent.exceptions.ConfigurationException;
import com.sas.ci360.agent.exceptions.EventHandlerException;
import com.sas.commons.crypto.SealedString;

public class CASEventHandler implements EventHandler {
	private static final Logger logger = LoggerFactory.getLogger(CASEventHandler.class);
	private static final Logger loggerBatch = LoggerFactory.getLogger("CASEventHandler.batchWriteProcess");
		
	private static final String DATATYPE_VARCHAR = "VARCHAR";
	private static final String DATATYPE_INT32 = "INT32";
	private static final String DATATYPE_INT64 = "INT64";
	private static final String DATATYPE_DOUBLE = "DOUBLE";
	
	private static final String TBL_ATTR_NAME = "name";
	private static final String TBL_ATTR_TYPE = "type";
	
	private static final String JSON_ATTRIBUTES = "attributes";
	private static final String JSON_EVENTNAME = "eventName";
	
	private CASClientInterface client;
	private boolean useCaslib = false;
	private AddTableOptions options = new AddTableOptions();

	private String eventTable;
	private String host;
	private String port;
	private String username;
	private String password;
	private String caslib;
	private boolean createTable;
	private TableColumn[] tableCols; 
	private int commitRowCount;
	private int maxBatchSize;
	
	private BlockingQueue<JSONObject> eventQueue = new LinkedBlockingQueue<JSONObject>();

	@Override
	public void initialize() {
		// default properties
		
	}

	@Override
	public void initialize(Properties config) throws ConfigurationException {	
		initialize();
		
		this.eventTable = config.getProperty("cas.tableName");
		if (this.eventTable == null) throw new ConfigurationException("Missing required configuration property cas.tableName");
		this.host = config.getProperty("cas.host");
		if (this.host == null) throw new ConfigurationException("Missing required configuration property cas.host");
		this.port = config.getProperty("cas.port");
		logger.info("CAS host: {} port: {}", this.host, this.port);
		this.username = config.getProperty("cas.username");
		this.password = config.getProperty("cas.password");
		this.caslib = config.getProperty("cas.caslib", "");
		if (!this.caslib.isEmpty()) {
			this.useCaslib = true;
		}
		logger.info("CAS table: {} caslib: {}", this.eventTable, this.caslib);
		this.createTable = "true".equalsIgnoreCase(config.getProperty("cas.createTable", "false"));
		String columns = config.getProperty("cas.tableColumns");
		if (columns == null) throw new ConfigurationException("Missing required configuration property cas.tableColumns");
		this.commitRowCount = Integer.parseInt(config.getProperty("cas.commitRowCount", "0"));
		this.maxBatchSize = Integer.parseInt(config.getProperty("cas.maxBatchSize", "0"));
		
		logger.info("Creating CAS Client connection");
		try {			
			// Create our connection
			createTarget();
		}
		catch (Exception ex) {
			logger.error("Exception caught: " + ex.getClass().getName() + ", error: " + ex.getMessage());
		}
		
		
		// Initialize CAS Table structure
		options = new AddTableOptions();
		options.setTable(eventTable);
		
		// Variables
		logger.info("Defining table variables");
		List<Addtablevariable> tableVars = new ArrayList<Addtablevariable>();
		int offset = 0;
		
		ArrayList<TableColumn> colList = new ArrayList<TableColumn>();
		JSONArray jsonCols = new JSONArray(columns);
		
		// iterate over configured columns and build CAS table variables list
		for (int i=0; i < jsonCols.length(); i++) {
			JSONObject col = jsonCols.getJSONObject(i);

			Addtablevariable tableVar = new Addtablevariable();
			tableVar.setName(col.getString(TBL_ATTR_NAME));
			
			if (col.getString(TBL_ATTR_TYPE).equals(DATATYPE_VARCHAR)) {
				tableVar.setLength(16);
				tableVar.setOffset(offset);
				tableVar.setRType(RTYPE.CHAR);
				tableVar.setType(TYPE.VARCHAR);
			}
			
			if (col.getString(TBL_ATTR_TYPE).equals(DATATYPE_INT32)) {
				tableVar.setLength(4);
				tableVar.setOffset(offset);
				tableVar.setRType(RTYPE.NUMERIC);
				tableVar.setType(TYPE.INT32);
			}

			if (col.getString(TBL_ATTR_TYPE).equals(DATATYPE_INT64)) {
				tableVar.setLength(8);
				tableVar.setOffset(offset);
				tableVar.setRType(RTYPE.NUMERIC);
				tableVar.setType(TYPE.INT64);
			}

			if (col.getString(TBL_ATTR_TYPE).equals(DATATYPE_DOUBLE)) {
				tableVar.setLength(8);
				tableVar.setOffset(offset);
				tableVar.setRType(RTYPE.NUMERIC);
				tableVar.setType(TYPE.SAS);
			}
			
			tableVars.add(tableVar);
			offset += tableVar.getLength();
						
			colList.add(new TableColumn(col.getString(TBL_ATTR_NAME), tableVar.getType()));
		}
		tableCols = colList.toArray(new TableColumn[colList.size()]);
		
		options.setVars(tableVars.toArray(new Addtablevariable[tableVars.size()]));

		// Set the record length
		options.setRecLen(offset);
				
		// check if table exists
		logger.info("Checking if CAS table (" + eventTable + ") exists");
		if (!tableExists()) {
			if (createTable) {
				createTable();
			}
			else {
				throw new ConfigurationException("CAS table does not exist");
			}
		}
		
		// set append options
		logger.info("Setting append options");
		options.setAppend(true);
		options.setPromote(false);
	}
	
	@Override
	public void registerCallback(PostEventCallback callback) {
		
	}
	
	@Override
	public void processEvent(final JSONObject jsonEvent) throws EventHandlerException {
		logger.trace("Event: {}", jsonEvent.toString());
		
		JSONObject eventAttr = jsonEvent.getJSONObject(JSON_ATTRIBUTES);
		logger.debug("Event name: {}", eventAttr.getString(JSON_EVENTNAME));
		
		// only store required fields (reduce memory bloat)
		JSONObject storedEvent = new JSONObject();
		for (int c = 0; c < tableCols.length; c++) {
			try {
				if (tableCols[c].getType() == TYPE.VARCHAR) {
					storedEvent.put( tableCols[c].getName(), eventAttr.getString(tableCols[c].getName()) );
				}
				if (tableCols[c].getType() == TYPE.INT32) {
					storedEvent.put( tableCols[c].getName(), eventAttr.getInt(tableCols[c].getName()) );
				}
				if (tableCols[c].getType() == TYPE.INT64) {
					storedEvent.put( tableCols[c].getName(), eventAttr.getLong(tableCols[c].getName()) );
				}
				if (tableCols[c].getType() == TYPE.SAS) {
					storedEvent.put( tableCols[c].getName(), eventAttr.getDouble(tableCols[c].getName()) );
				}
			}
			catch (JSONException ex) {
				logger.trace("JSON field not found: {}", tableCols[c].getName());
				// attribute not found, do nothing
			}
		}
		
		try {
			eventQueue.put(storedEvent);
		} catch (InterruptedException e) {
			logger.warn("InterruptedException: {}", e.getMessage());
			e.printStackTrace();
		}

	}

	
	@Override
	public synchronized void batchProcess() {
		int queueSize = eventQueue.size();
		loggerBatch.trace("Starting batch process");
		if (queueSize == 0) return;
		loggerBatch.info("Starting batch process, writting to CAS, event queue size: {}", queueSize);
		
		// define the tag handler callback
		CASMessageTagHandler tagHandler = new CASMessageTagHandler() {
			private int bufSize = 256;
			
			@Override
			public boolean handleMessageTag(CASMessageTagEvent event) throws CASException, IOException {
				// Get the variable list
				Addtablevariable[] vars = (Addtablevariable[]) event.getOptions().get(AddTableOptions.KEY_VARS);
				if (vars == null) {
					// This shouldn't happen.
					throw new CASException("Missing " + AddTableOptions.KEY_VARS);
				}
				
				Integer reclen = (Integer) event.getOptions().get(AddTableOptions.KEY_RECLEN);
				if (reclen == null) {
					// This shouldn't happen. The server should have verified.
					throw new CASException("Missing " + AddTableOptions.KEY_RECLEN);
				}
				
				// Create our data appender
				CASDataAppender appender = new CASDataAppender(event, vars, reclen, bufSize);
				
				loggerBatch.debug("Adding {} rows, reclen={}", eventQueue.size(), reclen);
				long start = System.currentTimeMillis();
				
				int rows = 0;
				JSONObject eventAttr = eventQueue.poll();
				while (eventAttr != null && (maxBatchSize == 0 || rows < maxBatchSize)) {
					loggerBatch.trace("Adding event {}", eventAttr.getString(JSON_EVENTNAME));
					for (int c = 0; c < tableCols.length; c++) {
						try {
							switch (tableCols[c].getType()) {
								case VARCHAR:
									appender.setString(c, eventAttr.getString(tableCols[c].getName()));
									break;
								case INT32:
									appender.setInt(c, eventAttr.getInt(tableCols[c].getName()));
									break;
								case INT64:
									appender.setLong(c, eventAttr.getLong(tableCols[c].getName()));
									break;
								case SAS:
									appender.setDouble(c, eventAttr.getDouble(tableCols[c].getName()));
									break;
								default:
									break;
							}
						}
						catch (JSONException ex) {
							loggerBatch.trace("JSON field not found: {}", tableCols[c].getName());
							// attribute not found, do nothing
						}
					}
					appender.appendRecord();
					rows++;
					
					// Commit every X rows
					if (commitRowCount > 0 && rows % commitRowCount == 0) {
						loggerBatch.debug("Committing at {} rows", rows);
						appender.commit();
						loggerBatch.debug("Events remaining in queue: {}", eventQueue.size());
					}
					
					eventAttr = eventQueue.poll();
				}

				appender.close();
				
				double elapsed = System.currentTimeMillis() - start;
				double rps = rows / (elapsed / 1000);
				loggerBatch.info("Written: {} rows in {}ms, {} rows/sec", rows, elapsed, rps);
				
				// Do not propagate the response
				return false;
			}
		};
		
		loggerBatch.debug("Executing CAS Client action");
		try {
			// Set the data tag handler which will be called when data is requested from the addtable action
			options.setMessageTagHandler(CASMessageHeader.TAG_DATA, tagHandler);
			
			// Invoke the action
			@SuppressWarnings("unused")
			CASActionResults<CASValue> results = getClient().invoke(options);
		}
		catch (CASException ex) {
			loggerBatch.error("Exception caught: class: " + ex.getClass().getName() + ", error: " + ex.getMessage());
		}
		catch (IOException ex) {
			loggerBatch.error("Exception caught: class: " + ex.getClass().getName() + ", error: " + ex.getMessage());
		}
		catch (Exception ex) {
			loggerBatch.error("Exception caught: class: " + ex.getClass().getName() + ", error: " + ex.getMessage());
		}
	}
	

	private void createTarget() throws Exception {
		if (client == null) {
			client = createCASClient();
		}
	}
	
	private CASClientInterface getClient() {
		return client;
	}
	
	
	/**
	 * Creates a new CASClient instance.
	 * @return The new CASClient object
	 */
	private CASClientInterface createCASClient() throws CASException, IOException {
		// Session ID?
		String sessionID = null;
		
		final CASClient client = new CASClient();
		client.setHost(host);
		if (port != null) {
			client.setPort(Integer.parseInt(port));
		}
		client.setUserName(username);
		client.setPassword(new SealedString(password));
		//client.setSessionID(sessionID);
		
		// Nodes?
		client.setNumberOfNodes(1);
		
		if (useCaslib) {
			if (caslib != null) {
				// Set the active caslib
				SetSessOptOptions options = new SetSessOptOptions();
				options.clear();
				options.setCaslib(caslib);
				client.invoke(options);
			}
		}
		
		client.setAuthenticatedUserEventListener(new CASAuthenticatedUserEventListener() {

			@Override
			public void handleAuthenticatedUserEvent(CASAuthenticatedUserInfo userInfo) {

			}
		});

		initialize(client);
		
		CASClientInterface c = client;
		return c;
	}
	


	/**
	 * Provides an opportunity to perform CASClient specific initialization.
	 * @param client The client instance
	 */
	private void initialize(CASClient client) throws CASException, IOException {
	}
	
	
	
	private boolean tableExists() {
		// check for table on CAS server
		CASActionResults<CASValue> results = null;
		boolean tableExists = false;
		long numRows = 0;
		TableInfoOptions tableInfo = new TableInfoOptions();
		if (useCaslib) {
			if (caslib != null) {
				tableInfo.setCaslib(caslib);
			}
		}
		try {
			results = getClient().invoke(tableInfo);
		} catch (CASException ex) {
			logger.error("Exception caught: class: " + ex.getClass().getName() + ", error: " + ex.getMessage());
			//System.exit(1);
		} catch (IOException ex) {
			logger.error("Exception caught: class: " + ex.getClass().getName() + ", error: " + ex.getMessage());
			//System.exit(1);
		}
		// show adapter session id
		logger.debug("CAS Adapter Session Id: " + getClient().getSessionID());

		for (int i = 0; i < results.getResultsCount(); i++) {
			CASValue value = results.getResult(i);
			CASTable casTable = (CASTable) value.getValue();
			for (int j = 0; j < casTable.getRowCount(); j++) {
				try {
					String name = casTable.getStringAt(j, "Name");
					logger.debug("Checking CAS table: " + name);
					if (name.regionMatches(true, 0, eventTable, 0, eventTable.length())) {
						if (name.length() == eventTable.length()) {
							tableExists = true;
							numRows = (long) casTable.getDoubleAt(j, "Rows");
						} 
					}
				} catch (IOException ex) {
					logger.error("Unable to get information about CAS tables on CAS Server: getStringAt() exception: " + ex.getClass().getName() + ", error: " + ex.getMessage());
					System.exit(1);
				}
				if (tableExists) {
					break;
				}
			}
			if (tableExists) {
				break;
			}
		}
		
		logger.debug("Table found: " + tableExists);
		logger.debug("Row count: " + numRows);
		return tableExists;
	}
	
	private void createTable() throws ConfigurationException {
		logger.info("Creating table");
		
		// set create options
		options.setAppend(false);
		//options.setReplace(false); // no need to specify, default is false
		options.setPromote(true);
		
		// define the tag handler callback
		CASMessageTagHandler tagHandlerEmptyTable = new CASMessageTagHandler() {
			@Override
			public boolean handleMessageTag(CASMessageTagEvent event) throws CASException, IOException {
				// Just create the table; don't send any rows
				CASDataAppender.sendZeroRows(event);

				// Do not propagate the response
				return false;
			}
		};

		// set the tag handler callback
		options.setMessageTagHandler(CASMessageHeader.TAG_DATA, tagHandlerEmptyTable);
		
		try {
			CASActionResults<CASValue> results = getClient().invoke(options);
			printResultValues(results, true);
		} catch (CASException e) {
			logger.error("Failed to add CAS table: AddTableOptions exception: " + e.toString());
			throw new ConfigurationException("Failed to create table");
		} catch (IOException e) {
			logger.error("Failed to add CAS table: AddTableOptions() exception: " + e.toString());
			throw new ConfigurationException("Failed to create table");
		}
	}
	
	
	/**
	 * Prints any result values to stdout.
	 * @param results The results
	 * @param csv if true, tables are printed in CSV format
	 */
	private static void printResultValues(CASActionResults<CASValue> results, boolean csv) {
		Iterator<CASValue> iter = results.getResults().iterator();
		if (!iter.hasNext()) {
			return;
		}
		
		try {
			logger.debug("CAS action results:");
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintStream ps = new PrintStream(baos);
			while (iter.hasNext()) {
				CASValue value = iter.next();
				
				if ((value != null) && (value instanceof CASTable)) {
					try {
						if (csv) {
							((CASTable) value).toStream(ps, OutputType.CSV, false, 0, 8);
						}
						else {
							((CASTable) value).toStream(ps, OutputType.PRETTY, true, CASBaseTable.DEFAULT_TOSTRING_ROWCOUNT, 8);
						}
					}
					catch (Exception ex) {
						ex.printStackTrace();
					}
				}
				else {
					ps.println(value);
					//System.out.println(value);
				}
			}
			ps.close();
			logger.debug(baos.toString());
			baos.close();
		}
		catch (Exception ex) {
			logger.error("Exception caught: {}", ex.getMessage());
		}

	}
}
