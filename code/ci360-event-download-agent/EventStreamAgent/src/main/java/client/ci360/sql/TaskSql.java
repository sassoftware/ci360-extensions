/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package client.ci360.sql;

/**
 *
 * @author sas
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import client.Timer;
import client.ci360.tasks.Source;
import client.ci360.tasks.Task;
/*
Modification History
01/16/2023:  Raja M.
			Add separate user/pw parameters in Agent config to connect to Driver Manager with more
			 standard call
*/
public class TaskSql {
	private static final Logger logger = (Logger) LoggerFactory.getLogger(TaskSql.class);
	private static final Object lock = new Object();
	private static String connectionName = "taskSql";
	private static String jdbcUser;
	private static String jdbcPw;
	private static String jdbcUrl;
	private static String taskTable;
	private static String customPropertiesTable;
	private static String taskByVersionQuery;
	private static String propertiesByVersionQuery;
	private static String taskByIdQuery;
	private static String propertiesByIdQuery;

	private static Connection jdbc = null;

	private static PreparedStatement taskByVersionStatement;
	private static PreparedStatement propertiesByVersionStatement;
	private static PreparedStatement taskByIdStatement;
	private static PreparedStatement propertiesByIdStatement;
	private static int versionIdLength;
	private static int taskIdLength;

	public static void init(ResourceBundle config) throws Exception {
		jdbcUser = config.getString("task.sql.user");
		jdbcPw = config.getString("task.sql.pw");
		jdbcUrl = config.getString("task.sql.jdbcURL");
		taskTable = config.getString("task.sql.taskTable");
		customPropertiesTable = config.getString("task.sql.customPropertiesTable");

		taskByVersionQuery = String.format(
				"select TASK_VERSION_ID, TASK_ID, TASK_NM, CHANNEL_NM, TASK_DELIVERY_TYPE_NM, TASK_STATUS_CD, LAST_PUBLISHED_DTTM"
						+ " from %s where TASK_VERSION_ID=?",
				taskTable);
		propertiesByVersionQuery = String.format("select PROPERTY_NM, PROPERTY_VAL from %s where TASK_VERSION_ID=?",
				customPropertiesTable);
		taskByIdQuery = String.format(
				"select TASK_VERSION_ID, TASK_ID, TASK_NM, CHANNEL_NM, TASK_DELIVERY_TYPE_NM, TASK_STATUS_CD, LAST_PUBLISHED_DTTM"
						+ " from %s where TASK_STATUS_CD='active' and TASK_ID=?",
				taskTable);
		propertiesByIdQuery = String.format(
				"select PROPERTY_NM, PROPERTY_VAL from %s where TASK_STATUS_CD='active' and TASK_ID=?",
				customPropertiesTable);
		logger.info("taskByVersionQuery: {}", taskByVersionQuery);
		logger.info("propertiesByVersionQuery: {}", propertiesByVersionQuery);
		logger.info("taskByIdQuery: {}", taskByIdQuery);
		logger.info("propertiesByIdQuery: {}", propertiesByIdQuery);

		checkConnection();

		versionIdLength = taskByVersionStatement.getParameterMetaData().getPrecision(1);
		taskIdLength = taskByIdStatement.getParameterMetaData().getPrecision(1);
	}

	public static void stop() {
		synchronized (lock) {
			disconnect();
		}
	}

	public static Task getTaskByVersion(String versionId) throws Exception {
		Timer timer = new Timer();
		Task task = getTaskByKey(Source.UDM_BY_VERSION, versionId, taskByVersionStatement, "TASK_VERSION_ID",
				versionIdLength);
		if (task == null)
			throw new Exception(String.format("Failed reading task version %s from %s. Duration: %s", versionId,
					taskTable, timer.msElapsed()));
		logger.debug("Read task version {} from {}. Duration: {}", versionId, taskTable, timer.msElapsed());
		task.setCustomProperties(getPropertiesByVersion(versionId));
		return task;
	}

	public static Task getTaskById(String taskId) throws Exception {
		Timer timer = new Timer();
		Task task = getTaskByKey(Source.UDM_BY_ID, taskId, taskByIdStatement, "TASK_ID", taskIdLength);
		if (task == null)
			throw new Exception(String.format("Failed reading task %s from %s. Duration: %s", taskId, taskTable,
					timer.msElapsed()));
		logger.debug("Read task {} from {}. Duration: {}", taskId, taskTable, timer.msElapsed());
		task.setCustomProperties(getPropertiesById(taskId));
		return task;
	}

	private static Map<String, String> getPropertiesByVersion(String versionId) throws Exception {
		Timer timer = new Timer();
		Map<String, String> customProperties = getPropertiesByKey(versionId, propertiesByVersionStatement,
				"TASK_VERSION_ID", versionIdLength);
		if (customProperties == null)
			throw new Exception(
					String.format("Failed reading custom properties for task version %s from %s. Duration: %s",
							customPropertiesTable, versionId, timer.msElapsed()));
		logger.debug("Read {} custom properties for task version {} from {}. Duration: {}", customProperties.size(),
				customPropertiesTable, versionId, timer.msElapsed());
		return customProperties;
	}

	public static Map<String, String> getPropertiesById(String taskId) throws Exception {
		Timer timer = new Timer();
		Map<String, String> customProperties = getPropertiesByKey(taskId, propertiesByIdStatement, "TASK_ID",
				taskIdLength);
		if (customProperties == null)
			throw new Exception(String.format("Failed reading custom properties for task %s from %s. Duration: %s",
					customPropertiesTable, taskId, timer.msElapsed()));
		logger.debug("Read {} custom properties for task {} from {}. Duration: {}", customProperties.size(),
				customPropertiesTable, taskId, timer.msElapsed());
		return customProperties;
	}

	private static void validateKeyLength(String key, String table, String column, int maxLength) throws Exception {
		if (key == null)
			throw new Exception(String.format("Query failed on %s: %s is null", table, column));
		if (key.length() > maxLength)
			throw new Exception(
					String.format("Query failed on %s: %s '%s' length exceeds %d", table, column, key, maxLength));
	}

	private static Map<String, String> getPropertiesByKey(String key, PreparedStatement statement, String column,
			int maxLength) throws Exception {
		Map<String, String> customProperties = new HashMap<String, String>();
		validateKeyLength(key, customPropertiesTable, column, maxLength);
		boolean failed = true;
		synchronized (lock) {
			checkConnection();
			ResultSet rs = null;
			try {
				statement.clearParameters();
				statement.setString(1, key);
				statement.execute();
				rs = statement.getResultSet();
				if (rs != null)
					while (rs.next())
						customProperties.put(rs.getString(1), rs.getString(2));
				failed = false;
			} catch (SQLException e) {
				logger.error(String.format("SQLException on statement '%s'", taskByIdQuery), e);
			} finally {
				try {
					rs.close();
				} catch (Exception e) {
				}
			}
		}
		if (failed)
			return null;
		return customProperties;
	}

	private static Task getTaskByKey(Source source, String key, PreparedStatement statement, String column,
			int maxLength) throws Exception {
		validateKeyLength(key, taskTable, column, maxLength);
		Task task = null;
		synchronized (lock) {
			checkConnection();
			ResultSet rs = null;
			try {
				statement.clearParameters();
				statement.setString(1, key);
				statement.execute();
				rs = statement.getResultSet();
				if (rs != null)
					if (rs.next())
						task = new Task(source, rs);
			} catch (SQLException e) {
				logger.error(String.format("SQLException on statement '%s'", taskByIdQuery), e);
			} finally {
				try {
					rs.close();
				} catch (Exception e) {
				}
			}
		}
		return task;
	}

	private static void checkConnection() throws SQLException {
		if (jdbc != null)
			if (!jdbc.isValid(10)) {
				disconnect();
				jdbc = null;
				taskByVersionStatement = null;
				propertiesByVersionStatement = null;
				taskByIdStatement = null;
				propertiesByIdStatement = null;
			}
		if (jdbc == null || taskByIdStatement == null || propertiesByIdStatement == null
				|| taskByVersionStatement == null || propertiesByVersionStatement == null) {
			jdbc = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPw);
			logger.info("{} connected to {}.", connectionName, jdbcUrl);
			taskByVersionStatement = jdbc.prepareStatement(taskByVersionQuery);
			propertiesByVersionStatement = jdbc.prepareStatement(propertiesByVersionQuery);
			taskByIdStatement = jdbc.prepareStatement(taskByIdQuery);
			propertiesByIdStatement = jdbc.prepareStatement(propertiesByIdQuery);
		}
	}

	private static void disconnect() {
		if (jdbc == null)
			return;
		try {
			jdbc.close();
			logger.info("{} connection closed.", connectionName);
		} catch (SQLException e) {
			logger.error("{} failed", connectionName, e);
		}
	}
}

