/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package client.ci360.sql;

/**
 *
 * @author sas
 */

import java.net.URI;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import client.Timer;
import client.ci360.tasks.Source;

/***
 * Modification History
 * 08/05/2025  Raja M.	Some databases do not have any column "length" for strings
 *                       When a sql statement does not return string length,
 *                       assume that "all is well" and proceed with SQL
 */
class SqlConnection {
	PreparedStatement insertStatement = null;
	private int batchCount;
	private String connectionName;
	private ScheduledExecutorService submitScheduler;
	Connection jdbc;
	private SqlPool sqlPool;
	private Logger logger;

	SqlConnection(SqlPool sqlPool, String prefix, int number) throws SQLException {
		logger = (Logger) LoggerFactory.getLogger(getClass());

		this.connectionName = String.format("%sSql#%d", prefix, number);
		this.sqlPool = sqlPool;
		checkConnection();
		batchCount = 0;

		Thread submitThread = new Thread() {
			public void run() {
				submitRows();
			}
		};
		submitScheduler = Executors.newScheduledThreadPool(1);
		submitScheduler.scheduleAtFixedRate(submitThread, sqlPool.secondsBetweenSubmits, sqlPool.secondsBetweenSubmits,
				TimeUnit.SECONDS);
	}

	void checkConnection() throws SQLException {
		if (jdbc != null) {
                        logger.trace("Check connection call: {} Batch count: {}", connectionName, batchCount);
			if (!jdbc.isValid(10)) {
				disconnect();
				jdbc = null;
				insertStatement = null;
			}
                }
		if (jdbc == null || insertStatement == null) {
			jdbc = DriverManager.getConnection(sqlPool.jdbcUrl, sqlPool.jdbcUser, sqlPool.jdbcPw);
			insertStatement = jdbc.prepareStatement(sqlPool.insertSql);
			logger.debug("{} connected to {}.", connectionName, sqlPool.jdbcUrl);
		}
	}

	void release() {
		try {
			insertStatement.addBatch();
			batchCount = batchCount + 1;
			if (batchCount >= sqlPool.bufferSize)
				submitRows();
		} catch (SQLException e) {
			logger.error("{} error at release(): ", connectionName, e);
		}
                logger.trace("{} Releasing connection: ", connectionName);
		sqlPool.returnConnection(this);
	}

	private void submitRows() {
                if (batchCount > 0) {
                    logger.trace("{} submitRows() starting with Batch count: {}.", connectionName, 
                                batchCount);
                }
		synchronized (this) {
			if (batchCount == 0)
				return;
			int[] rc = new int[0];
			Timer timer = new Timer();
			try {
				rc = insertStatement.executeBatch();
				logger.info("{} submitted {} inserts. Batch count: {}. Duration: {}.", connectionName, rc.length,
						batchCount, timer.msElapsed());
			} catch (SQLException e) {
				if (e instanceof BatchUpdateException) {
					rc = ((BatchUpdateException) e).getUpdateCounts();
					logger.error("{} error submitting {} inserts. {} of these committed successfully. Duration: {}.",
							connectionName, batchCount, rc.length, timer.msElapsed());
				} else
					logger.error("{} error submitting {} inserts. Duration: {}.", connectionName, batchCount,
							timer.msElapsed());
				logger.error("{} failed", connectionName, e);
			}
			batchCount = batchCount - rc.length;
                        logger.info("{} Submitted connection: ", connectionName);
		}
	}

	void disconnect() {
                logger.trace("{} About to disconnect: ", connectionName);
		if (jdbc == null)
			return;
		submitRows();
		try {
			jdbc.close();
			logger.trace("{} connection closed.", connectionName);
		} catch (SQLException e) {
			logger.error("{} failed", connectionName, e);
		}
	}

	void setValue(int parameterIndex, String value) throws SQLException {
		int maxLength = sqlPool.columnLengths[parameterIndex - 1];
		if (value == null || value.length() <= maxLength || maxLength == 0)
			insertStatement.setString(parameterIndex, value);
		else {
			insertStatement.setString(parameterIndex, value.substring(0, maxLength));
			logger.warn("{} string value truncated. {} exceeds maximum length of {} characters. Untruncated value:'{}'",
					connectionName, sqlPool.columns[parameterIndex - 1], maxLength, value);
		}
	}

	void setValue(int parameterIndex, Source source) throws SQLException {
		if (source == null)
			insertStatement.setString(parameterIndex, null);
		else
			setValue(parameterIndex, source.toString());
	}

	void setValue(int parameterIndex, LocalDateTime dateTime) throws SQLException {
		if (dateTime == null)
			insertStatement.setTimestamp(parameterIndex, null);
		else
			insertStatement.setTimestamp(parameterIndex, Timestamp.valueOf(dateTime));
	}

	void setValue(int parameterIndex, Number value) throws SQLException {
		insertStatement.setObject(parameterIndex, value);
	}

	void setValue(int parameterIndex, URI value) throws SQLException {
		if (value == null)
			insertStatement.setObject(parameterIndex, null);
		else
			setValue(parameterIndex, value.toString());
	}
}

