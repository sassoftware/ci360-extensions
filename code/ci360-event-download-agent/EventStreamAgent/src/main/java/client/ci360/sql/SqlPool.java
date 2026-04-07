/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package client.ci360.sql;

/**
 *
 * @author sas
 */

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ResourceBundle;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import client.ci360.eventstreamagent.Event;
/*
Modification History
01/14/2023	Add user/pw as jdbc parameters and separate user/pw from jdbc url
 */
public abstract class SqlPool {
	private static EventSqlPool eventSql;

	private List<SqlConnection> pool;
	Logger logger;
	String jdbcUser;
	String jdbcPw;
	String jdbcUrl;
	Integer bufferSize;
	Integer secondsBetweenSubmits;
	String insertSql;
	String[] columns;
	int[] columnLengths;

	public static void init(ResourceBundle config) throws Exception {
		eventSql = new EventSqlPool(config);
	}

	public static void stop() {
		TaskSql.stop();
		if (eventSql != null)
			eventSql.stopConnections();
	}

	public static void insertEvent(Event event) throws SQLException, InterruptedException {
		eventSql.insert(event);
	}

	SqlPool() {
		logger = (Logger) LoggerFactory.getLogger(getClass());
	}

	void init(ResourceBundle config, String prefix, String[] columns) throws Exception {
		this.columns = columns;

		jdbcUser = config.getString(prefix + ".sql.user");
		jdbcPw = config.getString(prefix + ".sql.pw");
		jdbcUrl = config.getString(prefix + ".sql.jdbcURL");
		int poolSize = Integer.valueOf(config.getString(prefix + ".sql.poolSize"));

		/* Create insert statement */
		bufferSize = Integer.valueOf(config.getString(prefix + ".sql.bufferSize"));
		secondsBetweenSubmits = Integer.valueOf(config.getString(prefix + ".sql.secondsBetweenSubmits"));
		String table = config.getString(prefix + ".sql.table").trim();
		List<String> values = Collections.nCopies(columns.length, "?");
		insertSql = String.format("insert into %s(%s) values(%s)", table, String.join(",", columns),
				String.join(",", values));
		logger.info("poolSize: {} insertSql: {}", poolSize, insertSql);
                logger.info("sql Buffersize: {}", bufferSize);
                logger.info("seconds between submits: {}", secondsBetweenSubmits);
		/* Connection pool */
		pool = new ArrayList<SqlConnection>();
		for (int i = 0; i < poolSize; i++)
			pool.add(new SqlConnection(this, prefix, i));

		/* Get column lengths */
		SqlConnection sql = pool.get(0);
		columnLengths = new int[columns.length];
		String columnList = "";
		for (int i = 0; i < columns.length; i++) {
			ParameterMetaData metadata = sql.insertStatement.getParameterMetaData();
			columnLengths[i] = metadata.getPrecision(i + 1);
			columnList = columnList + "\n" + columns[i] + " [" + metadata.getParameterTypeName(i + 1) + "] ("
					+ columnLengths[i] + ")";
		}
		logger.debug("columns: {}", columnList);

	/*	TaskSql.init(config);   */
	}

	void stopConnections() {
		for (SqlConnection sql : pool)
			sql.disconnect();
	}

	SqlConnection getConnection() throws InterruptedException, SQLException {
		synchronized (pool) {
			while (pool.isEmpty()) {
				pool.wait();
			}
			return pool.remove(0);
		}
	}

	void returnConnection(SqlConnection sql) {
		synchronized (pool) {
			pool.add(sql);
			pool.notify();
		}
	}
}
