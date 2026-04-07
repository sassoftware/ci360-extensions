/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package client.ci360.tasks;

/**
 *
 * @author sas
 */

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;

public class Task {
	private static final Logger logger = (Logger) LoggerFactory.getLogger(Task.class);
	private static long taskExpiresMinutes;

	private Source source;
	private String taskId;
	private String name;
	private String channel;
	private String deliveryType;
	private String state;
	private LocalDateTime lastPublishedDttm;
	private LocalDateTime cachedDttm;
	private LocalDateTime expiresDttm;
	private Map<String, String> customProperties = null;
	private String versionId;

	private static DateTimeFormatter dttmParser = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss z yyyy",
			Locale.ENGLISH);
	private static DateTimeFormatter dttmFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS",
			Locale.ENGLISH);

	static void init(ResourceBundle config) throws Exception {
		taskExpiresMinutes = Long.parseLong(config.getString("task.expiresMinutes"));
	}

	public void refresh() {
		cachedDttm = LocalDateTime.now();
		expiresDttm = cachedDttm.plusMinutes(taskExpiresMinutes);
		if (versionId != null)
			versionId = versionId.trim();
		if(taskId != null)
			taskId = taskId.trim();
	}

	public Task(Source source, ResultSet rs) throws SQLException {
		this.source = source;
		this.versionId = rs.getString(1);
		this.taskId = rs.getString(2);
		this.name = rs.getString(3);
		this.channel = rs.getString(4);
		this.deliveryType = rs.getString(5);
		this.state = rs.getString(6);
		Timestamp lastPublishedUTC = rs.getTimestamp(7);
		this.lastPublishedDttm = localDttm(lastPublishedUTC);
		refresh();
	}

	public Task(Source source, String versionId, JSONObject json) throws JSONException {
		refresh();
		this.source = source;
		this.versionId = versionId;
		this.taskId = json.getString("taskId");
		this.name = json.getString("name");
		this.channel = stringValue(json, "channel");
		this.deliveryType = stringValue(json, "deliveryType");
		this.lastPublishedDttm = localDttm(json, "lastPublishedTimeStamp");
		this.state = stringValue(json, "state");
		refresh();
		JSONArray jsonProperties = null;
		try {
			jsonProperties = json.getJSONArray("customProperties");
		} catch (Exception e) {
		}
		if (jsonProperties == null)
			return;
		this.customProperties = new HashMap<String, String>();
		for (int i = 0; i < jsonProperties.length(); i++) {
			try {
				JSONObject property = jsonProperties.getJSONObject(i);
				String propertyName = property.getString("propertyName");
				JSONArray propertyValues = property.getJSONArray("propertyValue");
				String propertyValue = propertyValues.optString(0);
				customProperties.put(propertyName, propertyValue);
			} catch (Exception e) {
				logger.error("Failed parsing cutom property", e);
			}
		}
	}

	public String getId() {
		return taskId;
	}

	public String getName() {
		return name;
	}

	public String getChannel() {
		return channel;
	}

	public String getdeliveryType() {
		return deliveryType;
	}

	public LocalDateTime getLastPublishedDttm() {
		return lastPublishedDttm;
	}

	public String getState() {
		return state;
	}

	public String getCustomProperty(String key) {
		if (customProperties == null)
			return null;
		return customProperties.get(key);
	}

	public String toString() {
		try {
			return toJson().toString(1);
		} catch (JSONException e) {
			return null;
		}
	}

	public String getCustomPropertiesAsJsonString() {
		if (customProperties == null)
			return null;
		try {
			return new JSONObject(customProperties).toString(1);
		} catch (Exception e) {
			logger.error("Failed", e);
		}
		return null;
	}

	Map<String, String> getCustomProperties() {
		return customProperties;
	}

	LocalDateTime getExpiresDttm() {
		return expiresDttm;
	}

	public void setCustomProperties(Map<String, String> customProperties) {
		this.customProperties = customProperties;
	}

	private JSONObject toJson() throws JSONException {
		JSONObject json = new JSONObject();
		json.put("taskVersionId", versionId);
		json.put("taskId", taskId);
		json.put("source", source);
		json.put("name", name);
		json.put("channel", channel);
		json.put("deliveryType", deliveryType);
		json.put("state", state);
		json.put("customProperties", (customProperties == null) ? null : new JSONObject(customProperties));
		json.put("lastPublishedDttm", (lastPublishedDttm == null) ? null : lastPublishedDttm.format(dttmFormat));
		json.put("cachedDttm", (cachedDttm == null) ? null : cachedDttm.format(dttmFormat));
		json.put("expiresDttm", (expiresDttm == null) ? null : expiresDttm.format(dttmFormat));
		return json;
	}

	private String stringValue(JSONObject json, String key) {
		try {
			return json.getString(key);
		} catch (JSONException e) {
			return null;
		}
	}

	private LocalDateTime localDttm(Timestamp utc) {
		if (utc == null)
			return null;
		try {
			ZonedDateTime zoned = ZonedDateTime.of(utc.toLocalDateTime(), ZoneOffset.UTC);
			LocalDateTime dttm = zoned.withZoneSameInstant(ZoneId.systemDefault()).toLocalDateTime();
			return dttm;
		} catch (Exception e) {
			logger.error("Failed", e);
		}
		return null;
	}

	private LocalDateTime localDttm(JSONObject json, String key) {
		String value = stringValue(json, key);
		if (value == null || "null".equals(value))
			return null;
		try {
			ZonedDateTime zoned = ZonedDateTime.parse(value, dttmParser);
			LocalDateTime dttm = zoned.withZoneSameInstant(ZoneId.systemDefault()).toLocalDateTime();
			return dttm;
		} catch (Exception e) {
			logger.warn("Error parsing dttm value {}", value, e);
			return null;
		}
	}

	public String getVersionId() {
		return versionId;
	}

	public Source getSource() {
		return source;
	}
}