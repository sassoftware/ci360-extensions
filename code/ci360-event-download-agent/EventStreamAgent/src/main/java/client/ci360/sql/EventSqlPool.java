/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package client.ci360.sql;

/**
 *
 * @author sas
 */

import java.sql.SQLException;
import java.util.ResourceBundle;

import client.Timer;
import client.ci360.eventstreamagent.Event;

/***
 * Modification History
 * 12/27/2022  Raja M.	Added segment_id/segment_version_id to the columns saved to Events table
 * 01/20/2022  Raja M.  Removed columns that are unnecessary today. These could be retrieved by parsing
 *                       JSON with Oracle SQL
 *                       "ACCOUNT", "ACTIVITY_IA_TAG_VALUE", "ACTIVITY_ID", "ACTIVITY_TASK_TYPE", "APPLICATIONID"
 *                       , "CHANNELID", "CHANNELTYPE",  "CHANNEL_USER_ID", "CHANNEL_USER_TYPE", "CREATIVE_CONTENT"
 *                       , "CREATIVE_ID", "DATAHUB_ID", "EVENT", "EVENTDESIGNEDNAME", "EVENTNAME"
 *                       , "EVENTSOURCE", "EVENTTYPE", "EVENT_CATEGORY", "EVENT_CHANNEL", "EVENT_UID"
 *                       , "EXTENDEDCUSTOMEVENTWITHREVENUEFLAG", "EXTERNALCODE", "GENERATEDTIMESTAMP", "GOAL_GUID",
 *                       , "GUID","IMPRINT_ID", "INTERNAL_TENANT_ID", "MESSAGE_ID", "PARENT_EVENT"
 *                       , "PARENT_EVENTNAME", "PARENT_EVENT_UID",RESPONSE_TRACKING_CODE",  "SCREEN_INFO",
 *                       , "SEGMENT_ID", "SEGMENT_VERSION_ID", "SESSIONID", "SUBJECT_ID"
 *                       , "VARIANT_ID", "VID", "TASK_VERSION_ID", "TASK_ID", "TASK_NM"
 *                       , "TASK_PUBLISHED_DTTM", "TASK_CUSTOM_PROPERTIES", "TASK_SOURCE"
 */
class EventSqlPool extends SqlPool {
	private static String[] columns = { "AGENT_DTTM", "EVENT_JSON" };

	EventSqlPool(ResourceBundle config) throws Exception {
		super();
		init(config, "event", columns);
	}

	void insert(Event event) throws SQLException, InterruptedException {
		SqlConnection sql = getConnection();
		synchronized (sql) {
			sql.checkConnection();
			Timer timer = new Timer();
			try {
                                
				sql.insertStatement.clearParameters();
				sql.setValue(1, event.getAgentDttm());
				sql.setValue(2, event.toString());
                                logger.trace("Inserted row by {}", insertSql);
				/*
				sql.setValue(3, event.getTaskName());
				sql.setValue(3, event.getString("account"));
				sql.setValue(4, event.getString("activity_ia_tag_value"));
				sql.setValue(5, event.getString("activity_id"));
				sql.setValue(6, event.getString("activity_task_type"));
				sql.setValue(7, event.getString("applicationId"));
				sql.setValue(8, event.getString("channelId"));
				sql.setValue(9, event.getString("channelType"));
				sql.setValue(10, event.getString("channel_user_id"));
				sql.setValue(11, event.getString("channel_user_type"));
				sql.setValue(12, event.getString("creative_content"));
				sql.setValue(13, event.getString("creative_id"));
				sql.setValue(14, event.getDatahubId());
				sql.setValue(15, event.getString("event"));
				sql.setValue(16, event.getString("eventDesignedName"));
				sql.setValue(17, event.getString("eventname"));
				sql.setValue(18, event.getString("eventSource"));
				sql.setValue(19, event.getString("eventType"));
				sql.setValue(20, event.getString("event_category"));
				sql.setValue(21, event.getString("event_channel"));
				sql.setValue(22, event.getString("event_uid"));
				sql.setValue(23, event.getString("extendedCustomEventWithRevenueFlag"));
				sql.setValue(24, event.getString("externalCode"));
				sql.setValue(25, event.getEventDttm());
				sql.setValue(26, event.getString("goal_guid"));
				sql.setValue(27, event.getEventGuid());
				sql.setValue(28, event.getImprintId());
				sql.setValue(29, event.getString("internal_tenant_id"));
				sql.setValue(30, event.getString("message_id"));
				sql.setValue(31, event.getString("parent_event"));
				sql.setValue(32, event.getString("parent_eventname"));
				sql.setValue(33, event.getString("parent_event_uid"));
				sql.setValue(34, event.getResponseTrackingCode());
				sql.setValue(35, event.getString("screen_info"));
				sql.setValue(36, event.getSegmentId());
				sql.setValue(37, event.getSegmentVersionId());
				sql.setValue(38, event.getString("sessionId"));
				sql.setValue(39, event.getSubjectId());
				sql.setValue(40, event.getString("variant_id"));
				sql.setValue(41, event.getString("vid"));
				sql.setValue(42, event.getTaskVersionId());
				sql.setValue(43, event.getTaskId());
				sql.setValue(44, event.getTaskName());
				sql.setValue(45, event.getTaskPublishedDttm());
				sql.setValue(46, event.getTaskCustomPropertiesAsJsonString());
				sql.setValue(47, event.getTaskSource());
				 */

			} catch (SQLException e) {
				logger.error(String.format("SQLException on statement '%s' Message: %s", insertSql, e.getMessage()), e);
			} finally {
				logger.trace("Duration: {}", timer.msElapsed());
				sql.release();
			}
		}
	}
}
