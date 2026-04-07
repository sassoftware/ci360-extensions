/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package client.ci360.eventstreamagent;

/**
 *
 * @author sas
 */
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.json.JSONObject;

import client.ci360.tasks.Source;
import client.ci360.tasks.Task;
import client.ci360.tasks.TaskCache;

/**
 * Modification History
 * 12/27/2022  Raja M   Extract the segment_version_id/segment_id of each event
 * 01/24/2023  Raja M   Disable the retrieval of task information from this process.  Tasks/Custom_Properties info
 *                       will be retrieved external to this process
 */
public class Event {
	private JSONObject json;
	private Task task;
	private String subjectId;
	private boolean isEmailContactEvent;
	private LocalDateTime eventDttm;
	private String eventGuid;
	private String recipientDomain;
	private String responseTrackingCode;
	private String segmentVersionId;
	private String segmentId;
	private LocalDateTime agentDttm;
	private String imprintId;
	private String datahubId;

	Event(JSONObject json, LocalDateTime agentDttm) {
		this.json = json;
		this.agentDttm = agentDttm;
		subjectId = getString("subject_id");
		if (subjectId != null)
			subjectId = subjectId.toUpperCase();
		imprintId = getString("imprint_id");
		/*
		task = TaskCache.getTask(getString("task_version_id"), getString("task_id"));
		*/
		task = null;
		eventDttm = getDttm("timestamp");
		eventGuid = getString("guid");
		datahubId = getString("datahub_id");
		recipientDomain = getString("recipientDomain");
		responseTrackingCode = getString("response_tracking_code");
		segmentVersionId = getString("segment_version_id");
		segmentId = getString("segment_id");
		isEmailContactEvent = "c_send".equals(getString("eventname")) & "email".equals(getString("event_channel"));
	}

	public String toString() {
		try {
			return json.toString(1);
		} catch (Exception e) {
			return null;
		}
	}

	public String getString(String key) {
		try {
			return json.getString(key).trim();
		} catch (Exception e) {
			return null;
		}
	}

	public LocalDateTime getDttm(String key) {
		LocalDateTime dttm = null;
		try {
			long eventEpochMilli = Long.parseLong(json.getString(key));
			dttm = LocalDateTime.ofInstant(Instant.ofEpochMilli(eventEpochMilli), ZoneId.systemDefault());
		} catch (Exception e) {
		}
		return dttm;
	}

	public Source getTaskSource() {
		return (task == null) ? null : task.getSource();
	}

	public String getTaskVersionId() {
		return (task == null) ? null : task.getVersionId();
	}

	public String getTaskId() {
		return (task == null) ? null : task.getId();
	}

	public String getOsirDescription() {
		return (task == null) ? null : task.getCustomProperty("osirDescription");
	}

	public LocalDateTime getTaskPublishedDttm() {
		return (task == null) ? null : task.getLastPublishedDttm();
	}

	public String getTaskName() {
		return (task == null) ? null : task.getName();
	}

	public String getResponseTrackingCode() {
		return responseTrackingCode;
	}

	public String getSegmentId() {
		return segmentId;
	}

	public String getSegmentVersionId() {
		return segmentVersionId;
	}
	public String getTaskCustomPropertiesAsJsonString() {
		return (task == null) ? null : task.getCustomPropertiesAsJsonString();
	}

	public String getRecipientDomain() {
		return recipientDomain;
	}

	public String getSubjectId() {
		return subjectId;
	}

	public String getImprintId() {
		return imprintId;
	}

	public boolean isEmailContactEvent() {
		return isEmailContactEvent;
	}

	public LocalDateTime getEventDttm() {
		return eventDttm;
	}

	public String getEventGuid() {
		return eventGuid;
	}

	public LocalDateTime getAgentDttm() {
		return agentDttm;
	}

	public String getDatahubId() {
		return datahubId;
	}

}

