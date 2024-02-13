/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
package com.sas.incubation.ci.agent.impl;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.json.JSONException;
import org.json.JSONObject;

import com.sas.incubation.ci.agent.Agent;
import com.sas.incubation.ci.agent.entities.ContactHistory;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventProcessor implements Runnable {
	
	private static final String JSON_ATTRIBUTES = "attributes";
	private static final String JSON_EVENTNAME = "eventName";

	private static final String JSON_DATAHUB_ID = "datahub_id";
	private static final String JSON_SUBJECT_ID = "subject_id";
	private static final String JSON_TIMESTAMP = "timestamp";
	private static final String JSON_EVENT_TASK_ID = "task_id";
	private static final String JSON_CONTACT_ID = "guid";

	private static final String JSON_CHANNEL_TYPE = "channelType";
	
	private String event;
	
	ConcurrentLinkedQueue<ContactHistory> queue;
	ConcurrentHashMap<String, Integer> statsHashMap;
	
	public EventProcessor(String event, ConcurrentLinkedQueue<ContactHistory> queue, ConcurrentHashMap<String, Integer> statsHashMap) {
		this.event = event;
		this.queue = queue;
		this.statsHashMap = statsHashMap;
	}

	@Override
	public void run() {
		log.trace("Event received");
		if (event.startsWith("CFG")) {
			log.debug("Config event received");
		} else {
			JSONObject jsonEvent = new JSONObject(event);
			log.trace("JSON: " + jsonEvent.toString());

			JSONObject eventAttr = jsonEvent.getJSONObject(JSON_ATTRIBUTES);
			String eventName = eventAttr.getString(JSON_EVENTNAME);
			
			statsHashMap.compute(Agent.STATS_EVENT_COUNT, (k, v) -> v == null ? 1 : v + 1);
			statsHashMap.compute(eventName, (k, v) -> v == null ? 1 : v + 1);

			// if (eventName.startsWith("c_")) {
				try {
					
					ContactHistory history = new ContactHistory();
					
					String datahub_id = null;
					String subject_id = null;
					String contact_id = null;
					Instant contact_dttm_utc = null;
					String task_id = null;
					String channel_type = null;

					try {
						datahub_id = jsonEvent.getJSONObject(JSON_ATTRIBUTES).getString(JSON_DATAHUB_ID);
						subject_id = jsonEvent.getJSONObject(JSON_ATTRIBUTES).isNull(JSON_SUBJECT_ID) ? "" : jsonEvent.getJSONObject(JSON_ATTRIBUTES).getString(JSON_SUBJECT_ID);
						contact_id = jsonEvent.getJSONObject(JSON_ATTRIBUTES).isNull(JSON_CONTACT_ID) ? "" : jsonEvent.getJSONObject(JSON_ATTRIBUTES).getString(JSON_CONTACT_ID);
						contact_dttm_utc = Instant.ofEpochMilli(Long.parseLong(jsonEvent.getJSONObject(JSON_ATTRIBUTES).getString(JSON_TIMESTAMP)));
						task_id = jsonEvent.getJSONObject(JSON_ATTRIBUTES).isNull(JSON_EVENT_TASK_ID) ? "" : jsonEvent.getJSONObject(JSON_ATTRIBUTES).getString(JSON_EVENT_TASK_ID);
						channel_type = jsonEvent.getJSONObject(JSON_ATTRIBUTES).getString(JSON_CHANNEL_TYPE);
					} catch (JSONException e) {
						log.error("Error parsing JSON:" + jsonEvent.toString());
					}
					
					history.setDatahub_id(datahub_id);
					history.setSubject_id(subject_id);
					history.setContact_id(contact_id);
					history.setContact_dttm_utc(LocalDateTime.ofInstant(contact_dttm_utc, ZoneId.of("UTC")));
					history.setTask_id(task_id);
					history.setChannel_type(channel_type);
					
					queue.offer(history);
					
				} catch (Exception e) {
					log.error("Failed to process event, unhandled error:", e);
				}
			// }
		}
	}
	
	private boolean convertToBoolean(String value) {
		boolean returnValue = false;
		if ("1".equalsIgnoreCase(value) || "true".equalsIgnoreCase(value))
			returnValue = true;
		return returnValue;
	}

}
