/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
package com.sas.incubation.ci.agent;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import com.sas.incubation.ci.agent.entities.ContactHistory;


public class JSONT {
	
	private static final String JSON_ATTRIBUTES = "attributes";
	private static final String JSON_DATAHUB_ID = "datahub_id";
	private static final String JSON_SUBJECT_ID = "subject_id";
	private static final String JSON_TIMESTAMP = "timestamp";
	private static final String JSON_TASK_ID = "taskId";
	private static final String JSON_EVENT_TASK_ID = "task_id";
	private static final String JSON_CHANNEL_TYPE = "channelType";
	
	private static final String ZONE_ID = "Europe/Berlin";
	
	@Test
	void test1() {
		
		String event = "{\"tenantId\":22011801,\"attributes\":{\"generatedTimestamp\":\"1674811110823\",\"screen_info\":\"x@\",\"task_id\":\"66b486ef-298c-45c3-b504-62d7ccacde86\",\"channelType\":\"email\",\"eventname\":\"c_send\",\"event_uid\":\"29f87914-76b7-4748-9639-999f5c08ec0c\",\"vid\":\"ac981271-6d46-36c3-9ce8-12aab78078f8\",\"extendedCustomEventWithRevenueFlag\":\"false\",\"parent_event_uid\":\"25958733-06c1-49a9-81a6-171ddfa198de\",\"emailProgramId\":\"Interessenten-Newsletter\",\"internalTenantId\":\"22011801\",\"response_tracking_code\":\"d87bb978-bf58-4f0f-97f7-ceb96c48a891\",\"variant\":\"0\",\"eventName\":\"c_send\",\"event\":\"c_send\",\"timestamp\":\"1674811110823\",\"event_channel\":\"email\",\"internal_tenant_id\":\"22011801\",\"task_version_id\":\"ZSQR_k2IpTqyGvpCJzWjBAjFxGp36Je0\",\"recipientDomain\":\"mail.de\",\"goal_guid\":\"e0c3d047-eea2-4438-9f33-b5e49cb5cee3\",\"event_category\":\"unifiedAndEngage\",\"imprint_id\":\"e8f68049-0c55-44de-9854-c90a2d053a83\",\"emailImprintURL\":\"https://d3on7v574i947w.cloudfront.net/e/cidkateu/e8f68049-0c55-44de-9854-c90a2d053a83.html\",\"emailSendAgentId\":\"9fa32d6b-0a10-45b6-a394-8add5abe4dd0\",\"datahub_id\":\"ac981271-6d46-36c3-9ce8-12aab78078f8\",\"guid\":\"6c1e8c13-1c54-4287-940e-8a45decbc484\",\"event_designed_name\":\"c_send\",\"account\":\"a44ad8608500011971e85674\",\"eventDesignedName\":\"c_send\"},\"rowKey\":\"6c1e8c13-1c54-4287-940e-8a45decbc484\"}";

		JSONObject jsonEvent = new JSONObject(event);
		ContactHistory history = new ContactHistory();


		String datahub_id = null;
		Instant contact_dttm_tz = null;
		String taskId = null;
		String channelType = null;
		
		try {
			datahub_id = jsonEvent.getJSONObject(JSON_ATTRIBUTES).getString(JSON_DATAHUB_ID);
			contact_dttm_tz = Instant.ofEpochMilli(Long.parseLong(jsonEvent.getJSONObject(JSON_ATTRIBUTES).getString(JSON_TIMESTAMP)));
			taskId = jsonEvent.getJSONObject(JSON_ATTRIBUTES).isNull(JSON_TASK_ID) ? jsonEvent.getJSONObject(JSON_ATTRIBUTES).getString(JSON_EVENT_TASK_ID) : jsonEvent.getJSONObject(JSON_ATTRIBUTES).getString(JSON_TASK_ID);
			channelType = jsonEvent.getJSONObject(JSON_ATTRIBUTES).getString(JSON_CHANNEL_TYPE);
     	} catch (JSONException e) {
			System.out.println(e);
		}
		
		history.setDatahub_id(datahub_id);
		history.setContact_dttm_utc(LocalDateTime.ofInstant(contact_dttm_tz, ZoneId.of(ZONE_ID)));
		history.setTask_id(taskId);
		history.setChannel_type(channelType);
		

	}
	
	
	boolean convertToBoolean(String value) {
		boolean returnValue = false;
		if ("1".equalsIgnoreCase(value) || "true".equalsIgnoreCase(value))
			returnValue = true;
		return returnValue;
	}


	
}
