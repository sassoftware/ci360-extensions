/* Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.sas.ci360.agent.errors;

import org.json.JSONObject;

public class FailedEvent {
	private JSONObject event;
	private int retryCount;
	
	public FailedEvent(JSONObject jsonEvent, int retryCount) {
		this.event = jsonEvent;
		this.retryCount = retryCount;
	}
	
	public FailedEvent(JSONObject event) {
		this(event, 0);
	}

	public JSONObject getEvent() {
		return event;
	}

	public void setEvent(JSONObject event) {
		this.event = event;
	}

	public int getRetryCount() {
		return retryCount;
	}

	public void setRetryCount(int retryCount) {
		this.retryCount = retryCount;
	}
	
	public void incrementRetryCount() {
		this.retryCount++;
	}
}
