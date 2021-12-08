package com.sas.ci360.agent;

import org.json.JSONObject;

public interface PostEventCallback {
	public void postEvent(JSONObject eventData) throws Exception;
}
