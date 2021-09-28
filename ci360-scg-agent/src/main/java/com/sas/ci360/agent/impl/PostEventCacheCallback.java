/* Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.sas.ci360.agent.impl;

import java.util.Properties;

import org.json.JSONObject;

import com.sas.ci360.agent.PostEventCallback;
import com.sas.ci360.agent.cache.IdentityCache;
import com.sas.ci360.agent.cache.MessageCache;

public class PostEventCacheCallback implements PostEventCallback {
	private static final String JSON_RECIPIENT = "recipient";
	private static final String JSON_DATAHUB_ID = "datahub_id";
	private static final String JSON_MSG_ID = "msgId";
	private static final String INTL_PREFIX = "+";
	
	private MessageCache msgCache;
	private IdentityCache identityCache;
	private boolean identityCacheEnabled = false;
	
	public PostEventCacheCallback(MessageCache msgCache, IdentityCache identityCache, Properties config) {
		this.msgCache = msgCache;
		this.identityCache = identityCache;
		
		if (config.getProperty("agent.twoWay.identityCacheEnabled", "false").equalsIgnoreCase("TRUE")) {
			identityCacheEnabled = true;
		}
	}
	
	@Override
	public void postEvent(JSONObject eventData) throws Exception {
		this.msgCache.put(eventData.getString(JSON_MSG_ID), eventData);
		if (identityCacheEnabled) {
			if (eventData.getString(JSON_RECIPIENT).startsWith(INTL_PREFIX)) {
				this.identityCache.put(eventData.getString(JSON_RECIPIENT), eventData.getString(JSON_DATAHUB_ID));
			}
			else {
				// prepend + to phone number (webhook phone numbers are always in full intl format)
				// TODO: check if we need to handle webhook for FB/WeChat differently
				this.identityCache.put(INTL_PREFIX + eventData.getString(JSON_RECIPIENT), eventData.getString(JSON_DATAHUB_ID));
			}
		}
	}

}
