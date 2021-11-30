/* Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.sas.ci360.agent;

import org.json.JSONObject;

public interface PostEventCallback {
	public void postEvent(JSONObject eventData) throws Exception;
}
