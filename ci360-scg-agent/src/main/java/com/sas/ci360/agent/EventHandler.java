/* Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.sas.ci360.agent;

import java.util.Properties;

import org.json.JSONObject;

import com.sas.ci360.agent.exceptions.ConfigurationException;
import com.sas.ci360.agent.exceptions.EventHandlerException;

public interface EventHandler {
	public void initialize();
	public void initialize(Properties config) throws ConfigurationException;
	public void registerCallback(PostEventCallback callback);
	public void processEvent(JSONObject jsonEvent) throws EventHandlerException;
}
