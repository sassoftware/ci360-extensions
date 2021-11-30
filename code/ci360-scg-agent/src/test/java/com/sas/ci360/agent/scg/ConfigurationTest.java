/* Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.sas.ci360.agent.scg;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class ConfigurationTest {

	@Test
	public void testResponseEventMap() {
		Map<String, String> responseEvents = new HashMap<String, String>();
		
		String responseEventNames = "DELIVERED:SMS Delivered,FAILED:SMS Failed,CLICKED:SMS Clicked";
		if (responseEventNames != null) {
			String dispositions[] = responseEventNames.split(",");
			for (int i=0; i < dispositions.length; i++) {
				String disposition[] = dispositions[i].split(":");
				String status = disposition[0];
				String eventName = disposition[1];
				responseEvents.put(status, eventName);
			}
		}
		
		System.out.println(responseEventNames);
		
		Assert.assertEquals(responseEvents.get("FAILED"), "SMS Failed");
	}
}
