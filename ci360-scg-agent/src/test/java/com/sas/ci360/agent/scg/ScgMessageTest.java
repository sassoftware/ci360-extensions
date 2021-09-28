/* Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.sas.ci360.agent.scg;

import java.util.Properties;

import org.json.JSONObject;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sas.ci360.agent.EventHandler;
import com.sas.ci360.agent.exceptions.EventHandlerException;
import com.sas.ci360.agent.impl.MessageEventHandler;
import com.sas.ci360.agent.util.AgentUtils;

public class ScgMessageTest {
	private static Properties config = new Properties();
	
	@BeforeClass
	public static void initTest() {
		config.setProperty("scg.defaultChannel", "SMS");
		config.setProperty("agent.creative.format", "PLAIN");
	}
	
	@Test
	public void testToSanitized() {
		String testNum1 = "+13125551234";
		String maskedNum1 = "********1234";
		MessageRequest mrq = new MessageRequest();
		mrq.setTo(testNum1);
		String san1 = AgentUtils.maskRecipients( mrq.getTo() ).get(0);
		
		Assert.assertEquals(maskedNum1, san1);
		
		String testNum2 = "+5511994395698";
		String maskedNum2 = "********395698";
		
		mrq.setTo(testNum2);
		String san2 = AgentUtils.maskRecipients( mrq.getTo() ).get(0);		
		Assert.assertEquals(maskedNum2, san2);
		
		String san3 = AgentUtils.maskRecipient( mrq.getTo().get(0) );
		Assert.assertEquals(maskedNum2, san3);
	}
	
	@Test
	public void testExternalId() {
		String datahubId = "safsadf-23423-sfasfdf-23423-sdfsdf";
		String taskCode = "TSK_1234";
		
		String externalId = datahubId + "|" + taskCode;
		
		String idComponents[] = externalId.split("\\|");
		
		Assert.assertEquals(2, idComponents.length);
		Assert.assertEquals(datahubId, idComponents[0]);
		Assert.assertEquals(taskCode, idComponents[1]);

	}
}
