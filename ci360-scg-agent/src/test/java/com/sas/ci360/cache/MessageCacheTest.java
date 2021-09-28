package com.sas.ci360.cache;

import java.util.Properties;

import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import com.sas.ci360.agent.cache.MessageCache;
import com.sas.ci360.agent.util.AgentUtils;

public class MessageCacheTest {
	@Test
	public void testCache() throws Exception {
		final Properties config = AgentUtils.readConfig("agent.config");
		MessageCache mc = new MessageCache(config);
		
		JSONObject msgObj = new JSONObject();
		String testId = "12312133123";
		msgObj.put("datahub_id", testId);
		
		mc.put("abcvdfsd", msgObj);
		
		JSONObject obj2 = mc.get("abcvdfsd");
		
		Assert.assertEquals(testId, obj2.get("datahub_id"));
		
		mc.remove("abcvdfsd");
		Assert.assertNull(mc.get("abcvdfsd"));
		
		mc.close();
	}
}
