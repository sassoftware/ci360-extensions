package com.sas.ci360.cache;

import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import com.sas.ci360.agent.cache.IdentityCache;
import com.sas.ci360.agent.util.AgentUtils;

public class IdentityCacheTest {
	@Test
	public void testCache() throws Exception {
		final Properties config = AgentUtils.readConfig("agent.config");
		IdentityCache idCache = new IdentityCache(config);
		
		String testPhone = "13125559999";
		String testId = "12312133123-1231231-12312312331532";
		
		idCache.put(testPhone, testId);
		
		String id2 = idCache.get(testPhone);
		
		Assert.assertEquals(testId, id2);
		
		idCache.remove(testPhone);
		Assert.assertNull(idCache.get(testPhone));
		
		idCache.close();
	}
}
