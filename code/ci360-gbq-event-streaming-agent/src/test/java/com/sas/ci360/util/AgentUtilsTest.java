package com.sas.ci360.util;

import java.util.Date;

/*import org.junit.Assert;*/
import org.junit.Test;

import com.sas.ci360.agent.util.AgentUtils;

public class AgentUtilsTest {
	@Test
	public void testFormatTimestamp() throws Exception {
		String timestamp = "1584465230379";
		String time = AgentUtils.formatTimestamp(timestamp);
		System.out.println("Time: " + time);
		//Assert.assertEquals("2020-03-17 12:13:50.379", time);
		
		Date date = new Date(Long.parseLong(timestamp));
		System.out.println("Date: " + date);
	}
}
