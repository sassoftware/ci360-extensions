package com.sas.ci360.agent;

import java.util.Properties;

import org.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sas.ci360.agent.exceptions.ConfigurationException;
import com.sas.ci360.agent.impl.CASEventHandler;
import com.sas.ci360.agent.util.AgentUtils;


public class CASEventHandlerTest {
	private static Properties config = new Properties();
	
	@BeforeClass
	public static void initTest() {
		config = AgentUtils.readConfig("agent.config");
	}
	
	@Test
	public void testProcessEvent() throws Exception {
		JSONObject jsonEvent = new JSONObject();		
		JSONObject jsonAttr = new JSONObject();
		jsonAttr.put("timestamp", "11111111111");
		jsonAttr.put("eventName", "CI360_event_name_JUNIT");
		jsonAttr.put("channelType", "JUNIT_type");
		jsonAttr.put("identityId", "sdfsdfsdf-sfdsdaf-asfsadf-sadf");		
		jsonEvent.put("attributes", jsonAttr);
		
		/*
		EventHandler eventHandler = new CASEventHandler();
		try {
			eventHandler.initialize(config);		
			eventHandler.processEvent(jsonEvent);
		}
		catch (ConfigurationException ex) {
			System.out.println(ex.getMessage());
		}
		*/

	}
}
