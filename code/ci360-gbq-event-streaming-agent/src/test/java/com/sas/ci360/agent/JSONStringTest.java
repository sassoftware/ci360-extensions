package com.sas.ci360.agent;

import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class JSONStringTest {	
	
	@Test
	public void testStringConversion() throws Exception {
		String page_title = "Test: Agent Unit Test Data.";
		String input = "{\"page_title\":\"" + page_title + "\"}";
		System.out.println("Input: " + input);
		
		JSONObject jo = new JSONObject(input);
		System.out.println("JSON: " + jo.toString());
		
		String page_test = jo.getString("page_title");
		System.out.println("page_title: " + page_test);
		
		Assert.assertEquals(page_title, page_test);
		
	}
}
