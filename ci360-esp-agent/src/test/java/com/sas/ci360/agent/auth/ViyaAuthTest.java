package com.sas.ci360.agent.auth;

import java.util.Properties;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ViyaAuthTest {
	private static Properties config = new Properties();
	
	@BeforeClass
	public static void setup() throws Exception {
		System.out.println("Initialize config for test");
		config.put("sid.grant_type", "password");
		config.put("sid.protocol", "http");
		config.put("sid.host", "sasserver.demo.sas.com");
		config.put("sid.username", "sasdemo");
		config.put("sid.password", "Orion123");
		config.put("sid.appID", "sas.ec");
		config.put("sid.appSecret", "");
	}
	
	@Before
	public void beforeEachTest() {
		System.out.println("Run before each test placeholder");
	}

	@After
	public void afterEachTest() {
		System.out.println("Run after each test placeholder");
	}
	
	@Test
	public void testGetToken() throws Exception {
		System.out.println("Get OAuthToken");
		String sidHost = config.getProperty("sid.host");
		//String token = ViyaAuth.getOAuthToken(sidHost, config.getProperty("sid.protocol"), config.getProperty("sid.grant_type"), config.getProperty("sid.username"), config.getProperty("sid.password"), config.getProperty("sid.appID"), config.getProperty("sid.appSecret"));
		//System.out.println("token: " + token);
		
		//Assert.assertNotNull(token);
	}
	
}
