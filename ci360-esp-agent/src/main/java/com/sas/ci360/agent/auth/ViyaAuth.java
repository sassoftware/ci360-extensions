package com.sas.ci360.agent.auth;

import java.util.Base64;
import java.util.Date;
import java.util.HashMap;

import org.json.JSONObject;

import com.sas.ci360.agent.util.HttpUtils;

public class ViyaAuth {
	private static String token;
	private static long tokenExpire;
	
	private static final String CONTENT_TYPE_JSON = "application/json";
	private static final String ENCODING_UTF8 = "UTF-8";
	
	// OAuth Token for MAS
	public static synchronized String getOAuthToken(String sidHost, String sidProtocol, String grantType, String sidUser, String sidPass, String appID, String appSecret, boolean ignoreCert) throws Exception {
		if (token != null && tokenExpire > System.currentTimeMillis()) {
			return token;
		}
		
		try {
			String querystring = "?grant_type=" + grantType;
			if (grantType.equals("password")) {
				querystring += "&username=" + sidUser + "&password=" + sidPass;
			}
			String urlGetToken = sidProtocol + "://" + sidHost + "/SASLogon/oauth/token" + querystring;
			
			String userPass = appID + ":" + (appSecret != null ? appSecret : "");
			String userpassbytes = Base64.getEncoder().encodeToString(userPass.getBytes(ENCODING_UTF8));
			
			HashMap<String, String> headers = new HashMap<String, String>();
			headers.put("Content-Type", CONTENT_TYPE_JSON);
			headers.put("Authorization", "Basic " + userpassbytes);

			// build payload
			String response = HttpUtils.httpGet(urlGetToken, headers, ignoreCert);
			if (response.contains("access_token") == true) {
				JSONObject responseJson = new JSONObject(response);
				token = responseJson.getString("access_token");
				
				long expiresInSec = responseJson.getLong("expires_in");
				long currentTime = System.currentTimeMillis();
				tokenExpire = currentTime + (expiresInSec * 1000);
				Date expireDate = new Date(tokenExpire);
				System.out.println("expiresIn: " + expiresInSec + ", expireTime: " + tokenExpire + ", expire Date: " + expireDate);
				
				return token;
			}
			else {
				throw new Exception("No access token found - check configuration for OAuth");
			}
		} 
		catch (Exception e1) {
			e1.printStackTrace();
			throw new Exception("Error occured - check configuration for OAuth");
		}
	}
}
