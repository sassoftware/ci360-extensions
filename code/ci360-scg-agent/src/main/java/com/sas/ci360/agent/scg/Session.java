package com.sas.ci360.agent.scg;

import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sas.ci360.agent.util.AgentUtils;
import com.sas.ci360.agent.util.HttpUtils;

public class Session {
	private static final Logger logger = LoggerFactory.getLogger(Session.class);
	
	private static final String CONTENT_TYPE_JSON = "application/json";
	private static final String BEARER_PREFIX = "Bearer ";
	
	private static final String URL_SLASH = "/";
	
	private static final String JSON_ID = "id";
	private static final String JSON_FROM = "from";
	private static final String JSON_TO = "to";
	private static final String JSON_STATE = "state";
	
	private static String API_URI = "/scg-external-api/api/v1/";
	private static String MSG_REQ_URI = "messaging/message_requests";

    private final String baseUrl;
    private final String apiUrl;
    private AuthInfo auth;
    private HttpHost proxy;
    
    private final String messageRequestUrl;
    private final String messageEncoding;
    
    public Session(final String baseUrl, AuthInfo auth, String encoding) {
        this.baseUrl = baseUrl;
        this.apiUrl = baseUrl + API_URI;
        this.auth = auth;
        
        this.messageRequestUrl = this.apiUrl + MSG_REQ_URI;
        this.messageEncoding = encoding;
    }
    
    public Session(final String baseUrl, AuthInfo auth, String encoding, String proxyHost, int proxyPort, String proxyScheme) {
        this(baseUrl, auth, encoding);
        this.proxy = new HttpHost(proxyHost, proxyPort, proxyScheme);
    }
    
    public String create(MessageRequest mrq) throws Exception {
        try {
            JSONObject mrqObj = new JSONObject(mrq);
            logger.debug("Request body: " + mrqObj.toString());
            
            Map<String, String> headers = new HashMap<String, String>();
            headers.put(HttpHeaders.AUTHORIZATION, BEARER_PREFIX + this.auth.getToken());
            String httpResponse = httpPostWithRetry(this.messageRequestUrl, mrqObj.toString(), CONTENT_TYPE_JSON, headers, this.messageEncoding, 0);
        	
            JSONObject responseJson = new JSONObject(httpResponse);
        	return responseJson.getString(JSON_ID);
        } catch (Exception e) {
        	logger.warn("Failed: " + e.getMessage());
            e.printStackTrace();
            throw new Exception("Call to service failed with Exception: " + e.getMessage());
        }
    }
    
    public MessageRequest get(String id) throws Exception {
        try {
        	String reqURL = this.messageRequestUrl + URL_SLASH + id;
            logger.debug("Request URL: " + reqURL);
            
            Map<String, String> headers = new HashMap<String, String>();
            headers.put(HttpHeaders.AUTHORIZATION, BEARER_PREFIX + this.auth.getToken());
            String httpResponse = HttpUtils.httpGet(reqURL, headers);        	
            JSONObject responseJson = new JSONObject(httpResponse);
            
            MessageRequest mrq = new MessageRequest();
            mrq.setId(responseJson.getString(JSON_ID));
            mrq.setFrom(responseJson.getString(JSON_FROM));
            mrq.setTo(AgentUtils.JSONArrayToStringList(responseJson.getJSONArray(JSON_TO)));
            mrq.setState(responseJson.getString(JSON_STATE));
        	return mrq;
        } catch (Exception e) {
        	logger.warn("Failed: " + e.getMessage());
            e.printStackTrace();
            throw new Exception("Call to service failed with Exception: " + e.getMessage());
        }
    }
    
    private String httpPostWithRetry(String url, String body, String contentType, Map<String, String> headers, String encoding, int attempt) throws Exception {
    	try {
    		return HttpUtils.httpPost(url, body, contentType, headers, encoding);
    	}
    	catch (Exception ex) {
    		if (attempt >= this.auth.getRetries()) {
    			throw ex;
    		}
    		logger.warn("HTTP request failed, attempting again");
    		return httpPostWithRetry(url, body, contentType, headers, encoding, attempt + 1);
    	}
    }
}
