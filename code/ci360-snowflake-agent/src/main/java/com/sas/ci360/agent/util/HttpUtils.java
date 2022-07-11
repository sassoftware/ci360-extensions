/* Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.sas.ci360.agent.util;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpUtils {
	private static final Logger logger = LoggerFactory.getLogger(HttpUtils.class);

	public static String httpPost(String url, String postBody, String contentType) throws Exception {
		return httpPost(url, postBody, contentType, null, null);
	}
	
	public static String httpPost(String url, String postBody, String contentType, Map<String, String> headers) throws Exception {
		return httpPost(url, postBody, contentType, headers, null);
	}

	public static String httpPost(String url, String postBody, String contentType, String encoding) throws Exception {
		return httpPost(url, postBody, contentType, null, encoding);
	}
	
	public static String httpPost(String url, String postBody, String contentType, Map<String, String> headers, String encoding) throws Exception {
        try {
        	logger.debug("Building HTTP POST request, postUrl: " + url);
            HttpPost post = new HttpPost(url);
            if (headers != null) {
            	addHeaders(post, headers);
            }

            StringEntity input = new StringEntity(postBody, encoding);
    		input.setContentType(contentType);
    		post.setEntity(input);

            try (CloseableHttpClient httpClient = HttpClients.createDefault();
                    CloseableHttpResponse response = httpClient.execute(post)) {
                logger.debug("Response Code: " + response.getStatusLine().getStatusCode());
                if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK && response.getStatusLine().getStatusCode() != HttpStatus.SC_CREATED) {
                	throw new Exception("Call to service failed with HTTP status code: " + response.getStatusLine().getStatusCode());
                }
                return EntityUtils.toString(response.getEntity());
            }
        } catch (ClientProtocolException e) {
        	logger.warn("Failed: " + e.getMessage());
            e.printStackTrace();
            throw new Exception("Call to service failed with ClientProtocolException: " + e.getMessage());
        } catch (IOException e) {
        	logger.warn("Failed: " + e.getMessage());
            e.printStackTrace();
            throw new Exception("Call to service failed with IOException: " + e.getMessage());
        }
	}
	
	public static String httpGet(String url, Map<String, String> headers) throws Exception {		
        try {            
        	logger.debug("Building HTTP GET request, getUrl: " + url);
            HttpGet post = new HttpGet(url);
            if (headers != null) {
            	addHeaders(post, headers);
            }

            try (CloseableHttpClient httpClient = HttpClients.createDefault();
                    CloseableHttpResponse response = httpClient.execute(post)) {
                logger.debug("Response Code: " + response.getStatusLine().getStatusCode());
                if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK && response.getStatusLine().getStatusCode() != HttpStatus.SC_CREATED) {
                	throw new Exception("Call to service failed with HTTP status code: " + response.getStatusLine().getStatusCode());
                }
                return EntityUtils.toString(response.getEntity());
            }
        } catch (ClientProtocolException e) {
        	logger.warn("Failed: " + e.getMessage());
            e.printStackTrace();
            throw new Exception("Call to service failed with ClientProtocolException: " + e.getMessage());
        } catch (IOException e) {
        	logger.warn("Failed: " + e.getMessage());
            e.printStackTrace();
            throw new Exception("Call to service failed with IOException: " + e.getMessage());
        }
	}
	
	private static void addHeaders(HttpRequestBase request, Map<String, String> headers) {
		// go through headers hash map and add each header line to request
		Iterator<Entry<String, String>> iterator = headers.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<String, String> entry = iterator.next();
			request.addHeader(entry.getKey(), entry.getValue());
		}
	}
	
}
