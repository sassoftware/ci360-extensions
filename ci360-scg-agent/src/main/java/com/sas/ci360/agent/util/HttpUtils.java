/* Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.sas.ci360.agent.util;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
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
		return httpPost(url, null, 0, null, postBody, contentType, headers, encoding);
	}
	
	public static String httpPost(String url, String proxyHost, int proxyPort, String proxyScheme, String postBody, String contentType, Map<String, String> headers, String encoding) throws Exception {
		return httpPost(url, proxyHost != null ? new HttpHost(proxyHost, proxyPort, proxyScheme) : null, postBody, contentType, headers, encoding);
	}
	
	public static String httpPost(String url, HttpHost proxy, String postBody, String contentType, Map<String, String> headers, String encoding) throws Exception {		
        try {
            //build & execute post request            
        	logger.debug("Building HTTP Post request, postUrl: " + url);
            HttpPost post = new HttpPost(url);
            if (headers != null) {
            	addHeaders(post, headers);
            }
            if (proxy != null) {
            	addProxy(post, proxy);
            }

            StringEntity input = new StringEntity(postBody, encoding);
    		input.setContentType(contentType);
    		post.setEntity(input);

    		return executeHttpRequest(post);
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
		return httpGet(url, null, 0, null, headers);
	}
	
	public static String httpGet(String url, String proxyHost, int proxyPort, String proxyScheme, Map<String, String> headers) throws Exception {
		return httpGet(url, proxyHost != null ? new HttpHost(proxyHost, proxyPort, proxyScheme) : null, headers);
	}

	public static String httpGet(String url, HttpHost proxy, Map<String, String> headers) throws Exception {		
        try {
            //build & execute post request            
        	logger.debug("Building HTTP Get request, getUrl: " + url);
            HttpGet get = new HttpGet(url);
            if (headers != null) {
            	addHeaders(get, headers);
            }
            if (proxy != null) {
            	addProxy(get, proxy);
            }

            return executeHttpRequest(get);
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
	
	private static void addProxy(HttpRequestBase request, HttpHost proxy) {
        RequestConfig config = RequestConfig.custom().setProxy(proxy).build();
        request.setConfig(config);
	}
	
	private static String executeHttpRequest(HttpUriRequest request) throws ClientProtocolException, IOException, Exception {
		String result = "";
        try (CloseableHttpClient httpClient = HttpClients.createDefault();
                CloseableHttpResponse response = httpClient.execute(request)) {
            logger.debug("Response Code: " + response.getStatusLine().getStatusCode());
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK && response.getStatusLine().getStatusCode() != HttpStatus.SC_CREATED) {
            	throw new Exception("Call to service failed with HTTP status code: " + response.getStatusLine().getStatusCode());
            }
            result = EntityUtils.toString(response.getEntity());
            logger.debug("Response Body: " + result);
        }
        return result;
	}
	
}
