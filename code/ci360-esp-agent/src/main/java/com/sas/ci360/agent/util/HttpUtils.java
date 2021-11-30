/* Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.sas.ci360.agent.util;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
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
		return httpPost(url, postBody, contentType, headers, encoding, false);
	}
	
	public static String httpPost(String url, String postBody, String contentType, Map<String, String> headers, String encoding, boolean ignoreCert) throws Exception {		
        try {
            //build & execute post request            
        	logger.debug("Building HTTP Post request, postUrl: " + url);
            HttpPost request = new HttpPost(url);
            if (headers != null) {
            	addHeaders(request, headers);
            }

            StringEntity input = new StringEntity(postBody, encoding);
    		input.setContentType(contentType);
    		request.setEntity(input);

            String result = "";
            try (CloseableHttpClient httpClient = createHttpClient(ignoreCert);
                    CloseableHttpResponse response = httpClient.execute(request)) {
                logger.debug("Response Code: " + response.getStatusLine().getStatusCode());
                if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK && response.getStatusLine().getStatusCode() != HttpStatus.SC_CREATED) {
                	throw new Exception("Call to service failed with HTTP status code: " + response.getStatusLine().getStatusCode());
                }
                result = EntityUtils.toString(response.getEntity());
                logger.debug("Response Body: " + result);
            }
            return result;
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
	
	public static String httpPut(String url, String postBody, String contentType) throws Exception {
		return httpPut(url, postBody, contentType, null, null, false);
	}
	
	public static String httpPut(String url, String postBody, String contentType, Map<String, String> headers, String encoding, boolean ignoreCert) throws Exception {		
        try {
            //build & execute post request            
        	logger.debug("Building HTTP PUT request, putUrl: " + url);
            HttpPut request = new HttpPut(url);
            if (headers != null) {
            	addHeaders(request, headers);
            }

            StringEntity input = new StringEntity(postBody, encoding);
    		input.setContentType(contentType);
    		request.setEntity(input);

            String result = "";
            try (CloseableHttpClient httpClient = createHttpClient(ignoreCert);
                    CloseableHttpResponse response = httpClient.execute(request)) {
                logger.debug("Response Code: " + response.getStatusLine().getStatusCode());
                if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK && response.getStatusLine().getStatusCode() != HttpStatus.SC_CREATED) {
                	throw new Exception("Call to service failed with HTTP status code: " + response.getStatusLine().getStatusCode());
                }
                result = EntityUtils.toString(response.getEntity());
                logger.debug("Response Body: " + result);
            }
            return result;
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
		return httpGet(url, headers, false);
	}
	
	public static String httpGet(String url, Map<String, String> headers, boolean ignoreCert) throws Exception {
        try {
            //build & execute post request            
        	logger.debug("Building HTTP Get request, getUrl: " + url);
            HttpGet request = new HttpGet(url);
            if (headers != null) {
            	addHeaders(request, headers);
            }

            String result = "";
            try (CloseableHttpClient httpClient = createHttpClient(ignoreCert);
                    CloseableHttpResponse response = httpClient.execute(request)) {
                logger.debug("Response Code: " + response.getStatusLine().getStatusCode());
                if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK && response.getStatusLine().getStatusCode() != HttpStatus.SC_CREATED) {
                	throw new Exception("Call to service failed with HTTP status code: " + response.getStatusLine().getStatusCode());
                }
                result = EntityUtils.toString(response.getEntity());
                logger.debug("Response Body: " + result);
            }
            return result;
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

	private static CloseableHttpClient createHttpClient()
            throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException {
		return createHttpClient(false);
	}
	
	private static CloseableHttpClient createHttpClient(boolean ignoreCert)
            throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException {

		if (ignoreCert) {
	        // use the TrustSelfSignedStrategy to allow Self Signed Certificates
	        SSLContext sslContext = SSLContextBuilder
	                .create()
	                .loadTrustMaterial(new TrustSelfSignedStrategy())
	                .build();

	        // we can optionally disable hostname verification. 
	        // if you don't want to further weaken the security, you don't have to include this.
	        HostnameVerifier allowAllHosts = new NoopHostnameVerifier();
	        
	        // create an SSL Socket Factory to use the SSLContext with the trust self signed certificate strategy
	        // and allow all hosts verifier.
	        SSLConnectionSocketFactory connectionFactory = new SSLConnectionSocketFactory(sslContext, allowAllHosts);
	        
	        // finally create the HttpClient using HttpClient factory methods and assign the ssl socket factory
	        return HttpClients
	                .custom()
	                .setSSLSocketFactory(connectionFactory)
	                .build();
		}
		else {
			return HttpClients.createDefault();
		}

    }
}
