/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package client.ci360.tasks;

/**
 *
 * Modified 7/29/2025
 *    Replaced deprecated JWT generation methods
 * @author sas
 */
import java.net.URI;
import java.util.Base64;
import java.util.ResourceBundle;

import org.json.JSONObject;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import io.jsonwebtoken.Jwts;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.util.concurrent.TimeUnit;
import javax.crypto.spec.SecretKeySpec;
public class TasksApi {
    private static final Logger logger = (Logger) LoggerFactory.getLogger(TasksApi.class);

    private static String bearerToken;
    private static String gatewayHost;
    private static URI tasksUri;
    private static int maxRetries;
    private static int httpTimeoutSeconds;
        
    private static Key getSigningKey(String secret) {
        byte[] keyBytes = Base64.getEncoder().encode(secret.getBytes(StandardCharsets.UTF_8));
        return new SecretKeySpec(keyBytes, "HmacSHA256");
    }
    
    private static String doGenerateToken(String tenantID, String clientSecret) {       
        return Jwts.builder()
            .claim("clientID", tenantID)
            .signWith(getSigningKey(clientSecret)) 
            .header()
            .type("JWT") 
            .and()
            .compact();
    }        

    static void init(ResourceBundle config) throws Exception {
        gatewayHost = config.getString("ci360.gatewayHost");
        String tenantID = config.getString("ci360.tenantID");
        String clientSecret = config.getString("ci360.clientSecret");
        maxRetries = Integer.parseInt(config.getString("task.httpRetries"));
        httpTimeoutSeconds = Integer.parseInt(config.getString("task.httpTimeoutSeconds"));

        String token = doGenerateToken(tenantID, clientSecret);
        bearerToken = "Bearer " + token;
        logger.trace("token: {}", token);

        tasksUri = new URI(String.format("https://%s/marketingDesign/tasks?state=active&limit=9999", gatewayHost));
    }
    
    static JSONObject getTaskJson(String taskId) throws Exception {
            return getJson(String.format("https://%s/marketingDesign/tasks/%s", gatewayHost, taskId));
    }
   
       
        private static String gethttp(String uri, String bearerToken, int maxRetries) {
            HttpURLConnection connection = null;
            StringBuilder response = null;
            try {
                URL obj = new URL(uri);
                int responseCode = 0;
                int retries = 0;

                while (((responseCode == 0) || (responseCode > 200)) & retries <= maxRetries) {
                    connection = (HttpURLConnection) obj.openConnection();

                    // Set request method and headers
                    connection.setRequestMethod("GET");
                    connection.setConnectTimeout(5000);
                    connection.setRequestProperty("Authorization", "Bearer " + bearerToken);

                    // Read response
                    responseCode = connection.getResponseCode();

                    InputStreamReader streamReader = null;
                    if (responseCode > 299) {
                        streamReader = new InputStreamReader(connection.getErrorStream());
                    } else {
                        streamReader = new InputStreamReader(connection.getInputStream());
                    }

                    BufferedReader in = new BufferedReader(streamReader);
                    String inputLine;
                    response = new StringBuilder();
                    while ((inputLine = in.readLine()) != null) {
                        response.append(inputLine);
                    }

                    retries = retries + 1;
                    if (retries <= maxRetries) {
                        if (responseCode > 299) {
                            logger.error("Response: " + ":" + response.toString());
                            TimeUnit.SECONDS.sleep(10);
                        }
                    }
                    in.close();
                    connection.disconnect();
                }               
            } catch (Exception e) {
                logger.error("Error at http.get", e);
                if (connection != null) {
                    connection.disconnect();
                }
             }

            return response.toString();
        }

    private static JSONObject getJson(String uri) throws Exception {
        
            String strResponse = gethttp(uri, bearerToken, 10);            
            JSONObject json = new JSONObject(strResponse);
            if (json == null)
                    throw new Exception(String.format("Failed getting JSON from %s", uri));
            return json;
    }
    
}

