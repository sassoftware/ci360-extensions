/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */
package client.ci360.eventstreamagent;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.MissingResourceException;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;


import org.slf4j.LoggerFactory;

import com.sas.mkt.agent.sdk.CI360Agent;

import ch.qos.logback.classic.Logger;
import client.ci360.sql.SqlPool;
import client.ci360.tasks.TaskCache;

/*
Modification history
8/6/2025 - Raja M. -    Removed HttpWrapper class dependency
 */
public class EventStreamAgent {

    private static final Logger logger = (Logger) LoggerFactory.getLogger(EventStreamAgent.class);
    private static CI360Agent agent;
    private static EventStreamListener streamListener;

    private static ResourceBundle readConfig(String fileName) throws IOException {
        Reader reader = Files.newBufferedReader(Paths.get(fileName));
        ResourceBundle config = new PropertyResourceBundle(reader);
        reader.close();
        return config;
    }

    public static void main(String[] args) {
        String configFile = System.getProperty("configFile");
        try {
            /* Read config file */
            if (configFile == null) {
                throw new Exception("Missing system property 'configFile'");
            }
            ResourceBundle config = readConfig(configFile);
            String gatewayHost = config.getString("ci360.gatewayHost");
            String tenantId = config.getString("ci360.tenantID");
            String clientSecret = config.getString("ci360.clientSecret");

            /* Initialize helper classes */
            SqlPool.init(config);
            /* TaskCache.init(config);   */

            /* Start agent and listen for events */
            agent = new CI360Agent(gatewayHost, tenantId, clientSecret);
            streamListener = new EventStreamListener(agent);
            agent.startStream(streamListener, true);

            /* If interactive - wait for input */
            if (System.console() != null) {
                System.console().readLine();
                stop("console", 0);
            }
        } catch (Exception e) {
            if (e instanceof MissingResourceException) {
                logger.error(String.format("Missing property '%s' in %s", ((MissingResourceException) e).getKey(),
                        configFile));
            } else {
                logger.error("Exception " + ": " + e.getMessage());
            }
            stop("error", -1);
        }
    }

    /* called by service */
    public static void stop(String[] args) {
        stop("service", 0);
    }

    static void stop(String reason, int rc) {
        try {
            logger.info("Stop reason: '{}'", reason);
            if (streamListener != null) {
                streamListener.setExiting(true);
            }
            if (agent != null) {
                agent.stopStream();
            }
            SqlPool.stop();
            Thread.sleep(500);
            logger.info("Exit code: {}", rc);
        } catch (Exception e) {
            logger.error("Exception", e);
            rc = -1;
            logger.info("Exit code: {}", rc);
        } finally {
            System.exit(rc);
        }
    }
}
