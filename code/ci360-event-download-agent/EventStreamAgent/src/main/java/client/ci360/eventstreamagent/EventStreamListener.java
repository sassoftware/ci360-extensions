/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package client.ci360.eventstreamagent;

/**
 *
 * @author sas
 */
import java.time.LocalDateTime;

import org.slf4j.LoggerFactory;

import com.sas.mkt.agent.sdk.CI360Agent;
import com.sas.mkt.agent.sdk.CI360AgentException;
import com.sas.mkt.agent.sdk.CI360StreamInterface;
import com.sas.mkt.agent.sdk.ErrorCode;
import java.util.concurrent.atomic.AtomicBoolean;

import ch.qos.logback.classic.Logger;

public class EventStreamListener implements CI360StreamInterface {

    private static final Logger logger = (Logger) LoggerFactory.getLogger(EventStreamListener.class);
    private static AtomicBoolean alreadySeenStreamClosedCall = new AtomicBoolean(false);
    private boolean exiting;
    private CI360Agent agent;
    private int retryCount = 0;
    private final int MAX_RETRIES = 6;
    private final long RETRY_INTERVAL = 300000L;

    public EventStreamListener(CI360Agent agent) {
        this.agent = agent;
        exiting = false;
    }

    public void setExiting(boolean exiting) {
        this.exiting = exiting;
    }

    @Override
    public boolean processEvent(String eventString) throws CI360AgentException {

        Thread eventThread = new EventThread(eventString, LocalDateTime.now());
        eventThread.start();

        return true;
    }

    @Override
    public void streamClosed(ErrorCode errorCode, String message) {
        if (this.exiting) {
            logger.info("Stream closed");
        } else {
            logger.error("Stream closed {}: {}", errorCode, message);
            if ((message!=null) && (
                message.contains("MKTCMN74224") ||   // incorrect JWT (bad format)
                message.contains("MKTCMN74248") ||   // tenant missing (unknown tenant.  maybe using wrong stack)
                message.contains("MKTCMN74261") ||   // invalid JWT (doesn't match any access points)
                message.contains("MKTCMN74265") ||   // agent out of date (version of API not supported by extapigw
                message.contains("MKTCMN74282")      // tenant is not licensed
                )) {
                System.exit(-1);
            }
            if (this.retryCount < 6) {
                this.retryCount++;
                try {
                    Thread.sleep(RETRY_INTERVAL);
                } catch (InterruptedException e) {
                    logger.error("Interrupted while waiting to retry connection", e);
                }
                try {
                    logger.info("Connection reattempt number: " + this.retryCount);
                    // Try to reconnect to the event stream.
                    agent.startStream(this, true);
                    logger.info("Retry counter reset to zero.");
                    this.retryCount = 0;
                } catch (CI360AgentException e) {
                    logger.error("ERROR " + e.getErrorCode() + ": " + e.getMessage());
                }
            } else {
                logger.error("Maximum retry attempts reached. Exiting...");
            }
        }
    }
}
