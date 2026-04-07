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

import org.json.JSONObject;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import client.Timer;
import client.ci360.sql.SqlPool;

public class EventThread extends Thread {
	private static final Logger logger = (Logger) LoggerFactory.getLogger(EventThread.class);
	private String eventString;
	private LocalDateTime agentDttm;
	private Timer timer;

	public EventThread(String eventString, LocalDateTime agentDttm) {
		this.timer = new Timer();
		this.eventString = eventString;
		this.agentDttm = agentDttm;
	}

	public void run() {
		if (eventString.startsWith("CFG")) {
			logger.info("Config Event: {}", eventString);
		}
		else {
			try {
				JSONObject json = new JSONObject(eventString);
				JSONObject attributes = json.getJSONObject("attributes");
				Event event = new Event(attributes, agentDttm);
				logger.debug("guid: {} started: {}", event.getString("guid"), agentDttm);
                                logger.debug("Event: {}", event.toString());
				try {
					SqlPool.insertEvent(event);
				} catch (Exception e) {
					logger.error("Failed inserting event.", e);
				}

				logger.debug("Guid: {} duration: {}", event.getString("guid"), timer.msElapsed());
			} catch (Exception e) {
				logger.error("Failed processing event: {}", eventString);
			}
		}
	}
}