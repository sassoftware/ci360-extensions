/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
package com.sas.incubation.ci.agent;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.stereotype.Component;

import com.sas.mkt.agent.sdk.CI360Agent;
import com.sas.mkt.agent.sdk.CI360AgentException;
import com.sas.mkt.agent.sdk.CI360StreamInterface;
import com.sas.mkt.agent.sdk.ErrorCode;

import com.sas.incubation.ci.agent.entities.ContactHistory;
import com.sas.incubation.ci.agent.impl.CHEventConsumer;
import com.sas.incubation.ci.agent.impl.EventProcessor;
import com.sas.incubation.ci.agent.repository.BatchContactHistoryRepository;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class Agent implements ApplicationRunner {
	
	public static final String STATS_EVENT_COUNT = "TotalEventCount";
	
	@Value("${ci360.gatewayHost}")
	private String ci360gatewayHost;

	@Value("${ci360.tenantID}")
	private String ci360tenantID;
	
	@Value("${ci360.clientSecret}")
	private String ci360clientSecret;
	
	@Value("${agent.monitorPrintInterval}")
	private int monitorPrintInterval;
	
	@Value("${agent.keepaliveInterval}")
	private int keepaliveInterval;
	
	@Value("${agent.insert.list.size}")
	int listSize;
	
	private static ConcurrentHashMap<String, Integer> statsHashMap = new ConcurrentHashMap<String, Integer>();
	private static ConcurrentLinkedQueue<ContactHistory> queue = new ConcurrentLinkedQueue<ContactHistory>();
	static private AtomicBoolean alreadySeenStreamClosedCall = new AtomicBoolean(false);

	@Autowired
	BatchContactHistoryRepository repo;

	static boolean exiting = false;
	
	ExecutorService eventProcessor = Executors.newCachedThreadPool(new CustomizableThreadFactory("eventProcessor-"));
	ExecutorService eventConsumer = Executors.newCachedThreadPool(new CustomizableThreadFactory("eventConsumer-"));
	
	final CI360Agent agent = new CI360Agent(ci360gatewayHost,ci360tenantID,ci360clientSecret);

	@Override
	public void run(ApplicationArguments args) throws Exception {
		

		try {
			final CI360Agent agent = new CI360Agent(ci360gatewayHost,ci360tenantID,ci360clientSecret);
			CI360StreamInterface streamListener = new CI360StreamInterface() {
				public boolean processEvent(final String event) {
					eventProcessor.execute(new EventProcessor(event, queue, statsHashMap));
					return true;
				}

				public void streamClosed(ErrorCode errorCode, String message) {
					if (exiting) {
						log.info("Stream closed");
					} else {
						log.info("Stream closed {}: {}", errorCode, message);
						if ((message != null) && (
								message.contains("MKTCMN74224") || // incorrect JWT (bad format)
								message.contains("MKTCMN74248") || // tenant missing (unknown tenant. maybe using wrong stack)
								message.contains("MKTCMN74261") || // invalid JWT (doesn't match any access points)
								message.contains("MKTCMN74265") || // agent out of date (version of API not supported by extapigw
								message.contains("MKTCMN74282")    // tenant is not licensed
						)) {
							System.exit(-1);
						}
						if (alreadySeenStreamClosedCall.compareAndSet(false, true)) {
							try {
								Thread.sleep(5000);
							} catch (InterruptedException e) {
								log.error("Thread interrupted");
							}
							alreadySeenStreamClosedCall.set(false);
							try {
								// Try to reconnect to the event stream.
								agent.startStream(this, true);
							} catch (CI360AgentException e) {
								log.error("ERROR {}: {}", e.getErrorCode(), e.getMessage());
							}
						}
					}
				}
				
			};
			agent.startStream(streamListener, true);
			log.info("Agent started");
			
			Thread consumerManager = new Thread("consumerManager") {
				public void run() {
					log.debug("Starting consumerManager thread");

					while (true) {
						if (!queue.isEmpty()) {
							log.debug("Start EventConsumer Thread");
							eventConsumer.execute(new CHEventConsumer(queue, repo, listSize));
						}
						try {
							int threads = getNumberOfThreads(eventConsumer);
							if (threads == 0) {
								Thread.sleep(1000);
							} else if (!exiting){
								Thread.sleep(60000 * threads);
							}
						} catch (InterruptedException e) {
							log.error("Thread interrupted");
						}
					}
				}
			};
			consumerManager.start();
			
			Thread monitorThread = new Thread("MonitorThread") {
				public void run() {
					log.debug("Starting monitor thread");
					
				    ThreadPoolExecutor poolReceiver = (ThreadPoolExecutor) eventProcessor;
				    ThreadPoolExecutor poolConsumer = (ThreadPoolExecutor) eventConsumer;

				    while (true) {
						log.trace("Monitor thread sleeping " + monitorPrintInterval + " ms");
						try {
							Thread.sleep(monitorPrintInterval);
						} catch (InterruptedException e) {
							log.error("Thread interrupted");
						}
						
						log.debug("eventReceiver-pool: Largest executions: " + poolReceiver.getLargestPoolSize() + 
								", Current threads in pool: " + poolReceiver.getPoolSize() + 
								", Currently executing threads: " + poolReceiver.getActiveCount());
						log.debug("eventConsumer-pool: Largest executions: " + poolConsumer.getLargestPoolSize() + 
								", Current threads in pool: " + poolConsumer.getPoolSize() + 
								", Currently executing threads: " + poolConsumer.getActiveCount());
						
						log.info("Event count: " + statsHashMap.getOrDefault(STATS_EVENT_COUNT, 0));
						log.debug("Event stats (full): " + statsHashMap.toString());
					}
				}
			};
			monitorThread.start();
			log.info("Monitor thread started");

			Thread keepaliveThread = new Thread("KeepaliveThread") {
				public void run() {
					log.debug("Starting keepalive thread");
					while (true) {
						log.trace("Keepalive thread sleeping " + keepaliveInterval + " ms");
						try {
							Thread.sleep(keepaliveInterval);
						} catch (InterruptedException e) {
							log.error("Thread interrupted");
						}
						
						try {
							log.debug("Pinging gateway");
							log.debug("Ping response: " + agent.healthcheck());
						} catch (CI360AgentException e) {
							log.error("CI360AgentException",e);
						}
					}
				}
			};
			keepaliveThread.start();
			log.info("Keepalive thread started");

		} catch (CI360AgentException e) {
			log.error("CI360AgentException",e);
		}
	}
	
    @PreDestroy
    public void preDestroy() {
    	log.info("Agent exiting.");
    	exiting = true;
    	agent.stopStream();
    	log.info("Agent stream stopped!");
    	
    	while(getNumberOfThreads(eventConsumer) > 0) {
    		log.info("There are still running consumers. Waiting...");
    		try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				log.error("",e);
			}
    	}
    	log.info("No more running consumers. Byebye!");
    }
	
	private int getNumberOfThreads(ExecutorService pool) {
		if (pool instanceof ThreadPoolExecutor) {
			return ((ThreadPoolExecutor) pool).getActiveCount();
		}
		return 0;
	}
}
