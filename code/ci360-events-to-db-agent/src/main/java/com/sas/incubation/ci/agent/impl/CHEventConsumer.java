/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
package com.sas.incubation.ci.agent.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.sas.incubation.ci.agent.entities.ContactHistory;
import com.sas.incubation.ci.agent.repository.BatchContactHistoryRepository;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CHEventConsumer implements Runnable {

	ConcurrentLinkedQueue<ContactHistory> queue;
	BatchContactHistoryRepository repo;
	int listSize;

	public CHEventConsumer(ConcurrentLinkedQueue<ContactHistory> queue, BatchContactHistoryRepository repo,	int listSize) {
		this.queue = queue;
		this.repo = repo;
		this.listSize = listSize;
	};

	@Override
	public void run() {
		int timerWaitMax30SecWithoutNewEvent = 0;
		int timerInsertAtLeastOncePerMinute = 0;
		int listCounter = 0;
		
		List<ContactHistory> history_list = new ArrayList<ContactHistory>();
		ContactHistory jsonEvent;

		while (true) {	
			timerWaitMax30SecWithoutNewEvent++;

			while ((jsonEvent = queue.poll()) != null) {
				listCounter++;
				timerWaitMax30SecWithoutNewEvent = 0;
				
				history_list.add(jsonEvent);
				
				if (listCounter == listSize || timerInsertAtLeastOncePerMinute >= 600) {
					log.trace("saveAll() in loop, listCounter= {}, timerWaitMax30SecWithoutNewEvent= {}, timerInsertAtLeastOncePerMinute= {}", listCounter, timerWaitMax30SecWithoutNewEvent, timerInsertAtLeastOncePerMinute);
					repo.saveAll(history_list);
					history_list.clear();
					listCounter = 0;
					timerWaitMax30SecWithoutNewEvent = 0;
					timerInsertAtLeastOncePerMinute = 0;
				}
			}

			if (timerWaitMax30SecWithoutNewEvent < 300) {
				try {
					Thread.sleep(100);
					timerInsertAtLeastOncePerMinute++;
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				continue;
			}

			if (!history_list.isEmpty()) {
				log.trace("saveAll() on empty queue, listCounter= {}, timerWaitMax30SecWithoutNewEvent= {}, timerInsertAtLeastOncePerMinute= {}", listCounter, timerWaitMax30SecWithoutNewEvent, timerInsertAtLeastOncePerMinute);
				repo.saveAll(history_list);
				history_list.clear();
				listCounter = 0;
				timerWaitMax30SecWithoutNewEvent = 0;
				timerInsertAtLeastOncePerMinute = 0;
			}

			if (queue.isEmpty()) {
				log.debug("Queue is empty, thread will be terminated");
				break;
			}
		}
	}
}
