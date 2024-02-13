/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
package com.sas.incubation.ci.agent.repository;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.sas.incubation.ci.agent.entities.ContactHistory;

@Repository
public class BatchContactHistoryRepository implements ContactHistoryRepository {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Value("${agent.insert.batch.size}")
	int batchSize;

	@Transactional
	@Override
	public void saveAll(List<ContactHistory> historys) {
		jdbcTemplate.batchUpdate(
				"INSERT INTO ContactHistory (datahub_id, subject_id, contact_id, contact_dttm_utc, task_id, channel_type) VALUES (?, ?, ?, ?, ?, ?)",
				historys, batchSize, (PreparedStatement ps, ContactHistory history) -> {
					ps.setString(1, history.getDatahub_id());
					ps.setString(2, history.getSubject_id());
					ps.setString(3, history.getContact_id());
					ps.setTimestamp(4, Timestamp.valueOf(history.getContact_dttm_utc()));
					ps.setString(5, history.getTask_id());
					ps.setString(6, history.getChannel_type());
				});
	}
}