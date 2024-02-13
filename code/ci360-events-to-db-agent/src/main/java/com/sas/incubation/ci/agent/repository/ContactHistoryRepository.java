/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
package com.sas.incubation.ci.agent.repository;

import java.util.List;

import com.sas.incubation.ci.agent.entities.ContactHistory;

public interface ContactHistoryRepository {
	 void saveAll(List<ContactHistory> history);
}
