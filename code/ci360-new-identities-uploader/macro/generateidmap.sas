/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%macro GenerateIDMap();
	%local msg recs_orig recs_new recs;
	%let rc = 0;

	%Log(1, %nrstr(%GenerateIDMap), **********************************************, 0);
	%Log(1, %nrstr(%GenerateIDMap), Generating pending identity records ..., 0);
	%Log(1, %nrstr(%GenerateIDMap), **********************************************, 0);

	/* ************************************************************** */
	/* CREATE THE LIST TO PROCESS
	/* ************************************************************** */

	/* check how many are already there so we know later what was added */
	PROC SQL NOPRINT;
		SELECT 	count(*)
		INTO	:recs_orig trimmed
		FROM	&sas_db_libref_idmap..CI360_IDMAP_PENDING
		WHERE	status = 'PENDING';
	QUIT;
	%PUT NOTE: &=recs_orig;
	%Log(2, %nrstr(%GenerateIDMap), Table contains %trim(&recs_orig.) pending records already., 0);

	/* add the new records */
	PROC SQL NOPRINT;
		INSERT INTO &sas_db_libref_idmap..CI360_IDMAP_PENDING (subject_id, customer_id, added_dttm, status)
		SELECT
			subject_id,
			customer_id,
			SYSDATE,
			'PENDING'
		FROM
			&sas_db_libref_idmap..&MTABLE
		WHERE
			subject_id NOT IN (SELECT subject_id FROM &sas_db_libref_idmap..CI360_IDMAP_PENDING) AND
			subject_id NOT IN (SELECT subject_id FROM &sas_db_libref_idmap..CI360_IDMAP_PROCESSED)
		;
	QUIT;

	%if (%eval(&SQLRC <= 4)) %then %do;
		%Log(2, %nrstr(%GenerateIDMap), Succesfully inserted the new list of pending IDs., 0);
	%end;
	%else %do;
		%let msg = Failed to create the list of new IDs to be processed: &SQLRC;
		%goto exit_fail;
	%end;

	/* count how many now */
	PROC SQL NOPRINT;
		SELECT 	count(*)
		INTO	:recs_now
		FROM	&sas_db_libref_idmap..CI360_IDMAP_PENDING
		WHERE	status = 'PENDING';
	QUIT;

	%let recs = %eval(&recs_now - &recs_orig);

	%Log(1, %nrstr(%GenerateIDMap), **********************************************, 0);
	%Log(1, %nrstr(%GenerateIDMap), Succesfully added &recs to the list to be processed., 0);
	%Log(1, %nrstr(%GenerateIDMap), **********************************************, 0);

	%let rc = 1;

	%goto exit_end;

%exit_fail:
	%Log(0, %nrstr(%GenerateIDMap), **********************************************, 0);
	%log(0, %nrstr(%GenerateIDMap), Processing failed: &msg.);
	%Log(0, %nrstr(%GenerateIDMap), **********************************************, 0);

	%let rc = 0;

%exit_end:

%mend GenerateIDMap;
