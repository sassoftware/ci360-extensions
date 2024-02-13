/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%macro LoadIdentities();

	%macro GetDate(sas_db_libref_idmap);
		PROC SQL;
			SELECT MAX(PROCESSED_DTTM) FORMAT=DATETIME22.3 INTO :last_update
            FROM &sas_db_libref_idmap..CI360_IDMAP_PROCESSED
			having MAX(PROCESSED_DTTM) is not null;
		QUIT;

		%put Last Update Date {&last_update};
	%mend;

	%macro GetData(sas_db_libref_cust, sas_db_libref_idmap);
		%if %sysfunc(exist(MSOURCEID)) %then
			%do;
				PROC SQL;
					DROP TABLE MSOURCEID;
				QUIT;
			%end;

		%if %length(&last_update) = 0 %then
			%do;
				PROC SQL;
					CREATE TABLE MSOURCEID AS 
						SELECT * 
						FROM &sas_db_libref_cust..INDIVIDUAL 
						ORDER BY INDIVIDUAL_KEY ASC;
				QUIT;
			%end;
		%else
			%do;
				PROC SQL;
					CREATE TABLE MSOURCEID AS 
						SELECT * 
						FROM &sas_db_libref_cust..INDIVIDUAL 
						WHERE LAST_UPDATED_TS >= "&last_update."d
						ORDER BY INDIVIDUAL_KEY ASC;
				QUIT;
			%end;
	%mend;

	%macro GetCounts(sas_db_libref_idmap);
		PROC SQL;
			SELECT 
				COUNT(*) FORMAT=12. AS TOTAL_COUNT, 
				COUNT(*) / &load_count. FORMAT=8. AS GROUP_COUNT 
				INTO :total_count, :group_count 
			FROM MSOURCEID;
		QUIT;
	%mend;

	%macro Split(sas_db_libref_idmap);
		%put Load Count {&load_count};
		%put Group Count {&group_count};

		%do i = 0 %to %eval(&group_count);
			%let MGRP = &MGRP MGRP&i;
		%end;

		%put Group {&MGRP};

		%let range_start = 1;
		%let range_end = &load_count;

		%do i = 0 %to %eval(&group_count);
			%put Range Start {&range_start};
			%put Range End {&range_end};

			%if %sysfunc(exist(&sas_db_libref_idmap..MGRP&i)) %then %do;
				PROC SQL;
					DROP TABLE &sas_db_libref_idmap..MGRP&i;
				QUIT;
			%end;

			DATA &sas_db_libref_idmap..MGRP&i;
				SET MSOURCEID(firstobs=&range_start obs=&range_end);
			RUN;

			%if %eval(i GE 1) %then %do;
				%let range_start = %eval(&range_end + 1);
				%let range_end = %eval(&range_end + &load_count);
			%end;
		%end;
	%mend;

	%macro Load();
		%put Load Count {&load_count};
		%put Group Count {&group_count};

		%do i = 0 %to %eval(&group_count);
			%let MTABLE = MGRP&i;
			%put MTABLE {&MTABLE};

			/* %include "&sas_idmapping_include_path/&sas_idmapping_process_file"; */
			%IDMappingProcess();
		%end;
	%mend;

	%LET load_count = 6000000;
	%LET group_count = 1;
	%LET last_update =;
	%LET MGRP =;

	/* %include "&sas_idmapping_include_path/&sas_idmapping_0_parameters_file"; */

	%GetDate(&sas_db_libref_idmap);
	%GetData(&sas_db_libref_cust, &sas_db_libref_idmap);
	%GetCounts(&sas_db_libref_idmap);
	%Split(&sas_db_libref_idmap);
	%Load();

%mend LoadIdentities;
