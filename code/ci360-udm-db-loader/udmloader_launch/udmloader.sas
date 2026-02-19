options mlogic mprint symbolgen;
/******************************************************************************/
/* Copyright(c)2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/

/* This version of the udmloader code is for schema V20 or later when metadata  */
/* history tables are available and a full load of metadat tables is sufficient. */

%macro udmloader;
	%let sysparameter=CREATEDDL; * REMOVE TO RUN IN BATCH! ;

	%include "/userdata/cdm-udmloader-sas/config/config.sas";

	%let database=%upcase(&database.);

	      %if "&sysparameter." = "CREATEDDL"     %then %create_main(database=&database., schema_version=&schema_version., DDL=1);
	%else %if "&sysparameter." = "EXECUTEDDL"    %then %do; %include "&codes_path.&slash.&database._V&schema_version._DDL.sas"; %end;
	%else %if "&sysparameter." = "CREATEETLCODE" %then %create_main(database=&database., schema_version=&schema_version., DDL=0);

	%else %if "&sysparameter." = "LOADDATA"      %then %do; %include "&codes_path.&slash.&database._V&schema_version._ETL.sas"; %end;

	%else %if "&sysparameter." = "CREATEMIGR"    %then %create_migration_ddl;
	%else %if "&sysparameter." = "MIGRATEUDM"    %then %do; %include "&codes_path.&slash.mig_&database._V&previous_schema_version._to_V&schema_version..sas"; %end;
	%else %do;
		%put Inappropriate parameter &sysparameter.; 
	%end;

	%put ******** UDM Utility ENDS HERE *********;
	%put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg.;

	/*	STOP PRINTING LOG  */
	proc printto; run;
	%put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg.;
%mend udmloader;

%udmloader;
