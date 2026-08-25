/*******************************************************************************/
/* Copyright(c) 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                         */
/* *****************************************************************************/
/* This is for the developers to refresh all codes for all DBs at once.        */

%include "<udmloader path>/config/config.sas";


%macro refresh_codes(database,schema_version,previous_schema_version);
	%local database schema_version previous_schema_version;
	PROC PRINTTO LOG="&UtilityLocation.&slash.logs&slash.udm_&database._all__%left(%sysfunc(datetime(),B8601DT15.)).log";
	RUN;
	%create_main(database=&database., schema_version=&schema_version., DDL=1);
	%create_main(database=&database., schema_version=&schema_version., DDL=0);
	%create_migration_ddl;
%mend refresh_codes;

%refresh_codes(database=BIGQUERY,schema_version=22,previous_schema_version=21);
%refresh_codes(database=SQLSVR,  schema_version=22,previous_schema_version=21);
%refresh_codes(database=ORACLE,  schema_version=22,previous_schema_version=21);
%refresh_codes(database=REDSHIFT,schema_version=22,previous_schema_version=21);
%refresh_codes(database=POSTGRES,schema_version=22,previous_schema_version=21);

proc printto; 
run;


