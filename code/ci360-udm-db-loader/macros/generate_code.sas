/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
OPTION SPOOL;

%macro generate_code;
	%if %upcase("&database.") eq "ORACLE" %then
		%let filenm=orclcode;
	%else %if %upcase("&database.") eq "TERADATA" %then
		%let filenm=trdtcode;
	%else %if %upcase("&database.") eq "REDSHIFT" %then
		%let filenm=rdstcode;
	%else %if %upcase("&database.") eq "DB2" %then
		%let filenm=db2code;
/*	%else %if %upcase("&database.") eq "AZURE" %then*/
/*		%let filenm=azcode;*/
	%else %if %upcase("&database.") eq "MSSQL" or %upcase("&database.") eq "AZURE" %then %do;
		%let filenm=mscode;
		%end;
	%else %if %upcase("&database.") eq "GREENPLM" %then
		%let filenm=grnpcode;
	%else %if %upcase("&database.") eq "POSTGRES" %then
		%let filenm=pstgcode;
	%else %put Database: %upcase("&database.") support not available...;
	%put *******open_macro_for_code_gen********;

	%if  "&sysparameter." = "CREATEETLCODE" %then
		%do;
			%let code_file_path=&cdmcodes_path.&slash.%upcase(&database.).sas;
			%if %sysfunc(fileexist("&code_file_path.")) ge 1 %then
				%do;
					%let rc=%sysfunc(filename(temp,"&code_file_path."));
					%let rc=%sysfunc(fdelete(&temp));
					%put Old Code file deleted...;
				%end;

			filename %upcase(&database.) DISK "&code_file_path.";

			data _null_;
				file %upcase(&database.) mod;
				put '%macro execute_'%trim("&database.")'_code;';
			run;

			filename _all_ list;
		%end;

	%if  "&sysparameter." = "CREATEDDL" %then
		%do;
			%let code_file_path=&cdmcodes_path.&slash.%upcase(&database.)DDL.sas;
			%if %sysfunc(fileexist("&code_file_path.")) ge 1 %then
				%do;
					%let rc=%sysfunc(filename(temp,"&code_file_path."));
					%let rc=%sysfunc(fdelete(&temp));
					%put Old Code &database. DDL file deleted...;
				%end;
		%end;

	%put ****** generate_code *******;
	filename sascode temp;

	data _null_;
		set cdmcnfg.table_list;
		file sascode;
		where execution_flag='Y';
		put '%generate_table_code(' table_name ',' mart_type ');';
	run;

	%include sascode;
	filename sascode;
	%put ******** close_macro_for_code_gen********;

	%if  "&sysparameter." = "CREATEETLCODE" %then
		%do;
			filename %upcase(&database.) DISK "&code_file_path.";

			data _null_;
				file %upcase(&database.) mod;
				put+1 '%mend execute_'%trim("&database.")'_code;';

				put+1 '%execute_'%trim("&database.")'_code;';
			run;

		%end;
%mend generate_code;
