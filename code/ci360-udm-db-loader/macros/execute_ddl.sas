/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/

%macro execute_ddl;
	%put "Executing DDL Script";
	%if %upcase("&database.") eq "ORACLE" %then %do;
	%include "&UtilityLocation.&slash.code&slash.&database.DDL.sas";
	%end;
	%if %upcase("&database.") eq "TERADATA" %then %do;
	%include "&UtilityLocation.&slash.code&slash.&database.DDL.sas";
	%end;
	%if %upcase("&database.") eq "REDSHIFT" %then %do;
	%include "&UtilityLocation.&slash.code&slash.&database.DDL.sas";
	%end;
	%if %upcase("&database.") eq "DB2" %then %do;
	%include "&UtilityLocation.&slash.code&slash.&database.DDL.sas";
	%end;
	%if %upcase("&database.") eq "GREENPLM" %then %do;
	%include "&UtilityLocation.&slash.code&slash.&database.DDL.sas";
	%end;
	%if %upcase("&database.") eq "MSSQL" or %upcase("&database.") eq "AZURE" %then %do;
	%include "&UtilityLocation.&slash.code&slash.&database.DDL.sas";
	%end;
	%if %upcase("&database.") eq "POSTGRES" %then %do;
	%include "&UtilityLocation.&slash.code&slash.&database.DDL.sas";
	%END;

%mend execute_ddl;
/*%execute_DDL;*/
