/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/

%macro execute_ddl;
	%put "Executing DDL Script";
	%if %upcase("&database.") eq "ORACLE" %then %do;
	%include "&UtilityLocation.&slash.code&slash.orclddl.sas";
	%end;
	%if %upcase("&database.") eq "TERADATA" %then %do;
	%include "&UtilityLocation.&slash.code&slash.trdtddl.sas";
	%end;
	%if %upcase("&database.") eq "REDSHIFT" %then %do;
	%include "&UtilityLocation.&slash.code&slash.rdsftddl.sas";
	%end;
	%if %upcase("&database.") eq "DB2" %then %do;
	%include "&UtilityLocation.&slash.code&slash.db2ddl.sas";
	%end;
	%if %upcase("&database.") eq "GREENPLM" %then %do;
	%include "&UtilityLocation.&slash.code&slash.grnplddl.sas";
	%end;
	%if %upcase("&database.") eq "MSSQL" %then %do;
	%include "&UtilityLocation.&slash.code&slash.MSSQLDDL.sas";
	%end;
	%if %upcase("&database.") eq "AZURE" %then %do;
	%include "&UtilityLocation.&slash.code&slash.AZUREDDL.sas";
	%end;
	%if %upcase("&database.") eq "POSTGRES" %then %do;
	%include "&UtilityLocation.&slash.code&slash.PSTGDDL.sas";
	%END;

%mend execute_ddl;
/*%execute_ddl;*/
