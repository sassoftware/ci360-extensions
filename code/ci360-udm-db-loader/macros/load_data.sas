/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%macro load_data;

	%if %upcase("&database.") eq "ORACLE" %then %do;
	%include "&UtilityLocation.&slash.code&slash.&database..sas";
	%end;

	%if %upcase("&database.") eq "TERADATA" %then %do;
	%include "&UtilityLocation.&slash.code&slash.trdtcode.sas";
	%end;

	%if %upcase("&database.") eq "REDSHIFT" %then %do;
	%include "&UtilityLocation.&slash.code&slash.&database..sas";
	%end;

	%if %upcase("&database.") eq "DB2" %then %do;
	%include "&UtilityLocation.&slash.code&slash.db2code.sas";
	%end;

	%if %upcase("&database.") eq "GREENPLM" %then %do;
	%include "&UtilityLocation.&slash.code&slash.grnpcode.sas";
	%end;

	%if %upcase("&database.") eq "MSSQL" or %upcase("&database.") eq "AZURE" %then %do;
	%include "&UtilityLocation.&slash.code&slash.&database..sas";
	%end;

	%if %upcase("&database.") eq "POSTGRES" %then %do;
	%include "&UtilityLocation.&slash.code&slash.pstgcode.sas";
	%end;

%mend load_data;
/*%load_data;*/
