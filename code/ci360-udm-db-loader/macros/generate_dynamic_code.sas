/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%macro generate_dynamic_code;

%put database= &database. ;

%if  "&sysparameter." = "CREATEDDL" %then %do;
	%if %upcase("&database.") eq "ORACLE" %then %create_oracle_ddl;
	%else %if %upcase("&database.") eq "TERADATA" %then %create_teradata_ddl;
	%else %if %upcase("&database.") eq "REDSHIFT" %then %create_redshift_ddl;
	%else %if %upcase("&database.") eq "DB2" %then %create_db2_ddl;
/*	%else %if %upcase("&database.") eq "AZURE" %then %create_azure_ddl; */
	%else %if %upcase("&database.") eq "GREENPLM" %then %create_greenplum_ddl;
	%else %if %upcase("&database.") eq "MSSQL" or %upcase("&database.") eq "AZURE"  %then %create_mssql_ddl;	
	%else %if %upcase("&database.") eq "POSTGRES" %then	%create_postgres_ddl;
 	%else %put ERROR: Unsupported database type: &database.;
%end;

%if "&sysparameter." = "CREATEETLCODE" %then %do;
	%if %upcase("&database.") eq "ORACLE" %then %create_oracle_code;
	%else %if %upcase("&database.") eq "TERADATA" %then %create_teradata_code;
	%else %if %upcase("&database.") eq "REDSHIFT" %then %create_redshift_code;
	%else %if %upcase("&database.") eq "DB2" %then %create_db2_code;
/*	%else %if %upcase("&database.") eq "AZURE" %then %create_azure_code; */
	%else %if %upcase("&database.") eq "GREENPLM" %then %create_greenplum_code;
	%else %if %upcase("&database.") eq "MSSQL" or %upcase("&database.") eq "AZURE"  %then %create_mssql_code;
	%else %if %upcase("&database.") eq "POSTGRES" %then %create_POSTGRES_code;
	%else %put ERROR: Unsupported database type: &database.;
%end;



%mend;
