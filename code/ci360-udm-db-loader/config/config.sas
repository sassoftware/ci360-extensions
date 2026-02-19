/******************************************************************************/
/* Copyright � 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/

/* Adjust log detail */
options notes mprint; * regular logging;
/*options mlogic mprint symbolgen; * debug logging - sas side; */
options sastrace=',,,' sastraceloc=saslog; * regular logging;
/*options sastrace=',,,d' sastraceloc=saslog; * debug version - database side; */
options nofullstimer; * regular logging;
/* options fullstimer; * performance logging; */

%let slash=/; * Set to / for Linux or \ for Windows;

/* Tenant Configuration */
%let DSC_TENANT_ID=%str(XXXXXXXXXXXXXXXXXXXXX);
%let DSC_SECRET_KEY=%str(XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX);
%let external_gateway=https://<external gateway host>/marketingGateway;

%let schema_version=20; 
%let previous_schema_version=19; * used only to create a migration script;
%let timezone_value='AMERICA/NEW_YORK'; * Used in tzoneu2s() function to convert datetime values in _dttm_tz columns from UTC to the chosen time;
%let DB_BL_THRESHOLD=100000; * Apply bulkload when row count exceeds the threshold value - 0 means no bulkload, like for SQL Server ;
%let DB_LD_OPTS =%str(INSERTBUFF=32767 DBCOMMIT=0); * alternative for bulk;

/* Extra */ %let DevLocation=/userdata/cdm-udmloader-sas; /* Extra */

/* Paths */
%let UtilityLocation = /userdata/cdm-udmloader-sas; * root path of this utility;
%let DownloadUtilitylocation = /userdata/ci360-download-client-sas ; * Download utility root path;
%let codes_path = &DevLocation.&slash.code; * path for DDL and ETL code ;

/* Set Log Location */
proc printto log="&DevLocation.&slash.logs&slash.udm_&sysparameter._%left(%sysfunc(datetime(),B8601DT15.)).log"; run;


/* Database parameter overview - parameter usage depends on the engine */
/* ------ SQL Sever & Azure SQL-specific section -----*/
%let dbengine=sqlsvr;
%let dbDataSrcName=UDMDB123;
%let dbschema=UDMSCHEMA123;
%let dbuser=UDMUSER123;
%let dbpass="{SAS005}xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
%let sql_passthru_connection = %str(user=&dbuser. pass=&dbpass. DATASRC=&dbDataSrcName.);   * SQL SERVER;
/*%let sql_passthru_connection =%str(noprompt="uid=&dbuser;pwd=&dbpass;dsn=&dbDataSrcName;"); * AZURE SQL;*/
%let trg_lib_attrib = &sql_passthru_connection. schema=&dbschema.;
%let DB_BL_OPTS= ; * Usage of bulkload option when dataset size is large;


/* ------ Oracle-specific section -----*/
/*%let dbengine=Oracle; * database engine, as used in libname statments;*/
/*%let dbpath=UDMDB123; */
/*%let dbschema=ORA_USER;*/
/*%let dbuser=OR_USER;*/
/*%let dbpass="{SAS002}xxxxxxxxxxxxxxxxxx"; */
/*%let sql_passthru_connection =%str(path=&dbpath. USER=&dbuser. PASSWORD=&dbpass.); * Oracle;*/
/*%let trg_lib_attrib = &sql_passthru_connection. schema=&dbschema.;*/
/*%let DB_BL_OPTS= %str(BULKLOAD=Yes); * Usage of bulkload option when dataset size is large;*/


/* ------ Amazone REDSHIFT-specific section -----*/
/*%let dbengine=REDSHIFT; * database engine, as used in libname statments;*/
/*%let dbserver="<server>.redshift.amazonaws.com";    */
/*%let dbport=5439;     * Redshift defailt si 5439;*/
/*%let dbname=UDMUSER123;   * Database instance;  */
/*%let dbschema=UDMUSER123; * provide database schema detail; */
/*%let dbuser=UDMUSER123;   * database user ID; */
/*%let dbpass="{SAS002}xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";   * specifies the database password that is associated with your user ID; */
/*%let sql_passthru_connection =%str(SERVER=&dbserver. port=&dbport. DATABASE=&dbname. USER=&dbuser. PASSWORD=&dbpass.); * REDSHIFT; */
/*%let trg_lib_attrib = &sql_passthru_connection. schema=&dbschema.;*/
/*%let DB_BL_OPTS= %str(BULKLOAD=Yes 	bl_bucket='<bulk load bucket name for aws>' bl_key=<bulk load key for aws> bl_secret='<bulk load secret for aws>' bl_default_dir='/tmp' bl_region='us-east-1'); * Usage of bulkload option when dataset size is large;*/


/* ------ Postgres-specific section -----*/
/* %let dbengine=Postgres; */
/* %let dbserver=localhost;   */
/* %let dbport=5432;      * needed for postgres e.g. 5432; */
/* %let dbname=postgres;  * Database instance - case sensitive;   */
/* %let dbschema=UDMSCHEMA123; * Schema is case sensitive; */
/* %let dbuser=UDMUSER123;   * user;  */
/* %let dbpass="{SAS005}xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"; * lets you connect to database with a user ID;  */
/* %let sql_passthru_connection =%str(SERVER=&dbserver. PORT=&dbport. USER=&dbuser. PASSWORD=&dbpass. DATABASE=&dbname.); * connection string for POSTGRES; */
/* %let trg_lib_attrib = &sql_passthru_connection. schema=&dbschema.; */
/* %let DB_BL_OPTS= %str(BULKLOAD=Yes BL_PSQL_PATH='C:\Program Files\PostgreSQL\17\bin\psql.exe'); * Usage of bulkload option when dataset size is large; */


/* Temporary schema details - same as target by default */
%let tmpdbschema=&dbschema.;
%let tmp_lib_attrib = &sql_passthru_connection. schema=&tmpdbschema.;

/* taget DB libname*/
%let trglib=target; * Target library name; 
%let tmplib=tmplib; * Temporary library name;
%let udmmart=udmmart; * provide library name for source data;
libname &trglib. &dbengine. &trg_lib_attrib.; * target DB location;
libname &tmplib. &dbengine. &tmp_lib_attrib.; * temporary DB location;
libname &udmmart. "&DownloadUtilityLocation.&slash.data&slash.dscwh"; *source data;
libname cdmcnfg   "&UtilityLocation.&slash.config"; * Configuration tables ;

/* Common Configurations */
%let database=%upcase(&dbengine.);
%let verbose=0; * set to 1 for increased logging;
%let errFlag=0; * initialize to 0;
%let format=datetime27.6; * provide prefered date time format - pre-sv20 logic;
%let ignore_duplicate_err=0 ; * 0=stop on dupliate key, 1=ignore duplicates and continue processing.;
options sasautos=(sasautos,"&UtilityLocation.&slash.macros&slash.");
