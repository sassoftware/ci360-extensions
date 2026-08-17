/******************************************************************************/
/* Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/

/* ---- Log verbosity ---- */
OPTIONS NOTES MPRINT;                  /* regular logging */
/* OPTIONS MLOGIC MPRINT SYMBOLGEN;   /* debug logging - SAS side */
OPTIONS SASTRACE=',,,' SASTRACELOC=SASLOG;  /* regular logging */
/* OPTIONS SASTRACE=',,,d' SASTRACELOC=SASLOG; /* debug - database side */
OPTIONS NOFULLSTIMER;                  /* regular logging */
/* OPTIONS FULLSTIMER MSGLEVEL=I;     /* performance logging */

%let slash = /;  * Set to / for Linux or \ for Windows;

/* ---- Tenant configuration ---- */
%let DSC_TENANT_ID   = %str(XXXXXXXXXXXXXXXXXXX);
%let DSC_SECRET_KEY  = %str(ODM1NTXXXXXXXXXXXXXZmNjXXXXXXNGdn);
%let External_gateway = https://<external gateway host>/marketingGateway;

/* ---- Schema version ---- */
%let schema_version          = 22;
%let previous_schema_version = 21;  * Used only to create a migration script;

/* ---- Execution options ---- */
%let include_CDM = 0;
%let include_dbtReport = 1;
/* future: %let include_partitioning_logic=1; */

/* ---- Load settings ---- */
%let DB_BL_THRESHOLD   = 100000;              * Apply bulkload when row count exceeds this 0 = no bulkload; 
%let DB_LD_OPTS        = %str(INSERTBUFF=32767 DBCOMMIT=0);  * Alternative to bulkload;

/* ---- Path configuration ---- */
%let UtilityLocation       = /<path to utility location>/cdm-udmloader-sas;  * Root path of this utility;
%let DownloadUtilityLocation = /<path to download utility>/ci360-download-client-sas;       * Download utility root path;
%let codes_path            = &UtilityLocation.&slash.code;  * Path for DDL and ETL code;

/* ---- Log location ---- */
PROC PRINTTO
    LOG="&UtilityLocation.&slash.logs&slash.udm_&sysparameter._%left(%sysfunc(datetime(),B8601DT15.)).log";
RUN;
%put ******** UDM Utility START AT %sysfunc(datetime(),E8601DT25.)  ********* ;

/* ===========================================================================
   Database connection parameters - uncomment the section for your engine
   =========================================================================*/

/* ---- SQL Server / Azure SQL ---- */
/*
%let dbengine         = sqlsvr;
%let dbDataSrcName    = CISQL;
%let dbschema         = dbo;
%let dbuser           = UDMLOAD;
%let dbpass           = "PASS123";
%let sql_passthru_connection = %str(user=&dbuser. pass=&dbpass. DATASRC=&dbDataSrcName.);     * SQL Server;
%let sql_passthru_connection = %str(noprompt="uid=&dbuser;pwd=&dbpass;dsn=&dbDataSrcName;");  * Azure SQL;
%let trg_lib_attrib   = &sql_passthru_connection. schema=&dbschema.;
%let DB_BL_OPTS       = %str(BULKLOAD=Yes);
*/

/* ---- Oracle ---- */
/*
%let dbengine  = Oracle;
%let dbpath    = CIORA;
%let dbschema  = UDMLOADTEST;
%let dbuser    = UDMLOADTEST;
%let dbpass    = "PASS123";
%let sql_passthru_connection = %str(path=&dbpath. USER=&dbuser. PASSWORD=&dbpass.);
%let trg_lib_attrib          = &sql_passthru_connection. schema=&dbschema.;
%let DB_BL_OPTS              = %str(BULKLOAD=Yes);
*/

/* ---- Amazon Redshift ---- */
/*
%let dbengine  = REDSHIFT;
%let dbserver  = "<server>.redshift.amazonaws.com";
%let dbport    = 5439;
%let dbname    = UDMUSER123;
%let dbschema  = UDMUSER123;
%let dbuser    = UDMUSER123;
%let dbpass    = "{SAS002}xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
%let sql_passthru_connection = %str(SERVER=&dbserver. PORT=&dbport. DATABASE=&dbname. USER=&dbuser. PASSWORD=&dbpass.);
%let trg_lib_attrib          = &sql_passthru_connection. schema=&dbschema.;
%let DB_BL_OPTS = %str(BULKLOAD=Yes
    BL_BUCKET='redshift bucket'
    BL_KEY=AXXXXXXXXXXXXTWI
    BL_SECRET='JH4k/d8XXXXxxxxXXXXtSWd+WD4EI'
    BL_DEFAULT_DIR='/tmp'
    BL_REGION='us-east-1');
*/

/* ---- Postgres ---- */
/*
%let dbengine  = Postgres;
%let dbserver  = localhost;
%let dbport    = 5432;
%let dbname    = postgres;
%let dbschema  = UDMSCHEMA123;
%let dbuser    = UDMUSER123;
%let dbpass    = "{SAS005}xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
%let sql_passthru_connection = %str(SERVER=&dbserver. PORT=&dbport. USER=&dbuser. PASSWORD=&dbpass. DATABASE=&dbname.);
%let trg_lib_attrib          = &sql_passthru_connection. schema=&dbschema.;
%let DB_BL_OPTS = %str(BULKLOAD=Yes BL_PSQL_PATH='C:\Program Files\PostgreSQL\17\bin\psql.exe');
*/

/* ---- BigQuery ---- */
/*
%let dbengine    = bigquery;
%let PROJECT     = PROJECT123;
%let dbschema    = UDMSCHEMA123;
%let project_list = PROJECTLIST123;
%let cred_path   = /<bigquery credentials json file path>.json;

%let sql_passthru_connection = %str(
    project="&PROJECT"
    schema="&dbschema"
    cred_path="&cred_path"
    project_list="&project_list"
);

%let trg_lib_attrib = &sql_passthru_connection.;
%let DB_BL_OPTS     = %str(BULKLOAD=YES BL_DEFAULT_DIR='/userdata/cdm-udmloader-sas/');
*/

/* ---- Temporary schema (same as target by default) ---- */
%let tmpdbschema    = &dbschema.;
%let tmp_lib_attrib = &sql_passthru_connection. schema=&tmpdbschema.; * BIGQUERY: Remove schema option. ;


/* ---- Data library assignments ---- */
%let trglib  = target;  * Target library name;
%let tmplib  = tmplib;  * Temporary library name;
%let udmmart = udmmart; * Source data library name;

LIBNAME &trglib.  &dbengine. &trg_lib_attrib.;                                * Target database;
LIBNAME &tmplib.  &dbengine. &tmp_lib_attrib.;                                * Temporary database location;
LIBNAME &udmmart. "&DownloadUtilityLocation.&slash.data&slash.dscwh";         * Source downloaded data;
LIBNAME cdmcnfg   "&UtilityLocation.&slash.config";                           * Configuration tables;

/* ---- Common settings ---- */
%let database           = %upcase(&dbengine.);
%let verbose            = 0;  * Set to 1 for increased logging;
%let errFlag            = 0;  * Initialise error flag;
%let format             = datetime27.6;  * Preferred datetime format (pre-SV20 logic);
%let ignore_duplicate_err = 0;  * 0 = stop on duplicate key 1 = ignore and continue;

OPTIONS SASAUTOS=(SASAUTOS, "&UtilityLocation.&slash.macros&slash.");
