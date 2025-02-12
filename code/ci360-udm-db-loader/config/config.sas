/******************************************************************************/
/* Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/

%global sysparm dbname HistoryFile CDM_ErrMsg timeZone_Value;
options notes;

%let errFlag=0;
%let slash=/;
%let CDM_ErrMsg = "Sucessfull Run";

/*Tenant Configuration*/
%let DSC_TENANT_ID=%str(XXXXXXXXXXXXXXXXX);
%let DSC_SECRET_KEY=%str(XXXXXXXXXXXXXXXXXXXXXXXX);
%let External_gateway=https://<external gateway host>/marketingGateway;
%let SCHEMA_VERSION=16;



/*Path Configurations - Start*/
%let UtilityLocation=/userdata/mydir/UDMLoader; /* location where UDMLoader is placed */
%let downloadutilitylocation=/userdata/mydir/sas-data-download ; /* location where ci360 data download utility is placed */
%let cdmcodes_path=&UtilityLocation.&slash.code; /*path where Utility will generate DDL and ETL code and also pick up DDL/codes for execution*/ 
%let cdmmartloc=&downloadutilitylocation.&slash.Data; /* Location where cdm-udm downloaded data is saved */
/*Path configurations - End*/

/* Set Log Loaction */
proc printto log="&UtilityLocation.&slash.logs&slash.udmloader_%left(%sysfunc(datetime(),B8601DT15.)).log"; run;


/* ------Parameters for Database - Start-----*/
/*NOTE : Make sure to provide database details for both Target and Staging Schema */*

/* Target Schema details */;
/*%let trglib=; /* Provide Target Library name */*/
/*%let dbname=; /* Provide Database */*/
/*%let dbsrc=; /* Specifies the Microsoft SQL Server data source to which you want to connect*/*/
/*%let dbpath=; /* specifies the database driver, node, and database*/*/
/*%let dbschema=; /* provide database schema detail */*/
/*%let dbuser=;/* lets you connect to database with a user ID.*/*/
/*%let dbpass=; /*specifies the database password that is associated with your user ID.*/*/


/*/* Staging schema details */*/
/*%let tmplib=&trglib.;*/
/*%let tmpdbname=&dbname.;*/
/*%let tmpdbsrc=&dbsrc.;*/
/*%let tmpdbschema=&dbschema.;*/
/*%let tmpdbuser=&dbuser.;*/
/*%let tmpdbpass=&dbpass.;*/
/*%let tmpdbpath=&dbpath.;*/
/*%let tmpdbdns=&dbdns.;*/


/* ------Parameters for Database - End------*/;


/* Target Schema details */
%let database=MSSQL; /* ORACLE,TERADATA,REDSHIFT,DB2,AZURE,MSSQL,GREENPLM  */

%let trglib=Target; /* Provide Target Library name */
%let dbname=mydbname; /* Provide Database */
%let dbsrc=mydbsrc; /* Specifies the Microsoft SQL Server data source to which you want to connect*/
%let dbschema=mydbschema; /* provide database schema detail */
%let dbuser=mydbuser;/* lets you connect to database with a user ID.*/
%let dbpass=mydbpass; /* specifies the database password that is associated with your user ID.*/
%let dbpath=mydbpath;
%let dbdns=mydbdns;

/* Staging schema details */
%let tmplib=&trglib.;
%let tmpdbname=&dbname.;
%let tmpdbsrc=&dbsrc.;
%let tmpdbschema=&dbschema.;
%let tmpdbuser=&dbuser.;
%let tmpdbpass=&dbpass.;
%let tmpdbpath=&dbpath.;
%let tmpdbdns=&dbdns.;

/* ------Parameters for MS SQL Server - End------*/


/*Common Configurations */
%let timeZone_Value=AMERICA/NEW_YORK; /* Provide timezone specific value for convertion of datetime fields into target tables */
%let format=datetime27.6; /*Provide prefered date time format */
%let udmmart=udmmart; /*provide library name for source data */
%let ignore_duplicate_err=0 ; /* 0=stop on dupliate key; 1=ignore duplicates and continue processing. */


/* ------ Parameters for Database Bulk Load options - Start ------ */
%let DB_BL_OPTS= %str(BULKLOAD=Yes); /* Usage of bulkload option when dataset size is large */
%let DB_BL_THRESHOLD=100000; /* Set bulkload threshold to apply bulkload option when data set count exceeds the threshold value */
%let DB_LD_OPTS =%str(INSERTBUFF=32767 DBCOMMIT=0);

/* ------ Parameters for Database Bulk Load options - End ------ */


/*Assignment of Libraries*/
/*filename &urlListMap "&UtilityLocation.&slash.config&slash.urlDataList.map";*/
libname &udmmart. "&downloadutilitylocation.&slash.data&slash.dscwh";

/*Oracle Libname */
/*libname &tmplib. &tmpdbname. path=&tmpdbpath user=&tmpdbuser pass=&tmpdbpass;*/
/*libname &trglib. &dbname. path=&dbpath user=&dbuser pass=&dbpass ;*/

/*MSSQL Libname*/
libname &tmplib. &tmpdbname. DATASRC=&tmpdbsrc schema=&tmpdbschema. user=&tmpdbuser pass=&tmpdbpass;
libname &trglib.  &dbname. DATASRC=&dbsrc schema=&dbschema. user=&dbuser pass=&dbpass;

/*Azure Libname*/
/*libname &tmplib. &tmpdbname. noprompt="uid=&tmpdbuser;schema=&tmpdbschema; pwd=&tmpdbpass;dsn=&tmpdbdns;" stringdates=yes;*/
/*libname &trglib.  &dbname. noprompt="uid=&dbuser; schema=&dbschema.; pwd=&dbpass;dsn=&dbdns;";*/
/**/

/*Postgres Libname*/
/*libname &trglib. postgres server=&dbsrc port=5432 user=&dbuser password=dbpass database=mydb1;*/
/*libname &tmplib. postgres server=&dbsrc port=5432 user=&dbuser password=dbpass database=mydb1;*/

libname cdmcnfg "&UtilityLocation.&slash.config&slash.";
options sasautos=(sasautos,"&UtilityLocation.&slash.macros&slash.");


%put "Config file for CDM-UDM Loader is loaded succesfully";
