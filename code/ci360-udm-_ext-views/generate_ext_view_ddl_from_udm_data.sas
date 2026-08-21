/********************************************************************************/
/* Copyright (c) 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                          */
/********************************************************************************/
/* If you are running this you chose the COMPLICATED approach.                  */
/*      For this to work the property data must already exists in the           */
/*      UDM MD_*_CUSTOM_PROP tables.                                            */
/*      This script generates a custom_properties_ext_views.sql and             */
/*      custom_properties_ext_views.sas script for the properties in your data. */
/* The EASY approach is                                                         */
/*      to generated sample scripts available in this repository.               */
/********************************************************************************/

/* Where you want to store the generated DDL scripts */
%let scriptLocation = C:\sas\; * Where you want to store the generated DDL scripts;


/* Assign the UDM Target library */
/* ------ Postgres-specific connection details - ADAPT TO YOUR DATABASE -----*/
%let dbengine=Postgres;
%let dbserver=localhost;  
%let dbport=5432;     * needed for postgres e.g. 5432;
%let dbname=postgres; * Database instance - case sensitive;  
%let dbschema=udm;    * Schema is case sensitive;
%let dbuser=xxxxxxxx; * user; 
%let dbpass=xxxxxxxx; * lets you connect to database with a user ID; 
%let sql_passthru_connection =%str(SERVER=&dbserver. PORT=&dbport. USER=&dbuser. PASSWORD=&dbpass. DATABASE=&dbname.); * connection string for POSTGRES;

libname TARGET &dbengine. &sql_passthru_connection. schema=&dbschema.; * target DB location;
/* If data is donwloaded using ci360-download-client-sas but not uploaded to DB yet you can use this: */
/*libname TARGET "C:\sas\ci360-download-client-sas\data\dscwh";*/

/* Prepare script generation - Get existing custom property names */
proc sql; 
	create table custom_prop_columns as
	select memname as table_nm
		, name as column_nm
		, libname
	from dictionary.columns 
	where libname='TARGET' and upcase(memname) ? '_CUSTOM_PROP' and upcase(name) = 'PROPERTY_NM';
quit;


/* Get data type of each custom property */
/* This step will generate code and immediately afterwards run it */
filename sascode temp;
data _null_;
	set custom_prop_columns end=last;
	file sascode;
	if _n_=1 then do;
		put "data property_values_all; length libname object_nm table_nm prop_column_nm $32 property_nm property_datatype_nm $256; stop; run;";
	end;
	put "proc sql;";
	put +3 "create table property_values_one_table as";
	put +3 "select distinct property_nm length=256";
	if index(table_nm,'MD_TASK_CUSTOM_PROP')
		then put +6 ", property_datatype_nm length=256";
		else put +6 ", property_datatype_cd as property_datatype_nm length=256";
	put +6 ",'" libname +(-1) "' as libname length=32";
	put +6 ",'" table_nm +(-1) "' as table_nm length=32";
	put +6 ", scan(scan('" table_nm +(-1) "',2,'_'),-1,'_') as object_nm length=32";
	put +6 ", upcase(substr(translate(strip(compress(property_nm,'()')),'___','- /'),1,32)) as prop_column_nm length=32";

	put +3 "from " libname +(-1) "." table_nm ";";
	put "quit;";
	put "proc append base=property_values_all data=property_values_one_table; run;";
	put;
run;
/* Add the generated code to the log. */
data _null_; infile sascode; input; put _infile_; run;
/* Run the generate code. */
%include sascode;

/* Remove potential duplicate column names */
proc sort data=property_values_all nodupkey dupout=should_be_empty;
	by table_nm prop_column_nm;
run;
/* Log an error in case of duplicate column names. */
/* Review generated script and custom property names in case of errors. */
data _null_;
 set should_be_empty;
 put "ERROR: Property " property_nm "in table " table_nm "is not transposed because column " prop_column_nm "would not be unique. " ;
run;


/* Generate the View scripts to run in SAS code. */
filename sascode "&scriptLocation./custom_properties_ext_views.sas";
data _null_;
	set property_values_all end=last;
	length view_nm $32;
	file sascode;
	by table_nm;

	if _n_=1 then do;
		put "PROC SQL;";
		put "CONNECT to &dbengine. (&sql_passthru_connection.);";
		put;
	end;

	if first.table_nm then do;
		view_nm=cats(substr(table_nm, 1, 28),"_EXT");
		put '%if %sysfunc(exist(TARGET.' view_nm ')) %then %do;'; 
		put +3 "EXECUTE (drop VIEW &dbschema.." view_nm ") by &dbengine.;";
		put '%end;';
		put "EXECUTE (";
		put +3 "create VIEW &dbschema.." view_nm "as";
	    if substr(table_nm,length(table_nm)-3,4)='_ALL'
			then put +3 "select " object_nm +(-1) '_version_id' ;
			else put +3 "select " object_nm +(-1) '_id' ;
		put +6 ", MAX(" object_nm +(-1) "_status_cd) AS " object_nm +(-1) "_status_cd";
	end;

	put +6 ", MAX(CASE WHEN property_nm = '" property_nm +(-1) "' " @;
    if      property_datatype_nm='BOOLEAN'  then put "THEN property_val END)" @;
    else if property_datatype_nm='INTEGER'  then put "THEN CAST(property_val AS int) END)" @ ;
    else if property_datatype_nm='DOUBLE'   then put "THEN CAST(property_val AS decimal) END)" @;
    else if property_datatype_nm='DATEONLY' then put "THEN CAST(property_val AS date) END)" @ ;
    else if property_datatype_nm='DATETIME' then put "THEN CAST(property_val AS dateti�e) END)" @ ;
    else put "THEN property_val END)" @;
	put " AS " prop_column_nm;

	if last.table_nm then do;
		put +3 "from &dbschema.." table_nm ;
	    if substr(table_nm,length(table_nm)-3,4)='_ALL'
			then put +3 "group by " object_nm +(-1) '_version_id' ;
			else put +3 "group by " object_nm +(-1) '_id' ;		
		put ") by &dbengine.;";
		put;
	end;

	if last then do;
		put "DISCONNECT FROM &dbengine.;";
		put "QUIT;";
	end;
run;


/* Generate the View scripts in plain SQL. */
filename sascode "&scriptLocation./custom_properties_ext_views.sql";
data _null_;
	set property_values_all end=last;
	length view_nm $32;
	file sascode;
	by table_nm;

	if first.table_nm then do;
		view_nm=cats(substr(table_nm, 1, 28),"_EXT");
		put +3 "create VIEW &dbschema.." view_nm "as";
	    if substr(table_nm,length(table_nm)-3,4)='_ALL'
			then put +3 "select " object_nm +(-1) '_version_id' ;
			else put +3 "select " object_nm +(-1) '_id' ;
		put +6 ", MAX(" object_nm +(-1) "_status_cd) AS " object_nm +(-1) "_status_cd";
	end;

	put +6 ", MAX(CASE WHEN property_nm = '" property_nm +(-1) "' " @;
    if      property_datatype_nm='BOOLEAN'  then put "THEN property_val END)" @;
    else if property_datatype_nm='INTEGER'  then put "THEN CAST(property_val AS int) END)" @ ;
    else if property_datatype_nm='DOUBLE'   then put "THEN CAST(property_val AS decimal) END)" @;
    else if property_datatype_nm='DATEONLY' then put "THEN CAST(property_val AS date) END)" @ ;
    else if property_datatype_nm='DATETIME' then put "THEN CAST(property_val AS dateti�e) END)" @ ;
    else put "THEN property_val END)" @;
	put " AS " prop_column_nm;

	if last.table_nm then do;
		put +3 "from &dbschema.." table_nm ;
	    if substr(table_nm,length(table_nm)-3,4)='_ALL'
			then put +3 "group by " object_nm +(-1) '_version_id' ;
			else put +3 "group by " object_nm +(-1) '_id' ;		
		put ";";
		put;
	end;
run;
