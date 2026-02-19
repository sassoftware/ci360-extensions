/*******************************************************************************/
/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                         */
/* *****************************************************************************/

/* Not a macro. Just a trick to drop the whole UDM during development */
%let sysparameter=DROPDDL; 
%include "C:\sas\ci360-udm-db-loader\config\config.sas"; 

%get_udm_schema(api_schema_version=&schema_version, schema_table=schema_details, partitioning_table=partitioning_table); 
%get_primary_keys(schema_table=schema_details, partitioning_table=partitioning_table, key_table=key_table);

%let code_file_path=&codes_path.&slash.DROP_V&schema_version._DDL.sas;
filename ddlfile "&code_file_path.";
data _null_;
	set key_table end=last;
    file ddlfile ; 
	if _n_=1 then do;
	    PUT  'PROC SQL ;';
	    PUT +3 'CONNECT to &database. (&sql_passthru_connection.);';       
	end;			   
	PUT +3 'EXECUTE (DROP TABLE &dbschema..' table_name ') by &database.;';
	if last then do;
	    PUT +3 'DISCONNECT FROM &database.;';
	    PUT 'QUIT;';
	end;			   
run;
filename ddlfile;

proc printto;run;