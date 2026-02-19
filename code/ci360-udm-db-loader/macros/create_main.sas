/******************************************************************************/
/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.*/
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
OPTION SPOOL;

%macro create_main(database=, schema_version=, DDL=);
	%local database schema_version DDL;

	%get_udm_schema(api_schema_version=&schema_version, schema_table=schema_details, partitioning_table=partitioning_table); 
	%get_primary_keys(schema_table=schema_details, partitioning_table=partitioning_table, key_table=key_table);
	/*Fix for Schema 20 */
	data schema_details;
		set schema_details;
		if data_type='' and column_type='tinyint' then data_type='tinyint';
	run;
	/*End Fix for schema 20*/
	%add_column_metadata(schema_table=schema_details, database=&database, metadata_table=column_metadata);

	%if  &DDL. %then %do;
		%let code_file_path=&codes_path.&slash.&database._V&schema_version._DDL.sas;
	%end;
	%else %do;
		%let code_file_path=&codes_path.&slash.&database._V&schema_version._ETL.sas;
	%end;

	%if %sysfunc(fileexist("&code_file_path.")) ge 1 %then %do;
		%let rc=%sysfunc(filename(temp,"&code_file_path."));
		%let rc=%sysfunc(fdelete(&temp));
		%put Old Code file deleted...;
	%end;

	filename codeout DISK "&code_file_path.";
	data _null_;
		file codeout mod;
		put	'/*******************************************************************************/';
		put '/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */';
		put	'/* SPDX-License-Identifier: Apache-2.0                                         */';
		put	'/* *****************************************************************************/';
		%if  &DDL. = 0 %then %do;
			put '%macro execute_' "&database." '_etl;';
		%end;
	run;

	/* Limit generated code to tables listed for execution. */	
	%if %sysfunc(exist(cdmcnfg.table_list)) %then %do;
		PROC SQL;
			create table table_details as
			select k.* 
			from key_table k
			left join cdmcnfg.table_list t
			on k.table_name=upcase(t.table_name)
			where t.execution_flag ne 'N';
		quit;
	%end;
	%else %do;
		data table_details;
		 set key_table;
		run;
	%end;

filename sascod temp;
	data _null_;
		set table_details end=last;
		file sascod;

		%if  &DDL. %then %do;

			put '%create_ddl(database=&database., table_name=' table_name ', column_table=column_metadata, key_list="' key_list +(-1) '");';

		%end;

		%else %do;
			
			put '%create_&database._etl(database=&database., table_name=' table_name ', column_table=column_metadata, key_list="' key_list +(-1) '");';

		%end;

	run;

 
	%include sascod;
	%if &verbose. %then %do;
		data _null_; infile sascod;input;put _infile_;run;
	%end;
	filename sascod;

	%if  &DDL. = 0 %then %do;
		data _null_;
			file codeout mod;
			put '%mend;';
			put '%execute_' "&database." '_etl;';
		run;
	%end;

%mend create_main;
