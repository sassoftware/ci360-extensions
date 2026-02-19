/******************************************************************************/
/* Copyright � 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%macro create_migration_ddl;

	%let code_file_path=&codes_path.&slash.mig_&database._V&previous_schema_version._to_V&schema_version..sas;

	%get_authentication_token;

	%get_udm_schema(api_schema_version=&previous_schema_version., schema_table=schema_details_previous, partitioning_table=partitioning_table_previous); 

	%get_udm_schema(api_schema_version=&schema_version., schema_table=schema_details, partitioning_table=partitioning_table); 
	%get_primary_keys(schema_table=schema_details, partitioning_table=partitioning_table, key_table=key_table);
	%add_column_metadata(schema_table=schema_details, database=&database, metadata_table=column_metadata);

	
	/* Existing tables will be altered */
	proc sql;
		create table existing_tables as 
		select distinct table_name 
		from schema_details_previous
	quit;
	
	/* Identify new tables vs new columns */
	proc sql;
		CREATE TABLE new_tables_and_columns AS
		SELECT n.*,
            case 
				when p.column_name is null and n.table_name not in (select table_name from existing_tables ) then "new table" 
                when p.column_name is null then "new column"
                else "no update" end as upgrade_activity length=10
		FROM column_metadata n
		LEFT JOIN schema_details_previous p 
			ON n.table_name = p.table_name and n.column_name = p.column_name
	 	WHERE n.column_name ne p.column_name
		ORDER BY n.table_name, n.column_name
		;
	quit;

	/* Use existing DDL logic for new tables */
	%if %sysfunc(fileexist("&code_file_path.")) ge 1 %then %do;
		%let rc=%sysfunc(filename(temp,"&code_file_path."));
		%let rc=%sysfunc(fdelete(&temp));
		%put Old &code_file_path. deleted...;
	%end;

	filename codeout DISK "&code_file_path.";
	data _null_;
		file codeout;
		put	'/*******************************************************************************/';
		put '/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */';
		put	'/* SPDX-License-Identifier: Apache-2.0                                         */';
		put	'/* *****************************************************************************/';
	run;
	filename codeout;

	/* CREATE new tables */
	proc sql;
		create table new_table_list as
		select distinct upcase(n.table_name) as table_name, k.key_list
		from new_tables_and_columns n
		inner join key_table k
		on lowcase(n.table_name) = lowcase(k.table_name)
		where n.upgrade_activity= "new table";
	quit;

	filename sascode temp;
	data _null_;
		set new_table_list;
		file sascode;
		put '%create_ddl(database=&database., table_name=' table_name ', column_table=column_metadata, key_list="' key_list +(-1) '");';
	run;
	%include sascode;
	filename sascode;

	/* ALTER tables */
	proc sql;
		create table add_columns as 
 		select n.table_name
			,n.column_name
			, n.rdbms_column_type as data_type
		from new_tables_and_columns n
		where n.upgrade_activity ? "new column";
	quit;

	filename codeout DISK "&code_file_path.";
	data _null_;
	    set add_columns;
	    by table_name;

	    file codeout mod;

	    if first.table_name then do;
	        put; /* blank line */
	        put "PROC SQL;";
	        put 'CONNECT TO &database. (&sql_passthru_connection);';
	    end;

	    /* One EXECUTE block per column */
	    put +3 'EXECUTE (ALTER TABLE &dbschema..' table_name 
	        ' ADD ' column_name ' ' data_type ' NULL) BY &database.;';

	    if last.table_name then do;
	        put 'DISCONNECT FROM &database.;';
	        put 'QUIT;';
	        PUT '%err_check (Failed to create Table: ' "&table_name., &table_name.);";
	    end;
	run;

    filename codeout;

%mend create_migration_ddl;

