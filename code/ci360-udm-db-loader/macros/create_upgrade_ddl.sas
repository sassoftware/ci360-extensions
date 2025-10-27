/******************************************************************************/
/* Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%macro create_upgrade_ddl;
	%get_authentication_token;

    /* Future: Add check if &schema_version. > &previous_schema_version.*/
	%get_udm_structure_from_api(api_schema_version=&previous_schema_version., schema_table=schema_details_previous);
	%get_udm_structure_from_api(api_schema_version=&schema_version., schema_table=schema_details);
	
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
				when p.column_name is null and n.table_name not in (select table_name from existing_tables ) then "new table " 
                when p.column_name is null then "new column"
                else "no update " end as upgrade_activity
		FROM schema_details n
		LEFT JOIN schema_details_previous p 
			ON n.table_name = p.table_name and n.column_name = p.column_name
	 	WHERE n.column_name ne p.column_name
		ORDER BY n.table_name, n.column_name
		;
	quit;

	/* Use existing DDL logic for new tables */
	%let code_file_path=&cdmcodes_path.&slash.mig_&database._udm_V&previous_schema_version._to_V&schema_version..sas;
	%if %sysfunc(fileexist("&code_file_path.")) ge 1 %then
		%do;
			%let rc=%sysfunc(filename(temp,"&code_file_path."));
			%let rc=%sysfunc(fdelete(&temp));
			%put Old &code_file_path. deleted...;
		%end;
	/* CREATE new tables */
	proc sql;
		create table table_list as
		select distinct table_name, mart_type
		from new_tables_and_columns
		where upgrade_activity= "new table";
	quit;

	%let sysparameter=CREATEDDL; /* Maybe instead allow UPGRADEUDM to generate DDL as well. */
	filename sascode temp;
	data _null_;
		set table_list;
		file sascode;
		put '%generate_table_code(' table_name ',' mart_type ');';
	run;
	%include sascode;
	filename sascode;

	/* New logic for ALTER tables */
	/* Get data types from CDMCNFG.DATATYPES */
	proc sql;
		create table add_columns as 
 		select n.table_name
			,n.column_name
			, case 
				when d.rdbms_datatype in('varchar','char') then cats(d.rdbms_datatype,'(',n.data_length,')')
				else d.rdbms_datatype /* string translates to varchar. Shouldn't that become varchar(something)? */ 
				end as data_type
		from new_tables_and_columns n
		inner join CDMCNFG.DATATYPES d
		on n.data_type=d.schema_datatype 
		where n.upgrade_activity ? "new column" and upcase(d.rdbms)=upcase("&DBNAME.");
	quit;

data _null_;
    set add_columns;
    by table_name;

    file "&code_file_path" mod;

    if first.table_name then do;
        put; /* blank line */
        put "PROC SQL;";
        put 'CONNECT TO &DBNAME (&sql_passthru_connection);';
    end;

    /* One EXECUTE block per column */
    put +3 'EXECUTE (ALTER TABLE &dbschema..' table_name 
        ' ADD ' column_name ' ' data_type ' NULL) BY &DBNAME;';

    if last.table_name then do;
        put 'DISCONNECT FROM &DBNAME;';
        put 'QUIT;';
        put '%ErrCheck (Failed to alter table: ' table_name ', ' table_name ');';
    end;
run;

%mend create_upgrade_ddl;

