/*******************************************************************************/
/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                         */
/* *****************************************************************************/

%macro get_primary_keys(schema_table=, partitioning_table=, key_table=);
	%local schema_table partitioning_table key_table; 


	/* Fill in primary keys for partitioned tables where missing */
	proc sql;
		create table Part_tables_without_primary_key as
		select table_name 
		from &partitioning_table.
		where partitioned_flg=1 and
			table_name not in (select distinct table_name from schema_details where primary_key=1);
	quit;

	%read_metadata_table_csv(IMPORTED_METADATA_TABLE=IMPORTED_METADATA_TABLE);

	proc sql; 
		create table Primary_keys_to_add as
		select lowcase(table_name) as table_name
			, lowcase(column_name) as column_name
			, 1 as primary_key_to_add
		from IMPORTED_METADATA_TABLE 
		where ispk ne "" and
			lowcase(table_name) in (select table_name from Part_tables_without_primary_key);
	quit;
	proc sql;
		create table schema_details_with_to_many_keys as
		select s.*
			, case when primary_key_to_add=1 then primary_key_to_add 
			 	when primary_key=. then 0
				else primary_key end as full_primary_key
		from schema_details s
		left join Primary_keys_to_add a
		on s.table_name=a.table_name and s.column_name=a.column_name;
	quit;
	proc sql;
		create table schema_details_corrected_keys as
		select s.*
			, p.partitioned_flg
			, case when p.partitioned_flg=0 then 0 
				else s.full_primary_key end as corrected_primary_key
		from schema_details_with_to_many_keys s
		inner join &partitioning_table. p
		on s.table_name=p.table_name;
	quit;

	%err_check(Unable to add primary keys 1, &SYSMACRONAME);
	%if &errFlag %then %do;
		%goto ERREXIT;
	%end;
	


	data schema_details (drop=corrected_primary_key full_primary_key);
	 set schema_details_corrected_keys;
		primary_key=corrected_primary_key;
	run;



	data keys (keep=mart_type table_name column_name corrected_primary_key partitioned_flg);
	 set schema_details_corrected_keys;
		table_name=upcase(table_name);
		column_name=upcase(column_name);
	run;
	
	/* This fixes an order of columns in column_list */
	proc sort data=keys;
		by mart_type table_name column_name;
	run;
	
	/* List key columns per table */
	data &key_table (drop=column_name corrected_primary_key);
		set keys;
	 	length key_list column_list $4000;
		retain key_list column_list;
	 	by mart_type table_name;
		if corrected_primary_key=1 
			then key_list    = catx(',',key_list,   column_name); 
			else column_list = catx(',',column_list,column_name); 
	 	if last.table_name then do;
	 		output;
	 		key_list="";
	 		column_list="";
	 	end;
	run;

	%err_check(Unable to add primary keys 2, &SYSMACRONAME);
	%ERREXIT: 

%mend get_primary_keys;
