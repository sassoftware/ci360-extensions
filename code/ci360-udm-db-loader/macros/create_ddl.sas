/******************************************************************************/
/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.*/
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%macro create_ddl(database=&database, table_name=,column_table=, key_list=) ;
	%local database mart_type table_name column_table key_list;

	%put ******create_&database._ddl****** &table_name;
	%let primary_key_defined=0;
	%let partition_column_name=;
	filename ddlfile DISK "&code_file_path.";
	data _null_;
		set &column_table. end=last;
		length part_col $600;
		length partition_column_name $32 partition_column_datatype $32 ;
		retain partition_column_name partition_column_datatype;
		where upcase(table_name)="&table_name.";
		
		file ddlfile mod; 
		if _n_=1 then do;
		    PUT  'PROC SQL ;';
		    PUT +3 'CONNECT to &database. (&sql_passthru_connection.);';       
			PUT +3 'EXECUTE (CREATE TABLE &dbschema..' table_name '(';
			PUT +6 @;
		end;

		if index(&key_list, strip(upcase(column_name))) ne 0 or partition_column = 1 then do;		
			comb_col = catx(' ', column_name, rdbms_column_type, 'NOT NULL');
			put comb_col @;
		end;
		else do;
			comb_col = catx(' ', column_name, rdbms_column_type, 'NULL');
			put comb_col @;
		end;
		if  not last then do;
			put  +(-1) ', ' @;
		end;
		if  mod(_n_,4)=0 then do; /* start new line evetry 4 variables */
			put;
			put +6 @;
		end;
		if partition_column = 1 then do;
	        partition_column_name= column_name;
			partition_column_datatype= rdbms_column_type;
		end;
		if last then do;
			put ')) by &database.;';
		
			if partition_column_name ne " " and &key_list ne " " then do;		
				call execute(cats('%nrstr(%add_partitioning_ddl)(', 'database=&database., ', 'table_name=&table_name., ', 'column_name=', strip(partition_column_name), ', ' , 'key_list=%superq(KEY_LIST)',', ','column_datatype=', strip(partition_column_datatype) , ');'));
			end;
		end;

		data _null_;
		set &column_table. end=last;
		where upcase(table_name)="&table_name.";
		
		file ddlfile mod; 
		if last then do;
			if &key_list ne " " and primary_key_defined eq 0 then do;
				pk_name=cats(substr("&table_name.",1,min(29,length("&table_name."))),"_pk");
				put +3 'execute (alter table &dbschema..' table_name;
				put +6 'add constraint ' pk_name ' primary key (' &key_list ')) by &database.;';
			end;
		    PUT +3 'DISCONNECT FROM &database.;';
		    PUT 'QUIT;';
			PUT '%err_check (Failed to create Table: ' "&table_name., &table_name.);";
		end;			   
	run;

	filename ddlfile;
	%err_check (Unable to generate ddl code for &database.,&SYSMACRONAME.);

%mend create_ddl;

