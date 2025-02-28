/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%macro generate_table_code(table_name,mart_type);
	%let errFlag = 0;
	%let total_sum =0;
	%let col_hash_flag=0;
	%put ******generate_table_code******;

	proc sql;
		create table work.metadata_table as 
		select 
			default_val,	isnull,	ispk,	isfk,	fk_reference,mart_type,
			table_name,	column_name,timeZone_flg,data_type,data_length,Staging_table,
			case
				when data_length=. and substr(column_type, 1, 6) = 'string' then 'varchar(4000)'
				when substr(column_type, 1, 5) = 'array' then 'varchar(4000)'
				when data_length ne . then cats(b.rdbms_datatype,'(',data_length,')')
				when data_length=. and data_type = 'decimal' then column_type
				when data_length=. and data_type ne 'decimal' then  b.rdbms_datatype
				when data_length=. and substr(column_type, 1, 6) = 'string' then 'varchar(4000)'
				else ''		
			end as column_type					
		from cdmcnfg.metadata_table a, cdmcnfg.datatypes b 
			where upcase(b.rdbms) = %unquote(%nrbquote(upcase('&dbname'))) and a.data_type =b.schema_datatype;
	quit;

	proc sql;
		select sum(data_length) into :total_sum
			from work.metadata_table
				where table_name = "&table_name"  and ispk = 'TS';
	quit;

	data work.temp_metadata;
		Length constraint $10 pk_name $200 fk_name $200 tmp_pk_name $100 merge_condition $200 merge_update $200 merge_insert $200 merge_value $200 staging_view $100 default_col $100 col_hash_flag $1 init_char_pk_col $200;
		set work.metadata_table;
		where table_name=%unquote(%nrbquote('&table_name'));
		col_hash_flag=0;
		if &total_sum > 900 then do;
			call symputx('col_hash_flag', 1); 
			col_hash_flag=1;
		end;
		if 	isnull='TS' and col_hash_flag=0 then 
			do;
				constraint='NOT NULL';
			end;
		else
			do;
				constraint='NULL';
			end;

		if default_val ne '' then
			do;
				source_col=default_val;
			end;
		else
			do;
				source_col=column_name;
			end;

		target_col=column_name;

		if find(ispk,'T') ge 1 then
			do;
				if length(trim(table_name)) > 25 then
					do;
						pk_name = cat(substr(trim(table_name), 1, 25), '_pk');
						tmp_pk_name = cat(substr(trim(table_name), 1, 25), '_tmp_pk');
					end;
				else
					do;
						pk_name = cat(trim(table_name), '_pk');
						tmp_pk_name = cat(trim(table_name), '_tmp_pk');
					end;

				by_condition = cat('first.',trim(column_name));
				merge_condition = cat(trim(table_name), '.', trim(column_name), '=', trim(Staging_table), '.', trim(column_name));
				if index(data_type,'char') then do;
					init_char_pk_col = cat("if ",trim(column_name), "='' then ",trim(column_name),"='-';");
				end;
			end;

		if default_val ne '' then
			do;
				if find(default_val,'datetime()') ge 1 then
					do;
						default_col=cat(trim(source_col), " as ",trim(target_col),' FORMAT = ', %unquote(%nrbquote('&format.')));
					end;
				else
					do;
						default_col=cat("'",trim(source_col),"' as ",trim(target_col) );
					end;
			end;

		if timeZone_flg eq 'Y' then
			do;
				timeZone_col =  cat('if ', trim(column_name), ' ne . then ', trim(column_name),' = ',"tzoneu2s(", trim(column_name), ",",trim(%nrstr('&timeZone_Value.')), ")");

			end;

		target_table=trim(table_name);
		staging_view=cat(substr(trim(staging_table),1,length(trim(staging_table))-3),'view');
		merge_update=cat(trim(table_name),'.',trim(column_name),' = ',trim(Staging_table),'.',trim(column_name));

		if upcase("&dbname.") eq "SQLSVR" then
			do;
				merge_insert=trim(column_name);
			end;
		else
			do;
				merge_insert=cat(trim(table_name),'.',trim(column_name));
			end;

		merge_value=cat(trim(Staging_table),'.',trim(column_name));
		comb_col_type= cat(trim(column_name),' ',trim(column_type),' ',trim(constraint));

		if  %unquote(%nrbquote('&database.')) eq 'teradata' then
			do;
				put '************** tera data condition applies';
			end;
		else
			do;
				put '*************** other data base condition applies';
				merge_condition=cat(trim(table_name),'.',trim(column_name),'=',trim(Staging_table),'.',trim(column_name));
		end;
	run;

%create_macro_var;

%mend generate_table_code;
/*%generate_table_code(md_segment_custom_prop,detail)*/
/*%generate_table_code;*/
/*generate table code*/
