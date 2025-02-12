/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/

%macro create_macro_var;
%put ******create_macro_var******;
/**/
%let errFlag = 0;
%let timeZone_coll=;
%let target_table_name=;
%let datetime_where_condtion=;
%let col_nm =;
%let staging_table =;
%let staging_view =;
%let default_col =;
%let pk_name =;
%let tmp_pk_name =;
%let merge_condition =;
%let fk_count =;
%let merge_update =;
%let fk_reference =;
%let merge_value =;
%let merge_insert =;
%let tmp_pk_col =;
%let by_cndtn=;
%let col_nm_type=;

%put inside capture data with table name: &table_name;

proc sql;
	select comb_col_type
							into : col_nm_type SEPARATED by ' ,'
							from work.temp_metadata
							where ispk eq '' and isfk eq '' ;
	select comb_col_type
							into : col_nm_key SEPARATED by ' ,'
							from work.temp_metadata
							where ispk ne '' OR isfk ne '';

	select distinct(staging_table)
							into : staging_table
							from work.temp_metadata;
	select distinct(staging_view)
							into : staging_view
							from work.temp_metadata;
	select distinct(table_name)
							into : target_table_name SEPARATED by ''
							from work.temp_metadata;

	select  column_name
							into :col_nm SEPARATED by ','
							from work.temp_metadata
							where default_val eq '' and upcase(timeZone_flg) eq '' ;

	select distinct (pk_name )
							into: pk_name SEPARATED BY ''
							from work.temp_metadata;
							proc sql;
	select distinct (tmp_pk_name)
							into:tmp_pk_name SEPARATED BY ''
							from work.temp_metadata;

	select distinct(merge_condition)
							into: merge_condition SEPARATED by ' and '
							from work.temp_metadata
							where ispk ne '' ;

	select column_name
							into: tmp_pk_col  SEPARATED BY ','
							from work.temp_metadata
							where tmp_pk_name ne '';
	select by_condition
							into: by_cndtn  SEPARATED BY ' and '
							from work.temp_metadata
							where tmp_pk_name ne '';


	select count(isfk)	 	into: fk_count trimmed
							from work.temp_metadata
							where isfk like 'T%';


						
	select merge_insert,merge_value
							into:merge_insert SEPARATED BY ',', :merge_value SEPARATED BY ','
							from work.temp_metadata;
						
							
	select fk_reference
							into:fk_reference
							from work.temp_metadata
							where isfk like 'T%';
							proc sql;
	select timeZone_col
							into: timeZone_coll  SEPARATED BY ' ; '
							from work.temp_metadata
							where timeZone_flg eq 'Y';

	
	select merge_update
							into:merge_update  SEPARATED BY ' , '
							from work.temp_metadata
							where ispk eq '' ;
	
	select init_char_pk_col
							into :init_char_pk_col  SEPARATED BY ' '
							from work.temp_metadata
							where init_char_pk_col ne '' ;
quit;

%if &errFlag=0 %then %generate_dynamic_code;
%ErrCheck(Unable to create macro variables,create_macro_var);

%mend create_macro_var;
/*%create_macro_var;*/

