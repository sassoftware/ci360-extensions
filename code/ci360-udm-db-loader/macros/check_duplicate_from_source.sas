/*******************************************************************************/
/* Copyright � 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%macro check_duplicate_from_source(table_nm, table_keys, out_table);

	%let table_keys_spaced=%sysfunc(translate(&table_keys., %str( ), %str(,)));
	/* check for duplicates based on the primary key */
	PROC SQL;
		create table test_dupkey as 
		select distinct &table_keys.
		from &udmmart..&table_nm.;
	QUIT;

	%let duplicate_keys=0;
	%let deduped_rows_by_key=0;
	%if %sysfunc(exist(&udmmart..&table_nm.)) %then %do;
		%let dsid=%sysfunc(open(&udmmart..&table_nm.));
		%let input_rows=%sysfunc(attrn(&dsid.,nlobs));
		%let dsid=%sysfunc(close(&dsid.));
		%if %sysfunc(exist(test_dupkey)) %then %do;
			%let dsid=%sysfunc(open(test_dupkey));
			%let deduped_rows_by_key=%sysfunc(attrn(&dsid.,nlobs));
			%let dsid=%sysfunc(close(&dsid.));
			%let duplicate_keys=%eval(&input_rows.-&deduped_rows_by_key.);
			%put NOTE: &udmmart..&table_nm. with key (&table_keys.) contains &input_rows. rows of which &duplicate_keys. are rows with duplicate keys.;
		%end;
	%end;

	%if &duplicate_keys.=0 %then %do;
		data &out_table. /view=&out_table.;
			set &udmmart..&table_nm.;
		run;
		%put NOTE: No duplicates found in table &table_nm..;
	%end;

	%else %do;
		/* duplicates based on the primary key */
		proc sort data=&udmmart..&table_nm. out=&out_table. nodupkey;
 			by &table_keys_spaced.;
		run;
		/* Exclude load_dttm from row-based deduplication */
		PROC CONTENTS NOPRINT DATA=&udmmart..&table_nm. OUT=COLUMNLIST(keep=name); 
		run;
		PROC SQL NOPRINT;
			SELECT name into :column_list separated by ','
			from COLUMNLIST
			where upcase(name) not in ('LOAD_DTTM'); 
		QUIT;
		PROC SQL;
			create table test_duprow as 
			select distinct &column_list.
			from &udmmart..&table_nm.;
		QUIT;
		%let duplicate_rows=0;
		%let deduped_rows=0;
		%if %sysfunc(exist(test_duprow)) %then %do;
			%let dsid=%sysfunc(open(test_duprow));
			%let deduped_rows=%sysfunc(attrn(&dsid.,nlobs));
			%let dsid=%sysfunc(close(&dsid.));
			%let duplicate_rows=%eval(&input_rows.-&deduped_rows.);
		%end;
		%put Duplicate keys count: &duplicate_keys.;
		%put Duplicate rows count: &duplicate_rows.;
		%if &duplicate_keys.=&duplicate_rows. %then %do;
			%put WARNING: &duplicate_rows. duplicate rows found in downloaded data. Rows deduplicated without data loss.;
		%end;
		%else %do;
			%put ERR%str()OR: Duplicate primary key found in &table_nm. with different data. Primary key (&table_keys.) may be wrong.;
			%if &ignore_duplicate_err=0 %then %do;
				%let errFlag=1;
			%end;
			%else %do;
				%put WARNING: Proccessing will countinue because of ignore_duplicate_err set to &ignore_duplicate_err..;
			%end;
		%end;
		proc delete data=test_duprow;
		run;
	%end;

	proc delete data=test_dupkey;
	run;
%mend;

