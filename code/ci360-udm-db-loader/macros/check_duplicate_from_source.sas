/*******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%macro check_duplicate_from_source(table_nm, table_keys, out_table);

	%let table_keys_spaced=%sysfunc(translate(&table_keys, %str( ), %str(,)));
	PROC SQL;/* check duplicate key row */
		create table test_dupkey as 
		select distinct &table_keys
		from &udmmart..&table_nm;
	QUIT;

	%let duplicate_keys=0;
	%if %sysfunc(exist(&udmmart..&table_nm)) %then %do;
		%let dsid=%sysfunc(open(&udmmart..&table_nm));
		%let input_rows=%sysfunc(attrn(&dsid,nlobs));
		%let dsid=%sysfunc(close(&dsid));
		%if %sysfunc(exist(test_dupkey)) %then %do;
			%let dsid=%sysfunc(open(test_dupkey));
			%let deduped_rows=%sysfunc(attrn(&dsid,nlobs));
			%let dsid=%sysfunc(close(&dsid));
			%let duplicate_keys=%eval(&input_rows.-&deduped_rows.);
		%end;
	%end;

	%if &duplicate_keys.=0 %then %do;
		data &out_table. /view=&out_table.;
			set &table_nm;
		run;
		%put NOTE: No duplicates found in table.;
	%end;
	%else %do;
		proc sort data=&udmmart..&table_nm dupout=test_duprow out=&out_table nodup;/* check whole row */
	 		by &table_keys_spaced;
		run;

		%let duplicate_rows=0;
		%if %sysfunc(exist(test_duprow)) %then %do;
			%let dsid=%sysfunc(open(test_duprow));
			%let duplicate_rows=%sysfunc(attrn(&dsid,nlobs));
			%let dsid=%sysfunc(close(&dsid));
		%end;
		%put Duplicate rows count: &duplicate_rows.;
		%put Duplicate keys count: &duplicate_keys.;
		%if &duplicate_keys.=&duplicate_rows. %then %do;
			%put WARNING: &duplicate_rows. duplicate rows found in downloaded data. Rows deduplicated without data loss.;
		%end;
		%else %do;
			%put ERR%str()OR: Duplicate primary key found with different data. Primary key may be wrong.;
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
