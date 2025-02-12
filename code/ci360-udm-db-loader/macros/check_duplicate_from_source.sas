/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%macro check_duplicate_from_source(table_nm, table_keys, out_table);

%let table_keys_nw=%sysfunc(translate(&table_keys, %str( ), %str(,)));

proc sort data=&udmmart..&table_nm dupout=test_dupkey out=&out_table.  nodupkey;
	by &table_keys_nw;
run;

%let duplicate_keys=0;
%if %sysfunc(exist(test_dupkey)) %then %do;
	%let dsid=%sysfunc(open(test_dupkey));
	%let duplicate_keys=%sysfunc(attrn(&dsid,nlobs));
	%let dsid=%sysfunc(close(&dsid));
%end;


/* if duplicates... */
%if &duplicate_keys. %then %do;
	proc sort data=&udmmart..&table_nm dupout=test_duprow out=deduped_rows nodup;/* check whole row */
 		by &table_keys_nw;
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
		%put WARNING: Duplicate rows found in downloaded data. Rows deduplicated without data loss.;
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
	proc delete data=test_duprow deduped_rows;
	run;
%end;
%else %do;
	%put NOTE: No duplicates found in table.;
%end;
proc delete data=test_dupkey;
run;
%mend;
