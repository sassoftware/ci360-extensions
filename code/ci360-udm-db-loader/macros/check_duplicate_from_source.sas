/*******************************************************************************/
/* Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/

%macro check_duplicate_from_source(table_nm, table_keys, out_table);

    %let table_keys_spaced = %sysfunc(translate(&table_keys., %str( ), %str(,)));

    /* Check for duplicate primary keys in the source dataset */
    PROC SQL NOERRORSTOP;
        CREATE TABLE test_dupkey AS
        SELECT DISTINCT &table_keys.
        FROM &udmmart..&table_nm.;
    QUIT;

    %let duplicate_keys    = 0;
    %let deduped_rows_by_key = 0;

    %if %sysfunc(exist(&udmmart..&table_nm.)) %then %do;
        %let dsid       = %sysfunc(open(&udmmart..&table_nm.));
        %let input_rows = %sysfunc(attrn(&dsid., nlobs));
        %let dsid       = %sysfunc(close(&dsid.));

        %if %sysfunc(exist(test_dupkey)) %then %do;
            %let dsid              = %sysfunc(open(test_dupkey));
            %let deduped_rows_by_key = %sysfunc(attrn(&dsid., nlobs));
            %let dsid              = %sysfunc(close(&dsid.));
            %let duplicate_keys    = %eval(&input_rows. - &deduped_rows_by_key.);
            %put NOTE: &udmmart..&table_nm. with key (&table_keys.) contains &input_rows. rows of which &duplicate_keys. are rows with duplicate keys.;
        %end;
    %end;

    %if &duplicate_keys. = 0 %then %do;
        /* No duplicates -- create a view for zero-copy performance */
        DATA &out_table. / VIEW=&out_table.;
            SET &udmmart..&table_nm.;
        RUN;
        %put NOTE: No duplicates found in table &table_nm..;
    %end;

    %else %do;
        /* Duplicates found -- deduplicate by primary key */
        PROC SORT DATA=&udmmart..&table_nm. OUT=&out_table. NODUPKEY;
            BY &table_keys_spaced.;
        RUN;

        /* Exclude load_dttm from row-level duplicate check */
        PROC CONTENTS NOPRINT DATA=&udmmart..&table_nm.
            OUT=COLUMNLIST (KEEP=name);
        RUN;

        PROC SQL NOPRINT NOERRORSTOP;
            SELECT name INTO :column_list SEPARATED BY ','
            FROM COLUMNLIST
            WHERE upcase(name) NOT IN ('LOAD_DTTM', 'DATEKEY');
        QUIT;

        PROC SQL NOERRORSTOP;
            CREATE TABLE test_duprow AS
            SELECT DISTINCT &column_list.
            FROM &udmmart..&table_nm.;
        QUIT;

        %let duplicate_rows = 0;
        %let deduped_rows   = 0;

        %if %sysfunc(exist(test_duprow)) %then %do;
            %let dsid         = %sysfunc(open(test_duprow));
            %let deduped_rows = %sysfunc(attrn(&dsid., nlobs));
            %let dsid         = %sysfunc(close(&dsid.));
            %let duplicate_rows = %eval(&input_rows. - &deduped_rows.);
        %end;

        %put Duplicate keys count: &duplicate_keys.;
        %put Duplicate rows count: &duplicate_rows.;

        %if &duplicate_keys. = &duplicate_rows. %then %do;
            %put WARNING: &duplicate_rows. duplicate rows found in downloaded data. Rows deduplicated without data loss.;
        %end;
        %else %do;
            %if &ignore_duplicate_err = 0  
				/* Aug2026 BUG FIX for historical data that doesn't contain the correct primary key */
				AND ("%sysfunc(SUBSTR(&table_nm.,1,4))" NE "DBT_" OR "&table_keys." NE "DETAIL_ID")
			%then %do;
	            %put ERR%str()OR: Duplicate primary key found in &table_nm. with different data. Primary key (&table_keys.) may be wrong.;
            	%let errFlag = 1;
            %end;
            %else %do;
                %put WARNING: Processing will continue because ignore_duplicate_err = &ignore_duplicate_err..;
            %end;
        %end;

        PROC DELETE DATA=test_duprow; RUN;
    %end;

    PROC DELETE DATA=test_dupkey; RUN;

%mend check_duplicate_from_source;
