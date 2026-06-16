/******************************************************************************/
/* Copyright(c)2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/

%macro create_redshift_etl(database=&database., table_name=, column_table=, key_list=);
    %local database mart_type table_name column_table key_list;

    FILENAME codefile DISK "&code_file_path.";

    /* Write table-existence guard and row-count probe into the generated ETL file */
    DATA _NULL_;
        FILE codefile MOD;
        PUT '%if %sysfunc(exist(&udmmart..' "&table_name." ')) %then %do;';
        PUT +3 '%let errFlag=0;';
        PUT +3 '%let nrows=0;';
        PUT +3 '%let dsid=%sysfunc(open(&udmmart..' "&table_name." '));';
        PUT +3 '%let nrows=%sysfunc(attrn(&dsid,nlobs));';
        PUT +3 '%let dsid=%sysfunc(close(&dsid));';
    RUN;

    /* ------------------------------------------------------------------ */
    /* Full load (no primary key — metadata-only tables)                   */
    /* ------------------------------------------------------------------ */
    %if &key_list. = " " %then %do;
        DATA _NULL_;
            SET &column_table. END=last;
            WHERE upcase(table_name) = "&table_name.";
            FILE codefile MOD;
            IF _n_ = 1 THEN DO;
                * Truncate target table;
                PUT +3 'PROC SQL NOERRORSTOP;';
                PUT +6 'CONNECT TO &database. (&sql_passthru_connection.);';
                PUT +6 'EXECUTE (TRUNCATE TABLE &dbschema..' "&table_name." ') BY &database.;';
                PUT +6 'DISCONNECT FROM &database.;';
                PUT +3 'QUIT;';
                PUT +3 '%err_check (Failed to truncate ' table_name ', ' table_name ');';

                * Full load with bulkload threshold check;
                PUT +3 'PROC APPEND DATA=&udmmart..' table_name ' BASE=&trglib..' table_name '(';
                PUT +6 '%if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;';
                PUT +9 '&DB_BL_OPTS.';
                PUT +6 '%end;';
                PUT +6 '%else %do;';
                PUT +9 '&DB_LD_OPTS.';
                PUT +6 '%end;';
                PUT +6 ') FORCE;';
                PUT +3 'RUN;';
                PUT +3 '%err_check (Failed to append to ' table_name ', ' table_name ');';
            END;
        RUN;
    %end;

    /* ------------------------------------------------------------------ */
    /* Incremental load (tables with a primary key — upsert via MERGE)     */
    /* ------------------------------------------------------------------ */
    %else %do;

        /* Step 1: Dedup source, apply timezone conversion, filter null keys */
        DATA _NULL_;
            SET &column_table. END=last;
            LENGTH staging_table $32;
            WHERE upcase(table_name) = "&table_name.";
            FILE codefile MOD;
            staging_table = cats(substr(table_name, 1, min(28, length(table_name))), "_tmp");

            IF _n_ = 1 THEN DO;
                PUT +3 '%if %sysfunc(exist(&tmplib..' staging_table ')) %then %do;';
                PUT +6 'PROC SQL NOERRORSTOP;';
                PUT +9 'DROP TABLE &tmplib..' staging_table ';';
                PUT +6 'QUIT;';
                PUT +3 '%end;';
                PUT +3 '%check_duplicate_from_source(table_nm=' table_name
                    ', table_keys=%str(' &key_list. '), out_table=work.' table_name ');';
                PUT +3 'DATA work.' staging_table '/VIEW=work.' staging_table ';';
                PUT +6 'SET work.' table_name ';';
            END;

            IF index(lowcase(column_name), '_dttm_tz') THEN DO;
                PUT +6 'IF ' column_name ' NE . THEN ' column_name
                    '=tzoneu2s(' column_name ',&timeZone_Value.);';
            END;

            IF last THEN DO;
                key_condition = tranwrd(&key_list., ',', ' IS NOT NULL AND ');
                PUT +6 'WHERE 1=1 AND ' key_condition 'IS NOT NULL;';
                PUT +3 'RUN;';
                PUT +3 '%err_check (Failed to add time zone adaptation :' staging_table ', ' table_name ');';

                /* Step 2: Upload to staging table with bulkload threshold check */
                PUT +3 '%if &errFlag = 0 %then %do;';
                PUT +6 '%if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;';
                PUT +9 'DATA &tmplib..' staging_table ';';
                PUT +12 'SET work.' staging_table ';';
                PUT +12 'STOP;';
                PUT +9 'RUN;';
                PUT +9 'PROC APPEND DATA=work.' staging_table ' BASE=&tmplib..' staging_table '(&DB_BL_OPTS) FORCE;';
                PUT +9 'RUN;';
                PUT +6 '%end;';
                PUT +6 '%else %do;';
                PUT +9 'DATA &tmplib..' staging_table ';';
                PUT +12 'SET work.' staging_table ';';
                PUT +9 'RUN;';
                PUT +6 '%end;';
                PUT +6 '%err_check (Failed to upload to temp location in DB :' staging_table ', ' table_name ');';
                PUT +3 '%end;';
            END;
        RUN;

        /* Step 3: MERGE ON condition (primary key columns) */
        DATA _NULL_;
            SET &column_table. END=last;
            LENGTH staging_table $32;
            WHERE upcase(table_name) = "&table_name." AND primary_key = 1;
            FILE codefile MOD;
            staging_table = cats(substr(table_name, 1, min(28, length(table_name))), "_tmp");

            IF _n_ = 1 THEN DO;
                PUT +3 '%if &errFlag = 0 %then %do;';
                PUT +6 'PROC SQL NOERRORSTOP;';
                PUT +9 'CONNECT TO &database. (&sql_passthru_connection.);';
                PUT +9 'EXECUTE (MERGE INTO &dbschema..' table_name 'USING &tmpdbschema..' staging_table 'AS d ON (';
                PUT +12 @;
            END;
            IF mod(_n_, 2) = 0 THEN DO;
                PUT;
                PUT +12 @;
            END;
            PUT table_name +(-1) '.' column_name '= d.' column_name @;
            IF NOT last THEN PUT +(-1) ' AND ' @;
            IF last THEN PUT ')';
        RUN;

        /* Step 4: WHEN MATCHED THEN UPDATE (non-key columns) */
        DATA _NULL_;
            SET &column_table. END=last;
            LENGTH staging_table $32;
            WHERE upcase(table_name) = "&table_name." AND primary_key NE 1;
            FILE codefile MOD;
            staging_table = cats(substr(table_name, 1, min(28, length(table_name))), "_tmp");

            IF _n_ = 1 THEN DO;
                PUT +9 'WHEN MATCHED THEN';
                PUT +9 'UPDATE SET';
                PUT +12 @;
            END;
            IF mod(_n_, 2) = 0 THEN DO;
                PUT;
                PUT +12 @;
            END;
            PUT column_name '= d.' column_name @;
            IF NOT last THEN PUT +(-1) ', ' @;
            IF last THEN PUT;
        RUN;

        /* Step 5: WHEN NOT MATCHED THEN INSERT — column list */
        DATA _NULL_;
            SET &column_table. END=last;
            LENGTH staging_table $32;
            WHERE upcase(table_name) = "&table_name.";
            FILE codefile MOD;
            staging_table = cats(substr(table_name, 1, min(28, length(table_name))), "_tmp");

            IF _n_ = 1 THEN DO;
                PUT +9 'WHEN NOT MATCHED THEN INSERT (';
                PUT +12 @;
            END;
            IF mod(_n_, 4) = 0 THEN DO;
                PUT;
                PUT +12 @;
            END;
            PUT column_name @;
            IF NOT last THEN PUT +(-1) ', ' @;
        RUN;

        /* Step 6: VALUES list and close the MERGE statement */
        DATA _NULL_;
            SET &column_table. END=last;
            LENGTH staging_table $32;
            WHERE upcase(table_name) = "&table_name.";
            FILE codefile MOD;
            staging_table = cats(substr(table_name, 1, min(28, length(table_name))), "_tmp");

            IF _n_ = 1 THEN DO;
                PUT +9 ') VALUES (';
                PUT +12 @;
            END;
            IF mod(_n_, 4) = 0 THEN DO;
                PUT;
                PUT +12 @;
            END;
            PUT 'd.' column_name @;
            IF NOT last THEN PUT +(-1) ', ' @;
            IF last THEN DO;
                PUT ' )) BY &database.;';
                PUT +9 'DISCONNECT FROM &database.;';
                PUT +6 'QUIT;';
                PUT +6 '%err_check (Failed to Update/Insert into :' staging_table ', ' table_name ', err_macro=SYSDBRC);';
                PUT +3 '%end;';

                /* Drop the staging table */
                PUT +3 '%if %sysfunc(exist(&tmplib..' staging_table ')) %then %do;';
                PUT +6 'PROC SQL NOERRORSTOP;';
                PUT +9 'DROP TABLE &tmplib..' staging_table ';';
                PUT +6 'QUIT;';
                PUT +3 '%end;';
            END;
        RUN;

    %end;

    /* ------------------------------------------------------------------ */
    /* Clean up: delete source dataset on success                          */
    /* ------------------------------------------------------------------ */
    DATA _NULL_;
        FILE codefile MOD;
        PUT +3 '%if &errFlag = 0 %then %do;';
        PUT +6 'PROC SQL NOERRORSTOP;';
        PUT +9 'DROP TABLE &udmmart..' "&table_name." ';';
        PUT +9 'DROP TABLE work.' "&table_name." ';';
        PUT +6 'QUIT;';
        PUT +3 '%end;';
        PUT +3 '%else %do;';
        PUT +6 '%put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;';
        PUT +3 '%end;';
        PUT +3 '%put %sysfunc(datetime(),E8601DT25.) --- Processing table ' "&table_name." ';';
        PUT +3 '%put------------------------------------------------------------------;';
        PUT '%end;';
    RUN;

    FILENAME codefile;

%mend create_redshift_etl;
