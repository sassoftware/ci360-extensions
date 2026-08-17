/******************************************************************************/
/* Copyright(c)2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/

%macro create_etl_full(database=&database., table_name=, codefref=);
    %local database mart_type table_name codefref; 

    /* ------------------------------------------------------------------ */
    /* Full load (no primary key -- metadata-only tables)                   */
    /* ------------------------------------------------------------------ */
        DATA _NULL_;
            SET &column_table. END=last;
            WHERE upcase(table_name) = "&table_name.";
            FILE &codefref. MOD;
            IF _n_ = 1 THEN DO;
                * Truncate target table;
                PUT +3 '%if &errFlag = 0 %then %do;';
                PUT +6 'PROC SQL NOERRORSTOP;';
                PUT +9 'CONNECT TO &database. (&sql_passthru_connection.);';
                PUT +9 'EXECUTE (TRUNCATE TABLE &dbschema..' "&table_name." ') BY &database.;';
                PUT +9 'DISCONNECT FROM &database.;';
                PUT +6 'QUIT;';
				PUT +6 '%err_check (Failed to truncate ' table_name ', ' table_name ');';
		 		PUT +3 '%end;';

                 * Full load with load threshold checks;
                PUT +3 '%if &errFlag = 0 %then %do;';
                PUT +6 'PROC APPEND DATA=&udmmart..' table_name ' BASE=&trglib..' table_name '(';
                PUT +9 '%if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;';
                PUT +12 '&DB_BL_OPTS.';
                PUT +9 '%end;';
                PUT +9 '%else %do;';
                PUT +12 '&DB_LD_OPTS.';
                PUT +9 '%end;';
                PUT +9 ') FORCE;';
                PUT +6 'RUN;';
                PUT +6 '%err_check (Failed to append to ' table_name ', ' table_name ');';
		 		PUT +3 '%end;';
            END;
        RUN;

%mend create_etl_full;
