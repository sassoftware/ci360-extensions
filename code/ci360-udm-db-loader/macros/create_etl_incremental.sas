/******************************************************************************/
/* Copyright(c)2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/

%macro create_etl_incremental(database=&database., table_name=, column_table=, key_list=,codefref=);
    %local database mart_type table_name column_table key_list codefref;

    /* ------------------------------------------------------------------ */
    /* Incremental load (tables with a primary key -- upsert via MERGE)   */
    /* ------------------------------------------------------------------ */

    /* Step 1: Dedup source, filter null keys */
    DATA _NULL_;
        SET &column_table. END=last;
        LENGTH staging_table $32;
        WHERE upcase(table_name) = "&table_name.";
        FILE codefile MOD;
        staging_table = cats(substr(table_name, 1, min(28, length(table_name))), "_tmp");

        IF _n_ = 1 THEN DO;
            PUT +3 '%if &errFlag = 0 %then %do;';
            PUT +6 'PROC SQL NOERRORSTOP;';
            PUT +9 'DROP TABLE &tmplib..' staging_table ';';
            PUT +6 'QUIT;';
            PUT +6 '%err_check (Failed to drop temporary DB table ' staging_table ', ' staging_table ');';
            PUT +3 '%end;';

            PUT +3 '%if &errFlag = 0 %then %do;';
            PUT +6 '%check_duplicate_from_source(table_nm=' table_name
                ', table_keys=%str(' &key_list. '), out_table=work.' table_name ');';
            PUT +6 'DATA work.' staging_table ';';
            PUT +9 'SET work.' table_name ';';
        END;

        IF lowcase(column_name)= 'load_dttm' THEN DO;
            PUT +9 'IF load_dttm = . THEN DO; ';
            PUT +12 'IF datekey < 30001231 THEN datekey=datekey*10;';
            PUT +12 'load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);';
            PUT +9 'END;';
        END;

        IF last THEN DO;
            key_condition = tranwrd(&key_list., ',', ' IS NOT NULL AND ');
            PUT +9 'WHERE 1=1 AND ' key_condition 'IS NOT NULL;';
            PUT +6 'RUN;';
            PUT +6 '%err_check (Failed to prepare staging table : ' staging_table ', ' staging_table ');';
            PUT +3 '%end;';

            /* Step 2: Upload to staging table with bulkload threshold check */
            PUT +3 '%if &errFlag = 0 %then %do;';
			put +6 'PROC SQL noerrorstop;';
			put +9  'connect to &database. (&sql_passthru_connection.);';
	 		%if "&database."="SQLSVR"  %then %do;
				put +9  'execute( SELECT * INTO &tmpdbschema..' staging_table 'FROM &dbschema..' table_name 'WHERE 1=0' @;
			%end;
			%else %do;
				put +9  'execute( create table &tmpdbschema..' staging_table 'as select * from &dbschema..' table_name 'where 1=0' @;
			%end;
	 		%if "&database."="POSTGRES"  %then %do;
				PUT ';' @;
			%end;
			put ') by &database.;';
			put +9  'disconnect from &database.;';
			put +6 'QUIT;';
            PUT +6 '%err_check (Failed to create table : ' staging_table ', ' table_name ', err_macro=SYSDBRC);';
            PUT +3 '%end;';

            PUT +3 '%if &errFlag = 0 %then %do;';
			put +6 'PROC APPEND data=work.' staging_table ' base=&tmplib..' staging_table ;
            PUT +9  '%if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;';
			PUT +12  '(&DB_BL_OPTS)';
            PUT +9  '%end;';
			PUT +9  'force;';
			PUT +6 'RUN;';
            PUT +6 '%err_check (Failed to upload to temp location in DB : ' staging_table ', ' table_name ');';
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
			%if "&database."="POSTGRES" OR 
				"&database."="SQLSVR" %then %do;
	            PUT +9 'EXECUTE (MERGE INTO &dbschema..' table_name 'AS b USING &tmpdbschema..' staging_table 'AS d ON (';
			%end;
			%if "&database."="ORACLE" OR 
				"&database."="BIGQUERY" %then %do;
 	        	PUT +9 'EXECUTE (MERGE INTO &dbschema..' table_name 'b USING &tmpdbschema..' staging_table 'd ON (';
			%end;
			%if "&database."="REDSHIFT" %then %do;
	            PUT +9 'EXECUTE (MERGE INTO &dbschema..' table_name 'USING &tmpdbschema..' staging_table 'AS d ON (';
			%end;
            PUT +12 @;
        END;
        IF mod(_n_, 2) = 0 THEN DO;
            PUT;
            PUT +12 @;
        END;
 		%if "&database."="REDSHIFT" %then %do;
            PUT table_name +(-1) '.' @;
		%end;
 		%else %do;
			PUT 'b.' @;
		%end;
		PUT column_name '= d.' column_name @;
        IF NOT last THEN PUT 'AND ' @;
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
 		%if "&database."="SQLSVR" OR "&database."="ORACLE" OR "&database."="BIGQUERY" %then %do;
			PUT 'b.' @;
		%end;
        PUT column_name '= d.' column_name @;
        IF NOT last THEN PUT +(-1) ', ' @;
        IF last THEN PUT;
    RUN;

    /* Step 5: WHEN NOT MATCHED THEN INSERT -- column list */
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
            PUT ' )' @;
	 		%if "&database."="POSTGRES" OR "&database."="SQLSVR" %then %do;
				PUT ';' @;
			%end;
			PUT ') BY &database.;';
            PUT +9 'DISCONNECT FROM &database.;';
            PUT +6 'QUIT;';
            PUT +6 '%err_check (Failed to Update/Insert into : ' staging_table ', ' table_name ', err_macro=SYSDBRC);';
            PUT +3 '%end;';

            /* Drop the staging table */
            PUT +3 '%if %sysfunc(exist(&tmplib..' staging_table ')) %then %do;';
            PUT +6 'PROC SQL NOERRORSTOP;';
            PUT +9 'DROP TABLE &tmplib..' staging_table ';';
            PUT +6 'QUIT;';
            PUT +3 '%end;';
        END;
    RUN;

%mend create_etl_incremental;
