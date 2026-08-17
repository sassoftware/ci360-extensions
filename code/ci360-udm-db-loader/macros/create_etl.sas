/******************************************************************************/
/* Copyright(c)2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/

%macro create_etl(database=&database., table_name=, column_table=, key_list=);
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
		PUT +3 '%if &nrows = 0 %then %do;';
        PUT +6 '%put NOTE: ' "&table_name." ' has 0 rows. Dropping and skipping load.;';
        PUT +6 'PROC SQL NOERRORSTOP;';
        PUT +9 'DROP TABLE &udmmart..' "&table_name." ';';
        PUT +6 'QUIT;';
        PUT +6 '%let errFlag=1;';
 		PUT +3 '%end;';
    RUN;

    /* ------------------------------------------------------------------ */
    /* Full load (no primary key -- metadata-only tables)                   */
    /* ------------------------------------------------------------------ */
    %if &key_list. = " " %then %do;
		%create_etl_full(database=&database., table_name=&table_name, codefref=codefile);
    %end;

    /* ------------------------------------------------------------------ */
    /* Incremental load (tables with a primary key -- upsert via MERGE)   */
    /* ------------------------------------------------------------------ */
    %else %do;
		%create_etl_incremental(database=&database., table_name=&table_name, 
			column_table=&column_table., key_list=&key_list. ,codefref=codefile);
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

%mend create_etl;
