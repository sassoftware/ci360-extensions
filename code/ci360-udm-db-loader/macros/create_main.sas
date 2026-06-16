/******************************************************************************/
/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.*/
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/

OPTION SPOOL;

%macro create_main(database=, schema_version=, DDL=);
    %local database schema_version DDL;

    /* Fetch schema and key metadata from the CI360 API */
    %get_udm_schema(
        api_schema_version=&schema_version.,
        schema_table=schema_details,
        partitioning_table=partitioning_table
    );
    %get_primary_keys(
        schema_table=schema_details,
        partitioning_table=partitioning_table,
        key_table=key_table
    );
    %add_column_metadata(
        schema_table=schema_details,
        database=&database.,
        metadata_table=column_metadata
    );

    %if %upcase(&database.) = %str(BIGQUERY) %then %do;
        %get_cluster_column(metadata_table=column_metadata);
    %end;

    /* Determine output file path */
    %if &DDL. %then %do;
        %let code_file_path = &codes_path.&slash.&database._V&schema_version._DDL.sas;
    %end;
    %else %do;
        %let code_file_path = &codes_path.&slash.&database._V&schema_version._ETL.sas;
    %end;

    /* Delete any pre-existing generated file */
    %if %sysfunc(fileexist("&code_file_path.")) GE 1 %then %do;
        %let rc = %sysfunc(filename(temp, "&code_file_path."));
        %let rc = %sysfunc(fdelete(&temp.));
        %put Old code file deleted...;
    %end;

    /* Write file header and optional macro wrapper */
    FILENAME codeout DISK "&code_file_path.";
    DATA _NULL_;
        FILE codeout MOD;
        PUT '/*******************************************************************************/';
        PUT '/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */';
        PUT '/* SPDX-License-Identifier: Apache-2.0                                         */';
        PUT '/* *****************************************************************************/';
        %if &DDL. = 0 %then %do;
            PUT '%macro execute_' "&database." '_etl;';
        %end;
    RUN;

    /* Limit generated code to tables marked for execution */
    %if %sysfunc(exist(cdmcnfg.table_list)) %then %do;
        PROC SQL;
            CREATE TABLE table_details AS
            SELECT k.*
            FROM key_table k
            LEFT JOIN cdmcnfg.table_list t
                ON k.table_name = upcase(t.table_name)
            WHERE t.execution_flag NE 'N';
        QUIT;
    %end;
    %else %do;
        DATA table_details;
            SET key_table;
        RUN;
    %end;

    /* Generate one macro call per table and include it immediately */
    FILENAME sascod TEMP;
    DATA _NULL_;
        SET table_details END=last;
        FILE sascod;
        %if &DDL. %then %do;
            PUT '%create_ddl(database=&database., table_name=' table_name
                ', column_table=column_metadata, key_list="' key_list +(-1) '");';
        %end;
        %else %do;
            PUT '%create_&database._etl(database=&database., table_name=' table_name
                ', column_table=column_metadata, key_list="' key_list +(-1) '");';
        %end;
    RUN;

    %include sascod;

    %if &verbose. %then %do;
        DATA _NULL_;
            INFILE sascod;
            INPUT;
            PUT _infile_;
        RUN;
    %end;

    FILENAME sascod;

    /* Close the ETL macro wrapper */
    %if &DDL. = 0 %then %do;
        DATA _NULL_;
            FILE codeout MOD;
            PUT '%mend;';
            PUT '%execute_' "&database." '_etl;';
        RUN;
    %end;

%mend create_main;
