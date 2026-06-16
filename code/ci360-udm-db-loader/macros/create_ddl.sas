/******************************************************************************/
/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.*/
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/

%macro create_ddl(database=&database., table_name=, column_table=, key_list=);
    %local database mart_type table_name column_table key_list primary_key_defined cluster_col;

    %put ****** create_&database._ddl ****** &table_name.;
    %let primary_key_defined    = 0;
    %let cluster_col            = ;

    FILENAME ddlfile DISK "&code_file_path.";

    /* Look up cluster column for BigQuery */
    %if %upcase(&database.) = %str(BIGQUERY) %then %do;
        DATA _NULL_;
            SET work.cluster_column;
            WHERE upcase(table_name) = "&table_name.";
            CALL SYMPUTX('cluster_col', cluster_col);
        RUN;
    %end;

    /* Write CREATE TABLE statement */
    DATA _NULL_;
        SET &column_table. END=last;
        LENGTH part_col $600;
        LENGTH partition_column_name $32 partition_column_datatype $32;
        RETAIN partition_column_name partition_column_datatype;
        _cluster_col = strip(symget('cluster_col'));
        WHERE upcase(table_name) = "&table_name.";
        FILE ddlfile MOD;

        IF _n_ = 1 THEN DO;
            PUT 'PROC SQL ;';
            PUT +3 'CONNECT TO &database. (&sql_passthru_connection.);';
            PUT +3 'EXECUTE (CREATE TABLE &dbschema..' table_name '(';
            PUT +6 @;
        END;

        IF index(&key_list., strip(upcase(column_name))) NE 0 OR partition_column = 1 THEN DO;
            comb_col = catx(' ', column_name, rdbms_column_type, 'NOT NULL');
            PUT comb_col @;
        END;
        ELSE DO;
            IF upcase("&database.") = 'BIGQUERY'
                THEN comb_col = catx(' ', column_name, rdbms_column_type);
                ELSE comb_col = catx(' ', column_name, rdbms_column_type, 'NULL');
            PUT comb_col @;
        END;

        IF NOT last THEN PUT +(-1) ', ' @;

        IF mod(_n_, 4) = 0 THEN DO;  /* Start a new line every 4 variables */
            PUT;
            PUT +6 @;
        END;

        IF partition_column = 1 THEN DO;
            partition_column_name     = column_name;
            partition_column_datatype = rdbms_column_type;
        END;

        IF last THEN DO;
            IF upcase("&database.") = 'BIGQUERY'
               AND partition_column_name NE " "
               AND NOT missing(symget('cluster_col'))
            THEN DO;
                PUT;
                IF upcase(partition_column_datatype) = 'DATE' THEN DO;
                    PUT +6 ') PARTITION BY ' partition_column_name;
                END;
                ELSE DO;
                    PUT +6 ') PARTITION BY DATE(' partition_column_name ')';
                END;
                PUT +6 'CLUSTER BY ' _cluster_col;
                PUT +3 ') BY &database.;';
            END;
            ELSE DO;
                PUT +3 ')) BY &database.;';
            END;
        END;
    RUN;

    %if &key_list. NE " " AND %upcase(&database.) NE %str(BIGQUERY) %then %do;
        FILENAME ddlcod TEMP;
        DATA _NULL_;
            SET &column_table.(WHERE=(upcase(table_name) = "&table_name." AND partition_column = 1) OBS=1);
            FILE ddlcod;
            PUT '%add_partitioning_ddl(database=&database., table_name=&table_name., column_name='
                column_name ', key_list=%superq(key_list), column_datatype='
                rdbms_column_type ');';
        RUN;

    %include ddlcod;

    %if &verbose. %then %do;
        DATA _NULL_;
            INFILE ddlcod;
            INPUT;
            PUT _infile_;
        RUN;
    %end;

    FILENAME ddlcod;

    %end;

    /* Add primary key constraint */
    DATA _NULL_;
        SET &column_table. END=last;
        WHERE upcase(table_name) = "&table_name.";
        FILE ddlfile MOD;

        IF last THEN DO;
            IF &key_list. NE " " AND &primary_key_defined. EQ 0 AND upcase("&database.") NE "SQLSVR" THEN DO;
                IF upcase("&database.") = 'BIGQUERY' THEN DO;
                    PUT +3 'EXECUTE (ALTER TABLE &dbschema..' table_name;
                    PUT +6 'ADD PRIMARY KEY (' &key_list. ') NOT ENFORCED ) BY &database.;';
                END;
                ELSE DO;
                    pk_name = cats(substr("&table_name.", 1, min(29, length("&table_name."))), "_pk");
                    PUT +3 'EXECUTE (ALTER TABLE &dbschema..' table_name;
                    PUT +6 'ADD CONSTRAINT ' pk_name ' PRIMARY KEY (' &key_list. ')) BY &database.;';
                END;
            END;
            PUT +3 'DISCONNECT FROM &database.;';
            PUT 'QUIT;';
            PUT '%err_check (Failed to create Table: ' "&table_name., &table_name.);";
        END;
    RUN;

    FILENAME ddlfile;

    %err_check(Unable to generate DDL code for &database., &SYSMACRONAME.);

%mend create_ddl;
