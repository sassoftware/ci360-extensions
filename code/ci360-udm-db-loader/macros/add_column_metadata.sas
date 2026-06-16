/******************************************************************************/
/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.*/
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/

%macro add_column_metadata(schema_table=, metadata_table=, database=);

    PROC SQL;
        CREATE TABLE work.metadata_table AS
        SELECT
            a.*,
            CASE
                WHEN upcase("&database") = 'BIGQUERY' AND substr(column_type, 1, 6) = 'string'
                    THEN 'STRING'
                WHEN upcase("&database") = 'BIGQUERY' AND index(column_type, 'array')
                    THEN 'STRING'
                WHEN upcase("&database") = 'BIGQUERY' AND data_length NE .
                    THEN b.rdbms_datatype
                WHEN index(column_type, 'array')
                    THEN 'varchar(4000)'
                WHEN data_length NE .
                    THEN cats(b.rdbms_datatype, '(', data_length, ')')
                WHEN data_length = . AND substr(column_type, 1, 6) = 'string'
                    THEN 'varchar(4000)'
                WHEN data_length = . AND data_type = 'decimal'
                    THEN column_type
                WHEN data_length = . AND data_type NE 'decimal'
                    THEN b.rdbms_datatype
                ELSE ''
            END AS rdbms_column_type,
            CASE
                WHEN prxmatch('/^identity_/i', table_name)                                           THEN 0
                WHEN upcase(table_name) = 'ABT_ATTRIBUTION'
                     AND upcase(column_name) = 'INTERACTION_DTTM'                                    THEN 1
                WHEN upcase(table_name) = 'CDM_CONTACT_HISTORY'
                     AND upcase(column_name) = 'CONTACT_DT'                                          THEN 1
                WHEN upcase(table_name) = 'CDM_RESPONSE_HISTORY'
                     AND upcase(column_name) = 'RESPONSE_DT'                                         THEN 1
                WHEN upcase(table_name) = 'CDM_RESPONSE_EXTENDED_ATTR'
                     AND upcase(column_name) = 'UPDATED_DTTM'                                        THEN 1
                WHEN prxmatch('/^dbt_/i', table_name)
                     AND upcase(column_name) = 'SESSION_COMPLETE_LOAD_DTTM'                          THEN 1
                WHEN partitioned_flg = 1 AND upcase(column_name) = 'LOAD_DTTM'                      THEN 1
                ELSE 0
            END AS partition_column
        FROM &schema_table. a
        LEFT JOIN cdmcnfg.datatypes b
            ON a.data_type = b.schema_datatype
        WHERE upcase(b.rdbms) = upcase("&database");
    QUIT;

    DATA &metadata_table.;
        SET work.metadata_table;
        table_name = upcase(table_name);
    RUN;

%mend add_column_metadata;
