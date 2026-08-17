/*******************************************************************************/
/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                         */
/* *****************************************************************************/

%macro get_primary_keys(schema_table=, partitioning_table=, key_table=);
    %local schema_table partitioning_table key_table;

    /* Identify partitioned tables that have no primary key defined in the schema */
    PROC SQL;
        CREATE TABLE Part_tables_without_primary_key AS
        SELECT table_name
        FROM &partitioning_table.
        WHERE partitioned_flg = 1
          AND table_name NOT IN (
              SELECT DISTINCT table_name
              FROM schema_details
              WHERE primary_key = 1
          );
    QUIT;

    %read_metadata_table_csv(IMPORTED_METADATA_TABLE=MISSING_PRIMARY_KEYS);

    /* Derive primary keys from the metadata CSV for partitioned tables that need them */
    PROC SQL;
        CREATE TABLE Primary_keys_to_add AS
        SELECT lowcase(table_name)  AS table_name,
               lowcase(column_name) AS column_name,
               1                    AS primary_key_to_add
        FROM MISSING_PRIMARY_KEYS
        WHERE lowcase(table_name) IN (SELECT table_name FROM Part_tables_without_primary_key)
		/* Aug 2026 bug fix for wrong key in identity_addressable_devices */
		OR lowcase(table_name) = 'identity_addressable_devices'
		;
    QUIT;

    /* Merge CSV-derived keys into the schema detail */
    PROC SQL;
        CREATE TABLE schema_details_with_to_many_keys AS
        SELECT s.*,
               CASE
                   WHEN primary_key_to_add = 1 THEN 1
                   WHEN primary_key = .        THEN 0
                   ELSE primary_key
               END AS full_primary_key
        FROM schema_details s
        LEFT JOIN Primary_keys_to_add a
            ON  s.table_name  = a.table_name
            AND s.column_name = a.column_name;
    QUIT;

    /* Apply partitioning flag — non-partitioned tables get primary_key = 0 */
    PROC SQL;
        CREATE TABLE schema_details_corrected_keys AS
        SELECT s.*,
               p.partitioned_flg,
               CASE
                   WHEN p.partitioned_flg = 0 THEN 0
                   ELSE s.full_primary_key
               END AS corrected_primary_key
        FROM schema_details_with_to_many_keys s
        INNER JOIN &partitioning_table. p
            ON s.table_name = p.table_name;
    QUIT;

    %err_check(Unable to add primary keys 1, &SYSMACRONAME.);
    %if &errFlag %then %do;
        %goto ERREXIT;
    %end;

    /* Write corrected primary keys back into schema_details */
    DATA schema_details (DROP=corrected_primary_key full_primary_key);
        SET schema_details_corrected_keys;
        primary_key = corrected_primary_key;
    RUN;

    /* Build an uppercase key dataset for downstream processing */
    DATA keys (KEEP=mart_type table_name column_name corrected_primary_key partitioned_flg);
        SET schema_details_corrected_keys;
        table_name  = upcase(table_name);
        column_name = upcase(column_name);
    RUN;

    PROC SORT DATA=keys;
        BY mart_type table_name column_name;
    RUN;

    /* Collapse key columns into a comma-separated list per table */
    DATA &key_table. (DROP=column_name corrected_primary_key);
        SET keys;
        LENGTH key_list column_list $4000;
        RETAIN key_list column_list;
        BY mart_type table_name;

        IF corrected_primary_key = 1
            THEN key_list    = catx(',', key_list,    column_name);
            ELSE column_list = catx(',', column_list, column_name);

        IF last.table_name THEN DO;
            OUTPUT;
            key_list    = "";
            column_list = "";
        END;
    RUN;

    %err_check(Unable to add primary keys 2, &SYSMACRONAME.);

    %ERREXIT:
%mend get_primary_keys;
