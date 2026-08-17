/******************************************************************************/
/* Copyright(r) 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/

%macro create_migration_ddl;

    %let code_file_path = &codes_path.&slash.mig_&database._V&previous_schema_version._to_V&schema_version..sas;

    %get_authentication_token;

    /* Fetch schema for both the previous and current versions */
    %get_udm_schema(
        api_schema_version=&previous_schema_version.,
        schema_table=schema_details_previous,
        partitioning_table=partitioning_table_previous
    );
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

    /* Collect all table names from the previous schema */
    PROC SQL;
        CREATE TABLE existing_tables AS
        SELECT DISTINCT table_name
        FROM schema_details_previous;
    QUIT;

    /* Identify new tables vs new columns in the current schema */
    PROC SQL;
        CREATE TABLE new_tables_and_columns AS
        SELECT n.*,
               CASE
                   WHEN p.column_name IS NULL
                        AND upcase(n.table_name) NOT IN (SELECT upcase(table_name) FROM existing_tables)
                        THEN "new table"
                   WHEN p.column_name IS NULL
                        THEN "new column"
                   ELSE "no update"
               END AS upgrade_activity LENGTH=10
        FROM column_metadata n
        LEFT JOIN schema_details_previous p
            ON  upcase(n.table_name)  = upcase(p.table_name)
            AND n.column_name         = p.column_name
        WHERE p.column_name IS NULL
        ORDER BY n.table_name, n.column_name;
    QUIT;

    /* ------------------------------------------------------------------ */
    /* Initialise output file                                              */
    /* ------------------------------------------------------------------ */
    %if %sysfunc(fileexist("&code_file_path.")) GE 1 %then %do;
        %let rc = %sysfunc(filename(temp, "&code_file_path."));
        %let rc = %sysfunc(fdelete(&temp.));
        %put Old &code_file_path. deleted...;
    %end;

    FILENAME codeout DISK "&code_file_path.";
    DATA _NULL_;
        FILE codeout;
        PUT '/*******************************************************************************/';
        PUT '/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */';
        PUT '/* SPDX-License-Identifier: Apache-2.0                                         */';
        PUT '/* *****************************************************************************/';
    RUN;
    FILENAME codeout;

    /* ------------------------------------------------------------------ */
    /* CREATE new tables                                                   */
    /* ------------------------------------------------------------------ */
    PROC SQL;
        CREATE TABLE new_table_list AS
        SELECT DISTINCT upcase(n.table_name) AS table_name, k.key_list
        FROM new_tables_and_columns n
        INNER JOIN key_table k
            ON lowcase(n.table_name) = lowcase(k.table_name)
        WHERE n.upgrade_activity = "new table";
    QUIT;

    FILENAME sascode TEMP;
    DATA _NULL_;
        SET new_table_list;
        FILE sascode;
        PUT '%create_ddl(database=&database., table_name=' table_name
            ', column_table=column_metadata, key_list="' key_list +(-1) '");';
    RUN;
    %include sascode;
    FILENAME sascode;

    /* ------------------------------------------------------------------ */
    /* ALTER existing tables to add new columns                            */
    /* ------------------------------------------------------------------ */
    PROC SQL;
        CREATE TABLE add_columns AS
        SELECT n.table_name,
               n.column_name,
               n.rdbms_column_type AS data_type,
               n.primary_key
        FROM new_tables_and_columns n
        WHERE n.upgrade_activity ? "new column";
    QUIT;

    FILENAME codeout DISK "&code_file_path.";
    DATA _NULL_;
        SET add_columns;
        BY table_name;
        FILE codeout MOD;

        IF first.table_name THEN DO;
            PUT;
            PUT "PROC SQL;";
            PUT 'CONNECT TO &database. (&sql_passthru_connection);';
        END;

        /* One EXECUTE block per column */
        PUT +3 'EXECUTE (ALTER TABLE &dbschema..' table_name
            ' ADD ' column_name ' ' data_type ' NULL) BY &database.;';

        /* Warn when a primary key column changes — requires manual intervention */
        IF primary_key = 1 THEN DO;
            PUTLOG "ERROR: The primary key of " table_name "has changed. Manual steps are required.";
            PUTLOG "ERROR- For snapshot tables, drop the table and its primary key, and recreate them.";
            PUTLOG "ERROR- For tables with historical data, execute these steps after running this migration script.";
            PUTLOG;
            PUTLOG "NOTE: Step 1: Primary key columns must contain unique data and cannot be blank.";
            PUTLOG "NOTE- Generate UUIDs for historical rows using a DB-specific UUID function:";
            PUTLOG "NOTE-     SQL Server  : newid()";
            PUTLOG "NOTE-     Oracle      : raw_to_uuid(uuid())";
            PUTLOG "NOTE-     BigQuery    : GENERATE_UUID()";
            PUTLOG "NOTE-     PostgreSQL  : uuidv4()";
            PUTLOG "NOTE- Syntax:";
            PUTLOG "NOTE- EXECUTE (UPDATE &dbschema.." table_name ' SET ' column_name " = <uuid_function>";
            PUTLOG "NOTE-              WHERE " column_name " IS NULL) BY &database.;";
            PUTLOG;
            PUTLOG "NOTE: Step 2: Change the column to NOT NULL.";
            PUTLOG "NOTE- Syntax:";
            PUTLOG "NOTE- EXECUTE (ALTER TABLE &dbschema.." table_name ' ALTER COLUMN  ' column_name data_type " NOT NULL) BY &database.;";
            PUTLOG;
            PUTLOG "NOTE: Step 3: Drop and recreate the primary key.";
            PUTLOG "NOTE- DB-, cluster-, and partition-specific syntax for the primary key of " table_name " is in:";
            PUTLOG "NOTE- &codes_path.&slash.&database._V&schema_version._DDL.sas";
            PUTLOG;
        END;

        IF last.table_name THEN DO;
            PUT 'DISCONNECT FROM &database.;';
            PUT 'QUIT;';
            PUT '%err_check (Failed to alter Table: ' table_name ',' table_name ');';
        END;
    RUN;
    FILENAME codeout;

%mend create_migration_ddl;
