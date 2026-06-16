/******************************************************************************/
/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.*/
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/

%macro get_cluster_column(metadata_table=);
    %local metadata_table;

    PROC SQL;
        CREATE TABLE use_identity_id_as_cluster_col AS
        SELECT DISTINCT table_name
        FROM &metadata_table.
        WHERE column_name = "identity_id"
        ORDER BY table_name;

        CREATE TABLE needs_partition_and_clustering AS
        SELECT DISTINCT table_name
        FROM &metadata_table.
        WHERE partition_column = 1
        ORDER BY table_name;

        CREATE TABLE has_session_id AS
        SELECT DISTINCT table_name
        FROM &metadata_table.
        WHERE column_name = "session_id"
        ORDER BY table_name;
    QUIT;

    DATA has_pk_no_idid;
        MERGE use_identity_id_as_cluster_col (IN=i)
              needs_partition_and_clustering  (IN=p);
        BY table_name;
        IF p AND NOT i THEN OUTPUT;
    RUN;

    DATA use_session_id_as_cluster_col;
        MERGE has_session_id (IN=s)
              has_pk_no_idid (IN=pni);
        BY table_name;
        IF pni AND s THEN OUTPUT;
    RUN;

    DATA still_has_no_cluster_col;
        MERGE use_session_id_as_cluster_col (IN=s)
              has_pk_no_idid                (IN=pni);
        BY table_name;
        IF pni AND NOT s THEN OUTPUT;
    RUN;

    PROC SQL;
        CREATE TABLE use_fk_col_as_cluster_col AS
        SELECT table_name, column_name
        FROM &metadata_table.
        WHERE table_name IN (SELECT table_name FROM still_has_no_cluster_col)
          AND foreign_key = 1
        ORDER BY table_name;
    QUIT;

    DATA no_clustering_or_use_any_column;
        MERGE use_fk_col_as_cluster_col (IN=f)
              still_has_no_cluster_col  (IN=pni);
        BY table_name;
        IF pni AND NOT f THEN OUTPUT;
    RUN;

    PROC SQL;
        CREATE TABLE cluster_column AS

        SELECT t.table_name,
               "IDENTITY_ID" AS cluster_col LENGTH=32,
               "IDENTITY_ID" AS reason      LENGTH=40
        FROM needs_partition_and_clustering t
        INNER JOIN use_identity_id_as_cluster_col i
            ON t.table_name = i.table_name

        UNION ALL

        SELECT t.table_name,
               "SESSION_ID" AS cluster_col,
               "SESSION_ID" AS reason
        FROM has_pk_no_idid t
        INNER JOIN use_session_id_as_cluster_col s
            ON t.table_name = s.table_name

        UNION ALL

        SELECT f.table_name,
               f.fks AS cluster_col,
               "FOREIGN_KEY" AS reason
        FROM (
            SELECT table_name,
                   MIN(column_name) AS fks
            FROM use_fk_col_as_cluster_col
            GROUP BY table_name
        ) f
        ;
    QUIT;

%mend get_cluster_column;
