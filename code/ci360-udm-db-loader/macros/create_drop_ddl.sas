/*******************************************************************************/
/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                         */
/* *****************************************************************************/

/* NOTE: This file is not a macro — it is a standalone script used during      */
/* development to drop all UDM tables from the target schema.                  */

%let sysparameter = DROPDDL;
%include "/userdata/dev/common/projects/UDMLoader_Git/cdm-udmloader-sas/config/config.sas";

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

%let code_file_path = &codes_path.&slash.DROP_V&schema_version._DDL.sas;

FILENAME ddlfile "&code_file_path.";
DATA _NULL_;
    SET key_table END=last;
    FILE ddlfile;

    IF _n_ = 1 THEN DO;
        PUT 'PROC SQL NOERRORSTOP;';
        PUT +3 'CONNECT TO &database. (&sql_passthru_connection.);';
    END;

    PUT +3 'EXECUTE (DROP TABLE IF EXISTS &dbschema..' table_name ') BY &database.;';

    IF last THEN DO;
        PUT +3 'DISCONNECT FROM &database.;';
        PUT 'QUIT;';
    END;
RUN;
FILENAME ddlfile;

PROC PRINTTO; RUN;
