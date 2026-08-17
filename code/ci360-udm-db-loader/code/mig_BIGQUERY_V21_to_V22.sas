/*******************************************************************************/
/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                         */
/* *****************************************************************************/
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..SEGMENT_MEMBERSHIP (
      processed_dttm timestamp, processed_dttm_tz timestamp, segment_id string, context_val string, 
      context_type_nm string, occurrence_id string NOT NULL, segment_version_id string NOT NULL, user_identifier_val string NOT NULL
         )) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..SEGMENT_MEMBERSHIP
      ADD PRIMARY KEY (OCCURRENCE_ID,SEGMENT_VERSION_ID,USER_IDENTIFIER_VAL) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SEGMENT_MEMBERSHIP, SEGMENT_MEMBERSHIP);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..DAILY_USAGE  ADD mai_scored_project_cnt  int64  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DAILY_USAGE  ADD sms_frag_cnt  int64  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DAILY_USAGE  ADD sms_frag_str  string  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DAILY_USAGE  ADD sms_output_result_cnt  int64  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DAILY_USAGE  ADD sms_output_result_str  string  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DAILY_USAGE  ADD sms_send_cnt  int64  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DAILY_USAGE  ADD sms_send_str  string  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: DAILY_USAGE ,DAILY_USAGE );

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..EMAIL_SEND  ADD email_send_agent_name  string  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: EMAIL_SEND ,EMAIL_SEND );

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..MONTHLY_USAGE  ADD mai_scored_project_cnt  int64  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..MONTHLY_USAGE  ADD sms_frag_cnt  int64  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..MONTHLY_USAGE  ADD sms_frag_str  string  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..MONTHLY_USAGE  ADD sms_output_result_cnt  int64  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..MONTHLY_USAGE  ADD sms_output_result_str  string  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..MONTHLY_USAGE  ADD sms_send_cnt  int64  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..MONTHLY_USAGE  ADD sms_send_str  string  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: MONTHLY_USAGE ,MONTHLY_USAGE );
