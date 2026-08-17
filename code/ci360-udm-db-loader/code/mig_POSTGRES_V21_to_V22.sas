/*******************************************************************************/
/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                         */
/* *****************************************************************************/
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..SEGMENT_MEMBERSHIP (
      processed_dttm timestamp NULL, processed_dttm_tz timestamp NULL, segment_id varchar(36) NULL, context_val varchar(256) NULL, 
      context_type_nm varchar(256) NULL, occurrence_id varchar(36) NOT NULL, segment_version_id varchar(36) NOT NULL, user_identifier_val varchar(256) NOT NULL
         )) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..SEGMENT_MEMBERSHIP
      ADD CONSTRAINT SEGMENT_MEMBERSHIP_pk  PRIMARY KEY (OCCURRENCE_ID,SEGMENT_VERSION_ID,USER_IDENTIFIER_VAL)) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SEGMENT_MEMBERSHIP, SEGMENT_MEMBERSHIP);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..DAILY_USAGE  ADD mai_scored_project_cnt  numeric  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DAILY_USAGE  ADD sms_frag_cnt  numeric  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DAILY_USAGE  ADD sms_frag_str  varchar(4000)  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DAILY_USAGE  ADD sms_output_result_cnt  numeric  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DAILY_USAGE  ADD sms_output_result_str  varchar(4000)  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DAILY_USAGE  ADD sms_send_cnt  numeric  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DAILY_USAGE  ADD sms_send_str  varchar(4000)  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: DAILY_USAGE ,DAILY_USAGE );

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..EMAIL_SEND  ADD email_send_agent_name  varchar(128)  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: EMAIL_SEND ,EMAIL_SEND );

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..MONTHLY_USAGE  ADD mai_scored_project_cnt  numeric  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..MONTHLY_USAGE  ADD sms_frag_cnt  numeric  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..MONTHLY_USAGE  ADD sms_frag_str  varchar(4000)  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..MONTHLY_USAGE  ADD sms_output_result_cnt  numeric  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..MONTHLY_USAGE  ADD sms_output_result_str  varchar(4000)  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..MONTHLY_USAGE  ADD sms_send_cnt  numeric  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..MONTHLY_USAGE  ADD sms_send_str  varchar(4000)  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: MONTHLY_USAGE ,MONTHLY_USAGE );
