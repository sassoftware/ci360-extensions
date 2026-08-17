/*******************************************************************************/
/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                         */
/* *****************************************************************************/
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DECISION_EXECUTION (
      input_json string, output_json string, execution_dttm timestamp, environment string, 
      hostname string, decision_id string, event_nm string, execution_id string NOT NULL, 
      image_url string    )) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DECISION_EXECUTION
      ADD PRIMARY KEY (EXECUTION_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DECISION_EXECUTION, DECISION_EXECUTION);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..IDENTITY_ADDRESSABLE_DEVICES (
      reachable_flg string, entrytime timestamp NOT NULL, mobile_app_id string, device_id string NOT NULL, 
      identity_id string NOT NULL    )) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..IDENTITY_ADDRESSABLE_DEVICES
      ADD PRIMARY KEY (DEVICE_ID,ENTRYTIME,IDENTITY_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: IDENTITY_ADDRESSABLE_DEVICES, IDENTITY_ADDRESSABLE_DEVICES);
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
   EXECUTE (ALTER TABLE &dbschema..DAILY_USAGE  ADD advertising_members_cnt_str  STRING  NULL) BY &database.;
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
   EXECUTE (ALTER TABLE &dbschema..IMPRESSION_SPOT_VIEWABLE  ADD journey_id  string  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..IMPRESSION_SPOT_VIEWABLE  ADD journey_occurrence_id  string  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: IMPRESSION_SPOT_VIEWABLE ,IMPRESSION_SPOT_VIEWABLE );

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..JOURNEY_EXIT  ADD drop_aud_occurrence_id  string  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..JOURNEY_EXIT  ADD drop_audience_id  string  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: JOURNEY_EXIT ,JOURNEY_EXIT );

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..JOURNEY_SUCCESS  ADD success_aud_occurrence_id  string  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..JOURNEY_SUCCESS  ADD success_audience_id  string  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: JOURNEY_SUCCESS ,JOURNEY_SUCCESS );

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..JOURNEY_TEST_SUCCESS  ADD success_aud_occurrence_id  string  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..JOURNEY_TEST_SUCCESS  ADD success_audience_id  string  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: JOURNEY_TEST_SUCCESS ,JOURNEY_TEST_SUCCESS );

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..MD_EVENT  ADD event_key_cd  string  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: MD_EVENT ,MD_EVENT );

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..MD_EVENT_ALL  ADD event_key_cd  string  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: MD_EVENT_ALL ,MD_EVENT_ALL );

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..MONTHLY_USAGE  ADD advertising_members_cnt_str  STRING  NULL) BY &database.;
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

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..SMS_OPTOUT  ADD optout_type_nm  string  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: SMS_OPTOUT ,SMS_OPTOUT );

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..SMS_OPTOUT_DETAILS  ADD optout_type_nm  string  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: SMS_OPTOUT_DETAILS ,SMS_OPTOUT_DETAILS );
