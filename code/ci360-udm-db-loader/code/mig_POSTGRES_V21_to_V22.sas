/*******************************************************************************/
/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                         */
/* *****************************************************************************/

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..DAILY_USAGE  ADD advertising_members_cnt_str  varchar(4000)  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DAILY_USAGE  ADD mai_scored_project_cnt  bigint  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DAILY_USAGE  ADD sms_frag_cnt  bigint  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DAILY_USAGE  ADD sms_frag_str  varchar(4000)  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DAILY_USAGE  ADD sms_output_result_cnt  bigint  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DAILY_USAGE  ADD sms_output_result_str  varchar(4000)  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DAILY_USAGE  ADD sms_send_cnt  bigint  NULL) BY &database.;
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
   EXECUTE (ALTER TABLE &dbschema..IMPRESSION_SPOT_VIEWABLE  ADD journey_id  varchar(36)  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..IMPRESSION_SPOT_VIEWABLE  ADD journey_occurrence_id  varchar(36)  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: IMPRESSION_SPOT_VIEWABLE ,IMPRESSION_SPOT_VIEWABLE );

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..JOURNEY_EXIT  ADD drop_aud_occurrence_id  varchar(36)  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..JOURNEY_EXIT  ADD drop_audience_id  varchar(36)  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: JOURNEY_EXIT ,JOURNEY_EXIT );

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..JOURNEY_SUCCESS  ADD success_aud_occurrence_id  varchar(36)  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..JOURNEY_SUCCESS  ADD success_audience_id  varchar(36)  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: JOURNEY_SUCCESS ,JOURNEY_SUCCESS );

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..JOURNEY_TEST_SUCCESS  ADD success_aud_occurrence_id  varchar(36)  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..JOURNEY_TEST_SUCCESS  ADD success_audience_id  varchar(36)  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: JOURNEY_TEST_SUCCESS ,JOURNEY_TEST_SUCCESS );

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..MD_EVENT  ADD event_key_cd  varchar(100)  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: MD_EVENT ,MD_EVENT );

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..MD_EVENT_ALL  ADD event_key_cd  varchar(100)  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: MD_EVENT_ALL ,MD_EVENT_ALL );

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..MONTHLY_USAGE  ADD advertising_members_cnt_str  varchar(4000)  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..MONTHLY_USAGE  ADD mai_scored_project_cnt  bigint  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..MONTHLY_USAGE  ADD sms_frag_cnt  bigint  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..MONTHLY_USAGE  ADD sms_frag_str  varchar(4000)  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..MONTHLY_USAGE  ADD sms_output_result_cnt  bigint  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..MONTHLY_USAGE  ADD sms_output_result_str  varchar(4000)  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..MONTHLY_USAGE  ADD sms_send_cnt  bigint  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..MONTHLY_USAGE  ADD sms_send_str  varchar(4000)  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: MONTHLY_USAGE ,MONTHLY_USAGE );

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..SMS_OPTOUT  ADD optout_type_nm  varchar(50)  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: SMS_OPTOUT ,SMS_OPTOUT );

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..SMS_OPTOUT_DETAILS  ADD optout_type_nm  varchar(50)  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: SMS_OPTOUT_DETAILS ,SMS_OPTOUT_DETAILS );
