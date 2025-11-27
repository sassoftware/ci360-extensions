 PROC SQL ;
 CONNECT TO REDSHIFT (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_audience_x_segment(
        audience_id varchar(36) NOT NULL
        ,
        segment_id varchar(36) NULL
        )) BY REDSHIFT;
      execute (alter table &dbschema..cdm_audience_x_segment add constraint cdm_audience_x_segment_pk primary key (audience_id)) BY REDSHIFT;
 DISCONNECT FROM REDSHIFT;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_audience_x_segment, cdm_audience_x_segment);
 PROC SQL ;
 CONNECT TO REDSHIFT (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_audience_x_segment(
        audience_id varchar(36) NOT NULL
        ,
        segment_id varchar(36) NULL
        )) BY REDSHIFT;
      execute (alter table &dbschema..md_audience_x_segment add constraint md_audience_x_segment_pk primary key (audience_id)) BY REDSHIFT;
 DISCONNECT FROM REDSHIFT;
 QUIT;
 %ErrCheck (Failed to create Table: md_audience_x_segment, md_audience_x_segment);

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..daily_usage  ADD customer_profiles_processed_str  varchar(.)  NULL) BY &DBNAME;
DISCONNECT FROM &DBNAME;
QUIT;
%ErrCheck (Failed to alter table: daily_usage , daily_usage );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..impression_delivered  ADD journey_id  varchar(36)  NULL) BY &DBNAME;
   EXECUTE (ALTER TABLE &dbschema..impression_delivered  ADD journey_occurrence_id  varchar(36)  NULL) BY &DBNAME;
DISCONNECT FROM &DBNAME;
QUIT;
%ErrCheck (Failed to alter table: impression_delivered , impression_delivered );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..journey_entry  ADD context_type_nm  varchar(256)  NULL) BY &DBNAME;
   EXECUTE (ALTER TABLE &dbschema..journey_entry  ADD context_val  varchar(256)  NULL) BY &DBNAME;
DISCONNECT FROM &DBNAME;
QUIT;
%ErrCheck (Failed to alter table: journey_entry , journey_entry );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..journey_exit  ADD context_type_nm  varchar(256)  NULL) BY &DBNAME;
   EXECUTE (ALTER TABLE &dbschema..journey_exit  ADD context_val  varchar(256)  NULL) BY &DBNAME;
DISCONNECT FROM &DBNAME;
QUIT;
%ErrCheck (Failed to alter table: journey_exit , journey_exit );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..journey_holdout  ADD context_type_nm  varchar(256)  NULL) BY &DBNAME;
   EXECUTE (ALTER TABLE &dbschema..journey_holdout  ADD context_val  varchar(256)  NULL) BY &DBNAME;
DISCONNECT FROM &DBNAME;
QUIT;
%ErrCheck (Failed to alter table: journey_holdout , journey_holdout );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..journey_node_entry  ADD context_type_nm  varchar(256)  NULL) BY &DBNAME;
   EXECUTE (ALTER TABLE &dbschema..journey_node_entry  ADD context_val  varchar(256)  NULL) BY &DBNAME;
DISCONNECT FROM &DBNAME;
QUIT;
%ErrCheck (Failed to alter table: journey_node_entry , journey_node_entry );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..journey_success  ADD context_type_nm  varchar(256)  NULL) BY &DBNAME;
   EXECUTE (ALTER TABLE &dbschema..journey_success  ADD context_val  varchar(256)  NULL) BY &DBNAME;
DISCONNECT FROM &DBNAME;
QUIT;
%ErrCheck (Failed to alter table: journey_success , journey_success );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..journey_suppression  ADD context_type_nm  varchar(256)  NULL) BY &DBNAME;
   EXECUTE (ALTER TABLE &dbschema..journey_suppression  ADD context_val  varchar(256)  NULL) BY &DBNAME;
DISCONNECT FROM &DBNAME;
QUIT;
%ErrCheck (Failed to alter table: journey_suppression , journey_suppression );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..monthly_usage  ADD customer_profiles_processed_str  varchar(.)  NULL) BY &DBNAME;
DISCONNECT FROM &DBNAME;
QUIT;
%ErrCheck (Failed to alter table: monthly_usage , monthly_usage );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..notification_targeting_request  ADD task_id  varchar(36)  NULL) BY &DBNAME;
DISCONNECT FROM &DBNAME;
QUIT;
%ErrCheck (Failed to alter table: notification_targeting_request , notification_targeting_request );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..search_results_ext  ADD event_id  varchar(36)  NULL) BY &DBNAME;
DISCONNECT FROM &DBNAME;
QUIT;
%ErrCheck (Failed to alter table: search_results_ext , search_results_ext );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..sms_message_clicked  ADD journey_id  varchar(36)  NULL) BY &DBNAME;
   EXECUTE (ALTER TABLE &dbschema..sms_message_clicked  ADD journey_occurrence_id  varchar(36)  NULL) BY &DBNAME;
DISCONNECT FROM &DBNAME;
QUIT;
%ErrCheck (Failed to alter table: sms_message_clicked , sms_message_clicked );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..sms_message_delivered  ADD journey_id  varchar(36)  NULL) BY &DBNAME;
   EXECUTE (ALTER TABLE &dbschema..sms_message_delivered  ADD journey_occurrence_id  varchar(36)  NULL) BY &DBNAME;
DISCONNECT FROM &DBNAME;
QUIT;
%ErrCheck (Failed to alter table: sms_message_delivered , sms_message_delivered );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..sms_message_failed  ADD journey_id  varchar(36)  NULL) BY &DBNAME;
   EXECUTE (ALTER TABLE &dbschema..sms_message_failed  ADD journey_occurrence_id  varchar(36)  NULL) BY &DBNAME;
DISCONNECT FROM &DBNAME;
QUIT;
%ErrCheck (Failed to alter table: sms_message_failed , sms_message_failed );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..sms_message_reply  ADD journey_id  varchar(36)  NULL) BY &DBNAME;
   EXECUTE (ALTER TABLE &dbschema..sms_message_reply  ADD journey_occurrence_id  varchar(36)  NULL) BY &DBNAME;
DISCONNECT FROM &DBNAME;
QUIT;
%ErrCheck (Failed to alter table: sms_message_reply , sms_message_reply );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..sms_message_send  ADD journey_id  varchar(36)  NULL) BY &DBNAME;
   EXECUTE (ALTER TABLE &dbschema..sms_message_send  ADD journey_occurrence_id  varchar(36)  NULL) BY &DBNAME;
DISCONNECT FROM &DBNAME;
QUIT;
%ErrCheck (Failed to alter table: sms_message_send , sms_message_send );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..sms_optout  ADD journey_id  varchar(36)  NULL) BY &DBNAME;
   EXECUTE (ALTER TABLE &dbschema..sms_optout  ADD journey_occurrence_id  varchar(36)  NULL) BY &DBNAME;
DISCONNECT FROM &DBNAME;
QUIT;
%ErrCheck (Failed to alter table: sms_optout , sms_optout );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..sms_optout_details  ADD journey_id  varchar(36)  NULL) BY &DBNAME;
   EXECUTE (ALTER TABLE &dbschema..sms_optout_details  ADD journey_occurrence_id  varchar(36)  NULL) BY &DBNAME;
DISCONNECT FROM &DBNAME;
QUIT;
%ErrCheck (Failed to alter table: sms_optout_details , sms_optout_details );
