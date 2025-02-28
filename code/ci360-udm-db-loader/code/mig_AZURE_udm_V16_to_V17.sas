 PROC SQL ;
 connect to SQLSVR (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE journey_entry(
        event_id varchar(36) NOT NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,entry_dttm datetime2 NULL ,entry_dttm_tz datetime2 NULL ,event_nm varchar(256) NULL ,identity_id varchar(36) NULL ,identity_type_nm varchar(100) NULL ,identity_type_val varchar(300) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm datetime2 NULL
        )) by SQLSVR;
      execute (alter table journey_entry add constraint journey_entry_pk primary key (event_id)) by SQLSVR;
 DISCONNECT FROM SQLSVR;
 QUIT;
 %ErrCheck (Failed to create Table: journey_entry, journey_entry);
 PROC SQL ;
 connect to SQLSVR (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE journey_exit(
        event_id varchar(36) NOT NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,event_nm varchar(256) NULL ,exit_dttm datetime2 NULL ,exit_dttm_tz datetime2 NULL ,identity_id varchar(36) NULL ,identity_type_nm varchar(100) NULL ,identity_type_val varchar(300) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,last_node_id varchar(36) NULL ,load_dttm datetime2 NULL ,reason_cd varchar(100) NULL ,reason_txt varchar(1000) NULL
        )) by SQLSVR;
      execute (alter table journey_exit add constraint journey_exit_pk primary key (event_id)) by SQLSVR;
 DISCONNECT FROM SQLSVR;
 QUIT;
 %ErrCheck (Failed to create Table: journey_exit, journey_exit);
 PROC SQL ;
 connect to SQLSVR (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE journey_holdout(
        event_id varchar(36) NOT NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,event_nm varchar(256) NULL ,holdout_dttm datetime2 NULL ,holdout_dttm_tz datetime2 NULL ,identity_id varchar(36) NULL ,identity_type_nm varchar(100) NULL ,identity_type_val varchar(300) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm datetime2 NULL
        )) by SQLSVR;
      execute (alter table journey_holdout add constraint journey_holdout_pk primary key (event_id)) by SQLSVR;
 DISCONNECT FROM SQLSVR;
 QUIT;
 %ErrCheck (Failed to create Table: journey_holdout, journey_holdout);
 PROC SQL ;
 connect to SQLSVR (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE journey_node_entry(
        event_id varchar(36) NOT NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,event_nm varchar(256) NULL ,identity_id varchar(36) NULL ,identity_type_nm varchar(100) NULL ,identity_type_val varchar(300) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm datetime2 NULL ,node_entry_dttm datetime2 NULL ,node_entry_dttm_tz datetime2 NULL ,node_id varchar(36) NULL ,node_type_nm varchar(256) NULL ,previous_node_id varchar(36) NULL
        )) by SQLSVR;
      execute (alter table journey_node_entry add constraint journey_node_entry_pk primary key (event_id)) by SQLSVR;
 DISCONNECT FROM SQLSVR;
 QUIT;
 %ErrCheck (Failed to create Table: journey_node_entry, journey_node_entry);
 PROC SQL ;
 connect to SQLSVR (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE journey_success(
        event_id varchar(36) NOT NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,event_nm varchar(256) NULL ,identity_id varchar(36) NULL ,identity_type_nm varchar(100) NULL ,identity_type_val varchar(300) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm datetime2 NULL ,success_dttm datetime2 NULL ,success_dttm_tz datetime2 NULL ,success_val int NULL ,unit_qty int NULL
        )) by SQLSVR;
      execute (alter table journey_success add constraint journey_success_pk primary key (event_id)) by SQLSVR;
 DISCONNECT FROM SQLSVR;
 QUIT;
 %ErrCheck (Failed to create Table: journey_success, journey_success);
 PROC SQL ;
 connect to SQLSVR (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE journey_suppression(
        event_id varchar(36) NOT NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,event_nm varchar(256) NULL ,identity_id varchar(36) NULL ,identity_type_nm varchar(100) NULL ,identity_type_val varchar(300) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm datetime2 NULL ,reason_cd varchar(100) NULL ,reason_txt varchar(1000) NULL ,suppression_dttm datetime2 NULL ,suppression_dttm_tz datetime2 NULL
        )) by SQLSVR;
      execute (alter table journey_suppression add constraint journey_suppression_pk primary key (event_id)) by SQLSVR;
 DISCONNECT FROM SQLSVR;
 QUIT;
 %ErrCheck (Failed to create Table: journey_suppression, journey_suppression);
 PROC SQL ;
 connect to SQLSVR (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE md_journey(
        journey_version_id varchar(36) NOT NULL
        ,
        activated_user_nm varchar(256) NULL ,control_group_flg char(1) NULL ,created_user_nm varchar(256) NULL ,journey_id varchar(36) NULL ,journey_nm varchar(256) NULL ,journey_status_cd varchar(20) NULL ,last_activated_dttm datetime2 NULL ,purpose_id varchar(36) NULL ,target_goal_qty int NULL ,target_goal_type_nm varchar(20) NULL
        )) by SQLSVR;
      execute (alter table md_journey add constraint md_journey_pk primary key (journey_version_id)) by SQLSVR;
 DISCONNECT FROM SQLSVR;
 QUIT;
 %ErrCheck (Failed to create Table: md_journey, md_journey);
 PROC SQL ;
 connect to SQLSVR (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE md_journey_node(
        journey_node_id varchar(36) NOT NULL
        ,
        journey_id varchar(36) NULL ,journey_version_id varchar(36) NULL ,next_node_id varchar(36) NULL ,node_nm varchar(100) NULL ,node_type varchar(36) NULL ,previous_node_id varchar(36) NULL
        )) by SQLSVR;
      execute (alter table md_journey_node add constraint md_journey_node_pk primary key (journey_node_id)) by SQLSVR;
 DISCONNECT FROM SQLSVR;
 QUIT;
 %ErrCheck (Failed to create Table: md_journey_node, md_journey_node);
 PROC SQL ;
 connect to SQLSVR (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE md_journey_node_occurrence(
        journey_node_occurrence_id varchar(36) NOT NULL
        ,
        end_dttm datetime2 NULL ,error_messages varchar(256) NULL ,execution_status varchar(36) NULL ,journey_id varchar(36) NULL ,journey_node_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,journey_version_id varchar(36) NULL ,num_of_contacts_entered int NULL ,start_dttm datetime2 NULL
        )) by SQLSVR;
      execute (alter table md_journey_node_occurrence add constraint md_journey_node_occurrenc_pk primary key (journey_node_occurrence_id)) by SQLSVR;
 DISCONNECT FROM SQLSVR;
 QUIT;
 %ErrCheck (Failed to create Table: md_journey_node_occurrence, md_journey_node_occurrence);
 PROC SQL ;
 connect to SQLSVR (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE md_journey_occurrence(
        journey_occurrence_id varchar(36) NOT NULL
        ,
        aud_occurrence_id varchar(36) NULL ,end_dttm datetime2 NULL ,error_messages varchar(256) NULL ,execution_status varchar(36) NULL ,journey_id varchar(36) NULL ,journey_occurrence_num int NULL ,journey_version_id varchar(36) NULL ,num_of_contacts_entered int NULL ,num_of_contacts_suppressed int NULL ,occurrence_type_nm varchar(36) NULL ,start_dttm datetime2 NULL ,started_by_nm varchar(128) NULL
        )) by SQLSVR;
      execute (alter table md_journey_occurrence add constraint md_journey_occurrence_pk primary key (journey_occurrence_id)) by SQLSVR;
 DISCONNECT FROM SQLSVR;
 QUIT;
 %ErrCheck (Failed to create Table: md_journey_occurrence, md_journey_occurrence);
 PROC SQL ;
 connect to SQLSVR (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE md_journey_x_audience(
        audience_id varchar(36) NOT NULL ,journey_version_id varchar(36) NOT NULL
        ,
        aud_relationship_nm varchar(100) NULL ,journey_id varchar(36) NULL ,journey_node_id varchar(36) NULL
        )) by SQLSVR;
      execute (alter table md_journey_x_audience add constraint md_journey_x_audience_pk primary key (audience_id,journey_version_id)) by SQLSVR;
 DISCONNECT FROM SQLSVR;
 QUIT;
 %ErrCheck (Failed to create Table: md_journey_x_audience, md_journey_x_audience);
 PROC SQL ;
 connect to SQLSVR (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE md_journey_x_event(
        event_id varchar(36) NOT NULL ,journey_node_id varchar(36) NOT NULL
        ,
        event_relationship_nm varchar(100) NULL ,journey_id varchar(36) NULL ,journey_version_id varchar(36) NULL
        )) by SQLSVR;
      execute (alter table md_journey_x_event add constraint md_journey_x_event_pk primary key (event_id,journey_node_id)) by SQLSVR;
 DISCONNECT FROM SQLSVR;
 QUIT;
 %ErrCheck (Failed to create Table: md_journey_x_event, md_journey_x_event);
 PROC SQL ;
 connect to SQLSVR (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE md_journey_x_task(
        journey_node_id varchar(36) NOT NULL
        ,
        journey_id varchar(36) NULL ,journey_version_id varchar(36) NULL ,task_id varchar(36) NULL ,task_version_id varchar(36) NULL
        )) by SQLSVR;
      execute (alter table md_journey_x_task add constraint md_journey_x_task_pk primary key (journey_node_id)) by SQLSVR;
 DISCONNECT FROM SQLSVR;
 QUIT;
 %ErrCheck (Failed to create Table: md_journey_x_task, md_journey_x_task);
 PROC SQL ;
 connect to SQLSVR (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE md_purpose(
        purpose_id varchar(36) NOT NULL
        ,
        purpose_nm varchar(256) NULL
        )) by SQLSVR;
      execute (alter table md_purpose add constraint md_purpose_pk primary key (purpose_id)) by SQLSVR;
 DISCONNECT FROM SQLSVR;
 QUIT;
 %ErrCheck (Failed to create Table: md_purpose, md_purpose);
 PROC SQL ;
 connect to SQLSVR (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE sms_message_clicked(
        event_id varchar(36) NOT NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,country_cd varchar(3) NULL ,creative_id varchar(36) NULL ,creative_version_id varchar(36) NULL ,event_designed_id varchar(36) NULL ,event_nm varchar(256) NULL ,identity_id varchar(36) NULL ,load_dttm datetime2 NULL ,occurrence_id varchar(36) NULL ,response_tracking_cd varchar(36) NULL ,sender_id varchar(40) NULL ,sms_click_dttm datetime2 NULL ,sms_click_dttm_tz datetime2 NULL ,sms_message_id varchar(40) NULL ,task_id varchar(36) NULL ,task_version_id varchar(36) NULL
        )) by SQLSVR;
      execute (alter table sms_message_clicked add constraint sms_message_clicked_pk primary key (event_id)) by SQLSVR;
 DISCONNECT FROM SQLSVR;
 QUIT;
 %ErrCheck (Failed to create Table: sms_message_clicked, sms_message_clicked);
 PROC SQL ;
 connect to SQLSVR (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE sms_message_delivered(
        event_id varchar(36) NOT NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,country_cd varchar(3) NULL ,creative_id varchar(36) NULL ,creative_version_id varchar(36) NULL ,event_designed_id varchar(36) NULL ,event_nm varchar(256) NULL ,identity_id varchar(36) NULL ,load_dttm datetime2 NULL ,occurrence_id varchar(36) NULL ,response_tracking_cd varchar(36) NULL ,sender_id varchar(40) NULL ,sms_delivered_dttm datetime2 NULL ,sms_delivered_dttm_tz datetime2 NULL ,sms_message_id varchar(40) NULL ,task_id varchar(36) NULL ,task_version_id varchar(36) NULL
        )) by SQLSVR;
      execute (alter table sms_message_delivered add constraint sms_message_delivered_pk primary key (event_id)) by SQLSVR;
 DISCONNECT FROM SQLSVR;
 QUIT;
 %ErrCheck (Failed to create Table: sms_message_delivered, sms_message_delivered);
 PROC SQL ;
 connect to SQLSVR (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE sms_message_failed(
        event_id varchar(36) NOT NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,country_cd varchar(3) NULL ,creative_id varchar(36) NULL ,creative_version_id varchar(36) NULL ,event_designed_id varchar(36) NULL ,event_nm varchar(256) NULL ,identity_id varchar(36) NULL ,load_dttm datetime2 NULL ,occurrence_id varchar(36) NULL ,reason_cd varchar(5) NULL ,reason_description_txt varchar(1500) NULL ,response_tracking_cd varchar(36) NULL ,sender_id varchar(40) NULL ,sms_failed_dttm datetime2 NULL ,sms_failed_dttm_tz datetime2 NULL ,sms_message_id varchar(40) NULL ,task_id varchar(36) NULL ,task_version_id varchar(36) NULL
        )) by SQLSVR;
      execute (alter table sms_message_failed add constraint sms_message_failed_pk primary key (event_id)) by SQLSVR;
 DISCONNECT FROM SQLSVR;
 QUIT;
 %ErrCheck (Failed to create Table: sms_message_failed, sms_message_failed);
 PROC SQL ;
 connect to SQLSVR (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE sms_message_reply(
        event_id varchar(36) NOT NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,country_cd varchar(3) NULL ,event_designed_id varchar(36) NULL ,event_nm varchar(256) NULL ,identity_id varchar(36) NULL ,load_dttm datetime2 NULL ,occurrence_id varchar(36) NULL ,response_tracking_cd varchar(36) NULL ,sender_id varchar(40) NULL ,sms_message_id varchar(40) NULL ,sms_reply_dttm datetime2 NULL ,sms_reply_dttm_tz datetime2 NULL ,task_id varchar(36) NULL ,task_version_id varchar(36) NULL
        )) by SQLSVR;
      execute (alter table sms_message_reply add constraint sms_message_reply_pk primary key (event_id)) by SQLSVR;
 DISCONNECT FROM SQLSVR;
 QUIT;
 %ErrCheck (Failed to create Table: sms_message_reply, sms_message_reply);
 PROC SQL ;
 connect to SQLSVR (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE sms_message_send(
        event_id varchar(36) NOT NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,country_cd varchar(3) NULL ,creative_id varchar(36) NULL ,creative_version_id varchar(36) NULL ,event_designed_id varchar(36) NULL ,event_nm varchar(256) NULL ,fragment_cnt int NULL ,identity_id varchar(36) NULL ,load_dttm datetime2 NULL ,occurrence_id varchar(36) NULL ,response_tracking_cd varchar(36) NULL ,sender_id varchar(40) NULL ,sms_message_id varchar(40) NULL ,sms_send_dttm datetime2 NULL ,sms_send_dttm_tz datetime2 NULL ,task_id varchar(36) NULL ,task_version_id varchar(36) NULL
        )) by SQLSVR;
      execute (alter table sms_message_send add constraint sms_message_send_pk primary key (event_id)) by SQLSVR;
 DISCONNECT FROM SQLSVR;
 QUIT;
 %ErrCheck (Failed to create Table: sms_message_send, sms_message_send);
 PROC SQL ;
 connect to SQLSVR (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE sms_optout(
        event_id varchar(36) NOT NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,country_cd varchar(3) NULL ,creative_id varchar(36) NULL ,creative_version_id varchar(36) NULL ,event_designed_id varchar(36) NULL ,event_nm varchar(256) NULL ,identity_id varchar(36) NULL ,load_dttm datetime2 NULL ,occurrence_id varchar(36) NULL ,response_tracking_cd varchar(36) NULL ,sender_id varchar(40) NULL ,sms_message_id varchar(40) NULL ,sms_optout_dttm datetime2 NULL ,sms_optout_dttm_tz datetime2 NULL ,task_id varchar(36) NULL ,task_version_id varchar(36) NULL
        )) by SQLSVR;
      execute (alter table sms_optout add constraint sms_optout_pk primary key (event_id)) by SQLSVR;
 DISCONNECT FROM SQLSVR;
 QUIT;
 %ErrCheck (Failed to create Table: sms_optout, sms_optout);
 PROC SQL ;
 connect to SQLSVR (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE sms_optout_details(
        event_id varchar(36) NOT NULL
        ,
        address_val varchar(20) NULL ,aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,country_cd varchar(3) NULL ,creative_id varchar(36) NULL ,creative_version_id varchar(36) NULL ,event_designed_id varchar(36) NULL ,event_nm varchar(256) NULL ,identity_id varchar(36) NULL ,load_dttm datetime2 NULL ,occurrence_id varchar(36) NULL ,response_tracking_cd varchar(36) NULL ,sender_id varchar(40) NULL ,sms_message_id varchar(40) NULL ,sms_optout_dttm datetime2 NULL ,sms_optout_dttm_tz datetime2 NULL ,task_id varchar(36) NULL ,task_version_id varchar(36) NULL
        )) by SQLSVR;
      execute (alter table sms_optout_details add constraint sms_optout_details_pk primary key (event_id)) by SQLSVR;
 DISCONNECT FROM SQLSVR;
 QUIT;
 %ErrCheck (Failed to create Table: sms_optout_details, sms_optout_details);
