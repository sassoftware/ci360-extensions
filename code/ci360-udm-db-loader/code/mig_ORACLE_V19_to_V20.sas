/*******************************************************************************/
/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                         */
/* *****************************************************************************/
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..event_errors (
      error_dttm_tz timestamp NULL, error_dttm timestamp NULL, ip_address varchar(64) NULL, event_source_cd varchar(100) NULL, 
      event_id varchar(36) NULL, error_cd varchar(65) NULL, error_txt varchar(4000) NULL, payload_txt varchar(4000) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EVENT_ERRORS, EVENT_ERRORS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..journey_test_success (
      success_dttm_tz timestamp NULL, success_dttm timestamp NULL, parent_event_designed_id varchar(36) NULL, journey_id varchar(36) NULL, 
      group_id varchar(36) NULL, event_nm varchar(256) NULL, event_id varchar(36) NOT NULL, context_type_nm varchar(256) NULL, 
      context_val varchar(256) NULL, identity_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: JOURNEY_TEST_SUCCESS, JOURNEY_TEST_SUCCESS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_activity_abtestpath_all (
      next_node_val varchar(4000) NULL, abtest_dist_pct char(3) NULL, control_flg char(1) NULL, valid_to_dttm timestamp NULL, 
      valid_from_dttm timestamp NULL, activity_version_id varchar(36) NULL, activity_status_cd varchar(20) NULL, activity_node_id varchar(36) NULL, 
      activity_id varchar(36) NULL, abtest_path_nm varchar(50) NULL, abtest_path_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_ABTESTPATH_ALL, MD_ACTIVITY_ABTESTPATH_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_activity_all (
      valid_to_dttm timestamp NULL, valid_from_dttm timestamp NULL, last_published_dttm timestamp NULL, folder_path_nm varchar(256) NULL, 
      business_context_id varchar(36) NULL, activity_version_id varchar(36) NULL, activity_status_cd varchar(20) NULL, activity_nm varchar(60) NULL, 
      activity_id varchar(36) NULL, activity_desc varchar(1332) NULL, activity_cd varchar(60) NULL, activity_category_nm varchar(100) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_ALL, MD_ACTIVITY_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_activity_custom_prop_all (
      valid_to_dttm timestamp NULL, valid_from_dttm timestamp NULL, property_val varchar(1332) NULL, property_nm varchar(256) NULL, 
      property_datatype_cd varchar(256) NULL, activity_version_id varchar(36) NULL, activity_status_cd varchar(36) NULL, activity_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_CUSTOM_PROP_ALL, MD_ACTIVITY_CUSTOM_PROP_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_activity_node_all (
      next_node_val varchar(4000) NULL, previous_node_val varchar(4000) NULL, wait_tm number NULL, time_boxed_flg char(1) NULL, 
      specific_wait_flg char(1) NULL, end_node_flg char(1) NULL, start_node_flg char(1) NULL, node_sequence_no int NULL, 
      valid_to_dttm timestamp NULL, valid_from_dttm timestamp NULL, activity_version_id varchar(36) NULL, activity_status_cd varchar(20) NULL, 
      activity_node_type_nm varchar(100) NULL, activity_node_nm varchar(256) NULL, activity_node_id varchar(36) NULL, activity_id varchar(36) NULL, 
      abtest_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_NODE_ALL, MD_ACTIVITY_NODE_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_activity_x_activity_node_all (
      activity_version_id varchar(36) NULL, activity_status_cd varchar(20) NULL, activity_node_id varchar(36) NULL, activity_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_X_ACTIVITY_NODE_ALL, MD_ACTIVITY_X_ACTIVITY_NODE_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_activity_x_task_all (
      task_version_id varchar(36) NULL, task_id varchar(36) NULL, activity_version_id varchar(36) NULL, activity_status_cd varchar(20) NULL, 
      activity_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_X_TASK_ALL, MD_ACTIVITY_X_TASK_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_asset_all (
      valid_to_dttm timestamp NULL, valid_from_dttm timestamp NULL, last_published_dttm timestamp NULL, owner_nm varchar(256) NULL, 
      created_user_nm varchar(256) NULL, asset_version_id varchar(36) NULL, asset_type_nm varchar(40) NULL, asset_status_cd varchar(20) NULL, 
      asset_nm varchar(256) NULL, asset_id varchar(36) NULL, asset_desc varchar(1332) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ASSET_ALL, MD_ASSET_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_business_context_all (
      valid_to_dttm timestamp NULL, last_published_dttm timestamp NULL, valid_from_dttm timestamp NULL, owner_nm varchar(256) NULL, 
      locked_information_map_nm varchar(40) NULL, information_map_nm varchar(40) NULL, created_user_nm varchar(256) NULL, business_context_version_id varchar(36) NULL, 
      business_context_status_cd varchar(20) NULL, business_context_src_cd varchar(40) NULL, business_context_nm varchar(256) NULL, business_context_id varchar(36) NULL, 
      business_context_desc varchar(1332) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_BUSINESS_CONTEXT_ALL, MD_BUSINESS_CONTEXT_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_creative_all (
      valid_to_dttm timestamp NULL, valid_from_dttm timestamp NULL, last_published_dttm timestamp NULL, recommender_template_nm varchar(60) NULL, 
      recommender_template_id varchar(36) NULL, owner_nm varchar(256) NULL, folder_path_nm varchar(256) NULL, creative_version_id varchar(36) NULL, 
      creative_type_nm varchar(40) NULL, creative_txt varchar(1500) NULL, creative_status_cd varchar(20) NULL, creative_nm varchar(60) NULL, 
      creative_id varchar(36) NULL, creative_desc varchar(256) NULL, creative_cd varchar(60) NULL, creative_category_nm varchar(100) NULL, 
      created_user_nm varchar(256) NULL, business_context_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_CREATIVE_ALL, MD_CREATIVE_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_creative_custom_prop_all (
      valid_from_dttm timestamp NULL, valid_to_dttm timestamp NULL, property_val varchar(1332) NULL, property_nm varchar(256) NULL, 
      property_datatype_cd varchar(256) NULL, creative_version_id varchar(36) NULL, creative_status_cd varchar(36) NULL, creative_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_CREATIVE_CUSTOM_PROP_ALL, MD_CREATIVE_CUSTOM_PROP_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_creative_x_asset_all (
      creative_version_id varchar(36) NULL, creative_status_cd varchar(20) NULL, creative_id varchar(36) NULL, asset_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_CREATIVE_X_ASSET_ALL, MD_CREATIVE_X_ASSET_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_dataview_all (
      include_internal_flg char(1) NULL, include_external_flg char(1) NULL, analytic_active_flg char(1) NULL, max_path_length_val int NULL, 
      half_life_time_val int NULL, analytics_period_val int NULL, max_path_time_val int NULL, valid_to_dttm timestamp NULL, 
      valid_from_dttm timestamp NULL, last_published_dttm timestamp NULL, selected_task_list varchar(1000) NULL, owner_nm varchar(256) NULL, 
      max_path_time_type_nm varchar(10) NULL, dataview_version_id varchar(36) NULL, dataview_status_cd varchar(20) NULL, dataview_nm varchar(60) NULL, 
      dataview_id varchar(36) NULL, dataview_desc varchar(1332) NULL, custom_recent_exclude_cd varchar(36) NULL, custom_recent_cd varchar(36) NULL, 
      created_user_nm varchar(256) NULL, analytics_period_type_nm varchar(10) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_DATAVIEW_ALL, MD_DATAVIEW_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_dataview_x_event_all (
      event_id varchar(36) NULL, dataview_version_id varchar(36) NULL, dataview_status_cd varchar(20) NULL, dataview_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_DATAVIEW_X_EVENT_ALL, MD_DATAVIEW_X_EVENT_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_event_all (
      last_published_dttm timestamp NULL, valid_to_dttm timestamp NULL, valid_from_dttm timestamp NULL, owner_nm varchar(256) NULL, 
      event_version_id varchar(36) NULL, event_type_nm varchar(40) NULL, event_subtype_nm varchar(100) NULL, event_status_cd varchar(20) NULL, 
      event_nm varchar(60) NULL, event_id varchar(36) NULL, event_desc varchar(1332) NULL, created_user_nm varchar(256) NULL, 
      channel_nm varchar(40) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_EVENT_ALL, MD_EVENT_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_journey_all (
      control_group_flg char(1) NULL, target_goal_qty int NULL, last_activated_dttm timestamp NULL, test_type_nm varchar(40) NULL, 
      target_goal_type_nm varchar(20) NULL, purpose_id varchar(36) NULL, journey_version_id varchar(36) NULL, journey_status_cd varchar(20) NULL, 
      journey_nm varchar(256) NULL, journey_id varchar(36) NULL, created_user_nm varchar(256) NULL, activated_user_nm varchar(256) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_ALL, MD_JOURNEY_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_journey_node_x_next_node (
      next_node_id varchar(36) NULL, journey_node_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_NODE_X_NEXT_NODE, MD_JOURNEY_NODE_X_NEXT_NODE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_journey_node_x_previous_node (
      previous_node_id varchar(36) NULL, journey_node_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_NODE_X_PREVIOUS_NODE, MD_JOURNEY_NODE_X_PREVIOUS_NODE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_journey_node_x_variant (
      control_flg char(1) NULL, analysis_period_duration decimal(4,2) NULL, variant_dist_pct decimal(3,2) NULL, variant_nm varchar(256) NULL, 
      journey_node_id varchar(36) NULL, analysis_group_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_NODE_X_VARIANT, MD_JOURNEY_NODE_X_VARIANT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_message_all (
      valid_from_dttm timestamp NULL, last_published_dttm timestamp NULL, valid_to_dttm timestamp NULL, owner_nm varchar(256) NULL, 
      message_version_id varchar(36) NULL, message_type_nm varchar(40) NULL, message_nm varchar(60) NULL, message_desc varchar(1332) NULL, 
      message_category_nm varchar(100) NULL, folder_path_nm varchar(256) NULL, message_cd varchar(60) NULL, message_id varchar(36) NULL, 
      message_status_cd varchar(20) NULL, created_user_nm varchar(256) NULL, business_context_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_MESSAGE_ALL, MD_MESSAGE_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_message_custom_prop_all (
      valid_to_dttm timestamp NULL, valid_from_dttm timestamp NULL, property_val varchar(1332) NULL, property_datatype_cd varchar(256) NULL, 
      message_status_cd varchar(36) NULL, message_id varchar(36) NULL, message_version_id varchar(36) NULL, property_nm varchar(256) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_MESSAGE_CUSTOM_PROP_ALL, MD_MESSAGE_CUSTOM_PROP_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_message_x_creative_all (
      message_version_id varchar(36) NULL, message_id varchar(36) NULL, creative_id varchar(36) NULL, message_status_cd varchar(20) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_MESSAGE_X_CREATIVE_ALL, MD_MESSAGE_X_CREATIVE_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_segment_all (
      last_published_dttm timestamp NULL, valid_from_dttm timestamp NULL, valid_to_dttm timestamp NULL, segment_version_id varchar(36) NULL, 
      segment_status_cd varchar(20) NULL, segment_nm varchar(60) NULL, segment_id varchar(36) NULL, segment_cd varchar(60) NULL, 
      segment_category_nm varchar(100) NULL, owner_nm varchar(256) NULL, folder_path_nm varchar(256) NULL, business_context_id varchar(36) NULL, 
      created_user_nm varchar(256) NULL, segment_desc varchar(1332) NULL, segment_map_id varchar(36) NULL, segment_src_cd varchar(40) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_ALL, MD_SEGMENT_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_segment_custom_prop_all (
      valid_from_dttm timestamp NULL, valid_to_dttm timestamp NULL, segment_version_id varchar(36) NULL, segment_status_cd varchar(36) NULL, 
      property_val varchar(1332) NULL, property_datatype_cd varchar(256) NULL, property_nm varchar(256) NULL, segment_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_CUSTOM_PROP_ALL, MD_SEGMENT_CUSTOM_PROP_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_segment_map_all (
      scheduled_flg char(1) NULL, recurrence_day_of_month_no int NULL, rec_scheduled_start_dttm timestamp NULL, valid_from_dttm timestamp NULL, 
      rec_scheduled_end_dttm timestamp NULL, scheduled_start_dttm timestamp NULL, last_published_dttm timestamp NULL, valid_to_dttm timestamp NULL, 
      scheduled_end_dttm timestamp NULL, segment_map_status_cd varchar(20) NULL, segment_map_nm varchar(60) NULL, segment_map_desc varchar(1332) NULL, 
      segment_map_category_nm varchar(100) NULL, recurrence_monthly_type_nm varchar(36) NULL, recurrence_days_of_week_txt varchar(100) NULL, recurrence_day_of_wk_ordinal_no varchar(36) NULL, 
      recurrence_day_of_week_txt varchar(100) NULL, rec_scheduled_start_tm varchar(20) NULL, owner_nm varchar(256) NULL, folder_path_nm varchar(256) NULL, 
      created_user_nm varchar(256) NULL, business_context_id varchar(36) NULL, recurrence_frequency_cd varchar(36) NULL, segment_map_cd varchar(60) NULL, 
      segment_map_id varchar(36) NULL, segment_map_src_cd varchar(10) NULL, segment_map_version_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_MAP_ALL, MD_SEGMENT_MAP_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_segment_map_custom_prop_all (
      valid_from_dttm timestamp NULL, valid_to_dttm timestamp NULL, segment_map_status_cd varchar(36) NULL, property_val varchar(1332) NULL, 
      property_nm varchar(256) NULL, property_datatype_cd varchar(256) NULL, segment_map_id varchar(36) NULL, segment_map_version_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_MAP_CUSTOM_PROP_ALL, MD_SEGMENT_MAP_CUSTOM_PROP_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_segment_map_x_segment_all (
      segment_map_version_id varchar(36) NULL, segment_map_status_cd varchar(20) NULL, segment_map_id varchar(36) NULL, segment_id varchar(36) NULL, 
      segment_version_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_MAP_X_SEGMENT_ALL, MD_SEGMENT_MAP_X_SEGMENT_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_segment_test_all (
      test_enabled_flg char(1) NULL, stratified_sampling_flg char(1) NULL, test_pct decimal(5,2) NULL, test_cnt int NULL, 
      test_type_nm varchar(10) NULL, test_sizing_type_nm varchar(65) NULL, test_nm varchar(65) NULL, task_version_id varchar(36) NULL, 
      task_id varchar(36) NULL, stratified_samp_criteria_txt varchar(1024) NULL, test_cd varchar(60) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_TEST_ALL, MD_SEGMENT_TEST_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_segment_test_x_segment_all (
      test_cd varchar(60) NULL, task_version_id varchar(36) NULL, segment_id varchar(36) NULL, task_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_TEST_X_SEGMENT_ALL, MD_SEGMENT_TEST_X_SEGMENT_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_segment_x_event_all (
      segment_version_id varchar(36) NULL, segment_status_cd varchar(20) NULL, event_id varchar(36) NULL, segment_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_X_EVENT_ALL, MD_SEGMENT_X_EVENT_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_spot_all (
      multi_page_flg char(1) NULL, location_selector_flg char(1) NULL, valid_from_dttm timestamp NULL, valid_to_dttm timestamp NULL, 
      last_published_dttm timestamp NULL, spot_version_id varchar(36) NULL, spot_status_cd varchar(20) NULL, spot_nm varchar(60) NULL, 
      spot_key varchar(40) NULL, spot_height_val_no varchar(10) NULL, owner_nm varchar(256) NULL, height_width_ratio_val_txt varchar(25) NULL, 
      created_user_nm varchar(256) NULL, channel_nm varchar(40) NULL, dimension_label_txt varchar(156) NULL, spot_desc varchar(1332) NULL, 
      spot_id varchar(36) NULL, spot_type_nm varchar(40) NULL, spot_width_val_no varchar(10) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SPOT_ALL, MD_SPOT_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_task_all (
      scheduled_flg char(1) NULL, rtdm_flg char(1) NULL, segment_tests_flg char(1) NULL, export_template_flg char(1) NULL, 
      use_modeling_flg char(1) NULL, activity_flg char(1) NULL, recurring_schedule_flg char(1) NULL, recurrence_day_of_month_no int NULL, 
      impressions_per_session_cnt int NULL, test_duration int NULL, display_priority_no int NULL, limit_period_unit_cnt int NULL, 
      impressions_qty_period_cnt int NULL, maximum_period_expression_cnt int NULL, impressions_life_time_cnt int NULL, last_published_dttm timestamp NULL, 
      model_start_dttm timestamp NULL, rec_scheduled_end_dttm timestamp NULL, rec_scheduled_start_dttm timestamp NULL, scheduled_end_dttm timestamp NULL, 
      valid_from_dttm timestamp NULL, scheduled_start_dttm timestamp NULL, valid_to_dttm timestamp NULL, template_id varchar(36) NULL, 
      task_version_id varchar(36) NULL, task_subtype_nm varchar(30) NULL, task_nm varchar(60) NULL, task_desc varchar(1332) NULL, 
      task_cd varchar(60) NULL, subject_line_txt varchar(1332) NULL, stratified_sampling_action_nm varchar(65) NULL, send_notification_locale_cd varchar(5) NULL, 
      secondary_status varchar(40) NULL, recurrence_frequency_cd varchar(36) NULL, recurrence_day_of_wk_ordinal_no varchar(36) NULL, recurrence_day_of_week_txt varchar(60) NULL, 
      rec_scheduled_start_tm varchar(20) NULL, period_type_nm varchar(36) NULL, owner_nm varchar(256) NULL, mobile_app_id varchar(60) NULL, 
      folder_path_nm varchar(256) NULL, delivery_config_type_nm varchar(36) NULL, control_group_action_nm varchar(65) NULL, channel_nm varchar(40) NULL, 
      business_context_id varchar(36) NULL, arbitration_method_cd varchar(36) NULL, created_user_nm varchar(256) NULL, mobile_app_nm varchar(60) NULL, 
      recurrence_days_of_week_txt varchar(60) NULL, recurrence_monthly_type_nm varchar(36) NULL, subject_line_source_nm varchar(100) NULL, task_category_nm varchar(100) NULL, 
      task_delivery_type_nm varchar(60) NULL, task_id varchar(36) NULL, task_status_cd varchar(20) NULL, task_type_nm varchar(40) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_ALL, MD_TASK_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_task_custom_prop_all (
      valid_from_dttm timestamp NULL, valid_to_dttm timestamp NULL, task_version_id varchar(36) NULL, task_status_cd varchar(36) NULL, 
      task_id varchar(36) NULL, property_val varchar(1332) NULL, property_datatype_nm varchar(256) NULL, property_nm varchar(256) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_CUSTOM_PROP_ALL, MD_TASK_CUSTOM_PROP_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_task_x_creative_all (
      variant_nm varchar(256) NULL, variant_id varchar(36) NULL, task_status_cd varchar(20) NULL, spot_id varchar(36) NULL, 
      arbitration_method_val varchar(3) NULL, arbitration_method_cd varchar(36) NULL, creative_id varchar(36) NULL, task_id varchar(36) NULL, 
      task_version_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_CREATIVE_ALL, MD_TASK_X_CREATIVE_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_task_x_dataview_all (
      targeting_flg char(1) NULL, primary_metric_flg char(1) NULL, secondary_metric_flg char(1) NULL, task_status_cd varchar(20) NULL, 
      task_id varchar(36) NULL, dataview_id varchar(36) NULL, task_version_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_DATAVIEW_ALL, MD_TASK_X_DATAVIEW_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_task_x_event_all (
      targeting_flg char(1) NULL, secondary_metric_flg char(1) NULL, primary_metric_flg char(1) NULL, task_status_cd varchar(20) NULL, 
      task_id varchar(36) NULL, event_id varchar(36) NULL, task_version_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_EVENT_ALL, MD_TASK_X_EVENT_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_task_x_message_all (
      task_status_cd varchar(20) NULL, message_id varchar(36) NULL, task_id varchar(36) NULL, task_version_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_MESSAGE_ALL, MD_TASK_X_MESSAGE_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_task_x_segment_all (
      task_status_cd varchar(20) NULL, segment_id varchar(36) NULL, task_id varchar(36) NULL, task_version_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_SEGMENT_ALL, MD_TASK_X_SEGMENT_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_task_x_spot_all (
      task_version_id varchar(36) NULL, task_status_cd varchar(20) NULL, spot_id varchar(36) NULL, task_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_SPOT_ALL, MD_TASK_X_SPOT_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_task_x_variant_all (
      variant_type_nm varchar(100) NULL, variant_nm varchar(256) NULL, task_status_cd varchar(20) NULL, analysis_group_id varchar(36) NULL, 
      task_id varchar(36) NULL, task_version_id varchar(36) NULL, variant_source_nm varchar(100) NULL, variant_val varchar(1332) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_VARIANT_ALL, MD_TASK_X_VARIANT_ALL);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..daily_usage  ADD dm_destinations_total_id_cnt  number  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..daily_usage  ADD dm_destinations_total_row_cnt  number  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..dbt_content  ADD detail_id  varchar(32)  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..dbt_documents  ADD detail_id  varchar(32)  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..dbt_forms  ADD detail_id  varchar(32)  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..dbt_goals  ADD detail_id  varchar(32)  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..dbt_promotions  ADD detail_id  varchar(32)  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..dbt_search  ADD detail_id  varchar(32)  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..email_view  ADD journey_id  varchar(36)  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..email_view  ADD journey_occurrence_id  varchar(36)  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..journey_exit  ADD group_id  varchar(36)  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..journey_node_entry  ADD group_id  varchar(36)  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..journey_success  ADD group_id  varchar(36)  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..journey_success  ADD parent_event_designed_id  varchar(36)  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..md_audience  ADD update_dttm  timestamp  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..md_audience_occurrence  ADD update_dttm  timestamp  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..md_journey  ADD test_type_nm  varchar(40)  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..md_journey_node_occurrence  ADD group_id  varchar(36)  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..media_activity_details  ADD event_id  varchar(36)  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..monthly_usage  ADD dm_destinations_total_id_cnt  number  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..monthly_usage  ADD dm_destinations_total_row_cnt  number  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..session_details  ADD eventsource_cd  varchar(36)  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: &table_name., &table_name.);
