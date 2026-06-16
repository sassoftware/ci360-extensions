/*******************************************************************************/
/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                         */
/* *****************************************************************************/
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..EVENT_ERRORS (
      error_dttm_tz timestamp, error_dttm timestamp, ip_address string, event_source_cd string, 
      event_id string, error_cd string, error_txt string, payload_txt string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EVENT_ERRORS, EVENT_ERRORS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..JOURNEY_TEST_SUCCESS (
      success_dttm_tz timestamp, success_dttm timestamp, parent_event_designed_id string, journey_id string, 
      group_id string, event_nm string, event_id string NOT NULL, context_type_nm string, 
      context_val string, identity_id string, journey_occurrence_id string    )) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..JOURNEY_TEST_SUCCESS
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: JOURNEY_TEST_SUCCESS, JOURNEY_TEST_SUCCESS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_ACTIVITY_ABTESTPATH_ALL (
      next_node_val STRING, abtest_dist_pct string, control_flg string, valid_to_dttm timestamp, 
      valid_from_dttm timestamp, activity_version_id string, activity_status_cd string, activity_node_id string, 
      activity_id string, abtest_path_nm string, abtest_path_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_ABTESTPATH_ALL, MD_ACTIVITY_ABTESTPATH_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_ACTIVITY_ALL (
      last_published_dttm timestamp, valid_from_dttm timestamp, valid_to_dttm timestamp, folder_path_nm string, 
      business_context_id string, activity_version_id string, activity_status_cd string, activity_nm string, 
      activity_id string, activity_desc string, activity_cd string, activity_category_nm string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_ALL, MD_ACTIVITY_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_ACTIVITY_CUSTOM_PROP_ALL (
      valid_to_dttm timestamp, valid_from_dttm timestamp, property_val string, property_nm string, 
      property_datatype_cd string, activity_version_id string, activity_status_cd string, activity_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_CUSTOM_PROP_ALL, MD_ACTIVITY_CUSTOM_PROP_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_ACTIVITY_NODE_ALL (
      previous_node_val STRING, next_node_val STRING, wait_tm int64, time_boxed_flg string, 
      end_node_flg string, start_node_flg string, specific_wait_flg string, node_sequence_no int64, 
      valid_to_dttm timestamp, valid_from_dttm timestamp, activity_version_id string, activity_status_cd string, 
      activity_node_type_nm string, activity_node_nm string, activity_node_id string, activity_id string, 
      abtest_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_NODE_ALL, MD_ACTIVITY_NODE_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_ACTIVITY_X_ACTIVITY_NODE_ALL (
      activity_version_id string, activity_status_cd string, activity_node_id string, activity_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_X_ACTIVITY_NODE_ALL, MD_ACTIVITY_X_ACTIVITY_NODE_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_ACTIVITY_X_TASK_ALL (
      task_version_id string, task_id string, activity_version_id string, activity_status_cd string, 
      activity_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_X_TASK_ALL, MD_ACTIVITY_X_TASK_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_ASSET_ALL (
      valid_to_dttm timestamp, valid_from_dttm timestamp, last_published_dttm timestamp, owner_nm string, 
      created_user_nm string, asset_version_id string, asset_type_nm string, asset_status_cd string, 
      asset_nm string, asset_id string, asset_desc string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ASSET_ALL, MD_ASSET_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_BUSINESS_CONTEXT_ALL (
      valid_to_dttm timestamp, last_published_dttm timestamp, valid_from_dttm timestamp, owner_nm string, 
      locked_information_map_nm string, information_map_nm string, created_user_nm string, business_context_version_id string, 
      business_context_status_cd string, business_context_src_cd string, business_context_nm string, business_context_id string, 
      business_context_desc string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_BUSINESS_CONTEXT_ALL, MD_BUSINESS_CONTEXT_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_CREATIVE_ALL (
      valid_to_dttm timestamp, valid_from_dttm timestamp, last_published_dttm timestamp, recommender_template_nm string, 
      recommender_template_id string, owner_nm string, folder_path_nm string, creative_version_id string, 
      creative_type_nm string, creative_txt string, creative_status_cd string, creative_nm string, 
      creative_id string, creative_desc string, creative_cd string, creative_category_nm string, 
      created_user_nm string, business_context_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_CREATIVE_ALL, MD_CREATIVE_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_CREATIVE_CUSTOM_PROP_ALL (
      valid_from_dttm timestamp, valid_to_dttm timestamp, property_val string, property_nm string, 
      property_datatype_cd string, creative_version_id string, creative_status_cd string, creative_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_CREATIVE_CUSTOM_PROP_ALL, MD_CREATIVE_CUSTOM_PROP_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_CREATIVE_X_ASSET_ALL (
      creative_version_id string, creative_status_cd string, creative_id string, asset_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_CREATIVE_X_ASSET_ALL, MD_CREATIVE_X_ASSET_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_DATAVIEW_ALL (
      include_internal_flg string, analytic_active_flg string, include_external_flg string, max_path_length_val int64, 
      half_life_time_val int64, analytics_period_val int64, max_path_time_val int64, valid_to_dttm timestamp, 
      valid_from_dttm timestamp, last_published_dttm timestamp, selected_task_list string, owner_nm string, 
      max_path_time_type_nm string, dataview_version_id string, dataview_status_cd string, dataview_nm string, 
      dataview_id string, dataview_desc string, custom_recent_exclude_cd string, custom_recent_cd string, 
      created_user_nm string, analytics_period_type_nm string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_DATAVIEW_ALL, MD_DATAVIEW_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_DATAVIEW_X_EVENT_ALL (
      event_id string, dataview_version_id string, dataview_status_cd string, dataview_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_DATAVIEW_X_EVENT_ALL, MD_DATAVIEW_X_EVENT_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_EVENT_ALL (
      last_published_dttm timestamp, valid_to_dttm timestamp, valid_from_dttm timestamp, owner_nm string, 
      event_version_id string, event_type_nm string, event_subtype_nm string, event_status_cd string, 
      event_nm string, event_id string, event_desc string, created_user_nm string, 
      channel_nm string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_EVENT_ALL, MD_EVENT_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_JOURNEY_ALL (
      control_group_flg string, target_goal_qty int64, last_activated_dttm timestamp, test_type_nm string, 
      target_goal_type_nm string, purpose_id string, journey_version_id string, journey_status_cd string, 
      journey_nm string, journey_id string, created_user_nm string, activated_user_nm string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_ALL, MD_JOURNEY_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_JOURNEY_NODE_X_NEXT_NODE (
      next_node_id string, journey_node_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_NODE_X_NEXT_NODE, MD_JOURNEY_NODE_X_NEXT_NODE);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_JOURNEY_NODE_X_PREVIOUS_NODE (
      previous_node_id string, journey_node_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_NODE_X_PREVIOUS_NODE, MD_JOURNEY_NODE_X_PREVIOUS_NODE);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_JOURNEY_NODE_X_VARIANT (
      control_flg string, analysis_period_duration decimal(4,2), variant_dist_pct decimal(3,2), variant_nm string, 
      journey_node_id string, analysis_group_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_NODE_X_VARIANT, MD_JOURNEY_NODE_X_VARIANT);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_MESSAGE_ALL (
      valid_from_dttm timestamp, last_published_dttm timestamp, valid_to_dttm timestamp, owner_nm string, 
      message_version_id string, message_type_nm string, message_nm string, message_desc string, 
      message_category_nm string, folder_path_nm string, message_cd string, message_id string, 
      message_status_cd string, created_user_nm string, business_context_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_MESSAGE_ALL, MD_MESSAGE_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_MESSAGE_CUSTOM_PROP_ALL (
      valid_to_dttm timestamp, valid_from_dttm timestamp, property_val string, property_datatype_cd string, 
      message_status_cd string, message_id string, message_version_id string, property_nm string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_MESSAGE_CUSTOM_PROP_ALL, MD_MESSAGE_CUSTOM_PROP_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_MESSAGE_X_CREATIVE_ALL (
      message_version_id string, message_id string, creative_id string, message_status_cd string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_MESSAGE_X_CREATIVE_ALL, MD_MESSAGE_X_CREATIVE_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_ALL (
      last_published_dttm timestamp, valid_from_dttm timestamp, valid_to_dttm timestamp, segment_version_id string, 
      segment_status_cd string, segment_nm string, segment_id string, segment_cd string, 
      segment_category_nm string, owner_nm string, folder_path_nm string, business_context_id string, 
      created_user_nm string, segment_desc string, segment_map_id string, segment_src_cd string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_ALL, MD_SEGMENT_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_CUSTOM_PROP_ALL (
      valid_from_dttm timestamp, valid_to_dttm timestamp, segment_version_id string, segment_status_cd string, 
      property_val string, property_datatype_cd string, property_nm string, segment_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_CUSTOM_PROP_ALL, MD_SEGMENT_CUSTOM_PROP_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_MAP_ALL (
      scheduled_flg string, recurrence_day_of_month_no int64, rec_scheduled_start_dttm timestamp, valid_from_dttm timestamp, 
      rec_scheduled_end_dttm timestamp, scheduled_start_dttm timestamp, last_published_dttm timestamp, valid_to_dttm timestamp, 
      scheduled_end_dttm timestamp, segment_map_status_cd string, segment_map_nm string, segment_map_desc string, 
      segment_map_category_nm string, recurrence_monthly_type_nm string, recurrence_days_of_week_txt string, recurrence_day_of_wk_ordinal_no string, 
      recurrence_day_of_week_txt string, rec_scheduled_start_tm string, owner_nm string, folder_path_nm string, 
      created_user_nm string, business_context_id string, recurrence_frequency_cd string, segment_map_cd string, 
      segment_map_id string, segment_map_src_cd string, segment_map_version_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_MAP_ALL, MD_SEGMENT_MAP_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_MAP_CUSTOM_PROP_ALL (
      valid_from_dttm timestamp, valid_to_dttm timestamp, segment_map_status_cd string, property_val string, 
      property_nm string, property_datatype_cd string, segment_map_id string, segment_map_version_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_MAP_CUSTOM_PROP_ALL, MD_SEGMENT_MAP_CUSTOM_PROP_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_MAP_X_SEGMENT_ALL (
      segment_map_version_id string, segment_map_status_cd string, segment_map_id string, segment_id string, 
      segment_version_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_MAP_X_SEGMENT_ALL, MD_SEGMENT_MAP_X_SEGMENT_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_TEST_ALL (
      test_enabled_flg string, stratified_sampling_flg string, test_pct decimal(5,2), test_cnt int64, 
      test_type_nm string, test_sizing_type_nm string, test_nm string, task_version_id string, 
      task_id string, stratified_samp_criteria_txt string, test_cd string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_TEST_ALL, MD_SEGMENT_TEST_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_TEST_X_SEGMENT_ALL (
      test_cd string, task_version_id string, segment_id string, task_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_TEST_X_SEGMENT_ALL, MD_SEGMENT_TEST_X_SEGMENT_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_X_EVENT_ALL (
      segment_version_id string, segment_status_cd string, event_id string, segment_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_X_EVENT_ALL, MD_SEGMENT_X_EVENT_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_SPOT_ALL (
      multi_page_flg string, location_selector_flg string, valid_from_dttm timestamp, valid_to_dttm timestamp, 
      last_published_dttm timestamp, spot_version_id string, spot_status_cd string, spot_nm string, 
      spot_key string, spot_height_val_no string, owner_nm string, height_width_ratio_val_txt string, 
      created_user_nm string, channel_nm string, dimension_label_txt string, spot_desc string, 
      spot_id string, spot_type_nm string, spot_width_val_no string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SPOT_ALL, MD_SPOT_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_ALL (
      use_modeling_flg string, activity_flg string, recurring_schedule_flg string, rtdm_flg string, 
      scheduled_flg string, export_template_flg string, segment_tests_flg string, recurrence_day_of_month_no int64, 
      impressions_per_session_cnt int64, test_duration int64, display_priority_no int64, limit_period_unit_cnt int64, 
      maximum_period_expression_cnt int64, impressions_qty_period_cnt int64, impressions_life_time_cnt int64, last_published_dttm timestamp, 
      model_start_dttm timestamp, scheduled_start_dttm timestamp, rec_scheduled_end_dttm timestamp, rec_scheduled_start_dttm timestamp, 
      scheduled_end_dttm timestamp, valid_from_dttm timestamp, valid_to_dttm timestamp, template_id string, 
      task_version_id string, task_subtype_nm string, task_nm string, task_desc string, 
      task_cd string, subject_line_txt string, stratified_sampling_action_nm string, send_notification_locale_cd string, 
      secondary_status string, recurrence_frequency_cd string, recurrence_day_of_wk_ordinal_no string, recurrence_day_of_week_txt string, 
      rec_scheduled_start_tm string, period_type_nm string, owner_nm string, mobile_app_id string, 
      folder_path_nm string, delivery_config_type_nm string, control_group_action_nm string, channel_nm string, 
      business_context_id string, arbitration_method_cd string, created_user_nm string, mobile_app_nm string, 
      recurrence_days_of_week_txt string, recurrence_monthly_type_nm string, subject_line_source_nm string, task_category_nm string, 
      task_delivery_type_nm string, task_id string, task_status_cd string, task_type_nm string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_ALL, MD_TASK_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_CUSTOM_PROP_ALL (
      valid_from_dttm timestamp, valid_to_dttm timestamp, task_version_id string, task_status_cd string, 
      task_id string, property_val string, property_datatype_nm string, property_nm string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_CUSTOM_PROP_ALL, MD_TASK_CUSTOM_PROP_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_CREATIVE_ALL (
      variant_nm string, variant_id string, task_status_cd string, spot_id string, 
      arbitration_method_val string, arbitration_method_cd string, creative_id string, task_id string, 
      task_version_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_CREATIVE_ALL, MD_TASK_X_CREATIVE_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_DATAVIEW_ALL (
      targeting_flg string, primary_metric_flg string, secondary_metric_flg string, task_status_cd string, 
      task_id string, dataview_id string, task_version_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_DATAVIEW_ALL, MD_TASK_X_DATAVIEW_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_EVENT_ALL (
      secondary_metric_flg string, targeting_flg string, primary_metric_flg string, task_status_cd string, 
      task_id string, event_id string, task_version_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_EVENT_ALL, MD_TASK_X_EVENT_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_MESSAGE_ALL (
      task_status_cd string, message_id string, task_id string, task_version_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_MESSAGE_ALL, MD_TASK_X_MESSAGE_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_SEGMENT_ALL (
      task_status_cd string, segment_id string, task_id string, task_version_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_SEGMENT_ALL, MD_TASK_X_SEGMENT_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_SPOT_ALL (
      task_version_id string, task_status_cd string, spot_id string, task_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_SPOT_ALL, MD_TASK_X_SPOT_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_VARIANT_ALL (
      variant_type_nm string, variant_nm string, task_status_cd string, analysis_group_id string, 
      task_id string, task_version_id string, variant_source_nm string, variant_val string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_VARIANT_ALL, MD_TASK_X_VARIANT_ALL);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..DAILY_USAGE  ADD dm_destinations_total_id_cnt  int64  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DAILY_USAGE  ADD dm_destinations_total_row_cnt  int64  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..DBT_CONTENT  ADD detail_id  string  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..DBT_DOCUMENTS  ADD detail_id  string  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..DBT_FORMS  ADD detail_id  string  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..DBT_GOALS  ADD detail_id  string  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..DBT_PROMOTIONS  ADD detail_id  string  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..DBT_SEARCH  ADD detail_id  string  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..EMAIL_VIEW  ADD journey_id  string  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..EMAIL_VIEW  ADD journey_occurrence_id  string  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..JOURNEY_EXIT  ADD group_id  string  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..JOURNEY_NODE_ENTRY  ADD group_id  string  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..JOURNEY_SUCCESS  ADD group_id  string  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..JOURNEY_SUCCESS  ADD parent_event_designed_id  string  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..MD_AUDIENCE  ADD update_dttm  timestamp  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..MD_AUDIENCE_OCCURRENCE  ADD update_dttm  timestamp  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..MD_JOURNEY  ADD test_type_nm  string  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..MD_JOURNEY_NODE_OCCURRENCE  ADD group_id  string  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..MEDIA_ACTIVITY_DETAILS  ADD event_id  string  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..MONTHLY_USAGE  ADD dm_destinations_total_id_cnt  int64  NULL) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..MONTHLY_USAGE  ADD dm_destinations_total_row_cnt  int64  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: &table_name., &table_name.);

PROC SQL;
CONNECT TO &database. (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..SESSION_DETAILS  ADD eventsource_cd  string  NULL) BY &database.;
DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to alter Table: &table_name., &table_name.);
