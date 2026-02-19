/*******************************************************************************/
/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                         */
/* *****************************************************************************/
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..abt_attribution (
      interaction_cost integer NULL, conversion_value integer NULL, interaction_dttm timestamp NOT NULL, task_id varchar(36) NULL, 
      load_id varchar(36) NULL, interaction_type varchar(15) NULL, interaction_subtype varchar(100) NULL, interaction_id varchar(36) NOT NULL, 
      interaction varchar(260) NOT NULL, identity_id varchar(36) NULL, creative_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ABT_ATTRIBUTION, ABT_ATTRIBUTION);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..ab_test_path_assignment (
      load_dttm timestamp NOT NULL, abtestpath_assignment_dttm_tz timestamp NULL, abtestpath_assignment_dttm timestamp NULL, session_id_hex varchar(29) NULL, 
      context_type_nm varchar(256) NULL, channel_user_id varchar(300) NULL, identity_id varchar(36) NULL, event_nm varchar(256) NULL, 
      channel_nm varchar(40) NULL, event_id varchar(36) NOT NULL, event_designed_id varchar(36) NULL, abtest_path_id varchar(36) NULL, 
      activity_id varchar(36) NULL, context_val varchar(256) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: AB_TEST_PATH_ASSIGNMENT, AB_TEST_PATH_ASSIGNMENT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..activity_conversion (
      activity_conversion_dttm_tz timestamp NULL, load_dttm timestamp NOT NULL, activity_conversion_dttm timestamp NULL, abtest_path_id varchar(36) NULL, 
      activity_id varchar(36) NULL, activity_node_id varchar(36) NULL, session_id_hex varchar(29) NULL, parent_event_designed_id varchar(36) NULL, 
      identity_id varchar(36) NULL, goal_id varchar(36) NULL, event_nm varchar(256) NULL, event_id varchar(36) NOT NULL, 
      event_designed_id varchar(36) NULL, detail_id_hex varchar(32) NULL, context_val varchar(256) NULL, channel_nm varchar(40) NULL, 
      context_type_nm varchar(256) NULL, channel_user_id varchar(300) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ACTIVITY_CONVERSION, ACTIVITY_CONVERSION);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..activity_flow_in (
      activity_flow_in_dttm timestamp NULL, activity_flow_in_dttm_tz timestamp NULL, load_dttm timestamp NOT NULL, task_id varchar(36) NULL, 
      identity_id varchar(36) NULL, context_val varchar(256) NULL, event_designed_id varchar(36) NULL, event_id varchar(36) NOT NULL, 
      channel_user_id varchar(300) NULL, activity_node_id varchar(36) NULL, activity_id varchar(36) NULL, abtest_path_id varchar(36) NULL, 
      channel_nm varchar(40) NULL, context_type_nm varchar(256) NULL, event_nm varchar(256) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ACTIVITY_FLOW_IN, ACTIVITY_FLOW_IN);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..activity_start (
      activity_start_dttm_tz timestamp NULL, load_dttm timestamp NOT NULL, activity_start_dttm timestamp NULL, channel_nm varchar(40) NULL, 
      activity_id varchar(36) NULL, identity_id varchar(36) NULL, event_nm varchar(256) NULL, event_id varchar(36) NOT NULL, 
      channel_user_id varchar(300) NULL, event_designed_id varchar(36) NULL, context_val varchar(256) NULL, context_type_nm varchar(256) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ACTIVITY_START, ACTIVITY_START);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..advertising_contact (
      load_dttm timestamp NOT NULL, advertising_contact_dttm_tz timestamp NULL, advertising_contact_dttm timestamp NULL, task_version_id varchar(36) NULL, 
      task_id varchar(36) NULL, task_action_nm varchar(40) NULL, segment_version_id varchar(36) NULL, segment_id varchar(36) NULL, 
      response_tracking_cd varchar(36) NULL, occurrence_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL, journey_id varchar(36) NULL, 
      identity_id varchar(36) NULL, event_nm varchar(256) NULL, event_id varchar(36) NOT NULL, event_designed_id varchar(36) NULL, 
      context_val varchar(256) NULL, context_type_nm varchar(256) NULL, channel_nm varchar(40) NULL, audience_id varchar(36) NULL, 
      aud_occurrence_id varchar(36) NULL, advertising_platform_nm varchar(100) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ADVERTISING_CONTACT, ADVERTISING_CONTACT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..asset_details (
      folder_sk bigint NULL, asset_sk bigint NULL, public_media_id bigint NULL, user_rating_cnt bigint NULL, 
      total_user_rating_val bigint NULL, entity_revision_enabled_flg char(1) NULL, folder_deleted_flg char(1) NULL, entity_attribute_enabled_flg char(1) NULL, 
      expired_flg char(1) NULL, asset_locked_flg char(1) NULL, download_disabled_flg char(1) NULL, entity_subtype_enabled_flg char(1) NULL, 
      asset_deleted_flg char(1) NULL, external_sharing_error_dt date NULL, average_user_rating_val decimal(4,2) NULL, folder_level integer NULL, 
      last_modified_dttm timestamp NULL, created_dttm timestamp NULL, download_disabled_dttm timestamp NULL, recycled_dttm timestamp NULL, 
      expired_dttm timestamp NULL, load_dttm timestamp NULL, asset_locked_dttm timestamp NULL, folder_desc varchar(1332) NULL, 
      external_sharing_error_msg varchar(1024) NULL, entity_table_nm varchar(128) NULL, download_disabled_by_usernm varchar(128) NULL, created_by_usernm varchar(128) NULL, 
      asset_source_type varchar(128) NULL, entity_subtype_nm varchar(128) NULL, entity_type_usage_cd varchar(3) NULL, folder_entity_status_cd varchar(3) NULL, 
      folder_id varchar(128) NULL, asset_owner_usernm varchar(128) NULL, asset_nm varchar(128) NULL, asset_locked_by_usernm varchar(128) NULL, 
      asset_id varchar(128) NULL, asset_desc varchar(1332) NULL, asset_process_status varchar(36) NULL, asset_source_nm varchar(128) NULL, 
      entity_status_cd varchar(3) NULL, recycled_by_usernm varchar(128) NULL, entity_type_nm varchar(128) NULL, public_url varchar(1024) NULL, 
      public_link varchar(1) NULL, process_task_id varchar(128) NULL, process_id varchar(128) NULL, last_modified_by_usernm varchar(128) NULL, 
      folder_path varchar(1024) NULL, folder_owner_usernm varchar(128) NULL, folder_nm varchar(128) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ASSET_DETAILS, ASSET_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..asset_details_custom_prop (
      attr_val varchar(4000) NULL, is_obsolete_flg char(1) NULL, is_grid_flg char(1) NULL, load_dttm timestamp NULL, 
      last_modified_dttm timestamp NULL, created_dttm timestamp NULL, remote_pklist_tab_col varchar(128) NULL, last_modified_usernm varchar(128) NULL, 
      data_type varchar(32) NULL, data_formatter varchar(64) NULL, created_by_usernm varchar(128) NULL, attr_nm varchar(128) NULL, 
      attr_id varchar(128) NULL, attr_group_nm varchar(128) NULL, attr_group_id varchar(128) NULL, attr_group_cd varchar(128) NULL, 
      attr_cd varchar(128) NULL, asset_id varchar(128) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ASSET_DETAILS_CUSTOM_PROP, ASSET_DETAILS_CUSTOM_PROP);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..asset_folder_details (
      deleted_flg char(1) NULL, folder_level integer NULL, load_dttm timestamp NULL, created_dttm timestamp NULL, 
      last_modified_dttm timestamp NULL, last_modified_by_usernm varchar(128) NULL, folder_owner_usernm varchar(128) NULL, folder_desc varchar(1332) NULL, 
      folder_id varchar(128) NULL, entity_status_cd varchar(3) NULL, folder_nm varchar(128) NULL, folder_path varchar(1024) NULL, 
      created_by_usernm varchar(128) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ASSET_FOLDER_DETAILS, ASSET_FOLDER_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..asset_rendition_details (
      download_cnt bigint NULL, revision_no bigint NULL, rend_deleted_flg char(1) NULL, rev_deleted_flg char(1) NULL, 
      current_revision_flg char(1) NULL, media_dpi decimal(10,2) NULL, file_size integer NULL, media_depth integer NULL, 
      media_height integer NULL, rend_duration integer NULL, media_width integer NULL, created_dttm timestamp NULL, 
      last_modified_dttm timestamp NULL, load_dttm timestamp NULL, revision_id varchar(128) NULL, revision_comment_txt varchar(512) NULL, 
      rendition_nm varchar(128) NULL, rendition_generated_type_cd varchar(3) NULL, last_modified_status_cd varchar(3) NULL, last_modified_by_usernm varchar(128) NULL, 
      file_nm varchar(128) NULL, file_format varchar(128) NULL, entity_status_cd varchar(3) NULL, created_by_usernm varchar(128) NULL, 
      asset_id varchar(128) NULL, rendition_id varchar(128) NULL, rendition_type_cd varchar(3) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ASSET_RENDITION_DETAILS, ASSET_RENDITION_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..asset_revision (
      revision_no bigint NULL, deleted_flg char(1) NULL, current_revision_flg char(1) NULL, load_dttm timestamp NULL, 
      created_dttm timestamp NULL, last_modified_dttm timestamp NULL, revision_id varchar(128) NULL, last_modified_by_usernm varchar(128) NULL, 
      revision_comment_txt varchar(512) NULL, entity_status_cd varchar(3) NULL, created_by_usernm varchar(128) NULL, asset_id varchar(128) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ASSET_REVISION, ASSET_REVISION);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..audience_membership_change (
      audience_change_dttm timestamp NULL, load_dttm timestamp NOT NULL, audience_change_dttm_tz timestamp NULL, identity_id varchar(36) NULL, 
      aud_occurrence_id varchar(36) NULL, event_id varchar(36) NOT NULL, audience_id varchar(36) NULL, event_nm varchar(256) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: AUDIENCE_MEMBERSHIP_CHANGE, AUDIENCE_MEMBERSHIP_CHANGE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..business_process_details (
      is_start_flg char(1) NULL, is_completion_flg char(1) NULL, process_attempt_cnt integer NULL, step_order_no integer NULL, 
      process_instance_no integer NULL, process_dttm_tz timestamp NULL, process_exception_dttm_tz timestamp NULL, load_dttm timestamp NOT NULL, 
      process_dttm timestamp NULL, process_exception_dttm timestamp NULL, visit_id varchar(32) NULL, process_step_nm varchar(130) NULL, 
      process_details_sk varchar(32) NULL, identity_id varchar(36) NULL, event_nm varchar(256) NULL, detail_id varchar(32) NULL, 
      attribute1_txt varchar(130) NULL, detail_id_hex varchar(32) NULL, event_designed_id varchar(36) NULL, event_id varchar(36) NOT NULL, 
      next_detail_id varchar(32) NULL, process_exception_txt varchar(1300) NULL, session_id varchar(29) NULL, session_id_hex varchar(29) NULL, 
      visit_id_hex varchar(32) NULL, attribute2_txt varchar(130) NULL, event_source_cd varchar(100) NULL, process_nm varchar(130) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: BUSINESS_PROCESS_DETAILS, BUSINESS_PROCESS_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cart_activity_details (
      unit_price_amt decimal(17,2) NULL, displayed_cart_amt decimal(17,2) NULL, quantity_val integer NULL, displayed_cart_items_no integer NULL, 
      properties_map_doc varchar(4000) NULL, activity_dttm timestamp NULL, load_dttm timestamp NOT NULL, activity_dttm_tz timestamp NULL, 
      cart_activity_sk varchar(32) NULL, activity_cd varchar(20) NULL, visit_id_hex varchar(32) NULL, visit_id varchar(32) NULL, 
      shipping_message_txt varchar(650) NULL, session_id_hex varchar(29) NULL, session_id varchar(29) NULL, saving_message_txt varchar(650) NULL, 
      product_sku varchar(100) NULL, product_nm varchar(130) NULL, product_id varchar(130) NULL, product_group_nm varchar(130) NULL, 
      mobile_app_id varchar(40) NULL, identity_id varchar(36) NULL, event_source_cd varchar(100) NULL, event_nm varchar(256) NULL, 
      availability_message_txt varchar(650) NULL, cart_id varchar(42) NULL, event_id varchar(36) NOT NULL, event_designed_id varchar(36) NULL, 
      detail_id_hex varchar(32) NULL, detail_id varchar(32) NULL, currency_cd varchar(6) NULL, event_key_cd varchar(100) NULL, 
      channel_nm varchar(40) NULL, cart_nm varchar(100) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CART_ACTIVITY_DETAILS, CART_ACTIVITY_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cc_budget_breakup (
      cc_obsolete_flg char(1) NULL, fin_accnt_obsolete_flg char(1) NULL, cc_budget_distribution decimal(17,2) NULL, load_dttm timestamp NULL, 
      last_modified_dttm timestamp NULL, created_dttm timestamp NULL, planning_nm varchar(128) NULL, planning_id varchar(128) NULL, 
      last_modified_usernm varchar(128) NULL, gen_ledger_cd varchar(128) NULL, fin_accnt_nm varchar(128) NULL, fin_accnt_desc varchar(1332) NULL, 
      created_by_usernm varchar(128) NULL, cost_center_id varchar(128) NULL, cc_owner_usernm varchar(128) NULL, cc_nm varchar(128) NULL, 
      cc_desc varchar(1332) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CC_BUDGET_BREAKUP, CC_BUDGET_BREAKUP);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cc_budget_breakup_ccbdgt (
      fin_accnt_obsolete_flg char(1) NULL, cc_obsolete_flg char(1) NULL, fp_obsolete_flg char(1) NULL, fp_end_dt date NULL, 
      fp_start_dt date NULL, cc_bdgt_invoiced_amt decimal(17,2) NULL, cc_lvl_distribution decimal(17,2) NULL, cc_bdgt_cmtmnt_invoice_amt decimal(17,2) NULL, 
      cc_rldup_total_expense decimal(17,2) NULL, cc_rldup_child_bdgt decimal(17,2) NULL, cc_level_expense decimal(17,2) NULL, cc_bdgt_cmtmnt_outstanding_amt decimal(17,2) NULL, 
      cc_bdgt_cmtmnt_overspent_amt decimal(17,2) NULL, cc_bdgt_amt decimal(17,2) NULL, cc_bdgt_budget_amt decimal(17,2) NULL, cc_budget_distribution decimal(17,2) NULL, 
      cc_bdgt_committed_amt decimal(17,2) NULL, cc_bdgt_direct_invoice_amt decimal(17,2) NULL, cc_bdgt_cmtmnt_invoice_cnt integer NULL, last_modified_dttm timestamp NULL, 
      load_dttm timestamp NULL, created_dttm timestamp NULL, planning_id varchar(128) NULL, last_modified_usernm varchar(128) NULL, 
      gen_ledger_cd varchar(128) NULL, fp_nm varchar(128) NULL, fp_id varchar(128) NULL, planning_nm varchar(128) NULL, 
      fp_desc varchar(1332) NULL, fp_cls_ver varchar(128) NULL, fin_accnt_nm varchar(128) NULL, fin_accnt_desc varchar(1332) NULL, 
      created_by_usernm varchar(128) NULL, cost_center_id varchar(128) NULL, cc_owner_usernm varchar(128) NULL, cc_number varchar(128) NULL, 
      cc_nm varchar(128) NULL, cc_desc varchar(1332) NULL, cc_bdgt_budget_desc varchar(1332) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CC_BUDGET_BREAKUP_CCBDGT, CC_BUDGET_BREAKUP_CCBDGT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_activity_custom_attr (
      attribute_numeric_val decimal(17,2) NULL, updated_dttm timestamp NULL, attribute_dttm_val timestamp NULL, updated_by_nm varchar(60) NULL, 
      attribute_character_val varchar(1500) NULL, activity_version_id varchar(36) NULL, activity_id varchar(36) NULL, attribute_data_type_cd varchar(30) NULL, 
      attribute_nm varchar(256) NULL, attribute_val varchar(1500) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_ACTIVITY_CUSTOM_ATTR, CDM_ACTIVITY_CUSTOM_ATTR);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_activity_detail (
      updated_dttm timestamp NULL, last_published_dttm timestamp NULL, valid_from_dttm timestamp NULL, valid_to_dttm timestamp NULL, 
      status_cd varchar(20) NULL, source_system_cd varchar(10) NULL, activity_nm varchar(256) NULL, activity_id varchar(36) NULL, 
      activity_desc varchar(1500) NULL, activity_category_nm varchar(100) NULL, activity_cd varchar(60) NULL, activity_version_id varchar(36) NULL, 
      updated_by_nm varchar(60) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_ACTIVITY_DETAIL, CDM_ACTIVITY_DETAIL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_activity_x_task (
      updated_dttm timestamp NULL, task_version_id varchar(36) NULL, activity_version_id varchar(36) NULL, activity_id varchar(36) NULL, 
      task_id varchar(36) NULL, updated_by_nm varchar(60) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_ACTIVITY_X_TASK, CDM_ACTIVITY_X_TASK);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_audience_detail (
      audience_schedule_flg char(1) NULL, create_dttm timestamp NULL, delete_dttm timestamp NULL, updated_dttm timestamp NULL, 
      created_user_nm varchar(256) NULL, audience_source_nm varchar(100) NULL, audience_nm varchar(128) NULL, audience_id varchar(36) NULL, 
      audience_desc varchar(1332) NULL, audience_data_source_nm varchar(100) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_AUDIENCE_DETAIL, CDM_AUDIENCE_DETAIL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_audience_occur_detail (
      audience_size_cnt integer NULL, end_dttm timestamp NULL, updated_dttm timestamp NULL, start_dttm timestamp NULL, 
      started_by_nm varchar(256) NULL, occurrence_type_nm varchar(100) NULL, audience_occur_id varchar(36) NULL, audience_id varchar(36) NULL, 
      execution_status_cd varchar(100) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_AUDIENCE_OCCUR_DETAIL, CDM_AUDIENCE_OCCUR_DETAIL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_audience_x_segment (
      segment_id varchar(36) NULL, audience_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_AUDIENCE_X_SEGMENT, CDM_AUDIENCE_X_SEGMENT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_business_context (
      updated_dttm timestamp NULL, updated_by_nm varchar(60) NULL, business_context_type_cd varchar(40) NULL, business_context_nm varchar(256) NULL, 
      business_context_id varchar(36) NULL, source_system_cd varchar(10) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_BUSINESS_CONTEXT, CDM_BUSINESS_CONTEXT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_campaign_custom_attr (
      attribute_numeric_val decimal(17,2) NULL, updated_dttm timestamp NULL, attribute_dttm_val timestamp NULL, page_nm varchar(60) NULL, 
      campaign_id varchar(36) NULL, attribute_character_val varchar(1500) NULL, attribute_data_type_cd varchar(30) NULL, attribute_nm varchar(256) NULL, 
      attribute_val varchar(1500) NULL, extension_attribute_nm varchar(256) NULL, updated_by_nm varchar(60) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_CAMPAIGN_CUSTOM_ATTR, CDM_CAMPAIGN_CUSTOM_ATTR);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_campaign_detail (
      deleted_flg char(1) NULL, current_version_flg char(1) NULL, max_budget_amt decimal(17,2) NULL, min_budget_offer_amt decimal(17,2) NULL, 
      min_budget_amt decimal(17,2) NULL, max_budget_offer_amt decimal(17,2) NULL, campaign_version_no integer NULL, deployment_version_no integer NULL, 
      campaign_group_sk integer NULL, approval_dttm timestamp NULL, valid_from_dttm timestamp NULL, run_dttm timestamp NULL, 
      updated_dttm timestamp NULL, valid_to_dttm timestamp NULL, start_dttm timestamp NULL, last_modified_dttm timestamp NULL, 
      end_dttm timestamp NULL, updated_by_nm varchar(60) NULL, source_system_cd varchar(10) NULL, campaign_status_cd varchar(3) NULL, 
      campaign_type_cd varchar(3) NULL, last_modified_by_user_nm varchar(60) NULL, campaign_nm varchar(60) NULL, campaign_folder_txt varchar(1024) NULL, 
      campaign_desc varchar(1500) NULL, campaign_cd varchar(60) NULL, approval_given_by_nm varchar(60) NULL, campaign_id varchar(36) NULL, 
      campaign_owner_nm varchar(60) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_CAMPAIGN_DETAIL, CDM_CAMPAIGN_DETAIL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_contact_channel (
      updated_dttm timestamp NULL, contact_channel_cd varchar(60) NULL, updated_by_nm varchar(60) NULL, contact_channel_nm varchar(40) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_CONTACT_CHANNEL, CDM_CONTACT_CHANNEL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_contact_history (
      optimization_backfill_flg char(1) NULL, control_group_flg char(1) NULL, contact_dt date NOT NULL, updated_dttm timestamp NULL, 
      contact_dttm_tz timestamp NULL, contact_dttm timestamp NULL, source_system_cd varchar(10) NULL, external_contact_info_2_id varchar(32) NULL, 
      context_type_nm varchar(256) NULL, audience_id varchar(36) NULL, contact_nm varchar(256) NULL, identity_id varchar(36) NULL, 
      audience_occur_id varchar(36) NULL, contact_id varchar(36) NOT NULL, contact_status_cd varchar(3) NULL, context_val varchar(256) NULL, 
      external_contact_info_1_id varchar(32) NULL, rtc_id varchar(36) NULL, updated_by_nm varchar(60) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_CONTACT_HISTORY, CDM_CONTACT_HISTORY);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_contact_status (
      updated_dttm timestamp NULL, contact_status_desc varchar(256) NULL, contact_status_cd varchar(3) NULL, updated_by_nm varchar(60) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_CONTACT_STATUS, CDM_CONTACT_STATUS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_content_custom_attr (
      attribute_numeric_val decimal(17,2) NULL, updated_dttm timestamp NULL, attribute_dttm_val timestamp NULL, updated_by_nm varchar(60) NULL, 
      attribute_val varchar(1500) NULL, attribute_data_type_cd varchar(30) NULL, attribute_nm varchar(256) NULL, content_version_id varchar(40) NULL, 
      attribute_character_val varchar(1500) NULL, content_id varchar(40) NULL, extension_attribute_nm varchar(256) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_CONTENT_CUSTOM_ATTR, CDM_CONTENT_CUSTOM_ATTR);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_content_detail (
      active_flg char(1) NULL, created_dt date NULL, updated_dttm timestamp NULL, valid_from_dttm timestamp NULL, 
      valid_to_dttm timestamp NULL, updated_by_nm varchar(60) NULL, owner_nm varchar(256) NULL, external_reference_url_txt varchar(1024) NULL, 
      content_id varchar(40) NULL, contact_content_status_cd varchar(60) NULL, contact_content_cd varchar(60) NULL, contact_content_class_nm varchar(100) NULL, 
      contact_content_desc varchar(1500) NULL, contact_content_nm varchar(256) NULL, contact_content_type_nm varchar(50) NULL, content_version_id varchar(40) NULL, 
      created_user_nm varchar(256) NULL, external_reference_txt varchar(1024) NULL, source_system_cd varchar(10) NULL, contact_content_category_nm varchar(256) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_CONTENT_DETAIL, CDM_CONTENT_DETAIL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_dyn_content_custom_attr (
      attribute_numeric_val decimal(17,2) NULL, attribute_dttm_val timestamp NULL, updated_dttm timestamp NULL, updated_by_nm varchar(60) NULL, 
      content_hash_val varchar(32) NULL, attribute_character_val varchar(1500) NULL, attribute_data_type_cd varchar(30) NULL, attribute_val varchar(1500) NULL, 
      content_version_id varchar(40) NULL, attribute_nm varchar(256) NULL, content_id varchar(40) NULL, extension_attribute_nm varchar(256) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_DYN_CONTENT_CUSTOM_ATTR, CDM_DYN_CONTENT_CUSTOM_ATTR);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_identifier_type (
      updated_dttm timestamp NULL, updated_by_nm varchar(60) NULL, identifier_type_desc varchar(100) NULL, identifier_type_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_IDENTIFIER_TYPE, CDM_IDENTIFIER_TYPE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_identity_attr (
      entry_dttm timestamp NULL, valid_to_dttm timestamp NULL, valid_from_dttm timestamp NULL, updated_dttm timestamp NULL, 
      identifier_type_id varchar(36) NULL, user_identifier_val varchar(5000) NULL, updated_by_nm varchar(60) NULL, source_system_cd varchar(10) NULL, 
      identity_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_IDENTITY_ATTR, CDM_IDENTITY_ATTR);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_identity_map (
      updated_dttm timestamp NULL, identity_type_cd varchar(40) NULL, identity_id varchar(36) NULL, updated_by_nm varchar(60) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_IDENTITY_MAP, CDM_IDENTITY_MAP);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_identity_type (
      updated_dttm timestamp NULL, updated_by_nm varchar(60) NULL, identity_type_desc varchar(100) NULL, identity_type_cd varchar(40) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_IDENTITY_TYPE, CDM_IDENTITY_TYPE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_occurrence_detail (
      occurrence_no integer NULL, end_dttm timestamp NULL, updated_dttm timestamp NULL, start_dttm timestamp NULL, 
      updated_by_nm varchar(60) NULL, source_system_cd varchar(10) NULL, occurrence_object_type_cd varchar(60) NULL, occurrence_object_id varchar(36) NULL, 
      occurrence_id varchar(36) NULL, execution_status_cd varchar(30) NULL, occurrence_type_cd varchar(100) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_OCCURRENCE_DETAIL, CDM_OCCURRENCE_DETAIL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_response_channel (
      updated_dttm timestamp NULL, updated_by_nm varchar(60) NULL, response_channel_nm varchar(60) NULL, response_channel_cd varchar(40) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_RESPONSE_CHANNEL, CDM_RESPONSE_CHANNEL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_response_extended_attr (
      updated_dttm timestamp NOT NULL, updated_by_nm varchar(60) NULL, response_id varchar(36) NOT NULL, response_attribute_type_cd varchar(10) NOT NULL, 
      attribute_val varchar(1500) NULL, attribute_nm varchar(256) NOT NULL, attribute_data_type_cd varchar(30) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_RESPONSE_EXTENDED_ATTR, CDM_RESPONSE_EXTENDED_ATTR);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_response_history (
      conversion_flg char(1) NULL, inferred_response_flg char(1) NULL, response_dt date NOT NULL, response_val_amt decimal(17,2) NULL, 
      properties_map_doc varchar(4000) NULL, updated_dttm timestamp NULL, response_dttm timestamp NULL, response_dttm_tz timestamp NULL, 
      updated_by_nm varchar(60) NULL, source_system_cd varchar(10) NULL, rtc_id varchar(36) NULL, response_type_cd varchar(60) NULL, 
      response_id varchar(36) NOT NULL, response_channel_cd varchar(40) NULL, response_cd varchar(256) NULL, identity_id varchar(36) NULL, 
      external_contact_info_2_id varchar(32) NULL, external_contact_info_1_id varchar(32) NULL, context_val varchar(256) NULL, context_type_nm varchar(256) NULL, 
      content_version_id varchar(40) NULL, content_id varchar(40) NULL, content_hash_val varchar(32) NULL, contact_id varchar(36) NULL, 
      audience_occur_id varchar(36) NULL, audience_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_RESPONSE_HISTORY, CDM_RESPONSE_HISTORY);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_response_lookup (
      updated_dttm timestamp NULL, updated_by_nm varchar(60) NULL, response_nm varchar(256) NULL, response_cd varchar(256) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_RESPONSE_LOOKUP, CDM_RESPONSE_LOOKUP);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_response_type (
      updated_dttm timestamp NULL, updated_by_nm varchar(60) NULL, response_type_desc varchar(256) NULL, response_type_cd varchar(60) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_RESPONSE_TYPE, CDM_RESPONSE_TYPE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_rtc_detail (
      deleted_flg char(1) NULL, response_tracking_flg char(1) NULL, task_occurrence_no integer NULL, processed_dttm timestamp NULL, 
      updated_dttm timestamp NULL, updated_by_nm varchar(60) NULL, task_version_id varchar(36) NULL, task_id varchar(36) NULL, 
      source_system_cd varchar(10) NULL, segment_version_id varchar(36) NULL, segment_id varchar(36) NULL, rtc_id varchar(36) NULL, 
      occurrence_id varchar(36) NULL, execution_status_cd varchar(30) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_RTC_DETAIL, CDM_RTC_DETAIL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_rtc_x_content (
      sequence_no integer NULL, updated_dttm timestamp NULL, updated_by_nm varchar(60) NULL, rtc_x_content_sk varchar(36) NULL, 
      rtc_id varchar(36) NULL, content_version_id varchar(40) NULL, content_id varchar(40) NULL, content_hash_val varchar(32) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_RTC_X_CONTENT, CDM_RTC_X_CONTENT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_segment_custom_attr (
      attribute_numeric_val decimal(17,2) NULL, updated_dttm timestamp NULL, attribute_dttm_val timestamp NULL, updated_by_nm varchar(60) NULL, 
      segment_version_id varchar(36) NULL, segment_id varchar(36) NULL, attribute_val varchar(1500) NULL, attribute_nm varchar(256) NULL, 
      attribute_data_type_cd varchar(30) NULL, attribute_character_val varchar(1500) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_SEGMENT_CUSTOM_ATTR, CDM_SEGMENT_CUSTOM_ATTR);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_segment_detail (
      valid_from_dttm timestamp NULL, updated_dttm timestamp NULL, valid_to_dttm timestamp NULL, updated_by_nm varchar(60) NULL, 
      source_system_cd varchar(10) NULL, segment_version_id varchar(36) NULL, segment_status_cd varchar(20) NULL, segment_src_nm varchar(40) NULL, 
      segment_nm varchar(256) NULL, segment_map_version_id varchar(36) NULL, segment_map_id varchar(36) NULL, segment_id varchar(36) NULL, 
      segment_desc varchar(1500) NULL, segment_cd varchar(60) NULL, segment_category_nm varchar(100) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_SEGMENT_DETAIL, CDM_SEGMENT_DETAIL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_segment_map (
      valid_to_dttm timestamp NULL, valid_from_dttm timestamp NULL, updated_dttm timestamp NULL, updated_by_nm varchar(60) NULL, 
      source_system_cd varchar(10) NULL, segment_map_version_id varchar(36) NULL, segment_map_status_cd varchar(20) NULL, segment_map_src_nm varchar(40) NULL, 
      segment_map_nm varchar(256) NULL, segment_map_id varchar(36) NULL, segment_map_desc varchar(1500) NULL, segment_map_cd varchar(60) NULL, 
      segment_map_category_nm varchar(100) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_SEGMENT_MAP, CDM_SEGMENT_MAP);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_segment_map_custom_attr (
      attribute_numeric_val decimal(17,2) NULL, updated_dttm timestamp NULL, attribute_dttm_val timestamp NULL, updated_by_nm varchar(60) NULL, 
      segment_map_version_id varchar(36) NULL, segment_map_id varchar(36) NULL, attribute_val varchar(1500) NULL, attribute_nm varchar(256) NULL, 
      attribute_data_type_cd varchar(30) NULL, attribute_character_val varchar(1500) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_SEGMENT_MAP_CUSTOM_ATTR, CDM_SEGMENT_MAP_CUSTOM_ATTR);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_segment_test (
      stratified_sampling_flg char(1) NULL, test_enabled_flg char(1) NULL, test_pct decimal(5,2) NULL, test_cnt integer NULL, 
      updated_dttm timestamp NULL, test_sizing_type_nm varchar(65) NULL, test_type_nm varchar(10) NULL, test_nm varchar(65) NULL, 
      test_cd varchar(60) NULL, task_version_id varchar(36) NULL, task_id varchar(36) NULL, stratified_samp_criteria_txt varchar(1024) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_SEGMENT_TEST, CDM_SEGMENT_TEST);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_segment_test_x_segment (
      updated_dttm timestamp NULL, test_cd varchar(60) NULL, task_id varchar(36) NULL, segment_id varchar(36) NULL, 
      task_version_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_SEGMENT_TEST_X_SEGMENT, CDM_SEGMENT_TEST_X_SEGMENT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_task_custom_attr (
      attribute_numeric_val decimal(17,2) NULL, attribute_dttm_val timestamp NULL, updated_dttm timestamp NULL, task_version_id varchar(36) NULL, 
      task_id varchar(36) NULL, extension_attribute_nm varchar(256) NULL, attribute_character_val varchar(1500) NULL, attribute_data_type_cd varchar(30) NULL, 
      attribute_nm varchar(256) NULL, attribute_val varchar(1500) NULL, updated_by_nm varchar(60) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_TASK_CUSTOM_ATTR, CDM_TASK_CUSTOM_ATTR);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..cdm_task_detail (
      segment_tests_flg char(1) NULL, saved_flg char(1) NULL, scheduled_flg char(1) NULL, recurring_schedule_flg char(1) NULL, 
      published_flg char(1) NULL, staged_flg char(1) NULL, limit_by_total_impression_flg char(1) NULL, update_contact_history_flg char(1) NULL, 
      standard_reply_flg char(1) NULL, active_flg char(1) NULL, created_dt date NULL, budget_unit_usage_amt decimal(17,2) NULL, 
      budget_unit_cost_amt decimal(17,2) NULL, max_budget_amt decimal(17,2) NULL, min_budget_amt decimal(17,2) NULL, min_budget_offer_amt decimal(17,2) NULL, 
      max_budget_offer_amt decimal(17,2) NULL, maximum_period_expression_cnt integer NULL, limit_period_unit_cnt integer NULL, scheduled_end_dttm timestamp NULL, 
      updated_dttm timestamp NULL, valid_from_dttm timestamp NULL, export_dttm timestamp NULL, valid_to_dttm timestamp NULL, 
      scheduled_start_dttm timestamp NULL, task_version_id varchar(36) NULL, task_type_nm varchar(40) NULL, task_subtype_nm varchar(100) NULL, 
      task_status_cd varchar(20) NULL, task_id varchar(36) NULL, task_delivery_type_nm varchar(60) NULL, subject_type_nm varchar(60) NULL, 
      source_system_cd varchar(10) NULL, modified_status_cd varchar(20) NULL, contact_channel_cd varchar(60) NULL, business_context_id varchar(36) NULL, 
      campaign_id varchar(36) NULL, control_group_action_nm varchar(65) NULL, created_user_nm varchar(256) NULL, owner_nm varchar(256) NULL, 
      recurr_type_cd varchar(3) NULL, stratified_sampling_action_nm varchar(65) NULL, task_cd varchar(60) NULL, task_desc varchar(1500) NULL, 
      task_nm varchar(256) NULL, updated_by_nm varchar(60) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_TASK_DETAIL, CDM_TASK_DETAIL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..commitment_details (
      vendor_obsolete_flg char(1) NULL, cmtmnt_overspent_amt decimal(17,2) NULL, vendor_amt decimal(17,2) NULL, cmtmnt_outstanding_amt decimal(17,2) NULL, 
      cmtmnt_amt decimal(17,2) NULL, last_modified_dttm timestamp NULL, cmtmnt_payment_dttm timestamp NULL, cmtmnt_created_dttm timestamp NULL, 
      load_dttm timestamp NULL, created_dttm timestamp NULL, vendor_number varchar(128) NULL, vendor_id varchar(128) NULL, 
      planning_nm varchar(128) NULL, planning_id varchar(128) NULL, last_modified_usernm varchar(128) NULL, created_by_usernm varchar(128) NULL, 
      cmtmnt_status varchar(128) NULL, cmtmnt_nm varchar(128) NULL, cmtmnt_desc varchar(1332) NULL, cmtmnt_closure_note varchar(1332) NULL, 
      cmtmnt_id varchar(128) NULL, cmtmnt_no varchar(128) NULL, planning_currency_cd varchar(10) NULL, vendor_currency_cd varchar(10) NULL, 
      vendor_nm varchar(128) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: COMMITMENT_DETAILS, COMMITMENT_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..commitment_line_items (
      item_qty bigint NULL, item_alloc_unit bigint NULL, item_vend_alloc_unit bigint NULL, vendor_obsolete_flg char(1) NULL, 
      cc_recon_alloc_amt decimal(17,2) NULL, cmtmnt_overspent_amt decimal(17,2) NULL, item_rate decimal(17,2) NULL, item_alloc_amt decimal(17,2) NULL, 
      cmtmnt_amt decimal(17,2) NULL, vendor_amt decimal(17,2) NULL, cmtmnt_outstanding_amt decimal(17,2) NULL, cc_allocated_amt decimal(17,2) NULL, 
      cc_available_amt decimal(17,2) NULL, item_vend_alloc_amt decimal(17,2) NULL, item_number integer NULL, created_dttm timestamp NULL, 
      last_modified_dttm timestamp NULL, load_dttm timestamp NULL, cmtmnt_payment_dttm timestamp NULL, cmtmnt_created_dttm timestamp NULL, 
      vendor_nm varchar(128) NULL, vendor_currency_cd varchar(10) NULL, planning_nm varchar(128) NULL, planning_currency_cd varchar(10) NULL, 
      last_modified_usernm varchar(128) NULL, gen_ledger_cd varchar(128) NULL, cost_center_id varchar(128) NULL, cmtmnt_status varchar(128) NULL, 
      cmtmnt_no varchar(128) NULL, cmtmnt_nm varchar(128) NULL, cmtmnt_desc varchar(1332) NULL, cmtmnt_closure_note varchar(1332) NULL, 
      ccat_nm varchar(128) NULL, cc_owner_usernm varchar(128) NULL, cc_nm varchar(128) NULL, cc_desc varchar(1332) NULL, 
      cmtmnt_id varchar(128) NULL, created_by_usernm varchar(128) NULL, fin_acc_nm varchar(128) NULL, item_nm varchar(128) NULL, 
      planning_id varchar(128) NULL, vendor_id varchar(128) NULL, vendor_number varchar(128) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: COMMITMENT_LINE_ITEMS, COMMITMENT_LINE_ITEMS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..commitment_line_items_ccbdgt (
      vendor_obsolete_flg char(1) NULL, fp_obsolete_flg char(1) NULL, cc_obsolete_flg char(1) NULL, fp_end_dt date NULL, 
      fp_start_dt date NULL, item_vend_alloc_amt decimal(17,2) NULL, cc_available_amt decimal(17,2) NULL, item_alloc_amt decimal(17,2) NULL, 
      cc_allocated_amt decimal(17,2) NULL, cc_bdgt_cmtmnt_invoice_amt decimal(17,2) NULL, vendor_amt decimal(17,2) NULL, cmtmnt_outstanding_amt decimal(17,2) NULL, 
      cmtmnt_amt decimal(17,2) NULL, cc_recon_alloc_amt decimal(17,2) NULL, cc_bdgt_budget_amt decimal(17,2) NULL, cc_bdgt_committed_amt decimal(17,2) NULL, 
      item_rate decimal(17,2) NULL, cc_bdgt_cmtmnt_overspent_amt decimal(17,2) NULL, cc_bdgt_invoiced_amt decimal(17,2) NULL, cc_bdgt_amt decimal(17,2) NULL, 
      cc_bdgt_cmtmnt_outstanding_amt decimal(17,2) NULL, cmtmnt_overspent_amt decimal(17,2) NULL, cc_bdgt_direct_invoice_amt decimal(17,2) NULL, item_number integer NULL, 
      item_alloc_unit integer NULL, item_qty integer NULL, cc_bdgt_cmtmnt_invoice_cnt integer NULL, item_vend_alloc_unit integer NULL, 
      created_dttm timestamp NULL, cmtmnt_payment_dttm timestamp NULL, last_modified_dttm timestamp NULL, load_dttm timestamp NULL, 
      cmtmnt_created_dttm timestamp NULL, vendor_currency_cd varchar(10) NULL, planning_currency_cd varchar(10) NULL, last_modified_usernm varchar(128) NULL, 
      gen_ledger_cd varchar(128) NULL, fp_id varchar(128) NULL, item_nm varchar(128) NULL, planning_nm varchar(128) NULL, 
      vendor_nm varchar(128) NULL, fp_desc varchar(1332) NULL, fp_cls_ver varchar(128) NULL, fin_acc_nm varchar(128) NULL, 
      created_by_usernm varchar(128) NULL, cmtmnt_status varchar(128) NULL, cmtmnt_no varchar(128) NULL, cmtmnt_id varchar(128) NULL, 
      cmtmnt_desc varchar(1332) NULL, cc_nm varchar(128) NULL, cc_desc varchar(1332) NULL, cc_bdgt_budget_desc varchar(1332) NULL, 
      cc_number varchar(128) NULL, cc_owner_usernm varchar(128) NULL, ccat_nm varchar(128) NULL, cmtmnt_closure_note varchar(1332) NULL, 
      cmtmnt_nm varchar(128) NULL, cost_center_id varchar(128) NULL, fp_nm varchar(128) NULL, planning_id varchar(128) NULL, 
      vendor_id varchar(128) NULL, vendor_number varchar(128) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: COMMITMENT_LINE_ITEMS_CCBDGT, COMMITMENT_LINE_ITEMS_CCBDGT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..contact_history (
      control_group_flg char(1) NULL, properties_map_doc varchar(4000) NULL, contact_dttm_tz timestamp NULL, load_dttm timestamp NOT NULL, 
      contact_dttm timestamp NULL, task_id varchar(36) NULL, parent_event_designed_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL, 
      detail_id_hex varchar(32) NULL, context_type_nm varchar(256) NULL, audience_id varchar(36) NULL, contact_id varchar(36) NOT NULL, 
      identity_id varchar(36) NULL, message_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, visit_id_hex varchar(32) NULL, 
      aud_occurrence_id varchar(36) NULL, contact_channel_nm varchar(19) NULL, contact_nm varchar(256) NULL, context_val varchar(256) NULL, 
      creative_id varchar(36) NULL, event_designed_id varchar(36) NULL, journey_id varchar(36) NULL, occurrence_id varchar(36) NULL, 
      session_id_hex varchar(29) NULL, task_version_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CONTACT_HISTORY, CONTACT_HISTORY);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..conversion_milestone (
      test_flg char(1) NULL, control_group_flg char(1) NULL, total_cost_amt decimal(17,2) NULL, properties_map_doc varchar(4000) NULL, 
      load_dttm timestamp NOT NULL, conversion_milestone_dttm timestamp NULL, conversion_milestone_dttm_tz timestamp NULL, visit_id_hex varchar(32) NULL, 
      task_id varchar(36) NULL, spot_id varchar(36) NULL, segment_version_id varchar(36) NULL, reserved_1_txt varchar(100) NULL, 
      occurrence_id varchar(36) NULL, message_version_id varchar(36) NULL, goal_id varchar(36) NULL, detail_id_hex varchar(32) NULL, 
      channel_user_id varchar(300) NULL, analysis_group_id varchar(36) NULL, audience_id varchar(36) NULL, context_val varchar(256) NULL, 
      creative_id varchar(36) NULL, event_id varchar(36) NOT NULL, journey_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, 
      activity_id varchar(36) NULL, aud_occurrence_id varchar(36) NULL, channel_nm varchar(36) NULL, context_type_nm varchar(256) NULL, 
      creative_version_id varchar(36) NULL, event_designed_id varchar(36) NULL, event_nm varchar(256) NULL, identity_id varchar(36) NULL, 
      journey_occurrence_id varchar(36) NULL, message_id varchar(36) NULL, mobile_app_id varchar(40) NULL, parent_event_designed_id varchar(36) NULL, 
      rec_group_id varchar(3) NULL, reserved_2_txt varchar(100) NULL, segment_id varchar(36) NULL, session_id_hex varchar(29) NULL, 
      subject_line_txt varchar(256) NULL, task_version_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CONVERSION_MILESTONE, CONVERSION_MILESTONE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..custom_events (
      custom_revenue_amt decimal(17,2) NULL, properties_map_doc varchar(4000) NULL, custom_event_dttm timestamp NULL, custom_event_dttm_tz timestamp NULL, 
      load_dttm timestamp NOT NULL, session_id varchar(29) NULL, page_id varchar(256) NULL, event_type_nm varchar(20) NULL, 
      event_id varchar(36) NOT NULL, channel_user_id varchar(300) NULL, custom_event_nm varchar(256) NULL, detail_id_hex varchar(32) NULL, 
      event_nm varchar(256) NULL, reserved_1_txt varchar(100) NULL, reserved_2_txt varchar(100) NULL, visit_id varchar(32) NULL, 
      channel_nm varchar(40) NULL, custom_event_group_nm varchar(256) NULL, custom_events_sk varchar(32) NULL, detail_id varchar(32) NULL, 
      event_designed_id varchar(36) NULL, event_key_cd varchar(100) NULL, event_source_cd varchar(100) NULL, identity_id varchar(36) NULL, 
      mobile_app_id varchar(64) NULL, session_id_hex varchar(29) NULL, visit_id_hex varchar(32) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CUSTOM_EVENTS, CUSTOM_EVENTS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..custom_events_ext (
      custom_revenue_amt decimal(17,2) NULL, load_dttm timestamp NOT NULL, event_designed_id varchar(36) NULL, custom_events_sk varchar(32) NOT NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CUSTOM_EVENTS_EXT, CUSTOM_EVENTS_EXT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..daily_usage (
      bc_subjcnt_str varchar(4000) NULL, customer_profiles_processed_str varchar(4000) NULL, api_usage_str varchar(4000) NULL, mob_impr_cnt bigint NULL, 
      dm_destinations_total_row_cnt bigint NULL, google_ads_cnt bigint NULL, mob_sesn_cnt bigint NULL, audience_usage_cnt bigint NULL, 
      mobile_in_app_msg_cnt bigint NULL, mobile_push_cnt bigint NULL, email_preview_cnt bigint NULL, facebook_ads_cnt bigint NULL, 
      web_sesn_cnt bigint NULL, plan_users_cnt bigint NULL, outbound_api_cnt bigint NULL, web_impr_cnt bigint NULL, 
      email_send_cnt bigint NULL, linkedin_ads_cnt bigint NULL, dm_destinations_total_id_cnt bigint NULL, asset_size decimal(17,2) NULL, 
      db_size decimal(17,2) NULL, admin_user_cnt integer NULL, event_day varchar(36) NOT NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DAILY_USAGE, DAILY_USAGE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..data_view_details (
      total_cost_amt decimal(17,2) NULL, properties_map_doc varchar(4000) NULL, data_view_dttm timestamp NULL, data_view_dttm_tz timestamp NULL, 
      load_dttm timestamp NOT NULL, visit_id varchar(32) NULL, reserved_2_txt varchar(100) NULL, event_designed_id varchar(36) NULL, 
      channel_user_id varchar(300) NULL, detail_id varchar(32) NULL, event_nm varchar(256) NULL, session_id_hex varchar(29) NULL, 
      detail_id_hex varchar(32) NULL, event_id varchar(36) NOT NULL, identity_id varchar(36) NULL, parent_event_designed_id varchar(36) NULL, 
      reserved_1_txt varchar(100) NULL, session_id varchar(29) NULL, visit_id_hex varchar(32) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DATA_VIEW_DETAILS, DATA_VIEW_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..dbt_adv_campaign_visitors (
      ge_longitude decimal(13,6) NULL, ge_latitude decimal(13,6) NULL, rv_revenue decimal(17,2) NULL, co_conversions integer NULL, 
      new_visitors integer NULL, return_visitors integer NULL, bouncers integer NULL, visits integer NULL, 
      page_views integer NULL, average_visit_duration integer NULL, session_complete_load_dttm timestamp NOT NULL, visit_dttm timestamp NULL, 
      visit_dttm_tz timestamp NULL, session_start_dttm_tz timestamp NULL, session_start_dttm timestamp NULL, se_external_search_engine varchar(130) NULL, 
      landing_page varchar(1332) NULL, ge_country varchar(85) NULL, cu_customer_id varchar(36) NULL, br_browser_version varchar(16) NULL, 
      device_type varchar(52) NULL, landing_page_url_domain varchar(215) NULL, se_external_search_engine_phrase varchar(1332) NULL, bouncer varchar(12) NULL, 
      br_browser_name varchar(52) NULL, device_name varchar(85) NULL, ge_city varchar(390) NULL, ge_state_region varchar(2) NULL, 
      landing_page_url varchar(1332) NULL, pl_device_operating_system varchar(78) NULL, se_external_search_engine_domain varchar(215) NULL, visitor_type varchar(10) NULL, 
      visitor_id varchar(32) NULL, visit_origination_type varchar(65) NULL, visit_origination_tracking_code varchar(65) NULL, visit_origination_placement varchar(390) NULL, 
      visit_origination_name varchar(260) NULL, visit_origination_creative varchar(260) NULL, visit_id varchar(32) NOT NULL, session_id varchar(29) NOT NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_ADV_CAMPAIGN_VISITORS, DBT_ADV_CAMPAIGN_VISITORS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..dbt_business_process (
      processes smallint NULL, steps_completed smallint NULL, step_count smallint NULL, processes_completed smallint NULL, 
      steps_abandoned smallint NULL, last_step smallint NULL, processes_abandoned smallint NULL, steps smallint NULL, 
      bus_process_started_dttm_tz timestamp NULL, session_start_dttm_tz timestamp NULL, session_start_dttm timestamp NULL, bus_process_started_dttm timestamp NOT NULL, 
      session_complete_load_dttm timestamp NOT NULL, visitor_id varchar(32) NULL, visit_origination_tracking_code varchar(65) NULL, visit_origination_name varchar(260) NULL, 
      visit_id varchar(32) NULL, session_id varchar(29) NOT NULL, device_name varchar(85) NULL, cu_customer_id varchar(36) NULL, 
      business_process_step_name varchar(130) NOT NULL, business_process_attribute_2 varchar(130) NULL, bouncer varchar(12) NULL, business_process_attribute_1 varchar(130) NULL, 
      business_process_name varchar(130) NOT NULL, device_type varchar(52) NULL, visit_origination_creative varchar(260) NULL, visit_origination_placement varchar(390) NULL, 
      visit_origination_type varchar(65) NULL, visitor_type varchar(10) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_BUSINESS_PROCESS, DBT_BUSINESS_PROCESS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..dbt_content (
      total_page_view_time bigint NULL, entry_pages integer NULL, active_page_view_time integer NULL, views integer NULL, 
      exit_pages integer NULL, visits integer NULL, bouncers integer NULL, session_start_dttm timestamp NULL, 
      session_complete_load_dttm timestamp NOT NULL, session_start_dttm_tz timestamp NULL, detail_dttm_tz timestamp NULL, detail_dttm timestamp NULL, 
      visitor_type varchar(10) NULL, visitor_id varchar(32) NULL, visit_origination_type varchar(65) NULL, visit_origination_tracking_code varchar(65) NULL, 
      visit_origination_placement varchar(390) NULL, visit_origination_name varchar(260) NULL, visit_origination_creative varchar(260) NULL, visit_id varchar(32) NULL, 
      session_id varchar(29) NULL, pg_page_url varchar(1332) NULL, pg_page varchar(1332) NULL, pg_domain_name varchar(215) NULL, 
      device_type varchar(52) NULL, device_name varchar(85) NULL, detail_id varchar(32) NOT NULL, cu_customer_id varchar(36) NULL, 
      class2_id varchar(650) NULL, bouncer varchar(12) NULL, class1_id varchar(650) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_CONTENT, DBT_CONTENT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..dbt_documents (
      document_downloads integer NULL, document_download_dttm_tz timestamp NULL, session_start_dttm_tz timestamp NULL, session_start_dttm timestamp NULL, 
      session_complete_load_dttm timestamp NOT NULL, document_download_dttm timestamp NULL, visitor_type varchar(10) NULL, visitor_id varchar(32) NULL, 
      visit_origination_type varchar(65) NULL, visit_origination_tracking_code varchar(65) NULL, visit_origination_placement varchar(390) NULL, visit_origination_name varchar(260) NULL, 
      visit_origination_creative varchar(260) NULL, visit_id varchar(32) NULL, session_id varchar(29) NULL, do_page_url varchar(1332) NULL, 
      do_page_description varchar(1332) NULL, device_type varchar(52) NULL, device_name varchar(85) NULL, detail_id varchar(32) NOT NULL, 
      cu_customer_id varchar(36) NULL, class2_id varchar(650) NULL, class1_id varchar(650) NULL, bouncer varchar(12) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_DOCUMENTS, DBT_DOCUMENTS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..dbt_ecommerce (
      product_purchase_revenues decimal(17,2) NULL, basket_adds_revenue decimal(17,2) NULL, basket_removes_revenue decimal(17,2) NULL, product_views integer NULL, 
      basket_adds integer NULL, basket_adds_units integer NULL, product_purchases integer NULL, product_purchase_units integer NULL, 
      basket_removes_units integer NULL, basket_removes integer NULL, baskets_abandoned smallint NULL, baskets_completed smallint NULL, 
      baskets_started smallint NULL, session_complete_load_dttm timestamp NOT NULL, session_start_dttm_tz timestamp NULL, product_activity_dttm_tz timestamp NULL, 
      product_activity_dttm timestamp NOT NULL, session_start_dttm timestamp NULL, visitor_type varchar(10) NULL, visitor_id varchar(32) NULL, 
      visit_origination_type varchar(65) NULL, visit_origination_tracking_code varchar(65) NULL, visit_origination_placement varchar(390) NULL, visit_origination_name varchar(260) NULL, 
      visit_origination_creative varchar(260) NULL, visit_id varchar(32) NOT NULL, session_id varchar(29) NULL, product_sku varchar(100) NOT NULL, 
      product_name varchar(130) NOT NULL, product_id varchar(130) NOT NULL, product_group_name varchar(130) NULL, device_type varchar(52) NULL, 
      device_name varchar(85) NULL, cu_customer_id varchar(36) NULL, bouncer varchar(12) NULL, basket_id varchar(42) NOT NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_ECOMMERCE, DBT_ECOMMERCE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..dbt_forms (
      attempts integer NULL, forms_completed smallint NULL, forms_not_submitted smallint NULL, forms_started smallint NULL, 
      form_attempt_dttm timestamp NULL, session_start_dttm timestamp NULL, form_attempt_dttm_tz timestamp NULL, session_complete_load_dttm timestamp NOT NULL, 
      session_start_dttm_tz timestamp NULL, visitor_type varchar(10) NULL, visitor_id varchar(32) NULL, visit_origination_type varchar(65) NULL, 
      visit_origination_tracking_code varchar(65) NULL, visit_origination_placement varchar(390) NULL, visit_origination_name varchar(260) NULL, visit_origination_creative varchar(260) NULL, 
      visit_id varchar(32) NULL, session_id varchar(29) NULL, last_field varchar(325) NULL, form_nm varchar(65) NULL, 
      device_type varchar(52) NULL, device_name varchar(85) NULL, detail_id varchar(32) NOT NULL, cu_customer_id varchar(36) NULL, 
      bouncer varchar(12) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_FORMS, DBT_FORMS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..dbt_goals (
      goal_revenue decimal(17,2) NULL, visits integer NULL, session_start_dttm timestamp NULL, goal_reached_dttm_tz timestamp NULL, 
      goal_reached_dttm timestamp NULL, session_complete_load_dttm timestamp NOT NULL, session_start_dttm_tz timestamp NULL, goals smallint NULL, 
      visitor_type varchar(10) NULL, visitor_id varchar(32) NULL, visit_origination_type varchar(65) NULL, visit_origination_tracking_code varchar(65) NULL, 
      visit_origination_placement varchar(390) NULL, visit_origination_name varchar(260) NULL, visit_origination_creative varchar(260) NULL, visit_id varchar(32) NULL, 
      session_id varchar(29) NULL, goal_name varchar(260) NULL, goal_group_name varchar(130) NULL, device_type varchar(52) NULL, 
      device_name varchar(85) NULL, detail_id varchar(32) NOT NULL, cu_customer_id varchar(36) NULL, bouncer varchar(12) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_GOALS, DBT_GOALS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..dbt_media_consumption (
      time_viewing decimal(11,3) NULL, duration decimal(11,3) NULL, maximum_progress decimal(11,3) NOT NULL, content_viewed decimal(11,3) NULL, 
      counter integer NULL, interactions_count integer NOT NULL, session_start_dttm_tz timestamp NULL, session_start_dttm timestamp NULL, 
      session_complete_load_dttm timestamp NOT NULL, media_start_dttm timestamp NULL, media_start_dttm_tz timestamp NULL, views_started smallint NULL, 
      views_completed smallint NULL, views smallint NULL, media_section_view smallint NULL, visitor_type varchar(10) NULL, 
      visitor_id varchar(32) NULL, visit_origination_type varchar(65) NULL, visit_origination_tracking_code varchar(65) NULL, visit_origination_placement varchar(390) NULL, 
      visit_origination_name varchar(260) NULL, visit_origination_creative varchar(260) NULL, visit_id varchar(32) NOT NULL, session_id varchar(29) NULL, 
      media_uri_txt varchar(2024) NULL, media_section varchar(35) NOT NULL, media_name varchar(260) NULL, media_completion_rate varchar(35) NOT NULL, 
      device_type varchar(52) NULL, device_name varchar(85) NULL, detail_id varchar(32) NOT NULL, cu_customer_id varchar(36) NULL, 
      bouncer varchar(12) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_MEDIA_CONSUMPTION, DBT_MEDIA_CONSUMPTION);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..dbt_promotions (
      click_throughs integer NULL, displays integer NULL, session_start_dttm_tz timestamp NULL, promotion_shown_dttm_tz timestamp NULL, 
      promotion_shown_dttm timestamp NULL, session_complete_load_dttm timestamp NOT NULL, session_start_dttm timestamp NULL, visitor_type varchar(10) NULL, 
      visitor_id varchar(32) NULL, visit_origination_type varchar(65) NULL, visit_origination_tracking_code varchar(65) NULL, visit_origination_placement varchar(390) NULL, 
      visit_origination_name varchar(260) NULL, visit_origination_creative varchar(260) NULL, visit_id varchar(32) NULL, session_id varchar(29) NULL, 
      promotion_type varchar(65) NULL, promotion_tracking_code varchar(65) NULL, promotion_placement varchar(260) NULL, promotion_name varchar(260) NULL, 
      promotion_creative varchar(260) NULL, device_type varchar(52) NULL, device_name varchar(85) NULL, detail_id varchar(32) NOT NULL, 
      cu_customer_id varchar(36) NULL, bouncer varchar(12) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_PROMOTIONS, DBT_PROMOTIONS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..dbt_search (
      num_additional_searches integer NULL, num_pages_viewed_afterwards integer NULL, searches integer NULL, visits integer NULL, 
      search_unknown_results integer NULL, search_returned_results integer NULL, exit_pages integer NULL, search_no_results_returned integer NULL, 
      search_results_dttm_tz timestamp NULL, session_start_dttm timestamp NULL, session_start_dttm_tz timestamp NULL, session_complete_load_dttm timestamp NOT NULL, 
      search_results_dttm timestamp NULL, visitor_type varchar(10) NULL, visitor_id varchar(32) NULL, visit_origination_type varchar(65) NULL, 
      visit_origination_tracking_code varchar(65) NULL, visit_origination_placement varchar(390) NULL, visit_origination_name varchar(260) NULL, visit_origination_creative varchar(260) NULL, 
      visit_id varchar(32) NULL, session_id varchar(29) NULL, search_name varchar(42) NULL, internal_search_term varchar(128) NULL, 
      device_type varchar(52) NULL, device_name varchar(85) NULL, detail_id varchar(32) NOT NULL, cu_customer_id varchar(36) NULL, 
      bouncer varchar(12) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_SEARCH, DBT_SEARCH);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..direct_contact (
      control_active_flg char(1) NULL, control_group_flg char(1) NULL, properties_map_doc varchar(4000) NULL, load_dttm timestamp NOT NULL, 
      direct_contact_dttm timestamp NULL, direct_contact_dttm_tz timestamp NULL, task_version_id varchar(36) NULL, task_id varchar(36) NULL, 
      segment_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, occurrence_id varchar(36) NULL, message_id varchar(36) NULL, 
      identity_type_nm varchar(36) NULL, identity_id varchar(36) NULL, event_nm varchar(256) NULL, event_id varchar(36) NOT NULL, 
      event_designed_id varchar(36) NULL, context_val varchar(256) NULL, context_type_nm varchar(256) NULL, channel_user_id varchar(300) NULL, 
      channel_nm varchar(40) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DIRECT_CONTACT, DIRECT_CONTACT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..document_details (
      load_dttm timestamp NOT NULL, link_event_dttm timestamp NULL, link_event_dttm_tz timestamp NULL, visit_id_hex varchar(32) NULL, 
      uri_txt varchar(1332) NULL, session_id varchar(29) NULL, link_selector_path varchar(1332) NULL, link_id varchar(1332) NULL, 
      link_name varchar(1332) NULL, identity_id varchar(36) NULL, event_source_cd varchar(100) NULL, session_id_hex varchar(29) NULL, 
      event_key_cd varchar(100) NULL, visit_id varchar(32) NULL, event_id varchar(36) NOT NULL, detail_id_hex varchar(32) NULL, 
      detail_id varchar(32) NULL, alt_txt varchar(1332) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DOCUMENT_DETAILS, DOCUMENT_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..email_bounce (
      test_flg char(1) NULL, properties_map_doc varchar(4000) NULL, load_dttm timestamp NOT NULL, email_bounce_dttm_tz timestamp NULL, 
      email_bounce_dttm timestamp NULL, task_id varchar(36) NULL, subject_line_txt varchar(256) NULL, segment_version_id varchar(36) NULL, 
      response_tracking_cd varchar(36) NULL, reason_txt varchar(1000) NULL, raw_reason_txt varchar(1000) NULL, occurrence_id varchar(36) NULL, 
      journey_occurrence_id varchar(36) NULL, imprint_id varchar(36) NULL, event_nm varchar(256) NULL, event_designed_id varchar(36) NULL, 
      context_type_nm varchar(256) NULL, bounce_class_cd varchar(5) NULL, aud_occurrence_id varchar(36) NULL, analysis_group_id varchar(36) NULL, 
      audience_id varchar(36) NULL, channel_user_id varchar(300) NULL, context_val varchar(256) NULL, event_id varchar(36) NOT NULL, 
      identity_id varchar(36) NULL, journey_id varchar(36) NULL, program_id varchar(50) NULL, recipient_domain_nm varchar(100) NULL, 
      segment_id varchar(36) NULL, task_version_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EMAIL_BOUNCE, EMAIL_BOUNCE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..email_click (
      test_flg char(1) NULL, open_tracking_flg char(1) NULL, is_mobile_flg char(1) NULL, click_tracking_flg char(1) NULL, 
      properties_map_doc varchar(4000) NULL, email_click_dttm timestamp NULL, email_click_dttm_tz timestamp NULL, load_dttm timestamp NOT NULL, 
      uri_txt varchar(1332) NULL, task_version_id varchar(36) NULL, task_id varchar(36) NULL, subject_line_txt varchar(256) NULL, 
      segment_id varchar(36) NULL, recipient_domain_nm varchar(100) NULL, program_id varchar(50) NULL, platform_version varchar(25) NULL, 
      platform_desc varchar(78) NULL, occurrence_id varchar(36) NULL, manufacturer_nm varchar(75) NULL, mailbox_provider_nm varchar(100) NULL, 
      link_tracking_label_txt varchar(256) NULL, link_tracking_id varchar(4) NULL, link_tracking_group_txt varchar(256) NULL, journey_id varchar(36) NULL, 
      imprint_id varchar(36) NULL, event_nm varchar(256) NULL, event_id varchar(36) NOT NULL, event_designed_id varchar(36) NULL, 
      context_val varchar(256) NULL, audience_id varchar(36) NULL, analysis_group_id varchar(36) NULL, agent_family_nm varchar(100) NULL, 
      aud_occurrence_id varchar(36) NULL, channel_user_id varchar(300) NULL, context_type_nm varchar(256) NULL, device_nm varchar(85) NULL, 
      identity_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, segment_version_id varchar(36) NULL, 
      user_agent_nm varchar(512) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EMAIL_CLICK, EMAIL_CLICK);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..email_complaint (
      test_flg char(1) NULL, properties_map_doc varchar(4000) NULL, load_dttm timestamp NOT NULL, email_complaint_dttm timestamp NULL, 
      email_complaint_dttm_tz timestamp NULL, task_id varchar(36) NULL, segment_version_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, 
      recipient_domain_nm varchar(100) NULL, occurrence_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL, imprint_id varchar(36) NULL, 
      event_nm varchar(256) NULL, event_id varchar(36) NOT NULL, event_designed_id varchar(36) NULL, context_type_nm varchar(256) NULL, 
      audience_id varchar(36) NULL, analysis_group_id varchar(36) NULL, aud_occurrence_id varchar(36) NULL, channel_user_id varchar(300) NULL, 
      context_val varchar(256) NULL, identity_id varchar(36) NULL, journey_id varchar(36) NULL, program_id varchar(50) NULL, 
      segment_id varchar(36) NULL, subject_line_txt varchar(256) NULL, task_version_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EMAIL_COMPLAINT, EMAIL_COMPLAINT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..email_open (
      prefetched_flg char(1) NULL, click_tracking_flg char(1) NULL, open_tracking_flg char(1) NULL, is_mobile_flg char(1) NULL, 
      test_flg char(1) NULL, properties_map_doc varchar(4000) NULL, email_open_dttm timestamp NULL, email_open_dttm_tz timestamp NULL, 
      load_dttm timestamp NOT NULL, user_agent_nm varchar(512) NULL, task_version_id varchar(36) NULL, subject_line_txt varchar(256) NULL, 
      segment_version_id varchar(36) NULL, segment_id varchar(36) NULL, recipient_domain_nm varchar(100) NULL, program_id varchar(50) NULL, 
      platform_version varchar(25) NULL, occurrence_id varchar(36) NULL, manufacturer_nm varchar(75) NULL, journey_id varchar(36) NULL, 
      imprint_id varchar(36) NULL, event_nm varchar(256) NULL, event_designed_id varchar(36) NULL, context_val varchar(256) NULL, 
      audience_id varchar(36) NULL, analysis_group_id varchar(36) NULL, agent_family_nm varchar(100) NULL, aud_occurrence_id varchar(36) NULL, 
      channel_user_id varchar(300) NULL, context_type_nm varchar(256) NULL, device_nm varchar(85) NULL, event_id varchar(36) NOT NULL, 
      identity_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL, mailbox_provider_nm varchar(100) NULL, platform_desc varchar(78) NULL, 
      response_tracking_cd varchar(36) NULL, task_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EMAIL_OPEN, EMAIL_OPEN);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..email_optout (
      test_flg char(1) NULL, properties_map_doc varchar(4000) NULL, email_optout_dttm_tz timestamp NULL, email_optout_dttm timestamp NULL, 
      load_dttm timestamp NOT NULL, task_version_id varchar(36) NULL, subject_line_txt varchar(256) NULL, segment_id varchar(36) NULL, 
      recipient_domain_nm varchar(100) NULL, program_id varchar(50) NULL, optout_type_nm varchar(50) NULL, occurrence_id varchar(36) NULL, 
      link_tracking_label_txt varchar(256) NULL, link_tracking_group_txt varchar(256) NULL, journey_id varchar(36) NULL, identity_id varchar(36) NULL, 
      event_nm varchar(256) NULL, event_id varchar(36) NOT NULL, context_val varchar(256) NULL, channel_user_id varchar(300) NULL, 
      audience_id varchar(36) NULL, aud_occurrence_id varchar(36) NULL, analysis_group_id varchar(36) NULL, context_type_nm varchar(256) NULL, 
      event_designed_id varchar(36) NULL, imprint_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL, link_tracking_id varchar(4) NULL, 
      response_tracking_cd varchar(36) NULL, segment_version_id varchar(36) NULL, task_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EMAIL_OPTOUT, EMAIL_OPTOUT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..email_optout_details (
      test_flg char(1) NULL, properties_map_doc varchar(4000) NULL, email_action_dttm_tz timestamp NULL, email_action_dttm timestamp NULL, 
      load_dttm timestamp NOT NULL, task_version_id varchar(36) NULL, subject_line_txt varchar(256) NULL, segment_id varchar(36) NULL, 
      recipient_domain_nm varchar(100) NULL, program_id varchar(50) NULL, optout_type_nm varchar(50) NULL, occurrence_id varchar(36) NULL, 
      journey_occurrence_id varchar(36) NULL, imprint_id varchar(36) NULL, event_nm varchar(256) NULL, event_designed_id varchar(36) NULL, 
      email_address varchar(300) NULL, context_val varchar(256) NULL, audience_id varchar(36) NULL, analysis_group_id varchar(36) NULL, 
      aud_occurrence_id varchar(36) NULL, context_type_nm varchar(256) NULL, event_id varchar(36) NOT NULL, identity_id varchar(36) NULL, 
      journey_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, segment_version_id varchar(36) NULL, task_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EMAIL_OPTOUT_DETAILS, EMAIL_OPTOUT_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..email_reply (
      test_flg char(1) NULL, properties_map_doc varchar(4000) NULL, email_reply_dttm timestamp NULL, email_reply_dttm_tz timestamp NULL, 
      load_dttm timestamp NOT NULL, uri_txt varchar(1332) NULL, task_id varchar(36) NULL, subject_line_txt varchar(256) NULL, 
      segment_version_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, occurrence_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL, 
      imprint_id varchar(36) NULL, event_nm varchar(256) NULL, event_designed_id varchar(36) NULL, context_type_nm varchar(256) NULL, 
      audience_id varchar(36) NULL, analysis_group_id varchar(36) NULL, aud_occurrence_id varchar(36) NULL, channel_user_id varchar(300) NULL, 
      context_val varchar(256) NULL, event_id varchar(36) NOT NULL, identity_id varchar(36) NULL, journey_id varchar(36) NULL, 
      program_id varchar(50) NULL, recipient_domain_nm varchar(100) NULL, segment_id varchar(36) NULL, task_version_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EMAIL_REPLY, EMAIL_REPLY);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..email_send (
      test_flg char(1) NULL, properties_map_doc varchar(4000) NULL, load_dttm timestamp NOT NULL, email_send_dttm_tz timestamp NULL, 
      email_send_dttm timestamp NULL, task_version_id varchar(36) NULL, subject_line_txt varchar(256) NULL, segment_id varchar(36) NULL, 
      recipient_domain_nm varchar(100) NULL, program_id varchar(50) NULL, journey_id varchar(36) NULL, imprint_id varchar(36) NULL, 
      event_nm varchar(256) NULL, event_designed_id varchar(36) NULL, context_type_nm varchar(256) NULL, channel_user_id varchar(300) NULL, 
      audience_id varchar(36) NULL, analysis_group_id varchar(36) NULL, aud_occurrence_id varchar(36) NULL, context_val varchar(256) NULL, 
      event_id varchar(36) NOT NULL, identity_id varchar(36) NULL, imprint_url_txt varchar(1332) NULL, journey_occurrence_id varchar(36) NULL, 
      occurrence_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, segment_version_id varchar(36) NULL, task_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EMAIL_SEND, EMAIL_SEND);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..email_view (
      test_flg char(1) NULL, properties_map_doc varchar(4000) NULL, load_dttm timestamp NOT NULL, email_view_dttm timestamp NULL, 
      email_view_dttm_tz timestamp NULL, task_version_id varchar(36) NULL, task_id varchar(36) NULL, subject_line_txt varchar(256) NULL, 
      segment_version_id varchar(36) NULL, segment_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, recipient_domain_nm varchar(100) NULL, 
      program_id varchar(50) NULL, occurrence_id varchar(36) NULL, link_tracking_id varchar(4) NULL, link_tracking_group_txt varchar(256) NULL, 
      journey_occurrence_id varchar(36) NULL, imprint_id varchar(36) NULL, event_nm varchar(256) NULL, event_designed_id varchar(36) NULL, 
      context_type_nm varchar(256) NULL, audience_id varchar(36) NULL, analysis_group_id varchar(36) NULL, aud_occurrence_id varchar(36) NULL, 
      channel_user_id varchar(300) NULL, context_val varchar(256) NULL, event_id varchar(36) NOT NULL, identity_id varchar(36) NULL, 
      journey_id varchar(36) NULL, link_tracking_label_txt varchar(256) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EMAIL_VIEW, EMAIL_VIEW);
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
   EXECUTE (CREATE TABLE &dbschema..external_event (
      properties_map_doc varchar(4000) NULL, external_event_dttm_tz timestamp NULL, load_dttm timestamp NOT NULL, external_event_dttm timestamp NULL, 
      response_tracking_cd varchar(36) NULL, identity_id varchar(36) NULL, event_nm varchar(256) NULL, event_designed_id varchar(36) NULL, 
      context_type_nm varchar(256) NULL, channel_nm varchar(40) NULL, channel_user_id varchar(300) NULL, context_val varchar(256) NULL, 
      event_id varchar(36) NOT NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EXTERNAL_EVENT, EXTERNAL_EVENT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..fiscal_cc_budget (
      cc_obsolete_flg char(1) NULL, fin_accnt_obsolete_flg char(1) NULL, fp_obsolete_flg char(1) NULL, fp_start_dt date NULL, 
      fp_end_dt date NULL, cc_bdgt_invoiced_amt decimal(17,2) NULL, cc_bdgt_cmtmnt_outstanding_amt decimal(17,2) NULL, cc_bdgt_cmtmnt_invoice_amt decimal(17,2) NULL, 
      cc_bdgt_budget_amt decimal(17,2) NULL, cc_bdgt_amt decimal(17,2) NULL, cc_bdgt_committed_amt decimal(17,2) NULL, cc_bdgt_cmtmnt_overspent_amt decimal(17,2) NULL, 
      cc_bdgt_direct_invoice_amt decimal(17,2) NULL, cc_bdgt_cmtmnt_invoice_cnt integer NULL, last_modified_dttm timestamp NULL, created_dttm timestamp NULL, 
      load_dttm timestamp NULL, gen_ledger_cd varchar(128) NULL, last_modified_usernm varchar(128) NULL, fp_nm varchar(128) NULL, 
      fp_id varchar(128) NULL, fp_desc varchar(1332) NULL, fp_cls_ver varchar(128) NULL, fin_accnt_nm varchar(128) NULL, 
      cost_center_id varchar(128) NULL, cc_number varchar(128) NULL, cc_nm varchar(128) NULL, cc_bdgt_budget_desc varchar(1332) NULL, 
      cc_desc varchar(1332) NULL, cc_owner_usernm varchar(128) NULL, created_by_usernm varchar(128) NULL, fin_accnt_desc varchar(1332) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: FISCAL_CC_BUDGET, FISCAL_CC_BUDGET);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..form_details (
      submit_flg char(1) NULL, change_index_no integer NULL, attempt_index_cnt integer NULL, load_dttm timestamp NOT NULL, 
      form_field_detail_dttm_tz timestamp NULL, form_field_detail_dttm timestamp NULL, visit_id varchar(32) NULL, form_field_nm varchar(325) NULL, 
      event_source_cd varchar(100) NULL, detail_id varchar(32) NULL, attempt_status_cd varchar(42) NULL, event_id varchar(36) NOT NULL, 
      form_field_value varchar(2600) NULL, form_nm varchar(65) NULL, session_id_hex varchar(29) NULL, detail_id_hex varchar(32) NULL, 
      event_key_cd varchar(100) NULL, form_field_id varchar(325) NULL, identity_id varchar(36) NULL, session_id varchar(29) NULL, 
      visit_id_hex varchar(32) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: FORM_DETAILS, FORM_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..identity_attributes (
      processed_dttm timestamp NULL, entrytime timestamp NOT NULL, identity_id varchar(36) NULL, user_identifier_val varchar(5000) NOT NULL, 
      identifier_type_id varchar(36) NOT NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: IDENTITY_ATTRIBUTES, IDENTITY_ATTRIBUTES);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..identity_map (
      processed_dttm timestamp NULL, entrytime timestamp NULL, target_identity_id varchar(36) NULL, source_identity_id varchar(36) NOT NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: IDENTITY_MAP, IDENTITY_MAP);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..impression_delivered (
      control_group_flg char(1) NULL, product_qty_no integer NULL, properties_map_doc varchar(4000) NULL, impression_delivered_dttm_tz timestamp NULL, 
      impression_delivered_dttm timestamp NULL, load_dttm timestamp NOT NULL, spot_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, 
      rec_group_id varchar(3) NULL, product_nm varchar(128) NULL, message_id varchar(36) NULL, event_nm varchar(256) NULL, 
      detail_id_hex varchar(32) NULL, context_val varchar(256) NULL, audience_id varchar(36) NULL, channel_user_id varchar(300) NULL, 
      creative_id varchar(36) NULL, event_id varchar(36) NOT NULL, identity_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL, 
      message_version_id varchar(36) NULL, mobile_app_id varchar(40) NULL, product_sku_no varchar(100) NULL, reserved_1_txt varchar(100) NULL, 
      segment_version_id varchar(36) NULL, task_version_id varchar(36) NULL, visit_id_hex varchar(32) NULL, aud_occurrence_id varchar(36) NULL, 
      channel_nm varchar(40) NULL, context_type_nm varchar(256) NULL, creative_version_id varchar(36) NULL, event_designed_id varchar(36) NULL, 
      event_key_cd varchar(100) NULL, event_source_cd varchar(100) NULL, journey_id varchar(36) NULL, product_id varchar(130) NULL, 
      request_id varchar(36) NULL, reserved_2_txt varchar(100) NULL, segment_id varchar(36) NULL, session_id_hex varchar(29) NULL, 
      task_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: IMPRESSION_DELIVERED, IMPRESSION_DELIVERED);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..impression_spot_viewable (
      control_group_flg char(1) NULL, product_qty_no integer NULL, properties_map_doc varchar(4000) NULL, impression_viewable_dttm_tz timestamp NULL, 
      load_dttm timestamp NOT NULL, impression_viewable_dttm timestamp NULL, visit_id_hex varchar(32) NULL, session_id_hex varchar(29) NULL, 
      reserved_2_txt varchar(100) NULL, product_id varchar(128) NULL, message_id varchar(36) NULL, identity_id varchar(36) NULL, 
      event_id varchar(36) NOT NULL, creative_id varchar(36) NULL, channel_user_id varchar(300) NULL, analysis_group_id varchar(36) NULL, 
      audience_id varchar(36) NULL, context_val varchar(256) NULL, detail_id_hex varchar(32) NULL, event_nm varchar(256) NULL, 
      event_source_cd varchar(100) NULL, mobile_app_id varchar(40) NULL, rec_group_id varchar(3) NULL, request_id varchar(36) NULL, 
      segment_id varchar(36) NULL, task_id varchar(36) NULL, aud_occurrence_id varchar(36) NULL, channel_nm varchar(40) NULL, 
      context_type_nm varchar(256) NULL, creative_version_id varchar(36) NULL, event_designed_id varchar(36) NULL, event_key_cd varchar(100) NULL, 
      message_version_id varchar(36) NULL, occurrence_id varchar(36) NULL, product_nm varchar(128) NULL, product_sku_no varchar(100) NULL, 
      reserved_1_txt varchar(100) NULL, response_tracking_cd varchar(36) NULL, segment_version_id varchar(36) NULL, spot_id varchar(36) NULL, 
      task_version_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: IMPRESSION_SPOT_VIEWABLE, IMPRESSION_SPOT_VIEWABLE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..invoice_details (
      vendor_obsolete_flg char(1) NULL, invoice_amt decimal(17,2) NULL, vendor_amt decimal(17,2) NULL, reconcile_amt decimal(17,2) NULL, 
      last_modified_dttm timestamp NULL, invoice_reconciled_dttm timestamp NULL, payment_dttm timestamp NULL, load_dttm timestamp NULL, 
      created_dttm timestamp NULL, invoice_created_dttm timestamp NULL, vendor_nm varchar(128) NULL, planning_id varchar(128) NULL, 
      last_modified_usernm varchar(128) NULL, invoice_number varchar(128) NULL, cmtmnt_nm varchar(128) NULL, invoice_id varchar(128) NULL, 
      invoice_status varchar(64) NULL, vendor_currency_cd varchar(10) NULL, vendor_desc varchar(1332) NULL, cmtmnt_id varchar(128) NULL, 
      created_by_usernm varchar(128) NULL, invoice_desc varchar(1332) NULL, invoice_nm varchar(128) NULL, plan_currency_cd varchar(10) NULL, 
      planning_nm varchar(128) NULL, reconcile_note varchar(1332) NULL, vendor_id varchar(128) NULL, vendor_number varchar(128) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: INVOICE_DETAILS, INVOICE_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..invoice_line_items (
      item_alloc_unit bigint NULL, item_vend_alloc_unit bigint NULL, item_qty bigint NULL, vendor_obsolete_flg char(1) NULL, 
      cc_available_amt decimal(17,2) NULL, vendor_amt decimal(17,2) NULL, item_alloc_amt decimal(17,2) NULL, item_rate decimal(17,2) NULL, 
      reconcile_amt decimal(17,2) NULL, item_vend_alloc_amt decimal(17,2) NULL, invoice_amt decimal(17,2) NULL, cc_recon_alloc_amt decimal(17,2) NULL, 
      cc_allocated_amt decimal(17,2) NULL, item_number integer NULL, payment_dttm timestamp NULL, load_dttm timestamp NULL, 
      invoice_created_dttm timestamp NULL, invoice_reconciled_dttm timestamp NULL, last_modified_dttm timestamp NULL, created_dttm timestamp NULL, 
      vendor_number varchar(128) NULL, vendor_desc varchar(1332) NULL, vendor_currency_cd varchar(10) NULL, reconcile_note varchar(1332) NULL, 
      planning_nm varchar(128) NULL, item_nm varchar(128) NULL, invoice_nm varchar(128) NULL, invoice_id varchar(128) NULL, 
      invoice_desc varchar(1332) NULL, fin_acc_nm varchar(128) NULL, cost_center_id varchar(128) NULL, cc_nm varchar(128) NULL, 
      ccat_nm varchar(128) NULL, cmtmnt_id varchar(128) NULL, plan_currency_cd varchar(10) NULL, vendor_id varchar(128) NULL, 
      cc_desc varchar(1332) NULL, cc_owner_usernm varchar(128) NULL, cmtmnt_nm varchar(128) NULL, created_by_usernm varchar(128) NULL, 
      fin_acc_ccat_nm varchar(128) NULL, gen_ledger_cd varchar(128) NULL, invoice_number varchar(128) NULL, invoice_status varchar(64) NULL, 
      last_modified_usernm varchar(128) NULL, planning_id varchar(128) NULL, vendor_nm varchar(128) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: INVOICE_LINE_ITEMS, INVOICE_LINE_ITEMS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..invoice_line_items_ccbdgt (
      cc_obsolete_flg char(1) NULL, vendor_obsolete_flg char(1) NULL, fp_obsolete_flg char(1) NULL, fp_start_dt date NULL, 
      fp_end_dt date NULL, cc_bdgt_cmtmnt_overspent_amt decimal(17,2) NULL, reconcile_amt decimal(17,2) NULL, vendor_amt decimal(17,2) NULL, 
      item_alloc_amt decimal(17,2) NULL, cc_bdgt_cmtmnt_outstanding_amt decimal(17,2) NULL, invoice_amt decimal(17,2) NULL, item_rate decimal(17,2) NULL, 
      cc_recon_alloc_amt decimal(17,2) NULL, cc_bdgt_direct_invoice_amt decimal(17,2) NULL, cc_available_amt decimal(17,2) NULL, cc_bdgt_invoiced_amt decimal(17,2) NULL, 
      item_vend_alloc_amt decimal(17,2) NULL, cc_bdgt_committed_amt decimal(17,2) NULL, cc_bdgt_budget_amt decimal(17,2) NULL, cc_bdgt_cmtmnt_invoice_amt decimal(17,2) NULL, 
      cc_bdgt_amt decimal(17,2) NULL, cc_allocated_amt decimal(17,2) NULL, item_qty integer NULL, item_number integer NULL, 
      item_vend_alloc_unit integer NULL, item_alloc_unit integer NULL, cc_bdgt_cmtmnt_invoice_cnt integer NULL, created_dttm timestamp NULL, 
      invoice_created_dttm timestamp NULL, invoice_reconciled_dttm timestamp NULL, last_modified_dttm timestamp NULL, payment_dttm timestamp NULL, 
      load_dttm timestamp NULL, vendor_id varchar(128) NULL, reconcile_note varchar(1332) NULL, planning_nm varchar(128) NULL, 
      plan_currency_cd varchar(10) NULL, invoice_nm varchar(128) NULL, fp_nm varchar(128) NULL, fp_cls_ver varchar(128) NULL, 
      created_by_usernm varchar(128) NULL, ccat_nm varchar(128) NULL, cc_number varchar(128) NULL, cc_bdgt_budget_desc varchar(1332) NULL, 
      cc_desc varchar(1332) NULL, cc_owner_usernm varchar(128) NULL, cmtmnt_nm varchar(128) NULL, fin_acc_ccat_nm varchar(128) NULL, 
      invoice_desc varchar(1332) NULL, item_nm varchar(128) NULL, vendor_currency_cd varchar(10) NULL, vendor_number varchar(128) NULL, 
      cc_nm varchar(128) NULL, cmtmnt_id varchar(128) NULL, cost_center_id varchar(128) NULL, fin_acc_nm varchar(128) NULL, 
      fp_desc varchar(1332) NULL, fp_id varchar(128) NULL, gen_ledger_cd varchar(128) NULL, invoice_id varchar(128) NULL, 
      invoice_number varchar(128) NULL, invoice_status varchar(64) NULL, last_modified_usernm varchar(128) NULL, planning_id varchar(128) NULL, 
      vendor_desc varchar(1332) NULL, vendor_nm varchar(128) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: INVOICE_LINE_ITEMS_CCBDGT, INVOICE_LINE_ITEMS_CCBDGT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..in_app_failed (
      properties_map_doc varchar(4000) NULL, in_app_failed_dttm timestamp NULL, in_app_failed_dttm_tz timestamp NULL, load_dttm timestamp NOT NULL, 
      task_version_id varchar(36) NULL, segment_id varchar(36) NULL, message_id varchar(36) NULL, identity_id varchar(36) NULL, 
      error_message_txt varchar(1332) NULL, context_val varchar(256) NULL, channel_user_id varchar(300) NULL, context_type_nm varchar(256) NULL, 
      creative_version_id varchar(36) NULL, event_id varchar(36) NOT NULL, mobile_app_id varchar(40) NULL, reserved_2_txt varchar(100) NULL, 
      spot_id varchar(36) NULL, channel_nm varchar(40) NULL, creative_id varchar(36) NULL, error_cd varchar(256) NULL, 
      event_designed_id varchar(36) NULL, event_nm varchar(256) NULL, message_version_id varchar(36) NULL, occurrence_id varchar(36) NULL, 
      reserved_1_txt varchar(100) NULL, response_tracking_cd varchar(36) NULL, segment_version_id varchar(36) NULL, task_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: IN_APP_FAILED, IN_APP_FAILED);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..in_app_message (
      properties_map_doc varchar(4000) NULL, in_app_action_dttm_tz timestamp NULL, load_dttm timestamp NOT NULL, in_app_action_dttm timestamp NULL, 
      segment_version_id varchar(36) NULL, reserved_2_txt varchar(100) NULL, mobile_app_id varchar(40) NULL, event_id varchar(36) NOT NULL, 
      context_val varchar(256) NULL, channel_user_id varchar(300) NULL, creative_version_id varchar(36) NULL, identity_id varchar(36) NULL, 
      message_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, task_id varchar(36) NULL, channel_nm varchar(40) NULL, 
      context_type_nm varchar(256) NULL, creative_id varchar(36) NULL, event_designed_id varchar(36) NULL, event_nm varchar(256) NULL, 
      message_version_id varchar(36) NULL, occurrence_id varchar(36) NULL, reserved_1_txt varchar(100) NULL, reserved_3_txt varchar(100) NULL, 
      segment_id varchar(36) NULL, spot_id varchar(36) NULL, task_version_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: IN_APP_MESSAGE, IN_APP_MESSAGE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..in_app_send (
      properties_map_doc varchar(4000) NULL, load_dttm timestamp NOT NULL, in_app_send_dttm_tz timestamp NULL, in_app_send_dttm timestamp NULL, 
      task_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, occurrence_id varchar(36) NULL, message_id varchar(36) NULL, 
      event_nm varchar(256) NULL, creative_id varchar(36) NULL, channel_nm varchar(40) NULL, context_type_nm varchar(256) NULL, 
      creative_version_id varchar(36) NULL, event_designed_id varchar(36) NULL, message_version_id varchar(36) NULL, reserved_1_txt varchar(100) NULL, 
      segment_version_id varchar(36) NULL, channel_user_id varchar(300) NULL, context_val varchar(256) NULL, event_id varchar(36) NOT NULL, 
      identity_id varchar(36) NULL, mobile_app_id varchar(40) NULL, reserved_2_txt varchar(100) NULL, segment_id varchar(36) NULL, 
      spot_id varchar(36) NULL, task_version_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: IN_APP_SEND, IN_APP_SEND);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..in_app_targeting_request (
      eligibility_flg char(1) NULL, in_app_tgt_request_dttm timestamp NULL, load_dttm timestamp NOT NULL, in_app_tgt_request_dttm_tz timestamp NULL, 
      event_id varchar(36) NOT NULL, context_type_nm varchar(256) NULL, channel_nm varchar(40) NULL, event_designed_id varchar(36) NULL, 
      identity_id varchar(36) NULL, mobile_app_id varchar(40) NULL, channel_user_id varchar(300) NULL, context_val varchar(256) NULL, 
      event_nm varchar(256) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: IN_APP_TARGETING_REQUEST, IN_APP_TARGETING_REQUEST);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..journey_entry (
      entry_dttm timestamp NULL, entry_dttm_tz timestamp NULL, load_dttm timestamp NOT NULL, journey_occurrence_id varchar(36) NULL, 
      identity_id varchar(36) NULL, aud_occurrence_id varchar(36) NULL, audience_id varchar(36) NULL, context_type_nm varchar(256) NULL, 
      event_id varchar(36) NOT NULL, identity_type_val varchar(300) NULL, context_val varchar(256) NULL, event_nm varchar(256) NULL, 
      identity_type_nm varchar(100) NULL, journey_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: JOURNEY_ENTRY, JOURNEY_ENTRY);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..journey_exit (
      exit_dttm timestamp NULL, exit_dttm_tz timestamp NULL, load_dttm timestamp NOT NULL, last_node_id varchar(36) NULL, 
      identity_type_nm varchar(100) NULL, context_type_nm varchar(256) NULL, aud_occurrence_id varchar(36) NULL, event_id varchar(36) NOT NULL, 
      group_id varchar(36) NULL, journey_id varchar(36) NULL, reason_cd varchar(100) NULL, audience_id varchar(36) NULL, 
      context_val varchar(256) NULL, event_nm varchar(256) NULL, identity_id varchar(36) NULL, identity_type_val varchar(300) NULL, 
      journey_occurrence_id varchar(36) NULL, reason_txt varchar(1000) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: JOURNEY_EXIT, JOURNEY_EXIT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..journey_holdout (
      holdout_dttm_tz timestamp NULL, load_dttm timestamp NOT NULL, holdout_dttm timestamp NULL, journey_occurrence_id varchar(36) NULL, 
      journey_id varchar(36) NULL, identity_type_val varchar(300) NULL, identity_type_nm varchar(100) NULL, identity_id varchar(36) NULL, 
      event_nm varchar(256) NULL, event_id varchar(36) NOT NULL, context_val varchar(256) NULL, context_type_nm varchar(256) NULL, 
      audience_id varchar(36) NULL, aud_occurrence_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: JOURNEY_HOLDOUT, JOURNEY_HOLDOUT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..journey_node_entry (
      node_entry_dttm timestamp NULL, load_dttm timestamp NOT NULL, node_entry_dttm_tz timestamp NULL, node_type_nm varchar(256) NULL, 
      node_id varchar(36) NULL, previous_node_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL, journey_id varchar(36) NULL, 
      identity_type_val varchar(300) NULL, identity_type_nm varchar(100) NULL, identity_id varchar(36) NULL, group_id varchar(36) NULL, 
      event_nm varchar(256) NULL, event_id varchar(36) NOT NULL, context_val varchar(256) NULL, context_type_nm varchar(256) NULL, 
      audience_id varchar(36) NULL, aud_occurrence_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: JOURNEY_NODE_ENTRY, JOURNEY_NODE_ENTRY);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..journey_success (
      unit_qty integer NULL, success_val integer NULL, success_dttm timestamp NULL, load_dttm timestamp NOT NULL, 
      success_dttm_tz timestamp NULL, parent_event_designed_id varchar(36) NULL, journey_id varchar(36) NULL, identity_type_nm varchar(100) NULL, 
      group_id varchar(36) NULL, event_id varchar(36) NOT NULL, context_type_nm varchar(256) NULL, audience_id varchar(36) NULL, 
      aud_occurrence_id varchar(36) NULL, context_val varchar(256) NULL, event_nm varchar(256) NULL, identity_id varchar(36) NULL, 
      identity_type_val varchar(300) NULL, journey_occurrence_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: JOURNEY_SUCCESS, JOURNEY_SUCCESS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..journey_suppression (
      load_dttm timestamp NOT NULL, suppression_dttm timestamp NULL, suppression_dttm_tz timestamp NULL, reason_txt varchar(1000) NULL, 
      reason_cd varchar(100) NULL, journey_occurrence_id varchar(36) NULL, identity_type_val varchar(300) NULL, identity_type_nm varchar(100) NULL, 
      identity_id varchar(36) NULL, event_id varchar(36) NOT NULL, context_type_nm varchar(256) NULL, audience_id varchar(36) NULL, 
      aud_occurrence_id varchar(36) NULL, context_val varchar(256) NULL, event_nm varchar(256) NULL, journey_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: JOURNEY_SUPPRESSION, JOURNEY_SUPPRESSION);
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
   EXECUTE (CREATE TABLE &dbschema..md_activity (
      valid_to_dttm timestamp NULL, last_published_dttm timestamp NULL, valid_from_dttm timestamp NULL, business_context_id varchar(36) NULL, 
      activity_version_id varchar(36) NULL, activity_status_cd varchar(20) NULL, activity_id varchar(36) NULL, activity_desc varchar(1332) NULL, 
      activity_cd varchar(60) NULL, activity_category_nm varchar(100) NULL, activity_nm varchar(60) NULL, folder_path_nm varchar(256) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY, MD_ACTIVITY);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_activity_abtestpath (
      next_node_val varchar(4000) NULL, abtest_dist_pct char(3) NULL, control_flg char(1) NULL, valid_to_dttm timestamp NULL, 
      valid_from_dttm timestamp NULL, activity_version_id varchar(36) NULL, activity_status_cd varchar(20) NULL, activity_id varchar(36) NULL, 
      abtest_path_id varchar(36) NULL, abtest_path_nm varchar(50) NULL, activity_node_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_ABTESTPATH, MD_ACTIVITY_ABTESTPATH);
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
      last_published_dttm timestamp NULL, valid_from_dttm timestamp NULL, valid_to_dttm timestamp NULL, folder_path_nm varchar(256) NULL, 
      business_context_id varchar(36) NULL, activity_version_id varchar(36) NULL, activity_status_cd varchar(20) NULL, activity_nm varchar(60) NULL, 
      activity_id varchar(36) NULL, activity_desc varchar(1332) NULL, activity_cd varchar(60) NULL, activity_category_nm varchar(100) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_ALL, MD_ACTIVITY_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_activity_custom_prop (
      valid_to_dttm timestamp NULL, valid_from_dttm timestamp NULL, property_val varchar(1332) NULL, property_nm varchar(256) NULL, 
      property_datatype_cd varchar(256) NULL, activity_version_id varchar(36) NULL, activity_status_cd varchar(36) NULL, activity_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_CUSTOM_PROP, MD_ACTIVITY_CUSTOM_PROP);
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
   EXECUTE (CREATE TABLE &dbschema..md_activity_node (
      next_node_val varchar(4000) NULL, previous_node_val varchar(4000) NULL, wait_tm bigint NULL, end_node_flg char(1) NULL, 
      time_boxed_flg char(1) NULL, specific_wait_flg char(1) NULL, start_node_flg char(1) NULL, node_sequence_no integer NULL, 
      valid_from_dttm timestamp NULL, valid_to_dttm timestamp NULL, activity_version_id varchar(36) NULL, activity_status_cd varchar(20) NULL, 
      activity_node_type_nm varchar(100) NULL, activity_node_nm varchar(256) NULL, activity_node_id varchar(36) NULL, activity_id varchar(36) NULL, 
      abtest_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_NODE, MD_ACTIVITY_NODE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_activity_node_all (
      previous_node_val varchar(4000) NULL, next_node_val varchar(4000) NULL, wait_tm bigint NULL, time_boxed_flg char(1) NULL, 
      end_node_flg char(1) NULL, start_node_flg char(1) NULL, specific_wait_flg char(1) NULL, node_sequence_no integer NULL, 
      valid_to_dttm timestamp NULL, valid_from_dttm timestamp NULL, activity_version_id varchar(36) NULL, activity_status_cd varchar(20) NULL, 
      activity_node_type_nm varchar(100) NULL, activity_node_nm varchar(256) NULL, activity_node_id varchar(36) NULL, activity_id varchar(36) NULL, 
      abtest_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_NODE_ALL, MD_ACTIVITY_NODE_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_activity_x_activity_node (
      activity_version_id varchar(36) NULL, activity_status_cd varchar(20) NULL, activity_node_id varchar(36) NULL, activity_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_X_ACTIVITY_NODE, MD_ACTIVITY_X_ACTIVITY_NODE);
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
   EXECUTE (CREATE TABLE &dbschema..md_activity_x_task (
      task_version_id varchar(36) NULL, task_id varchar(36) NULL, activity_version_id varchar(36) NULL, activity_status_cd varchar(20) NULL, 
      activity_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_X_TASK, MD_ACTIVITY_X_TASK);
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
   EXECUTE (CREATE TABLE &dbschema..md_asset (
      valid_to_dttm timestamp NULL, valid_from_dttm timestamp NULL, last_published_dttm timestamp NULL, owner_nm varchar(256) NULL, 
      created_user_nm varchar(256) NULL, asset_version_id varchar(36) NULL, asset_type_nm varchar(40) NULL, asset_status_cd varchar(20) NULL, 
      asset_nm varchar(256) NULL, asset_id varchar(36) NULL, asset_desc varchar(1332) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ASSET, MD_ASSET);
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
   EXECUTE (CREATE TABLE &dbschema..md_audience (
      audience_schedule_flg char(1) NULL, audience_expiration_val integer NULL, update_dttm timestamp NULL, create_dttm timestamp NULL, 
      delete_dttm timestamp NULL, created_user_nm varchar(256) NULL, audience_source_nm varchar(100) NULL, audience_nm varchar(128) NULL, 
      audience_id varchar(36) NULL, audience_desc varchar(1332) NULL, audience_data_source_nm varchar(100) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_AUDIENCE, MD_AUDIENCE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_audience_occurrence (
      audience_size_val integer NULL, update_dttm timestamp NULL, end_tm timestamp NULL, start_tm timestamp NULL, 
      started_by_nm varchar(256) NULL, occurrence_type_nm varchar(100) NULL, execution_status_cd varchar(100) NULL, audience_id varchar(36) NULL, 
      aud_occurrence_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_AUDIENCE_OCCURRENCE, MD_AUDIENCE_OCCURRENCE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_audience_x_segment (
      segment_id varchar(36) NULL, audience_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_AUDIENCE_X_SEGMENT, MD_AUDIENCE_X_SEGMENT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_bu (
      bu_obsolete_flg char(1) NULL, last_modified_dttm timestamp NULL, created_dttm timestamp NULL, load_dttm timestamp NULL, 
      last_modified_usernm varchar(128) NULL, created_by_usernm varchar(128) NULL, bu_parentid varchar(128) NULL, bu_owner_usernm varchar(128) NULL, 
      bu_nm varchar(128) NULL, bu_id varchar(128) NULL, bu_desc varchar(1332) NULL, bu_currency_cd varchar(10) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_BU, MD_BU);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_business_context (
      valid_to_dttm timestamp NULL, valid_from_dttm timestamp NULL, last_published_dttm timestamp NULL, owner_nm varchar(256) NULL, 
      locked_information_map_nm varchar(40) NULL, information_map_nm varchar(40) NULL, created_user_nm varchar(256) NULL, business_context_version_id varchar(36) NULL, 
      business_context_status_cd varchar(20) NULL, business_context_src_cd varchar(40) NULL, business_context_nm varchar(256) NULL, business_context_id varchar(36) NULL, 
      business_context_desc varchar(1332) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_BUSINESS_CONTEXT, MD_BUSINESS_CONTEXT);
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
   EXECUTE (CREATE TABLE &dbschema..md_costcenter (
      cc_obsolete_flg char(1) NULL, load_dttm timestamp NULL, created_dttm timestamp NULL, last_modified_dttm timestamp NULL, 
      last_modified_usernm varchar(128) NULL, gen_ledger_cd varchar(128) NULL, fin_accnt_nm varchar(128) NULL, created_by_usernm varchar(128) NULL, 
      cost_center_id varchar(128) NULL, cc_owner_usernm varchar(128) NULL, cc_nm varchar(128) NULL, cc_desc varchar(1332) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_COSTCENTER, MD_COSTCENTER);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_cost_category (
      ccat_obsolete_flg char(1) NULL, load_dttm timestamp NULL, created_dttm timestamp NULL, last_modified_dttm timestamp NULL, 
      last_modified_usernm varchar(128) NULL, gen_ledger_cd varchar(128) NULL, fin_accnt_nm varchar(128) NULL, created_by_usernm varchar(128) NULL, 
      ccat_owner_usernm varchar(128) NULL, ccat_nm varchar(128) NULL, ccat_id varchar(128) NULL, ccat_desc varchar(1332) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_COST_CATEGORY, MD_COST_CATEGORY);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_creative (
      last_published_dttm timestamp NULL, valid_to_dttm timestamp NULL, valid_from_dttm timestamp NULL, recommender_template_nm varchar(60) NULL, 
      recommender_template_id varchar(36) NULL, owner_nm varchar(256) NULL, folder_path_nm varchar(256) NULL, creative_version_id varchar(36) NULL, 
      creative_type_nm varchar(40) NULL, creative_txt varchar(1500) NULL, creative_status_cd varchar(20) NULL, creative_nm varchar(60) NULL, 
      creative_id varchar(36) NULL, creative_desc varchar(256) NULL, creative_cd varchar(60) NULL, creative_category_nm varchar(100) NULL, 
      created_user_nm varchar(256) NULL, business_context_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_CREATIVE, MD_CREATIVE);
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
   EXECUTE (CREATE TABLE &dbschema..md_creative_custom_prop (
      valid_to_dttm timestamp NULL, valid_from_dttm timestamp NULL, property_val varchar(1332) NULL, property_nm varchar(256) NULL, 
      property_datatype_cd varchar(256) NULL, creative_version_id varchar(36) NULL, creative_status_cd varchar(36) NULL, creative_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_CREATIVE_CUSTOM_PROP, MD_CREATIVE_CUSTOM_PROP);
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
   EXECUTE (CREATE TABLE &dbschema..md_creative_x_asset (
      creative_version_id varchar(36) NULL, creative_status_cd varchar(20) NULL, creative_id varchar(36) NULL, asset_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_CREATIVE_X_ASSET, MD_CREATIVE_X_ASSET);
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
   EXECUTE (CREATE TABLE &dbschema..md_custattrib_table_values (
      is_obsolete_flg char(1) NULL, load_dttm timestamp NULL, last_modified_dttm timestamp NULL, created_dttm timestamp NULL, 
      table_val varchar(256) NULL, last_modified_usernm varchar(128) NULL, data_type varchar(32) NULL, data_formatter varchar(64) NULL, 
      created_by_usernm varchar(128) NULL, attr_nm varchar(128) NULL, attr_id varchar(128) NULL, attr_group_nm varchar(128) NULL, 
      attr_group_id varchar(128) NULL, attr_group_cd varchar(128) NULL, attr_cd varchar(128) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_CUSTATTRIB_TABLE_VALUES, MD_CUSTATTRIB_TABLE_VALUES);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_cust_attrib (
      is_obsolete_flg char(1) NULL, is_grid_flg char(1) NULL, load_dttm timestamp NULL, last_modified_dttm timestamp NULL, 
      created_dttm timestamp NULL, remote_pklist_tab_col varchar(128) NULL, last_modified_usernm varchar(128) NULL, data_type varchar(32) NULL, 
      data_formatter varchar(64) NULL, created_by_usernm varchar(128) NULL, attr_nm varchar(128) NULL, attr_id varchar(128) NULL, 
      attr_group_nm varchar(128) NULL, attr_group_id varchar(128) NULL, attr_group_cd varchar(128) NULL, attr_cd varchar(128) NULL, 
      associated_grid varchar(128) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_CUST_ATTRIB, MD_CUST_ATTRIB);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_dataview (
      include_external_flg char(1) NULL, include_internal_flg char(1) NULL, analytic_active_flg char(1) NULL, max_path_time_val integer NULL, 
      analytics_period_val integer NULL, max_path_length_val integer NULL, half_life_time_val integer NULL, last_published_dttm timestamp NULL, 
      valid_from_dttm timestamp NULL, valid_to_dttm timestamp NULL, selected_task_list varchar(1000) NULL, owner_nm varchar(256) NULL, 
      max_path_time_type_nm varchar(10) NULL, dataview_version_id varchar(36) NULL, dataview_status_cd varchar(20) NULL, dataview_nm varchar(60) NULL, 
      dataview_id varchar(36) NULL, dataview_desc varchar(1332) NULL, custom_recent_exclude_cd varchar(36) NULL, custom_recent_cd varchar(36) NULL, 
      created_user_nm varchar(256) NULL, analytics_period_type_nm varchar(10) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_DATAVIEW, MD_DATAVIEW);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_dataview_all (
      include_internal_flg char(1) NULL, analytic_active_flg char(1) NULL, include_external_flg char(1) NULL, max_path_length_val integer NULL, 
      half_life_time_val integer NULL, analytics_period_val integer NULL, max_path_time_val integer NULL, valid_to_dttm timestamp NULL, 
      valid_from_dttm timestamp NULL, last_published_dttm timestamp NULL, selected_task_list varchar(1000) NULL, owner_nm varchar(256) NULL, 
      max_path_time_type_nm varchar(10) NULL, dataview_version_id varchar(36) NULL, dataview_status_cd varchar(20) NULL, dataview_nm varchar(60) NULL, 
      dataview_id varchar(36) NULL, dataview_desc varchar(1332) NULL, custom_recent_exclude_cd varchar(36) NULL, custom_recent_cd varchar(36) NULL, 
      created_user_nm varchar(256) NULL, analytics_period_type_nm varchar(10) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_DATAVIEW_ALL, MD_DATAVIEW_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_dataview_x_event (
      event_id varchar(36) NULL, dataview_version_id varchar(36) NULL, dataview_status_cd varchar(20) NULL, dataview_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_DATAVIEW_X_EVENT, MD_DATAVIEW_X_EVENT);
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
   EXECUTE (CREATE TABLE &dbschema..md_event (
      valid_from_dttm timestamp NULL, last_published_dttm timestamp NULL, valid_to_dttm timestamp NULL, owner_nm varchar(256) NULL, 
      event_version_id varchar(36) NULL, event_type_nm varchar(40) NULL, event_subtype_nm varchar(100) NULL, event_status_cd varchar(20) NULL, 
      event_nm varchar(60) NULL, event_id varchar(36) NULL, event_desc varchar(1332) NULL, created_user_nm varchar(256) NULL, 
      channel_nm varchar(40) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_EVENT, MD_EVENT);
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
   EXECUTE (CREATE TABLE &dbschema..md_fiscal_period (
      fp_obsolete_flg char(1) NULL, fp_end_dt date NULL, fp_start_dt date NULL, last_modified_dttm timestamp NULL, 
      created_dttm timestamp NULL, load_dttm timestamp NULL, last_modified_usernm varchar(128) NULL, fp_nm varchar(128) NULL, 
      fp_id varchar(128) NULL, fp_desc varchar(1332) NULL, fp_cls_ver varchar(128) NULL, created_by_usernm varchar(128) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_FISCAL_PERIOD, MD_FISCAL_PERIOD);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_grid_attr_defn (
      attr_obsolete_flg char(1) NULL, grid_mandatory_flg char(1) NULL, grid_obsolete_flg char(1) NULL, attr_order_no integer NULL, 
      load_dttm timestamp NULL, last_modified_dttm timestamp NULL, created_dttm timestamp NULL, remote_pklist_tab_col varchar(128) NULL, 
      last_modified_usernm varchar(128) NULL, grid_nm varchar(128) NULL, grid_id varchar(128) NULL, grid_desc varchar(4000) NULL, 
      grid_cd varchar(128) NULL, data_type varchar(32) NULL, data_formatter varchar(64) NULL, created_by_usernm varchar(128) NULL, 
      attr_nm varchar(128) NULL, attr_id varchar(128) NULL, attr_group_nm varchar(128) NULL, attr_group_id varchar(128) NULL, 
      attr_group_cd varchar(128) NULL, attr_desc varchar(4000) NULL, attr_cd varchar(128) NULL, associated_grid varchar(128) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_GRID_ATTR_DEFN, MD_GRID_ATTR_DEFN);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_journey (
      control_group_flg char(1) NULL, target_goal_qty integer NULL, last_activated_dttm timestamp NULL, test_type_nm varchar(40) NULL, 
      target_goal_type_nm varchar(20) NULL, purpose_id varchar(36) NULL, journey_version_id varchar(36) NULL, journey_status_cd varchar(20) NULL, 
      journey_nm varchar(256) NULL, journey_id varchar(36) NULL, created_user_nm varchar(256) NULL, activated_user_nm varchar(256) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY, MD_JOURNEY);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_journey_all (
      control_group_flg char(1) NULL, target_goal_qty integer NULL, last_activated_dttm timestamp NULL, test_type_nm varchar(40) NULL, 
      target_goal_type_nm varchar(20) NULL, purpose_id varchar(36) NULL, journey_version_id varchar(36) NULL, journey_status_cd varchar(20) NULL, 
      journey_nm varchar(256) NULL, journey_id varchar(36) NULL, created_user_nm varchar(256) NULL, activated_user_nm varchar(256) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_ALL, MD_JOURNEY_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_journey_node (
      previous_node_id varchar(36) NULL, node_type varchar(36) NULL, node_nm varchar(100) NULL, next_node_id varchar(36) NULL, 
      journey_version_id varchar(36) NULL, journey_node_id varchar(36) NULL, journey_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_NODE, MD_JOURNEY_NODE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_journey_node_occurrence (
      num_of_contacts_entered integer NULL, end_dttm timestamp NULL, start_dttm timestamp NULL, journey_version_id varchar(36) NULL, 
      journey_occurrence_id varchar(36) NULL, journey_node_occurrence_id varchar(36) NULL, journey_node_id varchar(36) NULL, journey_id varchar(36) NULL, 
      group_id varchar(36) NULL, execution_status varchar(36) NULL, error_messages varchar(256) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_NODE_OCCURRENCE, MD_JOURNEY_NODE_OCCURRENCE);
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
   EXECUTE (CREATE TABLE &dbschema..md_journey_occurrence (
      num_of_contacts_entered integer NULL, num_of_contacts_suppressed integer NULL, journey_occurrence_num integer NULL, start_dttm timestamp NULL, 
      end_dttm timestamp NULL, started_by_nm varchar(128) NULL, occurrence_type_nm varchar(36) NULL, journey_version_id varchar(36) NULL, 
      journey_occurrence_id varchar(36) NULL, journey_id varchar(36) NULL, execution_status varchar(36) NULL, error_messages varchar(256) NULL, 
      aud_occurrence_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_OCCURRENCE, MD_JOURNEY_OCCURRENCE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_journey_x_audience (
      journey_version_id varchar(36) NULL, journey_node_id varchar(36) NULL, journey_id varchar(36) NULL, audience_id varchar(36) NULL, 
      aud_relationship_nm varchar(100) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_X_AUDIENCE, MD_JOURNEY_X_AUDIENCE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_journey_x_event (
      journey_version_id varchar(36) NULL, journey_node_id varchar(36) NULL, journey_id varchar(36) NULL, event_relationship_nm varchar(100) NULL, 
      event_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_X_EVENT, MD_JOURNEY_X_EVENT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_journey_x_task (
      task_version_id varchar(36) NULL, task_id varchar(36) NULL, journey_version_id varchar(36) NULL, journey_node_id varchar(36) NULL, 
      journey_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_X_TASK, MD_JOURNEY_X_TASK);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_message (
      valid_to_dttm timestamp NULL, valid_from_dttm timestamp NULL, last_published_dttm timestamp NULL, owner_nm varchar(256) NULL, 
      message_version_id varchar(36) NULL, message_type_nm varchar(40) NULL, message_status_cd varchar(20) NULL, message_nm varchar(60) NULL, 
      message_id varchar(36) NULL, message_desc varchar(1332) NULL, message_cd varchar(60) NULL, message_category_nm varchar(100) NULL, 
      folder_path_nm varchar(256) NULL, created_user_nm varchar(256) NULL, business_context_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_MESSAGE, MD_MESSAGE);
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
   EXECUTE (CREATE TABLE &dbschema..md_message_custom_prop (
      valid_from_dttm timestamp NULL, valid_to_dttm timestamp NULL, property_val varchar(1332) NULL, property_datatype_cd varchar(256) NULL, 
      message_status_cd varchar(36) NULL, message_id varchar(36) NULL, message_version_id varchar(36) NULL, property_nm varchar(256) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_MESSAGE_CUSTOM_PROP, MD_MESSAGE_CUSTOM_PROP);
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
   EXECUTE (CREATE TABLE &dbschema..md_message_x_creative (
      message_version_id varchar(36) NULL, message_id varchar(36) NULL, creative_id varchar(36) NULL, message_status_cd varchar(20) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_MESSAGE_X_CREATIVE, MD_MESSAGE_X_CREATIVE);
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
   EXECUTE (CREATE TABLE &dbschema..md_object_type (
      is_obsolete_flg char(1) NULL, created_dttm timestamp NULL, last_modified_dttm timestamp NULL, load_dttm timestamp NULL, 
      object_type varchar(64) NULL, object_category varchar(64) NULL, last_modified_usernm varchar(128) NULL, data_type varchar(32) NULL, 
      data_formatter varchar(64) NULL, attr_nm varchar(128) NULL, attr_id varchar(128) NULL, attr_group_nm varchar(128) NULL, 
      attr_group_cd varchar(128) NULL, attr_cd varchar(128) NULL, attr_group_id varchar(128) NULL, created_by_usernm varchar(128) NULL, 
      remote_pklist_tab_col varchar(128) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_OBJECT_TYPE, MD_OBJECT_TYPE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_occurrence (
      occurrence_no integer NULL, properties_map_doc varchar(4000) NULL, start_tm timestamp NULL, end_tm timestamp NULL, 
      started_by_nm varchar(100) NULL, occurrence_type_nm varchar(100) NULL, object_version_id varchar(36) NULL, object_id varchar(36) NULL, 
      execution_status_cd varchar(50) NULL, object_type_nm varchar(100) NULL, occurrence_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_OCCURRENCE, MD_OCCURRENCE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_picklist (
      is_obsolete_flg char(1) NULL, last_modified_dttm timestamp NULL, created_dttm timestamp NULL, load_dttm timestamp NULL, 
      plist_val varchar(256) NULL, plist_id varchar(128) NULL, plist_cd varchar(256) NULL, last_modified_usernm varchar(128) NULL, 
      created_by_usernm varchar(128) NULL, attr_id varchar(128) NULL, attr_group_id varchar(128) NULL, attr_cd varchar(128) NULL, 
      attr_group_nm varchar(128) NULL, attr_nm varchar(128) NULL, plist_desc varchar(1332) NULL, plist_nm varchar(128) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_PICKLIST, MD_PICKLIST);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_purpose (
      purpose_nm varchar(256) NULL, purpose_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_PURPOSE, MD_PURPOSE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_rtc (
      occurrence_no integer NULL, content_map_doc varchar(4000) NULL, rtc_dttm timestamp NULL, task_id varchar(36) NULL, 
      segment_id varchar(36) NULL, rtc_id varchar(36) NULL, occurrence_id varchar(36) NULL, segment_version_id varchar(36) NULL, 
      task_version_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_RTC, MD_RTC);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_segment (
      last_published_dttm timestamp NULL, valid_from_dttm timestamp NULL, valid_to_dttm timestamp NULL, segment_version_id varchar(36) NULL, 
      segment_status_cd varchar(20) NULL, segment_nm varchar(60) NULL, segment_map_id varchar(36) NULL, segment_id varchar(36) NULL, 
      segment_cd varchar(60) NULL, owner_nm varchar(256) NULL, folder_path_nm varchar(256) NULL, created_user_nm varchar(256) NULL, 
      business_context_id varchar(36) NULL, segment_category_nm varchar(100) NULL, segment_desc varchar(1332) NULL, segment_src_cd varchar(40) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT, MD_SEGMENT);
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
   EXECUTE (CREATE TABLE &dbschema..md_segment_custom_prop (
      valid_to_dttm timestamp NULL, valid_from_dttm timestamp NULL, segment_version_id varchar(36) NULL, segment_status_cd varchar(36) NULL, 
      property_val varchar(1332) NULL, property_datatype_cd varchar(256) NULL, property_nm varchar(256) NULL, segment_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_CUSTOM_PROP, MD_SEGMENT_CUSTOM_PROP);
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
   EXECUTE (CREATE TABLE &dbschema..md_segment_map (
      scheduled_flg char(1) NULL, recurrence_day_of_month_no integer NULL, valid_to_dttm timestamp NULL, last_published_dttm timestamp NULL, 
      valid_from_dttm timestamp NULL, rec_scheduled_end_dttm timestamp NULL, rec_scheduled_start_dttm timestamp NULL, scheduled_end_dttm timestamp NULL, 
      scheduled_start_dttm timestamp NULL, segment_map_version_id varchar(36) NULL, segment_map_src_cd varchar(10) NULL, segment_map_nm varchar(60) NULL, 
      segment_map_id varchar(36) NULL, segment_map_cd varchar(60) NULL, segment_map_category_nm varchar(100) NULL, recurrence_monthly_type_nm varchar(36) NULL, 
      recurrence_frequency_cd varchar(36) NULL, recurrence_day_of_wk_ordinal_no varchar(36) NULL, recurrence_day_of_week_txt varchar(100) NULL, rec_scheduled_start_tm varchar(20) NULL, 
      owner_nm varchar(256) NULL, folder_path_nm varchar(256) NULL, business_context_id varchar(36) NULL, created_user_nm varchar(256) NULL, 
      recurrence_days_of_week_txt varchar(100) NULL, segment_map_desc varchar(1332) NULL, segment_map_status_cd varchar(20) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_MAP, MD_SEGMENT_MAP);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_segment_map_all (
      scheduled_flg char(1) NULL, recurrence_day_of_month_no integer NULL, rec_scheduled_start_dttm timestamp NULL, valid_from_dttm timestamp NULL, 
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
   EXECUTE (CREATE TABLE &dbschema..md_segment_map_custom_prop (
      valid_from_dttm timestamp NULL, valid_to_dttm timestamp NULL, segment_map_status_cd varchar(36) NULL, property_val varchar(1332) NULL, 
      property_nm varchar(256) NULL, property_datatype_cd varchar(256) NULL, segment_map_id varchar(36) NULL, segment_map_version_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_MAP_CUSTOM_PROP, MD_SEGMENT_MAP_CUSTOM_PROP);
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
   EXECUTE (CREATE TABLE &dbschema..md_segment_map_x_segment (
      segment_version_id varchar(36) NULL, segment_map_status_cd varchar(20) NULL, segment_id varchar(36) NULL, segment_map_id varchar(36) NULL, 
      segment_map_version_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_MAP_X_SEGMENT, MD_SEGMENT_MAP_X_SEGMENT);
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
   EXECUTE (CREATE TABLE &dbschema..md_segment_test (
      test_enabled_flg char(1) NULL, stratified_sampling_flg char(1) NULL, test_pct decimal(5,2) NULL, test_cnt integer NULL, 
      test_type_nm varchar(10) NULL, test_sizing_type_nm varchar(65) NULL, test_nm varchar(65) NULL, test_cd varchar(60) NULL, 
      task_id varchar(36) NULL, stratified_samp_criteria_txt varchar(1024) NULL, task_version_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_TEST, MD_SEGMENT_TEST);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_segment_test_all (
      test_enabled_flg char(1) NULL, stratified_sampling_flg char(1) NULL, test_pct decimal(5,2) NULL, test_cnt integer NULL, 
      test_type_nm varchar(10) NULL, test_sizing_type_nm varchar(65) NULL, test_nm varchar(65) NULL, task_version_id varchar(36) NULL, 
      task_id varchar(36) NULL, stratified_samp_criteria_txt varchar(1024) NULL, test_cd varchar(60) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_TEST_ALL, MD_SEGMENT_TEST_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_segment_test_x_segment (
      task_version_id varchar(36) NULL, segment_id varchar(36) NULL, task_id varchar(36) NULL, test_cd varchar(60) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_TEST_X_SEGMENT, MD_SEGMENT_TEST_X_SEGMENT);
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
   EXECUTE (CREATE TABLE &dbschema..md_segment_x_event (
      segment_status_cd varchar(20) NULL, event_id varchar(36) NULL, segment_id varchar(36) NULL, segment_version_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_X_EVENT, MD_SEGMENT_X_EVENT);
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
   EXECUTE (CREATE TABLE &dbschema..md_spot (
      location_selector_flg char(1) NULL, multi_page_flg char(1) NULL, last_published_dttm timestamp NULL, valid_from_dttm timestamp NULL, 
      valid_to_dttm timestamp NULL, spot_width_val_no varchar(10) NULL, spot_type_nm varchar(40) NULL, spot_nm varchar(60) NULL, 
      spot_id varchar(36) NULL, spot_desc varchar(1332) NULL, owner_nm varchar(256) NULL, dimension_label_txt varchar(156) NULL, 
      channel_nm varchar(40) NULL, created_user_nm varchar(256) NULL, height_width_ratio_val_txt varchar(25) NULL, spot_height_val_no varchar(10) NULL, 
      spot_key varchar(40) NULL, spot_status_cd varchar(20) NULL, spot_version_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SPOT, MD_SPOT);
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
   EXECUTE (CREATE TABLE &dbschema..md_target_assist (
      use_targeting_flg char(1) NULL, threshold_type_nm char(30) NULL, percent_target_population_size integer NULL, last_modified_dttm timestamp NULL, 
      model_available_dttm timestamp NULL, task_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TARGET_ASSIST, MD_TARGET_ASSIST);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_task (
      activity_flg char(1) NULL, scheduled_flg char(1) NULL, export_template_flg char(1) NULL, rtdm_flg char(1) NULL, 
      use_modeling_flg char(1) NULL, recurring_schedule_flg char(1) NULL, segment_tests_flg char(1) NULL, impressions_life_time_cnt integer NULL, 
      test_duration integer NULL, limit_period_unit_cnt integer NULL, impressions_per_session_cnt integer NULL, display_priority_no integer NULL, 
      recurrence_day_of_month_no integer NULL, impressions_qty_period_cnt integer NULL, maximum_period_expression_cnt integer NULL, valid_from_dttm timestamp NULL, 
      scheduled_start_dttm timestamp NULL, last_published_dttm timestamp NULL, valid_to_dttm timestamp NULL, rec_scheduled_start_dttm timestamp NULL, 
      model_start_dttm timestamp NULL, scheduled_end_dttm timestamp NULL, rec_scheduled_end_dttm timestamp NULL, task_version_id varchar(36) NULL, 
      task_subtype_nm varchar(30) NULL, task_nm varchar(60) NULL, task_desc varchar(1332) NULL, task_cd varchar(60) NULL, 
      subject_line_txt varchar(1332) NULL, stratified_sampling_action_nm varchar(65) NULL, send_notification_locale_cd varchar(5) NULL, secondary_status varchar(40) NULL, 
      recurrence_frequency_cd varchar(36) NULL, recurrence_day_of_wk_ordinal_no varchar(36) NULL, recurrence_day_of_week_txt varchar(60) NULL, rec_scheduled_start_tm varchar(20) NULL, 
      period_type_nm varchar(36) NULL, owner_nm varchar(256) NULL, mobile_app_id varchar(60) NULL, folder_path_nm varchar(256) NULL, 
      delivery_config_type_nm varchar(36) NULL, control_group_action_nm varchar(65) NULL, business_context_id varchar(36) NULL, arbitration_method_cd varchar(36) NULL, 
      channel_nm varchar(40) NULL, created_user_nm varchar(256) NULL, mobile_app_nm varchar(60) NULL, recurrence_days_of_week_txt varchar(60) NULL, 
      recurrence_monthly_type_nm varchar(36) NULL, subject_line_source_nm varchar(100) NULL, task_category_nm varchar(100) NULL, task_delivery_type_nm varchar(60) NULL, 
      task_id varchar(36) NULL, task_status_cd varchar(20) NULL, task_type_nm varchar(40) NULL, template_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK, MD_TASK);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_task_all (
      use_modeling_flg char(1) NULL, activity_flg char(1) NULL, recurring_schedule_flg char(1) NULL, rtdm_flg char(1) NULL, 
      scheduled_flg char(1) NULL, export_template_flg char(1) NULL, segment_tests_flg char(1) NULL, recurrence_day_of_month_no integer NULL, 
      impressions_per_session_cnt integer NULL, test_duration integer NULL, display_priority_no integer NULL, limit_period_unit_cnt integer NULL, 
      maximum_period_expression_cnt integer NULL, impressions_qty_period_cnt integer NULL, impressions_life_time_cnt integer NULL, last_published_dttm timestamp NULL, 
      model_start_dttm timestamp NULL, scheduled_start_dttm timestamp NULL, rec_scheduled_end_dttm timestamp NULL, rec_scheduled_start_dttm timestamp NULL, 
      scheduled_end_dttm timestamp NULL, valid_from_dttm timestamp NULL, valid_to_dttm timestamp NULL, template_id varchar(36) NULL, 
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
   EXECUTE (CREATE TABLE &dbschema..md_task_custom_prop (
      valid_from_dttm timestamp NULL, valid_to_dttm timestamp NULL, task_status_cd varchar(36) NULL, task_id varchar(36) NULL, 
      property_val varchar(1332) NULL, property_datatype_nm varchar(256) NULL, property_nm varchar(256) NULL, task_version_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_CUSTOM_PROP, MD_TASK_CUSTOM_PROP);
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
   EXECUTE (CREATE TABLE &dbschema..md_task_x_audience (
      audience_id varchar(36) NULL, task_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_AUDIENCE, MD_TASK_X_AUDIENCE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_task_x_creative (
      variant_nm varchar(256) NULL, task_version_id varchar(36) NULL, task_id varchar(36) NULL, creative_id varchar(36) NULL, 
      arbitration_method_cd varchar(36) NULL, arbitration_method_val varchar(3) NULL, spot_id varchar(36) NULL, task_status_cd varchar(20) NULL, 
      variant_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_CREATIVE, MD_TASK_X_CREATIVE);
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
   EXECUTE (CREATE TABLE &dbschema..md_task_x_dataview (
      targeting_flg char(1) NULL, primary_metric_flg char(1) NULL, secondary_metric_flg char(1) NULL, task_version_id varchar(36) NULL, 
      task_id varchar(36) NULL, dataview_id varchar(36) NULL, task_status_cd varchar(20) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_DATAVIEW, MD_TASK_X_DATAVIEW);
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
   EXECUTE (CREATE TABLE &dbschema..md_task_x_event (
      targeting_flg char(1) NULL, primary_metric_flg char(1) NULL, secondary_metric_flg char(1) NULL, task_version_id varchar(36) NULL, 
      task_id varchar(36) NULL, event_id varchar(36) NULL, task_status_cd varchar(20) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_EVENT, MD_TASK_X_EVENT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_task_x_event_all (
      secondary_metric_flg char(1) NULL, targeting_flg char(1) NULL, primary_metric_flg char(1) NULL, task_status_cd varchar(20) NULL, 
      task_id varchar(36) NULL, event_id varchar(36) NULL, task_version_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_EVENT_ALL, MD_TASK_X_EVENT_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_task_x_message (
      task_version_id varchar(36) NULL, task_status_cd varchar(20) NULL, message_id varchar(36) NULL, task_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_MESSAGE, MD_TASK_X_MESSAGE);
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
   EXECUTE (CREATE TABLE &dbschema..md_task_x_segment (
      task_version_id varchar(36) NULL, task_status_cd varchar(20) NULL, segment_id varchar(36) NULL, task_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_SEGMENT, MD_TASK_X_SEGMENT);
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
   EXECUTE (CREATE TABLE &dbschema..md_task_x_spot (
      task_status_cd varchar(20) NULL, spot_id varchar(36) NULL, task_id varchar(36) NULL, task_version_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_SPOT, MD_TASK_X_SPOT);
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
   EXECUTE (CREATE TABLE &dbschema..md_task_x_variant (
      variant_type_nm varchar(100) NULL, variant_nm varchar(256) NULL, task_status_cd varchar(20) NULL, analysis_group_id varchar(36) NULL, 
      task_id varchar(36) NULL, task_version_id varchar(36) NULL, variant_source_nm varchar(100) NULL, variant_val varchar(1332) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_VARIANT, MD_TASK_X_VARIANT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_task_x_variant_all (
      variant_type_nm varchar(100) NULL, variant_nm varchar(256) NULL, task_status_cd varchar(20) NULL, analysis_group_id varchar(36) NULL, 
      task_id varchar(36) NULL, task_version_id varchar(36) NULL, variant_source_nm varchar(100) NULL, variant_val varchar(1332) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_VARIANT_ALL, MD_TASK_X_VARIANT_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_vendor (
      is_obsolete_flg char(1) NULL, last_modified_dttm timestamp NULL, load_dttm timestamp NULL, created_dttm timestamp NULL, 
      vendor_number varchar(128) NULL, vendor_nm varchar(128) NULL, vendor_desc varchar(1332) NULL, owner_usernm varchar(128) NULL, 
      last_modified_usernm varchar(128) NULL, created_by_usernm varchar(128) NULL, vendor_currency_cd varchar(10) NULL, vendor_id varchar(128) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_VENDOR, MD_VENDOR);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_wf_process_def (
      version_num bigint NULL, file_tobecatlgd_flg char(1) NULL, default_approval_flg char(1) NULL, buildin_template_flg char(1) NULL, 
      latest_version_flg char(1) NULL, last_modified_dttm timestamp NULL, load_dttm timestamp NULL, created_dttm timestamp NULL, 
      pdef_state varchar(128) NULL, pdef_nm varchar(128) NULL, pdef_id varchar(128) NULL, owner_usernm varchar(128) NULL, 
      last_modified_usernm varchar(128) NULL, engine_pdef_key varchar(128) NULL, engine_pdef_id varchar(128) NULL, created_by_usernm varchar(128) NULL, 
      associated_object_type varchar(128) NULL, pdef_desc varchar(1332) NULL, pdef_type varchar(128) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_WF_PROCESS_DEF, MD_WF_PROCESS_DEF);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_wf_process_def_attr_grp (
      load_dttm timestamp NULL, pdef_id varchar(128) NULL, attr_group_id varchar(128) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_WF_PROCESS_DEF_ATTR_GRP, MD_WF_PROCESS_DEF_ATTR_GRP);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_wf_process_def_categories (
      default_category_flg char(1) NULL, load_dttm timestamp NULL, pdef_id varchar(128) NULL, category_type varchar(128) NULL, 
      category_id varchar(128) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_WF_PROCESS_DEF_CATEGORIES, MD_WF_PROCESS_DEF_CATEGORIES);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_wf_process_def_tasks (
      default_duration_perassignee bigint NULL, file_enabled_flg char(1) NULL, is_sequential_flg char(1) NULL, outgoing_flow_flg char(1) NULL, 
      resp_enabled_flg char(1) NULL, show_workflowlink_flg char(1) NULL, ciobject_enabled_flg char(1) NULL, file_mandatory_flg char(1) NULL, 
      url_enabled_flg char(1) NULL, multiple_asgnsuprt_flg char(1) NULL, comment_mandatory_flg char(1) NULL, comment_enabled_flg char(1) NULL, 
      resp_file_enabled_flg char(1) NULL, res_mandatory_flg char(1) NULL, show_sourceitemlink_flg char(1) NULL, load_dttm timestamp NULL, 
      task_type varchar(128) NULL, task_subtype varchar(128) NULL, task_instruction varchar(128) NULL, task_desc varchar(1332) NULL, 
      source_item_field varchar(128) NULL, predecessor_task_id varchar(128) NULL, pdef_id varchar(128) NULL, item_approval_state varchar(128) NULL, 
      assignee_type varchar(128) NULL, task_id varchar(128) NULL, task_nm varchar(128) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_WF_PROCESS_DEF_TASKS, MD_WF_PROCESS_DEF_TASKS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..md_wf_process_def_task_assg (
      load_dttm timestamp NULL, pdef_id varchar(128) NULL, assignee_type varchar(128) NULL, assignee_id varchar(128) NULL, 
      assignee_duration varchar(128) NULL, assignee_instruction varchar(128) NULL, task_id varchar(128) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_WF_PROCESS_DEF_TASK_ASSG, MD_WF_PROCESS_DEF_TASK_ASSG);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..media_activity_details (
      action_dttm timestamp NULL, action_dttm_tz timestamp NULL, load_dttm timestamp NOT NULL, playhead_position varchar(50) NULL, 
      media_nm varchar(260) NULL, event_id varchar(36) NOT NULL, detail_id varchar(32) NULL, action varchar(50) NULL, 
      detail_id_hex varchar(32) NULL, media_uri_txt varchar(2024) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MEDIA_ACTIVITY_DETAILS, MEDIA_ACTIVITY_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..media_details (
      media_duration_secs decimal(11,3) NULL, load_dttm timestamp NOT NULL, play_start_dttm_tz timestamp NULL, play_start_dttm timestamp NULL, 
      visit_id_hex varchar(32) NULL, visit_id varchar(32) NULL, session_id_hex varchar(29) NULL, session_id varchar(29) NULL, 
      media_uri_txt varchar(2024) NULL, media_player_nm varchar(30) NULL, media_nm varchar(260) NULL, identity_id varchar(36) NULL, 
      event_key_cd varchar(100) NULL, detail_id_hex varchar(32) NULL, detail_id varchar(32) NULL, event_id varchar(36) NOT NULL, 
      event_source_cd varchar(100) NULL, media_player_version_txt varchar(20) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MEDIA_DETAILS, MEDIA_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..media_details_ext (
      media_display_duration_secs decimal(11,3) NULL, view_duration_secs decimal(11,3) NULL, end_tm decimal(11,3) NULL, start_tm decimal(11,3) NULL, 
      exit_point_secs decimal(11,3) NULL, max_play_secs decimal(11,3) NULL, interaction_cnt integer NULL, play_end_dttm timestamp NULL, 
      play_end_dttm_tz timestamp NULL, load_dttm timestamp NOT NULL, media_uri_txt varchar(2024) NULL, media_nm varchar(260) NULL, 
      event_id varchar(36) NOT NULL, detail_id_hex varchar(32) NULL, detail_id varchar(32) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MEDIA_DETAILS_EXT, MEDIA_DETAILS_EXT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..mobile_focus_defocus (
      action_dttm_tz timestamp NULL, action_dttm timestamp NULL, load_dttm timestamp NOT NULL, visit_id_hex varchar(32) NULL, 
      session_id_hex varchar(29) NULL, reserved_1_txt varchar(100) NULL, mobile_app_id varchar(40) NULL, identity_id varchar(36) NULL, 
      event_nm varchar(256) NULL, event_designed_id varchar(36) NULL, detail_id_hex varchar(32) NULL, channel_user_id varchar(300) NULL, 
      event_id varchar(36) NOT NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MOBILE_FOCUS_DEFOCUS, MOBILE_FOCUS_DEFOCUS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..mobile_spots (
      action_dttm_tz timestamp NULL, action_dttm timestamp NULL, load_dttm timestamp NOT NULL, visit_id_hex varchar(32) NULL, 
      spot_id varchar(36) NULL, session_id_hex varchar(29) NULL, mobile_app_id varchar(40) NULL, event_nm varchar(256) NULL, 
      event_designed_id varchar(36) NULL, detail_id_hex varchar(32) NULL, creative_id varchar(36) NULL, context_type_nm varchar(256) NULL, 
      channel_user_id varchar(300) NULL, context_val varchar(256) NULL, event_id varchar(36) NOT NULL, identity_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MOBILE_SPOTS, MOBILE_SPOTS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..monthly_usage (
      api_usage_str varchar(4000) NULL, bc_subjcnt_str varchar(4000) NULL, customer_profiles_processed_str varchar(4000) NULL, web_impr_cnt bigint NULL, 
      web_sesn_cnt bigint NULL, mob_sesn_cnt bigint NULL, email_preview_cnt bigint NULL, outbound_api_cnt bigint NULL, 
      facebook_ads_cnt bigint NULL, mobile_push_cnt bigint NULL, google_ads_cnt bigint NULL, audience_usage_cnt bigint NULL, 
      plan_users_cnt bigint NULL, email_send_cnt bigint NULL, linkedin_ads_cnt bigint NULL, dm_destinations_total_row_cnt bigint NULL, 
      mob_impr_cnt bigint NULL, dm_destinations_total_id_cnt bigint NULL, mobile_in_app_msg_cnt bigint NULL, asset_size decimal(17,2) NULL, 
      db_size decimal(17,2) NULL, admin_user_cnt integer NULL, event_month varchar(36) NOT NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MONTHLY_USAGE, MONTHLY_USAGE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..notification_failed (
      properties_map_doc varchar(4000) NULL, notification_failed_dttm timestamp NULL, notification_failed_dttm_tz timestamp NULL, load_dttm timestamp NOT NULL, 
      task_id varchar(36) NULL, segment_version_id varchar(36) NULL, segment_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, 
      occurrence_id varchar(36) NULL, message_version_id varchar(36) NULL, journey_id varchar(36) NULL, event_designed_id varchar(36) NULL, 
      creative_id varchar(36) NULL, channel_user_id varchar(300) NULL, channel_nm varchar(40) NULL, aud_occurrence_id varchar(36) NULL, 
      context_type_nm varchar(256) NULL, error_cd varchar(256) NULL, event_id varchar(36) NOT NULL, event_nm varchar(256) NULL, 
      message_id varchar(36) NULL, mobile_app_id varchar(40) NULL, reserved_1_txt varchar(100) NULL, audience_id varchar(36) NULL, 
      context_val varchar(256) NULL, creative_version_id varchar(36) NULL, error_message_txt varchar(1332) NULL, identity_id varchar(36) NULL, 
      journey_occurrence_id varchar(36) NULL, reserved_2_txt varchar(100) NULL, spot_id varchar(36) NULL, task_version_id varchar(36) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: NOTIFICATION_FAILED, NOTIFICATION_FAILED);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..notification_opened (
      properties_map_doc varchar(4000) NULL, load_dttm timestamp NOT NULL, notification_opened_dttm_tz timestamp NULL, notification_opened_dttm timestamp NULL, 
      task_version_id varchar(36) NULL, segment_version_id varchar(36) NULL, segment_id varchar(36) NULL, reserved_1_txt varchar(100) NULL, 
      message_id varchar(36) NULL, identity_id varchar(36) NULL, event_nm varchar(256) NULL, creative_id varchar(36) NULL, 
      channel_user_id varchar(300) NULL, channel_nm varchar(40) NULL, aud_occurrence_id varchar(36) NULL, context_type_nm varchar(256) NULL, 
      event_designed_id varchar(36) NULL, journey_id varchar(36) NULL, message_version_id varchar(36) NULL, occurrence_id varchar(36) NULL, 
      reserved_3_txt varchar(100) NULL, spot_id varchar(36) NULL, audience_id varchar(36) NULL, context_val varchar(256) NULL, 
      creative_version_id varchar(36) NULL, event_id varchar(36) NOT NULL, journey_occurrence_id varchar(36) NULL, mobile_app_id varchar(40) NULL, 
      reserved_2_txt varchar(100) NULL, response_tracking_cd varchar(36) NULL, task_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: NOTIFICATION_OPENED, NOTIFICATION_OPENED);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..notification_send (
      properties_map_doc varchar(4000) NULL, load_dttm timestamp NOT NULL, notification_send_dttm_tz timestamp NULL, notification_send_dttm timestamp NULL, 
      task_id varchar(36) NULL, spot_id varchar(36) NULL, reserved_2_txt varchar(100) NULL, occurrence_id varchar(36) NULL, 
      message_id varchar(36) NULL, identity_id varchar(36) NULL, creative_version_id varchar(36) NULL, channel_user_id varchar(300) NULL, 
      audience_id varchar(36) NULL, context_val varchar(256) NULL, event_id varchar(36) NOT NULL, journey_id varchar(36) NULL, 
      journey_occurrence_id varchar(36) NULL, mobile_app_id varchar(40) NULL, reserved_1_txt varchar(100) NULL, segment_id varchar(36) NULL, 
      task_version_id varchar(36) NULL, aud_occurrence_id varchar(36) NULL, channel_nm varchar(40) NULL, context_type_nm varchar(256) NULL, 
      creative_id varchar(36) NULL, event_designed_id varchar(36) NULL, event_nm varchar(256) NULL, message_version_id varchar(36) NULL, 
      response_tracking_cd varchar(36) NULL, segment_version_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: NOTIFICATION_SEND, NOTIFICATION_SEND);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..notification_targeting_request (
      eligibility_flg char(1) NULL, notification_tgt_req_dttm timestamp NULL, load_dttm timestamp NOT NULL, notification_tgt_req_dttm_tz timestamp NULL, 
      task_id varchar(36) NULL, mobile_app_id varchar(40) NULL, event_nm varchar(256) NULL, context_val varchar(256) NULL, 
      audience_id varchar(36) NULL, channel_user_id varchar(300) NULL, event_designed_id varchar(36) NULL, journey_id varchar(36) NULL, 
      aud_occurrence_id varchar(36) NULL, channel_nm varchar(40) NULL, context_type_nm varchar(256) NULL, event_id varchar(36) NOT NULL, 
      identity_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: NOTIFICATION_TARGETING_REQUEST, NOTIFICATION_TARGETING_REQUEST);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..order_details (
      unit_price_amt decimal(17,2) NULL, quantity_amt integer NULL, properties_map_doc varchar(4000) NULL, load_dttm timestamp NOT NULL, 
      activity_dttm timestamp NULL, activity_dttm_tz timestamp NULL, visit_id varchar(32) NULL, session_id varchar(29) NULL, 
      record_type varchar(15) NULL, product_id varchar(130) NULL, mobile_app_id varchar(40) NULL, event_nm varchar(256) NULL, 
      event_key_cd varchar(100) NULL, detail_id varchar(32) NULL, cart_id varchar(42) NULL, availability_message_txt varchar(650) NULL, 
      channel_nm varchar(40) NULL, event_designed_id varchar(36) NULL, event_source_cd varchar(100) NULL, order_id varchar(42) NULL, 
      product_nm varchar(130) NULL, product_sku varchar(100) NULL, reserved_1_txt varchar(100) NULL, session_id_hex varchar(29) NULL, 
      shipping_message_txt varchar(650) NULL, cart_nm varchar(100) NULL, currency_cd varchar(6) NULL, detail_id_hex varchar(32) NULL, 
      event_id varchar(36) NOT NULL, identity_id varchar(36) NULL, product_group_nm varchar(130) NULL, saving_message_txt varchar(650) NULL, 
      visit_id_hex varchar(32) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ORDER_DETAILS, ORDER_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..order_summary (
      total_price_amt decimal(17,2) NULL, shipping_amt decimal(17,2) NULL, total_tax_amt decimal(17,2) NULL, total_unit_qty integer NULL, 
      properties_map_doc varchar(4000) NULL, load_dttm timestamp NOT NULL, activity_dttm_tz timestamp NULL, activity_dttm timestamp NULL, 
      visit_id varchar(32) NULL, shipping_postal_cd varchar(10) NULL, session_id_hex varchar(29) NULL, payment_type_desc varchar(42) NULL, 
      identity_id varchar(36) NULL, event_id varchar(36) NOT NULL, delivery_type_desc varchar(42) NULL, cart_id varchar(42) NULL, 
      billing_city_nm varchar(390) NULL, billing_postal_cd varchar(10) NULL, channel_nm varchar(40) NULL, detail_id_hex varchar(32) NULL, 
      event_nm varchar(256) NULL, mobile_app_id varchar(40) NULL, record_type varchar(15) NULL, shipping_city_nm varchar(390) NULL, 
      visit_id_hex varchar(32) NULL, billing_country_nm varchar(85) NULL, billing_state_region_cd varchar(256) NULL, cart_nm varchar(100) NULL, 
      currency_cd varchar(6) NULL, detail_id varchar(32) NULL, event_designed_id varchar(36) NULL, event_key_cd varchar(100) NULL, 
      event_source_cd varchar(100) NULL, order_id varchar(42) NULL, session_id varchar(29) NULL, shipping_country_nm varchar(85) NULL, 
      shipping_state_region_cd varchar(256) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ORDER_SUMMARY, ORDER_SUMMARY);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..outbound_system (
      properties_map_doc varchar(4000) NULL, outbound_system_dttm_tz timestamp NULL, outbound_system_dttm timestamp NULL, load_dttm timestamp NOT NULL, 
      visit_id_hex varchar(32) NULL, session_id_hex varchar(29) NULL, reserved_2_txt varchar(100) NULL, reserved_1_txt varchar(100) NULL, 
      parent_event_id varchar(36) NULL, message_version_id varchar(36) NULL, journey_id varchar(36) NULL, event_designed_id varchar(36) NULL, 
      context_val varchar(256) NULL, audience_id varchar(36) NULL, channel_nm varchar(40) NULL, channel_user_id varchar(300) NULL, 
      creative_id varchar(36) NULL, creative_version_id varchar(36) NULL, event_nm varchar(256) NULL, message_id varchar(36) NULL, 
      mobile_app_id varchar(40) NULL, occurrence_id varchar(36) NULL, segment_id varchar(36) NULL, task_id varchar(36) NULL, 
      aud_occurrence_id varchar(36) NULL, context_type_nm varchar(256) NULL, detail_id_hex varchar(32) NULL, event_id varchar(36) NOT NULL, 
      identity_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, segment_version_id varchar(36) NULL, 
      spot_id varchar(36) NULL, task_version_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: OUTBOUND_SYSTEM, OUTBOUND_SYSTEM);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..page_details (
      session_dt_tz date NULL, session_dt date NULL, page_load_sec_cnt integer NULL, page_complete_sec_cnt integer NULL, 
      bytes_sent_cnt integer NULL, detail_dttm_tz timestamp NULL, load_dttm timestamp NOT NULL, detail_dttm timestamp NULL, 
      url_domain varchar(215) NULL, session_id_hex varchar(29) NULL, session_id varchar(29) NULL, page_url_txt varchar(1332) NULL, 
      mobile_app_id varchar(40) NULL, event_key_cd varchar(100) NULL, detail_id_hex varchar(32) NULL, detail_id varchar(32) NULL, 
      class8_id varchar(650) NULL, class4_id varchar(650) NULL, class15_id varchar(650) NULL, class12_id varchar(650) NULL, 
      class11_id varchar(650) NULL, channel_nm varchar(40) NULL, class13_id varchar(650) NULL, class2_id varchar(650) NULL, 
      class6_id varchar(650) NULL, domain_nm varchar(165) NULL, event_source_cd varchar(100) NULL, page_desc varchar(1332) NULL, 
      protocol_nm varchar(8) NULL, visit_id varchar(32) NULL, visit_id_hex varchar(32) NULL, class10_id varchar(650) NULL, 
      class14_id varchar(650) NULL, class1_id varchar(650) NULL, class3_id varchar(650) NULL, class5_id varchar(650) NULL, 
      class7_id varchar(650) NULL, class9_id varchar(650) NULL, event_id varchar(36) NOT NULL, event_nm varchar(256) NULL, 
      identity_id varchar(36) NULL, referrer_url_txt varchar(1332) NULL, window_size_txt varchar(20) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: PAGE_DETAILS, PAGE_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..page_details_ext (
      active_sec_spent_on_page_cnt integer NULL, seconds_spent_on_page_cnt integer NULL, load_dttm timestamp NOT NULL, session_id varchar(29) NOT NULL, 
      detail_id varchar(32) NOT NULL, detail_id_hex varchar(32) NULL, session_id_hex varchar(29) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: PAGE_DETAILS_EXT, PAGE_DETAILS_EXT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..page_errors (
      in_page_error_dttm timestamp NULL, in_page_error_dttm_tz timestamp NULL, load_dttm timestamp NOT NULL, visit_id_hex varchar(32) NULL, 
      session_id varchar(29) NULL, identity_id varchar(36) NULL, error_location_txt varchar(41) NULL, detail_id_hex varchar(32) NULL, 
      event_id varchar(36) NOT NULL, in_page_error_txt varchar(260) NULL, session_id_hex varchar(29) NULL, detail_id varchar(32) NULL, 
      event_source_cd varchar(100) NULL, visit_id varchar(32) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: PAGE_ERRORS, PAGE_ERRORS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..planning_hierarchy_defn (
      level_no integer NULL, load_dttm timestamp NULL, created_dttm timestamp NULL, last_modified_dttm timestamp NULL, 
      last_modified_usernm varchar(128) NULL, hier_defn_nm varchar(128) NULL, hier_defn_id varchar(128) NULL, hier_defn_desc varchar(1332) NULL, 
      hier_defn_subtype varchar(128) NULL, level_desc varchar(1332) NULL, created_by_usernm varchar(128) NULL, hier_defn_type varchar(128) NULL, 
      level_nm varchar(128) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: PLANNING_HIERARCHY_DEFN, PLANNING_HIERARCHY_DEFN);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..planning_info (
      bu_obsolete_flg char(1) NULL, reserved_budget_same_flg char(1) NULL, alloc_budget decimal(17,2) NULL, rolledup_budget decimal(17,2) NULL, 
      tot_invoiced decimal(17,2) NULL, tot_expenses decimal(17,2) NULL, tot_cmtmnt_outstanding decimal(17,2) NULL, tot_committed decimal(17,2) NULL, 
      available_budget decimal(17,2) NULL, tot_cmtmnt_overspent decimal(17,2) NULL, reserved_budget decimal(17,2) NULL, total_budget decimal(17,2) NULL, 
      created_dttm timestamp NULL, last_modified_dttm timestamp NULL, planned_start_dttm timestamp NULL, load_dttm timestamp NULL, 
      planned_end_dttm timestamp NULL, task_id varchar(128) NULL, planning_owner_usernm varchar(128) NULL, planning_level_no varchar(10) NULL, 
      planning_desc varchar(1332) NULL, parent_id varchar(128) NULL, lev6_nm varchar(128) NULL, lev2_nm varchar(128) NULL, 
      last_modified_usernm varchar(128) NULL, hier_defn_id varchar(128) NULL, currency_cd varchar(10) NULL, bu_nm varchar(128) NULL, 
      bu_currency_cd varchar(10) NULL, activity_nm varchar(128) NULL, activity_desc varchar(1332) NULL, all_msgs varchar(4000) NULL, 
      bu_desc varchar(1332) NULL, category_nm varchar(128) NULL, hier_defn_nodeid varchar(128) NULL, lev10_nm varchar(128) NULL, 
      lev3_nm varchar(128) NULL, lev4_nm varchar(128) NULL, lev7_nm varchar(128) NULL, lev8_nm varchar(128) NULL, 
      parent_nm varchar(128) NULL, planning_id varchar(128) NULL, planning_level_type varchar(32) NULL, planning_nm varchar(128) NULL, 
      planning_type varchar(32) NULL, task_channel varchar(64) NULL, task_status varchar(64) NULL, activity_id varchar(128) NULL, 
      activity_status varchar(128) NULL, bu_id varchar(128) NULL, created_by_usernm varchar(128) NULL, lev1_nm varchar(128) NULL, 
      lev5_nm varchar(128) NULL, lev9_nm varchar(128) NULL, planning_item_path varchar(4000) NULL, planning_number varchar(128) NULL, 
      planning_status varchar(32) NULL, task_desc varchar(1332) NULL, task_nm varchar(128) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: PLANNING_INFO, PLANNING_INFO);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..planning_info_custom_prop (
      attr_val varchar(4000) NULL, is_obsolete_flg char(1) NULL, is_grid_flg char(1) NULL, last_modified_dttm timestamp NULL, 
      created_dttm timestamp NULL, load_dttm timestamp NULL, planning_id varchar(128) NULL, last_modified_usernm varchar(128) NULL, 
      created_by_usernm varchar(128) NULL, attr_group_nm varchar(128) NULL, attr_cd varchar(128) NULL, attr_group_cd varchar(128) NULL, 
      attr_id varchar(128) NULL, attr_nm varchar(128) NULL, data_formatter varchar(64) NULL, remote_pklist_tab_col varchar(128) NULL, 
      attr_group_id varchar(128) NULL, data_type varchar(32) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: PLANNING_INFO_CUSTOM_PROP, PLANNING_INFO_CUSTOM_PROP);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..product_views (
      price_val decimal(17,2) NULL, properties_map_doc varchar(4000) NULL, action_dttm_tz timestamp NULL, load_dttm timestamp NOT NULL, 
      action_dttm timestamp NULL, visit_id_hex varchar(32) NULL, visit_id varchar(32) NULL, saving_message_txt varchar(650) NULL, 
      product_id varchar(130) NULL, mobile_app_id varchar(40) NULL, event_nm varchar(256) NULL, event_key_cd varchar(100) NULL, 
      detail_id varchar(32) NULL, availability_message_txt varchar(650) NULL, channel_nm varchar(40) NULL, event_designed_id varchar(36) NULL, 
      event_source_cd varchar(100) NULL, product_group_nm varchar(130) NULL, product_sku varchar(100) NULL, session_id_hex varchar(29) NULL, 
      currency_cd varchar(6) NULL, detail_id_hex varchar(32) NULL, event_id varchar(36) NOT NULL, identity_id varchar(36) NULL, 
      product_nm varchar(130) NULL, session_id varchar(29) NULL, shipping_message_txt varchar(650) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: PRODUCT_VIEWS, PRODUCT_VIEWS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..promotion_displayed (
      derived_display_flg char(1) NULL, promotion_number integer NULL, properties_map_doc varchar(4000) NULL, display_dttm_tz timestamp NULL, 
      load_dttm timestamp NOT NULL, display_dttm timestamp NULL, session_id_hex varchar(29) NULL, promotion_tracking_cd varchar(65) NULL, 
      promotion_nm varchar(260) NULL, promotion_creative_nm varchar(260) NULL, event_source_cd varchar(100) NULL, event_designed_id varchar(36) NULL, 
      detail_id varchar(32) NULL, channel_nm varchar(40) NULL, detail_id_hex varchar(32) NULL, event_key_cd varchar(100) NULL, 
      mobile_app_id varchar(40) NULL, promotion_placement_nm varchar(260) NULL, session_id varchar(29) NULL, visit_id_hex varchar(32) NULL, 
      event_id varchar(36) NOT NULL, event_nm varchar(256) NULL, identity_id varchar(36) NULL, promotion_type_nm varchar(65) NULL, 
      visit_id varchar(32) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: PROMOTION_DISPLAYED, PROMOTION_DISPLAYED);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..promotion_used (
      promotion_number integer NULL, properties_map_doc varchar(4000) NULL, click_dttm_tz timestamp NULL, click_dttm timestamp NULL, 
      load_dttm timestamp NOT NULL, session_id_hex varchar(29) NULL, promotion_tracking_cd varchar(65) NULL, promotion_creative_nm varchar(260) NULL, 
      event_source_cd varchar(100) NULL, event_id varchar(36) NOT NULL, event_designed_id varchar(36) NULL, detail_id varchar(32) NULL, 
      detail_id_hex varchar(32) NULL, event_key_cd varchar(100) NULL, mobile_app_id varchar(40) NULL, promotion_nm varchar(260) NULL, 
      promotion_placement_nm varchar(260) NULL, session_id varchar(29) NULL, visit_id_hex varchar(32) NULL, channel_nm varchar(40) NULL, 
      event_nm varchar(256) NULL, identity_id varchar(36) NULL, promotion_type_nm varchar(65) NULL, visit_id varchar(32) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: PROMOTION_USED, PROMOTION_USED);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..response_history (
      properties_map_doc varchar(4000) NULL, load_dttm timestamp NOT NULL, response_dttm timestamp NULL, response_dttm_tz timestamp NULL, 
      session_id_hex varchar(29) NULL, response_id varchar(36) NOT NULL, response_channel_nm varchar(40) NULL, parent_event_designed_id varchar(36) NULL, 
      journey_occurrence_id varchar(36) NULL, detail_id_hex varchar(32) NULL, audience_id varchar(36) NULL, context_type_nm varchar(256) NULL, 
      context_val varchar(256) NULL, identity_id varchar(36) NULL, message_id varchar(36) NULL, response_nm varchar(256) NULL, 
      task_version_id varchar(36) NULL, aud_occurrence_id varchar(36) NULL, creative_id varchar(36) NULL, event_designed_id varchar(36) NULL, 
      journey_id varchar(36) NULL, occurrence_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, task_id varchar(36) NULL, 
      visit_id_hex varchar(32) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: RESPONSE_HISTORY, RESPONSE_HISTORY);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..search_results (
      results_displayed_flg char(1) NULL, search_results_displayed integer NULL, properties_map_doc varchar(4000) NULL, load_dttm timestamp NOT NULL, 
      search_results_dttm timestamp NULL, search_results_dttm_tz timestamp NULL, visit_id_hex varchar(32) NULL, srch_field_name varchar(325) NULL, 
      srch_field_id varchar(325) NULL, search_results_sk varchar(100) NULL, search_nm varchar(42) NULL, identity_id varchar(36) NULL, 
      event_key_cd varchar(100) NULL, event_id varchar(36) NOT NULL, channel_nm varchar(40) NULL, detail_id varchar(32) NULL, 
      detail_id_hex varchar(32) NULL, event_nm varchar(256) NULL, mobile_app_id varchar(40) NULL, session_id varchar(29) NULL, 
      srch_phrase varchar(2600) NULL, event_designed_id varchar(36) NULL, event_source_cd varchar(100) NULL, session_id_hex varchar(29) NULL, 
      visit_id varchar(32) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SEARCH_RESULTS, SEARCH_RESULTS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..search_results_ext (
      search_results_displayed integer NULL, load_dttm timestamp NOT NULL, search_results_sk varchar(100) NULL, event_designed_id varchar(36) NULL, 
      event_id varchar(36) NOT NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SEARCH_RESULTS_EXT, SEARCH_RESULTS_EXT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..session_details (
      java_enabled_flg char(1) NULL, java_script_enabled_flg char(1) NULL, cookies_enabled_flg char(1) NULL, is_portable_flag char(1) NULL, 
      flash_enabled_flg char(1) NULL, session_dt date NULL, session_dt_tz date NULL, longitude decimal(13,6) NULL, 
      latitude decimal(13,6) NULL, session_timeout integer NULL, metro_cd integer NULL, screen_color_depth_no integer NULL, 
      client_session_start_dttm timestamp NULL, load_dttm timestamp NOT NULL, session_start_dttm_tz timestamp NULL, session_start_dttm timestamp NULL, 
      client_session_start_dttm_tz timestamp NULL, user_agent_nm varchar(512) NULL, state_region_cd varchar(2) NULL, region_nm varchar(256) NULL, 
      profile_nm2 varchar(169) NULL, profile_nm1 varchar(169) NULL, previous_session_id_hex varchar(29) NULL, previous_session_id varchar(29) NULL, 
      postal_cd varchar(13) NULL, parent_event_id varchar(36) NULL, network_code varchar(10) NULL, mobile_country_code varchar(10) NULL, 
      manufacturer varchar(75) NULL, java_version_no varchar(12) NULL, flash_version_no varchar(16) NULL, event_id varchar(36) NOT NULL, 
      device_type_nm varchar(32) NULL, country_nm varchar(85) NULL, country_cd varchar(2) NULL, city_nm varchar(390) NULL, 
      browser_nm varchar(52) NULL, app_id varchar(36) NULL, browser_version_no varchar(16) NULL, carrier_name varchar(36) NULL, 
      device_language varchar(12) NULL, eventsource_cd varchar(36) NULL, identity_id varchar(36) NULL, ip_address varchar(64) NULL, 
      new_visitor_flg varchar(2) NULL, platform_desc varchar(78) NULL, platform_type_nm varchar(52) NULL, profile_nm4 varchar(169) NULL, 
      screen_size_txt varchar(12) NULL, session_id varchar(29) NULL, visitor_id varchar(128) NULL, app_version varchar(10) NULL, 
      channel_nm varchar(40) NULL, device_nm varchar(85) NULL, organization_nm varchar(256) NULL, platform_version varchar(25) NULL, 
      profile_nm3 varchar(169) NULL, profile_nm5 varchar(169) NULL, sdk_version varchar(25) NULL, session_id_hex varchar(29) NULL, 
      user_language_cd varchar(12) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SESSION_DETAILS, SESSION_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..session_details_ext (
      active_sec_spent_in_sessn_cnt integer NULL, seconds_spent_in_session_cnt integer NULL, load_dttm timestamp NOT NULL, last_session_activity_dttm timestamp NOT NULL, 
      session_expiration_dttm timestamp NULL, last_session_activity_dttm_tz timestamp NULL, session_expiration_dttm_tz timestamp NULL, session_id varchar(29) NOT NULL, 
      session_id_hex varchar(29) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SESSION_DETAILS_EXT, SESSION_DETAILS_EXT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..sms_message_clicked (
      sms_click_dttm_tz timestamp NULL, sms_click_dttm timestamp NULL, load_dttm timestamp NOT NULL, task_id varchar(36) NULL, 
      sms_message_id varchar(40) NULL, sender_id varchar(40) NULL, journey_occurrence_id varchar(36) NULL, event_nm varchar(256) NULL, 
      event_id varchar(36) NOT NULL, country_cd varchar(3) NULL, audience_id varchar(36) NULL, aud_occurrence_id varchar(36) NULL, 
      context_type_nm varchar(256) NULL, creative_version_id varchar(36) NULL, identity_id varchar(36) NULL, occurrence_id varchar(36) NULL, 
      context_val varchar(256) NULL, creative_id varchar(36) NULL, event_designed_id varchar(36) NULL, journey_id varchar(36) NULL, 
      response_tracking_cd varchar(36) NULL, task_version_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SMS_MESSAGE_CLICKED, SMS_MESSAGE_CLICKED);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..sms_message_delivered (
      sms_delivered_dttm_tz timestamp NULL, sms_delivered_dttm timestamp NULL, load_dttm timestamp NOT NULL, sms_message_id varchar(40) NULL, 
      occurrence_id varchar(36) NULL, journey_id varchar(36) NULL, identity_id varchar(36) NULL, creative_version_id varchar(36) NULL, 
      context_type_nm varchar(256) NULL, aud_occurrence_id varchar(36) NULL, country_cd varchar(3) NULL, event_id varchar(36) NOT NULL, 
      journey_occurrence_id varchar(36) NULL, sender_id varchar(40) NULL, task_id varchar(36) NULL, audience_id varchar(36) NULL, 
      context_val varchar(256) NULL, creative_id varchar(36) NULL, event_designed_id varchar(36) NULL, event_nm varchar(256) NULL, 
      response_tracking_cd varchar(36) NULL, task_version_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SMS_MESSAGE_DELIVERED, SMS_MESSAGE_DELIVERED);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..sms_message_failed (
      sms_failed_dttm_tz timestamp NULL, load_dttm timestamp NOT NULL, sms_failed_dttm timestamp NULL, task_version_id varchar(36) NULL, 
      task_id varchar(36) NULL, sms_message_id varchar(40) NULL, reason_description_txt varchar(1500) NULL, journey_occurrence_id varchar(36) NULL, 
      event_id varchar(36) NOT NULL, creative_id varchar(36) NULL, country_cd varchar(3) NULL, aud_occurrence_id varchar(36) NULL, 
      context_type_nm varchar(256) NULL, creative_version_id varchar(36) NULL, event_nm varchar(256) NULL, identity_id varchar(36) NULL, 
      occurrence_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, sender_id varchar(40) NULL, audience_id varchar(36) NULL, 
      context_val varchar(256) NULL, event_designed_id varchar(36) NULL, journey_id varchar(36) NULL, reason_cd varchar(5) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SMS_MESSAGE_FAILED, SMS_MESSAGE_FAILED);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..sms_message_reply (
      load_dttm timestamp NOT NULL, sms_reply_dttm_tz timestamp NULL, sms_reply_dttm timestamp NULL, task_version_id varchar(36) NULL, 
      sms_message_id varchar(40) NULL, response_tracking_cd varchar(36) NULL, occurrence_id varchar(36) NULL, identity_id varchar(36) NULL, 
      country_cd varchar(3) NULL, aud_occurrence_id varchar(36) NULL, context_type_nm varchar(256) NULL, event_id varchar(36) NOT NULL, 
      journey_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL, sender_id varchar(40) NULL, task_id varchar(36) NULL, 
      audience_id varchar(36) NULL, context_val varchar(256) NULL, event_designed_id varchar(36) NULL, event_nm varchar(256) NULL, 
      sms_content varchar(40) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SMS_MESSAGE_REPLY, SMS_MESSAGE_REPLY);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..sms_message_send (
      fragment_cnt integer NULL, sms_send_dttm timestamp NULL, sms_send_dttm_tz timestamp NULL, load_dttm timestamp NOT NULL, 
      occurrence_id varchar(36) NULL, identity_id varchar(36) NULL, event_id varchar(36) NOT NULL, event_designed_id varchar(36) NULL, 
      context_val varchar(256) NULL, aud_occurrence_id varchar(36) NULL, audience_id varchar(36) NULL, country_cd varchar(3) NULL, 
      creative_id varchar(36) NULL, event_nm varchar(256) NULL, journey_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL, 
      sender_id varchar(40) NULL, task_id varchar(36) NULL, context_type_nm varchar(256) NULL, creative_version_id varchar(36) NULL, 
      response_tracking_cd varchar(36) NULL, sms_message_id varchar(40) NULL, task_version_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SMS_MESSAGE_SEND, SMS_MESSAGE_SEND);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..sms_optout (
      load_dttm timestamp NOT NULL, sms_optout_dttm timestamp NULL, sms_optout_dttm_tz timestamp NULL, task_id varchar(36) NULL, 
      sms_message_id varchar(40) NULL, sender_id varchar(40) NULL, journey_occurrence_id varchar(36) NULL, event_id varchar(36) NOT NULL, 
      country_cd varchar(3) NULL, aud_occurrence_id varchar(36) NULL, context_type_nm varchar(256) NULL, creative_version_id varchar(36) NULL, 
      identity_id varchar(36) NULL, occurrence_id varchar(36) NULL, audience_id varchar(36) NULL, context_val varchar(256) NULL, 
      creative_id varchar(36) NULL, event_designed_id varchar(36) NULL, event_nm varchar(256) NULL, journey_id varchar(36) NULL, 
      response_tracking_cd varchar(36) NULL, task_version_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SMS_OPTOUT, SMS_OPTOUT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..sms_optout_details (
      load_dttm timestamp NOT NULL, sms_optout_dttm timestamp NULL, sms_optout_dttm_tz timestamp NULL, task_version_id varchar(36) NULL, 
      sms_message_id varchar(40) NULL, occurrence_id varchar(36) NULL, event_nm varchar(256) NULL, creative_id varchar(36) NULL, 
      context_type_nm varchar(256) NULL, audience_id varchar(36) NULL, address_val varchar(20) NULL, context_val varchar(256) NULL, 
      event_designed_id varchar(36) NULL, journey_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, task_id varchar(36) NULL, 
      aud_occurrence_id varchar(36) NULL, country_cd varchar(3) NULL, creative_version_id varchar(36) NULL, event_id varchar(36) NOT NULL, 
      identity_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL, sender_id varchar(40) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SMS_OPTOUT_DETAILS, SMS_OPTOUT_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..spot_clicked (
      control_group_flg char(1) NULL, product_qty_no integer NULL, properties_map_doc varchar(4000) NULL, spot_clicked_dttm timestamp NULL, 
      load_dttm timestamp NOT NULL, spot_clicked_dttm_tz timestamp NULL, session_id_hex varchar(29) NULL, reserved_2_txt varchar(100) NULL, 
      rec_group_id varchar(3) NULL, product_id varchar(128) NULL, message_id varchar(36) NULL, event_source_cd varchar(100) NULL, 
      event_nm varchar(256) NULL, detail_id_hex varchar(32) NULL, context_val varchar(256) NULL, channel_user_id varchar(300) NULL, 
      creative_id varchar(36) NULL, event_id varchar(36) NOT NULL, identity_id varchar(36) NULL, mobile_app_id varchar(40) NULL, 
      product_nm varchar(128) NULL, product_sku_no varchar(100) NULL, request_id varchar(36) NULL, segment_id varchar(36) NULL, 
      spot_id varchar(36) NULL, channel_nm varchar(40) NULL, context_type_nm varchar(256) NULL, creative_version_id varchar(36) NULL, 
      event_designed_id varchar(36) NULL, event_key_cd varchar(100) NULL, message_version_id varchar(36) NULL, occurrence_id varchar(36) NULL, 
      reserved_1_txt varchar(100) NULL, response_tracking_cd varchar(36) NULL, segment_version_id varchar(36) NULL, visit_id_hex varchar(32) NULL, 
      url_txt varchar(1332) NULL, task_version_id varchar(36) NULL, task_id varchar(36) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SPOT_CLICKED, SPOT_CLICKED);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..spot_requested (
      properties_map_doc varchar(4000) NULL, load_dttm timestamp NOT NULL, spot_requested_dttm_tz timestamp NULL, spot_requested_dttm timestamp NULL, 
      visit_id_hex varchar(32) NULL, spot_id varchar(36) NULL, session_id_hex varchar(29) NULL, request_id varchar(36) NULL, 
      mobile_app_id varchar(40) NULL, identity_id varchar(36) NULL, event_source_cd varchar(100) NULL, event_nm varchar(256) NULL, 
      event_id varchar(36) NOT NULL, event_designed_id varchar(36) NULL, detail_id_hex varchar(32) NULL, context_val varchar(256) NULL, 
      context_type_nm varchar(256) NULL, channel_user_id varchar(300) NULL, channel_nm varchar(40) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SPOT_REQUESTED, SPOT_REQUESTED);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..tag_details (
      created_dttm timestamp NULL, load_dttm timestamp NULL, last_modified_dttm timestamp NULL, tag_owner_usernm varchar(128) NULL, 
      tag_nm varchar(128) NULL, tag_id varchar(128) NULL, tag_desc varchar(1332) NULL, last_modified_usernm varchar(128) NULL, 
      identity_cd varchar(128) NULL, created_by_usernm varchar(128) NULL, component_type varchar(32) NULL, component_id varchar(128) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: TAG_DETAILS, TAG_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..visit_details (
      sequence_no integer NULL, visit_dttm_tz timestamp NULL, load_dttm timestamp NOT NULL, visit_dttm timestamp NULL, 
      visit_id_hex varchar(32) NULL, visit_id varchar(32) NULL, session_id_hex varchar(29) NULL, session_id varchar(29) NULL, 
      search_term_txt varchar(1332) NULL, search_engine_domain_txt varchar(215) NULL, search_engine_desc varchar(130) NULL, referrer_txt varchar(1332) NULL, 
      referrer_query_string_txt varchar(1332) NULL, referrer_domain_nm varchar(215) NULL, origination_type_nm varchar(65) NULL, origination_tracking_cd varchar(65) NULL, 
      origination_placement_nm varchar(390) NULL, origination_nm varchar(260) NULL, origination_creative_nm varchar(260) NULL, identity_id varchar(36) NULL, 
      event_id varchar(36) NOT NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: VISIT_DETAILS, VISIT_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..wf_process_details (
      delayed_by_day bigint NULL, percent_complete bigint NULL, user_tasks_cnt bigint NULL, completed_dttm timestamp NULL, 
      indexed_dttm timestamp NULL, planned_end_dttm timestamp NULL, load_dttm timestamp NULL, timeline_calculated_dttm timestamp NULL, 
      start_dttm timestamp NULL, published_dttm timestamp NULL, submitted_dttm timestamp NULL, deleted_dttm timestamp NULL, 
      created_dttm timestamp NULL, last_modified_dttm timestamp NULL, projected_end_dttm timestamp NULL, submitted_by_usernm varchar(128) NULL, 
      published_by_usernm varchar(128) NULL, process_type varchar(128) NULL, process_status varchar(128) NULL, process_nm varchar(128) NULL, 
      process_id varchar(128) NULL, process_comment varchar(128) NULL, process_category varchar(128) NULL, pdef_id varchar(128) NULL, 
      modified_status_cd varchar(128) NULL, last_modified_usernm varchar(128) NULL, deleted_by_usernm varchar(128) NULL, created_by_usernm varchar(128) NULL, 
      business_info_type varchar(128) NULL, business_info_nm varchar(128) NULL, business_info_id varchar(128) NULL, process_desc varchar(1332) NULL, 
      process_instance_version varchar(128) NULL, process_owner_usernm varchar(128) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: WF_PROCESS_DETAILS, WF_PROCESS_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..wf_process_details_custom_prop (
      attr_val varchar(4000) NULL, is_grid_flg char(1) NULL, is_obsolete_flg char(1) NULL, load_dttm timestamp NULL, 
      created_dttm timestamp NULL, last_modified_dttm timestamp NULL, process_id varchar(128) NULL, last_modified_usernm varchar(128) NULL, 
      data_type varchar(32) NULL, data_formatter varchar(64) NULL, created_by_usernm varchar(128) NULL, attr_id varchar(128) NULL, 
      attr_group_nm varchar(128) NULL, attr_group_id varchar(128) NULL, attr_cd varchar(128) NULL, attr_group_cd varchar(128) NULL, 
      attr_nm varchar(128) NULL, remote_pklist_tab_col varchar(128) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: WF_PROCESS_DETAILS_CUSTOM_PROP, WF_PROCESS_DETAILS_CUSTOM_PROP);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..wf_process_tasks (
      version_num bigint NULL, duration_per_assignee bigint NULL, delayed_by_day bigint NULL, percent_complete bigint NULL, 
      existobj_update_flg char(1) NULL, multi_assig_suprt_flg char(1) NULL, locally_updated_flg char(1) NULL, latest_flg char(1) NULL, 
      skip_peerupdate_scanning_flg char(1) NULL, approval_task_flg char(1) NULL, first_usertask_flg char(1) NULL, cancelled_task_flg char(1) NULL, 
      skip_update_scanning_flg char(1) NULL, is_sequential_flg char(1) NULL, projected_start_dttm timestamp NULL, published_dttm timestamp NULL, 
      load_dttm timestamp NULL, started_dttm timestamp NULL, indexed_dttm timestamp NULL, created_dttm timestamp NULL, 
      modified_dttm timestamp NULL, projected_end_dttm timestamp NULL, completed_dttm timestamp NULL, deleted_dttm timestamp NULL, 
      due_dttm timestamp NULL, engine_task_cancelled_dttm timestamp NULL, task_type varchar(128) NULL, task_status varchar(128) NULL, 
      task_instruction varchar(128) NULL, task_id varchar(128) NULL, task_desc varchar(1332) NULL, task_attachment varchar(128) NULL, 
      published_by_usernm varchar(128) NULL, process_id varchar(128) NULL, owner_usernm varchar(128) NULL, modified_status_cd varchar(128) NULL, 
      modified_by_usernm varchar(128) NULL, instance_version varchar(128) NULL, engine_taskdef_id varchar(128) NULL, deleted_by_usernm varchar(128) NULL, 
      created_by_usernm varchar(128) NULL, task_comment varchar(128) NULL, task_nm varchar(128) NULL, task_subtype varchar(128) NULL
      )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: WF_PROCESS_TASKS, WF_PROCESS_TASKS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..wf_tasks_user_assignment (
      usan_duration_day bigint NULL, delayed_by_day bigint NULL, is_assigned_flg char(1) NULL, is_replaced_flg char(1) NULL, 
      activation_completed_flg char(1) NULL, is_latest_flg char(1) NULL, created_dttm timestamp NULL, modified_dttm timestamp NULL, 
      projected_end_dttm timestamp NULL, deleted_dttm timestamp NULL, completed_dttm timestamp NULL, due_dttm timestamp NULL, 
      load_dttm timestamp NULL, projected_start_dttm timestamp NULL, start_dttm timestamp NULL, user_nm varchar(128) NULL, 
      user_assignment_id varchar(128) NULL, usan_status varchar(128) NULL, usan_instruction varchar(128) NULL, usan_desc varchar(1332) NULL, 
      task_id varchar(128) NULL, replacement_userid varchar(128) NULL, replacement_reason varchar(128) NULL, replacement_assignee_id varchar(128) NULL, 
      process_id varchar(128) NULL, owner_usernm varchar(128) NULL, modified_status_cd varchar(128) NULL, modified_by_usernm varchar(128) NULL, 
      instance_version varchar(128) NULL, initiator_comment varchar(128) NULL, deleted_by_usernm varchar(128) NULL, created_by_usernm varchar(128) NULL, 
      assignee_id varchar(128) NULL, approval_status varchar(128) NULL, assignee_type varchar(128) NULL, usan_comment varchar(128) NULL, 
      user_id varchar(128) NULL )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: WF_TASKS_USER_ASSIGNMENT, WF_TASKS_USER_ASSIGNMENT);
