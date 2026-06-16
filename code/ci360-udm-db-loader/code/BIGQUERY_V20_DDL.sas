/*******************************************************************************/
/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                         */
/* *****************************************************************************/
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..ABT_ATTRIBUTION (
      interaction_cost int64, conversion_value int64, interaction_dttm timestamp NOT NULL, task_id string, 
      load_id string, interaction_type string, interaction_subtype string, interaction_id string NOT NULL, 
      interaction string NOT NULL, identity_id string, creative_id string
      ) PARTITION BY DATE(interaction_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..ABT_ATTRIBUTION
      ADD PRIMARY KEY (INTERACTION_DTTM,INTERACTION_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ABT_ATTRIBUTION, ABT_ATTRIBUTION);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..AB_TEST_PATH_ASSIGNMENT (
      load_dttm timestamp NOT NULL, abtestpath_assignment_dttm_tz timestamp, abtestpath_assignment_dttm timestamp, session_id_hex string, 
      context_type_nm string, channel_user_id string, identity_id string, event_nm string, 
      channel_nm string, event_id string NOT NULL, event_designed_id string, abtest_path_id string, 
      activity_id string, context_val string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..AB_TEST_PATH_ASSIGNMENT
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: AB_TEST_PATH_ASSIGNMENT, AB_TEST_PATH_ASSIGNMENT);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..ACTIVITY_CONVERSION (
      activity_conversion_dttm_tz timestamp, load_dttm timestamp NOT NULL, activity_conversion_dttm timestamp, abtest_path_id string, 
      activity_id string, activity_node_id string, session_id_hex string, parent_event_designed_id string, 
      identity_id string, goal_id string, event_nm string, event_id string NOT NULL, 
      event_designed_id string, detail_id_hex string, context_val string, channel_nm string, 
      context_type_nm string, channel_user_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..ACTIVITY_CONVERSION
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ACTIVITY_CONVERSION, ACTIVITY_CONVERSION);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..ACTIVITY_FLOW_IN (
      activity_flow_in_dttm timestamp, activity_flow_in_dttm_tz timestamp, load_dttm timestamp NOT NULL, task_id string, 
      identity_id string, context_val string, event_designed_id string, event_id string NOT NULL, 
      channel_user_id string, activity_node_id string, activity_id string, abtest_path_id string, 
      channel_nm string, context_type_nm string, event_nm string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..ACTIVITY_FLOW_IN
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ACTIVITY_FLOW_IN, ACTIVITY_FLOW_IN);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..ACTIVITY_START (
      activity_start_dttm_tz timestamp, load_dttm timestamp NOT NULL, activity_start_dttm timestamp, channel_nm string, 
      activity_id string, identity_id string, event_nm string, event_id string NOT NULL, 
      channel_user_id string, event_designed_id string, context_val string, context_type_nm string

      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..ACTIVITY_START
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ACTIVITY_START, ACTIVITY_START);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..ADVERTISING_CONTACT (
      load_dttm timestamp NOT NULL, advertising_contact_dttm_tz timestamp, advertising_contact_dttm timestamp, task_version_id string, 
      task_id string, task_action_nm string, segment_version_id string, segment_id string, 
      response_tracking_cd string, occurrence_id string, journey_occurrence_id string, journey_id string, 
      identity_id string, event_nm string, event_id string NOT NULL, event_designed_id string, 
      context_val string, context_type_nm string, channel_nm string, audience_id string, 
      aud_occurrence_id string, advertising_platform_nm string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..ADVERTISING_CONTACT
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ADVERTISING_CONTACT, ADVERTISING_CONTACT);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..ASSET_DETAILS (
      folder_sk int64, asset_sk int64, public_media_id int64, user_rating_cnt int64, 
      total_user_rating_val int64, entity_revision_enabled_flg string, folder_deleted_flg string, entity_attribute_enabled_flg string, 
      expired_flg string, asset_locked_flg string, download_disabled_flg string, entity_subtype_enabled_flg string, 
      asset_deleted_flg string, external_sharing_error_dt date, average_user_rating_val decimal(4,2), folder_level int64, 
      last_modified_dttm timestamp, created_dttm timestamp, download_disabled_dttm timestamp, recycled_dttm timestamp, 
      expired_dttm timestamp, load_dttm timestamp, asset_locked_dttm timestamp, folder_desc string, 
      external_sharing_error_msg string, entity_table_nm string, download_disabled_by_usernm string, created_by_usernm string, 
      asset_source_type string, entity_subtype_nm string, entity_type_usage_cd string, folder_entity_status_cd string, 
      folder_id string, asset_owner_usernm string, asset_nm string, asset_locked_by_usernm string, 
      asset_id string, asset_desc string, asset_process_status string, asset_source_nm string, 
      entity_status_cd string, recycled_by_usernm string, entity_type_nm string, public_url string, 
      public_link string, process_task_id string, process_id string, last_modified_by_usernm string, 
      folder_path string, folder_owner_usernm string, folder_nm string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ASSET_DETAILS, ASSET_DETAILS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..ASSET_DETAILS_CUSTOM_PROP (
      attr_val STRING, is_obsolete_flg string, is_grid_flg string, load_dttm timestamp, 
      last_modified_dttm timestamp, created_dttm timestamp, remote_pklist_tab_col string, last_modified_usernm string, 
      data_type string, data_formatter string, created_by_usernm string, attr_nm string, 
      attr_id string, attr_group_nm string, attr_group_id string, attr_group_cd string, 
      attr_cd string, asset_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ASSET_DETAILS_CUSTOM_PROP, ASSET_DETAILS_CUSTOM_PROP);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..ASSET_FOLDER_DETAILS (
      deleted_flg string, folder_level int64, load_dttm timestamp, created_dttm timestamp, 
      last_modified_dttm timestamp, last_modified_by_usernm string, folder_owner_usernm string, folder_desc string, 
      folder_id string, entity_status_cd string, folder_nm string, folder_path string, 
      created_by_usernm string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ASSET_FOLDER_DETAILS, ASSET_FOLDER_DETAILS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..ASSET_RENDITION_DETAILS (
      download_cnt int64, revision_no int64, rend_deleted_flg string, rev_deleted_flg string, 
      current_revision_flg string, media_dpi decimal(10,2), file_size int64, media_depth int64, 
      media_height int64, rend_duration int64, media_width int64, created_dttm timestamp, 
      last_modified_dttm timestamp, load_dttm timestamp, revision_id string, revision_comment_txt string, 
      rendition_nm string, rendition_generated_type_cd string, last_modified_status_cd string, last_modified_by_usernm string, 
      file_nm string, file_format string, entity_status_cd string, created_by_usernm string, 
      asset_id string, rendition_id string, rendition_type_cd string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ASSET_RENDITION_DETAILS, ASSET_RENDITION_DETAILS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..ASSET_REVISION (
      revision_no int64, deleted_flg string, current_revision_flg string, load_dttm timestamp, 
      created_dttm timestamp, last_modified_dttm timestamp, revision_id string, last_modified_by_usernm string, 
      revision_comment_txt string, entity_status_cd string, created_by_usernm string, asset_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ASSET_REVISION, ASSET_REVISION);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..AUDIENCE_MEMBERSHIP_CHANGE (
      audience_change_dttm timestamp, load_dttm timestamp NOT NULL, audience_change_dttm_tz timestamp, identity_id string, 
      aud_occurrence_id string, event_id string NOT NULL, audience_id string, event_nm string

      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..AUDIENCE_MEMBERSHIP_CHANGE
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: AUDIENCE_MEMBERSHIP_CHANGE, AUDIENCE_MEMBERSHIP_CHANGE);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..BUSINESS_PROCESS_DETAILS (
      is_start_flg string, is_completion_flg string, process_attempt_cnt int64, step_order_no int64, 
      process_instance_no int64, process_dttm_tz timestamp, process_exception_dttm_tz timestamp, load_dttm timestamp NOT NULL, 
      process_dttm timestamp, process_exception_dttm timestamp, visit_id string, process_step_nm string, 
      process_details_sk string, identity_id string, event_nm string, detail_id string, 
      attribute1_txt string, detail_id_hex string, event_designed_id string, event_id string NOT NULL, 
      next_detail_id string, process_exception_txt string, session_id string, session_id_hex string, 
      visit_id_hex string, attribute2_txt string, event_source_cd string, process_nm string

      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..BUSINESS_PROCESS_DETAILS
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: BUSINESS_PROCESS_DETAILS, BUSINESS_PROCESS_DETAILS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CART_ACTIVITY_DETAILS (
      unit_price_amt decimal(17,2), displayed_cart_amt decimal(17,2), quantity_val int64, displayed_cart_items_no int64, 
      properties_map_doc string, activity_dttm timestamp, load_dttm timestamp NOT NULL, activity_dttm_tz timestamp, 
      cart_activity_sk string, activity_cd string, visit_id_hex string, visit_id string, 
      shipping_message_txt string, session_id_hex string, session_id string, saving_message_txt string, 
      product_sku string, product_nm string, product_id string, product_group_nm string, 
      mobile_app_id string, identity_id string, event_source_cd string, event_nm string, 
      availability_message_txt string, cart_id string, event_id string NOT NULL, event_designed_id string, 
      detail_id_hex string, detail_id string, currency_cd string, event_key_cd string, 
      channel_nm string, cart_nm string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..CART_ACTIVITY_DETAILS
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CART_ACTIVITY_DETAILS, CART_ACTIVITY_DETAILS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CC_BUDGET_BREAKUP (
      cc_obsolete_flg string, fin_accnt_obsolete_flg string, cc_budget_distribution decimal(17,2), load_dttm timestamp, 
      last_modified_dttm timestamp, created_dttm timestamp, planning_nm string, planning_id string, 
      last_modified_usernm string, gen_ledger_cd string, fin_accnt_nm string, fin_accnt_desc string, 
      created_by_usernm string, cost_center_id string, cc_owner_usernm string, cc_nm string, 
      cc_desc string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CC_BUDGET_BREAKUP, CC_BUDGET_BREAKUP);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CC_BUDGET_BREAKUP_CCBDGT (
      fin_accnt_obsolete_flg string, cc_obsolete_flg string, fp_obsolete_flg string, fp_end_dt date, 
      fp_start_dt date, cc_bdgt_invoiced_amt decimal(17,2), cc_lvl_distribution decimal(17,2), cc_bdgt_cmtmnt_invoice_amt decimal(17,2), 
      cc_rldup_total_expense decimal(17,2), cc_rldup_child_bdgt decimal(17,2), cc_level_expense decimal(17,2), cc_bdgt_cmtmnt_outstanding_amt decimal(17,2), 
      cc_bdgt_cmtmnt_overspent_amt decimal(17,2), cc_bdgt_amt decimal(17,2), cc_bdgt_budget_amt decimal(17,2), cc_budget_distribution decimal(17,2), 
      cc_bdgt_committed_amt decimal(17,2), cc_bdgt_direct_invoice_amt decimal(17,2), cc_bdgt_cmtmnt_invoice_cnt int64, last_modified_dttm timestamp, 
      load_dttm timestamp, created_dttm timestamp, planning_id string, last_modified_usernm string, 
      gen_ledger_cd string, fp_nm string, fp_id string, planning_nm string, 
      fp_desc string, fp_cls_ver string, fin_accnt_nm string, fin_accnt_desc string, 
      created_by_usernm string, cost_center_id string, cc_owner_usernm string, cc_number string, 
      cc_nm string, cc_desc string, cc_bdgt_budget_desc string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CC_BUDGET_BREAKUP_CCBDGT, CC_BUDGET_BREAKUP_CCBDGT);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_ACTIVITY_CUSTOM_ATTR (
      attribute_numeric_val decimal(17,2), updated_dttm timestamp, attribute_dttm_val timestamp, updated_by_nm string, 
      attribute_character_val string, activity_version_id string, activity_id string, attribute_data_type_cd string, 
      attribute_nm string, attribute_val string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_ACTIVITY_CUSTOM_ATTR, CDM_ACTIVITY_CUSTOM_ATTR);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_ACTIVITY_DETAIL (
      updated_dttm timestamp, last_published_dttm timestamp, valid_from_dttm timestamp, valid_to_dttm timestamp, 
      status_cd string, source_system_cd string, activity_nm string, activity_id string, 
      activity_desc string, activity_category_nm string, activity_cd string, activity_version_id string, 
      updated_by_nm string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_ACTIVITY_DETAIL, CDM_ACTIVITY_DETAIL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_ACTIVITY_X_TASK (
      updated_dttm timestamp, task_version_id string, activity_version_id string, activity_id string, 
      task_id string, updated_by_nm string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_ACTIVITY_X_TASK, CDM_ACTIVITY_X_TASK);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_AUDIENCE_DETAIL (
      audience_schedule_flg string, create_dttm timestamp, delete_dttm timestamp, updated_dttm timestamp, 
      created_user_nm string, audience_source_nm string, audience_nm string, audience_id string, 
      audience_desc string, audience_data_source_nm string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_AUDIENCE_DETAIL, CDM_AUDIENCE_DETAIL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_AUDIENCE_OCCUR_DETAIL (
      audience_size_cnt int64, end_dttm timestamp, updated_dttm timestamp, start_dttm timestamp, 
      started_by_nm string, occurrence_type_nm string, audience_occur_id string, audience_id string, 
      execution_status_cd string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_AUDIENCE_OCCUR_DETAIL, CDM_AUDIENCE_OCCUR_DETAIL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_AUDIENCE_X_SEGMENT (
      segment_id string, audience_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_AUDIENCE_X_SEGMENT, CDM_AUDIENCE_X_SEGMENT);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_BUSINESS_CONTEXT (
      updated_dttm timestamp, updated_by_nm string, business_context_type_cd string, business_context_nm string, 
      business_context_id string, source_system_cd string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_BUSINESS_CONTEXT, CDM_BUSINESS_CONTEXT);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_CAMPAIGN_CUSTOM_ATTR (
      attribute_numeric_val decimal(17,2), updated_dttm timestamp, attribute_dttm_val timestamp, page_nm string, 
      campaign_id string, attribute_character_val string, attribute_data_type_cd string, attribute_nm string, 
      attribute_val string, extension_attribute_nm string, updated_by_nm string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_CAMPAIGN_CUSTOM_ATTR, CDM_CAMPAIGN_CUSTOM_ATTR);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_CAMPAIGN_DETAIL (
      deleted_flg string, current_version_flg string, max_budget_amt decimal(17,2), min_budget_offer_amt decimal(17,2), 
      min_budget_amt decimal(17,2), max_budget_offer_amt decimal(17,2), campaign_version_no int64, deployment_version_no int64, 
      campaign_group_sk int64, approval_dttm timestamp, valid_from_dttm timestamp, run_dttm timestamp, 
      updated_dttm timestamp, valid_to_dttm timestamp, start_dttm timestamp, last_modified_dttm timestamp, 
      end_dttm timestamp, updated_by_nm string, source_system_cd string, campaign_status_cd string, 
      campaign_type_cd string, last_modified_by_user_nm string, campaign_nm string, campaign_folder_txt string, 
      campaign_desc string, campaign_cd string, approval_given_by_nm string, campaign_id string, 
      campaign_owner_nm string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_CAMPAIGN_DETAIL, CDM_CAMPAIGN_DETAIL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_CONTACT_CHANNEL (
      updated_dttm timestamp, contact_channel_cd string, updated_by_nm string, contact_channel_nm string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_CONTACT_CHANNEL, CDM_CONTACT_CHANNEL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_CONTACT_HISTORY (
      optimization_backfill_flg string, control_group_flg string, contact_dt date NOT NULL, updated_dttm timestamp, 
      contact_dttm_tz timestamp, contact_dttm timestamp, source_system_cd string, external_contact_info_2_id string, 
      context_type_nm string, audience_id string, contact_nm string, identity_id string, 
      audience_occur_id string, contact_id string NOT NULL, contact_status_cd string, context_val string, 
      external_contact_info_1_id string, rtc_id string, updated_by_nm string
      ) PARTITION BY contact_dt
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..CDM_CONTACT_HISTORY
      ADD PRIMARY KEY (CONTACT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_CONTACT_HISTORY, CDM_CONTACT_HISTORY);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_CONTACT_STATUS (
      updated_dttm timestamp, contact_status_desc string, contact_status_cd string, updated_by_nm string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_CONTACT_STATUS, CDM_CONTACT_STATUS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_CONTENT_CUSTOM_ATTR (
      attribute_numeric_val decimal(17,2), updated_dttm timestamp, attribute_dttm_val timestamp, updated_by_nm string, 
      attribute_val string, attribute_data_type_cd string, attribute_nm string, content_version_id string, 
      attribute_character_val string, content_id string, extension_attribute_nm string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_CONTENT_CUSTOM_ATTR, CDM_CONTENT_CUSTOM_ATTR);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_CONTENT_DETAIL (
      active_flg string, created_dt date, updated_dttm timestamp, valid_from_dttm timestamp, 
      valid_to_dttm timestamp, updated_by_nm string, owner_nm string, external_reference_url_txt string, 
      content_id string, contact_content_status_cd string, contact_content_cd string, contact_content_class_nm string, 
      contact_content_desc string, contact_content_nm string, contact_content_type_nm string, content_version_id string, 
      created_user_nm string, external_reference_txt string, source_system_cd string, contact_content_category_nm string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_CONTENT_DETAIL, CDM_CONTENT_DETAIL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_DYN_CONTENT_CUSTOM_ATTR (
      attribute_numeric_val decimal(17,2), attribute_dttm_val timestamp, updated_dttm timestamp, updated_by_nm string, 
      content_hash_val string, attribute_character_val string, attribute_data_type_cd string, attribute_val string, 
      content_version_id string, attribute_nm string, content_id string, extension_attribute_nm string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_DYN_CONTENT_CUSTOM_ATTR, CDM_DYN_CONTENT_CUSTOM_ATTR);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_IDENTIFIER_TYPE (
      updated_dttm timestamp, updated_by_nm string, identifier_type_desc string, identifier_type_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_IDENTIFIER_TYPE, CDM_IDENTIFIER_TYPE);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_IDENTITY_ATTR (
      entry_dttm timestamp, valid_to_dttm timestamp, valid_from_dttm timestamp, updated_dttm timestamp, 
      identifier_type_id string, user_identifier_val string, updated_by_nm string, source_system_cd string, 
      identity_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_IDENTITY_ATTR, CDM_IDENTITY_ATTR);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_IDENTITY_MAP (
      updated_dttm timestamp, identity_type_cd string, identity_id string, updated_by_nm string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_IDENTITY_MAP, CDM_IDENTITY_MAP);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_IDENTITY_TYPE (
      updated_dttm timestamp, updated_by_nm string, identity_type_desc string, identity_type_cd string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_IDENTITY_TYPE, CDM_IDENTITY_TYPE);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_OCCURRENCE_DETAIL (
      occurrence_no int64, end_dttm timestamp, updated_dttm timestamp, start_dttm timestamp, 
      updated_by_nm string, source_system_cd string, occurrence_object_type_cd string, occurrence_object_id string, 
      occurrence_id string, execution_status_cd string, occurrence_type_cd string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_OCCURRENCE_DETAIL, CDM_OCCURRENCE_DETAIL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_RESPONSE_CHANNEL (
      updated_dttm timestamp, updated_by_nm string, response_channel_nm string, response_channel_cd string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_RESPONSE_CHANNEL, CDM_RESPONSE_CHANNEL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_RESPONSE_EXTENDED_ATTR (
      updated_dttm timestamp NOT NULL, updated_by_nm string, response_id string NOT NULL, response_attribute_type_cd string NOT NULL, 
      attribute_val string, attribute_nm string NOT NULL, attribute_data_type_cd string    )) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..CDM_RESPONSE_EXTENDED_ATTR
      ADD PRIMARY KEY (ATTRIBUTE_NM,RESPONSE_ATTRIBUTE_TYPE_CD,RESPONSE_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_RESPONSE_EXTENDED_ATTR, CDM_RESPONSE_EXTENDED_ATTR);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_RESPONSE_HISTORY (
      conversion_flg string, inferred_response_flg string, response_dt date NOT NULL, response_val_amt decimal(17,2), 
      properties_map_doc string, updated_dttm timestamp, response_dttm timestamp, response_dttm_tz timestamp, 
      updated_by_nm string, source_system_cd string, rtc_id string, response_type_cd string, 
      response_id string NOT NULL, response_channel_cd string, response_cd string, identity_id string, 
      external_contact_info_2_id string, external_contact_info_1_id string, context_val string, context_type_nm string, 
      content_version_id string, content_id string, content_hash_val string, contact_id string, 
      audience_occur_id string, audience_id string
      ) PARTITION BY response_dt
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..CDM_RESPONSE_HISTORY
      ADD PRIMARY KEY (RESPONSE_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_RESPONSE_HISTORY, CDM_RESPONSE_HISTORY);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_RESPONSE_LOOKUP (
      updated_dttm timestamp, updated_by_nm string, response_nm string, response_cd string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_RESPONSE_LOOKUP, CDM_RESPONSE_LOOKUP);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_RESPONSE_TYPE (
      updated_dttm timestamp, updated_by_nm string, response_type_desc string, response_type_cd string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_RESPONSE_TYPE, CDM_RESPONSE_TYPE);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_RTC_DETAIL (
      deleted_flg string, response_tracking_flg string, task_occurrence_no int64, processed_dttm timestamp, 
      updated_dttm timestamp, updated_by_nm string, task_version_id string, task_id string, 
      source_system_cd string, segment_version_id string, segment_id string, rtc_id string, 
      occurrence_id string, execution_status_cd string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_RTC_DETAIL, CDM_RTC_DETAIL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_RTC_X_CONTENT (
      sequence_no int64, updated_dttm timestamp, updated_by_nm string, rtc_x_content_sk string, 
      rtc_id string, content_version_id string, content_id string, content_hash_val string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_RTC_X_CONTENT, CDM_RTC_X_CONTENT);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_SEGMENT_CUSTOM_ATTR (
      attribute_numeric_val decimal(17,2), updated_dttm timestamp, attribute_dttm_val timestamp, updated_by_nm string, 
      segment_version_id string, segment_id string, attribute_val string, attribute_nm string, 
      attribute_data_type_cd string, attribute_character_val string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_SEGMENT_CUSTOM_ATTR, CDM_SEGMENT_CUSTOM_ATTR);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_SEGMENT_DETAIL (
      valid_from_dttm timestamp, updated_dttm timestamp, valid_to_dttm timestamp, updated_by_nm string, 
      source_system_cd string, segment_version_id string, segment_status_cd string, segment_src_nm string, 
      segment_nm string, segment_map_version_id string, segment_map_id string, segment_id string, 
      segment_desc string, segment_cd string, segment_category_nm string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_SEGMENT_DETAIL, CDM_SEGMENT_DETAIL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_SEGMENT_MAP (
      valid_to_dttm timestamp, valid_from_dttm timestamp, updated_dttm timestamp, updated_by_nm string, 
      source_system_cd string, segment_map_version_id string, segment_map_status_cd string, segment_map_src_nm string, 
      segment_map_nm string, segment_map_id string, segment_map_desc string, segment_map_cd string, 
      segment_map_category_nm string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_SEGMENT_MAP, CDM_SEGMENT_MAP);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_SEGMENT_MAP_CUSTOM_ATTR (
      attribute_numeric_val decimal(17,2), updated_dttm timestamp, attribute_dttm_val timestamp, updated_by_nm string, 
      segment_map_version_id string, segment_map_id string, attribute_val string, attribute_nm string, 
      attribute_data_type_cd string, attribute_character_val string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_SEGMENT_MAP_CUSTOM_ATTR, CDM_SEGMENT_MAP_CUSTOM_ATTR);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_SEGMENT_TEST (
      stratified_sampling_flg string, test_enabled_flg string, test_pct decimal(5,2), test_cnt int64, 
      updated_dttm timestamp, test_sizing_type_nm string, test_type_nm string, test_nm string, 
      test_cd string, task_version_id string, task_id string, stratified_samp_criteria_txt string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_SEGMENT_TEST, CDM_SEGMENT_TEST);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_SEGMENT_TEST_X_SEGMENT (
      updated_dttm timestamp, test_cd string, task_id string, segment_id string, 
      task_version_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_SEGMENT_TEST_X_SEGMENT, CDM_SEGMENT_TEST_X_SEGMENT);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_TASK_CUSTOM_ATTR (
      attribute_numeric_val decimal(17,2), attribute_dttm_val timestamp, updated_dttm timestamp, task_version_id string, 
      task_id string, extension_attribute_nm string, attribute_character_val string, attribute_data_type_cd string, 
      attribute_nm string, attribute_val string, updated_by_nm string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_TASK_CUSTOM_ATTR, CDM_TASK_CUSTOM_ATTR);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_TASK_DETAIL (
      segment_tests_flg string, saved_flg string, scheduled_flg string, recurring_schedule_flg string, 
      published_flg string, staged_flg string, limit_by_total_impression_flg string, update_contact_history_flg string, 
      standard_reply_flg string, active_flg string, created_dt date, budget_unit_usage_amt decimal(17,2), 
      budget_unit_cost_amt decimal(17,2), max_budget_amt decimal(17,2), min_budget_amt decimal(17,2), min_budget_offer_amt decimal(17,2), 
      max_budget_offer_amt decimal(17,2), maximum_period_expression_cnt int64, limit_period_unit_cnt int64, scheduled_end_dttm timestamp, 
      updated_dttm timestamp, valid_from_dttm timestamp, export_dttm timestamp, valid_to_dttm timestamp, 
      scheduled_start_dttm timestamp, task_version_id string, task_type_nm string, task_subtype_nm string, 
      task_status_cd string, task_id string, task_delivery_type_nm string, subject_type_nm string, 
      source_system_cd string, modified_status_cd string, contact_channel_cd string, business_context_id string, 
      campaign_id string, control_group_action_nm string, created_user_nm string, owner_nm string, 
      recurr_type_cd string, stratified_sampling_action_nm string, task_cd string, task_desc string, 
      task_nm string, updated_by_nm string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_TASK_DETAIL, CDM_TASK_DETAIL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..COMMITMENT_DETAILS (
      vendor_obsolete_flg string, cmtmnt_overspent_amt decimal(17,2), vendor_amt decimal(17,2), cmtmnt_outstanding_amt decimal(17,2), 
      cmtmnt_amt decimal(17,2), last_modified_dttm timestamp, cmtmnt_payment_dttm timestamp, cmtmnt_created_dttm timestamp, 
      load_dttm timestamp, created_dttm timestamp, vendor_number string, vendor_id string, 
      planning_nm string, planning_id string, last_modified_usernm string, created_by_usernm string, 
      cmtmnt_status string, cmtmnt_nm string, cmtmnt_desc string, cmtmnt_closure_note string, 
      cmtmnt_id string, cmtmnt_no string, planning_currency_cd string, vendor_currency_cd string, 
      vendor_nm string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: COMMITMENT_DETAILS, COMMITMENT_DETAILS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..COMMITMENT_LINE_ITEMS (
      item_qty int64, item_alloc_unit int64, item_vend_alloc_unit int64, vendor_obsolete_flg string, 
      cc_recon_alloc_amt decimal(17,2), cmtmnt_overspent_amt decimal(17,2), item_rate decimal(17,2), item_alloc_amt decimal(17,2), 
      cmtmnt_amt decimal(17,2), vendor_amt decimal(17,2), cmtmnt_outstanding_amt decimal(17,2), cc_allocated_amt decimal(17,2), 
      cc_available_amt decimal(17,2), item_vend_alloc_amt decimal(17,2), item_number int64, created_dttm timestamp, 
      last_modified_dttm timestamp, load_dttm timestamp, cmtmnt_payment_dttm timestamp, cmtmnt_created_dttm timestamp, 
      vendor_nm string, vendor_currency_cd string, planning_nm string, planning_currency_cd string, 
      last_modified_usernm string, gen_ledger_cd string, cost_center_id string, cmtmnt_status string, 
      cmtmnt_no string, cmtmnt_nm string, cmtmnt_desc string, cmtmnt_closure_note string, 
      ccat_nm string, cc_owner_usernm string, cc_nm string, cc_desc string, 
      cmtmnt_id string, created_by_usernm string, fin_acc_nm string, item_nm string, 
      planning_id string, vendor_id string, vendor_number string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: COMMITMENT_LINE_ITEMS, COMMITMENT_LINE_ITEMS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..COMMITMENT_LINE_ITEMS_CCBDGT (
      vendor_obsolete_flg string, fp_obsolete_flg string, cc_obsolete_flg string, fp_end_dt date, 
      fp_start_dt date, item_vend_alloc_amt decimal(17,2), cc_available_amt decimal(17,2), item_alloc_amt decimal(17,2), 
      cc_allocated_amt decimal(17,2), cc_bdgt_cmtmnt_invoice_amt decimal(17,2), vendor_amt decimal(17,2), cmtmnt_outstanding_amt decimal(17,2), 
      cmtmnt_amt decimal(17,2), cc_recon_alloc_amt decimal(17,2), cc_bdgt_budget_amt decimal(17,2), cc_bdgt_committed_amt decimal(17,2), 
      item_rate decimal(17,2), cc_bdgt_cmtmnt_overspent_amt decimal(17,2), cc_bdgt_invoiced_amt decimal(17,2), cc_bdgt_amt decimal(17,2), 
      cc_bdgt_cmtmnt_outstanding_amt decimal(17,2), cmtmnt_overspent_amt decimal(17,2), cc_bdgt_direct_invoice_amt decimal(17,2), item_number int64, 
      item_alloc_unit int64, item_qty int64, cc_bdgt_cmtmnt_invoice_cnt int64, item_vend_alloc_unit int64, 
      created_dttm timestamp, cmtmnt_payment_dttm timestamp, last_modified_dttm timestamp, load_dttm timestamp, 
      cmtmnt_created_dttm timestamp, vendor_currency_cd string, planning_currency_cd string, last_modified_usernm string, 
      gen_ledger_cd string, fp_id string, item_nm string, planning_nm string, 
      vendor_nm string, fp_desc string, fp_cls_ver string, fin_acc_nm string, 
      created_by_usernm string, cmtmnt_status string, cmtmnt_no string, cmtmnt_id string, 
      cmtmnt_desc string, cc_nm string, cc_desc string, cc_bdgt_budget_desc string, 
      cc_number string, cc_owner_usernm string, ccat_nm string, cmtmnt_closure_note string, 
      cmtmnt_nm string, cost_center_id string, fp_nm string, planning_id string, 
      vendor_id string, vendor_number string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: COMMITMENT_LINE_ITEMS_CCBDGT, COMMITMENT_LINE_ITEMS_CCBDGT);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CONTACT_HISTORY (
      control_group_flg string, properties_map_doc string, contact_dttm_tz timestamp, load_dttm timestamp NOT NULL, 
      contact_dttm timestamp, task_id string, parent_event_designed_id string, journey_occurrence_id string, 
      detail_id_hex string, context_type_nm string, audience_id string, contact_id string NOT NULL, 
      identity_id string, message_id string, response_tracking_cd string, visit_id_hex string, 
      aud_occurrence_id string, contact_channel_nm string, contact_nm string, context_val string, 
      creative_id string, event_designed_id string, journey_id string, occurrence_id string, 
      session_id_hex string, task_version_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..CONTACT_HISTORY
      ADD PRIMARY KEY (CONTACT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CONTACT_HISTORY, CONTACT_HISTORY);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CONVERSION_MILESTONE (
      test_flg string, control_group_flg string, total_cost_amt decimal(17,2), properties_map_doc string, 
      load_dttm timestamp NOT NULL, conversion_milestone_dttm timestamp, conversion_milestone_dttm_tz timestamp, visit_id_hex string, 
      task_id string, spot_id string, segment_version_id string, reserved_1_txt string, 
      occurrence_id string, message_version_id string, goal_id string, detail_id_hex string, 
      channel_user_id string, analysis_group_id string, audience_id string, context_val string, 
      creative_id string, event_id string NOT NULL, journey_id string, response_tracking_cd string, 
      activity_id string, aud_occurrence_id string, channel_nm string, context_type_nm string, 
      creative_version_id string, event_designed_id string, event_nm string, identity_id string, 
      journey_occurrence_id string, message_id string, mobile_app_id string, parent_event_designed_id string, 
      rec_group_id string, reserved_2_txt string, segment_id string, session_id_hex string, 
      subject_line_txt string, task_version_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..CONVERSION_MILESTONE
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CONVERSION_MILESTONE, CONVERSION_MILESTONE);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CUSTOM_EVENTS (
      custom_revenue_amt decimal(17,2), properties_map_doc string, custom_event_dttm timestamp, custom_event_dttm_tz timestamp, 
      load_dttm timestamp NOT NULL, session_id string, page_id string, event_type_nm string, 
      event_id string NOT NULL, channel_user_id string, custom_event_nm string, detail_id_hex string, 
      event_nm string, reserved_1_txt string, reserved_2_txt string, visit_id string, 
      channel_nm string, custom_event_group_nm string, custom_events_sk string, detail_id string, 
      event_designed_id string, event_key_cd string, event_source_cd string, identity_id string, 
      mobile_app_id string, session_id_hex string, visit_id_hex string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..CUSTOM_EVENTS
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CUSTOM_EVENTS, CUSTOM_EVENTS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CUSTOM_EVENTS_EXT (
      custom_revenue_amt decimal(17,2), load_dttm timestamp NOT NULL, event_designed_id string, custom_events_sk string NOT NULL

      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY event_designed_id
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..CUSTOM_EVENTS_EXT
      ADD PRIMARY KEY (CUSTOM_EVENTS_SK) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CUSTOM_EVENTS_EXT, CUSTOM_EVENTS_EXT);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DAILY_USAGE (
      bc_subjcnt_str STRING, customer_profiles_processed_str STRING, api_usage_str STRING, mob_impr_cnt int64, 
      dm_destinations_total_row_cnt int64, google_ads_cnt int64, mob_sesn_cnt int64, audience_usage_cnt int64, 
      mobile_in_app_msg_cnt int64, mobile_push_cnt int64, email_preview_cnt int64, facebook_ads_cnt int64, 
      web_sesn_cnt int64, plan_users_cnt int64, outbound_api_cnt int64, web_impr_cnt int64, 
      email_send_cnt int64, linkedin_ads_cnt int64, dm_destinations_total_id_cnt int64, asset_size decimal(17,2), 
      db_size decimal(17,2), admin_user_cnt int64, event_day string NOT NULL    )) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DAILY_USAGE
      ADD PRIMARY KEY (EVENT_DAY) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DAILY_USAGE, DAILY_USAGE);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DATA_VIEW_DETAILS (
      total_cost_amt decimal(17,2), properties_map_doc string, data_view_dttm timestamp, data_view_dttm_tz timestamp, 
      load_dttm timestamp NOT NULL, visit_id string, reserved_2_txt string, event_designed_id string, 
      channel_user_id string, detail_id string, event_nm string, session_id_hex string, 
      detail_id_hex string, event_id string NOT NULL, identity_id string, parent_event_designed_id string, 
      reserved_1_txt string, session_id string, visit_id_hex string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DATA_VIEW_DETAILS
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DATA_VIEW_DETAILS, DATA_VIEW_DETAILS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DBT_ADV_CAMPAIGN_VISITORS (
      ge_longitude decimal(13,6), ge_latitude decimal(13,6), rv_revenue decimal(17,2), co_conversions int64, 
      new_visitors int64, return_visitors int64, bouncers int64, visits int64, 
      page_views int64, average_visit_duration int64, session_complete_load_dttm timestamp NOT NULL, visit_dttm timestamp, 
      visit_dttm_tz timestamp, session_start_dttm_tz timestamp, session_start_dttm timestamp, se_external_search_engine string, 
      landing_page string, ge_country string, cu_customer_id string, br_browser_version string, 
      device_type string, landing_page_url_domain string, se_external_search_engine_phrase string, bouncer string, 
      br_browser_name string, device_name string, ge_city string, ge_state_region string, 
      landing_page_url string, pl_device_operating_system string, se_external_search_engine_domain string, visitor_type string, 
      visitor_id string, visit_origination_type string, visit_origination_tracking_code string, visit_origination_placement string, 
      visit_origination_name string, visit_origination_creative string, visit_id string NOT NULL, session_id string NOT NULL

      ) PARTITION BY DATE(session_complete_load_dttm )
      CLUSTER BY SESSION_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DBT_ADV_CAMPAIGN_VISITORS
      ADD PRIMARY KEY (SESSION_ID,VISIT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_ADV_CAMPAIGN_VISITORS, DBT_ADV_CAMPAIGN_VISITORS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DBT_BUSINESS_PROCESS (
      processes int64, steps_completed int64, step_count int64, processes_completed int64, 
      steps_abandoned int64, last_step int64, processes_abandoned int64, steps int64, 
      bus_process_started_dttm_tz timestamp, session_start_dttm_tz timestamp, session_start_dttm timestamp, bus_process_started_dttm timestamp NOT NULL, 
      session_complete_load_dttm timestamp NOT NULL, visitor_id string, visit_origination_tracking_code string, visit_origination_name string, 
      visit_id string, session_id string NOT NULL, device_name string, cu_customer_id string, 
      business_process_step_name string NOT NULL, business_process_attribute_2 string, bouncer string, business_process_attribute_1 string, 
      business_process_name string NOT NULL, device_type string, visit_origination_creative string, visit_origination_placement string, 
      visit_origination_type string, visitor_type string
      ) PARTITION BY DATE(session_complete_load_dttm )
      CLUSTER BY SESSION_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DBT_BUSINESS_PROCESS
      ADD PRIMARY KEY (BUSINESS_PROCESS_NAME,BUSINESS_PROCESS_STEP_NAME,BUS_PROCESS_STARTED_DTTM,SESSION_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_BUSINESS_PROCESS, DBT_BUSINESS_PROCESS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DBT_CONTENT (
      total_page_view_time int64, entry_pages int64, active_page_view_time int64, views int64, 
      exit_pages int64, visits int64, bouncers int64, session_start_dttm timestamp, 
      session_complete_load_dttm timestamp NOT NULL, session_start_dttm_tz timestamp, detail_dttm_tz timestamp, detail_dttm timestamp, 
      visitor_type string, visitor_id string, visit_origination_type string, visit_origination_tracking_code string, 
      visit_origination_placement string, visit_origination_name string, visit_origination_creative string, visit_id string, 
      session_id string, pg_page_url string, pg_page string, pg_domain_name string, 
      device_type string, device_name string, detail_id string NOT NULL, cu_customer_id string, 
      class2_id string, bouncer string, class1_id string
      ) PARTITION BY DATE(session_complete_load_dttm )
      CLUSTER BY SESSION_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DBT_CONTENT
      ADD PRIMARY KEY (DETAIL_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_CONTENT, DBT_CONTENT);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DBT_DOCUMENTS (
      document_downloads int64, document_download_dttm_tz timestamp, session_start_dttm_tz timestamp, session_start_dttm timestamp, 
      session_complete_load_dttm timestamp NOT NULL, document_download_dttm timestamp, visitor_type string, visitor_id string, 
      visit_origination_type string, visit_origination_tracking_code string, visit_origination_placement string, visit_origination_name string, 
      visit_origination_creative string, visit_id string, session_id string, do_page_url string, 
      do_page_description string, device_type string, device_name string, detail_id string NOT NULL, 
      cu_customer_id string, class2_id string, class1_id string, bouncer string

      ) PARTITION BY DATE(session_complete_load_dttm )
      CLUSTER BY SESSION_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DBT_DOCUMENTS
      ADD PRIMARY KEY (DETAIL_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_DOCUMENTS, DBT_DOCUMENTS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DBT_ECOMMERCE (
      product_purchase_revenues decimal(17,2), basket_adds_revenue decimal(17,2), basket_removes_revenue decimal(17,2), product_views int64, 
      basket_adds int64, basket_adds_units int64, product_purchases int64, product_purchase_units int64, 
      basket_removes_units int64, basket_removes int64, baskets_abandoned int64, baskets_completed int64, 
      baskets_started int64, session_complete_load_dttm timestamp NOT NULL, session_start_dttm_tz timestamp, product_activity_dttm_tz timestamp, 
      product_activity_dttm timestamp NOT NULL, session_start_dttm timestamp, visitor_type string, visitor_id string, 
      visit_origination_type string, visit_origination_tracking_code string, visit_origination_placement string, visit_origination_name string, 
      visit_origination_creative string, visit_id string NOT NULL, session_id string, product_sku string NOT NULL, 
      product_name string NOT NULL, product_id string NOT NULL, product_group_name string, device_type string, 
      device_name string, cu_customer_id string, bouncer string, basket_id string NOT NULL

      ) PARTITION BY DATE(session_complete_load_dttm )
      CLUSTER BY SESSION_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DBT_ECOMMERCE
      ADD PRIMARY KEY (BASKET_ID,PRODUCT_ACTIVITY_DTTM,PRODUCT_ID,PRODUCT_NAME,PRODUCT_SKU,VISIT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_ECOMMERCE, DBT_ECOMMERCE);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DBT_FORMS (
      attempts int64, forms_completed int64, forms_not_submitted int64, forms_started int64, 
      form_attempt_dttm timestamp, session_start_dttm timestamp, form_attempt_dttm_tz timestamp, session_complete_load_dttm timestamp NOT NULL, 
      session_start_dttm_tz timestamp, visitor_type string, visitor_id string, visit_origination_type string, 
      visit_origination_tracking_code string, visit_origination_placement string, visit_origination_name string, visit_origination_creative string, 
      visit_id string, session_id string, last_field string, form_nm string, 
      device_type string, device_name string, detail_id string NOT NULL, cu_customer_id string, 
      bouncer string
      ) PARTITION BY DATE(session_complete_load_dttm )
      CLUSTER BY SESSION_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DBT_FORMS
      ADD PRIMARY KEY (DETAIL_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_FORMS, DBT_FORMS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DBT_GOALS (
      goal_revenue decimal(17,2), visits int64, session_start_dttm timestamp, goal_reached_dttm_tz timestamp, 
      goal_reached_dttm timestamp, session_complete_load_dttm timestamp NOT NULL, session_start_dttm_tz timestamp, goals int64, 
      visitor_type string, visitor_id string, visit_origination_type string, visit_origination_tracking_code string, 
      visit_origination_placement string, visit_origination_name string, visit_origination_creative string, visit_id string, 
      session_id string, goal_name string NOT NULL, goal_group_name string NOT NULL, device_type string, 
      device_name string, detail_id string NOT NULL, cu_customer_id string, bouncer string

      ) PARTITION BY DATE(session_complete_load_dttm )
      CLUSTER BY SESSION_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DBT_GOALS
      ADD PRIMARY KEY (DETAIL_ID,GOAL_GROUP_NAME,GOAL_NAME) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_GOALS, DBT_GOALS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DBT_MEDIA_CONSUMPTION (
      time_viewing decimal(11,3), duration decimal(11,3), maximum_progress decimal(11,3) NOT NULL, content_viewed decimal(11,3), 
      counter int64, interactions_count int64 NOT NULL, session_start_dttm_tz timestamp, session_start_dttm timestamp, 
      session_complete_load_dttm timestamp NOT NULL, media_start_dttm timestamp, media_start_dttm_tz timestamp, views_started int64, 
      views_completed int64, views int64, media_section_view int64, visitor_type string, 
      visitor_id string, visit_origination_type string, visit_origination_tracking_code string, visit_origination_placement string, 
      visit_origination_name string, visit_origination_creative string, visit_id string NOT NULL, session_id string, 
      media_uri_txt string, media_section string NOT NULL, media_name string, media_completion_rate string NOT NULL, 
      device_type string, device_name string, detail_id string NOT NULL, cu_customer_id string, 
      bouncer string
      ) PARTITION BY DATE(session_complete_load_dttm )
      CLUSTER BY SESSION_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DBT_MEDIA_CONSUMPTION
      ADD PRIMARY KEY (DETAIL_ID,INTERACTIONS_COUNT,MAXIMUM_PROGRESS,MEDIA_COMPLETION_RATE,MEDIA_SECTION,VISIT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_MEDIA_CONSUMPTION, DBT_MEDIA_CONSUMPTION);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DBT_PROMOTIONS (
      click_throughs int64, displays int64, session_start_dttm_tz timestamp, promotion_shown_dttm_tz timestamp, 
      promotion_shown_dttm timestamp, session_complete_load_dttm timestamp NOT NULL, session_start_dttm timestamp, visitor_type string, 
      visitor_id string, visit_origination_type string, visit_origination_tracking_code string, visit_origination_placement string, 
      visit_origination_name string, visit_origination_creative string, visit_id string, session_id string, 
      promotion_type string, promotion_tracking_code string, promotion_placement string, promotion_name string, 
      promotion_creative string, device_type string, device_name string, detail_id string NOT NULL, 
      cu_customer_id string, bouncer string
      ) PARTITION BY DATE(session_complete_load_dttm )
      CLUSTER BY SESSION_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DBT_PROMOTIONS
      ADD PRIMARY KEY (DETAIL_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_PROMOTIONS, DBT_PROMOTIONS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DBT_SEARCH (
      num_additional_searches int64, num_pages_viewed_afterwards int64, searches int64, visits int64, 
      search_unknown_results int64, search_returned_results int64, exit_pages int64, search_no_results_returned int64, 
      search_results_dttm_tz timestamp, session_start_dttm timestamp, session_start_dttm_tz timestamp, session_complete_load_dttm timestamp NOT NULL, 
      search_results_dttm timestamp, visitor_type string, visitor_id string, visit_origination_type string, 
      visit_origination_tracking_code string, visit_origination_placement string, visit_origination_name string, visit_origination_creative string, 
      visit_id string, session_id string, search_name string, internal_search_term string, 
      device_type string, device_name string, detail_id string NOT NULL, cu_customer_id string, 
      bouncer string
      ) PARTITION BY DATE(session_complete_load_dttm )
      CLUSTER BY SESSION_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DBT_SEARCH
      ADD PRIMARY KEY (DETAIL_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_SEARCH, DBT_SEARCH);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DIRECT_CONTACT (
      control_active_flg string, control_group_flg string, properties_map_doc string, load_dttm timestamp NOT NULL, 
      direct_contact_dttm timestamp, direct_contact_dttm_tz timestamp, task_version_id string, task_id string, 
      segment_id string, response_tracking_cd string, occurrence_id string, message_id string, 
      identity_type_nm string, identity_id string, event_nm string, event_id string NOT NULL, 
      event_designed_id string, context_val string, context_type_nm string, channel_user_id string, 
      channel_nm string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DIRECT_CONTACT
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DIRECT_CONTACT, DIRECT_CONTACT);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DOCUMENT_DETAILS (
      load_dttm timestamp NOT NULL, link_event_dttm timestamp, link_event_dttm_tz timestamp, visit_id_hex string, 
      uri_txt string, session_id string, link_selector_path string, link_id string, 
      link_name string, identity_id string, event_source_cd string, session_id_hex string, 
      event_key_cd string, visit_id string, event_id string NOT NULL, detail_id_hex string, 
      detail_id string, alt_txt string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..DOCUMENT_DETAILS
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DOCUMENT_DETAILS, DOCUMENT_DETAILS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..EMAIL_BOUNCE (
      test_flg string, properties_map_doc string, load_dttm timestamp NOT NULL, email_bounce_dttm_tz timestamp, 
      email_bounce_dttm timestamp, task_id string, subject_line_txt string, segment_version_id string, 
      response_tracking_cd string, reason_txt string, raw_reason_txt string, occurrence_id string, 
      journey_occurrence_id string, imprint_id string, event_nm string, event_designed_id string, 
      context_type_nm string, bounce_class_cd string, aud_occurrence_id string, analysis_group_id string, 
      audience_id string, channel_user_id string, context_val string, event_id string NOT NULL, 
      identity_id string, journey_id string, program_id string, recipient_domain_nm string, 
      segment_id string, task_version_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..EMAIL_BOUNCE
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EMAIL_BOUNCE, EMAIL_BOUNCE);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..EMAIL_CLICK (
      test_flg string, open_tracking_flg string, is_mobile_flg string, click_tracking_flg string, 
      properties_map_doc string, email_click_dttm timestamp, email_click_dttm_tz timestamp, load_dttm timestamp NOT NULL, 
      uri_txt string, task_version_id string, task_id string, subject_line_txt string, 
      segment_id string, recipient_domain_nm string, program_id string, platform_version string, 
      platform_desc string, occurrence_id string, manufacturer_nm string, mailbox_provider_nm string, 
      link_tracking_label_txt string, link_tracking_id string, link_tracking_group_txt string, journey_id string, 
      imprint_id string, event_nm string, event_id string NOT NULL, event_designed_id string, 
      context_val string, audience_id string, analysis_group_id string, agent_family_nm string, 
      aud_occurrence_id string, channel_user_id string, context_type_nm string, device_nm string, 
      identity_id string, journey_occurrence_id string, response_tracking_cd string, segment_version_id string, 
      user_agent_nm string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..EMAIL_CLICK
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EMAIL_CLICK, EMAIL_CLICK);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..EMAIL_COMPLAINT (
      test_flg string, properties_map_doc string, load_dttm timestamp NOT NULL, email_complaint_dttm timestamp, 
      email_complaint_dttm_tz timestamp, task_id string, segment_version_id string, response_tracking_cd string, 
      recipient_domain_nm string, occurrence_id string, journey_occurrence_id string, imprint_id string, 
      event_nm string, event_id string NOT NULL, event_designed_id string, context_type_nm string, 
      audience_id string, analysis_group_id string, aud_occurrence_id string, channel_user_id string, 
      context_val string, identity_id string, journey_id string, program_id string, 
      segment_id string, subject_line_txt string, task_version_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..EMAIL_COMPLAINT
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EMAIL_COMPLAINT, EMAIL_COMPLAINT);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..EMAIL_OPEN (
      prefetched_flg string, click_tracking_flg string, open_tracking_flg string, is_mobile_flg string, 
      test_flg string, properties_map_doc string, email_open_dttm timestamp, email_open_dttm_tz timestamp, 
      load_dttm timestamp NOT NULL, user_agent_nm string, task_version_id string, subject_line_txt string, 
      segment_version_id string, segment_id string, recipient_domain_nm string, program_id string, 
      platform_version string, occurrence_id string, manufacturer_nm string, journey_id string, 
      imprint_id string, event_nm string, event_designed_id string, context_val string, 
      audience_id string, analysis_group_id string, agent_family_nm string, aud_occurrence_id string, 
      channel_user_id string, context_type_nm string, device_nm string, event_id string NOT NULL, 
      identity_id string, journey_occurrence_id string, mailbox_provider_nm string, platform_desc string, 
      response_tracking_cd string, task_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..EMAIL_OPEN
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EMAIL_OPEN, EMAIL_OPEN);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..EMAIL_OPTOUT (
      test_flg string, properties_map_doc string, email_optout_dttm_tz timestamp, email_optout_dttm timestamp, 
      load_dttm timestamp NOT NULL, task_version_id string, subject_line_txt string, segment_id string, 
      recipient_domain_nm string, program_id string, optout_type_nm string, occurrence_id string, 
      link_tracking_label_txt string, link_tracking_group_txt string, journey_id string, identity_id string, 
      event_nm string, event_id string NOT NULL, context_val string, channel_user_id string, 
      audience_id string, aud_occurrence_id string, analysis_group_id string, context_type_nm string, 
      event_designed_id string, imprint_id string, journey_occurrence_id string, link_tracking_id string, 
      response_tracking_cd string, segment_version_id string, task_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..EMAIL_OPTOUT
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EMAIL_OPTOUT, EMAIL_OPTOUT);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..EMAIL_OPTOUT_DETAILS (
      test_flg string, properties_map_doc string, email_action_dttm_tz timestamp, email_action_dttm timestamp, 
      load_dttm timestamp NOT NULL, task_version_id string, subject_line_txt string, segment_id string, 
      recipient_domain_nm string, program_id string, optout_type_nm string, occurrence_id string, 
      journey_occurrence_id string, imprint_id string, event_nm string, event_designed_id string, 
      email_address string, context_val string, audience_id string, analysis_group_id string, 
      aud_occurrence_id string, context_type_nm string, event_id string NOT NULL, identity_id string, 
      journey_id string, response_tracking_cd string, segment_version_id string, task_id string

      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..EMAIL_OPTOUT_DETAILS
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EMAIL_OPTOUT_DETAILS, EMAIL_OPTOUT_DETAILS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..EMAIL_REPLY (
      test_flg string, properties_map_doc string, email_reply_dttm timestamp, email_reply_dttm_tz timestamp, 
      load_dttm timestamp NOT NULL, uri_txt string, task_id string, subject_line_txt string, 
      segment_version_id string, response_tracking_cd string, occurrence_id string, journey_occurrence_id string, 
      imprint_id string, event_nm string, event_designed_id string, context_type_nm string, 
      audience_id string, analysis_group_id string, aud_occurrence_id string, channel_user_id string, 
      context_val string, event_id string NOT NULL, identity_id string, journey_id string, 
      program_id string, recipient_domain_nm string, segment_id string, task_version_id string

      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..EMAIL_REPLY
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EMAIL_REPLY, EMAIL_REPLY);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..EMAIL_SEND (
      test_flg string, properties_map_doc string, load_dttm timestamp NOT NULL, email_send_dttm_tz timestamp, 
      email_send_dttm timestamp, task_version_id string, subject_line_txt string, segment_id string, 
      recipient_domain_nm string, program_id string, journey_id string, imprint_id string, 
      event_nm string, event_designed_id string, context_type_nm string, channel_user_id string, 
      audience_id string, analysis_group_id string, aud_occurrence_id string, context_val string, 
      event_id string NOT NULL, identity_id string, imprint_url_txt string, journey_occurrence_id string, 
      occurrence_id string, response_tracking_cd string, segment_version_id string, task_id string

      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..EMAIL_SEND
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EMAIL_SEND, EMAIL_SEND);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..EMAIL_VIEW (
      test_flg string, properties_map_doc string, load_dttm timestamp NOT NULL, email_view_dttm timestamp, 
      email_view_dttm_tz timestamp, task_version_id string, task_id string, subject_line_txt string, 
      segment_version_id string, segment_id string, response_tracking_cd string, recipient_domain_nm string, 
      program_id string, occurrence_id string, link_tracking_id string, link_tracking_group_txt string, 
      journey_occurrence_id string, imprint_id string, event_nm string, event_designed_id string, 
      context_type_nm string, audience_id string, analysis_group_id string, aud_occurrence_id string, 
      channel_user_id string, context_val string, event_id string NOT NULL, identity_id string, 
      journey_id string, link_tracking_label_txt string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..EMAIL_VIEW
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EMAIL_VIEW, EMAIL_VIEW);
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
   EXECUTE (CREATE TABLE &dbschema..EXTERNAL_EVENT (
      properties_map_doc string, external_event_dttm_tz timestamp, load_dttm timestamp NOT NULL, external_event_dttm timestamp, 
      response_tracking_cd string, identity_id string, event_nm string, event_designed_id string, 
      context_type_nm string, channel_nm string, channel_user_id string, context_val string, 
      event_id string NOT NULL
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..EXTERNAL_EVENT
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EXTERNAL_EVENT, EXTERNAL_EVENT);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..FISCAL_CC_BUDGET (
      cc_obsolete_flg string, fin_accnt_obsolete_flg string, fp_obsolete_flg string, fp_start_dt date, 
      fp_end_dt date, cc_bdgt_invoiced_amt decimal(17,2), cc_bdgt_cmtmnt_outstanding_amt decimal(17,2), cc_bdgt_cmtmnt_invoice_amt decimal(17,2), 
      cc_bdgt_budget_amt decimal(17,2), cc_bdgt_amt decimal(17,2), cc_bdgt_committed_amt decimal(17,2), cc_bdgt_cmtmnt_overspent_amt decimal(17,2), 
      cc_bdgt_direct_invoice_amt decimal(17,2), cc_bdgt_cmtmnt_invoice_cnt int64, last_modified_dttm timestamp, created_dttm timestamp, 
      load_dttm timestamp, gen_ledger_cd string, last_modified_usernm string, fp_nm string, 
      fp_id string, fp_desc string, fp_cls_ver string, fin_accnt_nm string, 
      cost_center_id string, cc_number string, cc_nm string, cc_bdgt_budget_desc string, 
      cc_desc string, cc_owner_usernm string, created_by_usernm string, fin_accnt_desc string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: FISCAL_CC_BUDGET, FISCAL_CC_BUDGET);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..FORM_DETAILS (
      submit_flg string, change_index_no int64, attempt_index_cnt int64, load_dttm timestamp NOT NULL, 
      form_field_detail_dttm_tz timestamp, form_field_detail_dttm timestamp, visit_id string, form_field_nm string, 
      event_source_cd string, detail_id string, attempt_status_cd string, event_id string NOT NULL, 
      form_field_value string, form_nm string, session_id_hex string, detail_id_hex string, 
      event_key_cd string, form_field_id string, identity_id string, session_id string, 
      visit_id_hex string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..FORM_DETAILS
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: FORM_DETAILS, FORM_DETAILS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..IDENTITY_ATTRIBUTES (
      processed_dttm timestamp, entrytime timestamp NOT NULL, identity_id string, user_identifier_val string NOT NULL, 
      identifier_type_id string NOT NULL    )) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..IDENTITY_ATTRIBUTES
      ADD PRIMARY KEY (ENTRYTIME,IDENTIFIER_TYPE_ID,USER_IDENTIFIER_VAL) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: IDENTITY_ATTRIBUTES, IDENTITY_ATTRIBUTES);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..IDENTITY_MAP (
      processed_dttm timestamp, entrytime timestamp, target_identity_id string, source_identity_id string NOT NULL
         )) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..IDENTITY_MAP
      ADD PRIMARY KEY (SOURCE_IDENTITY_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: IDENTITY_MAP, IDENTITY_MAP);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..IMPRESSION_DELIVERED (
      control_group_flg string, product_qty_no int64, properties_map_doc string, impression_delivered_dttm_tz timestamp, 
      impression_delivered_dttm timestamp, load_dttm timestamp NOT NULL, spot_id string, response_tracking_cd string, 
      rec_group_id string, product_nm string, message_id string, event_nm string, 
      detail_id_hex string, context_val string, audience_id string, channel_user_id string, 
      creative_id string, event_id string NOT NULL, identity_id string, journey_occurrence_id string, 
      message_version_id string, mobile_app_id string, product_sku_no string, reserved_1_txt string, 
      segment_version_id string, task_version_id string, visit_id_hex string, aud_occurrence_id string, 
      channel_nm string, context_type_nm string, creative_version_id string, event_designed_id string, 
      event_key_cd string, event_source_cd string, journey_id string, product_id string, 
      request_id string, reserved_2_txt string, segment_id string, session_id_hex string, 
      task_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..IMPRESSION_DELIVERED
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: IMPRESSION_DELIVERED, IMPRESSION_DELIVERED);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..IMPRESSION_SPOT_VIEWABLE (
      control_group_flg string, product_qty_no int64, properties_map_doc string, impression_viewable_dttm_tz timestamp, 
      load_dttm timestamp NOT NULL, impression_viewable_dttm timestamp, visit_id_hex string, session_id_hex string, 
      reserved_2_txt string, product_id string, message_id string, identity_id string, 
      event_id string NOT NULL, creative_id string, channel_user_id string, analysis_group_id string, 
      audience_id string, context_val string, detail_id_hex string, event_nm string, 
      event_source_cd string, mobile_app_id string, rec_group_id string, request_id string, 
      segment_id string, task_id string, aud_occurrence_id string, channel_nm string, 
      context_type_nm string, creative_version_id string, event_designed_id string, event_key_cd string, 
      message_version_id string, occurrence_id string, product_nm string, product_sku_no string, 
      reserved_1_txt string, response_tracking_cd string, segment_version_id string, spot_id string, 
      task_version_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..IMPRESSION_SPOT_VIEWABLE
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: IMPRESSION_SPOT_VIEWABLE, IMPRESSION_SPOT_VIEWABLE);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..INVOICE_DETAILS (
      vendor_obsolete_flg string, invoice_amt decimal(17,2), vendor_amt decimal(17,2), reconcile_amt decimal(17,2), 
      last_modified_dttm timestamp, invoice_reconciled_dttm timestamp, payment_dttm timestamp, load_dttm timestamp, 
      created_dttm timestamp, invoice_created_dttm timestamp, vendor_nm string, planning_id string, 
      last_modified_usernm string, invoice_number string, cmtmnt_nm string, invoice_id string, 
      invoice_status string, vendor_currency_cd string, vendor_desc string, cmtmnt_id string, 
      created_by_usernm string, invoice_desc string, invoice_nm string, plan_currency_cd string, 
      planning_nm string, reconcile_note string, vendor_id string, vendor_number string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: INVOICE_DETAILS, INVOICE_DETAILS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..INVOICE_LINE_ITEMS (
      item_alloc_unit int64, item_vend_alloc_unit int64, item_qty int64, vendor_obsolete_flg string, 
      cc_available_amt decimal(17,2), vendor_amt decimal(17,2), item_alloc_amt decimal(17,2), item_rate decimal(17,2), 
      reconcile_amt decimal(17,2), item_vend_alloc_amt decimal(17,2), invoice_amt decimal(17,2), cc_recon_alloc_amt decimal(17,2), 
      cc_allocated_amt decimal(17,2), item_number int64, payment_dttm timestamp, load_dttm timestamp, 
      invoice_created_dttm timestamp, invoice_reconciled_dttm timestamp, last_modified_dttm timestamp, created_dttm timestamp, 
      vendor_number string, vendor_desc string, vendor_currency_cd string, reconcile_note string, 
      planning_nm string, item_nm string, invoice_nm string, invoice_id string, 
      invoice_desc string, fin_acc_nm string, cost_center_id string, cc_nm string, 
      ccat_nm string, cmtmnt_id string, plan_currency_cd string, vendor_id string, 
      cc_desc string, cc_owner_usernm string, cmtmnt_nm string, created_by_usernm string, 
      fin_acc_ccat_nm string, gen_ledger_cd string, invoice_number string, invoice_status string, 
      last_modified_usernm string, planning_id string, vendor_nm string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: INVOICE_LINE_ITEMS, INVOICE_LINE_ITEMS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..INVOICE_LINE_ITEMS_CCBDGT (
      cc_obsolete_flg string, vendor_obsolete_flg string, fp_obsolete_flg string, fp_start_dt date, 
      fp_end_dt date, cc_bdgt_cmtmnt_overspent_amt decimal(17,2), reconcile_amt decimal(17,2), vendor_amt decimal(17,2), 
      item_alloc_amt decimal(17,2), cc_bdgt_cmtmnt_outstanding_amt decimal(17,2), invoice_amt decimal(17,2), item_rate decimal(17,2), 
      cc_recon_alloc_amt decimal(17,2), cc_bdgt_direct_invoice_amt decimal(17,2), cc_available_amt decimal(17,2), cc_bdgt_invoiced_amt decimal(17,2), 
      item_vend_alloc_amt decimal(17,2), cc_bdgt_committed_amt decimal(17,2), cc_bdgt_budget_amt decimal(17,2), cc_bdgt_cmtmnt_invoice_amt decimal(17,2), 
      cc_bdgt_amt decimal(17,2), cc_allocated_amt decimal(17,2), item_qty int64, item_number int64, 
      item_vend_alloc_unit int64, item_alloc_unit int64, cc_bdgt_cmtmnt_invoice_cnt int64, created_dttm timestamp, 
      invoice_created_dttm timestamp, invoice_reconciled_dttm timestamp, last_modified_dttm timestamp, payment_dttm timestamp, 
      load_dttm timestamp, vendor_id string, reconcile_note string, planning_nm string, 
      plan_currency_cd string, invoice_nm string, fp_nm string, fp_cls_ver string, 
      created_by_usernm string, ccat_nm string, cc_number string, cc_bdgt_budget_desc string, 
      cc_desc string, cc_owner_usernm string, cmtmnt_nm string, fin_acc_ccat_nm string, 
      invoice_desc string, item_nm string, vendor_currency_cd string, vendor_number string, 
      cc_nm string, cmtmnt_id string, cost_center_id string, fin_acc_nm string, 
      fp_desc string, fp_id string, gen_ledger_cd string, invoice_id string, 
      invoice_number string, invoice_status string, last_modified_usernm string, planning_id string, 
      vendor_desc string, vendor_nm string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: INVOICE_LINE_ITEMS_CCBDGT, INVOICE_LINE_ITEMS_CCBDGT);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..IN_APP_FAILED (
      properties_map_doc string, in_app_failed_dttm timestamp, in_app_failed_dttm_tz timestamp, load_dttm timestamp NOT NULL, 
      task_version_id string, segment_id string, message_id string, identity_id string, 
      error_message_txt string, context_val string, channel_user_id string, context_type_nm string, 
      creative_version_id string, event_id string NOT NULL, mobile_app_id string, reserved_2_txt string, 
      spot_id string, channel_nm string, creative_id string, error_cd string, 
      event_designed_id string, event_nm string, message_version_id string, occurrence_id string, 
      reserved_1_txt string, response_tracking_cd string, segment_version_id string, task_id string

      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..IN_APP_FAILED
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: IN_APP_FAILED, IN_APP_FAILED);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..IN_APP_MESSAGE (
      properties_map_doc string, in_app_action_dttm_tz timestamp, load_dttm timestamp NOT NULL, in_app_action_dttm timestamp, 
      segment_version_id string, reserved_2_txt string, mobile_app_id string, event_id string NOT NULL, 
      context_val string, channel_user_id string, creative_version_id string, identity_id string, 
      message_id string, response_tracking_cd string, task_id string, channel_nm string, 
      context_type_nm string, creative_id string, event_designed_id string, event_nm string, 
      message_version_id string, occurrence_id string, reserved_1_txt string, reserved_3_txt string, 
      segment_id string, spot_id string, task_version_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..IN_APP_MESSAGE
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: IN_APP_MESSAGE, IN_APP_MESSAGE);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..IN_APP_SEND (
      properties_map_doc string, load_dttm timestamp NOT NULL, in_app_send_dttm_tz timestamp, in_app_send_dttm timestamp, 
      task_id string, response_tracking_cd string, occurrence_id string, message_id string, 
      event_nm string, creative_id string, channel_nm string, context_type_nm string, 
      creative_version_id string, event_designed_id string, message_version_id string, reserved_1_txt string, 
      segment_version_id string, channel_user_id string, context_val string, event_id string NOT NULL, 
      identity_id string, mobile_app_id string, reserved_2_txt string, segment_id string, 
      spot_id string, task_version_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..IN_APP_SEND
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: IN_APP_SEND, IN_APP_SEND);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..IN_APP_TARGETING_REQUEST (
      eligibility_flg string, in_app_tgt_request_dttm timestamp, load_dttm timestamp NOT NULL, in_app_tgt_request_dttm_tz timestamp, 
      event_id string NOT NULL, context_type_nm string, channel_nm string, event_designed_id string, 
      identity_id string, mobile_app_id string, channel_user_id string, context_val string, 
      event_nm string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..IN_APP_TARGETING_REQUEST
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: IN_APP_TARGETING_REQUEST, IN_APP_TARGETING_REQUEST);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..JOURNEY_ENTRY (
      entry_dttm timestamp, entry_dttm_tz timestamp, load_dttm timestamp NOT NULL, journey_occurrence_id string, 
      identity_id string, aud_occurrence_id string, audience_id string, context_type_nm string, 
      event_id string NOT NULL, identity_type_val string, context_val string, event_nm string, 
      identity_type_nm string, journey_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..JOURNEY_ENTRY
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: JOURNEY_ENTRY, JOURNEY_ENTRY);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..JOURNEY_EXIT (
      exit_dttm timestamp, exit_dttm_tz timestamp, load_dttm timestamp NOT NULL, last_node_id string, 
      identity_type_nm string, context_type_nm string, aud_occurrence_id string, event_id string NOT NULL, 
      group_id string, journey_id string, reason_cd string, audience_id string, 
      context_val string, event_nm string, identity_id string, identity_type_val string, 
      journey_occurrence_id string, reason_txt string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..JOURNEY_EXIT
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: JOURNEY_EXIT, JOURNEY_EXIT);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..JOURNEY_HOLDOUT (
      holdout_dttm_tz timestamp, load_dttm timestamp NOT NULL, holdout_dttm timestamp, journey_occurrence_id string, 
      journey_id string, identity_type_val string, identity_type_nm string, identity_id string, 
      event_nm string, event_id string NOT NULL, context_val string, context_type_nm string, 
      audience_id string, aud_occurrence_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..JOURNEY_HOLDOUT
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: JOURNEY_HOLDOUT, JOURNEY_HOLDOUT);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..JOURNEY_NODE_ENTRY (
      node_entry_dttm timestamp, load_dttm timestamp NOT NULL, node_entry_dttm_tz timestamp, node_type_nm string, 
      node_id string, previous_node_id string, journey_occurrence_id string, journey_id string, 
      identity_type_val string, identity_type_nm string, identity_id string, group_id string, 
      event_nm string, event_id string NOT NULL, context_val string, context_type_nm string, 
      audience_id string, aud_occurrence_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..JOURNEY_NODE_ENTRY
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: JOURNEY_NODE_ENTRY, JOURNEY_NODE_ENTRY);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..JOURNEY_SUCCESS (
      unit_qty int64, success_val int64, success_dttm timestamp, load_dttm timestamp NOT NULL, 
      success_dttm_tz timestamp, parent_event_designed_id string, journey_id string, identity_type_nm string, 
      group_id string, event_id string NOT NULL, context_type_nm string, audience_id string, 
      aud_occurrence_id string, context_val string, event_nm string, identity_id string, 
      identity_type_val string, journey_occurrence_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..JOURNEY_SUCCESS
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: JOURNEY_SUCCESS, JOURNEY_SUCCESS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..JOURNEY_SUPPRESSION (
      load_dttm timestamp NOT NULL, suppression_dttm timestamp, suppression_dttm_tz timestamp, reason_txt string, 
      reason_cd string, journey_occurrence_id string, identity_type_val string, identity_type_nm string, 
      identity_id string, event_id string NOT NULL, context_type_nm string, audience_id string, 
      aud_occurrence_id string, context_val string, event_nm string, journey_id string

      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..JOURNEY_SUPPRESSION
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: JOURNEY_SUPPRESSION, JOURNEY_SUPPRESSION);
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
   EXECUTE (CREATE TABLE &dbschema..MD_ACTIVITY (
      valid_to_dttm timestamp, last_published_dttm timestamp, valid_from_dttm timestamp, business_context_id string, 
      activity_version_id string, activity_status_cd string, activity_id string, activity_desc string, 
      activity_cd string, activity_category_nm string, activity_nm string, folder_path_nm string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY, MD_ACTIVITY);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_ACTIVITY_ABTESTPATH (
      next_node_val STRING, abtest_dist_pct string, control_flg string, valid_to_dttm timestamp, 
      valid_from_dttm timestamp, activity_version_id string, activity_status_cd string, activity_id string, 
      abtest_path_id string, abtest_path_nm string, activity_node_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_ABTESTPATH, MD_ACTIVITY_ABTESTPATH);
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
   EXECUTE (CREATE TABLE &dbschema..MD_ACTIVITY_CUSTOM_PROP (
      valid_to_dttm timestamp, valid_from_dttm timestamp, property_val string, property_nm string, 
      property_datatype_cd string, activity_version_id string, activity_status_cd string, activity_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_CUSTOM_PROP, MD_ACTIVITY_CUSTOM_PROP);
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
   EXECUTE (CREATE TABLE &dbschema..MD_ACTIVITY_NODE (
      next_node_val STRING, previous_node_val STRING, wait_tm int64, end_node_flg string, 
      time_boxed_flg string, specific_wait_flg string, start_node_flg string, node_sequence_no int64, 
      valid_from_dttm timestamp, valid_to_dttm timestamp, activity_version_id string, activity_status_cd string, 
      activity_node_type_nm string, activity_node_nm string, activity_node_id string, activity_id string, 
      abtest_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_NODE, MD_ACTIVITY_NODE);
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
   EXECUTE (CREATE TABLE &dbschema..MD_ACTIVITY_X_ACTIVITY_NODE (
      activity_version_id string, activity_status_cd string, activity_node_id string, activity_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_X_ACTIVITY_NODE, MD_ACTIVITY_X_ACTIVITY_NODE);
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
   EXECUTE (CREATE TABLE &dbschema..MD_ACTIVITY_X_TASK (
      task_version_id string, task_id string, activity_version_id string, activity_status_cd string, 
      activity_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_X_TASK, MD_ACTIVITY_X_TASK);
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
   EXECUTE (CREATE TABLE &dbschema..MD_ASSET (
      valid_to_dttm timestamp, valid_from_dttm timestamp, last_published_dttm timestamp, owner_nm string, 
      created_user_nm string, asset_version_id string, asset_type_nm string, asset_status_cd string, 
      asset_nm string, asset_id string, asset_desc string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ASSET, MD_ASSET);
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
   EXECUTE (CREATE TABLE &dbschema..MD_AUDIENCE (
      audience_schedule_flg string, audience_expiration_val int64, update_dttm timestamp, create_dttm timestamp, 
      delete_dttm timestamp, created_user_nm string, audience_source_nm string, audience_nm string, 
      audience_id string, audience_desc string, audience_data_source_nm string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_AUDIENCE, MD_AUDIENCE);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_AUDIENCE_OCCURRENCE (
      audience_size_val int64, update_dttm timestamp, end_tm timestamp, start_tm timestamp, 
      started_by_nm string, occurrence_type_nm string, execution_status_cd string, audience_id string, 
      aud_occurrence_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_AUDIENCE_OCCURRENCE, MD_AUDIENCE_OCCURRENCE);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_AUDIENCE_X_SEGMENT (
      segment_id string, audience_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_AUDIENCE_X_SEGMENT, MD_AUDIENCE_X_SEGMENT);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_BU (
      bu_obsolete_flg string, last_modified_dttm timestamp, created_dttm timestamp, load_dttm timestamp, 
      last_modified_usernm string, created_by_usernm string, bu_parentid string, bu_owner_usernm string, 
      bu_nm string, bu_id string, bu_desc string, bu_currency_cd string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_BU, MD_BU);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_BUSINESS_CONTEXT (
      valid_to_dttm timestamp, valid_from_dttm timestamp, last_published_dttm timestamp, owner_nm string, 
      locked_information_map_nm string, information_map_nm string, created_user_nm string, business_context_version_id string, 
      business_context_status_cd string, business_context_src_cd string, business_context_nm string, business_context_id string, 
      business_context_desc string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_BUSINESS_CONTEXT, MD_BUSINESS_CONTEXT);
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
   EXECUTE (CREATE TABLE &dbschema..MD_COSTCENTER (
      cc_obsolete_flg string, load_dttm timestamp, created_dttm timestamp, last_modified_dttm timestamp, 
      last_modified_usernm string, gen_ledger_cd string, fin_accnt_nm string, created_by_usernm string, 
      cost_center_id string, cc_owner_usernm string, cc_nm string, cc_desc string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_COSTCENTER, MD_COSTCENTER);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_COST_CATEGORY (
      ccat_obsolete_flg string, load_dttm timestamp, created_dttm timestamp, last_modified_dttm timestamp, 
      last_modified_usernm string, gen_ledger_cd string, fin_accnt_nm string, created_by_usernm string, 
      ccat_owner_usernm string, ccat_nm string, ccat_id string, ccat_desc string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_COST_CATEGORY, MD_COST_CATEGORY);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_CREATIVE (
      last_published_dttm timestamp, valid_to_dttm timestamp, valid_from_dttm timestamp, recommender_template_nm string, 
      recommender_template_id string, owner_nm string, folder_path_nm string, creative_version_id string, 
      creative_type_nm string, creative_txt string, creative_status_cd string, creative_nm string, 
      creative_id string, creative_desc string, creative_cd string, creative_category_nm string, 
      created_user_nm string, business_context_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_CREATIVE, MD_CREATIVE);
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
   EXECUTE (CREATE TABLE &dbschema..MD_CREATIVE_CUSTOM_PROP (
      valid_to_dttm timestamp, valid_from_dttm timestamp, property_val string, property_nm string, 
      property_datatype_cd string, creative_version_id string, creative_status_cd string, creative_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_CREATIVE_CUSTOM_PROP, MD_CREATIVE_CUSTOM_PROP);
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
   EXECUTE (CREATE TABLE &dbschema..MD_CREATIVE_X_ASSET (
      creative_version_id string, creative_status_cd string, creative_id string, asset_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_CREATIVE_X_ASSET, MD_CREATIVE_X_ASSET);
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
   EXECUTE (CREATE TABLE &dbschema..MD_CUSTATTRIB_TABLE_VALUES (
      is_obsolete_flg string, load_dttm timestamp, last_modified_dttm timestamp, created_dttm timestamp, 
      table_val string, last_modified_usernm string, data_type string, data_formatter string, 
      created_by_usernm string, attr_nm string, attr_id string, attr_group_nm string, 
      attr_group_id string, attr_group_cd string, attr_cd string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_CUSTATTRIB_TABLE_VALUES, MD_CUSTATTRIB_TABLE_VALUES);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_CUST_ATTRIB (
      is_obsolete_flg string, is_grid_flg string, load_dttm timestamp, last_modified_dttm timestamp, 
      created_dttm timestamp, remote_pklist_tab_col string, last_modified_usernm string, data_type string, 
      data_formatter string, created_by_usernm string, attr_nm string, attr_id string, 
      attr_group_nm string, attr_group_id string, attr_group_cd string, attr_cd string, 
      associated_grid string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_CUST_ATTRIB, MD_CUST_ATTRIB);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_DATAVIEW (
      include_external_flg string, include_internal_flg string, analytic_active_flg string, max_path_time_val int64, 
      analytics_period_val int64, max_path_length_val int64, half_life_time_val int64, last_published_dttm timestamp, 
      valid_from_dttm timestamp, valid_to_dttm timestamp, selected_task_list string, owner_nm string, 
      max_path_time_type_nm string, dataview_version_id string, dataview_status_cd string, dataview_nm string, 
      dataview_id string, dataview_desc string, custom_recent_exclude_cd string, custom_recent_cd string, 
      created_user_nm string, analytics_period_type_nm string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_DATAVIEW, MD_DATAVIEW);
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
   EXECUTE (CREATE TABLE &dbschema..MD_DATAVIEW_X_EVENT (
      event_id string, dataview_version_id string, dataview_status_cd string, dataview_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_DATAVIEW_X_EVENT, MD_DATAVIEW_X_EVENT);
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
   EXECUTE (CREATE TABLE &dbschema..MD_EVENT (
      valid_from_dttm timestamp, last_published_dttm timestamp, valid_to_dttm timestamp, owner_nm string, 
      event_version_id string, event_type_nm string, event_subtype_nm string, event_status_cd string, 
      event_nm string, event_id string, event_desc string, created_user_nm string, 
      channel_nm string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_EVENT, MD_EVENT);
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
   EXECUTE (CREATE TABLE &dbschema..MD_FISCAL_PERIOD (
      fp_obsolete_flg string, fp_end_dt date, fp_start_dt date, last_modified_dttm timestamp, 
      created_dttm timestamp, load_dttm timestamp, last_modified_usernm string, fp_nm string, 
      fp_id string, fp_desc string, fp_cls_ver string, created_by_usernm string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_FISCAL_PERIOD, MD_FISCAL_PERIOD);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_GRID_ATTR_DEFN (
      attr_obsolete_flg string, grid_mandatory_flg string, grid_obsolete_flg string, attr_order_no int64, 
      load_dttm timestamp, last_modified_dttm timestamp, created_dttm timestamp, remote_pklist_tab_col string, 
      last_modified_usernm string, grid_nm string, grid_id string, grid_desc string, 
      grid_cd string, data_type string, data_formatter string, created_by_usernm string, 
      attr_nm string, attr_id string, attr_group_nm string, attr_group_id string, 
      attr_group_cd string, attr_desc string, attr_cd string, associated_grid string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_GRID_ATTR_DEFN, MD_GRID_ATTR_DEFN);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_JOURNEY (
      control_group_flg string, target_goal_qty int64, last_activated_dttm timestamp, test_type_nm string, 
      target_goal_type_nm string, purpose_id string, journey_version_id string, journey_status_cd string, 
      journey_nm string, journey_id string, created_user_nm string, activated_user_nm string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY, MD_JOURNEY);
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
   EXECUTE (CREATE TABLE &dbschema..MD_JOURNEY_NODE (
      previous_node_id string, node_type string, node_nm string, next_node_id string, 
      journey_version_id string, journey_node_id string, journey_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_NODE, MD_JOURNEY_NODE);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_JOURNEY_NODE_OCCURRENCE (
      num_of_contacts_entered int64, end_dttm timestamp, start_dttm timestamp, journey_version_id string, 
      journey_occurrence_id string, journey_node_occurrence_id string, journey_node_id string, journey_id string, 
      group_id string, execution_status string, error_messages string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_NODE_OCCURRENCE, MD_JOURNEY_NODE_OCCURRENCE);
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
   EXECUTE (CREATE TABLE &dbschema..MD_JOURNEY_OCCURRENCE (
      num_of_contacts_entered int64, num_of_contacts_suppressed int64, journey_occurrence_num int64, start_dttm timestamp, 
      end_dttm timestamp, started_by_nm string, occurrence_type_nm string, journey_version_id string, 
      journey_occurrence_id string, journey_id string, execution_status string, error_messages string, 
      aud_occurrence_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_OCCURRENCE, MD_JOURNEY_OCCURRENCE);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_JOURNEY_X_AUDIENCE (
      journey_version_id string, journey_node_id string, journey_id string, audience_id string, 
      aud_relationship_nm string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_X_AUDIENCE, MD_JOURNEY_X_AUDIENCE);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_JOURNEY_X_EVENT (
      journey_version_id string, journey_node_id string, journey_id string, event_relationship_nm string, 
      event_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_X_EVENT, MD_JOURNEY_X_EVENT);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_JOURNEY_X_TASK (
      task_version_id string, task_id string, journey_version_id string, journey_node_id string, 
      journey_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_X_TASK, MD_JOURNEY_X_TASK);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_MESSAGE (
      valid_to_dttm timestamp, valid_from_dttm timestamp, last_published_dttm timestamp, owner_nm string, 
      message_version_id string, message_type_nm string, message_status_cd string, message_nm string, 
      message_id string, message_desc string, message_cd string, message_category_nm string, 
      folder_path_nm string, created_user_nm string, business_context_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_MESSAGE, MD_MESSAGE);
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
   EXECUTE (CREATE TABLE &dbschema..MD_MESSAGE_CUSTOM_PROP (
      valid_from_dttm timestamp, valid_to_dttm timestamp, property_val string, property_datatype_cd string, 
      message_status_cd string, message_id string, message_version_id string, property_nm string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_MESSAGE_CUSTOM_PROP, MD_MESSAGE_CUSTOM_PROP);
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
   EXECUTE (CREATE TABLE &dbschema..MD_MESSAGE_X_CREATIVE (
      message_version_id string, message_id string, creative_id string, message_status_cd string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_MESSAGE_X_CREATIVE, MD_MESSAGE_X_CREATIVE);
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
   EXECUTE (CREATE TABLE &dbschema..MD_OBJECT_TYPE (
      is_obsolete_flg string, created_dttm timestamp, last_modified_dttm timestamp, load_dttm timestamp, 
      object_type string, object_category string, last_modified_usernm string, data_type string, 
      data_formatter string, attr_nm string, attr_id string, attr_group_nm string, 
      attr_group_cd string, attr_cd string, attr_group_id string, created_by_usernm string, 
      remote_pklist_tab_col string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_OBJECT_TYPE, MD_OBJECT_TYPE);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_OCCURRENCE (
      occurrence_no int64, properties_map_doc string, start_tm timestamp, end_tm timestamp, 
      started_by_nm string, occurrence_type_nm string, object_version_id string, object_id string, 
      execution_status_cd string, object_type_nm string, occurrence_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_OCCURRENCE, MD_OCCURRENCE);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_PICKLIST (
      is_obsolete_flg string, last_modified_dttm timestamp, created_dttm timestamp, load_dttm timestamp, 
      plist_val string, plist_id string, plist_cd string, last_modified_usernm string, 
      created_by_usernm string, attr_id string, attr_group_id string, attr_cd string, 
      attr_group_nm string, attr_nm string, plist_desc string, plist_nm string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_PICKLIST, MD_PICKLIST);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_PURPOSE (
      purpose_nm string, purpose_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_PURPOSE, MD_PURPOSE);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_RTC (
      occurrence_no int64, content_map_doc string, rtc_dttm timestamp, task_id string, 
      segment_id string, rtc_id string, occurrence_id string, segment_version_id string, 
      task_version_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_RTC, MD_RTC);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT (
      last_published_dttm timestamp, valid_from_dttm timestamp, valid_to_dttm timestamp, segment_version_id string, 
      segment_status_cd string, segment_nm string, segment_map_id string, segment_id string, 
      segment_cd string, owner_nm string, folder_path_nm string, created_user_nm string, 
      business_context_id string, segment_category_nm string, segment_desc string, segment_src_cd string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT, MD_SEGMENT);
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
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_CUSTOM_PROP (
      valid_to_dttm timestamp, valid_from_dttm timestamp, segment_version_id string, segment_status_cd string, 
      property_val string, property_datatype_cd string, property_nm string, segment_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_CUSTOM_PROP, MD_SEGMENT_CUSTOM_PROP);
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
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_MAP (
      scheduled_flg string, recurrence_day_of_month_no int64, valid_to_dttm timestamp, last_published_dttm timestamp, 
      valid_from_dttm timestamp, rec_scheduled_end_dttm timestamp, rec_scheduled_start_dttm timestamp, scheduled_end_dttm timestamp, 
      scheduled_start_dttm timestamp, segment_map_version_id string, segment_map_src_cd string, segment_map_nm string, 
      segment_map_id string, segment_map_cd string, segment_map_category_nm string, recurrence_monthly_type_nm string, 
      recurrence_frequency_cd string, recurrence_day_of_wk_ordinal_no string, recurrence_day_of_week_txt string, rec_scheduled_start_tm string, 
      owner_nm string, folder_path_nm string, business_context_id string, created_user_nm string, 
      recurrence_days_of_week_txt string, segment_map_desc string, segment_map_status_cd string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_MAP, MD_SEGMENT_MAP);
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
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_MAP_CUSTOM_PROP (
      valid_from_dttm timestamp, valid_to_dttm timestamp, segment_map_status_cd string, property_val string, 
      property_nm string, property_datatype_cd string, segment_map_id string, segment_map_version_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_MAP_CUSTOM_PROP, MD_SEGMENT_MAP_CUSTOM_PROP);
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
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_MAP_X_SEGMENT (
      segment_version_id string, segment_map_status_cd string, segment_id string, segment_map_id string, 
      segment_map_version_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_MAP_X_SEGMENT, MD_SEGMENT_MAP_X_SEGMENT);
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
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_TEST (
      test_enabled_flg string, stratified_sampling_flg string, test_pct decimal(5,2), test_cnt int64, 
      test_type_nm string, test_sizing_type_nm string, test_nm string, test_cd string, 
      task_id string, stratified_samp_criteria_txt string, task_version_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_TEST, MD_SEGMENT_TEST);
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
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_TEST_X_SEGMENT (
      task_version_id string, segment_id string, task_id string, test_cd string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_TEST_X_SEGMENT, MD_SEGMENT_TEST_X_SEGMENT);
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
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_X_EVENT (
      segment_status_cd string, event_id string, segment_id string, segment_version_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_X_EVENT, MD_SEGMENT_X_EVENT);
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
   EXECUTE (CREATE TABLE &dbschema..MD_SPOT (
      location_selector_flg string, multi_page_flg string, last_published_dttm timestamp, valid_from_dttm timestamp, 
      valid_to_dttm timestamp, spot_width_val_no string, spot_type_nm string, spot_nm string, 
      spot_id string, spot_desc string, owner_nm string, dimension_label_txt string, 
      channel_nm string, created_user_nm string, height_width_ratio_val_txt string, spot_height_val_no string, 
      spot_key string, spot_status_cd string, spot_version_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SPOT, MD_SPOT);
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
   EXECUTE (CREATE TABLE &dbschema..MD_TARGET_ASSIST (
      use_targeting_flg string, threshold_type_nm string, percent_target_population_size int64, last_modified_dttm timestamp, 
      model_available_dttm timestamp, task_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TARGET_ASSIST, MD_TARGET_ASSIST);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK (
      activity_flg string, scheduled_flg string, export_template_flg string, rtdm_flg string, 
      use_modeling_flg string, recurring_schedule_flg string, segment_tests_flg string, impressions_life_time_cnt int64, 
      test_duration int64, limit_period_unit_cnt int64, impressions_per_session_cnt int64, display_priority_no int64, 
      recurrence_day_of_month_no int64, impressions_qty_period_cnt int64, maximum_period_expression_cnt int64, valid_from_dttm timestamp, 
      scheduled_start_dttm timestamp, last_published_dttm timestamp, valid_to_dttm timestamp, rec_scheduled_start_dttm timestamp, 
      model_start_dttm timestamp, scheduled_end_dttm timestamp, rec_scheduled_end_dttm timestamp, task_version_id string, 
      task_subtype_nm string, task_nm string, task_desc string, task_cd string, 
      subject_line_txt string, stratified_sampling_action_nm string, send_notification_locale_cd string, secondary_status string, 
      recurrence_frequency_cd string, recurrence_day_of_wk_ordinal_no string, recurrence_day_of_week_txt string, rec_scheduled_start_tm string, 
      period_type_nm string, owner_nm string, mobile_app_id string, folder_path_nm string, 
      delivery_config_type_nm string, control_group_action_nm string, business_context_id string, arbitration_method_cd string, 
      channel_nm string, created_user_nm string, mobile_app_nm string, recurrence_days_of_week_txt string, 
      recurrence_monthly_type_nm string, subject_line_source_nm string, task_category_nm string, task_delivery_type_nm string, 
      task_id string, task_status_cd string, task_type_nm string, template_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK, MD_TASK);
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
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_CUSTOM_PROP (
      valid_from_dttm timestamp, valid_to_dttm timestamp, task_status_cd string, task_id string, 
      property_val string, property_datatype_nm string, property_nm string, task_version_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_CUSTOM_PROP, MD_TASK_CUSTOM_PROP);
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
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_AUDIENCE (
      audience_id string, task_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_AUDIENCE, MD_TASK_X_AUDIENCE);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_CREATIVE (
      variant_nm string, task_version_id string, task_id string, creative_id string, 
      arbitration_method_cd string, arbitration_method_val string, spot_id string, task_status_cd string, 
      variant_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_CREATIVE, MD_TASK_X_CREATIVE);
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
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_DATAVIEW (
      targeting_flg string, primary_metric_flg string, secondary_metric_flg string, task_version_id string, 
      task_id string, dataview_id string, task_status_cd string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_DATAVIEW, MD_TASK_X_DATAVIEW);
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
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_EVENT (
      targeting_flg string, primary_metric_flg string, secondary_metric_flg string, task_version_id string, 
      task_id string, event_id string, task_status_cd string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_EVENT, MD_TASK_X_EVENT);
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
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_MESSAGE (
      task_version_id string, task_status_cd string, message_id string, task_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_MESSAGE, MD_TASK_X_MESSAGE);
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
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_SEGMENT (
      task_version_id string, task_status_cd string, segment_id string, task_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_SEGMENT, MD_TASK_X_SEGMENT);
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
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_SPOT (
      task_status_cd string, spot_id string, task_id string, task_version_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_SPOT, MD_TASK_X_SPOT);
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
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_VARIANT (
      variant_type_nm string, variant_nm string, task_status_cd string, analysis_group_id string, 
      task_id string, task_version_id string, variant_source_nm string, variant_val string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_VARIANT, MD_TASK_X_VARIANT);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_VARIANT_ALL (
      variant_type_nm string, variant_nm string, task_status_cd string, analysis_group_id string, 
      task_id string, task_version_id string, variant_source_nm string, variant_val string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_VARIANT_ALL, MD_TASK_X_VARIANT_ALL);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_VENDOR (
      is_obsolete_flg string, last_modified_dttm timestamp, load_dttm timestamp, created_dttm timestamp, 
      vendor_number string, vendor_nm string, vendor_desc string, owner_usernm string, 
      last_modified_usernm string, created_by_usernm string, vendor_currency_cd string, vendor_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_VENDOR, MD_VENDOR);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_WF_PROCESS_DEF (
      version_num int64, file_tobecatlgd_flg string, default_approval_flg string, buildin_template_flg string, 
      latest_version_flg string, last_modified_dttm timestamp, load_dttm timestamp, created_dttm timestamp, 
      pdef_state string, pdef_nm string, pdef_id string, owner_usernm string, 
      last_modified_usernm string, engine_pdef_key string, engine_pdef_id string, created_by_usernm string, 
      associated_object_type string, pdef_desc string, pdef_type string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_WF_PROCESS_DEF, MD_WF_PROCESS_DEF);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_WF_PROCESS_DEF_ATTR_GRP (
      load_dttm timestamp, pdef_id string, attr_group_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_WF_PROCESS_DEF_ATTR_GRP, MD_WF_PROCESS_DEF_ATTR_GRP);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_WF_PROCESS_DEF_CATEGORIES (
      default_category_flg string, load_dttm timestamp, pdef_id string, category_type string, 
      category_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_WF_PROCESS_DEF_CATEGORIES, MD_WF_PROCESS_DEF_CATEGORIES);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_WF_PROCESS_DEF_TASKS (
      default_duration_perassignee int64, file_enabled_flg string, is_sequential_flg string, outgoing_flow_flg string, 
      resp_enabled_flg string, show_workflowlink_flg string, ciobject_enabled_flg string, file_mandatory_flg string, 
      url_enabled_flg string, multiple_asgnsuprt_flg string, comment_mandatory_flg string, comment_enabled_flg string, 
      resp_file_enabled_flg string, res_mandatory_flg string, show_sourceitemlink_flg string, load_dttm timestamp, 
      task_type string, task_subtype string, task_instruction string, task_desc string, 
      source_item_field string, predecessor_task_id string, pdef_id string, item_approval_state string, 
      assignee_type string, task_id string, task_nm string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_WF_PROCESS_DEF_TASKS, MD_WF_PROCESS_DEF_TASKS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_WF_PROCESS_DEF_TASK_ASSG (
      load_dttm timestamp, pdef_id string, assignee_type string, assignee_id string, 
      assignee_duration string, assignee_instruction string, task_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_WF_PROCESS_DEF_TASK_ASSG, MD_WF_PROCESS_DEF_TASK_ASSG);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MEDIA_ACTIVITY_DETAILS (
      action_dttm timestamp, action_dttm_tz timestamp, load_dttm timestamp NOT NULL, playhead_position string, 
      media_nm string, event_id string NOT NULL, detail_id string, action string, 
      detail_id_hex string, media_uri_txt string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY detail_id_hex
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..MEDIA_ACTIVITY_DETAILS
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MEDIA_ACTIVITY_DETAILS, MEDIA_ACTIVITY_DETAILS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MEDIA_DETAILS (
      media_duration_secs decimal(11,3), load_dttm timestamp NOT NULL, play_start_dttm_tz timestamp, play_start_dttm timestamp, 
      visit_id_hex string, visit_id string, session_id_hex string, session_id string, 
      media_uri_txt string, media_player_nm string, media_nm string, identity_id string, 
      event_key_cd string, detail_id_hex string, detail_id string, event_id string NOT NULL, 
      event_source_cd string, media_player_version_txt string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..MEDIA_DETAILS
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MEDIA_DETAILS, MEDIA_DETAILS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MEDIA_DETAILS_EXT (
      media_display_duration_secs decimal(11,3), view_duration_secs decimal(11,3), end_tm decimal(11,3), start_tm decimal(11,3), 
      exit_point_secs decimal(11,3), max_play_secs decimal(11,3), interaction_cnt int64, play_end_dttm timestamp, 
      play_end_dttm_tz timestamp, load_dttm timestamp NOT NULL, media_uri_txt string, media_nm string, 
      event_id string NOT NULL, detail_id_hex string, detail_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY detail_id_hex
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..MEDIA_DETAILS_EXT
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MEDIA_DETAILS_EXT, MEDIA_DETAILS_EXT);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MOBILE_FOCUS_DEFOCUS (
      action_dttm_tz timestamp, action_dttm timestamp, load_dttm timestamp NOT NULL, visit_id_hex string, 
      session_id_hex string, reserved_1_txt string, mobile_app_id string, identity_id string, 
      event_nm string, event_designed_id string, detail_id_hex string, channel_user_id string, 
      event_id string NOT NULL
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..MOBILE_FOCUS_DEFOCUS
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MOBILE_FOCUS_DEFOCUS, MOBILE_FOCUS_DEFOCUS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MOBILE_SPOTS (
      action_dttm_tz timestamp, action_dttm timestamp, load_dttm timestamp NOT NULL, visit_id_hex string, 
      spot_id string, session_id_hex string, mobile_app_id string, event_nm string, 
      event_designed_id string, detail_id_hex string, creative_id string, context_type_nm string, 
      channel_user_id string, context_val string, event_id string NOT NULL, identity_id string

      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..MOBILE_SPOTS
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MOBILE_SPOTS, MOBILE_SPOTS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MONTHLY_USAGE (
      api_usage_str STRING, bc_subjcnt_str STRING, customer_profiles_processed_str STRING, web_impr_cnt int64, 
      web_sesn_cnt int64, mob_sesn_cnt int64, email_preview_cnt int64, outbound_api_cnt int64, 
      facebook_ads_cnt int64, mobile_push_cnt int64, google_ads_cnt int64, audience_usage_cnt int64, 
      plan_users_cnt int64, email_send_cnt int64, linkedin_ads_cnt int64, dm_destinations_total_row_cnt int64, 
      mob_impr_cnt int64, dm_destinations_total_id_cnt int64, mobile_in_app_msg_cnt int64, asset_size decimal(17,2), 
      db_size decimal(17,2), admin_user_cnt int64, event_month string NOT NULL    )) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..MONTHLY_USAGE
      ADD PRIMARY KEY (EVENT_MONTH) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MONTHLY_USAGE, MONTHLY_USAGE);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..NOTIFICATION_FAILED (
      properties_map_doc string, notification_failed_dttm timestamp, notification_failed_dttm_tz timestamp, load_dttm timestamp NOT NULL, 
      task_id string, segment_version_id string, segment_id string, response_tracking_cd string, 
      occurrence_id string, message_version_id string, journey_id string, event_designed_id string, 
      creative_id string, channel_user_id string, channel_nm string, aud_occurrence_id string, 
      context_type_nm string, error_cd string, event_id string NOT NULL, event_nm string, 
      message_id string, mobile_app_id string, reserved_1_txt string, audience_id string, 
      context_val string, creative_version_id string, error_message_txt string, identity_id string, 
      journey_occurrence_id string, reserved_2_txt string, spot_id string, task_version_id string

      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..NOTIFICATION_FAILED
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: NOTIFICATION_FAILED, NOTIFICATION_FAILED);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..NOTIFICATION_OPENED (
      properties_map_doc string, load_dttm timestamp NOT NULL, notification_opened_dttm_tz timestamp, notification_opened_dttm timestamp, 
      task_version_id string, segment_version_id string, segment_id string, reserved_1_txt string, 
      message_id string, identity_id string, event_nm string, creative_id string, 
      channel_user_id string, channel_nm string, aud_occurrence_id string, context_type_nm string, 
      event_designed_id string, journey_id string, message_version_id string, occurrence_id string, 
      reserved_3_txt string, spot_id string, audience_id string, context_val string, 
      creative_version_id string, event_id string NOT NULL, journey_occurrence_id string, mobile_app_id string, 
      reserved_2_txt string, response_tracking_cd string, task_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..NOTIFICATION_OPENED
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: NOTIFICATION_OPENED, NOTIFICATION_OPENED);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..NOTIFICATION_SEND (
      properties_map_doc string, load_dttm timestamp NOT NULL, notification_send_dttm_tz timestamp, notification_send_dttm timestamp, 
      task_id string, spot_id string, reserved_2_txt string, occurrence_id string, 
      message_id string, identity_id string, creative_version_id string, channel_user_id string, 
      audience_id string, context_val string, event_id string NOT NULL, journey_id string, 
      journey_occurrence_id string, mobile_app_id string, reserved_1_txt string, segment_id string, 
      task_version_id string, aud_occurrence_id string, channel_nm string, context_type_nm string, 
      creative_id string, event_designed_id string, event_nm string, message_version_id string, 
      response_tracking_cd string, segment_version_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..NOTIFICATION_SEND
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: NOTIFICATION_SEND, NOTIFICATION_SEND);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..NOTIFICATION_TARGETING_REQUEST (
      eligibility_flg string, notification_tgt_req_dttm timestamp, load_dttm timestamp NOT NULL, notification_tgt_req_dttm_tz timestamp, 
      task_id string, mobile_app_id string, event_nm string, context_val string, 
      audience_id string, channel_user_id string, event_designed_id string, journey_id string, 
      aud_occurrence_id string, channel_nm string, context_type_nm string, event_id string NOT NULL, 
      identity_id string, journey_occurrence_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..NOTIFICATION_TARGETING_REQUEST
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: NOTIFICATION_TARGETING_REQUEST, NOTIFICATION_TARGETING_REQUEST);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..ORDER_DETAILS (
      unit_price_amt decimal(17,2), quantity_amt int64, properties_map_doc string, load_dttm timestamp NOT NULL, 
      activity_dttm timestamp, activity_dttm_tz timestamp, visit_id string, session_id string, 
      record_type string, product_id string, mobile_app_id string, event_nm string, 
      event_key_cd string, detail_id string, cart_id string, availability_message_txt string, 
      channel_nm string, event_designed_id string, event_source_cd string, order_id string, 
      product_nm string, product_sku string, reserved_1_txt string, session_id_hex string, 
      shipping_message_txt string, cart_nm string, currency_cd string, detail_id_hex string, 
      event_id string NOT NULL, identity_id string, product_group_nm string, saving_message_txt string, 
      visit_id_hex string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..ORDER_DETAILS
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ORDER_DETAILS, ORDER_DETAILS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..ORDER_SUMMARY (
      total_price_amt decimal(17,2), shipping_amt decimal(17,2), total_tax_amt decimal(17,2), total_unit_qty int64, 
      properties_map_doc string, load_dttm timestamp NOT NULL, activity_dttm_tz timestamp, activity_dttm timestamp, 
      visit_id string, shipping_postal_cd string, session_id_hex string, payment_type_desc string, 
      identity_id string, event_id string NOT NULL, delivery_type_desc string, cart_id string, 
      billing_city_nm string, billing_postal_cd string, channel_nm string, detail_id_hex string, 
      event_nm string, mobile_app_id string, record_type string, shipping_city_nm string, 
      visit_id_hex string, billing_country_nm string, billing_state_region_cd string, cart_nm string, 
      currency_cd string, detail_id string, event_designed_id string, event_key_cd string, 
      event_source_cd string, order_id string, session_id string, shipping_country_nm string, 
      shipping_state_region_cd string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..ORDER_SUMMARY
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ORDER_SUMMARY, ORDER_SUMMARY);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..OUTBOUND_SYSTEM (
      properties_map_doc string, outbound_system_dttm_tz timestamp, outbound_system_dttm timestamp, load_dttm timestamp NOT NULL, 
      visit_id_hex string, session_id_hex string, reserved_2_txt string, reserved_1_txt string, 
      parent_event_id string, message_version_id string, journey_id string, event_designed_id string, 
      context_val string, audience_id string, channel_nm string, channel_user_id string, 
      creative_id string, creative_version_id string, event_nm string, message_id string, 
      mobile_app_id string, occurrence_id string, segment_id string, task_id string, 
      aud_occurrence_id string, context_type_nm string, detail_id_hex string, event_id string NOT NULL, 
      identity_id string, journey_occurrence_id string, response_tracking_cd string, segment_version_id string, 
      spot_id string, task_version_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..OUTBOUND_SYSTEM
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: OUTBOUND_SYSTEM, OUTBOUND_SYSTEM);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..PAGE_DETAILS (
      session_dt_tz date, session_dt date, page_load_sec_cnt int64, page_complete_sec_cnt int64, 
      bytes_sent_cnt int64, detail_dttm_tz timestamp, load_dttm timestamp NOT NULL, detail_dttm timestamp, 
      url_domain string, session_id_hex string, session_id string, page_url_txt string, 
      mobile_app_id string, event_key_cd string, detail_id_hex string, detail_id string, 
      class8_id string, class4_id string, class15_id string, class12_id string, 
      class11_id string, channel_nm string, class13_id string, class2_id string, 
      class6_id string, domain_nm string, event_source_cd string, page_desc string, 
      protocol_nm string, visit_id string, visit_id_hex string, class10_id string, 
      class14_id string, class1_id string, class3_id string, class5_id string, 
      class7_id string, class9_id string, event_id string NOT NULL, event_nm string, 
      identity_id string, referrer_url_txt string, window_size_txt string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..PAGE_DETAILS
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: PAGE_DETAILS, PAGE_DETAILS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..PAGE_DETAILS_EXT (
      active_sec_spent_on_page_cnt int64, seconds_spent_on_page_cnt int64, load_dttm timestamp NOT NULL, session_id string NOT NULL, 
      detail_id string NOT NULL, detail_id_hex string, session_id_hex string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY SESSION_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..PAGE_DETAILS_EXT
      ADD PRIMARY KEY (DETAIL_ID,LOAD_DTTM,SESSION_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: PAGE_DETAILS_EXT, PAGE_DETAILS_EXT);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..PAGE_ERRORS (
      in_page_error_dttm timestamp, in_page_error_dttm_tz timestamp, load_dttm timestamp NOT NULL, visit_id_hex string, 
      session_id string, identity_id string, error_location_txt string, detail_id_hex string, 
      event_id string NOT NULL, in_page_error_txt string, session_id_hex string, detail_id string, 
      event_source_cd string, visit_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..PAGE_ERRORS
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: PAGE_ERRORS, PAGE_ERRORS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..PLANNING_HIERARCHY_DEFN (
      level_no int64, load_dttm timestamp, created_dttm timestamp, last_modified_dttm timestamp, 
      last_modified_usernm string, hier_defn_nm string, hier_defn_id string, hier_defn_desc string, 
      hier_defn_subtype string, level_desc string, created_by_usernm string, hier_defn_type string, 
      level_nm string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: PLANNING_HIERARCHY_DEFN, PLANNING_HIERARCHY_DEFN);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..PLANNING_INFO (
      bu_obsolete_flg string, reserved_budget_same_flg string, alloc_budget decimal(17,2), rolledup_budget decimal(17,2), 
      tot_invoiced decimal(17,2), tot_expenses decimal(17,2), tot_cmtmnt_outstanding decimal(17,2), tot_committed decimal(17,2), 
      available_budget decimal(17,2), tot_cmtmnt_overspent decimal(17,2), reserved_budget decimal(17,2), total_budget decimal(17,2), 
      created_dttm timestamp, last_modified_dttm timestamp, planned_start_dttm timestamp, load_dttm timestamp, 
      planned_end_dttm timestamp, task_id string, planning_owner_usernm string, planning_level_no string, 
      planning_desc string, parent_id string, lev6_nm string, lev2_nm string, 
      last_modified_usernm string, hier_defn_id string, currency_cd string, bu_nm string, 
      bu_currency_cd string, activity_nm string, activity_desc string, all_msgs string, 
      bu_desc string, category_nm string, hier_defn_nodeid string, lev10_nm string, 
      lev3_nm string, lev4_nm string, lev7_nm string, lev8_nm string, 
      parent_nm string, planning_id string, planning_level_type string, planning_nm string, 
      planning_type string, task_channel string, task_status string, activity_id string, 
      activity_status string, bu_id string, created_by_usernm string, lev1_nm string, 
      lev5_nm string, lev9_nm string, planning_item_path string, planning_number string, 
      planning_status string, task_desc string, task_nm string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: PLANNING_INFO, PLANNING_INFO);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..PLANNING_INFO_CUSTOM_PROP (
      attr_val STRING, is_obsolete_flg string, is_grid_flg string, last_modified_dttm timestamp, 
      created_dttm timestamp, load_dttm timestamp, planning_id string, last_modified_usernm string, 
      created_by_usernm string, attr_group_nm string, attr_cd string, attr_group_cd string, 
      attr_id string, attr_nm string, data_formatter string, remote_pklist_tab_col string, 
      attr_group_id string, data_type string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: PLANNING_INFO_CUSTOM_PROP, PLANNING_INFO_CUSTOM_PROP);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..PRODUCT_VIEWS (
      price_val decimal(17,2), properties_map_doc string, action_dttm_tz timestamp, load_dttm timestamp NOT NULL, 
      action_dttm timestamp, visit_id_hex string, visit_id string, saving_message_txt string, 
      product_id string, mobile_app_id string, event_nm string, event_key_cd string, 
      detail_id string, availability_message_txt string, channel_nm string, event_designed_id string, 
      event_source_cd string, product_group_nm string, product_sku string, session_id_hex string, 
      currency_cd string, detail_id_hex string, event_id string NOT NULL, identity_id string, 
      product_nm string, session_id string, shipping_message_txt string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..PRODUCT_VIEWS
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: PRODUCT_VIEWS, PRODUCT_VIEWS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..PROMOTION_DISPLAYED (
      derived_display_flg string, promotion_number int64, properties_map_doc string, display_dttm_tz timestamp, 
      load_dttm timestamp NOT NULL, display_dttm timestamp, session_id_hex string, promotion_tracking_cd string, 
      promotion_nm string, promotion_creative_nm string, event_source_cd string, event_designed_id string, 
      detail_id string, channel_nm string, detail_id_hex string, event_key_cd string, 
      mobile_app_id string, promotion_placement_nm string, session_id string, visit_id_hex string, 
      event_id string NOT NULL, event_nm string, identity_id string, promotion_type_nm string, 
      visit_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..PROMOTION_DISPLAYED
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: PROMOTION_DISPLAYED, PROMOTION_DISPLAYED);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..PROMOTION_USED (
      promotion_number int64, properties_map_doc string, click_dttm_tz timestamp, click_dttm timestamp, 
      load_dttm timestamp NOT NULL, session_id_hex string, promotion_tracking_cd string, promotion_creative_nm string, 
      event_source_cd string, event_id string NOT NULL, event_designed_id string, detail_id string, 
      detail_id_hex string, event_key_cd string, mobile_app_id string, promotion_nm string, 
      promotion_placement_nm string, session_id string, visit_id_hex string, channel_nm string, 
      event_nm string, identity_id string, promotion_type_nm string, visit_id string

      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..PROMOTION_USED
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: PROMOTION_USED, PROMOTION_USED);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..RESPONSE_HISTORY (
      properties_map_doc string, load_dttm timestamp NOT NULL, response_dttm timestamp, response_dttm_tz timestamp, 
      session_id_hex string, response_id string NOT NULL, response_channel_nm string, parent_event_designed_id string, 
      journey_occurrence_id string, detail_id_hex string, audience_id string, context_type_nm string, 
      context_val string, identity_id string, message_id string, response_nm string, 
      task_version_id string, aud_occurrence_id string, creative_id string, event_designed_id string, 
      journey_id string, occurrence_id string, response_tracking_cd string, task_id string, 
      visit_id_hex string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..RESPONSE_HISTORY
      ADD PRIMARY KEY (RESPONSE_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: RESPONSE_HISTORY, RESPONSE_HISTORY);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..SEARCH_RESULTS (
      results_displayed_flg string, search_results_displayed int64, properties_map_doc string, load_dttm timestamp NOT NULL, 
      search_results_dttm timestamp, search_results_dttm_tz timestamp, visit_id_hex string, srch_field_name string, 
      srch_field_id string, search_results_sk string, search_nm string, identity_id string, 
      event_key_cd string, event_id string NOT NULL, channel_nm string, detail_id string, 
      detail_id_hex string, event_nm string, mobile_app_id string, session_id string, 
      srch_phrase string, event_designed_id string, event_source_cd string, session_id_hex string, 
      visit_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..SEARCH_RESULTS
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SEARCH_RESULTS, SEARCH_RESULTS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..SEARCH_RESULTS_EXT (
      search_results_displayed int64, load_dttm timestamp NOT NULL, search_results_sk string, event_designed_id string, 
      event_id string NOT NULL
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY event_designed_id
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..SEARCH_RESULTS_EXT
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SEARCH_RESULTS_EXT, SEARCH_RESULTS_EXT);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..SESSION_DETAILS (
      java_enabled_flg string, java_script_enabled_flg string, cookies_enabled_flg string, is_portable_flag string, 
      flash_enabled_flg string, session_dt date, session_dt_tz date, longitude decimal(13,6), 
      latitude decimal(13,6), session_timeout int64, metro_cd int64, screen_color_depth_no int64, 
      client_session_start_dttm timestamp, load_dttm timestamp NOT NULL, session_start_dttm_tz timestamp, session_start_dttm timestamp, 
      client_session_start_dttm_tz timestamp, user_agent_nm string, state_region_cd string, region_nm string, 
      profile_nm2 string, profile_nm1 string, previous_session_id_hex string, previous_session_id string, 
      postal_cd string, parent_event_id string, network_code string, mobile_country_code string, 
      manufacturer string, java_version_no string, flash_version_no string, event_id string NOT NULL, 
      device_type_nm string, country_nm string, country_cd string, city_nm string, 
      browser_nm string, app_id string, browser_version_no string, carrier_name string, 
      device_language string, eventsource_cd string, identity_id string, ip_address string, 
      new_visitor_flg string, platform_desc string, platform_type_nm string, profile_nm4 string, 
      screen_size_txt string, session_id string, visitor_id string, app_version string, 
      channel_nm string, device_nm string, organization_nm string, platform_version string, 
      profile_nm3 string, profile_nm5 string, sdk_version string, session_id_hex string, 
      user_language_cd string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..SESSION_DETAILS
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SESSION_DETAILS, SESSION_DETAILS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..SESSION_DETAILS_EXT (
      active_sec_spent_in_sessn_cnt int64, seconds_spent_in_session_cnt int64, load_dttm timestamp NOT NULL, last_session_activity_dttm timestamp NOT NULL, 
      session_expiration_dttm timestamp, last_session_activity_dttm_tz timestamp, session_expiration_dttm_tz timestamp, session_id string NOT NULL, 
      session_id_hex string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY SESSION_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..SESSION_DETAILS_EXT
      ADD PRIMARY KEY (LAST_SESSION_ACTIVITY_DTTM,SESSION_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SESSION_DETAILS_EXT, SESSION_DETAILS_EXT);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..SMS_MESSAGE_CLICKED (
      sms_click_dttm_tz timestamp, sms_click_dttm timestamp, load_dttm timestamp NOT NULL, task_id string, 
      sms_message_id string, sender_id string, journey_occurrence_id string, event_nm string, 
      event_id string NOT NULL, country_cd string, audience_id string, aud_occurrence_id string, 
      context_type_nm string, creative_version_id string, identity_id string, occurrence_id string, 
      context_val string, creative_id string, event_designed_id string, journey_id string, 
      response_tracking_cd string, task_version_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..SMS_MESSAGE_CLICKED
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SMS_MESSAGE_CLICKED, SMS_MESSAGE_CLICKED);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..SMS_MESSAGE_DELIVERED (
      sms_delivered_dttm_tz timestamp, sms_delivered_dttm timestamp, load_dttm timestamp NOT NULL, sms_message_id string, 
      occurrence_id string, journey_id string, identity_id string, creative_version_id string, 
      context_type_nm string, aud_occurrence_id string, country_cd string, event_id string NOT NULL, 
      journey_occurrence_id string, sender_id string, task_id string, audience_id string, 
      context_val string, creative_id string, event_designed_id string, event_nm string, 
      response_tracking_cd string, task_version_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..SMS_MESSAGE_DELIVERED
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SMS_MESSAGE_DELIVERED, SMS_MESSAGE_DELIVERED);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..SMS_MESSAGE_FAILED (
      sms_failed_dttm_tz timestamp, load_dttm timestamp NOT NULL, sms_failed_dttm timestamp, task_version_id string, 
      task_id string, sms_message_id string, reason_description_txt string, journey_occurrence_id string, 
      event_id string NOT NULL, creative_id string, country_cd string, aud_occurrence_id string, 
      context_type_nm string, creative_version_id string, event_nm string, identity_id string, 
      occurrence_id string, response_tracking_cd string, sender_id string, audience_id string, 
      context_val string, event_designed_id string, journey_id string, reason_cd string

      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..SMS_MESSAGE_FAILED
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SMS_MESSAGE_FAILED, SMS_MESSAGE_FAILED);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..SMS_MESSAGE_REPLY (
      load_dttm timestamp NOT NULL, sms_reply_dttm_tz timestamp, sms_reply_dttm timestamp, task_version_id string, 
      sms_message_id string, response_tracking_cd string, occurrence_id string, identity_id string, 
      country_cd string, aud_occurrence_id string, context_type_nm string, event_id string NOT NULL, 
      journey_id string, journey_occurrence_id string, sender_id string, task_id string, 
      audience_id string, context_val string, event_designed_id string, event_nm string, 
      sms_content string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..SMS_MESSAGE_REPLY
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SMS_MESSAGE_REPLY, SMS_MESSAGE_REPLY);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..SMS_MESSAGE_SEND (
      fragment_cnt int64, sms_send_dttm timestamp, sms_send_dttm_tz timestamp, load_dttm timestamp NOT NULL, 
      occurrence_id string, identity_id string, event_id string NOT NULL, event_designed_id string, 
      context_val string, aud_occurrence_id string, audience_id string, country_cd string, 
      creative_id string, event_nm string, journey_id string, journey_occurrence_id string, 
      sender_id string, task_id string, context_type_nm string, creative_version_id string, 
      response_tracking_cd string, sms_message_id string, task_version_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..SMS_MESSAGE_SEND
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SMS_MESSAGE_SEND, SMS_MESSAGE_SEND);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..SMS_OPTOUT (
      load_dttm timestamp NOT NULL, sms_optout_dttm timestamp, sms_optout_dttm_tz timestamp, task_id string, 
      sms_message_id string, sender_id string, journey_occurrence_id string, event_id string NOT NULL, 
      country_cd string, aud_occurrence_id string, context_type_nm string, creative_version_id string, 
      identity_id string, occurrence_id string, audience_id string, context_val string, 
      creative_id string, event_designed_id string, event_nm string, journey_id string, 
      response_tracking_cd string, task_version_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..SMS_OPTOUT
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SMS_OPTOUT, SMS_OPTOUT);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..SMS_OPTOUT_DETAILS (
      load_dttm timestamp NOT NULL, sms_optout_dttm timestamp, sms_optout_dttm_tz timestamp, task_version_id string, 
      sms_message_id string, occurrence_id string, event_nm string, creative_id string, 
      context_type_nm string, audience_id string, address_val string, context_val string, 
      event_designed_id string, journey_id string, response_tracking_cd string, task_id string, 
      aud_occurrence_id string, country_cd string, creative_version_id string, event_id string NOT NULL, 
      identity_id string, journey_occurrence_id string, sender_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..SMS_OPTOUT_DETAILS
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SMS_OPTOUT_DETAILS, SMS_OPTOUT_DETAILS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..SPOT_CLICKED (
      control_group_flg string, product_qty_no int64, properties_map_doc string, spot_clicked_dttm timestamp, 
      load_dttm timestamp NOT NULL, spot_clicked_dttm_tz timestamp, session_id_hex string, reserved_2_txt string, 
      rec_group_id string, product_id string, message_id string, event_source_cd string, 
      event_nm string, detail_id_hex string, context_val string, channel_user_id string, 
      creative_id string, event_id string NOT NULL, identity_id string, mobile_app_id string, 
      product_nm string, product_sku_no string, request_id string, segment_id string, 
      spot_id string, channel_nm string, context_type_nm string, creative_version_id string, 
      event_designed_id string, event_key_cd string, message_version_id string, occurrence_id string, 
      reserved_1_txt string, response_tracking_cd string, segment_version_id string, visit_id_hex string, 
      url_txt string, task_version_id string, task_id string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..SPOT_CLICKED
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SPOT_CLICKED, SPOT_CLICKED);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..SPOT_REQUESTED (
      properties_map_doc string, load_dttm timestamp NOT NULL, spot_requested_dttm_tz timestamp, spot_requested_dttm timestamp, 
      visit_id_hex string, spot_id string, session_id_hex string, request_id string, 
      mobile_app_id string, identity_id string, event_source_cd string, event_nm string, 
      event_id string NOT NULL, event_designed_id string, detail_id_hex string, context_val string, 
      context_type_nm string, channel_user_id string, channel_nm string
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..SPOT_REQUESTED
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SPOT_REQUESTED, SPOT_REQUESTED);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..TAG_DETAILS (
      created_dttm timestamp, load_dttm timestamp, last_modified_dttm timestamp, tag_owner_usernm string, 
      tag_nm string, tag_id string, tag_desc string, last_modified_usernm string, 
      identity_cd string, created_by_usernm string, component_type string, component_id string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: TAG_DETAILS, TAG_DETAILS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..VISIT_DETAILS (
      sequence_no int64, visit_dttm_tz timestamp, load_dttm timestamp NOT NULL, visit_dttm timestamp, 
      visit_id_hex string, visit_id string, session_id_hex string, session_id string, 
      search_term_txt string, search_engine_domain_txt string, search_engine_desc string, referrer_txt string, 
      referrer_query_string_txt string, referrer_domain_nm string, origination_type_nm string, origination_tracking_cd string, 
      origination_placement_nm string, origination_nm string, origination_creative_nm string, identity_id string, 
      event_id string NOT NULL
      ) PARTITION BY DATE(load_dttm )
      CLUSTER BY IDENTITY_ID
   ) BY &database.;
   EXECUTE (ALTER TABLE &dbschema..VISIT_DETAILS
      ADD PRIMARY KEY (EVENT_ID) NOT ENFORCED ) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: VISIT_DETAILS, VISIT_DETAILS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..WF_PROCESS_DETAILS (
      delayed_by_day int64, percent_complete int64, user_tasks_cnt int64, completed_dttm timestamp, 
      indexed_dttm timestamp, planned_end_dttm timestamp, load_dttm timestamp, timeline_calculated_dttm timestamp, 
      start_dttm timestamp, published_dttm timestamp, submitted_dttm timestamp, deleted_dttm timestamp, 
      created_dttm timestamp, last_modified_dttm timestamp, projected_end_dttm timestamp, submitted_by_usernm string, 
      published_by_usernm string, process_type string, process_status string, process_nm string, 
      process_id string, process_comment string, process_category string, pdef_id string, 
      modified_status_cd string, last_modified_usernm string, deleted_by_usernm string, created_by_usernm string, 
      business_info_type string, business_info_nm string, business_info_id string, process_desc string, 
      process_instance_version string, process_owner_usernm string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: WF_PROCESS_DETAILS, WF_PROCESS_DETAILS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..WF_PROCESS_DETAILS_CUSTOM_PROP (
      attr_val STRING, is_grid_flg string, is_obsolete_flg string, load_dttm timestamp, 
      created_dttm timestamp, last_modified_dttm timestamp, process_id string, last_modified_usernm string, 
      data_type string, data_formatter string, created_by_usernm string, attr_id string, 
      attr_group_nm string, attr_group_id string, attr_cd string, attr_group_cd string, 
      attr_nm string, remote_pklist_tab_col string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: WF_PROCESS_DETAILS_CUSTOM_PROP, WF_PROCESS_DETAILS_CUSTOM_PROP);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..WF_PROCESS_TASKS (
      version_num int64, duration_per_assignee int64, delayed_by_day int64, percent_complete int64, 
      existobj_update_flg string, multi_assig_suprt_flg string, locally_updated_flg string, latest_flg string, 
      skip_peerupdate_scanning_flg string, approval_task_flg string, first_usertask_flg string, cancelled_task_flg string, 
      skip_update_scanning_flg string, is_sequential_flg string, projected_start_dttm timestamp, published_dttm timestamp, 
      load_dttm timestamp, started_dttm timestamp, indexed_dttm timestamp, created_dttm timestamp, 
      modified_dttm timestamp, projected_end_dttm timestamp, completed_dttm timestamp, deleted_dttm timestamp, 
      due_dttm timestamp, engine_task_cancelled_dttm timestamp, task_type string, task_status string, 
      task_instruction string, task_id string, task_desc string, task_attachment string, 
      published_by_usernm string, process_id string, owner_usernm string, modified_status_cd string, 
      modified_by_usernm string, instance_version string, engine_taskdef_id string, deleted_by_usernm string, 
      created_by_usernm string, task_comment string, task_nm string, task_subtype string
         )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: WF_PROCESS_TASKS, WF_PROCESS_TASKS);
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..WF_TASKS_USER_ASSIGNMENT (
      usan_duration_day int64, delayed_by_day int64, is_assigned_flg string, is_replaced_flg string, 
      activation_completed_flg string, is_latest_flg string, created_dttm timestamp, modified_dttm timestamp, 
      projected_end_dttm timestamp, deleted_dttm timestamp, completed_dttm timestamp, due_dttm timestamp, 
      load_dttm timestamp, projected_start_dttm timestamp, start_dttm timestamp, user_nm string, 
      user_assignment_id string, usan_status string, usan_instruction string, usan_desc string, 
      task_id string, replacement_userid string, replacement_reason string, replacement_assignee_id string, 
      process_id string, owner_usernm string, modified_status_cd string, modified_by_usernm string, 
      instance_version string, initiator_comment string, deleted_by_usernm string, created_by_usernm string, 
      assignee_id string, approval_status string, assignee_type string, usan_comment string, 
      user_id string    )) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: WF_TASKS_USER_ASSIGNMENT, WF_TASKS_USER_ASSIGNMENT);
