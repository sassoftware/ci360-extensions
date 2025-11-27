/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..ab_test_path_assignment(
        abtest_path_id varchar(36) NULL ,activity_id varchar(36) NULL ,event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,session_id_hex varchar(29) NULL
        ,
        abtestpath_assignment_dttm timestamp NULL ,abtestpath_assignment_dttm_tz timestamp NULL ,channel_nm varchar(40) NULL ,channel_user_id varchar(300) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,event_nm varchar(256) NULL ,load_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..ab_test_path_assignment add constraint ab_test_path_assignment_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: ab_test_path_assignment, ab_test_path_assignment);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..abt_attribution(
        interaction_dttm timestamp NOT NULL ,interaction_id varchar(36) NOT NULL
        ,
        conversion_value int NULL ,creative_id varchar(36) NULL ,identity_id varchar(36) NULL ,interaction varchar(260) NULL ,interaction_cost int NULL ,interaction_subtype varchar(100) NULL ,interaction_type varchar(15) NULL ,load_id varchar(36) NULL ,task_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..abt_attribution add constraint abt_attribution_pk primary key (interaction_dttm,interaction_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: abt_attribution, abt_attribution);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..activity_conversion(
        abtest_path_id varchar(36) NULL ,activity_id varchar(36) NULL ,activity_node_id varchar(36) NULL ,detail_id_hex varchar(32) NULL ,event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,parent_event_designed_id varchar(36) NULL ,session_id_hex varchar(29) NULL
        ,
        activity_conversion_dttm timestamp NULL ,activity_conversion_dttm_tz timestamp NULL ,channel_nm varchar(40) NULL ,channel_user_id varchar(300) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,event_nm varchar(256) NULL ,goal_id varchar(36) NULL ,load_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..activity_conversion add constraint activity_conversion_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: activity_conversion, activity_conversion);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..activity_flow_in(
        activity_id varchar(36) NULL ,event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,task_id varchar(36) NULL
        ,
        abtest_path_id varchar(36) NULL ,activity_flow_in_dttm timestamp NULL ,activity_flow_in_dttm_tz timestamp NULL ,activity_node_id varchar(36) NULL ,channel_nm varchar(40) NULL ,channel_user_id varchar(300) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,event_nm varchar(256) NULL ,load_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..activity_flow_in add constraint activity_flow_in_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: activity_flow_in, activity_flow_in);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..activity_start(
        activity_id varchar(36) NULL ,event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL
        ,
        activity_start_dttm timestamp NULL ,activity_start_dttm_tz timestamp NULL ,channel_nm varchar(40) NULL ,channel_user_id varchar(300) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,event_nm varchar(256) NULL ,load_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..activity_start add constraint activity_start_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: activity_start, activity_start);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..advertising_contact(
        event_id varchar(36) NOT NULL
        ,
        advertising_contact_dttm timestamp NULL ,advertising_contact_dttm_tz timestamp NULL ,advertising_platform_nm varchar(100) NULL ,aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,channel_nm varchar(40) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,event_designed_id varchar(36) NULL ,event_nm varchar(256) NULL ,identity_id varchar(36) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm timestamp NULL ,occurrence_id varchar(36) NULL ,response_tracking_cd varchar(36) NULL ,segment_id varchar(36) NULL ,segment_version_id varchar(36) NULL ,task_action_nm varchar(40) NULL ,task_id varchar(36) NULL ,task_version_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..advertising_contact add constraint advertising_contact_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: advertising_contact, advertising_contact);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..asset_details(
        asset_id varchar(128) NOT NULL ,folder_id varchar(128) NULL
        ,
        asset_deleted_flg char(1) NULL ,asset_desc varchar(1332) NULL ,asset_locked_by_usernm varchar(128) NULL ,asset_locked_dttm timestamp NULL ,asset_locked_flg char(1) NULL ,asset_nm varchar(128) NULL ,asset_owner_usernm varchar(128) NULL ,asset_process_status varchar(36) NULL ,asset_sk number NULL ,asset_source_nm varchar(128) NULL ,asset_source_type varchar(128) NULL ,average_user_rating_val decimal(4,2) NULL ,created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,download_disabled_by_usernm varchar(128) NULL ,download_disabled_dttm timestamp NULL ,download_disabled_flg char(1) NULL ,entity_attribute_enabled_flg char(1) NULL ,entity_revision_enabled_flg char(1) NULL ,entity_status_cd varchar(3) NULL ,entity_subtype_enabled_flg char(1) NULL ,entity_subtype_nm varchar(128) NULL ,entity_table_nm varchar(128) NULL ,entity_type_nm varchar(128) NULL ,entity_type_usage_cd varchar(3) NULL ,expired_dttm timestamp NULL ,expired_flg char(1) NULL ,external_sharing_error_dt date NULL ,external_sharing_error_msg varchar(1024) NULL ,folder_deleted_flg char(1) NULL ,folder_desc varchar(1332) NULL ,folder_entity_status_cd varchar(3) NULL ,folder_level int NULL ,folder_nm varchar(128) NULL ,folder_owner_usernm varchar(128) NULL ,folder_path varchar(1024) NULL ,folder_sk number NULL ,last_modified_by_usernm varchar(128) NULL ,last_modified_dttm timestamp NULL ,load_dttm timestamp NULL ,process_id varchar(128) NULL ,process_task_id varchar(128) NULL ,public_link varchar(1) NULL ,public_media_id number NULL ,public_url varchar(1024) NULL ,recycled_by_usernm varchar(128) NULL ,recycled_dttm timestamp NULL ,total_user_rating_val number NULL ,user_rating_cnt number NULL
        )) by ORACLE;
      execute (alter table &dbschema..asset_details add constraint asset_details_pk primary key (asset_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: asset_details, asset_details);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..asset_details_custom_prop(
        asset_id varchar(128) NOT NULL ,attr_id varchar(128) NOT NULL
        ,
        attr_cd varchar(128) NULL ,attr_group_cd varchar(128) NULL ,attr_group_id varchar(128) NULL ,attr_group_nm varchar(128) NULL ,attr_nm varchar(128) NULL ,attr_val varchar(4000) NULL ,created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,data_formatter varchar(64) NULL ,data_type varchar(32) NULL ,is_grid_flg char(1) NULL ,is_obsolete_flg char(1) NULL ,last_modified_dttm timestamp NULL ,last_modified_usernm varchar(128) NULL ,load_dttm timestamp NULL ,remote_pklist_tab_col varchar(128) NULL
        )) by ORACLE;
      execute (alter table &dbschema..asset_details_custom_prop add constraint asset_details_custom_prop_pk primary key (asset_id,attr_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: asset_details_custom_prop, asset_details_custom_prop);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..asset_folder_details(
        folder_id varchar(128) NOT NULL
        ,
        created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,deleted_flg char(1) NULL ,entity_status_cd varchar(3) NULL ,folder_desc varchar(1332) NULL ,folder_level int NULL ,folder_nm varchar(128) NULL ,folder_owner_usernm varchar(128) NULL ,folder_path varchar(1024) NULL ,last_modified_by_usernm varchar(128) NULL ,last_modified_dttm timestamp NULL ,load_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..asset_folder_details add constraint asset_folder_details_pk primary key (folder_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: asset_folder_details, asset_folder_details);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..asset_rendition_details(
        asset_id varchar(128) NOT NULL ,rendition_id varchar(128) NOT NULL ,revision_id varchar(128) NOT NULL ,revision_no number NOT NULL
        ,
        created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,current_revision_flg char(1) NULL ,download_cnt number NULL ,entity_status_cd varchar(3) NULL ,file_format varchar(128) NULL ,file_nm varchar(128) NULL ,file_size int NULL ,last_modified_by_usernm varchar(128) NULL ,last_modified_dttm timestamp NULL ,last_modified_status_cd varchar(3) NULL ,load_dttm timestamp NULL ,media_depth int NULL ,media_dpi decimal(10,2) NULL ,media_height int NULL ,media_width int NULL ,rend_deleted_flg char(1) NULL ,rend_duration int NULL ,rendition_generated_type_cd varchar(3) NULL ,rendition_nm varchar(128) NULL ,rendition_type_cd varchar(3) NULL ,rev_deleted_flg char(1) NULL ,revision_comment_txt varchar(512) NULL
        )) by ORACLE;
      execute (alter table &dbschema..asset_rendition_details add constraint asset_rendition_details_pk primary key (asset_id,rendition_id,revision_id,revision_no)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: asset_rendition_details, asset_rendition_details);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..asset_revision(
        asset_id varchar(128) NOT NULL ,revision_id varchar(128) NOT NULL ,revision_no number NOT NULL
        ,
        created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,current_revision_flg char(1) NULL ,deleted_flg char(1) NULL ,entity_status_cd varchar(3) NULL ,last_modified_by_usernm varchar(128) NULL ,last_modified_dttm timestamp NULL ,load_dttm timestamp NULL ,revision_comment_txt varchar(512) NULL
        )) by ORACLE;
      execute (alter table &dbschema..asset_revision add constraint asset_revision_pk primary key (asset_id,revision_id,revision_no)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: asset_revision, asset_revision);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..audience_membership_change(
        event_id varchar(36) NOT NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_change_dttm timestamp NULL ,audience_change_dttm_tz timestamp NULL ,audience_id varchar(36) NULL ,event_nm varchar(256) NULL ,identity_id varchar(36) NULL ,load_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..audience_membership_change add constraint audience_membership_chang_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: audience_membership_change, audience_membership_change);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..business_process_details(
        detail_id varchar(32) NOT NULL ,detail_id_hex varchar(32) NULL ,event_designed_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,process_dttm timestamp NOT NULL ,process_instance_no int NOT NULL ,process_nm varchar(130) NOT NULL ,process_step_nm varchar(130) NOT NULL ,session_id varchar(29) NULL ,session_id_hex varchar(29) NULL ,step_order_no int NOT NULL ,visit_id varchar(32) NULL ,visit_id_hex varchar(32) NULL
        ,
        attribute1_txt varchar(130) NULL ,attribute2_txt varchar(130) NULL ,event_id varchar(36) NULL ,event_nm varchar(256) NULL ,event_source_cd varchar(100) NULL ,is_completion_flg char(1) NULL ,is_start_flg char(1) NULL ,load_dttm timestamp NULL ,next_detail_id varchar(32) NULL ,process_attempt_cnt int NULL ,process_details_sk varchar(32) NULL ,process_dttm_tz timestamp NULL ,process_exception_dttm timestamp NULL ,process_exception_dttm_tz timestamp NULL ,process_exception_txt varchar(1300) NULL
        )) by ORACLE;
      execute (alter table &dbschema..business_process_details add constraint business_process_details_pk primary key (detail_id,event_designed_id,process_dttm,process_instance_no,process_nm,process_step_nm,step_order_no)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: business_process_details, business_process_details);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cart_activity_details(
        activity_dttm timestamp NOT NULL ,cart_activity_sk varchar(32) NULL ,detail_id varchar(32) NOT NULL ,detail_id_hex varchar(32) NULL ,event_designed_id varchar(36) NULL ,identity_id varchar(36) NULL ,product_id varchar(130) NOT NULL ,product_nm varchar(130) NOT NULL ,product_sku varchar(100) NOT NULL ,session_id varchar(29) NULL ,session_id_hex varchar(29) NULL ,visit_id varchar(32) NULL ,visit_id_hex varchar(32) NULL
        ,
        activity_cd varchar(20) NULL ,activity_dttm_tz timestamp NULL ,availability_message_txt varchar(650) NULL ,cart_id varchar(42) NULL ,cart_nm varchar(100) NULL ,channel_nm varchar(40) NULL ,currency_cd varchar(6) NULL ,displayed_cart_amt decimal(17,2) NULL ,displayed_cart_items_no int NULL ,event_id varchar(36) NULL ,event_key_cd varchar(100) NULL ,event_nm varchar(256) NULL ,event_source_cd varchar(100) NULL ,load_dttm timestamp NULL ,mobile_app_id varchar(40) NULL ,product_group_nm varchar(130) NULL ,properties_map_doc varchar(4000) NULL ,quantity_val int NULL ,saving_message_txt varchar(650) NULL ,shipping_message_txt varchar(650) NULL ,unit_price_amt decimal(17,2) NULL
        )) by ORACLE;
      execute (alter table &dbschema..cart_activity_details add constraint cart_activity_details_pk primary key (activity_dttm,detail_id,product_id,product_nm,product_sku)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cart_activity_details, cart_activity_details);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cc_budget_breakup(
        cost_center_id varchar(128) NOT NULL ,planning_id varchar(128) NOT NULL
        ,
        cc_budget_distribution decimal(17,2) NULL ,cc_desc varchar(1332) NULL ,cc_nm varchar(128) NULL ,cc_obsolete_flg char(1) NULL ,cc_owner_usernm varchar(128) NULL ,created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,fin_accnt_desc varchar(1332) NULL ,fin_accnt_nm varchar(128) NULL ,fin_accnt_obsolete_flg char(1) NULL ,gen_ledger_cd varchar(128) NULL ,last_modified_dttm timestamp NULL ,last_modified_usernm varchar(128) NULL ,load_dttm timestamp NULL ,planning_nm varchar(128) NULL
        )) by ORACLE;
      execute (alter table &dbschema..cc_budget_breakup add constraint cc_budget_breakup_pk primary key (cost_center_id,planning_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cc_budget_breakup, cc_budget_breakup);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cc_budget_breakup_ccbdgt(
        cost_center_id varchar(128) NOT NULL ,fp_id varchar(128) NOT NULL ,planning_id varchar(128) NOT NULL
        ,
        cc_bdgt_amt decimal(17,2) NULL ,cc_bdgt_budget_amt decimal(17,2) NULL ,cc_bdgt_budget_desc varchar(1332) NULL ,cc_bdgt_cmtmnt_invoice_amt decimal(17,2) NULL ,cc_bdgt_cmtmnt_invoice_cnt int NULL ,cc_bdgt_cmtmnt_outstanding_amt decimal(17,2) NULL ,cc_bdgt_cmtmnt_overspent_amt decimal(17,2) NULL ,cc_bdgt_committed_amt decimal(17,2) NULL ,cc_bdgt_direct_invoice_amt decimal(17,2) NULL ,cc_bdgt_invoiced_amt decimal(17,2) NULL ,cc_budget_distribution decimal(17,2) NULL ,cc_desc varchar(1332) NULL ,cc_level_expense decimal(17,2) NULL ,cc_lvl_distribution decimal(17,2) NULL ,cc_nm varchar(128) NULL ,cc_number varchar(128) NULL ,cc_obsolete_flg char(1) NULL ,cc_owner_usernm varchar(128) NULL ,cc_rldup_child_bdgt decimal(17,2) NULL ,cc_rldup_total_expense decimal(17,2) NULL ,created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,fin_accnt_desc varchar(1332) NULL ,fin_accnt_nm varchar(128) NULL ,fin_accnt_obsolete_flg char(1) NULL ,fp_cls_ver varchar(128) NULL ,fp_desc varchar(1332) NULL ,fp_end_dt date NULL ,fp_nm varchar(128) NULL ,fp_obsolete_flg char(1) NULL ,fp_start_dt date NULL ,gen_ledger_cd varchar(128) NULL ,last_modified_dttm timestamp NULL ,last_modified_usernm varchar(128) NULL ,load_dttm timestamp NULL ,planning_nm varchar(128) NULL
        )) by ORACLE;
      execute (alter table &dbschema..cc_budget_breakup_ccbdgt add constraint cc_budget_breakup_ccbdgt_pk primary key (cost_center_id,fp_id,planning_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cc_budget_breakup_ccbdgt, cc_budget_breakup_ccbdgt);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_activity_custom_attr(
        activity_version_id varchar(36) NULL ,attribute_data_type_cd varchar(30) NULL ,attribute_nm varchar(256) NULL ,attribute_val varchar(1500) NULL
        ,
        activity_id varchar(36) NULL ,attribute_character_val varchar(1500) NULL ,attribute_dttm_val timestamp NULL ,attribute_numeric_val decimal(17,2) NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL
        ,Hashed_pk_col varchar(64) NOT NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_activity_custom_attr add constraint cdm_activity_custom_attr_pk primary key (Hashed_pk_col)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_activity_custom_attr, cdm_activity_custom_attr);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_activity_detail(
        activity_version_id varchar(36) NOT NULL
        ,
        activity_category_nm varchar(100) NULL ,activity_cd varchar(60) NULL ,activity_desc varchar(1500) NULL ,activity_id varchar(36) NULL ,activity_nm varchar(256) NULL ,last_published_dttm timestamp NULL ,source_system_cd varchar(10) NULL ,status_cd varchar(20) NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL ,valid_from_dttm timestamp NULL ,valid_to_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_activity_detail add constraint cdm_activity_detail_pk primary key (activity_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_activity_detail, cdm_activity_detail);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_activity_x_task(
        activity_version_id varchar(36) NOT NULL ,task_version_id varchar(36) NOT NULL
        ,
        activity_id varchar(36) NULL ,task_id varchar(36) NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_activity_x_task add constraint cdm_activity_x_task_pk primary key (activity_version_id,task_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_activity_x_task, cdm_activity_x_task);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_audience_detail(
        audience_id varchar(36) NOT NULL
        ,
        audience_data_source_nm varchar(100) NULL ,audience_desc varchar(1332) NULL ,audience_nm varchar(128) NULL ,audience_schedule_flg char(1) NULL ,audience_source_nm varchar(100) NULL ,create_dttm timestamp NULL ,created_user_nm varchar(256) NULL ,delete_dttm timestamp NULL ,updated_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_audience_detail add constraint cdm_audience_detail_pk primary key (audience_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_audience_detail, cdm_audience_detail);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_audience_occur_detail(
        audience_occur_id varchar(36) NOT NULL
        ,
        audience_id varchar(36) NULL ,audience_size_cnt int NULL ,end_dttm timestamp NULL ,execution_status_cd varchar(100) NULL ,occurrence_type_nm varchar(100) NULL ,start_dttm timestamp NULL ,started_by_nm varchar(256) NULL ,updated_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_audience_occur_detail add constraint cdm_audience_occur_detail_pk primary key (audience_occur_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_audience_occur_detail, cdm_audience_occur_detail);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_audience_x_segment(
        audience_id varchar(36) NOT NULL
        ,
        segment_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_audience_x_segment add constraint cdm_audience_x_segment_pk primary key (audience_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_audience_x_segment, cdm_audience_x_segment);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_business_context(
        business_context_id varchar(36) NOT NULL
        ,
        business_context_nm varchar(256) NULL ,business_context_type_cd varchar(40) NULL ,source_system_cd varchar(10) NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_business_context add constraint cdm_business_context_pk primary key (business_context_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_business_context, cdm_business_context);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_campaign_custom_attr(
        attribute_data_type_cd varchar(30) NULL ,attribute_nm varchar(256) NULL ,attribute_val varchar(1500) NULL ,campaign_id varchar(36) NULL ,page_nm varchar(60) NULL
        ,
        attribute_character_val varchar(1500) NULL ,attribute_dttm_val timestamp NULL ,attribute_numeric_val decimal(17,2) NULL ,extension_attribute_nm varchar(256) NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL
        ,Hashed_pk_col varchar(64) NOT NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_campaign_custom_attr add constraint cdm_campaign_custom_attr_pk primary key (Hashed_pk_col)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_campaign_custom_attr, cdm_campaign_custom_attr);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_campaign_detail(
        campaign_id varchar(36) NOT NULL
        ,
        approval_dttm timestamp NULL ,approval_given_by_nm varchar(60) NULL ,campaign_cd varchar(60) NULL ,campaign_desc varchar(1500) NULL ,campaign_folder_txt varchar(1024) NULL ,campaign_group_sk int NULL ,campaign_nm varchar(60) NULL ,campaign_owner_nm varchar(60) NULL ,campaign_status_cd varchar(3) NULL ,campaign_type_cd varchar(3) NULL ,campaign_version_no int NULL ,current_version_flg char(1) NULL ,deleted_flg char(1) NULL ,deployment_version_no int NULL ,end_dttm timestamp NULL ,last_modified_by_user_nm varchar(60) NULL ,last_modified_dttm timestamp NULL ,max_budget_amt decimal(17,2) NULL ,max_budget_offer_amt decimal(17,2) NULL ,min_budget_amt decimal(17,2) NULL ,min_budget_offer_amt decimal(17,2) NULL ,run_dttm timestamp NULL ,source_system_cd varchar(10) NULL ,start_dttm timestamp NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL ,valid_from_dttm timestamp NULL ,valid_to_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_campaign_detail add constraint cdm_campaign_detail_pk primary key (campaign_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_campaign_detail, cdm_campaign_detail);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_contact_channel(
        contact_channel_cd varchar(60) NOT NULL
        ,
        contact_channel_nm varchar(40) NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_contact_channel add constraint cdm_contact_channel_pk primary key (contact_channel_cd)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_contact_channel, cdm_contact_channel);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_contact_history(
        contact_id varchar(36) NOT NULL ,contact_status_cd varchar(3) NULL ,identity_id varchar(36) NULL ,rtc_id varchar(36) NULL
        ,
        audience_id varchar(36) NULL ,audience_occur_id varchar(36) NULL ,contact_dt date NULL ,contact_dttm timestamp NULL ,contact_dttm_tz timestamp NULL ,contact_nm varchar(256) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,control_group_flg char(1) NULL ,external_contact_info_1_id varchar(32) NULL ,external_contact_info_2_id varchar(32) NULL ,optimization_backfill_flg char(1) NULL ,source_system_cd varchar(10) NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_contact_history add constraint cdm_contact_history_pk primary key (contact_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_contact_history, cdm_contact_history);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_contact_status(
        contact_status_cd varchar(3) NOT NULL
        ,
        contact_status_desc varchar(256) NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_contact_status add constraint cdm_contact_status_pk primary key (contact_status_cd)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_contact_status, cdm_contact_status);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_content_custom_attr(
        attribute_character_val varchar(1500) NULL ,attribute_data_type_cd varchar(30) NULL ,attribute_dttm_val timestamp NULL ,attribute_nm varchar(256) NULL ,attribute_numeric_val decimal(17,2) NULL ,attribute_val varchar(1500) NULL ,content_version_id varchar(40) NULL
        ,
        content_id varchar(40) NULL ,extension_attribute_nm varchar(256) NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL
        ,Hashed_pk_col varchar(64) NOT NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_content_custom_attr add constraint cdm_content_custom_attr_pk primary key (Hashed_pk_col)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_content_custom_attr, cdm_content_custom_attr);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_content_detail(
        content_version_id varchar(40) NOT NULL
        ,
        active_flg char(1) NULL ,contact_content_category_nm varchar(256) NULL ,contact_content_cd varchar(60) NULL ,contact_content_class_nm varchar(100) NULL ,contact_content_desc varchar(1500) NULL ,contact_content_nm varchar(256) NULL ,contact_content_status_cd varchar(60) NULL ,contact_content_type_nm varchar(50) NULL ,content_id varchar(40) NULL ,created_dt date NULL ,created_user_nm varchar(256) NULL ,external_reference_txt varchar(1024) NULL ,external_reference_url_txt varchar(1024) NULL ,owner_nm varchar(256) NULL ,source_system_cd varchar(10) NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL ,valid_from_dttm timestamp NULL ,valid_to_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_content_detail add constraint cdm_content_detail_pk primary key (content_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_content_detail, cdm_content_detail);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_dyn_content_custom_attr(
        attribute_data_type_cd varchar(30) NULL ,attribute_nm varchar(256) NULL ,attribute_val varchar(1500) NULL ,content_hash_val varchar(32) NULL ,content_version_id varchar(40) NULL
        ,
        attribute_character_val varchar(1500) NULL ,attribute_dttm_val timestamp NULL ,attribute_numeric_val decimal(17,2) NULL ,content_id varchar(40) NULL ,extension_attribute_nm varchar(256) NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL
        ,Hashed_pk_col varchar(64) NOT NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_dyn_content_custom_attr add constraint cdm_dyn_content_custom_at_pk primary key (Hashed_pk_col)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_dyn_content_custom_attr, cdm_dyn_content_custom_attr);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_identifier_type(
        identifier_type_id varchar(36) NOT NULL
        ,
        identifier_type_desc varchar(100) NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_identifier_type add constraint cdm_identifier_type_pk primary key (identifier_type_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_identifier_type, cdm_identifier_type);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_identity_attr(
        identifier_type_id varchar(36) NOT NULL ,identity_id varchar(36) NOT NULL
        ,
        entry_dttm timestamp NULL ,source_system_cd varchar(10) NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL ,user_identifier_val varchar(5000) NULL ,valid_from_dttm timestamp NULL ,valid_to_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_identity_attr add constraint cdm_identity_attr_pk primary key (identifier_type_id,identity_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_identity_attr, cdm_identity_attr);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_identity_map(
        identity_id varchar(36) NOT NULL ,identity_type_cd varchar(40) NULL
        ,
        updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_identity_map add constraint cdm_identity_map_pk primary key (identity_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_identity_map, cdm_identity_map);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_identity_type(
        identity_type_cd varchar(40) NOT NULL
        ,
        identity_type_desc varchar(100) NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_identity_type add constraint cdm_identity_type_pk primary key (identity_type_cd)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_identity_type, cdm_identity_type);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_occurrence_detail(
        occurrence_id varchar(36) NOT NULL
        ,
        end_dttm timestamp NULL ,execution_status_cd varchar(30) NULL ,occurrence_no int NULL ,occurrence_object_id varchar(36) NULL ,occurrence_object_type_cd varchar(60) NULL ,occurrence_type_cd varchar(30) NULL ,source_system_cd varchar(10) NULL ,start_dttm timestamp NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_occurrence_detail add constraint cdm_occurrence_detail_pk primary key (occurrence_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_occurrence_detail, cdm_occurrence_detail);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_response_channel(
        response_channel_cd varchar(40) NOT NULL
        ,
        response_channel_nm varchar(60) NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_response_channel add constraint cdm_response_channel_pk primary key (response_channel_cd)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_response_channel, cdm_response_channel);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_response_extended_attr(
        attribute_nm varchar(256) NOT NULL ,response_attribute_type_cd varchar(10) NOT NULL ,response_id varchar(36) NOT NULL
        ,
        attribute_data_type_cd varchar(30) NULL ,attribute_val varchar(1500) NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_response_extended_attr add constraint cdm_response_extended_att_pk primary key (attribute_nm,response_attribute_type_cd,response_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_response_extended_attr, cdm_response_extended_attr);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_response_history(
        contact_id varchar(36) NULL ,content_version_id varchar(40) NULL ,identity_id varchar(36) NULL ,response_cd varchar(256) NULL ,response_channel_cd varchar(40) NULL ,response_id varchar(36) NOT NULL ,response_type_cd varchar(60) NULL ,rtc_id varchar(36) NULL
        ,
        audience_id varchar(36) NULL ,audience_occur_id varchar(36) NULL ,content_hash_val varchar(32) NULL ,content_id varchar(40) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,conversion_flg char(1) NULL ,external_contact_info_1_id varchar(32) NULL ,external_contact_info_2_id varchar(32) NULL ,inferred_response_flg char(1) NULL ,properties_map_doc varchar(4000) NULL ,response_dt date NULL ,response_dttm timestamp NULL ,response_dttm_tz timestamp NULL ,response_val_amt decimal(17,2) NULL ,source_system_cd varchar(10) NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_response_history add constraint cdm_response_history_pk primary key (response_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_response_history, cdm_response_history);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_response_lookup(
        response_cd varchar(256) NOT NULL
        ,
        response_nm varchar(256) NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_response_lookup add constraint cdm_response_lookup_pk primary key (response_cd)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_response_lookup, cdm_response_lookup);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_response_type(
        response_type_cd varchar(60) NOT NULL
        ,
        response_type_desc varchar(256) NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_response_type add constraint cdm_response_type_pk primary key (response_type_cd)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_response_type, cdm_response_type);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_rtc_detail(
        occurrence_id varchar(36) NULL ,rtc_id varchar(36) NOT NULL ,segment_version_id varchar(36) NULL ,task_version_id varchar(36) NULL
        ,
        deleted_flg char(1) NULL ,execution_status_cd varchar(30) NULL ,processed_dttm timestamp NULL ,response_tracking_flg char(1) NULL ,segment_id varchar(36) NULL ,source_system_cd varchar(10) NULL ,task_id varchar(36) NULL ,task_occurrence_no int NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_rtc_detail add constraint cdm_rtc_detail_pk primary key (rtc_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_rtc_detail, cdm_rtc_detail);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_rtc_x_content(
        content_version_id varchar(40) NOT NULL ,rtc_id varchar(36) NOT NULL
        ,
        content_hash_val varchar(32) NULL ,content_id varchar(40) NULL ,rtc_x_content_sk varchar(36) NULL ,sequence_no int NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_rtc_x_content add constraint cdm_rtc_x_content_pk primary key (content_version_id,rtc_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_rtc_x_content, cdm_rtc_x_content);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_segment_custom_attr(
        attribute_data_type_cd varchar(30) NULL ,attribute_nm varchar(256) NULL ,attribute_val varchar(1500) NULL ,segment_version_id varchar(36) NULL
        ,
        attribute_character_val varchar(1500) NULL ,attribute_dttm_val timestamp NULL ,attribute_numeric_val decimal(17,2) NULL ,segment_id varchar(36) NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL
        ,Hashed_pk_col varchar(64) NOT NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_segment_custom_attr add constraint cdm_segment_custom_attr_pk primary key (Hashed_pk_col)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_segment_custom_attr, cdm_segment_custom_attr);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_segment_detail(
        segment_version_id varchar(36) NOT NULL
        ,
        segment_category_nm varchar(100) NULL ,segment_cd varchar(60) NULL ,segment_desc varchar(1500) NULL ,segment_id varchar(36) NULL ,segment_map_id varchar(36) NULL ,segment_map_version_id varchar(36) NULL ,segment_nm varchar(256) NULL ,segment_src_nm varchar(40) NULL ,segment_status_cd varchar(20) NULL ,source_system_cd varchar(10) NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL ,valid_from_dttm timestamp NULL ,valid_to_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_segment_detail add constraint cdm_segment_detail_pk primary key (segment_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_segment_detail, cdm_segment_detail);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_segment_map(
        segment_map_version_id varchar(36) NOT NULL
        ,
        segment_map_category_nm varchar(100) NULL ,segment_map_cd varchar(60) NULL ,segment_map_desc varchar(1500) NULL ,segment_map_id varchar(36) NULL ,segment_map_nm varchar(256) NULL ,segment_map_src_nm varchar(40) NULL ,segment_map_status_cd varchar(20) NULL ,source_system_cd varchar(10) NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL ,valid_from_dttm timestamp NULL ,valid_to_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_segment_map add constraint cdm_segment_map_pk primary key (segment_map_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_segment_map, cdm_segment_map);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_segment_map_custom_attr(
        attribute_data_type_cd varchar(30) NULL ,attribute_nm varchar(256) NULL ,attribute_val varchar(1500) NULL ,segment_map_version_id varchar(36) NULL
        ,
        attribute_character_val varchar(1500) NULL ,attribute_dttm_val timestamp NULL ,attribute_numeric_val decimal(17,2) NULL ,segment_map_id varchar(36) NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL
        ,Hashed_pk_col varchar(64) NOT NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_segment_map_custom_attr add constraint cdm_segment_map_custom_at_pk primary key (Hashed_pk_col)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_segment_map_custom_attr, cdm_segment_map_custom_attr);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_segment_test(
        task_version_id varchar(36) NOT NULL ,test_cd varchar(60) NOT NULL
        ,
        stratified_samp_criteria_txt varchar(1024) NULL ,stratified_sampling_flg char(1) NULL ,task_id varchar(36) NULL ,test_cnt int NULL ,test_enabled_flg char(1) NULL ,test_nm varchar(65) NULL ,test_pct decimal(5,2) NULL ,test_sizing_type_nm varchar(65) NULL ,test_type_nm varchar(10) NULL ,updated_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_segment_test add constraint cdm_segment_test_pk primary key (task_version_id,test_cd)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_segment_test, cdm_segment_test);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_segment_test_x_segment(
        segment_id varchar(36) NOT NULL ,task_version_id varchar(36) NOT NULL ,test_cd varchar(60) NOT NULL
        ,
        task_id varchar(36) NULL ,updated_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_segment_test_x_segment add constraint cdm_segment_test_x_segmen_pk primary key (segment_id,task_version_id,test_cd)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_segment_test_x_segment, cdm_segment_test_x_segment);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_task_custom_attr(
        attribute_data_type_cd varchar(30) NULL ,attribute_nm varchar(256) NULL ,attribute_val varchar(1500) NULL ,task_version_id varchar(36) NULL
        ,
        attribute_character_val varchar(1500) NULL ,attribute_dttm_val timestamp NULL ,attribute_numeric_val decimal(17,2) NULL ,extension_attribute_nm varchar(256) NULL ,task_id varchar(36) NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL
        ,Hashed_pk_col varchar(64) NOT NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_task_custom_attr add constraint cdm_task_custom_attr_pk primary key (Hashed_pk_col)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_task_custom_attr, cdm_task_custom_attr);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..cdm_task_detail(
        business_context_id varchar(36) NULL ,campaign_id varchar(36) NULL ,contact_channel_cd varchar(60) NULL ,task_version_id varchar(36) NOT NULL
        ,
        active_flg char(1) NULL ,budget_unit_cost_amt decimal(17,2) NULL ,budget_unit_usage_amt decimal(17,2) NULL ,control_group_action_nm varchar(65) NULL ,created_dt date NULL ,created_user_nm varchar(256) NULL ,export_dttm timestamp NULL ,limit_by_total_impression_flg char(1) NULL ,limit_period_unit_cnt int NULL ,max_budget_amt decimal(17,2) NULL ,max_budget_offer_amt decimal(17,2) NULL ,maximum_period_expression_cnt int NULL ,min_budget_amt decimal(17,2) NULL ,min_budget_offer_amt decimal(17,2) NULL ,modified_status_cd varchar(20) NULL ,owner_nm varchar(256) NULL ,published_flg char(1) NULL ,recurr_type_cd varchar(3) NULL ,recurring_schedule_flg char(1) NULL ,saved_flg char(1) NULL ,scheduled_end_dttm timestamp NULL ,scheduled_flg char(1) NULL ,scheduled_start_dttm timestamp NULL ,segment_tests_flg char(1) NULL ,source_system_cd varchar(10) NULL ,staged_flg char(1) NULL ,standard_reply_flg char(1) NULL ,stratified_sampling_action_nm varchar(65) NULL ,subject_type_nm varchar(60) NULL ,task_cd varchar(60) NULL ,task_delivery_type_nm varchar(60) NULL ,task_desc varchar(1500) NULL ,task_id varchar(36) NULL ,task_nm varchar(256) NULL ,task_status_cd varchar(20) NULL ,task_subtype_nm varchar(100) NULL ,task_type_nm varchar(40) NULL ,update_contact_history_flg char(1) NULL ,updated_by_nm varchar(60) NULL ,updated_dttm timestamp NULL ,valid_from_dttm timestamp NULL ,valid_to_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..cdm_task_detail add constraint cdm_task_detail_pk primary key (task_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: cdm_task_detail, cdm_task_detail);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..commitment_details(
        cmtmnt_id varchar(128) NOT NULL ,planning_id varchar(128) NOT NULL ,vendor_id varchar(128) NULL
        ,
        cmtmnt_amt decimal(17,2) NULL ,cmtmnt_closure_note varchar(1332) NULL ,cmtmnt_created_dttm timestamp NULL ,cmtmnt_desc varchar(1332) NULL ,cmtmnt_nm varchar(128) NULL ,cmtmnt_no varchar(128) NULL ,cmtmnt_outstanding_amt decimal(17,2) NULL ,cmtmnt_overspent_amt decimal(17,2) NULL ,cmtmnt_payment_dttm timestamp NULL ,cmtmnt_status varchar(128) NULL ,created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,last_modified_dttm timestamp NULL ,last_modified_usernm varchar(128) NULL ,load_dttm timestamp NULL ,planning_currency_cd varchar(10) NULL ,planning_nm varchar(128) NULL ,vendor_amt decimal(17,2) NULL ,vendor_currency_cd varchar(10) NULL ,vendor_nm varchar(128) NULL ,vendor_number varchar(128) NULL ,vendor_obsolete_flg char(1) NULL
        )) by ORACLE;
      execute (alter table &dbschema..commitment_details add constraint commitment_details_pk primary key (cmtmnt_id,planning_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: commitment_details, commitment_details);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..commitment_line_items(
        cmtmnt_id varchar(128) NOT NULL ,cost_center_id varchar(128) NULL ,item_nm varchar(128) NOT NULL ,item_number int NOT NULL ,planning_id varchar(128) NOT NULL ,vendor_id varchar(128) NULL
        ,
        cc_allocated_amt decimal(17,2) NULL ,cc_available_amt decimal(17,2) NULL ,cc_desc varchar(1332) NULL ,cc_nm varchar(128) NULL ,cc_owner_usernm varchar(128) NULL ,cc_recon_alloc_amt decimal(17,2) NULL ,ccat_nm varchar(128) NULL ,cmtmnt_amt decimal(17,2) NULL ,cmtmnt_closure_note varchar(1332) NULL ,cmtmnt_created_dttm timestamp NULL ,cmtmnt_desc varchar(1332) NULL ,cmtmnt_nm varchar(128) NULL ,cmtmnt_no varchar(128) NULL ,cmtmnt_outstanding_amt decimal(17,2) NULL ,cmtmnt_overspent_amt decimal(17,2) NULL ,cmtmnt_payment_dttm timestamp NULL ,cmtmnt_status varchar(128) NULL ,created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,fin_acc_nm varchar(128) NULL ,gen_ledger_cd varchar(128) NULL ,item_alloc_amt decimal(17,2) NULL ,item_alloc_unit number NULL ,item_qty number NULL ,item_rate decimal(17,2) NULL ,item_vend_alloc_amt decimal(17,2) NULL ,item_vend_alloc_unit number NULL ,last_modified_dttm timestamp NULL ,last_modified_usernm varchar(128) NULL ,load_dttm timestamp NULL ,planning_currency_cd varchar(10) NULL ,planning_nm varchar(128) NULL ,vendor_amt decimal(17,2) NULL ,vendor_currency_cd varchar(10) NULL ,vendor_nm varchar(128) NULL ,vendor_number varchar(128) NULL ,vendor_obsolete_flg char(1) NULL
        )) by ORACLE;
      execute (alter table &dbschema..commitment_line_items add constraint commitment_line_items_pk primary key (cmtmnt_id,item_nm,item_number,planning_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: commitment_line_items, commitment_line_items);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..commitment_line_items_ccbdgt(
        cmtmnt_id varchar(128) NOT NULL ,item_number int NOT NULL
        ,
        cc_allocated_amt decimal(17,2) NULL ,cc_available_amt decimal(17,2) NULL ,cc_bdgt_amt decimal(17,2) NULL ,cc_bdgt_budget_amt decimal(17,2) NULL ,cc_bdgt_budget_desc varchar(1332) NULL ,cc_bdgt_cmtmnt_invoice_amt decimal(17,2) NULL ,cc_bdgt_cmtmnt_invoice_cnt int NULL ,cc_bdgt_cmtmnt_outstanding_amt decimal(17,2) NULL ,cc_bdgt_cmtmnt_overspent_amt decimal(17,2) NULL ,cc_bdgt_committed_amt decimal(17,2) NULL ,cc_bdgt_direct_invoice_amt decimal(17,2) NULL ,cc_bdgt_invoiced_amt decimal(17,2) NULL ,cc_desc varchar(1332) NULL ,cc_nm varchar(128) NULL ,cc_number varchar(128) NULL ,cc_obsolete_flg char(1) NULL ,cc_owner_usernm varchar(128) NULL ,cc_recon_alloc_amt decimal(17,2) NULL ,ccat_nm varchar(128) NULL ,cmtmnt_amt decimal(17,2) NULL ,cmtmnt_closure_note varchar(1332) NULL ,cmtmnt_created_dttm timestamp NULL ,cmtmnt_desc varchar(1332) NULL ,cmtmnt_nm varchar(128) NULL ,cmtmnt_no varchar(128) NULL ,cmtmnt_outstanding_amt decimal(17,2) NULL ,cmtmnt_overspent_amt decimal(17,2) NULL ,cmtmnt_payment_dttm timestamp NULL ,cmtmnt_status varchar(128) NULL ,cost_center_id varchar(128) NULL ,created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,fin_acc_nm varchar(128) NULL ,fp_cls_ver varchar(128) NULL ,fp_desc varchar(1332) NULL ,fp_end_dt date NULL ,fp_id varchar(128) NULL ,fp_nm varchar(128) NULL ,fp_obsolete_flg char(1) NULL ,fp_start_dt date NULL ,gen_ledger_cd varchar(128) NULL ,item_alloc_amt decimal(17,2) NULL ,item_alloc_unit int NULL ,item_nm varchar(128) NULL ,item_qty int NULL ,item_rate decimal(17,2) NULL ,item_vend_alloc_amt decimal(17,2) NULL ,item_vend_alloc_unit int NULL ,last_modified_dttm timestamp NULL ,last_modified_usernm varchar(128) NULL ,load_dttm timestamp NULL ,planning_currency_cd varchar(10) NULL ,planning_id varchar(128) NULL ,planning_nm varchar(128) NULL ,vendor_amt decimal(17,2) NULL ,vendor_currency_cd varchar(10) NULL ,vendor_id varchar(128) NULL ,vendor_nm varchar(128) NULL ,vendor_number varchar(128) NULL ,vendor_obsolete_flg char(1) NULL
        )) by ORACLE;
      execute (alter table &dbschema..commitment_line_items_ccbdgt add constraint commitment_line_items_ccb_pk primary key (cmtmnt_id,item_number)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: commitment_line_items_ccbdgt, commitment_line_items_ccbdgt);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..contact_history(
        contact_id varchar(36) NOT NULL ,creative_id varchar(36) NULL ,detail_id_hex varchar(32) NULL ,event_designed_id varchar(36) NULL ,identity_id varchar(36) NULL ,message_id varchar(36) NULL ,occurrence_id varchar(36) NULL ,parent_event_designed_id varchar(36) NULL ,response_tracking_cd varchar(36) NULL ,session_id_hex varchar(29) NULL ,task_id varchar(36) NULL ,visit_id_hex varchar(32) NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,contact_channel_nm varchar(19) NULL ,contact_dttm timestamp NULL ,contact_dttm_tz timestamp NULL ,contact_nm varchar(256) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,control_group_flg char(1) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm timestamp NULL ,properties_map_doc varchar(4000) NULL ,task_version_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..contact_history add constraint contact_history_pk primary key (contact_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: contact_history, contact_history);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..conversion_milestone(
        activity_id varchar(36) NULL ,creative_id varchar(36) NULL ,detail_id_hex varchar(32) NULL ,event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,occurrence_id varchar(36) NULL ,parent_event_designed_id varchar(36) NULL ,session_id_hex varchar(29) NULL ,spot_id varchar(36) NULL ,task_id varchar(36) NULL ,visit_id_hex varchar(32) NULL
        ,
        analysis_group_id varchar(36) NULL ,aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,channel_nm varchar(36) NULL ,channel_user_id varchar(300) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,control_group_flg char(1) NULL ,conversion_milestone_dttm timestamp NULL ,conversion_milestone_dttm_tz timestamp NULL ,creative_version_id varchar(36) NULL ,event_nm varchar(256) NULL ,goal_id varchar(36) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm timestamp NULL ,message_id varchar(36) NULL ,message_version_id varchar(36) NULL ,mobile_app_id varchar(40) NULL ,properties_map_doc varchar(4000) NULL ,rec_group_id varchar(3) NULL ,reserved_1_txt varchar(100) NULL ,reserved_2_txt varchar(100) NULL ,response_tracking_cd varchar(36) NULL ,segment_id varchar(36) NULL ,segment_version_id varchar(36) NULL ,subject_line_txt varchar(256) NULL ,task_version_id varchar(36) NULL ,test_flg char(1) NULL ,total_cost_amt decimal(17,2) NULL
        )) by ORACLE;
      execute (alter table &dbschema..conversion_milestone add constraint conversion_milestone_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: conversion_milestone, conversion_milestone);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..custom_events(
        custom_events_sk varchar(32) NULL ,detail_id varchar(32) NULL ,detail_id_hex varchar(32) NULL ,event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,session_id varchar(29) NULL ,session_id_hex varchar(29) NULL ,visit_id varchar(32) NULL ,visit_id_hex varchar(32) NULL
        ,
        channel_nm varchar(40) NULL ,channel_user_id varchar(300) NULL ,custom_event_dttm timestamp NULL ,custom_event_dttm_tz timestamp NULL ,custom_event_group_nm varchar(256) NULL ,custom_event_nm varchar(256) NULL ,custom_revenue_amt decimal(17,2) NULL ,event_key_cd varchar(100) NULL ,event_nm varchar(256) NULL ,event_source_cd varchar(100) NULL ,event_type_nm varchar(20) NULL ,load_dttm timestamp NULL ,mobile_app_id varchar(64) NULL ,page_id varchar(256) NULL ,properties_map_doc varchar(4000) NULL ,reserved_1_txt varchar(100) NULL ,reserved_2_txt varchar(100) NULL
        )) by ORACLE;
      execute (alter table &dbschema..custom_events add constraint custom_events_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: custom_events, custom_events);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..custom_events_ext(
        custom_events_sk varchar(32) NOT NULL ,event_designed_id varchar(36) NULL
        ,
        custom_revenue_amt decimal(17,2) NULL ,load_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..custom_events_ext add constraint custom_events_ext_pk primary key (custom_events_sk)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: custom_events_ext, custom_events_ext);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..daily_usage(
        event_day varchar(36) NOT NULL
        ,
        admin_user_cnt int NULL ,api_usage_str varchar(4000) NULL ,asset_size decimal(17,2) NULL ,audience_usage_cnt number NULL ,bc_subjcnt_str varchar(4000) NULL ,customer_profiles_processed_str varchar(4000) NULL ,db_size decimal(17,2) NULL ,email_preview_cnt number NULL ,email_send_cnt number NULL ,facebook_ads_cnt number NULL ,google_ads_cnt number NULL ,linkedin_ads_cnt number NULL ,mob_impr_cnt number NULL ,mob_sesn_cnt number NULL ,mobile_in_app_msg_cnt number NULL ,mobile_push_cnt number NULL ,outbound_api_cnt number NULL ,plan_users_cnt number NULL ,web_impr_cnt number NULL ,web_sesn_cnt number NULL
        )) by ORACLE;
      execute (alter table &dbschema..daily_usage add constraint daily_usage_pk primary key (event_day)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: daily_usage, daily_usage);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..data_view_details(
        detail_id varchar(32) NULL ,detail_id_hex varchar(32) NOT NULL ,event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,parent_event_designed_id varchar(36) NULL ,session_id varchar(29) NULL ,session_id_hex varchar(29) NULL ,visit_id varchar(32) NULL ,visit_id_hex varchar(32) NULL
        ,
        channel_user_id varchar(300) NULL ,data_view_dttm timestamp NULL ,data_view_dttm_tz timestamp NULL ,event_nm varchar(256) NULL ,load_dttm timestamp NULL ,properties_map_doc varchar(4000) NULL ,reserved_1_txt varchar(100) NULL ,reserved_2_txt varchar(100) NULL ,total_cost_amt decimal(17,2) NULL
        )) by ORACLE;
      execute (alter table &dbschema..data_view_details add constraint data_view_details_pk primary key (detail_id_hex,event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: data_view_details, data_view_details);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..dbt_adv_campaign_visitors(
        session_id varchar(29) NOT NULL ,visit_id varchar(32) NOT NULL
        ,
        average_visit_duration int NULL ,bouncer varchar(12) NULL ,bouncers int NULL ,br_browser_name varchar(52) NULL ,br_browser_version varchar(16) NULL ,co_conversions int NULL ,cu_customer_id varchar(36) NULL ,device_name varchar(85) NULL ,device_type varchar(52) NULL ,ge_city varchar(390) NULL ,ge_country varchar(85) NULL ,ge_latitude decimal(13,6) NULL ,ge_longitude decimal(13,6) NULL ,ge_state_region varchar(2) NULL ,landing_page varchar(1332) NULL ,landing_page_url varchar(1332) NULL ,landing_page_url_domain varchar(215) NULL ,new_visitors int NULL ,page_views int NULL ,pl_device_operating_system varchar(78) NULL ,return_visitors int NULL ,rv_revenue decimal(17,2) NULL ,se_external_search_engine varchar(130) NULL ,se_external_search_engine_domain varchar(215) NULL ,se_external_search_engine_phrase varchar(1332) NULL ,session_complete_load_dttm timestamp NULL ,session_start_dttm timestamp NULL ,session_start_dttm_tz timestamp NULL ,visit_dttm timestamp NULL ,visit_dttm_tz timestamp NULL ,visit_origination_creative varchar(260) NULL ,visit_origination_name varchar(260) NULL ,visit_origination_placement varchar(390) NULL ,visit_origination_tracking_code varchar(65) NULL ,visit_origination_type varchar(65) NULL ,visitor_id varchar(32) NULL ,visitor_type varchar(10) NULL ,visits int NULL
        )) by ORACLE;
      execute (alter table &dbschema..dbt_adv_campaign_visitors add constraint dbt_adv_campaign_visitors_pk primary key (session_id,visit_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: dbt_adv_campaign_visitors, dbt_adv_campaign_visitors);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..dbt_business_process(
        bus_process_started_dttm timestamp NOT NULL ,business_process_name varchar(130) NOT NULL ,business_process_step_name varchar(130) NOT NULL ,session_id varchar(29) NOT NULL
        ,
        bouncer varchar(12) NULL ,bus_process_started_dttm_tz timestamp NULL ,business_process_attribute_1 varchar(130) NULL ,business_process_attribute_2 varchar(130) NULL ,cu_customer_id varchar(36) NULL ,device_name varchar(85) NULL ,device_type varchar(52) NULL ,last_step number NULL ,processes number NULL ,processes_abandoned number NULL ,processes_completed number NULL ,session_complete_load_dttm timestamp NULL ,session_start_dttm timestamp NULL ,session_start_dttm_tz timestamp NULL ,step_count number NULL ,steps number NULL ,steps_abandoned number NULL ,steps_completed number NULL ,visit_id varchar(32) NULL ,visit_origination_creative varchar(260) NULL ,visit_origination_name varchar(260) NULL ,visit_origination_placement varchar(390) NULL ,visit_origination_tracking_code varchar(65) NULL ,visit_origination_type varchar(65) NULL ,visitor_id varchar(32) NULL ,visitor_type varchar(10) NULL
        )) by ORACLE;
      execute (alter table &dbschema..dbt_business_process add constraint dbt_business_process_pk primary key (bus_process_started_dttm,business_process_name,business_process_step_name,session_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: dbt_business_process, dbt_business_process);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..dbt_content(
        active_page_view_time int NULL ,detail_dttm timestamp NULL ,entry_pages int NULL ,exit_pages int NULL ,pg_page_url varchar(1332) NULL ,session_id varchar(29) NULL
        ,
        bouncer varchar(12) NULL ,bouncers int NULL ,class1_id varchar(650) NULL ,class2_id varchar(650) NULL ,cu_customer_id varchar(36) NULL ,detail_dttm_tz timestamp NULL ,device_name varchar(85) NULL ,device_type varchar(52) NULL ,pg_domain_name varchar(215) NULL ,pg_page varchar(1332) NULL ,session_complete_load_dttm timestamp NULL ,session_start_dttm timestamp NULL ,session_start_dttm_tz timestamp NULL ,total_page_view_time number NULL ,views int NULL ,visit_id varchar(32) NULL ,visit_origination_creative varchar(260) NULL ,visit_origination_name varchar(260) NULL ,visit_origination_placement varchar(390) NULL ,visit_origination_tracking_code varchar(65) NULL ,visit_origination_type varchar(65) NULL ,visitor_id varchar(32) NULL ,visitor_type varchar(10) NULL ,visits int NULL
        ,Hashed_pk_col varchar(64) NOT NULL
        )) by ORACLE;
      execute (alter table &dbschema..dbt_content add constraint dbt_content_pk primary key (Hashed_pk_col)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: dbt_content, dbt_content);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..dbt_documents(
        document_download_dttm timestamp NOT NULL ,session_id varchar(29) NOT NULL
        ,
        bouncer varchar(12) NULL ,class1_id varchar(650) NULL ,class2_id varchar(650) NULL ,cu_customer_id varchar(36) NULL ,device_name varchar(85) NULL ,device_type varchar(52) NULL ,do_page_description varchar(1332) NULL ,do_page_url varchar(1332) NULL ,document_download_dttm_tz timestamp NULL ,document_downloads int NULL ,session_complete_load_dttm timestamp NULL ,session_start_dttm timestamp NULL ,session_start_dttm_tz timestamp NULL ,visit_id varchar(32) NULL ,visit_origination_creative varchar(260) NULL ,visit_origination_name varchar(260) NULL ,visit_origination_placement varchar(390) NULL ,visit_origination_tracking_code varchar(65) NULL ,visit_origination_type varchar(65) NULL ,visitor_id varchar(32) NULL ,visitor_type varchar(10) NULL
        )) by ORACLE;
      execute (alter table &dbschema..dbt_documents add constraint dbt_documents_pk primary key (document_download_dttm,session_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: dbt_documents, dbt_documents);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..dbt_ecommerce(
        product_activity_dttm timestamp NOT NULL ,product_id varchar(130) NOT NULL ,session_id varchar(29) NOT NULL
        ,
        basket_adds int NULL ,basket_adds_revenue decimal(17,2) NULL ,basket_adds_units int NULL ,basket_id varchar(42) NULL ,basket_removes int NULL ,basket_removes_revenue decimal(17,2) NULL ,basket_removes_units int NULL ,baskets_abandoned number NULL ,baskets_completed number NULL ,baskets_started number NULL ,bouncer varchar(12) NULL ,cu_customer_id varchar(36) NULL ,device_name varchar(85) NULL ,device_type varchar(52) NULL ,product_activity_dttm_tz timestamp NULL ,product_group_name varchar(130) NULL ,product_name varchar(130) NULL ,product_purchase_revenues decimal(17,2) NULL ,product_purchase_units int NULL ,product_purchases int NULL ,product_sku varchar(100) NULL ,product_views int NULL ,session_complete_load_dttm timestamp NULL ,session_start_dttm timestamp NULL ,session_start_dttm_tz timestamp NULL ,visit_id varchar(32) NULL ,visit_origination_creative varchar(260) NULL ,visit_origination_name varchar(260) NULL ,visit_origination_placement varchar(390) NULL ,visit_origination_tracking_code varchar(65) NULL ,visit_origination_type varchar(65) NULL ,visitor_id varchar(32) NULL ,visitor_type varchar(10) NULL
        )) by ORACLE;
      execute (alter table &dbschema..dbt_ecommerce add constraint dbt_ecommerce_pk primary key (product_activity_dttm,product_id,session_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: dbt_ecommerce, dbt_ecommerce);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..dbt_forms(
        form_attempt_dttm timestamp NOT NULL ,form_nm varchar(65) NOT NULL ,session_id varchar(29) NOT NULL
        ,
        attempts int NULL ,bouncer varchar(12) NULL ,cu_customer_id varchar(36) NULL ,device_name varchar(85) NULL ,device_type varchar(52) NULL ,form_attempt_dttm_tz timestamp NULL ,forms_completed number NULL ,forms_not_submitted number NULL ,forms_started number NULL ,last_field varchar(325) NULL ,session_complete_load_dttm timestamp NULL ,session_start_dttm timestamp NULL ,session_start_dttm_tz timestamp NULL ,visit_id varchar(32) NULL ,visit_origination_creative varchar(260) NULL ,visit_origination_name varchar(260) NULL ,visit_origination_placement varchar(390) NULL ,visit_origination_tracking_code varchar(65) NULL ,visit_origination_type varchar(65) NULL ,visitor_id varchar(32) NULL ,visitor_type varchar(10) NULL
        )) by ORACLE;
      execute (alter table &dbschema..dbt_forms add constraint dbt_forms_pk primary key (form_attempt_dttm,form_nm,session_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: dbt_forms, dbt_forms);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..dbt_goals(
        goal_reached_dttm timestamp NOT NULL ,session_id varchar(29) NOT NULL
        ,
        bouncer varchar(12) NULL ,cu_customer_id varchar(36) NULL ,device_name varchar(85) NULL ,device_type varchar(52) NULL ,goal_group_name varchar(130) NULL ,goal_name varchar(260) NULL ,goal_reached_dttm_tz timestamp NULL ,goal_revenue decimal(17,2) NULL ,goals number NULL ,session_complete_load_dttm timestamp NULL ,session_start_dttm timestamp NULL ,session_start_dttm_tz timestamp NULL ,visit_id varchar(32) NULL ,visit_origination_creative varchar(260) NULL ,visit_origination_name varchar(260) NULL ,visit_origination_placement varchar(390) NULL ,visit_origination_tracking_code varchar(65) NULL ,visit_origination_type varchar(65) NULL ,visitor_id varchar(32) NULL ,visitor_type varchar(10) NULL ,visits int NULL
        )) by ORACLE;
      execute (alter table &dbschema..dbt_goals add constraint dbt_goals_pk primary key (goal_reached_dttm,session_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: dbt_goals, dbt_goals);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..dbt_media_consumption(
        detail_id varchar(32) NOT NULL ,interactions_count int NOT NULL ,maximum_progress decimal(11,3) NOT NULL ,media_completion_rate varchar(35) NOT NULL ,media_section varchar(35) NOT NULL ,visit_id varchar(32) NOT NULL
        ,
        bouncer varchar(12) NULL ,content_viewed decimal(11,3) NULL ,counter int NULL ,cu_customer_id varchar(36) NULL ,device_name varchar(85) NULL ,device_type varchar(52) NULL ,duration decimal(11,3) NULL ,media_name varchar(260) NULL ,media_section_view number NULL ,media_start_dttm timestamp NULL ,media_start_dttm_tz timestamp NULL ,media_uri_txt varchar(2024) NULL ,session_complete_load_dttm timestamp NULL ,session_id varchar(29) NULL ,session_start_dttm timestamp NULL ,session_start_dttm_tz timestamp NULL ,time_viewing decimal(11,3) NULL ,views number NULL ,views_completed number NULL ,views_started number NULL ,visit_origination_creative varchar(260) NULL ,visit_origination_name varchar(260) NULL ,visit_origination_placement varchar(390) NULL ,visit_origination_tracking_code varchar(65) NULL ,visit_origination_type varchar(65) NULL ,visitor_id varchar(32) NULL ,visitor_type varchar(10) NULL
        )) by ORACLE;
      execute (alter table &dbschema..dbt_media_consumption add constraint dbt_media_consumption_pk primary key (detail_id,interactions_count,maximum_progress,media_completion_rate,media_section,visit_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: dbt_media_consumption, dbt_media_consumption);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..dbt_promotions(
        promotion_name varchar(260) NOT NULL ,promotion_shown_dttm timestamp NOT NULL ,session_id varchar(29) NOT NULL
        ,
        bouncer varchar(12) NULL ,click_throughs int NULL ,cu_customer_id varchar(36) NULL ,device_name varchar(85) NULL ,device_type varchar(52) NULL ,displays int NULL ,promotion_creative varchar(260) NULL ,promotion_placement varchar(260) NULL ,promotion_shown_dttm_tz timestamp NULL ,promotion_tracking_code varchar(65) NULL ,promotion_type varchar(65) NULL ,session_complete_load_dttm timestamp NULL ,session_start_dttm timestamp NULL ,session_start_dttm_tz timestamp NULL ,visit_id varchar(32) NULL ,visit_origination_creative varchar(260) NULL ,visit_origination_name varchar(260) NULL ,visit_origination_placement varchar(390) NULL ,visit_origination_tracking_code varchar(65) NULL ,visit_origination_type varchar(65) NULL ,visitor_id varchar(32) NULL ,visitor_type varchar(10) NULL
        )) by ORACLE;
      execute (alter table &dbschema..dbt_promotions add constraint dbt_promotions_pk primary key (promotion_name,promotion_shown_dttm,session_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: dbt_promotions, dbt_promotions);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..dbt_search(
        search_name varchar(42) NOT NULL ,search_results_dttm timestamp NOT NULL ,session_id varchar(29) NOT NULL
        ,
        bouncer varchar(12) NULL ,cu_customer_id varchar(36) NULL ,device_name varchar(85) NULL ,device_type varchar(52) NULL ,exit_pages int NULL ,internal_search_term varchar(128) NULL ,num_additional_searches int NULL ,num_pages_viewed_afterwards int NULL ,search_no_results_returned int NULL ,search_results_dttm_tz timestamp NULL ,search_returned_results int NULL ,search_unknown_results int NULL ,searches int NULL ,session_complete_load_dttm timestamp NULL ,session_start_dttm timestamp NULL ,session_start_dttm_tz timestamp NULL ,visit_id varchar(32) NULL ,visit_origination_creative varchar(260) NULL ,visit_origination_name varchar(260) NULL ,visit_origination_placement varchar(390) NULL ,visit_origination_tracking_code varchar(65) NULL ,visit_origination_type varchar(65) NULL ,visitor_id varchar(32) NULL ,visitor_type varchar(10) NULL ,visits int NULL
        )) by ORACLE;
      execute (alter table &dbschema..dbt_search add constraint dbt_search_pk primary key (search_name,search_results_dttm,session_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: dbt_search, dbt_search);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..direct_contact(
        event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,message_id varchar(36) NULL ,occurrence_id varchar(36) NULL ,task_id varchar(36) NULL
        ,
        channel_nm varchar(40) NULL ,channel_user_id varchar(300) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,control_active_flg char(1) NULL ,control_group_flg char(1) NULL ,direct_contact_dttm timestamp NULL ,direct_contact_dttm_tz timestamp NULL ,event_nm varchar(256) NULL ,identity_type_nm varchar(36) NULL ,load_dttm timestamp NULL ,properties_map_doc varchar(4000) NULL ,response_tracking_cd varchar(36) NULL ,segment_id varchar(36) NULL ,task_version_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..direct_contact add constraint direct_contact_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: direct_contact, direct_contact);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..document_details(
        detail_id varchar(32) NULL ,detail_id_hex varchar(32) NULL ,identity_id varchar(36) NULL ,link_event_dttm timestamp NULL ,session_id varchar(29) NULL ,session_id_hex varchar(29) NULL ,uri_txt varchar(1332) NULL ,visit_id varchar(32) NULL ,visit_id_hex varchar(32) NULL
        ,
        alt_txt varchar(1332) NULL ,event_id varchar(36) NULL ,event_key_cd varchar(100) NULL ,event_source_cd varchar(100) NULL ,link_event_dttm_tz timestamp NULL ,link_id varchar(1332) NULL ,link_name varchar(1332) NULL ,link_selector_path varchar(1332) NULL ,load_dttm timestamp NULL
        ,Hashed_pk_col varchar(64) NOT NULL
        )) by ORACLE;
      execute (alter table &dbschema..document_details add constraint document_details_pk primary key (Hashed_pk_col)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: document_details, document_details);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..email_bounce(
        event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,occurrence_id varchar(36) NULL ,task_id varchar(36) NULL
        ,
        analysis_group_id varchar(36) NULL ,aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,bounce_class_cd varchar(5) NULL ,channel_user_id varchar(300) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,email_bounce_dttm timestamp NULL ,email_bounce_dttm_tz timestamp NULL ,event_nm varchar(256) NULL ,imprint_id varchar(36) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm timestamp NULL ,program_id varchar(50) NULL ,properties_map_doc varchar(4000) NULL ,raw_reason_txt varchar(1000) NULL ,reason_txt varchar(1000) NULL ,recipient_domain_nm varchar(100) NULL ,response_tracking_cd varchar(36) NULL ,segment_id varchar(36) NULL ,segment_version_id varchar(36) NULL ,subject_line_txt varchar(256) NULL ,task_version_id varchar(36) NULL ,test_flg char(1) NULL
        )) by ORACLE;
      execute (alter table &dbschema..email_bounce add constraint email_bounce_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: email_bounce, email_bounce);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..email_click(
        event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,occurrence_id varchar(36) NULL ,task_id varchar(36) NULL
        ,
        agent_family_nm varchar(100) NULL ,analysis_group_id varchar(36) NULL ,aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,channel_user_id varchar(300) NULL ,click_tracking_flg char(1) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,device_nm varchar(85) NULL ,email_click_dttm timestamp NULL ,email_click_dttm_tz timestamp NULL ,event_nm varchar(256) NULL ,imprint_id varchar(36) NULL ,is_mobile_flg char(1) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,link_tracking_group_txt varchar(256) NULL ,link_tracking_id varchar(4) NULL ,link_tracking_label_txt varchar(256) NULL ,load_dttm timestamp NULL ,mailbox_provider_nm varchar(100) NULL ,manufacturer_nm varchar(75) NULL ,open_tracking_flg char(1) NULL ,platform_desc varchar(78) NULL ,platform_version varchar(25) NULL ,program_id varchar(50) NULL ,properties_map_doc varchar(4000) NULL ,recipient_domain_nm varchar(100) NULL ,response_tracking_cd varchar(36) NULL ,segment_id varchar(36) NULL ,segment_version_id varchar(36) NULL ,subject_line_txt varchar(256) NULL ,task_version_id varchar(36) NULL ,test_flg char(1) NULL ,uri_txt varchar(1332) NULL ,user_agent_nm varchar(512) NULL
        )) by ORACLE;
      execute (alter table &dbschema..email_click add constraint email_click_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: email_click, email_click);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..email_complaint(
        event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,occurrence_id varchar(36) NULL ,task_id varchar(36) NULL
        ,
        analysis_group_id varchar(36) NULL ,aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,channel_user_id varchar(300) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,email_complaint_dttm timestamp NULL ,email_complaint_dttm_tz timestamp NULL ,event_nm varchar(256) NULL ,imprint_id varchar(36) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm timestamp NULL ,program_id varchar(50) NULL ,properties_map_doc varchar(4000) NULL ,recipient_domain_nm varchar(100) NULL ,response_tracking_cd varchar(36) NULL ,segment_id varchar(36) NULL ,segment_version_id varchar(36) NULL ,subject_line_txt varchar(256) NULL ,task_version_id varchar(36) NULL ,test_flg char(1) NULL
        )) by ORACLE;
      execute (alter table &dbschema..email_complaint add constraint email_complaint_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: email_complaint, email_complaint);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..email_open(
        event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,occurrence_id varchar(36) NULL ,task_id varchar(36) NULL
        ,
        agent_family_nm varchar(100) NULL ,analysis_group_id varchar(36) NULL ,aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,channel_user_id varchar(300) NULL ,click_tracking_flg char(1) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,device_nm varchar(85) NULL ,email_open_dttm timestamp NULL ,email_open_dttm_tz timestamp NULL ,event_nm varchar(256) NULL ,imprint_id varchar(36) NULL ,is_mobile_flg char(1) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm timestamp NULL ,mailbox_provider_nm varchar(100) NULL ,manufacturer_nm varchar(75) NULL ,open_tracking_flg char(1) NULL ,platform_desc varchar(78) NULL ,platform_version varchar(25) NULL ,prefetched_flg char(1) NULL ,program_id varchar(50) NULL ,properties_map_doc varchar(4000) NULL ,recipient_domain_nm varchar(100) NULL ,response_tracking_cd varchar(36) NULL ,segment_id varchar(36) NULL ,segment_version_id varchar(36) NULL ,subject_line_txt varchar(256) NULL ,task_version_id varchar(36) NULL ,test_flg char(1) NULL ,user_agent_nm varchar(512) NULL
        )) by ORACLE;
      execute (alter table &dbschema..email_open add constraint email_open_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: email_open, email_open);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..email_optout(
        event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,occurrence_id varchar(36) NULL ,task_id varchar(36) NULL
        ,
        analysis_group_id varchar(36) NULL ,aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,channel_user_id varchar(300) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,email_optout_dttm timestamp NULL ,email_optout_dttm_tz timestamp NULL ,event_nm varchar(256) NULL ,imprint_id varchar(36) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,link_tracking_group_txt varchar(256) NULL ,link_tracking_id varchar(4) NULL ,link_tracking_label_txt varchar(256) NULL ,load_dttm timestamp NULL ,optout_type_nm varchar(50) NULL ,program_id varchar(50) NULL ,properties_map_doc varchar(4000) NULL ,recipient_domain_nm varchar(100) NULL ,response_tracking_cd varchar(36) NULL ,segment_id varchar(36) NULL ,segment_version_id varchar(36) NULL ,subject_line_txt varchar(256) NULL ,task_version_id varchar(36) NULL ,test_flg char(1) NULL
        )) by ORACLE;
      execute (alter table &dbschema..email_optout add constraint email_optout_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: email_optout, email_optout);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..email_optout_details(
        event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,occurrence_id varchar(36) NULL ,task_id varchar(36) NULL
        ,
        analysis_group_id varchar(36) NULL ,aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,email_action_dttm timestamp NULL ,email_action_dttm_tz timestamp NULL ,email_address varchar(300) NULL ,event_nm varchar(256) NULL ,imprint_id varchar(36) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm timestamp NULL ,optout_type_nm varchar(50) NULL ,program_id varchar(50) NULL ,properties_map_doc varchar(4000) NULL ,recipient_domain_nm varchar(100) NULL ,response_tracking_cd varchar(36) NULL ,segment_id varchar(36) NULL ,segment_version_id varchar(36) NULL ,subject_line_txt varchar(256) NULL ,task_version_id varchar(36) NULL ,test_flg char(1) NULL
        )) by ORACLE;
      execute (alter table &dbschema..email_optout_details add constraint email_optout_details_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: email_optout_details, email_optout_details);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..email_reply(
        event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,occurrence_id varchar(36) NULL ,task_id varchar(36) NULL
        ,
        analysis_group_id varchar(36) NULL ,aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,channel_user_id varchar(300) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,email_reply_dttm timestamp NULL ,email_reply_dttm_tz timestamp NULL ,event_nm varchar(256) NULL ,imprint_id varchar(36) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm timestamp NULL ,program_id varchar(50) NULL ,properties_map_doc varchar(4000) NULL ,recipient_domain_nm varchar(100) NULL ,response_tracking_cd varchar(36) NULL ,segment_id varchar(36) NULL ,segment_version_id varchar(36) NULL ,subject_line_txt varchar(256) NULL ,task_version_id varchar(36) NULL ,test_flg char(1) NULL ,uri_txt varchar(1332) NULL
        )) by ORACLE;
      execute (alter table &dbschema..email_reply add constraint email_reply_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: email_reply, email_reply);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..email_send(
        event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,occurrence_id varchar(36) NULL ,task_id varchar(36) NULL
        ,
        analysis_group_id varchar(36) NULL ,aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,channel_user_id varchar(300) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,email_send_dttm timestamp NULL ,email_send_dttm_tz timestamp NULL ,event_nm varchar(256) NULL ,imprint_id varchar(36) NULL ,imprint_url_txt varchar(1332) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm timestamp NULL ,program_id varchar(50) NULL ,properties_map_doc varchar(4000) NULL ,recipient_domain_nm varchar(100) NULL ,response_tracking_cd varchar(36) NULL ,segment_id varchar(36) NULL ,segment_version_id varchar(36) NULL ,subject_line_txt varchar(256) NULL ,task_version_id varchar(36) NULL ,test_flg char(1) NULL
        )) by ORACLE;
      execute (alter table &dbschema..email_send add constraint email_send_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: email_send, email_send);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..email_view(
        event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,occurrence_id varchar(36) NULL ,task_id varchar(36) NULL
        ,
        analysis_group_id varchar(36) NULL ,aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,channel_user_id varchar(300) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,email_view_dttm timestamp NULL ,email_view_dttm_tz timestamp NULL ,event_nm varchar(256) NULL ,imprint_id varchar(36) NULL ,link_tracking_group_txt varchar(256) NULL ,link_tracking_id varchar(4) NULL ,link_tracking_label_txt varchar(256) NULL ,load_dttm timestamp NULL ,program_id varchar(50) NULL ,properties_map_doc varchar(4000) NULL ,recipient_domain_nm varchar(100) NULL ,response_tracking_cd varchar(36) NULL ,segment_id varchar(36) NULL ,segment_version_id varchar(36) NULL ,subject_line_txt varchar(256) NULL ,task_version_id varchar(36) NULL ,test_flg char(1) NULL
        )) by ORACLE;
      execute (alter table &dbschema..email_view add constraint email_view_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: email_view, email_view);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..external_event(
        event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL
        ,
        channel_nm varchar(40) NULL ,channel_user_id varchar(300) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,event_nm varchar(256) NULL ,external_event_dttm timestamp NULL ,external_event_dttm_tz timestamp NULL ,load_dttm timestamp NULL ,properties_map_doc varchar(4000) NULL ,response_tracking_cd varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..external_event add constraint external_event_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: external_event, external_event);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..fiscal_cc_budget(
        cost_center_id varchar(128) NOT NULL ,fp_id varchar(128) NOT NULL
        ,
        cc_bdgt_amt decimal(17,2) NULL ,cc_bdgt_budget_amt decimal(17,2) NULL ,cc_bdgt_budget_desc varchar(1332) NULL ,cc_bdgt_cmtmnt_invoice_amt decimal(17,2) NULL ,cc_bdgt_cmtmnt_invoice_cnt int NULL ,cc_bdgt_cmtmnt_outstanding_amt decimal(17,2) NULL ,cc_bdgt_cmtmnt_overspent_amt decimal(17,2) NULL ,cc_bdgt_committed_amt decimal(17,2) NULL ,cc_bdgt_direct_invoice_amt decimal(17,2) NULL ,cc_bdgt_invoiced_amt decimal(17,2) NULL ,cc_desc varchar(1332) NULL ,cc_nm varchar(128) NULL ,cc_number varchar(128) NULL ,cc_obsolete_flg char(1) NULL ,cc_owner_usernm varchar(128) NULL ,created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,fin_accnt_desc varchar(1332) NULL ,fin_accnt_nm varchar(128) NULL ,fin_accnt_obsolete_flg char(1) NULL ,fp_cls_ver varchar(128) NULL ,fp_desc varchar(1332) NULL ,fp_end_dt date NULL ,fp_nm varchar(128) NULL ,fp_obsolete_flg char(1) NULL ,fp_start_dt date NULL ,gen_ledger_cd varchar(128) NULL ,last_modified_dttm timestamp NULL ,last_modified_usernm varchar(128) NULL ,load_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..fiscal_cc_budget add constraint fiscal_cc_budget_pk primary key (cost_center_id,fp_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: fiscal_cc_budget, fiscal_cc_budget);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..form_details(
        detail_id varchar(32) NULL ,detail_id_hex varchar(32) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,session_id varchar(29) NULL ,session_id_hex varchar(29) NULL ,visit_id varchar(32) NULL ,visit_id_hex varchar(32) NULL
        ,
        attempt_index_cnt int NULL ,attempt_status_cd varchar(42) NULL ,change_index_no int NULL ,event_key_cd varchar(100) NULL ,event_source_cd varchar(100) NULL ,form_field_detail_dttm timestamp NULL ,form_field_detail_dttm_tz timestamp NULL ,form_field_id varchar(325) NULL ,form_field_nm varchar(325) NULL ,form_field_value varchar(2600) NULL ,form_nm varchar(65) NULL ,load_dttm timestamp NULL ,submit_flg char(1) NULL
        )) by ORACLE;
      execute (alter table &dbschema..form_details add constraint form_details_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: form_details, form_details);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..identity_attributes(
        entrytime timestamp NOT NULL ,identifier_type_id varchar(36) NOT NULL ,identity_id varchar(36) NOT NULL
        ,
        processed_dttm timestamp NULL ,user_identifier_val varchar(5000) NULL
        )) by ORACLE;
      execute (alter table &dbschema..identity_attributes add constraint identity_attributes_pk primary key (entrytime,identifier_type_id,identity_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: identity_attributes, identity_attributes);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..identity_map(
        source_identity_id varchar(36) NOT NULL ,target_identity_id varchar(36) NULL
        ,
        entrytime timestamp NULL ,processed_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..identity_map add constraint identity_map_pk primary key (source_identity_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: identity_map, identity_map);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..impression_delivered(
        creative_id varchar(36) NULL ,detail_id_hex varchar(32) NULL ,event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,session_id_hex varchar(29) NULL ,spot_id varchar(36) NULL ,task_id varchar(36) NULL ,visit_id_hex varchar(32) NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,channel_nm varchar(40) NULL ,channel_user_id varchar(300) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,control_group_flg char(1) NULL ,creative_version_id varchar(36) NULL ,event_key_cd varchar(100) NULL ,event_nm varchar(256) NULL ,event_source_cd varchar(100) NULL ,impression_delivered_dttm timestamp NULL ,impression_delivered_dttm_tz timestamp NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm timestamp NULL ,message_id varchar(36) NULL ,message_version_id varchar(36) NULL ,mobile_app_id varchar(40) NULL ,product_id varchar(130) NULL ,product_nm varchar(128) NULL ,product_qty_no int NULL ,product_sku_no varchar(100) NULL ,properties_map_doc varchar(4000) NULL ,rec_group_id varchar(3) NULL ,request_id varchar(36) NULL ,reserved_1_txt varchar(100) NULL ,reserved_2_txt varchar(100) NULL ,response_tracking_cd varchar(36) NULL ,segment_id varchar(36) NULL ,segment_version_id varchar(36) NULL ,task_version_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..impression_delivered add constraint impression_delivered_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: impression_delivered, impression_delivered);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..impression_spot_viewable(
        creative_id varchar(36) NULL ,detail_id_hex varchar(32) NULL ,event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,occurrence_id varchar(36) NULL ,session_id_hex varchar(29) NULL ,spot_id varchar(36) NULL ,task_id varchar(36) NULL ,visit_id_hex varchar(32) NULL
        ,
        analysis_group_id varchar(36) NULL ,aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,channel_nm varchar(40) NULL ,channel_user_id varchar(300) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,control_group_flg char(1) NULL ,creative_version_id varchar(36) NULL ,event_key_cd varchar(100) NULL ,event_nm varchar(256) NULL ,event_source_cd varchar(100) NULL ,impression_viewable_dttm timestamp NULL ,impression_viewable_dttm_tz timestamp NULL ,load_dttm timestamp NULL ,message_id varchar(36) NULL ,message_version_id varchar(36) NULL ,mobile_app_id varchar(40) NULL ,product_id varchar(128) NULL ,product_nm varchar(128) NULL ,product_qty_no int NULL ,product_sku_no varchar(100) NULL ,properties_map_doc varchar(4000) NULL ,rec_group_id varchar(3) NULL ,request_id varchar(36) NULL ,reserved_1_txt varchar(100) NULL ,reserved_2_txt varchar(100) NULL ,response_tracking_cd varchar(36) NULL ,segment_id varchar(36) NULL ,segment_version_id varchar(36) NULL ,task_version_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..impression_spot_viewable add constraint impression_spot_viewable_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: impression_spot_viewable, impression_spot_viewable);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..in_app_failed(
        creative_id varchar(36) NULL ,event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,occurrence_id varchar(36) NULL ,spot_id varchar(36) NULL ,task_id varchar(36) NULL
        ,
        channel_nm varchar(40) NULL ,channel_user_id varchar(300) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,creative_version_id varchar(36) NULL ,error_cd varchar(256) NULL ,error_message_txt varchar(1332) NULL ,event_nm varchar(256) NULL ,in_app_failed_dttm timestamp NULL ,in_app_failed_dttm_tz timestamp NULL ,load_dttm timestamp NULL ,message_id varchar(36) NULL ,message_version_id varchar(36) NULL ,mobile_app_id varchar(40) NULL ,properties_map_doc varchar(4000) NULL ,reserved_1_txt varchar(100) NULL ,reserved_2_txt varchar(100) NULL ,response_tracking_cd varchar(36) NULL ,segment_id varchar(36) NULL ,segment_version_id varchar(36) NULL ,task_version_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..in_app_failed add constraint in_app_failed_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: in_app_failed, in_app_failed);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..in_app_message(
        creative_id varchar(36) NULL ,event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,occurrence_id varchar(36) NULL ,spot_id varchar(36) NULL ,task_id varchar(36) NULL
        ,
        channel_nm varchar(40) NULL ,channel_user_id varchar(300) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,creative_version_id varchar(36) NULL ,event_nm varchar(256) NULL ,in_app_action_dttm timestamp NULL ,in_app_action_dttm_tz timestamp NULL ,load_dttm timestamp NULL ,message_id varchar(36) NULL ,message_version_id varchar(36) NULL ,mobile_app_id varchar(40) NULL ,properties_map_doc varchar(4000) NULL ,reserved_1_txt varchar(100) NULL ,reserved_2_txt varchar(100) NULL ,reserved_3_txt varchar(100) NULL ,response_tracking_cd varchar(36) NULL ,segment_id varchar(36) NULL ,segment_version_id varchar(36) NULL ,task_version_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..in_app_message add constraint in_app_message_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: in_app_message, in_app_message);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..in_app_send(
        creative_id varchar(36) NULL ,event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,occurrence_id varchar(36) NULL ,spot_id varchar(36) NULL ,task_id varchar(36) NULL
        ,
        channel_nm varchar(40) NULL ,channel_user_id varchar(300) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,creative_version_id varchar(36) NULL ,event_nm varchar(256) NULL ,in_app_send_dttm timestamp NULL ,in_app_send_dttm_tz timestamp NULL ,load_dttm timestamp NULL ,message_id varchar(36) NULL ,message_version_id varchar(36) NULL ,mobile_app_id varchar(40) NULL ,properties_map_doc varchar(4000) NULL ,reserved_1_txt varchar(100) NULL ,reserved_2_txt varchar(100) NULL ,response_tracking_cd varchar(36) NULL ,segment_id varchar(36) NULL ,segment_version_id varchar(36) NULL ,task_version_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..in_app_send add constraint in_app_send_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: in_app_send, in_app_send);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..in_app_targeting_request(
        event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL
        ,
        channel_nm varchar(40) NULL ,channel_user_id varchar(300) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,eligibility_flg char(1) NULL ,event_nm varchar(256) NULL ,in_app_tgt_request_dttm timestamp NULL ,in_app_tgt_request_dttm_tz timestamp NULL ,load_dttm timestamp NULL ,mobile_app_id varchar(40) NULL
        )) by ORACLE;
      execute (alter table &dbschema..in_app_targeting_request add constraint in_app_targeting_request_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: in_app_targeting_request, in_app_targeting_request);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..invoice_details(
        cmtmnt_id varchar(128) NOT NULL ,invoice_id varchar(128) NOT NULL ,planning_id varchar(128) NOT NULL ,vendor_id varchar(128) NULL
        ,
        cmtmnt_nm varchar(128) NULL ,created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,invoice_amt decimal(17,2) NULL ,invoice_created_dttm timestamp NULL ,invoice_desc varchar(1332) NULL ,invoice_nm varchar(128) NULL ,invoice_number varchar(128) NULL ,invoice_reconciled_dttm timestamp NULL ,invoice_status varchar(64) NULL ,last_modified_dttm timestamp NULL ,last_modified_usernm varchar(128) NULL ,load_dttm timestamp NULL ,payment_dttm timestamp NULL ,plan_currency_cd varchar(10) NULL ,planning_nm varchar(128) NULL ,reconcile_amt decimal(17,2) NULL ,reconcile_note varchar(1332) NULL ,vendor_amt decimal(17,2) NULL ,vendor_currency_cd varchar(10) NULL ,vendor_desc varchar(1332) NULL ,vendor_nm varchar(128) NULL ,vendor_number varchar(128) NULL ,vendor_obsolete_flg char(1) NULL
        )) by ORACLE;
      execute (alter table &dbschema..invoice_details add constraint invoice_details_pk primary key (cmtmnt_id,invoice_id,planning_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: invoice_details, invoice_details);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..invoice_line_items(
        cmtmnt_id varchar(128) NOT NULL ,cost_center_id varchar(128) NULL ,invoice_id varchar(128) NOT NULL ,invoice_nm varchar(128) NOT NULL ,invoice_number varchar(128) NOT NULL ,planning_id varchar(128) NOT NULL ,vendor_id varchar(128) NULL
        ,
        cc_allocated_amt decimal(17,2) NULL ,cc_available_amt decimal(17,2) NULL ,cc_desc varchar(1332) NULL ,cc_nm varchar(128) NULL ,cc_owner_usernm varchar(128) NULL ,cc_recon_alloc_amt decimal(17,2) NULL ,ccat_nm varchar(128) NULL ,cmtmnt_nm varchar(128) NULL ,created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,fin_acc_ccat_nm varchar(128) NULL ,fin_acc_nm varchar(128) NULL ,gen_ledger_cd varchar(128) NULL ,invoice_amt decimal(17,2) NULL ,invoice_created_dttm timestamp NULL ,invoice_desc varchar(1332) NULL ,invoice_reconciled_dttm timestamp NULL ,invoice_status varchar(64) NULL ,item_alloc_amt decimal(17,2) NULL ,item_alloc_unit number NULL ,item_nm varchar(128) NULL ,item_number int NULL ,item_qty number NULL ,item_rate decimal(17,2) NULL ,item_vend_alloc_amt decimal(17,2) NULL ,item_vend_alloc_unit number NULL ,last_modified_dttm timestamp NULL ,last_modified_usernm varchar(128) NULL ,load_dttm timestamp NULL ,payment_dttm timestamp NULL ,plan_currency_cd varchar(10) NULL ,planning_nm varchar(128) NULL ,reconcile_amt decimal(17,2) NULL ,reconcile_note varchar(1332) NULL ,vendor_amt decimal(17,2) NULL ,vendor_currency_cd varchar(10) NULL ,vendor_desc varchar(1332) NULL ,vendor_nm varchar(128) NULL ,vendor_number varchar(128) NULL ,vendor_obsolete_flg char(1) NULL
        )) by ORACLE;
      execute (alter table &dbschema..invoice_line_items add constraint invoice_line_items_pk primary key (cmtmnt_id,invoice_id,invoice_nm,invoice_number,planning_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: invoice_line_items, invoice_line_items);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..invoice_line_items_ccbdgt(
        invoice_id varchar(128) NOT NULL ,item_number int NOT NULL
        ,
        cc_allocated_amt decimal(17,2) NULL ,cc_available_amt decimal(17,2) NULL ,cc_bdgt_amt decimal(17,2) NULL ,cc_bdgt_budget_amt decimal(17,2) NULL ,cc_bdgt_budget_desc varchar(1332) NULL ,cc_bdgt_cmtmnt_invoice_amt decimal(17,2) NULL ,cc_bdgt_cmtmnt_invoice_cnt int NULL ,cc_bdgt_cmtmnt_outstanding_amt decimal(17,2) NULL ,cc_bdgt_cmtmnt_overspent_amt decimal(17,2) NULL ,cc_bdgt_committed_amt decimal(17,2) NULL ,cc_bdgt_direct_invoice_amt decimal(17,2) NULL ,cc_bdgt_invoiced_amt decimal(17,2) NULL ,cc_desc varchar(1332) NULL ,cc_nm varchar(128) NULL ,cc_number varchar(128) NULL ,cc_obsolete_flg char(1) NULL ,cc_owner_usernm varchar(128) NULL ,cc_recon_alloc_amt decimal(17,2) NULL ,ccat_nm varchar(128) NULL ,cmtmnt_id varchar(128) NULL ,cmtmnt_nm varchar(128) NULL ,cost_center_id varchar(128) NULL ,created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,fin_acc_ccat_nm varchar(128) NULL ,fin_acc_nm varchar(128) NULL ,fp_cls_ver varchar(128) NULL ,fp_desc varchar(1332) NULL ,fp_end_dt date NULL ,fp_id varchar(128) NULL ,fp_nm varchar(128) NULL ,fp_obsolete_flg char(1) NULL ,fp_start_dt date NULL ,gen_ledger_cd varchar(128) NULL ,invoice_amt decimal(17,2) NULL ,invoice_created_dttm timestamp NULL ,invoice_desc varchar(1332) NULL ,invoice_nm varchar(128) NULL ,invoice_number varchar(128) NULL ,invoice_reconciled_dttm timestamp NULL ,invoice_status varchar(64) NULL ,item_alloc_amt decimal(17,2) NULL ,item_alloc_unit int NULL ,item_nm varchar(128) NULL ,item_qty int NULL ,item_rate decimal(17,2) NULL ,item_vend_alloc_amt decimal(17,2) NULL ,item_vend_alloc_unit int NULL ,last_modified_dttm timestamp NULL ,last_modified_usernm varchar(128) NULL ,load_dttm timestamp NULL ,payment_dttm timestamp NULL ,plan_currency_cd varchar(10) NULL ,planning_id varchar(128) NULL ,planning_nm varchar(128) NULL ,reconcile_amt decimal(17,2) NULL ,reconcile_note varchar(1332) NULL ,vendor_amt decimal(17,2) NULL ,vendor_currency_cd varchar(10) NULL ,vendor_desc varchar(1332) NULL ,vendor_id varchar(128) NULL ,vendor_nm varchar(128) NULL ,vendor_number varchar(128) NULL ,vendor_obsolete_flg char(1) NULL
        )) by ORACLE;
      execute (alter table &dbschema..invoice_line_items_ccbdgt add constraint invoice_line_items_ccbdgt_pk primary key (invoice_id,item_number)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: invoice_line_items_ccbdgt, invoice_line_items_ccbdgt);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..journey_entry(
        event_id varchar(36) NOT NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,entry_dttm timestamp NULL ,entry_dttm_tz timestamp NULL ,event_nm varchar(256) NULL ,identity_id varchar(36) NULL ,identity_type_nm varchar(100) NULL ,identity_type_val varchar(300) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..journey_entry add constraint journey_entry_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: journey_entry, journey_entry);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..journey_exit(
        event_id varchar(36) NOT NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,event_nm varchar(256) NULL ,exit_dttm timestamp NULL ,exit_dttm_tz timestamp NULL ,identity_id varchar(36) NULL ,identity_type_nm varchar(100) NULL ,identity_type_val varchar(300) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,last_node_id varchar(36) NULL ,load_dttm timestamp NULL ,reason_cd varchar(100) NULL ,reason_txt varchar(1000) NULL
        )) by ORACLE;
      execute (alter table &dbschema..journey_exit add constraint journey_exit_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: journey_exit, journey_exit);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..journey_holdout(
        event_id varchar(36) NOT NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,event_nm varchar(256) NULL ,holdout_dttm timestamp NULL ,holdout_dttm_tz timestamp NULL ,identity_id varchar(36) NULL ,identity_type_nm varchar(100) NULL ,identity_type_val varchar(300) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..journey_holdout add constraint journey_holdout_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: journey_holdout, journey_holdout);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..journey_node_entry(
        event_id varchar(36) NOT NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,event_nm varchar(256) NULL ,identity_id varchar(36) NULL ,identity_type_nm varchar(100) NULL ,identity_type_val varchar(300) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm timestamp NULL ,node_entry_dttm timestamp NULL ,node_entry_dttm_tz timestamp NULL ,node_id varchar(36) NULL ,node_type_nm varchar(256) NULL ,previous_node_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..journey_node_entry add constraint journey_node_entry_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: journey_node_entry, journey_node_entry);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..journey_success(
        event_id varchar(36) NOT NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,event_nm varchar(256) NULL ,identity_id varchar(36) NULL ,identity_type_nm varchar(100) NULL ,identity_type_val varchar(300) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm timestamp NULL ,success_dttm timestamp NULL ,success_dttm_tz timestamp NULL ,success_val int NULL ,unit_qty int NULL
        )) by ORACLE;
      execute (alter table &dbschema..journey_success add constraint journey_success_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: journey_success, journey_success);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..journey_suppression(
        event_id varchar(36) NOT NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,event_nm varchar(256) NULL ,identity_id varchar(36) NULL ,identity_type_nm varchar(100) NULL ,identity_type_val varchar(300) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm timestamp NULL ,reason_cd varchar(100) NULL ,reason_txt varchar(1000) NULL ,suppression_dttm timestamp NULL ,suppression_dttm_tz timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..journey_suppression add constraint journey_suppression_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: journey_suppression, journey_suppression);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_activity(
        activity_version_id varchar(36) NOT NULL ,business_context_id varchar(36) NULL
        ,
        activity_category_nm varchar(100) NULL ,activity_cd varchar(60) NULL ,activity_desc varchar(1332) NULL ,activity_id varchar(36) NULL ,activity_nm varchar(60) NULL ,activity_status_cd varchar(20) NULL ,folder_path_nm varchar(256) NULL ,last_published_dttm timestamp NULL ,valid_from_dttm timestamp NULL ,valid_to_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_activity add constraint md_activity_pk primary key (activity_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_activity, md_activity);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_activity_abtestpath(
        abtest_path_id varchar(36) NOT NULL ,activity_id varchar(36) NULL ,activity_node_id varchar(36) NOT NULL ,activity_version_id varchar(36) NOT NULL
        ,
        abtest_dist_pct char(3) NULL ,abtest_path_nm varchar(50) NULL ,activity_status_cd varchar(20) NULL ,control_flg char(1) NULL ,next_node_val varchar(4000) NULL ,valid_from_dttm timestamp NULL ,valid_to_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_activity_abtestpath add constraint md_activity_abtestpath_pk primary key (abtest_path_id,activity_node_id,activity_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_activity_abtestpath, md_activity_abtestpath);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_activity_custom_prop(
        activity_id varchar(36) NULL ,activity_version_id varchar(36) NULL ,property_datatype_cd varchar(256) NULL ,property_nm varchar(256) NULL ,property_val varchar(1332) NULL
        ,
        activity_status_cd varchar(36) NULL ,valid_from_dttm timestamp NULL ,valid_to_dttm timestamp NULL
        ,Hashed_pk_col varchar(64) NOT NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_activity_custom_prop add constraint md_activity_custom_prop_pk primary key (Hashed_pk_col)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_activity_custom_prop, md_activity_custom_prop);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_activity_node(
        activity_id varchar(36) NULL ,activity_node_id varchar(36) NOT NULL ,activity_version_id varchar(36) NOT NULL
        ,
        abtest_id varchar(36) NULL ,activity_node_nm varchar(256) NULL ,activity_node_type_nm varchar(100) NULL ,activity_status_cd varchar(20) NULL ,end_node_flg char(1) NULL ,next_node_val varchar(4000) NULL ,node_sequence_no int NULL ,previous_node_val varchar(4000) NULL ,specific_wait_flg char(1) NULL ,start_node_flg char(1) NULL ,time_boxed_flg char(1) NULL ,valid_from_dttm timestamp NULL ,valid_to_dttm timestamp NULL ,wait_tm int NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_activity_node add constraint md_activity_node_pk primary key (activity_node_id,activity_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_activity_node, md_activity_node);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_activity_x_activity_node(
        activity_id varchar(36) NULL ,activity_node_id varchar(36) NOT NULL ,activity_version_id varchar(36) NOT NULL
        ,
        activity_status_cd varchar(20) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_activity_x_activity_node add constraint md_activity_x_activity_no_pk primary key (activity_node_id,activity_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_activity_x_activity_node, md_activity_x_activity_node);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_activity_x_task(
        activity_version_id varchar(36) NOT NULL ,task_id varchar(36) NOT NULL
        ,
        activity_id varchar(36) NULL ,activity_status_cd varchar(20) NULL ,task_version_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_activity_x_task add constraint md_activity_x_task_pk primary key (activity_version_id,task_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_activity_x_task, md_activity_x_task);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_asset(
        asset_version_id varchar(36) NOT NULL
        ,
        asset_desc varchar(1332) NULL ,asset_id varchar(36) NULL ,asset_nm varchar(256) NULL ,asset_status_cd varchar(20) NULL ,asset_type_nm varchar(40) NULL ,created_user_nm varchar(256) NULL ,last_published_dttm timestamp NULL ,owner_nm varchar(256) NULL ,valid_from_dttm timestamp NULL ,valid_to_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_asset add constraint md_asset_pk primary key (asset_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_asset, md_asset);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_audience(
        audience_id varchar(36) NOT NULL
        ,
        audience_data_source_nm varchar(100) NULL ,audience_desc varchar(1332) NULL ,audience_expiration_val int NULL ,audience_nm varchar(128) NULL ,audience_schedule_flg char(1) NULL ,audience_source_nm varchar(100) NULL ,create_dttm timestamp NULL ,created_user_nm varchar(256) NULL ,delete_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_audience add constraint md_audience_pk primary key (audience_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_audience, md_audience);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_audience_occurrence(
        aud_occurrence_id varchar(36) NOT NULL
        ,
        audience_id varchar(36) NULL ,audience_size_val int NULL ,end_tm timestamp NULL ,execution_status_cd varchar(100) NULL ,occurrence_type_nm varchar(100) NULL ,start_tm timestamp NULL ,started_by_nm varchar(256) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_audience_occurrence add constraint md_audience_occurrence_pk primary key (aud_occurrence_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_audience_occurrence, md_audience_occurrence);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_audience_x_segment(
        audience_id varchar(36) NOT NULL
        ,
        segment_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_audience_x_segment add constraint md_audience_x_segment_pk primary key (audience_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_audience_x_segment, md_audience_x_segment);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_bu(
        bu_id varchar(128) NOT NULL
        ,
        bu_currency_cd varchar(10) NULL ,bu_desc varchar(1332) NULL ,bu_nm varchar(128) NULL ,bu_obsolete_flg char(1) NULL ,bu_owner_usernm varchar(128) NULL ,bu_parentid varchar(128) NULL ,created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,last_modified_dttm timestamp NULL ,last_modified_usernm varchar(128) NULL ,load_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_bu add constraint md_bu_pk primary key (bu_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_bu, md_bu);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_business_context(
        business_context_version_id varchar(36) NOT NULL
        ,
        business_context_desc varchar(1332) NULL ,business_context_id varchar(36) NULL ,business_context_nm varchar(256) NULL ,business_context_src_cd varchar(40) NULL ,business_context_status_cd varchar(20) NULL ,created_user_nm varchar(256) NULL ,information_map_nm varchar(40) NULL ,last_published_dttm timestamp NULL ,locked_information_map_nm varchar(40) NULL ,owner_nm varchar(256) NULL ,valid_from_dttm timestamp NULL ,valid_to_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_business_context add constraint md_business_context_pk primary key (business_context_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_business_context, md_business_context);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_cost_category(
        ccat_id varchar(128) NOT NULL
        ,
        ccat_desc varchar(1332) NULL ,ccat_nm varchar(128) NULL ,ccat_obsolete_flg char(1) NULL ,ccat_owner_usernm varchar(128) NULL ,created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,fin_accnt_nm varchar(128) NULL ,gen_ledger_cd varchar(128) NULL ,last_modified_dttm timestamp NULL ,last_modified_usernm varchar(128) NULL ,load_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_cost_category add constraint md_cost_category_pk primary key (ccat_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_cost_category, md_cost_category);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_costcenter(
        cost_center_id varchar(128) NOT NULL
        ,
        cc_desc varchar(1332) NULL ,cc_nm varchar(128) NULL ,cc_obsolete_flg char(1) NULL ,cc_owner_usernm varchar(128) NULL ,created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,fin_accnt_nm varchar(128) NULL ,gen_ledger_cd varchar(128) NULL ,last_modified_dttm timestamp NULL ,last_modified_usernm varchar(128) NULL ,load_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_costcenter add constraint md_costcenter_pk primary key (cost_center_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_costcenter, md_costcenter);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_creative(
        creative_version_id varchar(36) NOT NULL
        ,
        business_context_id varchar(36) NULL ,created_user_nm varchar(256) NULL ,creative_category_nm varchar(100) NULL ,creative_cd varchar(60) NULL ,creative_desc varchar(256) NULL ,creative_id varchar(36) NULL ,creative_nm varchar(60) NULL ,creative_status_cd varchar(20) NULL ,creative_txt varchar(1500) NULL ,creative_type_nm varchar(40) NULL ,folder_path_nm varchar(256) NULL ,last_published_dttm timestamp NULL ,owner_nm varchar(256) NULL ,recommender_template_id varchar(36) NULL ,recommender_template_nm varchar(60) NULL ,valid_from_dttm timestamp NULL ,valid_to_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_creative add constraint md_creative_pk primary key (creative_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_creative, md_creative);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_creative_custom_prop(
        creative_version_id varchar(36) NULL ,property_datatype_cd varchar(256) NULL ,property_nm varchar(256) NULL ,property_val varchar(1332) NULL
        ,
        creative_id varchar(36) NULL ,creative_status_cd varchar(36) NULL ,valid_from_dttm timestamp NULL ,valid_to_dttm timestamp NULL
        ,Hashed_pk_col varchar(64) NOT NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_creative_custom_prop add constraint md_creative_custom_prop_pk primary key (Hashed_pk_col)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_creative_custom_prop, md_creative_custom_prop);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_creative_x_asset(
        asset_id varchar(36) NOT NULL ,creative_id varchar(36) NULL ,creative_version_id varchar(36) NOT NULL
        ,
        creative_status_cd varchar(20) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_creative_x_asset add constraint md_creative_x_asset_pk primary key (asset_id,creative_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_creative_x_asset, md_creative_x_asset);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_cust_attrib(
        attr_group_id varchar(128) NOT NULL ,attr_id varchar(128) NOT NULL
        ,
        associated_grid varchar(128) NULL ,attr_cd varchar(128) NULL ,attr_group_cd varchar(128) NULL ,attr_group_nm varchar(128) NULL ,attr_nm varchar(128) NULL ,created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,data_formatter varchar(64) NULL ,data_type varchar(32) NULL ,is_grid_flg char(1) NULL ,is_obsolete_flg char(1) NULL ,last_modified_dttm timestamp NULL ,last_modified_usernm varchar(128) NULL ,load_dttm timestamp NULL ,remote_pklist_tab_col varchar(128) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_cust_attrib add constraint md_cust_attrib_pk primary key (attr_group_id,attr_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_cust_attrib, md_cust_attrib);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_custattrib_table_values(
        attr_id varchar(128) NOT NULL ,table_val varchar(256) NOT NULL
        ,
        attr_cd varchar(128) NULL ,attr_group_cd varchar(128) NULL ,attr_group_id varchar(128) NULL ,attr_group_nm varchar(128) NULL ,attr_nm varchar(128) NULL ,created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,data_formatter varchar(64) NULL ,data_type varchar(32) NULL ,is_obsolete_flg char(1) NULL ,last_modified_dttm timestamp NULL ,last_modified_usernm varchar(128) NULL ,load_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_custattrib_table_values add constraint md_custattrib_table_value_pk primary key (attr_id,table_val)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_custattrib_table_values, md_custattrib_table_values);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_dataview(
        dataview_version_id varchar(36) NOT NULL
        ,
        analytic_active_flg char(1) NULL ,analytics_period_type_nm varchar(10) NULL ,analytics_period_val int NULL ,created_user_nm varchar(256) NULL ,custom_recent_cd varchar(36) NULL ,custom_recent_exclude_cd varchar(36) NULL ,dataview_desc varchar(1332) NULL ,dataview_id varchar(36) NULL ,dataview_nm varchar(60) NULL ,dataview_status_cd varchar(20) NULL ,half_life_time_val int NULL ,include_external_flg char(1) NULL ,include_internal_flg char(1) NULL ,last_published_dttm timestamp NULL ,max_path_length_val int NULL ,max_path_time_type_nm varchar(10) NULL ,max_path_time_val int NULL ,owner_nm varchar(256) NULL ,selected_task_list varchar(1000) NULL ,valid_from_dttm timestamp NULL ,valid_to_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_dataview add constraint md_dataview_pk primary key (dataview_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_dataview, md_dataview);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_dataview_x_event(
        dataview_id varchar(36) NULL ,dataview_version_id varchar(36) NOT NULL ,event_id varchar(36) NOT NULL
        ,
        dataview_status_cd varchar(20) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_dataview_x_event add constraint md_dataview_x_event_pk primary key (dataview_version_id,event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_dataview_x_event, md_dataview_x_event);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_event(
        event_version_id varchar(36) NOT NULL
        ,
        channel_nm varchar(40) NULL ,created_user_nm varchar(256) NULL ,event_desc varchar(1332) NULL ,event_id varchar(36) NULL ,event_nm varchar(60) NULL ,event_status_cd varchar(20) NULL ,event_subtype_nm varchar(100) NULL ,event_type_nm varchar(40) NULL ,last_published_dttm timestamp NULL ,owner_nm varchar(256) NULL ,valid_from_dttm timestamp NULL ,valid_to_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_event add constraint md_event_pk primary key (event_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_event, md_event);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_fiscal_period(
        fp_id varchar(128) NOT NULL
        ,
        created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,fp_cls_ver varchar(128) NULL ,fp_desc varchar(1332) NULL ,fp_end_dt date NULL ,fp_nm varchar(128) NULL ,fp_obsolete_flg char(1) NULL ,fp_start_dt date NULL ,last_modified_dttm timestamp NULL ,last_modified_usernm varchar(128) NULL ,load_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_fiscal_period add constraint md_fiscal_period_pk primary key (fp_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_fiscal_period, md_fiscal_period);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_grid_attr_defn(
        attr_group_id varchar(128) NOT NULL ,attr_id varchar(128) NOT NULL ,grid_id varchar(128) NOT NULL
        ,
        associated_grid varchar(128) NULL ,attr_cd varchar(128) NULL ,attr_desc varchar(4000) NULL ,attr_group_cd varchar(128) NULL ,attr_group_nm varchar(128) NULL ,attr_nm varchar(128) NULL ,attr_obsolete_flg char(1) NULL ,attr_order_no int NULL ,created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,data_formatter varchar(64) NULL ,data_type varchar(32) NULL ,grid_cd varchar(128) NULL ,grid_desc varchar(4000) NULL ,grid_mandatory_flg char(1) NULL ,grid_nm varchar(128) NULL ,grid_obsolete_flg char(1) NULL ,last_modified_dttm timestamp NULL ,last_modified_usernm varchar(128) NULL ,load_dttm timestamp NULL ,remote_pklist_tab_col varchar(128) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_grid_attr_defn add constraint md_grid_attr_defn_pk primary key (attr_group_id,attr_id,grid_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_grid_attr_defn, md_grid_attr_defn);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_journey(
        journey_version_id varchar(36) NOT NULL
        ,
        activated_user_nm varchar(256) NULL ,control_group_flg char(1) NULL ,created_user_nm varchar(256) NULL ,journey_id varchar(36) NULL ,journey_nm varchar(256) NULL ,journey_status_cd varchar(20) NULL ,last_activated_dttm timestamp NULL ,purpose_id varchar(36) NULL ,target_goal_qty int NULL ,target_goal_type_nm varchar(20) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_journey add constraint md_journey_pk primary key (journey_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_journey, md_journey);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_journey_node(
        journey_node_id varchar(36) NOT NULL
        ,
        journey_id varchar(36) NULL ,journey_version_id varchar(36) NULL ,next_node_id varchar(36) NULL ,node_nm varchar(100) NULL ,node_type varchar(36) NULL ,previous_node_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_journey_node add constraint md_journey_node_pk primary key (journey_node_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_journey_node, md_journey_node);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_journey_node_occurrence(
        journey_node_occurrence_id varchar(36) NOT NULL
        ,
        end_dttm timestamp NULL ,error_messages varchar(256) NULL ,execution_status varchar(36) NULL ,journey_id varchar(36) NULL ,journey_node_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,journey_version_id varchar(36) NULL ,num_of_contacts_entered int NULL ,start_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_journey_node_occurrence add constraint md_journey_node_occurrenc_pk primary key (journey_node_occurrence_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_journey_node_occurrence, md_journey_node_occurrence);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_journey_occurrence(
        journey_occurrence_id varchar(36) NOT NULL
        ,
        aud_occurrence_id varchar(36) NULL ,end_dttm timestamp NULL ,error_messages varchar(256) NULL ,execution_status varchar(36) NULL ,journey_id varchar(36) NULL ,journey_occurrence_num int NULL ,journey_version_id varchar(36) NULL ,num_of_contacts_entered int NULL ,num_of_contacts_suppressed int NULL ,occurrence_type_nm varchar(36) NULL ,start_dttm timestamp NULL ,started_by_nm varchar(128) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_journey_occurrence add constraint md_journey_occurrence_pk primary key (journey_occurrence_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_journey_occurrence, md_journey_occurrence);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_journey_x_audience(
        audience_id varchar(36) NOT NULL ,journey_version_id varchar(36) NOT NULL
        ,
        aud_relationship_nm varchar(100) NULL ,journey_id varchar(36) NULL ,journey_node_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_journey_x_audience add constraint md_journey_x_audience_pk primary key (audience_id,journey_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_journey_x_audience, md_journey_x_audience);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_journey_x_event(
        event_id varchar(36) NOT NULL ,journey_node_id varchar(36) NOT NULL
        ,
        event_relationship_nm varchar(100) NULL ,journey_id varchar(36) NULL ,journey_version_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_journey_x_event add constraint md_journey_x_event_pk primary key (event_id,journey_node_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_journey_x_event, md_journey_x_event);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_journey_x_task(
        journey_node_id varchar(36) NOT NULL
        ,
        journey_id varchar(36) NULL ,journey_version_id varchar(36) NULL ,task_id varchar(36) NULL ,task_version_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_journey_x_task add constraint md_journey_x_task_pk primary key (journey_node_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_journey_x_task, md_journey_x_task);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_message(
        message_version_id varchar(36) NOT NULL
        ,
        business_context_id varchar(36) NULL ,created_user_nm varchar(256) NULL ,folder_path_nm varchar(256) NULL ,last_published_dttm timestamp NULL ,message_category_nm varchar(100) NULL ,message_cd varchar(60) NULL ,message_desc varchar(1332) NULL ,message_id varchar(36) NULL ,message_nm varchar(60) NULL ,message_status_cd varchar(20) NULL ,message_type_nm varchar(40) NULL ,owner_nm varchar(256) NULL ,valid_from_dttm timestamp NULL ,valid_to_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_message add constraint md_message_pk primary key (message_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_message, md_message);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_message_custom_prop(
        message_id varchar(36) NULL ,message_version_id varchar(36) NULL ,property_datatype_cd varchar(256) NULL ,property_nm varchar(256) NULL ,property_val varchar(1332) NULL
        ,
        message_status_cd varchar(36) NULL ,valid_from_dttm timestamp NULL ,valid_to_dttm timestamp NULL
        ,Hashed_pk_col varchar(64) NOT NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_message_custom_prop add constraint md_message_custom_prop_pk primary key (Hashed_pk_col)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_message_custom_prop, md_message_custom_prop);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_message_x_creative(
        creative_id varchar(36) NOT NULL ,message_id varchar(36) NULL ,message_version_id varchar(36) NOT NULL
        ,
        message_status_cd varchar(20) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_message_x_creative add constraint md_message_x_creative_pk primary key (creative_id,message_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_message_x_creative, md_message_x_creative);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_object_type(
        attr_group_id varchar(128) NOT NULL ,attr_id varchar(128) NOT NULL ,object_type varchar(64) NOT NULL
        ,
        attr_cd varchar(128) NULL ,attr_group_cd varchar(128) NULL ,attr_group_nm varchar(128) NULL ,attr_nm varchar(128) NULL ,created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,data_formatter varchar(64) NULL ,data_type varchar(32) NULL ,is_obsolete_flg char(1) NULL ,last_modified_dttm timestamp NULL ,last_modified_usernm varchar(128) NULL ,load_dttm timestamp NULL ,object_category varchar(64) NULL ,remote_pklist_tab_col varchar(128) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_object_type add constraint md_object_type_pk primary key (attr_group_id,attr_id,object_type)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_object_type, md_object_type);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_occurrence(
        occurrence_id varchar(36) NOT NULL
        ,
        end_tm timestamp NULL ,execution_status_cd varchar(50) NULL ,object_id varchar(36) NULL ,object_type_nm varchar(100) NULL ,object_version_id varchar(36) NULL ,occurrence_no int NULL ,occurrence_type_nm varchar(100) NULL ,properties_map_doc varchar(4000) NULL ,start_tm timestamp NULL ,started_by_nm varchar(100) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_occurrence add constraint md_occurrence_pk primary key (occurrence_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_occurrence, md_occurrence);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_picklist(
        attr_id varchar(128) NOT NULL ,plist_id varchar(128) NOT NULL
        ,
        attr_cd varchar(128) NULL ,attr_group_id varchar(128) NULL ,attr_group_nm varchar(128) NULL ,attr_nm varchar(128) NULL ,created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,is_obsolete_flg char(1) NULL ,last_modified_dttm timestamp NULL ,last_modified_usernm varchar(128) NULL ,load_dttm timestamp NULL ,plist_cd varchar(256) NULL ,plist_desc varchar(1332) NULL ,plist_nm varchar(128) NULL ,plist_val varchar(256) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_picklist add constraint md_picklist_pk primary key (attr_id,plist_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_picklist, md_picklist);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_purpose(
        purpose_id varchar(36) NOT NULL
        ,
        purpose_nm varchar(256) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_purpose add constraint md_purpose_pk primary key (purpose_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_purpose, md_purpose);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_rtc(
        occurrence_id varchar(36) NULL ,rtc_id varchar(36) NOT NULL ,segment_id varchar(36) NULL ,task_id varchar(36) NULL
        ,
        content_map_doc varchar(4000) NULL ,occurrence_no int NULL ,rtc_dttm timestamp NULL ,segment_version_id varchar(36) NULL ,task_version_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_rtc add constraint md_rtc_pk primary key (rtc_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_rtc, md_rtc);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_segment(
        business_context_id varchar(36) NULL ,segment_map_id varchar(36) NULL ,segment_version_id varchar(36) NOT NULL
        ,
        created_user_nm varchar(256) NULL ,folder_path_nm varchar(256) NULL ,last_published_dttm timestamp NULL ,owner_nm varchar(256) NULL ,segment_category_nm varchar(100) NULL ,segment_cd varchar(60) NULL ,segment_desc varchar(1332) NULL ,segment_id varchar(36) NULL ,segment_nm varchar(60) NULL ,segment_src_cd varchar(40) NULL ,segment_status_cd varchar(20) NULL ,valid_from_dttm timestamp NULL ,valid_to_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_segment add constraint md_segment_pk primary key (segment_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_segment, md_segment);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_segment_custom_prop(
        property_datatype_cd varchar(256) NULL ,property_nm varchar(256) NULL ,property_val varchar(1332) NULL ,segment_id varchar(36) NULL ,segment_version_id varchar(36) NULL
        ,
        segment_status_cd varchar(36) NULL ,valid_from_dttm timestamp NULL ,valid_to_dttm timestamp NULL
        ,Hashed_pk_col varchar(64) NOT NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_segment_custom_prop add constraint md_segment_custom_prop_pk primary key (Hashed_pk_col)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_segment_custom_prop, md_segment_custom_prop);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_segment_map(
        rec_scheduled_end_dttm timestamp NULL ,segment_map_version_id varchar(36) NOT NULL
        ,
        business_context_id varchar(36) NULL ,created_user_nm varchar(256) NULL ,folder_path_nm varchar(256) NULL ,last_published_dttm timestamp NULL ,owner_nm varchar(256) NULL ,rec_scheduled_start_dttm timestamp NULL ,rec_scheduled_start_tm varchar(20) NULL ,recurrence_day_of_month_no int NULL ,recurrence_day_of_week_txt varchar(100) NULL ,recurrence_day_of_wk_ordinal_no varchar(36) NULL ,recurrence_days_of_week_txt varchar(100) NULL ,recurrence_frequency_cd varchar(36) NULL ,recurrence_monthly_type_nm varchar(36) NULL ,scheduled_end_dttm timestamp NULL ,scheduled_flg char(1) NULL ,scheduled_start_dttm timestamp NULL ,segment_map_category_nm varchar(100) NULL ,segment_map_cd varchar(60) NULL ,segment_map_desc varchar(1332) NULL ,segment_map_id varchar(36) NULL ,segment_map_nm varchar(60) NULL ,segment_map_src_cd varchar(10) NULL ,segment_map_status_cd varchar(20) NULL ,valid_from_dttm timestamp NULL ,valid_to_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_segment_map add constraint md_segment_map_pk primary key (segment_map_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_segment_map, md_segment_map);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_segment_map_custom_prop(
        property_datatype_cd varchar(256) NULL ,property_nm varchar(256) NULL ,property_val varchar(1332) NULL ,segment_map_id varchar(36) NULL ,segment_map_version_id varchar(36) NULL
        ,
        segment_map_status_cd varchar(36) NULL ,valid_from_dttm timestamp NULL ,valid_to_dttm timestamp NULL
        ,Hashed_pk_col varchar(64) NOT NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_segment_map_custom_prop add constraint md_segment_map_custom_pro_pk primary key (Hashed_pk_col)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_segment_map_custom_prop, md_segment_map_custom_prop);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_segment_map_x_segment(
        segment_id varchar(36) NOT NULL ,segment_map_id varchar(36) NULL ,segment_map_version_id varchar(36) NOT NULL
        ,
        segment_map_status_cd varchar(20) NULL ,segment_version_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_segment_map_x_segment add constraint md_segment_map_x_segment_pk primary key (segment_id,segment_map_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_segment_map_x_segment, md_segment_map_x_segment);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_segment_test(
        task_version_id varchar(36) NOT NULL ,test_cd varchar(60) NOT NULL
        ,
        stratified_samp_criteria_txt varchar(1024) NULL ,stratified_sampling_flg char(1) NULL ,task_id varchar(36) NULL ,test_cnt int NULL ,test_enabled_flg char(1) NULL ,test_nm varchar(65) NULL ,test_pct decimal(5,2) NULL ,test_sizing_type_nm varchar(65) NULL ,test_type_nm varchar(10) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_segment_test add constraint md_segment_test_pk primary key (task_version_id,test_cd)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_segment_test, md_segment_test);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_segment_test_x_segment(
        segment_id varchar(36) NOT NULL ,task_version_id varchar(36) NOT NULL ,test_cd varchar(60) NOT NULL
        ,
        task_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_segment_test_x_segment add constraint md_segment_test_x_segment_pk primary key (segment_id,task_version_id,test_cd)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_segment_test_x_segment, md_segment_test_x_segment);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_segment_x_event(
        event_id varchar(36) NOT NULL ,segment_id varchar(36) NULL ,segment_version_id varchar(36) NOT NULL
        ,
        segment_status_cd varchar(20) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_segment_x_event add constraint md_segment_x_event_pk primary key (event_id,segment_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_segment_x_event, md_segment_x_event);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_spot(
        spot_version_id varchar(36) NOT NULL
        ,
        channel_nm varchar(40) NULL ,created_user_nm varchar(256) NULL ,dimension_label_txt varchar(156) NULL ,height_width_ratio_val_txt varchar(25) NULL ,last_published_dttm timestamp NULL ,location_selector_flg char(1) NULL ,multi_page_flg char(1) NULL ,owner_nm varchar(256) NULL ,spot_desc varchar(1332) NULL ,spot_height_val_no varchar(10) NULL ,spot_id varchar(36) NULL ,spot_key varchar(40) NULL ,spot_nm varchar(60) NULL ,spot_status_cd varchar(20) NULL ,spot_type_nm varchar(40) NULL ,spot_width_val_no varchar(10) NULL ,valid_from_dttm timestamp NULL ,valid_to_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_spot add constraint md_spot_pk primary key (spot_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_spot, md_spot);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_target_assist(
        task_id varchar(36) NOT NULL
        ,
        last_modified_dttm timestamp NULL ,model_available_dttm timestamp NULL ,percent_target_population_size int NULL ,threshold_type_nm char(30) NULL ,use_targeting_flg char(1) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_target_assist add constraint md_target_assist_pk primary key (task_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_target_assist, md_target_assist);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_task(
        business_context_id varchar(36) NULL ,task_version_id varchar(36) NOT NULL
        ,
        activity_flg char(1) NULL ,arbitration_method_cd varchar(36) NULL ,channel_nm varchar(40) NULL ,control_group_action_nm varchar(65) NULL ,created_user_nm varchar(256) NULL ,delivery_config_type_nm varchar(36) NULL ,display_priority_no int NULL ,export_template_flg char(1) NULL ,folder_path_nm varchar(256) NULL ,impressions_life_time_cnt int NULL ,impressions_per_session_cnt int NULL ,impressions_qty_period_cnt int NULL ,last_published_dttm timestamp NULL ,limit_period_unit_cnt int NULL ,maximum_period_expression_cnt int NULL ,mobile_app_id varchar(60) NULL ,mobile_app_nm varchar(60) NULL ,model_start_dttm timestamp NULL ,owner_nm varchar(256) NULL ,period_type_nm varchar(36) NULL ,rec_scheduled_end_dttm timestamp NULL ,rec_scheduled_start_dttm timestamp NULL ,rec_scheduled_start_tm varchar(20) NULL ,recurrence_day_of_month_no int NULL ,recurrence_day_of_week_txt varchar(60) NULL ,recurrence_day_of_wk_ordinal_no varchar(36) NULL ,recurrence_days_of_week_txt varchar(60) NULL ,recurrence_frequency_cd varchar(36) NULL ,recurrence_monthly_type_nm varchar(36) NULL ,recurring_schedule_flg char(1) NULL ,rtdm_flg char(1) NULL ,scheduled_end_dttm timestamp NULL ,scheduled_flg char(1) NULL ,scheduled_start_dttm timestamp NULL ,secondary_status varchar(40) NULL ,segment_tests_flg char(1) NULL ,send_notification_locale_cd varchar(5) NULL ,stratified_sampling_action_nm varchar(65) NULL ,subject_line_source_nm varchar(100) NULL ,subject_line_txt varchar(1332) NULL ,task_category_nm varchar(100) NULL ,task_cd varchar(60) NULL ,task_delivery_type_nm varchar(60) NULL ,task_desc varchar(1332) NULL ,task_id varchar(36) NULL ,task_nm varchar(60) NULL ,task_status_cd varchar(20) NULL ,task_subtype_nm varchar(30) NULL ,task_type_nm varchar(40) NULL ,template_id varchar(36) NULL ,test_duration int NULL ,use_modeling_flg char(1) NULL ,valid_from_dttm timestamp NULL ,valid_to_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_task add constraint md_task_pk primary key (task_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_task, md_task);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_task_custom_prop(
        property_datatype_nm varchar(256) NULL ,property_nm varchar(256) NULL ,property_val varchar(1332) NULL ,task_id varchar(36) NULL ,task_version_id varchar(36) NULL
        ,
        task_status_cd varchar(36) NULL ,valid_from_dttm timestamp NULL ,valid_to_dttm timestamp NULL
        ,Hashed_pk_col varchar(64) NOT NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_task_custom_prop add constraint md_task_custom_prop_pk primary key (Hashed_pk_col)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_task_custom_prop, md_task_custom_prop);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_task_x_audience(
        audience_id varchar(36) NOT NULL ,task_id varchar(36) NOT NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_task_x_audience add constraint md_task_x_audience_pk primary key (audience_id,task_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_task_x_audience, md_task_x_audience);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_task_x_creative(
        creative_id varchar(36) NOT NULL ,spot_id varchar(36) NOT NULL ,task_id varchar(36) NULL ,task_version_id varchar(36) NOT NULL
        ,
        arbitration_method_cd varchar(36) NULL ,arbitration_method_val varchar(3) NULL ,task_status_cd varchar(20) NULL ,variant_id varchar(36) NULL ,variant_nm varchar(256) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_task_x_creative add constraint md_task_x_creative_pk primary key (creative_id,spot_id,task_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_task_x_creative, md_task_x_creative);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_task_x_dataview(
        dataview_id varchar(36) NOT NULL ,task_id varchar(36) NULL ,task_version_id varchar(36) NOT NULL
        ,
        primary_metric_flg char(1) NULL ,secondary_metric_flg char(1) NULL ,targeting_flg char(1) NULL ,task_status_cd varchar(20) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_task_x_dataview add constraint md_task_x_dataview_pk primary key (dataview_id,task_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_task_x_dataview, md_task_x_dataview);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_task_x_event(
        event_id varchar(36) NOT NULL ,task_id varchar(36) NULL ,task_version_id varchar(36) NOT NULL
        ,
        primary_metric_flg char(1) NULL ,secondary_metric_flg char(1) NULL ,targeting_flg char(1) NULL ,task_status_cd varchar(20) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_task_x_event add constraint md_task_x_event_pk primary key (event_id,task_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_task_x_event, md_task_x_event);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_task_x_message(
        message_id varchar(36) NOT NULL ,task_id varchar(36) NULL ,task_version_id varchar(36) NOT NULL
        ,
        task_status_cd varchar(20) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_task_x_message add constraint md_task_x_message_pk primary key (message_id,task_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_task_x_message, md_task_x_message);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_task_x_segment(
        segment_id varchar(36) NOT NULL ,task_id varchar(36) NULL ,task_version_id varchar(36) NOT NULL
        ,
        task_status_cd varchar(20) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_task_x_segment add constraint md_task_x_segment_pk primary key (segment_id,task_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_task_x_segment, md_task_x_segment);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_task_x_spot(
        spot_id varchar(36) NOT NULL ,task_id varchar(36) NULL ,task_version_id varchar(36) NOT NULL
        ,
        task_status_cd varchar(20) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_task_x_spot add constraint md_task_x_spot_pk primary key (spot_id,task_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_task_x_spot, md_task_x_spot);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_task_x_variant(
        analysis_group_id varchar(36) NOT NULL ,task_id varchar(36) NULL ,task_version_id varchar(36) NOT NULL
        ,
        task_status_cd varchar(20) NULL ,variant_nm varchar(256) NULL ,variant_source_nm varchar(100) NULL ,variant_type_nm varchar(100) NULL ,variant_val varchar(1332) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_task_x_variant add constraint md_task_x_variant_pk primary key (analysis_group_id,task_version_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_task_x_variant, md_task_x_variant);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_vendor(
        vendor_id varchar(128) NOT NULL
        ,
        created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,is_obsolete_flg char(1) NULL ,last_modified_dttm timestamp NULL ,last_modified_usernm varchar(128) NULL ,load_dttm timestamp NULL ,owner_usernm varchar(128) NULL ,vendor_currency_cd varchar(10) NULL ,vendor_desc varchar(1332) NULL ,vendor_nm varchar(128) NULL ,vendor_number varchar(128) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_vendor add constraint md_vendor_pk primary key (vendor_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_vendor, md_vendor);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_wf_process_def(
        engine_pdef_id varchar(128) NOT NULL ,pdef_id varchar(128) NOT NULL
        ,
        associated_object_type varchar(128) NULL ,buildin_template_flg char(1) NULL ,created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,default_approval_flg char(1) NULL ,engine_pdef_key varchar(128) NULL ,file_tobecatlgd_flg char(1) NULL ,last_modified_dttm timestamp NULL ,last_modified_usernm varchar(128) NULL ,latest_version_flg char(1) NULL ,load_dttm timestamp NULL ,owner_usernm varchar(128) NULL ,pdef_desc varchar(1332) NULL ,pdef_nm varchar(128) NULL ,pdef_state varchar(128) NULL ,pdef_type varchar(128) NULL ,version_num number NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_wf_process_def add constraint md_wf_process_def_pk primary key (engine_pdef_id,pdef_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_wf_process_def, md_wf_process_def);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_wf_process_def_attr_grp(
        attr_group_id varchar(128) NOT NULL ,pdef_id varchar(128) NOT NULL
        ,
        load_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_wf_process_def_attr_grp add constraint md_wf_process_def_attr_gr_pk primary key (attr_group_id,pdef_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_wf_process_def_attr_grp, md_wf_process_def_attr_grp);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_wf_process_def_categories(
        category_id varchar(128) NOT NULL ,pdef_id varchar(128) NOT NULL
        ,
        category_type varchar(128) NULL ,default_category_flg char(1) NULL ,load_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_wf_process_def_categories add constraint md_wf_process_def_categor_pk primary key (category_id,pdef_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_wf_process_def_categories, md_wf_process_def_categories);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_wf_process_def_task_assg(
        assignee_id varchar(128) NOT NULL ,assignee_type varchar(128) NOT NULL ,pdef_id varchar(128) NOT NULL ,task_id varchar(128) NOT NULL
        ,
        assignee_duration varchar(128) NULL ,assignee_instruction varchar(128) NULL ,load_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_wf_process_def_task_assg add constraint md_wf_process_def_task_as_pk primary key (assignee_id,assignee_type,pdef_id,task_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_wf_process_def_task_assg, md_wf_process_def_task_assg);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..md_wf_process_def_tasks(
        pdef_id varchar(128) NOT NULL ,task_id varchar(128) NOT NULL
        ,
        assignee_type varchar(128) NULL ,ciobject_enabled_flg char(1) NULL ,comment_enabled_flg char(1) NULL ,comment_mandatory_flg char(1) NULL ,default_duration_perassignee number NULL ,file_enabled_flg char(1) NULL ,file_mandatory_flg char(1) NULL ,is_sequential_flg char(1) NULL ,item_approval_state varchar(128) NULL ,load_dttm timestamp NULL ,multiple_asgnsuprt_flg char(1) NULL ,outgoing_flow_flg char(1) NULL ,predecessor_task_id varchar(128) NULL ,res_mandatory_flg char(1) NULL ,resp_enabled_flg char(1) NULL ,resp_file_enabled_flg char(1) NULL ,show_sourceitemlink_flg char(1) NULL ,show_workflowlink_flg char(1) NULL ,source_item_field varchar(128) NULL ,task_desc varchar(1332) NULL ,task_instruction varchar(128) NULL ,task_nm varchar(128) NULL ,task_subtype varchar(128) NULL ,task_type varchar(128) NULL ,url_enabled_flg char(1) NULL
        )) by ORACLE;
      execute (alter table &dbschema..md_wf_process_def_tasks add constraint md_wf_process_def_tasks_pk primary key (pdef_id,task_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: md_wf_process_def_tasks, md_wf_process_def_tasks);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..media_activity_details(
        action_dttm timestamp NOT NULL ,detail_id varchar(36) NOT NULL ,detail_id_hex varchar(32) NULL ,media_nm varchar(260) NOT NULL
        ,
        action varchar(50) NULL ,action_dttm_tz timestamp NULL ,load_dttm timestamp NULL ,media_uri_txt varchar(2024) NULL ,playhead_position varchar(50) NULL
        )) by ORACLE;
      execute (alter table &dbschema..media_activity_details add constraint media_activity_details_pk primary key (action_dttm,detail_id,media_nm)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: media_activity_details, media_activity_details);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..media_details(
        detail_id varchar(32) NULL ,detail_id_hex varchar(32) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,session_id varchar(29) NULL ,session_id_hex varchar(29) NULL ,visit_id varchar(32) NULL ,visit_id_hex varchar(32) NULL
        ,
        event_key_cd varchar(100) NULL ,event_source_cd varchar(100) NULL ,load_dttm timestamp NULL ,media_duration_secs decimal(11,3) NULL ,media_nm varchar(260) NULL ,media_player_nm varchar(30) NULL ,media_player_version_txt varchar(20) NULL ,media_uri_txt varchar(2024) NULL ,play_start_dttm timestamp NULL ,play_start_dttm_tz timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..media_details add constraint media_details_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: media_details, media_details);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..media_details_ext(
        detail_id varchar(36) NOT NULL ,detail_id_hex varchar(32) NULL ,media_nm varchar(260) NOT NULL ,play_end_dttm timestamp NOT NULL
        ,
        end_tm decimal(11,3) NULL ,exit_point_secs decimal(11,3) NULL ,interaction_cnt int NULL ,load_dttm timestamp NULL ,max_play_secs decimal(11,3) NULL ,media_display_duration_secs decimal(11,3) NULL ,media_uri_txt varchar(2024) NULL ,play_end_dttm_tz timestamp NULL ,start_tm decimal(11,3) NULL ,view_duration_secs decimal(11,3) NULL
        )) by ORACLE;
      execute (alter table &dbschema..media_details_ext add constraint media_details_ext_pk primary key (detail_id,media_nm,play_end_dttm)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: media_details_ext, media_details_ext);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..mobile_focus_defocus(
        detail_id_hex varchar(32) NULL ,event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,session_id_hex varchar(29) NULL ,visit_id_hex varchar(32) NULL
        ,
        action_dttm timestamp NULL ,action_dttm_tz timestamp NULL ,channel_user_id varchar(300) NULL ,event_nm varchar(256) NULL ,load_dttm timestamp NULL ,mobile_app_id varchar(40) NULL ,reserved_1_txt varchar(100) NULL
        )) by ORACLE;
      execute (alter table &dbschema..mobile_focus_defocus add constraint mobile_focus_defocus_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: mobile_focus_defocus, mobile_focus_defocus);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..mobile_spots(
        creative_id varchar(36) NULL ,detail_id_hex varchar(32) NULL ,event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,session_id_hex varchar(29) NULL ,spot_id varchar(36) NULL ,visit_id_hex varchar(32) NULL
        ,
        action_dttm timestamp NULL ,action_dttm_tz timestamp NULL ,channel_user_id varchar(300) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,event_nm varchar(256) NULL ,load_dttm timestamp NULL ,mobile_app_id varchar(40) NULL
        )) by ORACLE;
      execute (alter table &dbschema..mobile_spots add constraint mobile_spots_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: mobile_spots, mobile_spots);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..monthly_usage(
        event_month varchar(36) NOT NULL
        ,
        admin_user_cnt int NULL ,api_usage_str varchar(4000) NULL ,asset_size decimal(17,2) NULL ,audience_usage_cnt number NULL ,bc_subjcnt_str varchar(4000) NULL ,customer_profiles_processed_str varchar NULL ,db_size decimal(17,2) NULL ,email_preview_cnt number NULL ,email_send_cnt number NULL ,facebook_ads_cnt number NULL ,google_ads_cnt number NULL ,linkedin_ads_cnt number NULL ,mob_impr_cnt number NULL ,mob_sesn_cnt number NULL ,mobile_in_app_msg_cnt number NULL ,mobile_push_cnt number NULL ,outbound_api_cnt number NULL ,plan_users_cnt number NULL ,web_impr_cnt number NULL ,web_sesn_cnt number NULL
        )) by ORACLE;
      execute (alter table &dbschema..monthly_usage add constraint monthly_usage_pk primary key (event_month)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: monthly_usage, monthly_usage);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..notification_failed(
        creative_id varchar(36) NULL ,event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,occurrence_id varchar(36) NULL ,spot_id varchar(36) NULL ,task_id varchar(36) NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,channel_nm varchar(40) NULL ,channel_user_id varchar(300) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,creative_version_id varchar(36) NULL ,error_cd varchar(256) NULL ,error_message_txt varchar(1332) NULL ,event_nm varchar(256) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm timestamp NULL ,message_id varchar(36) NULL ,message_version_id varchar(36) NULL ,mobile_app_id varchar(40) NULL ,notification_failed_dttm timestamp NULL ,notification_failed_dttm_tz timestamp NULL ,properties_map_doc varchar(4000) NULL ,reserved_1_txt varchar(100) NULL ,reserved_2_txt varchar(100) NULL ,response_tracking_cd varchar(36) NULL ,segment_id varchar(36) NULL ,segment_version_id varchar(36) NULL ,task_version_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..notification_failed add constraint notification_failed_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: notification_failed, notification_failed);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..notification_opened(
        creative_id varchar(36) NULL ,event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,occurrence_id varchar(36) NULL ,spot_id varchar(36) NULL ,task_id varchar(36) NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,channel_nm varchar(40) NULL ,channel_user_id varchar(300) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,creative_version_id varchar(36) NULL ,event_nm varchar(256) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm timestamp NULL ,message_id varchar(36) NULL ,message_version_id varchar(36) NULL ,mobile_app_id varchar(40) NULL ,notification_opened_dttm timestamp NULL ,notification_opened_dttm_tz timestamp NULL ,properties_map_doc varchar(4000) NULL ,reserved_1_txt varchar(100) NULL ,reserved_2_txt varchar(100) NULL ,reserved_3_txt varchar(100) NULL ,response_tracking_cd varchar(36) NULL ,segment_id varchar(36) NULL ,segment_version_id varchar(36) NULL ,task_version_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..notification_opened add constraint notification_opened_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: notification_opened, notification_opened);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..notification_send(
        creative_id varchar(36) NULL ,event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,occurrence_id varchar(36) NULL ,spot_id varchar(36) NULL ,task_id varchar(36) NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,channel_nm varchar(40) NULL ,channel_user_id varchar(300) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,creative_version_id varchar(36) NULL ,event_nm varchar(256) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm timestamp NULL ,message_id varchar(36) NULL ,message_version_id varchar(36) NULL ,mobile_app_id varchar(40) NULL ,notification_send_dttm timestamp NULL ,notification_send_dttm_tz timestamp NULL ,properties_map_doc varchar(4000) NULL ,reserved_1_txt varchar(100) NULL ,reserved_2_txt varchar(100) NULL ,response_tracking_cd varchar(36) NULL ,segment_id varchar(36) NULL ,segment_version_id varchar(36) NULL ,task_version_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..notification_send add constraint notification_send_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: notification_send, notification_send);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..notification_targeting_request(
        event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,channel_nm varchar(40) NULL ,channel_user_id varchar(300) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,eligibility_flg char(1) NULL ,event_nm varchar(256) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm timestamp NULL ,mobile_app_id varchar(40) NULL ,notification_tgt_req_dttm timestamp NULL ,notification_tgt_req_dttm_tz timestamp NULL ,task_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..notification_targeting_request add constraint notification_targeting_re_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: notification_targeting_request, notification_targeting_request);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..order_details(
        detail_id varchar(32) NOT NULL ,detail_id_hex varchar(32) NULL ,event_designed_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,product_id varchar(130) NOT NULL ,product_nm varchar(130) NOT NULL ,product_sku varchar(100) NOT NULL ,record_type varchar(15) NOT NULL ,session_id varchar(29) NULL ,session_id_hex varchar(29) NULL ,visit_id varchar(32) NULL ,visit_id_hex varchar(32) NULL
        ,
        activity_dttm timestamp NULL ,activity_dttm_tz timestamp NULL ,availability_message_txt varchar(650) NULL ,cart_id varchar(42) NULL ,cart_nm varchar(100) NULL ,channel_nm varchar(40) NULL ,currency_cd varchar(6) NULL ,event_id varchar(36) NULL ,event_key_cd varchar(100) NULL ,event_nm varchar(256) NULL ,event_source_cd varchar(100) NULL ,load_dttm timestamp NULL ,mobile_app_id varchar(40) NULL ,order_id varchar(42) NULL ,product_group_nm varchar(130) NULL ,properties_map_doc varchar(4000) NULL ,quantity_amt int NULL ,reserved_1_txt varchar(100) NULL ,saving_message_txt varchar(650) NULL ,shipping_message_txt varchar(650) NULL ,unit_price_amt decimal(17,2) NULL
        )) by ORACLE;
      execute (alter table &dbschema..order_details add constraint order_details_pk primary key (detail_id,event_designed_id,product_id,product_nm,product_sku,record_type)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: order_details, order_details);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..order_summary(
        detail_id varchar(32) NOT NULL ,detail_id_hex varchar(32) NULL ,event_designed_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,record_type varchar(15) NOT NULL ,session_id varchar(29) NULL ,session_id_hex varchar(29) NULL ,visit_id varchar(32) NULL ,visit_id_hex varchar(32) NULL
        ,
        activity_dttm timestamp NULL ,activity_dttm_tz timestamp NULL ,billing_city_nm varchar(390) NULL ,billing_country_nm varchar(85) NULL ,billing_postal_cd varchar(10) NULL ,billing_state_region_cd varchar(256) NULL ,cart_id varchar(42) NULL ,cart_nm varchar(100) NULL ,channel_nm varchar(40) NULL ,currency_cd varchar(6) NULL ,delivery_type_desc varchar(42) NULL ,event_id varchar(36) NULL ,event_key_cd varchar(100) NULL ,event_nm varchar(256) NULL ,event_source_cd varchar(100) NULL ,load_dttm timestamp NULL ,mobile_app_id varchar(40) NULL ,order_id varchar(42) NULL ,payment_type_desc varchar(42) NULL ,properties_map_doc varchar(4000) NULL ,shipping_amt decimal(17,2) NULL ,shipping_city_nm varchar(390) NULL ,shipping_country_nm varchar(85) NULL ,shipping_postal_cd varchar(10) NULL ,shipping_state_region_cd varchar(256) NULL ,total_price_amt decimal(17,2) NULL ,total_tax_amt decimal(17,2) NULL ,total_unit_qty int NULL
        )) by ORACLE;
      execute (alter table &dbschema..order_summary add constraint order_summary_pk primary key (detail_id,event_designed_id,record_type)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: order_summary, order_summary);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..outbound_system(
        creative_id varchar(36) NULL ,detail_id_hex varchar(32) NULL ,event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,occurrence_id varchar(36) NULL ,parent_event_id varchar(36) NULL ,session_id_hex varchar(29) NULL ,spot_id varchar(36) NULL ,task_id varchar(36) NULL ,visit_id_hex varchar(32) NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,channel_nm varchar(40) NULL ,channel_user_id varchar(300) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,creative_version_id varchar(36) NULL ,event_nm varchar(256) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm timestamp NULL ,message_id varchar(36) NULL ,message_version_id varchar(36) NULL ,mobile_app_id varchar(40) NULL ,outbound_system_dttm timestamp NULL ,outbound_system_dttm_tz timestamp NULL ,properties_map_doc varchar(4000) NULL ,reserved_1_txt varchar(100) NULL ,reserved_2_txt varchar(100) NULL ,response_tracking_cd varchar(36) NULL ,segment_id varchar(36) NULL ,segment_version_id varchar(36) NULL ,task_version_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..outbound_system add constraint outbound_system_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: outbound_system, outbound_system);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..page_details(
        event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,session_id varchar(29) NULL ,session_id_hex varchar(29) NULL ,visit_id varchar(32) NULL ,visit_id_hex varchar(32) NULL
        ,
        bytes_sent_cnt int NULL ,channel_nm varchar(40) NULL ,class10_id varchar(650) NULL ,class11_id varchar(650) NULL ,class12_id varchar(650) NULL ,class13_id varchar(650) NULL ,class14_id varchar(650) NULL ,class15_id varchar(650) NULL ,class1_id varchar(650) NULL ,class2_id varchar(650) NULL ,class3_id varchar(650) NULL ,class4_id varchar(650) NULL ,class5_id varchar(650) NULL ,class6_id varchar(650) NULL ,class7_id varchar(650) NULL ,class8_id varchar(650) NULL ,class9_id varchar(650) NULL ,detail_dttm timestamp NULL ,detail_dttm_tz timestamp NULL ,detail_id varchar(32) NULL ,detail_id_hex varchar(32) NULL ,domain_nm varchar(165) NULL ,event_key_cd varchar(100) NULL ,event_nm varchar(256) NULL ,event_source_cd varchar(100) NULL ,load_dttm timestamp NULL ,mobile_app_id varchar(40) NULL ,page_complete_sec_cnt int NULL ,page_desc varchar(1332) NULL ,page_load_sec_cnt int NULL ,page_url_txt varchar(1332) NULL ,protocol_nm varchar(8) NULL ,referrer_url_txt varchar(1332) NULL ,session_dt date NULL ,session_dt_tz date NULL ,url_domain varchar(215) NULL ,window_size_txt varchar(20) NULL
        )) by ORACLE;
      execute (alter table &dbschema..page_details add constraint page_details_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: page_details, page_details);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..page_details_ext(
        detail_id varchar(32) NOT NULL ,detail_id_hex varchar(32) NULL ,load_dttm timestamp NOT NULL ,session_id varchar(29) NOT NULL ,session_id_hex varchar(29) NULL
        ,
        active_sec_spent_on_page_cnt int NULL ,seconds_spent_on_page_cnt int NULL
        )) by ORACLE;
      execute (alter table &dbschema..page_details_ext add constraint page_details_ext_pk primary key (detail_id,load_dttm,session_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: page_details_ext, page_details_ext);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..page_errors(
        detail_id varchar(32) NULL ,detail_id_hex varchar(32) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,session_id varchar(29) NULL ,session_id_hex varchar(29) NULL ,visit_id varchar(32) NULL ,visit_id_hex varchar(32) NULL
        ,
        error_location_txt varchar(41) NULL ,event_source_cd varchar(100) NULL ,in_page_error_dttm timestamp NULL ,in_page_error_dttm_tz timestamp NULL ,in_page_error_txt varchar(260) NULL ,load_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..page_errors add constraint page_errors_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: page_errors, page_errors);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..planning_hierarchy_defn(
        hier_defn_id varchar(128) NOT NULL ,level_nm varchar(128) NOT NULL ,level_no int NOT NULL
        ,
        created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,hier_defn_desc varchar(1332) NULL ,hier_defn_nm varchar(128) NULL ,hier_defn_subtype varchar(128) NULL ,hier_defn_type varchar(128) NULL ,last_modified_dttm timestamp NULL ,last_modified_usernm varchar(128) NULL ,level_desc varchar(1332) NULL ,load_dttm timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..planning_hierarchy_defn add constraint planning_hierarchy_defn_pk primary key (hier_defn_id,level_nm,level_no)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: planning_hierarchy_defn, planning_hierarchy_defn);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..planning_info(
        activity_id varchar(128) NULL ,bu_id varchar(128) NULL ,hier_defn_id varchar(128) NOT NULL ,planning_id varchar(128) NOT NULL
        ,
        activity_desc varchar(1332) NULL ,activity_nm varchar(128) NULL ,activity_status varchar(128) NULL ,all_msgs varchar(4000) NULL ,alloc_budget decimal(17,2) NULL ,available_budget decimal(17,2) NULL ,bu_currency_cd varchar(10) NULL ,bu_desc varchar(1332) NULL ,bu_nm varchar(128) NULL ,bu_obsolete_flg char(1) NULL ,category_nm varchar(128) NULL ,created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,currency_cd varchar(10) NULL ,hier_defn_nodeid varchar(128) NULL ,last_modified_dttm timestamp NULL ,last_modified_usernm varchar(128) NULL ,lev10_nm varchar(128) NULL ,lev1_nm varchar(128) NULL ,lev2_nm varchar(128) NULL ,lev3_nm varchar(128) NULL ,lev4_nm varchar(128) NULL ,lev5_nm varchar(128) NULL ,lev6_nm varchar(128) NULL ,lev7_nm varchar(128) NULL ,lev8_nm varchar(128) NULL ,lev9_nm varchar(128) NULL ,load_dttm timestamp NULL ,parent_id varchar(128) NULL ,parent_nm varchar(128) NULL ,planned_end_dttm timestamp NULL ,planned_start_dttm timestamp NULL ,planning_desc varchar(1332) NULL ,planning_item_path varchar(4000) NULL ,planning_level_no varchar(10) NULL ,planning_level_type varchar(32) NULL ,planning_nm varchar(128) NULL ,planning_number varchar(128) NULL ,planning_owner_usernm varchar(128) NULL ,planning_status varchar(32) NULL ,planning_type varchar(32) NULL ,reserved_budget decimal(17,2) NULL ,reserved_budget_same_flg char(1) NULL ,rolledup_budget decimal(17,2) NULL ,task_channel varchar(64) NULL ,task_desc varchar(1332) NULL ,task_id varchar(128) NULL ,task_nm varchar(128) NULL ,task_status varchar(64) NULL ,tot_cmtmnt_outstanding decimal(17,2) NULL ,tot_cmtmnt_overspent decimal(17,2) NULL ,tot_committed decimal(17,2) NULL ,tot_expenses decimal(17,2) NULL ,tot_invoiced decimal(17,2) NULL ,total_budget decimal(17,2) NULL
        )) by ORACLE;
      execute (alter table &dbschema..planning_info add constraint planning_info_pk primary key (hier_defn_id,planning_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: planning_info, planning_info);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..planning_info_custom_prop(
        attr_group_id varchar(128) NOT NULL ,attr_id varchar(128) NOT NULL ,planning_id varchar(128) NOT NULL
        ,
        attr_cd varchar(128) NULL ,attr_group_cd varchar(128) NULL ,attr_group_nm varchar(128) NULL ,attr_nm varchar(128) NULL ,attr_val varchar(4000) NULL ,created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,data_formatter varchar(64) NULL ,data_type varchar(32) NULL ,is_grid_flg char(1) NULL ,is_obsolete_flg char(1) NULL ,last_modified_dttm timestamp NULL ,last_modified_usernm varchar(128) NULL ,load_dttm timestamp NULL ,remote_pklist_tab_col varchar(128) NULL
        )) by ORACLE;
      execute (alter table &dbschema..planning_info_custom_prop add constraint planning_info_custom_prop_pk primary key (attr_group_id,attr_id,planning_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: planning_info_custom_prop, planning_info_custom_prop);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..product_views(
        detail_id varchar(32) NOT NULL ,detail_id_hex varchar(32) NULL ,event_designed_id varchar(36) NULL ,identity_id varchar(36) NULL ,product_id varchar(130) NOT NULL ,product_nm varchar(130) NOT NULL ,product_sku varchar(100) NOT NULL ,session_id varchar(29) NULL ,session_id_hex varchar(29) NULL ,visit_id varchar(32) NULL ,visit_id_hex varchar(32) NULL
        ,
        action_dttm timestamp NULL ,action_dttm_tz timestamp NULL ,availability_message_txt varchar(650) NULL ,channel_nm varchar(40) NULL ,currency_cd varchar(6) NULL ,event_id varchar(36) NULL ,event_key_cd varchar(100) NULL ,event_nm varchar(256) NULL ,event_source_cd varchar(100) NULL ,load_dttm timestamp NULL ,mobile_app_id varchar(40) NULL ,price_val decimal(17,2) NULL ,product_group_nm varchar(130) NULL ,properties_map_doc varchar(4000) NULL ,saving_message_txt varchar(650) NULL ,shipping_message_txt varchar(650) NULL
        )) by ORACLE;
      execute (alter table &dbschema..product_views add constraint product_views_pk primary key (detail_id,product_id,product_nm,product_sku)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: product_views, product_views);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..promotion_displayed(
        detail_id varchar(32) NULL ,detail_id_hex varchar(32) NULL ,event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,session_id varchar(29) NULL ,session_id_hex varchar(29) NULL ,visit_id varchar(32) NULL ,visit_id_hex varchar(32) NULL
        ,
        channel_nm varchar(40) NULL ,derived_display_flg char(1) NULL ,display_dttm timestamp NULL ,display_dttm_tz timestamp NULL ,event_key_cd varchar(100) NULL ,event_nm varchar(256) NULL ,event_source_cd varchar(100) NULL ,load_dttm timestamp NULL ,mobile_app_id varchar(40) NULL ,promotion_creative_nm varchar(260) NULL ,promotion_nm varchar(260) NULL ,promotion_number int NULL ,promotion_placement_nm varchar(260) NULL ,promotion_tracking_cd varchar(65) NULL ,promotion_type_nm varchar(65) NULL ,properties_map_doc varchar(4000) NULL
        )) by ORACLE;
      execute (alter table &dbschema..promotion_displayed add constraint promotion_displayed_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: promotion_displayed, promotion_displayed);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..promotion_used(
        detail_id varchar(32) NULL ,detail_id_hex varchar(32) NULL ,event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,session_id varchar(29) NULL ,session_id_hex varchar(29) NULL ,visit_id varchar(32) NULL ,visit_id_hex varchar(32) NULL
        ,
        channel_nm varchar(40) NULL ,click_dttm timestamp NULL ,click_dttm_tz timestamp NULL ,event_key_cd varchar(100) NULL ,event_nm varchar(256) NULL ,event_source_cd varchar(100) NULL ,load_dttm timestamp NULL ,mobile_app_id varchar(40) NULL ,promotion_creative_nm varchar(260) NULL ,promotion_nm varchar(260) NULL ,promotion_number int NULL ,promotion_placement_nm varchar(260) NULL ,promotion_tracking_cd varchar(65) NULL ,promotion_type_nm varchar(65) NULL ,properties_map_doc varchar(4000) NULL
        )) by ORACLE;
      execute (alter table &dbschema..promotion_used add constraint promotion_used_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: promotion_used, promotion_used);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..response_history(
        creative_id varchar(36) NULL ,detail_id_hex varchar(32) NULL ,event_designed_id varchar(36) NULL ,identity_id varchar(36) NULL ,message_id varchar(36) NULL ,occurrence_id varchar(36) NULL ,parent_event_designed_id varchar(36) NULL ,response_id varchar(36) NOT NULL ,response_tracking_cd varchar(36) NULL ,session_id_hex varchar(29) NULL ,task_id varchar(36) NULL ,visit_id_hex varchar(32) NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm timestamp NULL ,properties_map_doc varchar(4000) NULL ,response_channel_nm varchar(40) NULL ,response_dttm timestamp NULL ,response_dttm_tz timestamp NULL ,response_nm varchar(256) NULL ,task_version_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..response_history add constraint response_history_pk primary key (response_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: response_history, response_history);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..search_results(
        detail_id varchar(32) NULL ,detail_id_hex varchar(32) NULL ,event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,search_results_sk varchar(100) NULL ,session_id varchar(29) NULL ,session_id_hex varchar(29) NULL ,visit_id varchar(32) NULL ,visit_id_hex varchar(32) NULL
        ,
        channel_nm varchar(40) NULL ,event_key_cd varchar(100) NULL ,event_nm varchar(256) NULL ,event_source_cd varchar(100) NULL ,load_dttm timestamp NULL ,mobile_app_id varchar(40) NULL ,properties_map_doc varchar(4000) NULL ,results_displayed_flg char(1) NULL ,search_nm varchar(42) NULL ,search_results_displayed int NULL ,search_results_dttm timestamp NULL ,search_results_dttm_tz timestamp NULL ,srch_field_id varchar(325) NULL ,srch_field_name varchar(325) NULL ,srch_phrase varchar(2600) NULL
        )) by ORACLE;
      execute (alter table &dbschema..search_results add constraint search_results_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: search_results, search_results);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..search_results_ext(
        event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL
        ,
        load_dttm timestamp NULL ,search_results_displayed int NULL ,search_results_sk varchar(100) NULL
        )) by ORACLE;
      execute (alter table &dbschema..search_results_ext add constraint search_results_ext_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: search_results_ext, search_results_ext);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..session_details(
        identity_id varchar(36) NULL ,session_id varchar(29) NOT NULL
        ,
        app_id varchar(36) NULL ,app_version varchar(10) NULL ,browser_nm varchar(52) NULL ,browser_version_no varchar(16) NULL ,carrier_name varchar(36) NULL ,channel_nm varchar(40) NULL ,city_nm varchar(390) NULL ,client_session_start_dttm timestamp NULL ,client_session_start_dttm_tz timestamp NULL ,cookies_enabled_flg char(1) NULL ,country_cd varchar(2) NULL ,country_nm varchar(85) NULL ,device_language varchar(12) NULL ,device_nm varchar(85) NULL ,device_type_nm varchar(32) NULL ,event_id varchar(36) NULL ,flash_enabled_flg char(1) NULL ,flash_version_no varchar(16) NULL ,ip_address varchar(64) NULL ,is_portable_flag char(1) NULL ,java_enabled_flg char(1) NULL ,java_script_enabled_flg char(1) NULL ,java_version_no varchar(12) NULL ,latitude decimal(13,6) NULL ,load_dttm timestamp NULL ,longitude decimal(13,6) NULL ,manufacturer varchar(75) NULL ,metro_cd int NULL ,mobile_country_code varchar(10) NULL ,network_code varchar(10) NULL ,new_visitor_flg varchar(2) NULL ,organization_nm varchar(256) NULL ,parent_event_id varchar(36) NULL ,platform_desc varchar(78) NULL ,platform_type_nm varchar(52) NULL ,platform_version varchar(25) NULL ,postal_cd varchar(13) NULL ,previous_session_id varchar(29) NULL ,previous_session_id_hex varchar(29) NULL ,profile_nm1 varchar(169) NULL ,profile_nm2 varchar(169) NULL ,profile_nm3 varchar(169) NULL ,profile_nm4 varchar(169) NULL ,profile_nm5 varchar(169) NULL ,region_nm varchar(256) NULL ,screen_color_depth_no int NULL ,screen_size_txt varchar(12) NULL ,sdk_version varchar(25) NULL ,session_dt date NULL ,session_dt_tz date NULL ,session_id_hex varchar(29) NULL ,session_start_dttm timestamp NULL ,session_start_dttm_tz timestamp NULL ,session_timeout int NULL ,state_region_cd varchar(2) NULL ,user_agent_nm varchar(512) NULL ,user_language_cd varchar(12) NULL ,visitor_id varchar(32) NULL
        )) by ORACLE;
      execute (alter table &dbschema..session_details add constraint session_details_pk primary key (session_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: session_details, session_details);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..session_details_ext(
        last_session_activity_dttm timestamp NOT NULL ,session_id varchar(29) NOT NULL
        ,
        active_sec_spent_in_sessn_cnt int NULL ,last_session_activity_dttm_tz timestamp NULL ,load_dttm timestamp NULL ,seconds_spent_in_session_cnt int NULL ,session_expiration_dttm timestamp NULL ,session_expiration_dttm_tz timestamp NULL ,session_id_hex varchar(29) NULL
        )) by ORACLE;
      execute (alter table &dbschema..session_details_ext add constraint session_details_ext_pk primary key (last_session_activity_dttm,session_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: session_details_ext, session_details_ext);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..sms_message_clicked(
        event_id varchar(36) NOT NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,country_cd varchar(3) NULL ,creative_id varchar(36) NULL ,creative_version_id varchar(36) NULL ,event_designed_id varchar(36) NULL ,event_nm varchar(256) NULL ,identity_id varchar(36) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm timestamp NULL ,occurrence_id varchar(36) NULL ,response_tracking_cd varchar(36) NULL ,sender_id varchar(40) NULL ,sms_click_dttm timestamp NULL ,sms_click_dttm_tz timestamp NULL ,sms_message_id varchar(40) NULL ,task_id varchar(36) NULL ,task_version_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..sms_message_clicked add constraint sms_message_clicked_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: sms_message_clicked, sms_message_clicked);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..sms_message_delivered(
        event_id varchar(36) NOT NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,country_cd varchar(3) NULL ,creative_id varchar(36) NULL ,creative_version_id varchar(36) NULL ,event_designed_id varchar(36) NULL ,event_nm varchar(256) NULL ,identity_id varchar(36) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm timestamp NULL ,occurrence_id varchar(36) NULL ,response_tracking_cd varchar(36) NULL ,sender_id varchar(40) NULL ,sms_delivered_dttm timestamp NULL ,sms_delivered_dttm_tz timestamp NULL ,sms_message_id varchar(40) NULL ,task_id varchar(36) NULL ,task_version_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..sms_message_delivered add constraint sms_message_delivered_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: sms_message_delivered, sms_message_delivered);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..sms_message_failed(
        event_id varchar(36) NOT NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,country_cd varchar(3) NULL ,creative_id varchar(36) NULL ,creative_version_id varchar(36) NULL ,event_designed_id varchar(36) NULL ,event_nm varchar(256) NULL ,identity_id varchar(36) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm timestamp NULL ,occurrence_id varchar(36) NULL ,reason_cd varchar(5) NULL ,reason_description_txt varchar(1500) NULL ,response_tracking_cd varchar(36) NULL ,sender_id varchar(40) NULL ,sms_failed_dttm timestamp NULL ,sms_failed_dttm_tz timestamp NULL ,sms_message_id varchar(40) NULL ,task_id varchar(36) NULL ,task_version_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..sms_message_failed add constraint sms_message_failed_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: sms_message_failed, sms_message_failed);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..sms_message_reply(
        event_id varchar(36) NOT NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,country_cd varchar(3) NULL ,event_designed_id varchar(36) NULL ,event_nm varchar(256) NULL ,identity_id varchar(36) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm timestamp NULL ,occurrence_id varchar(36) NULL ,response_tracking_cd varchar(36) NULL ,sender_id varchar(40) NULL ,sms_content varchar(40) NULL ,sms_message_id varchar(40) NULL ,sms_reply_dttm timestamp NULL ,sms_reply_dttm_tz timestamp NULL ,task_id varchar(36) NULL ,task_version_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..sms_message_reply add constraint sms_message_reply_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: sms_message_reply, sms_message_reply);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..sms_message_send(
        event_id varchar(36) NOT NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,country_cd varchar(3) NULL ,creative_id varchar(36) NULL ,creative_version_id varchar(36) NULL ,event_designed_id varchar(36) NULL ,event_nm varchar(256) NULL ,fragment_cnt int NULL ,identity_id varchar(36) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm timestamp NULL ,occurrence_id varchar(36) NULL ,response_tracking_cd varchar(36) NULL ,sender_id varchar(40) NULL ,sms_message_id varchar(40) NULL ,sms_send_dttm timestamp NULL ,sms_send_dttm_tz timestamp NULL ,task_id varchar(36) NULL ,task_version_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..sms_message_send add constraint sms_message_send_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: sms_message_send, sms_message_send);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..sms_optout(
        event_id varchar(36) NOT NULL
        ,
        aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,country_cd varchar(3) NULL ,creative_id varchar(36) NULL ,creative_version_id varchar(36) NULL ,event_designed_id varchar(36) NULL ,event_nm varchar(256) NULL ,identity_id varchar(36) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm timestamp NULL ,occurrence_id varchar(36) NULL ,response_tracking_cd varchar(36) NULL ,sender_id varchar(40) NULL ,sms_message_id varchar(40) NULL ,sms_optout_dttm timestamp NULL ,sms_optout_dttm_tz timestamp NULL ,task_id varchar(36) NULL ,task_version_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..sms_optout add constraint sms_optout_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: sms_optout, sms_optout);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..sms_optout_details(
        event_id varchar(36) NOT NULL
        ,
        address_val varchar(20) NULL ,aud_occurrence_id varchar(36) NULL ,audience_id varchar(36) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,country_cd varchar(3) NULL ,creative_id varchar(36) NULL ,creative_version_id varchar(36) NULL ,event_designed_id varchar(36) NULL ,event_nm varchar(256) NULL ,identity_id varchar(36) NULL ,journey_id varchar(36) NULL ,journey_occurrence_id varchar(36) NULL ,load_dttm timestamp NULL ,occurrence_id varchar(36) NULL ,response_tracking_cd varchar(36) NULL ,sender_id varchar(40) NULL ,sms_message_id varchar(40) NULL ,sms_optout_dttm timestamp NULL ,sms_optout_dttm_tz timestamp NULL ,task_id varchar(36) NULL ,task_version_id varchar(36) NULL
        )) by ORACLE;
      execute (alter table &dbschema..sms_optout_details add constraint sms_optout_details_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: sms_optout_details, sms_optout_details);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..spot_clicked(
        creative_id varchar(36) NULL ,detail_id_hex varchar(32) NULL ,event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,occurrence_id varchar(36) NULL ,session_id_hex varchar(29) NULL ,spot_id varchar(36) NULL ,task_id varchar(36) NULL ,visit_id_hex varchar(32) NULL
        ,
        channel_nm varchar(40) NULL ,channel_user_id varchar(300) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,control_group_flg char(1) NULL ,creative_version_id varchar(36) NULL ,event_key_cd varchar(100) NULL ,event_nm varchar(256) NULL ,event_source_cd varchar(100) NULL ,load_dttm timestamp NULL ,message_id varchar(36) NULL ,message_version_id varchar(36) NULL ,mobile_app_id varchar(40) NULL ,product_id varchar(128) NULL ,product_nm varchar(128) NULL ,product_qty_no int NULL ,product_sku_no varchar(100) NULL ,properties_map_doc varchar(4000) NULL ,rec_group_id varchar(3) NULL ,request_id varchar(36) NULL ,reserved_1_txt varchar(100) NULL ,reserved_2_txt varchar(100) NULL ,response_tracking_cd varchar(36) NULL ,segment_id varchar(36) NULL ,segment_version_id varchar(36) NULL ,spot_clicked_dttm timestamp NULL ,spot_clicked_dttm_tz timestamp NULL ,task_version_id varchar(36) NULL ,url_txt varchar(1332) NULL
        )) by ORACLE;
      execute (alter table &dbschema..spot_clicked add constraint spot_clicked_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: spot_clicked, spot_clicked);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..spot_requested(
        detail_id_hex varchar(32) NULL ,event_designed_id varchar(36) NULL ,event_id varchar(36) NOT NULL ,identity_id varchar(36) NULL ,session_id_hex varchar(29) NULL ,spot_id varchar(36) NULL ,visit_id_hex varchar(32) NULL
        ,
        channel_nm varchar(40) NULL ,channel_user_id varchar(300) NULL ,context_type_nm varchar(256) NULL ,context_val varchar(256) NULL ,event_nm varchar(256) NULL ,event_source_cd varchar(100) NULL ,load_dttm timestamp NULL ,mobile_app_id varchar(40) NULL ,properties_map_doc varchar(4000) NULL ,request_id varchar(36) NULL ,spot_requested_dttm timestamp NULL ,spot_requested_dttm_tz timestamp NULL
        )) by ORACLE;
      execute (alter table &dbschema..spot_requested add constraint spot_requested_pk primary key (event_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: spot_requested, spot_requested);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..tag_details(
        component_id varchar(128) NOT NULL ,component_type varchar(32) NOT NULL ,tag_id varchar(128) NOT NULL
        ,
        created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,identity_cd varchar(128) NULL ,last_modified_dttm timestamp NULL ,last_modified_usernm varchar(128) NULL ,load_dttm timestamp NULL ,tag_desc varchar(1332) NULL ,tag_nm varchar(128) NULL ,tag_owner_usernm varchar(128) NULL
        )) by ORACLE;
      execute (alter table &dbschema..tag_details add constraint tag_details_pk primary key (component_id,component_type,tag_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: tag_details, tag_details);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..visit_details(
        identity_id varchar(36) NULL ,session_id varchar(29) NULL ,session_id_hex varchar(29) NULL ,visit_id varchar(32) NOT NULL
        ,
        event_id varchar(36) NULL ,load_dttm timestamp NULL ,origination_creative_nm varchar(260) NULL ,origination_nm varchar(260) NULL ,origination_placement_nm varchar(390) NULL ,origination_tracking_cd varchar(65) NULL ,origination_type_nm varchar(65) NULL ,referrer_domain_nm varchar(215) NULL ,referrer_query_string_txt varchar(1332) NULL ,referrer_txt varchar(1332) NULL ,search_engine_desc varchar(130) NULL ,search_engine_domain_txt varchar(215) NULL ,search_term_txt varchar(1332) NULL ,sequence_no int NULL ,visit_dttm timestamp NULL ,visit_dttm_tz timestamp NULL ,visit_id_hex varchar(32) NULL
        )) by ORACLE;
      execute (alter table &dbschema..visit_details add constraint visit_details_pk primary key (visit_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: visit_details, visit_details);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..wf_process_details(
        pdef_id varchar(128) NOT NULL ,process_id varchar(128) NOT NULL
        ,
        business_info_id varchar(128) NULL ,business_info_nm varchar(128) NULL ,business_info_type varchar(128) NULL ,completed_dttm timestamp NULL ,created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,delayed_by_day number NULL ,deleted_by_usernm varchar(128) NULL ,deleted_dttm timestamp NULL ,indexed_dttm timestamp NULL ,last_modified_dttm timestamp NULL ,last_modified_usernm varchar(128) NULL ,load_dttm timestamp NULL ,modified_status_cd varchar(128) NULL ,percent_complete number NULL ,planned_end_dttm timestamp NULL ,process_category varchar(128) NULL ,process_comment varchar(128) NULL ,process_desc varchar(1332) NULL ,process_instance_version varchar(128) NULL ,process_nm varchar(128) NULL ,process_owner_usernm varchar(128) NULL ,process_status varchar(128) NULL ,process_type varchar(128) NULL ,projected_end_dttm timestamp NULL ,published_by_usernm varchar(128) NULL ,published_dttm timestamp NULL ,start_dttm timestamp NULL ,submitted_by_usernm varchar(128) NULL ,submitted_dttm timestamp NULL ,timeline_calculated_dttm timestamp NULL ,user_tasks_cnt number NULL
        )) by ORACLE;
      execute (alter table &dbschema..wf_process_details add constraint wf_process_details_pk primary key (pdef_id,process_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: wf_process_details, wf_process_details);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..wf_process_details_custom_prop(
        attr_group_id varchar(128) NOT NULL ,attr_id varchar(128) NOT NULL ,process_id varchar(128) NOT NULL
        ,
        attr_cd varchar(128) NULL ,attr_group_cd varchar(128) NULL ,attr_group_nm varchar(128) NULL ,attr_nm varchar(128) NULL ,attr_val varchar(4000) NULL ,created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,data_formatter varchar(64) NULL ,data_type varchar(32) NULL ,is_grid_flg char(1) NULL ,is_obsolete_flg char(1) NULL ,last_modified_dttm timestamp NULL ,last_modified_usernm varchar(128) NULL ,load_dttm timestamp NULL ,remote_pklist_tab_col varchar(128) NULL
        )) by ORACLE;
      execute (alter table &dbschema..wf_process_details_custom_prop add constraint wf_process_details_custom_pk primary key (attr_group_id,attr_id,process_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: wf_process_details_custom_prop, wf_process_details_custom_prop);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..wf_process_tasks(
        engine_taskdef_id varchar(128) NOT NULL ,process_id varchar(128) NOT NULL ,task_id varchar(128) NOT NULL
        ,
        approval_task_flg char(1) NULL ,cancelled_task_flg char(1) NULL ,completed_dttm timestamp NULL ,created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,delayed_by_day number NULL ,deleted_by_usernm varchar(128) NULL ,deleted_dttm timestamp NULL ,due_dttm timestamp NULL ,duration_per_assignee number NULL ,engine_task_cancelled_dttm timestamp NULL ,existobj_update_flg char(1) NULL ,first_usertask_flg char(1) NULL ,indexed_dttm timestamp NULL ,instance_version varchar(128) NULL ,is_sequential_flg char(1) NULL ,latest_flg char(1) NULL ,load_dttm timestamp NULL ,locally_updated_flg char(1) NULL ,modified_by_usernm varchar(128) NULL ,modified_dttm timestamp NULL ,modified_status_cd varchar(128) NULL ,multi_assig_suprt_flg char(1) NULL ,owner_usernm varchar(128) NULL ,percent_complete number NULL ,projected_end_dttm timestamp NULL ,projected_start_dttm timestamp NULL ,published_by_usernm varchar(128) NULL ,published_dttm timestamp NULL ,skip_peerupdate_scanning_flg char(1) NULL ,skip_update_scanning_flg char(1) NULL ,started_dttm timestamp NULL ,task_attachment varchar(128) NULL ,task_comment varchar(128) NULL ,task_desc varchar(1332) NULL ,task_instruction varchar(128) NULL ,task_nm varchar(128) NULL ,task_status varchar(128) NULL ,task_subtype varchar(128) NULL ,task_type varchar(128) NULL ,version_num number NULL
        )) by ORACLE;
      execute (alter table &dbschema..wf_process_tasks add constraint wf_process_tasks_pk primary key (engine_taskdef_id,process_id,task_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: wf_process_tasks, wf_process_tasks);
 PROC SQL ;
 connect to ORACLE (&sql_passthru_connection.);
    EXECUTE (CREATE TABLE &dbschema..wf_tasks_user_assignment(
        assignee_id varchar(128) NULL ,process_id varchar(128) NOT NULL ,task_id varchar(128) NOT NULL ,user_assignment_id varchar(128) NOT NULL ,user_id varchar(128) NOT NULL
        ,
        activation_completed_flg char(1) NULL ,approval_status varchar(128) NULL ,assignee_type varchar(128) NULL ,completed_dttm timestamp NULL ,created_by_usernm varchar(128) NULL ,created_dttm timestamp NULL ,delayed_by_day number NULL ,deleted_by_usernm varchar(128) NULL ,deleted_dttm timestamp NULL ,due_dttm timestamp NULL ,initiator_comment varchar(128) NULL ,instance_version varchar(128) NULL ,is_assigned_flg char(1) NULL ,is_latest_flg char(1) NULL ,is_replaced_flg char(1) NULL ,load_dttm timestamp NULL ,modified_by_usernm varchar(128) NULL ,modified_dttm timestamp NULL ,modified_status_cd varchar(128) NULL ,owner_usernm varchar(128) NULL ,projected_end_dttm timestamp NULL ,projected_start_dttm timestamp NULL ,replacement_assignee_id varchar(128) NULL ,replacement_reason varchar(128) NULL ,replacement_userid varchar(128) NULL ,start_dttm timestamp NULL ,usan_comment varchar(128) NULL ,usan_desc varchar(1332) NULL ,usan_duration_day number NULL ,usan_instruction varchar(128) NULL ,usan_status varchar(128) NULL ,user_nm varchar(128) NULL
        )) by ORACLE;
      execute (alter table &dbschema..wf_tasks_user_assignment add constraint wf_tasks_user_assignment_pk primary key (process_id,task_id,user_assignment_id,user_id)) by ORACLE;
 DISCONNECT FROM ORACLE;
 QUIT;
 %ErrCheck (Failed to create Table: wf_tasks_user_assignment, wf_tasks_user_assignment);
