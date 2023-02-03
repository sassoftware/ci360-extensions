/*
Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

create pipe <PREFIX>_ab_test_path_assignment AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_ab_test_path_assignment  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_activity_conversion AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_activity_conversion  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_activity_flow_in AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_activity_flow_in  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_activity_start AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_activity_start  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_asset_details AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_asset_details  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_asset_details_custom_prop AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_asset_details_custom_prop  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_asset_folder_details AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_asset_folder_details  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_asset_rendition_details AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_asset_rendition_details  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_asset_revision AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_asset_revision  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_business_process_details AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_business_process_details  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_cart_activity_details AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_cart_activity_details  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_cc_budget_breakup AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_cc_budget_breakup  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_cc_budget_breakup_ccbdgt AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_cc_budget_breakup_ccbdgt  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_commitment_details AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_commitment_details  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_commitment_line_items AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_commitment_line_items  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_commitment_line_items_ccbdgt AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_commitment_line_items_ccbdgt  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_contact_history AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_contact_history  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_conversion_milestone AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_conversion_milestone  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_custom_attributes AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_custom_attributes  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_custom_events AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_custom_events  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_custom_events_ext AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_custom_events_ext  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_daily_usage AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_daily_usage  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_data_view_details AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_data_view_details  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_direct_contact AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_direct_contact  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_document_details AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_document_details  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_email_bounce AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_email_bounce  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_email_click AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_email_click  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_email_complaint AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_email_complaint  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_email_open AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_email_open  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_email_optout AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_email_optout  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_email_optout_details AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_email_optout_details  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_email_reply AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_email_reply  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_email_send AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_email_send  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_email_view AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_email_view  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_external_event AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_external_event  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_fiscal_cc_budget AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_fiscal_cc_budget  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_form_details AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_form_details  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_goal_details AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_goal_details  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_goal_details_ext AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_goal_details_ext  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_identity_attributes AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_identity_attributes  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_identity_map AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_identity_map  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_impression_delivered AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_impression_delivered  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_impression_spot_viewable AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_impression_spot_viewable  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_in_app_failed AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_in_app_failed  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_in_app_message AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_in_app_message  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_in_app_send AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_in_app_send  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_in_app_targeting_request AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_in_app_targeting_request  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_invoice_details AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_invoice_details  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_invoice_line_items AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_invoice_line_items  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_invoice_line_items_ccbdgt AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_invoice_line_items_ccbdgt  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_activity AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_activity  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_activity_abtestpath AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_activity_abtestpath  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_activity_custom_prop AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_activity_custom_prop  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_activity_node AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_activity_node  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_activity_x_activity_node AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_activity_x_activity_node  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_activity_x_task AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_activity_x_task  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_asset AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_asset  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_bu AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_bu  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_business_context AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_business_context  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_cost_category AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_cost_category  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_costcenter AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_costcenter  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_creative AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_creative  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_creative_custom_prop AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_creative_custom_prop  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_creative_x_asset AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_creative_x_asset  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_cust_attrib AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_cust_attrib  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_custattrib_table_values AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_custattrib_table_values  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_dataview AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_dataview  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_dataview_x_event AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_dataview_x_event  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_event AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_event  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_fiscal_period AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_fiscal_period  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_grid_attr_defn AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_grid_attr_defn  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_message AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_message  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_message_custom_prop AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_message_custom_prop  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_message_x_creative AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_message_x_creative  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_object_type AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_object_type  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_occurrence AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_occurrence  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_picklist AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_picklist  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_rtc AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_rtc  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_segment AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_segment  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_segment_custom_prop AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_segment_custom_prop  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_segment_map AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_segment_map  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_segment_map_custom_prop AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_segment_map_custom_prop  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_segment_map_x_segment AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_segment_map_x_segment  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_segment_x_event AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_segment_x_event  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_spot AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_spot  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_target_assist AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_target_assist  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_task AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_task  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_task_custom_prop AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_task_custom_prop  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_task_x_creative AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_task_x_creative  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_task_x_dataview AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_task_x_dataview  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_task_x_event AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_task_x_event  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_task_x_message AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_task_x_message  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_task_x_segment AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_task_x_segment  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_task_x_spot AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_task_x_spot  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_vendor AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_vendor  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_wf_process_def AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_wf_process_def  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_wf_process_def_attr_grp AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_wf_process_def_attr_grp  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_wf_process_def_categories AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_wf_process_def_categories  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_wf_process_def_task_assg AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_wf_process_def_task_assg  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_md_wf_process_def_tasks AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_md_wf_process_def_tasks  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_media_activity_details AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_media_activity_details  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_media_details AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_media_details  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_media_details_ext AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_media_details_ext  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_mobile_focus_defocus AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_mobile_focus_defocus  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_mobile_spots AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_mobile_spots  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_monthly_usage AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_monthly_usage  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_notification_failed AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_notification_failed  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_notification_opened AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_notification_opened  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_notification_send AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_notification_send  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_notification_targeting_request AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_notification_targeting_request  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_order_details AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_order_details  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_order_summary AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_order_summary  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_outbound_system AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_outbound_system  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_page_details AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_page_details  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_page_details_ext AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_page_details_ext  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_page_errors AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_page_errors  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_planning_hierarchy_defn AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_planning_hierarchy_defn  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_planning_info AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_planning_info  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_planning_info_custom_prop AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_planning_info_custom_prop  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_product_views AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_product_views  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_promotion_displayed AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_promotion_displayed  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_promotion_used AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_promotion_used  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_response_history AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_response_history  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_search_results AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_search_results  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_search_results_ext AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_search_results_ext  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_session_details AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_session_details  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_session_details_ext AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_session_details_ext  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_spot_clicked AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_spot_clicked  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_spot_requested AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_spot_requested  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_tag_details AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_tag_details  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_visit_details AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_visit_details  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_wf_process_details AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_wf_process_details  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_wf_process_details_custom_prop AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_wf_process_details_custom_prop  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_wf_process_tasks AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_wf_process_tasks  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_wf_tasks_user_assignment AUTO_INGEST = FALSE AS COPY INTO  <PREFIX>_wf_tasks_user_assignment  FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);