/*******************************************************************************/
/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                         */
/* *****************************************************************************/
%macro execute_SQLSVR_etl;
%if %sysfunc(exist(&udmmart..ABT_ATTRIBUTION)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..ABT_ATTRIBUTION));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..abt_attribution_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..abt_attribution_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=abt_attribution , table_keys=%str(INTERACTION_DTTM,INTERACTION_ID), out_table=work.abt_attribution );
   data work.abt_attribution_tmp /view=work.abt_attribution_tmp ;
      set work.abt_attribution ;
   run;
   %err_check (Failed to add time zone adaptation :abt_attribution_tmp , abt_attribution );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..abt_attribution_tmp ;
            set work.abt_attribution_tmp ;
            stop;
         run;
         proc append data=work.abt_attribution_tmp  base=&tmplib..abt_attribution_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..abt_attribution_tmp ;
            set work.abt_attribution_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :abt_attribution_tmp , abt_attribution );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..abt_attribution as b using &tmpdbschema..abt_attribution_tmp as d on( 
            b.interaction_dttm = d.interaction_dttm and 
            b.interaction_id = d.interaction_id )
         when matched then  
         update set 
            b.interaction_cost = d.interaction_cost, 
            b.conversion_value = d.conversion_value, b.task_id = d.task_id, 
            b.load_id = d.load_id, b.interaction_type = d.interaction_type, 
            b.interaction_subtype = d.interaction_subtype, b.interaction = d.interaction, 
            b.identity_id = d.identity_id, b.creative_id = d.creative_id
         when not matched then insert ( 
            interaction_cost, conversion_value, interaction_dttm, 
            task_id, load_id, interaction_type, interaction_subtype, 
            interaction_id, interaction, identity_id, creative_id
         ) values ( 
            d.interaction_cost, d.conversion_value, d.interaction_dttm, 
            d.task_id, d.load_id, d.interaction_type, d.interaction_subtype, 
            d.interaction_id, d.interaction, d.identity_id, d.creative_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :abt_attribution_tmp , abt_attribution , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..abt_attribution_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..abt_attribution_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..ABT_ATTRIBUTION;
         drop table work.ABT_ATTRIBUTION;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table ABT_ATTRIBUTION;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..AB_TEST_PATH_ASSIGNMENT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..AB_TEST_PATH_ASSIGNMENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..ab_test_path_assignment_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..ab_test_path_assignment_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=ab_test_path_assignment , table_keys=%str(EVENT_ID), out_table=work.ab_test_path_assignment );
   data work.ab_test_path_assignment_tmp /view=work.ab_test_path_assignment_tmp ;
      set work.ab_test_path_assignment ;
      if abtestpath_assignment_dttm_tz  ne . then abtestpath_assignment_dttm_tz = tzoneu2s(abtestpath_assignment_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :ab_test_path_assignment_tmp , ab_test_path_assignment );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..ab_test_path_assignment_tmp ;
            set work.ab_test_path_assignment_tmp ;
            stop;
         run;
         proc append data=work.ab_test_path_assignment_tmp  base=&tmplib..ab_test_path_assignment_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..ab_test_path_assignment_tmp ;
            set work.ab_test_path_assignment_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :ab_test_path_assignment_tmp , ab_test_path_assignment );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..ab_test_path_assignment as b using &tmpdbschema..ab_test_path_assignment_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.load_dttm = d.load_dttm, 
            b.abtestpath_assignment_dttm_tz = d.abtestpath_assignment_dttm_tz, b.abtestpath_assignment_dttm = d.abtestpath_assignment_dttm, 
            b.session_id_hex = d.session_id_hex, b.context_type_nm = d.context_type_nm, 
            b.channel_user_id = d.channel_user_id, b.identity_id = d.identity_id, 
            b.event_nm = d.event_nm, b.channel_nm = d.channel_nm, 
            b.event_designed_id = d.event_designed_id, b.abtest_path_id = d.abtest_path_id, 
            b.activity_id = d.activity_id, b.context_val = d.context_val
         when not matched then insert ( 
            load_dttm, abtestpath_assignment_dttm_tz, abtestpath_assignment_dttm, 
            session_id_hex, context_type_nm, channel_user_id, identity_id, 
            event_nm, channel_nm, event_id, event_designed_id, 
            abtest_path_id, activity_id, context_val
         ) values ( 
            d.load_dttm, d.abtestpath_assignment_dttm_tz, d.abtestpath_assignment_dttm, 
            d.session_id_hex, d.context_type_nm, d.channel_user_id, d.identity_id, 
            d.event_nm, d.channel_nm, d.event_id, d.event_designed_id, 
            d.abtest_path_id, d.activity_id, d.context_val  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :ab_test_path_assignment_tmp , ab_test_path_assignment , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..ab_test_path_assignment_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..ab_test_path_assignment_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..AB_TEST_PATH_ASSIGNMENT;
         drop table work.AB_TEST_PATH_ASSIGNMENT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table AB_TEST_PATH_ASSIGNMENT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..ACTIVITY_CONVERSION)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..ACTIVITY_CONVERSION));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..activity_conversion_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..activity_conversion_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=activity_conversion , table_keys=%str(EVENT_ID), out_table=work.activity_conversion );
   data work.activity_conversion_tmp /view=work.activity_conversion_tmp ;
      set work.activity_conversion ;
      if activity_conversion_dttm_tz  ne . then activity_conversion_dttm_tz = tzoneu2s(activity_conversion_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :activity_conversion_tmp , activity_conversion );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..activity_conversion_tmp ;
            set work.activity_conversion_tmp ;
            stop;
         run;
         proc append data=work.activity_conversion_tmp  base=&tmplib..activity_conversion_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..activity_conversion_tmp ;
            set work.activity_conversion_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :activity_conversion_tmp , activity_conversion );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..activity_conversion as b using &tmpdbschema..activity_conversion_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.activity_conversion_dttm_tz = d.activity_conversion_dttm_tz, 
            b.load_dttm = d.load_dttm, b.activity_conversion_dttm = d.activity_conversion_dttm, 
            b.abtest_path_id = d.abtest_path_id, b.activity_id = d.activity_id, 
            b.activity_node_id = d.activity_node_id, b.session_id_hex = d.session_id_hex, 
            b.parent_event_designed_id = d.parent_event_designed_id, b.identity_id = d.identity_id, 
            b.goal_id = d.goal_id, b.event_nm = d.event_nm, 
            b.event_designed_id = d.event_designed_id, b.detail_id_hex = d.detail_id_hex, 
            b.context_val = d.context_val, b.channel_nm = d.channel_nm, 
            b.context_type_nm = d.context_type_nm, b.channel_user_id = d.channel_user_id
         when not matched then insert ( 
            activity_conversion_dttm_tz, load_dttm, activity_conversion_dttm, 
            abtest_path_id, activity_id, activity_node_id, session_id_hex, 
            parent_event_designed_id, identity_id, goal_id, event_nm, 
            event_id, event_designed_id, detail_id_hex, context_val, 
            channel_nm, context_type_nm, channel_user_id
         ) values ( 
            d.activity_conversion_dttm_tz, d.load_dttm, d.activity_conversion_dttm, 
            d.abtest_path_id, d.activity_id, d.activity_node_id, d.session_id_hex, 
            d.parent_event_designed_id, d.identity_id, d.goal_id, d.event_nm, 
            d.event_id, d.event_designed_id, d.detail_id_hex, d.context_val, 
            d.channel_nm, d.context_type_nm, d.channel_user_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :activity_conversion_tmp , activity_conversion , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..activity_conversion_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..activity_conversion_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..ACTIVITY_CONVERSION;
         drop table work.ACTIVITY_CONVERSION;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table ACTIVITY_CONVERSION;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..ACTIVITY_FLOW_IN)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..ACTIVITY_FLOW_IN));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..activity_flow_in_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..activity_flow_in_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=activity_flow_in , table_keys=%str(EVENT_ID), out_table=work.activity_flow_in );
   data work.activity_flow_in_tmp /view=work.activity_flow_in_tmp ;
      set work.activity_flow_in ;
      if activity_flow_in_dttm_tz  ne . then activity_flow_in_dttm_tz = tzoneu2s(activity_flow_in_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :activity_flow_in_tmp , activity_flow_in );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..activity_flow_in_tmp ;
            set work.activity_flow_in_tmp ;
            stop;
         run;
         proc append data=work.activity_flow_in_tmp  base=&tmplib..activity_flow_in_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..activity_flow_in_tmp ;
            set work.activity_flow_in_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :activity_flow_in_tmp , activity_flow_in );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..activity_flow_in as b using &tmpdbschema..activity_flow_in_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.activity_flow_in_dttm = d.activity_flow_in_dttm, 
            b.activity_flow_in_dttm_tz = d.activity_flow_in_dttm_tz, b.load_dttm = d.load_dttm, 
            b.task_id = d.task_id, b.identity_id = d.identity_id, 
            b.context_val = d.context_val, b.event_designed_id = d.event_designed_id, 
            b.channel_user_id = d.channel_user_id, b.activity_node_id = d.activity_node_id, 
            b.activity_id = d.activity_id, b.abtest_path_id = d.abtest_path_id, 
            b.channel_nm = d.channel_nm, b.context_type_nm = d.context_type_nm, 
            b.event_nm = d.event_nm
         when not matched then insert ( 
            activity_flow_in_dttm, activity_flow_in_dttm_tz, load_dttm, 
            task_id, identity_id, context_val, event_designed_id, 
            event_id, channel_user_id, activity_node_id, activity_id, 
            abtest_path_id, channel_nm, context_type_nm, event_nm
         ) values ( 
            d.activity_flow_in_dttm, d.activity_flow_in_dttm_tz, d.load_dttm, 
            d.task_id, d.identity_id, d.context_val, d.event_designed_id, 
            d.event_id, d.channel_user_id, d.activity_node_id, d.activity_id, 
            d.abtest_path_id, d.channel_nm, d.context_type_nm, d.event_nm  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :activity_flow_in_tmp , activity_flow_in , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..activity_flow_in_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..activity_flow_in_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..ACTIVITY_FLOW_IN;
         drop table work.ACTIVITY_FLOW_IN;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table ACTIVITY_FLOW_IN;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..ACTIVITY_START)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..ACTIVITY_START));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..activity_start_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..activity_start_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=activity_start , table_keys=%str(EVENT_ID), out_table=work.activity_start );
   data work.activity_start_tmp /view=work.activity_start_tmp ;
      set work.activity_start ;
      if activity_start_dttm_tz  ne . then activity_start_dttm_tz = tzoneu2s(activity_start_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :activity_start_tmp , activity_start );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..activity_start_tmp ;
            set work.activity_start_tmp ;
            stop;
         run;
         proc append data=work.activity_start_tmp  base=&tmplib..activity_start_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..activity_start_tmp ;
            set work.activity_start_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :activity_start_tmp , activity_start );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..activity_start as b using &tmpdbschema..activity_start_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.activity_start_dttm_tz = d.activity_start_dttm_tz, 
            b.load_dttm = d.load_dttm, b.activity_start_dttm = d.activity_start_dttm, 
            b.channel_nm = d.channel_nm, b.activity_id = d.activity_id, 
            b.identity_id = d.identity_id, b.event_nm = d.event_nm, 
            b.channel_user_id = d.channel_user_id, b.event_designed_id = d.event_designed_id, 
            b.context_val = d.context_val, b.context_type_nm = d.context_type_nm
         when not matched then insert ( 
            activity_start_dttm_tz, load_dttm, activity_start_dttm, 
            channel_nm, activity_id, identity_id, event_nm, 
            event_id, channel_user_id, event_designed_id, context_val, 
            context_type_nm
         ) values ( 
            d.activity_start_dttm_tz, d.load_dttm, d.activity_start_dttm, 
            d.channel_nm, d.activity_id, d.identity_id, d.event_nm, 
            d.event_id, d.channel_user_id, d.event_designed_id, d.context_val, 
            d.context_type_nm  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :activity_start_tmp , activity_start , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..activity_start_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..activity_start_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..ACTIVITY_START;
         drop table work.ACTIVITY_START;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table ACTIVITY_START;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..ADVERTISING_CONTACT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..ADVERTISING_CONTACT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..advertising_contact_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..advertising_contact_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=advertising_contact , table_keys=%str(EVENT_ID), out_table=work.advertising_contact );
   data work.advertising_contact_tmp /view=work.advertising_contact_tmp ;
      set work.advertising_contact ;
      if advertising_contact_dttm_tz  ne . then advertising_contact_dttm_tz = tzoneu2s(advertising_contact_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :advertising_contact_tmp , advertising_contact );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..advertising_contact_tmp ;
            set work.advertising_contact_tmp ;
            stop;
         run;
         proc append data=work.advertising_contact_tmp  base=&tmplib..advertising_contact_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..advertising_contact_tmp ;
            set work.advertising_contact_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :advertising_contact_tmp , advertising_contact );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..advertising_contact as b using &tmpdbschema..advertising_contact_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.load_dttm = d.load_dttm, 
            b.advertising_contact_dttm_tz = d.advertising_contact_dttm_tz, b.advertising_contact_dttm = d.advertising_contact_dttm, 
            b.task_version_id = d.task_version_id, b.task_id = d.task_id, 
            b.task_action_nm = d.task_action_nm, b.segment_version_id = d.segment_version_id, 
            b.segment_id = d.segment_id, b.response_tracking_cd = d.response_tracking_cd, 
            b.occurrence_id = d.occurrence_id, b.journey_occurrence_id = d.journey_occurrence_id, 
            b.journey_id = d.journey_id, b.identity_id = d.identity_id, 
            b.event_nm = d.event_nm, b.event_designed_id = d.event_designed_id, 
            b.context_val = d.context_val, b.context_type_nm = d.context_type_nm, 
            b.channel_nm = d.channel_nm, b.audience_id = d.audience_id, 
            b.aud_occurrence_id = d.aud_occurrence_id, b.advertising_platform_nm = d.advertising_platform_nm
         when not matched then insert ( 
            load_dttm, advertising_contact_dttm_tz, advertising_contact_dttm, 
            task_version_id, task_id, task_action_nm, segment_version_id, 
            segment_id, response_tracking_cd, occurrence_id, journey_occurrence_id, 
            journey_id, identity_id, event_nm, event_id, 
            event_designed_id, context_val, context_type_nm, channel_nm, 
            audience_id, aud_occurrence_id, advertising_platform_nm
         ) values ( 
            d.load_dttm, d.advertising_contact_dttm_tz, d.advertising_contact_dttm, 
            d.task_version_id, d.task_id, d.task_action_nm, d.segment_version_id, 
            d.segment_id, d.response_tracking_cd, d.occurrence_id, d.journey_occurrence_id, 
            d.journey_id, d.identity_id, d.event_nm, d.event_id, 
            d.event_designed_id, d.context_val, d.context_type_nm, d.channel_nm, 
            d.audience_id, d.aud_occurrence_id, d.advertising_platform_nm  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :advertising_contact_tmp , advertising_contact , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..advertising_contact_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..advertising_contact_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..ADVERTISING_CONTACT;
         drop table work.ADVERTISING_CONTACT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table ADVERTISING_CONTACT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..ASSET_DETAILS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..ASSET_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..ASSET_DETAILS) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..asset_details  base=&trglib..asset_details (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..ASSET_DETAILS (
            folder_sk, asset_sk, public_media_id, 
            user_rating_cnt, total_user_rating_val, entity_revision_enabled_flg, folder_deleted_flg, 
            entity_attribute_enabled_flg, expired_flg, asset_locked_flg, download_disabled_flg, 
            entity_subtype_enabled_flg, asset_deleted_flg, external_sharing_error_dt, average_user_rating_val, 
            folder_level, last_modified_dttm, created_dttm, download_disabled_dttm, 
            recycled_dttm, expired_dttm, load_dttm, asset_locked_dttm, 
            folder_desc, external_sharing_error_msg, entity_table_nm, download_disabled_by_usernm, 
            created_by_usernm, asset_source_type, entity_subtype_nm, entity_type_usage_cd, 
            folder_entity_status_cd, folder_id, asset_owner_usernm, asset_nm, 
            asset_locked_by_usernm, asset_id, asset_desc, asset_process_status, 
            asset_source_nm, entity_status_cd, recycled_by_usernm, entity_type_nm, 
            public_url, public_link, process_task_id, process_id, 
            last_modified_by_usernm, folder_path, folder_owner_usernm, folder_nm )
      select folder_sk, asset_sk, public_media_id, 
            user_rating_cnt, total_user_rating_val, entity_revision_enabled_flg, folder_deleted_flg, 
            entity_attribute_enabled_flg, expired_flg, asset_locked_flg, download_disabled_flg, 
            entity_subtype_enabled_flg, asset_deleted_flg, external_sharing_error_dt, average_user_rating_val, 
            folder_level, last_modified_dttm, created_dttm, download_disabled_dttm, 
            recycled_dttm, expired_dttm, load_dttm, asset_locked_dttm, 
            folder_desc, external_sharing_error_msg, entity_table_nm, download_disabled_by_usernm, 
            created_by_usernm, asset_source_type, entity_subtype_nm, entity_type_usage_cd, 
            folder_entity_status_cd, folder_id, asset_owner_usernm, asset_nm, 
            asset_locked_by_usernm, asset_id, asset_desc, asset_process_status, 
            asset_source_nm, entity_status_cd, recycled_by_usernm, entity_type_nm, 
            public_url, public_link, process_task_id, process_id, 
            last_modified_by_usernm, folder_path, folder_owner_usernm, folder_nm
         from &udmmart..asset_details ;
      quit;
   %end;
   %err_check (Failed to insert into asset_details , asset_details );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..ASSET_DETAILS;
         drop table work.ASSET_DETAILS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table ASSET_DETAILS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..ASSET_DETAILS_CUSTOM_PROP)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..ASSET_DETAILS_CUSTOM_PROP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..ASSET_DETAILS_CUSTOM_PROP) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..asset_details_custom_prop  base=&trglib..asset_details_custom_prop (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..ASSET_DETAILS_CUSTOM_PROP (
            attr_val, is_obsolete_flg, is_grid_flg, 
            load_dttm, last_modified_dttm, created_dttm, remote_pklist_tab_col, 
            last_modified_usernm, data_type, data_formatter, created_by_usernm, 
            attr_nm, attr_id, attr_group_nm, attr_group_id, 
            attr_group_cd, attr_cd, asset_id )
      select attr_val, is_obsolete_flg, is_grid_flg, 
            load_dttm, last_modified_dttm, created_dttm, remote_pklist_tab_col, 
            last_modified_usernm, data_type, data_formatter, created_by_usernm, 
            attr_nm, attr_id, attr_group_nm, attr_group_id, 
            attr_group_cd, attr_cd, asset_id
         from &udmmart..asset_details_custom_prop ;
      quit;
   %end;
   %err_check (Failed to insert into asset_details_custom_prop , asset_details_custom_prop );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..ASSET_DETAILS_CUSTOM_PROP;
         drop table work.ASSET_DETAILS_CUSTOM_PROP;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table ASSET_DETAILS_CUSTOM_PROP;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..ASSET_FOLDER_DETAILS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..ASSET_FOLDER_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..ASSET_FOLDER_DETAILS) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..asset_folder_details  base=&trglib..asset_folder_details (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..ASSET_FOLDER_DETAILS (
            deleted_flg, folder_level, load_dttm, 
            created_dttm, last_modified_dttm, last_modified_by_usernm, folder_owner_usernm, 
            folder_desc, folder_id, entity_status_cd, folder_nm, 
            folder_path, created_by_usernm )
      select deleted_flg, folder_level, load_dttm, 
            created_dttm, last_modified_dttm, last_modified_by_usernm, folder_owner_usernm, 
            folder_desc, folder_id, entity_status_cd, folder_nm, 
            folder_path, created_by_usernm
         from &udmmart..asset_folder_details ;
      quit;
   %end;
   %err_check (Failed to insert into asset_folder_details , asset_folder_details );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..ASSET_FOLDER_DETAILS;
         drop table work.ASSET_FOLDER_DETAILS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table ASSET_FOLDER_DETAILS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..ASSET_RENDITION_DETAILS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..ASSET_RENDITION_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..ASSET_RENDITION_DETAILS) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..asset_rendition_details  base=&trglib..asset_rendition_details (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..ASSET_RENDITION_DETAILS (
            download_cnt, revision_no, rend_deleted_flg, 
            rev_deleted_flg, current_revision_flg, media_dpi, file_size, 
            media_depth, media_height, rend_duration, media_width, 
            created_dttm, last_modified_dttm, load_dttm, revision_id, 
            revision_comment_txt, rendition_nm, rendition_generated_type_cd, last_modified_status_cd, 
            last_modified_by_usernm, file_nm, file_format, entity_status_cd, 
            created_by_usernm, asset_id, rendition_id, rendition_type_cd )
      select download_cnt, revision_no, rend_deleted_flg, 
            rev_deleted_flg, current_revision_flg, media_dpi, file_size, 
            media_depth, media_height, rend_duration, media_width, 
            created_dttm, last_modified_dttm, load_dttm, revision_id, 
            revision_comment_txt, rendition_nm, rendition_generated_type_cd, last_modified_status_cd, 
            last_modified_by_usernm, file_nm, file_format, entity_status_cd, 
            created_by_usernm, asset_id, rendition_id, rendition_type_cd
         from &udmmart..asset_rendition_details ;
      quit;
   %end;
   %err_check (Failed to insert into asset_rendition_details , asset_rendition_details );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..ASSET_RENDITION_DETAILS;
         drop table work.ASSET_RENDITION_DETAILS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table ASSET_RENDITION_DETAILS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..ASSET_REVISION)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..ASSET_REVISION));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..ASSET_REVISION) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..asset_revision  base=&trglib..asset_revision (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..ASSET_REVISION (
            revision_no, deleted_flg, current_revision_flg, 
            load_dttm, created_dttm, last_modified_dttm, revision_id, 
            last_modified_by_usernm, revision_comment_txt, entity_status_cd, created_by_usernm, 
            asset_id )
      select revision_no, deleted_flg, current_revision_flg, 
            load_dttm, created_dttm, last_modified_dttm, revision_id, 
            last_modified_by_usernm, revision_comment_txt, entity_status_cd, created_by_usernm, 
            asset_id
         from &udmmart..asset_revision ;
      quit;
   %end;
   %err_check (Failed to insert into asset_revision , asset_revision );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..ASSET_REVISION;
         drop table work.ASSET_REVISION;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table ASSET_REVISION;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..AUDIENCE_MEMBERSHIP_CHANGE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..AUDIENCE_MEMBERSHIP_CHANGE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..audience_membership_change_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..audience_membership_change_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=audience_membership_change , table_keys=%str(EVENT_ID), out_table=work.audience_membership_change );
   data work.audience_membership_change_tmp /view=work.audience_membership_change_tmp ;
      set work.audience_membership_change ;
      if audience_change_dttm_tz  ne . then audience_change_dttm_tz = tzoneu2s(audience_change_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :audience_membership_change_tmp , audience_membership_change );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..audience_membership_change_tmp ;
            set work.audience_membership_change_tmp ;
            stop;
         run;
         proc append data=work.audience_membership_change_tmp  base=&tmplib..audience_membership_change_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..audience_membership_change_tmp ;
            set work.audience_membership_change_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :audience_membership_change_tmp , audience_membership_change );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..audience_membership_change as b using &tmpdbschema..audience_membership_change_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.audience_change_dttm = d.audience_change_dttm, 
            b.load_dttm = d.load_dttm, b.audience_change_dttm_tz = d.audience_change_dttm_tz, 
            b.identity_id = d.identity_id, b.aud_occurrence_id = d.aud_occurrence_id, 
            b.audience_id = d.audience_id, b.event_nm = d.event_nm
         when not matched then insert ( 
            audience_change_dttm, load_dttm, audience_change_dttm_tz, 
            identity_id, aud_occurrence_id, event_id, audience_id, 
            event_nm
         ) values ( 
            d.audience_change_dttm, d.load_dttm, d.audience_change_dttm_tz, 
            d.identity_id, d.aud_occurrence_id, d.event_id, d.audience_id, 
            d.event_nm  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :audience_membership_change_tmp , audience_membership_change , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..audience_membership_change_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..audience_membership_change_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..AUDIENCE_MEMBERSHIP_CHANGE;
         drop table work.AUDIENCE_MEMBERSHIP_CHANGE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table AUDIENCE_MEMBERSHIP_CHANGE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..BUSINESS_PROCESS_DETAILS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..BUSINESS_PROCESS_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..business_process_details_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..business_process_details_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=business_process_details , table_keys=%str(EVENT_ID), out_table=work.business_process_details );
   data work.business_process_details_tmp /view=work.business_process_details_tmp ;
      set work.business_process_details ;
      if process_dttm_tz  ne . then process_dttm_tz = tzoneu2s(process_dttm_tz ,&timeZone_Value.);
      if process_exception_dttm_tz  ne . then process_exception_dttm_tz = tzoneu2s(process_exception_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :business_process_details_tmp , business_process_details );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..business_process_details_tmp ;
            set work.business_process_details_tmp ;
            stop;
         run;
         proc append data=work.business_process_details_tmp  base=&tmplib..business_process_details_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..business_process_details_tmp ;
            set work.business_process_details_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :business_process_details_tmp , business_process_details );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..business_process_details as b using &tmpdbschema..business_process_details_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.is_start_flg = d.is_start_flg, 
            b.is_completion_flg = d.is_completion_flg, b.process_attempt_cnt = d.process_attempt_cnt, 
            b.step_order_no = d.step_order_no, b.process_instance_no = d.process_instance_no, 
            b.process_dttm_tz = d.process_dttm_tz, b.process_exception_dttm_tz = d.process_exception_dttm_tz, 
            b.load_dttm = d.load_dttm, b.process_dttm = d.process_dttm, 
            b.process_exception_dttm = d.process_exception_dttm, b.visit_id = d.visit_id, 
            b.process_step_nm = d.process_step_nm, b.process_details_sk = d.process_details_sk, 
            b.identity_id = d.identity_id, b.event_nm = d.event_nm, 
            b.detail_id = d.detail_id, b.attribute1_txt = d.attribute1_txt, 
            b.detail_id_hex = d.detail_id_hex, b.event_designed_id = d.event_designed_id, 
            b.next_detail_id = d.next_detail_id, b.process_exception_txt = d.process_exception_txt, 
            b.session_id = d.session_id, b.session_id_hex = d.session_id_hex, 
            b.visit_id_hex = d.visit_id_hex, b.attribute2_txt = d.attribute2_txt, 
            b.event_source_cd = d.event_source_cd, b.process_nm = d.process_nm
         when not matched then insert ( 
            is_start_flg, is_completion_flg, process_attempt_cnt, 
            step_order_no, process_instance_no, process_dttm_tz, process_exception_dttm_tz, 
            load_dttm, process_dttm, process_exception_dttm, visit_id, 
            process_step_nm, process_details_sk, identity_id, event_nm, 
            detail_id, attribute1_txt, detail_id_hex, event_designed_id, 
            event_id, next_detail_id, process_exception_txt, session_id, 
            session_id_hex, visit_id_hex, attribute2_txt, event_source_cd, 
            process_nm
         ) values ( 
            d.is_start_flg, d.is_completion_flg, d.process_attempt_cnt, 
            d.step_order_no, d.process_instance_no, d.process_dttm_tz, d.process_exception_dttm_tz, 
            d.load_dttm, d.process_dttm, d.process_exception_dttm, d.visit_id, 
            d.process_step_nm, d.process_details_sk, d.identity_id, d.event_nm, 
            d.detail_id, d.attribute1_txt, d.detail_id_hex, d.event_designed_id, 
            d.event_id, d.next_detail_id, d.process_exception_txt, d.session_id, 
            d.session_id_hex, d.visit_id_hex, d.attribute2_txt, d.event_source_cd, 
            d.process_nm  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :business_process_details_tmp , business_process_details , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..business_process_details_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..business_process_details_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..BUSINESS_PROCESS_DETAILS;
         drop table work.BUSINESS_PROCESS_DETAILS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table BUSINESS_PROCESS_DETAILS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CART_ACTIVITY_DETAILS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CART_ACTIVITY_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..cart_activity_details_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..cart_activity_details_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=cart_activity_details , table_keys=%str(EVENT_ID), out_table=work.cart_activity_details );
   data work.cart_activity_details_tmp /view=work.cart_activity_details_tmp ;
      set work.cart_activity_details ;
      if activity_dttm_tz  ne . then activity_dttm_tz = tzoneu2s(activity_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :cart_activity_details_tmp , cart_activity_details );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..cart_activity_details_tmp ;
            set work.cart_activity_details_tmp ;
            stop;
         run;
         proc append data=work.cart_activity_details_tmp  base=&tmplib..cart_activity_details_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..cart_activity_details_tmp ;
            set work.cart_activity_details_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :cart_activity_details_tmp , cart_activity_details );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..cart_activity_details as b using &tmpdbschema..cart_activity_details_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.unit_price_amt = d.unit_price_amt, 
            b.displayed_cart_amt = d.displayed_cart_amt, b.quantity_val = d.quantity_val, 
            b.displayed_cart_items_no = d.displayed_cart_items_no, b.properties_map_doc = d.properties_map_doc, 
            b.activity_dttm = d.activity_dttm, b.load_dttm = d.load_dttm, 
            b.activity_dttm_tz = d.activity_dttm_tz, b.cart_activity_sk = d.cart_activity_sk, 
            b.activity_cd = d.activity_cd, b.visit_id_hex = d.visit_id_hex, 
            b.visit_id = d.visit_id, b.shipping_message_txt = d.shipping_message_txt, 
            b.session_id_hex = d.session_id_hex, b.session_id = d.session_id, 
            b.saving_message_txt = d.saving_message_txt, b.product_sku = d.product_sku, 
            b.product_nm = d.product_nm, b.product_id = d.product_id, 
            b.product_group_nm = d.product_group_nm, b.mobile_app_id = d.mobile_app_id, 
            b.identity_id = d.identity_id, b.event_source_cd = d.event_source_cd, 
            b.event_nm = d.event_nm, b.availability_message_txt = d.availability_message_txt, 
            b.cart_id = d.cart_id, b.event_designed_id = d.event_designed_id, 
            b.detail_id_hex = d.detail_id_hex, b.detail_id = d.detail_id, 
            b.currency_cd = d.currency_cd, b.event_key_cd = d.event_key_cd, 
            b.channel_nm = d.channel_nm, b.cart_nm = d.cart_nm
         when not matched then insert ( 
            unit_price_amt, displayed_cart_amt, quantity_val, 
            displayed_cart_items_no, properties_map_doc, activity_dttm, load_dttm, 
            activity_dttm_tz, cart_activity_sk, activity_cd, visit_id_hex, 
            visit_id, shipping_message_txt, session_id_hex, session_id, 
            saving_message_txt, product_sku, product_nm, product_id, 
            product_group_nm, mobile_app_id, identity_id, event_source_cd, 
            event_nm, availability_message_txt, cart_id, event_id, 
            event_designed_id, detail_id_hex, detail_id, currency_cd, 
            event_key_cd, channel_nm, cart_nm
         ) values ( 
            d.unit_price_amt, d.displayed_cart_amt, d.quantity_val, 
            d.displayed_cart_items_no, d.properties_map_doc, d.activity_dttm, d.load_dttm, 
            d.activity_dttm_tz, d.cart_activity_sk, d.activity_cd, d.visit_id_hex, 
            d.visit_id, d.shipping_message_txt, d.session_id_hex, d.session_id, 
            d.saving_message_txt, d.product_sku, d.product_nm, d.product_id, 
            d.product_group_nm, d.mobile_app_id, d.identity_id, d.event_source_cd, 
            d.event_nm, d.availability_message_txt, d.cart_id, d.event_id, 
            d.event_designed_id, d.detail_id_hex, d.detail_id, d.currency_cd, 
            d.event_key_cd, d.channel_nm, d.cart_nm  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :cart_activity_details_tmp , cart_activity_details , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..cart_activity_details_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..cart_activity_details_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CART_ACTIVITY_DETAILS;
         drop table work.CART_ACTIVITY_DETAILS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CART_ACTIVITY_DETAILS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CC_BUDGET_BREAKUP)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CC_BUDGET_BREAKUP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CC_BUDGET_BREAKUP) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cc_budget_breakup  base=&trglib..cc_budget_breakup (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CC_BUDGET_BREAKUP (
            cc_obsolete_flg, fin_accnt_obsolete_flg, cc_budget_distribution, 
            load_dttm, last_modified_dttm, created_dttm, planning_nm, 
            planning_id, last_modified_usernm, gen_ledger_cd, fin_accnt_nm, 
            fin_accnt_desc, created_by_usernm, cost_center_id, cc_owner_usernm, 
            cc_nm, cc_desc )
      select cc_obsolete_flg, fin_accnt_obsolete_flg, cc_budget_distribution, 
            load_dttm, last_modified_dttm, created_dttm, planning_nm, 
            planning_id, last_modified_usernm, gen_ledger_cd, fin_accnt_nm, 
            fin_accnt_desc, created_by_usernm, cost_center_id, cc_owner_usernm, 
            cc_nm, cc_desc
         from &udmmart..cc_budget_breakup ;
      quit;
   %end;
   %err_check (Failed to insert into cc_budget_breakup , cc_budget_breakup );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CC_BUDGET_BREAKUP;
         drop table work.CC_BUDGET_BREAKUP;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CC_BUDGET_BREAKUP;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CC_BUDGET_BREAKUP_CCBDGT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CC_BUDGET_BREAKUP_CCBDGT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CC_BUDGET_BREAKUP_CCBDGT) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cc_budget_breakup_ccbdgt  base=&trglib..cc_budget_breakup_ccbdgt (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CC_BUDGET_BREAKUP_CCBDGT (
            fin_accnt_obsolete_flg, cc_obsolete_flg, fp_obsolete_flg, 
            fp_end_dt, fp_start_dt, cc_bdgt_invoiced_amt, cc_lvl_distribution, 
            cc_bdgt_cmtmnt_invoice_amt, cc_rldup_total_expense, cc_rldup_child_bdgt, cc_level_expense, 
            cc_bdgt_cmtmnt_outstanding_amt, cc_bdgt_cmtmnt_overspent_amt, cc_bdgt_amt, cc_bdgt_budget_amt, 
            cc_budget_distribution, cc_bdgt_committed_amt, cc_bdgt_direct_invoice_amt, cc_bdgt_cmtmnt_invoice_cnt, 
            last_modified_dttm, load_dttm, created_dttm, planning_id, 
            last_modified_usernm, gen_ledger_cd, fp_nm, fp_id, 
            planning_nm, fp_desc, fp_cls_ver, fin_accnt_nm, 
            fin_accnt_desc, created_by_usernm, cost_center_id, cc_owner_usernm, 
            cc_number, cc_nm, cc_desc, cc_bdgt_budget_desc )
      select fin_accnt_obsolete_flg, cc_obsolete_flg, fp_obsolete_flg, 
            fp_end_dt, fp_start_dt, cc_bdgt_invoiced_amt, cc_lvl_distribution, 
            cc_bdgt_cmtmnt_invoice_amt, cc_rldup_total_expense, cc_rldup_child_bdgt, cc_level_expense, 
            cc_bdgt_cmtmnt_outstanding_amt, cc_bdgt_cmtmnt_overspent_amt, cc_bdgt_amt, cc_bdgt_budget_amt, 
            cc_budget_distribution, cc_bdgt_committed_amt, cc_bdgt_direct_invoice_amt, cc_bdgt_cmtmnt_invoice_cnt, 
            last_modified_dttm, load_dttm, created_dttm, planning_id, 
            last_modified_usernm, gen_ledger_cd, fp_nm, fp_id, 
            planning_nm, fp_desc, fp_cls_ver, fin_accnt_nm, 
            fin_accnt_desc, created_by_usernm, cost_center_id, cc_owner_usernm, 
            cc_number, cc_nm, cc_desc, cc_bdgt_budget_desc
         from &udmmart..cc_budget_breakup_ccbdgt ;
      quit;
   %end;
   %err_check (Failed to insert into cc_budget_breakup_ccbdgt , cc_budget_breakup_ccbdgt );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CC_BUDGET_BREAKUP_CCBDGT;
         drop table work.CC_BUDGET_BREAKUP_CCBDGT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CC_BUDGET_BREAKUP_CCBDGT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_ACTIVITY_CUSTOM_ATTR)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_ACTIVITY_CUSTOM_ATTR));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_ACTIVITY_CUSTOM_ATTR) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_activity_custom_attr  base=&trglib..cdm_activity_custom_attr (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_ACTIVITY_CUSTOM_ATTR (
            attribute_numeric_val, updated_dttm, attribute_dttm_val, 
            updated_by_nm, attribute_character_val, activity_version_id, activity_id, 
            attribute_data_type_cd, attribute_nm, attribute_val )
      select attribute_numeric_val, updated_dttm, attribute_dttm_val, 
            updated_by_nm, attribute_character_val, activity_version_id, activity_id, 
            attribute_data_type_cd, attribute_nm, attribute_val
         from &udmmart..cdm_activity_custom_attr ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_activity_custom_attr , cdm_activity_custom_attr );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_ACTIVITY_CUSTOM_ATTR;
         drop table work.CDM_ACTIVITY_CUSTOM_ATTR;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_ACTIVITY_CUSTOM_ATTR;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_ACTIVITY_DETAIL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_ACTIVITY_DETAIL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_ACTIVITY_DETAIL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_activity_detail  base=&trglib..cdm_activity_detail (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_ACTIVITY_DETAIL (
            updated_dttm, last_published_dttm, valid_from_dttm, 
            valid_to_dttm, status_cd, source_system_cd, activity_nm, 
            activity_id, activity_desc, activity_category_nm, activity_cd, 
            activity_version_id, updated_by_nm )
      select updated_dttm, last_published_dttm, valid_from_dttm, 
            valid_to_dttm, status_cd, source_system_cd, activity_nm, 
            activity_id, activity_desc, activity_category_nm, activity_cd, 
            activity_version_id, updated_by_nm
         from &udmmart..cdm_activity_detail ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_activity_detail , cdm_activity_detail );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_ACTIVITY_DETAIL;
         drop table work.CDM_ACTIVITY_DETAIL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_ACTIVITY_DETAIL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_ACTIVITY_X_TASK)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_ACTIVITY_X_TASK));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_ACTIVITY_X_TASK) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_activity_x_task  base=&trglib..cdm_activity_x_task (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_ACTIVITY_X_TASK (
            updated_dttm, task_version_id, activity_version_id, 
            activity_id, task_id, updated_by_nm )
      select updated_dttm, task_version_id, activity_version_id, 
            activity_id, task_id, updated_by_nm
         from &udmmart..cdm_activity_x_task ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_activity_x_task , cdm_activity_x_task );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_ACTIVITY_X_TASK;
         drop table work.CDM_ACTIVITY_X_TASK;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_ACTIVITY_X_TASK;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_AUDIENCE_DETAIL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_AUDIENCE_DETAIL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_AUDIENCE_DETAIL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_audience_detail  base=&trglib..cdm_audience_detail (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_AUDIENCE_DETAIL (
            audience_schedule_flg, create_dttm, delete_dttm, 
            updated_dttm, created_user_nm, audience_source_nm, audience_nm, 
            audience_id, audience_desc, audience_data_source_nm )
      select audience_schedule_flg, create_dttm, delete_dttm, 
            updated_dttm, created_user_nm, audience_source_nm, audience_nm, 
            audience_id, audience_desc, audience_data_source_nm
         from &udmmart..cdm_audience_detail ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_audience_detail , cdm_audience_detail );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_AUDIENCE_DETAIL;
         drop table work.CDM_AUDIENCE_DETAIL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_AUDIENCE_DETAIL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_AUDIENCE_OCCUR_DETAIL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_AUDIENCE_OCCUR_DETAIL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_AUDIENCE_OCCUR_DETAIL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_audience_occur_detail  base=&trglib..cdm_audience_occur_detail (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_AUDIENCE_OCCUR_DETAIL (
            audience_size_cnt, end_dttm, updated_dttm, 
            start_dttm, started_by_nm, occurrence_type_nm, audience_occur_id, 
            audience_id, execution_status_cd )
      select audience_size_cnt, end_dttm, updated_dttm, 
            start_dttm, started_by_nm, occurrence_type_nm, audience_occur_id, 
            audience_id, execution_status_cd
         from &udmmart..cdm_audience_occur_detail ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_audience_occur_detail , cdm_audience_occur_detail );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_AUDIENCE_OCCUR_DETAIL;
         drop table work.CDM_AUDIENCE_OCCUR_DETAIL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_AUDIENCE_OCCUR_DETAIL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_AUDIENCE_X_SEGMENT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_AUDIENCE_X_SEGMENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_AUDIENCE_X_SEGMENT) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_audience_x_segment  base=&trglib..cdm_audience_x_segment (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_AUDIENCE_X_SEGMENT (
            segment_id, audience_id )
      select segment_id, audience_id
         from &udmmart..cdm_audience_x_segment ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_audience_x_segment , cdm_audience_x_segment );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_AUDIENCE_X_SEGMENT;
         drop table work.CDM_AUDIENCE_X_SEGMENT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_AUDIENCE_X_SEGMENT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_BUSINESS_CONTEXT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_BUSINESS_CONTEXT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_BUSINESS_CONTEXT) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_business_context  base=&trglib..cdm_business_context (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_BUSINESS_CONTEXT (
            updated_dttm, updated_by_nm, business_context_type_cd, 
            business_context_nm, business_context_id, source_system_cd )
      select updated_dttm, updated_by_nm, business_context_type_cd, 
            business_context_nm, business_context_id, source_system_cd
         from &udmmart..cdm_business_context ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_business_context , cdm_business_context );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_BUSINESS_CONTEXT;
         drop table work.CDM_BUSINESS_CONTEXT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_BUSINESS_CONTEXT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_CAMPAIGN_CUSTOM_ATTR)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_CAMPAIGN_CUSTOM_ATTR));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_CAMPAIGN_CUSTOM_ATTR) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_campaign_custom_attr  base=&trglib..cdm_campaign_custom_attr (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_CAMPAIGN_CUSTOM_ATTR (
            attribute_numeric_val, updated_dttm, attribute_dttm_val, 
            page_nm, campaign_id, attribute_character_val, attribute_data_type_cd, 
            attribute_nm, attribute_val, extension_attribute_nm, updated_by_nm )
      select attribute_numeric_val, updated_dttm, attribute_dttm_val, 
            page_nm, campaign_id, attribute_character_val, attribute_data_type_cd, 
            attribute_nm, attribute_val, extension_attribute_nm, updated_by_nm
         from &udmmart..cdm_campaign_custom_attr ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_campaign_custom_attr , cdm_campaign_custom_attr );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_CAMPAIGN_CUSTOM_ATTR;
         drop table work.CDM_CAMPAIGN_CUSTOM_ATTR;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_CAMPAIGN_CUSTOM_ATTR;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_CAMPAIGN_DETAIL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_CAMPAIGN_DETAIL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_CAMPAIGN_DETAIL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_campaign_detail  base=&trglib..cdm_campaign_detail (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_CAMPAIGN_DETAIL (
            deleted_flg, current_version_flg, max_budget_amt, 
            min_budget_offer_amt, min_budget_amt, max_budget_offer_amt, campaign_version_no, 
            deployment_version_no, campaign_group_sk, approval_dttm, valid_from_dttm, 
            run_dttm, updated_dttm, valid_to_dttm, start_dttm, 
            last_modified_dttm, end_dttm, updated_by_nm, source_system_cd, 
            campaign_status_cd, campaign_type_cd, last_modified_by_user_nm, campaign_nm, 
            campaign_folder_txt, campaign_desc, campaign_cd, approval_given_by_nm, 
            campaign_id, campaign_owner_nm )
      select deleted_flg, current_version_flg, max_budget_amt, 
            min_budget_offer_amt, min_budget_amt, max_budget_offer_amt, campaign_version_no, 
            deployment_version_no, campaign_group_sk, approval_dttm, valid_from_dttm, 
            run_dttm, updated_dttm, valid_to_dttm, start_dttm, 
            last_modified_dttm, end_dttm, updated_by_nm, source_system_cd, 
            campaign_status_cd, campaign_type_cd, last_modified_by_user_nm, campaign_nm, 
            campaign_folder_txt, campaign_desc, campaign_cd, approval_given_by_nm, 
            campaign_id, campaign_owner_nm
         from &udmmart..cdm_campaign_detail ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_campaign_detail , cdm_campaign_detail );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_CAMPAIGN_DETAIL;
         drop table work.CDM_CAMPAIGN_DETAIL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_CAMPAIGN_DETAIL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_CONTACT_CHANNEL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_CONTACT_CHANNEL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_CONTACT_CHANNEL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_contact_channel  base=&trglib..cdm_contact_channel (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_CONTACT_CHANNEL (
            updated_dttm, contact_channel_cd, updated_by_nm, 
            contact_channel_nm )
      select updated_dttm, contact_channel_cd, updated_by_nm, 
            contact_channel_nm
         from &udmmart..cdm_contact_channel ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_contact_channel , cdm_contact_channel );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_CONTACT_CHANNEL;
         drop table work.CDM_CONTACT_CHANNEL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_CONTACT_CHANNEL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_CONTACT_HISTORY)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_CONTACT_HISTORY));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..cdm_contact_history_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..cdm_contact_history_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=cdm_contact_history , table_keys=%str(CONTACT_ID), out_table=work.cdm_contact_history );
   data work.cdm_contact_history_tmp /view=work.cdm_contact_history_tmp ;
      set work.cdm_contact_history ;
      if contact_dttm_tz  ne . then contact_dttm_tz = tzoneu2s(contact_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :cdm_contact_history_tmp , cdm_contact_history );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..cdm_contact_history_tmp ;
            set work.cdm_contact_history_tmp ;
            stop;
         run;
         proc append data=work.cdm_contact_history_tmp  base=&tmplib..cdm_contact_history_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..cdm_contact_history_tmp ;
            set work.cdm_contact_history_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :cdm_contact_history_tmp , cdm_contact_history );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..cdm_contact_history as b using &tmpdbschema..cdm_contact_history_tmp as d on( 
            b.contact_id = d.contact_id )
         when matched then  
         update set 
            b.optimization_backfill_flg = d.optimization_backfill_flg, 
            b.control_group_flg = d.control_group_flg, b.contact_dt = d.contact_dt, 
            b.updated_dttm = d.updated_dttm, b.contact_dttm_tz = d.contact_dttm_tz, 
            b.contact_dttm = d.contact_dttm, b.source_system_cd = d.source_system_cd, 
            b.external_contact_info_2_id = d.external_contact_info_2_id, b.context_type_nm = d.context_type_nm, 
            b.audience_id = d.audience_id, b.contact_nm = d.contact_nm, 
            b.identity_id = d.identity_id, b.audience_occur_id = d.audience_occur_id, 
            b.contact_status_cd = d.contact_status_cd, b.context_val = d.context_val, 
            b.external_contact_info_1_id = d.external_contact_info_1_id, b.rtc_id = d.rtc_id, 
            b.updated_by_nm = d.updated_by_nm
         when not matched then insert ( 
            optimization_backfill_flg, control_group_flg, contact_dt, 
            updated_dttm, contact_dttm_tz, contact_dttm, source_system_cd, 
            external_contact_info_2_id, context_type_nm, audience_id, contact_nm, 
            identity_id, audience_occur_id, contact_id, contact_status_cd, 
            context_val, external_contact_info_1_id, rtc_id, updated_by_nm
         ) values ( 
            d.optimization_backfill_flg, d.control_group_flg, d.contact_dt, 
            d.updated_dttm, d.contact_dttm_tz, d.contact_dttm, d.source_system_cd, 
            d.external_contact_info_2_id, d.context_type_nm, d.audience_id, d.contact_nm, 
            d.identity_id, d.audience_occur_id, d.contact_id, d.contact_status_cd, 
            d.context_val, d.external_contact_info_1_id, d.rtc_id, d.updated_by_nm  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :cdm_contact_history_tmp , cdm_contact_history , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..cdm_contact_history_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..cdm_contact_history_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_CONTACT_HISTORY;
         drop table work.CDM_CONTACT_HISTORY;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_CONTACT_HISTORY;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_CONTACT_STATUS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_CONTACT_STATUS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_CONTACT_STATUS) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_contact_status  base=&trglib..cdm_contact_status (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_CONTACT_STATUS (
            updated_dttm, contact_status_desc, contact_status_cd, 
            updated_by_nm )
      select updated_dttm, contact_status_desc, contact_status_cd, 
            updated_by_nm
         from &udmmart..cdm_contact_status ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_contact_status , cdm_contact_status );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_CONTACT_STATUS;
         drop table work.CDM_CONTACT_STATUS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_CONTACT_STATUS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_CONTENT_CUSTOM_ATTR)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_CONTENT_CUSTOM_ATTR));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_CONTENT_CUSTOM_ATTR) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_content_custom_attr  base=&trglib..cdm_content_custom_attr (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_CONTENT_CUSTOM_ATTR (
            attribute_numeric_val, updated_dttm, attribute_dttm_val, 
            updated_by_nm, attribute_val, attribute_data_type_cd, attribute_nm, 
            content_version_id, attribute_character_val, content_id, extension_attribute_nm )
      select attribute_numeric_val, updated_dttm, attribute_dttm_val, 
            updated_by_nm, attribute_val, attribute_data_type_cd, attribute_nm, 
            content_version_id, attribute_character_val, content_id, extension_attribute_nm
         from &udmmart..cdm_content_custom_attr ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_content_custom_attr , cdm_content_custom_attr );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_CONTENT_CUSTOM_ATTR;
         drop table work.CDM_CONTENT_CUSTOM_ATTR;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_CONTENT_CUSTOM_ATTR;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_CONTENT_DETAIL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_CONTENT_DETAIL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_CONTENT_DETAIL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_content_detail  base=&trglib..cdm_content_detail (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_CONTENT_DETAIL (
            active_flg, created_dt, updated_dttm, 
            valid_from_dttm, valid_to_dttm, updated_by_nm, owner_nm, 
            external_reference_url_txt, content_id, contact_content_status_cd, contact_content_cd, 
            contact_content_class_nm, contact_content_desc, contact_content_nm, contact_content_type_nm, 
            content_version_id, created_user_nm, external_reference_txt, source_system_cd, 
            contact_content_category_nm )
      select active_flg, created_dt, updated_dttm, 
            valid_from_dttm, valid_to_dttm, updated_by_nm, owner_nm, 
            external_reference_url_txt, content_id, contact_content_status_cd, contact_content_cd, 
            contact_content_class_nm, contact_content_desc, contact_content_nm, contact_content_type_nm, 
            content_version_id, created_user_nm, external_reference_txt, source_system_cd, 
            contact_content_category_nm
         from &udmmart..cdm_content_detail ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_content_detail , cdm_content_detail );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_CONTENT_DETAIL;
         drop table work.CDM_CONTENT_DETAIL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_CONTENT_DETAIL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_DYN_CONTENT_CUSTOM_ATTR)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_DYN_CONTENT_CUSTOM_ATTR));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_DYN_CONTENT_CUSTOM_ATTR) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_dyn_content_custom_attr  base=&trglib..cdm_dyn_content_custom_attr (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_DYN_CONTENT_CUSTOM_ATTR (
            attribute_numeric_val, attribute_dttm_val, updated_dttm, 
            updated_by_nm, content_hash_val, attribute_character_val, attribute_data_type_cd, 
            attribute_val, content_version_id, attribute_nm, content_id, 
            extension_attribute_nm )
      select attribute_numeric_val, attribute_dttm_val, updated_dttm, 
            updated_by_nm, content_hash_val, attribute_character_val, attribute_data_type_cd, 
            attribute_val, content_version_id, attribute_nm, content_id, 
            extension_attribute_nm
         from &udmmart..cdm_dyn_content_custom_attr ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_dyn_content_custom_attr , cdm_dyn_content_custom_attr );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_DYN_CONTENT_CUSTOM_ATTR;
         drop table work.CDM_DYN_CONTENT_CUSTOM_ATTR;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_DYN_CONTENT_CUSTOM_ATTR;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_IDENTIFIER_TYPE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_IDENTIFIER_TYPE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_IDENTIFIER_TYPE) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_identifier_type  base=&trglib..cdm_identifier_type (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_IDENTIFIER_TYPE (
            updated_dttm, updated_by_nm, identifier_type_desc, 
            identifier_type_id )
      select updated_dttm, updated_by_nm, identifier_type_desc, 
            identifier_type_id
         from &udmmart..cdm_identifier_type ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_identifier_type , cdm_identifier_type );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_IDENTIFIER_TYPE;
         drop table work.CDM_IDENTIFIER_TYPE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_IDENTIFIER_TYPE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_IDENTITY_ATTR)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_IDENTITY_ATTR));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_IDENTITY_ATTR) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_identity_attr  base=&trglib..cdm_identity_attr (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_IDENTITY_ATTR (
            entry_dttm, valid_to_dttm, valid_from_dttm, 
            updated_dttm, identifier_type_id, user_identifier_val, updated_by_nm, 
            source_system_cd, identity_id )
      select entry_dttm, valid_to_dttm, valid_from_dttm, 
            updated_dttm, identifier_type_id, user_identifier_val, updated_by_nm, 
            source_system_cd, identity_id
         from &udmmart..cdm_identity_attr ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_identity_attr , cdm_identity_attr );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_IDENTITY_ATTR;
         drop table work.CDM_IDENTITY_ATTR;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_IDENTITY_ATTR;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_IDENTITY_MAP)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_IDENTITY_MAP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_IDENTITY_MAP) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_identity_map  base=&trglib..cdm_identity_map (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_IDENTITY_MAP (
            updated_dttm, identity_type_cd, identity_id, 
            updated_by_nm )
      select updated_dttm, identity_type_cd, identity_id, 
            updated_by_nm
         from &udmmart..cdm_identity_map ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_identity_map , cdm_identity_map );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_IDENTITY_MAP;
         drop table work.CDM_IDENTITY_MAP;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_IDENTITY_MAP;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_IDENTITY_TYPE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_IDENTITY_TYPE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_IDENTITY_TYPE) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_identity_type  base=&trglib..cdm_identity_type (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_IDENTITY_TYPE (
            updated_dttm, updated_by_nm, identity_type_desc, 
            identity_type_cd )
      select updated_dttm, updated_by_nm, identity_type_desc, 
            identity_type_cd
         from &udmmart..cdm_identity_type ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_identity_type , cdm_identity_type );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_IDENTITY_TYPE;
         drop table work.CDM_IDENTITY_TYPE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_IDENTITY_TYPE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_OCCURRENCE_DETAIL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_OCCURRENCE_DETAIL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_OCCURRENCE_DETAIL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_occurrence_detail  base=&trglib..cdm_occurrence_detail (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_OCCURRENCE_DETAIL (
            occurrence_no, end_dttm, updated_dttm, 
            start_dttm, updated_by_nm, source_system_cd, occurrence_object_type_cd, 
            occurrence_object_id, occurrence_id, execution_status_cd, occurrence_type_cd )
      select occurrence_no, end_dttm, updated_dttm, 
            start_dttm, updated_by_nm, source_system_cd, occurrence_object_type_cd, 
            occurrence_object_id, occurrence_id, execution_status_cd, occurrence_type_cd
         from &udmmart..cdm_occurrence_detail ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_occurrence_detail , cdm_occurrence_detail );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_OCCURRENCE_DETAIL;
         drop table work.CDM_OCCURRENCE_DETAIL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_OCCURRENCE_DETAIL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_RESPONSE_CHANNEL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_RESPONSE_CHANNEL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_RESPONSE_CHANNEL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_response_channel  base=&trglib..cdm_response_channel (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_RESPONSE_CHANNEL (
            updated_dttm, updated_by_nm, response_channel_nm, 
            response_channel_cd )
      select updated_dttm, updated_by_nm, response_channel_nm, 
            response_channel_cd
         from &udmmart..cdm_response_channel ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_response_channel , cdm_response_channel );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_RESPONSE_CHANNEL;
         drop table work.CDM_RESPONSE_CHANNEL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_RESPONSE_CHANNEL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_RESPONSE_EXTENDED_ATTR)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_RESPONSE_EXTENDED_ATTR));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..cdm_response_extended_attr_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..cdm_response_extended_attr_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=cdm_response_extended_attr , table_keys=%str(ATTRIBUTE_NM,RESPONSE_ATTRIBUTE_TYPE_CD,RESPONSE_ID), out_table=work.cdm_response_extended_attr );
   data work.cdm_response_extended_attr_tmp /view=work.cdm_response_extended_attr_tmp ;
      set work.cdm_response_extended_attr ;
   run;
   %err_check (Failed to add time zone adaptation :cdm_response_extended_attr_tmp , cdm_response_extended_attr );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..cdm_response_extended_attr_tmp ;
            set work.cdm_response_extended_attr_tmp ;
            stop;
         run;
         proc append data=work.cdm_response_extended_attr_tmp  base=&tmplib..cdm_response_extended_attr_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..cdm_response_extended_attr_tmp ;
            set work.cdm_response_extended_attr_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :cdm_response_extended_attr_tmp , cdm_response_extended_attr );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..cdm_response_extended_attr as b using &tmpdbschema..cdm_response_extended_attr_tmp as d on( 
            b.response_id = d.response_id and 
            b.response_attribute_type_cd = d.response_attribute_type_cd and b.attribute_nm = d.attribute_nm )
         when matched then  
         update set 
            b.updated_dttm = d.updated_dttm, 
            b.updated_by_nm = d.updated_by_nm, b.attribute_val = d.attribute_val, 
            b.attribute_data_type_cd = d.attribute_data_type_cd
         when not matched then insert ( 
            updated_dttm, updated_by_nm, response_id, 
            response_attribute_type_cd, attribute_val, attribute_nm, attribute_data_type_cd
         ) values ( 
            d.updated_dttm, d.updated_by_nm, d.response_id, 
            d.response_attribute_type_cd, d.attribute_val, d.attribute_nm, d.attribute_data_type_cd  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :cdm_response_extended_attr_tmp , cdm_response_extended_attr , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..cdm_response_extended_attr_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..cdm_response_extended_attr_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_RESPONSE_EXTENDED_ATTR;
         drop table work.CDM_RESPONSE_EXTENDED_ATTR;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_RESPONSE_EXTENDED_ATTR;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_RESPONSE_HISTORY)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_RESPONSE_HISTORY));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..cdm_response_history_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..cdm_response_history_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=cdm_response_history , table_keys=%str(RESPONSE_ID), out_table=work.cdm_response_history );
   data work.cdm_response_history_tmp /view=work.cdm_response_history_tmp ;
      set work.cdm_response_history ;
      if response_dttm_tz  ne . then response_dttm_tz = tzoneu2s(response_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :cdm_response_history_tmp , cdm_response_history );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..cdm_response_history_tmp ;
            set work.cdm_response_history_tmp ;
            stop;
         run;
         proc append data=work.cdm_response_history_tmp  base=&tmplib..cdm_response_history_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..cdm_response_history_tmp ;
            set work.cdm_response_history_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :cdm_response_history_tmp , cdm_response_history );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..cdm_response_history as b using &tmpdbschema..cdm_response_history_tmp as d on( 
            b.response_id = d.response_id )
         when matched then  
         update set 
            b.conversion_flg = d.conversion_flg, 
            b.inferred_response_flg = d.inferred_response_flg, b.response_dt = d.response_dt, 
            b.response_val_amt = d.response_val_amt, b.properties_map_doc = d.properties_map_doc, 
            b.updated_dttm = d.updated_dttm, b.response_dttm = d.response_dttm, 
            b.response_dttm_tz = d.response_dttm_tz, b.updated_by_nm = d.updated_by_nm, 
            b.source_system_cd = d.source_system_cd, b.rtc_id = d.rtc_id, 
            b.response_type_cd = d.response_type_cd, b.response_channel_cd = d.response_channel_cd, 
            b.response_cd = d.response_cd, b.identity_id = d.identity_id, 
            b.external_contact_info_2_id = d.external_contact_info_2_id, b.external_contact_info_1_id = d.external_contact_info_1_id, 
            b.context_val = d.context_val, b.context_type_nm = d.context_type_nm, 
            b.content_version_id = d.content_version_id, b.content_id = d.content_id, 
            b.content_hash_val = d.content_hash_val, b.contact_id = d.contact_id, 
            b.audience_occur_id = d.audience_occur_id, b.audience_id = d.audience_id
         when not matched then insert ( 
            conversion_flg, inferred_response_flg, response_dt, 
            response_val_amt, properties_map_doc, updated_dttm, response_dttm, 
            response_dttm_tz, updated_by_nm, source_system_cd, rtc_id, 
            response_type_cd, response_id, response_channel_cd, response_cd, 
            identity_id, external_contact_info_2_id, external_contact_info_1_id, context_val, 
            context_type_nm, content_version_id, content_id, content_hash_val, 
            contact_id, audience_occur_id, audience_id
         ) values ( 
            d.conversion_flg, d.inferred_response_flg, d.response_dt, 
            d.response_val_amt, d.properties_map_doc, d.updated_dttm, d.response_dttm, 
            d.response_dttm_tz, d.updated_by_nm, d.source_system_cd, d.rtc_id, 
            d.response_type_cd, d.response_id, d.response_channel_cd, d.response_cd, 
            d.identity_id, d.external_contact_info_2_id, d.external_contact_info_1_id, d.context_val, 
            d.context_type_nm, d.content_version_id, d.content_id, d.content_hash_val, 
            d.contact_id, d.audience_occur_id, d.audience_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :cdm_response_history_tmp , cdm_response_history , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..cdm_response_history_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..cdm_response_history_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_RESPONSE_HISTORY;
         drop table work.CDM_RESPONSE_HISTORY;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_RESPONSE_HISTORY;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_RESPONSE_LOOKUP)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_RESPONSE_LOOKUP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_RESPONSE_LOOKUP) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_response_lookup  base=&trglib..cdm_response_lookup (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_RESPONSE_LOOKUP (
            updated_dttm, updated_by_nm, response_nm, 
            response_cd )
      select updated_dttm, updated_by_nm, response_nm, 
            response_cd
         from &udmmart..cdm_response_lookup ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_response_lookup , cdm_response_lookup );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_RESPONSE_LOOKUP;
         drop table work.CDM_RESPONSE_LOOKUP;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_RESPONSE_LOOKUP;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_RESPONSE_TYPE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_RESPONSE_TYPE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_RESPONSE_TYPE) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_response_type  base=&trglib..cdm_response_type (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_RESPONSE_TYPE (
            updated_dttm, updated_by_nm, response_type_desc, 
            response_type_cd )
      select updated_dttm, updated_by_nm, response_type_desc, 
            response_type_cd
         from &udmmart..cdm_response_type ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_response_type , cdm_response_type );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_RESPONSE_TYPE;
         drop table work.CDM_RESPONSE_TYPE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_RESPONSE_TYPE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_RTC_DETAIL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_RTC_DETAIL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_RTC_DETAIL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_rtc_detail  base=&trglib..cdm_rtc_detail (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_RTC_DETAIL (
            deleted_flg, response_tracking_flg, task_occurrence_no, 
            processed_dttm, updated_dttm, updated_by_nm, task_version_id, 
            task_id, source_system_cd, segment_version_id, segment_id, 
            rtc_id, occurrence_id, execution_status_cd )
      select deleted_flg, response_tracking_flg, task_occurrence_no, 
            processed_dttm, updated_dttm, updated_by_nm, task_version_id, 
            task_id, source_system_cd, segment_version_id, segment_id, 
            rtc_id, occurrence_id, execution_status_cd
         from &udmmart..cdm_rtc_detail ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_rtc_detail , cdm_rtc_detail );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_RTC_DETAIL;
         drop table work.CDM_RTC_DETAIL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_RTC_DETAIL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_RTC_X_CONTENT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_RTC_X_CONTENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_RTC_X_CONTENT) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_rtc_x_content  base=&trglib..cdm_rtc_x_content (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_RTC_X_CONTENT (
            sequence_no, updated_dttm, updated_by_nm, 
            rtc_x_content_sk, rtc_id, content_version_id, content_id, 
            content_hash_val )
      select sequence_no, updated_dttm, updated_by_nm, 
            rtc_x_content_sk, rtc_id, content_version_id, content_id, 
            content_hash_val
         from &udmmart..cdm_rtc_x_content ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_rtc_x_content , cdm_rtc_x_content );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_RTC_X_CONTENT;
         drop table work.CDM_RTC_X_CONTENT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_RTC_X_CONTENT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_SEGMENT_CUSTOM_ATTR)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_SEGMENT_CUSTOM_ATTR));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_SEGMENT_CUSTOM_ATTR) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_segment_custom_attr  base=&trglib..cdm_segment_custom_attr (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_SEGMENT_CUSTOM_ATTR (
            attribute_numeric_val, updated_dttm, attribute_dttm_val, 
            updated_by_nm, segment_version_id, segment_id, attribute_val, 
            attribute_nm, attribute_data_type_cd, attribute_character_val )
      select attribute_numeric_val, updated_dttm, attribute_dttm_val, 
            updated_by_nm, segment_version_id, segment_id, attribute_val, 
            attribute_nm, attribute_data_type_cd, attribute_character_val
         from &udmmart..cdm_segment_custom_attr ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_segment_custom_attr , cdm_segment_custom_attr );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_SEGMENT_CUSTOM_ATTR;
         drop table work.CDM_SEGMENT_CUSTOM_ATTR;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_SEGMENT_CUSTOM_ATTR;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_SEGMENT_DETAIL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_SEGMENT_DETAIL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_SEGMENT_DETAIL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_segment_detail  base=&trglib..cdm_segment_detail (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_SEGMENT_DETAIL (
            valid_from_dttm, updated_dttm, valid_to_dttm, 
            updated_by_nm, source_system_cd, segment_version_id, segment_status_cd, 
            segment_src_nm, segment_nm, segment_map_version_id, segment_map_id, 
            segment_id, segment_desc, segment_cd, segment_category_nm )
      select valid_from_dttm, updated_dttm, valid_to_dttm, 
            updated_by_nm, source_system_cd, segment_version_id, segment_status_cd, 
            segment_src_nm, segment_nm, segment_map_version_id, segment_map_id, 
            segment_id, segment_desc, segment_cd, segment_category_nm
         from &udmmart..cdm_segment_detail ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_segment_detail , cdm_segment_detail );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_SEGMENT_DETAIL;
         drop table work.CDM_SEGMENT_DETAIL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_SEGMENT_DETAIL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_SEGMENT_MAP)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_SEGMENT_MAP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_SEGMENT_MAP) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_segment_map  base=&trglib..cdm_segment_map (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_SEGMENT_MAP (
            valid_to_dttm, valid_from_dttm, updated_dttm, 
            updated_by_nm, source_system_cd, segment_map_version_id, segment_map_status_cd, 
            segment_map_src_nm, segment_map_nm, segment_map_id, segment_map_desc, 
            segment_map_cd, segment_map_category_nm )
      select valid_to_dttm, valid_from_dttm, updated_dttm, 
            updated_by_nm, source_system_cd, segment_map_version_id, segment_map_status_cd, 
            segment_map_src_nm, segment_map_nm, segment_map_id, segment_map_desc, 
            segment_map_cd, segment_map_category_nm
         from &udmmart..cdm_segment_map ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_segment_map , cdm_segment_map );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_SEGMENT_MAP;
         drop table work.CDM_SEGMENT_MAP;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_SEGMENT_MAP;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_SEGMENT_MAP_CUSTOM_ATTR)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_SEGMENT_MAP_CUSTOM_ATTR));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_SEGMENT_MAP_CUSTOM_ATTR) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_segment_map_custom_attr  base=&trglib..cdm_segment_map_custom_attr (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_SEGMENT_MAP_CUSTOM_ATTR (
            attribute_numeric_val, updated_dttm, attribute_dttm_val, 
            updated_by_nm, segment_map_version_id, segment_map_id, attribute_val, 
            attribute_nm, attribute_data_type_cd, attribute_character_val )
      select attribute_numeric_val, updated_dttm, attribute_dttm_val, 
            updated_by_nm, segment_map_version_id, segment_map_id, attribute_val, 
            attribute_nm, attribute_data_type_cd, attribute_character_val
         from &udmmart..cdm_segment_map_custom_attr ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_segment_map_custom_attr , cdm_segment_map_custom_attr );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_SEGMENT_MAP_CUSTOM_ATTR;
         drop table work.CDM_SEGMENT_MAP_CUSTOM_ATTR;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_SEGMENT_MAP_CUSTOM_ATTR;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_SEGMENT_TEST)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_SEGMENT_TEST));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_SEGMENT_TEST) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_segment_test  base=&trglib..cdm_segment_test (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_SEGMENT_TEST (
            stratified_sampling_flg, test_enabled_flg, test_pct, 
            test_cnt, updated_dttm, test_sizing_type_nm, test_type_nm, 
            test_nm, test_cd, task_version_id, task_id, 
            stratified_samp_criteria_txt )
      select stratified_sampling_flg, test_enabled_flg, test_pct, 
            test_cnt, updated_dttm, test_sizing_type_nm, test_type_nm, 
            test_nm, test_cd, task_version_id, task_id, 
            stratified_samp_criteria_txt
         from &udmmart..cdm_segment_test ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_segment_test , cdm_segment_test );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_SEGMENT_TEST;
         drop table work.CDM_SEGMENT_TEST;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_SEGMENT_TEST;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_SEGMENT_TEST_X_SEGMENT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_SEGMENT_TEST_X_SEGMENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_SEGMENT_TEST_X_SEGMENT) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_segment_test_x_segment  base=&trglib..cdm_segment_test_x_segment (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_SEGMENT_TEST_X_SEGMENT (
            updated_dttm, test_cd, task_id, 
            segment_id, task_version_id )
      select updated_dttm, test_cd, task_id, 
            segment_id, task_version_id
         from &udmmart..cdm_segment_test_x_segment ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_segment_test_x_segment , cdm_segment_test_x_segment );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_SEGMENT_TEST_X_SEGMENT;
         drop table work.CDM_SEGMENT_TEST_X_SEGMENT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_SEGMENT_TEST_X_SEGMENT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_TASK_CUSTOM_ATTR)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_TASK_CUSTOM_ATTR));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_TASK_CUSTOM_ATTR) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_task_custom_attr  base=&trglib..cdm_task_custom_attr (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_TASK_CUSTOM_ATTR (
            attribute_numeric_val, attribute_dttm_val, updated_dttm, 
            task_version_id, task_id, extension_attribute_nm, attribute_character_val, 
            attribute_data_type_cd, attribute_nm, attribute_val, updated_by_nm )
      select attribute_numeric_val, attribute_dttm_val, updated_dttm, 
            task_version_id, task_id, extension_attribute_nm, attribute_character_val, 
            attribute_data_type_cd, attribute_nm, attribute_val, updated_by_nm
         from &udmmart..cdm_task_custom_attr ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_task_custom_attr , cdm_task_custom_attr );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_TASK_CUSTOM_ATTR;
         drop table work.CDM_TASK_CUSTOM_ATTR;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_TASK_CUSTOM_ATTR;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CDM_TASK_DETAIL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CDM_TASK_DETAIL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..CDM_TASK_DETAIL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..cdm_task_detail  base=&trglib..cdm_task_detail (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..CDM_TASK_DETAIL (
            segment_tests_flg, saved_flg, scheduled_flg, 
            recurring_schedule_flg, published_flg, staged_flg, limit_by_total_impression_flg, 
            update_contact_history_flg, standard_reply_flg, active_flg, created_dt, 
            budget_unit_usage_amt, budget_unit_cost_amt, max_budget_amt, min_budget_amt, 
            min_budget_offer_amt, max_budget_offer_amt, maximum_period_expression_cnt, limit_period_unit_cnt, 
            scheduled_end_dttm, updated_dttm, valid_from_dttm, export_dttm, 
            valid_to_dttm, scheduled_start_dttm, task_version_id, task_type_nm, 
            task_subtype_nm, task_status_cd, task_id, task_delivery_type_nm, 
            subject_type_nm, source_system_cd, modified_status_cd, contact_channel_cd, 
            business_context_id, campaign_id, control_group_action_nm, created_user_nm, 
            owner_nm, recurr_type_cd, stratified_sampling_action_nm, task_cd, 
            task_desc, task_nm, updated_by_nm )
      select segment_tests_flg, saved_flg, scheduled_flg, 
            recurring_schedule_flg, published_flg, staged_flg, limit_by_total_impression_flg, 
            update_contact_history_flg, standard_reply_flg, active_flg, created_dt, 
            budget_unit_usage_amt, budget_unit_cost_amt, max_budget_amt, min_budget_amt, 
            min_budget_offer_amt, max_budget_offer_amt, maximum_period_expression_cnt, limit_period_unit_cnt, 
            scheduled_end_dttm, updated_dttm, valid_from_dttm, export_dttm, 
            valid_to_dttm, scheduled_start_dttm, task_version_id, task_type_nm, 
            task_subtype_nm, task_status_cd, task_id, task_delivery_type_nm, 
            subject_type_nm, source_system_cd, modified_status_cd, contact_channel_cd, 
            business_context_id, campaign_id, control_group_action_nm, created_user_nm, 
            owner_nm, recurr_type_cd, stratified_sampling_action_nm, task_cd, 
            task_desc, task_nm, updated_by_nm
         from &udmmart..cdm_task_detail ;
      quit;
   %end;
   %err_check (Failed to insert into cdm_task_detail , cdm_task_detail );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CDM_TASK_DETAIL;
         drop table work.CDM_TASK_DETAIL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CDM_TASK_DETAIL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..COMMITMENT_DETAILS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..COMMITMENT_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..COMMITMENT_DETAILS) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..commitment_details  base=&trglib..commitment_details (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..COMMITMENT_DETAILS (
            vendor_obsolete_flg, cmtmnt_overspent_amt, vendor_amt, 
            cmtmnt_outstanding_amt, cmtmnt_amt, last_modified_dttm, cmtmnt_payment_dttm, 
            cmtmnt_created_dttm, load_dttm, created_dttm, vendor_number, 
            vendor_id, planning_nm, planning_id, last_modified_usernm, 
            created_by_usernm, cmtmnt_status, cmtmnt_nm, cmtmnt_desc, 
            cmtmnt_closure_note, cmtmnt_id, cmtmnt_no, planning_currency_cd, 
            vendor_currency_cd, vendor_nm )
      select vendor_obsolete_flg, cmtmnt_overspent_amt, vendor_amt, 
            cmtmnt_outstanding_amt, cmtmnt_amt, last_modified_dttm, cmtmnt_payment_dttm, 
            cmtmnt_created_dttm, load_dttm, created_dttm, vendor_number, 
            vendor_id, planning_nm, planning_id, last_modified_usernm, 
            created_by_usernm, cmtmnt_status, cmtmnt_nm, cmtmnt_desc, 
            cmtmnt_closure_note, cmtmnt_id, cmtmnt_no, planning_currency_cd, 
            vendor_currency_cd, vendor_nm
         from &udmmart..commitment_details ;
      quit;
   %end;
   %err_check (Failed to insert into commitment_details , commitment_details );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..COMMITMENT_DETAILS;
         drop table work.COMMITMENT_DETAILS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table COMMITMENT_DETAILS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..COMMITMENT_LINE_ITEMS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..COMMITMENT_LINE_ITEMS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..COMMITMENT_LINE_ITEMS) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..commitment_line_items  base=&trglib..commitment_line_items (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..COMMITMENT_LINE_ITEMS (
            item_qty, item_alloc_unit, item_vend_alloc_unit, 
            vendor_obsolete_flg, cc_recon_alloc_amt, cmtmnt_overspent_amt, item_rate, 
            item_alloc_amt, cmtmnt_amt, vendor_amt, cmtmnt_outstanding_amt, 
            cc_allocated_amt, cc_available_amt, item_vend_alloc_amt, item_number, 
            created_dttm, last_modified_dttm, load_dttm, cmtmnt_payment_dttm, 
            cmtmnt_created_dttm, vendor_nm, vendor_currency_cd, planning_nm, 
            planning_currency_cd, last_modified_usernm, gen_ledger_cd, cost_center_id, 
            cmtmnt_status, cmtmnt_no, cmtmnt_nm, cmtmnt_desc, 
            cmtmnt_closure_note, ccat_nm, cc_owner_usernm, cc_nm, 
            cc_desc, cmtmnt_id, created_by_usernm, fin_acc_nm, 
            item_nm, planning_id, vendor_id, vendor_number )
      select item_qty, item_alloc_unit, item_vend_alloc_unit, 
            vendor_obsolete_flg, cc_recon_alloc_amt, cmtmnt_overspent_amt, item_rate, 
            item_alloc_amt, cmtmnt_amt, vendor_amt, cmtmnt_outstanding_amt, 
            cc_allocated_amt, cc_available_amt, item_vend_alloc_amt, item_number, 
            created_dttm, last_modified_dttm, load_dttm, cmtmnt_payment_dttm, 
            cmtmnt_created_dttm, vendor_nm, vendor_currency_cd, planning_nm, 
            planning_currency_cd, last_modified_usernm, gen_ledger_cd, cost_center_id, 
            cmtmnt_status, cmtmnt_no, cmtmnt_nm, cmtmnt_desc, 
            cmtmnt_closure_note, ccat_nm, cc_owner_usernm, cc_nm, 
            cc_desc, cmtmnt_id, created_by_usernm, fin_acc_nm, 
            item_nm, planning_id, vendor_id, vendor_number
         from &udmmart..commitment_line_items ;
      quit;
   %end;
   %err_check (Failed to insert into commitment_line_items , commitment_line_items );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..COMMITMENT_LINE_ITEMS;
         drop table work.COMMITMENT_LINE_ITEMS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table COMMITMENT_LINE_ITEMS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..COMMITMENT_LINE_ITEMS_CCBDGT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..COMMITMENT_LINE_ITEMS_CCBDGT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..COMMITMENT_LINE_ITEMS_CCBDGT) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..commitment_line_items_ccbdgt  base=&trglib..commitment_line_items_ccbdgt (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..COMMITMENT_LINE_ITEMS_CCBDGT (
            vendor_obsolete_flg, fp_obsolete_flg, cc_obsolete_flg, 
            fp_end_dt, fp_start_dt, item_vend_alloc_amt, cc_available_amt, 
            item_alloc_amt, cc_allocated_amt, cc_bdgt_cmtmnt_invoice_amt, vendor_amt, 
            cmtmnt_outstanding_amt, cmtmnt_amt, cc_recon_alloc_amt, cc_bdgt_budget_amt, 
            cc_bdgt_committed_amt, item_rate, cc_bdgt_cmtmnt_overspent_amt, cc_bdgt_invoiced_amt, 
            cc_bdgt_amt, cc_bdgt_cmtmnt_outstanding_amt, cmtmnt_overspent_amt, cc_bdgt_direct_invoice_amt, 
            item_number, item_alloc_unit, item_qty, cc_bdgt_cmtmnt_invoice_cnt, 
            item_vend_alloc_unit, created_dttm, cmtmnt_payment_dttm, last_modified_dttm, 
            load_dttm, cmtmnt_created_dttm, vendor_currency_cd, planning_currency_cd, 
            last_modified_usernm, gen_ledger_cd, fp_id, item_nm, 
            planning_nm, vendor_nm, fp_desc, fp_cls_ver, 
            fin_acc_nm, created_by_usernm, cmtmnt_status, cmtmnt_no, 
            cmtmnt_id, cmtmnt_desc, cc_nm, cc_desc, 
            cc_bdgt_budget_desc, cc_number, cc_owner_usernm, ccat_nm, 
            cmtmnt_closure_note, cmtmnt_nm, cost_center_id, fp_nm, 
            planning_id, vendor_id, vendor_number )
      select vendor_obsolete_flg, fp_obsolete_flg, cc_obsolete_flg, 
            fp_end_dt, fp_start_dt, item_vend_alloc_amt, cc_available_amt, 
            item_alloc_amt, cc_allocated_amt, cc_bdgt_cmtmnt_invoice_amt, vendor_amt, 
            cmtmnt_outstanding_amt, cmtmnt_amt, cc_recon_alloc_amt, cc_bdgt_budget_amt, 
            cc_bdgt_committed_amt, item_rate, cc_bdgt_cmtmnt_overspent_amt, cc_bdgt_invoiced_amt, 
            cc_bdgt_amt, cc_bdgt_cmtmnt_outstanding_amt, cmtmnt_overspent_amt, cc_bdgt_direct_invoice_amt, 
            item_number, item_alloc_unit, item_qty, cc_bdgt_cmtmnt_invoice_cnt, 
            item_vend_alloc_unit, created_dttm, cmtmnt_payment_dttm, last_modified_dttm, 
            load_dttm, cmtmnt_created_dttm, vendor_currency_cd, planning_currency_cd, 
            last_modified_usernm, gen_ledger_cd, fp_id, item_nm, 
            planning_nm, vendor_nm, fp_desc, fp_cls_ver, 
            fin_acc_nm, created_by_usernm, cmtmnt_status, cmtmnt_no, 
            cmtmnt_id, cmtmnt_desc, cc_nm, cc_desc, 
            cc_bdgt_budget_desc, cc_number, cc_owner_usernm, ccat_nm, 
            cmtmnt_closure_note, cmtmnt_nm, cost_center_id, fp_nm, 
            planning_id, vendor_id, vendor_number
         from &udmmart..commitment_line_items_ccbdgt ;
      quit;
   %end;
   %err_check (Failed to insert into commitment_line_items_ccbdgt , commitment_line_items_ccbdgt );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..COMMITMENT_LINE_ITEMS_CCBDGT;
         drop table work.COMMITMENT_LINE_ITEMS_CCBDGT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table COMMITMENT_LINE_ITEMS_CCBDGT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CONTACT_HISTORY)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CONTACT_HISTORY));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..contact_history_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..contact_history_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=contact_history , table_keys=%str(CONTACT_ID), out_table=work.contact_history );
   data work.contact_history_tmp /view=work.contact_history_tmp ;
      set work.contact_history ;
      if contact_dttm_tz  ne . then contact_dttm_tz = tzoneu2s(contact_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :contact_history_tmp , contact_history );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..contact_history_tmp ;
            set work.contact_history_tmp ;
            stop;
         run;
         proc append data=work.contact_history_tmp  base=&tmplib..contact_history_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..contact_history_tmp ;
            set work.contact_history_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :contact_history_tmp , contact_history );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..contact_history as b using &tmpdbschema..contact_history_tmp as d on( 
            b.contact_id = d.contact_id )
         when matched then  
         update set 
            b.control_group_flg = d.control_group_flg, 
            b.properties_map_doc = d.properties_map_doc, b.contact_dttm_tz = d.contact_dttm_tz, 
            b.load_dttm = d.load_dttm, b.contact_dttm = d.contact_dttm, 
            b.task_id = d.task_id, b.parent_event_designed_id = d.parent_event_designed_id, 
            b.journey_occurrence_id = d.journey_occurrence_id, b.detail_id_hex = d.detail_id_hex, 
            b.context_type_nm = d.context_type_nm, b.audience_id = d.audience_id, 
            b.identity_id = d.identity_id, b.message_id = d.message_id, 
            b.response_tracking_cd = d.response_tracking_cd, b.visit_id_hex = d.visit_id_hex, 
            b.aud_occurrence_id = d.aud_occurrence_id, b.contact_channel_nm = d.contact_channel_nm, 
            b.contact_nm = d.contact_nm, b.context_val = d.context_val, 
            b.creative_id = d.creative_id, b.event_designed_id = d.event_designed_id, 
            b.journey_id = d.journey_id, b.occurrence_id = d.occurrence_id, 
            b.session_id_hex = d.session_id_hex, b.task_version_id = d.task_version_id
         when not matched then insert ( 
            control_group_flg, properties_map_doc, contact_dttm_tz, 
            load_dttm, contact_dttm, task_id, parent_event_designed_id, 
            journey_occurrence_id, detail_id_hex, context_type_nm, audience_id, 
            contact_id, identity_id, message_id, response_tracking_cd, 
            visit_id_hex, aud_occurrence_id, contact_channel_nm, contact_nm, 
            context_val, creative_id, event_designed_id, journey_id, 
            occurrence_id, session_id_hex, task_version_id
         ) values ( 
            d.control_group_flg, d.properties_map_doc, d.contact_dttm_tz, 
            d.load_dttm, d.contact_dttm, d.task_id, d.parent_event_designed_id, 
            d.journey_occurrence_id, d.detail_id_hex, d.context_type_nm, d.audience_id, 
            d.contact_id, d.identity_id, d.message_id, d.response_tracking_cd, 
            d.visit_id_hex, d.aud_occurrence_id, d.contact_channel_nm, d.contact_nm, 
            d.context_val, d.creative_id, d.event_designed_id, d.journey_id, 
            d.occurrence_id, d.session_id_hex, d.task_version_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :contact_history_tmp , contact_history , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..contact_history_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..contact_history_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CONTACT_HISTORY;
         drop table work.CONTACT_HISTORY;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CONTACT_HISTORY;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CONVERSION_MILESTONE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CONVERSION_MILESTONE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..conversion_milestone_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..conversion_milestone_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=conversion_milestone , table_keys=%str(EVENT_ID), out_table=work.conversion_milestone );
   data work.conversion_milestone_tmp /view=work.conversion_milestone_tmp ;
      set work.conversion_milestone ;
      if conversion_milestone_dttm_tz  ne . then conversion_milestone_dttm_tz = tzoneu2s(conversion_milestone_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :conversion_milestone_tmp , conversion_milestone );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..conversion_milestone_tmp ;
            set work.conversion_milestone_tmp ;
            stop;
         run;
         proc append data=work.conversion_milestone_tmp  base=&tmplib..conversion_milestone_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..conversion_milestone_tmp ;
            set work.conversion_milestone_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :conversion_milestone_tmp , conversion_milestone );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..conversion_milestone as b using &tmpdbschema..conversion_milestone_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.test_flg = d.test_flg, 
            b.control_group_flg = d.control_group_flg, b.total_cost_amt = d.total_cost_amt, 
            b.properties_map_doc = d.properties_map_doc, b.load_dttm = d.load_dttm, 
            b.conversion_milestone_dttm = d.conversion_milestone_dttm, b.conversion_milestone_dttm_tz = d.conversion_milestone_dttm_tz, 
            b.visit_id_hex = d.visit_id_hex, b.task_id = d.task_id, 
            b.spot_id = d.spot_id, b.segment_version_id = d.segment_version_id, 
            b.reserved_1_txt = d.reserved_1_txt, b.occurrence_id = d.occurrence_id, 
            b.message_version_id = d.message_version_id, b.goal_id = d.goal_id, 
            b.detail_id_hex = d.detail_id_hex, b.channel_user_id = d.channel_user_id, 
            b.analysis_group_id = d.analysis_group_id, b.audience_id = d.audience_id, 
            b.context_val = d.context_val, b.creative_id = d.creative_id, 
            b.journey_id = d.journey_id, b.response_tracking_cd = d.response_tracking_cd, 
            b.activity_id = d.activity_id, b.aud_occurrence_id = d.aud_occurrence_id, 
            b.channel_nm = d.channel_nm, b.context_type_nm = d.context_type_nm, 
            b.creative_version_id = d.creative_version_id, b.event_designed_id = d.event_designed_id, 
            b.event_nm = d.event_nm, b.identity_id = d.identity_id, 
            b.journey_occurrence_id = d.journey_occurrence_id, b.message_id = d.message_id, 
            b.mobile_app_id = d.mobile_app_id, b.parent_event_designed_id = d.parent_event_designed_id, 
            b.rec_group_id = d.rec_group_id, b.reserved_2_txt = d.reserved_2_txt, 
            b.segment_id = d.segment_id, b.session_id_hex = d.session_id_hex, 
            b.subject_line_txt = d.subject_line_txt, b.task_version_id = d.task_version_id
         when not matched then insert ( 
            test_flg, control_group_flg, total_cost_amt, 
            properties_map_doc, load_dttm, conversion_milestone_dttm, conversion_milestone_dttm_tz, 
            visit_id_hex, task_id, spot_id, segment_version_id, 
            reserved_1_txt, occurrence_id, message_version_id, goal_id, 
            detail_id_hex, channel_user_id, analysis_group_id, audience_id, 
            context_val, creative_id, event_id, journey_id, 
            response_tracking_cd, activity_id, aud_occurrence_id, channel_nm, 
            context_type_nm, creative_version_id, event_designed_id, event_nm, 
            identity_id, journey_occurrence_id, message_id, mobile_app_id, 
            parent_event_designed_id, rec_group_id, reserved_2_txt, segment_id, 
            session_id_hex, subject_line_txt, task_version_id
         ) values ( 
            d.test_flg, d.control_group_flg, d.total_cost_amt, 
            d.properties_map_doc, d.load_dttm, d.conversion_milestone_dttm, d.conversion_milestone_dttm_tz, 
            d.visit_id_hex, d.task_id, d.spot_id, d.segment_version_id, 
            d.reserved_1_txt, d.occurrence_id, d.message_version_id, d.goal_id, 
            d.detail_id_hex, d.channel_user_id, d.analysis_group_id, d.audience_id, 
            d.context_val, d.creative_id, d.event_id, d.journey_id, 
            d.response_tracking_cd, d.activity_id, d.aud_occurrence_id, d.channel_nm, 
            d.context_type_nm, d.creative_version_id, d.event_designed_id, d.event_nm, 
            d.identity_id, d.journey_occurrence_id, d.message_id, d.mobile_app_id, 
            d.parent_event_designed_id, d.rec_group_id, d.reserved_2_txt, d.segment_id, 
            d.session_id_hex, d.subject_line_txt, d.task_version_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :conversion_milestone_tmp , conversion_milestone , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..conversion_milestone_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..conversion_milestone_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CONVERSION_MILESTONE;
         drop table work.CONVERSION_MILESTONE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CONVERSION_MILESTONE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CUSTOM_EVENTS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CUSTOM_EVENTS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..custom_events_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..custom_events_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=custom_events , table_keys=%str(EVENT_ID), out_table=work.custom_events );
   data work.custom_events_tmp /view=work.custom_events_tmp ;
      set work.custom_events ;
      if custom_event_dttm_tz  ne . then custom_event_dttm_tz = tzoneu2s(custom_event_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :custom_events_tmp , custom_events );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..custom_events_tmp ;
            set work.custom_events_tmp ;
            stop;
         run;
         proc append data=work.custom_events_tmp  base=&tmplib..custom_events_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..custom_events_tmp ;
            set work.custom_events_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :custom_events_tmp , custom_events );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..custom_events as b using &tmpdbschema..custom_events_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.custom_revenue_amt = d.custom_revenue_amt, 
            b.properties_map_doc = d.properties_map_doc, b.custom_event_dttm = d.custom_event_dttm, 
            b.custom_event_dttm_tz = d.custom_event_dttm_tz, b.load_dttm = d.load_dttm, 
            b.session_id = d.session_id, b.page_id = d.page_id, 
            b.event_type_nm = d.event_type_nm, b.channel_user_id = d.channel_user_id, 
            b.custom_event_nm = d.custom_event_nm, b.detail_id_hex = d.detail_id_hex, 
            b.event_nm = d.event_nm, b.reserved_1_txt = d.reserved_1_txt, 
            b.reserved_2_txt = d.reserved_2_txt, b.visit_id = d.visit_id, 
            b.channel_nm = d.channel_nm, b.custom_event_group_nm = d.custom_event_group_nm, 
            b.custom_events_sk = d.custom_events_sk, b.detail_id = d.detail_id, 
            b.event_designed_id = d.event_designed_id, b.event_key_cd = d.event_key_cd, 
            b.event_source_cd = d.event_source_cd, b.identity_id = d.identity_id, 
            b.mobile_app_id = d.mobile_app_id, b.session_id_hex = d.session_id_hex, 
            b.visit_id_hex = d.visit_id_hex
         when not matched then insert ( 
            custom_revenue_amt, properties_map_doc, custom_event_dttm, 
            custom_event_dttm_tz, load_dttm, session_id, page_id, 
            event_type_nm, event_id, channel_user_id, custom_event_nm, 
            detail_id_hex, event_nm, reserved_1_txt, reserved_2_txt, 
            visit_id, channel_nm, custom_event_group_nm, custom_events_sk, 
            detail_id, event_designed_id, event_key_cd, event_source_cd, 
            identity_id, mobile_app_id, session_id_hex, visit_id_hex
         ) values ( 
            d.custom_revenue_amt, d.properties_map_doc, d.custom_event_dttm, 
            d.custom_event_dttm_tz, d.load_dttm, d.session_id, d.page_id, 
            d.event_type_nm, d.event_id, d.channel_user_id, d.custom_event_nm, 
            d.detail_id_hex, d.event_nm, d.reserved_1_txt, d.reserved_2_txt, 
            d.visit_id, d.channel_nm, d.custom_event_group_nm, d.custom_events_sk, 
            d.detail_id, d.event_designed_id, d.event_key_cd, d.event_source_cd, 
            d.identity_id, d.mobile_app_id, d.session_id_hex, d.visit_id_hex  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :custom_events_tmp , custom_events , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..custom_events_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..custom_events_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CUSTOM_EVENTS;
         drop table work.CUSTOM_EVENTS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CUSTOM_EVENTS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..CUSTOM_EVENTS_EXT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..CUSTOM_EVENTS_EXT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..custom_events_ext_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..custom_events_ext_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=custom_events_ext , table_keys=%str(CUSTOM_EVENTS_SK), out_table=work.custom_events_ext );
   data work.custom_events_ext_tmp /view=work.custom_events_ext_tmp ;
      set work.custom_events_ext ;
   run;
   %err_check (Failed to add time zone adaptation :custom_events_ext_tmp , custom_events_ext );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..custom_events_ext_tmp ;
            set work.custom_events_ext_tmp ;
            stop;
         run;
         proc append data=work.custom_events_ext_tmp  base=&tmplib..custom_events_ext_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..custom_events_ext_tmp ;
            set work.custom_events_ext_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :custom_events_ext_tmp , custom_events_ext );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..custom_events_ext as b using &tmpdbschema..custom_events_ext_tmp as d on( 
            b.custom_events_sk = d.custom_events_sk )
         when matched then  
         update set 
            b.custom_revenue_amt = d.custom_revenue_amt, 
            b.load_dttm = d.load_dttm, b.event_designed_id = d.event_designed_id
         when not matched then insert ( 
            custom_revenue_amt, load_dttm, event_designed_id, 
            custom_events_sk
         ) values ( 
            d.custom_revenue_amt, d.load_dttm, d.event_designed_id, 
            d.custom_events_sk  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :custom_events_ext_tmp , custom_events_ext , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..custom_events_ext_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..custom_events_ext_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..CUSTOM_EVENTS_EXT;
         drop table work.CUSTOM_EVENTS_EXT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table CUSTOM_EVENTS_EXT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..DAILY_USAGE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..DAILY_USAGE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..daily_usage_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..daily_usage_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=daily_usage , table_keys=%str(EVENT_DAY), out_table=work.daily_usage );
   data work.daily_usage_tmp /view=work.daily_usage_tmp ;
      set work.daily_usage ;
   run;
   %err_check (Failed to add time zone adaptation :daily_usage_tmp , daily_usage );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..daily_usage_tmp ;
            set work.daily_usage_tmp ;
            stop;
         run;
         proc append data=work.daily_usage_tmp  base=&tmplib..daily_usage_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..daily_usage_tmp ;
            set work.daily_usage_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :daily_usage_tmp , daily_usage );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..daily_usage as b using &tmpdbschema..daily_usage_tmp as d on( 
            b.event_day = d.event_day )
         when matched then  
         update set 
            b.bc_subjcnt_str = d.bc_subjcnt_str, 
            b.customer_profiles_processed_str = d.customer_profiles_processed_str, b.api_usage_str = d.api_usage_str, 
            b.mob_impr_cnt = d.mob_impr_cnt, b.dm_destinations_total_row_cnt = d.dm_destinations_total_row_cnt, 
            b.google_ads_cnt = d.google_ads_cnt, b.mob_sesn_cnt = d.mob_sesn_cnt, 
            b.audience_usage_cnt = d.audience_usage_cnt, b.mobile_in_app_msg_cnt = d.mobile_in_app_msg_cnt, 
            b.mobile_push_cnt = d.mobile_push_cnt, b.email_preview_cnt = d.email_preview_cnt, 
            b.facebook_ads_cnt = d.facebook_ads_cnt, b.web_sesn_cnt = d.web_sesn_cnt, 
            b.plan_users_cnt = d.plan_users_cnt, b.outbound_api_cnt = d.outbound_api_cnt, 
            b.web_impr_cnt = d.web_impr_cnt, b.email_send_cnt = d.email_send_cnt, 
            b.linkedin_ads_cnt = d.linkedin_ads_cnt, b.dm_destinations_total_id_cnt = d.dm_destinations_total_id_cnt, 
            b.asset_size = d.asset_size, b.db_size = d.db_size, 
            b.admin_user_cnt = d.admin_user_cnt
         when not matched then insert ( 
            bc_subjcnt_str, customer_profiles_processed_str, api_usage_str, 
            mob_impr_cnt, dm_destinations_total_row_cnt, google_ads_cnt, mob_sesn_cnt, 
            audience_usage_cnt, mobile_in_app_msg_cnt, mobile_push_cnt, email_preview_cnt, 
            facebook_ads_cnt, web_sesn_cnt, plan_users_cnt, outbound_api_cnt, 
            web_impr_cnt, email_send_cnt, linkedin_ads_cnt, dm_destinations_total_id_cnt, 
            asset_size, db_size, admin_user_cnt, event_day
         ) values ( 
            d.bc_subjcnt_str, d.customer_profiles_processed_str, d.api_usage_str, 
            d.mob_impr_cnt, d.dm_destinations_total_row_cnt, d.google_ads_cnt, d.mob_sesn_cnt, 
            d.audience_usage_cnt, d.mobile_in_app_msg_cnt, d.mobile_push_cnt, d.email_preview_cnt, 
            d.facebook_ads_cnt, d.web_sesn_cnt, d.plan_users_cnt, d.outbound_api_cnt, 
            d.web_impr_cnt, d.email_send_cnt, d.linkedin_ads_cnt, d.dm_destinations_total_id_cnt, 
            d.asset_size, d.db_size, d.admin_user_cnt, d.event_day  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :daily_usage_tmp , daily_usage , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..daily_usage_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..daily_usage_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..DAILY_USAGE;
         drop table work.DAILY_USAGE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table DAILY_USAGE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..DATA_VIEW_DETAILS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..DATA_VIEW_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..data_view_details_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..data_view_details_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=data_view_details , table_keys=%str(EVENT_ID), out_table=work.data_view_details );
   data work.data_view_details_tmp /view=work.data_view_details_tmp ;
      set work.data_view_details ;
      if data_view_dttm_tz  ne . then data_view_dttm_tz = tzoneu2s(data_view_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :data_view_details_tmp , data_view_details );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..data_view_details_tmp ;
            set work.data_view_details_tmp ;
            stop;
         run;
         proc append data=work.data_view_details_tmp  base=&tmplib..data_view_details_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..data_view_details_tmp ;
            set work.data_view_details_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :data_view_details_tmp , data_view_details );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..data_view_details as b using &tmpdbschema..data_view_details_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.total_cost_amt = d.total_cost_amt, 
            b.properties_map_doc = d.properties_map_doc, b.data_view_dttm = d.data_view_dttm, 
            b.data_view_dttm_tz = d.data_view_dttm_tz, b.load_dttm = d.load_dttm, 
            b.visit_id = d.visit_id, b.reserved_2_txt = d.reserved_2_txt, 
            b.event_designed_id = d.event_designed_id, b.channel_user_id = d.channel_user_id, 
            b.detail_id = d.detail_id, b.event_nm = d.event_nm, 
            b.session_id_hex = d.session_id_hex, b.detail_id_hex = d.detail_id_hex, 
            b.identity_id = d.identity_id, b.parent_event_designed_id = d.parent_event_designed_id, 
            b.reserved_1_txt = d.reserved_1_txt, b.session_id = d.session_id, 
            b.visit_id_hex = d.visit_id_hex
         when not matched then insert ( 
            total_cost_amt, properties_map_doc, data_view_dttm, 
            data_view_dttm_tz, load_dttm, visit_id, reserved_2_txt, 
            event_designed_id, channel_user_id, detail_id, event_nm, 
            session_id_hex, detail_id_hex, event_id, identity_id, 
            parent_event_designed_id, reserved_1_txt, session_id, visit_id_hex
         ) values ( 
            d.total_cost_amt, d.properties_map_doc, d.data_view_dttm, 
            d.data_view_dttm_tz, d.load_dttm, d.visit_id, d.reserved_2_txt, 
            d.event_designed_id, d.channel_user_id, d.detail_id, d.event_nm, 
            d.session_id_hex, d.detail_id_hex, d.event_id, d.identity_id, 
            d.parent_event_designed_id, d.reserved_1_txt, d.session_id, d.visit_id_hex  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :data_view_details_tmp , data_view_details , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..data_view_details_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..data_view_details_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..DATA_VIEW_DETAILS;
         drop table work.DATA_VIEW_DETAILS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table DATA_VIEW_DETAILS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..DBT_ADV_CAMPAIGN_VISITORS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..DBT_ADV_CAMPAIGN_VISITORS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..dbt_adv_campaign_visitors_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..dbt_adv_campaign_visitors_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=dbt_adv_campaign_visitors , table_keys=%str(SESSION_ID,VISIT_ID), out_table=work.dbt_adv_campaign_visitors );
   data work.dbt_adv_campaign_visitors_tmp /view=work.dbt_adv_campaign_visitors_tmp ;
      set work.dbt_adv_campaign_visitors ;
      if visit_dttm_tz  ne . then visit_dttm_tz = tzoneu2s(visit_dttm_tz ,&timeZone_Value.);
      if session_start_dttm_tz  ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :dbt_adv_campaign_visitors_tmp , dbt_adv_campaign_visitors );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..dbt_adv_campaign_visitors_tmp ;
            set work.dbt_adv_campaign_visitors_tmp ;
            stop;
         run;
         proc append data=work.dbt_adv_campaign_visitors_tmp  base=&tmplib..dbt_adv_campaign_visitors_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..dbt_adv_campaign_visitors_tmp ;
            set work.dbt_adv_campaign_visitors_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :dbt_adv_campaign_visitors_tmp , dbt_adv_campaign_visitors );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..dbt_adv_campaign_visitors as b using &tmpdbschema..dbt_adv_campaign_visitors_tmp as d on( 
            b.visit_id = d.visit_id and 
            b.session_id = d.session_id )
         when matched then  
         update set 
            b.ge_longitude = d.ge_longitude, 
            b.ge_latitude = d.ge_latitude, b.rv_revenue = d.rv_revenue, 
            b.co_conversions = d.co_conversions, b.new_visitors = d.new_visitors, 
            b.return_visitors = d.return_visitors, b.bouncers = d.bouncers, 
            b.visits = d.visits, b.page_views = d.page_views, 
            b.average_visit_duration = d.average_visit_duration, b.session_complete_load_dttm = d.session_complete_load_dttm, 
            b.visit_dttm = d.visit_dttm, b.visit_dttm_tz = d.visit_dttm_tz, 
            b.session_start_dttm_tz = d.session_start_dttm_tz, b.session_start_dttm = d.session_start_dttm, 
            b.se_external_search_engine = d.se_external_search_engine, b.landing_page = d.landing_page, 
            b.ge_country = d.ge_country, b.cu_customer_id = d.cu_customer_id, 
            b.br_browser_version = d.br_browser_version, b.device_type = d.device_type, 
            b.landing_page_url_domain = d.landing_page_url_domain, b.se_external_search_engine_phrase = d.se_external_search_engine_phrase, 
            b.bouncer = d.bouncer, b.br_browser_name = d.br_browser_name, 
            b.device_name = d.device_name, b.ge_city = d.ge_city, 
            b.ge_state_region = d.ge_state_region, b.landing_page_url = d.landing_page_url, 
            b.pl_device_operating_system = d.pl_device_operating_system, b.se_external_search_engine_domain = d.se_external_search_engine_domain, 
            b.visitor_type = d.visitor_type, b.visitor_id = d.visitor_id, 
            b.visit_origination_type = d.visit_origination_type, b.visit_origination_tracking_code = d.visit_origination_tracking_code, 
            b.visit_origination_placement = d.visit_origination_placement, b.visit_origination_name = d.visit_origination_name, 
            b.visit_origination_creative = d.visit_origination_creative
         when not matched then insert ( 
            ge_longitude, ge_latitude, rv_revenue, 
            co_conversions, new_visitors, return_visitors, bouncers, 
            visits, page_views, average_visit_duration, session_complete_load_dttm, 
            visit_dttm, visit_dttm_tz, session_start_dttm_tz, session_start_dttm, 
            se_external_search_engine, landing_page, ge_country, cu_customer_id, 
            br_browser_version, device_type, landing_page_url_domain, se_external_search_engine_phrase, 
            bouncer, br_browser_name, device_name, ge_city, 
            ge_state_region, landing_page_url, pl_device_operating_system, se_external_search_engine_domain, 
            visitor_type, visitor_id, visit_origination_type, visit_origination_tracking_code, 
            visit_origination_placement, visit_origination_name, visit_origination_creative, visit_id, 
            session_id
         ) values ( 
            d.ge_longitude, d.ge_latitude, d.rv_revenue, 
            d.co_conversions, d.new_visitors, d.return_visitors, d.bouncers, 
            d.visits, d.page_views, d.average_visit_duration, d.session_complete_load_dttm, 
            d.visit_dttm, d.visit_dttm_tz, d.session_start_dttm_tz, d.session_start_dttm, 
            d.se_external_search_engine, d.landing_page, d.ge_country, d.cu_customer_id, 
            d.br_browser_version, d.device_type, d.landing_page_url_domain, d.se_external_search_engine_phrase, 
            d.bouncer, d.br_browser_name, d.device_name, d.ge_city, 
            d.ge_state_region, d.landing_page_url, d.pl_device_operating_system, d.se_external_search_engine_domain, 
            d.visitor_type, d.visitor_id, d.visit_origination_type, d.visit_origination_tracking_code, 
            d.visit_origination_placement, d.visit_origination_name, d.visit_origination_creative, d.visit_id, 
            d.session_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :dbt_adv_campaign_visitors_tmp , dbt_adv_campaign_visitors , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..dbt_adv_campaign_visitors_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..dbt_adv_campaign_visitors_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..DBT_ADV_CAMPAIGN_VISITORS;
         drop table work.DBT_ADV_CAMPAIGN_VISITORS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table DBT_ADV_CAMPAIGN_VISITORS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..DBT_BUSINESS_PROCESS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..DBT_BUSINESS_PROCESS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..dbt_business_process_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..dbt_business_process_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=dbt_business_process , table_keys=%str(BUSINESS_PROCESS_NAME,BUSINESS_PROCESS_STEP_NAME,BUS_PROCESS_STARTED_DTTM,SESSION_ID), out_table=work.dbt_business_process );
   data work.dbt_business_process_tmp /view=work.dbt_business_process_tmp ;
      set work.dbt_business_process ;
      if bus_process_started_dttm_tz  ne . then bus_process_started_dttm_tz = tzoneu2s(bus_process_started_dttm_tz ,&timeZone_Value.);
      if session_start_dttm_tz  ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :dbt_business_process_tmp , dbt_business_process );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..dbt_business_process_tmp ;
            set work.dbt_business_process_tmp ;
            stop;
         run;
         proc append data=work.dbt_business_process_tmp  base=&tmplib..dbt_business_process_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..dbt_business_process_tmp ;
            set work.dbt_business_process_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :dbt_business_process_tmp , dbt_business_process );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..dbt_business_process as b using &tmpdbschema..dbt_business_process_tmp as d on( 
            b.bus_process_started_dttm = d.bus_process_started_dttm and 
            b.session_id = d.session_id and b.business_process_step_name = d.business_process_step_name and 
            b.business_process_name = d.business_process_name )
         when matched then  
         update set 
            b.processes = d.processes, 
            b.steps_completed = d.steps_completed, b.step_count = d.step_count, 
            b.processes_completed = d.processes_completed, b.steps_abandoned = d.steps_abandoned, 
            b.last_step = d.last_step, b.processes_abandoned = d.processes_abandoned, 
            b.steps = d.steps, b.bus_process_started_dttm_tz = d.bus_process_started_dttm_tz, 
            b.session_start_dttm_tz = d.session_start_dttm_tz, b.session_start_dttm = d.session_start_dttm, 
            b.session_complete_load_dttm = d.session_complete_load_dttm, b.visitor_id = d.visitor_id, 
            b.visit_origination_tracking_code = d.visit_origination_tracking_code, b.visit_origination_name = d.visit_origination_name, 
            b.visit_id = d.visit_id, b.device_name = d.device_name, 
            b.cu_customer_id = d.cu_customer_id, b.business_process_attribute_2 = d.business_process_attribute_2, 
            b.bouncer = d.bouncer, b.business_process_attribute_1 = d.business_process_attribute_1, 
            b.device_type = d.device_type, b.visit_origination_creative = d.visit_origination_creative, 
            b.visit_origination_placement = d.visit_origination_placement, b.visit_origination_type = d.visit_origination_type, 
            b.visitor_type = d.visitor_type
         when not matched then insert ( 
            processes, steps_completed, step_count, 
            processes_completed, steps_abandoned, last_step, processes_abandoned, 
            steps, bus_process_started_dttm_tz, session_start_dttm_tz, session_start_dttm, 
            bus_process_started_dttm, session_complete_load_dttm, visitor_id, visit_origination_tracking_code, 
            visit_origination_name, visit_id, session_id, device_name, 
            cu_customer_id, business_process_step_name, business_process_attribute_2, bouncer, 
            business_process_attribute_1, business_process_name, device_type, visit_origination_creative, 
            visit_origination_placement, visit_origination_type, visitor_type
         ) values ( 
            d.processes, d.steps_completed, d.step_count, 
            d.processes_completed, d.steps_abandoned, d.last_step, d.processes_abandoned, 
            d.steps, d.bus_process_started_dttm_tz, d.session_start_dttm_tz, d.session_start_dttm, 
            d.bus_process_started_dttm, d.session_complete_load_dttm, d.visitor_id, d.visit_origination_tracking_code, 
            d.visit_origination_name, d.visit_id, d.session_id, d.device_name, 
            d.cu_customer_id, d.business_process_step_name, d.business_process_attribute_2, d.bouncer, 
            d.business_process_attribute_1, d.business_process_name, d.device_type, d.visit_origination_creative, 
            d.visit_origination_placement, d.visit_origination_type, d.visitor_type  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :dbt_business_process_tmp , dbt_business_process , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..dbt_business_process_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..dbt_business_process_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..DBT_BUSINESS_PROCESS;
         drop table work.DBT_BUSINESS_PROCESS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table DBT_BUSINESS_PROCESS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..DBT_CONTENT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..DBT_CONTENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..dbt_content_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..dbt_content_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=dbt_content , table_keys=%str(DETAIL_ID), out_table=work.dbt_content );
   data work.dbt_content_tmp /view=work.dbt_content_tmp ;
      set work.dbt_content ;
      if session_start_dttm_tz  ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz ,&timeZone_Value.);
      if detail_dttm_tz  ne . then detail_dttm_tz = tzoneu2s(detail_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :dbt_content_tmp , dbt_content );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..dbt_content_tmp ;
            set work.dbt_content_tmp ;
            stop;
         run;
         proc append data=work.dbt_content_tmp  base=&tmplib..dbt_content_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..dbt_content_tmp ;
            set work.dbt_content_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :dbt_content_tmp , dbt_content );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..dbt_content as b using &tmpdbschema..dbt_content_tmp as d on( 
            b.detail_id = d.detail_id )
         when matched then  
         update set 
            b.total_page_view_time = d.total_page_view_time, 
            b.entry_pages = d.entry_pages, b.active_page_view_time = d.active_page_view_time, 
            b.views = d.views, b.exit_pages = d.exit_pages, 
            b.visits = d.visits, b.bouncers = d.bouncers, 
            b.session_start_dttm = d.session_start_dttm, b.session_complete_load_dttm = d.session_complete_load_dttm, 
            b.session_start_dttm_tz = d.session_start_dttm_tz, b.detail_dttm_tz = d.detail_dttm_tz, 
            b.detail_dttm = d.detail_dttm, b.visitor_type = d.visitor_type, 
            b.visitor_id = d.visitor_id, b.visit_origination_type = d.visit_origination_type, 
            b.visit_origination_tracking_code = d.visit_origination_tracking_code, b.visit_origination_placement = d.visit_origination_placement, 
            b.visit_origination_name = d.visit_origination_name, b.visit_origination_creative = d.visit_origination_creative, 
            b.visit_id = d.visit_id, b.session_id = d.session_id, 
            b.pg_page_url = d.pg_page_url, b.pg_page = d.pg_page, 
            b.pg_domain_name = d.pg_domain_name, b.device_type = d.device_type, 
            b.device_name = d.device_name, b.cu_customer_id = d.cu_customer_id, 
            b.class2_id = d.class2_id, b.bouncer = d.bouncer, 
            b.class1_id = d.class1_id
         when not matched then insert ( 
            total_page_view_time, entry_pages, active_page_view_time, 
            views, exit_pages, visits, bouncers, 
            session_start_dttm, session_complete_load_dttm, session_start_dttm_tz, detail_dttm_tz, 
            detail_dttm, visitor_type, visitor_id, visit_origination_type, 
            visit_origination_tracking_code, visit_origination_placement, visit_origination_name, visit_origination_creative, 
            visit_id, session_id, pg_page_url, pg_page, 
            pg_domain_name, device_type, device_name, detail_id, 
            cu_customer_id, class2_id, bouncer, class1_id
         ) values ( 
            d.total_page_view_time, d.entry_pages, d.active_page_view_time, 
            d.views, d.exit_pages, d.visits, d.bouncers, 
            d.session_start_dttm, d.session_complete_load_dttm, d.session_start_dttm_tz, d.detail_dttm_tz, 
            d.detail_dttm, d.visitor_type, d.visitor_id, d.visit_origination_type, 
            d.visit_origination_tracking_code, d.visit_origination_placement, d.visit_origination_name, d.visit_origination_creative, 
            d.visit_id, d.session_id, d.pg_page_url, d.pg_page, 
            d.pg_domain_name, d.device_type, d.device_name, d.detail_id, 
            d.cu_customer_id, d.class2_id, d.bouncer, d.class1_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :dbt_content_tmp , dbt_content , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..dbt_content_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..dbt_content_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..DBT_CONTENT;
         drop table work.DBT_CONTENT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table DBT_CONTENT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..DBT_DOCUMENTS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..DBT_DOCUMENTS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..dbt_documents_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..dbt_documents_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=dbt_documents , table_keys=%str(DETAIL_ID), out_table=work.dbt_documents );
   data work.dbt_documents_tmp /view=work.dbt_documents_tmp ;
      set work.dbt_documents ;
      if document_download_dttm_tz  ne . then document_download_dttm_tz = tzoneu2s(document_download_dttm_tz ,&timeZone_Value.);
      if session_start_dttm_tz  ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :dbt_documents_tmp , dbt_documents );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..dbt_documents_tmp ;
            set work.dbt_documents_tmp ;
            stop;
         run;
         proc append data=work.dbt_documents_tmp  base=&tmplib..dbt_documents_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..dbt_documents_tmp ;
            set work.dbt_documents_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :dbt_documents_tmp , dbt_documents );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..dbt_documents as b using &tmpdbschema..dbt_documents_tmp as d on( 
            b.detail_id = d.detail_id )
         when matched then  
         update set 
            b.document_downloads = d.document_downloads, 
            b.document_download_dttm_tz = d.document_download_dttm_tz, b.session_start_dttm_tz = d.session_start_dttm_tz, 
            b.session_start_dttm = d.session_start_dttm, b.session_complete_load_dttm = d.session_complete_load_dttm, 
            b.document_download_dttm = d.document_download_dttm, b.visitor_type = d.visitor_type, 
            b.visitor_id = d.visitor_id, b.visit_origination_type = d.visit_origination_type, 
            b.visit_origination_tracking_code = d.visit_origination_tracking_code, b.visit_origination_placement = d.visit_origination_placement, 
            b.visit_origination_name = d.visit_origination_name, b.visit_origination_creative = d.visit_origination_creative, 
            b.visit_id = d.visit_id, b.session_id = d.session_id, 
            b.do_page_url = d.do_page_url, b.do_page_description = d.do_page_description, 
            b.device_type = d.device_type, b.device_name = d.device_name, 
            b.cu_customer_id = d.cu_customer_id, b.class2_id = d.class2_id, 
            b.class1_id = d.class1_id, b.bouncer = d.bouncer
         when not matched then insert ( 
            document_downloads, document_download_dttm_tz, session_start_dttm_tz, 
            session_start_dttm, session_complete_load_dttm, document_download_dttm, visitor_type, 
            visitor_id, visit_origination_type, visit_origination_tracking_code, visit_origination_placement, 
            visit_origination_name, visit_origination_creative, visit_id, session_id, 
            do_page_url, do_page_description, device_type, device_name, 
            detail_id, cu_customer_id, class2_id, class1_id, 
            bouncer
         ) values ( 
            d.document_downloads, d.document_download_dttm_tz, d.session_start_dttm_tz, 
            d.session_start_dttm, d.session_complete_load_dttm, d.document_download_dttm, d.visitor_type, 
            d.visitor_id, d.visit_origination_type, d.visit_origination_tracking_code, d.visit_origination_placement, 
            d.visit_origination_name, d.visit_origination_creative, d.visit_id, d.session_id, 
            d.do_page_url, d.do_page_description, d.device_type, d.device_name, 
            d.detail_id, d.cu_customer_id, d.class2_id, d.class1_id, 
            d.bouncer  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :dbt_documents_tmp , dbt_documents , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..dbt_documents_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..dbt_documents_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..DBT_DOCUMENTS;
         drop table work.DBT_DOCUMENTS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table DBT_DOCUMENTS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..DBT_ECOMMERCE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..DBT_ECOMMERCE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..dbt_ecommerce_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..dbt_ecommerce_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=dbt_ecommerce , table_keys=%str(BASKET_ID,PRODUCT_ACTIVITY_DTTM,PRODUCT_ID,PRODUCT_NAME,PRODUCT_SKU,VISIT_ID), out_table=work.dbt_ecommerce );
   data work.dbt_ecommerce_tmp /view=work.dbt_ecommerce_tmp ;
      set work.dbt_ecommerce ;
      if session_start_dttm_tz  ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz ,&timeZone_Value.);
      if product_activity_dttm_tz  ne . then product_activity_dttm_tz = tzoneu2s(product_activity_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :dbt_ecommerce_tmp , dbt_ecommerce );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..dbt_ecommerce_tmp ;
            set work.dbt_ecommerce_tmp ;
            stop;
         run;
         proc append data=work.dbt_ecommerce_tmp  base=&tmplib..dbt_ecommerce_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..dbt_ecommerce_tmp ;
            set work.dbt_ecommerce_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :dbt_ecommerce_tmp , dbt_ecommerce );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..dbt_ecommerce as b using &tmpdbschema..dbt_ecommerce_tmp as d on( 
            b.product_activity_dttm = d.product_activity_dttm and 
            b.visit_id = d.visit_id and b.product_sku = d.product_sku and 
            b.product_name = d.product_name and b.product_id = d.product_id and 
            b.basket_id = d.basket_id )
         when matched then  
         update set 
            b.product_purchase_revenues = d.product_purchase_revenues, 
            b.basket_adds_revenue = d.basket_adds_revenue, b.basket_removes_revenue = d.basket_removes_revenue, 
            b.product_views = d.product_views, b.basket_adds = d.basket_adds, 
            b.basket_adds_units = d.basket_adds_units, b.product_purchases = d.product_purchases, 
            b.product_purchase_units = d.product_purchase_units, b.basket_removes_units = d.basket_removes_units, 
            b.basket_removes = d.basket_removes, b.baskets_abandoned = d.baskets_abandoned, 
            b.baskets_completed = d.baskets_completed, b.baskets_started = d.baskets_started, 
            b.session_complete_load_dttm = d.session_complete_load_dttm, b.session_start_dttm_tz = d.session_start_dttm_tz, 
            b.product_activity_dttm_tz = d.product_activity_dttm_tz, b.session_start_dttm = d.session_start_dttm, 
            b.visitor_type = d.visitor_type, b.visitor_id = d.visitor_id, 
            b.visit_origination_type = d.visit_origination_type, b.visit_origination_tracking_code = d.visit_origination_tracking_code, 
            b.visit_origination_placement = d.visit_origination_placement, b.visit_origination_name = d.visit_origination_name, 
            b.visit_origination_creative = d.visit_origination_creative, b.session_id = d.session_id, 
            b.product_group_name = d.product_group_name, b.device_type = d.device_type, 
            b.device_name = d.device_name, b.cu_customer_id = d.cu_customer_id, 
            b.bouncer = d.bouncer
         when not matched then insert ( 
            product_purchase_revenues, basket_adds_revenue, basket_removes_revenue, 
            product_views, basket_adds, basket_adds_units, product_purchases, 
            product_purchase_units, basket_removes_units, basket_removes, baskets_abandoned, 
            baskets_completed, baskets_started, session_complete_load_dttm, session_start_dttm_tz, 
            product_activity_dttm_tz, product_activity_dttm, session_start_dttm, visitor_type, 
            visitor_id, visit_origination_type, visit_origination_tracking_code, visit_origination_placement, 
            visit_origination_name, visit_origination_creative, visit_id, session_id, 
            product_sku, product_name, product_id, product_group_name, 
            device_type, device_name, cu_customer_id, bouncer, 
            basket_id
         ) values ( 
            d.product_purchase_revenues, d.basket_adds_revenue, d.basket_removes_revenue, 
            d.product_views, d.basket_adds, d.basket_adds_units, d.product_purchases, 
            d.product_purchase_units, d.basket_removes_units, d.basket_removes, d.baskets_abandoned, 
            d.baskets_completed, d.baskets_started, d.session_complete_load_dttm, d.session_start_dttm_tz, 
            d.product_activity_dttm_tz, d.product_activity_dttm, d.session_start_dttm, d.visitor_type, 
            d.visitor_id, d.visit_origination_type, d.visit_origination_tracking_code, d.visit_origination_placement, 
            d.visit_origination_name, d.visit_origination_creative, d.visit_id, d.session_id, 
            d.product_sku, d.product_name, d.product_id, d.product_group_name, 
            d.device_type, d.device_name, d.cu_customer_id, d.bouncer, 
            d.basket_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :dbt_ecommerce_tmp , dbt_ecommerce , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..dbt_ecommerce_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..dbt_ecommerce_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..DBT_ECOMMERCE;
         drop table work.DBT_ECOMMERCE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table DBT_ECOMMERCE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..DBT_FORMS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..DBT_FORMS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..dbt_forms_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..dbt_forms_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=dbt_forms , table_keys=%str(DETAIL_ID), out_table=work.dbt_forms );
   data work.dbt_forms_tmp /view=work.dbt_forms_tmp ;
      set work.dbt_forms ;
      if form_attempt_dttm_tz  ne . then form_attempt_dttm_tz = tzoneu2s(form_attempt_dttm_tz ,&timeZone_Value.);
      if session_start_dttm_tz  ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :dbt_forms_tmp , dbt_forms );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..dbt_forms_tmp ;
            set work.dbt_forms_tmp ;
            stop;
         run;
         proc append data=work.dbt_forms_tmp  base=&tmplib..dbt_forms_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..dbt_forms_tmp ;
            set work.dbt_forms_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :dbt_forms_tmp , dbt_forms );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..dbt_forms as b using &tmpdbschema..dbt_forms_tmp as d on( 
            b.detail_id = d.detail_id )
         when matched then  
         update set 
            b.attempts = d.attempts, 
            b.forms_completed = d.forms_completed, b.forms_not_submitted = d.forms_not_submitted, 
            b.forms_started = d.forms_started, b.form_attempt_dttm = d.form_attempt_dttm, 
            b.session_start_dttm = d.session_start_dttm, b.form_attempt_dttm_tz = d.form_attempt_dttm_tz, 
            b.session_complete_load_dttm = d.session_complete_load_dttm, b.session_start_dttm_tz = d.session_start_dttm_tz, 
            b.visitor_type = d.visitor_type, b.visitor_id = d.visitor_id, 
            b.visit_origination_type = d.visit_origination_type, b.visit_origination_tracking_code = d.visit_origination_tracking_code, 
            b.visit_origination_placement = d.visit_origination_placement, b.visit_origination_name = d.visit_origination_name, 
            b.visit_origination_creative = d.visit_origination_creative, b.visit_id = d.visit_id, 
            b.session_id = d.session_id, b.last_field = d.last_field, 
            b.form_nm = d.form_nm, b.device_type = d.device_type, 
            b.device_name = d.device_name, b.cu_customer_id = d.cu_customer_id, 
            b.bouncer = d.bouncer
         when not matched then insert ( 
            attempts, forms_completed, forms_not_submitted, 
            forms_started, form_attempt_dttm, session_start_dttm, form_attempt_dttm_tz, 
            session_complete_load_dttm, session_start_dttm_tz, visitor_type, visitor_id, 
            visit_origination_type, visit_origination_tracking_code, visit_origination_placement, visit_origination_name, 
            visit_origination_creative, visit_id, session_id, last_field, 
            form_nm, device_type, device_name, detail_id, 
            cu_customer_id, bouncer
         ) values ( 
            d.attempts, d.forms_completed, d.forms_not_submitted, 
            d.forms_started, d.form_attempt_dttm, d.session_start_dttm, d.form_attempt_dttm_tz, 
            d.session_complete_load_dttm, d.session_start_dttm_tz, d.visitor_type, d.visitor_id, 
            d.visit_origination_type, d.visit_origination_tracking_code, d.visit_origination_placement, d.visit_origination_name, 
            d.visit_origination_creative, d.visit_id, d.session_id, d.last_field, 
            d.form_nm, d.device_type, d.device_name, d.detail_id, 
            d.cu_customer_id, d.bouncer  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :dbt_forms_tmp , dbt_forms , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..dbt_forms_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..dbt_forms_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..DBT_FORMS;
         drop table work.DBT_FORMS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table DBT_FORMS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..DBT_GOALS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..DBT_GOALS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..dbt_goals_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..dbt_goals_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=dbt_goals , table_keys=%str(DETAIL_ID), out_table=work.dbt_goals );
   data work.dbt_goals_tmp /view=work.dbt_goals_tmp ;
      set work.dbt_goals ;
      if goal_reached_dttm_tz  ne . then goal_reached_dttm_tz = tzoneu2s(goal_reached_dttm_tz ,&timeZone_Value.);
      if session_start_dttm_tz  ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :dbt_goals_tmp , dbt_goals );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..dbt_goals_tmp ;
            set work.dbt_goals_tmp ;
            stop;
         run;
         proc append data=work.dbt_goals_tmp  base=&tmplib..dbt_goals_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..dbt_goals_tmp ;
            set work.dbt_goals_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :dbt_goals_tmp , dbt_goals );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..dbt_goals as b using &tmpdbschema..dbt_goals_tmp as d on( 
            b.detail_id = d.detail_id )
         when matched then  
         update set 
            b.goal_revenue = d.goal_revenue, 
            b.visits = d.visits, b.session_start_dttm = d.session_start_dttm, 
            b.goal_reached_dttm_tz = d.goal_reached_dttm_tz, b.goal_reached_dttm = d.goal_reached_dttm, 
            b.session_complete_load_dttm = d.session_complete_load_dttm, b.session_start_dttm_tz = d.session_start_dttm_tz, 
            b.goals = d.goals, b.visitor_type = d.visitor_type, 
            b.visitor_id = d.visitor_id, b.visit_origination_type = d.visit_origination_type, 
            b.visit_origination_tracking_code = d.visit_origination_tracking_code, b.visit_origination_placement = d.visit_origination_placement, 
            b.visit_origination_name = d.visit_origination_name, b.visit_origination_creative = d.visit_origination_creative, 
            b.visit_id = d.visit_id, b.session_id = d.session_id, 
            b.goal_name = d.goal_name, b.goal_group_name = d.goal_group_name, 
            b.device_type = d.device_type, b.device_name = d.device_name, 
            b.cu_customer_id = d.cu_customer_id, b.bouncer = d.bouncer
         when not matched then insert ( 
            goal_revenue, visits, session_start_dttm, 
            goal_reached_dttm_tz, goal_reached_dttm, session_complete_load_dttm, session_start_dttm_tz, 
            goals, visitor_type, visitor_id, visit_origination_type, 
            visit_origination_tracking_code, visit_origination_placement, visit_origination_name, visit_origination_creative, 
            visit_id, session_id, goal_name, goal_group_name, 
            device_type, device_name, detail_id, cu_customer_id, 
            bouncer
         ) values ( 
            d.goal_revenue, d.visits, d.session_start_dttm, 
            d.goal_reached_dttm_tz, d.goal_reached_dttm, d.session_complete_load_dttm, d.session_start_dttm_tz, 
            d.goals, d.visitor_type, d.visitor_id, d.visit_origination_type, 
            d.visit_origination_tracking_code, d.visit_origination_placement, d.visit_origination_name, d.visit_origination_creative, 
            d.visit_id, d.session_id, d.goal_name, d.goal_group_name, 
            d.device_type, d.device_name, d.detail_id, d.cu_customer_id, 
            d.bouncer  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :dbt_goals_tmp , dbt_goals , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..dbt_goals_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..dbt_goals_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..DBT_GOALS;
         drop table work.DBT_GOALS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table DBT_GOALS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..DBT_MEDIA_CONSUMPTION)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..DBT_MEDIA_CONSUMPTION));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..dbt_media_consumption_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..dbt_media_consumption_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=dbt_media_consumption , table_keys=%str(DETAIL_ID,INTERACTIONS_COUNT,MAXIMUM_PROGRESS,MEDIA_COMPLETION_RATE,MEDIA_SECTION,VISIT_ID), out_table=work.dbt_media_consumption );
   data work.dbt_media_consumption_tmp /view=work.dbt_media_consumption_tmp ;
      set work.dbt_media_consumption ;
      if session_start_dttm_tz  ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz ,&timeZone_Value.);
      if media_start_dttm_tz  ne . then media_start_dttm_tz = tzoneu2s(media_start_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :dbt_media_consumption_tmp , dbt_media_consumption );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..dbt_media_consumption_tmp ;
            set work.dbt_media_consumption_tmp ;
            stop;
         run;
         proc append data=work.dbt_media_consumption_tmp  base=&tmplib..dbt_media_consumption_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..dbt_media_consumption_tmp ;
            set work.dbt_media_consumption_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :dbt_media_consumption_tmp , dbt_media_consumption );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..dbt_media_consumption as b using &tmpdbschema..dbt_media_consumption_tmp as d on( 
            b.maximum_progress = d.maximum_progress and 
            b.interactions_count = d.interactions_count and b.visit_id = d.visit_id and 
            b.media_section = d.media_section and b.media_completion_rate = d.media_completion_rate and 
            b.detail_id = d.detail_id )
         when matched then  
         update set 
            b.time_viewing = d.time_viewing, 
            b.duration = d.duration, b.content_viewed = d.content_viewed, 
            b.counter = d.counter, b.session_start_dttm_tz = d.session_start_dttm_tz, 
            b.session_start_dttm = d.session_start_dttm, b.session_complete_load_dttm = d.session_complete_load_dttm, 
            b.media_start_dttm = d.media_start_dttm, b.media_start_dttm_tz = d.media_start_dttm_tz, 
            b.views_started = d.views_started, b.views_completed = d.views_completed, 
            b.views = d.views, b.media_section_view = d.media_section_view, 
            b.visitor_type = d.visitor_type, b.visitor_id = d.visitor_id, 
            b.visit_origination_type = d.visit_origination_type, b.visit_origination_tracking_code = d.visit_origination_tracking_code, 
            b.visit_origination_placement = d.visit_origination_placement, b.visit_origination_name = d.visit_origination_name, 
            b.visit_origination_creative = d.visit_origination_creative, b.session_id = d.session_id, 
            b.media_uri_txt = d.media_uri_txt, b.media_name = d.media_name, 
            b.device_type = d.device_type, b.device_name = d.device_name, 
            b.cu_customer_id = d.cu_customer_id, b.bouncer = d.bouncer
         when not matched then insert ( 
            time_viewing, duration, maximum_progress, 
            content_viewed, counter, interactions_count, session_start_dttm_tz, 
            session_start_dttm, session_complete_load_dttm, media_start_dttm, media_start_dttm_tz, 
            views_started, views_completed, views, media_section_view, 
            visitor_type, visitor_id, visit_origination_type, visit_origination_tracking_code, 
            visit_origination_placement, visit_origination_name, visit_origination_creative, visit_id, 
            session_id, media_uri_txt, media_section, media_name, 
            media_completion_rate, device_type, device_name, detail_id, 
            cu_customer_id, bouncer
         ) values ( 
            d.time_viewing, d.duration, d.maximum_progress, 
            d.content_viewed, d.counter, d.interactions_count, d.session_start_dttm_tz, 
            d.session_start_dttm, d.session_complete_load_dttm, d.media_start_dttm, d.media_start_dttm_tz, 
            d.views_started, d.views_completed, d.views, d.media_section_view, 
            d.visitor_type, d.visitor_id, d.visit_origination_type, d.visit_origination_tracking_code, 
            d.visit_origination_placement, d.visit_origination_name, d.visit_origination_creative, d.visit_id, 
            d.session_id, d.media_uri_txt, d.media_section, d.media_name, 
            d.media_completion_rate, d.device_type, d.device_name, d.detail_id, 
            d.cu_customer_id, d.bouncer  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :dbt_media_consumption_tmp , dbt_media_consumption , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..dbt_media_consumption_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..dbt_media_consumption_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..DBT_MEDIA_CONSUMPTION;
         drop table work.DBT_MEDIA_CONSUMPTION;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table DBT_MEDIA_CONSUMPTION;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..DBT_PROMOTIONS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..DBT_PROMOTIONS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..dbt_promotions_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..dbt_promotions_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=dbt_promotions , table_keys=%str(DETAIL_ID), out_table=work.dbt_promotions );
   data work.dbt_promotions_tmp /view=work.dbt_promotions_tmp ;
      set work.dbt_promotions ;
      if session_start_dttm_tz  ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz ,&timeZone_Value.);
      if promotion_shown_dttm_tz  ne . then promotion_shown_dttm_tz = tzoneu2s(promotion_shown_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :dbt_promotions_tmp , dbt_promotions );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..dbt_promotions_tmp ;
            set work.dbt_promotions_tmp ;
            stop;
         run;
         proc append data=work.dbt_promotions_tmp  base=&tmplib..dbt_promotions_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..dbt_promotions_tmp ;
            set work.dbt_promotions_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :dbt_promotions_tmp , dbt_promotions );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..dbt_promotions as b using &tmpdbschema..dbt_promotions_tmp as d on( 
            b.detail_id = d.detail_id )
         when matched then  
         update set 
            b.click_throughs = d.click_throughs, 
            b.displays = d.displays, b.session_start_dttm_tz = d.session_start_dttm_tz, 
            b.promotion_shown_dttm_tz = d.promotion_shown_dttm_tz, b.promotion_shown_dttm = d.promotion_shown_dttm, 
            b.session_complete_load_dttm = d.session_complete_load_dttm, b.session_start_dttm = d.session_start_dttm, 
            b.visitor_type = d.visitor_type, b.visitor_id = d.visitor_id, 
            b.visit_origination_type = d.visit_origination_type, b.visit_origination_tracking_code = d.visit_origination_tracking_code, 
            b.visit_origination_placement = d.visit_origination_placement, b.visit_origination_name = d.visit_origination_name, 
            b.visit_origination_creative = d.visit_origination_creative, b.visit_id = d.visit_id, 
            b.session_id = d.session_id, b.promotion_type = d.promotion_type, 
            b.promotion_tracking_code = d.promotion_tracking_code, b.promotion_placement = d.promotion_placement, 
            b.promotion_name = d.promotion_name, b.promotion_creative = d.promotion_creative, 
            b.device_type = d.device_type, b.device_name = d.device_name, 
            b.cu_customer_id = d.cu_customer_id, b.bouncer = d.bouncer
         when not matched then insert ( 
            click_throughs, displays, session_start_dttm_tz, 
            promotion_shown_dttm_tz, promotion_shown_dttm, session_complete_load_dttm, session_start_dttm, 
            visitor_type, visitor_id, visit_origination_type, visit_origination_tracking_code, 
            visit_origination_placement, visit_origination_name, visit_origination_creative, visit_id, 
            session_id, promotion_type, promotion_tracking_code, promotion_placement, 
            promotion_name, promotion_creative, device_type, device_name, 
            detail_id, cu_customer_id, bouncer
         ) values ( 
            d.click_throughs, d.displays, d.session_start_dttm_tz, 
            d.promotion_shown_dttm_tz, d.promotion_shown_dttm, d.session_complete_load_dttm, d.session_start_dttm, 
            d.visitor_type, d.visitor_id, d.visit_origination_type, d.visit_origination_tracking_code, 
            d.visit_origination_placement, d.visit_origination_name, d.visit_origination_creative, d.visit_id, 
            d.session_id, d.promotion_type, d.promotion_tracking_code, d.promotion_placement, 
            d.promotion_name, d.promotion_creative, d.device_type, d.device_name, 
            d.detail_id, d.cu_customer_id, d.bouncer  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :dbt_promotions_tmp , dbt_promotions , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..dbt_promotions_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..dbt_promotions_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..DBT_PROMOTIONS;
         drop table work.DBT_PROMOTIONS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table DBT_PROMOTIONS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..DBT_SEARCH)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..DBT_SEARCH));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..dbt_search_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..dbt_search_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=dbt_search , table_keys=%str(DETAIL_ID), out_table=work.dbt_search );
   data work.dbt_search_tmp /view=work.dbt_search_tmp ;
      set work.dbt_search ;
      if search_results_dttm_tz  ne . then search_results_dttm_tz = tzoneu2s(search_results_dttm_tz ,&timeZone_Value.);
      if session_start_dttm_tz  ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :dbt_search_tmp , dbt_search );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..dbt_search_tmp ;
            set work.dbt_search_tmp ;
            stop;
         run;
         proc append data=work.dbt_search_tmp  base=&tmplib..dbt_search_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..dbt_search_tmp ;
            set work.dbt_search_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :dbt_search_tmp , dbt_search );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..dbt_search as b using &tmpdbschema..dbt_search_tmp as d on( 
            b.detail_id = d.detail_id )
         when matched then  
         update set 
            b.num_additional_searches = d.num_additional_searches, 
            b.num_pages_viewed_afterwards = d.num_pages_viewed_afterwards, b.searches = d.searches, 
            b.visits = d.visits, b.search_unknown_results = d.search_unknown_results, 
            b.search_returned_results = d.search_returned_results, b.exit_pages = d.exit_pages, 
            b.search_no_results_returned = d.search_no_results_returned, b.search_results_dttm_tz = d.search_results_dttm_tz, 
            b.session_start_dttm = d.session_start_dttm, b.session_start_dttm_tz = d.session_start_dttm_tz, 
            b.session_complete_load_dttm = d.session_complete_load_dttm, b.search_results_dttm = d.search_results_dttm, 
            b.visitor_type = d.visitor_type, b.visitor_id = d.visitor_id, 
            b.visit_origination_type = d.visit_origination_type, b.visit_origination_tracking_code = d.visit_origination_tracking_code, 
            b.visit_origination_placement = d.visit_origination_placement, b.visit_origination_name = d.visit_origination_name, 
            b.visit_origination_creative = d.visit_origination_creative, b.visit_id = d.visit_id, 
            b.session_id = d.session_id, b.search_name = d.search_name, 
            b.internal_search_term = d.internal_search_term, b.device_type = d.device_type, 
            b.device_name = d.device_name, b.cu_customer_id = d.cu_customer_id, 
            b.bouncer = d.bouncer
         when not matched then insert ( 
            num_additional_searches, num_pages_viewed_afterwards, searches, 
            visits, search_unknown_results, search_returned_results, exit_pages, 
            search_no_results_returned, search_results_dttm_tz, session_start_dttm, session_start_dttm_tz, 
            session_complete_load_dttm, search_results_dttm, visitor_type, visitor_id, 
            visit_origination_type, visit_origination_tracking_code, visit_origination_placement, visit_origination_name, 
            visit_origination_creative, visit_id, session_id, search_name, 
            internal_search_term, device_type, device_name, detail_id, 
            cu_customer_id, bouncer
         ) values ( 
            d.num_additional_searches, d.num_pages_viewed_afterwards, d.searches, 
            d.visits, d.search_unknown_results, d.search_returned_results, d.exit_pages, 
            d.search_no_results_returned, d.search_results_dttm_tz, d.session_start_dttm, d.session_start_dttm_tz, 
            d.session_complete_load_dttm, d.search_results_dttm, d.visitor_type, d.visitor_id, 
            d.visit_origination_type, d.visit_origination_tracking_code, d.visit_origination_placement, d.visit_origination_name, 
            d.visit_origination_creative, d.visit_id, d.session_id, d.search_name, 
            d.internal_search_term, d.device_type, d.device_name, d.detail_id, 
            d.cu_customer_id, d.bouncer  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :dbt_search_tmp , dbt_search , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..dbt_search_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..dbt_search_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..DBT_SEARCH;
         drop table work.DBT_SEARCH;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table DBT_SEARCH;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..DIRECT_CONTACT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..DIRECT_CONTACT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..direct_contact_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..direct_contact_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=direct_contact , table_keys=%str(EVENT_ID), out_table=work.direct_contact );
   data work.direct_contact_tmp /view=work.direct_contact_tmp ;
      set work.direct_contact ;
      if direct_contact_dttm_tz  ne . then direct_contact_dttm_tz = tzoneu2s(direct_contact_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :direct_contact_tmp , direct_contact );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..direct_contact_tmp ;
            set work.direct_contact_tmp ;
            stop;
         run;
         proc append data=work.direct_contact_tmp  base=&tmplib..direct_contact_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..direct_contact_tmp ;
            set work.direct_contact_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :direct_contact_tmp , direct_contact );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..direct_contact as b using &tmpdbschema..direct_contact_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.control_active_flg = d.control_active_flg, 
            b.control_group_flg = d.control_group_flg, b.properties_map_doc = d.properties_map_doc, 
            b.load_dttm = d.load_dttm, b.direct_contact_dttm = d.direct_contact_dttm, 
            b.direct_contact_dttm_tz = d.direct_contact_dttm_tz, b.task_version_id = d.task_version_id, 
            b.task_id = d.task_id, b.segment_id = d.segment_id, 
            b.response_tracking_cd = d.response_tracking_cd, b.occurrence_id = d.occurrence_id, 
            b.message_id = d.message_id, b.identity_type_nm = d.identity_type_nm, 
            b.identity_id = d.identity_id, b.event_nm = d.event_nm, 
            b.event_designed_id = d.event_designed_id, b.context_val = d.context_val, 
            b.context_type_nm = d.context_type_nm, b.channel_user_id = d.channel_user_id, 
            b.channel_nm = d.channel_nm
         when not matched then insert ( 
            control_active_flg, control_group_flg, properties_map_doc, 
            load_dttm, direct_contact_dttm, direct_contact_dttm_tz, task_version_id, 
            task_id, segment_id, response_tracking_cd, occurrence_id, 
            message_id, identity_type_nm, identity_id, event_nm, 
            event_id, event_designed_id, context_val, context_type_nm, 
            channel_user_id, channel_nm
         ) values ( 
            d.control_active_flg, d.control_group_flg, d.properties_map_doc, 
            d.load_dttm, d.direct_contact_dttm, d.direct_contact_dttm_tz, d.task_version_id, 
            d.task_id, d.segment_id, d.response_tracking_cd, d.occurrence_id, 
            d.message_id, d.identity_type_nm, d.identity_id, d.event_nm, 
            d.event_id, d.event_designed_id, d.context_val, d.context_type_nm, 
            d.channel_user_id, d.channel_nm  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :direct_contact_tmp , direct_contact , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..direct_contact_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..direct_contact_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..DIRECT_CONTACT;
         drop table work.DIRECT_CONTACT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table DIRECT_CONTACT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..DOCUMENT_DETAILS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..DOCUMENT_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..document_details_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..document_details_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=document_details , table_keys=%str(EVENT_ID), out_table=work.document_details );
   data work.document_details_tmp /view=work.document_details_tmp ;
      set work.document_details ;
      if link_event_dttm_tz  ne . then link_event_dttm_tz = tzoneu2s(link_event_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :document_details_tmp , document_details );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..document_details_tmp ;
            set work.document_details_tmp ;
            stop;
         run;
         proc append data=work.document_details_tmp  base=&tmplib..document_details_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..document_details_tmp ;
            set work.document_details_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :document_details_tmp , document_details );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..document_details as b using &tmpdbschema..document_details_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.load_dttm = d.load_dttm, 
            b.link_event_dttm = d.link_event_dttm, b.link_event_dttm_tz = d.link_event_dttm_tz, 
            b.visit_id_hex = d.visit_id_hex, b.uri_txt = d.uri_txt, 
            b.session_id = d.session_id, b.link_selector_path = d.link_selector_path, 
            b.link_id = d.link_id, b.link_name = d.link_name, 
            b.identity_id = d.identity_id, b.event_source_cd = d.event_source_cd, 
            b.session_id_hex = d.session_id_hex, b.event_key_cd = d.event_key_cd, 
            b.visit_id = d.visit_id, b.detail_id_hex = d.detail_id_hex, 
            b.detail_id = d.detail_id, b.alt_txt = d.alt_txt
         when not matched then insert ( 
            load_dttm, link_event_dttm, link_event_dttm_tz, 
            visit_id_hex, uri_txt, session_id, link_selector_path, 
            link_id, link_name, identity_id, event_source_cd, 
            session_id_hex, event_key_cd, visit_id, event_id, 
            detail_id_hex, detail_id, alt_txt
         ) values ( 
            d.load_dttm, d.link_event_dttm, d.link_event_dttm_tz, 
            d.visit_id_hex, d.uri_txt, d.session_id, d.link_selector_path, 
            d.link_id, d.link_name, d.identity_id, d.event_source_cd, 
            d.session_id_hex, d.event_key_cd, d.visit_id, d.event_id, 
            d.detail_id_hex, d.detail_id, d.alt_txt  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :document_details_tmp , document_details , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..document_details_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..document_details_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..DOCUMENT_DETAILS;
         drop table work.DOCUMENT_DETAILS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table DOCUMENT_DETAILS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..EMAIL_BOUNCE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..EMAIL_BOUNCE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..email_bounce_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..email_bounce_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=email_bounce , table_keys=%str(EVENT_ID), out_table=work.email_bounce );
   data work.email_bounce_tmp /view=work.email_bounce_tmp ;
      set work.email_bounce ;
      if email_bounce_dttm_tz  ne . then email_bounce_dttm_tz = tzoneu2s(email_bounce_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :email_bounce_tmp , email_bounce );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..email_bounce_tmp ;
            set work.email_bounce_tmp ;
            stop;
         run;
         proc append data=work.email_bounce_tmp  base=&tmplib..email_bounce_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..email_bounce_tmp ;
            set work.email_bounce_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :email_bounce_tmp , email_bounce );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..email_bounce as b using &tmpdbschema..email_bounce_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.test_flg = d.test_flg, 
            b.properties_map_doc = d.properties_map_doc, b.load_dttm = d.load_dttm, 
            b.email_bounce_dttm_tz = d.email_bounce_dttm_tz, b.email_bounce_dttm = d.email_bounce_dttm, 
            b.task_id = d.task_id, b.subject_line_txt = d.subject_line_txt, 
            b.segment_version_id = d.segment_version_id, b.response_tracking_cd = d.response_tracking_cd, 
            b.reason_txt = d.reason_txt, b.raw_reason_txt = d.raw_reason_txt, 
            b.occurrence_id = d.occurrence_id, b.journey_occurrence_id = d.journey_occurrence_id, 
            b.imprint_id = d.imprint_id, b.event_nm = d.event_nm, 
            b.event_designed_id = d.event_designed_id, b.context_type_nm = d.context_type_nm, 
            b.bounce_class_cd = d.bounce_class_cd, b.aud_occurrence_id = d.aud_occurrence_id, 
            b.analysis_group_id = d.analysis_group_id, b.audience_id = d.audience_id, 
            b.channel_user_id = d.channel_user_id, b.context_val = d.context_val, 
            b.identity_id = d.identity_id, b.journey_id = d.journey_id, 
            b.program_id = d.program_id, b.recipient_domain_nm = d.recipient_domain_nm, 
            b.segment_id = d.segment_id, b.task_version_id = d.task_version_id
         when not matched then insert ( 
            test_flg, properties_map_doc, load_dttm, 
            email_bounce_dttm_tz, email_bounce_dttm, task_id, subject_line_txt, 
            segment_version_id, response_tracking_cd, reason_txt, raw_reason_txt, 
            occurrence_id, journey_occurrence_id, imprint_id, event_nm, 
            event_designed_id, context_type_nm, bounce_class_cd, aud_occurrence_id, 
            analysis_group_id, audience_id, channel_user_id, context_val, 
            event_id, identity_id, journey_id, program_id, 
            recipient_domain_nm, segment_id, task_version_id
         ) values ( 
            d.test_flg, d.properties_map_doc, d.load_dttm, 
            d.email_bounce_dttm_tz, d.email_bounce_dttm, d.task_id, d.subject_line_txt, 
            d.segment_version_id, d.response_tracking_cd, d.reason_txt, d.raw_reason_txt, 
            d.occurrence_id, d.journey_occurrence_id, d.imprint_id, d.event_nm, 
            d.event_designed_id, d.context_type_nm, d.bounce_class_cd, d.aud_occurrence_id, 
            d.analysis_group_id, d.audience_id, d.channel_user_id, d.context_val, 
            d.event_id, d.identity_id, d.journey_id, d.program_id, 
            d.recipient_domain_nm, d.segment_id, d.task_version_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :email_bounce_tmp , email_bounce , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..email_bounce_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..email_bounce_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..EMAIL_BOUNCE;
         drop table work.EMAIL_BOUNCE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table EMAIL_BOUNCE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..EMAIL_CLICK)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..EMAIL_CLICK));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..email_click_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..email_click_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=email_click , table_keys=%str(EVENT_ID), out_table=work.email_click );
   data work.email_click_tmp /view=work.email_click_tmp ;
      set work.email_click ;
      if email_click_dttm_tz  ne . then email_click_dttm_tz = tzoneu2s(email_click_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :email_click_tmp , email_click );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..email_click_tmp ;
            set work.email_click_tmp ;
            stop;
         run;
         proc append data=work.email_click_tmp  base=&tmplib..email_click_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..email_click_tmp ;
            set work.email_click_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :email_click_tmp , email_click );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..email_click as b using &tmpdbschema..email_click_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.test_flg = d.test_flg, 
            b.open_tracking_flg = d.open_tracking_flg, b.is_mobile_flg = d.is_mobile_flg, 
            b.click_tracking_flg = d.click_tracking_flg, b.properties_map_doc = d.properties_map_doc, 
            b.email_click_dttm = d.email_click_dttm, b.email_click_dttm_tz = d.email_click_dttm_tz, 
            b.load_dttm = d.load_dttm, b.uri_txt = d.uri_txt, 
            b.task_version_id = d.task_version_id, b.task_id = d.task_id, 
            b.subject_line_txt = d.subject_line_txt, b.segment_id = d.segment_id, 
            b.recipient_domain_nm = d.recipient_domain_nm, b.program_id = d.program_id, 
            b.platform_version = d.platform_version, b.platform_desc = d.platform_desc, 
            b.occurrence_id = d.occurrence_id, b.manufacturer_nm = d.manufacturer_nm, 
            b.mailbox_provider_nm = d.mailbox_provider_nm, b.link_tracking_label_txt = d.link_tracking_label_txt, 
            b.link_tracking_id = d.link_tracking_id, b.link_tracking_group_txt = d.link_tracking_group_txt, 
            b.journey_id = d.journey_id, b.imprint_id = d.imprint_id, 
            b.event_nm = d.event_nm, b.event_designed_id = d.event_designed_id, 
            b.context_val = d.context_val, b.audience_id = d.audience_id, 
            b.analysis_group_id = d.analysis_group_id, b.agent_family_nm = d.agent_family_nm, 
            b.aud_occurrence_id = d.aud_occurrence_id, b.channel_user_id = d.channel_user_id, 
            b.context_type_nm = d.context_type_nm, b.device_nm = d.device_nm, 
            b.identity_id = d.identity_id, b.journey_occurrence_id = d.journey_occurrence_id, 
            b.response_tracking_cd = d.response_tracking_cd, b.segment_version_id = d.segment_version_id, 
            b.user_agent_nm = d.user_agent_nm
         when not matched then insert ( 
            test_flg, open_tracking_flg, is_mobile_flg, 
            click_tracking_flg, properties_map_doc, email_click_dttm, email_click_dttm_tz, 
            load_dttm, uri_txt, task_version_id, task_id, 
            subject_line_txt, segment_id, recipient_domain_nm, program_id, 
            platform_version, platform_desc, occurrence_id, manufacturer_nm, 
            mailbox_provider_nm, link_tracking_label_txt, link_tracking_id, link_tracking_group_txt, 
            journey_id, imprint_id, event_nm, event_id, 
            event_designed_id, context_val, audience_id, analysis_group_id, 
            agent_family_nm, aud_occurrence_id, channel_user_id, context_type_nm, 
            device_nm, identity_id, journey_occurrence_id, response_tracking_cd, 
            segment_version_id, user_agent_nm
         ) values ( 
            d.test_flg, d.open_tracking_flg, d.is_mobile_flg, 
            d.click_tracking_flg, d.properties_map_doc, d.email_click_dttm, d.email_click_dttm_tz, 
            d.load_dttm, d.uri_txt, d.task_version_id, d.task_id, 
            d.subject_line_txt, d.segment_id, d.recipient_domain_nm, d.program_id, 
            d.platform_version, d.platform_desc, d.occurrence_id, d.manufacturer_nm, 
            d.mailbox_provider_nm, d.link_tracking_label_txt, d.link_tracking_id, d.link_tracking_group_txt, 
            d.journey_id, d.imprint_id, d.event_nm, d.event_id, 
            d.event_designed_id, d.context_val, d.audience_id, d.analysis_group_id, 
            d.agent_family_nm, d.aud_occurrence_id, d.channel_user_id, d.context_type_nm, 
            d.device_nm, d.identity_id, d.journey_occurrence_id, d.response_tracking_cd, 
            d.segment_version_id, d.user_agent_nm  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :email_click_tmp , email_click , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..email_click_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..email_click_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..EMAIL_CLICK;
         drop table work.EMAIL_CLICK;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table EMAIL_CLICK;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..EMAIL_COMPLAINT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..EMAIL_COMPLAINT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..email_complaint_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..email_complaint_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=email_complaint , table_keys=%str(EVENT_ID), out_table=work.email_complaint );
   data work.email_complaint_tmp /view=work.email_complaint_tmp ;
      set work.email_complaint ;
      if email_complaint_dttm_tz  ne . then email_complaint_dttm_tz = tzoneu2s(email_complaint_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :email_complaint_tmp , email_complaint );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..email_complaint_tmp ;
            set work.email_complaint_tmp ;
            stop;
         run;
         proc append data=work.email_complaint_tmp  base=&tmplib..email_complaint_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..email_complaint_tmp ;
            set work.email_complaint_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :email_complaint_tmp , email_complaint );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..email_complaint as b using &tmpdbschema..email_complaint_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.test_flg = d.test_flg, 
            b.properties_map_doc = d.properties_map_doc, b.load_dttm = d.load_dttm, 
            b.email_complaint_dttm = d.email_complaint_dttm, b.email_complaint_dttm_tz = d.email_complaint_dttm_tz, 
            b.task_id = d.task_id, b.segment_version_id = d.segment_version_id, 
            b.response_tracking_cd = d.response_tracking_cd, b.recipient_domain_nm = d.recipient_domain_nm, 
            b.occurrence_id = d.occurrence_id, b.journey_occurrence_id = d.journey_occurrence_id, 
            b.imprint_id = d.imprint_id, b.event_nm = d.event_nm, 
            b.event_designed_id = d.event_designed_id, b.context_type_nm = d.context_type_nm, 
            b.audience_id = d.audience_id, b.analysis_group_id = d.analysis_group_id, 
            b.aud_occurrence_id = d.aud_occurrence_id, b.channel_user_id = d.channel_user_id, 
            b.context_val = d.context_val, b.identity_id = d.identity_id, 
            b.journey_id = d.journey_id, b.program_id = d.program_id, 
            b.segment_id = d.segment_id, b.subject_line_txt = d.subject_line_txt, 
            b.task_version_id = d.task_version_id
         when not matched then insert ( 
            test_flg, properties_map_doc, load_dttm, 
            email_complaint_dttm, email_complaint_dttm_tz, task_id, segment_version_id, 
            response_tracking_cd, recipient_domain_nm, occurrence_id, journey_occurrence_id, 
            imprint_id, event_nm, event_id, event_designed_id, 
            context_type_nm, audience_id, analysis_group_id, aud_occurrence_id, 
            channel_user_id, context_val, identity_id, journey_id, 
            program_id, segment_id, subject_line_txt, task_version_id
         ) values ( 
            d.test_flg, d.properties_map_doc, d.load_dttm, 
            d.email_complaint_dttm, d.email_complaint_dttm_tz, d.task_id, d.segment_version_id, 
            d.response_tracking_cd, d.recipient_domain_nm, d.occurrence_id, d.journey_occurrence_id, 
            d.imprint_id, d.event_nm, d.event_id, d.event_designed_id, 
            d.context_type_nm, d.audience_id, d.analysis_group_id, d.aud_occurrence_id, 
            d.channel_user_id, d.context_val, d.identity_id, d.journey_id, 
            d.program_id, d.segment_id, d.subject_line_txt, d.task_version_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :email_complaint_tmp , email_complaint , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..email_complaint_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..email_complaint_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..EMAIL_COMPLAINT;
         drop table work.EMAIL_COMPLAINT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table EMAIL_COMPLAINT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..EMAIL_OPEN)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..EMAIL_OPEN));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..email_open_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..email_open_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=email_open , table_keys=%str(EVENT_ID), out_table=work.email_open );
   data work.email_open_tmp /view=work.email_open_tmp ;
      set work.email_open ;
      if email_open_dttm_tz  ne . then email_open_dttm_tz = tzoneu2s(email_open_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :email_open_tmp , email_open );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..email_open_tmp ;
            set work.email_open_tmp ;
            stop;
         run;
         proc append data=work.email_open_tmp  base=&tmplib..email_open_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..email_open_tmp ;
            set work.email_open_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :email_open_tmp , email_open );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..email_open as b using &tmpdbschema..email_open_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.prefetched_flg = d.prefetched_flg, 
            b.click_tracking_flg = d.click_tracking_flg, b.open_tracking_flg = d.open_tracking_flg, 
            b.is_mobile_flg = d.is_mobile_flg, b.test_flg = d.test_flg, 
            b.properties_map_doc = d.properties_map_doc, b.email_open_dttm = d.email_open_dttm, 
            b.email_open_dttm_tz = d.email_open_dttm_tz, b.load_dttm = d.load_dttm, 
            b.user_agent_nm = d.user_agent_nm, b.task_version_id = d.task_version_id, 
            b.subject_line_txt = d.subject_line_txt, b.segment_version_id = d.segment_version_id, 
            b.segment_id = d.segment_id, b.recipient_domain_nm = d.recipient_domain_nm, 
            b.program_id = d.program_id, b.platform_version = d.platform_version, 
            b.occurrence_id = d.occurrence_id, b.manufacturer_nm = d.manufacturer_nm, 
            b.journey_id = d.journey_id, b.imprint_id = d.imprint_id, 
            b.event_nm = d.event_nm, b.event_designed_id = d.event_designed_id, 
            b.context_val = d.context_val, b.audience_id = d.audience_id, 
            b.analysis_group_id = d.analysis_group_id, b.agent_family_nm = d.agent_family_nm, 
            b.aud_occurrence_id = d.aud_occurrence_id, b.channel_user_id = d.channel_user_id, 
            b.context_type_nm = d.context_type_nm, b.device_nm = d.device_nm, 
            b.identity_id = d.identity_id, b.journey_occurrence_id = d.journey_occurrence_id, 
            b.mailbox_provider_nm = d.mailbox_provider_nm, b.platform_desc = d.platform_desc, 
            b.response_tracking_cd = d.response_tracking_cd, b.task_id = d.task_id
         when not matched then insert ( 
            prefetched_flg, click_tracking_flg, open_tracking_flg, 
            is_mobile_flg, test_flg, properties_map_doc, email_open_dttm, 
            email_open_dttm_tz, load_dttm, user_agent_nm, task_version_id, 
            subject_line_txt, segment_version_id, segment_id, recipient_domain_nm, 
            program_id, platform_version, occurrence_id, manufacturer_nm, 
            journey_id, imprint_id, event_nm, event_designed_id, 
            context_val, audience_id, analysis_group_id, agent_family_nm, 
            aud_occurrence_id, channel_user_id, context_type_nm, device_nm, 
            event_id, identity_id, journey_occurrence_id, mailbox_provider_nm, 
            platform_desc, response_tracking_cd, task_id
         ) values ( 
            d.prefetched_flg, d.click_tracking_flg, d.open_tracking_flg, 
            d.is_mobile_flg, d.test_flg, d.properties_map_doc, d.email_open_dttm, 
            d.email_open_dttm_tz, d.load_dttm, d.user_agent_nm, d.task_version_id, 
            d.subject_line_txt, d.segment_version_id, d.segment_id, d.recipient_domain_nm, 
            d.program_id, d.platform_version, d.occurrence_id, d.manufacturer_nm, 
            d.journey_id, d.imprint_id, d.event_nm, d.event_designed_id, 
            d.context_val, d.audience_id, d.analysis_group_id, d.agent_family_nm, 
            d.aud_occurrence_id, d.channel_user_id, d.context_type_nm, d.device_nm, 
            d.event_id, d.identity_id, d.journey_occurrence_id, d.mailbox_provider_nm, 
            d.platform_desc, d.response_tracking_cd, d.task_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :email_open_tmp , email_open , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..email_open_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..email_open_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..EMAIL_OPEN;
         drop table work.EMAIL_OPEN;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table EMAIL_OPEN;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..EMAIL_OPTOUT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..EMAIL_OPTOUT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..email_optout_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..email_optout_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=email_optout , table_keys=%str(EVENT_ID), out_table=work.email_optout );
   data work.email_optout_tmp /view=work.email_optout_tmp ;
      set work.email_optout ;
      if email_optout_dttm_tz  ne . then email_optout_dttm_tz = tzoneu2s(email_optout_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :email_optout_tmp , email_optout );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..email_optout_tmp ;
            set work.email_optout_tmp ;
            stop;
         run;
         proc append data=work.email_optout_tmp  base=&tmplib..email_optout_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..email_optout_tmp ;
            set work.email_optout_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :email_optout_tmp , email_optout );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..email_optout as b using &tmpdbschema..email_optout_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.test_flg = d.test_flg, 
            b.properties_map_doc = d.properties_map_doc, b.email_optout_dttm_tz = d.email_optout_dttm_tz, 
            b.email_optout_dttm = d.email_optout_dttm, b.load_dttm = d.load_dttm, 
            b.task_version_id = d.task_version_id, b.subject_line_txt = d.subject_line_txt, 
            b.segment_id = d.segment_id, b.recipient_domain_nm = d.recipient_domain_nm, 
            b.program_id = d.program_id, b.optout_type_nm = d.optout_type_nm, 
            b.occurrence_id = d.occurrence_id, b.link_tracking_label_txt = d.link_tracking_label_txt, 
            b.link_tracking_group_txt = d.link_tracking_group_txt, b.journey_id = d.journey_id, 
            b.identity_id = d.identity_id, b.event_nm = d.event_nm, 
            b.context_val = d.context_val, b.channel_user_id = d.channel_user_id, 
            b.audience_id = d.audience_id, b.aud_occurrence_id = d.aud_occurrence_id, 
            b.analysis_group_id = d.analysis_group_id, b.context_type_nm = d.context_type_nm, 
            b.event_designed_id = d.event_designed_id, b.imprint_id = d.imprint_id, 
            b.journey_occurrence_id = d.journey_occurrence_id, b.link_tracking_id = d.link_tracking_id, 
            b.response_tracking_cd = d.response_tracking_cd, b.segment_version_id = d.segment_version_id, 
            b.task_id = d.task_id
         when not matched then insert ( 
            test_flg, properties_map_doc, email_optout_dttm_tz, 
            email_optout_dttm, load_dttm, task_version_id, subject_line_txt, 
            segment_id, recipient_domain_nm, program_id, optout_type_nm, 
            occurrence_id, link_tracking_label_txt, link_tracking_group_txt, journey_id, 
            identity_id, event_nm, event_id, context_val, 
            channel_user_id, audience_id, aud_occurrence_id, analysis_group_id, 
            context_type_nm, event_designed_id, imprint_id, journey_occurrence_id, 
            link_tracking_id, response_tracking_cd, segment_version_id, task_id
         ) values ( 
            d.test_flg, d.properties_map_doc, d.email_optout_dttm_tz, 
            d.email_optout_dttm, d.load_dttm, d.task_version_id, d.subject_line_txt, 
            d.segment_id, d.recipient_domain_nm, d.program_id, d.optout_type_nm, 
            d.occurrence_id, d.link_tracking_label_txt, d.link_tracking_group_txt, d.journey_id, 
            d.identity_id, d.event_nm, d.event_id, d.context_val, 
            d.channel_user_id, d.audience_id, d.aud_occurrence_id, d.analysis_group_id, 
            d.context_type_nm, d.event_designed_id, d.imprint_id, d.journey_occurrence_id, 
            d.link_tracking_id, d.response_tracking_cd, d.segment_version_id, d.task_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :email_optout_tmp , email_optout , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..email_optout_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..email_optout_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..EMAIL_OPTOUT;
         drop table work.EMAIL_OPTOUT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table EMAIL_OPTOUT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..EMAIL_OPTOUT_DETAILS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..EMAIL_OPTOUT_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..email_optout_details_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..email_optout_details_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=email_optout_details , table_keys=%str(EVENT_ID), out_table=work.email_optout_details );
   data work.email_optout_details_tmp /view=work.email_optout_details_tmp ;
      set work.email_optout_details ;
      if email_action_dttm_tz  ne . then email_action_dttm_tz = tzoneu2s(email_action_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :email_optout_details_tmp , email_optout_details );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..email_optout_details_tmp ;
            set work.email_optout_details_tmp ;
            stop;
         run;
         proc append data=work.email_optout_details_tmp  base=&tmplib..email_optout_details_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..email_optout_details_tmp ;
            set work.email_optout_details_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :email_optout_details_tmp , email_optout_details );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..email_optout_details as b using &tmpdbschema..email_optout_details_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.test_flg = d.test_flg, 
            b.properties_map_doc = d.properties_map_doc, b.email_action_dttm_tz = d.email_action_dttm_tz, 
            b.email_action_dttm = d.email_action_dttm, b.load_dttm = d.load_dttm, 
            b.task_version_id = d.task_version_id, b.subject_line_txt = d.subject_line_txt, 
            b.segment_id = d.segment_id, b.recipient_domain_nm = d.recipient_domain_nm, 
            b.program_id = d.program_id, b.optout_type_nm = d.optout_type_nm, 
            b.occurrence_id = d.occurrence_id, b.journey_occurrence_id = d.journey_occurrence_id, 
            b.imprint_id = d.imprint_id, b.event_nm = d.event_nm, 
            b.event_designed_id = d.event_designed_id, b.email_address = d.email_address, 
            b.context_val = d.context_val, b.audience_id = d.audience_id, 
            b.analysis_group_id = d.analysis_group_id, b.aud_occurrence_id = d.aud_occurrence_id, 
            b.context_type_nm = d.context_type_nm, b.identity_id = d.identity_id, 
            b.journey_id = d.journey_id, b.response_tracking_cd = d.response_tracking_cd, 
            b.segment_version_id = d.segment_version_id, b.task_id = d.task_id
         when not matched then insert ( 
            test_flg, properties_map_doc, email_action_dttm_tz, 
            email_action_dttm, load_dttm, task_version_id, subject_line_txt, 
            segment_id, recipient_domain_nm, program_id, optout_type_nm, 
            occurrence_id, journey_occurrence_id, imprint_id, event_nm, 
            event_designed_id, email_address, context_val, audience_id, 
            analysis_group_id, aud_occurrence_id, context_type_nm, event_id, 
            identity_id, journey_id, response_tracking_cd, segment_version_id, 
            task_id
         ) values ( 
            d.test_flg, d.properties_map_doc, d.email_action_dttm_tz, 
            d.email_action_dttm, d.load_dttm, d.task_version_id, d.subject_line_txt, 
            d.segment_id, d.recipient_domain_nm, d.program_id, d.optout_type_nm, 
            d.occurrence_id, d.journey_occurrence_id, d.imprint_id, d.event_nm, 
            d.event_designed_id, d.email_address, d.context_val, d.audience_id, 
            d.analysis_group_id, d.aud_occurrence_id, d.context_type_nm, d.event_id, 
            d.identity_id, d.journey_id, d.response_tracking_cd, d.segment_version_id, 
            d.task_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :email_optout_details_tmp , email_optout_details , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..email_optout_details_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..email_optout_details_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..EMAIL_OPTOUT_DETAILS;
         drop table work.EMAIL_OPTOUT_DETAILS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table EMAIL_OPTOUT_DETAILS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..EMAIL_REPLY)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..EMAIL_REPLY));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..email_reply_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..email_reply_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=email_reply , table_keys=%str(EVENT_ID), out_table=work.email_reply );
   data work.email_reply_tmp /view=work.email_reply_tmp ;
      set work.email_reply ;
      if email_reply_dttm_tz  ne . then email_reply_dttm_tz = tzoneu2s(email_reply_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :email_reply_tmp , email_reply );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..email_reply_tmp ;
            set work.email_reply_tmp ;
            stop;
         run;
         proc append data=work.email_reply_tmp  base=&tmplib..email_reply_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..email_reply_tmp ;
            set work.email_reply_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :email_reply_tmp , email_reply );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..email_reply as b using &tmpdbschema..email_reply_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.test_flg = d.test_flg, 
            b.properties_map_doc = d.properties_map_doc, b.email_reply_dttm = d.email_reply_dttm, 
            b.email_reply_dttm_tz = d.email_reply_dttm_tz, b.load_dttm = d.load_dttm, 
            b.uri_txt = d.uri_txt, b.task_id = d.task_id, 
            b.subject_line_txt = d.subject_line_txt, b.segment_version_id = d.segment_version_id, 
            b.response_tracking_cd = d.response_tracking_cd, b.occurrence_id = d.occurrence_id, 
            b.journey_occurrence_id = d.journey_occurrence_id, b.imprint_id = d.imprint_id, 
            b.event_nm = d.event_nm, b.event_designed_id = d.event_designed_id, 
            b.context_type_nm = d.context_type_nm, b.audience_id = d.audience_id, 
            b.analysis_group_id = d.analysis_group_id, b.aud_occurrence_id = d.aud_occurrence_id, 
            b.channel_user_id = d.channel_user_id, b.context_val = d.context_val, 
            b.identity_id = d.identity_id, b.journey_id = d.journey_id, 
            b.program_id = d.program_id, b.recipient_domain_nm = d.recipient_domain_nm, 
            b.segment_id = d.segment_id, b.task_version_id = d.task_version_id
         when not matched then insert ( 
            test_flg, properties_map_doc, email_reply_dttm, 
            email_reply_dttm_tz, load_dttm, uri_txt, task_id, 
            subject_line_txt, segment_version_id, response_tracking_cd, occurrence_id, 
            journey_occurrence_id, imprint_id, event_nm, event_designed_id, 
            context_type_nm, audience_id, analysis_group_id, aud_occurrence_id, 
            channel_user_id, context_val, event_id, identity_id, 
            journey_id, program_id, recipient_domain_nm, segment_id, 
            task_version_id
         ) values ( 
            d.test_flg, d.properties_map_doc, d.email_reply_dttm, 
            d.email_reply_dttm_tz, d.load_dttm, d.uri_txt, d.task_id, 
            d.subject_line_txt, d.segment_version_id, d.response_tracking_cd, d.occurrence_id, 
            d.journey_occurrence_id, d.imprint_id, d.event_nm, d.event_designed_id, 
            d.context_type_nm, d.audience_id, d.analysis_group_id, d.aud_occurrence_id, 
            d.channel_user_id, d.context_val, d.event_id, d.identity_id, 
            d.journey_id, d.program_id, d.recipient_domain_nm, d.segment_id, 
            d.task_version_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :email_reply_tmp , email_reply , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..email_reply_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..email_reply_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..EMAIL_REPLY;
         drop table work.EMAIL_REPLY;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table EMAIL_REPLY;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..EMAIL_SEND)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..EMAIL_SEND));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..email_send_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..email_send_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=email_send , table_keys=%str(EVENT_ID), out_table=work.email_send );
   data work.email_send_tmp /view=work.email_send_tmp ;
      set work.email_send ;
      if email_send_dttm_tz  ne . then email_send_dttm_tz = tzoneu2s(email_send_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :email_send_tmp , email_send );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..email_send_tmp ;
            set work.email_send_tmp ;
            stop;
         run;
         proc append data=work.email_send_tmp  base=&tmplib..email_send_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..email_send_tmp ;
            set work.email_send_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :email_send_tmp , email_send );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..email_send as b using &tmpdbschema..email_send_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.test_flg = d.test_flg, 
            b.properties_map_doc = d.properties_map_doc, b.load_dttm = d.load_dttm, 
            b.email_send_dttm_tz = d.email_send_dttm_tz, b.email_send_dttm = d.email_send_dttm, 
            b.task_version_id = d.task_version_id, b.subject_line_txt = d.subject_line_txt, 
            b.segment_id = d.segment_id, b.recipient_domain_nm = d.recipient_domain_nm, 
            b.program_id = d.program_id, b.journey_id = d.journey_id, 
            b.imprint_id = d.imprint_id, b.event_nm = d.event_nm, 
            b.event_designed_id = d.event_designed_id, b.context_type_nm = d.context_type_nm, 
            b.channel_user_id = d.channel_user_id, b.audience_id = d.audience_id, 
            b.analysis_group_id = d.analysis_group_id, b.aud_occurrence_id = d.aud_occurrence_id, 
            b.context_val = d.context_val, b.identity_id = d.identity_id, 
            b.imprint_url_txt = d.imprint_url_txt, b.journey_occurrence_id = d.journey_occurrence_id, 
            b.occurrence_id = d.occurrence_id, b.response_tracking_cd = d.response_tracking_cd, 
            b.segment_version_id = d.segment_version_id, b.task_id = d.task_id
         when not matched then insert ( 
            test_flg, properties_map_doc, load_dttm, 
            email_send_dttm_tz, email_send_dttm, task_version_id, subject_line_txt, 
            segment_id, recipient_domain_nm, program_id, journey_id, 
            imprint_id, event_nm, event_designed_id, context_type_nm, 
            channel_user_id, audience_id, analysis_group_id, aud_occurrence_id, 
            context_val, event_id, identity_id, imprint_url_txt, 
            journey_occurrence_id, occurrence_id, response_tracking_cd, segment_version_id, 
            task_id
         ) values ( 
            d.test_flg, d.properties_map_doc, d.load_dttm, 
            d.email_send_dttm_tz, d.email_send_dttm, d.task_version_id, d.subject_line_txt, 
            d.segment_id, d.recipient_domain_nm, d.program_id, d.journey_id, 
            d.imprint_id, d.event_nm, d.event_designed_id, d.context_type_nm, 
            d.channel_user_id, d.audience_id, d.analysis_group_id, d.aud_occurrence_id, 
            d.context_val, d.event_id, d.identity_id, d.imprint_url_txt, 
            d.journey_occurrence_id, d.occurrence_id, d.response_tracking_cd, d.segment_version_id, 
            d.task_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :email_send_tmp , email_send , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..email_send_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..email_send_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..EMAIL_SEND;
         drop table work.EMAIL_SEND;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table EMAIL_SEND;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..EMAIL_VIEW)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..EMAIL_VIEW));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..email_view_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..email_view_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=email_view , table_keys=%str(EVENT_ID), out_table=work.email_view );
   data work.email_view_tmp /view=work.email_view_tmp ;
      set work.email_view ;
      if email_view_dttm_tz  ne . then email_view_dttm_tz = tzoneu2s(email_view_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :email_view_tmp , email_view );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..email_view_tmp ;
            set work.email_view_tmp ;
            stop;
         run;
         proc append data=work.email_view_tmp  base=&tmplib..email_view_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..email_view_tmp ;
            set work.email_view_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :email_view_tmp , email_view );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..email_view as b using &tmpdbschema..email_view_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.test_flg = d.test_flg, 
            b.properties_map_doc = d.properties_map_doc, b.load_dttm = d.load_dttm, 
            b.email_view_dttm = d.email_view_dttm, b.email_view_dttm_tz = d.email_view_dttm_tz, 
            b.task_version_id = d.task_version_id, b.task_id = d.task_id, 
            b.subject_line_txt = d.subject_line_txt, b.segment_version_id = d.segment_version_id, 
            b.segment_id = d.segment_id, b.response_tracking_cd = d.response_tracking_cd, 
            b.recipient_domain_nm = d.recipient_domain_nm, b.program_id = d.program_id, 
            b.occurrence_id = d.occurrence_id, b.link_tracking_id = d.link_tracking_id, 
            b.link_tracking_group_txt = d.link_tracking_group_txt, b.journey_occurrence_id = d.journey_occurrence_id, 
            b.imprint_id = d.imprint_id, b.event_nm = d.event_nm, 
            b.event_designed_id = d.event_designed_id, b.context_type_nm = d.context_type_nm, 
            b.audience_id = d.audience_id, b.analysis_group_id = d.analysis_group_id, 
            b.aud_occurrence_id = d.aud_occurrence_id, b.channel_user_id = d.channel_user_id, 
            b.context_val = d.context_val, b.identity_id = d.identity_id, 
            b.journey_id = d.journey_id, b.link_tracking_label_txt = d.link_tracking_label_txt
         when not matched then insert ( 
            test_flg, properties_map_doc, load_dttm, 
            email_view_dttm, email_view_dttm_tz, task_version_id, task_id, 
            subject_line_txt, segment_version_id, segment_id, response_tracking_cd, 
            recipient_domain_nm, program_id, occurrence_id, link_tracking_id, 
            link_tracking_group_txt, journey_occurrence_id, imprint_id, event_nm, 
            event_designed_id, context_type_nm, audience_id, analysis_group_id, 
            aud_occurrence_id, channel_user_id, context_val, event_id, 
            identity_id, journey_id, link_tracking_label_txt
         ) values ( 
            d.test_flg, d.properties_map_doc, d.load_dttm, 
            d.email_view_dttm, d.email_view_dttm_tz, d.task_version_id, d.task_id, 
            d.subject_line_txt, d.segment_version_id, d.segment_id, d.response_tracking_cd, 
            d.recipient_domain_nm, d.program_id, d.occurrence_id, d.link_tracking_id, 
            d.link_tracking_group_txt, d.journey_occurrence_id, d.imprint_id, d.event_nm, 
            d.event_designed_id, d.context_type_nm, d.audience_id, d.analysis_group_id, 
            d.aud_occurrence_id, d.channel_user_id, d.context_val, d.event_id, 
            d.identity_id, d.journey_id, d.link_tracking_label_txt  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :email_view_tmp , email_view , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..email_view_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..email_view_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..EMAIL_VIEW;
         drop table work.EMAIL_VIEW;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table EMAIL_VIEW;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..EVENT_ERRORS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..EVENT_ERRORS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..EVENT_ERRORS) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..event_errors  base=&trglib..event_errors (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..EVENT_ERRORS (
            error_dttm_tz, error_dttm, ip_address, 
            event_source_cd, event_id, error_cd, error_txt, 
            payload_txt )
      select error_dttm_tz, error_dttm, ip_address, 
            event_source_cd, event_id, error_cd, error_txt, 
            payload_txt
         from &udmmart..event_errors ;
      quit;
   %end;
   %err_check (Failed to insert into event_errors , event_errors );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..EVENT_ERRORS;
         drop table work.EVENT_ERRORS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table EVENT_ERRORS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..EXTERNAL_EVENT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..EXTERNAL_EVENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..external_event_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..external_event_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=external_event , table_keys=%str(EVENT_ID), out_table=work.external_event );
   data work.external_event_tmp /view=work.external_event_tmp ;
      set work.external_event ;
      if external_event_dttm_tz  ne . then external_event_dttm_tz = tzoneu2s(external_event_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :external_event_tmp , external_event );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..external_event_tmp ;
            set work.external_event_tmp ;
            stop;
         run;
         proc append data=work.external_event_tmp  base=&tmplib..external_event_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..external_event_tmp ;
            set work.external_event_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :external_event_tmp , external_event );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..external_event as b using &tmpdbschema..external_event_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.properties_map_doc = d.properties_map_doc, 
            b.external_event_dttm_tz = d.external_event_dttm_tz, b.load_dttm = d.load_dttm, 
            b.external_event_dttm = d.external_event_dttm, b.response_tracking_cd = d.response_tracking_cd, 
            b.identity_id = d.identity_id, b.event_nm = d.event_nm, 
            b.event_designed_id = d.event_designed_id, b.context_type_nm = d.context_type_nm, 
            b.channel_nm = d.channel_nm, b.channel_user_id = d.channel_user_id, 
            b.context_val = d.context_val
         when not matched then insert ( 
            properties_map_doc, external_event_dttm_tz, load_dttm, 
            external_event_dttm, response_tracking_cd, identity_id, event_nm, 
            event_designed_id, context_type_nm, channel_nm, channel_user_id, 
            context_val, event_id
         ) values ( 
            d.properties_map_doc, d.external_event_dttm_tz, d.load_dttm, 
            d.external_event_dttm, d.response_tracking_cd, d.identity_id, d.event_nm, 
            d.event_designed_id, d.context_type_nm, d.channel_nm, d.channel_user_id, 
            d.context_val, d.event_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :external_event_tmp , external_event , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..external_event_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..external_event_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..EXTERNAL_EVENT;
         drop table work.EXTERNAL_EVENT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table EXTERNAL_EVENT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..FISCAL_CC_BUDGET)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..FISCAL_CC_BUDGET));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..FISCAL_CC_BUDGET) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..fiscal_cc_budget  base=&trglib..fiscal_cc_budget (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..FISCAL_CC_BUDGET (
            cc_obsolete_flg, fin_accnt_obsolete_flg, fp_obsolete_flg, 
            fp_start_dt, fp_end_dt, cc_bdgt_invoiced_amt, cc_bdgt_cmtmnt_outstanding_amt, 
            cc_bdgt_cmtmnt_invoice_amt, cc_bdgt_budget_amt, cc_bdgt_amt, cc_bdgt_committed_amt, 
            cc_bdgt_cmtmnt_overspent_amt, cc_bdgt_direct_invoice_amt, cc_bdgt_cmtmnt_invoice_cnt, last_modified_dttm, 
            created_dttm, load_dttm, gen_ledger_cd, last_modified_usernm, 
            fp_nm, fp_id, fp_desc, fp_cls_ver, 
            fin_accnt_nm, cost_center_id, cc_number, cc_nm, 
            cc_bdgt_budget_desc, cc_desc, cc_owner_usernm, created_by_usernm, 
            fin_accnt_desc )
      select cc_obsolete_flg, fin_accnt_obsolete_flg, fp_obsolete_flg, 
            fp_start_dt, fp_end_dt, cc_bdgt_invoiced_amt, cc_bdgt_cmtmnt_outstanding_amt, 
            cc_bdgt_cmtmnt_invoice_amt, cc_bdgt_budget_amt, cc_bdgt_amt, cc_bdgt_committed_amt, 
            cc_bdgt_cmtmnt_overspent_amt, cc_bdgt_direct_invoice_amt, cc_bdgt_cmtmnt_invoice_cnt, last_modified_dttm, 
            created_dttm, load_dttm, gen_ledger_cd, last_modified_usernm, 
            fp_nm, fp_id, fp_desc, fp_cls_ver, 
            fin_accnt_nm, cost_center_id, cc_number, cc_nm, 
            cc_bdgt_budget_desc, cc_desc, cc_owner_usernm, created_by_usernm, 
            fin_accnt_desc
         from &udmmart..fiscal_cc_budget ;
      quit;
   %end;
   %err_check (Failed to insert into fiscal_cc_budget , fiscal_cc_budget );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..FISCAL_CC_BUDGET;
         drop table work.FISCAL_CC_BUDGET;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table FISCAL_CC_BUDGET;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..FORM_DETAILS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..FORM_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..form_details_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..form_details_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=form_details , table_keys=%str(EVENT_ID), out_table=work.form_details );
   data work.form_details_tmp /view=work.form_details_tmp ;
      set work.form_details ;
      if form_field_detail_dttm_tz  ne . then form_field_detail_dttm_tz = tzoneu2s(form_field_detail_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :form_details_tmp , form_details );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..form_details_tmp ;
            set work.form_details_tmp ;
            stop;
         run;
         proc append data=work.form_details_tmp  base=&tmplib..form_details_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..form_details_tmp ;
            set work.form_details_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :form_details_tmp , form_details );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..form_details as b using &tmpdbschema..form_details_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.submit_flg = d.submit_flg, 
            b.change_index_no = d.change_index_no, b.attempt_index_cnt = d.attempt_index_cnt, 
            b.load_dttm = d.load_dttm, b.form_field_detail_dttm_tz = d.form_field_detail_dttm_tz, 
            b.form_field_detail_dttm = d.form_field_detail_dttm, b.visit_id = d.visit_id, 
            b.form_field_nm = d.form_field_nm, b.event_source_cd = d.event_source_cd, 
            b.detail_id = d.detail_id, b.attempt_status_cd = d.attempt_status_cd, 
            b.form_field_value = d.form_field_value, b.form_nm = d.form_nm, 
            b.session_id_hex = d.session_id_hex, b.detail_id_hex = d.detail_id_hex, 
            b.event_key_cd = d.event_key_cd, b.form_field_id = d.form_field_id, 
            b.identity_id = d.identity_id, b.session_id = d.session_id, 
            b.visit_id_hex = d.visit_id_hex
         when not matched then insert ( 
            submit_flg, change_index_no, attempt_index_cnt, 
            load_dttm, form_field_detail_dttm_tz, form_field_detail_dttm, visit_id, 
            form_field_nm, event_source_cd, detail_id, attempt_status_cd, 
            event_id, form_field_value, form_nm, session_id_hex, 
            detail_id_hex, event_key_cd, form_field_id, identity_id, 
            session_id, visit_id_hex
         ) values ( 
            d.submit_flg, d.change_index_no, d.attempt_index_cnt, 
            d.load_dttm, d.form_field_detail_dttm_tz, d.form_field_detail_dttm, d.visit_id, 
            d.form_field_nm, d.event_source_cd, d.detail_id, d.attempt_status_cd, 
            d.event_id, d.form_field_value, d.form_nm, d.session_id_hex, 
            d.detail_id_hex, d.event_key_cd, d.form_field_id, d.identity_id, 
            d.session_id, d.visit_id_hex  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :form_details_tmp , form_details , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..form_details_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..form_details_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..FORM_DETAILS;
         drop table work.FORM_DETAILS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table FORM_DETAILS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..IDENTITY_ATTRIBUTES)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..IDENTITY_ATTRIBUTES));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..identity_attributes_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..identity_attributes_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=identity_attributes , table_keys=%str(ENTRYTIME,IDENTIFIER_TYPE_ID,USER_IDENTIFIER_VAL), out_table=work.identity_attributes );
   data work.identity_attributes_tmp /view=work.identity_attributes_tmp ;
      set work.identity_attributes ;
   run;
   %err_check (Failed to add time zone adaptation :identity_attributes_tmp , identity_attributes );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..identity_attributes_tmp ;
            set work.identity_attributes_tmp ;
            stop;
         run;
         proc append data=work.identity_attributes_tmp  base=&tmplib..identity_attributes_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..identity_attributes_tmp ;
            set work.identity_attributes_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :identity_attributes_tmp , identity_attributes );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..identity_attributes as b using &tmpdbschema..identity_attributes_tmp as d on( 
            b.entrytime = d.entrytime and 
            b.user_identifier_val = d.user_identifier_val and b.identifier_type_id = d.identifier_type_id )
         when matched then  
         update set 
            b.processed_dttm = d.processed_dttm, 
            b.identity_id = d.identity_id
         when not matched then insert ( 
            processed_dttm, entrytime, identity_id, 
            user_identifier_val, identifier_type_id
         ) values ( 
            d.processed_dttm, d.entrytime, d.identity_id, 
            d.user_identifier_val, d.identifier_type_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :identity_attributes_tmp , identity_attributes , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..identity_attributes_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..identity_attributes_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..IDENTITY_ATTRIBUTES;
         drop table work.IDENTITY_ATTRIBUTES;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table IDENTITY_ATTRIBUTES;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..IDENTITY_MAP)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..IDENTITY_MAP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..identity_map_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..identity_map_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=identity_map , table_keys=%str(SOURCE_IDENTITY_ID), out_table=work.identity_map );
   data work.identity_map_tmp /view=work.identity_map_tmp ;
      set work.identity_map ;
   run;
   %err_check (Failed to add time zone adaptation :identity_map_tmp , identity_map );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..identity_map_tmp ;
            set work.identity_map_tmp ;
            stop;
         run;
         proc append data=work.identity_map_tmp  base=&tmplib..identity_map_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..identity_map_tmp ;
            set work.identity_map_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :identity_map_tmp , identity_map );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..identity_map as b using &tmpdbschema..identity_map_tmp as d on( 
            b.source_identity_id = d.source_identity_id )
         when matched then  
         update set 
            b.processed_dttm = d.processed_dttm, 
            b.entrytime = d.entrytime, b.target_identity_id = d.target_identity_id
         when not matched then insert ( 
            processed_dttm, entrytime, target_identity_id, 
            source_identity_id
         ) values ( 
            d.processed_dttm, d.entrytime, d.target_identity_id, 
            d.source_identity_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :identity_map_tmp , identity_map , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..identity_map_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..identity_map_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..IDENTITY_MAP;
         drop table work.IDENTITY_MAP;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table IDENTITY_MAP;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..IMPRESSION_DELIVERED)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..IMPRESSION_DELIVERED));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..impression_delivered_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..impression_delivered_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=impression_delivered , table_keys=%str(EVENT_ID), out_table=work.impression_delivered );
   data work.impression_delivered_tmp /view=work.impression_delivered_tmp ;
      set work.impression_delivered ;
      if impression_delivered_dttm_tz  ne . then impression_delivered_dttm_tz = tzoneu2s(impression_delivered_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :impression_delivered_tmp , impression_delivered );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..impression_delivered_tmp ;
            set work.impression_delivered_tmp ;
            stop;
         run;
         proc append data=work.impression_delivered_tmp  base=&tmplib..impression_delivered_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..impression_delivered_tmp ;
            set work.impression_delivered_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :impression_delivered_tmp , impression_delivered );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..impression_delivered as b using &tmpdbschema..impression_delivered_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.control_group_flg = d.control_group_flg, 
            b.product_qty_no = d.product_qty_no, b.properties_map_doc = d.properties_map_doc, 
            b.impression_delivered_dttm_tz = d.impression_delivered_dttm_tz, b.impression_delivered_dttm = d.impression_delivered_dttm, 
            b.load_dttm = d.load_dttm, b.spot_id = d.spot_id, 
            b.response_tracking_cd = d.response_tracking_cd, b.rec_group_id = d.rec_group_id, 
            b.product_nm = d.product_nm, b.message_id = d.message_id, 
            b.event_nm = d.event_nm, b.detail_id_hex = d.detail_id_hex, 
            b.context_val = d.context_val, b.audience_id = d.audience_id, 
            b.channel_user_id = d.channel_user_id, b.creative_id = d.creative_id, 
            b.identity_id = d.identity_id, b.journey_occurrence_id = d.journey_occurrence_id, 
            b.message_version_id = d.message_version_id, b.mobile_app_id = d.mobile_app_id, 
            b.product_sku_no = d.product_sku_no, b.reserved_1_txt = d.reserved_1_txt, 
            b.segment_version_id = d.segment_version_id, b.task_version_id = d.task_version_id, 
            b.visit_id_hex = d.visit_id_hex, b.aud_occurrence_id = d.aud_occurrence_id, 
            b.channel_nm = d.channel_nm, b.context_type_nm = d.context_type_nm, 
            b.creative_version_id = d.creative_version_id, b.event_designed_id = d.event_designed_id, 
            b.event_key_cd = d.event_key_cd, b.event_source_cd = d.event_source_cd, 
            b.journey_id = d.journey_id, b.product_id = d.product_id, 
            b.request_id = d.request_id, b.reserved_2_txt = d.reserved_2_txt, 
            b.segment_id = d.segment_id, b.session_id_hex = d.session_id_hex, 
            b.task_id = d.task_id
         when not matched then insert ( 
            control_group_flg, product_qty_no, properties_map_doc, 
            impression_delivered_dttm_tz, impression_delivered_dttm, load_dttm, spot_id, 
            response_tracking_cd, rec_group_id, product_nm, message_id, 
            event_nm, detail_id_hex, context_val, audience_id, 
            channel_user_id, creative_id, event_id, identity_id, 
            journey_occurrence_id, message_version_id, mobile_app_id, product_sku_no, 
            reserved_1_txt, segment_version_id, task_version_id, visit_id_hex, 
            aud_occurrence_id, channel_nm, context_type_nm, creative_version_id, 
            event_designed_id, event_key_cd, event_source_cd, journey_id, 
            product_id, request_id, reserved_2_txt, segment_id, 
            session_id_hex, task_id
         ) values ( 
            d.control_group_flg, d.product_qty_no, d.properties_map_doc, 
            d.impression_delivered_dttm_tz, d.impression_delivered_dttm, d.load_dttm, d.spot_id, 
            d.response_tracking_cd, d.rec_group_id, d.product_nm, d.message_id, 
            d.event_nm, d.detail_id_hex, d.context_val, d.audience_id, 
            d.channel_user_id, d.creative_id, d.event_id, d.identity_id, 
            d.journey_occurrence_id, d.message_version_id, d.mobile_app_id, d.product_sku_no, 
            d.reserved_1_txt, d.segment_version_id, d.task_version_id, d.visit_id_hex, 
            d.aud_occurrence_id, d.channel_nm, d.context_type_nm, d.creative_version_id, 
            d.event_designed_id, d.event_key_cd, d.event_source_cd, d.journey_id, 
            d.product_id, d.request_id, d.reserved_2_txt, d.segment_id, 
            d.session_id_hex, d.task_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :impression_delivered_tmp , impression_delivered , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..impression_delivered_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..impression_delivered_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..IMPRESSION_DELIVERED;
         drop table work.IMPRESSION_DELIVERED;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table IMPRESSION_DELIVERED;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..IMPRESSION_SPOT_VIEWABLE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..IMPRESSION_SPOT_VIEWABLE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..impression_spot_viewable_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..impression_spot_viewable_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=impression_spot_viewable , table_keys=%str(EVENT_ID), out_table=work.impression_spot_viewable );
   data work.impression_spot_viewable_tmp /view=work.impression_spot_viewable_tmp ;
      set work.impression_spot_viewable ;
      if impression_viewable_dttm_tz  ne . then impression_viewable_dttm_tz = tzoneu2s(impression_viewable_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :impression_spot_viewable_tmp , impression_spot_viewable );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..impression_spot_viewable_tmp ;
            set work.impression_spot_viewable_tmp ;
            stop;
         run;
         proc append data=work.impression_spot_viewable_tmp  base=&tmplib..impression_spot_viewable_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..impression_spot_viewable_tmp ;
            set work.impression_spot_viewable_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :impression_spot_viewable_tmp , impression_spot_viewable );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..impression_spot_viewable as b using &tmpdbschema..impression_spot_viewable_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.control_group_flg = d.control_group_flg, 
            b.product_qty_no = d.product_qty_no, b.properties_map_doc = d.properties_map_doc, 
            b.impression_viewable_dttm_tz = d.impression_viewable_dttm_tz, b.load_dttm = d.load_dttm, 
            b.impression_viewable_dttm = d.impression_viewable_dttm, b.visit_id_hex = d.visit_id_hex, 
            b.session_id_hex = d.session_id_hex, b.reserved_2_txt = d.reserved_2_txt, 
            b.product_id = d.product_id, b.message_id = d.message_id, 
            b.identity_id = d.identity_id, b.creative_id = d.creative_id, 
            b.channel_user_id = d.channel_user_id, b.analysis_group_id = d.analysis_group_id, 
            b.audience_id = d.audience_id, b.context_val = d.context_val, 
            b.detail_id_hex = d.detail_id_hex, b.event_nm = d.event_nm, 
            b.event_source_cd = d.event_source_cd, b.mobile_app_id = d.mobile_app_id, 
            b.rec_group_id = d.rec_group_id, b.request_id = d.request_id, 
            b.segment_id = d.segment_id, b.task_id = d.task_id, 
            b.aud_occurrence_id = d.aud_occurrence_id, b.channel_nm = d.channel_nm, 
            b.context_type_nm = d.context_type_nm, b.creative_version_id = d.creative_version_id, 
            b.event_designed_id = d.event_designed_id, b.event_key_cd = d.event_key_cd, 
            b.message_version_id = d.message_version_id, b.occurrence_id = d.occurrence_id, 
            b.product_nm = d.product_nm, b.product_sku_no = d.product_sku_no, 
            b.reserved_1_txt = d.reserved_1_txt, b.response_tracking_cd = d.response_tracking_cd, 
            b.segment_version_id = d.segment_version_id, b.spot_id = d.spot_id, 
            b.task_version_id = d.task_version_id
         when not matched then insert ( 
            control_group_flg, product_qty_no, properties_map_doc, 
            impression_viewable_dttm_tz, load_dttm, impression_viewable_dttm, visit_id_hex, 
            session_id_hex, reserved_2_txt, product_id, message_id, 
            identity_id, event_id, creative_id, channel_user_id, 
            analysis_group_id, audience_id, context_val, detail_id_hex, 
            event_nm, event_source_cd, mobile_app_id, rec_group_id, 
            request_id, segment_id, task_id, aud_occurrence_id, 
            channel_nm, context_type_nm, creative_version_id, event_designed_id, 
            event_key_cd, message_version_id, occurrence_id, product_nm, 
            product_sku_no, reserved_1_txt, response_tracking_cd, segment_version_id, 
            spot_id, task_version_id
         ) values ( 
            d.control_group_flg, d.product_qty_no, d.properties_map_doc, 
            d.impression_viewable_dttm_tz, d.load_dttm, d.impression_viewable_dttm, d.visit_id_hex, 
            d.session_id_hex, d.reserved_2_txt, d.product_id, d.message_id, 
            d.identity_id, d.event_id, d.creative_id, d.channel_user_id, 
            d.analysis_group_id, d.audience_id, d.context_val, d.detail_id_hex, 
            d.event_nm, d.event_source_cd, d.mobile_app_id, d.rec_group_id, 
            d.request_id, d.segment_id, d.task_id, d.aud_occurrence_id, 
            d.channel_nm, d.context_type_nm, d.creative_version_id, d.event_designed_id, 
            d.event_key_cd, d.message_version_id, d.occurrence_id, d.product_nm, 
            d.product_sku_no, d.reserved_1_txt, d.response_tracking_cd, d.segment_version_id, 
            d.spot_id, d.task_version_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :impression_spot_viewable_tmp , impression_spot_viewable , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..impression_spot_viewable_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..impression_spot_viewable_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..IMPRESSION_SPOT_VIEWABLE;
         drop table work.IMPRESSION_SPOT_VIEWABLE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table IMPRESSION_SPOT_VIEWABLE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..INVOICE_DETAILS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..INVOICE_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..INVOICE_DETAILS) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..invoice_details  base=&trglib..invoice_details (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..INVOICE_DETAILS (
            vendor_obsolete_flg, invoice_amt, vendor_amt, 
            reconcile_amt, last_modified_dttm, invoice_reconciled_dttm, payment_dttm, 
            load_dttm, created_dttm, invoice_created_dttm, vendor_nm, 
            planning_id, last_modified_usernm, invoice_number, cmtmnt_nm, 
            invoice_id, invoice_status, vendor_currency_cd, vendor_desc, 
            cmtmnt_id, created_by_usernm, invoice_desc, invoice_nm, 
            plan_currency_cd, planning_nm, reconcile_note, vendor_id, 
            vendor_number )
      select vendor_obsolete_flg, invoice_amt, vendor_amt, 
            reconcile_amt, last_modified_dttm, invoice_reconciled_dttm, payment_dttm, 
            load_dttm, created_dttm, invoice_created_dttm, vendor_nm, 
            planning_id, last_modified_usernm, invoice_number, cmtmnt_nm, 
            invoice_id, invoice_status, vendor_currency_cd, vendor_desc, 
            cmtmnt_id, created_by_usernm, invoice_desc, invoice_nm, 
            plan_currency_cd, planning_nm, reconcile_note, vendor_id, 
            vendor_number
         from &udmmart..invoice_details ;
      quit;
   %end;
   %err_check (Failed to insert into invoice_details , invoice_details );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..INVOICE_DETAILS;
         drop table work.INVOICE_DETAILS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table INVOICE_DETAILS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..INVOICE_LINE_ITEMS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..INVOICE_LINE_ITEMS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..INVOICE_LINE_ITEMS) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..invoice_line_items  base=&trglib..invoice_line_items (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..INVOICE_LINE_ITEMS (
            item_alloc_unit, item_vend_alloc_unit, item_qty, 
            vendor_obsolete_flg, cc_available_amt, vendor_amt, item_alloc_amt, 
            item_rate, reconcile_amt, item_vend_alloc_amt, invoice_amt, 
            cc_recon_alloc_amt, cc_allocated_amt, item_number, payment_dttm, 
            load_dttm, invoice_created_dttm, invoice_reconciled_dttm, last_modified_dttm, 
            created_dttm, vendor_number, vendor_desc, vendor_currency_cd, 
            reconcile_note, planning_nm, item_nm, invoice_nm, 
            invoice_id, invoice_desc, fin_acc_nm, cost_center_id, 
            cc_nm, ccat_nm, cmtmnt_id, plan_currency_cd, 
            vendor_id, cc_desc, cc_owner_usernm, cmtmnt_nm, 
            created_by_usernm, fin_acc_ccat_nm, gen_ledger_cd, invoice_number, 
            invoice_status, last_modified_usernm, planning_id, vendor_nm )
      select item_alloc_unit, item_vend_alloc_unit, item_qty, 
            vendor_obsolete_flg, cc_available_amt, vendor_amt, item_alloc_amt, 
            item_rate, reconcile_amt, item_vend_alloc_amt, invoice_amt, 
            cc_recon_alloc_amt, cc_allocated_amt, item_number, payment_dttm, 
            load_dttm, invoice_created_dttm, invoice_reconciled_dttm, last_modified_dttm, 
            created_dttm, vendor_number, vendor_desc, vendor_currency_cd, 
            reconcile_note, planning_nm, item_nm, invoice_nm, 
            invoice_id, invoice_desc, fin_acc_nm, cost_center_id, 
            cc_nm, ccat_nm, cmtmnt_id, plan_currency_cd, 
            vendor_id, cc_desc, cc_owner_usernm, cmtmnt_nm, 
            created_by_usernm, fin_acc_ccat_nm, gen_ledger_cd, invoice_number, 
            invoice_status, last_modified_usernm, planning_id, vendor_nm
         from &udmmart..invoice_line_items ;
      quit;
   %end;
   %err_check (Failed to insert into invoice_line_items , invoice_line_items );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..INVOICE_LINE_ITEMS;
         drop table work.INVOICE_LINE_ITEMS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table INVOICE_LINE_ITEMS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..INVOICE_LINE_ITEMS_CCBDGT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..INVOICE_LINE_ITEMS_CCBDGT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..INVOICE_LINE_ITEMS_CCBDGT) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..invoice_line_items_ccbdgt  base=&trglib..invoice_line_items_ccbdgt (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..INVOICE_LINE_ITEMS_CCBDGT (
            cc_obsolete_flg, vendor_obsolete_flg, fp_obsolete_flg, 
            fp_start_dt, fp_end_dt, cc_bdgt_cmtmnt_overspent_amt, reconcile_amt, 
            vendor_amt, item_alloc_amt, cc_bdgt_cmtmnt_outstanding_amt, invoice_amt, 
            item_rate, cc_recon_alloc_amt, cc_bdgt_direct_invoice_amt, cc_available_amt, 
            cc_bdgt_invoiced_amt, item_vend_alloc_amt, cc_bdgt_committed_amt, cc_bdgt_budget_amt, 
            cc_bdgt_cmtmnt_invoice_amt, cc_bdgt_amt, cc_allocated_amt, item_qty, 
            item_number, item_vend_alloc_unit, item_alloc_unit, cc_bdgt_cmtmnt_invoice_cnt, 
            created_dttm, invoice_created_dttm, invoice_reconciled_dttm, last_modified_dttm, 
            payment_dttm, load_dttm, vendor_id, reconcile_note, 
            planning_nm, plan_currency_cd, invoice_nm, fp_nm, 
            fp_cls_ver, created_by_usernm, ccat_nm, cc_number, 
            cc_bdgt_budget_desc, cc_desc, cc_owner_usernm, cmtmnt_nm, 
            fin_acc_ccat_nm, invoice_desc, item_nm, vendor_currency_cd, 
            vendor_number, cc_nm, cmtmnt_id, cost_center_id, 
            fin_acc_nm, fp_desc, fp_id, gen_ledger_cd, 
            invoice_id, invoice_number, invoice_status, last_modified_usernm, 
            planning_id, vendor_desc, vendor_nm )
      select cc_obsolete_flg, vendor_obsolete_flg, fp_obsolete_flg, 
            fp_start_dt, fp_end_dt, cc_bdgt_cmtmnt_overspent_amt, reconcile_amt, 
            vendor_amt, item_alloc_amt, cc_bdgt_cmtmnt_outstanding_amt, invoice_amt, 
            item_rate, cc_recon_alloc_amt, cc_bdgt_direct_invoice_amt, cc_available_amt, 
            cc_bdgt_invoiced_amt, item_vend_alloc_amt, cc_bdgt_committed_amt, cc_bdgt_budget_amt, 
            cc_bdgt_cmtmnt_invoice_amt, cc_bdgt_amt, cc_allocated_amt, item_qty, 
            item_number, item_vend_alloc_unit, item_alloc_unit, cc_bdgt_cmtmnt_invoice_cnt, 
            created_dttm, invoice_created_dttm, invoice_reconciled_dttm, last_modified_dttm, 
            payment_dttm, load_dttm, vendor_id, reconcile_note, 
            planning_nm, plan_currency_cd, invoice_nm, fp_nm, 
            fp_cls_ver, created_by_usernm, ccat_nm, cc_number, 
            cc_bdgt_budget_desc, cc_desc, cc_owner_usernm, cmtmnt_nm, 
            fin_acc_ccat_nm, invoice_desc, item_nm, vendor_currency_cd, 
            vendor_number, cc_nm, cmtmnt_id, cost_center_id, 
            fin_acc_nm, fp_desc, fp_id, gen_ledger_cd, 
            invoice_id, invoice_number, invoice_status, last_modified_usernm, 
            planning_id, vendor_desc, vendor_nm
         from &udmmart..invoice_line_items_ccbdgt ;
      quit;
   %end;
   %err_check (Failed to insert into invoice_line_items_ccbdgt , invoice_line_items_ccbdgt );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..INVOICE_LINE_ITEMS_CCBDGT;
         drop table work.INVOICE_LINE_ITEMS_CCBDGT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table INVOICE_LINE_ITEMS_CCBDGT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..IN_APP_FAILED)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..IN_APP_FAILED));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..in_app_failed_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..in_app_failed_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=in_app_failed , table_keys=%str(EVENT_ID), out_table=work.in_app_failed );
   data work.in_app_failed_tmp /view=work.in_app_failed_tmp ;
      set work.in_app_failed ;
      if in_app_failed_dttm_tz  ne . then in_app_failed_dttm_tz = tzoneu2s(in_app_failed_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :in_app_failed_tmp , in_app_failed );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..in_app_failed_tmp ;
            set work.in_app_failed_tmp ;
            stop;
         run;
         proc append data=work.in_app_failed_tmp  base=&tmplib..in_app_failed_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..in_app_failed_tmp ;
            set work.in_app_failed_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :in_app_failed_tmp , in_app_failed );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..in_app_failed as b using &tmpdbschema..in_app_failed_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.properties_map_doc = d.properties_map_doc, 
            b.in_app_failed_dttm = d.in_app_failed_dttm, b.in_app_failed_dttm_tz = d.in_app_failed_dttm_tz, 
            b.load_dttm = d.load_dttm, b.task_version_id = d.task_version_id, 
            b.segment_id = d.segment_id, b.message_id = d.message_id, 
            b.identity_id = d.identity_id, b.error_message_txt = d.error_message_txt, 
            b.context_val = d.context_val, b.channel_user_id = d.channel_user_id, 
            b.context_type_nm = d.context_type_nm, b.creative_version_id = d.creative_version_id, 
            b.mobile_app_id = d.mobile_app_id, b.reserved_2_txt = d.reserved_2_txt, 
            b.spot_id = d.spot_id, b.channel_nm = d.channel_nm, 
            b.creative_id = d.creative_id, b.error_cd = d.error_cd, 
            b.event_designed_id = d.event_designed_id, b.event_nm = d.event_nm, 
            b.message_version_id = d.message_version_id, b.occurrence_id = d.occurrence_id, 
            b.reserved_1_txt = d.reserved_1_txt, b.response_tracking_cd = d.response_tracking_cd, 
            b.segment_version_id = d.segment_version_id, b.task_id = d.task_id
         when not matched then insert ( 
            properties_map_doc, in_app_failed_dttm, in_app_failed_dttm_tz, 
            load_dttm, task_version_id, segment_id, message_id, 
            identity_id, error_message_txt, context_val, channel_user_id, 
            context_type_nm, creative_version_id, event_id, mobile_app_id, 
            reserved_2_txt, spot_id, channel_nm, creative_id, 
            error_cd, event_designed_id, event_nm, message_version_id, 
            occurrence_id, reserved_1_txt, response_tracking_cd, segment_version_id, 
            task_id
         ) values ( 
            d.properties_map_doc, d.in_app_failed_dttm, d.in_app_failed_dttm_tz, 
            d.load_dttm, d.task_version_id, d.segment_id, d.message_id, 
            d.identity_id, d.error_message_txt, d.context_val, d.channel_user_id, 
            d.context_type_nm, d.creative_version_id, d.event_id, d.mobile_app_id, 
            d.reserved_2_txt, d.spot_id, d.channel_nm, d.creative_id, 
            d.error_cd, d.event_designed_id, d.event_nm, d.message_version_id, 
            d.occurrence_id, d.reserved_1_txt, d.response_tracking_cd, d.segment_version_id, 
            d.task_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :in_app_failed_tmp , in_app_failed , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..in_app_failed_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..in_app_failed_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..IN_APP_FAILED;
         drop table work.IN_APP_FAILED;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table IN_APP_FAILED;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..IN_APP_MESSAGE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..IN_APP_MESSAGE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..in_app_message_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..in_app_message_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=in_app_message , table_keys=%str(EVENT_ID), out_table=work.in_app_message );
   data work.in_app_message_tmp /view=work.in_app_message_tmp ;
      set work.in_app_message ;
      if in_app_action_dttm_tz  ne . then in_app_action_dttm_tz = tzoneu2s(in_app_action_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :in_app_message_tmp , in_app_message );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..in_app_message_tmp ;
            set work.in_app_message_tmp ;
            stop;
         run;
         proc append data=work.in_app_message_tmp  base=&tmplib..in_app_message_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..in_app_message_tmp ;
            set work.in_app_message_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :in_app_message_tmp , in_app_message );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..in_app_message as b using &tmpdbschema..in_app_message_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.properties_map_doc = d.properties_map_doc, 
            b.in_app_action_dttm_tz = d.in_app_action_dttm_tz, b.load_dttm = d.load_dttm, 
            b.in_app_action_dttm = d.in_app_action_dttm, b.segment_version_id = d.segment_version_id, 
            b.reserved_2_txt = d.reserved_2_txt, b.mobile_app_id = d.mobile_app_id, 
            b.context_val = d.context_val, b.channel_user_id = d.channel_user_id, 
            b.creative_version_id = d.creative_version_id, b.identity_id = d.identity_id, 
            b.message_id = d.message_id, b.response_tracking_cd = d.response_tracking_cd, 
            b.task_id = d.task_id, b.channel_nm = d.channel_nm, 
            b.context_type_nm = d.context_type_nm, b.creative_id = d.creative_id, 
            b.event_designed_id = d.event_designed_id, b.event_nm = d.event_nm, 
            b.message_version_id = d.message_version_id, b.occurrence_id = d.occurrence_id, 
            b.reserved_1_txt = d.reserved_1_txt, b.reserved_3_txt = d.reserved_3_txt, 
            b.segment_id = d.segment_id, b.spot_id = d.spot_id, 
            b.task_version_id = d.task_version_id
         when not matched then insert ( 
            properties_map_doc, in_app_action_dttm_tz, load_dttm, 
            in_app_action_dttm, segment_version_id, reserved_2_txt, mobile_app_id, 
            event_id, context_val, channel_user_id, creative_version_id, 
            identity_id, message_id, response_tracking_cd, task_id, 
            channel_nm, context_type_nm, creative_id, event_designed_id, 
            event_nm, message_version_id, occurrence_id, reserved_1_txt, 
            reserved_3_txt, segment_id, spot_id, task_version_id
         ) values ( 
            d.properties_map_doc, d.in_app_action_dttm_tz, d.load_dttm, 
            d.in_app_action_dttm, d.segment_version_id, d.reserved_2_txt, d.mobile_app_id, 
            d.event_id, d.context_val, d.channel_user_id, d.creative_version_id, 
            d.identity_id, d.message_id, d.response_tracking_cd, d.task_id, 
            d.channel_nm, d.context_type_nm, d.creative_id, d.event_designed_id, 
            d.event_nm, d.message_version_id, d.occurrence_id, d.reserved_1_txt, 
            d.reserved_3_txt, d.segment_id, d.spot_id, d.task_version_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :in_app_message_tmp , in_app_message , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..in_app_message_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..in_app_message_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..IN_APP_MESSAGE;
         drop table work.IN_APP_MESSAGE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table IN_APP_MESSAGE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..IN_APP_SEND)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..IN_APP_SEND));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..in_app_send_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..in_app_send_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=in_app_send , table_keys=%str(EVENT_ID), out_table=work.in_app_send );
   data work.in_app_send_tmp /view=work.in_app_send_tmp ;
      set work.in_app_send ;
      if in_app_send_dttm_tz  ne . then in_app_send_dttm_tz = tzoneu2s(in_app_send_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :in_app_send_tmp , in_app_send );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..in_app_send_tmp ;
            set work.in_app_send_tmp ;
            stop;
         run;
         proc append data=work.in_app_send_tmp  base=&tmplib..in_app_send_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..in_app_send_tmp ;
            set work.in_app_send_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :in_app_send_tmp , in_app_send );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..in_app_send as b using &tmpdbschema..in_app_send_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.properties_map_doc = d.properties_map_doc, 
            b.load_dttm = d.load_dttm, b.in_app_send_dttm_tz = d.in_app_send_dttm_tz, 
            b.in_app_send_dttm = d.in_app_send_dttm, b.task_id = d.task_id, 
            b.response_tracking_cd = d.response_tracking_cd, b.occurrence_id = d.occurrence_id, 
            b.message_id = d.message_id, b.event_nm = d.event_nm, 
            b.creative_id = d.creative_id, b.channel_nm = d.channel_nm, 
            b.context_type_nm = d.context_type_nm, b.creative_version_id = d.creative_version_id, 
            b.event_designed_id = d.event_designed_id, b.message_version_id = d.message_version_id, 
            b.reserved_1_txt = d.reserved_1_txt, b.segment_version_id = d.segment_version_id, 
            b.channel_user_id = d.channel_user_id, b.context_val = d.context_val, 
            b.identity_id = d.identity_id, b.mobile_app_id = d.mobile_app_id, 
            b.reserved_2_txt = d.reserved_2_txt, b.segment_id = d.segment_id, 
            b.spot_id = d.spot_id, b.task_version_id = d.task_version_id
         when not matched then insert ( 
            properties_map_doc, load_dttm, in_app_send_dttm_tz, 
            in_app_send_dttm, task_id, response_tracking_cd, occurrence_id, 
            message_id, event_nm, creative_id, channel_nm, 
            context_type_nm, creative_version_id, event_designed_id, message_version_id, 
            reserved_1_txt, segment_version_id, channel_user_id, context_val, 
            event_id, identity_id, mobile_app_id, reserved_2_txt, 
            segment_id, spot_id, task_version_id
         ) values ( 
            d.properties_map_doc, d.load_dttm, d.in_app_send_dttm_tz, 
            d.in_app_send_dttm, d.task_id, d.response_tracking_cd, d.occurrence_id, 
            d.message_id, d.event_nm, d.creative_id, d.channel_nm, 
            d.context_type_nm, d.creative_version_id, d.event_designed_id, d.message_version_id, 
            d.reserved_1_txt, d.segment_version_id, d.channel_user_id, d.context_val, 
            d.event_id, d.identity_id, d.mobile_app_id, d.reserved_2_txt, 
            d.segment_id, d.spot_id, d.task_version_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :in_app_send_tmp , in_app_send , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..in_app_send_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..in_app_send_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..IN_APP_SEND;
         drop table work.IN_APP_SEND;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table IN_APP_SEND;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..IN_APP_TARGETING_REQUEST)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..IN_APP_TARGETING_REQUEST));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..in_app_targeting_request_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..in_app_targeting_request_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=in_app_targeting_request , table_keys=%str(EVENT_ID), out_table=work.in_app_targeting_request );
   data work.in_app_targeting_request_tmp /view=work.in_app_targeting_request_tmp ;
      set work.in_app_targeting_request ;
      if in_app_tgt_request_dttm_tz  ne . then in_app_tgt_request_dttm_tz = tzoneu2s(in_app_tgt_request_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :in_app_targeting_request_tmp , in_app_targeting_request );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..in_app_targeting_request_tmp ;
            set work.in_app_targeting_request_tmp ;
            stop;
         run;
         proc append data=work.in_app_targeting_request_tmp  base=&tmplib..in_app_targeting_request_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..in_app_targeting_request_tmp ;
            set work.in_app_targeting_request_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :in_app_targeting_request_tmp , in_app_targeting_request );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..in_app_targeting_request as b using &tmpdbschema..in_app_targeting_request_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.eligibility_flg = d.eligibility_flg, 
            b.in_app_tgt_request_dttm = d.in_app_tgt_request_dttm, b.load_dttm = d.load_dttm, 
            b.in_app_tgt_request_dttm_tz = d.in_app_tgt_request_dttm_tz, b.context_type_nm = d.context_type_nm, 
            b.channel_nm = d.channel_nm, b.event_designed_id = d.event_designed_id, 
            b.identity_id = d.identity_id, b.mobile_app_id = d.mobile_app_id, 
            b.channel_user_id = d.channel_user_id, b.context_val = d.context_val, 
            b.event_nm = d.event_nm
         when not matched then insert ( 
            eligibility_flg, in_app_tgt_request_dttm, load_dttm, 
            in_app_tgt_request_dttm_tz, event_id, context_type_nm, channel_nm, 
            event_designed_id, identity_id, mobile_app_id, channel_user_id, 
            context_val, event_nm
         ) values ( 
            d.eligibility_flg, d.in_app_tgt_request_dttm, d.load_dttm, 
            d.in_app_tgt_request_dttm_tz, d.event_id, d.context_type_nm, d.channel_nm, 
            d.event_designed_id, d.identity_id, d.mobile_app_id, d.channel_user_id, 
            d.context_val, d.event_nm  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :in_app_targeting_request_tmp , in_app_targeting_request , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..in_app_targeting_request_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..in_app_targeting_request_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..IN_APP_TARGETING_REQUEST;
         drop table work.IN_APP_TARGETING_REQUEST;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table IN_APP_TARGETING_REQUEST;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..JOURNEY_ENTRY)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..JOURNEY_ENTRY));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..journey_entry_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..journey_entry_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=journey_entry , table_keys=%str(EVENT_ID), out_table=work.journey_entry );
   data work.journey_entry_tmp /view=work.journey_entry_tmp ;
      set work.journey_entry ;
      if entry_dttm_tz  ne . then entry_dttm_tz = tzoneu2s(entry_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :journey_entry_tmp , journey_entry );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..journey_entry_tmp ;
            set work.journey_entry_tmp ;
            stop;
         run;
         proc append data=work.journey_entry_tmp  base=&tmplib..journey_entry_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..journey_entry_tmp ;
            set work.journey_entry_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :journey_entry_tmp , journey_entry );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..journey_entry as b using &tmpdbschema..journey_entry_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.entry_dttm = d.entry_dttm, 
            b.entry_dttm_tz = d.entry_dttm_tz, b.load_dttm = d.load_dttm, 
            b.journey_occurrence_id = d.journey_occurrence_id, b.identity_id = d.identity_id, 
            b.aud_occurrence_id = d.aud_occurrence_id, b.audience_id = d.audience_id, 
            b.context_type_nm = d.context_type_nm, b.identity_type_val = d.identity_type_val, 
            b.context_val = d.context_val, b.event_nm = d.event_nm, 
            b.identity_type_nm = d.identity_type_nm, b.journey_id = d.journey_id
         when not matched then insert ( 
            entry_dttm, entry_dttm_tz, load_dttm, 
            journey_occurrence_id, identity_id, aud_occurrence_id, audience_id, 
            context_type_nm, event_id, identity_type_val, context_val, 
            event_nm, identity_type_nm, journey_id
         ) values ( 
            d.entry_dttm, d.entry_dttm_tz, d.load_dttm, 
            d.journey_occurrence_id, d.identity_id, d.aud_occurrence_id, d.audience_id, 
            d.context_type_nm, d.event_id, d.identity_type_val, d.context_val, 
            d.event_nm, d.identity_type_nm, d.journey_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :journey_entry_tmp , journey_entry , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..journey_entry_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..journey_entry_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..JOURNEY_ENTRY;
         drop table work.JOURNEY_ENTRY;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table JOURNEY_ENTRY;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..JOURNEY_EXIT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..JOURNEY_EXIT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..journey_exit_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..journey_exit_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=journey_exit , table_keys=%str(EVENT_ID), out_table=work.journey_exit );
   data work.journey_exit_tmp /view=work.journey_exit_tmp ;
      set work.journey_exit ;
      if exit_dttm_tz  ne . then exit_dttm_tz = tzoneu2s(exit_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :journey_exit_tmp , journey_exit );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..journey_exit_tmp ;
            set work.journey_exit_tmp ;
            stop;
         run;
         proc append data=work.journey_exit_tmp  base=&tmplib..journey_exit_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..journey_exit_tmp ;
            set work.journey_exit_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :journey_exit_tmp , journey_exit );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..journey_exit as b using &tmpdbschema..journey_exit_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.exit_dttm = d.exit_dttm, 
            b.exit_dttm_tz = d.exit_dttm_tz, b.load_dttm = d.load_dttm, 
            b.last_node_id = d.last_node_id, b.identity_type_nm = d.identity_type_nm, 
            b.context_type_nm = d.context_type_nm, b.aud_occurrence_id = d.aud_occurrence_id, 
            b.group_id = d.group_id, b.journey_id = d.journey_id, 
            b.reason_cd = d.reason_cd, b.audience_id = d.audience_id, 
            b.context_val = d.context_val, b.event_nm = d.event_nm, 
            b.identity_id = d.identity_id, b.identity_type_val = d.identity_type_val, 
            b.journey_occurrence_id = d.journey_occurrence_id, b.reason_txt = d.reason_txt
         when not matched then insert ( 
            exit_dttm, exit_dttm_tz, load_dttm, 
            last_node_id, identity_type_nm, context_type_nm, aud_occurrence_id, 
            event_id, group_id, journey_id, reason_cd, 
            audience_id, context_val, event_nm, identity_id, 
            identity_type_val, journey_occurrence_id, reason_txt
         ) values ( 
            d.exit_dttm, d.exit_dttm_tz, d.load_dttm, 
            d.last_node_id, d.identity_type_nm, d.context_type_nm, d.aud_occurrence_id, 
            d.event_id, d.group_id, d.journey_id, d.reason_cd, 
            d.audience_id, d.context_val, d.event_nm, d.identity_id, 
            d.identity_type_val, d.journey_occurrence_id, d.reason_txt  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :journey_exit_tmp , journey_exit , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..journey_exit_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..journey_exit_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..JOURNEY_EXIT;
         drop table work.JOURNEY_EXIT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table JOURNEY_EXIT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..JOURNEY_HOLDOUT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..JOURNEY_HOLDOUT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..journey_holdout_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..journey_holdout_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=journey_holdout , table_keys=%str(EVENT_ID), out_table=work.journey_holdout );
   data work.journey_holdout_tmp /view=work.journey_holdout_tmp ;
      set work.journey_holdout ;
      if holdout_dttm_tz  ne . then holdout_dttm_tz = tzoneu2s(holdout_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :journey_holdout_tmp , journey_holdout );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..journey_holdout_tmp ;
            set work.journey_holdout_tmp ;
            stop;
         run;
         proc append data=work.journey_holdout_tmp  base=&tmplib..journey_holdout_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..journey_holdout_tmp ;
            set work.journey_holdout_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :journey_holdout_tmp , journey_holdout );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..journey_holdout as b using &tmpdbschema..journey_holdout_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.holdout_dttm_tz = d.holdout_dttm_tz, 
            b.load_dttm = d.load_dttm, b.holdout_dttm = d.holdout_dttm, 
            b.journey_occurrence_id = d.journey_occurrence_id, b.journey_id = d.journey_id, 
            b.identity_type_val = d.identity_type_val, b.identity_type_nm = d.identity_type_nm, 
            b.identity_id = d.identity_id, b.event_nm = d.event_nm, 
            b.context_val = d.context_val, b.context_type_nm = d.context_type_nm, 
            b.audience_id = d.audience_id, b.aud_occurrence_id = d.aud_occurrence_id
         when not matched then insert ( 
            holdout_dttm_tz, load_dttm, holdout_dttm, 
            journey_occurrence_id, journey_id, identity_type_val, identity_type_nm, 
            identity_id, event_nm, event_id, context_val, 
            context_type_nm, audience_id, aud_occurrence_id
         ) values ( 
            d.holdout_dttm_tz, d.load_dttm, d.holdout_dttm, 
            d.journey_occurrence_id, d.journey_id, d.identity_type_val, d.identity_type_nm, 
            d.identity_id, d.event_nm, d.event_id, d.context_val, 
            d.context_type_nm, d.audience_id, d.aud_occurrence_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :journey_holdout_tmp , journey_holdout , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..journey_holdout_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..journey_holdout_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..JOURNEY_HOLDOUT;
         drop table work.JOURNEY_HOLDOUT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table JOURNEY_HOLDOUT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..JOURNEY_NODE_ENTRY)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..JOURNEY_NODE_ENTRY));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..journey_node_entry_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..journey_node_entry_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=journey_node_entry , table_keys=%str(EVENT_ID), out_table=work.journey_node_entry );
   data work.journey_node_entry_tmp /view=work.journey_node_entry_tmp ;
      set work.journey_node_entry ;
      if node_entry_dttm_tz  ne . then node_entry_dttm_tz = tzoneu2s(node_entry_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :journey_node_entry_tmp , journey_node_entry );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..journey_node_entry_tmp ;
            set work.journey_node_entry_tmp ;
            stop;
         run;
         proc append data=work.journey_node_entry_tmp  base=&tmplib..journey_node_entry_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..journey_node_entry_tmp ;
            set work.journey_node_entry_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :journey_node_entry_tmp , journey_node_entry );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..journey_node_entry as b using &tmpdbschema..journey_node_entry_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.node_entry_dttm = d.node_entry_dttm, 
            b.load_dttm = d.load_dttm, b.node_entry_dttm_tz = d.node_entry_dttm_tz, 
            b.node_type_nm = d.node_type_nm, b.node_id = d.node_id, 
            b.previous_node_id = d.previous_node_id, b.journey_occurrence_id = d.journey_occurrence_id, 
            b.journey_id = d.journey_id, b.identity_type_val = d.identity_type_val, 
            b.identity_type_nm = d.identity_type_nm, b.identity_id = d.identity_id, 
            b.group_id = d.group_id, b.event_nm = d.event_nm, 
            b.context_val = d.context_val, b.context_type_nm = d.context_type_nm, 
            b.audience_id = d.audience_id, b.aud_occurrence_id = d.aud_occurrence_id
         when not matched then insert ( 
            node_entry_dttm, load_dttm, node_entry_dttm_tz, 
            node_type_nm, node_id, previous_node_id, journey_occurrence_id, 
            journey_id, identity_type_val, identity_type_nm, identity_id, 
            group_id, event_nm, event_id, context_val, 
            context_type_nm, audience_id, aud_occurrence_id
         ) values ( 
            d.node_entry_dttm, d.load_dttm, d.node_entry_dttm_tz, 
            d.node_type_nm, d.node_id, d.previous_node_id, d.journey_occurrence_id, 
            d.journey_id, d.identity_type_val, d.identity_type_nm, d.identity_id, 
            d.group_id, d.event_nm, d.event_id, d.context_val, 
            d.context_type_nm, d.audience_id, d.aud_occurrence_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :journey_node_entry_tmp , journey_node_entry , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..journey_node_entry_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..journey_node_entry_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..JOURNEY_NODE_ENTRY;
         drop table work.JOURNEY_NODE_ENTRY;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table JOURNEY_NODE_ENTRY;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..JOURNEY_SUCCESS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..JOURNEY_SUCCESS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..journey_success_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..journey_success_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=journey_success , table_keys=%str(EVENT_ID), out_table=work.journey_success );
   data work.journey_success_tmp /view=work.journey_success_tmp ;
      set work.journey_success ;
      if success_dttm_tz  ne . then success_dttm_tz = tzoneu2s(success_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :journey_success_tmp , journey_success );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..journey_success_tmp ;
            set work.journey_success_tmp ;
            stop;
         run;
         proc append data=work.journey_success_tmp  base=&tmplib..journey_success_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..journey_success_tmp ;
            set work.journey_success_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :journey_success_tmp , journey_success );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..journey_success as b using &tmpdbschema..journey_success_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.unit_qty = d.unit_qty, 
            b.success_val = d.success_val, b.success_dttm = d.success_dttm, 
            b.load_dttm = d.load_dttm, b.success_dttm_tz = d.success_dttm_tz, 
            b.parent_event_designed_id = d.parent_event_designed_id, b.journey_id = d.journey_id, 
            b.identity_type_nm = d.identity_type_nm, b.group_id = d.group_id, 
            b.context_type_nm = d.context_type_nm, b.audience_id = d.audience_id, 
            b.aud_occurrence_id = d.aud_occurrence_id, b.context_val = d.context_val, 
            b.event_nm = d.event_nm, b.identity_id = d.identity_id, 
            b.identity_type_val = d.identity_type_val, b.journey_occurrence_id = d.journey_occurrence_id
         when not matched then insert ( 
            unit_qty, success_val, success_dttm, 
            load_dttm, success_dttm_tz, parent_event_designed_id, journey_id, 
            identity_type_nm, group_id, event_id, context_type_nm, 
            audience_id, aud_occurrence_id, context_val, event_nm, 
            identity_id, identity_type_val, journey_occurrence_id
         ) values ( 
            d.unit_qty, d.success_val, d.success_dttm, 
            d.load_dttm, d.success_dttm_tz, d.parent_event_designed_id, d.journey_id, 
            d.identity_type_nm, d.group_id, d.event_id, d.context_type_nm, 
            d.audience_id, d.aud_occurrence_id, d.context_val, d.event_nm, 
            d.identity_id, d.identity_type_val, d.journey_occurrence_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :journey_success_tmp , journey_success , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..journey_success_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..journey_success_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..JOURNEY_SUCCESS;
         drop table work.JOURNEY_SUCCESS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table JOURNEY_SUCCESS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..JOURNEY_SUPPRESSION)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..JOURNEY_SUPPRESSION));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..journey_suppression_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..journey_suppression_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=journey_suppression , table_keys=%str(EVENT_ID), out_table=work.journey_suppression );
   data work.journey_suppression_tmp /view=work.journey_suppression_tmp ;
      set work.journey_suppression ;
      if suppression_dttm_tz  ne . then suppression_dttm_tz = tzoneu2s(suppression_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :journey_suppression_tmp , journey_suppression );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..journey_suppression_tmp ;
            set work.journey_suppression_tmp ;
            stop;
         run;
         proc append data=work.journey_suppression_tmp  base=&tmplib..journey_suppression_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..journey_suppression_tmp ;
            set work.journey_suppression_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :journey_suppression_tmp , journey_suppression );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..journey_suppression as b using &tmpdbschema..journey_suppression_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.load_dttm = d.load_dttm, 
            b.suppression_dttm = d.suppression_dttm, b.suppression_dttm_tz = d.suppression_dttm_tz, 
            b.reason_txt = d.reason_txt, b.reason_cd = d.reason_cd, 
            b.journey_occurrence_id = d.journey_occurrence_id, b.identity_type_val = d.identity_type_val, 
            b.identity_type_nm = d.identity_type_nm, b.identity_id = d.identity_id, 
            b.context_type_nm = d.context_type_nm, b.audience_id = d.audience_id, 
            b.aud_occurrence_id = d.aud_occurrence_id, b.context_val = d.context_val, 
            b.event_nm = d.event_nm, b.journey_id = d.journey_id
         when not matched then insert ( 
            load_dttm, suppression_dttm, suppression_dttm_tz, 
            reason_txt, reason_cd, journey_occurrence_id, identity_type_val, 
            identity_type_nm, identity_id, event_id, context_type_nm, 
            audience_id, aud_occurrence_id, context_val, event_nm, 
            journey_id
         ) values ( 
            d.load_dttm, d.suppression_dttm, d.suppression_dttm_tz, 
            d.reason_txt, d.reason_cd, d.journey_occurrence_id, d.identity_type_val, 
            d.identity_type_nm, d.identity_id, d.event_id, d.context_type_nm, 
            d.audience_id, d.aud_occurrence_id, d.context_val, d.event_nm, 
            d.journey_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :journey_suppression_tmp , journey_suppression , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..journey_suppression_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..journey_suppression_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..JOURNEY_SUPPRESSION;
         drop table work.JOURNEY_SUPPRESSION;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table JOURNEY_SUPPRESSION;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..JOURNEY_TEST_SUCCESS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..JOURNEY_TEST_SUCCESS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..journey_test_success_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..journey_test_success_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=journey_test_success , table_keys=%str(EVENT_ID), out_table=work.journey_test_success );
   data work.journey_test_success_tmp /view=work.journey_test_success_tmp ;
      set work.journey_test_success ;
      if success_dttm_tz  ne . then success_dttm_tz = tzoneu2s(success_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :journey_test_success_tmp , journey_test_success );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..journey_test_success_tmp ;
            set work.journey_test_success_tmp ;
            stop;
         run;
         proc append data=work.journey_test_success_tmp  base=&tmplib..journey_test_success_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..journey_test_success_tmp ;
            set work.journey_test_success_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :journey_test_success_tmp , journey_test_success );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..journey_test_success as b using &tmpdbschema..journey_test_success_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.success_dttm_tz = d.success_dttm_tz, 
            b.success_dttm = d.success_dttm, b.parent_event_designed_id = d.parent_event_designed_id, 
            b.journey_id = d.journey_id, b.group_id = d.group_id, 
            b.event_nm = d.event_nm, b.context_type_nm = d.context_type_nm, 
            b.context_val = d.context_val, b.identity_id = d.identity_id, 
            b.journey_occurrence_id = d.journey_occurrence_id
         when not matched then insert ( 
            success_dttm_tz, success_dttm, parent_event_designed_id, 
            journey_id, group_id, event_nm, event_id, 
            context_type_nm, context_val, identity_id, journey_occurrence_id
         ) values ( 
            d.success_dttm_tz, d.success_dttm, d.parent_event_designed_id, 
            d.journey_id, d.group_id, d.event_nm, d.event_id, 
            d.context_type_nm, d.context_val, d.identity_id, d.journey_occurrence_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :journey_test_success_tmp , journey_test_success , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..journey_test_success_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..journey_test_success_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..JOURNEY_TEST_SUCCESS;
         drop table work.JOURNEY_TEST_SUCCESS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table JOURNEY_TEST_SUCCESS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_ACTIVITY)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_ACTIVITY));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_ACTIVITY) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_activity  base=&trglib..md_activity (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_ACTIVITY (
            valid_to_dttm, last_published_dttm, valid_from_dttm, 
            business_context_id, activity_version_id, activity_status_cd, activity_id, 
            activity_desc, activity_cd, activity_category_nm, activity_nm, 
            folder_path_nm )
      select valid_to_dttm, last_published_dttm, valid_from_dttm, 
            business_context_id, activity_version_id, activity_status_cd, activity_id, 
            activity_desc, activity_cd, activity_category_nm, activity_nm, 
            folder_path_nm
         from &udmmart..md_activity ;
      quit;
   %end;
   %err_check (Failed to insert into md_activity , md_activity );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_ACTIVITY;
         drop table work.MD_ACTIVITY;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_ACTIVITY;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_ACTIVITY_ABTESTPATH)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_ACTIVITY_ABTESTPATH));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_ACTIVITY_ABTESTPATH) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_activity_abtestpath  base=&trglib..md_activity_abtestpath (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_ACTIVITY_ABTESTPATH (
            next_node_val, abtest_dist_pct, control_flg, 
            valid_to_dttm, valid_from_dttm, activity_version_id, activity_status_cd, 
            activity_id, abtest_path_id, abtest_path_nm, activity_node_id )
      select next_node_val, abtest_dist_pct, control_flg, 
            valid_to_dttm, valid_from_dttm, activity_version_id, activity_status_cd, 
            activity_id, abtest_path_id, abtest_path_nm, activity_node_id
         from &udmmart..md_activity_abtestpath ;
      quit;
   %end;
   %err_check (Failed to insert into md_activity_abtestpath , md_activity_abtestpath );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_ACTIVITY_ABTESTPATH;
         drop table work.MD_ACTIVITY_ABTESTPATH;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_ACTIVITY_ABTESTPATH;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_ACTIVITY_ABTESTPATH_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_ACTIVITY_ABTESTPATH_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_ACTIVITY_ABTESTPATH_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_activity_abtestpath_all  base=&trglib..md_activity_abtestpath_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_ACTIVITY_ABTESTPATH_ALL (
            next_node_val, abtest_dist_pct, control_flg, 
            valid_to_dttm, valid_from_dttm, activity_version_id, activity_status_cd, 
            activity_node_id, activity_id, abtest_path_nm, abtest_path_id )
      select next_node_val, abtest_dist_pct, control_flg, 
            valid_to_dttm, valid_from_dttm, activity_version_id, activity_status_cd, 
            activity_node_id, activity_id, abtest_path_nm, abtest_path_id
         from &udmmart..md_activity_abtestpath_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_activity_abtestpath_all , md_activity_abtestpath_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_ACTIVITY_ABTESTPATH_ALL;
         drop table work.MD_ACTIVITY_ABTESTPATH_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_ACTIVITY_ABTESTPATH_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_ACTIVITY_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_ACTIVITY_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_ACTIVITY_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_activity_all  base=&trglib..md_activity_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_ACTIVITY_ALL (
            last_published_dttm, valid_from_dttm, valid_to_dttm, 
            folder_path_nm, business_context_id, activity_version_id, activity_status_cd, 
            activity_nm, activity_id, activity_desc, activity_cd, 
            activity_category_nm )
      select last_published_dttm, valid_from_dttm, valid_to_dttm, 
            folder_path_nm, business_context_id, activity_version_id, activity_status_cd, 
            activity_nm, activity_id, activity_desc, activity_cd, 
            activity_category_nm
         from &udmmart..md_activity_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_activity_all , md_activity_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_ACTIVITY_ALL;
         drop table work.MD_ACTIVITY_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_ACTIVITY_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_ACTIVITY_CUSTOM_PROP)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_ACTIVITY_CUSTOM_PROP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_ACTIVITY_CUSTOM_PROP) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_activity_custom_prop  base=&trglib..md_activity_custom_prop (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_ACTIVITY_CUSTOM_PROP (
            valid_to_dttm, valid_from_dttm, property_val, 
            property_nm, property_datatype_cd, activity_version_id, activity_status_cd, 
            activity_id )
      select valid_to_dttm, valid_from_dttm, property_val, 
            property_nm, property_datatype_cd, activity_version_id, activity_status_cd, 
            activity_id
         from &udmmart..md_activity_custom_prop ;
      quit;
   %end;
   %err_check (Failed to insert into md_activity_custom_prop , md_activity_custom_prop );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_ACTIVITY_CUSTOM_PROP;
         drop table work.MD_ACTIVITY_CUSTOM_PROP;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_ACTIVITY_CUSTOM_PROP;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_ACTIVITY_CUSTOM_PROP_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_ACTIVITY_CUSTOM_PROP_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_ACTIVITY_CUSTOM_PROP_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_activity_custom_prop_all  base=&trglib..md_activity_custom_prop_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_ACTIVITY_CUSTOM_PROP_ALL (
            valid_to_dttm, valid_from_dttm, property_val, 
            property_nm, property_datatype_cd, activity_version_id, activity_status_cd, 
            activity_id )
      select valid_to_dttm, valid_from_dttm, property_val, 
            property_nm, property_datatype_cd, activity_version_id, activity_status_cd, 
            activity_id
         from &udmmart..md_activity_custom_prop_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_activity_custom_prop_all , md_activity_custom_prop_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_ACTIVITY_CUSTOM_PROP_ALL;
         drop table work.MD_ACTIVITY_CUSTOM_PROP_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_ACTIVITY_CUSTOM_PROP_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_ACTIVITY_NODE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_ACTIVITY_NODE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_ACTIVITY_NODE) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_activity_node  base=&trglib..md_activity_node (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_ACTIVITY_NODE (
            next_node_val, previous_node_val, wait_tm, 
            end_node_flg, time_boxed_flg, specific_wait_flg, start_node_flg, 
            node_sequence_no, valid_from_dttm, valid_to_dttm, activity_version_id, 
            activity_status_cd, activity_node_type_nm, activity_node_nm, activity_node_id, 
            activity_id, abtest_id )
      select next_node_val, previous_node_val, wait_tm, 
            end_node_flg, time_boxed_flg, specific_wait_flg, start_node_flg, 
            node_sequence_no, valid_from_dttm, valid_to_dttm, activity_version_id, 
            activity_status_cd, activity_node_type_nm, activity_node_nm, activity_node_id, 
            activity_id, abtest_id
         from &udmmart..md_activity_node ;
      quit;
   %end;
   %err_check (Failed to insert into md_activity_node , md_activity_node );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_ACTIVITY_NODE;
         drop table work.MD_ACTIVITY_NODE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_ACTIVITY_NODE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_ACTIVITY_NODE_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_ACTIVITY_NODE_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_ACTIVITY_NODE_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_activity_node_all  base=&trglib..md_activity_node_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_ACTIVITY_NODE_ALL (
            previous_node_val, next_node_val, wait_tm, 
            time_boxed_flg, end_node_flg, start_node_flg, specific_wait_flg, 
            node_sequence_no, valid_to_dttm, valid_from_dttm, activity_version_id, 
            activity_status_cd, activity_node_type_nm, activity_node_nm, activity_node_id, 
            activity_id, abtest_id )
      select previous_node_val, next_node_val, wait_tm, 
            time_boxed_flg, end_node_flg, start_node_flg, specific_wait_flg, 
            node_sequence_no, valid_to_dttm, valid_from_dttm, activity_version_id, 
            activity_status_cd, activity_node_type_nm, activity_node_nm, activity_node_id, 
            activity_id, abtest_id
         from &udmmart..md_activity_node_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_activity_node_all , md_activity_node_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_ACTIVITY_NODE_ALL;
         drop table work.MD_ACTIVITY_NODE_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_ACTIVITY_NODE_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_ACTIVITY_X_ACTIVITY_NODE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_ACTIVITY_X_ACTIVITY_NODE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_ACTIVITY_X_ACTIVITY_NODE) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_activity_x_activity_node  base=&trglib..md_activity_x_activity_node (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_ACTIVITY_X_ACTIVITY_NODE (
            activity_version_id, activity_status_cd, activity_node_id, 
            activity_id )
      select activity_version_id, activity_status_cd, activity_node_id, 
            activity_id
         from &udmmart..md_activity_x_activity_node ;
      quit;
   %end;
   %err_check (Failed to insert into md_activity_x_activity_node , md_activity_x_activity_node );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_ACTIVITY_X_ACTIVITY_NODE;
         drop table work.MD_ACTIVITY_X_ACTIVITY_NODE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_ACTIVITY_X_ACTIVITY_NODE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_ACTIVITY_X_ACTIVITY_NODE_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_ACTIVITY_X_ACTIVITY_NODE_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_ACTIVITY_X_ACTIVITY_NODE_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_activity_x_activity_node_all  base=&trglib..md_activity_x_activity_node_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_ACTIVITY_X_ACTIVITY_NODE_ALL (
            activity_version_id, activity_status_cd, activity_node_id, 
            activity_id )
      select activity_version_id, activity_status_cd, activity_node_id, 
            activity_id
         from &udmmart..md_activity_x_activity_node_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_activity_x_activity_node_all , md_activity_x_activity_node_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_ACTIVITY_X_ACTIVITY_NODE_ALL;
         drop table work.MD_ACTIVITY_X_ACTIVITY_NODE_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_ACTIVITY_X_ACTIVITY_NODE_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_ACTIVITY_X_TASK)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_ACTIVITY_X_TASK));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_ACTIVITY_X_TASK) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_activity_x_task  base=&trglib..md_activity_x_task (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_ACTIVITY_X_TASK (
            task_version_id, task_id, activity_version_id, 
            activity_status_cd, activity_id )
      select task_version_id, task_id, activity_version_id, 
            activity_status_cd, activity_id
         from &udmmart..md_activity_x_task ;
      quit;
   %end;
   %err_check (Failed to insert into md_activity_x_task , md_activity_x_task );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_ACTIVITY_X_TASK;
         drop table work.MD_ACTIVITY_X_TASK;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_ACTIVITY_X_TASK;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_ACTIVITY_X_TASK_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_ACTIVITY_X_TASK_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_ACTIVITY_X_TASK_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_activity_x_task_all  base=&trglib..md_activity_x_task_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_ACTIVITY_X_TASK_ALL (
            task_version_id, task_id, activity_version_id, 
            activity_status_cd, activity_id )
      select task_version_id, task_id, activity_version_id, 
            activity_status_cd, activity_id
         from &udmmart..md_activity_x_task_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_activity_x_task_all , md_activity_x_task_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_ACTIVITY_X_TASK_ALL;
         drop table work.MD_ACTIVITY_X_TASK_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_ACTIVITY_X_TASK_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_ASSET)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_ASSET));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_ASSET) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_asset  base=&trglib..md_asset (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_ASSET (
            valid_to_dttm, valid_from_dttm, last_published_dttm, 
            owner_nm, created_user_nm, asset_version_id, asset_type_nm, 
            asset_status_cd, asset_nm, asset_id, asset_desc )
      select valid_to_dttm, valid_from_dttm, last_published_dttm, 
            owner_nm, created_user_nm, asset_version_id, asset_type_nm, 
            asset_status_cd, asset_nm, asset_id, asset_desc
         from &udmmart..md_asset ;
      quit;
   %end;
   %err_check (Failed to insert into md_asset , md_asset );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_ASSET;
         drop table work.MD_ASSET;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_ASSET;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_ASSET_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_ASSET_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_ASSET_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_asset_all  base=&trglib..md_asset_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_ASSET_ALL (
            valid_to_dttm, valid_from_dttm, last_published_dttm, 
            owner_nm, created_user_nm, asset_version_id, asset_type_nm, 
            asset_status_cd, asset_nm, asset_id, asset_desc )
      select valid_to_dttm, valid_from_dttm, last_published_dttm, 
            owner_nm, created_user_nm, asset_version_id, asset_type_nm, 
            asset_status_cd, asset_nm, asset_id, asset_desc
         from &udmmart..md_asset_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_asset_all , md_asset_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_ASSET_ALL;
         drop table work.MD_ASSET_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_ASSET_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_AUDIENCE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_AUDIENCE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_AUDIENCE) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_audience  base=&trglib..md_audience (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_AUDIENCE (
            audience_schedule_flg, audience_expiration_val, update_dttm, 
            create_dttm, delete_dttm, created_user_nm, audience_source_nm, 
            audience_nm, audience_id, audience_desc, audience_data_source_nm )
      select audience_schedule_flg, audience_expiration_val, update_dttm, 
            create_dttm, delete_dttm, created_user_nm, audience_source_nm, 
            audience_nm, audience_id, audience_desc, audience_data_source_nm
         from &udmmart..md_audience ;
      quit;
   %end;
   %err_check (Failed to insert into md_audience , md_audience );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_AUDIENCE;
         drop table work.MD_AUDIENCE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_AUDIENCE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_AUDIENCE_OCCURRENCE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_AUDIENCE_OCCURRENCE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_AUDIENCE_OCCURRENCE) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_audience_occurrence  base=&trglib..md_audience_occurrence (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_AUDIENCE_OCCURRENCE (
            audience_size_val, update_dttm, end_tm, 
            start_tm, started_by_nm, occurrence_type_nm, execution_status_cd, 
            audience_id, aud_occurrence_id )
      select audience_size_val, update_dttm, end_tm, 
            start_tm, started_by_nm, occurrence_type_nm, execution_status_cd, 
            audience_id, aud_occurrence_id
         from &udmmart..md_audience_occurrence ;
      quit;
   %end;
   %err_check (Failed to insert into md_audience_occurrence , md_audience_occurrence );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_AUDIENCE_OCCURRENCE;
         drop table work.MD_AUDIENCE_OCCURRENCE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_AUDIENCE_OCCURRENCE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_AUDIENCE_X_SEGMENT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_AUDIENCE_X_SEGMENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_AUDIENCE_X_SEGMENT) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_audience_x_segment  base=&trglib..md_audience_x_segment (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_AUDIENCE_X_SEGMENT (
            segment_id, audience_id )
      select segment_id, audience_id
         from &udmmart..md_audience_x_segment ;
      quit;
   %end;
   %err_check (Failed to insert into md_audience_x_segment , md_audience_x_segment );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_AUDIENCE_X_SEGMENT;
         drop table work.MD_AUDIENCE_X_SEGMENT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_AUDIENCE_X_SEGMENT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_BU)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_BU));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_BU) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_bu  base=&trglib..md_bu (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_BU (
            bu_obsolete_flg, last_modified_dttm, created_dttm, 
            load_dttm, last_modified_usernm, created_by_usernm, bu_parentid, 
            bu_owner_usernm, bu_nm, bu_id, bu_desc, 
            bu_currency_cd )
      select bu_obsolete_flg, last_modified_dttm, created_dttm, 
            load_dttm, last_modified_usernm, created_by_usernm, bu_parentid, 
            bu_owner_usernm, bu_nm, bu_id, bu_desc, 
            bu_currency_cd
         from &udmmart..md_bu ;
      quit;
   %end;
   %err_check (Failed to insert into md_bu , md_bu );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_BU;
         drop table work.MD_BU;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_BU;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_BUSINESS_CONTEXT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_BUSINESS_CONTEXT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_BUSINESS_CONTEXT) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_business_context  base=&trglib..md_business_context (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_BUSINESS_CONTEXT (
            valid_to_dttm, valid_from_dttm, last_published_dttm, 
            owner_nm, locked_information_map_nm, information_map_nm, created_user_nm, 
            business_context_version_id, business_context_status_cd, business_context_src_cd, business_context_nm, 
            business_context_id, business_context_desc )
      select valid_to_dttm, valid_from_dttm, last_published_dttm, 
            owner_nm, locked_information_map_nm, information_map_nm, created_user_nm, 
            business_context_version_id, business_context_status_cd, business_context_src_cd, business_context_nm, 
            business_context_id, business_context_desc
         from &udmmart..md_business_context ;
      quit;
   %end;
   %err_check (Failed to insert into md_business_context , md_business_context );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_BUSINESS_CONTEXT;
         drop table work.MD_BUSINESS_CONTEXT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_BUSINESS_CONTEXT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_BUSINESS_CONTEXT_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_BUSINESS_CONTEXT_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_BUSINESS_CONTEXT_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_business_context_all  base=&trglib..md_business_context_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_BUSINESS_CONTEXT_ALL (
            valid_to_dttm, last_published_dttm, valid_from_dttm, 
            owner_nm, locked_information_map_nm, information_map_nm, created_user_nm, 
            business_context_version_id, business_context_status_cd, business_context_src_cd, business_context_nm, 
            business_context_id, business_context_desc )
      select valid_to_dttm, last_published_dttm, valid_from_dttm, 
            owner_nm, locked_information_map_nm, information_map_nm, created_user_nm, 
            business_context_version_id, business_context_status_cd, business_context_src_cd, business_context_nm, 
            business_context_id, business_context_desc
         from &udmmart..md_business_context_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_business_context_all , md_business_context_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_BUSINESS_CONTEXT_ALL;
         drop table work.MD_BUSINESS_CONTEXT_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_BUSINESS_CONTEXT_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_COSTCENTER)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_COSTCENTER));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_COSTCENTER) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_costcenter  base=&trglib..md_costcenter (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_COSTCENTER (
            cc_obsolete_flg, load_dttm, created_dttm, 
            last_modified_dttm, last_modified_usernm, gen_ledger_cd, fin_accnt_nm, 
            created_by_usernm, cost_center_id, cc_owner_usernm, cc_nm, 
            cc_desc )
      select cc_obsolete_flg, load_dttm, created_dttm, 
            last_modified_dttm, last_modified_usernm, gen_ledger_cd, fin_accnt_nm, 
            created_by_usernm, cost_center_id, cc_owner_usernm, cc_nm, 
            cc_desc
         from &udmmart..md_costcenter ;
      quit;
   %end;
   %err_check (Failed to insert into md_costcenter , md_costcenter );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_COSTCENTER;
         drop table work.MD_COSTCENTER;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_COSTCENTER;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_COST_CATEGORY)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_COST_CATEGORY));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_COST_CATEGORY) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_cost_category  base=&trglib..md_cost_category (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_COST_CATEGORY (
            ccat_obsolete_flg, load_dttm, created_dttm, 
            last_modified_dttm, last_modified_usernm, gen_ledger_cd, fin_accnt_nm, 
            created_by_usernm, ccat_owner_usernm, ccat_nm, ccat_id, 
            ccat_desc )
      select ccat_obsolete_flg, load_dttm, created_dttm, 
            last_modified_dttm, last_modified_usernm, gen_ledger_cd, fin_accnt_nm, 
            created_by_usernm, ccat_owner_usernm, ccat_nm, ccat_id, 
            ccat_desc
         from &udmmart..md_cost_category ;
      quit;
   %end;
   %err_check (Failed to insert into md_cost_category , md_cost_category );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_COST_CATEGORY;
         drop table work.MD_COST_CATEGORY;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_COST_CATEGORY;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_CREATIVE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_CREATIVE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_CREATIVE) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_creative  base=&trglib..md_creative (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_CREATIVE (
            last_published_dttm, valid_to_dttm, valid_from_dttm, 
            recommender_template_nm, recommender_template_id, owner_nm, folder_path_nm, 
            creative_version_id, creative_type_nm, creative_txt, creative_status_cd, 
            creative_nm, creative_id, creative_desc, creative_cd, 
            creative_category_nm, created_user_nm, business_context_id )
      select last_published_dttm, valid_to_dttm, valid_from_dttm, 
            recommender_template_nm, recommender_template_id, owner_nm, folder_path_nm, 
            creative_version_id, creative_type_nm, creative_txt, creative_status_cd, 
            creative_nm, creative_id, creative_desc, creative_cd, 
            creative_category_nm, created_user_nm, business_context_id
         from &udmmart..md_creative ;
      quit;
   %end;
   %err_check (Failed to insert into md_creative , md_creative );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_CREATIVE;
         drop table work.MD_CREATIVE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_CREATIVE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_CREATIVE_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_CREATIVE_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_CREATIVE_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_creative_all  base=&trglib..md_creative_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_CREATIVE_ALL (
            valid_to_dttm, valid_from_dttm, last_published_dttm, 
            recommender_template_nm, recommender_template_id, owner_nm, folder_path_nm, 
            creative_version_id, creative_type_nm, creative_txt, creative_status_cd, 
            creative_nm, creative_id, creative_desc, creative_cd, 
            creative_category_nm, created_user_nm, business_context_id )
      select valid_to_dttm, valid_from_dttm, last_published_dttm, 
            recommender_template_nm, recommender_template_id, owner_nm, folder_path_nm, 
            creative_version_id, creative_type_nm, creative_txt, creative_status_cd, 
            creative_nm, creative_id, creative_desc, creative_cd, 
            creative_category_nm, created_user_nm, business_context_id
         from &udmmart..md_creative_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_creative_all , md_creative_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_CREATIVE_ALL;
         drop table work.MD_CREATIVE_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_CREATIVE_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_CREATIVE_CUSTOM_PROP)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_CREATIVE_CUSTOM_PROP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_CREATIVE_CUSTOM_PROP) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_creative_custom_prop  base=&trglib..md_creative_custom_prop (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_CREATIVE_CUSTOM_PROP (
            valid_to_dttm, valid_from_dttm, property_val, 
            property_nm, property_datatype_cd, creative_version_id, creative_status_cd, 
            creative_id )
      select valid_to_dttm, valid_from_dttm, property_val, 
            property_nm, property_datatype_cd, creative_version_id, creative_status_cd, 
            creative_id
         from &udmmart..md_creative_custom_prop ;
      quit;
   %end;
   %err_check (Failed to insert into md_creative_custom_prop , md_creative_custom_prop );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_CREATIVE_CUSTOM_PROP;
         drop table work.MD_CREATIVE_CUSTOM_PROP;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_CREATIVE_CUSTOM_PROP;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_CREATIVE_CUSTOM_PROP_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_CREATIVE_CUSTOM_PROP_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_CREATIVE_CUSTOM_PROP_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_creative_custom_prop_all  base=&trglib..md_creative_custom_prop_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_CREATIVE_CUSTOM_PROP_ALL (
            valid_from_dttm, valid_to_dttm, property_val, 
            property_nm, property_datatype_cd, creative_version_id, creative_status_cd, 
            creative_id )
      select valid_from_dttm, valid_to_dttm, property_val, 
            property_nm, property_datatype_cd, creative_version_id, creative_status_cd, 
            creative_id
         from &udmmart..md_creative_custom_prop_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_creative_custom_prop_all , md_creative_custom_prop_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_CREATIVE_CUSTOM_PROP_ALL;
         drop table work.MD_CREATIVE_CUSTOM_PROP_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_CREATIVE_CUSTOM_PROP_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_CREATIVE_X_ASSET)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_CREATIVE_X_ASSET));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_CREATIVE_X_ASSET) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_creative_x_asset  base=&trglib..md_creative_x_asset (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_CREATIVE_X_ASSET (
            creative_version_id, creative_status_cd, creative_id, 
            asset_id )
      select creative_version_id, creative_status_cd, creative_id, 
            asset_id
         from &udmmart..md_creative_x_asset ;
      quit;
   %end;
   %err_check (Failed to insert into md_creative_x_asset , md_creative_x_asset );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_CREATIVE_X_ASSET;
         drop table work.MD_CREATIVE_X_ASSET;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_CREATIVE_X_ASSET;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_CREATIVE_X_ASSET_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_CREATIVE_X_ASSET_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_CREATIVE_X_ASSET_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_creative_x_asset_all  base=&trglib..md_creative_x_asset_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_CREATIVE_X_ASSET_ALL (
            creative_version_id, creative_status_cd, creative_id, 
            asset_id )
      select creative_version_id, creative_status_cd, creative_id, 
            asset_id
         from &udmmart..md_creative_x_asset_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_creative_x_asset_all , md_creative_x_asset_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_CREATIVE_X_ASSET_ALL;
         drop table work.MD_CREATIVE_X_ASSET_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_CREATIVE_X_ASSET_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_CUSTATTRIB_TABLE_VALUES)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_CUSTATTRIB_TABLE_VALUES));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_CUSTATTRIB_TABLE_VALUES) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_custattrib_table_values  base=&trglib..md_custattrib_table_values (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_CUSTATTRIB_TABLE_VALUES (
            is_obsolete_flg, load_dttm, last_modified_dttm, 
            created_dttm, table_val, last_modified_usernm, data_type, 
            data_formatter, created_by_usernm, attr_nm, attr_id, 
            attr_group_nm, attr_group_id, attr_group_cd, attr_cd )
      select is_obsolete_flg, load_dttm, last_modified_dttm, 
            created_dttm, table_val, last_modified_usernm, data_type, 
            data_formatter, created_by_usernm, attr_nm, attr_id, 
            attr_group_nm, attr_group_id, attr_group_cd, attr_cd
         from &udmmart..md_custattrib_table_values ;
      quit;
   %end;
   %err_check (Failed to insert into md_custattrib_table_values , md_custattrib_table_values );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_CUSTATTRIB_TABLE_VALUES;
         drop table work.MD_CUSTATTRIB_TABLE_VALUES;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_CUSTATTRIB_TABLE_VALUES;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_CUST_ATTRIB)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_CUST_ATTRIB));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_CUST_ATTRIB) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_cust_attrib  base=&trglib..md_cust_attrib (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_CUST_ATTRIB (
            is_obsolete_flg, is_grid_flg, load_dttm, 
            last_modified_dttm, created_dttm, remote_pklist_tab_col, last_modified_usernm, 
            data_type, data_formatter, created_by_usernm, attr_nm, 
            attr_id, attr_group_nm, attr_group_id, attr_group_cd, 
            attr_cd, associated_grid )
      select is_obsolete_flg, is_grid_flg, load_dttm, 
            last_modified_dttm, created_dttm, remote_pklist_tab_col, last_modified_usernm, 
            data_type, data_formatter, created_by_usernm, attr_nm, 
            attr_id, attr_group_nm, attr_group_id, attr_group_cd, 
            attr_cd, associated_grid
         from &udmmart..md_cust_attrib ;
      quit;
   %end;
   %err_check (Failed to insert into md_cust_attrib , md_cust_attrib );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_CUST_ATTRIB;
         drop table work.MD_CUST_ATTRIB;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_CUST_ATTRIB;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_DATAVIEW)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_DATAVIEW));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_DATAVIEW) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_dataview  base=&trglib..md_dataview (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_DATAVIEW (
            include_external_flg, include_internal_flg, analytic_active_flg, 
            max_path_time_val, analytics_period_val, max_path_length_val, half_life_time_val, 
            last_published_dttm, valid_from_dttm, valid_to_dttm, selected_task_list, 
            owner_nm, max_path_time_type_nm, dataview_version_id, dataview_status_cd, 
            dataview_nm, dataview_id, dataview_desc, custom_recent_exclude_cd, 
            custom_recent_cd, created_user_nm, analytics_period_type_nm )
      select include_external_flg, include_internal_flg, analytic_active_flg, 
            max_path_time_val, analytics_period_val, max_path_length_val, half_life_time_val, 
            last_published_dttm, valid_from_dttm, valid_to_dttm, selected_task_list, 
            owner_nm, max_path_time_type_nm, dataview_version_id, dataview_status_cd, 
            dataview_nm, dataview_id, dataview_desc, custom_recent_exclude_cd, 
            custom_recent_cd, created_user_nm, analytics_period_type_nm
         from &udmmart..md_dataview ;
      quit;
   %end;
   %err_check (Failed to insert into md_dataview , md_dataview );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_DATAVIEW;
         drop table work.MD_DATAVIEW;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_DATAVIEW;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_DATAVIEW_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_DATAVIEW_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_DATAVIEW_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_dataview_all  base=&trglib..md_dataview_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_DATAVIEW_ALL (
            include_internal_flg, analytic_active_flg, include_external_flg, 
            max_path_length_val, half_life_time_val, analytics_period_val, max_path_time_val, 
            valid_to_dttm, valid_from_dttm, last_published_dttm, selected_task_list, 
            owner_nm, max_path_time_type_nm, dataview_version_id, dataview_status_cd, 
            dataview_nm, dataview_id, dataview_desc, custom_recent_exclude_cd, 
            custom_recent_cd, created_user_nm, analytics_period_type_nm )
      select include_internal_flg, analytic_active_flg, include_external_flg, 
            max_path_length_val, half_life_time_val, analytics_period_val, max_path_time_val, 
            valid_to_dttm, valid_from_dttm, last_published_dttm, selected_task_list, 
            owner_nm, max_path_time_type_nm, dataview_version_id, dataview_status_cd, 
            dataview_nm, dataview_id, dataview_desc, custom_recent_exclude_cd, 
            custom_recent_cd, created_user_nm, analytics_period_type_nm
         from &udmmart..md_dataview_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_dataview_all , md_dataview_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_DATAVIEW_ALL;
         drop table work.MD_DATAVIEW_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_DATAVIEW_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_DATAVIEW_X_EVENT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_DATAVIEW_X_EVENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_DATAVIEW_X_EVENT) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_dataview_x_event  base=&trglib..md_dataview_x_event (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_DATAVIEW_X_EVENT (
            event_id, dataview_version_id, dataview_status_cd, 
            dataview_id )
      select event_id, dataview_version_id, dataview_status_cd, 
            dataview_id
         from &udmmart..md_dataview_x_event ;
      quit;
   %end;
   %err_check (Failed to insert into md_dataview_x_event , md_dataview_x_event );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_DATAVIEW_X_EVENT;
         drop table work.MD_DATAVIEW_X_EVENT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_DATAVIEW_X_EVENT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_DATAVIEW_X_EVENT_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_DATAVIEW_X_EVENT_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_DATAVIEW_X_EVENT_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_dataview_x_event_all  base=&trglib..md_dataview_x_event_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_DATAVIEW_X_EVENT_ALL (
            event_id, dataview_version_id, dataview_status_cd, 
            dataview_id )
      select event_id, dataview_version_id, dataview_status_cd, 
            dataview_id
         from &udmmart..md_dataview_x_event_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_dataview_x_event_all , md_dataview_x_event_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_DATAVIEW_X_EVENT_ALL;
         drop table work.MD_DATAVIEW_X_EVENT_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_DATAVIEW_X_EVENT_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_EVENT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_EVENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_EVENT) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_event  base=&trglib..md_event (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_EVENT (
            valid_from_dttm, last_published_dttm, valid_to_dttm, 
            owner_nm, event_version_id, event_type_nm, event_subtype_nm, 
            event_status_cd, event_nm, event_id, event_desc, 
            created_user_nm, channel_nm )
      select valid_from_dttm, last_published_dttm, valid_to_dttm, 
            owner_nm, event_version_id, event_type_nm, event_subtype_nm, 
            event_status_cd, event_nm, event_id, event_desc, 
            created_user_nm, channel_nm
         from &udmmart..md_event ;
      quit;
   %end;
   %err_check (Failed to insert into md_event , md_event );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_EVENT;
         drop table work.MD_EVENT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_EVENT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_EVENT_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_EVENT_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_EVENT_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_event_all  base=&trglib..md_event_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_EVENT_ALL (
            last_published_dttm, valid_to_dttm, valid_from_dttm, 
            owner_nm, event_version_id, event_type_nm, event_subtype_nm, 
            event_status_cd, event_nm, event_id, event_desc, 
            created_user_nm, channel_nm )
      select last_published_dttm, valid_to_dttm, valid_from_dttm, 
            owner_nm, event_version_id, event_type_nm, event_subtype_nm, 
            event_status_cd, event_nm, event_id, event_desc, 
            created_user_nm, channel_nm
         from &udmmart..md_event_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_event_all , md_event_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_EVENT_ALL;
         drop table work.MD_EVENT_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_EVENT_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_FISCAL_PERIOD)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_FISCAL_PERIOD));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_FISCAL_PERIOD) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_fiscal_period  base=&trglib..md_fiscal_period (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_FISCAL_PERIOD (
            fp_obsolete_flg, fp_end_dt, fp_start_dt, 
            last_modified_dttm, created_dttm, load_dttm, last_modified_usernm, 
            fp_nm, fp_id, fp_desc, fp_cls_ver, 
            created_by_usernm )
      select fp_obsolete_flg, fp_end_dt, fp_start_dt, 
            last_modified_dttm, created_dttm, load_dttm, last_modified_usernm, 
            fp_nm, fp_id, fp_desc, fp_cls_ver, 
            created_by_usernm
         from &udmmart..md_fiscal_period ;
      quit;
   %end;
   %err_check (Failed to insert into md_fiscal_period , md_fiscal_period );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_FISCAL_PERIOD;
         drop table work.MD_FISCAL_PERIOD;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_FISCAL_PERIOD;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_GRID_ATTR_DEFN)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_GRID_ATTR_DEFN));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_GRID_ATTR_DEFN) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_grid_attr_defn  base=&trglib..md_grid_attr_defn (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_GRID_ATTR_DEFN (
            attr_obsolete_flg, grid_mandatory_flg, grid_obsolete_flg, 
            attr_order_no, load_dttm, last_modified_dttm, created_dttm, 
            remote_pklist_tab_col, last_modified_usernm, grid_nm, grid_id, 
            grid_desc, grid_cd, data_type, data_formatter, 
            created_by_usernm, attr_nm, attr_id, attr_group_nm, 
            attr_group_id, attr_group_cd, attr_desc, attr_cd, 
            associated_grid )
      select attr_obsolete_flg, grid_mandatory_flg, grid_obsolete_flg, 
            attr_order_no, load_dttm, last_modified_dttm, created_dttm, 
            remote_pklist_tab_col, last_modified_usernm, grid_nm, grid_id, 
            grid_desc, grid_cd, data_type, data_formatter, 
            created_by_usernm, attr_nm, attr_id, attr_group_nm, 
            attr_group_id, attr_group_cd, attr_desc, attr_cd, 
            associated_grid
         from &udmmart..md_grid_attr_defn ;
      quit;
   %end;
   %err_check (Failed to insert into md_grid_attr_defn , md_grid_attr_defn );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_GRID_ATTR_DEFN;
         drop table work.MD_GRID_ATTR_DEFN;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_GRID_ATTR_DEFN;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_JOURNEY)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_JOURNEY));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_JOURNEY) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_journey  base=&trglib..md_journey (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_JOURNEY (
            control_group_flg, target_goal_qty, last_activated_dttm, 
            test_type_nm, target_goal_type_nm, purpose_id, journey_version_id, 
            journey_status_cd, journey_nm, journey_id, created_user_nm, 
            activated_user_nm )
      select control_group_flg, target_goal_qty, last_activated_dttm, 
            test_type_nm, target_goal_type_nm, purpose_id, journey_version_id, 
            journey_status_cd, journey_nm, journey_id, created_user_nm, 
            activated_user_nm
         from &udmmart..md_journey ;
      quit;
   %end;
   %err_check (Failed to insert into md_journey , md_journey );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_JOURNEY;
         drop table work.MD_JOURNEY;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_JOURNEY;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_JOURNEY_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_JOURNEY_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_JOURNEY_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_journey_all  base=&trglib..md_journey_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_JOURNEY_ALL (
            control_group_flg, target_goal_qty, last_activated_dttm, 
            test_type_nm, target_goal_type_nm, purpose_id, journey_version_id, 
            journey_status_cd, journey_nm, journey_id, created_user_nm, 
            activated_user_nm )
      select control_group_flg, target_goal_qty, last_activated_dttm, 
            test_type_nm, target_goal_type_nm, purpose_id, journey_version_id, 
            journey_status_cd, journey_nm, journey_id, created_user_nm, 
            activated_user_nm
         from &udmmart..md_journey_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_journey_all , md_journey_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_JOURNEY_ALL;
         drop table work.MD_JOURNEY_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_JOURNEY_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_JOURNEY_NODE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_JOURNEY_NODE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_JOURNEY_NODE) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_journey_node  base=&trglib..md_journey_node (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_JOURNEY_NODE (
            previous_node_id, node_type, node_nm, 
            next_node_id, journey_version_id, journey_node_id, journey_id )
      select previous_node_id, node_type, node_nm, 
            next_node_id, journey_version_id, journey_node_id, journey_id
         from &udmmart..md_journey_node ;
      quit;
   %end;
   %err_check (Failed to insert into md_journey_node , md_journey_node );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_JOURNEY_NODE;
         drop table work.MD_JOURNEY_NODE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_JOURNEY_NODE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_JOURNEY_NODE_OCCURRENCE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_JOURNEY_NODE_OCCURRENCE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_JOURNEY_NODE_OCCURRENCE) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_journey_node_occurrence  base=&trglib..md_journey_node_occurrence (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_JOURNEY_NODE_OCCURRENCE (
            num_of_contacts_entered, end_dttm, start_dttm, 
            journey_version_id, journey_occurrence_id, journey_node_occurrence_id, journey_node_id, 
            journey_id, group_id, execution_status, error_messages )
      select num_of_contacts_entered, end_dttm, start_dttm, 
            journey_version_id, journey_occurrence_id, journey_node_occurrence_id, journey_node_id, 
            journey_id, group_id, execution_status, error_messages
         from &udmmart..md_journey_node_occurrence ;
      quit;
   %end;
   %err_check (Failed to insert into md_journey_node_occurrence , md_journey_node_occurrence );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_JOURNEY_NODE_OCCURRENCE;
         drop table work.MD_JOURNEY_NODE_OCCURRENCE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_JOURNEY_NODE_OCCURRENCE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_JOURNEY_NODE_X_NEXT_NODE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_JOURNEY_NODE_X_NEXT_NODE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_JOURNEY_NODE_X_NEXT_NODE) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_journey_node_x_next_node  base=&trglib..md_journey_node_x_next_node (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_JOURNEY_NODE_X_NEXT_NODE (
            next_node_id, journey_node_id )
      select next_node_id, journey_node_id
         from &udmmart..md_journey_node_x_next_node ;
      quit;
   %end;
   %err_check (Failed to insert into md_journey_node_x_next_node , md_journey_node_x_next_node );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_JOURNEY_NODE_X_NEXT_NODE;
         drop table work.MD_JOURNEY_NODE_X_NEXT_NODE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_JOURNEY_NODE_X_NEXT_NODE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_JOURNEY_NODE_X_PREVIOUS_NODE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_JOURNEY_NODE_X_PREVIOUS_NODE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_JOURNEY_NODE_X_PREVIOUS_NODE) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_journey_node_x_previous_node  base=&trglib..md_journey_node_x_previous_node (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_JOURNEY_NODE_X_PREVIOUS_NODE (
            previous_node_id, journey_node_id )
      select previous_node_id, journey_node_id
         from &udmmart..md_journey_node_x_previous_node ;
      quit;
   %end;
   %err_check (Failed to insert into md_journey_node_x_previous_node , md_journey_node_x_previous_node );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_JOURNEY_NODE_X_PREVIOUS_NODE;
         drop table work.MD_JOURNEY_NODE_X_PREVIOUS_NODE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_JOURNEY_NODE_X_PREVIOUS_NODE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_JOURNEY_NODE_X_VARIANT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_JOURNEY_NODE_X_VARIANT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_JOURNEY_NODE_X_VARIANT) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_journey_node_x_variant  base=&trglib..md_journey_node_x_variant (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_JOURNEY_NODE_X_VARIANT (
            control_flg, analysis_period_duration, variant_dist_pct, 
            variant_nm, journey_node_id, analysis_group_id )
      select control_flg, analysis_period_duration, variant_dist_pct, 
            variant_nm, journey_node_id, analysis_group_id
         from &udmmart..md_journey_node_x_variant ;
      quit;
   %end;
   %err_check (Failed to insert into md_journey_node_x_variant , md_journey_node_x_variant );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_JOURNEY_NODE_X_VARIANT;
         drop table work.MD_JOURNEY_NODE_X_VARIANT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_JOURNEY_NODE_X_VARIANT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_JOURNEY_OCCURRENCE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_JOURNEY_OCCURRENCE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_JOURNEY_OCCURRENCE) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_journey_occurrence  base=&trglib..md_journey_occurrence (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_JOURNEY_OCCURRENCE (
            num_of_contacts_entered, num_of_contacts_suppressed, journey_occurrence_num, 
            start_dttm, end_dttm, started_by_nm, occurrence_type_nm, 
            journey_version_id, journey_occurrence_id, journey_id, execution_status, 
            error_messages, aud_occurrence_id )
      select num_of_contacts_entered, num_of_contacts_suppressed, journey_occurrence_num, 
            start_dttm, end_dttm, started_by_nm, occurrence_type_nm, 
            journey_version_id, journey_occurrence_id, journey_id, execution_status, 
            error_messages, aud_occurrence_id
         from &udmmart..md_journey_occurrence ;
      quit;
   %end;
   %err_check (Failed to insert into md_journey_occurrence , md_journey_occurrence );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_JOURNEY_OCCURRENCE;
         drop table work.MD_JOURNEY_OCCURRENCE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_JOURNEY_OCCURRENCE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_JOURNEY_X_AUDIENCE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_JOURNEY_X_AUDIENCE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_JOURNEY_X_AUDIENCE) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_journey_x_audience  base=&trglib..md_journey_x_audience (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_JOURNEY_X_AUDIENCE (
            journey_version_id, journey_node_id, journey_id, 
            audience_id, aud_relationship_nm )
      select journey_version_id, journey_node_id, journey_id, 
            audience_id, aud_relationship_nm
         from &udmmart..md_journey_x_audience ;
      quit;
   %end;
   %err_check (Failed to insert into md_journey_x_audience , md_journey_x_audience );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_JOURNEY_X_AUDIENCE;
         drop table work.MD_JOURNEY_X_AUDIENCE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_JOURNEY_X_AUDIENCE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_JOURNEY_X_EVENT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_JOURNEY_X_EVENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_JOURNEY_X_EVENT) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_journey_x_event  base=&trglib..md_journey_x_event (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_JOURNEY_X_EVENT (
            journey_version_id, journey_node_id, journey_id, 
            event_relationship_nm, event_id )
      select journey_version_id, journey_node_id, journey_id, 
            event_relationship_nm, event_id
         from &udmmart..md_journey_x_event ;
      quit;
   %end;
   %err_check (Failed to insert into md_journey_x_event , md_journey_x_event );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_JOURNEY_X_EVENT;
         drop table work.MD_JOURNEY_X_EVENT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_JOURNEY_X_EVENT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_JOURNEY_X_TASK)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_JOURNEY_X_TASK));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_JOURNEY_X_TASK) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_journey_x_task  base=&trglib..md_journey_x_task (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_JOURNEY_X_TASK (
            task_version_id, task_id, journey_version_id, 
            journey_node_id, journey_id )
      select task_version_id, task_id, journey_version_id, 
            journey_node_id, journey_id
         from &udmmart..md_journey_x_task ;
      quit;
   %end;
   %err_check (Failed to insert into md_journey_x_task , md_journey_x_task );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_JOURNEY_X_TASK;
         drop table work.MD_JOURNEY_X_TASK;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_JOURNEY_X_TASK;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_MESSAGE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_MESSAGE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_MESSAGE) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_message  base=&trglib..md_message (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_MESSAGE (
            valid_to_dttm, valid_from_dttm, last_published_dttm, 
            owner_nm, message_version_id, message_type_nm, message_status_cd, 
            message_nm, message_id, message_desc, message_cd, 
            message_category_nm, folder_path_nm, created_user_nm, business_context_id )
      select valid_to_dttm, valid_from_dttm, last_published_dttm, 
            owner_nm, message_version_id, message_type_nm, message_status_cd, 
            message_nm, message_id, message_desc, message_cd, 
            message_category_nm, folder_path_nm, created_user_nm, business_context_id
         from &udmmart..md_message ;
      quit;
   %end;
   %err_check (Failed to insert into md_message , md_message );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_MESSAGE;
         drop table work.MD_MESSAGE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_MESSAGE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_MESSAGE_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_MESSAGE_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_MESSAGE_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_message_all  base=&trglib..md_message_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_MESSAGE_ALL (
            valid_from_dttm, last_published_dttm, valid_to_dttm, 
            owner_nm, message_version_id, message_type_nm, message_nm, 
            message_desc, message_category_nm, folder_path_nm, message_cd, 
            message_id, message_status_cd, created_user_nm, business_context_id )
      select valid_from_dttm, last_published_dttm, valid_to_dttm, 
            owner_nm, message_version_id, message_type_nm, message_nm, 
            message_desc, message_category_nm, folder_path_nm, message_cd, 
            message_id, message_status_cd, created_user_nm, business_context_id
         from &udmmart..md_message_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_message_all , md_message_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_MESSAGE_ALL;
         drop table work.MD_MESSAGE_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_MESSAGE_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_MESSAGE_CUSTOM_PROP)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_MESSAGE_CUSTOM_PROP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_MESSAGE_CUSTOM_PROP) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_message_custom_prop  base=&trglib..md_message_custom_prop (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_MESSAGE_CUSTOM_PROP (
            valid_from_dttm, valid_to_dttm, property_val, 
            property_datatype_cd, message_status_cd, message_id, message_version_id, 
            property_nm )
      select valid_from_dttm, valid_to_dttm, property_val, 
            property_datatype_cd, message_status_cd, message_id, message_version_id, 
            property_nm
         from &udmmart..md_message_custom_prop ;
      quit;
   %end;
   %err_check (Failed to insert into md_message_custom_prop , md_message_custom_prop );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_MESSAGE_CUSTOM_PROP;
         drop table work.MD_MESSAGE_CUSTOM_PROP;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_MESSAGE_CUSTOM_PROP;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_MESSAGE_CUSTOM_PROP_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_MESSAGE_CUSTOM_PROP_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_MESSAGE_CUSTOM_PROP_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_message_custom_prop_all  base=&trglib..md_message_custom_prop_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_MESSAGE_CUSTOM_PROP_ALL (
            valid_to_dttm, valid_from_dttm, property_val, 
            property_datatype_cd, message_status_cd, message_id, message_version_id, 
            property_nm )
      select valid_to_dttm, valid_from_dttm, property_val, 
            property_datatype_cd, message_status_cd, message_id, message_version_id, 
            property_nm
         from &udmmart..md_message_custom_prop_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_message_custom_prop_all , md_message_custom_prop_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_MESSAGE_CUSTOM_PROP_ALL;
         drop table work.MD_MESSAGE_CUSTOM_PROP_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_MESSAGE_CUSTOM_PROP_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_MESSAGE_X_CREATIVE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_MESSAGE_X_CREATIVE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_MESSAGE_X_CREATIVE) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_message_x_creative  base=&trglib..md_message_x_creative (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_MESSAGE_X_CREATIVE (
            message_version_id, message_id, creative_id, 
            message_status_cd )
      select message_version_id, message_id, creative_id, 
            message_status_cd
         from &udmmart..md_message_x_creative ;
      quit;
   %end;
   %err_check (Failed to insert into md_message_x_creative , md_message_x_creative );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_MESSAGE_X_CREATIVE;
         drop table work.MD_MESSAGE_X_CREATIVE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_MESSAGE_X_CREATIVE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_MESSAGE_X_CREATIVE_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_MESSAGE_X_CREATIVE_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_MESSAGE_X_CREATIVE_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_message_x_creative_all  base=&trglib..md_message_x_creative_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_MESSAGE_X_CREATIVE_ALL (
            message_version_id, message_id, creative_id, 
            message_status_cd )
      select message_version_id, message_id, creative_id, 
            message_status_cd
         from &udmmart..md_message_x_creative_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_message_x_creative_all , md_message_x_creative_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_MESSAGE_X_CREATIVE_ALL;
         drop table work.MD_MESSAGE_X_CREATIVE_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_MESSAGE_X_CREATIVE_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_OBJECT_TYPE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_OBJECT_TYPE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_OBJECT_TYPE) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_object_type  base=&trglib..md_object_type (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_OBJECT_TYPE (
            is_obsolete_flg, created_dttm, last_modified_dttm, 
            load_dttm, object_type, object_category, last_modified_usernm, 
            data_type, data_formatter, attr_nm, attr_id, 
            attr_group_nm, attr_group_cd, attr_cd, attr_group_id, 
            created_by_usernm, remote_pklist_tab_col )
      select is_obsolete_flg, created_dttm, last_modified_dttm, 
            load_dttm, object_type, object_category, last_modified_usernm, 
            data_type, data_formatter, attr_nm, attr_id, 
            attr_group_nm, attr_group_cd, attr_cd, attr_group_id, 
            created_by_usernm, remote_pklist_tab_col
         from &udmmart..md_object_type ;
      quit;
   %end;
   %err_check (Failed to insert into md_object_type , md_object_type );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_OBJECT_TYPE;
         drop table work.MD_OBJECT_TYPE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_OBJECT_TYPE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_OCCURRENCE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_OCCURRENCE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_OCCURRENCE) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_occurrence  base=&trglib..md_occurrence (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_OCCURRENCE (
            occurrence_no, properties_map_doc, start_tm, 
            end_tm, started_by_nm, occurrence_type_nm, object_version_id, 
            object_id, execution_status_cd, object_type_nm, occurrence_id )
      select occurrence_no, properties_map_doc, start_tm, 
            end_tm, started_by_nm, occurrence_type_nm, object_version_id, 
            object_id, execution_status_cd, object_type_nm, occurrence_id
         from &udmmart..md_occurrence ;
      quit;
   %end;
   %err_check (Failed to insert into md_occurrence , md_occurrence );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_OCCURRENCE;
         drop table work.MD_OCCURRENCE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_OCCURRENCE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_PICKLIST)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_PICKLIST));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_PICKLIST) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_picklist  base=&trglib..md_picklist (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_PICKLIST (
            is_obsolete_flg, last_modified_dttm, created_dttm, 
            load_dttm, plist_val, plist_id, plist_cd, 
            last_modified_usernm, created_by_usernm, attr_id, attr_group_id, 
            attr_cd, attr_group_nm, attr_nm, plist_desc, 
            plist_nm )
      select is_obsolete_flg, last_modified_dttm, created_dttm, 
            load_dttm, plist_val, plist_id, plist_cd, 
            last_modified_usernm, created_by_usernm, attr_id, attr_group_id, 
            attr_cd, attr_group_nm, attr_nm, plist_desc, 
            plist_nm
         from &udmmart..md_picklist ;
      quit;
   %end;
   %err_check (Failed to insert into md_picklist , md_picklist );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_PICKLIST;
         drop table work.MD_PICKLIST;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_PICKLIST;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_PURPOSE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_PURPOSE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_PURPOSE) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_purpose  base=&trglib..md_purpose (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_PURPOSE (
            purpose_nm, purpose_id )
      select purpose_nm, purpose_id
         from &udmmart..md_purpose ;
      quit;
   %end;
   %err_check (Failed to insert into md_purpose , md_purpose );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_PURPOSE;
         drop table work.MD_PURPOSE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_PURPOSE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_RTC)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_RTC));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_RTC) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_rtc  base=&trglib..md_rtc (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_RTC (
            occurrence_no, content_map_doc, rtc_dttm, 
            task_id, segment_id, rtc_id, occurrence_id, 
            segment_version_id, task_version_id )
      select occurrence_no, content_map_doc, rtc_dttm, 
            task_id, segment_id, rtc_id, occurrence_id, 
            segment_version_id, task_version_id
         from &udmmart..md_rtc ;
      quit;
   %end;
   %err_check (Failed to insert into md_rtc , md_rtc );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_RTC;
         drop table work.MD_RTC;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_RTC;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_SEGMENT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_SEGMENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_SEGMENT) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_segment  base=&trglib..md_segment (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_SEGMENT (
            last_published_dttm, valid_from_dttm, valid_to_dttm, 
            segment_version_id, segment_status_cd, segment_nm, segment_map_id, 
            segment_id, segment_cd, owner_nm, folder_path_nm, 
            created_user_nm, business_context_id, segment_category_nm, segment_desc, 
            segment_src_cd )
      select last_published_dttm, valid_from_dttm, valid_to_dttm, 
            segment_version_id, segment_status_cd, segment_nm, segment_map_id, 
            segment_id, segment_cd, owner_nm, folder_path_nm, 
            created_user_nm, business_context_id, segment_category_nm, segment_desc, 
            segment_src_cd
         from &udmmart..md_segment ;
      quit;
   %end;
   %err_check (Failed to insert into md_segment , md_segment );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_SEGMENT;
         drop table work.MD_SEGMENT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_SEGMENT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_SEGMENT_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_SEGMENT_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_SEGMENT_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_segment_all  base=&trglib..md_segment_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_SEGMENT_ALL (
            last_published_dttm, valid_from_dttm, valid_to_dttm, 
            segment_version_id, segment_status_cd, segment_nm, segment_id, 
            segment_cd, segment_category_nm, owner_nm, folder_path_nm, 
            business_context_id, created_user_nm, segment_desc, segment_map_id, 
            segment_src_cd )
      select last_published_dttm, valid_from_dttm, valid_to_dttm, 
            segment_version_id, segment_status_cd, segment_nm, segment_id, 
            segment_cd, segment_category_nm, owner_nm, folder_path_nm, 
            business_context_id, created_user_nm, segment_desc, segment_map_id, 
            segment_src_cd
         from &udmmart..md_segment_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_segment_all , md_segment_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_SEGMENT_ALL;
         drop table work.MD_SEGMENT_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_SEGMENT_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_SEGMENT_CUSTOM_PROP)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_SEGMENT_CUSTOM_PROP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_SEGMENT_CUSTOM_PROP) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_segment_custom_prop  base=&trglib..md_segment_custom_prop (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_SEGMENT_CUSTOM_PROP (
            valid_to_dttm, valid_from_dttm, segment_version_id, 
            segment_status_cd, property_val, property_datatype_cd, property_nm, 
            segment_id )
      select valid_to_dttm, valid_from_dttm, segment_version_id, 
            segment_status_cd, property_val, property_datatype_cd, property_nm, 
            segment_id
         from &udmmart..md_segment_custom_prop ;
      quit;
   %end;
   %err_check (Failed to insert into md_segment_custom_prop , md_segment_custom_prop );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_SEGMENT_CUSTOM_PROP;
         drop table work.MD_SEGMENT_CUSTOM_PROP;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_SEGMENT_CUSTOM_PROP;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_SEGMENT_CUSTOM_PROP_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_SEGMENT_CUSTOM_PROP_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_SEGMENT_CUSTOM_PROP_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_segment_custom_prop_all  base=&trglib..md_segment_custom_prop_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_SEGMENT_CUSTOM_PROP_ALL (
            valid_from_dttm, valid_to_dttm, segment_version_id, 
            segment_status_cd, property_val, property_datatype_cd, property_nm, 
            segment_id )
      select valid_from_dttm, valid_to_dttm, segment_version_id, 
            segment_status_cd, property_val, property_datatype_cd, property_nm, 
            segment_id
         from &udmmart..md_segment_custom_prop_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_segment_custom_prop_all , md_segment_custom_prop_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_SEGMENT_CUSTOM_PROP_ALL;
         drop table work.MD_SEGMENT_CUSTOM_PROP_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_SEGMENT_CUSTOM_PROP_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_SEGMENT_MAP)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_SEGMENT_MAP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_SEGMENT_MAP) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_segment_map  base=&trglib..md_segment_map (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_SEGMENT_MAP (
            scheduled_flg, recurrence_day_of_month_no, valid_to_dttm, 
            last_published_dttm, valid_from_dttm, rec_scheduled_end_dttm, rec_scheduled_start_dttm, 
            scheduled_end_dttm, scheduled_start_dttm, segment_map_version_id, segment_map_src_cd, 
            segment_map_nm, segment_map_id, segment_map_cd, segment_map_category_nm, 
            recurrence_monthly_type_nm, recurrence_frequency_cd, recurrence_day_of_wk_ordinal_no, recurrence_day_of_week_txt, 
            rec_scheduled_start_tm, owner_nm, folder_path_nm, business_context_id, 
            created_user_nm, recurrence_days_of_week_txt, segment_map_desc, segment_map_status_cd )
      select scheduled_flg, recurrence_day_of_month_no, valid_to_dttm, 
            last_published_dttm, valid_from_dttm, rec_scheduled_end_dttm, rec_scheduled_start_dttm, 
            scheduled_end_dttm, scheduled_start_dttm, segment_map_version_id, segment_map_src_cd, 
            segment_map_nm, segment_map_id, segment_map_cd, segment_map_category_nm, 
            recurrence_monthly_type_nm, recurrence_frequency_cd, recurrence_day_of_wk_ordinal_no, recurrence_day_of_week_txt, 
            rec_scheduled_start_tm, owner_nm, folder_path_nm, business_context_id, 
            created_user_nm, recurrence_days_of_week_txt, segment_map_desc, segment_map_status_cd
         from &udmmart..md_segment_map ;
      quit;
   %end;
   %err_check (Failed to insert into md_segment_map , md_segment_map );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_SEGMENT_MAP;
         drop table work.MD_SEGMENT_MAP;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_SEGMENT_MAP;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_SEGMENT_MAP_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_SEGMENT_MAP_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_SEGMENT_MAP_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_segment_map_all  base=&trglib..md_segment_map_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_SEGMENT_MAP_ALL (
            scheduled_flg, recurrence_day_of_month_no, rec_scheduled_start_dttm, 
            valid_from_dttm, rec_scheduled_end_dttm, scheduled_start_dttm, last_published_dttm, 
            valid_to_dttm, scheduled_end_dttm, segment_map_status_cd, segment_map_nm, 
            segment_map_desc, segment_map_category_nm, recurrence_monthly_type_nm, recurrence_days_of_week_txt, 
            recurrence_day_of_wk_ordinal_no, recurrence_day_of_week_txt, rec_scheduled_start_tm, owner_nm, 
            folder_path_nm, created_user_nm, business_context_id, recurrence_frequency_cd, 
            segment_map_cd, segment_map_id, segment_map_src_cd, segment_map_version_id )
      select scheduled_flg, recurrence_day_of_month_no, rec_scheduled_start_dttm, 
            valid_from_dttm, rec_scheduled_end_dttm, scheduled_start_dttm, last_published_dttm, 
            valid_to_dttm, scheduled_end_dttm, segment_map_status_cd, segment_map_nm, 
            segment_map_desc, segment_map_category_nm, recurrence_monthly_type_nm, recurrence_days_of_week_txt, 
            recurrence_day_of_wk_ordinal_no, recurrence_day_of_week_txt, rec_scheduled_start_tm, owner_nm, 
            folder_path_nm, created_user_nm, business_context_id, recurrence_frequency_cd, 
            segment_map_cd, segment_map_id, segment_map_src_cd, segment_map_version_id
         from &udmmart..md_segment_map_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_segment_map_all , md_segment_map_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_SEGMENT_MAP_ALL;
         drop table work.MD_SEGMENT_MAP_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_SEGMENT_MAP_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_SEGMENT_MAP_CUSTOM_PROP)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_SEGMENT_MAP_CUSTOM_PROP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_SEGMENT_MAP_CUSTOM_PROP) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_segment_map_custom_prop  base=&trglib..md_segment_map_custom_prop (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_SEGMENT_MAP_CUSTOM_PROP (
            valid_from_dttm, valid_to_dttm, segment_map_status_cd, 
            property_val, property_nm, property_datatype_cd, segment_map_id, 
            segment_map_version_id )
      select valid_from_dttm, valid_to_dttm, segment_map_status_cd, 
            property_val, property_nm, property_datatype_cd, segment_map_id, 
            segment_map_version_id
         from &udmmart..md_segment_map_custom_prop ;
      quit;
   %end;
   %err_check (Failed to insert into md_segment_map_custom_prop , md_segment_map_custom_prop );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_SEGMENT_MAP_CUSTOM_PROP;
         drop table work.MD_SEGMENT_MAP_CUSTOM_PROP;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_SEGMENT_MAP_CUSTOM_PROP;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_SEGMENT_MAP_CUSTOM_PROP_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_SEGMENT_MAP_CUSTOM_PROP_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_SEGMENT_MAP_CUSTOM_PROP_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_segment_map_custom_prop_all  base=&trglib..md_segment_map_custom_prop_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_SEGMENT_MAP_CUSTOM_PROP_ALL (
            valid_from_dttm, valid_to_dttm, segment_map_status_cd, 
            property_val, property_nm, property_datatype_cd, segment_map_id, 
            segment_map_version_id )
      select valid_from_dttm, valid_to_dttm, segment_map_status_cd, 
            property_val, property_nm, property_datatype_cd, segment_map_id, 
            segment_map_version_id
         from &udmmart..md_segment_map_custom_prop_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_segment_map_custom_prop_all , md_segment_map_custom_prop_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_SEGMENT_MAP_CUSTOM_PROP_ALL;
         drop table work.MD_SEGMENT_MAP_CUSTOM_PROP_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_SEGMENT_MAP_CUSTOM_PROP_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_SEGMENT_MAP_X_SEGMENT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_SEGMENT_MAP_X_SEGMENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_SEGMENT_MAP_X_SEGMENT) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_segment_map_x_segment  base=&trglib..md_segment_map_x_segment (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_SEGMENT_MAP_X_SEGMENT (
            segment_version_id, segment_map_status_cd, segment_id, 
            segment_map_id, segment_map_version_id )
      select segment_version_id, segment_map_status_cd, segment_id, 
            segment_map_id, segment_map_version_id
         from &udmmart..md_segment_map_x_segment ;
      quit;
   %end;
   %err_check (Failed to insert into md_segment_map_x_segment , md_segment_map_x_segment );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_SEGMENT_MAP_X_SEGMENT;
         drop table work.MD_SEGMENT_MAP_X_SEGMENT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_SEGMENT_MAP_X_SEGMENT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_SEGMENT_MAP_X_SEGMENT_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_SEGMENT_MAP_X_SEGMENT_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_SEGMENT_MAP_X_SEGMENT_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_segment_map_x_segment_all  base=&trglib..md_segment_map_x_segment_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_SEGMENT_MAP_X_SEGMENT_ALL (
            segment_map_version_id, segment_map_status_cd, segment_map_id, 
            segment_id, segment_version_id )
      select segment_map_version_id, segment_map_status_cd, segment_map_id, 
            segment_id, segment_version_id
         from &udmmart..md_segment_map_x_segment_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_segment_map_x_segment_all , md_segment_map_x_segment_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_SEGMENT_MAP_X_SEGMENT_ALL;
         drop table work.MD_SEGMENT_MAP_X_SEGMENT_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_SEGMENT_MAP_X_SEGMENT_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_SEGMENT_TEST)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_SEGMENT_TEST));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_SEGMENT_TEST) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_segment_test  base=&trglib..md_segment_test (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_SEGMENT_TEST (
            test_enabled_flg, stratified_sampling_flg, test_pct, 
            test_cnt, test_type_nm, test_sizing_type_nm, test_nm, 
            test_cd, task_id, stratified_samp_criteria_txt, task_version_id )
      select test_enabled_flg, stratified_sampling_flg, test_pct, 
            test_cnt, test_type_nm, test_sizing_type_nm, test_nm, 
            test_cd, task_id, stratified_samp_criteria_txt, task_version_id
         from &udmmart..md_segment_test ;
      quit;
   %end;
   %err_check (Failed to insert into md_segment_test , md_segment_test );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_SEGMENT_TEST;
         drop table work.MD_SEGMENT_TEST;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_SEGMENT_TEST;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_SEGMENT_TEST_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_SEGMENT_TEST_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_SEGMENT_TEST_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_segment_test_all  base=&trglib..md_segment_test_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_SEGMENT_TEST_ALL (
            test_enabled_flg, stratified_sampling_flg, test_pct, 
            test_cnt, test_type_nm, test_sizing_type_nm, test_nm, 
            task_version_id, task_id, stratified_samp_criteria_txt, test_cd )
      select test_enabled_flg, stratified_sampling_flg, test_pct, 
            test_cnt, test_type_nm, test_sizing_type_nm, test_nm, 
            task_version_id, task_id, stratified_samp_criteria_txt, test_cd
         from &udmmart..md_segment_test_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_segment_test_all , md_segment_test_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_SEGMENT_TEST_ALL;
         drop table work.MD_SEGMENT_TEST_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_SEGMENT_TEST_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_SEGMENT_TEST_X_SEGMENT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_SEGMENT_TEST_X_SEGMENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_SEGMENT_TEST_X_SEGMENT) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_segment_test_x_segment  base=&trglib..md_segment_test_x_segment (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_SEGMENT_TEST_X_SEGMENT (
            task_version_id, segment_id, task_id, 
            test_cd )
      select task_version_id, segment_id, task_id, 
            test_cd
         from &udmmart..md_segment_test_x_segment ;
      quit;
   %end;
   %err_check (Failed to insert into md_segment_test_x_segment , md_segment_test_x_segment );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_SEGMENT_TEST_X_SEGMENT;
         drop table work.MD_SEGMENT_TEST_X_SEGMENT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_SEGMENT_TEST_X_SEGMENT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_SEGMENT_TEST_X_SEGMENT_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_SEGMENT_TEST_X_SEGMENT_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_SEGMENT_TEST_X_SEGMENT_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_segment_test_x_segment_all  base=&trglib..md_segment_test_x_segment_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_SEGMENT_TEST_X_SEGMENT_ALL (
            test_cd, task_version_id, segment_id, 
            task_id )
      select test_cd, task_version_id, segment_id, 
            task_id
         from &udmmart..md_segment_test_x_segment_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_segment_test_x_segment_all , md_segment_test_x_segment_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_SEGMENT_TEST_X_SEGMENT_ALL;
         drop table work.MD_SEGMENT_TEST_X_SEGMENT_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_SEGMENT_TEST_X_SEGMENT_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_SEGMENT_X_EVENT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_SEGMENT_X_EVENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_SEGMENT_X_EVENT) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_segment_x_event  base=&trglib..md_segment_x_event (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_SEGMENT_X_EVENT (
            segment_status_cd, event_id, segment_id, 
            segment_version_id )
      select segment_status_cd, event_id, segment_id, 
            segment_version_id
         from &udmmart..md_segment_x_event ;
      quit;
   %end;
   %err_check (Failed to insert into md_segment_x_event , md_segment_x_event );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_SEGMENT_X_EVENT;
         drop table work.MD_SEGMENT_X_EVENT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_SEGMENT_X_EVENT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_SEGMENT_X_EVENT_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_SEGMENT_X_EVENT_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_SEGMENT_X_EVENT_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_segment_x_event_all  base=&trglib..md_segment_x_event_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_SEGMENT_X_EVENT_ALL (
            segment_version_id, segment_status_cd, event_id, 
            segment_id )
      select segment_version_id, segment_status_cd, event_id, 
            segment_id
         from &udmmart..md_segment_x_event_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_segment_x_event_all , md_segment_x_event_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_SEGMENT_X_EVENT_ALL;
         drop table work.MD_SEGMENT_X_EVENT_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_SEGMENT_X_EVENT_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_SPOT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_SPOT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_SPOT) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_spot  base=&trglib..md_spot (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_SPOT (
            location_selector_flg, multi_page_flg, last_published_dttm, 
            valid_from_dttm, valid_to_dttm, spot_width_val_no, spot_type_nm, 
            spot_nm, spot_id, spot_desc, owner_nm, 
            dimension_label_txt, channel_nm, created_user_nm, height_width_ratio_val_txt, 
            spot_height_val_no, spot_key, spot_status_cd, spot_version_id )
      select location_selector_flg, multi_page_flg, last_published_dttm, 
            valid_from_dttm, valid_to_dttm, spot_width_val_no, spot_type_nm, 
            spot_nm, spot_id, spot_desc, owner_nm, 
            dimension_label_txt, channel_nm, created_user_nm, height_width_ratio_val_txt, 
            spot_height_val_no, spot_key, spot_status_cd, spot_version_id
         from &udmmart..md_spot ;
      quit;
   %end;
   %err_check (Failed to insert into md_spot , md_spot );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_SPOT;
         drop table work.MD_SPOT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_SPOT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_SPOT_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_SPOT_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_SPOT_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_spot_all  base=&trglib..md_spot_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_SPOT_ALL (
            multi_page_flg, location_selector_flg, valid_from_dttm, 
            valid_to_dttm, last_published_dttm, spot_version_id, spot_status_cd, 
            spot_nm, spot_key, spot_height_val_no, owner_nm, 
            height_width_ratio_val_txt, created_user_nm, channel_nm, dimension_label_txt, 
            spot_desc, spot_id, spot_type_nm, spot_width_val_no )
      select multi_page_flg, location_selector_flg, valid_from_dttm, 
            valid_to_dttm, last_published_dttm, spot_version_id, spot_status_cd, 
            spot_nm, spot_key, spot_height_val_no, owner_nm, 
            height_width_ratio_val_txt, created_user_nm, channel_nm, dimension_label_txt, 
            spot_desc, spot_id, spot_type_nm, spot_width_val_no
         from &udmmart..md_spot_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_spot_all , md_spot_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_SPOT_ALL;
         drop table work.MD_SPOT_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_SPOT_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_TARGET_ASSIST)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_TARGET_ASSIST));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_TARGET_ASSIST) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_target_assist  base=&trglib..md_target_assist (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_TARGET_ASSIST (
            use_targeting_flg, threshold_type_nm, percent_target_population_size, 
            last_modified_dttm, model_available_dttm, task_id )
      select use_targeting_flg, threshold_type_nm, percent_target_population_size, 
            last_modified_dttm, model_available_dttm, task_id
         from &udmmart..md_target_assist ;
      quit;
   %end;
   %err_check (Failed to insert into md_target_assist , md_target_assist );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_TARGET_ASSIST;
         drop table work.MD_TARGET_ASSIST;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_TARGET_ASSIST;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_TASK)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_TASK));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_TASK) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_task  base=&trglib..md_task (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_TASK (
            activity_flg, scheduled_flg, export_template_flg, 
            rtdm_flg, use_modeling_flg, recurring_schedule_flg, segment_tests_flg, 
            impressions_life_time_cnt, test_duration, limit_period_unit_cnt, impressions_per_session_cnt, 
            display_priority_no, recurrence_day_of_month_no, impressions_qty_period_cnt, maximum_period_expression_cnt, 
            valid_from_dttm, scheduled_start_dttm, last_published_dttm, valid_to_dttm, 
            rec_scheduled_start_dttm, model_start_dttm, scheduled_end_dttm, rec_scheduled_end_dttm, 
            task_version_id, task_subtype_nm, task_nm, task_desc, 
            task_cd, subject_line_txt, stratified_sampling_action_nm, send_notification_locale_cd, 
            secondary_status, recurrence_frequency_cd, recurrence_day_of_wk_ordinal_no, recurrence_day_of_week_txt, 
            rec_scheduled_start_tm, period_type_nm, owner_nm, mobile_app_id, 
            folder_path_nm, delivery_config_type_nm, control_group_action_nm, business_context_id, 
            arbitration_method_cd, channel_nm, created_user_nm, mobile_app_nm, 
            recurrence_days_of_week_txt, recurrence_monthly_type_nm, subject_line_source_nm, task_category_nm, 
            task_delivery_type_nm, task_id, task_status_cd, task_type_nm, 
            template_id )
      select activity_flg, scheduled_flg, export_template_flg, 
            rtdm_flg, use_modeling_flg, recurring_schedule_flg, segment_tests_flg, 
            impressions_life_time_cnt, test_duration, limit_period_unit_cnt, impressions_per_session_cnt, 
            display_priority_no, recurrence_day_of_month_no, impressions_qty_period_cnt, maximum_period_expression_cnt, 
            valid_from_dttm, scheduled_start_dttm, last_published_dttm, valid_to_dttm, 
            rec_scheduled_start_dttm, model_start_dttm, scheduled_end_dttm, rec_scheduled_end_dttm, 
            task_version_id, task_subtype_nm, task_nm, task_desc, 
            task_cd, subject_line_txt, stratified_sampling_action_nm, send_notification_locale_cd, 
            secondary_status, recurrence_frequency_cd, recurrence_day_of_wk_ordinal_no, recurrence_day_of_week_txt, 
            rec_scheduled_start_tm, period_type_nm, owner_nm, mobile_app_id, 
            folder_path_nm, delivery_config_type_nm, control_group_action_nm, business_context_id, 
            arbitration_method_cd, channel_nm, created_user_nm, mobile_app_nm, 
            recurrence_days_of_week_txt, recurrence_monthly_type_nm, subject_line_source_nm, task_category_nm, 
            task_delivery_type_nm, task_id, task_status_cd, task_type_nm, 
            template_id
         from &udmmart..md_task ;
      quit;
   %end;
   %err_check (Failed to insert into md_task , md_task );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_TASK;
         drop table work.MD_TASK;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_TASK;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_TASK_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_TASK_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_TASK_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_task_all  base=&trglib..md_task_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_TASK_ALL (
            use_modeling_flg, activity_flg, recurring_schedule_flg, 
            rtdm_flg, scheduled_flg, export_template_flg, segment_tests_flg, 
            recurrence_day_of_month_no, impressions_per_session_cnt, test_duration, display_priority_no, 
            limit_period_unit_cnt, maximum_period_expression_cnt, impressions_qty_period_cnt, impressions_life_time_cnt, 
            last_published_dttm, model_start_dttm, scheduled_start_dttm, rec_scheduled_end_dttm, 
            rec_scheduled_start_dttm, scheduled_end_dttm, valid_from_dttm, valid_to_dttm, 
            template_id, task_version_id, task_subtype_nm, task_nm, 
            task_desc, task_cd, subject_line_txt, stratified_sampling_action_nm, 
            send_notification_locale_cd, secondary_status, recurrence_frequency_cd, recurrence_day_of_wk_ordinal_no, 
            recurrence_day_of_week_txt, rec_scheduled_start_tm, period_type_nm, owner_nm, 
            mobile_app_id, folder_path_nm, delivery_config_type_nm, control_group_action_nm, 
            channel_nm, business_context_id, arbitration_method_cd, created_user_nm, 
            mobile_app_nm, recurrence_days_of_week_txt, recurrence_monthly_type_nm, subject_line_source_nm, 
            task_category_nm, task_delivery_type_nm, task_id, task_status_cd, 
            task_type_nm )
      select use_modeling_flg, activity_flg, recurring_schedule_flg, 
            rtdm_flg, scheduled_flg, export_template_flg, segment_tests_flg, 
            recurrence_day_of_month_no, impressions_per_session_cnt, test_duration, display_priority_no, 
            limit_period_unit_cnt, maximum_period_expression_cnt, impressions_qty_period_cnt, impressions_life_time_cnt, 
            last_published_dttm, model_start_dttm, scheduled_start_dttm, rec_scheduled_end_dttm, 
            rec_scheduled_start_dttm, scheduled_end_dttm, valid_from_dttm, valid_to_dttm, 
            template_id, task_version_id, task_subtype_nm, task_nm, 
            task_desc, task_cd, subject_line_txt, stratified_sampling_action_nm, 
            send_notification_locale_cd, secondary_status, recurrence_frequency_cd, recurrence_day_of_wk_ordinal_no, 
            recurrence_day_of_week_txt, rec_scheduled_start_tm, period_type_nm, owner_nm, 
            mobile_app_id, folder_path_nm, delivery_config_type_nm, control_group_action_nm, 
            channel_nm, business_context_id, arbitration_method_cd, created_user_nm, 
            mobile_app_nm, recurrence_days_of_week_txt, recurrence_monthly_type_nm, subject_line_source_nm, 
            task_category_nm, task_delivery_type_nm, task_id, task_status_cd, 
            task_type_nm
         from &udmmart..md_task_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_task_all , md_task_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_TASK_ALL;
         drop table work.MD_TASK_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_TASK_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_TASK_CUSTOM_PROP)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_TASK_CUSTOM_PROP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_TASK_CUSTOM_PROP) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_task_custom_prop  base=&trglib..md_task_custom_prop (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_TASK_CUSTOM_PROP (
            valid_from_dttm, valid_to_dttm, task_status_cd, 
            task_id, property_val, property_datatype_nm, property_nm, 
            task_version_id )
      select valid_from_dttm, valid_to_dttm, task_status_cd, 
            task_id, property_val, property_datatype_nm, property_nm, 
            task_version_id
         from &udmmart..md_task_custom_prop ;
      quit;
   %end;
   %err_check (Failed to insert into md_task_custom_prop , md_task_custom_prop );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_TASK_CUSTOM_PROP;
         drop table work.MD_TASK_CUSTOM_PROP;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_TASK_CUSTOM_PROP;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_TASK_CUSTOM_PROP_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_TASK_CUSTOM_PROP_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_TASK_CUSTOM_PROP_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_task_custom_prop_all  base=&trglib..md_task_custom_prop_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_TASK_CUSTOM_PROP_ALL (
            valid_from_dttm, valid_to_dttm, task_version_id, 
            task_status_cd, task_id, property_val, property_datatype_nm, 
            property_nm )
      select valid_from_dttm, valid_to_dttm, task_version_id, 
            task_status_cd, task_id, property_val, property_datatype_nm, 
            property_nm
         from &udmmart..md_task_custom_prop_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_task_custom_prop_all , md_task_custom_prop_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_TASK_CUSTOM_PROP_ALL;
         drop table work.MD_TASK_CUSTOM_PROP_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_TASK_CUSTOM_PROP_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_TASK_X_AUDIENCE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_TASK_X_AUDIENCE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_TASK_X_AUDIENCE) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_task_x_audience  base=&trglib..md_task_x_audience (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_TASK_X_AUDIENCE (
            audience_id, task_id )
      select audience_id, task_id
         from &udmmart..md_task_x_audience ;
      quit;
   %end;
   %err_check (Failed to insert into md_task_x_audience , md_task_x_audience );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_TASK_X_AUDIENCE;
         drop table work.MD_TASK_X_AUDIENCE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_TASK_X_AUDIENCE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_TASK_X_CREATIVE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_TASK_X_CREATIVE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_TASK_X_CREATIVE) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_task_x_creative  base=&trglib..md_task_x_creative (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_TASK_X_CREATIVE (
            variant_nm, task_version_id, task_id, 
            creative_id, arbitration_method_cd, arbitration_method_val, spot_id, 
            task_status_cd, variant_id )
      select variant_nm, task_version_id, task_id, 
            creative_id, arbitration_method_cd, arbitration_method_val, spot_id, 
            task_status_cd, variant_id
         from &udmmart..md_task_x_creative ;
      quit;
   %end;
   %err_check (Failed to insert into md_task_x_creative , md_task_x_creative );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_TASK_X_CREATIVE;
         drop table work.MD_TASK_X_CREATIVE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_TASK_X_CREATIVE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_TASK_X_CREATIVE_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_TASK_X_CREATIVE_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_TASK_X_CREATIVE_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_task_x_creative_all  base=&trglib..md_task_x_creative_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_TASK_X_CREATIVE_ALL (
            variant_nm, variant_id, task_status_cd, 
            spot_id, arbitration_method_val, arbitration_method_cd, creative_id, 
            task_id, task_version_id )
      select variant_nm, variant_id, task_status_cd, 
            spot_id, arbitration_method_val, arbitration_method_cd, creative_id, 
            task_id, task_version_id
         from &udmmart..md_task_x_creative_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_task_x_creative_all , md_task_x_creative_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_TASK_X_CREATIVE_ALL;
         drop table work.MD_TASK_X_CREATIVE_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_TASK_X_CREATIVE_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_TASK_X_DATAVIEW)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_TASK_X_DATAVIEW));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_TASK_X_DATAVIEW) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_task_x_dataview  base=&trglib..md_task_x_dataview (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_TASK_X_DATAVIEW (
            targeting_flg, primary_metric_flg, secondary_metric_flg, 
            task_version_id, task_id, dataview_id, task_status_cd )
      select targeting_flg, primary_metric_flg, secondary_metric_flg, 
            task_version_id, task_id, dataview_id, task_status_cd
         from &udmmart..md_task_x_dataview ;
      quit;
   %end;
   %err_check (Failed to insert into md_task_x_dataview , md_task_x_dataview );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_TASK_X_DATAVIEW;
         drop table work.MD_TASK_X_DATAVIEW;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_TASK_X_DATAVIEW;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_TASK_X_DATAVIEW_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_TASK_X_DATAVIEW_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_TASK_X_DATAVIEW_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_task_x_dataview_all  base=&trglib..md_task_x_dataview_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_TASK_X_DATAVIEW_ALL (
            targeting_flg, primary_metric_flg, secondary_metric_flg, 
            task_status_cd, task_id, dataview_id, task_version_id )
      select targeting_flg, primary_metric_flg, secondary_metric_flg, 
            task_status_cd, task_id, dataview_id, task_version_id
         from &udmmart..md_task_x_dataview_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_task_x_dataview_all , md_task_x_dataview_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_TASK_X_DATAVIEW_ALL;
         drop table work.MD_TASK_X_DATAVIEW_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_TASK_X_DATAVIEW_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_TASK_X_EVENT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_TASK_X_EVENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_TASK_X_EVENT) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_task_x_event  base=&trglib..md_task_x_event (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_TASK_X_EVENT (
            targeting_flg, primary_metric_flg, secondary_metric_flg, 
            task_version_id, task_id, event_id, task_status_cd )
      select targeting_flg, primary_metric_flg, secondary_metric_flg, 
            task_version_id, task_id, event_id, task_status_cd
         from &udmmart..md_task_x_event ;
      quit;
   %end;
   %err_check (Failed to insert into md_task_x_event , md_task_x_event );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_TASK_X_EVENT;
         drop table work.MD_TASK_X_EVENT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_TASK_X_EVENT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_TASK_X_EVENT_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_TASK_X_EVENT_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_TASK_X_EVENT_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_task_x_event_all  base=&trglib..md_task_x_event_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_TASK_X_EVENT_ALL (
            secondary_metric_flg, targeting_flg, primary_metric_flg, 
            task_status_cd, task_id, event_id, task_version_id )
      select secondary_metric_flg, targeting_flg, primary_metric_flg, 
            task_status_cd, task_id, event_id, task_version_id
         from &udmmart..md_task_x_event_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_task_x_event_all , md_task_x_event_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_TASK_X_EVENT_ALL;
         drop table work.MD_TASK_X_EVENT_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_TASK_X_EVENT_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_TASK_X_MESSAGE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_TASK_X_MESSAGE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_TASK_X_MESSAGE) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_task_x_message  base=&trglib..md_task_x_message (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_TASK_X_MESSAGE (
            task_version_id, task_status_cd, message_id, 
            task_id )
      select task_version_id, task_status_cd, message_id, 
            task_id
         from &udmmart..md_task_x_message ;
      quit;
   %end;
   %err_check (Failed to insert into md_task_x_message , md_task_x_message );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_TASK_X_MESSAGE;
         drop table work.MD_TASK_X_MESSAGE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_TASK_X_MESSAGE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_TASK_X_MESSAGE_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_TASK_X_MESSAGE_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_TASK_X_MESSAGE_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_task_x_message_all  base=&trglib..md_task_x_message_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_TASK_X_MESSAGE_ALL (
            task_status_cd, message_id, task_id, 
            task_version_id )
      select task_status_cd, message_id, task_id, 
            task_version_id
         from &udmmart..md_task_x_message_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_task_x_message_all , md_task_x_message_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_TASK_X_MESSAGE_ALL;
         drop table work.MD_TASK_X_MESSAGE_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_TASK_X_MESSAGE_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_TASK_X_SEGMENT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_TASK_X_SEGMENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_TASK_X_SEGMENT) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_task_x_segment  base=&trglib..md_task_x_segment (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_TASK_X_SEGMENT (
            task_version_id, task_status_cd, segment_id, 
            task_id )
      select task_version_id, task_status_cd, segment_id, 
            task_id
         from &udmmart..md_task_x_segment ;
      quit;
   %end;
   %err_check (Failed to insert into md_task_x_segment , md_task_x_segment );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_TASK_X_SEGMENT;
         drop table work.MD_TASK_X_SEGMENT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_TASK_X_SEGMENT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_TASK_X_SEGMENT_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_TASK_X_SEGMENT_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_TASK_X_SEGMENT_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_task_x_segment_all  base=&trglib..md_task_x_segment_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_TASK_X_SEGMENT_ALL (
            task_status_cd, segment_id, task_id, 
            task_version_id )
      select task_status_cd, segment_id, task_id, 
            task_version_id
         from &udmmart..md_task_x_segment_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_task_x_segment_all , md_task_x_segment_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_TASK_X_SEGMENT_ALL;
         drop table work.MD_TASK_X_SEGMENT_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_TASK_X_SEGMENT_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_TASK_X_SPOT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_TASK_X_SPOT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_TASK_X_SPOT) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_task_x_spot  base=&trglib..md_task_x_spot (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_TASK_X_SPOT (
            task_status_cd, spot_id, task_id, 
            task_version_id )
      select task_status_cd, spot_id, task_id, 
            task_version_id
         from &udmmart..md_task_x_spot ;
      quit;
   %end;
   %err_check (Failed to insert into md_task_x_spot , md_task_x_spot );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_TASK_X_SPOT;
         drop table work.MD_TASK_X_SPOT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_TASK_X_SPOT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_TASK_X_SPOT_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_TASK_X_SPOT_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_TASK_X_SPOT_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_task_x_spot_all  base=&trglib..md_task_x_spot_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_TASK_X_SPOT_ALL (
            task_version_id, task_status_cd, spot_id, 
            task_id )
      select task_version_id, task_status_cd, spot_id, 
            task_id
         from &udmmart..md_task_x_spot_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_task_x_spot_all , md_task_x_spot_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_TASK_X_SPOT_ALL;
         drop table work.MD_TASK_X_SPOT_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_TASK_X_SPOT_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_TASK_X_VARIANT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_TASK_X_VARIANT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_TASK_X_VARIANT) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_task_x_variant  base=&trglib..md_task_x_variant (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_TASK_X_VARIANT (
            variant_type_nm, variant_nm, task_status_cd, 
            analysis_group_id, task_id, task_version_id, variant_source_nm, 
            variant_val )
      select variant_type_nm, variant_nm, task_status_cd, 
            analysis_group_id, task_id, task_version_id, variant_source_nm, 
            variant_val
         from &udmmart..md_task_x_variant ;
      quit;
   %end;
   %err_check (Failed to insert into md_task_x_variant , md_task_x_variant );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_TASK_X_VARIANT;
         drop table work.MD_TASK_X_VARIANT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_TASK_X_VARIANT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_TASK_X_VARIANT_ALL)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_TASK_X_VARIANT_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_TASK_X_VARIANT_ALL) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_task_x_variant_all  base=&trglib..md_task_x_variant_all (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_TASK_X_VARIANT_ALL (
            variant_type_nm, variant_nm, task_status_cd, 
            analysis_group_id, task_id, task_version_id, variant_source_nm, 
            variant_val )
      select variant_type_nm, variant_nm, task_status_cd, 
            analysis_group_id, task_id, task_version_id, variant_source_nm, 
            variant_val
         from &udmmart..md_task_x_variant_all ;
      quit;
   %end;
   %err_check (Failed to insert into md_task_x_variant_all , md_task_x_variant_all );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_TASK_X_VARIANT_ALL;
         drop table work.MD_TASK_X_VARIANT_ALL;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_TASK_X_VARIANT_ALL;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_VENDOR)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_VENDOR));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_VENDOR) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_vendor  base=&trglib..md_vendor (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_VENDOR (
            is_obsolete_flg, last_modified_dttm, load_dttm, 
            created_dttm, vendor_number, vendor_nm, vendor_desc, 
            owner_usernm, last_modified_usernm, created_by_usernm, vendor_currency_cd, 
            vendor_id )
      select is_obsolete_flg, last_modified_dttm, load_dttm, 
            created_dttm, vendor_number, vendor_nm, vendor_desc, 
            owner_usernm, last_modified_usernm, created_by_usernm, vendor_currency_cd, 
            vendor_id
         from &udmmart..md_vendor ;
      quit;
   %end;
   %err_check (Failed to insert into md_vendor , md_vendor );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_VENDOR;
         drop table work.MD_VENDOR;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_VENDOR;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_WF_PROCESS_DEF)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_WF_PROCESS_DEF));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_WF_PROCESS_DEF) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_wf_process_def  base=&trglib..md_wf_process_def (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_WF_PROCESS_DEF (
            version_num, file_tobecatlgd_flg, default_approval_flg, 
            buildin_template_flg, latest_version_flg, last_modified_dttm, load_dttm, 
            created_dttm, pdef_state, pdef_nm, pdef_id, 
            owner_usernm, last_modified_usernm, engine_pdef_key, engine_pdef_id, 
            created_by_usernm, associated_object_type, pdef_desc, pdef_type )
      select version_num, file_tobecatlgd_flg, default_approval_flg, 
            buildin_template_flg, latest_version_flg, last_modified_dttm, load_dttm, 
            created_dttm, pdef_state, pdef_nm, pdef_id, 
            owner_usernm, last_modified_usernm, engine_pdef_key, engine_pdef_id, 
            created_by_usernm, associated_object_type, pdef_desc, pdef_type
         from &udmmart..md_wf_process_def ;
      quit;
   %end;
   %err_check (Failed to insert into md_wf_process_def , md_wf_process_def );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_WF_PROCESS_DEF;
         drop table work.MD_WF_PROCESS_DEF;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_WF_PROCESS_DEF;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_WF_PROCESS_DEF_ATTR_GRP)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_WF_PROCESS_DEF_ATTR_GRP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_WF_PROCESS_DEF_ATTR_GRP) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_wf_process_def_attr_grp  base=&trglib..md_wf_process_def_attr_grp (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_WF_PROCESS_DEF_ATTR_GRP (
            load_dttm, pdef_id, attr_group_id )
      select load_dttm, pdef_id, attr_group_id
         from &udmmart..md_wf_process_def_attr_grp ;
      quit;
   %end;
   %err_check (Failed to insert into md_wf_process_def_attr_grp , md_wf_process_def_attr_grp );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_WF_PROCESS_DEF_ATTR_GRP;
         drop table work.MD_WF_PROCESS_DEF_ATTR_GRP;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_WF_PROCESS_DEF_ATTR_GRP;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_WF_PROCESS_DEF_CATEGORIES)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_WF_PROCESS_DEF_CATEGORIES));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_WF_PROCESS_DEF_CATEGORIES) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_wf_process_def_categories  base=&trglib..md_wf_process_def_categories (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_WF_PROCESS_DEF_CATEGORIES (
            default_category_flg, load_dttm, pdef_id, 
            category_type, category_id )
      select default_category_flg, load_dttm, pdef_id, 
            category_type, category_id
         from &udmmart..md_wf_process_def_categories ;
      quit;
   %end;
   %err_check (Failed to insert into md_wf_process_def_categories , md_wf_process_def_categories );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_WF_PROCESS_DEF_CATEGORIES;
         drop table work.MD_WF_PROCESS_DEF_CATEGORIES;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_WF_PROCESS_DEF_CATEGORIES;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_WF_PROCESS_DEF_TASKS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_WF_PROCESS_DEF_TASKS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_WF_PROCESS_DEF_TASKS) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_wf_process_def_tasks  base=&trglib..md_wf_process_def_tasks (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_WF_PROCESS_DEF_TASKS (
            default_duration_perassignee, file_enabled_flg, is_sequential_flg, 
            outgoing_flow_flg, resp_enabled_flg, show_workflowlink_flg, ciobject_enabled_flg, 
            file_mandatory_flg, url_enabled_flg, multiple_asgnsuprt_flg, comment_mandatory_flg, 
            comment_enabled_flg, resp_file_enabled_flg, res_mandatory_flg, show_sourceitemlink_flg, 
            load_dttm, task_type, task_subtype, task_instruction, 
            task_desc, source_item_field, predecessor_task_id, pdef_id, 
            item_approval_state, assignee_type, task_id, task_nm )
      select default_duration_perassignee, file_enabled_flg, is_sequential_flg, 
            outgoing_flow_flg, resp_enabled_flg, show_workflowlink_flg, ciobject_enabled_flg, 
            file_mandatory_flg, url_enabled_flg, multiple_asgnsuprt_flg, comment_mandatory_flg, 
            comment_enabled_flg, resp_file_enabled_flg, res_mandatory_flg, show_sourceitemlink_flg, 
            load_dttm, task_type, task_subtype, task_instruction, 
            task_desc, source_item_field, predecessor_task_id, pdef_id, 
            item_approval_state, assignee_type, task_id, task_nm
         from &udmmart..md_wf_process_def_tasks ;
      quit;
   %end;
   %err_check (Failed to insert into md_wf_process_def_tasks , md_wf_process_def_tasks );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_WF_PROCESS_DEF_TASKS;
         drop table work.MD_WF_PROCESS_DEF_TASKS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_WF_PROCESS_DEF_TASKS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MD_WF_PROCESS_DEF_TASK_ASSG)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MD_WF_PROCESS_DEF_TASK_ASSG));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..MD_WF_PROCESS_DEF_TASK_ASSG) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..md_wf_process_def_task_assg  base=&trglib..md_wf_process_def_task_assg (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..MD_WF_PROCESS_DEF_TASK_ASSG (
            load_dttm, pdef_id, assignee_type, 
            assignee_id, assignee_duration, assignee_instruction, task_id )
      select load_dttm, pdef_id, assignee_type, 
            assignee_id, assignee_duration, assignee_instruction, task_id
         from &udmmart..md_wf_process_def_task_assg ;
      quit;
   %end;
   %err_check (Failed to insert into md_wf_process_def_task_assg , md_wf_process_def_task_assg );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MD_WF_PROCESS_DEF_TASK_ASSG;
         drop table work.MD_WF_PROCESS_DEF_TASK_ASSG;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MD_WF_PROCESS_DEF_TASK_ASSG;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MEDIA_ACTIVITY_DETAILS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MEDIA_ACTIVITY_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..media_activity_details_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..media_activity_details_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=media_activity_details , table_keys=%str(EVENT_ID), out_table=work.media_activity_details );
   data work.media_activity_details_tmp /view=work.media_activity_details_tmp ;
      set work.media_activity_details ;
      if action_dttm_tz  ne . then action_dttm_tz = tzoneu2s(action_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :media_activity_details_tmp , media_activity_details );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..media_activity_details_tmp ;
            set work.media_activity_details_tmp ;
            stop;
         run;
         proc append data=work.media_activity_details_tmp  base=&tmplib..media_activity_details_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..media_activity_details_tmp ;
            set work.media_activity_details_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :media_activity_details_tmp , media_activity_details );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..media_activity_details as b using &tmpdbschema..media_activity_details_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.action_dttm = d.action_dttm, 
            b.action_dttm_tz = d.action_dttm_tz, b.load_dttm = d.load_dttm, 
            b.playhead_position = d.playhead_position, b.media_nm = d.media_nm, 
            b.detail_id = d.detail_id, b.action = d.action, 
            b.detail_id_hex = d.detail_id_hex, b.media_uri_txt = d.media_uri_txt
         when not matched then insert ( 
            action_dttm, action_dttm_tz, load_dttm, 
            playhead_position, media_nm, event_id, detail_id, 
            action, detail_id_hex, media_uri_txt
         ) values ( 
            d.action_dttm, d.action_dttm_tz, d.load_dttm, 
            d.playhead_position, d.media_nm, d.event_id, d.detail_id, 
            d.action, d.detail_id_hex, d.media_uri_txt  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :media_activity_details_tmp , media_activity_details , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..media_activity_details_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..media_activity_details_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MEDIA_ACTIVITY_DETAILS;
         drop table work.MEDIA_ACTIVITY_DETAILS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MEDIA_ACTIVITY_DETAILS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MEDIA_DETAILS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MEDIA_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..media_details_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..media_details_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=media_details , table_keys=%str(EVENT_ID), out_table=work.media_details );
   data work.media_details_tmp /view=work.media_details_tmp ;
      set work.media_details ;
      if play_start_dttm_tz  ne . then play_start_dttm_tz = tzoneu2s(play_start_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :media_details_tmp , media_details );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..media_details_tmp ;
            set work.media_details_tmp ;
            stop;
         run;
         proc append data=work.media_details_tmp  base=&tmplib..media_details_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..media_details_tmp ;
            set work.media_details_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :media_details_tmp , media_details );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..media_details as b using &tmpdbschema..media_details_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.media_duration_secs = d.media_duration_secs, 
            b.load_dttm = d.load_dttm, b.play_start_dttm_tz = d.play_start_dttm_tz, 
            b.play_start_dttm = d.play_start_dttm, b.visit_id_hex = d.visit_id_hex, 
            b.visit_id = d.visit_id, b.session_id_hex = d.session_id_hex, 
            b.session_id = d.session_id, b.media_uri_txt = d.media_uri_txt, 
            b.media_player_nm = d.media_player_nm, b.media_nm = d.media_nm, 
            b.identity_id = d.identity_id, b.event_key_cd = d.event_key_cd, 
            b.detail_id_hex = d.detail_id_hex, b.detail_id = d.detail_id, 
            b.event_source_cd = d.event_source_cd, b.media_player_version_txt = d.media_player_version_txt
         when not matched then insert ( 
            media_duration_secs, load_dttm, play_start_dttm_tz, 
            play_start_dttm, visit_id_hex, visit_id, session_id_hex, 
            session_id, media_uri_txt, media_player_nm, media_nm, 
            identity_id, event_key_cd, detail_id_hex, detail_id, 
            event_id, event_source_cd, media_player_version_txt
         ) values ( 
            d.media_duration_secs, d.load_dttm, d.play_start_dttm_tz, 
            d.play_start_dttm, d.visit_id_hex, d.visit_id, d.session_id_hex, 
            d.session_id, d.media_uri_txt, d.media_player_nm, d.media_nm, 
            d.identity_id, d.event_key_cd, d.detail_id_hex, d.detail_id, 
            d.event_id, d.event_source_cd, d.media_player_version_txt  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :media_details_tmp , media_details , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..media_details_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..media_details_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MEDIA_DETAILS;
         drop table work.MEDIA_DETAILS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MEDIA_DETAILS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MEDIA_DETAILS_EXT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MEDIA_DETAILS_EXT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..media_details_ext_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..media_details_ext_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=media_details_ext , table_keys=%str(EVENT_ID), out_table=work.media_details_ext );
   data work.media_details_ext_tmp /view=work.media_details_ext_tmp ;
      set work.media_details_ext ;
      if play_end_dttm_tz  ne . then play_end_dttm_tz = tzoneu2s(play_end_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :media_details_ext_tmp , media_details_ext );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..media_details_ext_tmp ;
            set work.media_details_ext_tmp ;
            stop;
         run;
         proc append data=work.media_details_ext_tmp  base=&tmplib..media_details_ext_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..media_details_ext_tmp ;
            set work.media_details_ext_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :media_details_ext_tmp , media_details_ext );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..media_details_ext as b using &tmpdbschema..media_details_ext_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.media_display_duration_secs = d.media_display_duration_secs, 
            b.view_duration_secs = d.view_duration_secs, b.end_tm = d.end_tm, 
            b.start_tm = d.start_tm, b.exit_point_secs = d.exit_point_secs, 
            b.max_play_secs = d.max_play_secs, b.interaction_cnt = d.interaction_cnt, 
            b.play_end_dttm = d.play_end_dttm, b.play_end_dttm_tz = d.play_end_dttm_tz, 
            b.load_dttm = d.load_dttm, b.media_uri_txt = d.media_uri_txt, 
            b.media_nm = d.media_nm, b.detail_id_hex = d.detail_id_hex, 
            b.detail_id = d.detail_id
         when not matched then insert ( 
            media_display_duration_secs, view_duration_secs, end_tm, 
            start_tm, exit_point_secs, max_play_secs, interaction_cnt, 
            play_end_dttm, play_end_dttm_tz, load_dttm, media_uri_txt, 
            media_nm, event_id, detail_id_hex, detail_id
         ) values ( 
            d.media_display_duration_secs, d.view_duration_secs, d.end_tm, 
            d.start_tm, d.exit_point_secs, d.max_play_secs, d.interaction_cnt, 
            d.play_end_dttm, d.play_end_dttm_tz, d.load_dttm, d.media_uri_txt, 
            d.media_nm, d.event_id, d.detail_id_hex, d.detail_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :media_details_ext_tmp , media_details_ext , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..media_details_ext_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..media_details_ext_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MEDIA_DETAILS_EXT;
         drop table work.MEDIA_DETAILS_EXT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MEDIA_DETAILS_EXT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MOBILE_FOCUS_DEFOCUS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MOBILE_FOCUS_DEFOCUS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..mobile_focus_defocus_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..mobile_focus_defocus_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=mobile_focus_defocus , table_keys=%str(EVENT_ID), out_table=work.mobile_focus_defocus );
   data work.mobile_focus_defocus_tmp /view=work.mobile_focus_defocus_tmp ;
      set work.mobile_focus_defocus ;
      if action_dttm_tz  ne . then action_dttm_tz = tzoneu2s(action_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :mobile_focus_defocus_tmp , mobile_focus_defocus );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..mobile_focus_defocus_tmp ;
            set work.mobile_focus_defocus_tmp ;
            stop;
         run;
         proc append data=work.mobile_focus_defocus_tmp  base=&tmplib..mobile_focus_defocus_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..mobile_focus_defocus_tmp ;
            set work.mobile_focus_defocus_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :mobile_focus_defocus_tmp , mobile_focus_defocus );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..mobile_focus_defocus as b using &tmpdbschema..mobile_focus_defocus_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.action_dttm_tz = d.action_dttm_tz, 
            b.action_dttm = d.action_dttm, b.load_dttm = d.load_dttm, 
            b.visit_id_hex = d.visit_id_hex, b.session_id_hex = d.session_id_hex, 
            b.reserved_1_txt = d.reserved_1_txt, b.mobile_app_id = d.mobile_app_id, 
            b.identity_id = d.identity_id, b.event_nm = d.event_nm, 
            b.event_designed_id = d.event_designed_id, b.detail_id_hex = d.detail_id_hex, 
            b.channel_user_id = d.channel_user_id
         when not matched then insert ( 
            action_dttm_tz, action_dttm, load_dttm, 
            visit_id_hex, session_id_hex, reserved_1_txt, mobile_app_id, 
            identity_id, event_nm, event_designed_id, detail_id_hex, 
            channel_user_id, event_id
         ) values ( 
            d.action_dttm_tz, d.action_dttm, d.load_dttm, 
            d.visit_id_hex, d.session_id_hex, d.reserved_1_txt, d.mobile_app_id, 
            d.identity_id, d.event_nm, d.event_designed_id, d.detail_id_hex, 
            d.channel_user_id, d.event_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :mobile_focus_defocus_tmp , mobile_focus_defocus , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..mobile_focus_defocus_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..mobile_focus_defocus_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MOBILE_FOCUS_DEFOCUS;
         drop table work.MOBILE_FOCUS_DEFOCUS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MOBILE_FOCUS_DEFOCUS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MOBILE_SPOTS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MOBILE_SPOTS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..mobile_spots_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..mobile_spots_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=mobile_spots , table_keys=%str(EVENT_ID), out_table=work.mobile_spots );
   data work.mobile_spots_tmp /view=work.mobile_spots_tmp ;
      set work.mobile_spots ;
      if action_dttm_tz  ne . then action_dttm_tz = tzoneu2s(action_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :mobile_spots_tmp , mobile_spots );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..mobile_spots_tmp ;
            set work.mobile_spots_tmp ;
            stop;
         run;
         proc append data=work.mobile_spots_tmp  base=&tmplib..mobile_spots_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..mobile_spots_tmp ;
            set work.mobile_spots_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :mobile_spots_tmp , mobile_spots );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..mobile_spots as b using &tmpdbschema..mobile_spots_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.action_dttm_tz = d.action_dttm_tz, 
            b.action_dttm = d.action_dttm, b.load_dttm = d.load_dttm, 
            b.visit_id_hex = d.visit_id_hex, b.spot_id = d.spot_id, 
            b.session_id_hex = d.session_id_hex, b.mobile_app_id = d.mobile_app_id, 
            b.event_nm = d.event_nm, b.event_designed_id = d.event_designed_id, 
            b.detail_id_hex = d.detail_id_hex, b.creative_id = d.creative_id, 
            b.context_type_nm = d.context_type_nm, b.channel_user_id = d.channel_user_id, 
            b.context_val = d.context_val, b.identity_id = d.identity_id
         when not matched then insert ( 
            action_dttm_tz, action_dttm, load_dttm, 
            visit_id_hex, spot_id, session_id_hex, mobile_app_id, 
            event_nm, event_designed_id, detail_id_hex, creative_id, 
            context_type_nm, channel_user_id, context_val, event_id, 
            identity_id
         ) values ( 
            d.action_dttm_tz, d.action_dttm, d.load_dttm, 
            d.visit_id_hex, d.spot_id, d.session_id_hex, d.mobile_app_id, 
            d.event_nm, d.event_designed_id, d.detail_id_hex, d.creative_id, 
            d.context_type_nm, d.channel_user_id, d.context_val, d.event_id, 
            d.identity_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :mobile_spots_tmp , mobile_spots , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..mobile_spots_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..mobile_spots_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MOBILE_SPOTS;
         drop table work.MOBILE_SPOTS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MOBILE_SPOTS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..MONTHLY_USAGE)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..MONTHLY_USAGE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..monthly_usage_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..monthly_usage_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=monthly_usage , table_keys=%str(EVENT_MONTH), out_table=work.monthly_usage );
   data work.monthly_usage_tmp /view=work.monthly_usage_tmp ;
      set work.monthly_usage ;
   run;
   %err_check (Failed to add time zone adaptation :monthly_usage_tmp , monthly_usage );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..monthly_usage_tmp ;
            set work.monthly_usage_tmp ;
            stop;
         run;
         proc append data=work.monthly_usage_tmp  base=&tmplib..monthly_usage_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..monthly_usage_tmp ;
            set work.monthly_usage_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :monthly_usage_tmp , monthly_usage );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..monthly_usage as b using &tmpdbschema..monthly_usage_tmp as d on( 
            b.event_month = d.event_month )
         when matched then  
         update set 
            b.api_usage_str = d.api_usage_str, 
            b.bc_subjcnt_str = d.bc_subjcnt_str, b.customer_profiles_processed_str = d.customer_profiles_processed_str, 
            b.web_impr_cnt = d.web_impr_cnt, b.web_sesn_cnt = d.web_sesn_cnt, 
            b.mob_sesn_cnt = d.mob_sesn_cnt, b.email_preview_cnt = d.email_preview_cnt, 
            b.outbound_api_cnt = d.outbound_api_cnt, b.facebook_ads_cnt = d.facebook_ads_cnt, 
            b.mobile_push_cnt = d.mobile_push_cnt, b.google_ads_cnt = d.google_ads_cnt, 
            b.audience_usage_cnt = d.audience_usage_cnt, b.plan_users_cnt = d.plan_users_cnt, 
            b.email_send_cnt = d.email_send_cnt, b.linkedin_ads_cnt = d.linkedin_ads_cnt, 
            b.dm_destinations_total_row_cnt = d.dm_destinations_total_row_cnt, b.mob_impr_cnt = d.mob_impr_cnt, 
            b.dm_destinations_total_id_cnt = d.dm_destinations_total_id_cnt, b.mobile_in_app_msg_cnt = d.mobile_in_app_msg_cnt, 
            b.asset_size = d.asset_size, b.db_size = d.db_size, 
            b.admin_user_cnt = d.admin_user_cnt
         when not matched then insert ( 
            api_usage_str, bc_subjcnt_str, customer_profiles_processed_str, 
            web_impr_cnt, web_sesn_cnt, mob_sesn_cnt, email_preview_cnt, 
            outbound_api_cnt, facebook_ads_cnt, mobile_push_cnt, google_ads_cnt, 
            audience_usage_cnt, plan_users_cnt, email_send_cnt, linkedin_ads_cnt, 
            dm_destinations_total_row_cnt, mob_impr_cnt, dm_destinations_total_id_cnt, mobile_in_app_msg_cnt, 
            asset_size, db_size, admin_user_cnt, event_month
         ) values ( 
            d.api_usage_str, d.bc_subjcnt_str, d.customer_profiles_processed_str, 
            d.web_impr_cnt, d.web_sesn_cnt, d.mob_sesn_cnt, d.email_preview_cnt, 
            d.outbound_api_cnt, d.facebook_ads_cnt, d.mobile_push_cnt, d.google_ads_cnt, 
            d.audience_usage_cnt, d.plan_users_cnt, d.email_send_cnt, d.linkedin_ads_cnt, 
            d.dm_destinations_total_row_cnt, d.mob_impr_cnt, d.dm_destinations_total_id_cnt, d.mobile_in_app_msg_cnt, 
            d.asset_size, d.db_size, d.admin_user_cnt, d.event_month  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :monthly_usage_tmp , monthly_usage , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..monthly_usage_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..monthly_usage_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..MONTHLY_USAGE;
         drop table work.MONTHLY_USAGE;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table MONTHLY_USAGE;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..NOTIFICATION_FAILED)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..NOTIFICATION_FAILED));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..notification_failed_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..notification_failed_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=notification_failed , table_keys=%str(EVENT_ID), out_table=work.notification_failed );
   data work.notification_failed_tmp /view=work.notification_failed_tmp ;
      set work.notification_failed ;
      if notification_failed_dttm_tz  ne . then notification_failed_dttm_tz = tzoneu2s(notification_failed_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :notification_failed_tmp , notification_failed );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..notification_failed_tmp ;
            set work.notification_failed_tmp ;
            stop;
         run;
         proc append data=work.notification_failed_tmp  base=&tmplib..notification_failed_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..notification_failed_tmp ;
            set work.notification_failed_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :notification_failed_tmp , notification_failed );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..notification_failed as b using &tmpdbschema..notification_failed_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.properties_map_doc = d.properties_map_doc, 
            b.notification_failed_dttm = d.notification_failed_dttm, b.notification_failed_dttm_tz = d.notification_failed_dttm_tz, 
            b.load_dttm = d.load_dttm, b.task_id = d.task_id, 
            b.segment_version_id = d.segment_version_id, b.segment_id = d.segment_id, 
            b.response_tracking_cd = d.response_tracking_cd, b.occurrence_id = d.occurrence_id, 
            b.message_version_id = d.message_version_id, b.journey_id = d.journey_id, 
            b.event_designed_id = d.event_designed_id, b.creative_id = d.creative_id, 
            b.channel_user_id = d.channel_user_id, b.channel_nm = d.channel_nm, 
            b.aud_occurrence_id = d.aud_occurrence_id, b.context_type_nm = d.context_type_nm, 
            b.error_cd = d.error_cd, b.event_nm = d.event_nm, 
            b.message_id = d.message_id, b.mobile_app_id = d.mobile_app_id, 
            b.reserved_1_txt = d.reserved_1_txt, b.audience_id = d.audience_id, 
            b.context_val = d.context_val, b.creative_version_id = d.creative_version_id, 
            b.error_message_txt = d.error_message_txt, b.identity_id = d.identity_id, 
            b.journey_occurrence_id = d.journey_occurrence_id, b.reserved_2_txt = d.reserved_2_txt, 
            b.spot_id = d.spot_id, b.task_version_id = d.task_version_id
         when not matched then insert ( 
            properties_map_doc, notification_failed_dttm, notification_failed_dttm_tz, 
            load_dttm, task_id, segment_version_id, segment_id, 
            response_tracking_cd, occurrence_id, message_version_id, journey_id, 
            event_designed_id, creative_id, channel_user_id, channel_nm, 
            aud_occurrence_id, context_type_nm, error_cd, event_id, 
            event_nm, message_id, mobile_app_id, reserved_1_txt, 
            audience_id, context_val, creative_version_id, error_message_txt, 
            identity_id, journey_occurrence_id, reserved_2_txt, spot_id, 
            task_version_id
         ) values ( 
            d.properties_map_doc, d.notification_failed_dttm, d.notification_failed_dttm_tz, 
            d.load_dttm, d.task_id, d.segment_version_id, d.segment_id, 
            d.response_tracking_cd, d.occurrence_id, d.message_version_id, d.journey_id, 
            d.event_designed_id, d.creative_id, d.channel_user_id, d.channel_nm, 
            d.aud_occurrence_id, d.context_type_nm, d.error_cd, d.event_id, 
            d.event_nm, d.message_id, d.mobile_app_id, d.reserved_1_txt, 
            d.audience_id, d.context_val, d.creative_version_id, d.error_message_txt, 
            d.identity_id, d.journey_occurrence_id, d.reserved_2_txt, d.spot_id, 
            d.task_version_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :notification_failed_tmp , notification_failed , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..notification_failed_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..notification_failed_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..NOTIFICATION_FAILED;
         drop table work.NOTIFICATION_FAILED;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table NOTIFICATION_FAILED;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..NOTIFICATION_OPENED)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..NOTIFICATION_OPENED));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..notification_opened_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..notification_opened_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=notification_opened , table_keys=%str(EVENT_ID), out_table=work.notification_opened );
   data work.notification_opened_tmp /view=work.notification_opened_tmp ;
      set work.notification_opened ;
      if notification_opened_dttm_tz  ne . then notification_opened_dttm_tz = tzoneu2s(notification_opened_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :notification_opened_tmp , notification_opened );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..notification_opened_tmp ;
            set work.notification_opened_tmp ;
            stop;
         run;
         proc append data=work.notification_opened_tmp  base=&tmplib..notification_opened_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..notification_opened_tmp ;
            set work.notification_opened_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :notification_opened_tmp , notification_opened );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..notification_opened as b using &tmpdbschema..notification_opened_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.properties_map_doc = d.properties_map_doc, 
            b.load_dttm = d.load_dttm, b.notification_opened_dttm_tz = d.notification_opened_dttm_tz, 
            b.notification_opened_dttm = d.notification_opened_dttm, b.task_version_id = d.task_version_id, 
            b.segment_version_id = d.segment_version_id, b.segment_id = d.segment_id, 
            b.reserved_1_txt = d.reserved_1_txt, b.message_id = d.message_id, 
            b.identity_id = d.identity_id, b.event_nm = d.event_nm, 
            b.creative_id = d.creative_id, b.channel_user_id = d.channel_user_id, 
            b.channel_nm = d.channel_nm, b.aud_occurrence_id = d.aud_occurrence_id, 
            b.context_type_nm = d.context_type_nm, b.event_designed_id = d.event_designed_id, 
            b.journey_id = d.journey_id, b.message_version_id = d.message_version_id, 
            b.occurrence_id = d.occurrence_id, b.reserved_3_txt = d.reserved_3_txt, 
            b.spot_id = d.spot_id, b.audience_id = d.audience_id, 
            b.context_val = d.context_val, b.creative_version_id = d.creative_version_id, 
            b.journey_occurrence_id = d.journey_occurrence_id, b.mobile_app_id = d.mobile_app_id, 
            b.reserved_2_txt = d.reserved_2_txt, b.response_tracking_cd = d.response_tracking_cd, 
            b.task_id = d.task_id
         when not matched then insert ( 
            properties_map_doc, load_dttm, notification_opened_dttm_tz, 
            notification_opened_dttm, task_version_id, segment_version_id, segment_id, 
            reserved_1_txt, message_id, identity_id, event_nm, 
            creative_id, channel_user_id, channel_nm, aud_occurrence_id, 
            context_type_nm, event_designed_id, journey_id, message_version_id, 
            occurrence_id, reserved_3_txt, spot_id, audience_id, 
            context_val, creative_version_id, event_id, journey_occurrence_id, 
            mobile_app_id, reserved_2_txt, response_tracking_cd, task_id
         ) values ( 
            d.properties_map_doc, d.load_dttm, d.notification_opened_dttm_tz, 
            d.notification_opened_dttm, d.task_version_id, d.segment_version_id, d.segment_id, 
            d.reserved_1_txt, d.message_id, d.identity_id, d.event_nm, 
            d.creative_id, d.channel_user_id, d.channel_nm, d.aud_occurrence_id, 
            d.context_type_nm, d.event_designed_id, d.journey_id, d.message_version_id, 
            d.occurrence_id, d.reserved_3_txt, d.spot_id, d.audience_id, 
            d.context_val, d.creative_version_id, d.event_id, d.journey_occurrence_id, 
            d.mobile_app_id, d.reserved_2_txt, d.response_tracking_cd, d.task_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :notification_opened_tmp , notification_opened , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..notification_opened_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..notification_opened_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..NOTIFICATION_OPENED;
         drop table work.NOTIFICATION_OPENED;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table NOTIFICATION_OPENED;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..NOTIFICATION_SEND)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..NOTIFICATION_SEND));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..notification_send_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..notification_send_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=notification_send , table_keys=%str(EVENT_ID), out_table=work.notification_send );
   data work.notification_send_tmp /view=work.notification_send_tmp ;
      set work.notification_send ;
      if notification_send_dttm_tz  ne . then notification_send_dttm_tz = tzoneu2s(notification_send_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :notification_send_tmp , notification_send );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..notification_send_tmp ;
            set work.notification_send_tmp ;
            stop;
         run;
         proc append data=work.notification_send_tmp  base=&tmplib..notification_send_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..notification_send_tmp ;
            set work.notification_send_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :notification_send_tmp , notification_send );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..notification_send as b using &tmpdbschema..notification_send_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.properties_map_doc = d.properties_map_doc, 
            b.load_dttm = d.load_dttm, b.notification_send_dttm_tz = d.notification_send_dttm_tz, 
            b.notification_send_dttm = d.notification_send_dttm, b.task_id = d.task_id, 
            b.spot_id = d.spot_id, b.reserved_2_txt = d.reserved_2_txt, 
            b.occurrence_id = d.occurrence_id, b.message_id = d.message_id, 
            b.identity_id = d.identity_id, b.creative_version_id = d.creative_version_id, 
            b.channel_user_id = d.channel_user_id, b.audience_id = d.audience_id, 
            b.context_val = d.context_val, b.journey_id = d.journey_id, 
            b.journey_occurrence_id = d.journey_occurrence_id, b.mobile_app_id = d.mobile_app_id, 
            b.reserved_1_txt = d.reserved_1_txt, b.segment_id = d.segment_id, 
            b.task_version_id = d.task_version_id, b.aud_occurrence_id = d.aud_occurrence_id, 
            b.channel_nm = d.channel_nm, b.context_type_nm = d.context_type_nm, 
            b.creative_id = d.creative_id, b.event_designed_id = d.event_designed_id, 
            b.event_nm = d.event_nm, b.message_version_id = d.message_version_id, 
            b.response_tracking_cd = d.response_tracking_cd, b.segment_version_id = d.segment_version_id
         when not matched then insert ( 
            properties_map_doc, load_dttm, notification_send_dttm_tz, 
            notification_send_dttm, task_id, spot_id, reserved_2_txt, 
            occurrence_id, message_id, identity_id, creative_version_id, 
            channel_user_id, audience_id, context_val, event_id, 
            journey_id, journey_occurrence_id, mobile_app_id, reserved_1_txt, 
            segment_id, task_version_id, aud_occurrence_id, channel_nm, 
            context_type_nm, creative_id, event_designed_id, event_nm, 
            message_version_id, response_tracking_cd, segment_version_id
         ) values ( 
            d.properties_map_doc, d.load_dttm, d.notification_send_dttm_tz, 
            d.notification_send_dttm, d.task_id, d.spot_id, d.reserved_2_txt, 
            d.occurrence_id, d.message_id, d.identity_id, d.creative_version_id, 
            d.channel_user_id, d.audience_id, d.context_val, d.event_id, 
            d.journey_id, d.journey_occurrence_id, d.mobile_app_id, d.reserved_1_txt, 
            d.segment_id, d.task_version_id, d.aud_occurrence_id, d.channel_nm, 
            d.context_type_nm, d.creative_id, d.event_designed_id, d.event_nm, 
            d.message_version_id, d.response_tracking_cd, d.segment_version_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :notification_send_tmp , notification_send , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..notification_send_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..notification_send_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..NOTIFICATION_SEND;
         drop table work.NOTIFICATION_SEND;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table NOTIFICATION_SEND;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..NOTIFICATION_TARGETING_REQUEST)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..NOTIFICATION_TARGETING_REQUEST));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..notification_targeting_reque_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..notification_targeting_reque_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=notification_targeting_request , table_keys=%str(EVENT_ID), out_table=work.notification_targeting_request );
   data work.notification_targeting_reque_tmp /view=work.notification_targeting_reque_tmp ;
      set work.notification_targeting_request ;
      if notification_tgt_req_dttm_tz  ne . then notification_tgt_req_dttm_tz = tzoneu2s(notification_tgt_req_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :notification_targeting_reque_tmp , notification_targeting_request );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..notification_targeting_reque_tmp ;
            set work.notification_targeting_reque_tmp ;
            stop;
         run;
         proc append data=work.notification_targeting_reque_tmp  base=&tmplib..notification_targeting_reque_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..notification_targeting_reque_tmp ;
            set work.notification_targeting_reque_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :notification_targeting_reque_tmp , notification_targeting_request );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..notification_targeting_request as b using &tmpdbschema..notification_targeting_reque_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.eligibility_flg = d.eligibility_flg, 
            b.notification_tgt_req_dttm = d.notification_tgt_req_dttm, b.load_dttm = d.load_dttm, 
            b.notification_tgt_req_dttm_tz = d.notification_tgt_req_dttm_tz, b.task_id = d.task_id, 
            b.mobile_app_id = d.mobile_app_id, b.event_nm = d.event_nm, 
            b.context_val = d.context_val, b.audience_id = d.audience_id, 
            b.channel_user_id = d.channel_user_id, b.event_designed_id = d.event_designed_id, 
            b.journey_id = d.journey_id, b.aud_occurrence_id = d.aud_occurrence_id, 
            b.channel_nm = d.channel_nm, b.context_type_nm = d.context_type_nm, 
            b.identity_id = d.identity_id, b.journey_occurrence_id = d.journey_occurrence_id
         when not matched then insert ( 
            eligibility_flg, notification_tgt_req_dttm, load_dttm, 
            notification_tgt_req_dttm_tz, task_id, mobile_app_id, event_nm, 
            context_val, audience_id, channel_user_id, event_designed_id, 
            journey_id, aud_occurrence_id, channel_nm, context_type_nm, 
            event_id, identity_id, journey_occurrence_id
         ) values ( 
            d.eligibility_flg, d.notification_tgt_req_dttm, d.load_dttm, 
            d.notification_tgt_req_dttm_tz, d.task_id, d.mobile_app_id, d.event_nm, 
            d.context_val, d.audience_id, d.channel_user_id, d.event_designed_id, 
            d.journey_id, d.aud_occurrence_id, d.channel_nm, d.context_type_nm, 
            d.event_id, d.identity_id, d.journey_occurrence_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :notification_targeting_reque_tmp , notification_targeting_request , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..notification_targeting_reque_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..notification_targeting_reque_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..NOTIFICATION_TARGETING_REQUEST;
         drop table work.NOTIFICATION_TARGETING_REQUEST;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table NOTIFICATION_TARGETING_REQUEST;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..ORDER_DETAILS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..ORDER_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..order_details_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..order_details_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=order_details , table_keys=%str(EVENT_ID), out_table=work.order_details );
   data work.order_details_tmp /view=work.order_details_tmp ;
      set work.order_details ;
      if activity_dttm_tz  ne . then activity_dttm_tz = tzoneu2s(activity_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :order_details_tmp , order_details );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..order_details_tmp ;
            set work.order_details_tmp ;
            stop;
         run;
         proc append data=work.order_details_tmp  base=&tmplib..order_details_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..order_details_tmp ;
            set work.order_details_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :order_details_tmp , order_details );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..order_details as b using &tmpdbschema..order_details_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.unit_price_amt = d.unit_price_amt, 
            b.quantity_amt = d.quantity_amt, b.properties_map_doc = d.properties_map_doc, 
            b.load_dttm = d.load_dttm, b.activity_dttm = d.activity_dttm, 
            b.activity_dttm_tz = d.activity_dttm_tz, b.visit_id = d.visit_id, 
            b.session_id = d.session_id, b.record_type = d.record_type, 
            b.product_id = d.product_id, b.mobile_app_id = d.mobile_app_id, 
            b.event_nm = d.event_nm, b.event_key_cd = d.event_key_cd, 
            b.detail_id = d.detail_id, b.cart_id = d.cart_id, 
            b.availability_message_txt = d.availability_message_txt, b.channel_nm = d.channel_nm, 
            b.event_designed_id = d.event_designed_id, b.event_source_cd = d.event_source_cd, 
            b.order_id = d.order_id, b.product_nm = d.product_nm, 
            b.product_sku = d.product_sku, b.reserved_1_txt = d.reserved_1_txt, 
            b.session_id_hex = d.session_id_hex, b.shipping_message_txt = d.shipping_message_txt, 
            b.cart_nm = d.cart_nm, b.currency_cd = d.currency_cd, 
            b.detail_id_hex = d.detail_id_hex, b.identity_id = d.identity_id, 
            b.product_group_nm = d.product_group_nm, b.saving_message_txt = d.saving_message_txt, 
            b.visit_id_hex = d.visit_id_hex
         when not matched then insert ( 
            unit_price_amt, quantity_amt, properties_map_doc, 
            load_dttm, activity_dttm, activity_dttm_tz, visit_id, 
            session_id, record_type, product_id, mobile_app_id, 
            event_nm, event_key_cd, detail_id, cart_id, 
            availability_message_txt, channel_nm, event_designed_id, event_source_cd, 
            order_id, product_nm, product_sku, reserved_1_txt, 
            session_id_hex, shipping_message_txt, cart_nm, currency_cd, 
            detail_id_hex, event_id, identity_id, product_group_nm, 
            saving_message_txt, visit_id_hex
         ) values ( 
            d.unit_price_amt, d.quantity_amt, d.properties_map_doc, 
            d.load_dttm, d.activity_dttm, d.activity_dttm_tz, d.visit_id, 
            d.session_id, d.record_type, d.product_id, d.mobile_app_id, 
            d.event_nm, d.event_key_cd, d.detail_id, d.cart_id, 
            d.availability_message_txt, d.channel_nm, d.event_designed_id, d.event_source_cd, 
            d.order_id, d.product_nm, d.product_sku, d.reserved_1_txt, 
            d.session_id_hex, d.shipping_message_txt, d.cart_nm, d.currency_cd, 
            d.detail_id_hex, d.event_id, d.identity_id, d.product_group_nm, 
            d.saving_message_txt, d.visit_id_hex  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :order_details_tmp , order_details , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..order_details_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..order_details_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..ORDER_DETAILS;
         drop table work.ORDER_DETAILS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table ORDER_DETAILS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..ORDER_SUMMARY)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..ORDER_SUMMARY));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..order_summary_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..order_summary_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=order_summary , table_keys=%str(EVENT_ID), out_table=work.order_summary );
   data work.order_summary_tmp /view=work.order_summary_tmp ;
      set work.order_summary ;
      if activity_dttm_tz  ne . then activity_dttm_tz = tzoneu2s(activity_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :order_summary_tmp , order_summary );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..order_summary_tmp ;
            set work.order_summary_tmp ;
            stop;
         run;
         proc append data=work.order_summary_tmp  base=&tmplib..order_summary_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..order_summary_tmp ;
            set work.order_summary_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :order_summary_tmp , order_summary );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..order_summary as b using &tmpdbschema..order_summary_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.total_price_amt = d.total_price_amt, 
            b.shipping_amt = d.shipping_amt, b.total_tax_amt = d.total_tax_amt, 
            b.total_unit_qty = d.total_unit_qty, b.properties_map_doc = d.properties_map_doc, 
            b.load_dttm = d.load_dttm, b.activity_dttm_tz = d.activity_dttm_tz, 
            b.activity_dttm = d.activity_dttm, b.visit_id = d.visit_id, 
            b.shipping_postal_cd = d.shipping_postal_cd, b.session_id_hex = d.session_id_hex, 
            b.payment_type_desc = d.payment_type_desc, b.identity_id = d.identity_id, 
            b.delivery_type_desc = d.delivery_type_desc, b.cart_id = d.cart_id, 
            b.billing_city_nm = d.billing_city_nm, b.billing_postal_cd = d.billing_postal_cd, 
            b.channel_nm = d.channel_nm, b.detail_id_hex = d.detail_id_hex, 
            b.event_nm = d.event_nm, b.mobile_app_id = d.mobile_app_id, 
            b.record_type = d.record_type, b.shipping_city_nm = d.shipping_city_nm, 
            b.visit_id_hex = d.visit_id_hex, b.billing_country_nm = d.billing_country_nm, 
            b.billing_state_region_cd = d.billing_state_region_cd, b.cart_nm = d.cart_nm, 
            b.currency_cd = d.currency_cd, b.detail_id = d.detail_id, 
            b.event_designed_id = d.event_designed_id, b.event_key_cd = d.event_key_cd, 
            b.event_source_cd = d.event_source_cd, b.order_id = d.order_id, 
            b.session_id = d.session_id, b.shipping_country_nm = d.shipping_country_nm, 
            b.shipping_state_region_cd = d.shipping_state_region_cd
         when not matched then insert ( 
            total_price_amt, shipping_amt, total_tax_amt, 
            total_unit_qty, properties_map_doc, load_dttm, activity_dttm_tz, 
            activity_dttm, visit_id, shipping_postal_cd, session_id_hex, 
            payment_type_desc, identity_id, event_id, delivery_type_desc, 
            cart_id, billing_city_nm, billing_postal_cd, channel_nm, 
            detail_id_hex, event_nm, mobile_app_id, record_type, 
            shipping_city_nm, visit_id_hex, billing_country_nm, billing_state_region_cd, 
            cart_nm, currency_cd, detail_id, event_designed_id, 
            event_key_cd, event_source_cd, order_id, session_id, 
            shipping_country_nm, shipping_state_region_cd
         ) values ( 
            d.total_price_amt, d.shipping_amt, d.total_tax_amt, 
            d.total_unit_qty, d.properties_map_doc, d.load_dttm, d.activity_dttm_tz, 
            d.activity_dttm, d.visit_id, d.shipping_postal_cd, d.session_id_hex, 
            d.payment_type_desc, d.identity_id, d.event_id, d.delivery_type_desc, 
            d.cart_id, d.billing_city_nm, d.billing_postal_cd, d.channel_nm, 
            d.detail_id_hex, d.event_nm, d.mobile_app_id, d.record_type, 
            d.shipping_city_nm, d.visit_id_hex, d.billing_country_nm, d.billing_state_region_cd, 
            d.cart_nm, d.currency_cd, d.detail_id, d.event_designed_id, 
            d.event_key_cd, d.event_source_cd, d.order_id, d.session_id, 
            d.shipping_country_nm, d.shipping_state_region_cd  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :order_summary_tmp , order_summary , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..order_summary_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..order_summary_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..ORDER_SUMMARY;
         drop table work.ORDER_SUMMARY;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table ORDER_SUMMARY;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..OUTBOUND_SYSTEM)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..OUTBOUND_SYSTEM));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..outbound_system_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..outbound_system_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=outbound_system , table_keys=%str(EVENT_ID), out_table=work.outbound_system );
   data work.outbound_system_tmp /view=work.outbound_system_tmp ;
      set work.outbound_system ;
      if outbound_system_dttm_tz  ne . then outbound_system_dttm_tz = tzoneu2s(outbound_system_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :outbound_system_tmp , outbound_system );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..outbound_system_tmp ;
            set work.outbound_system_tmp ;
            stop;
         run;
         proc append data=work.outbound_system_tmp  base=&tmplib..outbound_system_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..outbound_system_tmp ;
            set work.outbound_system_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :outbound_system_tmp , outbound_system );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..outbound_system as b using &tmpdbschema..outbound_system_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.properties_map_doc = d.properties_map_doc, 
            b.outbound_system_dttm_tz = d.outbound_system_dttm_tz, b.outbound_system_dttm = d.outbound_system_dttm, 
            b.load_dttm = d.load_dttm, b.visit_id_hex = d.visit_id_hex, 
            b.session_id_hex = d.session_id_hex, b.reserved_2_txt = d.reserved_2_txt, 
            b.reserved_1_txt = d.reserved_1_txt, b.parent_event_id = d.parent_event_id, 
            b.message_version_id = d.message_version_id, b.journey_id = d.journey_id, 
            b.event_designed_id = d.event_designed_id, b.context_val = d.context_val, 
            b.audience_id = d.audience_id, b.channel_nm = d.channel_nm, 
            b.channel_user_id = d.channel_user_id, b.creative_id = d.creative_id, 
            b.creative_version_id = d.creative_version_id, b.event_nm = d.event_nm, 
            b.message_id = d.message_id, b.mobile_app_id = d.mobile_app_id, 
            b.occurrence_id = d.occurrence_id, b.segment_id = d.segment_id, 
            b.task_id = d.task_id, b.aud_occurrence_id = d.aud_occurrence_id, 
            b.context_type_nm = d.context_type_nm, b.detail_id_hex = d.detail_id_hex, 
            b.identity_id = d.identity_id, b.journey_occurrence_id = d.journey_occurrence_id, 
            b.response_tracking_cd = d.response_tracking_cd, b.segment_version_id = d.segment_version_id, 
            b.spot_id = d.spot_id, b.task_version_id = d.task_version_id
         when not matched then insert ( 
            properties_map_doc, outbound_system_dttm_tz, outbound_system_dttm, 
            load_dttm, visit_id_hex, session_id_hex, reserved_2_txt, 
            reserved_1_txt, parent_event_id, message_version_id, journey_id, 
            event_designed_id, context_val, audience_id, channel_nm, 
            channel_user_id, creative_id, creative_version_id, event_nm, 
            message_id, mobile_app_id, occurrence_id, segment_id, 
            task_id, aud_occurrence_id, context_type_nm, detail_id_hex, 
            event_id, identity_id, journey_occurrence_id, response_tracking_cd, 
            segment_version_id, spot_id, task_version_id
         ) values ( 
            d.properties_map_doc, d.outbound_system_dttm_tz, d.outbound_system_dttm, 
            d.load_dttm, d.visit_id_hex, d.session_id_hex, d.reserved_2_txt, 
            d.reserved_1_txt, d.parent_event_id, d.message_version_id, d.journey_id, 
            d.event_designed_id, d.context_val, d.audience_id, d.channel_nm, 
            d.channel_user_id, d.creative_id, d.creative_version_id, d.event_nm, 
            d.message_id, d.mobile_app_id, d.occurrence_id, d.segment_id, 
            d.task_id, d.aud_occurrence_id, d.context_type_nm, d.detail_id_hex, 
            d.event_id, d.identity_id, d.journey_occurrence_id, d.response_tracking_cd, 
            d.segment_version_id, d.spot_id, d.task_version_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :outbound_system_tmp , outbound_system , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..outbound_system_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..outbound_system_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..OUTBOUND_SYSTEM;
         drop table work.OUTBOUND_SYSTEM;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table OUTBOUND_SYSTEM;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..PAGE_DETAILS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..PAGE_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..page_details_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..page_details_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=page_details , table_keys=%str(EVENT_ID), out_table=work.page_details );
   data work.page_details_tmp /view=work.page_details_tmp ;
      set work.page_details ;
      if detail_dttm_tz  ne . then detail_dttm_tz = tzoneu2s(detail_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :page_details_tmp , page_details );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..page_details_tmp ;
            set work.page_details_tmp ;
            stop;
         run;
         proc append data=work.page_details_tmp  base=&tmplib..page_details_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..page_details_tmp ;
            set work.page_details_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :page_details_tmp , page_details );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..page_details as b using &tmpdbschema..page_details_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.session_dt_tz = d.session_dt_tz, 
            b.session_dt = d.session_dt, b.page_load_sec_cnt = d.page_load_sec_cnt, 
            b.page_complete_sec_cnt = d.page_complete_sec_cnt, b.bytes_sent_cnt = d.bytes_sent_cnt, 
            b.detail_dttm_tz = d.detail_dttm_tz, b.load_dttm = d.load_dttm, 
            b.detail_dttm = d.detail_dttm, b.url_domain = d.url_domain, 
            b.session_id_hex = d.session_id_hex, b.session_id = d.session_id, 
            b.page_url_txt = d.page_url_txt, b.mobile_app_id = d.mobile_app_id, 
            b.event_key_cd = d.event_key_cd, b.detail_id_hex = d.detail_id_hex, 
            b.detail_id = d.detail_id, b.class8_id = d.class8_id, 
            b.class4_id = d.class4_id, b.class15_id = d.class15_id, 
            b.class12_id = d.class12_id, b.class11_id = d.class11_id, 
            b.channel_nm = d.channel_nm, b.class13_id = d.class13_id, 
            b.class2_id = d.class2_id, b.class6_id = d.class6_id, 
            b.domain_nm = d.domain_nm, b.event_source_cd = d.event_source_cd, 
            b.page_desc = d.page_desc, b.protocol_nm = d.protocol_nm, 
            b.visit_id = d.visit_id, b.visit_id_hex = d.visit_id_hex, 
            b.class10_id = d.class10_id, b.class14_id = d.class14_id, 
            b.class1_id = d.class1_id, b.class3_id = d.class3_id, 
            b.class5_id = d.class5_id, b.class7_id = d.class7_id, 
            b.class9_id = d.class9_id, b.event_nm = d.event_nm, 
            b.identity_id = d.identity_id, b.referrer_url_txt = d.referrer_url_txt, 
            b.window_size_txt = d.window_size_txt
         when not matched then insert ( 
            session_dt_tz, session_dt, page_load_sec_cnt, 
            page_complete_sec_cnt, bytes_sent_cnt, detail_dttm_tz, load_dttm, 
            detail_dttm, url_domain, session_id_hex, session_id, 
            page_url_txt, mobile_app_id, event_key_cd, detail_id_hex, 
            detail_id, class8_id, class4_id, class15_id, 
            class12_id, class11_id, channel_nm, class13_id, 
            class2_id, class6_id, domain_nm, event_source_cd, 
            page_desc, protocol_nm, visit_id, visit_id_hex, 
            class10_id, class14_id, class1_id, class3_id, 
            class5_id, class7_id, class9_id, event_id, 
            event_nm, identity_id, referrer_url_txt, window_size_txt
         ) values ( 
            d.session_dt_tz, d.session_dt, d.page_load_sec_cnt, 
            d.page_complete_sec_cnt, d.bytes_sent_cnt, d.detail_dttm_tz, d.load_dttm, 
            d.detail_dttm, d.url_domain, d.session_id_hex, d.session_id, 
            d.page_url_txt, d.mobile_app_id, d.event_key_cd, d.detail_id_hex, 
            d.detail_id, d.class8_id, d.class4_id, d.class15_id, 
            d.class12_id, d.class11_id, d.channel_nm, d.class13_id, 
            d.class2_id, d.class6_id, d.domain_nm, d.event_source_cd, 
            d.page_desc, d.protocol_nm, d.visit_id, d.visit_id_hex, 
            d.class10_id, d.class14_id, d.class1_id, d.class3_id, 
            d.class5_id, d.class7_id, d.class9_id, d.event_id, 
            d.event_nm, d.identity_id, d.referrer_url_txt, d.window_size_txt  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :page_details_tmp , page_details , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..page_details_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..page_details_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..PAGE_DETAILS;
         drop table work.PAGE_DETAILS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table PAGE_DETAILS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..PAGE_DETAILS_EXT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..PAGE_DETAILS_EXT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..page_details_ext_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..page_details_ext_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=page_details_ext , table_keys=%str(DETAIL_ID,LOAD_DTTM,SESSION_ID), out_table=work.page_details_ext );
   data work.page_details_ext_tmp /view=work.page_details_ext_tmp ;
      set work.page_details_ext ;
   run;
   %err_check (Failed to add time zone adaptation :page_details_ext_tmp , page_details_ext );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..page_details_ext_tmp ;
            set work.page_details_ext_tmp ;
            stop;
         run;
         proc append data=work.page_details_ext_tmp  base=&tmplib..page_details_ext_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..page_details_ext_tmp ;
            set work.page_details_ext_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :page_details_ext_tmp , page_details_ext );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..page_details_ext as b using &tmpdbschema..page_details_ext_tmp as d on( 
            b.load_dttm = d.load_dttm and 
            b.session_id = d.session_id and b.detail_id = d.detail_id )
         when matched then  
         update set 
            b.active_sec_spent_on_page_cnt = d.active_sec_spent_on_page_cnt, 
            b.seconds_spent_on_page_cnt = d.seconds_spent_on_page_cnt, b.detail_id_hex = d.detail_id_hex, 
            b.session_id_hex = d.session_id_hex
         when not matched then insert ( 
            active_sec_spent_on_page_cnt, seconds_spent_on_page_cnt, load_dttm, 
            session_id, detail_id, detail_id_hex, session_id_hex
         ) values ( 
            d.active_sec_spent_on_page_cnt, d.seconds_spent_on_page_cnt, d.load_dttm, 
            d.session_id, d.detail_id, d.detail_id_hex, d.session_id_hex  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :page_details_ext_tmp , page_details_ext , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..page_details_ext_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..page_details_ext_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..PAGE_DETAILS_EXT;
         drop table work.PAGE_DETAILS_EXT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table PAGE_DETAILS_EXT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..PAGE_ERRORS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..PAGE_ERRORS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..page_errors_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..page_errors_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=page_errors , table_keys=%str(EVENT_ID), out_table=work.page_errors );
   data work.page_errors_tmp /view=work.page_errors_tmp ;
      set work.page_errors ;
      if in_page_error_dttm_tz  ne . then in_page_error_dttm_tz = tzoneu2s(in_page_error_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :page_errors_tmp , page_errors );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..page_errors_tmp ;
            set work.page_errors_tmp ;
            stop;
         run;
         proc append data=work.page_errors_tmp  base=&tmplib..page_errors_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..page_errors_tmp ;
            set work.page_errors_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :page_errors_tmp , page_errors );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..page_errors as b using &tmpdbschema..page_errors_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.in_page_error_dttm = d.in_page_error_dttm, 
            b.in_page_error_dttm_tz = d.in_page_error_dttm_tz, b.load_dttm = d.load_dttm, 
            b.visit_id_hex = d.visit_id_hex, b.session_id = d.session_id, 
            b.identity_id = d.identity_id, b.error_location_txt = d.error_location_txt, 
            b.detail_id_hex = d.detail_id_hex, b.in_page_error_txt = d.in_page_error_txt, 
            b.session_id_hex = d.session_id_hex, b.detail_id = d.detail_id, 
            b.event_source_cd = d.event_source_cd, b.visit_id = d.visit_id
         when not matched then insert ( 
            in_page_error_dttm, in_page_error_dttm_tz, load_dttm, 
            visit_id_hex, session_id, identity_id, error_location_txt, 
            detail_id_hex, event_id, in_page_error_txt, session_id_hex, 
            detail_id, event_source_cd, visit_id
         ) values ( 
            d.in_page_error_dttm, d.in_page_error_dttm_tz, d.load_dttm, 
            d.visit_id_hex, d.session_id, d.identity_id, d.error_location_txt, 
            d.detail_id_hex, d.event_id, d.in_page_error_txt, d.session_id_hex, 
            d.detail_id, d.event_source_cd, d.visit_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :page_errors_tmp , page_errors , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..page_errors_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..page_errors_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..PAGE_ERRORS;
         drop table work.PAGE_ERRORS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table PAGE_ERRORS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..PLANNING_HIERARCHY_DEFN)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..PLANNING_HIERARCHY_DEFN));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..PLANNING_HIERARCHY_DEFN) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..planning_hierarchy_defn  base=&trglib..planning_hierarchy_defn (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..PLANNING_HIERARCHY_DEFN (
            level_no, load_dttm, created_dttm, 
            last_modified_dttm, last_modified_usernm, hier_defn_nm, hier_defn_id, 
            hier_defn_desc, hier_defn_subtype, level_desc, created_by_usernm, 
            hier_defn_type, level_nm )
      select level_no, load_dttm, created_dttm, 
            last_modified_dttm, last_modified_usernm, hier_defn_nm, hier_defn_id, 
            hier_defn_desc, hier_defn_subtype, level_desc, created_by_usernm, 
            hier_defn_type, level_nm
         from &udmmart..planning_hierarchy_defn ;
      quit;
   %end;
   %err_check (Failed to insert into planning_hierarchy_defn , planning_hierarchy_defn );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..PLANNING_HIERARCHY_DEFN;
         drop table work.PLANNING_HIERARCHY_DEFN;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table PLANNING_HIERARCHY_DEFN;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..PLANNING_INFO)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..PLANNING_INFO));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..PLANNING_INFO) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..planning_info  base=&trglib..planning_info (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..PLANNING_INFO (
            bu_obsolete_flg, reserved_budget_same_flg, alloc_budget, 
            rolledup_budget, tot_invoiced, tot_expenses, tot_cmtmnt_outstanding, 
            tot_committed, available_budget, tot_cmtmnt_overspent, reserved_budget, 
            total_budget, created_dttm, last_modified_dttm, planned_start_dttm, 
            load_dttm, planned_end_dttm, task_id, planning_owner_usernm, 
            planning_level_no, planning_desc, parent_id, lev6_nm, 
            lev2_nm, last_modified_usernm, hier_defn_id, currency_cd, 
            bu_nm, bu_currency_cd, activity_nm, activity_desc, 
            all_msgs, bu_desc, category_nm, hier_defn_nodeid, 
            lev10_nm, lev3_nm, lev4_nm, lev7_nm, 
            lev8_nm, parent_nm, planning_id, planning_level_type, 
            planning_nm, planning_type, task_channel, task_status, 
            activity_id, activity_status, bu_id, created_by_usernm, 
            lev1_nm, lev5_nm, lev9_nm, planning_item_path, 
            planning_number, planning_status, task_desc, task_nm )
      select bu_obsolete_flg, reserved_budget_same_flg, alloc_budget, 
            rolledup_budget, tot_invoiced, tot_expenses, tot_cmtmnt_outstanding, 
            tot_committed, available_budget, tot_cmtmnt_overspent, reserved_budget, 
            total_budget, created_dttm, last_modified_dttm, planned_start_dttm, 
            load_dttm, planned_end_dttm, task_id, planning_owner_usernm, 
            planning_level_no, planning_desc, parent_id, lev6_nm, 
            lev2_nm, last_modified_usernm, hier_defn_id, currency_cd, 
            bu_nm, bu_currency_cd, activity_nm, activity_desc, 
            all_msgs, bu_desc, category_nm, hier_defn_nodeid, 
            lev10_nm, lev3_nm, lev4_nm, lev7_nm, 
            lev8_nm, parent_nm, planning_id, planning_level_type, 
            planning_nm, planning_type, task_channel, task_status, 
            activity_id, activity_status, bu_id, created_by_usernm, 
            lev1_nm, lev5_nm, lev9_nm, planning_item_path, 
            planning_number, planning_status, task_desc, task_nm
         from &udmmart..planning_info ;
      quit;
   %end;
   %err_check (Failed to insert into planning_info , planning_info );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..PLANNING_INFO;
         drop table work.PLANNING_INFO;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table PLANNING_INFO;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..PLANNING_INFO_CUSTOM_PROP)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..PLANNING_INFO_CUSTOM_PROP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..PLANNING_INFO_CUSTOM_PROP) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..planning_info_custom_prop  base=&trglib..planning_info_custom_prop (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..PLANNING_INFO_CUSTOM_PROP (
            attr_val, is_obsolete_flg, is_grid_flg, 
            last_modified_dttm, created_dttm, load_dttm, planning_id, 
            last_modified_usernm, created_by_usernm, attr_group_nm, attr_cd, 
            attr_group_cd, attr_id, attr_nm, data_formatter, 
            remote_pklist_tab_col, attr_group_id, data_type )
      select attr_val, is_obsolete_flg, is_grid_flg, 
            last_modified_dttm, created_dttm, load_dttm, planning_id, 
            last_modified_usernm, created_by_usernm, attr_group_nm, attr_cd, 
            attr_group_cd, attr_id, attr_nm, data_formatter, 
            remote_pklist_tab_col, attr_group_id, data_type
         from &udmmart..planning_info_custom_prop ;
      quit;
   %end;
   %err_check (Failed to insert into planning_info_custom_prop , planning_info_custom_prop );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..PLANNING_INFO_CUSTOM_PROP;
         drop table work.PLANNING_INFO_CUSTOM_PROP;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table PLANNING_INFO_CUSTOM_PROP;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..PRODUCT_VIEWS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..PRODUCT_VIEWS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..product_views_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..product_views_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=product_views , table_keys=%str(EVENT_ID), out_table=work.product_views );
   data work.product_views_tmp /view=work.product_views_tmp ;
      set work.product_views ;
      if action_dttm_tz  ne . then action_dttm_tz = tzoneu2s(action_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :product_views_tmp , product_views );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..product_views_tmp ;
            set work.product_views_tmp ;
            stop;
         run;
         proc append data=work.product_views_tmp  base=&tmplib..product_views_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..product_views_tmp ;
            set work.product_views_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :product_views_tmp , product_views );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..product_views as b using &tmpdbschema..product_views_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.price_val = d.price_val, 
            b.properties_map_doc = d.properties_map_doc, b.action_dttm_tz = d.action_dttm_tz, 
            b.load_dttm = d.load_dttm, b.action_dttm = d.action_dttm, 
            b.visit_id_hex = d.visit_id_hex, b.visit_id = d.visit_id, 
            b.saving_message_txt = d.saving_message_txt, b.product_id = d.product_id, 
            b.mobile_app_id = d.mobile_app_id, b.event_nm = d.event_nm, 
            b.event_key_cd = d.event_key_cd, b.detail_id = d.detail_id, 
            b.availability_message_txt = d.availability_message_txt, b.channel_nm = d.channel_nm, 
            b.event_designed_id = d.event_designed_id, b.event_source_cd = d.event_source_cd, 
            b.product_group_nm = d.product_group_nm, b.product_sku = d.product_sku, 
            b.session_id_hex = d.session_id_hex, b.currency_cd = d.currency_cd, 
            b.detail_id_hex = d.detail_id_hex, b.identity_id = d.identity_id, 
            b.product_nm = d.product_nm, b.session_id = d.session_id, 
            b.shipping_message_txt = d.shipping_message_txt
         when not matched then insert ( 
            price_val, properties_map_doc, action_dttm_tz, 
            load_dttm, action_dttm, visit_id_hex, visit_id, 
            saving_message_txt, product_id, mobile_app_id, event_nm, 
            event_key_cd, detail_id, availability_message_txt, channel_nm, 
            event_designed_id, event_source_cd, product_group_nm, product_sku, 
            session_id_hex, currency_cd, detail_id_hex, event_id, 
            identity_id, product_nm, session_id, shipping_message_txt
         ) values ( 
            d.price_val, d.properties_map_doc, d.action_dttm_tz, 
            d.load_dttm, d.action_dttm, d.visit_id_hex, d.visit_id, 
            d.saving_message_txt, d.product_id, d.mobile_app_id, d.event_nm, 
            d.event_key_cd, d.detail_id, d.availability_message_txt, d.channel_nm, 
            d.event_designed_id, d.event_source_cd, d.product_group_nm, d.product_sku, 
            d.session_id_hex, d.currency_cd, d.detail_id_hex, d.event_id, 
            d.identity_id, d.product_nm, d.session_id, d.shipping_message_txt  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :product_views_tmp , product_views , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..product_views_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..product_views_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..PRODUCT_VIEWS;
         drop table work.PRODUCT_VIEWS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table PRODUCT_VIEWS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..PROMOTION_DISPLAYED)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..PROMOTION_DISPLAYED));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..promotion_displayed_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..promotion_displayed_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=promotion_displayed , table_keys=%str(EVENT_ID), out_table=work.promotion_displayed );
   data work.promotion_displayed_tmp /view=work.promotion_displayed_tmp ;
      set work.promotion_displayed ;
      if display_dttm_tz  ne . then display_dttm_tz = tzoneu2s(display_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :promotion_displayed_tmp , promotion_displayed );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..promotion_displayed_tmp ;
            set work.promotion_displayed_tmp ;
            stop;
         run;
         proc append data=work.promotion_displayed_tmp  base=&tmplib..promotion_displayed_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..promotion_displayed_tmp ;
            set work.promotion_displayed_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :promotion_displayed_tmp , promotion_displayed );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..promotion_displayed as b using &tmpdbschema..promotion_displayed_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.derived_display_flg = d.derived_display_flg, 
            b.promotion_number = d.promotion_number, b.properties_map_doc = d.properties_map_doc, 
            b.display_dttm_tz = d.display_dttm_tz, b.load_dttm = d.load_dttm, 
            b.display_dttm = d.display_dttm, b.session_id_hex = d.session_id_hex, 
            b.promotion_tracking_cd = d.promotion_tracking_cd, b.promotion_nm = d.promotion_nm, 
            b.promotion_creative_nm = d.promotion_creative_nm, b.event_source_cd = d.event_source_cd, 
            b.event_designed_id = d.event_designed_id, b.detail_id = d.detail_id, 
            b.channel_nm = d.channel_nm, b.detail_id_hex = d.detail_id_hex, 
            b.event_key_cd = d.event_key_cd, b.mobile_app_id = d.mobile_app_id, 
            b.promotion_placement_nm = d.promotion_placement_nm, b.session_id = d.session_id, 
            b.visit_id_hex = d.visit_id_hex, b.event_nm = d.event_nm, 
            b.identity_id = d.identity_id, b.promotion_type_nm = d.promotion_type_nm, 
            b.visit_id = d.visit_id
         when not matched then insert ( 
            derived_display_flg, promotion_number, properties_map_doc, 
            display_dttm_tz, load_dttm, display_dttm, session_id_hex, 
            promotion_tracking_cd, promotion_nm, promotion_creative_nm, event_source_cd, 
            event_designed_id, detail_id, channel_nm, detail_id_hex, 
            event_key_cd, mobile_app_id, promotion_placement_nm, session_id, 
            visit_id_hex, event_id, event_nm, identity_id, 
            promotion_type_nm, visit_id
         ) values ( 
            d.derived_display_flg, d.promotion_number, d.properties_map_doc, 
            d.display_dttm_tz, d.load_dttm, d.display_dttm, d.session_id_hex, 
            d.promotion_tracking_cd, d.promotion_nm, d.promotion_creative_nm, d.event_source_cd, 
            d.event_designed_id, d.detail_id, d.channel_nm, d.detail_id_hex, 
            d.event_key_cd, d.mobile_app_id, d.promotion_placement_nm, d.session_id, 
            d.visit_id_hex, d.event_id, d.event_nm, d.identity_id, 
            d.promotion_type_nm, d.visit_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :promotion_displayed_tmp , promotion_displayed , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..promotion_displayed_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..promotion_displayed_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..PROMOTION_DISPLAYED;
         drop table work.PROMOTION_DISPLAYED;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table PROMOTION_DISPLAYED;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..PROMOTION_USED)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..PROMOTION_USED));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..promotion_used_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..promotion_used_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=promotion_used , table_keys=%str(EVENT_ID), out_table=work.promotion_used );
   data work.promotion_used_tmp /view=work.promotion_used_tmp ;
      set work.promotion_used ;
      if click_dttm_tz  ne . then click_dttm_tz = tzoneu2s(click_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :promotion_used_tmp , promotion_used );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..promotion_used_tmp ;
            set work.promotion_used_tmp ;
            stop;
         run;
         proc append data=work.promotion_used_tmp  base=&tmplib..promotion_used_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..promotion_used_tmp ;
            set work.promotion_used_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :promotion_used_tmp , promotion_used );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..promotion_used as b using &tmpdbschema..promotion_used_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.promotion_number = d.promotion_number, 
            b.properties_map_doc = d.properties_map_doc, b.click_dttm_tz = d.click_dttm_tz, 
            b.click_dttm = d.click_dttm, b.load_dttm = d.load_dttm, 
            b.session_id_hex = d.session_id_hex, b.promotion_tracking_cd = d.promotion_tracking_cd, 
            b.promotion_creative_nm = d.promotion_creative_nm, b.event_source_cd = d.event_source_cd, 
            b.event_designed_id = d.event_designed_id, b.detail_id = d.detail_id, 
            b.detail_id_hex = d.detail_id_hex, b.event_key_cd = d.event_key_cd, 
            b.mobile_app_id = d.mobile_app_id, b.promotion_nm = d.promotion_nm, 
            b.promotion_placement_nm = d.promotion_placement_nm, b.session_id = d.session_id, 
            b.visit_id_hex = d.visit_id_hex, b.channel_nm = d.channel_nm, 
            b.event_nm = d.event_nm, b.identity_id = d.identity_id, 
            b.promotion_type_nm = d.promotion_type_nm, b.visit_id = d.visit_id
         when not matched then insert ( 
            promotion_number, properties_map_doc, click_dttm_tz, 
            click_dttm, load_dttm, session_id_hex, promotion_tracking_cd, 
            promotion_creative_nm, event_source_cd, event_id, event_designed_id, 
            detail_id, detail_id_hex, event_key_cd, mobile_app_id, 
            promotion_nm, promotion_placement_nm, session_id, visit_id_hex, 
            channel_nm, event_nm, identity_id, promotion_type_nm, 
            visit_id
         ) values ( 
            d.promotion_number, d.properties_map_doc, d.click_dttm_tz, 
            d.click_dttm, d.load_dttm, d.session_id_hex, d.promotion_tracking_cd, 
            d.promotion_creative_nm, d.event_source_cd, d.event_id, d.event_designed_id, 
            d.detail_id, d.detail_id_hex, d.event_key_cd, d.mobile_app_id, 
            d.promotion_nm, d.promotion_placement_nm, d.session_id, d.visit_id_hex, 
            d.channel_nm, d.event_nm, d.identity_id, d.promotion_type_nm, 
            d.visit_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :promotion_used_tmp , promotion_used , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..promotion_used_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..promotion_used_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..PROMOTION_USED;
         drop table work.PROMOTION_USED;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table PROMOTION_USED;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..RESPONSE_HISTORY)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..RESPONSE_HISTORY));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..response_history_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..response_history_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=response_history , table_keys=%str(RESPONSE_ID), out_table=work.response_history );
   data work.response_history_tmp /view=work.response_history_tmp ;
      set work.response_history ;
      if response_dttm_tz  ne . then response_dttm_tz = tzoneu2s(response_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :response_history_tmp , response_history );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..response_history_tmp ;
            set work.response_history_tmp ;
            stop;
         run;
         proc append data=work.response_history_tmp  base=&tmplib..response_history_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..response_history_tmp ;
            set work.response_history_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :response_history_tmp , response_history );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..response_history as b using &tmpdbschema..response_history_tmp as d on( 
            b.response_id = d.response_id )
         when matched then  
         update set 
            b.properties_map_doc = d.properties_map_doc, 
            b.load_dttm = d.load_dttm, b.response_dttm = d.response_dttm, 
            b.response_dttm_tz = d.response_dttm_tz, b.session_id_hex = d.session_id_hex, 
            b.response_channel_nm = d.response_channel_nm, b.parent_event_designed_id = d.parent_event_designed_id, 
            b.journey_occurrence_id = d.journey_occurrence_id, b.detail_id_hex = d.detail_id_hex, 
            b.audience_id = d.audience_id, b.context_type_nm = d.context_type_nm, 
            b.context_val = d.context_val, b.identity_id = d.identity_id, 
            b.message_id = d.message_id, b.response_nm = d.response_nm, 
            b.task_version_id = d.task_version_id, b.aud_occurrence_id = d.aud_occurrence_id, 
            b.creative_id = d.creative_id, b.event_designed_id = d.event_designed_id, 
            b.journey_id = d.journey_id, b.occurrence_id = d.occurrence_id, 
            b.response_tracking_cd = d.response_tracking_cd, b.task_id = d.task_id, 
            b.visit_id_hex = d.visit_id_hex
         when not matched then insert ( 
            properties_map_doc, load_dttm, response_dttm, 
            response_dttm_tz, session_id_hex, response_id, response_channel_nm, 
            parent_event_designed_id, journey_occurrence_id, detail_id_hex, audience_id, 
            context_type_nm, context_val, identity_id, message_id, 
            response_nm, task_version_id, aud_occurrence_id, creative_id, 
            event_designed_id, journey_id, occurrence_id, response_tracking_cd, 
            task_id, visit_id_hex
         ) values ( 
            d.properties_map_doc, d.load_dttm, d.response_dttm, 
            d.response_dttm_tz, d.session_id_hex, d.response_id, d.response_channel_nm, 
            d.parent_event_designed_id, d.journey_occurrence_id, d.detail_id_hex, d.audience_id, 
            d.context_type_nm, d.context_val, d.identity_id, d.message_id, 
            d.response_nm, d.task_version_id, d.aud_occurrence_id, d.creative_id, 
            d.event_designed_id, d.journey_id, d.occurrence_id, d.response_tracking_cd, 
            d.task_id, d.visit_id_hex  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :response_history_tmp , response_history , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..response_history_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..response_history_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..RESPONSE_HISTORY;
         drop table work.RESPONSE_HISTORY;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table RESPONSE_HISTORY;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..SEARCH_RESULTS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..SEARCH_RESULTS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..search_results_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..search_results_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=search_results , table_keys=%str(EVENT_ID), out_table=work.search_results );
   data work.search_results_tmp /view=work.search_results_tmp ;
      set work.search_results ;
      if search_results_dttm_tz  ne . then search_results_dttm_tz = tzoneu2s(search_results_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :search_results_tmp , search_results );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..search_results_tmp ;
            set work.search_results_tmp ;
            stop;
         run;
         proc append data=work.search_results_tmp  base=&tmplib..search_results_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..search_results_tmp ;
            set work.search_results_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :search_results_tmp , search_results );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..search_results as b using &tmpdbschema..search_results_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.results_displayed_flg = d.results_displayed_flg, 
            b.search_results_displayed = d.search_results_displayed, b.properties_map_doc = d.properties_map_doc, 
            b.load_dttm = d.load_dttm, b.search_results_dttm = d.search_results_dttm, 
            b.search_results_dttm_tz = d.search_results_dttm_tz, b.visit_id_hex = d.visit_id_hex, 
            b.srch_field_name = d.srch_field_name, b.srch_field_id = d.srch_field_id, 
            b.search_results_sk = d.search_results_sk, b.search_nm = d.search_nm, 
            b.identity_id = d.identity_id, b.event_key_cd = d.event_key_cd, 
            b.channel_nm = d.channel_nm, b.detail_id = d.detail_id, 
            b.detail_id_hex = d.detail_id_hex, b.event_nm = d.event_nm, 
            b.mobile_app_id = d.mobile_app_id, b.session_id = d.session_id, 
            b.srch_phrase = d.srch_phrase, b.event_designed_id = d.event_designed_id, 
            b.event_source_cd = d.event_source_cd, b.session_id_hex = d.session_id_hex, 
            b.visit_id = d.visit_id
         when not matched then insert ( 
            results_displayed_flg, search_results_displayed, properties_map_doc, 
            load_dttm, search_results_dttm, search_results_dttm_tz, visit_id_hex, 
            srch_field_name, srch_field_id, search_results_sk, search_nm, 
            identity_id, event_key_cd, event_id, channel_nm, 
            detail_id, detail_id_hex, event_nm, mobile_app_id, 
            session_id, srch_phrase, event_designed_id, event_source_cd, 
            session_id_hex, visit_id
         ) values ( 
            d.results_displayed_flg, d.search_results_displayed, d.properties_map_doc, 
            d.load_dttm, d.search_results_dttm, d.search_results_dttm_tz, d.visit_id_hex, 
            d.srch_field_name, d.srch_field_id, d.search_results_sk, d.search_nm, 
            d.identity_id, d.event_key_cd, d.event_id, d.channel_nm, 
            d.detail_id, d.detail_id_hex, d.event_nm, d.mobile_app_id, 
            d.session_id, d.srch_phrase, d.event_designed_id, d.event_source_cd, 
            d.session_id_hex, d.visit_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :search_results_tmp , search_results , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..search_results_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..search_results_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..SEARCH_RESULTS;
         drop table work.SEARCH_RESULTS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table SEARCH_RESULTS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..SEARCH_RESULTS_EXT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..SEARCH_RESULTS_EXT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..search_results_ext_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..search_results_ext_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=search_results_ext , table_keys=%str(EVENT_ID), out_table=work.search_results_ext );
   data work.search_results_ext_tmp /view=work.search_results_ext_tmp ;
      set work.search_results_ext ;
   run;
   %err_check (Failed to add time zone adaptation :search_results_ext_tmp , search_results_ext );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..search_results_ext_tmp ;
            set work.search_results_ext_tmp ;
            stop;
         run;
         proc append data=work.search_results_ext_tmp  base=&tmplib..search_results_ext_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..search_results_ext_tmp ;
            set work.search_results_ext_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :search_results_ext_tmp , search_results_ext );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..search_results_ext as b using &tmpdbschema..search_results_ext_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.search_results_displayed = d.search_results_displayed, 
            b.load_dttm = d.load_dttm, b.search_results_sk = d.search_results_sk, 
            b.event_designed_id = d.event_designed_id
         when not matched then insert ( 
            search_results_displayed, load_dttm, search_results_sk, 
            event_designed_id, event_id
         ) values ( 
            d.search_results_displayed, d.load_dttm, d.search_results_sk, 
            d.event_designed_id, d.event_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :search_results_ext_tmp , search_results_ext , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..search_results_ext_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..search_results_ext_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..SEARCH_RESULTS_EXT;
         drop table work.SEARCH_RESULTS_EXT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table SEARCH_RESULTS_EXT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..SESSION_DETAILS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..SESSION_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..session_details_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..session_details_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=session_details , table_keys=%str(EVENT_ID), out_table=work.session_details );
   data work.session_details_tmp /view=work.session_details_tmp ;
      set work.session_details ;
      if session_start_dttm_tz  ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz ,&timeZone_Value.);
      if client_session_start_dttm_tz  ne . then client_session_start_dttm_tz = tzoneu2s(client_session_start_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :session_details_tmp , session_details );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..session_details_tmp ;
            set work.session_details_tmp ;
            stop;
         run;
         proc append data=work.session_details_tmp  base=&tmplib..session_details_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..session_details_tmp ;
            set work.session_details_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :session_details_tmp , session_details );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..session_details as b using &tmpdbschema..session_details_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.java_enabled_flg = d.java_enabled_flg, 
            b.java_script_enabled_flg = d.java_script_enabled_flg, b.cookies_enabled_flg = d.cookies_enabled_flg, 
            b.is_portable_flag = d.is_portable_flag, b.flash_enabled_flg = d.flash_enabled_flg, 
            b.session_dt = d.session_dt, b.session_dt_tz = d.session_dt_tz, 
            b.longitude = d.longitude, b.latitude = d.latitude, 
            b.session_timeout = d.session_timeout, b.metro_cd = d.metro_cd, 
            b.screen_color_depth_no = d.screen_color_depth_no, b.client_session_start_dttm = d.client_session_start_dttm, 
            b.load_dttm = d.load_dttm, b.session_start_dttm_tz = d.session_start_dttm_tz, 
            b.session_start_dttm = d.session_start_dttm, b.client_session_start_dttm_tz = d.client_session_start_dttm_tz, 
            b.user_agent_nm = d.user_agent_nm, b.state_region_cd = d.state_region_cd, 
            b.region_nm = d.region_nm, b.profile_nm2 = d.profile_nm2, 
            b.profile_nm1 = d.profile_nm1, b.previous_session_id_hex = d.previous_session_id_hex, 
            b.previous_session_id = d.previous_session_id, b.postal_cd = d.postal_cd, 
            b.parent_event_id = d.parent_event_id, b.network_code = d.network_code, 
            b.mobile_country_code = d.mobile_country_code, b.manufacturer = d.manufacturer, 
            b.java_version_no = d.java_version_no, b.flash_version_no = d.flash_version_no, 
            b.device_type_nm = d.device_type_nm, b.country_nm = d.country_nm, 
            b.country_cd = d.country_cd, b.city_nm = d.city_nm, 
            b.browser_nm = d.browser_nm, b.app_id = d.app_id, 
            b.browser_version_no = d.browser_version_no, b.carrier_name = d.carrier_name, 
            b.device_language = d.device_language, b.eventsource_cd = d.eventsource_cd, 
            b.identity_id = d.identity_id, b.ip_address = d.ip_address, 
            b.new_visitor_flg = d.new_visitor_flg, b.platform_desc = d.platform_desc, 
            b.platform_type_nm = d.platform_type_nm, b.profile_nm4 = d.profile_nm4, 
            b.screen_size_txt = d.screen_size_txt, b.session_id = d.session_id, 
            b.visitor_id = d.visitor_id, b.app_version = d.app_version, 
            b.channel_nm = d.channel_nm, b.device_nm = d.device_nm, 
            b.organization_nm = d.organization_nm, b.platform_version = d.platform_version, 
            b.profile_nm3 = d.profile_nm3, b.profile_nm5 = d.profile_nm5, 
            b.sdk_version = d.sdk_version, b.session_id_hex = d.session_id_hex, 
            b.user_language_cd = d.user_language_cd
         when not matched then insert ( 
            java_enabled_flg, java_script_enabled_flg, cookies_enabled_flg, 
            is_portable_flag, flash_enabled_flg, session_dt, session_dt_tz, 
            longitude, latitude, session_timeout, metro_cd, 
            screen_color_depth_no, client_session_start_dttm, load_dttm, session_start_dttm_tz, 
            session_start_dttm, client_session_start_dttm_tz, user_agent_nm, state_region_cd, 
            region_nm, profile_nm2, profile_nm1, previous_session_id_hex, 
            previous_session_id, postal_cd, parent_event_id, network_code, 
            mobile_country_code, manufacturer, java_version_no, flash_version_no, 
            event_id, device_type_nm, country_nm, country_cd, 
            city_nm, browser_nm, app_id, browser_version_no, 
            carrier_name, device_language, eventsource_cd, identity_id, 
            ip_address, new_visitor_flg, platform_desc, platform_type_nm, 
            profile_nm4, screen_size_txt, session_id, visitor_id, 
            app_version, channel_nm, device_nm, organization_nm, 
            platform_version, profile_nm3, profile_nm5, sdk_version, 
            session_id_hex, user_language_cd
         ) values ( 
            d.java_enabled_flg, d.java_script_enabled_flg, d.cookies_enabled_flg, 
            d.is_portable_flag, d.flash_enabled_flg, d.session_dt, d.session_dt_tz, 
            d.longitude, d.latitude, d.session_timeout, d.metro_cd, 
            d.screen_color_depth_no, d.client_session_start_dttm, d.load_dttm, d.session_start_dttm_tz, 
            d.session_start_dttm, d.client_session_start_dttm_tz, d.user_agent_nm, d.state_region_cd, 
            d.region_nm, d.profile_nm2, d.profile_nm1, d.previous_session_id_hex, 
            d.previous_session_id, d.postal_cd, d.parent_event_id, d.network_code, 
            d.mobile_country_code, d.manufacturer, d.java_version_no, d.flash_version_no, 
            d.event_id, d.device_type_nm, d.country_nm, d.country_cd, 
            d.city_nm, d.browser_nm, d.app_id, d.browser_version_no, 
            d.carrier_name, d.device_language, d.eventsource_cd, d.identity_id, 
            d.ip_address, d.new_visitor_flg, d.platform_desc, d.platform_type_nm, 
            d.profile_nm4, d.screen_size_txt, d.session_id, d.visitor_id, 
            d.app_version, d.channel_nm, d.device_nm, d.organization_nm, 
            d.platform_version, d.profile_nm3, d.profile_nm5, d.sdk_version, 
            d.session_id_hex, d.user_language_cd  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :session_details_tmp , session_details , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..session_details_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..session_details_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..SESSION_DETAILS;
         drop table work.SESSION_DETAILS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table SESSION_DETAILS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..SESSION_DETAILS_EXT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..SESSION_DETAILS_EXT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..session_details_ext_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..session_details_ext_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=session_details_ext , table_keys=%str(LAST_SESSION_ACTIVITY_DTTM,SESSION_ID), out_table=work.session_details_ext );
   data work.session_details_ext_tmp /view=work.session_details_ext_tmp ;
      set work.session_details_ext ;
      if last_session_activity_dttm_tz  ne . then last_session_activity_dttm_tz = tzoneu2s(last_session_activity_dttm_tz ,&timeZone_Value.);
      if session_expiration_dttm_tz  ne . then session_expiration_dttm_tz = tzoneu2s(session_expiration_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :session_details_ext_tmp , session_details_ext );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..session_details_ext_tmp ;
            set work.session_details_ext_tmp ;
            stop;
         run;
         proc append data=work.session_details_ext_tmp  base=&tmplib..session_details_ext_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..session_details_ext_tmp ;
            set work.session_details_ext_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :session_details_ext_tmp , session_details_ext );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..session_details_ext as b using &tmpdbschema..session_details_ext_tmp as d on( 
            b.last_session_activity_dttm = d.last_session_activity_dttm and 
            b.session_id = d.session_id )
         when matched then  
         update set 
            b.active_sec_spent_in_sessn_cnt = d.active_sec_spent_in_sessn_cnt, 
            b.seconds_spent_in_session_cnt = d.seconds_spent_in_session_cnt, b.load_dttm = d.load_dttm, 
            b.session_expiration_dttm = d.session_expiration_dttm, b.last_session_activity_dttm_tz = d.last_session_activity_dttm_tz, 
            b.session_expiration_dttm_tz = d.session_expiration_dttm_tz, b.session_id_hex = d.session_id_hex
         when not matched then insert ( 
            active_sec_spent_in_sessn_cnt, seconds_spent_in_session_cnt, load_dttm, 
            last_session_activity_dttm, session_expiration_dttm, last_session_activity_dttm_tz, session_expiration_dttm_tz, 
            session_id, session_id_hex
         ) values ( 
            d.active_sec_spent_in_sessn_cnt, d.seconds_spent_in_session_cnt, d.load_dttm, 
            d.last_session_activity_dttm, d.session_expiration_dttm, d.last_session_activity_dttm_tz, d.session_expiration_dttm_tz, 
            d.session_id, d.session_id_hex  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :session_details_ext_tmp , session_details_ext , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..session_details_ext_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..session_details_ext_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..SESSION_DETAILS_EXT;
         drop table work.SESSION_DETAILS_EXT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table SESSION_DETAILS_EXT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..SMS_MESSAGE_CLICKED)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..SMS_MESSAGE_CLICKED));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..sms_message_clicked_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..sms_message_clicked_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=sms_message_clicked , table_keys=%str(EVENT_ID), out_table=work.sms_message_clicked );
   data work.sms_message_clicked_tmp /view=work.sms_message_clicked_tmp ;
      set work.sms_message_clicked ;
      if sms_click_dttm_tz  ne . then sms_click_dttm_tz = tzoneu2s(sms_click_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :sms_message_clicked_tmp , sms_message_clicked );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..sms_message_clicked_tmp ;
            set work.sms_message_clicked_tmp ;
            stop;
         run;
         proc append data=work.sms_message_clicked_tmp  base=&tmplib..sms_message_clicked_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..sms_message_clicked_tmp ;
            set work.sms_message_clicked_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :sms_message_clicked_tmp , sms_message_clicked );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..sms_message_clicked as b using &tmpdbschema..sms_message_clicked_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.sms_click_dttm_tz = d.sms_click_dttm_tz, 
            b.sms_click_dttm = d.sms_click_dttm, b.load_dttm = d.load_dttm, 
            b.task_id = d.task_id, b.sms_message_id = d.sms_message_id, 
            b.sender_id = d.sender_id, b.journey_occurrence_id = d.journey_occurrence_id, 
            b.event_nm = d.event_nm, b.country_cd = d.country_cd, 
            b.audience_id = d.audience_id, b.aud_occurrence_id = d.aud_occurrence_id, 
            b.context_type_nm = d.context_type_nm, b.creative_version_id = d.creative_version_id, 
            b.identity_id = d.identity_id, b.occurrence_id = d.occurrence_id, 
            b.context_val = d.context_val, b.creative_id = d.creative_id, 
            b.event_designed_id = d.event_designed_id, b.journey_id = d.journey_id, 
            b.response_tracking_cd = d.response_tracking_cd, b.task_version_id = d.task_version_id
         when not matched then insert ( 
            sms_click_dttm_tz, sms_click_dttm, load_dttm, 
            task_id, sms_message_id, sender_id, journey_occurrence_id, 
            event_nm, event_id, country_cd, audience_id, 
            aud_occurrence_id, context_type_nm, creative_version_id, identity_id, 
            occurrence_id, context_val, creative_id, event_designed_id, 
            journey_id, response_tracking_cd, task_version_id
         ) values ( 
            d.sms_click_dttm_tz, d.sms_click_dttm, d.load_dttm, 
            d.task_id, d.sms_message_id, d.sender_id, d.journey_occurrence_id, 
            d.event_nm, d.event_id, d.country_cd, d.audience_id, 
            d.aud_occurrence_id, d.context_type_nm, d.creative_version_id, d.identity_id, 
            d.occurrence_id, d.context_val, d.creative_id, d.event_designed_id, 
            d.journey_id, d.response_tracking_cd, d.task_version_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :sms_message_clicked_tmp , sms_message_clicked , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..sms_message_clicked_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..sms_message_clicked_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..SMS_MESSAGE_CLICKED;
         drop table work.SMS_MESSAGE_CLICKED;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table SMS_MESSAGE_CLICKED;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..SMS_MESSAGE_DELIVERED)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..SMS_MESSAGE_DELIVERED));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..sms_message_delivered_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..sms_message_delivered_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=sms_message_delivered , table_keys=%str(EVENT_ID), out_table=work.sms_message_delivered );
   data work.sms_message_delivered_tmp /view=work.sms_message_delivered_tmp ;
      set work.sms_message_delivered ;
      if sms_delivered_dttm_tz  ne . then sms_delivered_dttm_tz = tzoneu2s(sms_delivered_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :sms_message_delivered_tmp , sms_message_delivered );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..sms_message_delivered_tmp ;
            set work.sms_message_delivered_tmp ;
            stop;
         run;
         proc append data=work.sms_message_delivered_tmp  base=&tmplib..sms_message_delivered_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..sms_message_delivered_tmp ;
            set work.sms_message_delivered_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :sms_message_delivered_tmp , sms_message_delivered );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..sms_message_delivered as b using &tmpdbschema..sms_message_delivered_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.sms_delivered_dttm_tz = d.sms_delivered_dttm_tz, 
            b.sms_delivered_dttm = d.sms_delivered_dttm, b.load_dttm = d.load_dttm, 
            b.sms_message_id = d.sms_message_id, b.occurrence_id = d.occurrence_id, 
            b.journey_id = d.journey_id, b.identity_id = d.identity_id, 
            b.creative_version_id = d.creative_version_id, b.context_type_nm = d.context_type_nm, 
            b.aud_occurrence_id = d.aud_occurrence_id, b.country_cd = d.country_cd, 
            b.journey_occurrence_id = d.journey_occurrence_id, b.sender_id = d.sender_id, 
            b.task_id = d.task_id, b.audience_id = d.audience_id, 
            b.context_val = d.context_val, b.creative_id = d.creative_id, 
            b.event_designed_id = d.event_designed_id, b.event_nm = d.event_nm, 
            b.response_tracking_cd = d.response_tracking_cd, b.task_version_id = d.task_version_id
         when not matched then insert ( 
            sms_delivered_dttm_tz, sms_delivered_dttm, load_dttm, 
            sms_message_id, occurrence_id, journey_id, identity_id, 
            creative_version_id, context_type_nm, aud_occurrence_id, country_cd, 
            event_id, journey_occurrence_id, sender_id, task_id, 
            audience_id, context_val, creative_id, event_designed_id, 
            event_nm, response_tracking_cd, task_version_id
         ) values ( 
            d.sms_delivered_dttm_tz, d.sms_delivered_dttm, d.load_dttm, 
            d.sms_message_id, d.occurrence_id, d.journey_id, d.identity_id, 
            d.creative_version_id, d.context_type_nm, d.aud_occurrence_id, d.country_cd, 
            d.event_id, d.journey_occurrence_id, d.sender_id, d.task_id, 
            d.audience_id, d.context_val, d.creative_id, d.event_designed_id, 
            d.event_nm, d.response_tracking_cd, d.task_version_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :sms_message_delivered_tmp , sms_message_delivered , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..sms_message_delivered_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..sms_message_delivered_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..SMS_MESSAGE_DELIVERED;
         drop table work.SMS_MESSAGE_DELIVERED;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table SMS_MESSAGE_DELIVERED;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..SMS_MESSAGE_FAILED)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..SMS_MESSAGE_FAILED));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..sms_message_failed_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..sms_message_failed_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=sms_message_failed , table_keys=%str(EVENT_ID), out_table=work.sms_message_failed );
   data work.sms_message_failed_tmp /view=work.sms_message_failed_tmp ;
      set work.sms_message_failed ;
      if sms_failed_dttm_tz  ne . then sms_failed_dttm_tz = tzoneu2s(sms_failed_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :sms_message_failed_tmp , sms_message_failed );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..sms_message_failed_tmp ;
            set work.sms_message_failed_tmp ;
            stop;
         run;
         proc append data=work.sms_message_failed_tmp  base=&tmplib..sms_message_failed_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..sms_message_failed_tmp ;
            set work.sms_message_failed_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :sms_message_failed_tmp , sms_message_failed );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..sms_message_failed as b using &tmpdbschema..sms_message_failed_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.sms_failed_dttm_tz = d.sms_failed_dttm_tz, 
            b.load_dttm = d.load_dttm, b.sms_failed_dttm = d.sms_failed_dttm, 
            b.task_version_id = d.task_version_id, b.task_id = d.task_id, 
            b.sms_message_id = d.sms_message_id, b.reason_description_txt = d.reason_description_txt, 
            b.journey_occurrence_id = d.journey_occurrence_id, b.creative_id = d.creative_id, 
            b.country_cd = d.country_cd, b.aud_occurrence_id = d.aud_occurrence_id, 
            b.context_type_nm = d.context_type_nm, b.creative_version_id = d.creative_version_id, 
            b.event_nm = d.event_nm, b.identity_id = d.identity_id, 
            b.occurrence_id = d.occurrence_id, b.response_tracking_cd = d.response_tracking_cd, 
            b.sender_id = d.sender_id, b.audience_id = d.audience_id, 
            b.context_val = d.context_val, b.event_designed_id = d.event_designed_id, 
            b.journey_id = d.journey_id, b.reason_cd = d.reason_cd
         when not matched then insert ( 
            sms_failed_dttm_tz, load_dttm, sms_failed_dttm, 
            task_version_id, task_id, sms_message_id, reason_description_txt, 
            journey_occurrence_id, event_id, creative_id, country_cd, 
            aud_occurrence_id, context_type_nm, creative_version_id, event_nm, 
            identity_id, occurrence_id, response_tracking_cd, sender_id, 
            audience_id, context_val, event_designed_id, journey_id, 
            reason_cd
         ) values ( 
            d.sms_failed_dttm_tz, d.load_dttm, d.sms_failed_dttm, 
            d.task_version_id, d.task_id, d.sms_message_id, d.reason_description_txt, 
            d.journey_occurrence_id, d.event_id, d.creative_id, d.country_cd, 
            d.aud_occurrence_id, d.context_type_nm, d.creative_version_id, d.event_nm, 
            d.identity_id, d.occurrence_id, d.response_tracking_cd, d.sender_id, 
            d.audience_id, d.context_val, d.event_designed_id, d.journey_id, 
            d.reason_cd  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :sms_message_failed_tmp , sms_message_failed , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..sms_message_failed_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..sms_message_failed_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..SMS_MESSAGE_FAILED;
         drop table work.SMS_MESSAGE_FAILED;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table SMS_MESSAGE_FAILED;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..SMS_MESSAGE_REPLY)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..SMS_MESSAGE_REPLY));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..sms_message_reply_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..sms_message_reply_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=sms_message_reply , table_keys=%str(EVENT_ID), out_table=work.sms_message_reply );
   data work.sms_message_reply_tmp /view=work.sms_message_reply_tmp ;
      set work.sms_message_reply ;
      if sms_reply_dttm_tz  ne . then sms_reply_dttm_tz = tzoneu2s(sms_reply_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :sms_message_reply_tmp , sms_message_reply );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..sms_message_reply_tmp ;
            set work.sms_message_reply_tmp ;
            stop;
         run;
         proc append data=work.sms_message_reply_tmp  base=&tmplib..sms_message_reply_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..sms_message_reply_tmp ;
            set work.sms_message_reply_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :sms_message_reply_tmp , sms_message_reply );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..sms_message_reply as b using &tmpdbschema..sms_message_reply_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.load_dttm = d.load_dttm, 
            b.sms_reply_dttm_tz = d.sms_reply_dttm_tz, b.sms_reply_dttm = d.sms_reply_dttm, 
            b.task_version_id = d.task_version_id, b.sms_message_id = d.sms_message_id, 
            b.response_tracking_cd = d.response_tracking_cd, b.occurrence_id = d.occurrence_id, 
            b.identity_id = d.identity_id, b.country_cd = d.country_cd, 
            b.aud_occurrence_id = d.aud_occurrence_id, b.context_type_nm = d.context_type_nm, 
            b.journey_id = d.journey_id, b.journey_occurrence_id = d.journey_occurrence_id, 
            b.sender_id = d.sender_id, b.task_id = d.task_id, 
            b.audience_id = d.audience_id, b.context_val = d.context_val, 
            b.event_designed_id = d.event_designed_id, b.event_nm = d.event_nm, 
            b.sms_content = d.sms_content
         when not matched then insert ( 
            load_dttm, sms_reply_dttm_tz, sms_reply_dttm, 
            task_version_id, sms_message_id, response_tracking_cd, occurrence_id, 
            identity_id, country_cd, aud_occurrence_id, context_type_nm, 
            event_id, journey_id, journey_occurrence_id, sender_id, 
            task_id, audience_id, context_val, event_designed_id, 
            event_nm, sms_content
         ) values ( 
            d.load_dttm, d.sms_reply_dttm_tz, d.sms_reply_dttm, 
            d.task_version_id, d.sms_message_id, d.response_tracking_cd, d.occurrence_id, 
            d.identity_id, d.country_cd, d.aud_occurrence_id, d.context_type_nm, 
            d.event_id, d.journey_id, d.journey_occurrence_id, d.sender_id, 
            d.task_id, d.audience_id, d.context_val, d.event_designed_id, 
            d.event_nm, d.sms_content  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :sms_message_reply_tmp , sms_message_reply , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..sms_message_reply_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..sms_message_reply_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..SMS_MESSAGE_REPLY;
         drop table work.SMS_MESSAGE_REPLY;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table SMS_MESSAGE_REPLY;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..SMS_MESSAGE_SEND)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..SMS_MESSAGE_SEND));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..sms_message_send_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..sms_message_send_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=sms_message_send , table_keys=%str(EVENT_ID), out_table=work.sms_message_send );
   data work.sms_message_send_tmp /view=work.sms_message_send_tmp ;
      set work.sms_message_send ;
      if sms_send_dttm_tz  ne . then sms_send_dttm_tz = tzoneu2s(sms_send_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :sms_message_send_tmp , sms_message_send );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..sms_message_send_tmp ;
            set work.sms_message_send_tmp ;
            stop;
         run;
         proc append data=work.sms_message_send_tmp  base=&tmplib..sms_message_send_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..sms_message_send_tmp ;
            set work.sms_message_send_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :sms_message_send_tmp , sms_message_send );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..sms_message_send as b using &tmpdbschema..sms_message_send_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.fragment_cnt = d.fragment_cnt, 
            b.sms_send_dttm = d.sms_send_dttm, b.sms_send_dttm_tz = d.sms_send_dttm_tz, 
            b.load_dttm = d.load_dttm, b.occurrence_id = d.occurrence_id, 
            b.identity_id = d.identity_id, b.event_designed_id = d.event_designed_id, 
            b.context_val = d.context_val, b.aud_occurrence_id = d.aud_occurrence_id, 
            b.audience_id = d.audience_id, b.country_cd = d.country_cd, 
            b.creative_id = d.creative_id, b.event_nm = d.event_nm, 
            b.journey_id = d.journey_id, b.journey_occurrence_id = d.journey_occurrence_id, 
            b.sender_id = d.sender_id, b.task_id = d.task_id, 
            b.context_type_nm = d.context_type_nm, b.creative_version_id = d.creative_version_id, 
            b.response_tracking_cd = d.response_tracking_cd, b.sms_message_id = d.sms_message_id, 
            b.task_version_id = d.task_version_id
         when not matched then insert ( 
            fragment_cnt, sms_send_dttm, sms_send_dttm_tz, 
            load_dttm, occurrence_id, identity_id, event_id, 
            event_designed_id, context_val, aud_occurrence_id, audience_id, 
            country_cd, creative_id, event_nm, journey_id, 
            journey_occurrence_id, sender_id, task_id, context_type_nm, 
            creative_version_id, response_tracking_cd, sms_message_id, task_version_id
         ) values ( 
            d.fragment_cnt, d.sms_send_dttm, d.sms_send_dttm_tz, 
            d.load_dttm, d.occurrence_id, d.identity_id, d.event_id, 
            d.event_designed_id, d.context_val, d.aud_occurrence_id, d.audience_id, 
            d.country_cd, d.creative_id, d.event_nm, d.journey_id, 
            d.journey_occurrence_id, d.sender_id, d.task_id, d.context_type_nm, 
            d.creative_version_id, d.response_tracking_cd, d.sms_message_id, d.task_version_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :sms_message_send_tmp , sms_message_send , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..sms_message_send_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..sms_message_send_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..SMS_MESSAGE_SEND;
         drop table work.SMS_MESSAGE_SEND;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table SMS_MESSAGE_SEND;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..SMS_OPTOUT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..SMS_OPTOUT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..sms_optout_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..sms_optout_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=sms_optout , table_keys=%str(EVENT_ID), out_table=work.sms_optout );
   data work.sms_optout_tmp /view=work.sms_optout_tmp ;
      set work.sms_optout ;
      if sms_optout_dttm_tz  ne . then sms_optout_dttm_tz = tzoneu2s(sms_optout_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :sms_optout_tmp , sms_optout );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..sms_optout_tmp ;
            set work.sms_optout_tmp ;
            stop;
         run;
         proc append data=work.sms_optout_tmp  base=&tmplib..sms_optout_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..sms_optout_tmp ;
            set work.sms_optout_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :sms_optout_tmp , sms_optout );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..sms_optout as b using &tmpdbschema..sms_optout_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.load_dttm = d.load_dttm, 
            b.sms_optout_dttm = d.sms_optout_dttm, b.sms_optout_dttm_tz = d.sms_optout_dttm_tz, 
            b.task_id = d.task_id, b.sms_message_id = d.sms_message_id, 
            b.sender_id = d.sender_id, b.journey_occurrence_id = d.journey_occurrence_id, 
            b.country_cd = d.country_cd, b.aud_occurrence_id = d.aud_occurrence_id, 
            b.context_type_nm = d.context_type_nm, b.creative_version_id = d.creative_version_id, 
            b.identity_id = d.identity_id, b.occurrence_id = d.occurrence_id, 
            b.audience_id = d.audience_id, b.context_val = d.context_val, 
            b.creative_id = d.creative_id, b.event_designed_id = d.event_designed_id, 
            b.event_nm = d.event_nm, b.journey_id = d.journey_id, 
            b.response_tracking_cd = d.response_tracking_cd, b.task_version_id = d.task_version_id
         when not matched then insert ( 
            load_dttm, sms_optout_dttm, sms_optout_dttm_tz, 
            task_id, sms_message_id, sender_id, journey_occurrence_id, 
            event_id, country_cd, aud_occurrence_id, context_type_nm, 
            creative_version_id, identity_id, occurrence_id, audience_id, 
            context_val, creative_id, event_designed_id, event_nm, 
            journey_id, response_tracking_cd, task_version_id
         ) values ( 
            d.load_dttm, d.sms_optout_dttm, d.sms_optout_dttm_tz, 
            d.task_id, d.sms_message_id, d.sender_id, d.journey_occurrence_id, 
            d.event_id, d.country_cd, d.aud_occurrence_id, d.context_type_nm, 
            d.creative_version_id, d.identity_id, d.occurrence_id, d.audience_id, 
            d.context_val, d.creative_id, d.event_designed_id, d.event_nm, 
            d.journey_id, d.response_tracking_cd, d.task_version_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :sms_optout_tmp , sms_optout , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..sms_optout_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..sms_optout_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..SMS_OPTOUT;
         drop table work.SMS_OPTOUT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table SMS_OPTOUT;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..SMS_OPTOUT_DETAILS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..SMS_OPTOUT_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..sms_optout_details_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..sms_optout_details_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=sms_optout_details , table_keys=%str(EVENT_ID), out_table=work.sms_optout_details );
   data work.sms_optout_details_tmp /view=work.sms_optout_details_tmp ;
      set work.sms_optout_details ;
      if sms_optout_dttm_tz  ne . then sms_optout_dttm_tz = tzoneu2s(sms_optout_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :sms_optout_details_tmp , sms_optout_details );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..sms_optout_details_tmp ;
            set work.sms_optout_details_tmp ;
            stop;
         run;
         proc append data=work.sms_optout_details_tmp  base=&tmplib..sms_optout_details_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..sms_optout_details_tmp ;
            set work.sms_optout_details_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :sms_optout_details_tmp , sms_optout_details );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..sms_optout_details as b using &tmpdbschema..sms_optout_details_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.load_dttm = d.load_dttm, 
            b.sms_optout_dttm = d.sms_optout_dttm, b.sms_optout_dttm_tz = d.sms_optout_dttm_tz, 
            b.task_version_id = d.task_version_id, b.sms_message_id = d.sms_message_id, 
            b.occurrence_id = d.occurrence_id, b.event_nm = d.event_nm, 
            b.creative_id = d.creative_id, b.context_type_nm = d.context_type_nm, 
            b.audience_id = d.audience_id, b.address_val = d.address_val, 
            b.context_val = d.context_val, b.event_designed_id = d.event_designed_id, 
            b.journey_id = d.journey_id, b.response_tracking_cd = d.response_tracking_cd, 
            b.task_id = d.task_id, b.aud_occurrence_id = d.aud_occurrence_id, 
            b.country_cd = d.country_cd, b.creative_version_id = d.creative_version_id, 
            b.identity_id = d.identity_id, b.journey_occurrence_id = d.journey_occurrence_id, 
            b.sender_id = d.sender_id
         when not matched then insert ( 
            load_dttm, sms_optout_dttm, sms_optout_dttm_tz, 
            task_version_id, sms_message_id, occurrence_id, event_nm, 
            creative_id, context_type_nm, audience_id, address_val, 
            context_val, event_designed_id, journey_id, response_tracking_cd, 
            task_id, aud_occurrence_id, country_cd, creative_version_id, 
            event_id, identity_id, journey_occurrence_id, sender_id
         ) values ( 
            d.load_dttm, d.sms_optout_dttm, d.sms_optout_dttm_tz, 
            d.task_version_id, d.sms_message_id, d.occurrence_id, d.event_nm, 
            d.creative_id, d.context_type_nm, d.audience_id, d.address_val, 
            d.context_val, d.event_designed_id, d.journey_id, d.response_tracking_cd, 
            d.task_id, d.aud_occurrence_id, d.country_cd, d.creative_version_id, 
            d.event_id, d.identity_id, d.journey_occurrence_id, d.sender_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :sms_optout_details_tmp , sms_optout_details , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..sms_optout_details_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..sms_optout_details_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..SMS_OPTOUT_DETAILS;
         drop table work.SMS_OPTOUT_DETAILS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table SMS_OPTOUT_DETAILS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..SPOT_CLICKED)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..SPOT_CLICKED));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..spot_clicked_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..spot_clicked_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=spot_clicked , table_keys=%str(EVENT_ID), out_table=work.spot_clicked );
   data work.spot_clicked_tmp /view=work.spot_clicked_tmp ;
      set work.spot_clicked ;
      if spot_clicked_dttm_tz  ne . then spot_clicked_dttm_tz = tzoneu2s(spot_clicked_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :spot_clicked_tmp , spot_clicked );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..spot_clicked_tmp ;
            set work.spot_clicked_tmp ;
            stop;
         run;
         proc append data=work.spot_clicked_tmp  base=&tmplib..spot_clicked_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..spot_clicked_tmp ;
            set work.spot_clicked_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :spot_clicked_tmp , spot_clicked );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..spot_clicked as b using &tmpdbschema..spot_clicked_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.control_group_flg = d.control_group_flg, 
            b.product_qty_no = d.product_qty_no, b.properties_map_doc = d.properties_map_doc, 
            b.spot_clicked_dttm = d.spot_clicked_dttm, b.load_dttm = d.load_dttm, 
            b.spot_clicked_dttm_tz = d.spot_clicked_dttm_tz, b.session_id_hex = d.session_id_hex, 
            b.reserved_2_txt = d.reserved_2_txt, b.rec_group_id = d.rec_group_id, 
            b.product_id = d.product_id, b.message_id = d.message_id, 
            b.event_source_cd = d.event_source_cd, b.event_nm = d.event_nm, 
            b.detail_id_hex = d.detail_id_hex, b.context_val = d.context_val, 
            b.channel_user_id = d.channel_user_id, b.creative_id = d.creative_id, 
            b.identity_id = d.identity_id, b.mobile_app_id = d.mobile_app_id, 
            b.product_nm = d.product_nm, b.product_sku_no = d.product_sku_no, 
            b.request_id = d.request_id, b.segment_id = d.segment_id, 
            b.spot_id = d.spot_id, b.channel_nm = d.channel_nm, 
            b.context_type_nm = d.context_type_nm, b.creative_version_id = d.creative_version_id, 
            b.event_designed_id = d.event_designed_id, b.event_key_cd = d.event_key_cd, 
            b.message_version_id = d.message_version_id, b.occurrence_id = d.occurrence_id, 
            b.reserved_1_txt = d.reserved_1_txt, b.response_tracking_cd = d.response_tracking_cd, 
            b.segment_version_id = d.segment_version_id, b.visit_id_hex = d.visit_id_hex, 
            b.url_txt = d.url_txt, b.task_version_id = d.task_version_id, 
            b.task_id = d.task_id
         when not matched then insert ( 
            control_group_flg, product_qty_no, properties_map_doc, 
            spot_clicked_dttm, load_dttm, spot_clicked_dttm_tz, session_id_hex, 
            reserved_2_txt, rec_group_id, product_id, message_id, 
            event_source_cd, event_nm, detail_id_hex, context_val, 
            channel_user_id, creative_id, event_id, identity_id, 
            mobile_app_id, product_nm, product_sku_no, request_id, 
            segment_id, spot_id, channel_nm, context_type_nm, 
            creative_version_id, event_designed_id, event_key_cd, message_version_id, 
            occurrence_id, reserved_1_txt, response_tracking_cd, segment_version_id, 
            visit_id_hex, url_txt, task_version_id, task_id
         ) values ( 
            d.control_group_flg, d.product_qty_no, d.properties_map_doc, 
            d.spot_clicked_dttm, d.load_dttm, d.spot_clicked_dttm_tz, d.session_id_hex, 
            d.reserved_2_txt, d.rec_group_id, d.product_id, d.message_id, 
            d.event_source_cd, d.event_nm, d.detail_id_hex, d.context_val, 
            d.channel_user_id, d.creative_id, d.event_id, d.identity_id, 
            d.mobile_app_id, d.product_nm, d.product_sku_no, d.request_id, 
            d.segment_id, d.spot_id, d.channel_nm, d.context_type_nm, 
            d.creative_version_id, d.event_designed_id, d.event_key_cd, d.message_version_id, 
            d.occurrence_id, d.reserved_1_txt, d.response_tracking_cd, d.segment_version_id, 
            d.visit_id_hex, d.url_txt, d.task_version_id, d.task_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :spot_clicked_tmp , spot_clicked , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..spot_clicked_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..spot_clicked_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..SPOT_CLICKED;
         drop table work.SPOT_CLICKED;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table SPOT_CLICKED;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..SPOT_REQUESTED)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..SPOT_REQUESTED));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..spot_requested_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..spot_requested_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=spot_requested , table_keys=%str(EVENT_ID), out_table=work.spot_requested );
   data work.spot_requested_tmp /view=work.spot_requested_tmp ;
      set work.spot_requested ;
      if spot_requested_dttm_tz  ne . then spot_requested_dttm_tz = tzoneu2s(spot_requested_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :spot_requested_tmp , spot_requested );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..spot_requested_tmp ;
            set work.spot_requested_tmp ;
            stop;
         run;
         proc append data=work.spot_requested_tmp  base=&tmplib..spot_requested_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..spot_requested_tmp ;
            set work.spot_requested_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :spot_requested_tmp , spot_requested );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..spot_requested as b using &tmpdbschema..spot_requested_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.properties_map_doc = d.properties_map_doc, 
            b.load_dttm = d.load_dttm, b.spot_requested_dttm_tz = d.spot_requested_dttm_tz, 
            b.spot_requested_dttm = d.spot_requested_dttm, b.visit_id_hex = d.visit_id_hex, 
            b.spot_id = d.spot_id, b.session_id_hex = d.session_id_hex, 
            b.request_id = d.request_id, b.mobile_app_id = d.mobile_app_id, 
            b.identity_id = d.identity_id, b.event_source_cd = d.event_source_cd, 
            b.event_nm = d.event_nm, b.event_designed_id = d.event_designed_id, 
            b.detail_id_hex = d.detail_id_hex, b.context_val = d.context_val, 
            b.context_type_nm = d.context_type_nm, b.channel_user_id = d.channel_user_id, 
            b.channel_nm = d.channel_nm
         when not matched then insert ( 
            properties_map_doc, load_dttm, spot_requested_dttm_tz, 
            spot_requested_dttm, visit_id_hex, spot_id, session_id_hex, 
            request_id, mobile_app_id, identity_id, event_source_cd, 
            event_nm, event_id, event_designed_id, detail_id_hex, 
            context_val, context_type_nm, channel_user_id, channel_nm
         ) values ( 
            d.properties_map_doc, d.load_dttm, d.spot_requested_dttm_tz, 
            d.spot_requested_dttm, d.visit_id_hex, d.spot_id, d.session_id_hex, 
            d.request_id, d.mobile_app_id, d.identity_id, d.event_source_cd, 
            d.event_nm, d.event_id, d.event_designed_id, d.detail_id_hex, 
            d.context_val, d.context_type_nm, d.channel_user_id, d.channel_nm  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :spot_requested_tmp , spot_requested , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..spot_requested_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..spot_requested_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..SPOT_REQUESTED;
         drop table work.SPOT_REQUESTED;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table SPOT_REQUESTED;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..TAG_DETAILS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..TAG_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..TAG_DETAILS) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..tag_details  base=&trglib..tag_details (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..TAG_DETAILS (
            created_dttm, load_dttm, last_modified_dttm, 
            tag_owner_usernm, tag_nm, tag_id, tag_desc, 
            last_modified_usernm, identity_cd, created_by_usernm, component_type, 
            component_id )
      select created_dttm, load_dttm, last_modified_dttm, 
            tag_owner_usernm, tag_nm, tag_id, tag_desc, 
            last_modified_usernm, identity_cd, created_by_usernm, component_type, 
            component_id
         from &udmmart..tag_details ;
      quit;
   %end;
   %err_check (Failed to insert into tag_details , tag_details );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..TAG_DETAILS;
         drop table work.TAG_DETAILS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table TAG_DETAILS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..VISIT_DETAILS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..VISIT_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..visit_details_tmp ) ) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..visit_details_tmp ;
      quit;
   %end;
   %check_duplicate_from_source(table_nm=visit_details , table_keys=%str(EVENT_ID), out_table=work.visit_details );
   data work.visit_details_tmp /view=work.visit_details_tmp ;
      set work.visit_details ;
      if visit_dttm_tz  ne . then visit_dttm_tz = tzoneu2s(visit_dttm_tz ,&timeZone_Value.);
   run;
   %err_check (Failed to add time zone adaptation :visit_details_tmp , visit_details );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         data &tmplib..visit_details_tmp ;
            set work.visit_details_tmp ;
            stop;
         run;
         proc append data=work.visit_details_tmp  base=&tmplib..visit_details_tmp (&DB_BL_OPTS) force;
         run;
      %end;
      %else %do;
         data &tmplib..visit_details_tmp ;
            set work.visit_details_tmp ;
         run;
      %end;
      %err_check (Failed to upload to temp location in DB :visit_details_tmp , visit_details );
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute (merge into &dbschema..visit_details as b using &tmpdbschema..visit_details_tmp as d on( 
            b.event_id = d.event_id )
         when matched then  
         update set 
            b.sequence_no = d.sequence_no, 
            b.visit_dttm_tz = d.visit_dttm_tz, b.load_dttm = d.load_dttm, 
            b.visit_dttm = d.visit_dttm, b.visit_id_hex = d.visit_id_hex, 
            b.visit_id = d.visit_id, b.session_id_hex = d.session_id_hex, 
            b.session_id = d.session_id, b.search_term_txt = d.search_term_txt, 
            b.search_engine_domain_txt = d.search_engine_domain_txt, b.search_engine_desc = d.search_engine_desc, 
            b.referrer_txt = d.referrer_txt, b.referrer_query_string_txt = d.referrer_query_string_txt, 
            b.referrer_domain_nm = d.referrer_domain_nm, b.origination_type_nm = d.origination_type_nm, 
            b.origination_tracking_cd = d.origination_tracking_cd, b.origination_placement_nm = d.origination_placement_nm, 
            b.origination_nm = d.origination_nm, b.origination_creative_nm = d.origination_creative_nm, 
            b.identity_id = d.identity_id
         when not matched then insert ( 
            sequence_no, visit_dttm_tz, load_dttm, 
            visit_dttm, visit_id_hex, visit_id, session_id_hex, 
            session_id, search_term_txt, search_engine_domain_txt, search_engine_desc, 
            referrer_txt, referrer_query_string_txt, referrer_domain_nm, origination_type_nm, 
            origination_tracking_cd, origination_placement_nm, origination_nm, origination_creative_nm, 
            identity_id, event_id
         ) values ( 
            d.sequence_no, d.visit_dttm_tz, d.load_dttm, 
            d.visit_dttm, d.visit_id_hex, d.visit_id, d.session_id_hex, 
            d.session_id, d.search_term_txt, d.search_engine_domain_txt, d.search_engine_desc, 
            d.referrer_txt, d.referrer_query_string_txt, d.referrer_domain_nm, d.origination_type_nm, 
            d.origination_tracking_cd, d.origination_placement_nm, d.origination_nm, d.origination_creative_nm, 
            d.identity_id, d.event_id  );) by &database.;
         disconnect from &database.;
      quit;
      %err_check (Failed to Update/Insert into  :visit_details_tmp , visit_details , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..visit_details_tmp )) %then %do;
      proc sql noerrorstop;
         drop table &tmplib..visit_details_tmp ;
      quit;
   %end;
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..VISIT_DETAILS;
         drop table work.VISIT_DETAILS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table VISIT_DETAILS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..WF_PROCESS_DETAILS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..WF_PROCESS_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..WF_PROCESS_DETAILS) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..wf_process_details  base=&trglib..wf_process_details (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..WF_PROCESS_DETAILS (
            delayed_by_day, percent_complete, user_tasks_cnt, 
            completed_dttm, indexed_dttm, planned_end_dttm, load_dttm, 
            timeline_calculated_dttm, start_dttm, published_dttm, submitted_dttm, 
            deleted_dttm, created_dttm, last_modified_dttm, projected_end_dttm, 
            submitted_by_usernm, published_by_usernm, process_type, process_status, 
            process_nm, process_id, process_comment, process_category, 
            pdef_id, modified_status_cd, last_modified_usernm, deleted_by_usernm, 
            created_by_usernm, business_info_type, business_info_nm, business_info_id, 
            process_desc, process_instance_version, process_owner_usernm )
      select delayed_by_day, percent_complete, user_tasks_cnt, 
            completed_dttm, indexed_dttm, planned_end_dttm, load_dttm, 
            timeline_calculated_dttm, start_dttm, published_dttm, submitted_dttm, 
            deleted_dttm, created_dttm, last_modified_dttm, projected_end_dttm, 
            submitted_by_usernm, published_by_usernm, process_type, process_status, 
            process_nm, process_id, process_comment, process_category, 
            pdef_id, modified_status_cd, last_modified_usernm, deleted_by_usernm, 
            created_by_usernm, business_info_type, business_info_nm, business_info_id, 
            process_desc, process_instance_version, process_owner_usernm
         from &udmmart..wf_process_details ;
      quit;
   %end;
   %err_check (Failed to insert into wf_process_details , wf_process_details );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..WF_PROCESS_DETAILS;
         drop table work.WF_PROCESS_DETAILS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table WF_PROCESS_DETAILS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..WF_PROCESS_DETAILS_CUSTOM_PROP)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..WF_PROCESS_DETAILS_CUSTOM_PROP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..WF_PROCESS_DETAILS_CUSTOM_PROP) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..wf_process_details_custom_prop  base=&trglib..wf_process_details_custom_prop (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..WF_PROCESS_DETAILS_CUSTOM_PROP (
            attr_val, is_grid_flg, is_obsolete_flg, 
            load_dttm, created_dttm, last_modified_dttm, process_id, 
            last_modified_usernm, data_type, data_formatter, created_by_usernm, 
            attr_id, attr_group_nm, attr_group_id, attr_cd, 
            attr_group_cd, attr_nm, remote_pklist_tab_col )
      select attr_val, is_grid_flg, is_obsolete_flg, 
            load_dttm, created_dttm, last_modified_dttm, process_id, 
            last_modified_usernm, data_type, data_formatter, created_by_usernm, 
            attr_id, attr_group_nm, attr_group_id, attr_cd, 
            attr_group_cd, attr_nm, remote_pklist_tab_col
         from &udmmart..wf_process_details_custom_prop ;
      quit;
   %end;
   %err_check (Failed to insert into wf_process_details_custom_prop , wf_process_details_custom_prop );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..WF_PROCESS_DETAILS_CUSTOM_PROP;
         drop table work.WF_PROCESS_DETAILS_CUSTOM_PROP;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table WF_PROCESS_DETAILS_CUSTOM_PROP;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..WF_PROCESS_TASKS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..WF_PROCESS_TASKS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..WF_PROCESS_TASKS) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..wf_process_tasks  base=&trglib..wf_process_tasks (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..WF_PROCESS_TASKS (
            version_num, duration_per_assignee, delayed_by_day, 
            percent_complete, existobj_update_flg, multi_assig_suprt_flg, locally_updated_flg, 
            latest_flg, skip_peerupdate_scanning_flg, approval_task_flg, first_usertask_flg, 
            cancelled_task_flg, skip_update_scanning_flg, is_sequential_flg, projected_start_dttm, 
            published_dttm, load_dttm, started_dttm, indexed_dttm, 
            created_dttm, modified_dttm, projected_end_dttm, completed_dttm, 
            deleted_dttm, due_dttm, engine_task_cancelled_dttm, task_type, 
            task_status, task_instruction, task_id, task_desc, 
            task_attachment, published_by_usernm, process_id, owner_usernm, 
            modified_status_cd, modified_by_usernm, instance_version, engine_taskdef_id, 
            deleted_by_usernm, created_by_usernm, task_comment, task_nm, 
            task_subtype )
      select version_num, duration_per_assignee, delayed_by_day, 
            percent_complete, existobj_update_flg, multi_assig_suprt_flg, locally_updated_flg, 
            latest_flg, skip_peerupdate_scanning_flg, approval_task_flg, first_usertask_flg, 
            cancelled_task_flg, skip_update_scanning_flg, is_sequential_flg, projected_start_dttm, 
            published_dttm, load_dttm, started_dttm, indexed_dttm, 
            created_dttm, modified_dttm, projected_end_dttm, completed_dttm, 
            deleted_dttm, due_dttm, engine_task_cancelled_dttm, task_type, 
            task_status, task_instruction, task_id, task_desc, 
            task_attachment, published_by_usernm, process_id, owner_usernm, 
            modified_status_cd, modified_by_usernm, instance_version, engine_taskdef_id, 
            deleted_by_usernm, created_by_usernm, task_comment, task_nm, 
            task_subtype
         from &udmmart..wf_process_tasks ;
      quit;
   %end;
   %err_check (Failed to insert into wf_process_tasks , wf_process_tasks );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..WF_PROCESS_TASKS;
         drop table work.WF_PROCESS_TASKS;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table WF_PROCESS_TASKS;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..WF_TASKS_USER_ASSIGNMENT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid =%sysfunc(open(&udmmart..WF_TASKS_USER_ASSIGNMENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid =%sysfunc(close(&dsid));
   proc sql noerrorstop;
      connect to &database. (&sql_passthru_connection.);
      execute (truncate table &dbschema..WF_TASKS_USER_ASSIGNMENT) by &database.;
      disconnect from &database.;
   quit;
   %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
      proc append data=&udmmart..wf_tasks_user_assignment  base=&trglib..wf_tasks_user_assignment (&DB_BL_OPTS) force;
      run;
   %end;
   %else %do;
      proc sql;
         insert into &trglib..WF_TASKS_USER_ASSIGNMENT (
            usan_duration_day, delayed_by_day, is_assigned_flg, 
            is_replaced_flg, activation_completed_flg, is_latest_flg, created_dttm, 
            modified_dttm, projected_end_dttm, deleted_dttm, completed_dttm, 
            due_dttm, load_dttm, projected_start_dttm, start_dttm, 
            user_nm, user_assignment_id, usan_status, usan_instruction, 
            usan_desc, task_id, replacement_userid, replacement_reason, 
            replacement_assignee_id, process_id, owner_usernm, modified_status_cd, 
            modified_by_usernm, instance_version, initiator_comment, deleted_by_usernm, 
            created_by_usernm, assignee_id, approval_status, assignee_type, 
            usan_comment, user_id )
      select usan_duration_day, delayed_by_day, is_assigned_flg, 
            is_replaced_flg, activation_completed_flg, is_latest_flg, created_dttm, 
            modified_dttm, projected_end_dttm, deleted_dttm, completed_dttm, 
            due_dttm, load_dttm, projected_start_dttm, start_dttm, 
            user_nm, user_assignment_id, usan_status, usan_instruction, 
            usan_desc, task_id, replacement_userid, replacement_reason, 
            replacement_assignee_id, process_id, owner_usernm, modified_status_cd, 
            modified_by_usernm, instance_version, initiator_comment, deleted_by_usernm, 
            created_by_usernm, assignee_id, approval_status, assignee_type, 
            usan_comment, user_id
         from &udmmart..wf_tasks_user_assignment ;
      quit;
   %end;
   %err_check (Failed to insert into wf_tasks_user_assignment , wf_tasks_user_assignment );
   %if &errFlag = 0 %then %do;
      proc sql noerrorstop;
         drop table &udmmart..WF_TASKS_USER_ASSIGNMENT;
         drop table work.WF_TASKS_USER_ASSIGNMENT;
      quit;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table WF_TASKS_USER_ASSIGNMENT;
   %put------------------------------------------------------------------;
%end;
%mend;
%execute_SQLSVR_etl;
