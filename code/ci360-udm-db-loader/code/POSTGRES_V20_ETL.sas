/*******************************************************************************/
/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                         */
/* *****************************************************************************/
%macro execute_POSTGRES_etl;
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
            interaction_cost = d.interaction_cost, 
            conversion_value = d.conversion_value, task_id = d.task_id, 
            load_id = d.load_id, interaction_type = d.interaction_type, 
            interaction_subtype = d.interaction_subtype, interaction = d.interaction, 
            identity_id = d.identity_id, creative_id = d.creative_id
         when not matched and INTERACTION_DTTM is NOT NULL and INTERACTION_ID is NOT NULL then insert (
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
            load_dttm = d.load_dttm, 
            abtestpath_assignment_dttm_tz = d.abtestpath_assignment_dttm_tz, abtestpath_assignment_dttm = d.abtestpath_assignment_dttm, 
            session_id_hex = d.session_id_hex, context_type_nm = d.context_type_nm, 
            channel_user_id = d.channel_user_id, identity_id = d.identity_id, 
            event_nm = d.event_nm, channel_nm = d.channel_nm, 
            event_designed_id = d.event_designed_id, abtest_path_id = d.abtest_path_id, 
            activity_id = d.activity_id, context_val = d.context_val
         when not matched and EVENT_ID is NOT NULL then insert (
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
            activity_conversion_dttm_tz = d.activity_conversion_dttm_tz, 
            load_dttm = d.load_dttm, activity_conversion_dttm = d.activity_conversion_dttm, 
            abtest_path_id = d.abtest_path_id, activity_id = d.activity_id, 
            activity_node_id = d.activity_node_id, session_id_hex = d.session_id_hex, 
            parent_event_designed_id = d.parent_event_designed_id, identity_id = d.identity_id, 
            goal_id = d.goal_id, event_nm = d.event_nm, 
            event_designed_id = d.event_designed_id, detail_id_hex = d.detail_id_hex, 
            context_val = d.context_val, channel_nm = d.channel_nm, 
            context_type_nm = d.context_type_nm, channel_user_id = d.channel_user_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            activity_flow_in_dttm = d.activity_flow_in_dttm, 
            activity_flow_in_dttm_tz = d.activity_flow_in_dttm_tz, load_dttm = d.load_dttm, 
            task_id = d.task_id, identity_id = d.identity_id, 
            context_val = d.context_val, event_designed_id = d.event_designed_id, 
            channel_user_id = d.channel_user_id, activity_node_id = d.activity_node_id, 
            activity_id = d.activity_id, abtest_path_id = d.abtest_path_id, 
            channel_nm = d.channel_nm, context_type_nm = d.context_type_nm, 
            event_nm = d.event_nm
         when not matched and EVENT_ID is NOT NULL then insert (
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
            activity_start_dttm_tz = d.activity_start_dttm_tz, 
            load_dttm = d.load_dttm, activity_start_dttm = d.activity_start_dttm, 
            channel_nm = d.channel_nm, activity_id = d.activity_id, 
            identity_id = d.identity_id, event_nm = d.event_nm, 
            channel_user_id = d.channel_user_id, event_designed_id = d.event_designed_id, 
            context_val = d.context_val, context_type_nm = d.context_type_nm
         when not matched and EVENT_ID is NOT NULL then insert (
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
            load_dttm = d.load_dttm, 
            advertising_contact_dttm_tz = d.advertising_contact_dttm_tz, advertising_contact_dttm = d.advertising_contact_dttm, 
            task_version_id = d.task_version_id, task_id = d.task_id, 
            task_action_nm = d.task_action_nm, segment_version_id = d.segment_version_id, 
            segment_id = d.segment_id, response_tracking_cd = d.response_tracking_cd, 
            occurrence_id = d.occurrence_id, journey_occurrence_id = d.journey_occurrence_id, 
            journey_id = d.journey_id, identity_id = d.identity_id, 
            event_nm = d.event_nm, event_designed_id = d.event_designed_id, 
            context_val = d.context_val, context_type_nm = d.context_type_nm, 
            channel_nm = d.channel_nm, audience_id = d.audience_id, 
            aud_occurrence_id = d.aud_occurrence_id, advertising_platform_nm = d.advertising_platform_nm
         when not matched and EVENT_ID is NOT NULL then insert (
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
   %err_check (Failed to truncate asset_details , asset_details );
   proc append data=&udmmart..asset_details  base=&trglib..asset_details (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to asset_details , asset_details );
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
   %err_check (Failed to truncate asset_details_custom_prop , asset_details_custom_prop );
   proc append data=&udmmart..asset_details_custom_prop  base=&trglib..asset_details_custom_prop (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to asset_details_custom_prop , asset_details_custom_prop );
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
   %err_check (Failed to truncate asset_folder_details , asset_folder_details );
   proc append data=&udmmart..asset_folder_details  base=&trglib..asset_folder_details (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to asset_folder_details , asset_folder_details );
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
   %err_check (Failed to truncate asset_rendition_details , asset_rendition_details );
   proc append data=&udmmart..asset_rendition_details  base=&trglib..asset_rendition_details (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to asset_rendition_details , asset_rendition_details );
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
   %err_check (Failed to truncate asset_revision , asset_revision );
   proc append data=&udmmart..asset_revision  base=&trglib..asset_revision (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to asset_revision , asset_revision );
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
            audience_change_dttm = d.audience_change_dttm, 
            load_dttm = d.load_dttm, audience_change_dttm_tz = d.audience_change_dttm_tz, 
            identity_id = d.identity_id, aud_occurrence_id = d.aud_occurrence_id, 
            audience_id = d.audience_id, event_nm = d.event_nm
         when not matched and EVENT_ID is NOT NULL then insert (
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
            is_start_flg = d.is_start_flg, 
            is_completion_flg = d.is_completion_flg, process_attempt_cnt = d.process_attempt_cnt, 
            step_order_no = d.step_order_no, process_instance_no = d.process_instance_no, 
            process_dttm_tz = d.process_dttm_tz, process_exception_dttm_tz = d.process_exception_dttm_tz, 
            load_dttm = d.load_dttm, process_dttm = d.process_dttm, 
            process_exception_dttm = d.process_exception_dttm, visit_id = d.visit_id, 
            process_step_nm = d.process_step_nm, process_details_sk = d.process_details_sk, 
            identity_id = d.identity_id, event_nm = d.event_nm, 
            detail_id = d.detail_id, attribute1_txt = d.attribute1_txt, 
            detail_id_hex = d.detail_id_hex, event_designed_id = d.event_designed_id, 
            next_detail_id = d.next_detail_id, process_exception_txt = d.process_exception_txt, 
            session_id = d.session_id, session_id_hex = d.session_id_hex, 
            visit_id_hex = d.visit_id_hex, attribute2_txt = d.attribute2_txt, 
            event_source_cd = d.event_source_cd, process_nm = d.process_nm
         when not matched and EVENT_ID is NOT NULL then insert (
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
            unit_price_amt = d.unit_price_amt, 
            displayed_cart_amt = d.displayed_cart_amt, quantity_val = d.quantity_val, 
            displayed_cart_items_no = d.displayed_cart_items_no, properties_map_doc = d.properties_map_doc, 
            activity_dttm = d.activity_dttm, load_dttm = d.load_dttm, 
            activity_dttm_tz = d.activity_dttm_tz, cart_activity_sk = d.cart_activity_sk, 
            activity_cd = d.activity_cd, visit_id_hex = d.visit_id_hex, 
            visit_id = d.visit_id, shipping_message_txt = d.shipping_message_txt, 
            session_id_hex = d.session_id_hex, session_id = d.session_id, 
            saving_message_txt = d.saving_message_txt, product_sku = d.product_sku, 
            product_nm = d.product_nm, product_id = d.product_id, 
            product_group_nm = d.product_group_nm, mobile_app_id = d.mobile_app_id, 
            identity_id = d.identity_id, event_source_cd = d.event_source_cd, 
            event_nm = d.event_nm, availability_message_txt = d.availability_message_txt, 
            cart_id = d.cart_id, event_designed_id = d.event_designed_id, 
            detail_id_hex = d.detail_id_hex, detail_id = d.detail_id, 
            currency_cd = d.currency_cd, event_key_cd = d.event_key_cd, 
            channel_nm = d.channel_nm, cart_nm = d.cart_nm
         when not matched and EVENT_ID is NOT NULL then insert (
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
   %err_check (Failed to truncate cc_budget_breakup , cc_budget_breakup );
   proc append data=&udmmart..cc_budget_breakup  base=&trglib..cc_budget_breakup (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cc_budget_breakup , cc_budget_breakup );
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
   %err_check (Failed to truncate cc_budget_breakup_ccbdgt , cc_budget_breakup_ccbdgt );
   proc append data=&udmmart..cc_budget_breakup_ccbdgt  base=&trglib..cc_budget_breakup_ccbdgt (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cc_budget_breakup_ccbdgt , cc_budget_breakup_ccbdgt );
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
   %err_check (Failed to truncate cdm_activity_custom_attr , cdm_activity_custom_attr );
   proc append data=&udmmart..cdm_activity_custom_attr  base=&trglib..cdm_activity_custom_attr (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_activity_custom_attr , cdm_activity_custom_attr );
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
   %err_check (Failed to truncate cdm_activity_detail , cdm_activity_detail );
   proc append data=&udmmart..cdm_activity_detail  base=&trglib..cdm_activity_detail (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_activity_detail , cdm_activity_detail );
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
   %err_check (Failed to truncate cdm_activity_x_task , cdm_activity_x_task );
   proc append data=&udmmart..cdm_activity_x_task  base=&trglib..cdm_activity_x_task (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_activity_x_task , cdm_activity_x_task );
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
   %err_check (Failed to truncate cdm_audience_detail , cdm_audience_detail );
   proc append data=&udmmart..cdm_audience_detail  base=&trglib..cdm_audience_detail (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_audience_detail , cdm_audience_detail );
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
   %err_check (Failed to truncate cdm_audience_occur_detail , cdm_audience_occur_detail );
   proc append data=&udmmart..cdm_audience_occur_detail  base=&trglib..cdm_audience_occur_detail (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_audience_occur_detail , cdm_audience_occur_detail );
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
   %err_check (Failed to truncate cdm_audience_x_segment , cdm_audience_x_segment );
   proc append data=&udmmart..cdm_audience_x_segment  base=&trglib..cdm_audience_x_segment (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_audience_x_segment , cdm_audience_x_segment );
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
   %err_check (Failed to truncate cdm_business_context , cdm_business_context );
   proc append data=&udmmart..cdm_business_context  base=&trglib..cdm_business_context (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_business_context , cdm_business_context );
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
   %err_check (Failed to truncate cdm_campaign_custom_attr , cdm_campaign_custom_attr );
   proc append data=&udmmart..cdm_campaign_custom_attr  base=&trglib..cdm_campaign_custom_attr (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_campaign_custom_attr , cdm_campaign_custom_attr );
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
   %err_check (Failed to truncate cdm_campaign_detail , cdm_campaign_detail );
   proc append data=&udmmart..cdm_campaign_detail  base=&trglib..cdm_campaign_detail (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_campaign_detail , cdm_campaign_detail );
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
   %err_check (Failed to truncate cdm_contact_channel , cdm_contact_channel );
   proc append data=&udmmart..cdm_contact_channel  base=&trglib..cdm_contact_channel (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_contact_channel , cdm_contact_channel );
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
            optimization_backfill_flg = d.optimization_backfill_flg, 
            control_group_flg = d.control_group_flg, contact_dt = d.contact_dt, 
            updated_dttm = d.updated_dttm, contact_dttm_tz = d.contact_dttm_tz, 
            contact_dttm = d.contact_dttm, source_system_cd = d.source_system_cd, 
            external_contact_info_2_id = d.external_contact_info_2_id, context_type_nm = d.context_type_nm, 
            audience_id = d.audience_id, contact_nm = d.contact_nm, 
            identity_id = d.identity_id, audience_occur_id = d.audience_occur_id, 
            contact_status_cd = d.contact_status_cd, context_val = d.context_val, 
            external_contact_info_1_id = d.external_contact_info_1_id, rtc_id = d.rtc_id, 
            updated_by_nm = d.updated_by_nm
         when not matched and CONTACT_ID is NOT NULL then insert (
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
   %err_check (Failed to truncate cdm_contact_status , cdm_contact_status );
   proc append data=&udmmart..cdm_contact_status  base=&trglib..cdm_contact_status (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_contact_status , cdm_contact_status );
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
   %err_check (Failed to truncate cdm_content_custom_attr , cdm_content_custom_attr );
   proc append data=&udmmart..cdm_content_custom_attr  base=&trglib..cdm_content_custom_attr (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_content_custom_attr , cdm_content_custom_attr );
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
   %err_check (Failed to truncate cdm_content_detail , cdm_content_detail );
   proc append data=&udmmart..cdm_content_detail  base=&trglib..cdm_content_detail (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_content_detail , cdm_content_detail );
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
   %err_check (Failed to truncate cdm_dyn_content_custom_attr , cdm_dyn_content_custom_attr );
   proc append data=&udmmart..cdm_dyn_content_custom_attr  base=&trglib..cdm_dyn_content_custom_attr (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_dyn_content_custom_attr , cdm_dyn_content_custom_attr );
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
   %err_check (Failed to truncate cdm_identifier_type , cdm_identifier_type );
   proc append data=&udmmart..cdm_identifier_type  base=&trglib..cdm_identifier_type (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_identifier_type , cdm_identifier_type );
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
   %err_check (Failed to truncate cdm_identity_attr , cdm_identity_attr );
   proc append data=&udmmart..cdm_identity_attr  base=&trglib..cdm_identity_attr (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_identity_attr , cdm_identity_attr );
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
   %err_check (Failed to truncate cdm_identity_map , cdm_identity_map );
   proc append data=&udmmart..cdm_identity_map  base=&trglib..cdm_identity_map (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_identity_map , cdm_identity_map );
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
   %err_check (Failed to truncate cdm_identity_type , cdm_identity_type );
   proc append data=&udmmart..cdm_identity_type  base=&trglib..cdm_identity_type (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_identity_type , cdm_identity_type );
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
   %err_check (Failed to truncate cdm_occurrence_detail , cdm_occurrence_detail );
   proc append data=&udmmart..cdm_occurrence_detail  base=&trglib..cdm_occurrence_detail (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_occurrence_detail , cdm_occurrence_detail );
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
   %err_check (Failed to truncate cdm_response_channel , cdm_response_channel );
   proc append data=&udmmart..cdm_response_channel  base=&trglib..cdm_response_channel (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_response_channel , cdm_response_channel );
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
            updated_dttm = d.updated_dttm, 
            updated_by_nm = d.updated_by_nm, attribute_val = d.attribute_val, 
            attribute_data_type_cd = d.attribute_data_type_cd
         when not matched and ATTRIBUTE_NM is NOT NULL and RESPONSE_ATTRIBUTE_TYPE_CD is NOT NULL and RESPONSE_ID is NOT NULL then insert (
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
            conversion_flg = d.conversion_flg, 
            inferred_response_flg = d.inferred_response_flg, response_dt = d.response_dt, 
            response_val_amt = d.response_val_amt, properties_map_doc = d.properties_map_doc, 
            updated_dttm = d.updated_dttm, response_dttm = d.response_dttm, 
            response_dttm_tz = d.response_dttm_tz, updated_by_nm = d.updated_by_nm, 
            source_system_cd = d.source_system_cd, rtc_id = d.rtc_id, 
            response_type_cd = d.response_type_cd, response_channel_cd = d.response_channel_cd, 
            response_cd = d.response_cd, identity_id = d.identity_id, 
            external_contact_info_2_id = d.external_contact_info_2_id, external_contact_info_1_id = d.external_contact_info_1_id, 
            context_val = d.context_val, context_type_nm = d.context_type_nm, 
            content_version_id = d.content_version_id, content_id = d.content_id, 
            content_hash_val = d.content_hash_val, contact_id = d.contact_id, 
            audience_occur_id = d.audience_occur_id, audience_id = d.audience_id
         when not matched and RESPONSE_ID is NOT NULL then insert (
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
   %err_check (Failed to truncate cdm_response_lookup , cdm_response_lookup );
   proc append data=&udmmart..cdm_response_lookup  base=&trglib..cdm_response_lookup (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_response_lookup , cdm_response_lookup );
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
   %err_check (Failed to truncate cdm_response_type , cdm_response_type );
   proc append data=&udmmart..cdm_response_type  base=&trglib..cdm_response_type (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_response_type , cdm_response_type );
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
   %err_check (Failed to truncate cdm_rtc_detail , cdm_rtc_detail );
   proc append data=&udmmart..cdm_rtc_detail  base=&trglib..cdm_rtc_detail (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_rtc_detail , cdm_rtc_detail );
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
   %err_check (Failed to truncate cdm_rtc_x_content , cdm_rtc_x_content );
   proc append data=&udmmart..cdm_rtc_x_content  base=&trglib..cdm_rtc_x_content (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_rtc_x_content , cdm_rtc_x_content );
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
   %err_check (Failed to truncate cdm_segment_custom_attr , cdm_segment_custom_attr );
   proc append data=&udmmart..cdm_segment_custom_attr  base=&trglib..cdm_segment_custom_attr (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_segment_custom_attr , cdm_segment_custom_attr );
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
   %err_check (Failed to truncate cdm_segment_detail , cdm_segment_detail );
   proc append data=&udmmart..cdm_segment_detail  base=&trglib..cdm_segment_detail (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_segment_detail , cdm_segment_detail );
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
   %err_check (Failed to truncate cdm_segment_map , cdm_segment_map );
   proc append data=&udmmart..cdm_segment_map  base=&trglib..cdm_segment_map (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_segment_map , cdm_segment_map );
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
   %err_check (Failed to truncate cdm_segment_map_custom_attr , cdm_segment_map_custom_attr );
   proc append data=&udmmart..cdm_segment_map_custom_attr  base=&trglib..cdm_segment_map_custom_attr (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_segment_map_custom_attr , cdm_segment_map_custom_attr );
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
   %err_check (Failed to truncate cdm_segment_test , cdm_segment_test );
   proc append data=&udmmart..cdm_segment_test  base=&trglib..cdm_segment_test (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_segment_test , cdm_segment_test );
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
   %err_check (Failed to truncate cdm_segment_test_x_segment , cdm_segment_test_x_segment );
   proc append data=&udmmart..cdm_segment_test_x_segment  base=&trglib..cdm_segment_test_x_segment (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_segment_test_x_segment , cdm_segment_test_x_segment );
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
   %err_check (Failed to truncate cdm_task_custom_attr , cdm_task_custom_attr );
   proc append data=&udmmart..cdm_task_custom_attr  base=&trglib..cdm_task_custom_attr (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_task_custom_attr , cdm_task_custom_attr );
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
   %err_check (Failed to truncate cdm_task_detail , cdm_task_detail );
   proc append data=&udmmart..cdm_task_detail  base=&trglib..cdm_task_detail (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to cdm_task_detail , cdm_task_detail );
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
   %err_check (Failed to truncate commitment_details , commitment_details );
   proc append data=&udmmart..commitment_details  base=&trglib..commitment_details (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to commitment_details , commitment_details );
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
   %err_check (Failed to truncate commitment_line_items , commitment_line_items );
   proc append data=&udmmart..commitment_line_items  base=&trglib..commitment_line_items (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to commitment_line_items , commitment_line_items );
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
   %err_check (Failed to truncate commitment_line_items_ccbdgt , commitment_line_items_ccbdgt );
   proc append data=&udmmart..commitment_line_items_ccbdgt  base=&trglib..commitment_line_items_ccbdgt (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to commitment_line_items_ccbdgt , commitment_line_items_ccbdgt );
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
            control_group_flg = d.control_group_flg, 
            properties_map_doc = d.properties_map_doc, contact_dttm_tz = d.contact_dttm_tz, 
            load_dttm = d.load_dttm, contact_dttm = d.contact_dttm, 
            task_id = d.task_id, parent_event_designed_id = d.parent_event_designed_id, 
            journey_occurrence_id = d.journey_occurrence_id, detail_id_hex = d.detail_id_hex, 
            context_type_nm = d.context_type_nm, audience_id = d.audience_id, 
            identity_id = d.identity_id, message_id = d.message_id, 
            response_tracking_cd = d.response_tracking_cd, visit_id_hex = d.visit_id_hex, 
            aud_occurrence_id = d.aud_occurrence_id, contact_channel_nm = d.contact_channel_nm, 
            contact_nm = d.contact_nm, context_val = d.context_val, 
            creative_id = d.creative_id, event_designed_id = d.event_designed_id, 
            journey_id = d.journey_id, occurrence_id = d.occurrence_id, 
            session_id_hex = d.session_id_hex, task_version_id = d.task_version_id
         when not matched and CONTACT_ID is NOT NULL then insert (
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
            test_flg = d.test_flg, 
            control_group_flg = d.control_group_flg, total_cost_amt = d.total_cost_amt, 
            properties_map_doc = d.properties_map_doc, load_dttm = d.load_dttm, 
            conversion_milestone_dttm = d.conversion_milestone_dttm, conversion_milestone_dttm_tz = d.conversion_milestone_dttm_tz, 
            visit_id_hex = d.visit_id_hex, task_id = d.task_id, 
            spot_id = d.spot_id, segment_version_id = d.segment_version_id, 
            reserved_1_txt = d.reserved_1_txt, occurrence_id = d.occurrence_id, 
            message_version_id = d.message_version_id, goal_id = d.goal_id, 
            detail_id_hex = d.detail_id_hex, channel_user_id = d.channel_user_id, 
            analysis_group_id = d.analysis_group_id, audience_id = d.audience_id, 
            context_val = d.context_val, creative_id = d.creative_id, 
            journey_id = d.journey_id, response_tracking_cd = d.response_tracking_cd, 
            activity_id = d.activity_id, aud_occurrence_id = d.aud_occurrence_id, 
            channel_nm = d.channel_nm, context_type_nm = d.context_type_nm, 
            creative_version_id = d.creative_version_id, event_designed_id = d.event_designed_id, 
            event_nm = d.event_nm, identity_id = d.identity_id, 
            journey_occurrence_id = d.journey_occurrence_id, message_id = d.message_id, 
            mobile_app_id = d.mobile_app_id, parent_event_designed_id = d.parent_event_designed_id, 
            rec_group_id = d.rec_group_id, reserved_2_txt = d.reserved_2_txt, 
            segment_id = d.segment_id, session_id_hex = d.session_id_hex, 
            subject_line_txt = d.subject_line_txt, task_version_id = d.task_version_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            custom_revenue_amt = d.custom_revenue_amt, 
            properties_map_doc = d.properties_map_doc, custom_event_dttm = d.custom_event_dttm, 
            custom_event_dttm_tz = d.custom_event_dttm_tz, load_dttm = d.load_dttm, 
            session_id = d.session_id, page_id = d.page_id, 
            event_type_nm = d.event_type_nm, channel_user_id = d.channel_user_id, 
            custom_event_nm = d.custom_event_nm, detail_id_hex = d.detail_id_hex, 
            event_nm = d.event_nm, reserved_1_txt = d.reserved_1_txt, 
            reserved_2_txt = d.reserved_2_txt, visit_id = d.visit_id, 
            channel_nm = d.channel_nm, custom_event_group_nm = d.custom_event_group_nm, 
            custom_events_sk = d.custom_events_sk, detail_id = d.detail_id, 
            event_designed_id = d.event_designed_id, event_key_cd = d.event_key_cd, 
            event_source_cd = d.event_source_cd, identity_id = d.identity_id, 
            mobile_app_id = d.mobile_app_id, session_id_hex = d.session_id_hex, 
            visit_id_hex = d.visit_id_hex
         when not matched and EVENT_ID is NOT NULL then insert (
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
            custom_revenue_amt = d.custom_revenue_amt, 
            load_dttm = d.load_dttm, event_designed_id = d.event_designed_id
         when not matched and CUSTOM_EVENTS_SK is NOT NULL then insert (
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
            bc_subjcnt_str = d.bc_subjcnt_str, 
            customer_profiles_processed_str = d.customer_profiles_processed_str, api_usage_str = d.api_usage_str, 
            mob_impr_cnt = d.mob_impr_cnt, dm_destinations_total_row_cnt = d.dm_destinations_total_row_cnt, 
            google_ads_cnt = d.google_ads_cnt, mob_sesn_cnt = d.mob_sesn_cnt, 
            audience_usage_cnt = d.audience_usage_cnt, mobile_in_app_msg_cnt = d.mobile_in_app_msg_cnt, 
            mobile_push_cnt = d.mobile_push_cnt, email_preview_cnt = d.email_preview_cnt, 
            facebook_ads_cnt = d.facebook_ads_cnt, web_sesn_cnt = d.web_sesn_cnt, 
            plan_users_cnt = d.plan_users_cnt, outbound_api_cnt = d.outbound_api_cnt, 
            web_impr_cnt = d.web_impr_cnt, email_send_cnt = d.email_send_cnt, 
            linkedin_ads_cnt = d.linkedin_ads_cnt, dm_destinations_total_id_cnt = d.dm_destinations_total_id_cnt, 
            asset_size = d.asset_size, db_size = d.db_size, 
            admin_user_cnt = d.admin_user_cnt
         when not matched and EVENT_DAY is NOT NULL then insert (
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
            total_cost_amt = d.total_cost_amt, 
            properties_map_doc = d.properties_map_doc, data_view_dttm = d.data_view_dttm, 
            data_view_dttm_tz = d.data_view_dttm_tz, load_dttm = d.load_dttm, 
            visit_id = d.visit_id, reserved_2_txt = d.reserved_2_txt, 
            event_designed_id = d.event_designed_id, channel_user_id = d.channel_user_id, 
            detail_id = d.detail_id, event_nm = d.event_nm, 
            session_id_hex = d.session_id_hex, detail_id_hex = d.detail_id_hex, 
            identity_id = d.identity_id, parent_event_designed_id = d.parent_event_designed_id, 
            reserved_1_txt = d.reserved_1_txt, session_id = d.session_id, 
            visit_id_hex = d.visit_id_hex
         when not matched and EVENT_ID is NOT NULL then insert (
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
            ge_longitude = d.ge_longitude, 
            ge_latitude = d.ge_latitude, rv_revenue = d.rv_revenue, 
            co_conversions = d.co_conversions, new_visitors = d.new_visitors, 
            return_visitors = d.return_visitors, bouncers = d.bouncers, 
            visits = d.visits, page_views = d.page_views, 
            average_visit_duration = d.average_visit_duration, session_complete_load_dttm = d.session_complete_load_dttm, 
            visit_dttm = d.visit_dttm, visit_dttm_tz = d.visit_dttm_tz, 
            session_start_dttm_tz = d.session_start_dttm_tz, session_start_dttm = d.session_start_dttm, 
            se_external_search_engine = d.se_external_search_engine, landing_page = d.landing_page, 
            ge_country = d.ge_country, cu_customer_id = d.cu_customer_id, 
            br_browser_version = d.br_browser_version, device_type = d.device_type, 
            landing_page_url_domain = d.landing_page_url_domain, se_external_search_engine_phrase = d.se_external_search_engine_phrase, 
            bouncer = d.bouncer, br_browser_name = d.br_browser_name, 
            device_name = d.device_name, ge_city = d.ge_city, 
            ge_state_region = d.ge_state_region, landing_page_url = d.landing_page_url, 
            pl_device_operating_system = d.pl_device_operating_system, se_external_search_engine_domain = d.se_external_search_engine_domain, 
            visitor_type = d.visitor_type, visitor_id = d.visitor_id, 
            visit_origination_type = d.visit_origination_type, visit_origination_tracking_code = d.visit_origination_tracking_code, 
            visit_origination_placement = d.visit_origination_placement, visit_origination_name = d.visit_origination_name, 
            visit_origination_creative = d.visit_origination_creative
         when not matched and SESSION_ID is NOT NULL and VISIT_ID is NOT NULL then insert (
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
            processes = d.processes, 
            steps_completed = d.steps_completed, step_count = d.step_count, 
            processes_completed = d.processes_completed, steps_abandoned = d.steps_abandoned, 
            last_step = d.last_step, processes_abandoned = d.processes_abandoned, 
            steps = d.steps, bus_process_started_dttm_tz = d.bus_process_started_dttm_tz, 
            session_start_dttm_tz = d.session_start_dttm_tz, session_start_dttm = d.session_start_dttm, 
            session_complete_load_dttm = d.session_complete_load_dttm, visitor_id = d.visitor_id, 
            visit_origination_tracking_code = d.visit_origination_tracking_code, visit_origination_name = d.visit_origination_name, 
            visit_id = d.visit_id, device_name = d.device_name, 
            cu_customer_id = d.cu_customer_id, business_process_attribute_2 = d.business_process_attribute_2, 
            bouncer = d.bouncer, business_process_attribute_1 = d.business_process_attribute_1, 
            device_type = d.device_type, visit_origination_creative = d.visit_origination_creative, 
            visit_origination_placement = d.visit_origination_placement, visit_origination_type = d.visit_origination_type, 
            visitor_type = d.visitor_type
         when not matched and BUSINESS_PROCESS_NAME is NOT NULL and BUSINESS_PROCESS_STEP_NAME is NOT NULL and BUS_PROCESS_STARTED_DTTM is NOT NULL and SESSION_ID is NOT NULL then insert (
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
            total_page_view_time = d.total_page_view_time, 
            entry_pages = d.entry_pages, active_page_view_time = d.active_page_view_time, 
            views = d.views, exit_pages = d.exit_pages, 
            visits = d.visits, bouncers = d.bouncers, 
            session_start_dttm = d.session_start_dttm, session_complete_load_dttm = d.session_complete_load_dttm, 
            session_start_dttm_tz = d.session_start_dttm_tz, detail_dttm_tz = d.detail_dttm_tz, 
            detail_dttm = d.detail_dttm, visitor_type = d.visitor_type, 
            visitor_id = d.visitor_id, visit_origination_type = d.visit_origination_type, 
            visit_origination_tracking_code = d.visit_origination_tracking_code, visit_origination_placement = d.visit_origination_placement, 
            visit_origination_name = d.visit_origination_name, visit_origination_creative = d.visit_origination_creative, 
            visit_id = d.visit_id, session_id = d.session_id, 
            pg_page_url = d.pg_page_url, pg_page = d.pg_page, 
            pg_domain_name = d.pg_domain_name, device_type = d.device_type, 
            device_name = d.device_name, cu_customer_id = d.cu_customer_id, 
            class2_id = d.class2_id, bouncer = d.bouncer, 
            class1_id = d.class1_id
         when not matched and DETAIL_ID is NOT NULL then insert (
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
            document_downloads = d.document_downloads, 
            document_download_dttm_tz = d.document_download_dttm_tz, session_start_dttm_tz = d.session_start_dttm_tz, 
            session_start_dttm = d.session_start_dttm, session_complete_load_dttm = d.session_complete_load_dttm, 
            document_download_dttm = d.document_download_dttm, visitor_type = d.visitor_type, 
            visitor_id = d.visitor_id, visit_origination_type = d.visit_origination_type, 
            visit_origination_tracking_code = d.visit_origination_tracking_code, visit_origination_placement = d.visit_origination_placement, 
            visit_origination_name = d.visit_origination_name, visit_origination_creative = d.visit_origination_creative, 
            visit_id = d.visit_id, session_id = d.session_id, 
            do_page_url = d.do_page_url, do_page_description = d.do_page_description, 
            device_type = d.device_type, device_name = d.device_name, 
            cu_customer_id = d.cu_customer_id, class2_id = d.class2_id, 
            class1_id = d.class1_id, bouncer = d.bouncer
         when not matched and DETAIL_ID is NOT NULL then insert (
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
            product_purchase_revenues = d.product_purchase_revenues, 
            basket_adds_revenue = d.basket_adds_revenue, basket_removes_revenue = d.basket_removes_revenue, 
            product_views = d.product_views, basket_adds = d.basket_adds, 
            basket_adds_units = d.basket_adds_units, product_purchases = d.product_purchases, 
            product_purchase_units = d.product_purchase_units, basket_removes_units = d.basket_removes_units, 
            basket_removes = d.basket_removes, baskets_abandoned = d.baskets_abandoned, 
            baskets_completed = d.baskets_completed, baskets_started = d.baskets_started, 
            session_complete_load_dttm = d.session_complete_load_dttm, session_start_dttm_tz = d.session_start_dttm_tz, 
            product_activity_dttm_tz = d.product_activity_dttm_tz, session_start_dttm = d.session_start_dttm, 
            visitor_type = d.visitor_type, visitor_id = d.visitor_id, 
            visit_origination_type = d.visit_origination_type, visit_origination_tracking_code = d.visit_origination_tracking_code, 
            visit_origination_placement = d.visit_origination_placement, visit_origination_name = d.visit_origination_name, 
            visit_origination_creative = d.visit_origination_creative, session_id = d.session_id, 
            product_group_name = d.product_group_name, device_type = d.device_type, 
            device_name = d.device_name, cu_customer_id = d.cu_customer_id, 
            bouncer = d.bouncer
         when not matched and BASKET_ID is NOT NULL and PRODUCT_ACTIVITY_DTTM is NOT NULL and PRODUCT_ID is NOT NULL and PRODUCT_NAME is NOT NULL and PRODUCT_SKU is NOT NULL and VISIT_ID is NOT NULL then insert (
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
            attempts = d.attempts, 
            forms_completed = d.forms_completed, forms_not_submitted = d.forms_not_submitted, 
            forms_started = d.forms_started, form_attempt_dttm = d.form_attempt_dttm, 
            session_start_dttm = d.session_start_dttm, form_attempt_dttm_tz = d.form_attempt_dttm_tz, 
            session_complete_load_dttm = d.session_complete_load_dttm, session_start_dttm_tz = d.session_start_dttm_tz, 
            visitor_type = d.visitor_type, visitor_id = d.visitor_id, 
            visit_origination_type = d.visit_origination_type, visit_origination_tracking_code = d.visit_origination_tracking_code, 
            visit_origination_placement = d.visit_origination_placement, visit_origination_name = d.visit_origination_name, 
            visit_origination_creative = d.visit_origination_creative, visit_id = d.visit_id, 
            session_id = d.session_id, last_field = d.last_field, 
            form_nm = d.form_nm, device_type = d.device_type, 
            device_name = d.device_name, cu_customer_id = d.cu_customer_id, 
            bouncer = d.bouncer
         when not matched and DETAIL_ID is NOT NULL then insert (
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
            goal_revenue = d.goal_revenue, 
            visits = d.visits, session_start_dttm = d.session_start_dttm, 
            goal_reached_dttm_tz = d.goal_reached_dttm_tz, goal_reached_dttm = d.goal_reached_dttm, 
            session_complete_load_dttm = d.session_complete_load_dttm, session_start_dttm_tz = d.session_start_dttm_tz, 
            goals = d.goals, visitor_type = d.visitor_type, 
            visitor_id = d.visitor_id, visit_origination_type = d.visit_origination_type, 
            visit_origination_tracking_code = d.visit_origination_tracking_code, visit_origination_placement = d.visit_origination_placement, 
            visit_origination_name = d.visit_origination_name, visit_origination_creative = d.visit_origination_creative, 
            visit_id = d.visit_id, session_id = d.session_id, 
            goal_name = d.goal_name, goal_group_name = d.goal_group_name, 
            device_type = d.device_type, device_name = d.device_name, 
            cu_customer_id = d.cu_customer_id, bouncer = d.bouncer
         when not matched and DETAIL_ID is NOT NULL then insert (
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
            time_viewing = d.time_viewing, 
            duration = d.duration, content_viewed = d.content_viewed, 
            counter = d.counter, session_start_dttm_tz = d.session_start_dttm_tz, 
            session_start_dttm = d.session_start_dttm, session_complete_load_dttm = d.session_complete_load_dttm, 
            media_start_dttm = d.media_start_dttm, media_start_dttm_tz = d.media_start_dttm_tz, 
            views_started = d.views_started, views_completed = d.views_completed, 
            views = d.views, media_section_view = d.media_section_view, 
            visitor_type = d.visitor_type, visitor_id = d.visitor_id, 
            visit_origination_type = d.visit_origination_type, visit_origination_tracking_code = d.visit_origination_tracking_code, 
            visit_origination_placement = d.visit_origination_placement, visit_origination_name = d.visit_origination_name, 
            visit_origination_creative = d.visit_origination_creative, session_id = d.session_id, 
            media_uri_txt = d.media_uri_txt, media_name = d.media_name, 
            device_type = d.device_type, device_name = d.device_name, 
            cu_customer_id = d.cu_customer_id, bouncer = d.bouncer
         when not matched and DETAIL_ID is NOT NULL and INTERACTIONS_COUNT is NOT NULL and MAXIMUM_PROGRESS is NOT NULL and MEDIA_COMPLETION_RATE is NOT NULL and MEDIA_SECTION is NOT NULL and VISIT_ID is NOT NULL then insert (
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
            click_throughs = d.click_throughs, 
            displays = d.displays, session_start_dttm_tz = d.session_start_dttm_tz, 
            promotion_shown_dttm_tz = d.promotion_shown_dttm_tz, promotion_shown_dttm = d.promotion_shown_dttm, 
            session_complete_load_dttm = d.session_complete_load_dttm, session_start_dttm = d.session_start_dttm, 
            visitor_type = d.visitor_type, visitor_id = d.visitor_id, 
            visit_origination_type = d.visit_origination_type, visit_origination_tracking_code = d.visit_origination_tracking_code, 
            visit_origination_placement = d.visit_origination_placement, visit_origination_name = d.visit_origination_name, 
            visit_origination_creative = d.visit_origination_creative, visit_id = d.visit_id, 
            session_id = d.session_id, promotion_type = d.promotion_type, 
            promotion_tracking_code = d.promotion_tracking_code, promotion_placement = d.promotion_placement, 
            promotion_name = d.promotion_name, promotion_creative = d.promotion_creative, 
            device_type = d.device_type, device_name = d.device_name, 
            cu_customer_id = d.cu_customer_id, bouncer = d.bouncer
         when not matched and DETAIL_ID is NOT NULL then insert (
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
            num_additional_searches = d.num_additional_searches, 
            num_pages_viewed_afterwards = d.num_pages_viewed_afterwards, searches = d.searches, 
            visits = d.visits, search_unknown_results = d.search_unknown_results, 
            search_returned_results = d.search_returned_results, exit_pages = d.exit_pages, 
            search_no_results_returned = d.search_no_results_returned, search_results_dttm_tz = d.search_results_dttm_tz, 
            session_start_dttm = d.session_start_dttm, session_start_dttm_tz = d.session_start_dttm_tz, 
            session_complete_load_dttm = d.session_complete_load_dttm, search_results_dttm = d.search_results_dttm, 
            visitor_type = d.visitor_type, visitor_id = d.visitor_id, 
            visit_origination_type = d.visit_origination_type, visit_origination_tracking_code = d.visit_origination_tracking_code, 
            visit_origination_placement = d.visit_origination_placement, visit_origination_name = d.visit_origination_name, 
            visit_origination_creative = d.visit_origination_creative, visit_id = d.visit_id, 
            session_id = d.session_id, search_name = d.search_name, 
            internal_search_term = d.internal_search_term, device_type = d.device_type, 
            device_name = d.device_name, cu_customer_id = d.cu_customer_id, 
            bouncer = d.bouncer
         when not matched and DETAIL_ID is NOT NULL then insert (
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
            control_active_flg = d.control_active_flg, 
            control_group_flg = d.control_group_flg, properties_map_doc = d.properties_map_doc, 
            load_dttm = d.load_dttm, direct_contact_dttm = d.direct_contact_dttm, 
            direct_contact_dttm_tz = d.direct_contact_dttm_tz, task_version_id = d.task_version_id, 
            task_id = d.task_id, segment_id = d.segment_id, 
            response_tracking_cd = d.response_tracking_cd, occurrence_id = d.occurrence_id, 
            message_id = d.message_id, identity_type_nm = d.identity_type_nm, 
            identity_id = d.identity_id, event_nm = d.event_nm, 
            event_designed_id = d.event_designed_id, context_val = d.context_val, 
            context_type_nm = d.context_type_nm, channel_user_id = d.channel_user_id, 
            channel_nm = d.channel_nm
         when not matched and EVENT_ID is NOT NULL then insert (
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
            load_dttm = d.load_dttm, 
            link_event_dttm = d.link_event_dttm, link_event_dttm_tz = d.link_event_dttm_tz, 
            visit_id_hex = d.visit_id_hex, uri_txt = d.uri_txt, 
            session_id = d.session_id, link_selector_path = d.link_selector_path, 
            link_id = d.link_id, link_name = d.link_name, 
            identity_id = d.identity_id, event_source_cd = d.event_source_cd, 
            session_id_hex = d.session_id_hex, event_key_cd = d.event_key_cd, 
            visit_id = d.visit_id, detail_id_hex = d.detail_id_hex, 
            detail_id = d.detail_id, alt_txt = d.alt_txt
         when not matched and EVENT_ID is NOT NULL then insert (
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
            test_flg = d.test_flg, 
            properties_map_doc = d.properties_map_doc, load_dttm = d.load_dttm, 
            email_bounce_dttm_tz = d.email_bounce_dttm_tz, email_bounce_dttm = d.email_bounce_dttm, 
            task_id = d.task_id, subject_line_txt = d.subject_line_txt, 
            segment_version_id = d.segment_version_id, response_tracking_cd = d.response_tracking_cd, 
            reason_txt = d.reason_txt, raw_reason_txt = d.raw_reason_txt, 
            occurrence_id = d.occurrence_id, journey_occurrence_id = d.journey_occurrence_id, 
            imprint_id = d.imprint_id, event_nm = d.event_nm, 
            event_designed_id = d.event_designed_id, context_type_nm = d.context_type_nm, 
            bounce_class_cd = d.bounce_class_cd, aud_occurrence_id = d.aud_occurrence_id, 
            analysis_group_id = d.analysis_group_id, audience_id = d.audience_id, 
            channel_user_id = d.channel_user_id, context_val = d.context_val, 
            identity_id = d.identity_id, journey_id = d.journey_id, 
            program_id = d.program_id, recipient_domain_nm = d.recipient_domain_nm, 
            segment_id = d.segment_id, task_version_id = d.task_version_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            test_flg = d.test_flg, 
            open_tracking_flg = d.open_tracking_flg, is_mobile_flg = d.is_mobile_flg, 
            click_tracking_flg = d.click_tracking_flg, properties_map_doc = d.properties_map_doc, 
            email_click_dttm = d.email_click_dttm, email_click_dttm_tz = d.email_click_dttm_tz, 
            load_dttm = d.load_dttm, uri_txt = d.uri_txt, 
            task_version_id = d.task_version_id, task_id = d.task_id, 
            subject_line_txt = d.subject_line_txt, segment_id = d.segment_id, 
            recipient_domain_nm = d.recipient_domain_nm, program_id = d.program_id, 
            platform_version = d.platform_version, platform_desc = d.platform_desc, 
            occurrence_id = d.occurrence_id, manufacturer_nm = d.manufacturer_nm, 
            mailbox_provider_nm = d.mailbox_provider_nm, link_tracking_label_txt = d.link_tracking_label_txt, 
            link_tracking_id = d.link_tracking_id, link_tracking_group_txt = d.link_tracking_group_txt, 
            journey_id = d.journey_id, imprint_id = d.imprint_id, 
            event_nm = d.event_nm, event_designed_id = d.event_designed_id, 
            context_val = d.context_val, audience_id = d.audience_id, 
            analysis_group_id = d.analysis_group_id, agent_family_nm = d.agent_family_nm, 
            aud_occurrence_id = d.aud_occurrence_id, channel_user_id = d.channel_user_id, 
            context_type_nm = d.context_type_nm, device_nm = d.device_nm, 
            identity_id = d.identity_id, journey_occurrence_id = d.journey_occurrence_id, 
            response_tracking_cd = d.response_tracking_cd, segment_version_id = d.segment_version_id, 
            user_agent_nm = d.user_agent_nm
         when not matched and EVENT_ID is NOT NULL then insert (
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
            test_flg = d.test_flg, 
            properties_map_doc = d.properties_map_doc, load_dttm = d.load_dttm, 
            email_complaint_dttm = d.email_complaint_dttm, email_complaint_dttm_tz = d.email_complaint_dttm_tz, 
            task_id = d.task_id, segment_version_id = d.segment_version_id, 
            response_tracking_cd = d.response_tracking_cd, recipient_domain_nm = d.recipient_domain_nm, 
            occurrence_id = d.occurrence_id, journey_occurrence_id = d.journey_occurrence_id, 
            imprint_id = d.imprint_id, event_nm = d.event_nm, 
            event_designed_id = d.event_designed_id, context_type_nm = d.context_type_nm, 
            audience_id = d.audience_id, analysis_group_id = d.analysis_group_id, 
            aud_occurrence_id = d.aud_occurrence_id, channel_user_id = d.channel_user_id, 
            context_val = d.context_val, identity_id = d.identity_id, 
            journey_id = d.journey_id, program_id = d.program_id, 
            segment_id = d.segment_id, subject_line_txt = d.subject_line_txt, 
            task_version_id = d.task_version_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            prefetched_flg = d.prefetched_flg, 
            click_tracking_flg = d.click_tracking_flg, open_tracking_flg = d.open_tracking_flg, 
            is_mobile_flg = d.is_mobile_flg, test_flg = d.test_flg, 
            properties_map_doc = d.properties_map_doc, email_open_dttm = d.email_open_dttm, 
            email_open_dttm_tz = d.email_open_dttm_tz, load_dttm = d.load_dttm, 
            user_agent_nm = d.user_agent_nm, task_version_id = d.task_version_id, 
            subject_line_txt = d.subject_line_txt, segment_version_id = d.segment_version_id, 
            segment_id = d.segment_id, recipient_domain_nm = d.recipient_domain_nm, 
            program_id = d.program_id, platform_version = d.platform_version, 
            occurrence_id = d.occurrence_id, manufacturer_nm = d.manufacturer_nm, 
            journey_id = d.journey_id, imprint_id = d.imprint_id, 
            event_nm = d.event_nm, event_designed_id = d.event_designed_id, 
            context_val = d.context_val, audience_id = d.audience_id, 
            analysis_group_id = d.analysis_group_id, agent_family_nm = d.agent_family_nm, 
            aud_occurrence_id = d.aud_occurrence_id, channel_user_id = d.channel_user_id, 
            context_type_nm = d.context_type_nm, device_nm = d.device_nm, 
            identity_id = d.identity_id, journey_occurrence_id = d.journey_occurrence_id, 
            mailbox_provider_nm = d.mailbox_provider_nm, platform_desc = d.platform_desc, 
            response_tracking_cd = d.response_tracking_cd, task_id = d.task_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            test_flg = d.test_flg, 
            properties_map_doc = d.properties_map_doc, email_optout_dttm_tz = d.email_optout_dttm_tz, 
            email_optout_dttm = d.email_optout_dttm, load_dttm = d.load_dttm, 
            task_version_id = d.task_version_id, subject_line_txt = d.subject_line_txt, 
            segment_id = d.segment_id, recipient_domain_nm = d.recipient_domain_nm, 
            program_id = d.program_id, optout_type_nm = d.optout_type_nm, 
            occurrence_id = d.occurrence_id, link_tracking_label_txt = d.link_tracking_label_txt, 
            link_tracking_group_txt = d.link_tracking_group_txt, journey_id = d.journey_id, 
            identity_id = d.identity_id, event_nm = d.event_nm, 
            context_val = d.context_val, channel_user_id = d.channel_user_id, 
            audience_id = d.audience_id, aud_occurrence_id = d.aud_occurrence_id, 
            analysis_group_id = d.analysis_group_id, context_type_nm = d.context_type_nm, 
            event_designed_id = d.event_designed_id, imprint_id = d.imprint_id, 
            journey_occurrence_id = d.journey_occurrence_id, link_tracking_id = d.link_tracking_id, 
            response_tracking_cd = d.response_tracking_cd, segment_version_id = d.segment_version_id, 
            task_id = d.task_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            test_flg = d.test_flg, 
            properties_map_doc = d.properties_map_doc, email_action_dttm_tz = d.email_action_dttm_tz, 
            email_action_dttm = d.email_action_dttm, load_dttm = d.load_dttm, 
            task_version_id = d.task_version_id, subject_line_txt = d.subject_line_txt, 
            segment_id = d.segment_id, recipient_domain_nm = d.recipient_domain_nm, 
            program_id = d.program_id, optout_type_nm = d.optout_type_nm, 
            occurrence_id = d.occurrence_id, journey_occurrence_id = d.journey_occurrence_id, 
            imprint_id = d.imprint_id, event_nm = d.event_nm, 
            event_designed_id = d.event_designed_id, email_address = d.email_address, 
            context_val = d.context_val, audience_id = d.audience_id, 
            analysis_group_id = d.analysis_group_id, aud_occurrence_id = d.aud_occurrence_id, 
            context_type_nm = d.context_type_nm, identity_id = d.identity_id, 
            journey_id = d.journey_id, response_tracking_cd = d.response_tracking_cd, 
            segment_version_id = d.segment_version_id, task_id = d.task_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            test_flg = d.test_flg, 
            properties_map_doc = d.properties_map_doc, email_reply_dttm = d.email_reply_dttm, 
            email_reply_dttm_tz = d.email_reply_dttm_tz, load_dttm = d.load_dttm, 
            uri_txt = d.uri_txt, task_id = d.task_id, 
            subject_line_txt = d.subject_line_txt, segment_version_id = d.segment_version_id, 
            response_tracking_cd = d.response_tracking_cd, occurrence_id = d.occurrence_id, 
            journey_occurrence_id = d.journey_occurrence_id, imprint_id = d.imprint_id, 
            event_nm = d.event_nm, event_designed_id = d.event_designed_id, 
            context_type_nm = d.context_type_nm, audience_id = d.audience_id, 
            analysis_group_id = d.analysis_group_id, aud_occurrence_id = d.aud_occurrence_id, 
            channel_user_id = d.channel_user_id, context_val = d.context_val, 
            identity_id = d.identity_id, journey_id = d.journey_id, 
            program_id = d.program_id, recipient_domain_nm = d.recipient_domain_nm, 
            segment_id = d.segment_id, task_version_id = d.task_version_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            test_flg = d.test_flg, 
            properties_map_doc = d.properties_map_doc, load_dttm = d.load_dttm, 
            email_send_dttm_tz = d.email_send_dttm_tz, email_send_dttm = d.email_send_dttm, 
            task_version_id = d.task_version_id, subject_line_txt = d.subject_line_txt, 
            segment_id = d.segment_id, recipient_domain_nm = d.recipient_domain_nm, 
            program_id = d.program_id, journey_id = d.journey_id, 
            imprint_id = d.imprint_id, event_nm = d.event_nm, 
            event_designed_id = d.event_designed_id, context_type_nm = d.context_type_nm, 
            channel_user_id = d.channel_user_id, audience_id = d.audience_id, 
            analysis_group_id = d.analysis_group_id, aud_occurrence_id = d.aud_occurrence_id, 
            context_val = d.context_val, identity_id = d.identity_id, 
            imprint_url_txt = d.imprint_url_txt, journey_occurrence_id = d.journey_occurrence_id, 
            occurrence_id = d.occurrence_id, response_tracking_cd = d.response_tracking_cd, 
            segment_version_id = d.segment_version_id, task_id = d.task_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            test_flg = d.test_flg, 
            properties_map_doc = d.properties_map_doc, load_dttm = d.load_dttm, 
            email_view_dttm = d.email_view_dttm, email_view_dttm_tz = d.email_view_dttm_tz, 
            task_version_id = d.task_version_id, task_id = d.task_id, 
            subject_line_txt = d.subject_line_txt, segment_version_id = d.segment_version_id, 
            segment_id = d.segment_id, response_tracking_cd = d.response_tracking_cd, 
            recipient_domain_nm = d.recipient_domain_nm, program_id = d.program_id, 
            occurrence_id = d.occurrence_id, link_tracking_id = d.link_tracking_id, 
            link_tracking_group_txt = d.link_tracking_group_txt, journey_occurrence_id = d.journey_occurrence_id, 
            imprint_id = d.imprint_id, event_nm = d.event_nm, 
            event_designed_id = d.event_designed_id, context_type_nm = d.context_type_nm, 
            audience_id = d.audience_id, analysis_group_id = d.analysis_group_id, 
            aud_occurrence_id = d.aud_occurrence_id, channel_user_id = d.channel_user_id, 
            context_val = d.context_val, identity_id = d.identity_id, 
            journey_id = d.journey_id, link_tracking_label_txt = d.link_tracking_label_txt
         when not matched and EVENT_ID is NOT NULL then insert (
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
   %err_check (Failed to truncate event_errors , event_errors );
   proc append data=&udmmart..event_errors  base=&trglib..event_errors (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to event_errors , event_errors );
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
            properties_map_doc = d.properties_map_doc, 
            external_event_dttm_tz = d.external_event_dttm_tz, load_dttm = d.load_dttm, 
            external_event_dttm = d.external_event_dttm, response_tracking_cd = d.response_tracking_cd, 
            identity_id = d.identity_id, event_nm = d.event_nm, 
            event_designed_id = d.event_designed_id, context_type_nm = d.context_type_nm, 
            channel_nm = d.channel_nm, channel_user_id = d.channel_user_id, 
            context_val = d.context_val
         when not matched and EVENT_ID is NOT NULL then insert (
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
   %err_check (Failed to truncate fiscal_cc_budget , fiscal_cc_budget );
   proc append data=&udmmart..fiscal_cc_budget  base=&trglib..fiscal_cc_budget (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to fiscal_cc_budget , fiscal_cc_budget );
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
            submit_flg = d.submit_flg, 
            change_index_no = d.change_index_no, attempt_index_cnt = d.attempt_index_cnt, 
            load_dttm = d.load_dttm, form_field_detail_dttm_tz = d.form_field_detail_dttm_tz, 
            form_field_detail_dttm = d.form_field_detail_dttm, visit_id = d.visit_id, 
            form_field_nm = d.form_field_nm, event_source_cd = d.event_source_cd, 
            detail_id = d.detail_id, attempt_status_cd = d.attempt_status_cd, 
            form_field_value = d.form_field_value, form_nm = d.form_nm, 
            session_id_hex = d.session_id_hex, detail_id_hex = d.detail_id_hex, 
            event_key_cd = d.event_key_cd, form_field_id = d.form_field_id, 
            identity_id = d.identity_id, session_id = d.session_id, 
            visit_id_hex = d.visit_id_hex
         when not matched and EVENT_ID is NOT NULL then insert (
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
            processed_dttm = d.processed_dttm, 
            identity_id = d.identity_id
         when not matched and ENTRYTIME is NOT NULL and IDENTIFIER_TYPE_ID is NOT NULL and USER_IDENTIFIER_VAL is NOT NULL then insert (
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
            processed_dttm = d.processed_dttm, 
            entrytime = d.entrytime, target_identity_id = d.target_identity_id
         when not matched and SOURCE_IDENTITY_ID is NOT NULL then insert (
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
            control_group_flg = d.control_group_flg, 
            product_qty_no = d.product_qty_no, properties_map_doc = d.properties_map_doc, 
            impression_delivered_dttm_tz = d.impression_delivered_dttm_tz, impression_delivered_dttm = d.impression_delivered_dttm, 
            load_dttm = d.load_dttm, spot_id = d.spot_id, 
            response_tracking_cd = d.response_tracking_cd, rec_group_id = d.rec_group_id, 
            product_nm = d.product_nm, message_id = d.message_id, 
            event_nm = d.event_nm, detail_id_hex = d.detail_id_hex, 
            context_val = d.context_val, audience_id = d.audience_id, 
            channel_user_id = d.channel_user_id, creative_id = d.creative_id, 
            identity_id = d.identity_id, journey_occurrence_id = d.journey_occurrence_id, 
            message_version_id = d.message_version_id, mobile_app_id = d.mobile_app_id, 
            product_sku_no = d.product_sku_no, reserved_1_txt = d.reserved_1_txt, 
            segment_version_id = d.segment_version_id, task_version_id = d.task_version_id, 
            visit_id_hex = d.visit_id_hex, aud_occurrence_id = d.aud_occurrence_id, 
            channel_nm = d.channel_nm, context_type_nm = d.context_type_nm, 
            creative_version_id = d.creative_version_id, event_designed_id = d.event_designed_id, 
            event_key_cd = d.event_key_cd, event_source_cd = d.event_source_cd, 
            journey_id = d.journey_id, product_id = d.product_id, 
            request_id = d.request_id, reserved_2_txt = d.reserved_2_txt, 
            segment_id = d.segment_id, session_id_hex = d.session_id_hex, 
            task_id = d.task_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            control_group_flg = d.control_group_flg, 
            product_qty_no = d.product_qty_no, properties_map_doc = d.properties_map_doc, 
            impression_viewable_dttm_tz = d.impression_viewable_dttm_tz, load_dttm = d.load_dttm, 
            impression_viewable_dttm = d.impression_viewable_dttm, visit_id_hex = d.visit_id_hex, 
            session_id_hex = d.session_id_hex, reserved_2_txt = d.reserved_2_txt, 
            product_id = d.product_id, message_id = d.message_id, 
            identity_id = d.identity_id, creative_id = d.creative_id, 
            channel_user_id = d.channel_user_id, analysis_group_id = d.analysis_group_id, 
            audience_id = d.audience_id, context_val = d.context_val, 
            detail_id_hex = d.detail_id_hex, event_nm = d.event_nm, 
            event_source_cd = d.event_source_cd, mobile_app_id = d.mobile_app_id, 
            rec_group_id = d.rec_group_id, request_id = d.request_id, 
            segment_id = d.segment_id, task_id = d.task_id, 
            aud_occurrence_id = d.aud_occurrence_id, channel_nm = d.channel_nm, 
            context_type_nm = d.context_type_nm, creative_version_id = d.creative_version_id, 
            event_designed_id = d.event_designed_id, event_key_cd = d.event_key_cd, 
            message_version_id = d.message_version_id, occurrence_id = d.occurrence_id, 
            product_nm = d.product_nm, product_sku_no = d.product_sku_no, 
            reserved_1_txt = d.reserved_1_txt, response_tracking_cd = d.response_tracking_cd, 
            segment_version_id = d.segment_version_id, spot_id = d.spot_id, 
            task_version_id = d.task_version_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
   %err_check (Failed to truncate invoice_details , invoice_details );
   proc append data=&udmmart..invoice_details  base=&trglib..invoice_details (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to invoice_details , invoice_details );
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
   %err_check (Failed to truncate invoice_line_items , invoice_line_items );
   proc append data=&udmmart..invoice_line_items  base=&trglib..invoice_line_items (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to invoice_line_items , invoice_line_items );
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
   %err_check (Failed to truncate invoice_line_items_ccbdgt , invoice_line_items_ccbdgt );
   proc append data=&udmmart..invoice_line_items_ccbdgt  base=&trglib..invoice_line_items_ccbdgt (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to invoice_line_items_ccbdgt , invoice_line_items_ccbdgt );
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
            properties_map_doc = d.properties_map_doc, 
            in_app_failed_dttm = d.in_app_failed_dttm, in_app_failed_dttm_tz = d.in_app_failed_dttm_tz, 
            load_dttm = d.load_dttm, task_version_id = d.task_version_id, 
            segment_id = d.segment_id, message_id = d.message_id, 
            identity_id = d.identity_id, error_message_txt = d.error_message_txt, 
            context_val = d.context_val, channel_user_id = d.channel_user_id, 
            context_type_nm = d.context_type_nm, creative_version_id = d.creative_version_id, 
            mobile_app_id = d.mobile_app_id, reserved_2_txt = d.reserved_2_txt, 
            spot_id = d.spot_id, channel_nm = d.channel_nm, 
            creative_id = d.creative_id, error_cd = d.error_cd, 
            event_designed_id = d.event_designed_id, event_nm = d.event_nm, 
            message_version_id = d.message_version_id, occurrence_id = d.occurrence_id, 
            reserved_1_txt = d.reserved_1_txt, response_tracking_cd = d.response_tracking_cd, 
            segment_version_id = d.segment_version_id, task_id = d.task_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            properties_map_doc = d.properties_map_doc, 
            in_app_action_dttm_tz = d.in_app_action_dttm_tz, load_dttm = d.load_dttm, 
            in_app_action_dttm = d.in_app_action_dttm, segment_version_id = d.segment_version_id, 
            reserved_2_txt = d.reserved_2_txt, mobile_app_id = d.mobile_app_id, 
            context_val = d.context_val, channel_user_id = d.channel_user_id, 
            creative_version_id = d.creative_version_id, identity_id = d.identity_id, 
            message_id = d.message_id, response_tracking_cd = d.response_tracking_cd, 
            task_id = d.task_id, channel_nm = d.channel_nm, 
            context_type_nm = d.context_type_nm, creative_id = d.creative_id, 
            event_designed_id = d.event_designed_id, event_nm = d.event_nm, 
            message_version_id = d.message_version_id, occurrence_id = d.occurrence_id, 
            reserved_1_txt = d.reserved_1_txt, reserved_3_txt = d.reserved_3_txt, 
            segment_id = d.segment_id, spot_id = d.spot_id, 
            task_version_id = d.task_version_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            properties_map_doc = d.properties_map_doc, 
            load_dttm = d.load_dttm, in_app_send_dttm_tz = d.in_app_send_dttm_tz, 
            in_app_send_dttm = d.in_app_send_dttm, task_id = d.task_id, 
            response_tracking_cd = d.response_tracking_cd, occurrence_id = d.occurrence_id, 
            message_id = d.message_id, event_nm = d.event_nm, 
            creative_id = d.creative_id, channel_nm = d.channel_nm, 
            context_type_nm = d.context_type_nm, creative_version_id = d.creative_version_id, 
            event_designed_id = d.event_designed_id, message_version_id = d.message_version_id, 
            reserved_1_txt = d.reserved_1_txt, segment_version_id = d.segment_version_id, 
            channel_user_id = d.channel_user_id, context_val = d.context_val, 
            identity_id = d.identity_id, mobile_app_id = d.mobile_app_id, 
            reserved_2_txt = d.reserved_2_txt, segment_id = d.segment_id, 
            spot_id = d.spot_id, task_version_id = d.task_version_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            eligibility_flg = d.eligibility_flg, 
            in_app_tgt_request_dttm = d.in_app_tgt_request_dttm, load_dttm = d.load_dttm, 
            in_app_tgt_request_dttm_tz = d.in_app_tgt_request_dttm_tz, context_type_nm = d.context_type_nm, 
            channel_nm = d.channel_nm, event_designed_id = d.event_designed_id, 
            identity_id = d.identity_id, mobile_app_id = d.mobile_app_id, 
            channel_user_id = d.channel_user_id, context_val = d.context_val, 
            event_nm = d.event_nm
         when not matched and EVENT_ID is NOT NULL then insert (
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
            entry_dttm = d.entry_dttm, 
            entry_dttm_tz = d.entry_dttm_tz, load_dttm = d.load_dttm, 
            journey_occurrence_id = d.journey_occurrence_id, identity_id = d.identity_id, 
            aud_occurrence_id = d.aud_occurrence_id, audience_id = d.audience_id, 
            context_type_nm = d.context_type_nm, identity_type_val = d.identity_type_val, 
            context_val = d.context_val, event_nm = d.event_nm, 
            identity_type_nm = d.identity_type_nm, journey_id = d.journey_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            exit_dttm = d.exit_dttm, 
            exit_dttm_tz = d.exit_dttm_tz, load_dttm = d.load_dttm, 
            last_node_id = d.last_node_id, identity_type_nm = d.identity_type_nm, 
            context_type_nm = d.context_type_nm, aud_occurrence_id = d.aud_occurrence_id, 
            group_id = d.group_id, journey_id = d.journey_id, 
            reason_cd = d.reason_cd, audience_id = d.audience_id, 
            context_val = d.context_val, event_nm = d.event_nm, 
            identity_id = d.identity_id, identity_type_val = d.identity_type_val, 
            journey_occurrence_id = d.journey_occurrence_id, reason_txt = d.reason_txt
         when not matched and EVENT_ID is NOT NULL then insert (
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
            holdout_dttm_tz = d.holdout_dttm_tz, 
            load_dttm = d.load_dttm, holdout_dttm = d.holdout_dttm, 
            journey_occurrence_id = d.journey_occurrence_id, journey_id = d.journey_id, 
            identity_type_val = d.identity_type_val, identity_type_nm = d.identity_type_nm, 
            identity_id = d.identity_id, event_nm = d.event_nm, 
            context_val = d.context_val, context_type_nm = d.context_type_nm, 
            audience_id = d.audience_id, aud_occurrence_id = d.aud_occurrence_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            node_entry_dttm = d.node_entry_dttm, 
            load_dttm = d.load_dttm, node_entry_dttm_tz = d.node_entry_dttm_tz, 
            node_type_nm = d.node_type_nm, node_id = d.node_id, 
            previous_node_id = d.previous_node_id, journey_occurrence_id = d.journey_occurrence_id, 
            journey_id = d.journey_id, identity_type_val = d.identity_type_val, 
            identity_type_nm = d.identity_type_nm, identity_id = d.identity_id, 
            group_id = d.group_id, event_nm = d.event_nm, 
            context_val = d.context_val, context_type_nm = d.context_type_nm, 
            audience_id = d.audience_id, aud_occurrence_id = d.aud_occurrence_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            unit_qty = d.unit_qty, 
            success_val = d.success_val, success_dttm = d.success_dttm, 
            load_dttm = d.load_dttm, success_dttm_tz = d.success_dttm_tz, 
            parent_event_designed_id = d.parent_event_designed_id, journey_id = d.journey_id, 
            identity_type_nm = d.identity_type_nm, group_id = d.group_id, 
            context_type_nm = d.context_type_nm, audience_id = d.audience_id, 
            aud_occurrence_id = d.aud_occurrence_id, context_val = d.context_val, 
            event_nm = d.event_nm, identity_id = d.identity_id, 
            identity_type_val = d.identity_type_val, journey_occurrence_id = d.journey_occurrence_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            load_dttm = d.load_dttm, 
            suppression_dttm = d.suppression_dttm, suppression_dttm_tz = d.suppression_dttm_tz, 
            reason_txt = d.reason_txt, reason_cd = d.reason_cd, 
            journey_occurrence_id = d.journey_occurrence_id, identity_type_val = d.identity_type_val, 
            identity_type_nm = d.identity_type_nm, identity_id = d.identity_id, 
            context_type_nm = d.context_type_nm, audience_id = d.audience_id, 
            aud_occurrence_id = d.aud_occurrence_id, context_val = d.context_val, 
            event_nm = d.event_nm, journey_id = d.journey_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            success_dttm_tz = d.success_dttm_tz, 
            success_dttm = d.success_dttm, parent_event_designed_id = d.parent_event_designed_id, 
            journey_id = d.journey_id, group_id = d.group_id, 
            event_nm = d.event_nm, context_type_nm = d.context_type_nm, 
            context_val = d.context_val, identity_id = d.identity_id, 
            journey_occurrence_id = d.journey_occurrence_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
   %err_check (Failed to truncate md_activity , md_activity );
   proc append data=&udmmart..md_activity  base=&trglib..md_activity (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_activity , md_activity );
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
   %err_check (Failed to truncate md_activity_abtestpath , md_activity_abtestpath );
   proc append data=&udmmart..md_activity_abtestpath  base=&trglib..md_activity_abtestpath (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_activity_abtestpath , md_activity_abtestpath );
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
   %err_check (Failed to truncate md_activity_abtestpath_all , md_activity_abtestpath_all );
   proc append data=&udmmart..md_activity_abtestpath_all  base=&trglib..md_activity_abtestpath_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_activity_abtestpath_all , md_activity_abtestpath_all );
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
   %err_check (Failed to truncate md_activity_all , md_activity_all );
   proc append data=&udmmart..md_activity_all  base=&trglib..md_activity_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_activity_all , md_activity_all );
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
   %err_check (Failed to truncate md_activity_custom_prop , md_activity_custom_prop );
   proc append data=&udmmart..md_activity_custom_prop  base=&trglib..md_activity_custom_prop (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_activity_custom_prop , md_activity_custom_prop );
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
   %err_check (Failed to truncate md_activity_custom_prop_all , md_activity_custom_prop_all );
   proc append data=&udmmart..md_activity_custom_prop_all  base=&trglib..md_activity_custom_prop_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_activity_custom_prop_all , md_activity_custom_prop_all );
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
   %err_check (Failed to truncate md_activity_node , md_activity_node );
   proc append data=&udmmart..md_activity_node  base=&trglib..md_activity_node (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_activity_node , md_activity_node );
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
   %err_check (Failed to truncate md_activity_node_all , md_activity_node_all );
   proc append data=&udmmart..md_activity_node_all  base=&trglib..md_activity_node_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_activity_node_all , md_activity_node_all );
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
   %err_check (Failed to truncate md_activity_x_activity_node , md_activity_x_activity_node );
   proc append data=&udmmart..md_activity_x_activity_node  base=&trglib..md_activity_x_activity_node (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_activity_x_activity_node , md_activity_x_activity_node );
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
   %err_check (Failed to truncate md_activity_x_activity_node_all , md_activity_x_activity_node_all );
   proc append data=&udmmart..md_activity_x_activity_node_all  base=&trglib..md_activity_x_activity_node_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_activity_x_activity_node_all , md_activity_x_activity_node_all );
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
   %err_check (Failed to truncate md_activity_x_task , md_activity_x_task );
   proc append data=&udmmart..md_activity_x_task  base=&trglib..md_activity_x_task (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_activity_x_task , md_activity_x_task );
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
   %err_check (Failed to truncate md_activity_x_task_all , md_activity_x_task_all );
   proc append data=&udmmart..md_activity_x_task_all  base=&trglib..md_activity_x_task_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_activity_x_task_all , md_activity_x_task_all );
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
   %err_check (Failed to truncate md_asset , md_asset );
   proc append data=&udmmart..md_asset  base=&trglib..md_asset (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_asset , md_asset );
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
   %err_check (Failed to truncate md_asset_all , md_asset_all );
   proc append data=&udmmart..md_asset_all  base=&trglib..md_asset_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_asset_all , md_asset_all );
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
   %err_check (Failed to truncate md_audience , md_audience );
   proc append data=&udmmart..md_audience  base=&trglib..md_audience (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_audience , md_audience );
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
   %err_check (Failed to truncate md_audience_occurrence , md_audience_occurrence );
   proc append data=&udmmart..md_audience_occurrence  base=&trglib..md_audience_occurrence (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_audience_occurrence , md_audience_occurrence );
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
   %err_check (Failed to truncate md_audience_x_segment , md_audience_x_segment );
   proc append data=&udmmart..md_audience_x_segment  base=&trglib..md_audience_x_segment (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_audience_x_segment , md_audience_x_segment );
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
   %err_check (Failed to truncate md_bu , md_bu );
   proc append data=&udmmart..md_bu  base=&trglib..md_bu (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_bu , md_bu );
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
   %err_check (Failed to truncate md_business_context , md_business_context );
   proc append data=&udmmart..md_business_context  base=&trglib..md_business_context (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_business_context , md_business_context );
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
   %err_check (Failed to truncate md_business_context_all , md_business_context_all );
   proc append data=&udmmart..md_business_context_all  base=&trglib..md_business_context_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_business_context_all , md_business_context_all );
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
   %err_check (Failed to truncate md_costcenter , md_costcenter );
   proc append data=&udmmart..md_costcenter  base=&trglib..md_costcenter (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_costcenter , md_costcenter );
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
   %err_check (Failed to truncate md_cost_category , md_cost_category );
   proc append data=&udmmart..md_cost_category  base=&trglib..md_cost_category (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_cost_category , md_cost_category );
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
   %err_check (Failed to truncate md_creative , md_creative );
   proc append data=&udmmart..md_creative  base=&trglib..md_creative (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_creative , md_creative );
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
   %err_check (Failed to truncate md_creative_all , md_creative_all );
   proc append data=&udmmart..md_creative_all  base=&trglib..md_creative_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_creative_all , md_creative_all );
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
   %err_check (Failed to truncate md_creative_custom_prop , md_creative_custom_prop );
   proc append data=&udmmart..md_creative_custom_prop  base=&trglib..md_creative_custom_prop (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_creative_custom_prop , md_creative_custom_prop );
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
   %err_check (Failed to truncate md_creative_custom_prop_all , md_creative_custom_prop_all );
   proc append data=&udmmart..md_creative_custom_prop_all  base=&trglib..md_creative_custom_prop_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_creative_custom_prop_all , md_creative_custom_prop_all );
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
   %err_check (Failed to truncate md_creative_x_asset , md_creative_x_asset );
   proc append data=&udmmart..md_creative_x_asset  base=&trglib..md_creative_x_asset (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_creative_x_asset , md_creative_x_asset );
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
   %err_check (Failed to truncate md_creative_x_asset_all , md_creative_x_asset_all );
   proc append data=&udmmart..md_creative_x_asset_all  base=&trglib..md_creative_x_asset_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_creative_x_asset_all , md_creative_x_asset_all );
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
   %err_check (Failed to truncate md_custattrib_table_values , md_custattrib_table_values );
   proc append data=&udmmart..md_custattrib_table_values  base=&trglib..md_custattrib_table_values (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_custattrib_table_values , md_custattrib_table_values );
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
   %err_check (Failed to truncate md_cust_attrib , md_cust_attrib );
   proc append data=&udmmart..md_cust_attrib  base=&trglib..md_cust_attrib (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_cust_attrib , md_cust_attrib );
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
   %err_check (Failed to truncate md_dataview , md_dataview );
   proc append data=&udmmart..md_dataview  base=&trglib..md_dataview (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_dataview , md_dataview );
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
   %err_check (Failed to truncate md_dataview_all , md_dataview_all );
   proc append data=&udmmart..md_dataview_all  base=&trglib..md_dataview_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_dataview_all , md_dataview_all );
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
   %err_check (Failed to truncate md_dataview_x_event , md_dataview_x_event );
   proc append data=&udmmart..md_dataview_x_event  base=&trglib..md_dataview_x_event (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_dataview_x_event , md_dataview_x_event );
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
   %err_check (Failed to truncate md_dataview_x_event_all , md_dataview_x_event_all );
   proc append data=&udmmart..md_dataview_x_event_all  base=&trglib..md_dataview_x_event_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_dataview_x_event_all , md_dataview_x_event_all );
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
   %err_check (Failed to truncate md_event , md_event );
   proc append data=&udmmart..md_event  base=&trglib..md_event (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_event , md_event );
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
   %err_check (Failed to truncate md_event_all , md_event_all );
   proc append data=&udmmart..md_event_all  base=&trglib..md_event_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_event_all , md_event_all );
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
   %err_check (Failed to truncate md_fiscal_period , md_fiscal_period );
   proc append data=&udmmart..md_fiscal_period  base=&trglib..md_fiscal_period (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_fiscal_period , md_fiscal_period );
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
   %err_check (Failed to truncate md_grid_attr_defn , md_grid_attr_defn );
   proc append data=&udmmart..md_grid_attr_defn  base=&trglib..md_grid_attr_defn (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_grid_attr_defn , md_grid_attr_defn );
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
   %err_check (Failed to truncate md_journey , md_journey );
   proc append data=&udmmart..md_journey  base=&trglib..md_journey (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_journey , md_journey );
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
   %err_check (Failed to truncate md_journey_all , md_journey_all );
   proc append data=&udmmart..md_journey_all  base=&trglib..md_journey_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_journey_all , md_journey_all );
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
   %err_check (Failed to truncate md_journey_node , md_journey_node );
   proc append data=&udmmart..md_journey_node  base=&trglib..md_journey_node (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_journey_node , md_journey_node );
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
   %err_check (Failed to truncate md_journey_node_occurrence , md_journey_node_occurrence );
   proc append data=&udmmart..md_journey_node_occurrence  base=&trglib..md_journey_node_occurrence (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_journey_node_occurrence , md_journey_node_occurrence );
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
   %err_check (Failed to truncate md_journey_node_x_next_node , md_journey_node_x_next_node );
   proc append data=&udmmart..md_journey_node_x_next_node  base=&trglib..md_journey_node_x_next_node (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_journey_node_x_next_node , md_journey_node_x_next_node );
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
   %err_check (Failed to truncate md_journey_node_x_previous_node , md_journey_node_x_previous_node );
   proc append data=&udmmart..md_journey_node_x_previous_node  base=&trglib..md_journey_node_x_previous_node (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_journey_node_x_previous_node , md_journey_node_x_previous_node );
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
   %err_check (Failed to truncate md_journey_node_x_variant , md_journey_node_x_variant );
   proc append data=&udmmart..md_journey_node_x_variant  base=&trglib..md_journey_node_x_variant (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_journey_node_x_variant , md_journey_node_x_variant );
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
   %err_check (Failed to truncate md_journey_occurrence , md_journey_occurrence );
   proc append data=&udmmart..md_journey_occurrence  base=&trglib..md_journey_occurrence (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_journey_occurrence , md_journey_occurrence );
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
   %err_check (Failed to truncate md_journey_x_audience , md_journey_x_audience );
   proc append data=&udmmart..md_journey_x_audience  base=&trglib..md_journey_x_audience (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_journey_x_audience , md_journey_x_audience );
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
   %err_check (Failed to truncate md_journey_x_event , md_journey_x_event );
   proc append data=&udmmart..md_journey_x_event  base=&trglib..md_journey_x_event (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_journey_x_event , md_journey_x_event );
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
   %err_check (Failed to truncate md_journey_x_task , md_journey_x_task );
   proc append data=&udmmart..md_journey_x_task  base=&trglib..md_journey_x_task (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_journey_x_task , md_journey_x_task );
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
   %err_check (Failed to truncate md_message , md_message );
   proc append data=&udmmart..md_message  base=&trglib..md_message (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_message , md_message );
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
   %err_check (Failed to truncate md_message_all , md_message_all );
   proc append data=&udmmart..md_message_all  base=&trglib..md_message_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_message_all , md_message_all );
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
   %err_check (Failed to truncate md_message_custom_prop , md_message_custom_prop );
   proc append data=&udmmart..md_message_custom_prop  base=&trglib..md_message_custom_prop (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_message_custom_prop , md_message_custom_prop );
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
   %err_check (Failed to truncate md_message_custom_prop_all , md_message_custom_prop_all );
   proc append data=&udmmart..md_message_custom_prop_all  base=&trglib..md_message_custom_prop_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_message_custom_prop_all , md_message_custom_prop_all );
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
   %err_check (Failed to truncate md_message_x_creative , md_message_x_creative );
   proc append data=&udmmart..md_message_x_creative  base=&trglib..md_message_x_creative (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_message_x_creative , md_message_x_creative );
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
   %err_check (Failed to truncate md_message_x_creative_all , md_message_x_creative_all );
   proc append data=&udmmart..md_message_x_creative_all  base=&trglib..md_message_x_creative_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_message_x_creative_all , md_message_x_creative_all );
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
   %err_check (Failed to truncate md_object_type , md_object_type );
   proc append data=&udmmart..md_object_type  base=&trglib..md_object_type (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_object_type , md_object_type );
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
   %err_check (Failed to truncate md_occurrence , md_occurrence );
   proc append data=&udmmart..md_occurrence  base=&trglib..md_occurrence (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_occurrence , md_occurrence );
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
   %err_check (Failed to truncate md_picklist , md_picklist );
   proc append data=&udmmart..md_picklist  base=&trglib..md_picklist (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_picklist , md_picklist );
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
   %err_check (Failed to truncate md_purpose , md_purpose );
   proc append data=&udmmart..md_purpose  base=&trglib..md_purpose (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_purpose , md_purpose );
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
   %err_check (Failed to truncate md_rtc , md_rtc );
   proc append data=&udmmart..md_rtc  base=&trglib..md_rtc (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_rtc , md_rtc );
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
   %err_check (Failed to truncate md_segment , md_segment );
   proc append data=&udmmart..md_segment  base=&trglib..md_segment (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_segment , md_segment );
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
   %err_check (Failed to truncate md_segment_all , md_segment_all );
   proc append data=&udmmart..md_segment_all  base=&trglib..md_segment_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_segment_all , md_segment_all );
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
   %err_check (Failed to truncate md_segment_custom_prop , md_segment_custom_prop );
   proc append data=&udmmart..md_segment_custom_prop  base=&trglib..md_segment_custom_prop (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_segment_custom_prop , md_segment_custom_prop );
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
   %err_check (Failed to truncate md_segment_custom_prop_all , md_segment_custom_prop_all );
   proc append data=&udmmart..md_segment_custom_prop_all  base=&trglib..md_segment_custom_prop_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_segment_custom_prop_all , md_segment_custom_prop_all );
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
   %err_check (Failed to truncate md_segment_map , md_segment_map );
   proc append data=&udmmart..md_segment_map  base=&trglib..md_segment_map (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_segment_map , md_segment_map );
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
   %err_check (Failed to truncate md_segment_map_all , md_segment_map_all );
   proc append data=&udmmart..md_segment_map_all  base=&trglib..md_segment_map_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_segment_map_all , md_segment_map_all );
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
   %err_check (Failed to truncate md_segment_map_custom_prop , md_segment_map_custom_prop );
   proc append data=&udmmart..md_segment_map_custom_prop  base=&trglib..md_segment_map_custom_prop (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_segment_map_custom_prop , md_segment_map_custom_prop );
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
   %err_check (Failed to truncate md_segment_map_custom_prop_all , md_segment_map_custom_prop_all );
   proc append data=&udmmart..md_segment_map_custom_prop_all  base=&trglib..md_segment_map_custom_prop_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_segment_map_custom_prop_all , md_segment_map_custom_prop_all );
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
   %err_check (Failed to truncate md_segment_map_x_segment , md_segment_map_x_segment );
   proc append data=&udmmart..md_segment_map_x_segment  base=&trglib..md_segment_map_x_segment (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_segment_map_x_segment , md_segment_map_x_segment );
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
   %err_check (Failed to truncate md_segment_map_x_segment_all , md_segment_map_x_segment_all );
   proc append data=&udmmart..md_segment_map_x_segment_all  base=&trglib..md_segment_map_x_segment_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_segment_map_x_segment_all , md_segment_map_x_segment_all );
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
   %err_check (Failed to truncate md_segment_test , md_segment_test );
   proc append data=&udmmart..md_segment_test  base=&trglib..md_segment_test (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_segment_test , md_segment_test );
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
   %err_check (Failed to truncate md_segment_test_all , md_segment_test_all );
   proc append data=&udmmart..md_segment_test_all  base=&trglib..md_segment_test_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_segment_test_all , md_segment_test_all );
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
   %err_check (Failed to truncate md_segment_test_x_segment , md_segment_test_x_segment );
   proc append data=&udmmart..md_segment_test_x_segment  base=&trglib..md_segment_test_x_segment (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_segment_test_x_segment , md_segment_test_x_segment );
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
   %err_check (Failed to truncate md_segment_test_x_segment_all , md_segment_test_x_segment_all );
   proc append data=&udmmart..md_segment_test_x_segment_all  base=&trglib..md_segment_test_x_segment_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_segment_test_x_segment_all , md_segment_test_x_segment_all );
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
   %err_check (Failed to truncate md_segment_x_event , md_segment_x_event );
   proc append data=&udmmart..md_segment_x_event  base=&trglib..md_segment_x_event (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_segment_x_event , md_segment_x_event );
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
   %err_check (Failed to truncate md_segment_x_event_all , md_segment_x_event_all );
   proc append data=&udmmart..md_segment_x_event_all  base=&trglib..md_segment_x_event_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_segment_x_event_all , md_segment_x_event_all );
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
   %err_check (Failed to truncate md_spot , md_spot );
   proc append data=&udmmart..md_spot  base=&trglib..md_spot (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_spot , md_spot );
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
   %err_check (Failed to truncate md_spot_all , md_spot_all );
   proc append data=&udmmart..md_spot_all  base=&trglib..md_spot_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_spot_all , md_spot_all );
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
   %err_check (Failed to truncate md_target_assist , md_target_assist );
   proc append data=&udmmart..md_target_assist  base=&trglib..md_target_assist (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_target_assist , md_target_assist );
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
   %err_check (Failed to truncate md_task , md_task );
   proc append data=&udmmart..md_task  base=&trglib..md_task (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_task , md_task );
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
   %err_check (Failed to truncate md_task_all , md_task_all );
   proc append data=&udmmart..md_task_all  base=&trglib..md_task_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_task_all , md_task_all );
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
   %err_check (Failed to truncate md_task_custom_prop , md_task_custom_prop );
   proc append data=&udmmart..md_task_custom_prop  base=&trglib..md_task_custom_prop (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_task_custom_prop , md_task_custom_prop );
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
   %err_check (Failed to truncate md_task_custom_prop_all , md_task_custom_prop_all );
   proc append data=&udmmart..md_task_custom_prop_all  base=&trglib..md_task_custom_prop_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_task_custom_prop_all , md_task_custom_prop_all );
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
   %err_check (Failed to truncate md_task_x_audience , md_task_x_audience );
   proc append data=&udmmart..md_task_x_audience  base=&trglib..md_task_x_audience (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_task_x_audience , md_task_x_audience );
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
   %err_check (Failed to truncate md_task_x_creative , md_task_x_creative );
   proc append data=&udmmart..md_task_x_creative  base=&trglib..md_task_x_creative (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_task_x_creative , md_task_x_creative );
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
   %err_check (Failed to truncate md_task_x_creative_all , md_task_x_creative_all );
   proc append data=&udmmart..md_task_x_creative_all  base=&trglib..md_task_x_creative_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_task_x_creative_all , md_task_x_creative_all );
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
   %err_check (Failed to truncate md_task_x_dataview , md_task_x_dataview );
   proc append data=&udmmart..md_task_x_dataview  base=&trglib..md_task_x_dataview (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_task_x_dataview , md_task_x_dataview );
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
   %err_check (Failed to truncate md_task_x_dataview_all , md_task_x_dataview_all );
   proc append data=&udmmart..md_task_x_dataview_all  base=&trglib..md_task_x_dataview_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_task_x_dataview_all , md_task_x_dataview_all );
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
   %err_check (Failed to truncate md_task_x_event , md_task_x_event );
   proc append data=&udmmart..md_task_x_event  base=&trglib..md_task_x_event (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_task_x_event , md_task_x_event );
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
   %err_check (Failed to truncate md_task_x_event_all , md_task_x_event_all );
   proc append data=&udmmart..md_task_x_event_all  base=&trglib..md_task_x_event_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_task_x_event_all , md_task_x_event_all );
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
   %err_check (Failed to truncate md_task_x_message , md_task_x_message );
   proc append data=&udmmart..md_task_x_message  base=&trglib..md_task_x_message (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_task_x_message , md_task_x_message );
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
   %err_check (Failed to truncate md_task_x_message_all , md_task_x_message_all );
   proc append data=&udmmart..md_task_x_message_all  base=&trglib..md_task_x_message_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_task_x_message_all , md_task_x_message_all );
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
   %err_check (Failed to truncate md_task_x_segment , md_task_x_segment );
   proc append data=&udmmart..md_task_x_segment  base=&trglib..md_task_x_segment (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_task_x_segment , md_task_x_segment );
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
   %err_check (Failed to truncate md_task_x_segment_all , md_task_x_segment_all );
   proc append data=&udmmart..md_task_x_segment_all  base=&trglib..md_task_x_segment_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_task_x_segment_all , md_task_x_segment_all );
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
   %err_check (Failed to truncate md_task_x_spot , md_task_x_spot );
   proc append data=&udmmart..md_task_x_spot  base=&trglib..md_task_x_spot (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_task_x_spot , md_task_x_spot );
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
   %err_check (Failed to truncate md_task_x_spot_all , md_task_x_spot_all );
   proc append data=&udmmart..md_task_x_spot_all  base=&trglib..md_task_x_spot_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_task_x_spot_all , md_task_x_spot_all );
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
   %err_check (Failed to truncate md_task_x_variant , md_task_x_variant );
   proc append data=&udmmart..md_task_x_variant  base=&trglib..md_task_x_variant (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_task_x_variant , md_task_x_variant );
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
   %err_check (Failed to truncate md_task_x_variant_all , md_task_x_variant_all );
   proc append data=&udmmart..md_task_x_variant_all  base=&trglib..md_task_x_variant_all (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_task_x_variant_all , md_task_x_variant_all );
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
   %err_check (Failed to truncate md_vendor , md_vendor );
   proc append data=&udmmart..md_vendor  base=&trglib..md_vendor (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_vendor , md_vendor );
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
   %err_check (Failed to truncate md_wf_process_def , md_wf_process_def );
   proc append data=&udmmart..md_wf_process_def  base=&trglib..md_wf_process_def (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_wf_process_def , md_wf_process_def );
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
   %err_check (Failed to truncate md_wf_process_def_attr_grp , md_wf_process_def_attr_grp );
   proc append data=&udmmart..md_wf_process_def_attr_grp  base=&trglib..md_wf_process_def_attr_grp (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_wf_process_def_attr_grp , md_wf_process_def_attr_grp );
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
   %err_check (Failed to truncate md_wf_process_def_categories , md_wf_process_def_categories );
   proc append data=&udmmart..md_wf_process_def_categories  base=&trglib..md_wf_process_def_categories (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_wf_process_def_categories , md_wf_process_def_categories );
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
   %err_check (Failed to truncate md_wf_process_def_tasks , md_wf_process_def_tasks );
   proc append data=&udmmart..md_wf_process_def_tasks  base=&trglib..md_wf_process_def_tasks (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_wf_process_def_tasks , md_wf_process_def_tasks );
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
   %err_check (Failed to truncate md_wf_process_def_task_assg , md_wf_process_def_task_assg );
   proc append data=&udmmart..md_wf_process_def_task_assg  base=&trglib..md_wf_process_def_task_assg (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to md_wf_process_def_task_assg , md_wf_process_def_task_assg );
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
            action_dttm = d.action_dttm, 
            action_dttm_tz = d.action_dttm_tz, load_dttm = d.load_dttm, 
            playhead_position = d.playhead_position, media_nm = d.media_nm, 
            detail_id = d.detail_id, action = d.action, 
            detail_id_hex = d.detail_id_hex, media_uri_txt = d.media_uri_txt
         when not matched and EVENT_ID is NOT NULL then insert (
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
            media_duration_secs = d.media_duration_secs, 
            load_dttm = d.load_dttm, play_start_dttm_tz = d.play_start_dttm_tz, 
            play_start_dttm = d.play_start_dttm, visit_id_hex = d.visit_id_hex, 
            visit_id = d.visit_id, session_id_hex = d.session_id_hex, 
            session_id = d.session_id, media_uri_txt = d.media_uri_txt, 
            media_player_nm = d.media_player_nm, media_nm = d.media_nm, 
            identity_id = d.identity_id, event_key_cd = d.event_key_cd, 
            detail_id_hex = d.detail_id_hex, detail_id = d.detail_id, 
            event_source_cd = d.event_source_cd, media_player_version_txt = d.media_player_version_txt
         when not matched and EVENT_ID is NOT NULL then insert (
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
            media_display_duration_secs = d.media_display_duration_secs, 
            view_duration_secs = d.view_duration_secs, end_tm = d.end_tm, 
            start_tm = d.start_tm, exit_point_secs = d.exit_point_secs, 
            max_play_secs = d.max_play_secs, interaction_cnt = d.interaction_cnt, 
            play_end_dttm = d.play_end_dttm, play_end_dttm_tz = d.play_end_dttm_tz, 
            load_dttm = d.load_dttm, media_uri_txt = d.media_uri_txt, 
            media_nm = d.media_nm, detail_id_hex = d.detail_id_hex, 
            detail_id = d.detail_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            action_dttm_tz = d.action_dttm_tz, 
            action_dttm = d.action_dttm, load_dttm = d.load_dttm, 
            visit_id_hex = d.visit_id_hex, session_id_hex = d.session_id_hex, 
            reserved_1_txt = d.reserved_1_txt, mobile_app_id = d.mobile_app_id, 
            identity_id = d.identity_id, event_nm = d.event_nm, 
            event_designed_id = d.event_designed_id, detail_id_hex = d.detail_id_hex, 
            channel_user_id = d.channel_user_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            action_dttm_tz = d.action_dttm_tz, 
            action_dttm = d.action_dttm, load_dttm = d.load_dttm, 
            visit_id_hex = d.visit_id_hex, spot_id = d.spot_id, 
            session_id_hex = d.session_id_hex, mobile_app_id = d.mobile_app_id, 
            event_nm = d.event_nm, event_designed_id = d.event_designed_id, 
            detail_id_hex = d.detail_id_hex, creative_id = d.creative_id, 
            context_type_nm = d.context_type_nm, channel_user_id = d.channel_user_id, 
            context_val = d.context_val, identity_id = d.identity_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            api_usage_str = d.api_usage_str, 
            bc_subjcnt_str = d.bc_subjcnt_str, customer_profiles_processed_str = d.customer_profiles_processed_str, 
            web_impr_cnt = d.web_impr_cnt, web_sesn_cnt = d.web_sesn_cnt, 
            mob_sesn_cnt = d.mob_sesn_cnt, email_preview_cnt = d.email_preview_cnt, 
            outbound_api_cnt = d.outbound_api_cnt, facebook_ads_cnt = d.facebook_ads_cnt, 
            mobile_push_cnt = d.mobile_push_cnt, google_ads_cnt = d.google_ads_cnt, 
            audience_usage_cnt = d.audience_usage_cnt, plan_users_cnt = d.plan_users_cnt, 
            email_send_cnt = d.email_send_cnt, linkedin_ads_cnt = d.linkedin_ads_cnt, 
            dm_destinations_total_row_cnt = d.dm_destinations_total_row_cnt, mob_impr_cnt = d.mob_impr_cnt, 
            dm_destinations_total_id_cnt = d.dm_destinations_total_id_cnt, mobile_in_app_msg_cnt = d.mobile_in_app_msg_cnt, 
            asset_size = d.asset_size, db_size = d.db_size, 
            admin_user_cnt = d.admin_user_cnt
         when not matched and EVENT_MONTH is NOT NULL then insert (
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
            properties_map_doc = d.properties_map_doc, 
            notification_failed_dttm = d.notification_failed_dttm, notification_failed_dttm_tz = d.notification_failed_dttm_tz, 
            load_dttm = d.load_dttm, task_id = d.task_id, 
            segment_version_id = d.segment_version_id, segment_id = d.segment_id, 
            response_tracking_cd = d.response_tracking_cd, occurrence_id = d.occurrence_id, 
            message_version_id = d.message_version_id, journey_id = d.journey_id, 
            event_designed_id = d.event_designed_id, creative_id = d.creative_id, 
            channel_user_id = d.channel_user_id, channel_nm = d.channel_nm, 
            aud_occurrence_id = d.aud_occurrence_id, context_type_nm = d.context_type_nm, 
            error_cd = d.error_cd, event_nm = d.event_nm, 
            message_id = d.message_id, mobile_app_id = d.mobile_app_id, 
            reserved_1_txt = d.reserved_1_txt, audience_id = d.audience_id, 
            context_val = d.context_val, creative_version_id = d.creative_version_id, 
            error_message_txt = d.error_message_txt, identity_id = d.identity_id, 
            journey_occurrence_id = d.journey_occurrence_id, reserved_2_txt = d.reserved_2_txt, 
            spot_id = d.spot_id, task_version_id = d.task_version_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            properties_map_doc = d.properties_map_doc, 
            load_dttm = d.load_dttm, notification_opened_dttm_tz = d.notification_opened_dttm_tz, 
            notification_opened_dttm = d.notification_opened_dttm, task_version_id = d.task_version_id, 
            segment_version_id = d.segment_version_id, segment_id = d.segment_id, 
            reserved_1_txt = d.reserved_1_txt, message_id = d.message_id, 
            identity_id = d.identity_id, event_nm = d.event_nm, 
            creative_id = d.creative_id, channel_user_id = d.channel_user_id, 
            channel_nm = d.channel_nm, aud_occurrence_id = d.aud_occurrence_id, 
            context_type_nm = d.context_type_nm, event_designed_id = d.event_designed_id, 
            journey_id = d.journey_id, message_version_id = d.message_version_id, 
            occurrence_id = d.occurrence_id, reserved_3_txt = d.reserved_3_txt, 
            spot_id = d.spot_id, audience_id = d.audience_id, 
            context_val = d.context_val, creative_version_id = d.creative_version_id, 
            journey_occurrence_id = d.journey_occurrence_id, mobile_app_id = d.mobile_app_id, 
            reserved_2_txt = d.reserved_2_txt, response_tracking_cd = d.response_tracking_cd, 
            task_id = d.task_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            properties_map_doc = d.properties_map_doc, 
            load_dttm = d.load_dttm, notification_send_dttm_tz = d.notification_send_dttm_tz, 
            notification_send_dttm = d.notification_send_dttm, task_id = d.task_id, 
            spot_id = d.spot_id, reserved_2_txt = d.reserved_2_txt, 
            occurrence_id = d.occurrence_id, message_id = d.message_id, 
            identity_id = d.identity_id, creative_version_id = d.creative_version_id, 
            channel_user_id = d.channel_user_id, audience_id = d.audience_id, 
            context_val = d.context_val, journey_id = d.journey_id, 
            journey_occurrence_id = d.journey_occurrence_id, mobile_app_id = d.mobile_app_id, 
            reserved_1_txt = d.reserved_1_txt, segment_id = d.segment_id, 
            task_version_id = d.task_version_id, aud_occurrence_id = d.aud_occurrence_id, 
            channel_nm = d.channel_nm, context_type_nm = d.context_type_nm, 
            creative_id = d.creative_id, event_designed_id = d.event_designed_id, 
            event_nm = d.event_nm, message_version_id = d.message_version_id, 
            response_tracking_cd = d.response_tracking_cd, segment_version_id = d.segment_version_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            eligibility_flg = d.eligibility_flg, 
            notification_tgt_req_dttm = d.notification_tgt_req_dttm, load_dttm = d.load_dttm, 
            notification_tgt_req_dttm_tz = d.notification_tgt_req_dttm_tz, task_id = d.task_id, 
            mobile_app_id = d.mobile_app_id, event_nm = d.event_nm, 
            context_val = d.context_val, audience_id = d.audience_id, 
            channel_user_id = d.channel_user_id, event_designed_id = d.event_designed_id, 
            journey_id = d.journey_id, aud_occurrence_id = d.aud_occurrence_id, 
            channel_nm = d.channel_nm, context_type_nm = d.context_type_nm, 
            identity_id = d.identity_id, journey_occurrence_id = d.journey_occurrence_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            unit_price_amt = d.unit_price_amt, 
            quantity_amt = d.quantity_amt, properties_map_doc = d.properties_map_doc, 
            load_dttm = d.load_dttm, activity_dttm = d.activity_dttm, 
            activity_dttm_tz = d.activity_dttm_tz, visit_id = d.visit_id, 
            session_id = d.session_id, record_type = d.record_type, 
            product_id = d.product_id, mobile_app_id = d.mobile_app_id, 
            event_nm = d.event_nm, event_key_cd = d.event_key_cd, 
            detail_id = d.detail_id, cart_id = d.cart_id, 
            availability_message_txt = d.availability_message_txt, channel_nm = d.channel_nm, 
            event_designed_id = d.event_designed_id, event_source_cd = d.event_source_cd, 
            order_id = d.order_id, product_nm = d.product_nm, 
            product_sku = d.product_sku, reserved_1_txt = d.reserved_1_txt, 
            session_id_hex = d.session_id_hex, shipping_message_txt = d.shipping_message_txt, 
            cart_nm = d.cart_nm, currency_cd = d.currency_cd, 
            detail_id_hex = d.detail_id_hex, identity_id = d.identity_id, 
            product_group_nm = d.product_group_nm, saving_message_txt = d.saving_message_txt, 
            visit_id_hex = d.visit_id_hex
         when not matched and EVENT_ID is NOT NULL then insert (
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
            total_price_amt = d.total_price_amt, 
            shipping_amt = d.shipping_amt, total_tax_amt = d.total_tax_amt, 
            total_unit_qty = d.total_unit_qty, properties_map_doc = d.properties_map_doc, 
            load_dttm = d.load_dttm, activity_dttm_tz = d.activity_dttm_tz, 
            activity_dttm = d.activity_dttm, visit_id = d.visit_id, 
            shipping_postal_cd = d.shipping_postal_cd, session_id_hex = d.session_id_hex, 
            payment_type_desc = d.payment_type_desc, identity_id = d.identity_id, 
            delivery_type_desc = d.delivery_type_desc, cart_id = d.cart_id, 
            billing_city_nm = d.billing_city_nm, billing_postal_cd = d.billing_postal_cd, 
            channel_nm = d.channel_nm, detail_id_hex = d.detail_id_hex, 
            event_nm = d.event_nm, mobile_app_id = d.mobile_app_id, 
            record_type = d.record_type, shipping_city_nm = d.shipping_city_nm, 
            visit_id_hex = d.visit_id_hex, billing_country_nm = d.billing_country_nm, 
            billing_state_region_cd = d.billing_state_region_cd, cart_nm = d.cart_nm, 
            currency_cd = d.currency_cd, detail_id = d.detail_id, 
            event_designed_id = d.event_designed_id, event_key_cd = d.event_key_cd, 
            event_source_cd = d.event_source_cd, order_id = d.order_id, 
            session_id = d.session_id, shipping_country_nm = d.shipping_country_nm, 
            shipping_state_region_cd = d.shipping_state_region_cd
         when not matched and EVENT_ID is NOT NULL then insert (
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
            properties_map_doc = d.properties_map_doc, 
            outbound_system_dttm_tz = d.outbound_system_dttm_tz, outbound_system_dttm = d.outbound_system_dttm, 
            load_dttm = d.load_dttm, visit_id_hex = d.visit_id_hex, 
            session_id_hex = d.session_id_hex, reserved_2_txt = d.reserved_2_txt, 
            reserved_1_txt = d.reserved_1_txt, parent_event_id = d.parent_event_id, 
            message_version_id = d.message_version_id, journey_id = d.journey_id, 
            event_designed_id = d.event_designed_id, context_val = d.context_val, 
            audience_id = d.audience_id, channel_nm = d.channel_nm, 
            channel_user_id = d.channel_user_id, creative_id = d.creative_id, 
            creative_version_id = d.creative_version_id, event_nm = d.event_nm, 
            message_id = d.message_id, mobile_app_id = d.mobile_app_id, 
            occurrence_id = d.occurrence_id, segment_id = d.segment_id, 
            task_id = d.task_id, aud_occurrence_id = d.aud_occurrence_id, 
            context_type_nm = d.context_type_nm, detail_id_hex = d.detail_id_hex, 
            identity_id = d.identity_id, journey_occurrence_id = d.journey_occurrence_id, 
            response_tracking_cd = d.response_tracking_cd, segment_version_id = d.segment_version_id, 
            spot_id = d.spot_id, task_version_id = d.task_version_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            session_dt_tz = d.session_dt_tz, 
            session_dt = d.session_dt, page_load_sec_cnt = d.page_load_sec_cnt, 
            page_complete_sec_cnt = d.page_complete_sec_cnt, bytes_sent_cnt = d.bytes_sent_cnt, 
            detail_dttm_tz = d.detail_dttm_tz, load_dttm = d.load_dttm, 
            detail_dttm = d.detail_dttm, url_domain = d.url_domain, 
            session_id_hex = d.session_id_hex, session_id = d.session_id, 
            page_url_txt = d.page_url_txt, mobile_app_id = d.mobile_app_id, 
            event_key_cd = d.event_key_cd, detail_id_hex = d.detail_id_hex, 
            detail_id = d.detail_id, class8_id = d.class8_id, 
            class4_id = d.class4_id, class15_id = d.class15_id, 
            class12_id = d.class12_id, class11_id = d.class11_id, 
            channel_nm = d.channel_nm, class13_id = d.class13_id, 
            class2_id = d.class2_id, class6_id = d.class6_id, 
            domain_nm = d.domain_nm, event_source_cd = d.event_source_cd, 
            page_desc = d.page_desc, protocol_nm = d.protocol_nm, 
            visit_id = d.visit_id, visit_id_hex = d.visit_id_hex, 
            class10_id = d.class10_id, class14_id = d.class14_id, 
            class1_id = d.class1_id, class3_id = d.class3_id, 
            class5_id = d.class5_id, class7_id = d.class7_id, 
            class9_id = d.class9_id, event_nm = d.event_nm, 
            identity_id = d.identity_id, referrer_url_txt = d.referrer_url_txt, 
            window_size_txt = d.window_size_txt
         when not matched and EVENT_ID is NOT NULL then insert (
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
            active_sec_spent_on_page_cnt = d.active_sec_spent_on_page_cnt, 
            seconds_spent_on_page_cnt = d.seconds_spent_on_page_cnt, detail_id_hex = d.detail_id_hex, 
            session_id_hex = d.session_id_hex
         when not matched and DETAIL_ID is NOT NULL and LOAD_DTTM is NOT NULL and SESSION_ID is NOT NULL then insert (
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
            in_page_error_dttm = d.in_page_error_dttm, 
            in_page_error_dttm_tz = d.in_page_error_dttm_tz, load_dttm = d.load_dttm, 
            visit_id_hex = d.visit_id_hex, session_id = d.session_id, 
            identity_id = d.identity_id, error_location_txt = d.error_location_txt, 
            detail_id_hex = d.detail_id_hex, in_page_error_txt = d.in_page_error_txt, 
            session_id_hex = d.session_id_hex, detail_id = d.detail_id, 
            event_source_cd = d.event_source_cd, visit_id = d.visit_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
   %err_check (Failed to truncate planning_hierarchy_defn , planning_hierarchy_defn );
   proc append data=&udmmart..planning_hierarchy_defn  base=&trglib..planning_hierarchy_defn (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to planning_hierarchy_defn , planning_hierarchy_defn );
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
   %err_check (Failed to truncate planning_info , planning_info );
   proc append data=&udmmart..planning_info  base=&trglib..planning_info (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to planning_info , planning_info );
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
   %err_check (Failed to truncate planning_info_custom_prop , planning_info_custom_prop );
   proc append data=&udmmart..planning_info_custom_prop  base=&trglib..planning_info_custom_prop (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to planning_info_custom_prop , planning_info_custom_prop );
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
            price_val = d.price_val, 
            properties_map_doc = d.properties_map_doc, action_dttm_tz = d.action_dttm_tz, 
            load_dttm = d.load_dttm, action_dttm = d.action_dttm, 
            visit_id_hex = d.visit_id_hex, visit_id = d.visit_id, 
            saving_message_txt = d.saving_message_txt, product_id = d.product_id, 
            mobile_app_id = d.mobile_app_id, event_nm = d.event_nm, 
            event_key_cd = d.event_key_cd, detail_id = d.detail_id, 
            availability_message_txt = d.availability_message_txt, channel_nm = d.channel_nm, 
            event_designed_id = d.event_designed_id, event_source_cd = d.event_source_cd, 
            product_group_nm = d.product_group_nm, product_sku = d.product_sku, 
            session_id_hex = d.session_id_hex, currency_cd = d.currency_cd, 
            detail_id_hex = d.detail_id_hex, identity_id = d.identity_id, 
            product_nm = d.product_nm, session_id = d.session_id, 
            shipping_message_txt = d.shipping_message_txt
         when not matched and EVENT_ID is NOT NULL then insert (
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
            derived_display_flg = d.derived_display_flg, 
            promotion_number = d.promotion_number, properties_map_doc = d.properties_map_doc, 
            display_dttm_tz = d.display_dttm_tz, load_dttm = d.load_dttm, 
            display_dttm = d.display_dttm, session_id_hex = d.session_id_hex, 
            promotion_tracking_cd = d.promotion_tracking_cd, promotion_nm = d.promotion_nm, 
            promotion_creative_nm = d.promotion_creative_nm, event_source_cd = d.event_source_cd, 
            event_designed_id = d.event_designed_id, detail_id = d.detail_id, 
            channel_nm = d.channel_nm, detail_id_hex = d.detail_id_hex, 
            event_key_cd = d.event_key_cd, mobile_app_id = d.mobile_app_id, 
            promotion_placement_nm = d.promotion_placement_nm, session_id = d.session_id, 
            visit_id_hex = d.visit_id_hex, event_nm = d.event_nm, 
            identity_id = d.identity_id, promotion_type_nm = d.promotion_type_nm, 
            visit_id = d.visit_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            promotion_number = d.promotion_number, 
            properties_map_doc = d.properties_map_doc, click_dttm_tz = d.click_dttm_tz, 
            click_dttm = d.click_dttm, load_dttm = d.load_dttm, 
            session_id_hex = d.session_id_hex, promotion_tracking_cd = d.promotion_tracking_cd, 
            promotion_creative_nm = d.promotion_creative_nm, event_source_cd = d.event_source_cd, 
            event_designed_id = d.event_designed_id, detail_id = d.detail_id, 
            detail_id_hex = d.detail_id_hex, event_key_cd = d.event_key_cd, 
            mobile_app_id = d.mobile_app_id, promotion_nm = d.promotion_nm, 
            promotion_placement_nm = d.promotion_placement_nm, session_id = d.session_id, 
            visit_id_hex = d.visit_id_hex, channel_nm = d.channel_nm, 
            event_nm = d.event_nm, identity_id = d.identity_id, 
            promotion_type_nm = d.promotion_type_nm, visit_id = d.visit_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            properties_map_doc = d.properties_map_doc, 
            load_dttm = d.load_dttm, response_dttm = d.response_dttm, 
            response_dttm_tz = d.response_dttm_tz, session_id_hex = d.session_id_hex, 
            response_channel_nm = d.response_channel_nm, parent_event_designed_id = d.parent_event_designed_id, 
            journey_occurrence_id = d.journey_occurrence_id, detail_id_hex = d.detail_id_hex, 
            audience_id = d.audience_id, context_type_nm = d.context_type_nm, 
            context_val = d.context_val, identity_id = d.identity_id, 
            message_id = d.message_id, response_nm = d.response_nm, 
            task_version_id = d.task_version_id, aud_occurrence_id = d.aud_occurrence_id, 
            creative_id = d.creative_id, event_designed_id = d.event_designed_id, 
            journey_id = d.journey_id, occurrence_id = d.occurrence_id, 
            response_tracking_cd = d.response_tracking_cd, task_id = d.task_id, 
            visit_id_hex = d.visit_id_hex
         when not matched and RESPONSE_ID is NOT NULL then insert (
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
            results_displayed_flg = d.results_displayed_flg, 
            search_results_displayed = d.search_results_displayed, properties_map_doc = d.properties_map_doc, 
            load_dttm = d.load_dttm, search_results_dttm = d.search_results_dttm, 
            search_results_dttm_tz = d.search_results_dttm_tz, visit_id_hex = d.visit_id_hex, 
            srch_field_name = d.srch_field_name, srch_field_id = d.srch_field_id, 
            search_results_sk = d.search_results_sk, search_nm = d.search_nm, 
            identity_id = d.identity_id, event_key_cd = d.event_key_cd, 
            channel_nm = d.channel_nm, detail_id = d.detail_id, 
            detail_id_hex = d.detail_id_hex, event_nm = d.event_nm, 
            mobile_app_id = d.mobile_app_id, session_id = d.session_id, 
            srch_phrase = d.srch_phrase, event_designed_id = d.event_designed_id, 
            event_source_cd = d.event_source_cd, session_id_hex = d.session_id_hex, 
            visit_id = d.visit_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            search_results_displayed = d.search_results_displayed, 
            load_dttm = d.load_dttm, search_results_sk = d.search_results_sk, 
            event_designed_id = d.event_designed_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            java_enabled_flg = d.java_enabled_flg, 
            java_script_enabled_flg = d.java_script_enabled_flg, cookies_enabled_flg = d.cookies_enabled_flg, 
            is_portable_flag = d.is_portable_flag, flash_enabled_flg = d.flash_enabled_flg, 
            session_dt = d.session_dt, session_dt_tz = d.session_dt_tz, 
            longitude = d.longitude, latitude = d.latitude, 
            session_timeout = d.session_timeout, metro_cd = d.metro_cd, 
            screen_color_depth_no = d.screen_color_depth_no, client_session_start_dttm = d.client_session_start_dttm, 
            load_dttm = d.load_dttm, session_start_dttm_tz = d.session_start_dttm_tz, 
            session_start_dttm = d.session_start_dttm, client_session_start_dttm_tz = d.client_session_start_dttm_tz, 
            user_agent_nm = d.user_agent_nm, state_region_cd = d.state_region_cd, 
            region_nm = d.region_nm, profile_nm2 = d.profile_nm2, 
            profile_nm1 = d.profile_nm1, previous_session_id_hex = d.previous_session_id_hex, 
            previous_session_id = d.previous_session_id, postal_cd = d.postal_cd, 
            parent_event_id = d.parent_event_id, network_code = d.network_code, 
            mobile_country_code = d.mobile_country_code, manufacturer = d.manufacturer, 
            java_version_no = d.java_version_no, flash_version_no = d.flash_version_no, 
            device_type_nm = d.device_type_nm, country_nm = d.country_nm, 
            country_cd = d.country_cd, city_nm = d.city_nm, 
            browser_nm = d.browser_nm, app_id = d.app_id, 
            browser_version_no = d.browser_version_no, carrier_name = d.carrier_name, 
            device_language = d.device_language, eventsource_cd = d.eventsource_cd, 
            identity_id = d.identity_id, ip_address = d.ip_address, 
            new_visitor_flg = d.new_visitor_flg, platform_desc = d.platform_desc, 
            platform_type_nm = d.platform_type_nm, profile_nm4 = d.profile_nm4, 
            screen_size_txt = d.screen_size_txt, session_id = d.session_id, 
            visitor_id = d.visitor_id, app_version = d.app_version, 
            channel_nm = d.channel_nm, device_nm = d.device_nm, 
            organization_nm = d.organization_nm, platform_version = d.platform_version, 
            profile_nm3 = d.profile_nm3, profile_nm5 = d.profile_nm5, 
            sdk_version = d.sdk_version, session_id_hex = d.session_id_hex, 
            user_language_cd = d.user_language_cd
         when not matched and EVENT_ID is NOT NULL then insert (
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
            active_sec_spent_in_sessn_cnt = d.active_sec_spent_in_sessn_cnt, 
            seconds_spent_in_session_cnt = d.seconds_spent_in_session_cnt, load_dttm = d.load_dttm, 
            session_expiration_dttm = d.session_expiration_dttm, last_session_activity_dttm_tz = d.last_session_activity_dttm_tz, 
            session_expiration_dttm_tz = d.session_expiration_dttm_tz, session_id_hex = d.session_id_hex
         when not matched and LAST_SESSION_ACTIVITY_DTTM is NOT NULL and SESSION_ID is NOT NULL then insert (
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
            sms_click_dttm_tz = d.sms_click_dttm_tz, 
            sms_click_dttm = d.sms_click_dttm, load_dttm = d.load_dttm, 
            task_id = d.task_id, sms_message_id = d.sms_message_id, 
            sender_id = d.sender_id, journey_occurrence_id = d.journey_occurrence_id, 
            event_nm = d.event_nm, country_cd = d.country_cd, 
            audience_id = d.audience_id, aud_occurrence_id = d.aud_occurrence_id, 
            context_type_nm = d.context_type_nm, creative_version_id = d.creative_version_id, 
            identity_id = d.identity_id, occurrence_id = d.occurrence_id, 
            context_val = d.context_val, creative_id = d.creative_id, 
            event_designed_id = d.event_designed_id, journey_id = d.journey_id, 
            response_tracking_cd = d.response_tracking_cd, task_version_id = d.task_version_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            sms_delivered_dttm_tz = d.sms_delivered_dttm_tz, 
            sms_delivered_dttm = d.sms_delivered_dttm, load_dttm = d.load_dttm, 
            sms_message_id = d.sms_message_id, occurrence_id = d.occurrence_id, 
            journey_id = d.journey_id, identity_id = d.identity_id, 
            creative_version_id = d.creative_version_id, context_type_nm = d.context_type_nm, 
            aud_occurrence_id = d.aud_occurrence_id, country_cd = d.country_cd, 
            journey_occurrence_id = d.journey_occurrence_id, sender_id = d.sender_id, 
            task_id = d.task_id, audience_id = d.audience_id, 
            context_val = d.context_val, creative_id = d.creative_id, 
            event_designed_id = d.event_designed_id, event_nm = d.event_nm, 
            response_tracking_cd = d.response_tracking_cd, task_version_id = d.task_version_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            sms_failed_dttm_tz = d.sms_failed_dttm_tz, 
            load_dttm = d.load_dttm, sms_failed_dttm = d.sms_failed_dttm, 
            task_version_id = d.task_version_id, task_id = d.task_id, 
            sms_message_id = d.sms_message_id, reason_description_txt = d.reason_description_txt, 
            journey_occurrence_id = d.journey_occurrence_id, creative_id = d.creative_id, 
            country_cd = d.country_cd, aud_occurrence_id = d.aud_occurrence_id, 
            context_type_nm = d.context_type_nm, creative_version_id = d.creative_version_id, 
            event_nm = d.event_nm, identity_id = d.identity_id, 
            occurrence_id = d.occurrence_id, response_tracking_cd = d.response_tracking_cd, 
            sender_id = d.sender_id, audience_id = d.audience_id, 
            context_val = d.context_val, event_designed_id = d.event_designed_id, 
            journey_id = d.journey_id, reason_cd = d.reason_cd
         when not matched and EVENT_ID is NOT NULL then insert (
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
            load_dttm = d.load_dttm, 
            sms_reply_dttm_tz = d.sms_reply_dttm_tz, sms_reply_dttm = d.sms_reply_dttm, 
            task_version_id = d.task_version_id, sms_message_id = d.sms_message_id, 
            response_tracking_cd = d.response_tracking_cd, occurrence_id = d.occurrence_id, 
            identity_id = d.identity_id, country_cd = d.country_cd, 
            aud_occurrence_id = d.aud_occurrence_id, context_type_nm = d.context_type_nm, 
            journey_id = d.journey_id, journey_occurrence_id = d.journey_occurrence_id, 
            sender_id = d.sender_id, task_id = d.task_id, 
            audience_id = d.audience_id, context_val = d.context_val, 
            event_designed_id = d.event_designed_id, event_nm = d.event_nm, 
            sms_content = d.sms_content
         when not matched and EVENT_ID is NOT NULL then insert (
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
            fragment_cnt = d.fragment_cnt, 
            sms_send_dttm = d.sms_send_dttm, sms_send_dttm_tz = d.sms_send_dttm_tz, 
            load_dttm = d.load_dttm, occurrence_id = d.occurrence_id, 
            identity_id = d.identity_id, event_designed_id = d.event_designed_id, 
            context_val = d.context_val, aud_occurrence_id = d.aud_occurrence_id, 
            audience_id = d.audience_id, country_cd = d.country_cd, 
            creative_id = d.creative_id, event_nm = d.event_nm, 
            journey_id = d.journey_id, journey_occurrence_id = d.journey_occurrence_id, 
            sender_id = d.sender_id, task_id = d.task_id, 
            context_type_nm = d.context_type_nm, creative_version_id = d.creative_version_id, 
            response_tracking_cd = d.response_tracking_cd, sms_message_id = d.sms_message_id, 
            task_version_id = d.task_version_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            load_dttm = d.load_dttm, 
            sms_optout_dttm = d.sms_optout_dttm, sms_optout_dttm_tz = d.sms_optout_dttm_tz, 
            task_id = d.task_id, sms_message_id = d.sms_message_id, 
            sender_id = d.sender_id, journey_occurrence_id = d.journey_occurrence_id, 
            country_cd = d.country_cd, aud_occurrence_id = d.aud_occurrence_id, 
            context_type_nm = d.context_type_nm, creative_version_id = d.creative_version_id, 
            identity_id = d.identity_id, occurrence_id = d.occurrence_id, 
            audience_id = d.audience_id, context_val = d.context_val, 
            creative_id = d.creative_id, event_designed_id = d.event_designed_id, 
            event_nm = d.event_nm, journey_id = d.journey_id, 
            response_tracking_cd = d.response_tracking_cd, task_version_id = d.task_version_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            load_dttm = d.load_dttm, 
            sms_optout_dttm = d.sms_optout_dttm, sms_optout_dttm_tz = d.sms_optout_dttm_tz, 
            task_version_id = d.task_version_id, sms_message_id = d.sms_message_id, 
            occurrence_id = d.occurrence_id, event_nm = d.event_nm, 
            creative_id = d.creative_id, context_type_nm = d.context_type_nm, 
            audience_id = d.audience_id, address_val = d.address_val, 
            context_val = d.context_val, event_designed_id = d.event_designed_id, 
            journey_id = d.journey_id, response_tracking_cd = d.response_tracking_cd, 
            task_id = d.task_id, aud_occurrence_id = d.aud_occurrence_id, 
            country_cd = d.country_cd, creative_version_id = d.creative_version_id, 
            identity_id = d.identity_id, journey_occurrence_id = d.journey_occurrence_id, 
            sender_id = d.sender_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            control_group_flg = d.control_group_flg, 
            product_qty_no = d.product_qty_no, properties_map_doc = d.properties_map_doc, 
            spot_clicked_dttm = d.spot_clicked_dttm, load_dttm = d.load_dttm, 
            spot_clicked_dttm_tz = d.spot_clicked_dttm_tz, session_id_hex = d.session_id_hex, 
            reserved_2_txt = d.reserved_2_txt, rec_group_id = d.rec_group_id, 
            product_id = d.product_id, message_id = d.message_id, 
            event_source_cd = d.event_source_cd, event_nm = d.event_nm, 
            detail_id_hex = d.detail_id_hex, context_val = d.context_val, 
            channel_user_id = d.channel_user_id, creative_id = d.creative_id, 
            identity_id = d.identity_id, mobile_app_id = d.mobile_app_id, 
            product_nm = d.product_nm, product_sku_no = d.product_sku_no, 
            request_id = d.request_id, segment_id = d.segment_id, 
            spot_id = d.spot_id, channel_nm = d.channel_nm, 
            context_type_nm = d.context_type_nm, creative_version_id = d.creative_version_id, 
            event_designed_id = d.event_designed_id, event_key_cd = d.event_key_cd, 
            message_version_id = d.message_version_id, occurrence_id = d.occurrence_id, 
            reserved_1_txt = d.reserved_1_txt, response_tracking_cd = d.response_tracking_cd, 
            segment_version_id = d.segment_version_id, visit_id_hex = d.visit_id_hex, 
            url_txt = d.url_txt, task_version_id = d.task_version_id, 
            task_id = d.task_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
            properties_map_doc = d.properties_map_doc, 
            load_dttm = d.load_dttm, spot_requested_dttm_tz = d.spot_requested_dttm_tz, 
            spot_requested_dttm = d.spot_requested_dttm, visit_id_hex = d.visit_id_hex, 
            spot_id = d.spot_id, session_id_hex = d.session_id_hex, 
            request_id = d.request_id, mobile_app_id = d.mobile_app_id, 
            identity_id = d.identity_id, event_source_cd = d.event_source_cd, 
            event_nm = d.event_nm, event_designed_id = d.event_designed_id, 
            detail_id_hex = d.detail_id_hex, context_val = d.context_val, 
            context_type_nm = d.context_type_nm, channel_user_id = d.channel_user_id, 
            channel_nm = d.channel_nm
         when not matched and EVENT_ID is NOT NULL then insert (
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
   %err_check (Failed to truncate tag_details , tag_details );
   proc append data=&udmmart..tag_details  base=&trglib..tag_details (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to tag_details , tag_details );
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
            sequence_no = d.sequence_no, 
            visit_dttm_tz = d.visit_dttm_tz, load_dttm = d.load_dttm, 
            visit_dttm = d.visit_dttm, visit_id_hex = d.visit_id_hex, 
            visit_id = d.visit_id, session_id_hex = d.session_id_hex, 
            session_id = d.session_id, search_term_txt = d.search_term_txt, 
            search_engine_domain_txt = d.search_engine_domain_txt, search_engine_desc = d.search_engine_desc, 
            referrer_txt = d.referrer_txt, referrer_query_string_txt = d.referrer_query_string_txt, 
            referrer_domain_nm = d.referrer_domain_nm, origination_type_nm = d.origination_type_nm, 
            origination_tracking_cd = d.origination_tracking_cd, origination_placement_nm = d.origination_placement_nm, 
            origination_nm = d.origination_nm, origination_creative_nm = d.origination_creative_nm, 
            identity_id = d.identity_id
         when not matched and EVENT_ID is NOT NULL then insert (
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
   %err_check (Failed to truncate wf_process_details , wf_process_details );
   proc append data=&udmmart..wf_process_details  base=&trglib..wf_process_details (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to wf_process_details , wf_process_details );
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
   %err_check (Failed to truncate wf_process_details_custom_prop , wf_process_details_custom_prop );
   proc append data=&udmmart..wf_process_details_custom_prop  base=&trglib..wf_process_details_custom_prop (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to wf_process_details_custom_prop , wf_process_details_custom_prop );
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
   %err_check (Failed to truncate wf_process_tasks , wf_process_tasks );
   proc append data=&udmmart..wf_process_tasks  base=&trglib..wf_process_tasks (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to wf_process_tasks , wf_process_tasks );
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
   %err_check (Failed to truncate wf_tasks_user_assignment , wf_tasks_user_assignment );
   proc append data=&udmmart..wf_tasks_user_assignment  base=&trglib..wf_tasks_user_assignment (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) force;
   run;
   %err_check (Failed to append to wf_tasks_user_assignment , wf_tasks_user_assignment );
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
%execute_POSTGRES_etl;
