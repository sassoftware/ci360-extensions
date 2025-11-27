/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%macro execute_REDSHIFT_code;
%if %sysfunc(exist(&udmmart..ab_test_path_assignment) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..ab_test_path_assignment_tmp     ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..ab_test_path_assignment_tmp     ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=ab_test_path_assignment, table_keys=%str(event_id), out_table=work.ab_test_path_assignment);
 data &tmplib..ab_test_path_assignment_tmp     ;
     set work.ab_test_path_assignment;
  if abtestpath_assignment_dttm ne . then abtestpath_assignment_dttm = tzoneu2s(abtestpath_assignment_dttm,&timeZone_Value.);if abtestpath_assignment_dttm_tz ne . then abtestpath_assignment_dttm_tz = tzoneu2s(abtestpath_assignment_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :ab_test_path_assignment_tmp     , ab_test_path_assignment);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..ab_test_path_assignment using &tmpdbschema..ab_test_path_assignment_tmp     
         ON (ab_test_path_assignment.event_id=ab_test_path_assignment_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET abtest_path_id = ab_test_path_assignment_tmp.abtest_path_id , abtestpath_assignment_dttm = ab_test_path_assignment_tmp.abtestpath_assignment_dttm , abtestpath_assignment_dttm_tz = ab_test_path_assignment_tmp.abtestpath_assignment_dttm_tz , activity_id = ab_test_path_assignment_tmp.activity_id , channel_nm = ab_test_path_assignment_tmp.channel_nm , channel_user_id = ab_test_path_assignment_tmp.channel_user_id , context_type_nm = ab_test_path_assignment_tmp.context_type_nm , context_val = ab_test_path_assignment_tmp.context_val , event_designed_id = ab_test_path_assignment_tmp.event_designed_id , event_nm = ab_test_path_assignment_tmp.event_nm , identity_id = ab_test_path_assignment_tmp.identity_id , load_dttm = ab_test_path_assignment_tmp.load_dttm , session_id_hex = ab_test_path_assignment_tmp.session_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        abtest_path_id,abtestpath_assignment_dttm,abtestpath_assignment_dttm_tz,activity_id,channel_nm,channel_user_id,context_type_nm,context_val,event_designed_id,event_id,event_nm,identity_id,load_dttm,session_id_hex
         ) values ( 
        ab_test_path_assignment_tmp.abtest_path_id,ab_test_path_assignment_tmp.abtestpath_assignment_dttm,ab_test_path_assignment_tmp.abtestpath_assignment_dttm_tz,ab_test_path_assignment_tmp.activity_id,ab_test_path_assignment_tmp.channel_nm,ab_test_path_assignment_tmp.channel_user_id,ab_test_path_assignment_tmp.context_type_nm,ab_test_path_assignment_tmp.context_val,ab_test_path_assignment_tmp.event_designed_id,ab_test_path_assignment_tmp.event_id,ab_test_path_assignment_tmp.event_nm,ab_test_path_assignment_tmp.identity_id,ab_test_path_assignment_tmp.load_dttm,ab_test_path_assignment_tmp.session_id_hex
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :ab_test_path_assignment_tmp     , ab_test_path_assignment, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..ab_test_path_assignment_tmp     ;
    QUIT;
    %put ######## Staging table: ab_test_path_assignment_tmp      Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..ab_test_path_assignment;
      DROP TABLE work.ab_test_path_assignment;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table ab_test_path_assignment;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..abt_attribution) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..abt_attribution_tmp             ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..abt_attribution_tmp             ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=abt_attribution, table_keys=%str(interaction_dttm,interaction_id), out_table=work.abt_attribution);
 data &tmplib..abt_attribution_tmp             ;
     set work.abt_attribution;
  if interaction_dttm ne . then interaction_dttm = tzoneu2s(interaction_dttm,&timeZone_Value.) ;
  if interaction_id='' then interaction_id='-';
 run;
 %ErrCheck (Failed to Append Data to :abt_attribution_tmp             , abt_attribution);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..abt_attribution using &tmpdbschema..abt_attribution_tmp             
         ON (abt_attribution.interaction_dttm=abt_attribution_tmp.interaction_dttm and abt_attribution.interaction_id=abt_attribution_tmp.interaction_id)
        WHEN MATCHED THEN  
        UPDATE SET conversion_value = abt_attribution_tmp.conversion_value , creative_id = abt_attribution_tmp.creative_id , identity_id = abt_attribution_tmp.identity_id , interaction = abt_attribution_tmp.interaction , interaction_cost = abt_attribution_tmp.interaction_cost , interaction_subtype = abt_attribution_tmp.interaction_subtype , interaction_type = abt_attribution_tmp.interaction_type , load_id = abt_attribution_tmp.load_id , task_id = abt_attribution_tmp.task_id
        WHEN NOT MATCHED THEN INSERT ( 
        conversion_value,creative_id,identity_id,interaction,interaction_cost,interaction_dttm,interaction_id,interaction_subtype,interaction_type,load_id,task_id
         ) values ( 
        abt_attribution_tmp.conversion_value,abt_attribution_tmp.creative_id,abt_attribution_tmp.identity_id,abt_attribution_tmp.interaction,abt_attribution_tmp.interaction_cost,abt_attribution_tmp.interaction_dttm,abt_attribution_tmp.interaction_id,abt_attribution_tmp.interaction_subtype,abt_attribution_tmp.interaction_type,abt_attribution_tmp.load_id,abt_attribution_tmp.task_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :abt_attribution_tmp             , abt_attribution, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..abt_attribution_tmp             ;
    QUIT;
    %put ######## Staging table: abt_attribution_tmp              Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..abt_attribution;
      DROP TABLE work.abt_attribution;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table abt_attribution;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..activity_conversion) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..activity_conversion_tmp         ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..activity_conversion_tmp         ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=activity_conversion, table_keys=%str(event_id), out_table=work.activity_conversion);
 data &tmplib..activity_conversion_tmp         ;
     set work.activity_conversion;
  if activity_conversion_dttm ne . then activity_conversion_dttm = tzoneu2s(activity_conversion_dttm,&timeZone_Value.);if activity_conversion_dttm_tz ne . then activity_conversion_dttm_tz = tzoneu2s(activity_conversion_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :activity_conversion_tmp         , activity_conversion);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..activity_conversion using &tmpdbschema..activity_conversion_tmp         
         ON (activity_conversion.event_id=activity_conversion_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET abtest_path_id = activity_conversion_tmp.abtest_path_id , activity_conversion_dttm = activity_conversion_tmp.activity_conversion_dttm , activity_conversion_dttm_tz = activity_conversion_tmp.activity_conversion_dttm_tz , activity_id = activity_conversion_tmp.activity_id , activity_node_id = activity_conversion_tmp.activity_node_id , channel_nm = activity_conversion_tmp.channel_nm , channel_user_id = activity_conversion_tmp.channel_user_id , context_type_nm = activity_conversion_tmp.context_type_nm , context_val = activity_conversion_tmp.context_val , detail_id_hex = activity_conversion_tmp.detail_id_hex , event_designed_id = activity_conversion_tmp.event_designed_id , event_nm = activity_conversion_tmp.event_nm , goal_id = activity_conversion_tmp.goal_id , identity_id = activity_conversion_tmp.identity_id , load_dttm = activity_conversion_tmp.load_dttm , parent_event_designed_id = activity_conversion_tmp.parent_event_designed_id , session_id_hex = activity_conversion_tmp.session_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        abtest_path_id,activity_conversion_dttm,activity_conversion_dttm_tz,activity_id,activity_node_id,channel_nm,channel_user_id,context_type_nm,context_val,detail_id_hex,event_designed_id,event_id,event_nm,goal_id,identity_id,load_dttm,parent_event_designed_id,session_id_hex
         ) values ( 
        activity_conversion_tmp.abtest_path_id,activity_conversion_tmp.activity_conversion_dttm,activity_conversion_tmp.activity_conversion_dttm_tz,activity_conversion_tmp.activity_id,activity_conversion_tmp.activity_node_id,activity_conversion_tmp.channel_nm,activity_conversion_tmp.channel_user_id,activity_conversion_tmp.context_type_nm,activity_conversion_tmp.context_val,activity_conversion_tmp.detail_id_hex,activity_conversion_tmp.event_designed_id,activity_conversion_tmp.event_id,activity_conversion_tmp.event_nm,activity_conversion_tmp.goal_id,activity_conversion_tmp.identity_id,activity_conversion_tmp.load_dttm,activity_conversion_tmp.parent_event_designed_id,activity_conversion_tmp.session_id_hex
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :activity_conversion_tmp         , activity_conversion, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..activity_conversion_tmp         ;
    QUIT;
    %put ######## Staging table: activity_conversion_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..activity_conversion;
      DROP TABLE work.activity_conversion;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table activity_conversion;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..activity_flow_in) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..activity_flow_in_tmp            ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..activity_flow_in_tmp            ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=activity_flow_in, table_keys=%str(event_id), out_table=work.activity_flow_in);
 data &tmplib..activity_flow_in_tmp            ;
     set work.activity_flow_in;
  if activity_flow_in_dttm ne . then activity_flow_in_dttm = tzoneu2s(activity_flow_in_dttm,&timeZone_Value.);if activity_flow_in_dttm_tz ne . then activity_flow_in_dttm_tz = tzoneu2s(activity_flow_in_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :activity_flow_in_tmp            , activity_flow_in);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..activity_flow_in using &tmpdbschema..activity_flow_in_tmp            
         ON (activity_flow_in.event_id=activity_flow_in_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET abtest_path_id = activity_flow_in_tmp.abtest_path_id , activity_flow_in_dttm = activity_flow_in_tmp.activity_flow_in_dttm , activity_flow_in_dttm_tz = activity_flow_in_tmp.activity_flow_in_dttm_tz , activity_id = activity_flow_in_tmp.activity_id , activity_node_id = activity_flow_in_tmp.activity_node_id , channel_nm = activity_flow_in_tmp.channel_nm , channel_user_id = activity_flow_in_tmp.channel_user_id , context_type_nm = activity_flow_in_tmp.context_type_nm , context_val = activity_flow_in_tmp.context_val , event_designed_id = activity_flow_in_tmp.event_designed_id , event_nm = activity_flow_in_tmp.event_nm , identity_id = activity_flow_in_tmp.identity_id , load_dttm = activity_flow_in_tmp.load_dttm , task_id = activity_flow_in_tmp.task_id
        WHEN NOT MATCHED THEN INSERT ( 
        abtest_path_id,activity_flow_in_dttm,activity_flow_in_dttm_tz,activity_id,activity_node_id,channel_nm,channel_user_id,context_type_nm,context_val,event_designed_id,event_id,event_nm,identity_id,load_dttm,task_id
         ) values ( 
        activity_flow_in_tmp.abtest_path_id,activity_flow_in_tmp.activity_flow_in_dttm,activity_flow_in_tmp.activity_flow_in_dttm_tz,activity_flow_in_tmp.activity_id,activity_flow_in_tmp.activity_node_id,activity_flow_in_tmp.channel_nm,activity_flow_in_tmp.channel_user_id,activity_flow_in_tmp.context_type_nm,activity_flow_in_tmp.context_val,activity_flow_in_tmp.event_designed_id,activity_flow_in_tmp.event_id,activity_flow_in_tmp.event_nm,activity_flow_in_tmp.identity_id,activity_flow_in_tmp.load_dttm,activity_flow_in_tmp.task_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :activity_flow_in_tmp            , activity_flow_in, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..activity_flow_in_tmp            ;
    QUIT;
    %put ######## Staging table: activity_flow_in_tmp             Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..activity_flow_in;
      DROP TABLE work.activity_flow_in;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table activity_flow_in;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..activity_start) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..activity_start_tmp              ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..activity_start_tmp              ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=activity_start, table_keys=%str(event_id), out_table=work.activity_start);
 data &tmplib..activity_start_tmp              ;
     set work.activity_start;
  if activity_start_dttm ne . then activity_start_dttm = tzoneu2s(activity_start_dttm,&timeZone_Value.);if activity_start_dttm_tz ne . then activity_start_dttm_tz = tzoneu2s(activity_start_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :activity_start_tmp              , activity_start);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..activity_start using &tmpdbschema..activity_start_tmp              
         ON (activity_start.event_id=activity_start_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET activity_id = activity_start_tmp.activity_id , activity_start_dttm = activity_start_tmp.activity_start_dttm , activity_start_dttm_tz = activity_start_tmp.activity_start_dttm_tz , channel_nm = activity_start_tmp.channel_nm , channel_user_id = activity_start_tmp.channel_user_id , context_type_nm = activity_start_tmp.context_type_nm , context_val = activity_start_tmp.context_val , event_designed_id = activity_start_tmp.event_designed_id , event_nm = activity_start_tmp.event_nm , identity_id = activity_start_tmp.identity_id , load_dttm = activity_start_tmp.load_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        activity_id,activity_start_dttm,activity_start_dttm_tz,channel_nm,channel_user_id,context_type_nm,context_val,event_designed_id,event_id,event_nm,identity_id,load_dttm
         ) values ( 
        activity_start_tmp.activity_id,activity_start_tmp.activity_start_dttm,activity_start_tmp.activity_start_dttm_tz,activity_start_tmp.channel_nm,activity_start_tmp.channel_user_id,activity_start_tmp.context_type_nm,activity_start_tmp.context_val,activity_start_tmp.event_designed_id,activity_start_tmp.event_id,activity_start_tmp.event_nm,activity_start_tmp.identity_id,activity_start_tmp.load_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :activity_start_tmp              , activity_start, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..activity_start_tmp              ;
    QUIT;
    %put ######## Staging table: activity_start_tmp               Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..activity_start;
      DROP TABLE work.activity_start;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table activity_start;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..advertising_contact) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..advertising_contact_tmp         ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..advertising_contact_tmp         ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=advertising_contact, table_keys=%str(event_id), out_table=work.advertising_contact);
 data &tmplib..advertising_contact_tmp         ;
     set work.advertising_contact;
  if advertising_contact_dttm ne . then advertising_contact_dttm = tzoneu2s(advertising_contact_dttm,&timeZone_Value.);if advertising_contact_dttm_tz ne . then advertising_contact_dttm_tz = tzoneu2s(advertising_contact_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :advertising_contact_tmp         , advertising_contact);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..advertising_contact using &tmpdbschema..advertising_contact_tmp         
         ON (advertising_contact.event_id=advertising_contact_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET advertising_contact_dttm = advertising_contact_tmp.advertising_contact_dttm , advertising_contact_dttm_tz = advertising_contact_tmp.advertising_contact_dttm_tz , advertising_platform_nm = advertising_contact_tmp.advertising_platform_nm , aud_occurrence_id = advertising_contact_tmp.aud_occurrence_id , audience_id = advertising_contact_tmp.audience_id , channel_nm = advertising_contact_tmp.channel_nm , context_type_nm = advertising_contact_tmp.context_type_nm , context_val = advertising_contact_tmp.context_val , event_designed_id = advertising_contact_tmp.event_designed_id , event_nm = advertising_contact_tmp.event_nm , identity_id = advertising_contact_tmp.identity_id , journey_id = advertising_contact_tmp.journey_id , journey_occurrence_id = advertising_contact_tmp.journey_occurrence_id , load_dttm = advertising_contact_tmp.load_dttm , occurrence_id = advertising_contact_tmp.occurrence_id , response_tracking_cd = advertising_contact_tmp.response_tracking_cd , segment_id = advertising_contact_tmp.segment_id , segment_version_id = advertising_contact_tmp.segment_version_id , task_action_nm = advertising_contact_tmp.task_action_nm , task_id = advertising_contact_tmp.task_id , task_version_id = advertising_contact_tmp.task_version_id
        WHEN NOT MATCHED THEN INSERT ( 
        advertising_contact_dttm,advertising_contact_dttm_tz,advertising_platform_nm,aud_occurrence_id,audience_id,channel_nm,context_type_nm,context_val,event_designed_id,event_id,event_nm,identity_id,journey_id,journey_occurrence_id,load_dttm,occurrence_id,response_tracking_cd,segment_id,segment_version_id,task_action_nm,task_id,task_version_id
         ) values ( 
        advertising_contact_tmp.advertising_contact_dttm,advertising_contact_tmp.advertising_contact_dttm_tz,advertising_contact_tmp.advertising_platform_nm,advertising_contact_tmp.aud_occurrence_id,advertising_contact_tmp.audience_id,advertising_contact_tmp.channel_nm,advertising_contact_tmp.context_type_nm,advertising_contact_tmp.context_val,advertising_contact_tmp.event_designed_id,advertising_contact_tmp.event_id,advertising_contact_tmp.event_nm,advertising_contact_tmp.identity_id,advertising_contact_tmp.journey_id,advertising_contact_tmp.journey_occurrence_id,advertising_contact_tmp.load_dttm,advertising_contact_tmp.occurrence_id,advertising_contact_tmp.response_tracking_cd,advertising_contact_tmp.segment_id,advertising_contact_tmp.segment_version_id,advertising_contact_tmp.task_action_nm,advertising_contact_tmp.task_id,advertising_contact_tmp.task_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :advertising_contact_tmp         , advertising_contact, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..advertising_contact_tmp         ;
    QUIT;
    %put ######## Staging table: advertising_contact_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..advertising_contact;
      DROP TABLE work.advertising_contact;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table advertising_contact;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..asset_details) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..asset_details_tmp               ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..asset_details_tmp               ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=asset_details, table_keys=%str(asset_id), out_table=work.asset_details);
 data &tmplib..asset_details_tmp               ;
     set work.asset_details;
  if asset_locked_dttm ne . then asset_locked_dttm = tzoneu2s(asset_locked_dttm,&timeZone_Value.);if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if download_disabled_dttm ne . then download_disabled_dttm = tzoneu2s(download_disabled_dttm,&timeZone_Value.);if expired_dttm ne . then expired_dttm = tzoneu2s(expired_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if recycled_dttm ne . then recycled_dttm = tzoneu2s(recycled_dttm,&timeZone_Value.) ;
  if asset_id='' then asset_id='-';
 run;
 %ErrCheck (Failed to Append Data to :asset_details_tmp               , asset_details);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..asset_details using &tmpdbschema..asset_details_tmp               
         ON (asset_details.asset_id=asset_details_tmp.asset_id)
        WHEN MATCHED THEN  
        UPDATE SET asset_deleted_flg = asset_details_tmp.asset_deleted_flg , asset_desc = asset_details_tmp.asset_desc , asset_locked_by_usernm = asset_details_tmp.asset_locked_by_usernm , asset_locked_dttm = asset_details_tmp.asset_locked_dttm , asset_locked_flg = asset_details_tmp.asset_locked_flg , asset_nm = asset_details_tmp.asset_nm , asset_owner_usernm = asset_details_tmp.asset_owner_usernm , asset_process_status = asset_details_tmp.asset_process_status , asset_sk = asset_details_tmp.asset_sk , asset_source_nm = asset_details_tmp.asset_source_nm , asset_source_type = asset_details_tmp.asset_source_type , average_user_rating_val = asset_details_tmp.average_user_rating_val , created_by_usernm = asset_details_tmp.created_by_usernm , created_dttm = asset_details_tmp.created_dttm , download_disabled_by_usernm = asset_details_tmp.download_disabled_by_usernm , download_disabled_dttm = asset_details_tmp.download_disabled_dttm , download_disabled_flg = asset_details_tmp.download_disabled_flg , entity_attribute_enabled_flg = asset_details_tmp.entity_attribute_enabled_flg , entity_revision_enabled_flg = asset_details_tmp.entity_revision_enabled_flg , entity_status_cd = asset_details_tmp.entity_status_cd , entity_subtype_enabled_flg = asset_details_tmp.entity_subtype_enabled_flg , entity_subtype_nm = asset_details_tmp.entity_subtype_nm , entity_table_nm = asset_details_tmp.entity_table_nm , entity_type_nm = asset_details_tmp.entity_type_nm , entity_type_usage_cd = asset_details_tmp.entity_type_usage_cd , expired_dttm = asset_details_tmp.expired_dttm , expired_flg = asset_details_tmp.expired_flg , external_sharing_error_dt = asset_details_tmp.external_sharing_error_dt , external_sharing_error_msg = asset_details_tmp.external_sharing_error_msg , folder_deleted_flg = asset_details_tmp.folder_deleted_flg , folder_desc = asset_details_tmp.folder_desc , folder_entity_status_cd = asset_details_tmp.folder_entity_status_cd , folder_id = asset_details_tmp.folder_id , folder_level = asset_details_tmp.folder_level , folder_nm = asset_details_tmp.folder_nm , folder_owner_usernm = asset_details_tmp.folder_owner_usernm , folder_path = asset_details_tmp.folder_path , folder_sk = asset_details_tmp.folder_sk , last_modified_by_usernm = asset_details_tmp.last_modified_by_usernm , last_modified_dttm = asset_details_tmp.last_modified_dttm , load_dttm = asset_details_tmp.load_dttm , process_id = asset_details_tmp.process_id , process_task_id = asset_details_tmp.process_task_id , public_link = asset_details_tmp.public_link , public_media_id = asset_details_tmp.public_media_id , public_url = asset_details_tmp.public_url , recycled_by_usernm = asset_details_tmp.recycled_by_usernm , recycled_dttm = asset_details_tmp.recycled_dttm , total_user_rating_val = asset_details_tmp.total_user_rating_val , user_rating_cnt = asset_details_tmp.user_rating_cnt
        WHEN NOT MATCHED THEN INSERT ( 
        asset_deleted_flg,asset_desc,asset_id,asset_locked_by_usernm,asset_locked_dttm,asset_locked_flg,asset_nm,asset_owner_usernm,asset_process_status,asset_sk,asset_source_nm,asset_source_type,average_user_rating_val,created_by_usernm,created_dttm,download_disabled_by_usernm,download_disabled_dttm,download_disabled_flg,entity_attribute_enabled_flg,entity_revision_enabled_flg,entity_status_cd,entity_subtype_enabled_flg,entity_subtype_nm,entity_table_nm,entity_type_nm,entity_type_usage_cd,expired_dttm,expired_flg,external_sharing_error_dt,external_sharing_error_msg,folder_deleted_flg,folder_desc,folder_entity_status_cd,folder_id,folder_level,folder_nm,folder_owner_usernm,folder_path,folder_sk,last_modified_by_usernm,last_modified_dttm,load_dttm,process_id,process_task_id,public_link,public_media_id,public_url,recycled_by_usernm,recycled_dttm,total_user_rating_val,user_rating_cnt
         ) values ( 
        asset_details_tmp.asset_deleted_flg,asset_details_tmp.asset_desc,asset_details_tmp.asset_id,asset_details_tmp.asset_locked_by_usernm,asset_details_tmp.asset_locked_dttm,asset_details_tmp.asset_locked_flg,asset_details_tmp.asset_nm,asset_details_tmp.asset_owner_usernm,asset_details_tmp.asset_process_status,asset_details_tmp.asset_sk,asset_details_tmp.asset_source_nm,asset_details_tmp.asset_source_type,asset_details_tmp.average_user_rating_val,asset_details_tmp.created_by_usernm,asset_details_tmp.created_dttm,asset_details_tmp.download_disabled_by_usernm,asset_details_tmp.download_disabled_dttm,asset_details_tmp.download_disabled_flg,asset_details_tmp.entity_attribute_enabled_flg,asset_details_tmp.entity_revision_enabled_flg,asset_details_tmp.entity_status_cd,asset_details_tmp.entity_subtype_enabled_flg,asset_details_tmp.entity_subtype_nm,asset_details_tmp.entity_table_nm,asset_details_tmp.entity_type_nm,asset_details_tmp.entity_type_usage_cd,asset_details_tmp.expired_dttm,asset_details_tmp.expired_flg,asset_details_tmp.external_sharing_error_dt,asset_details_tmp.external_sharing_error_msg,asset_details_tmp.folder_deleted_flg,asset_details_tmp.folder_desc,asset_details_tmp.folder_entity_status_cd,asset_details_tmp.folder_id,asset_details_tmp.folder_level,asset_details_tmp.folder_nm,asset_details_tmp.folder_owner_usernm,asset_details_tmp.folder_path,asset_details_tmp.folder_sk,asset_details_tmp.last_modified_by_usernm,asset_details_tmp.last_modified_dttm,asset_details_tmp.load_dttm,asset_details_tmp.process_id,asset_details_tmp.process_task_id,asset_details_tmp.public_link,asset_details_tmp.public_media_id,asset_details_tmp.public_url,asset_details_tmp.recycled_by_usernm,asset_details_tmp.recycled_dttm,asset_details_tmp.total_user_rating_val,asset_details_tmp.user_rating_cnt
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :asset_details_tmp               , asset_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..asset_details_tmp               ;
    QUIT;
    %put ######## Staging table: asset_details_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..asset_details;
      DROP TABLE work.asset_details;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table asset_details;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..asset_details_custom_prop) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..asset_details_custom_prop_tmp   ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..asset_details_custom_prop_tmp   ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=asset_details_custom_prop, table_keys=%str(asset_id,attr_id), out_table=work.asset_details_custom_prop);
 data &tmplib..asset_details_custom_prop_tmp   ;
     set work.asset_details_custom_prop;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if asset_id='' then asset_id='-'; if attr_id='' then attr_id='-';
 run;
 %ErrCheck (Failed to Append Data to :asset_details_custom_prop_tmp   , asset_details_custom_prop);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..asset_details_custom_prop using &tmpdbschema..asset_details_custom_prop_tmp   
         ON (asset_details_custom_prop.asset_id=asset_details_custom_prop_tmp.asset_id and asset_details_custom_prop.attr_id=asset_details_custom_prop_tmp.attr_id)
        WHEN MATCHED THEN  
        UPDATE SET attr_cd = asset_details_custom_prop_tmp.attr_cd , attr_group_cd = asset_details_custom_prop_tmp.attr_group_cd , attr_group_id = asset_details_custom_prop_tmp.attr_group_id , attr_group_nm = asset_details_custom_prop_tmp.attr_group_nm , attr_nm = asset_details_custom_prop_tmp.attr_nm , attr_val = asset_details_custom_prop_tmp.attr_val , created_by_usernm = asset_details_custom_prop_tmp.created_by_usernm , created_dttm = asset_details_custom_prop_tmp.created_dttm , data_formatter = asset_details_custom_prop_tmp.data_formatter , data_type = asset_details_custom_prop_tmp.data_type , is_grid_flg = asset_details_custom_prop_tmp.is_grid_flg , is_obsolete_flg = asset_details_custom_prop_tmp.is_obsolete_flg , last_modified_dttm = asset_details_custom_prop_tmp.last_modified_dttm , last_modified_usernm = asset_details_custom_prop_tmp.last_modified_usernm , load_dttm = asset_details_custom_prop_tmp.load_dttm , remote_pklist_tab_col = asset_details_custom_prop_tmp.remote_pklist_tab_col
        WHEN NOT MATCHED THEN INSERT ( 
        asset_id,attr_cd,attr_group_cd,attr_group_id,attr_group_nm,attr_id,attr_nm,attr_val,created_by_usernm,created_dttm,data_formatter,data_type,is_grid_flg,is_obsolete_flg,last_modified_dttm,last_modified_usernm,load_dttm,remote_pklist_tab_col
         ) values ( 
        asset_details_custom_prop_tmp.asset_id,asset_details_custom_prop_tmp.attr_cd,asset_details_custom_prop_tmp.attr_group_cd,asset_details_custom_prop_tmp.attr_group_id,asset_details_custom_prop_tmp.attr_group_nm,asset_details_custom_prop_tmp.attr_id,asset_details_custom_prop_tmp.attr_nm,asset_details_custom_prop_tmp.attr_val,asset_details_custom_prop_tmp.created_by_usernm,asset_details_custom_prop_tmp.created_dttm,asset_details_custom_prop_tmp.data_formatter,asset_details_custom_prop_tmp.data_type,asset_details_custom_prop_tmp.is_grid_flg,asset_details_custom_prop_tmp.is_obsolete_flg,asset_details_custom_prop_tmp.last_modified_dttm,asset_details_custom_prop_tmp.last_modified_usernm,asset_details_custom_prop_tmp.load_dttm,asset_details_custom_prop_tmp.remote_pklist_tab_col
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :asset_details_custom_prop_tmp   , asset_details_custom_prop, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..asset_details_custom_prop_tmp   ;
    QUIT;
    %put ######## Staging table: asset_details_custom_prop_tmp    Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..asset_details_custom_prop;
      DROP TABLE work.asset_details_custom_prop;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table asset_details_custom_prop;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..asset_folder_details) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..asset_folder_details_tmp        ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..asset_folder_details_tmp        ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=asset_folder_details, table_keys=%str(folder_id), out_table=work.asset_folder_details);
 data &tmplib..asset_folder_details_tmp        ;
     set work.asset_folder_details;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if folder_id='' then folder_id='-';
 run;
 %ErrCheck (Failed to Append Data to :asset_folder_details_tmp        , asset_folder_details);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..asset_folder_details using &tmpdbschema..asset_folder_details_tmp        
         ON (asset_folder_details.folder_id=asset_folder_details_tmp.folder_id)
        WHEN MATCHED THEN  
        UPDATE SET created_by_usernm = asset_folder_details_tmp.created_by_usernm , created_dttm = asset_folder_details_tmp.created_dttm , deleted_flg = asset_folder_details_tmp.deleted_flg , entity_status_cd = asset_folder_details_tmp.entity_status_cd , folder_desc = asset_folder_details_tmp.folder_desc , folder_level = asset_folder_details_tmp.folder_level , folder_nm = asset_folder_details_tmp.folder_nm , folder_owner_usernm = asset_folder_details_tmp.folder_owner_usernm , folder_path = asset_folder_details_tmp.folder_path , last_modified_by_usernm = asset_folder_details_tmp.last_modified_by_usernm , last_modified_dttm = asset_folder_details_tmp.last_modified_dttm , load_dttm = asset_folder_details_tmp.load_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        created_by_usernm,created_dttm,deleted_flg,entity_status_cd,folder_desc,folder_id,folder_level,folder_nm,folder_owner_usernm,folder_path,last_modified_by_usernm,last_modified_dttm,load_dttm
         ) values ( 
        asset_folder_details_tmp.created_by_usernm,asset_folder_details_tmp.created_dttm,asset_folder_details_tmp.deleted_flg,asset_folder_details_tmp.entity_status_cd,asset_folder_details_tmp.folder_desc,asset_folder_details_tmp.folder_id,asset_folder_details_tmp.folder_level,asset_folder_details_tmp.folder_nm,asset_folder_details_tmp.folder_owner_usernm,asset_folder_details_tmp.folder_path,asset_folder_details_tmp.last_modified_by_usernm,asset_folder_details_tmp.last_modified_dttm,asset_folder_details_tmp.load_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :asset_folder_details_tmp        , asset_folder_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..asset_folder_details_tmp        ;
    QUIT;
    %put ######## Staging table: asset_folder_details_tmp         Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..asset_folder_details;
      DROP TABLE work.asset_folder_details;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table asset_folder_details;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..asset_rendition_details) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..asset_rendition_details_tmp     ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..asset_rendition_details_tmp     ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=asset_rendition_details, table_keys=%str(asset_id,rendition_id,revision_id,revision_no), out_table=work.asset_rendition_details);
 data &tmplib..asset_rendition_details_tmp     ;
     set work.asset_rendition_details;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if asset_id='' then asset_id='-'; if rendition_id='' then rendition_id='-'; if revision_id='' then revision_id='-';
 run;
 %ErrCheck (Failed to Append Data to :asset_rendition_details_tmp     , asset_rendition_details);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..asset_rendition_details using &tmpdbschema..asset_rendition_details_tmp     
         ON (asset_rendition_details.asset_id=asset_rendition_details_tmp.asset_id and asset_rendition_details.rendition_id=asset_rendition_details_tmp.rendition_id and asset_rendition_details.revision_id=asset_rendition_details_tmp.revision_id and asset_rendition_details.revision_no=asset_rendition_details_tmp.revision_no)
        WHEN MATCHED THEN  
        UPDATE SET created_by_usernm = asset_rendition_details_tmp.created_by_usernm , created_dttm = asset_rendition_details_tmp.created_dttm , current_revision_flg = asset_rendition_details_tmp.current_revision_flg , download_cnt = asset_rendition_details_tmp.download_cnt , entity_status_cd = asset_rendition_details_tmp.entity_status_cd , file_format = asset_rendition_details_tmp.file_format , file_nm = asset_rendition_details_tmp.file_nm , file_size = asset_rendition_details_tmp.file_size , last_modified_by_usernm = asset_rendition_details_tmp.last_modified_by_usernm , last_modified_dttm = asset_rendition_details_tmp.last_modified_dttm , last_modified_status_cd = asset_rendition_details_tmp.last_modified_status_cd , load_dttm = asset_rendition_details_tmp.load_dttm , media_depth = asset_rendition_details_tmp.media_depth , media_dpi = asset_rendition_details_tmp.media_dpi , media_height = asset_rendition_details_tmp.media_height , media_width = asset_rendition_details_tmp.media_width , rend_deleted_flg = asset_rendition_details_tmp.rend_deleted_flg , rend_duration = asset_rendition_details_tmp.rend_duration , rendition_generated_type_cd = asset_rendition_details_tmp.rendition_generated_type_cd , rendition_nm = asset_rendition_details_tmp.rendition_nm , rendition_type_cd = asset_rendition_details_tmp.rendition_type_cd , rev_deleted_flg = asset_rendition_details_tmp.rev_deleted_flg , revision_comment_txt = asset_rendition_details_tmp.revision_comment_txt
        WHEN NOT MATCHED THEN INSERT ( 
        asset_id,created_by_usernm,created_dttm,current_revision_flg,download_cnt,entity_status_cd,file_format,file_nm,file_size,last_modified_by_usernm,last_modified_dttm,last_modified_status_cd,load_dttm,media_depth,media_dpi,media_height,media_width,rend_deleted_flg,rend_duration,rendition_generated_type_cd,rendition_id,rendition_nm,rendition_type_cd,rev_deleted_flg,revision_comment_txt,revision_id,revision_no
         ) values ( 
        asset_rendition_details_tmp.asset_id,asset_rendition_details_tmp.created_by_usernm,asset_rendition_details_tmp.created_dttm,asset_rendition_details_tmp.current_revision_flg,asset_rendition_details_tmp.download_cnt,asset_rendition_details_tmp.entity_status_cd,asset_rendition_details_tmp.file_format,asset_rendition_details_tmp.file_nm,asset_rendition_details_tmp.file_size,asset_rendition_details_tmp.last_modified_by_usernm,asset_rendition_details_tmp.last_modified_dttm,asset_rendition_details_tmp.last_modified_status_cd,asset_rendition_details_tmp.load_dttm,asset_rendition_details_tmp.media_depth,asset_rendition_details_tmp.media_dpi,asset_rendition_details_tmp.media_height,asset_rendition_details_tmp.media_width,asset_rendition_details_tmp.rend_deleted_flg,asset_rendition_details_tmp.rend_duration,asset_rendition_details_tmp.rendition_generated_type_cd,asset_rendition_details_tmp.rendition_id,asset_rendition_details_tmp.rendition_nm,asset_rendition_details_tmp.rendition_type_cd,asset_rendition_details_tmp.rev_deleted_flg,asset_rendition_details_tmp.revision_comment_txt,asset_rendition_details_tmp.revision_id,asset_rendition_details_tmp.revision_no
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :asset_rendition_details_tmp     , asset_rendition_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..asset_rendition_details_tmp     ;
    QUIT;
    %put ######## Staging table: asset_rendition_details_tmp      Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..asset_rendition_details;
      DROP TABLE work.asset_rendition_details;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table asset_rendition_details;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..asset_revision) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..asset_revision_tmp              ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..asset_revision_tmp              ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=asset_revision, table_keys=%str(asset_id,revision_id,revision_no), out_table=work.asset_revision);
 data &tmplib..asset_revision_tmp              ;
     set work.asset_revision;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if asset_id='' then asset_id='-'; if revision_id='' then revision_id='-';
 run;
 %ErrCheck (Failed to Append Data to :asset_revision_tmp              , asset_revision);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..asset_revision using &tmpdbschema..asset_revision_tmp              
         ON (asset_revision.asset_id=asset_revision_tmp.asset_id and asset_revision.revision_id=asset_revision_tmp.revision_id and asset_revision.revision_no=asset_revision_tmp.revision_no)
        WHEN MATCHED THEN  
        UPDATE SET created_by_usernm = asset_revision_tmp.created_by_usernm , created_dttm = asset_revision_tmp.created_dttm , current_revision_flg = asset_revision_tmp.current_revision_flg , deleted_flg = asset_revision_tmp.deleted_flg , entity_status_cd = asset_revision_tmp.entity_status_cd , last_modified_by_usernm = asset_revision_tmp.last_modified_by_usernm , last_modified_dttm = asset_revision_tmp.last_modified_dttm , load_dttm = asset_revision_tmp.load_dttm , revision_comment_txt = asset_revision_tmp.revision_comment_txt
        WHEN NOT MATCHED THEN INSERT ( 
        asset_id,created_by_usernm,created_dttm,current_revision_flg,deleted_flg,entity_status_cd,last_modified_by_usernm,last_modified_dttm,load_dttm,revision_comment_txt,revision_id,revision_no
         ) values ( 
        asset_revision_tmp.asset_id,asset_revision_tmp.created_by_usernm,asset_revision_tmp.created_dttm,asset_revision_tmp.current_revision_flg,asset_revision_tmp.deleted_flg,asset_revision_tmp.entity_status_cd,asset_revision_tmp.last_modified_by_usernm,asset_revision_tmp.last_modified_dttm,asset_revision_tmp.load_dttm,asset_revision_tmp.revision_comment_txt,asset_revision_tmp.revision_id,asset_revision_tmp.revision_no
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :asset_revision_tmp              , asset_revision, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..asset_revision_tmp              ;
    QUIT;
    %put ######## Staging table: asset_revision_tmp               Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..asset_revision;
      DROP TABLE work.asset_revision;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table asset_revision;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..audience_membership_change) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..audience_membership_change_tmp  ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..audience_membership_change_tmp  ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=audience_membership_change, table_keys=%str(event_id), out_table=work.audience_membership_change);
 data &tmplib..audience_membership_change_tmp  ;
     set work.audience_membership_change;
  if audience_change_dttm ne . then audience_change_dttm = tzoneu2s(audience_change_dttm,&timeZone_Value.);if audience_change_dttm_tz ne . then audience_change_dttm_tz = tzoneu2s(audience_change_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :audience_membership_change_tmp  , audience_membership_change);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..audience_membership_change using &tmpdbschema..audience_membership_change_tmp  
         ON (audience_membership_change.event_id=audience_membership_change_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET aud_occurrence_id = audience_membership_change_tmp.aud_occurrence_id , audience_change_dttm = audience_membership_change_tmp.audience_change_dttm , audience_change_dttm_tz = audience_membership_change_tmp.audience_change_dttm_tz , audience_id = audience_membership_change_tmp.audience_id , event_nm = audience_membership_change_tmp.event_nm , identity_id = audience_membership_change_tmp.identity_id , load_dttm = audience_membership_change_tmp.load_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        aud_occurrence_id,audience_change_dttm,audience_change_dttm_tz,audience_id,event_id,event_nm,identity_id,load_dttm
         ) values ( 
        audience_membership_change_tmp.aud_occurrence_id,audience_membership_change_tmp.audience_change_dttm,audience_membership_change_tmp.audience_change_dttm_tz,audience_membership_change_tmp.audience_id,audience_membership_change_tmp.event_id,audience_membership_change_tmp.event_nm,audience_membership_change_tmp.identity_id,audience_membership_change_tmp.load_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :audience_membership_change_tmp  , audience_membership_change, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..audience_membership_change_tmp  ;
    QUIT;
    %put ######## Staging table: audience_membership_change_tmp   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..audience_membership_change;
      DROP TABLE work.audience_membership_change;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table audience_membership_change;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..business_process_details) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..business_process_details_tmp    ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..business_process_details_tmp    ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=business_process_details, table_keys=%str(detail_id,event_designed_id,process_dttm,process_instance_no,process_nm,process_step_nm,step_order_no), out_table=work.business_process_details);
 data &tmplib..business_process_details_tmp    ;
     set work.business_process_details;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if process_dttm ne . then process_dttm = tzoneu2s(process_dttm,&timeZone_Value.);if process_dttm_tz ne . then process_dttm_tz = tzoneu2s(process_dttm_tz,&timeZone_Value.);if process_exception_dttm ne . then process_exception_dttm = tzoneu2s(process_exception_dttm,&timeZone_Value.);if process_exception_dttm_tz ne . then process_exception_dttm_tz = tzoneu2s(process_exception_dttm_tz,&timeZone_Value.) ;
  if detail_id='' then detail_id='-'; if event_designed_id='' then event_designed_id='-'; if process_nm='' then process_nm='-'; if process_step_nm='' then process_step_nm='-';
 run;
 %ErrCheck (Failed to Append Data to :business_process_details_tmp    , business_process_details);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..business_process_details using &tmpdbschema..business_process_details_tmp    
         ON (business_process_details.detail_id=business_process_details_tmp.detail_id and business_process_details.event_designed_id=business_process_details_tmp.event_designed_id and business_process_details.process_dttm=business_process_details_tmp.process_dttm and business_process_details.process_instance_no=business_process_details_tmp.process_instance_no and business_process_details.process_nm=business_process_details_tmp.process_nm and business_process_details.process_step_nm=business_process_details_tmp.process_step_nm and business_process_details.step_order_no=business_process_details_tmp.step_order_no)
        WHEN MATCHED THEN  
        UPDATE SET attribute1_txt = business_process_details_tmp.attribute1_txt , attribute2_txt = business_process_details_tmp.attribute2_txt , detail_id_hex = business_process_details_tmp.detail_id_hex , event_id = business_process_details_tmp.event_id , event_nm = business_process_details_tmp.event_nm , event_source_cd = business_process_details_tmp.event_source_cd , identity_id = business_process_details_tmp.identity_id , is_completion_flg = business_process_details_tmp.is_completion_flg , is_start_flg = business_process_details_tmp.is_start_flg , load_dttm = business_process_details_tmp.load_dttm , next_detail_id = business_process_details_tmp.next_detail_id , process_attempt_cnt = business_process_details_tmp.process_attempt_cnt , process_details_sk = business_process_details_tmp.process_details_sk , process_dttm_tz = business_process_details_tmp.process_dttm_tz , process_exception_dttm = business_process_details_tmp.process_exception_dttm , process_exception_dttm_tz = business_process_details_tmp.process_exception_dttm_tz , process_exception_txt = business_process_details_tmp.process_exception_txt , session_id = business_process_details_tmp.session_id , session_id_hex = business_process_details_tmp.session_id_hex , visit_id = business_process_details_tmp.visit_id , visit_id_hex = business_process_details_tmp.visit_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        attribute1_txt,attribute2_txt,detail_id,detail_id_hex,event_designed_id,event_id,event_nm,event_source_cd,identity_id,is_completion_flg,is_start_flg,load_dttm,next_detail_id,process_attempt_cnt,process_details_sk,process_dttm,process_dttm_tz,process_exception_dttm,process_exception_dttm_tz,process_exception_txt,process_instance_no,process_nm,process_step_nm,session_id,session_id_hex,step_order_no,visit_id,visit_id_hex
         ) values ( 
        business_process_details_tmp.attribute1_txt,business_process_details_tmp.attribute2_txt,business_process_details_tmp.detail_id,business_process_details_tmp.detail_id_hex,business_process_details_tmp.event_designed_id,business_process_details_tmp.event_id,business_process_details_tmp.event_nm,business_process_details_tmp.event_source_cd,business_process_details_tmp.identity_id,business_process_details_tmp.is_completion_flg,business_process_details_tmp.is_start_flg,business_process_details_tmp.load_dttm,business_process_details_tmp.next_detail_id,business_process_details_tmp.process_attempt_cnt,business_process_details_tmp.process_details_sk,business_process_details_tmp.process_dttm,business_process_details_tmp.process_dttm_tz,business_process_details_tmp.process_exception_dttm,business_process_details_tmp.process_exception_dttm_tz,business_process_details_tmp.process_exception_txt,business_process_details_tmp.process_instance_no,business_process_details_tmp.process_nm,business_process_details_tmp.process_step_nm,business_process_details_tmp.session_id,business_process_details_tmp.session_id_hex,business_process_details_tmp.step_order_no,business_process_details_tmp.visit_id,business_process_details_tmp.visit_id_hex
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :business_process_details_tmp    , business_process_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..business_process_details_tmp    ;
    QUIT;
    %put ######## Staging table: business_process_details_tmp     Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..business_process_details;
      DROP TABLE work.business_process_details;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table business_process_details;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cart_activity_details) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cart_activity_details_tmp       ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cart_activity_details_tmp       ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cart_activity_details, table_keys=%str(activity_dttm,detail_id,product_id,product_nm,product_sku), out_table=work.cart_activity_details);
 data &tmplib..cart_activity_details_tmp       ;
     set work.cart_activity_details;
  if activity_dttm ne . then activity_dttm = tzoneu2s(activity_dttm,&timeZone_Value.);if activity_dttm_tz ne . then activity_dttm_tz = tzoneu2s(activity_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if detail_id='' then detail_id='-'; if product_id='' then product_id='-'; if product_nm='' then product_nm='-'; if product_sku='' then product_sku='-';
 run;
 %ErrCheck (Failed to Append Data to :cart_activity_details_tmp       , cart_activity_details);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cart_activity_details using &tmpdbschema..cart_activity_details_tmp       
         ON (cart_activity_details.activity_dttm=cart_activity_details_tmp.activity_dttm and cart_activity_details.detail_id=cart_activity_details_tmp.detail_id and cart_activity_details.product_id=cart_activity_details_tmp.product_id and cart_activity_details.product_nm=cart_activity_details_tmp.product_nm and cart_activity_details.product_sku=cart_activity_details_tmp.product_sku)
        WHEN MATCHED THEN  
        UPDATE SET activity_cd = cart_activity_details_tmp.activity_cd , activity_dttm_tz = cart_activity_details_tmp.activity_dttm_tz , availability_message_txt = cart_activity_details_tmp.availability_message_txt , cart_activity_sk = cart_activity_details_tmp.cart_activity_sk , cart_id = cart_activity_details_tmp.cart_id , cart_nm = cart_activity_details_tmp.cart_nm , channel_nm = cart_activity_details_tmp.channel_nm , currency_cd = cart_activity_details_tmp.currency_cd , detail_id_hex = cart_activity_details_tmp.detail_id_hex , displayed_cart_amt = cart_activity_details_tmp.displayed_cart_amt , displayed_cart_items_no = cart_activity_details_tmp.displayed_cart_items_no , event_designed_id = cart_activity_details_tmp.event_designed_id , event_id = cart_activity_details_tmp.event_id , event_key_cd = cart_activity_details_tmp.event_key_cd , event_nm = cart_activity_details_tmp.event_nm , event_source_cd = cart_activity_details_tmp.event_source_cd , identity_id = cart_activity_details_tmp.identity_id , load_dttm = cart_activity_details_tmp.load_dttm , mobile_app_id = cart_activity_details_tmp.mobile_app_id , product_group_nm = cart_activity_details_tmp.product_group_nm , properties_map_doc = cart_activity_details_tmp.properties_map_doc , quantity_val = cart_activity_details_tmp.quantity_val , saving_message_txt = cart_activity_details_tmp.saving_message_txt , session_id = cart_activity_details_tmp.session_id , session_id_hex = cart_activity_details_tmp.session_id_hex , shipping_message_txt = cart_activity_details_tmp.shipping_message_txt , unit_price_amt = cart_activity_details_tmp.unit_price_amt , visit_id = cart_activity_details_tmp.visit_id , visit_id_hex = cart_activity_details_tmp.visit_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        activity_cd,activity_dttm,activity_dttm_tz,availability_message_txt,cart_activity_sk,cart_id,cart_nm,channel_nm,currency_cd,detail_id,detail_id_hex,displayed_cart_amt,displayed_cart_items_no,event_designed_id,event_id,event_key_cd,event_nm,event_source_cd,identity_id,load_dttm,mobile_app_id,product_group_nm,product_id,product_nm,product_sku,properties_map_doc,quantity_val,saving_message_txt,session_id,session_id_hex,shipping_message_txt,unit_price_amt,visit_id,visit_id_hex
         ) values ( 
        cart_activity_details_tmp.activity_cd,cart_activity_details_tmp.activity_dttm,cart_activity_details_tmp.activity_dttm_tz,cart_activity_details_tmp.availability_message_txt,cart_activity_details_tmp.cart_activity_sk,cart_activity_details_tmp.cart_id,cart_activity_details_tmp.cart_nm,cart_activity_details_tmp.channel_nm,cart_activity_details_tmp.currency_cd,cart_activity_details_tmp.detail_id,cart_activity_details_tmp.detail_id_hex,cart_activity_details_tmp.displayed_cart_amt,cart_activity_details_tmp.displayed_cart_items_no,cart_activity_details_tmp.event_designed_id,cart_activity_details_tmp.event_id,cart_activity_details_tmp.event_key_cd,cart_activity_details_tmp.event_nm,cart_activity_details_tmp.event_source_cd,cart_activity_details_tmp.identity_id,cart_activity_details_tmp.load_dttm,cart_activity_details_tmp.mobile_app_id,cart_activity_details_tmp.product_group_nm,cart_activity_details_tmp.product_id,cart_activity_details_tmp.product_nm,cart_activity_details_tmp.product_sku,cart_activity_details_tmp.properties_map_doc,cart_activity_details_tmp.quantity_val,cart_activity_details_tmp.saving_message_txt,cart_activity_details_tmp.session_id,cart_activity_details_tmp.session_id_hex,cart_activity_details_tmp.shipping_message_txt,cart_activity_details_tmp.unit_price_amt,cart_activity_details_tmp.visit_id,cart_activity_details_tmp.visit_id_hex
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cart_activity_details_tmp       , cart_activity_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cart_activity_details_tmp       ;
    QUIT;
    %put ######## Staging table: cart_activity_details_tmp        Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cart_activity_details;
      DROP TABLE work.cart_activity_details;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cart_activity_details;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cc_budget_breakup) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cc_budget_breakup_tmp           ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cc_budget_breakup_tmp           ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cc_budget_breakup, table_keys=%str(cost_center_id,planning_id), out_table=work.cc_budget_breakup);
 data &tmplib..cc_budget_breakup_tmp           ;
     set work.cc_budget_breakup;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if cost_center_id='' then cost_center_id='-'; if planning_id='' then planning_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cc_budget_breakup_tmp           , cc_budget_breakup);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cc_budget_breakup using &tmpdbschema..cc_budget_breakup_tmp           
         ON (cc_budget_breakup.cost_center_id=cc_budget_breakup_tmp.cost_center_id and cc_budget_breakup.planning_id=cc_budget_breakup_tmp.planning_id)
        WHEN MATCHED THEN  
        UPDATE SET cc_budget_distribution = cc_budget_breakup_tmp.cc_budget_distribution , cc_desc = cc_budget_breakup_tmp.cc_desc , cc_nm = cc_budget_breakup_tmp.cc_nm , cc_obsolete_flg = cc_budget_breakup_tmp.cc_obsolete_flg , cc_owner_usernm = cc_budget_breakup_tmp.cc_owner_usernm , created_by_usernm = cc_budget_breakup_tmp.created_by_usernm , created_dttm = cc_budget_breakup_tmp.created_dttm , fin_accnt_desc = cc_budget_breakup_tmp.fin_accnt_desc , fin_accnt_nm = cc_budget_breakup_tmp.fin_accnt_nm , fin_accnt_obsolete_flg = cc_budget_breakup_tmp.fin_accnt_obsolete_flg , gen_ledger_cd = cc_budget_breakup_tmp.gen_ledger_cd , last_modified_dttm = cc_budget_breakup_tmp.last_modified_dttm , last_modified_usernm = cc_budget_breakup_tmp.last_modified_usernm , load_dttm = cc_budget_breakup_tmp.load_dttm , planning_nm = cc_budget_breakup_tmp.planning_nm
        WHEN NOT MATCHED THEN INSERT ( 
        cc_budget_distribution,cc_desc,cc_nm,cc_obsolete_flg,cc_owner_usernm,cost_center_id,created_by_usernm,created_dttm,fin_accnt_desc,fin_accnt_nm,fin_accnt_obsolete_flg,gen_ledger_cd,last_modified_dttm,last_modified_usernm,load_dttm,planning_id,planning_nm
         ) values ( 
        cc_budget_breakup_tmp.cc_budget_distribution,cc_budget_breakup_tmp.cc_desc,cc_budget_breakup_tmp.cc_nm,cc_budget_breakup_tmp.cc_obsolete_flg,cc_budget_breakup_tmp.cc_owner_usernm,cc_budget_breakup_tmp.cost_center_id,cc_budget_breakup_tmp.created_by_usernm,cc_budget_breakup_tmp.created_dttm,cc_budget_breakup_tmp.fin_accnt_desc,cc_budget_breakup_tmp.fin_accnt_nm,cc_budget_breakup_tmp.fin_accnt_obsolete_flg,cc_budget_breakup_tmp.gen_ledger_cd,cc_budget_breakup_tmp.last_modified_dttm,cc_budget_breakup_tmp.last_modified_usernm,cc_budget_breakup_tmp.load_dttm,cc_budget_breakup_tmp.planning_id,cc_budget_breakup_tmp.planning_nm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cc_budget_breakup_tmp           , cc_budget_breakup, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cc_budget_breakup_tmp           ;
    QUIT;
    %put ######## Staging table: cc_budget_breakup_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cc_budget_breakup;
      DROP TABLE work.cc_budget_breakup;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cc_budget_breakup;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cc_budget_breakup_ccbdgt) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cc_budget_breakup_ccbdgt_tmp    ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cc_budget_breakup_ccbdgt_tmp    ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cc_budget_breakup_ccbdgt, table_keys=%str(cost_center_id,fp_id,planning_id), out_table=work.cc_budget_breakup_ccbdgt);
 data &tmplib..cc_budget_breakup_ccbdgt_tmp    ;
     set work.cc_budget_breakup_ccbdgt;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if cost_center_id='' then cost_center_id='-'; if fp_id='' then fp_id='-'; if planning_id='' then planning_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cc_budget_breakup_ccbdgt_tmp    , cc_budget_breakup_ccbdgt);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cc_budget_breakup_ccbdgt using &tmpdbschema..cc_budget_breakup_ccbdgt_tmp    
         ON (cc_budget_breakup_ccbdgt.cost_center_id=cc_budget_breakup_ccbdgt_tmp.cost_center_id and cc_budget_breakup_ccbdgt.fp_id=cc_budget_breakup_ccbdgt_tmp.fp_id and cc_budget_breakup_ccbdgt.planning_id=cc_budget_breakup_ccbdgt_tmp.planning_id)
        WHEN MATCHED THEN  
        UPDATE SET cc_bdgt_amt = cc_budget_breakup_ccbdgt_tmp.cc_bdgt_amt , cc_bdgt_budget_amt = cc_budget_breakup_ccbdgt_tmp.cc_bdgt_budget_amt , cc_bdgt_budget_desc = cc_budget_breakup_ccbdgt_tmp.cc_bdgt_budget_desc , cc_bdgt_cmtmnt_invoice_amt = cc_budget_breakup_ccbdgt_tmp.cc_bdgt_cmtmnt_invoice_amt , cc_bdgt_cmtmnt_invoice_cnt = cc_budget_breakup_ccbdgt_tmp.cc_bdgt_cmtmnt_invoice_cnt , cc_bdgt_cmtmnt_outstanding_amt = cc_budget_breakup_ccbdgt_tmp.cc_bdgt_cmtmnt_outstanding_amt , cc_bdgt_cmtmnt_overspent_amt = cc_budget_breakup_ccbdgt_tmp.cc_bdgt_cmtmnt_overspent_amt , cc_bdgt_committed_amt = cc_budget_breakup_ccbdgt_tmp.cc_bdgt_committed_amt , cc_bdgt_direct_invoice_amt = cc_budget_breakup_ccbdgt_tmp.cc_bdgt_direct_invoice_amt , cc_bdgt_invoiced_amt = cc_budget_breakup_ccbdgt_tmp.cc_bdgt_invoiced_amt , cc_budget_distribution = cc_budget_breakup_ccbdgt_tmp.cc_budget_distribution , cc_desc = cc_budget_breakup_ccbdgt_tmp.cc_desc , cc_level_expense = cc_budget_breakup_ccbdgt_tmp.cc_level_expense , cc_lvl_distribution = cc_budget_breakup_ccbdgt_tmp.cc_lvl_distribution , cc_nm = cc_budget_breakup_ccbdgt_tmp.cc_nm , cc_number = cc_budget_breakup_ccbdgt_tmp.cc_number , cc_obsolete_flg = cc_budget_breakup_ccbdgt_tmp.cc_obsolete_flg , cc_owner_usernm = cc_budget_breakup_ccbdgt_tmp.cc_owner_usernm , cc_rldup_child_bdgt = cc_budget_breakup_ccbdgt_tmp.cc_rldup_child_bdgt , cc_rldup_total_expense = cc_budget_breakup_ccbdgt_tmp.cc_rldup_total_expense , created_by_usernm = cc_budget_breakup_ccbdgt_tmp.created_by_usernm , created_dttm = cc_budget_breakup_ccbdgt_tmp.created_dttm , fin_accnt_desc = cc_budget_breakup_ccbdgt_tmp.fin_accnt_desc , fin_accnt_nm = cc_budget_breakup_ccbdgt_tmp.fin_accnt_nm , fin_accnt_obsolete_flg = cc_budget_breakup_ccbdgt_tmp.fin_accnt_obsolete_flg , fp_cls_ver = cc_budget_breakup_ccbdgt_tmp.fp_cls_ver , fp_desc = cc_budget_breakup_ccbdgt_tmp.fp_desc , fp_end_dt = cc_budget_breakup_ccbdgt_tmp.fp_end_dt , fp_nm = cc_budget_breakup_ccbdgt_tmp.fp_nm , fp_obsolete_flg = cc_budget_breakup_ccbdgt_tmp.fp_obsolete_flg , fp_start_dt = cc_budget_breakup_ccbdgt_tmp.fp_start_dt , gen_ledger_cd = cc_budget_breakup_ccbdgt_tmp.gen_ledger_cd , last_modified_dttm = cc_budget_breakup_ccbdgt_tmp.last_modified_dttm , last_modified_usernm = cc_budget_breakup_ccbdgt_tmp.last_modified_usernm , load_dttm = cc_budget_breakup_ccbdgt_tmp.load_dttm , planning_nm = cc_budget_breakup_ccbdgt_tmp.planning_nm
        WHEN NOT MATCHED THEN INSERT ( 
        cc_bdgt_amt,cc_bdgt_budget_amt,cc_bdgt_budget_desc,cc_bdgt_cmtmnt_invoice_amt,cc_bdgt_cmtmnt_invoice_cnt,cc_bdgt_cmtmnt_outstanding_amt,cc_bdgt_cmtmnt_overspent_amt,cc_bdgt_committed_amt,cc_bdgt_direct_invoice_amt,cc_bdgt_invoiced_amt,cc_budget_distribution,cc_desc,cc_level_expense,cc_lvl_distribution,cc_nm,cc_number,cc_obsolete_flg,cc_owner_usernm,cc_rldup_child_bdgt,cc_rldup_total_expense,cost_center_id,created_by_usernm,created_dttm,fin_accnt_desc,fin_accnt_nm,fin_accnt_obsolete_flg,fp_cls_ver,fp_desc,fp_end_dt,fp_id,fp_nm,fp_obsolete_flg,fp_start_dt,gen_ledger_cd,last_modified_dttm,last_modified_usernm,load_dttm,planning_id,planning_nm
         ) values ( 
        cc_budget_breakup_ccbdgt_tmp.cc_bdgt_amt,cc_budget_breakup_ccbdgt_tmp.cc_bdgt_budget_amt,cc_budget_breakup_ccbdgt_tmp.cc_bdgt_budget_desc,cc_budget_breakup_ccbdgt_tmp.cc_bdgt_cmtmnt_invoice_amt,cc_budget_breakup_ccbdgt_tmp.cc_bdgt_cmtmnt_invoice_cnt,cc_budget_breakup_ccbdgt_tmp.cc_bdgt_cmtmnt_outstanding_amt,cc_budget_breakup_ccbdgt_tmp.cc_bdgt_cmtmnt_overspent_amt,cc_budget_breakup_ccbdgt_tmp.cc_bdgt_committed_amt,cc_budget_breakup_ccbdgt_tmp.cc_bdgt_direct_invoice_amt,cc_budget_breakup_ccbdgt_tmp.cc_bdgt_invoiced_amt,cc_budget_breakup_ccbdgt_tmp.cc_budget_distribution,cc_budget_breakup_ccbdgt_tmp.cc_desc,cc_budget_breakup_ccbdgt_tmp.cc_level_expense,cc_budget_breakup_ccbdgt_tmp.cc_lvl_distribution,cc_budget_breakup_ccbdgt_tmp.cc_nm,cc_budget_breakup_ccbdgt_tmp.cc_number,cc_budget_breakup_ccbdgt_tmp.cc_obsolete_flg,cc_budget_breakup_ccbdgt_tmp.cc_owner_usernm,cc_budget_breakup_ccbdgt_tmp.cc_rldup_child_bdgt,cc_budget_breakup_ccbdgt_tmp.cc_rldup_total_expense,cc_budget_breakup_ccbdgt_tmp.cost_center_id,cc_budget_breakup_ccbdgt_tmp.created_by_usernm,cc_budget_breakup_ccbdgt_tmp.created_dttm,cc_budget_breakup_ccbdgt_tmp.fin_accnt_desc,cc_budget_breakup_ccbdgt_tmp.fin_accnt_nm,cc_budget_breakup_ccbdgt_tmp.fin_accnt_obsolete_flg,cc_budget_breakup_ccbdgt_tmp.fp_cls_ver,cc_budget_breakup_ccbdgt_tmp.fp_desc,cc_budget_breakup_ccbdgt_tmp.fp_end_dt,cc_budget_breakup_ccbdgt_tmp.fp_id,cc_budget_breakup_ccbdgt_tmp.fp_nm,cc_budget_breakup_ccbdgt_tmp.fp_obsolete_flg,cc_budget_breakup_ccbdgt_tmp.fp_start_dt,cc_budget_breakup_ccbdgt_tmp.gen_ledger_cd,cc_budget_breakup_ccbdgt_tmp.last_modified_dttm,cc_budget_breakup_ccbdgt_tmp.last_modified_usernm,cc_budget_breakup_ccbdgt_tmp.load_dttm,cc_budget_breakup_ccbdgt_tmp.planning_id,cc_budget_breakup_ccbdgt_tmp.planning_nm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cc_budget_breakup_ccbdgt_tmp    , cc_budget_breakup_ccbdgt, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cc_budget_breakup_ccbdgt_tmp    ;
    QUIT;
    %put ######## Staging table: cc_budget_breakup_ccbdgt_tmp     Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cc_budget_breakup_ccbdgt;
      DROP TABLE work.cc_budget_breakup_ccbdgt;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cc_budget_breakup_ccbdgt;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_activity_custom_attr) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_activity_custom_attr_tmp    ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_activity_custom_attr_tmp    ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_activity_custom_attr, table_keys=%str(activity_version_id,attribute_data_type_cd,attribute_nm,attribute_val), out_table=work.cdm_activity_custom_attr);
 data &tmplib..cdm_activity_custom_attr_tmp    ;
     set work.cdm_activity_custom_attr;
  if attribute_dttm_val ne . then attribute_dttm_val = tzoneu2s(attribute_dttm_val,&timeZone_Value.);if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  Hashed_pk_col = put(sha256(catx('|',activity_version_id,attribute_data_type_cd,attribute_nm,attribute_val)), $hex64.);
  if activity_version_id='' then activity_version_id='-'; if attribute_data_type_cd='' then attribute_data_type_cd='-'; if attribute_nm='' then attribute_nm='-'; if attribute_val='' then attribute_val='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_activity_custom_attr_tmp    , cdm_activity_custom_attr);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_activity_custom_attr using &tmpdbschema..cdm_activity_custom_attr_tmp    
         ON (cdm_activity_custom_attr.Hashed_pk_col = cdm_activity_custom_attr_tmp.Hashed_pk_col)
        WHEN MATCHED THEN  
        UPDATE SET activity_id = cdm_activity_custom_attr_tmp.activity_id , attribute_character_val = cdm_activity_custom_attr_tmp.attribute_character_val , attribute_dttm_val = cdm_activity_custom_attr_tmp.attribute_dttm_val , attribute_numeric_val = cdm_activity_custom_attr_tmp.attribute_numeric_val , updated_by_nm = cdm_activity_custom_attr_tmp.updated_by_nm , updated_dttm = cdm_activity_custom_attr_tmp.updated_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        activity_id,activity_version_id,attribute_character_val,attribute_data_type_cd,attribute_dttm_val,attribute_nm,attribute_numeric_val,attribute_val,updated_by_nm,updated_dttm
        ,Hashed_pk_col ) VALUES ( 
        cdm_activity_custom_attr_tmp.activity_id,cdm_activity_custom_attr_tmp.activity_version_id,cdm_activity_custom_attr_tmp.attribute_character_val,cdm_activity_custom_attr_tmp.attribute_data_type_cd,cdm_activity_custom_attr_tmp.attribute_dttm_val,cdm_activity_custom_attr_tmp.attribute_nm,cdm_activity_custom_attr_tmp.attribute_numeric_val,cdm_activity_custom_attr_tmp.attribute_val,cdm_activity_custom_attr_tmp.updated_by_nm,cdm_activity_custom_attr_tmp.updated_dttm,cdm_activity_custom_attr_tmp.Hashed_pk_col
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_activity_custom_attr_tmp    , cdm_activity_custom_attr, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_activity_custom_attr_tmp    ;
    QUIT;
    %put ######## Staging table: cdm_activity_custom_attr_tmp     Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_activity_custom_attr;
      DROP TABLE work.cdm_activity_custom_attr;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_activity_custom_attr;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_activity_detail) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_activity_detail_tmp         ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_activity_detail_tmp         ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_activity_detail, table_keys=%str(activity_version_id), out_table=work.cdm_activity_detail);
 data &tmplib..cdm_activity_detail_tmp         ;
     set work.cdm_activity_detail;
  if last_published_dttm ne . then last_published_dttm = tzoneu2s(last_published_dttm,&timeZone_Value.);if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if activity_version_id='' then activity_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_activity_detail_tmp         , cdm_activity_detail);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_activity_detail using &tmpdbschema..cdm_activity_detail_tmp         
         ON (cdm_activity_detail.activity_version_id=cdm_activity_detail_tmp.activity_version_id)
        WHEN MATCHED THEN  
        UPDATE SET activity_category_nm = cdm_activity_detail_tmp.activity_category_nm , activity_cd = cdm_activity_detail_tmp.activity_cd , activity_desc = cdm_activity_detail_tmp.activity_desc , activity_id = cdm_activity_detail_tmp.activity_id , activity_nm = cdm_activity_detail_tmp.activity_nm , last_published_dttm = cdm_activity_detail_tmp.last_published_dttm , source_system_cd = cdm_activity_detail_tmp.source_system_cd , status_cd = cdm_activity_detail_tmp.status_cd , updated_by_nm = cdm_activity_detail_tmp.updated_by_nm , updated_dttm = cdm_activity_detail_tmp.updated_dttm , valid_from_dttm = cdm_activity_detail_tmp.valid_from_dttm , valid_to_dttm = cdm_activity_detail_tmp.valid_to_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        activity_category_nm,activity_cd,activity_desc,activity_id,activity_nm,activity_version_id,last_published_dttm,source_system_cd,status_cd,updated_by_nm,updated_dttm,valid_from_dttm,valid_to_dttm
         ) values ( 
        cdm_activity_detail_tmp.activity_category_nm,cdm_activity_detail_tmp.activity_cd,cdm_activity_detail_tmp.activity_desc,cdm_activity_detail_tmp.activity_id,cdm_activity_detail_tmp.activity_nm,cdm_activity_detail_tmp.activity_version_id,cdm_activity_detail_tmp.last_published_dttm,cdm_activity_detail_tmp.source_system_cd,cdm_activity_detail_tmp.status_cd,cdm_activity_detail_tmp.updated_by_nm,cdm_activity_detail_tmp.updated_dttm,cdm_activity_detail_tmp.valid_from_dttm,cdm_activity_detail_tmp.valid_to_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_activity_detail_tmp         , cdm_activity_detail, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_activity_detail_tmp         ;
    QUIT;
    %put ######## Staging table: cdm_activity_detail_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_activity_detail;
      DROP TABLE work.cdm_activity_detail;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_activity_detail;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_activity_x_task) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_activity_x_task_tmp         ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_activity_x_task_tmp         ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_activity_x_task, table_keys=%str(activity_version_id,task_version_id), out_table=work.cdm_activity_x_task);
 data &tmplib..cdm_activity_x_task_tmp         ;
     set work.cdm_activity_x_task;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if activity_version_id='' then activity_version_id='-'; if task_version_id='' then task_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_activity_x_task_tmp         , cdm_activity_x_task);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_activity_x_task using &tmpdbschema..cdm_activity_x_task_tmp         
         ON (cdm_activity_x_task.activity_version_id=cdm_activity_x_task_tmp.activity_version_id and cdm_activity_x_task.task_version_id=cdm_activity_x_task_tmp.task_version_id)
        WHEN MATCHED THEN  
        UPDATE SET activity_id = cdm_activity_x_task_tmp.activity_id , task_id = cdm_activity_x_task_tmp.task_id , updated_by_nm = cdm_activity_x_task_tmp.updated_by_nm , updated_dttm = cdm_activity_x_task_tmp.updated_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        activity_id,activity_version_id,task_id,task_version_id,updated_by_nm,updated_dttm
         ) values ( 
        cdm_activity_x_task_tmp.activity_id,cdm_activity_x_task_tmp.activity_version_id,cdm_activity_x_task_tmp.task_id,cdm_activity_x_task_tmp.task_version_id,cdm_activity_x_task_tmp.updated_by_nm,cdm_activity_x_task_tmp.updated_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_activity_x_task_tmp         , cdm_activity_x_task, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_activity_x_task_tmp         ;
    QUIT;
    %put ######## Staging table: cdm_activity_x_task_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_activity_x_task;
      DROP TABLE work.cdm_activity_x_task;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_activity_x_task;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_audience_detail) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_audience_detail_tmp         ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_audience_detail_tmp         ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_audience_detail, table_keys=%str(audience_id), out_table=work.cdm_audience_detail);
 data &tmplib..cdm_audience_detail_tmp         ;
     set work.cdm_audience_detail;
  if create_dttm ne . then create_dttm = tzoneu2s(create_dttm,&timeZone_Value.);if delete_dttm ne . then delete_dttm = tzoneu2s(delete_dttm,&timeZone_Value.);if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if audience_id='' then audience_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_audience_detail_tmp         , cdm_audience_detail);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_audience_detail using &tmpdbschema..cdm_audience_detail_tmp         
         ON (cdm_audience_detail.audience_id=cdm_audience_detail_tmp.audience_id)
        WHEN MATCHED THEN  
        UPDATE SET audience_data_source_nm = cdm_audience_detail_tmp.audience_data_source_nm , audience_desc = cdm_audience_detail_tmp.audience_desc , audience_nm = cdm_audience_detail_tmp.audience_nm , audience_schedule_flg = cdm_audience_detail_tmp.audience_schedule_flg , audience_source_nm = cdm_audience_detail_tmp.audience_source_nm , create_dttm = cdm_audience_detail_tmp.create_dttm , created_user_nm = cdm_audience_detail_tmp.created_user_nm , delete_dttm = cdm_audience_detail_tmp.delete_dttm , updated_dttm = cdm_audience_detail_tmp.updated_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        audience_data_source_nm,audience_desc,audience_id,audience_nm,audience_schedule_flg,audience_source_nm,create_dttm,created_user_nm,delete_dttm,updated_dttm
         ) values ( 
        cdm_audience_detail_tmp.audience_data_source_nm,cdm_audience_detail_tmp.audience_desc,cdm_audience_detail_tmp.audience_id,cdm_audience_detail_tmp.audience_nm,cdm_audience_detail_tmp.audience_schedule_flg,cdm_audience_detail_tmp.audience_source_nm,cdm_audience_detail_tmp.create_dttm,cdm_audience_detail_tmp.created_user_nm,cdm_audience_detail_tmp.delete_dttm,cdm_audience_detail_tmp.updated_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_audience_detail_tmp         , cdm_audience_detail, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_audience_detail_tmp         ;
    QUIT;
    %put ######## Staging table: cdm_audience_detail_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_audience_detail;
      DROP TABLE work.cdm_audience_detail;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_audience_detail;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_audience_occur_detail) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_audience_occur_detail_tmp   ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_audience_occur_detail_tmp   ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_audience_occur_detail, table_keys=%str(audience_occur_id), out_table=work.cdm_audience_occur_detail);
 data &tmplib..cdm_audience_occur_detail_tmp   ;
     set work.cdm_audience_occur_detail;
  if end_dttm ne . then end_dttm = tzoneu2s(end_dttm,&timeZone_Value.);if start_dttm ne . then start_dttm = tzoneu2s(start_dttm,&timeZone_Value.);if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if audience_occur_id='' then audience_occur_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_audience_occur_detail_tmp   , cdm_audience_occur_detail);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_audience_occur_detail using &tmpdbschema..cdm_audience_occur_detail_tmp   
         ON (cdm_audience_occur_detail.audience_occur_id=cdm_audience_occur_detail_tmp.audience_occur_id)
        WHEN MATCHED THEN  
        UPDATE SET audience_id = cdm_audience_occur_detail_tmp.audience_id , audience_size_cnt = cdm_audience_occur_detail_tmp.audience_size_cnt , end_dttm = cdm_audience_occur_detail_tmp.end_dttm , execution_status_cd = cdm_audience_occur_detail_tmp.execution_status_cd , occurrence_type_nm = cdm_audience_occur_detail_tmp.occurrence_type_nm , start_dttm = cdm_audience_occur_detail_tmp.start_dttm , started_by_nm = cdm_audience_occur_detail_tmp.started_by_nm , updated_dttm = cdm_audience_occur_detail_tmp.updated_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        audience_id,audience_occur_id,audience_size_cnt,end_dttm,execution_status_cd,occurrence_type_nm,start_dttm,started_by_nm,updated_dttm
         ) values ( 
        cdm_audience_occur_detail_tmp.audience_id,cdm_audience_occur_detail_tmp.audience_occur_id,cdm_audience_occur_detail_tmp.audience_size_cnt,cdm_audience_occur_detail_tmp.end_dttm,cdm_audience_occur_detail_tmp.execution_status_cd,cdm_audience_occur_detail_tmp.occurrence_type_nm,cdm_audience_occur_detail_tmp.start_dttm,cdm_audience_occur_detail_tmp.started_by_nm,cdm_audience_occur_detail_tmp.updated_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_audience_occur_detail_tmp   , cdm_audience_occur_detail, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_audience_occur_detail_tmp   ;
    QUIT;
    %put ######## Staging table: cdm_audience_occur_detail_tmp    Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_audience_occur_detail;
      DROP TABLE work.cdm_audience_occur_detail;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_audience_occur_detail;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_audience_x_segment) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_audience_x_segment_tmp      ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_audience_x_segment_tmp      ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_audience_x_segment, table_keys=%str(audience_id), out_table=work.cdm_audience_x_segment);
 data &tmplib..cdm_audience_x_segment_tmp      ;
     set work.cdm_audience_x_segment;
  if audience_id='' then audience_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_audience_x_segment_tmp      , cdm_audience_x_segment);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_audience_x_segment using &tmpdbschema..cdm_audience_x_segment_tmp      
         ON (cdm_audience_x_segment.audience_id=cdm_audience_x_segment_tmp.audience_id)
        WHEN MATCHED THEN  
        UPDATE SET segment_id = cdm_audience_x_segment_tmp.segment_id
        WHEN NOT MATCHED THEN INSERT ( 
        audience_id,segment_id
         ) values ( 
        cdm_audience_x_segment_tmp.audience_id,cdm_audience_x_segment_tmp.segment_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_audience_x_segment_tmp      , cdm_audience_x_segment, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_audience_x_segment_tmp      ;
    QUIT;
    %put ######## Staging table: cdm_audience_x_segment_tmp       Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_audience_x_segment;
      DROP TABLE work.cdm_audience_x_segment;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_audience_x_segment;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_business_context) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_business_context_tmp        ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_business_context_tmp        ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_business_context, table_keys=%str(business_context_id), out_table=work.cdm_business_context);
 data &tmplib..cdm_business_context_tmp        ;
     set work.cdm_business_context;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if business_context_id='' then business_context_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_business_context_tmp        , cdm_business_context);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_business_context using &tmpdbschema..cdm_business_context_tmp        
         ON (cdm_business_context.business_context_id=cdm_business_context_tmp.business_context_id)
        WHEN MATCHED THEN  
        UPDATE SET business_context_nm = cdm_business_context_tmp.business_context_nm , business_context_type_cd = cdm_business_context_tmp.business_context_type_cd , source_system_cd = cdm_business_context_tmp.source_system_cd , updated_by_nm = cdm_business_context_tmp.updated_by_nm , updated_dttm = cdm_business_context_tmp.updated_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        business_context_id,business_context_nm,business_context_type_cd,source_system_cd,updated_by_nm,updated_dttm
         ) values ( 
        cdm_business_context_tmp.business_context_id,cdm_business_context_tmp.business_context_nm,cdm_business_context_tmp.business_context_type_cd,cdm_business_context_tmp.source_system_cd,cdm_business_context_tmp.updated_by_nm,cdm_business_context_tmp.updated_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_business_context_tmp        , cdm_business_context, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_business_context_tmp        ;
    QUIT;
    %put ######## Staging table: cdm_business_context_tmp         Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_business_context;
      DROP TABLE work.cdm_business_context;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_business_context;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_campaign_custom_attr) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_campaign_custom_attr_tmp    ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_campaign_custom_attr_tmp    ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_campaign_custom_attr, table_keys=%str(attribute_data_type_cd,attribute_nm,attribute_val,campaign_id,page_nm), out_table=work.cdm_campaign_custom_attr);
 data &tmplib..cdm_campaign_custom_attr_tmp    ;
     set work.cdm_campaign_custom_attr;
  if attribute_dttm_val ne . then attribute_dttm_val = tzoneu2s(attribute_dttm_val,&timeZone_Value.);if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  Hashed_pk_col = put(sha256(catx('|',attribute_data_type_cd,attribute_nm,attribute_val,campaign_id,page_nm)), $hex64.);
  if attribute_data_type_cd='' then attribute_data_type_cd='-'; if attribute_nm='' then attribute_nm='-'; if attribute_val='' then attribute_val='-'; if campaign_id='' then campaign_id='-'; if page_nm='' then page_nm='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_campaign_custom_attr_tmp    , cdm_campaign_custom_attr);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_campaign_custom_attr using &tmpdbschema..cdm_campaign_custom_attr_tmp    
         ON (cdm_campaign_custom_attr.Hashed_pk_col = cdm_campaign_custom_attr_tmp.Hashed_pk_col)
        WHEN MATCHED THEN  
        UPDATE SET attribute_character_val = cdm_campaign_custom_attr_tmp.attribute_character_val , attribute_dttm_val = cdm_campaign_custom_attr_tmp.attribute_dttm_val , attribute_numeric_val = cdm_campaign_custom_attr_tmp.attribute_numeric_val , extension_attribute_nm = cdm_campaign_custom_attr_tmp.extension_attribute_nm , updated_by_nm = cdm_campaign_custom_attr_tmp.updated_by_nm , updated_dttm = cdm_campaign_custom_attr_tmp.updated_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        attribute_character_val,attribute_data_type_cd,attribute_dttm_val,attribute_nm,attribute_numeric_val,attribute_val,campaign_id,extension_attribute_nm,page_nm,updated_by_nm,updated_dttm
        ,Hashed_pk_col ) VALUES ( 
        cdm_campaign_custom_attr_tmp.attribute_character_val,cdm_campaign_custom_attr_tmp.attribute_data_type_cd,cdm_campaign_custom_attr_tmp.attribute_dttm_val,cdm_campaign_custom_attr_tmp.attribute_nm,cdm_campaign_custom_attr_tmp.attribute_numeric_val,cdm_campaign_custom_attr_tmp.attribute_val,cdm_campaign_custom_attr_tmp.campaign_id,cdm_campaign_custom_attr_tmp.extension_attribute_nm,cdm_campaign_custom_attr_tmp.page_nm,cdm_campaign_custom_attr_tmp.updated_by_nm,cdm_campaign_custom_attr_tmp.updated_dttm,cdm_campaign_custom_attr_tmp.Hashed_pk_col
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_campaign_custom_attr_tmp    , cdm_campaign_custom_attr, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_campaign_custom_attr_tmp    ;
    QUIT;
    %put ######## Staging table: cdm_campaign_custom_attr_tmp     Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_campaign_custom_attr;
      DROP TABLE work.cdm_campaign_custom_attr;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_campaign_custom_attr;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_campaign_detail) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_campaign_detail_tmp         ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_campaign_detail_tmp         ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_campaign_detail, table_keys=%str(campaign_id), out_table=work.cdm_campaign_detail);
 data &tmplib..cdm_campaign_detail_tmp         ;
     set work.cdm_campaign_detail;
  if approval_dttm ne . then approval_dttm = tzoneu2s(approval_dttm,&timeZone_Value.);if end_dttm ne . then end_dttm = tzoneu2s(end_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if run_dttm ne . then run_dttm = tzoneu2s(run_dttm,&timeZone_Value.);if start_dttm ne . then start_dttm = tzoneu2s(start_dttm,&timeZone_Value.);if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if campaign_id='' then campaign_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_campaign_detail_tmp         , cdm_campaign_detail);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_campaign_detail using &tmpdbschema..cdm_campaign_detail_tmp         
         ON (cdm_campaign_detail.campaign_id=cdm_campaign_detail_tmp.campaign_id)
        WHEN MATCHED THEN  
        UPDATE SET approval_dttm = cdm_campaign_detail_tmp.approval_dttm , approval_given_by_nm = cdm_campaign_detail_tmp.approval_given_by_nm , campaign_cd = cdm_campaign_detail_tmp.campaign_cd , campaign_desc = cdm_campaign_detail_tmp.campaign_desc , campaign_folder_txt = cdm_campaign_detail_tmp.campaign_folder_txt , campaign_group_sk = cdm_campaign_detail_tmp.campaign_group_sk , campaign_nm = cdm_campaign_detail_tmp.campaign_nm , campaign_owner_nm = cdm_campaign_detail_tmp.campaign_owner_nm , campaign_status_cd = cdm_campaign_detail_tmp.campaign_status_cd , campaign_type_cd = cdm_campaign_detail_tmp.campaign_type_cd , campaign_version_no = cdm_campaign_detail_tmp.campaign_version_no , current_version_flg = cdm_campaign_detail_tmp.current_version_flg , deleted_flg = cdm_campaign_detail_tmp.deleted_flg , deployment_version_no = cdm_campaign_detail_tmp.deployment_version_no , end_dttm = cdm_campaign_detail_tmp.end_dttm , last_modified_by_user_nm = cdm_campaign_detail_tmp.last_modified_by_user_nm , last_modified_dttm = cdm_campaign_detail_tmp.last_modified_dttm , max_budget_amt = cdm_campaign_detail_tmp.max_budget_amt , max_budget_offer_amt = cdm_campaign_detail_tmp.max_budget_offer_amt , min_budget_amt = cdm_campaign_detail_tmp.min_budget_amt , min_budget_offer_amt = cdm_campaign_detail_tmp.min_budget_offer_amt , run_dttm = cdm_campaign_detail_tmp.run_dttm , source_system_cd = cdm_campaign_detail_tmp.source_system_cd , start_dttm = cdm_campaign_detail_tmp.start_dttm , updated_by_nm = cdm_campaign_detail_tmp.updated_by_nm , updated_dttm = cdm_campaign_detail_tmp.updated_dttm , valid_from_dttm = cdm_campaign_detail_tmp.valid_from_dttm , valid_to_dttm = cdm_campaign_detail_tmp.valid_to_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        approval_dttm,approval_given_by_nm,campaign_cd,campaign_desc,campaign_folder_txt,campaign_group_sk,campaign_id,campaign_nm,campaign_owner_nm,campaign_status_cd,campaign_type_cd,campaign_version_no,current_version_flg,deleted_flg,deployment_version_no,end_dttm,last_modified_by_user_nm,last_modified_dttm,max_budget_amt,max_budget_offer_amt,min_budget_amt,min_budget_offer_amt,run_dttm,source_system_cd,start_dttm,updated_by_nm,updated_dttm,valid_from_dttm,valid_to_dttm
         ) values ( 
        cdm_campaign_detail_tmp.approval_dttm,cdm_campaign_detail_tmp.approval_given_by_nm,cdm_campaign_detail_tmp.campaign_cd,cdm_campaign_detail_tmp.campaign_desc,cdm_campaign_detail_tmp.campaign_folder_txt,cdm_campaign_detail_tmp.campaign_group_sk,cdm_campaign_detail_tmp.campaign_id,cdm_campaign_detail_tmp.campaign_nm,cdm_campaign_detail_tmp.campaign_owner_nm,cdm_campaign_detail_tmp.campaign_status_cd,cdm_campaign_detail_tmp.campaign_type_cd,cdm_campaign_detail_tmp.campaign_version_no,cdm_campaign_detail_tmp.current_version_flg,cdm_campaign_detail_tmp.deleted_flg,cdm_campaign_detail_tmp.deployment_version_no,cdm_campaign_detail_tmp.end_dttm,cdm_campaign_detail_tmp.last_modified_by_user_nm,cdm_campaign_detail_tmp.last_modified_dttm,cdm_campaign_detail_tmp.max_budget_amt,cdm_campaign_detail_tmp.max_budget_offer_amt,cdm_campaign_detail_tmp.min_budget_amt,cdm_campaign_detail_tmp.min_budget_offer_amt,cdm_campaign_detail_tmp.run_dttm,cdm_campaign_detail_tmp.source_system_cd,cdm_campaign_detail_tmp.start_dttm,cdm_campaign_detail_tmp.updated_by_nm,cdm_campaign_detail_tmp.updated_dttm,cdm_campaign_detail_tmp.valid_from_dttm,cdm_campaign_detail_tmp.valid_to_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_campaign_detail_tmp         , cdm_campaign_detail, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_campaign_detail_tmp         ;
    QUIT;
    %put ######## Staging table: cdm_campaign_detail_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_campaign_detail;
      DROP TABLE work.cdm_campaign_detail;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_campaign_detail;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_contact_channel) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_contact_channel_tmp         ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_contact_channel_tmp         ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_contact_channel, table_keys=%str(contact_channel_cd), out_table=work.cdm_contact_channel);
 data &tmplib..cdm_contact_channel_tmp         ;
     set work.cdm_contact_channel;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if contact_channel_cd='' then contact_channel_cd='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_contact_channel_tmp         , cdm_contact_channel);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_contact_channel using &tmpdbschema..cdm_contact_channel_tmp         
         ON (cdm_contact_channel.contact_channel_cd=cdm_contact_channel_tmp.contact_channel_cd)
        WHEN MATCHED THEN  
        UPDATE SET contact_channel_nm = cdm_contact_channel_tmp.contact_channel_nm , updated_by_nm = cdm_contact_channel_tmp.updated_by_nm , updated_dttm = cdm_contact_channel_tmp.updated_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        contact_channel_cd,contact_channel_nm,updated_by_nm,updated_dttm
         ) values ( 
        cdm_contact_channel_tmp.contact_channel_cd,cdm_contact_channel_tmp.contact_channel_nm,cdm_contact_channel_tmp.updated_by_nm,cdm_contact_channel_tmp.updated_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_contact_channel_tmp         , cdm_contact_channel, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_contact_channel_tmp         ;
    QUIT;
    %put ######## Staging table: cdm_contact_channel_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_contact_channel;
      DROP TABLE work.cdm_contact_channel;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_contact_channel;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_contact_history) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_contact_history_tmp         ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_contact_history_tmp         ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_contact_history, table_keys=%str(contact_id), out_table=work.cdm_contact_history);
 data &tmplib..cdm_contact_history_tmp         ;
     set work.cdm_contact_history;
  if contact_dttm ne . then contact_dttm = tzoneu2s(contact_dttm,&timeZone_Value.);if contact_dttm_tz ne . then contact_dttm_tz = tzoneu2s(contact_dttm_tz,&timeZone_Value.);if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if contact_id='' then contact_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_contact_history_tmp         , cdm_contact_history);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_contact_history using &tmpdbschema..cdm_contact_history_tmp         
         ON (cdm_contact_history.contact_id=cdm_contact_history_tmp.contact_id)
        WHEN MATCHED THEN  
        UPDATE SET audience_id = cdm_contact_history_tmp.audience_id , audience_occur_id = cdm_contact_history_tmp.audience_occur_id , contact_dt = cdm_contact_history_tmp.contact_dt , contact_dttm = cdm_contact_history_tmp.contact_dttm , contact_dttm_tz = cdm_contact_history_tmp.contact_dttm_tz , contact_nm = cdm_contact_history_tmp.contact_nm , contact_status_cd = cdm_contact_history_tmp.contact_status_cd , context_type_nm = cdm_contact_history_tmp.context_type_nm , context_val = cdm_contact_history_tmp.context_val , control_group_flg = cdm_contact_history_tmp.control_group_flg , external_contact_info_1_id = cdm_contact_history_tmp.external_contact_info_1_id , external_contact_info_2_id = cdm_contact_history_tmp.external_contact_info_2_id , identity_id = cdm_contact_history_tmp.identity_id , optimization_backfill_flg = cdm_contact_history_tmp.optimization_backfill_flg , rtc_id = cdm_contact_history_tmp.rtc_id , source_system_cd = cdm_contact_history_tmp.source_system_cd , updated_by_nm = cdm_contact_history_tmp.updated_by_nm , updated_dttm = cdm_contact_history_tmp.updated_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        audience_id,audience_occur_id,contact_dt,contact_dttm,contact_dttm_tz,contact_id,contact_nm,contact_status_cd,context_type_nm,context_val,control_group_flg,external_contact_info_1_id,external_contact_info_2_id,identity_id,optimization_backfill_flg,rtc_id,source_system_cd,updated_by_nm,updated_dttm
         ) values ( 
        cdm_contact_history_tmp.audience_id,cdm_contact_history_tmp.audience_occur_id,cdm_contact_history_tmp.contact_dt,cdm_contact_history_tmp.contact_dttm,cdm_contact_history_tmp.contact_dttm_tz,cdm_contact_history_tmp.contact_id,cdm_contact_history_tmp.contact_nm,cdm_contact_history_tmp.contact_status_cd,cdm_contact_history_tmp.context_type_nm,cdm_contact_history_tmp.context_val,cdm_contact_history_tmp.control_group_flg,cdm_contact_history_tmp.external_contact_info_1_id,cdm_contact_history_tmp.external_contact_info_2_id,cdm_contact_history_tmp.identity_id,cdm_contact_history_tmp.optimization_backfill_flg,cdm_contact_history_tmp.rtc_id,cdm_contact_history_tmp.source_system_cd,cdm_contact_history_tmp.updated_by_nm,cdm_contact_history_tmp.updated_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_contact_history_tmp         , cdm_contact_history, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_contact_history_tmp         ;
    QUIT;
    %put ######## Staging table: cdm_contact_history_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_contact_history;
      DROP TABLE work.cdm_contact_history;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_contact_history;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_contact_status) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_contact_status_tmp          ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_contact_status_tmp          ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_contact_status, table_keys=%str(contact_status_cd), out_table=work.cdm_contact_status);
 data &tmplib..cdm_contact_status_tmp          ;
     set work.cdm_contact_status;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if contact_status_cd='' then contact_status_cd='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_contact_status_tmp          , cdm_contact_status);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_contact_status using &tmpdbschema..cdm_contact_status_tmp          
         ON (cdm_contact_status.contact_status_cd=cdm_contact_status_tmp.contact_status_cd)
        WHEN MATCHED THEN  
        UPDATE SET contact_status_desc = cdm_contact_status_tmp.contact_status_desc , updated_by_nm = cdm_contact_status_tmp.updated_by_nm , updated_dttm = cdm_contact_status_tmp.updated_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        contact_status_cd,contact_status_desc,updated_by_nm,updated_dttm
         ) values ( 
        cdm_contact_status_tmp.contact_status_cd,cdm_contact_status_tmp.contact_status_desc,cdm_contact_status_tmp.updated_by_nm,cdm_contact_status_tmp.updated_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_contact_status_tmp          , cdm_contact_status, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_contact_status_tmp          ;
    QUIT;
    %put ######## Staging table: cdm_contact_status_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_contact_status;
      DROP TABLE work.cdm_contact_status;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_contact_status;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_content_custom_attr) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_content_custom_attr_tmp     ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_content_custom_attr_tmp     ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_content_custom_attr, table_keys=%str(attribute_character_val,attribute_data_type_cd,attribute_dttm_val,attribute_nm,attribute_numeric_val,attribute_val,content_version_id), out_table=work.cdm_content_custom_attr);
 data &tmplib..cdm_content_custom_attr_tmp     ;
     set work.cdm_content_custom_attr;
  if attribute_dttm_val ne . then attribute_dttm_val = tzoneu2s(attribute_dttm_val,&timeZone_Value.);if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  Hashed_pk_col = put(sha256(catx('|',attribute_character_val,attribute_data_type_cd,attribute_dttm_val,attribute_nm,attribute_numeric_val,attribute_val,content_version_id)), $hex64.);
  if attribute_character_val='' then attribute_character_val='-'; if attribute_data_type_cd='' then attribute_data_type_cd='-'; if attribute_nm='' then attribute_nm='-'; if attribute_val='' then attribute_val='-'; if content_version_id='' then content_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_content_custom_attr_tmp     , cdm_content_custom_attr);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_content_custom_attr using &tmpdbschema..cdm_content_custom_attr_tmp     
         ON (cdm_content_custom_attr.Hashed_pk_col = cdm_content_custom_attr_tmp.Hashed_pk_col)
        WHEN MATCHED THEN  
        UPDATE SET content_id = cdm_content_custom_attr_tmp.content_id , extension_attribute_nm = cdm_content_custom_attr_tmp.extension_attribute_nm , updated_by_nm = cdm_content_custom_attr_tmp.updated_by_nm , updated_dttm = cdm_content_custom_attr_tmp.updated_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        attribute_character_val,attribute_data_type_cd,attribute_dttm_val,attribute_nm,attribute_numeric_val,attribute_val,content_id,content_version_id,extension_attribute_nm,updated_by_nm,updated_dttm
        ,Hashed_pk_col ) VALUES ( 
        cdm_content_custom_attr_tmp.attribute_character_val,cdm_content_custom_attr_tmp.attribute_data_type_cd,cdm_content_custom_attr_tmp.attribute_dttm_val,cdm_content_custom_attr_tmp.attribute_nm,cdm_content_custom_attr_tmp.attribute_numeric_val,cdm_content_custom_attr_tmp.attribute_val,cdm_content_custom_attr_tmp.content_id,cdm_content_custom_attr_tmp.content_version_id,cdm_content_custom_attr_tmp.extension_attribute_nm,cdm_content_custom_attr_tmp.updated_by_nm,cdm_content_custom_attr_tmp.updated_dttm,cdm_content_custom_attr_tmp.Hashed_pk_col
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_content_custom_attr_tmp     , cdm_content_custom_attr, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_content_custom_attr_tmp     ;
    QUIT;
    %put ######## Staging table: cdm_content_custom_attr_tmp      Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_content_custom_attr;
      DROP TABLE work.cdm_content_custom_attr;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_content_custom_attr;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_content_detail) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_content_detail_tmp          ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_content_detail_tmp          ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_content_detail, table_keys=%str(content_version_id), out_table=work.cdm_content_detail);
 data &tmplib..cdm_content_detail_tmp          ;
     set work.cdm_content_detail;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if content_version_id='' then content_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_content_detail_tmp          , cdm_content_detail);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_content_detail using &tmpdbschema..cdm_content_detail_tmp          
         ON (cdm_content_detail.content_version_id=cdm_content_detail_tmp.content_version_id)
        WHEN MATCHED THEN  
        UPDATE SET active_flg = cdm_content_detail_tmp.active_flg , contact_content_category_nm = cdm_content_detail_tmp.contact_content_category_nm , contact_content_cd = cdm_content_detail_tmp.contact_content_cd , contact_content_class_nm = cdm_content_detail_tmp.contact_content_class_nm , contact_content_desc = cdm_content_detail_tmp.contact_content_desc , contact_content_nm = cdm_content_detail_tmp.contact_content_nm , contact_content_status_cd = cdm_content_detail_tmp.contact_content_status_cd , contact_content_type_nm = cdm_content_detail_tmp.contact_content_type_nm , content_id = cdm_content_detail_tmp.content_id , created_dt = cdm_content_detail_tmp.created_dt , created_user_nm = cdm_content_detail_tmp.created_user_nm , external_reference_txt = cdm_content_detail_tmp.external_reference_txt , external_reference_url_txt = cdm_content_detail_tmp.external_reference_url_txt , owner_nm = cdm_content_detail_tmp.owner_nm , source_system_cd = cdm_content_detail_tmp.source_system_cd , updated_by_nm = cdm_content_detail_tmp.updated_by_nm , updated_dttm = cdm_content_detail_tmp.updated_dttm , valid_from_dttm = cdm_content_detail_tmp.valid_from_dttm , valid_to_dttm = cdm_content_detail_tmp.valid_to_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        active_flg,contact_content_category_nm,contact_content_cd,contact_content_class_nm,contact_content_desc,contact_content_nm,contact_content_status_cd,contact_content_type_nm,content_id,content_version_id,created_dt,created_user_nm,external_reference_txt,external_reference_url_txt,owner_nm,source_system_cd,updated_by_nm,updated_dttm,valid_from_dttm,valid_to_dttm
         ) values ( 
        cdm_content_detail_tmp.active_flg,cdm_content_detail_tmp.contact_content_category_nm,cdm_content_detail_tmp.contact_content_cd,cdm_content_detail_tmp.contact_content_class_nm,cdm_content_detail_tmp.contact_content_desc,cdm_content_detail_tmp.contact_content_nm,cdm_content_detail_tmp.contact_content_status_cd,cdm_content_detail_tmp.contact_content_type_nm,cdm_content_detail_tmp.content_id,cdm_content_detail_tmp.content_version_id,cdm_content_detail_tmp.created_dt,cdm_content_detail_tmp.created_user_nm,cdm_content_detail_tmp.external_reference_txt,cdm_content_detail_tmp.external_reference_url_txt,cdm_content_detail_tmp.owner_nm,cdm_content_detail_tmp.source_system_cd,cdm_content_detail_tmp.updated_by_nm,cdm_content_detail_tmp.updated_dttm,cdm_content_detail_tmp.valid_from_dttm,cdm_content_detail_tmp.valid_to_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_content_detail_tmp          , cdm_content_detail, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_content_detail_tmp          ;
    QUIT;
    %put ######## Staging table: cdm_content_detail_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_content_detail;
      DROP TABLE work.cdm_content_detail;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_content_detail;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_dyn_content_custom_attr) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_dyn_content_custom_attr_tmp ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_dyn_content_custom_attr_tmp ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_dyn_content_custom_attr, table_keys=%str(attribute_data_type_cd,attribute_nm,attribute_val,content_hash_val,content_version_id), out_table=work.cdm_dyn_content_custom_attr);
 data &tmplib..cdm_dyn_content_custom_attr_tmp ;
     set work.cdm_dyn_content_custom_attr;
  if attribute_dttm_val ne . then attribute_dttm_val = tzoneu2s(attribute_dttm_val,&timeZone_Value.);if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  Hashed_pk_col = put(sha256(catx('|',attribute_data_type_cd,attribute_nm,attribute_val,content_hash_val,content_version_id)), $hex64.);
  if attribute_data_type_cd='' then attribute_data_type_cd='-'; if attribute_nm='' then attribute_nm='-'; if attribute_val='' then attribute_val='-'; if content_hash_val='' then content_hash_val='-'; if content_version_id='' then content_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_dyn_content_custom_attr_tmp , cdm_dyn_content_custom_attr);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_dyn_content_custom_attr using &tmpdbschema..cdm_dyn_content_custom_attr_tmp 
         ON (cdm_dyn_content_custom_attr.Hashed_pk_col = cdm_dyn_content_custom_attr_tmp.Hashed_pk_col)
        WHEN MATCHED THEN  
        UPDATE SET attribute_character_val = cdm_dyn_content_custom_attr_tmp.attribute_character_val , attribute_dttm_val = cdm_dyn_content_custom_attr_tmp.attribute_dttm_val , attribute_numeric_val = cdm_dyn_content_custom_attr_tmp.attribute_numeric_val , content_id = cdm_dyn_content_custom_attr_tmp.content_id , extension_attribute_nm = cdm_dyn_content_custom_attr_tmp.extension_attribute_nm , updated_by_nm = cdm_dyn_content_custom_attr_tmp.updated_by_nm , updated_dttm = cdm_dyn_content_custom_attr_tmp.updated_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        attribute_character_val,attribute_data_type_cd,attribute_dttm_val,attribute_nm,attribute_numeric_val,attribute_val,content_hash_val,content_id,content_version_id,extension_attribute_nm,updated_by_nm,updated_dttm
        ,Hashed_pk_col ) VALUES ( 
        cdm_dyn_content_custom_attr_tmp.attribute_character_val,cdm_dyn_content_custom_attr_tmp.attribute_data_type_cd,cdm_dyn_content_custom_attr_tmp.attribute_dttm_val,cdm_dyn_content_custom_attr_tmp.attribute_nm,cdm_dyn_content_custom_attr_tmp.attribute_numeric_val,cdm_dyn_content_custom_attr_tmp.attribute_val,cdm_dyn_content_custom_attr_tmp.content_hash_val,cdm_dyn_content_custom_attr_tmp.content_id,cdm_dyn_content_custom_attr_tmp.content_version_id,cdm_dyn_content_custom_attr_tmp.extension_attribute_nm,cdm_dyn_content_custom_attr_tmp.updated_by_nm,cdm_dyn_content_custom_attr_tmp.updated_dttm,cdm_dyn_content_custom_attr_tmp.Hashed_pk_col
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_dyn_content_custom_attr_tmp , cdm_dyn_content_custom_attr, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_dyn_content_custom_attr_tmp ;
    QUIT;
    %put ######## Staging table: cdm_dyn_content_custom_attr_tmp  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_dyn_content_custom_attr;
      DROP TABLE work.cdm_dyn_content_custom_attr;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_dyn_content_custom_attr;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_identifier_type) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_identifier_type_tmp         ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_identifier_type_tmp         ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_identifier_type, table_keys=%str(identifier_type_id), out_table=work.cdm_identifier_type);
 data &tmplib..cdm_identifier_type_tmp         ;
     set work.cdm_identifier_type;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if identifier_type_id='' then identifier_type_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_identifier_type_tmp         , cdm_identifier_type);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_identifier_type using &tmpdbschema..cdm_identifier_type_tmp         
         ON (cdm_identifier_type.identifier_type_id=cdm_identifier_type_tmp.identifier_type_id)
        WHEN MATCHED THEN  
        UPDATE SET identifier_type_desc = cdm_identifier_type_tmp.identifier_type_desc , updated_by_nm = cdm_identifier_type_tmp.updated_by_nm , updated_dttm = cdm_identifier_type_tmp.updated_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        identifier_type_desc,identifier_type_id,updated_by_nm,updated_dttm
         ) values ( 
        cdm_identifier_type_tmp.identifier_type_desc,cdm_identifier_type_tmp.identifier_type_id,cdm_identifier_type_tmp.updated_by_nm,cdm_identifier_type_tmp.updated_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_identifier_type_tmp         , cdm_identifier_type, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_identifier_type_tmp         ;
    QUIT;
    %put ######## Staging table: cdm_identifier_type_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_identifier_type;
      DROP TABLE work.cdm_identifier_type;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_identifier_type;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_identity_attr) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_identity_attr_tmp           ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_identity_attr_tmp           ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_identity_attr, table_keys=%str(identifier_type_id,identity_id), out_table=work.cdm_identity_attr);
 data &tmplib..cdm_identity_attr_tmp           ;
     set work.cdm_identity_attr;
  if entry_dttm ne . then entry_dttm = tzoneu2s(entry_dttm,&timeZone_Value.);if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if identifier_type_id='' then identifier_type_id='-'; if identity_id='' then identity_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_identity_attr_tmp           , cdm_identity_attr);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_identity_attr using &tmpdbschema..cdm_identity_attr_tmp           
         ON (cdm_identity_attr.identifier_type_id=cdm_identity_attr_tmp.identifier_type_id and cdm_identity_attr.identity_id=cdm_identity_attr_tmp.identity_id)
        WHEN MATCHED THEN  
        UPDATE SET entry_dttm = cdm_identity_attr_tmp.entry_dttm , source_system_cd = cdm_identity_attr_tmp.source_system_cd , updated_by_nm = cdm_identity_attr_tmp.updated_by_nm , updated_dttm = cdm_identity_attr_tmp.updated_dttm , user_identifier_val = cdm_identity_attr_tmp.user_identifier_val , valid_from_dttm = cdm_identity_attr_tmp.valid_from_dttm , valid_to_dttm = cdm_identity_attr_tmp.valid_to_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        entry_dttm,identifier_type_id,identity_id,source_system_cd,updated_by_nm,updated_dttm,user_identifier_val,valid_from_dttm,valid_to_dttm
         ) values ( 
        cdm_identity_attr_tmp.entry_dttm,cdm_identity_attr_tmp.identifier_type_id,cdm_identity_attr_tmp.identity_id,cdm_identity_attr_tmp.source_system_cd,cdm_identity_attr_tmp.updated_by_nm,cdm_identity_attr_tmp.updated_dttm,cdm_identity_attr_tmp.user_identifier_val,cdm_identity_attr_tmp.valid_from_dttm,cdm_identity_attr_tmp.valid_to_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_identity_attr_tmp           , cdm_identity_attr, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_identity_attr_tmp           ;
    QUIT;
    %put ######## Staging table: cdm_identity_attr_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_identity_attr;
      DROP TABLE work.cdm_identity_attr;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_identity_attr;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_identity_map) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_identity_map_tmp            ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_identity_map_tmp            ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_identity_map, table_keys=%str(identity_id), out_table=work.cdm_identity_map);
 data &tmplib..cdm_identity_map_tmp            ;
     set work.cdm_identity_map;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if identity_id='' then identity_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_identity_map_tmp            , cdm_identity_map);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_identity_map using &tmpdbschema..cdm_identity_map_tmp            
         ON (cdm_identity_map.identity_id=cdm_identity_map_tmp.identity_id)
        WHEN MATCHED THEN  
        UPDATE SET identity_type_cd = cdm_identity_map_tmp.identity_type_cd , updated_by_nm = cdm_identity_map_tmp.updated_by_nm , updated_dttm = cdm_identity_map_tmp.updated_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        identity_id,identity_type_cd,updated_by_nm,updated_dttm
         ) values ( 
        cdm_identity_map_tmp.identity_id,cdm_identity_map_tmp.identity_type_cd,cdm_identity_map_tmp.updated_by_nm,cdm_identity_map_tmp.updated_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_identity_map_tmp            , cdm_identity_map, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_identity_map_tmp            ;
    QUIT;
    %put ######## Staging table: cdm_identity_map_tmp             Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_identity_map;
      DROP TABLE work.cdm_identity_map;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_identity_map;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_identity_type) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_identity_type_tmp           ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_identity_type_tmp           ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_identity_type, table_keys=%str(identity_type_cd), out_table=work.cdm_identity_type);
 data &tmplib..cdm_identity_type_tmp           ;
     set work.cdm_identity_type;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if identity_type_cd='' then identity_type_cd='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_identity_type_tmp           , cdm_identity_type);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_identity_type using &tmpdbschema..cdm_identity_type_tmp           
         ON (cdm_identity_type.identity_type_cd=cdm_identity_type_tmp.identity_type_cd)
        WHEN MATCHED THEN  
        UPDATE SET identity_type_desc = cdm_identity_type_tmp.identity_type_desc , updated_by_nm = cdm_identity_type_tmp.updated_by_nm , updated_dttm = cdm_identity_type_tmp.updated_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        identity_type_cd,identity_type_desc,updated_by_nm,updated_dttm
         ) values ( 
        cdm_identity_type_tmp.identity_type_cd,cdm_identity_type_tmp.identity_type_desc,cdm_identity_type_tmp.updated_by_nm,cdm_identity_type_tmp.updated_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_identity_type_tmp           , cdm_identity_type, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_identity_type_tmp           ;
    QUIT;
    %put ######## Staging table: cdm_identity_type_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_identity_type;
      DROP TABLE work.cdm_identity_type;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_identity_type;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_occurrence_detail) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_occurrence_detail_tmp       ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_occurrence_detail_tmp       ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_occurrence_detail, table_keys=%str(occurrence_id), out_table=work.cdm_occurrence_detail);
 data &tmplib..cdm_occurrence_detail_tmp       ;
     set work.cdm_occurrence_detail;
  if end_dttm ne . then end_dttm = tzoneu2s(end_dttm,&timeZone_Value.);if start_dttm ne . then start_dttm = tzoneu2s(start_dttm,&timeZone_Value.);if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if occurrence_id='' then occurrence_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_occurrence_detail_tmp       , cdm_occurrence_detail);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_occurrence_detail using &tmpdbschema..cdm_occurrence_detail_tmp       
         ON (cdm_occurrence_detail.occurrence_id=cdm_occurrence_detail_tmp.occurrence_id)
        WHEN MATCHED THEN  
        UPDATE SET end_dttm = cdm_occurrence_detail_tmp.end_dttm , execution_status_cd = cdm_occurrence_detail_tmp.execution_status_cd , occurrence_no = cdm_occurrence_detail_tmp.occurrence_no , occurrence_object_id = cdm_occurrence_detail_tmp.occurrence_object_id , occurrence_object_type_cd = cdm_occurrence_detail_tmp.occurrence_object_type_cd , occurrence_type_cd = cdm_occurrence_detail_tmp.occurrence_type_cd , source_system_cd = cdm_occurrence_detail_tmp.source_system_cd , start_dttm = cdm_occurrence_detail_tmp.start_dttm , updated_by_nm = cdm_occurrence_detail_tmp.updated_by_nm , updated_dttm = cdm_occurrence_detail_tmp.updated_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        end_dttm,execution_status_cd,occurrence_id,occurrence_no,occurrence_object_id,occurrence_object_type_cd,occurrence_type_cd,source_system_cd,start_dttm,updated_by_nm,updated_dttm
         ) values ( 
        cdm_occurrence_detail_tmp.end_dttm,cdm_occurrence_detail_tmp.execution_status_cd,cdm_occurrence_detail_tmp.occurrence_id,cdm_occurrence_detail_tmp.occurrence_no,cdm_occurrence_detail_tmp.occurrence_object_id,cdm_occurrence_detail_tmp.occurrence_object_type_cd,cdm_occurrence_detail_tmp.occurrence_type_cd,cdm_occurrence_detail_tmp.source_system_cd,cdm_occurrence_detail_tmp.start_dttm,cdm_occurrence_detail_tmp.updated_by_nm,cdm_occurrence_detail_tmp.updated_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_occurrence_detail_tmp       , cdm_occurrence_detail, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_occurrence_detail_tmp       ;
    QUIT;
    %put ######## Staging table: cdm_occurrence_detail_tmp        Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_occurrence_detail;
      DROP TABLE work.cdm_occurrence_detail;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_occurrence_detail;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_response_channel) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_response_channel_tmp        ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_response_channel_tmp        ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_response_channel, table_keys=%str(response_channel_cd), out_table=work.cdm_response_channel);
 data &tmplib..cdm_response_channel_tmp        ;
     set work.cdm_response_channel;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if response_channel_cd='' then response_channel_cd='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_response_channel_tmp        , cdm_response_channel);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_response_channel using &tmpdbschema..cdm_response_channel_tmp        
         ON (cdm_response_channel.response_channel_cd=cdm_response_channel_tmp.response_channel_cd)
        WHEN MATCHED THEN  
        UPDATE SET response_channel_nm = cdm_response_channel_tmp.response_channel_nm , updated_by_nm = cdm_response_channel_tmp.updated_by_nm , updated_dttm = cdm_response_channel_tmp.updated_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        response_channel_cd,response_channel_nm,updated_by_nm,updated_dttm
         ) values ( 
        cdm_response_channel_tmp.response_channel_cd,cdm_response_channel_tmp.response_channel_nm,cdm_response_channel_tmp.updated_by_nm,cdm_response_channel_tmp.updated_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_response_channel_tmp        , cdm_response_channel, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_response_channel_tmp        ;
    QUIT;
    %put ######## Staging table: cdm_response_channel_tmp         Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_response_channel;
      DROP TABLE work.cdm_response_channel;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_response_channel;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_response_extended_attr) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_response_extended_attr_tmp  ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_response_extended_attr_tmp  ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_response_extended_attr, table_keys=%str(attribute_nm,response_attribute_type_cd,response_id), out_table=work.cdm_response_extended_attr);
 data &tmplib..cdm_response_extended_attr_tmp  ;
     set work.cdm_response_extended_attr;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if attribute_nm='' then attribute_nm='-'; if response_attribute_type_cd='' then response_attribute_type_cd='-'; if response_id='' then response_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_response_extended_attr_tmp  , cdm_response_extended_attr);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_response_extended_attr using &tmpdbschema..cdm_response_extended_attr_tmp  
         ON (cdm_response_extended_attr.attribute_nm=cdm_response_extended_attr_tmp.attribute_nm and cdm_response_extended_attr.response_attribute_type_cd=cdm_response_extended_attr_tmp.response_attribute_type_cd and cdm_response_extended_attr.response_id=cdm_response_extended_attr_tmp.response_id)
        WHEN MATCHED THEN  
        UPDATE SET attribute_data_type_cd = cdm_response_extended_attr_tmp.attribute_data_type_cd , attribute_val = cdm_response_extended_attr_tmp.attribute_val , updated_by_nm = cdm_response_extended_attr_tmp.updated_by_nm , updated_dttm = cdm_response_extended_attr_tmp.updated_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        attribute_data_type_cd,attribute_nm,attribute_val,response_attribute_type_cd,response_id,updated_by_nm,updated_dttm
         ) values ( 
        cdm_response_extended_attr_tmp.attribute_data_type_cd,cdm_response_extended_attr_tmp.attribute_nm,cdm_response_extended_attr_tmp.attribute_val,cdm_response_extended_attr_tmp.response_attribute_type_cd,cdm_response_extended_attr_tmp.response_id,cdm_response_extended_attr_tmp.updated_by_nm,cdm_response_extended_attr_tmp.updated_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_response_extended_attr_tmp  , cdm_response_extended_attr, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_response_extended_attr_tmp  ;
    QUIT;
    %put ######## Staging table: cdm_response_extended_attr_tmp   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_response_extended_attr;
      DROP TABLE work.cdm_response_extended_attr;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_response_extended_attr;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_response_history) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_response_history_tmp        ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_response_history_tmp        ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_response_history, table_keys=%str(response_id), out_table=work.cdm_response_history);
 data &tmplib..cdm_response_history_tmp        ;
     set work.cdm_response_history;
  if response_dttm ne . then response_dttm = tzoneu2s(response_dttm,&timeZone_Value.);if response_dttm_tz ne . then response_dttm_tz = tzoneu2s(response_dttm_tz,&timeZone_Value.);if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if response_id='' then response_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_response_history_tmp        , cdm_response_history);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_response_history using &tmpdbschema..cdm_response_history_tmp        
         ON (cdm_response_history.response_id=cdm_response_history_tmp.response_id)
        WHEN MATCHED THEN  
        UPDATE SET audience_id = cdm_response_history_tmp.audience_id , audience_occur_id = cdm_response_history_tmp.audience_occur_id , contact_id = cdm_response_history_tmp.contact_id , content_hash_val = cdm_response_history_tmp.content_hash_val , content_id = cdm_response_history_tmp.content_id , content_version_id = cdm_response_history_tmp.content_version_id , context_type_nm = cdm_response_history_tmp.context_type_nm , context_val = cdm_response_history_tmp.context_val , conversion_flg = cdm_response_history_tmp.conversion_flg , external_contact_info_1_id = cdm_response_history_tmp.external_contact_info_1_id , external_contact_info_2_id = cdm_response_history_tmp.external_contact_info_2_id , identity_id = cdm_response_history_tmp.identity_id , inferred_response_flg = cdm_response_history_tmp.inferred_response_flg , properties_map_doc = cdm_response_history_tmp.properties_map_doc , response_cd = cdm_response_history_tmp.response_cd , response_channel_cd = cdm_response_history_tmp.response_channel_cd , response_dt = cdm_response_history_tmp.response_dt , response_dttm = cdm_response_history_tmp.response_dttm , response_dttm_tz = cdm_response_history_tmp.response_dttm_tz , response_type_cd = cdm_response_history_tmp.response_type_cd , response_val_amt = cdm_response_history_tmp.response_val_amt , rtc_id = cdm_response_history_tmp.rtc_id , source_system_cd = cdm_response_history_tmp.source_system_cd , updated_by_nm = cdm_response_history_tmp.updated_by_nm , updated_dttm = cdm_response_history_tmp.updated_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        audience_id,audience_occur_id,contact_id,content_hash_val,content_id,content_version_id,context_type_nm,context_val,conversion_flg,external_contact_info_1_id,external_contact_info_2_id,identity_id,inferred_response_flg,properties_map_doc,response_cd,response_channel_cd,response_dt,response_dttm,response_dttm_tz,response_id,response_type_cd,response_val_amt,rtc_id,source_system_cd,updated_by_nm,updated_dttm
         ) values ( 
        cdm_response_history_tmp.audience_id,cdm_response_history_tmp.audience_occur_id,cdm_response_history_tmp.contact_id,cdm_response_history_tmp.content_hash_val,cdm_response_history_tmp.content_id,cdm_response_history_tmp.content_version_id,cdm_response_history_tmp.context_type_nm,cdm_response_history_tmp.context_val,cdm_response_history_tmp.conversion_flg,cdm_response_history_tmp.external_contact_info_1_id,cdm_response_history_tmp.external_contact_info_2_id,cdm_response_history_tmp.identity_id,cdm_response_history_tmp.inferred_response_flg,cdm_response_history_tmp.properties_map_doc,cdm_response_history_tmp.response_cd,cdm_response_history_tmp.response_channel_cd,cdm_response_history_tmp.response_dt,cdm_response_history_tmp.response_dttm,cdm_response_history_tmp.response_dttm_tz,cdm_response_history_tmp.response_id,cdm_response_history_tmp.response_type_cd,cdm_response_history_tmp.response_val_amt,cdm_response_history_tmp.rtc_id,cdm_response_history_tmp.source_system_cd,cdm_response_history_tmp.updated_by_nm,cdm_response_history_tmp.updated_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_response_history_tmp        , cdm_response_history, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_response_history_tmp        ;
    QUIT;
    %put ######## Staging table: cdm_response_history_tmp         Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_response_history;
      DROP TABLE work.cdm_response_history;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_response_history;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_response_lookup) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_response_lookup_tmp         ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_response_lookup_tmp         ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_response_lookup, table_keys=%str(response_cd), out_table=work.cdm_response_lookup);
 data &tmplib..cdm_response_lookup_tmp         ;
     set work.cdm_response_lookup;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if response_cd='' then response_cd='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_response_lookup_tmp         , cdm_response_lookup);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_response_lookup using &tmpdbschema..cdm_response_lookup_tmp         
         ON (cdm_response_lookup.response_cd=cdm_response_lookup_tmp.response_cd)
        WHEN MATCHED THEN  
        UPDATE SET response_nm = cdm_response_lookup_tmp.response_nm , updated_by_nm = cdm_response_lookup_tmp.updated_by_nm , updated_dttm = cdm_response_lookup_tmp.updated_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        response_cd,response_nm,updated_by_nm,updated_dttm
         ) values ( 
        cdm_response_lookup_tmp.response_cd,cdm_response_lookup_tmp.response_nm,cdm_response_lookup_tmp.updated_by_nm,cdm_response_lookup_tmp.updated_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_response_lookup_tmp         , cdm_response_lookup, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_response_lookup_tmp         ;
    QUIT;
    %put ######## Staging table: cdm_response_lookup_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_response_lookup;
      DROP TABLE work.cdm_response_lookup;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_response_lookup;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_response_type) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_response_type_tmp           ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_response_type_tmp           ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_response_type, table_keys=%str(response_type_cd), out_table=work.cdm_response_type);
 data &tmplib..cdm_response_type_tmp           ;
     set work.cdm_response_type;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if response_type_cd='' then response_type_cd='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_response_type_tmp           , cdm_response_type);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_response_type using &tmpdbschema..cdm_response_type_tmp           
         ON (cdm_response_type.response_type_cd=cdm_response_type_tmp.response_type_cd)
        WHEN MATCHED THEN  
        UPDATE SET response_type_desc = cdm_response_type_tmp.response_type_desc , updated_by_nm = cdm_response_type_tmp.updated_by_nm , updated_dttm = cdm_response_type_tmp.updated_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        response_type_cd,response_type_desc,updated_by_nm,updated_dttm
         ) values ( 
        cdm_response_type_tmp.response_type_cd,cdm_response_type_tmp.response_type_desc,cdm_response_type_tmp.updated_by_nm,cdm_response_type_tmp.updated_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_response_type_tmp           , cdm_response_type, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_response_type_tmp           ;
    QUIT;
    %put ######## Staging table: cdm_response_type_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_response_type;
      DROP TABLE work.cdm_response_type;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_response_type;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_rtc_detail) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_rtc_detail_tmp              ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_rtc_detail_tmp              ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_rtc_detail, table_keys=%str(rtc_id), out_table=work.cdm_rtc_detail);
 data &tmplib..cdm_rtc_detail_tmp              ;
     set work.cdm_rtc_detail;
  if processed_dttm ne . then processed_dttm = tzoneu2s(processed_dttm,&timeZone_Value.);if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if rtc_id='' then rtc_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_rtc_detail_tmp              , cdm_rtc_detail);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_rtc_detail using &tmpdbschema..cdm_rtc_detail_tmp              
         ON (cdm_rtc_detail.rtc_id=cdm_rtc_detail_tmp.rtc_id)
        WHEN MATCHED THEN  
        UPDATE SET deleted_flg = cdm_rtc_detail_tmp.deleted_flg , execution_status_cd = cdm_rtc_detail_tmp.execution_status_cd , occurrence_id = cdm_rtc_detail_tmp.occurrence_id , processed_dttm = cdm_rtc_detail_tmp.processed_dttm , response_tracking_flg = cdm_rtc_detail_tmp.response_tracking_flg , segment_id = cdm_rtc_detail_tmp.segment_id , segment_version_id = cdm_rtc_detail_tmp.segment_version_id , source_system_cd = cdm_rtc_detail_tmp.source_system_cd , task_id = cdm_rtc_detail_tmp.task_id , task_occurrence_no = cdm_rtc_detail_tmp.task_occurrence_no , task_version_id = cdm_rtc_detail_tmp.task_version_id , updated_by_nm = cdm_rtc_detail_tmp.updated_by_nm , updated_dttm = cdm_rtc_detail_tmp.updated_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        deleted_flg,execution_status_cd,occurrence_id,processed_dttm,response_tracking_flg,rtc_id,segment_id,segment_version_id,source_system_cd,task_id,task_occurrence_no,task_version_id,updated_by_nm,updated_dttm
         ) values ( 
        cdm_rtc_detail_tmp.deleted_flg,cdm_rtc_detail_tmp.execution_status_cd,cdm_rtc_detail_tmp.occurrence_id,cdm_rtc_detail_tmp.processed_dttm,cdm_rtc_detail_tmp.response_tracking_flg,cdm_rtc_detail_tmp.rtc_id,cdm_rtc_detail_tmp.segment_id,cdm_rtc_detail_tmp.segment_version_id,cdm_rtc_detail_tmp.source_system_cd,cdm_rtc_detail_tmp.task_id,cdm_rtc_detail_tmp.task_occurrence_no,cdm_rtc_detail_tmp.task_version_id,cdm_rtc_detail_tmp.updated_by_nm,cdm_rtc_detail_tmp.updated_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_rtc_detail_tmp              , cdm_rtc_detail, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_rtc_detail_tmp              ;
    QUIT;
    %put ######## Staging table: cdm_rtc_detail_tmp               Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_rtc_detail;
      DROP TABLE work.cdm_rtc_detail;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_rtc_detail;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_rtc_x_content) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_rtc_x_content_tmp           ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_rtc_x_content_tmp           ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_rtc_x_content, table_keys=%str(content_version_id,rtc_id), out_table=work.cdm_rtc_x_content);
 data &tmplib..cdm_rtc_x_content_tmp           ;
     set work.cdm_rtc_x_content;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if content_version_id='' then content_version_id='-'; if rtc_id='' then rtc_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_rtc_x_content_tmp           , cdm_rtc_x_content);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_rtc_x_content using &tmpdbschema..cdm_rtc_x_content_tmp           
         ON (cdm_rtc_x_content.content_version_id=cdm_rtc_x_content_tmp.content_version_id and cdm_rtc_x_content.rtc_id=cdm_rtc_x_content_tmp.rtc_id)
        WHEN MATCHED THEN  
        UPDATE SET content_hash_val = cdm_rtc_x_content_tmp.content_hash_val , content_id = cdm_rtc_x_content_tmp.content_id , rtc_x_content_sk = cdm_rtc_x_content_tmp.rtc_x_content_sk , sequence_no = cdm_rtc_x_content_tmp.sequence_no , updated_by_nm = cdm_rtc_x_content_tmp.updated_by_nm , updated_dttm = cdm_rtc_x_content_tmp.updated_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        content_hash_val,content_id,content_version_id,rtc_id,rtc_x_content_sk,sequence_no,updated_by_nm,updated_dttm
         ) values ( 
        cdm_rtc_x_content_tmp.content_hash_val,cdm_rtc_x_content_tmp.content_id,cdm_rtc_x_content_tmp.content_version_id,cdm_rtc_x_content_tmp.rtc_id,cdm_rtc_x_content_tmp.rtc_x_content_sk,cdm_rtc_x_content_tmp.sequence_no,cdm_rtc_x_content_tmp.updated_by_nm,cdm_rtc_x_content_tmp.updated_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_rtc_x_content_tmp           , cdm_rtc_x_content, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_rtc_x_content_tmp           ;
    QUIT;
    %put ######## Staging table: cdm_rtc_x_content_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_rtc_x_content;
      DROP TABLE work.cdm_rtc_x_content;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_rtc_x_content;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_segment_custom_attr) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_segment_custom_attr_tmp     ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_segment_custom_attr_tmp     ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_segment_custom_attr, table_keys=%str(attribute_data_type_cd,attribute_nm,attribute_val,segment_version_id), out_table=work.cdm_segment_custom_attr);
 data &tmplib..cdm_segment_custom_attr_tmp     ;
     set work.cdm_segment_custom_attr;
  if attribute_dttm_val ne . then attribute_dttm_val = tzoneu2s(attribute_dttm_val,&timeZone_Value.);if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  Hashed_pk_col = put(sha256(catx('|',attribute_data_type_cd,attribute_nm,attribute_val,segment_version_id)), $hex64.);
  if attribute_data_type_cd='' then attribute_data_type_cd='-'; if attribute_nm='' then attribute_nm='-'; if attribute_val='' then attribute_val='-'; if segment_version_id='' then segment_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_segment_custom_attr_tmp     , cdm_segment_custom_attr);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_segment_custom_attr using &tmpdbschema..cdm_segment_custom_attr_tmp     
         ON (cdm_segment_custom_attr.Hashed_pk_col = cdm_segment_custom_attr_tmp.Hashed_pk_col)
        WHEN MATCHED THEN  
        UPDATE SET attribute_character_val = cdm_segment_custom_attr_tmp.attribute_character_val , attribute_dttm_val = cdm_segment_custom_attr_tmp.attribute_dttm_val , attribute_numeric_val = cdm_segment_custom_attr_tmp.attribute_numeric_val , segment_id = cdm_segment_custom_attr_tmp.segment_id , updated_by_nm = cdm_segment_custom_attr_tmp.updated_by_nm , updated_dttm = cdm_segment_custom_attr_tmp.updated_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        attribute_character_val,attribute_data_type_cd,attribute_dttm_val,attribute_nm,attribute_numeric_val,attribute_val,segment_id,segment_version_id,updated_by_nm,updated_dttm
        ,Hashed_pk_col ) VALUES ( 
        cdm_segment_custom_attr_tmp.attribute_character_val,cdm_segment_custom_attr_tmp.attribute_data_type_cd,cdm_segment_custom_attr_tmp.attribute_dttm_val,cdm_segment_custom_attr_tmp.attribute_nm,cdm_segment_custom_attr_tmp.attribute_numeric_val,cdm_segment_custom_attr_tmp.attribute_val,cdm_segment_custom_attr_tmp.segment_id,cdm_segment_custom_attr_tmp.segment_version_id,cdm_segment_custom_attr_tmp.updated_by_nm,cdm_segment_custom_attr_tmp.updated_dttm,cdm_segment_custom_attr_tmp.Hashed_pk_col
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_segment_custom_attr_tmp     , cdm_segment_custom_attr, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_segment_custom_attr_tmp     ;
    QUIT;
    %put ######## Staging table: cdm_segment_custom_attr_tmp      Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_segment_custom_attr;
      DROP TABLE work.cdm_segment_custom_attr;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_segment_custom_attr;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_segment_detail) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_segment_detail_tmp          ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_segment_detail_tmp          ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_segment_detail, table_keys=%str(segment_version_id), out_table=work.cdm_segment_detail);
 data &tmplib..cdm_segment_detail_tmp          ;
     set work.cdm_segment_detail;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if segment_version_id='' then segment_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_segment_detail_tmp          , cdm_segment_detail);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_segment_detail using &tmpdbschema..cdm_segment_detail_tmp          
         ON (cdm_segment_detail.segment_version_id=cdm_segment_detail_tmp.segment_version_id)
        WHEN MATCHED THEN  
        UPDATE SET segment_category_nm = cdm_segment_detail_tmp.segment_category_nm , segment_cd = cdm_segment_detail_tmp.segment_cd , segment_desc = cdm_segment_detail_tmp.segment_desc , segment_id = cdm_segment_detail_tmp.segment_id , segment_map_id = cdm_segment_detail_tmp.segment_map_id , segment_map_version_id = cdm_segment_detail_tmp.segment_map_version_id , segment_nm = cdm_segment_detail_tmp.segment_nm , segment_src_nm = cdm_segment_detail_tmp.segment_src_nm , segment_status_cd = cdm_segment_detail_tmp.segment_status_cd , source_system_cd = cdm_segment_detail_tmp.source_system_cd , updated_by_nm = cdm_segment_detail_tmp.updated_by_nm , updated_dttm = cdm_segment_detail_tmp.updated_dttm , valid_from_dttm = cdm_segment_detail_tmp.valid_from_dttm , valid_to_dttm = cdm_segment_detail_tmp.valid_to_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        segment_category_nm,segment_cd,segment_desc,segment_id,segment_map_id,segment_map_version_id,segment_nm,segment_src_nm,segment_status_cd,segment_version_id,source_system_cd,updated_by_nm,updated_dttm,valid_from_dttm,valid_to_dttm
         ) values ( 
        cdm_segment_detail_tmp.segment_category_nm,cdm_segment_detail_tmp.segment_cd,cdm_segment_detail_tmp.segment_desc,cdm_segment_detail_tmp.segment_id,cdm_segment_detail_tmp.segment_map_id,cdm_segment_detail_tmp.segment_map_version_id,cdm_segment_detail_tmp.segment_nm,cdm_segment_detail_tmp.segment_src_nm,cdm_segment_detail_tmp.segment_status_cd,cdm_segment_detail_tmp.segment_version_id,cdm_segment_detail_tmp.source_system_cd,cdm_segment_detail_tmp.updated_by_nm,cdm_segment_detail_tmp.updated_dttm,cdm_segment_detail_tmp.valid_from_dttm,cdm_segment_detail_tmp.valid_to_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_segment_detail_tmp          , cdm_segment_detail, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_segment_detail_tmp          ;
    QUIT;
    %put ######## Staging table: cdm_segment_detail_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_segment_detail;
      DROP TABLE work.cdm_segment_detail;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_segment_detail;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_segment_map) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_segment_map_tmp             ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_segment_map_tmp             ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_segment_map, table_keys=%str(segment_map_version_id), out_table=work.cdm_segment_map);
 data &tmplib..cdm_segment_map_tmp             ;
     set work.cdm_segment_map;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if segment_map_version_id='' then segment_map_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_segment_map_tmp             , cdm_segment_map);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_segment_map using &tmpdbschema..cdm_segment_map_tmp             
         ON (cdm_segment_map.segment_map_version_id=cdm_segment_map_tmp.segment_map_version_id)
        WHEN MATCHED THEN  
        UPDATE SET segment_map_category_nm = cdm_segment_map_tmp.segment_map_category_nm , segment_map_cd = cdm_segment_map_tmp.segment_map_cd , segment_map_desc = cdm_segment_map_tmp.segment_map_desc , segment_map_id = cdm_segment_map_tmp.segment_map_id , segment_map_nm = cdm_segment_map_tmp.segment_map_nm , segment_map_src_nm = cdm_segment_map_tmp.segment_map_src_nm , segment_map_status_cd = cdm_segment_map_tmp.segment_map_status_cd , source_system_cd = cdm_segment_map_tmp.source_system_cd , updated_by_nm = cdm_segment_map_tmp.updated_by_nm , updated_dttm = cdm_segment_map_tmp.updated_dttm , valid_from_dttm = cdm_segment_map_tmp.valid_from_dttm , valid_to_dttm = cdm_segment_map_tmp.valid_to_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        segment_map_category_nm,segment_map_cd,segment_map_desc,segment_map_id,segment_map_nm,segment_map_src_nm,segment_map_status_cd,segment_map_version_id,source_system_cd,updated_by_nm,updated_dttm,valid_from_dttm,valid_to_dttm
         ) values ( 
        cdm_segment_map_tmp.segment_map_category_nm,cdm_segment_map_tmp.segment_map_cd,cdm_segment_map_tmp.segment_map_desc,cdm_segment_map_tmp.segment_map_id,cdm_segment_map_tmp.segment_map_nm,cdm_segment_map_tmp.segment_map_src_nm,cdm_segment_map_tmp.segment_map_status_cd,cdm_segment_map_tmp.segment_map_version_id,cdm_segment_map_tmp.source_system_cd,cdm_segment_map_tmp.updated_by_nm,cdm_segment_map_tmp.updated_dttm,cdm_segment_map_tmp.valid_from_dttm,cdm_segment_map_tmp.valid_to_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_segment_map_tmp             , cdm_segment_map, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_segment_map_tmp             ;
    QUIT;
    %put ######## Staging table: cdm_segment_map_tmp              Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_segment_map;
      DROP TABLE work.cdm_segment_map;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_segment_map;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_segment_map_custom_attr) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_segment_map_custom_attr_tmp ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_segment_map_custom_attr_tmp ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_segment_map_custom_attr, table_keys=%str(attribute_data_type_cd,attribute_nm,attribute_val,segment_map_version_id), out_table=work.cdm_segment_map_custom_attr);
 data &tmplib..cdm_segment_map_custom_attr_tmp ;
     set work.cdm_segment_map_custom_attr;
  if attribute_dttm_val ne . then attribute_dttm_val = tzoneu2s(attribute_dttm_val,&timeZone_Value.);if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  Hashed_pk_col = put(sha256(catx('|',attribute_data_type_cd,attribute_nm,attribute_val,segment_map_version_id)), $hex64.);
  if attribute_data_type_cd='' then attribute_data_type_cd='-'; if attribute_nm='' then attribute_nm='-'; if attribute_val='' then attribute_val='-'; if segment_map_version_id='' then segment_map_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_segment_map_custom_attr_tmp , cdm_segment_map_custom_attr);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_segment_map_custom_attr using &tmpdbschema..cdm_segment_map_custom_attr_tmp 
         ON (cdm_segment_map_custom_attr.Hashed_pk_col = cdm_segment_map_custom_attr_tmp.Hashed_pk_col)
        WHEN MATCHED THEN  
        UPDATE SET attribute_character_val = cdm_segment_map_custom_attr_tmp.attribute_character_val , attribute_dttm_val = cdm_segment_map_custom_attr_tmp.attribute_dttm_val , attribute_numeric_val = cdm_segment_map_custom_attr_tmp.attribute_numeric_val , segment_map_id = cdm_segment_map_custom_attr_tmp.segment_map_id , updated_by_nm = cdm_segment_map_custom_attr_tmp.updated_by_nm , updated_dttm = cdm_segment_map_custom_attr_tmp.updated_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        attribute_character_val,attribute_data_type_cd,attribute_dttm_val,attribute_nm,attribute_numeric_val,attribute_val,segment_map_id,segment_map_version_id,updated_by_nm,updated_dttm
        ,Hashed_pk_col ) VALUES ( 
        cdm_segment_map_custom_attr_tmp.attribute_character_val,cdm_segment_map_custom_attr_tmp.attribute_data_type_cd,cdm_segment_map_custom_attr_tmp.attribute_dttm_val,cdm_segment_map_custom_attr_tmp.attribute_nm,cdm_segment_map_custom_attr_tmp.attribute_numeric_val,cdm_segment_map_custom_attr_tmp.attribute_val,cdm_segment_map_custom_attr_tmp.segment_map_id,cdm_segment_map_custom_attr_tmp.segment_map_version_id,cdm_segment_map_custom_attr_tmp.updated_by_nm,cdm_segment_map_custom_attr_tmp.updated_dttm,cdm_segment_map_custom_attr_tmp.Hashed_pk_col
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_segment_map_custom_attr_tmp , cdm_segment_map_custom_attr, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_segment_map_custom_attr_tmp ;
    QUIT;
    %put ######## Staging table: cdm_segment_map_custom_attr_tmp  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_segment_map_custom_attr;
      DROP TABLE work.cdm_segment_map_custom_attr;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_segment_map_custom_attr;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_segment_test) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_segment_test_tmp            ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_segment_test_tmp            ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_segment_test, table_keys=%str(task_version_id,test_cd), out_table=work.cdm_segment_test);
 data &tmplib..cdm_segment_test_tmp            ;
     set work.cdm_segment_test;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if task_version_id='' then task_version_id='-'; if test_cd='' then test_cd='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_segment_test_tmp            , cdm_segment_test);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_segment_test using &tmpdbschema..cdm_segment_test_tmp            
         ON (cdm_segment_test.task_version_id=cdm_segment_test_tmp.task_version_id and cdm_segment_test.test_cd=cdm_segment_test_tmp.test_cd)
        WHEN MATCHED THEN  
        UPDATE SET stratified_samp_criteria_txt = cdm_segment_test_tmp.stratified_samp_criteria_txt , stratified_sampling_flg = cdm_segment_test_tmp.stratified_sampling_flg , task_id = cdm_segment_test_tmp.task_id , test_cnt = cdm_segment_test_tmp.test_cnt , test_enabled_flg = cdm_segment_test_tmp.test_enabled_flg , test_nm = cdm_segment_test_tmp.test_nm , test_pct = cdm_segment_test_tmp.test_pct , test_sizing_type_nm = cdm_segment_test_tmp.test_sizing_type_nm , test_type_nm = cdm_segment_test_tmp.test_type_nm , updated_dttm = cdm_segment_test_tmp.updated_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        stratified_samp_criteria_txt,stratified_sampling_flg,task_id,task_version_id,test_cd,test_cnt,test_enabled_flg,test_nm,test_pct,test_sizing_type_nm,test_type_nm,updated_dttm
         ) values ( 
        cdm_segment_test_tmp.stratified_samp_criteria_txt,cdm_segment_test_tmp.stratified_sampling_flg,cdm_segment_test_tmp.task_id,cdm_segment_test_tmp.task_version_id,cdm_segment_test_tmp.test_cd,cdm_segment_test_tmp.test_cnt,cdm_segment_test_tmp.test_enabled_flg,cdm_segment_test_tmp.test_nm,cdm_segment_test_tmp.test_pct,cdm_segment_test_tmp.test_sizing_type_nm,cdm_segment_test_tmp.test_type_nm,cdm_segment_test_tmp.updated_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_segment_test_tmp            , cdm_segment_test, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_segment_test_tmp            ;
    QUIT;
    %put ######## Staging table: cdm_segment_test_tmp             Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_segment_test;
      DROP TABLE work.cdm_segment_test;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_segment_test;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_segment_test_x_segment) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_segment_test_x_segment_tmp  ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_segment_test_x_segment_tmp  ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_segment_test_x_segment, table_keys=%str(segment_id,task_version_id,test_cd), out_table=work.cdm_segment_test_x_segment);
 data &tmplib..cdm_segment_test_x_segment_tmp  ;
     set work.cdm_segment_test_x_segment;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if segment_id='' then segment_id='-'; if task_version_id='' then task_version_id='-'; if test_cd='' then test_cd='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_segment_test_x_segment_tmp  , cdm_segment_test_x_segment);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_segment_test_x_segment using &tmpdbschema..cdm_segment_test_x_segment_tmp  
         ON (cdm_segment_test_x_segment.segment_id=cdm_segment_test_x_segment_tmp.segment_id and cdm_segment_test_x_segment.task_version_id=cdm_segment_test_x_segment_tmp.task_version_id and cdm_segment_test_x_segment.test_cd=cdm_segment_test_x_segment_tmp.test_cd)
        WHEN MATCHED THEN  
        UPDATE SET task_id = cdm_segment_test_x_segment_tmp.task_id , updated_dttm = cdm_segment_test_x_segment_tmp.updated_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        segment_id,task_id,task_version_id,test_cd,updated_dttm
         ) values ( 
        cdm_segment_test_x_segment_tmp.segment_id,cdm_segment_test_x_segment_tmp.task_id,cdm_segment_test_x_segment_tmp.task_version_id,cdm_segment_test_x_segment_tmp.test_cd,cdm_segment_test_x_segment_tmp.updated_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_segment_test_x_segment_tmp  , cdm_segment_test_x_segment, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_segment_test_x_segment_tmp  ;
    QUIT;
    %put ######## Staging table: cdm_segment_test_x_segment_tmp   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_segment_test_x_segment;
      DROP TABLE work.cdm_segment_test_x_segment;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_segment_test_x_segment;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_task_custom_attr) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_task_custom_attr_tmp        ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_task_custom_attr_tmp        ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_task_custom_attr, table_keys=%str(attribute_data_type_cd,attribute_nm,attribute_val,task_version_id), out_table=work.cdm_task_custom_attr);
 data &tmplib..cdm_task_custom_attr_tmp        ;
     set work.cdm_task_custom_attr;
  if attribute_dttm_val ne . then attribute_dttm_val = tzoneu2s(attribute_dttm_val,&timeZone_Value.);if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  Hashed_pk_col = put(sha256(catx('|',attribute_data_type_cd,attribute_nm,attribute_val,task_version_id)), $hex64.);
  if attribute_data_type_cd='' then attribute_data_type_cd='-'; if attribute_nm='' then attribute_nm='-'; if attribute_val='' then attribute_val='-'; if task_version_id='' then task_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_task_custom_attr_tmp        , cdm_task_custom_attr);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_task_custom_attr using &tmpdbschema..cdm_task_custom_attr_tmp        
         ON (cdm_task_custom_attr.Hashed_pk_col = cdm_task_custom_attr_tmp.Hashed_pk_col)
        WHEN MATCHED THEN  
        UPDATE SET attribute_character_val = cdm_task_custom_attr_tmp.attribute_character_val , attribute_dttm_val = cdm_task_custom_attr_tmp.attribute_dttm_val , attribute_numeric_val = cdm_task_custom_attr_tmp.attribute_numeric_val , extension_attribute_nm = cdm_task_custom_attr_tmp.extension_attribute_nm , task_id = cdm_task_custom_attr_tmp.task_id , updated_by_nm = cdm_task_custom_attr_tmp.updated_by_nm , updated_dttm = cdm_task_custom_attr_tmp.updated_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        attribute_character_val,attribute_data_type_cd,attribute_dttm_val,attribute_nm,attribute_numeric_val,attribute_val,extension_attribute_nm,task_id,task_version_id,updated_by_nm,updated_dttm
        ,Hashed_pk_col ) VALUES ( 
        cdm_task_custom_attr_tmp.attribute_character_val,cdm_task_custom_attr_tmp.attribute_data_type_cd,cdm_task_custom_attr_tmp.attribute_dttm_val,cdm_task_custom_attr_tmp.attribute_nm,cdm_task_custom_attr_tmp.attribute_numeric_val,cdm_task_custom_attr_tmp.attribute_val,cdm_task_custom_attr_tmp.extension_attribute_nm,cdm_task_custom_attr_tmp.task_id,cdm_task_custom_attr_tmp.task_version_id,cdm_task_custom_attr_tmp.updated_by_nm,cdm_task_custom_attr_tmp.updated_dttm,cdm_task_custom_attr_tmp.Hashed_pk_col
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_task_custom_attr_tmp        , cdm_task_custom_attr, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_task_custom_attr_tmp        ;
    QUIT;
    %put ######## Staging table: cdm_task_custom_attr_tmp         Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_task_custom_attr;
      DROP TABLE work.cdm_task_custom_attr;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_task_custom_attr;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_task_detail) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_task_detail_tmp             ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_task_detail_tmp             ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=cdm_task_detail, table_keys=%str(task_version_id), out_table=work.cdm_task_detail);
 data &tmplib..cdm_task_detail_tmp             ;
     set work.cdm_task_detail;
  if export_dttm ne . then export_dttm = tzoneu2s(export_dttm,&timeZone_Value.);if scheduled_end_dttm ne . then scheduled_end_dttm = tzoneu2s(scheduled_end_dttm,&timeZone_Value.);if scheduled_start_dttm ne . then scheduled_start_dttm = tzoneu2s(scheduled_start_dttm,&timeZone_Value.);if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if task_version_id='' then task_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_task_detail_tmp             , cdm_task_detail);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..cdm_task_detail using &tmpdbschema..cdm_task_detail_tmp             
         ON (cdm_task_detail.task_version_id=cdm_task_detail_tmp.task_version_id)
        WHEN MATCHED THEN  
        UPDATE SET active_flg = cdm_task_detail_tmp.active_flg , budget_unit_cost_amt = cdm_task_detail_tmp.budget_unit_cost_amt , budget_unit_usage_amt = cdm_task_detail_tmp.budget_unit_usage_amt , business_context_id = cdm_task_detail_tmp.business_context_id , campaign_id = cdm_task_detail_tmp.campaign_id , contact_channel_cd = cdm_task_detail_tmp.contact_channel_cd , control_group_action_nm = cdm_task_detail_tmp.control_group_action_nm , created_dt = cdm_task_detail_tmp.created_dt , created_user_nm = cdm_task_detail_tmp.created_user_nm , export_dttm = cdm_task_detail_tmp.export_dttm , limit_by_total_impression_flg = cdm_task_detail_tmp.limit_by_total_impression_flg , limit_period_unit_cnt = cdm_task_detail_tmp.limit_period_unit_cnt , max_budget_amt = cdm_task_detail_tmp.max_budget_amt , max_budget_offer_amt = cdm_task_detail_tmp.max_budget_offer_amt , maximum_period_expression_cnt = cdm_task_detail_tmp.maximum_period_expression_cnt , min_budget_amt = cdm_task_detail_tmp.min_budget_amt , min_budget_offer_amt = cdm_task_detail_tmp.min_budget_offer_amt , modified_status_cd = cdm_task_detail_tmp.modified_status_cd , owner_nm = cdm_task_detail_tmp.owner_nm , published_flg = cdm_task_detail_tmp.published_flg , recurr_type_cd = cdm_task_detail_tmp.recurr_type_cd , recurring_schedule_flg = cdm_task_detail_tmp.recurring_schedule_flg , saved_flg = cdm_task_detail_tmp.saved_flg , scheduled_end_dttm = cdm_task_detail_tmp.scheduled_end_dttm , scheduled_flg = cdm_task_detail_tmp.scheduled_flg , scheduled_start_dttm = cdm_task_detail_tmp.scheduled_start_dttm , segment_tests_flg = cdm_task_detail_tmp.segment_tests_flg , source_system_cd = cdm_task_detail_tmp.source_system_cd , staged_flg = cdm_task_detail_tmp.staged_flg , standard_reply_flg = cdm_task_detail_tmp.standard_reply_flg , stratified_sampling_action_nm = cdm_task_detail_tmp.stratified_sampling_action_nm , subject_type_nm = cdm_task_detail_tmp.subject_type_nm , task_cd = cdm_task_detail_tmp.task_cd , task_delivery_type_nm = cdm_task_detail_tmp.task_delivery_type_nm , task_desc = cdm_task_detail_tmp.task_desc , task_id = cdm_task_detail_tmp.task_id , task_nm = cdm_task_detail_tmp.task_nm , task_status_cd = cdm_task_detail_tmp.task_status_cd , task_subtype_nm = cdm_task_detail_tmp.task_subtype_nm , task_type_nm = cdm_task_detail_tmp.task_type_nm , update_contact_history_flg = cdm_task_detail_tmp.update_contact_history_flg , updated_by_nm = cdm_task_detail_tmp.updated_by_nm , updated_dttm = cdm_task_detail_tmp.updated_dttm , valid_from_dttm = cdm_task_detail_tmp.valid_from_dttm , valid_to_dttm = cdm_task_detail_tmp.valid_to_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        active_flg,budget_unit_cost_amt,budget_unit_usage_amt,business_context_id,campaign_id,contact_channel_cd,control_group_action_nm,created_dt,created_user_nm,export_dttm,limit_by_total_impression_flg,limit_period_unit_cnt,max_budget_amt,max_budget_offer_amt,maximum_period_expression_cnt,min_budget_amt,min_budget_offer_amt,modified_status_cd,owner_nm,published_flg,recurr_type_cd,recurring_schedule_flg,saved_flg,scheduled_end_dttm,scheduled_flg,scheduled_start_dttm,segment_tests_flg,source_system_cd,staged_flg,standard_reply_flg,stratified_sampling_action_nm,subject_type_nm,task_cd,task_delivery_type_nm,task_desc,task_id,task_nm,task_status_cd,task_subtype_nm,task_type_nm,task_version_id,update_contact_history_flg,updated_by_nm,updated_dttm,valid_from_dttm,valid_to_dttm
         ) values ( 
        cdm_task_detail_tmp.active_flg,cdm_task_detail_tmp.budget_unit_cost_amt,cdm_task_detail_tmp.budget_unit_usage_amt,cdm_task_detail_tmp.business_context_id,cdm_task_detail_tmp.campaign_id,cdm_task_detail_tmp.contact_channel_cd,cdm_task_detail_tmp.control_group_action_nm,cdm_task_detail_tmp.created_dt,cdm_task_detail_tmp.created_user_nm,cdm_task_detail_tmp.export_dttm,cdm_task_detail_tmp.limit_by_total_impression_flg,cdm_task_detail_tmp.limit_period_unit_cnt,cdm_task_detail_tmp.max_budget_amt,cdm_task_detail_tmp.max_budget_offer_amt,cdm_task_detail_tmp.maximum_period_expression_cnt,cdm_task_detail_tmp.min_budget_amt,cdm_task_detail_tmp.min_budget_offer_amt,cdm_task_detail_tmp.modified_status_cd,cdm_task_detail_tmp.owner_nm,cdm_task_detail_tmp.published_flg,cdm_task_detail_tmp.recurr_type_cd,cdm_task_detail_tmp.recurring_schedule_flg,cdm_task_detail_tmp.saved_flg,cdm_task_detail_tmp.scheduled_end_dttm,cdm_task_detail_tmp.scheduled_flg,cdm_task_detail_tmp.scheduled_start_dttm,cdm_task_detail_tmp.segment_tests_flg,cdm_task_detail_tmp.source_system_cd,cdm_task_detail_tmp.staged_flg,cdm_task_detail_tmp.standard_reply_flg,cdm_task_detail_tmp.stratified_sampling_action_nm,cdm_task_detail_tmp.subject_type_nm,cdm_task_detail_tmp.task_cd,cdm_task_detail_tmp.task_delivery_type_nm,cdm_task_detail_tmp.task_desc,cdm_task_detail_tmp.task_id,cdm_task_detail_tmp.task_nm,cdm_task_detail_tmp.task_status_cd,cdm_task_detail_tmp.task_subtype_nm,cdm_task_detail_tmp.task_type_nm,cdm_task_detail_tmp.task_version_id,cdm_task_detail_tmp.update_contact_history_flg,cdm_task_detail_tmp.updated_by_nm,cdm_task_detail_tmp.updated_dttm,cdm_task_detail_tmp.valid_from_dttm,cdm_task_detail_tmp.valid_to_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :cdm_task_detail_tmp             , cdm_task_detail, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..cdm_task_detail_tmp             ;
    QUIT;
    %put ######## Staging table: cdm_task_detail_tmp              Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..cdm_task_detail;
      DROP TABLE work.cdm_task_detail;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_task_detail;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..commitment_details) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..commitment_details_tmp          ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..commitment_details_tmp          ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=commitment_details, table_keys=%str(cmtmnt_id,planning_id), out_table=work.commitment_details);
 data &tmplib..commitment_details_tmp          ;
     set work.commitment_details;
  if cmtmnt_created_dttm ne . then cmtmnt_created_dttm = tzoneu2s(cmtmnt_created_dttm,&timeZone_Value.);if cmtmnt_payment_dttm ne . then cmtmnt_payment_dttm = tzoneu2s(cmtmnt_payment_dttm,&timeZone_Value.);if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if cmtmnt_id='' then cmtmnt_id='-'; if planning_id='' then planning_id='-';
 run;
 %ErrCheck (Failed to Append Data to :commitment_details_tmp          , commitment_details);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..commitment_details using &tmpdbschema..commitment_details_tmp          
         ON (commitment_details.cmtmnt_id=commitment_details_tmp.cmtmnt_id and commitment_details.planning_id=commitment_details_tmp.planning_id)
        WHEN MATCHED THEN  
        UPDATE SET cmtmnt_amt = commitment_details_tmp.cmtmnt_amt , cmtmnt_closure_note = commitment_details_tmp.cmtmnt_closure_note , cmtmnt_created_dttm = commitment_details_tmp.cmtmnt_created_dttm , cmtmnt_desc = commitment_details_tmp.cmtmnt_desc , cmtmnt_nm = commitment_details_tmp.cmtmnt_nm , cmtmnt_no = commitment_details_tmp.cmtmnt_no , cmtmnt_outstanding_amt = commitment_details_tmp.cmtmnt_outstanding_amt , cmtmnt_overspent_amt = commitment_details_tmp.cmtmnt_overspent_amt , cmtmnt_payment_dttm = commitment_details_tmp.cmtmnt_payment_dttm , cmtmnt_status = commitment_details_tmp.cmtmnt_status , created_by_usernm = commitment_details_tmp.created_by_usernm , created_dttm = commitment_details_tmp.created_dttm , last_modified_dttm = commitment_details_tmp.last_modified_dttm , last_modified_usernm = commitment_details_tmp.last_modified_usernm , load_dttm = commitment_details_tmp.load_dttm , planning_currency_cd = commitment_details_tmp.planning_currency_cd , planning_nm = commitment_details_tmp.planning_nm , vendor_amt = commitment_details_tmp.vendor_amt , vendor_currency_cd = commitment_details_tmp.vendor_currency_cd , vendor_id = commitment_details_tmp.vendor_id , vendor_nm = commitment_details_tmp.vendor_nm , vendor_number = commitment_details_tmp.vendor_number , vendor_obsolete_flg = commitment_details_tmp.vendor_obsolete_flg
        WHEN NOT MATCHED THEN INSERT ( 
        cmtmnt_amt,cmtmnt_closure_note,cmtmnt_created_dttm,cmtmnt_desc,cmtmnt_id,cmtmnt_nm,cmtmnt_no,cmtmnt_outstanding_amt,cmtmnt_overspent_amt,cmtmnt_payment_dttm,cmtmnt_status,created_by_usernm,created_dttm,last_modified_dttm,last_modified_usernm,load_dttm,planning_currency_cd,planning_id,planning_nm,vendor_amt,vendor_currency_cd,vendor_id,vendor_nm,vendor_number,vendor_obsolete_flg
         ) values ( 
        commitment_details_tmp.cmtmnt_amt,commitment_details_tmp.cmtmnt_closure_note,commitment_details_tmp.cmtmnt_created_dttm,commitment_details_tmp.cmtmnt_desc,commitment_details_tmp.cmtmnt_id,commitment_details_tmp.cmtmnt_nm,commitment_details_tmp.cmtmnt_no,commitment_details_tmp.cmtmnt_outstanding_amt,commitment_details_tmp.cmtmnt_overspent_amt,commitment_details_tmp.cmtmnt_payment_dttm,commitment_details_tmp.cmtmnt_status,commitment_details_tmp.created_by_usernm,commitment_details_tmp.created_dttm,commitment_details_tmp.last_modified_dttm,commitment_details_tmp.last_modified_usernm,commitment_details_tmp.load_dttm,commitment_details_tmp.planning_currency_cd,commitment_details_tmp.planning_id,commitment_details_tmp.planning_nm,commitment_details_tmp.vendor_amt,commitment_details_tmp.vendor_currency_cd,commitment_details_tmp.vendor_id,commitment_details_tmp.vendor_nm,commitment_details_tmp.vendor_number,commitment_details_tmp.vendor_obsolete_flg
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :commitment_details_tmp          , commitment_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..commitment_details_tmp          ;
    QUIT;
    %put ######## Staging table: commitment_details_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..commitment_details;
      DROP TABLE work.commitment_details;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table commitment_details;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..commitment_line_items) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..commitment_line_items_tmp       ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..commitment_line_items_tmp       ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=commitment_line_items, table_keys=%str(cmtmnt_id,item_nm,item_number,planning_id), out_table=work.commitment_line_items);
 data &tmplib..commitment_line_items_tmp       ;
     set work.commitment_line_items;
  if cmtmnt_created_dttm ne . then cmtmnt_created_dttm = tzoneu2s(cmtmnt_created_dttm,&timeZone_Value.);if cmtmnt_payment_dttm ne . then cmtmnt_payment_dttm = tzoneu2s(cmtmnt_payment_dttm,&timeZone_Value.);if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if cmtmnt_id='' then cmtmnt_id='-'; if item_nm='' then item_nm='-'; if planning_id='' then planning_id='-';
 run;
 %ErrCheck (Failed to Append Data to :commitment_line_items_tmp       , commitment_line_items);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..commitment_line_items using &tmpdbschema..commitment_line_items_tmp       
         ON (commitment_line_items.cmtmnt_id=commitment_line_items_tmp.cmtmnt_id and commitment_line_items.item_nm=commitment_line_items_tmp.item_nm and commitment_line_items.item_number=commitment_line_items_tmp.item_number and commitment_line_items.planning_id=commitment_line_items_tmp.planning_id)
        WHEN MATCHED THEN  
        UPDATE SET cc_allocated_amt = commitment_line_items_tmp.cc_allocated_amt , cc_available_amt = commitment_line_items_tmp.cc_available_amt , cc_desc = commitment_line_items_tmp.cc_desc , cc_nm = commitment_line_items_tmp.cc_nm , cc_owner_usernm = commitment_line_items_tmp.cc_owner_usernm , cc_recon_alloc_amt = commitment_line_items_tmp.cc_recon_alloc_amt , ccat_nm = commitment_line_items_tmp.ccat_nm , cmtmnt_amt = commitment_line_items_tmp.cmtmnt_amt , cmtmnt_closure_note = commitment_line_items_tmp.cmtmnt_closure_note , cmtmnt_created_dttm = commitment_line_items_tmp.cmtmnt_created_dttm , cmtmnt_desc = commitment_line_items_tmp.cmtmnt_desc , cmtmnt_nm = commitment_line_items_tmp.cmtmnt_nm , cmtmnt_no = commitment_line_items_tmp.cmtmnt_no , cmtmnt_outstanding_amt = commitment_line_items_tmp.cmtmnt_outstanding_amt , cmtmnt_overspent_amt = commitment_line_items_tmp.cmtmnt_overspent_amt , cmtmnt_payment_dttm = commitment_line_items_tmp.cmtmnt_payment_dttm , cmtmnt_status = commitment_line_items_tmp.cmtmnt_status , cost_center_id = commitment_line_items_tmp.cost_center_id , created_by_usernm = commitment_line_items_tmp.created_by_usernm , created_dttm = commitment_line_items_tmp.created_dttm , fin_acc_nm = commitment_line_items_tmp.fin_acc_nm , gen_ledger_cd = commitment_line_items_tmp.gen_ledger_cd , item_alloc_amt = commitment_line_items_tmp.item_alloc_amt , item_alloc_unit = commitment_line_items_tmp.item_alloc_unit , item_qty = commitment_line_items_tmp.item_qty , item_rate = commitment_line_items_tmp.item_rate , item_vend_alloc_amt = commitment_line_items_tmp.item_vend_alloc_amt , item_vend_alloc_unit = commitment_line_items_tmp.item_vend_alloc_unit , last_modified_dttm = commitment_line_items_tmp.last_modified_dttm , last_modified_usernm = commitment_line_items_tmp.last_modified_usernm , load_dttm = commitment_line_items_tmp.load_dttm , planning_currency_cd = commitment_line_items_tmp.planning_currency_cd , planning_nm = commitment_line_items_tmp.planning_nm , vendor_amt = commitment_line_items_tmp.vendor_amt , vendor_currency_cd = commitment_line_items_tmp.vendor_currency_cd , vendor_id = commitment_line_items_tmp.vendor_id , vendor_nm = commitment_line_items_tmp.vendor_nm , vendor_number = commitment_line_items_tmp.vendor_number , vendor_obsolete_flg = commitment_line_items_tmp.vendor_obsolete_flg
        WHEN NOT MATCHED THEN INSERT ( 
        cc_allocated_amt,cc_available_amt,cc_desc,cc_nm,cc_owner_usernm,cc_recon_alloc_amt,ccat_nm,cmtmnt_amt,cmtmnt_closure_note,cmtmnt_created_dttm,cmtmnt_desc,cmtmnt_id,cmtmnt_nm,cmtmnt_no,cmtmnt_outstanding_amt,cmtmnt_overspent_amt,cmtmnt_payment_dttm,cmtmnt_status,cost_center_id,created_by_usernm,created_dttm,fin_acc_nm,gen_ledger_cd,item_alloc_amt,item_alloc_unit,item_nm,item_number,item_qty,item_rate,item_vend_alloc_amt,item_vend_alloc_unit,last_modified_dttm,last_modified_usernm,load_dttm,planning_currency_cd,planning_id,planning_nm,vendor_amt,vendor_currency_cd,vendor_id,vendor_nm,vendor_number,vendor_obsolete_flg
         ) values ( 
        commitment_line_items_tmp.cc_allocated_amt,commitment_line_items_tmp.cc_available_amt,commitment_line_items_tmp.cc_desc,commitment_line_items_tmp.cc_nm,commitment_line_items_tmp.cc_owner_usernm,commitment_line_items_tmp.cc_recon_alloc_amt,commitment_line_items_tmp.ccat_nm,commitment_line_items_tmp.cmtmnt_amt,commitment_line_items_tmp.cmtmnt_closure_note,commitment_line_items_tmp.cmtmnt_created_dttm,commitment_line_items_tmp.cmtmnt_desc,commitment_line_items_tmp.cmtmnt_id,commitment_line_items_tmp.cmtmnt_nm,commitment_line_items_tmp.cmtmnt_no,commitment_line_items_tmp.cmtmnt_outstanding_amt,commitment_line_items_tmp.cmtmnt_overspent_amt,commitment_line_items_tmp.cmtmnt_payment_dttm,commitment_line_items_tmp.cmtmnt_status,commitment_line_items_tmp.cost_center_id,commitment_line_items_tmp.created_by_usernm,commitment_line_items_tmp.created_dttm,commitment_line_items_tmp.fin_acc_nm,commitment_line_items_tmp.gen_ledger_cd,commitment_line_items_tmp.item_alloc_amt,commitment_line_items_tmp.item_alloc_unit,commitment_line_items_tmp.item_nm,commitment_line_items_tmp.item_number,commitment_line_items_tmp.item_qty,commitment_line_items_tmp.item_rate,commitment_line_items_tmp.item_vend_alloc_amt,commitment_line_items_tmp.item_vend_alloc_unit,commitment_line_items_tmp.last_modified_dttm,commitment_line_items_tmp.last_modified_usernm,commitment_line_items_tmp.load_dttm,commitment_line_items_tmp.planning_currency_cd,commitment_line_items_tmp.planning_id,commitment_line_items_tmp.planning_nm,commitment_line_items_tmp.vendor_amt,commitment_line_items_tmp.vendor_currency_cd,commitment_line_items_tmp.vendor_id,commitment_line_items_tmp.vendor_nm,commitment_line_items_tmp.vendor_number,commitment_line_items_tmp.vendor_obsolete_flg
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :commitment_line_items_tmp       , commitment_line_items, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..commitment_line_items_tmp       ;
    QUIT;
    %put ######## Staging table: commitment_line_items_tmp        Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..commitment_line_items;
      DROP TABLE work.commitment_line_items;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table commitment_line_items;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..commitment_line_items_ccbdgt) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..commitment_line_items_ccbdgt_tmp) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..commitment_line_items_ccbdgt_tmp;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=commitment_line_items_ccbdgt, table_keys=%str(cmtmnt_id,item_number), out_table=work.commitment_line_items_ccbdgt);
 data &tmplib..commitment_line_items_ccbdgt_tmp;
     set work.commitment_line_items_ccbdgt;
  if cmtmnt_created_dttm ne . then cmtmnt_created_dttm = tzoneu2s(cmtmnt_created_dttm,&timeZone_Value.);if cmtmnt_payment_dttm ne . then cmtmnt_payment_dttm = tzoneu2s(cmtmnt_payment_dttm,&timeZone_Value.);if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if cmtmnt_id='' then cmtmnt_id='-';
 run;
 %ErrCheck (Failed to Append Data to :commitment_line_items_ccbdgt_tmp, commitment_line_items_ccbdgt);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..commitment_line_items_ccbdgt using &tmpdbschema..commitment_line_items_ccbdgt_tmp
         ON (commitment_line_items_ccbdgt.cmtmnt_id=commitment_line_items_ccbdgt_tmp.cmtmnt_id and commitment_line_items_ccbdgt.item_number=commitment_line_items_ccbdgt_tmp.item_number)
        WHEN MATCHED THEN  
        UPDATE SET cc_allocated_amt = commitment_line_items_ccbdgt_tmp.cc_allocated_amt , cc_available_amt = commitment_line_items_ccbdgt_tmp.cc_available_amt , cc_bdgt_amt = commitment_line_items_ccbdgt_tmp.cc_bdgt_amt , cc_bdgt_budget_amt = commitment_line_items_ccbdgt_tmp.cc_bdgt_budget_amt , cc_bdgt_budget_desc = commitment_line_items_ccbdgt_tmp.cc_bdgt_budget_desc , cc_bdgt_cmtmnt_invoice_amt = commitment_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_invoice_amt , cc_bdgt_cmtmnt_invoice_cnt = commitment_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_invoice_cnt , cc_bdgt_cmtmnt_outstanding_amt = commitment_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_outstanding_amt , cc_bdgt_cmtmnt_overspent_amt = commitment_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_overspent_amt , cc_bdgt_committed_amt = commitment_line_items_ccbdgt_tmp.cc_bdgt_committed_amt , cc_bdgt_direct_invoice_amt = commitment_line_items_ccbdgt_tmp.cc_bdgt_direct_invoice_amt , cc_bdgt_invoiced_amt = commitment_line_items_ccbdgt_tmp.cc_bdgt_invoiced_amt , cc_desc = commitment_line_items_ccbdgt_tmp.cc_desc , cc_nm = commitment_line_items_ccbdgt_tmp.cc_nm , cc_number = commitment_line_items_ccbdgt_tmp.cc_number , cc_obsolete_flg = commitment_line_items_ccbdgt_tmp.cc_obsolete_flg , cc_owner_usernm = commitment_line_items_ccbdgt_tmp.cc_owner_usernm , cc_recon_alloc_amt = commitment_line_items_ccbdgt_tmp.cc_recon_alloc_amt , ccat_nm = commitment_line_items_ccbdgt_tmp.ccat_nm , cmtmnt_amt = commitment_line_items_ccbdgt_tmp.cmtmnt_amt , cmtmnt_closure_note = commitment_line_items_ccbdgt_tmp.cmtmnt_closure_note , cmtmnt_created_dttm = commitment_line_items_ccbdgt_tmp.cmtmnt_created_dttm , cmtmnt_desc = commitment_line_items_ccbdgt_tmp.cmtmnt_desc , cmtmnt_nm = commitment_line_items_ccbdgt_tmp.cmtmnt_nm , cmtmnt_no = commitment_line_items_ccbdgt_tmp.cmtmnt_no , cmtmnt_outstanding_amt = commitment_line_items_ccbdgt_tmp.cmtmnt_outstanding_amt , cmtmnt_overspent_amt = commitment_line_items_ccbdgt_tmp.cmtmnt_overspent_amt , cmtmnt_payment_dttm = commitment_line_items_ccbdgt_tmp.cmtmnt_payment_dttm , cmtmnt_status = commitment_line_items_ccbdgt_tmp.cmtmnt_status , cost_center_id = commitment_line_items_ccbdgt_tmp.cost_center_id , created_by_usernm = commitment_line_items_ccbdgt_tmp.created_by_usernm , created_dttm = commitment_line_items_ccbdgt_tmp.created_dttm , fin_acc_nm = commitment_line_items_ccbdgt_tmp.fin_acc_nm , fp_cls_ver = commitment_line_items_ccbdgt_tmp.fp_cls_ver , fp_desc = commitment_line_items_ccbdgt_tmp.fp_desc , fp_end_dt = commitment_line_items_ccbdgt_tmp.fp_end_dt , fp_id = commitment_line_items_ccbdgt_tmp.fp_id , fp_nm = commitment_line_items_ccbdgt_tmp.fp_nm , fp_obsolete_flg = commitment_line_items_ccbdgt_tmp.fp_obsolete_flg , fp_start_dt = commitment_line_items_ccbdgt_tmp.fp_start_dt , gen_ledger_cd = commitment_line_items_ccbdgt_tmp.gen_ledger_cd , item_alloc_amt = commitment_line_items_ccbdgt_tmp.item_alloc_amt , item_alloc_unit = commitment_line_items_ccbdgt_tmp.item_alloc_unit , item_nm = commitment_line_items_ccbdgt_tmp.item_nm , item_qty = commitment_line_items_ccbdgt_tmp.item_qty , item_rate = commitment_line_items_ccbdgt_tmp.item_rate , item_vend_alloc_amt = commitment_line_items_ccbdgt_tmp.item_vend_alloc_amt , item_vend_alloc_unit = commitment_line_items_ccbdgt_tmp.item_vend_alloc_unit , last_modified_dttm = commitment_line_items_ccbdgt_tmp.last_modified_dttm , last_modified_usernm = commitment_line_items_ccbdgt_tmp.last_modified_usernm , load_dttm = commitment_line_items_ccbdgt_tmp.load_dttm , planning_currency_cd = commitment_line_items_ccbdgt_tmp.planning_currency_cd , planning_id = commitment_line_items_ccbdgt_tmp.planning_id , planning_nm = commitment_line_items_ccbdgt_tmp.planning_nm , vendor_amt = commitment_line_items_ccbdgt_tmp.vendor_amt , vendor_currency_cd = commitment_line_items_ccbdgt_tmp.vendor_currency_cd , vendor_id = commitment_line_items_ccbdgt_tmp.vendor_id , vendor_nm = commitment_line_items_ccbdgt_tmp.vendor_nm , vendor_number = commitment_line_items_ccbdgt_tmp.vendor_number , vendor_obsolete_flg = commitment_line_items_ccbdgt_tmp.vendor_obsolete_flg
        WHEN NOT MATCHED THEN INSERT ( 
        cc_allocated_amt,cc_available_amt,cc_bdgt_amt,cc_bdgt_budget_amt,cc_bdgt_budget_desc,cc_bdgt_cmtmnt_invoice_amt,cc_bdgt_cmtmnt_invoice_cnt,cc_bdgt_cmtmnt_outstanding_amt,cc_bdgt_cmtmnt_overspent_amt,cc_bdgt_committed_amt,cc_bdgt_direct_invoice_amt,cc_bdgt_invoiced_amt,cc_desc,cc_nm,cc_number,cc_obsolete_flg,cc_owner_usernm,cc_recon_alloc_amt,ccat_nm,cmtmnt_amt,cmtmnt_closure_note,cmtmnt_created_dttm,cmtmnt_desc,cmtmnt_id,cmtmnt_nm,cmtmnt_no,cmtmnt_outstanding_amt,cmtmnt_overspent_amt,cmtmnt_payment_dttm,cmtmnt_status,cost_center_id,created_by_usernm,created_dttm,fin_acc_nm,fp_cls_ver,fp_desc,fp_end_dt,fp_id,fp_nm,fp_obsolete_flg,fp_start_dt,gen_ledger_cd,item_alloc_amt,item_alloc_unit,item_nm,item_number,item_qty,item_rate,item_vend_alloc_amt,item_vend_alloc_unit,last_modified_dttm,last_modified_usernm,load_dttm,planning_currency_cd,planning_id,planning_nm,vendor_amt,vendor_currency_cd,vendor_id,vendor_nm,vendor_number,vendor_obsolete_flg
         ) values ( 
        commitment_line_items_ccbdgt_tmp.cc_allocated_amt,commitment_line_items_ccbdgt_tmp.cc_available_amt,commitment_line_items_ccbdgt_tmp.cc_bdgt_amt,commitment_line_items_ccbdgt_tmp.cc_bdgt_budget_amt,commitment_line_items_ccbdgt_tmp.cc_bdgt_budget_desc,commitment_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_invoice_amt,commitment_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_invoice_cnt,commitment_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_outstanding_amt,commitment_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_overspent_amt,commitment_line_items_ccbdgt_tmp.cc_bdgt_committed_amt,commitment_line_items_ccbdgt_tmp.cc_bdgt_direct_invoice_amt,commitment_line_items_ccbdgt_tmp.cc_bdgt_invoiced_amt,commitment_line_items_ccbdgt_tmp.cc_desc,commitment_line_items_ccbdgt_tmp.cc_nm,commitment_line_items_ccbdgt_tmp.cc_number,commitment_line_items_ccbdgt_tmp.cc_obsolete_flg,commitment_line_items_ccbdgt_tmp.cc_owner_usernm,commitment_line_items_ccbdgt_tmp.cc_recon_alloc_amt,commitment_line_items_ccbdgt_tmp.ccat_nm,commitment_line_items_ccbdgt_tmp.cmtmnt_amt,commitment_line_items_ccbdgt_tmp.cmtmnt_closure_note,commitment_line_items_ccbdgt_tmp.cmtmnt_created_dttm,commitment_line_items_ccbdgt_tmp.cmtmnt_desc,commitment_line_items_ccbdgt_tmp.cmtmnt_id,commitment_line_items_ccbdgt_tmp.cmtmnt_nm,commitment_line_items_ccbdgt_tmp.cmtmnt_no,commitment_line_items_ccbdgt_tmp.cmtmnt_outstanding_amt,commitment_line_items_ccbdgt_tmp.cmtmnt_overspent_amt,commitment_line_items_ccbdgt_tmp.cmtmnt_payment_dttm,commitment_line_items_ccbdgt_tmp.cmtmnt_status,commitment_line_items_ccbdgt_tmp.cost_center_id,commitment_line_items_ccbdgt_tmp.created_by_usernm,commitment_line_items_ccbdgt_tmp.created_dttm,commitment_line_items_ccbdgt_tmp.fin_acc_nm,commitment_line_items_ccbdgt_tmp.fp_cls_ver,commitment_line_items_ccbdgt_tmp.fp_desc,commitment_line_items_ccbdgt_tmp.fp_end_dt,commitment_line_items_ccbdgt_tmp.fp_id,commitment_line_items_ccbdgt_tmp.fp_nm,commitment_line_items_ccbdgt_tmp.fp_obsolete_flg,commitment_line_items_ccbdgt_tmp.fp_start_dt,commitment_line_items_ccbdgt_tmp.gen_ledger_cd,commitment_line_items_ccbdgt_tmp.item_alloc_amt,commitment_line_items_ccbdgt_tmp.item_alloc_unit,commitment_line_items_ccbdgt_tmp.item_nm,commitment_line_items_ccbdgt_tmp.item_number,commitment_line_items_ccbdgt_tmp.item_qty,commitment_line_items_ccbdgt_tmp.item_rate,commitment_line_items_ccbdgt_tmp.item_vend_alloc_amt,commitment_line_items_ccbdgt_tmp.item_vend_alloc_unit,commitment_line_items_ccbdgt_tmp.last_modified_dttm,commitment_line_items_ccbdgt_tmp.last_modified_usernm,commitment_line_items_ccbdgt_tmp.load_dttm,commitment_line_items_ccbdgt_tmp.planning_currency_cd,commitment_line_items_ccbdgt_tmp.planning_id,commitment_line_items_ccbdgt_tmp.planning_nm,commitment_line_items_ccbdgt_tmp.vendor_amt,commitment_line_items_ccbdgt_tmp.vendor_currency_cd,commitment_line_items_ccbdgt_tmp.vendor_id,commitment_line_items_ccbdgt_tmp.vendor_nm,commitment_line_items_ccbdgt_tmp.vendor_number,commitment_line_items_ccbdgt_tmp.vendor_obsolete_flg
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :commitment_line_items_ccbdgt_tmp, commitment_line_items_ccbdgt, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..commitment_line_items_ccbdgt_tmp;
    QUIT;
    %put ######## Staging table: commitment_line_items_ccbdgt_tmp Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..commitment_line_items_ccbdgt;
      DROP TABLE work.commitment_line_items_ccbdgt;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table commitment_line_items_ccbdgt;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..contact_history) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..contact_history_tmp             ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..contact_history_tmp             ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=contact_history, table_keys=%str(contact_id), out_table=work.contact_history);
 data &tmplib..contact_history_tmp             ;
     set work.contact_history;
  if contact_dttm ne . then contact_dttm = tzoneu2s(contact_dttm,&timeZone_Value.);if contact_dttm_tz ne . then contact_dttm_tz = tzoneu2s(contact_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if contact_id='' then contact_id='-';
 run;
 %ErrCheck (Failed to Append Data to :contact_history_tmp             , contact_history);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..contact_history using &tmpdbschema..contact_history_tmp             
         ON (contact_history.contact_id=contact_history_tmp.contact_id)
        WHEN MATCHED THEN  
        UPDATE SET aud_occurrence_id = contact_history_tmp.aud_occurrence_id , audience_id = contact_history_tmp.audience_id , contact_channel_nm = contact_history_tmp.contact_channel_nm , contact_dttm = contact_history_tmp.contact_dttm , contact_dttm_tz = contact_history_tmp.contact_dttm_tz , contact_nm = contact_history_tmp.contact_nm , context_type_nm = contact_history_tmp.context_type_nm , context_val = contact_history_tmp.context_val , control_group_flg = contact_history_tmp.control_group_flg , creative_id = contact_history_tmp.creative_id , detail_id_hex = contact_history_tmp.detail_id_hex , event_designed_id = contact_history_tmp.event_designed_id , identity_id = contact_history_tmp.identity_id , journey_id = contact_history_tmp.journey_id , journey_occurrence_id = contact_history_tmp.journey_occurrence_id , load_dttm = contact_history_tmp.load_dttm , message_id = contact_history_tmp.message_id , occurrence_id = contact_history_tmp.occurrence_id , parent_event_designed_id = contact_history_tmp.parent_event_designed_id , properties_map_doc = contact_history_tmp.properties_map_doc , response_tracking_cd = contact_history_tmp.response_tracking_cd , session_id_hex = contact_history_tmp.session_id_hex , task_id = contact_history_tmp.task_id , task_version_id = contact_history_tmp.task_version_id , visit_id_hex = contact_history_tmp.visit_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        aud_occurrence_id,audience_id,contact_channel_nm,contact_dttm,contact_dttm_tz,contact_id,contact_nm,context_type_nm,context_val,control_group_flg,creative_id,detail_id_hex,event_designed_id,identity_id,journey_id,journey_occurrence_id,load_dttm,message_id,occurrence_id,parent_event_designed_id,properties_map_doc,response_tracking_cd,session_id_hex,task_id,task_version_id,visit_id_hex
         ) values ( 
        contact_history_tmp.aud_occurrence_id,contact_history_tmp.audience_id,contact_history_tmp.contact_channel_nm,contact_history_tmp.contact_dttm,contact_history_tmp.contact_dttm_tz,contact_history_tmp.contact_id,contact_history_tmp.contact_nm,contact_history_tmp.context_type_nm,contact_history_tmp.context_val,contact_history_tmp.control_group_flg,contact_history_tmp.creative_id,contact_history_tmp.detail_id_hex,contact_history_tmp.event_designed_id,contact_history_tmp.identity_id,contact_history_tmp.journey_id,contact_history_tmp.journey_occurrence_id,contact_history_tmp.load_dttm,contact_history_tmp.message_id,contact_history_tmp.occurrence_id,contact_history_tmp.parent_event_designed_id,contact_history_tmp.properties_map_doc,contact_history_tmp.response_tracking_cd,contact_history_tmp.session_id_hex,contact_history_tmp.task_id,contact_history_tmp.task_version_id,contact_history_tmp.visit_id_hex
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :contact_history_tmp             , contact_history, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..contact_history_tmp             ;
    QUIT;
    %put ######## Staging table: contact_history_tmp              Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..contact_history;
      DROP TABLE work.contact_history;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table contact_history;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..conversion_milestone) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..conversion_milestone_tmp        ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..conversion_milestone_tmp        ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=conversion_milestone, table_keys=%str(event_id), out_table=work.conversion_milestone);
 data &tmplib..conversion_milestone_tmp        ;
     set work.conversion_milestone;
  if conversion_milestone_dttm ne . then conversion_milestone_dttm = tzoneu2s(conversion_milestone_dttm,&timeZone_Value.);if conversion_milestone_dttm_tz ne . then conversion_milestone_dttm_tz = tzoneu2s(conversion_milestone_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :conversion_milestone_tmp        , conversion_milestone);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..conversion_milestone using &tmpdbschema..conversion_milestone_tmp        
         ON (conversion_milestone.event_id=conversion_milestone_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET activity_id = conversion_milestone_tmp.activity_id , analysis_group_id = conversion_milestone_tmp.analysis_group_id , aud_occurrence_id = conversion_milestone_tmp.aud_occurrence_id , audience_id = conversion_milestone_tmp.audience_id , channel_nm = conversion_milestone_tmp.channel_nm , channel_user_id = conversion_milestone_tmp.channel_user_id , context_type_nm = conversion_milestone_tmp.context_type_nm , context_val = conversion_milestone_tmp.context_val , control_group_flg = conversion_milestone_tmp.control_group_flg , conversion_milestone_dttm = conversion_milestone_tmp.conversion_milestone_dttm , conversion_milestone_dttm_tz = conversion_milestone_tmp.conversion_milestone_dttm_tz , creative_id = conversion_milestone_tmp.creative_id , creative_version_id = conversion_milestone_tmp.creative_version_id , detail_id_hex = conversion_milestone_tmp.detail_id_hex , event_designed_id = conversion_milestone_tmp.event_designed_id , event_nm = conversion_milestone_tmp.event_nm , goal_id = conversion_milestone_tmp.goal_id , identity_id = conversion_milestone_tmp.identity_id , journey_id = conversion_milestone_tmp.journey_id , journey_occurrence_id = conversion_milestone_tmp.journey_occurrence_id , load_dttm = conversion_milestone_tmp.load_dttm , message_id = conversion_milestone_tmp.message_id , message_version_id = conversion_milestone_tmp.message_version_id , mobile_app_id = conversion_milestone_tmp.mobile_app_id , occurrence_id = conversion_milestone_tmp.occurrence_id , parent_event_designed_id = conversion_milestone_tmp.parent_event_designed_id , properties_map_doc = conversion_milestone_tmp.properties_map_doc , rec_group_id = conversion_milestone_tmp.rec_group_id , reserved_1_txt = conversion_milestone_tmp.reserved_1_txt , reserved_2_txt = conversion_milestone_tmp.reserved_2_txt , response_tracking_cd = conversion_milestone_tmp.response_tracking_cd , segment_id = conversion_milestone_tmp.segment_id , segment_version_id = conversion_milestone_tmp.segment_version_id , session_id_hex = conversion_milestone_tmp.session_id_hex , spot_id = conversion_milestone_tmp.spot_id , subject_line_txt = conversion_milestone_tmp.subject_line_txt , task_id = conversion_milestone_tmp.task_id , task_version_id = conversion_milestone_tmp.task_version_id , test_flg = conversion_milestone_tmp.test_flg , total_cost_amt = conversion_milestone_tmp.total_cost_amt , visit_id_hex = conversion_milestone_tmp.visit_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        activity_id,analysis_group_id,aud_occurrence_id,audience_id,channel_nm,channel_user_id,context_type_nm,context_val,control_group_flg,conversion_milestone_dttm,conversion_milestone_dttm_tz,creative_id,creative_version_id,detail_id_hex,event_designed_id,event_id,event_nm,goal_id,identity_id,journey_id,journey_occurrence_id,load_dttm,message_id,message_version_id,mobile_app_id,occurrence_id,parent_event_designed_id,properties_map_doc,rec_group_id,reserved_1_txt,reserved_2_txt,response_tracking_cd,segment_id,segment_version_id,session_id_hex,spot_id,subject_line_txt,task_id,task_version_id,test_flg,total_cost_amt,visit_id_hex
         ) values ( 
        conversion_milestone_tmp.activity_id,conversion_milestone_tmp.analysis_group_id,conversion_milestone_tmp.aud_occurrence_id,conversion_milestone_tmp.audience_id,conversion_milestone_tmp.channel_nm,conversion_milestone_tmp.channel_user_id,conversion_milestone_tmp.context_type_nm,conversion_milestone_tmp.context_val,conversion_milestone_tmp.control_group_flg,conversion_milestone_tmp.conversion_milestone_dttm,conversion_milestone_tmp.conversion_milestone_dttm_tz,conversion_milestone_tmp.creative_id,conversion_milestone_tmp.creative_version_id,conversion_milestone_tmp.detail_id_hex,conversion_milestone_tmp.event_designed_id,conversion_milestone_tmp.event_id,conversion_milestone_tmp.event_nm,conversion_milestone_tmp.goal_id,conversion_milestone_tmp.identity_id,conversion_milestone_tmp.journey_id,conversion_milestone_tmp.journey_occurrence_id,conversion_milestone_tmp.load_dttm,conversion_milestone_tmp.message_id,conversion_milestone_tmp.message_version_id,conversion_milestone_tmp.mobile_app_id,conversion_milestone_tmp.occurrence_id,conversion_milestone_tmp.parent_event_designed_id,conversion_milestone_tmp.properties_map_doc,conversion_milestone_tmp.rec_group_id,conversion_milestone_tmp.reserved_1_txt,conversion_milestone_tmp.reserved_2_txt,conversion_milestone_tmp.response_tracking_cd,conversion_milestone_tmp.segment_id,conversion_milestone_tmp.segment_version_id,conversion_milestone_tmp.session_id_hex,conversion_milestone_tmp.spot_id,conversion_milestone_tmp.subject_line_txt,conversion_milestone_tmp.task_id,conversion_milestone_tmp.task_version_id,conversion_milestone_tmp.test_flg,conversion_milestone_tmp.total_cost_amt,conversion_milestone_tmp.visit_id_hex
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :conversion_milestone_tmp        , conversion_milestone, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..conversion_milestone_tmp        ;
    QUIT;
    %put ######## Staging table: conversion_milestone_tmp         Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..conversion_milestone;
      DROP TABLE work.conversion_milestone;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table conversion_milestone;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..custom_events) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..custom_events_tmp               ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..custom_events_tmp               ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=custom_events, table_keys=%str(event_id), out_table=work.custom_events);
 data &tmplib..custom_events_tmp               ;
     set work.custom_events;
  if custom_event_dttm ne . then custom_event_dttm = tzoneu2s(custom_event_dttm,&timeZone_Value.);if custom_event_dttm_tz ne . then custom_event_dttm_tz = tzoneu2s(custom_event_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :custom_events_tmp               , custom_events);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..custom_events using &tmpdbschema..custom_events_tmp               
         ON (custom_events.event_id=custom_events_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET channel_nm = custom_events_tmp.channel_nm , channel_user_id = custom_events_tmp.channel_user_id , custom_event_dttm = custom_events_tmp.custom_event_dttm , custom_event_dttm_tz = custom_events_tmp.custom_event_dttm_tz , custom_event_group_nm = custom_events_tmp.custom_event_group_nm , custom_event_nm = custom_events_tmp.custom_event_nm , custom_events_sk = custom_events_tmp.custom_events_sk , custom_revenue_amt = custom_events_tmp.custom_revenue_amt , detail_id = custom_events_tmp.detail_id , detail_id_hex = custom_events_tmp.detail_id_hex , event_designed_id = custom_events_tmp.event_designed_id , event_key_cd = custom_events_tmp.event_key_cd , event_nm = custom_events_tmp.event_nm , event_source_cd = custom_events_tmp.event_source_cd , event_type_nm = custom_events_tmp.event_type_nm , identity_id = custom_events_tmp.identity_id , load_dttm = custom_events_tmp.load_dttm , mobile_app_id = custom_events_tmp.mobile_app_id , page_id = custom_events_tmp.page_id , properties_map_doc = custom_events_tmp.properties_map_doc , reserved_1_txt = custom_events_tmp.reserved_1_txt , reserved_2_txt = custom_events_tmp.reserved_2_txt , session_id = custom_events_tmp.session_id , session_id_hex = custom_events_tmp.session_id_hex , visit_id = custom_events_tmp.visit_id , visit_id_hex = custom_events_tmp.visit_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        channel_nm,channel_user_id,custom_event_dttm,custom_event_dttm_tz,custom_event_group_nm,custom_event_nm,custom_events_sk,custom_revenue_amt,detail_id,detail_id_hex,event_designed_id,event_id,event_key_cd,event_nm,event_source_cd,event_type_nm,identity_id,load_dttm,mobile_app_id,page_id,properties_map_doc,reserved_1_txt,reserved_2_txt,session_id,session_id_hex,visit_id,visit_id_hex
         ) values ( 
        custom_events_tmp.channel_nm,custom_events_tmp.channel_user_id,custom_events_tmp.custom_event_dttm,custom_events_tmp.custom_event_dttm_tz,custom_events_tmp.custom_event_group_nm,custom_events_tmp.custom_event_nm,custom_events_tmp.custom_events_sk,custom_events_tmp.custom_revenue_amt,custom_events_tmp.detail_id,custom_events_tmp.detail_id_hex,custom_events_tmp.event_designed_id,custom_events_tmp.event_id,custom_events_tmp.event_key_cd,custom_events_tmp.event_nm,custom_events_tmp.event_source_cd,custom_events_tmp.event_type_nm,custom_events_tmp.identity_id,custom_events_tmp.load_dttm,custom_events_tmp.mobile_app_id,custom_events_tmp.page_id,custom_events_tmp.properties_map_doc,custom_events_tmp.reserved_1_txt,custom_events_tmp.reserved_2_txt,custom_events_tmp.session_id,custom_events_tmp.session_id_hex,custom_events_tmp.visit_id,custom_events_tmp.visit_id_hex
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :custom_events_tmp               , custom_events, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..custom_events_tmp               ;
    QUIT;
    %put ######## Staging table: custom_events_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..custom_events;
      DROP TABLE work.custom_events;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table custom_events;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..custom_events_ext) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..custom_events_ext_tmp           ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..custom_events_ext_tmp           ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=custom_events_ext, table_keys=%str(custom_events_sk), out_table=work.custom_events_ext);
 data &tmplib..custom_events_ext_tmp           ;
     set work.custom_events_ext;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if custom_events_sk='' then custom_events_sk='-';
 run;
 %ErrCheck (Failed to Append Data to :custom_events_ext_tmp           , custom_events_ext);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..custom_events_ext using &tmpdbschema..custom_events_ext_tmp           
         ON (custom_events_ext.custom_events_sk=custom_events_ext_tmp.custom_events_sk)
        WHEN MATCHED THEN  
        UPDATE SET custom_revenue_amt = custom_events_ext_tmp.custom_revenue_amt , event_designed_id = custom_events_ext_tmp.event_designed_id , load_dttm = custom_events_ext_tmp.load_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        custom_events_sk,custom_revenue_amt,event_designed_id,load_dttm
         ) values ( 
        custom_events_ext_tmp.custom_events_sk,custom_events_ext_tmp.custom_revenue_amt,custom_events_ext_tmp.event_designed_id,custom_events_ext_tmp.load_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :custom_events_ext_tmp           , custom_events_ext, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..custom_events_ext_tmp           ;
    QUIT;
    %put ######## Staging table: custom_events_ext_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..custom_events_ext;
      DROP TABLE work.custom_events_ext;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table custom_events_ext;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..daily_usage) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..daily_usage_tmp                 ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..daily_usage_tmp                 ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=daily_usage, table_keys=%str(event_day), out_table=work.daily_usage);
 data &tmplib..daily_usage_tmp                 ;
     set work.daily_usage;
  if event_day='' then event_day='-';
 run;
 %ErrCheck (Failed to Append Data to :daily_usage_tmp                 , daily_usage);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..daily_usage using &tmpdbschema..daily_usage_tmp                 
         ON (daily_usage.event_day=daily_usage_tmp.event_day)
        WHEN MATCHED THEN  
        UPDATE SET admin_user_cnt = daily_usage_tmp.admin_user_cnt , api_usage_str = daily_usage_tmp.api_usage_str , asset_size = daily_usage_tmp.asset_size , audience_usage_cnt = daily_usage_tmp.audience_usage_cnt , bc_subjcnt_str = daily_usage_tmp.bc_subjcnt_str , customer_profiles_processed_str = daily_usage_tmp.customer_profiles_processed_str , db_size = daily_usage_tmp.db_size , email_preview_cnt = daily_usage_tmp.email_preview_cnt , email_send_cnt = daily_usage_tmp.email_send_cnt , facebook_ads_cnt = daily_usage_tmp.facebook_ads_cnt , google_ads_cnt = daily_usage_tmp.google_ads_cnt , linkedin_ads_cnt = daily_usage_tmp.linkedin_ads_cnt , mob_impr_cnt = daily_usage_tmp.mob_impr_cnt , mob_sesn_cnt = daily_usage_tmp.mob_sesn_cnt , mobile_in_app_msg_cnt = daily_usage_tmp.mobile_in_app_msg_cnt , mobile_push_cnt = daily_usage_tmp.mobile_push_cnt , outbound_api_cnt = daily_usage_tmp.outbound_api_cnt , plan_users_cnt = daily_usage_tmp.plan_users_cnt , web_impr_cnt = daily_usage_tmp.web_impr_cnt , web_sesn_cnt = daily_usage_tmp.web_sesn_cnt
        WHEN NOT MATCHED THEN INSERT ( 
        admin_user_cnt,api_usage_str,asset_size,audience_usage_cnt,bc_subjcnt_str,customer_profiles_processed_str,db_size,email_preview_cnt,email_send_cnt,event_day,facebook_ads_cnt,google_ads_cnt,linkedin_ads_cnt,mob_impr_cnt,mob_sesn_cnt,mobile_in_app_msg_cnt,mobile_push_cnt,outbound_api_cnt,plan_users_cnt,web_impr_cnt,web_sesn_cnt
         ) values ( 
        daily_usage_tmp.admin_user_cnt,daily_usage_tmp.api_usage_str,daily_usage_tmp.asset_size,daily_usage_tmp.audience_usage_cnt,daily_usage_tmp.bc_subjcnt_str,daily_usage_tmp.customer_profiles_processed_str,daily_usage_tmp.db_size,daily_usage_tmp.email_preview_cnt,daily_usage_tmp.email_send_cnt,daily_usage_tmp.event_day,daily_usage_tmp.facebook_ads_cnt,daily_usage_tmp.google_ads_cnt,daily_usage_tmp.linkedin_ads_cnt,daily_usage_tmp.mob_impr_cnt,daily_usage_tmp.mob_sesn_cnt,daily_usage_tmp.mobile_in_app_msg_cnt,daily_usage_tmp.mobile_push_cnt,daily_usage_tmp.outbound_api_cnt,daily_usage_tmp.plan_users_cnt,daily_usage_tmp.web_impr_cnt,daily_usage_tmp.web_sesn_cnt
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :daily_usage_tmp                 , daily_usage, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..daily_usage_tmp                 ;
    QUIT;
    %put ######## Staging table: daily_usage_tmp                  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..daily_usage;
      DROP TABLE work.daily_usage;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table daily_usage;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..data_view_details) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..data_view_details_tmp           ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..data_view_details_tmp           ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=data_view_details, table_keys=%str(detail_id_hex,event_id), out_table=work.data_view_details);
 data &tmplib..data_view_details_tmp           ;
     set work.data_view_details;
  if data_view_dttm ne . then data_view_dttm = tzoneu2s(data_view_dttm,&timeZone_Value.);if data_view_dttm_tz ne . then data_view_dttm_tz = tzoneu2s(data_view_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if detail_id_hex='' then detail_id_hex='-'; if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :data_view_details_tmp           , data_view_details);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..data_view_details using &tmpdbschema..data_view_details_tmp           
         ON (data_view_details.detail_id_hex=data_view_details_tmp.detail_id_hex and data_view_details.event_id=data_view_details_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET channel_user_id = data_view_details_tmp.channel_user_id , data_view_dttm = data_view_details_tmp.data_view_dttm , data_view_dttm_tz = data_view_details_tmp.data_view_dttm_tz , detail_id = data_view_details_tmp.detail_id , event_designed_id = data_view_details_tmp.event_designed_id , event_nm = data_view_details_tmp.event_nm , identity_id = data_view_details_tmp.identity_id , load_dttm = data_view_details_tmp.load_dttm , parent_event_designed_id = data_view_details_tmp.parent_event_designed_id , properties_map_doc = data_view_details_tmp.properties_map_doc , reserved_1_txt = data_view_details_tmp.reserved_1_txt , reserved_2_txt = data_view_details_tmp.reserved_2_txt , session_id = data_view_details_tmp.session_id , session_id_hex = data_view_details_tmp.session_id_hex , total_cost_amt = data_view_details_tmp.total_cost_amt , visit_id = data_view_details_tmp.visit_id , visit_id_hex = data_view_details_tmp.visit_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        channel_user_id,data_view_dttm,data_view_dttm_tz,detail_id,detail_id_hex,event_designed_id,event_id,event_nm,identity_id,load_dttm,parent_event_designed_id,properties_map_doc,reserved_1_txt,reserved_2_txt,session_id,session_id_hex,total_cost_amt,visit_id,visit_id_hex
         ) values ( 
        data_view_details_tmp.channel_user_id,data_view_details_tmp.data_view_dttm,data_view_details_tmp.data_view_dttm_tz,data_view_details_tmp.detail_id,data_view_details_tmp.detail_id_hex,data_view_details_tmp.event_designed_id,data_view_details_tmp.event_id,data_view_details_tmp.event_nm,data_view_details_tmp.identity_id,data_view_details_tmp.load_dttm,data_view_details_tmp.parent_event_designed_id,data_view_details_tmp.properties_map_doc,data_view_details_tmp.reserved_1_txt,data_view_details_tmp.reserved_2_txt,data_view_details_tmp.session_id,data_view_details_tmp.session_id_hex,data_view_details_tmp.total_cost_amt,data_view_details_tmp.visit_id,data_view_details_tmp.visit_id_hex
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :data_view_details_tmp           , data_view_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..data_view_details_tmp           ;
    QUIT;
    %put ######## Staging table: data_view_details_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..data_view_details;
      DROP TABLE work.data_view_details;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table data_view_details;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..dbt_adv_campaign_visitors) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..dbt_adv_campaign_visitors_tmp   ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..dbt_adv_campaign_visitors_tmp   ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=dbt_adv_campaign_visitors, table_keys=%str(session_id,visit_id), out_table=work.dbt_adv_campaign_visitors);
 data &tmplib..dbt_adv_campaign_visitors_tmp   ;
     set work.dbt_adv_campaign_visitors;
  if session_complete_load_dttm ne . then session_complete_load_dttm = tzoneu2s(session_complete_load_dttm,&timeZone_Value.);if session_start_dttm ne . then session_start_dttm = tzoneu2s(session_start_dttm,&timeZone_Value.);if session_start_dttm_tz ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz,&timeZone_Value.);if visit_dttm ne . then visit_dttm = tzoneu2s(visit_dttm,&timeZone_Value.);if visit_dttm_tz ne . then visit_dttm_tz = tzoneu2s(visit_dttm_tz,&timeZone_Value.) ;
  if session_id='' then session_id='-'; if visit_id='' then visit_id='-';
 run;
 %ErrCheck (Failed to Append Data to :dbt_adv_campaign_visitors_tmp   , dbt_adv_campaign_visitors);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..dbt_adv_campaign_visitors using &tmpdbschema..dbt_adv_campaign_visitors_tmp   
         ON (dbt_adv_campaign_visitors.session_id=dbt_adv_campaign_visitors_tmp.session_id and dbt_adv_campaign_visitors.visit_id=dbt_adv_campaign_visitors_tmp.visit_id)
        WHEN MATCHED THEN  
        UPDATE SET average_visit_duration = dbt_adv_campaign_visitors_tmp.average_visit_duration , bouncer = dbt_adv_campaign_visitors_tmp.bouncer , bouncers = dbt_adv_campaign_visitors_tmp.bouncers , br_browser_name = dbt_adv_campaign_visitors_tmp.br_browser_name , br_browser_version = dbt_adv_campaign_visitors_tmp.br_browser_version , co_conversions = dbt_adv_campaign_visitors_tmp.co_conversions , cu_customer_id = dbt_adv_campaign_visitors_tmp.cu_customer_id , device_name = dbt_adv_campaign_visitors_tmp.device_name , device_type = dbt_adv_campaign_visitors_tmp.device_type , ge_city = dbt_adv_campaign_visitors_tmp.ge_city , ge_country = dbt_adv_campaign_visitors_tmp.ge_country , ge_latitude = dbt_adv_campaign_visitors_tmp.ge_latitude , ge_longitude = dbt_adv_campaign_visitors_tmp.ge_longitude , ge_state_region = dbt_adv_campaign_visitors_tmp.ge_state_region , landing_page = dbt_adv_campaign_visitors_tmp.landing_page , landing_page_url = dbt_adv_campaign_visitors_tmp.landing_page_url , landing_page_url_domain = dbt_adv_campaign_visitors_tmp.landing_page_url_domain , new_visitors = dbt_adv_campaign_visitors_tmp.new_visitors , page_views = dbt_adv_campaign_visitors_tmp.page_views , pl_device_operating_system = dbt_adv_campaign_visitors_tmp.pl_device_operating_system , return_visitors = dbt_adv_campaign_visitors_tmp.return_visitors , rv_revenue = dbt_adv_campaign_visitors_tmp.rv_revenue , se_external_search_engine = dbt_adv_campaign_visitors_tmp.se_external_search_engine , se_external_search_engine_domain = dbt_adv_campaign_visitors_tmp.se_external_search_engine_domain , se_external_search_engine_phrase = dbt_adv_campaign_visitors_tmp.se_external_search_engine_phrase , session_complete_load_dttm = dbt_adv_campaign_visitors_tmp.session_complete_load_dttm , session_start_dttm = dbt_adv_campaign_visitors_tmp.session_start_dttm , session_start_dttm_tz = dbt_adv_campaign_visitors_tmp.session_start_dttm_tz , visit_dttm = dbt_adv_campaign_visitors_tmp.visit_dttm , visit_dttm_tz = dbt_adv_campaign_visitors_tmp.visit_dttm_tz , visit_origination_creative = dbt_adv_campaign_visitors_tmp.visit_origination_creative , visit_origination_name = dbt_adv_campaign_visitors_tmp.visit_origination_name , visit_origination_placement = dbt_adv_campaign_visitors_tmp.visit_origination_placement , visit_origination_tracking_code = dbt_adv_campaign_visitors_tmp.visit_origination_tracking_code , visit_origination_type = dbt_adv_campaign_visitors_tmp.visit_origination_type , visitor_id = dbt_adv_campaign_visitors_tmp.visitor_id , visitor_type = dbt_adv_campaign_visitors_tmp.visitor_type , visits = dbt_adv_campaign_visitors_tmp.visits
        WHEN NOT MATCHED THEN INSERT ( 
        average_visit_duration,bouncer,bouncers,br_browser_name,br_browser_version,co_conversions,cu_customer_id,device_name,device_type,ge_city,ge_country,ge_latitude,ge_longitude,ge_state_region,landing_page,landing_page_url,landing_page_url_domain,new_visitors,page_views,pl_device_operating_system,return_visitors,rv_revenue,se_external_search_engine,se_external_search_engine_domain,se_external_search_engine_phrase,session_complete_load_dttm,session_id,session_start_dttm,session_start_dttm_tz,visit_dttm,visit_dttm_tz,visit_id,visit_origination_creative,visit_origination_name,visit_origination_placement,visit_origination_tracking_code,visit_origination_type,visitor_id,visitor_type,visits
         ) values ( 
        dbt_adv_campaign_visitors_tmp.average_visit_duration,dbt_adv_campaign_visitors_tmp.bouncer,dbt_adv_campaign_visitors_tmp.bouncers,dbt_adv_campaign_visitors_tmp.br_browser_name,dbt_adv_campaign_visitors_tmp.br_browser_version,dbt_adv_campaign_visitors_tmp.co_conversions,dbt_adv_campaign_visitors_tmp.cu_customer_id,dbt_adv_campaign_visitors_tmp.device_name,dbt_adv_campaign_visitors_tmp.device_type,dbt_adv_campaign_visitors_tmp.ge_city,dbt_adv_campaign_visitors_tmp.ge_country,dbt_adv_campaign_visitors_tmp.ge_latitude,dbt_adv_campaign_visitors_tmp.ge_longitude,dbt_adv_campaign_visitors_tmp.ge_state_region,dbt_adv_campaign_visitors_tmp.landing_page,dbt_adv_campaign_visitors_tmp.landing_page_url,dbt_adv_campaign_visitors_tmp.landing_page_url_domain,dbt_adv_campaign_visitors_tmp.new_visitors,dbt_adv_campaign_visitors_tmp.page_views,dbt_adv_campaign_visitors_tmp.pl_device_operating_system,dbt_adv_campaign_visitors_tmp.return_visitors,dbt_adv_campaign_visitors_tmp.rv_revenue,dbt_adv_campaign_visitors_tmp.se_external_search_engine,dbt_adv_campaign_visitors_tmp.se_external_search_engine_domain,dbt_adv_campaign_visitors_tmp.se_external_search_engine_phrase,dbt_adv_campaign_visitors_tmp.session_complete_load_dttm,dbt_adv_campaign_visitors_tmp.session_id,dbt_adv_campaign_visitors_tmp.session_start_dttm,dbt_adv_campaign_visitors_tmp.session_start_dttm_tz,dbt_adv_campaign_visitors_tmp.visit_dttm,dbt_adv_campaign_visitors_tmp.visit_dttm_tz,dbt_adv_campaign_visitors_tmp.visit_id,dbt_adv_campaign_visitors_tmp.visit_origination_creative,dbt_adv_campaign_visitors_tmp.visit_origination_name,dbt_adv_campaign_visitors_tmp.visit_origination_placement,dbt_adv_campaign_visitors_tmp.visit_origination_tracking_code,dbt_adv_campaign_visitors_tmp.visit_origination_type,dbt_adv_campaign_visitors_tmp.visitor_id,dbt_adv_campaign_visitors_tmp.visitor_type,dbt_adv_campaign_visitors_tmp.visits
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :dbt_adv_campaign_visitors_tmp   , dbt_adv_campaign_visitors, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..dbt_adv_campaign_visitors_tmp   ;
    QUIT;
    %put ######## Staging table: dbt_adv_campaign_visitors_tmp    Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..dbt_adv_campaign_visitors;
      DROP TABLE work.dbt_adv_campaign_visitors;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table dbt_adv_campaign_visitors;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..dbt_business_process) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..dbt_business_process_tmp        ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..dbt_business_process_tmp        ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=dbt_business_process, table_keys=%str(bus_process_started_dttm,business_process_name,business_process_step_name,session_id), out_table=work.dbt_business_process);
 data &tmplib..dbt_business_process_tmp        ;
     set work.dbt_business_process;
  if bus_process_started_dttm ne . then bus_process_started_dttm = tzoneu2s(bus_process_started_dttm,&timeZone_Value.);if bus_process_started_dttm_tz ne . then bus_process_started_dttm_tz = tzoneu2s(bus_process_started_dttm_tz,&timeZone_Value.);if session_complete_load_dttm ne . then session_complete_load_dttm = tzoneu2s(session_complete_load_dttm,&timeZone_Value.);if session_start_dttm ne . then session_start_dttm = tzoneu2s(session_start_dttm,&timeZone_Value.);if session_start_dttm_tz ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz,&timeZone_Value.) ;
  if business_process_name='' then business_process_name='-'; if business_process_step_name='' then business_process_step_name='-'; if session_id='' then session_id='-';
 run;
 %ErrCheck (Failed to Append Data to :dbt_business_process_tmp        , dbt_business_process);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..dbt_business_process using &tmpdbschema..dbt_business_process_tmp        
         ON (dbt_business_process.bus_process_started_dttm=dbt_business_process_tmp.bus_process_started_dttm and dbt_business_process.business_process_name=dbt_business_process_tmp.business_process_name and dbt_business_process.business_process_step_name=dbt_business_process_tmp.business_process_step_name and dbt_business_process.session_id=dbt_business_process_tmp.session_id)
        WHEN MATCHED THEN  
        UPDATE SET bouncer = dbt_business_process_tmp.bouncer , bus_process_started_dttm_tz = dbt_business_process_tmp.bus_process_started_dttm_tz , business_process_attribute_1 = dbt_business_process_tmp.business_process_attribute_1 , business_process_attribute_2 = dbt_business_process_tmp.business_process_attribute_2 , cu_customer_id = dbt_business_process_tmp.cu_customer_id , device_name = dbt_business_process_tmp.device_name , device_type = dbt_business_process_tmp.device_type , last_step = dbt_business_process_tmp.last_step , processes = dbt_business_process_tmp.processes , processes_abandoned = dbt_business_process_tmp.processes_abandoned , processes_completed = dbt_business_process_tmp.processes_completed , session_complete_load_dttm = dbt_business_process_tmp.session_complete_load_dttm , session_start_dttm = dbt_business_process_tmp.session_start_dttm , session_start_dttm_tz = dbt_business_process_tmp.session_start_dttm_tz , step_count = dbt_business_process_tmp.step_count , steps = dbt_business_process_tmp.steps , steps_abandoned = dbt_business_process_tmp.steps_abandoned , steps_completed = dbt_business_process_tmp.steps_completed , visit_id = dbt_business_process_tmp.visit_id , visit_origination_creative = dbt_business_process_tmp.visit_origination_creative , visit_origination_name = dbt_business_process_tmp.visit_origination_name , visit_origination_placement = dbt_business_process_tmp.visit_origination_placement , visit_origination_tracking_code = dbt_business_process_tmp.visit_origination_tracking_code , visit_origination_type = dbt_business_process_tmp.visit_origination_type , visitor_id = dbt_business_process_tmp.visitor_id , visitor_type = dbt_business_process_tmp.visitor_type
        WHEN NOT MATCHED THEN INSERT ( 
        bouncer,bus_process_started_dttm,bus_process_started_dttm_tz,business_process_attribute_1,business_process_attribute_2,business_process_name,business_process_step_name,cu_customer_id,device_name,device_type,last_step,processes,processes_abandoned,processes_completed,session_complete_load_dttm,session_id,session_start_dttm,session_start_dttm_tz,step_count,steps,steps_abandoned,steps_completed,visit_id,visit_origination_creative,visit_origination_name,visit_origination_placement,visit_origination_tracking_code,visit_origination_type,visitor_id,visitor_type
         ) values ( 
        dbt_business_process_tmp.bouncer,dbt_business_process_tmp.bus_process_started_dttm,dbt_business_process_tmp.bus_process_started_dttm_tz,dbt_business_process_tmp.business_process_attribute_1,dbt_business_process_tmp.business_process_attribute_2,dbt_business_process_tmp.business_process_name,dbt_business_process_tmp.business_process_step_name,dbt_business_process_tmp.cu_customer_id,dbt_business_process_tmp.device_name,dbt_business_process_tmp.device_type,dbt_business_process_tmp.last_step,dbt_business_process_tmp.processes,dbt_business_process_tmp.processes_abandoned,dbt_business_process_tmp.processes_completed,dbt_business_process_tmp.session_complete_load_dttm,dbt_business_process_tmp.session_id,dbt_business_process_tmp.session_start_dttm,dbt_business_process_tmp.session_start_dttm_tz,dbt_business_process_tmp.step_count,dbt_business_process_tmp.steps,dbt_business_process_tmp.steps_abandoned,dbt_business_process_tmp.steps_completed,dbt_business_process_tmp.visit_id,dbt_business_process_tmp.visit_origination_creative,dbt_business_process_tmp.visit_origination_name,dbt_business_process_tmp.visit_origination_placement,dbt_business_process_tmp.visit_origination_tracking_code,dbt_business_process_tmp.visit_origination_type,dbt_business_process_tmp.visitor_id,dbt_business_process_tmp.visitor_type
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :dbt_business_process_tmp        , dbt_business_process, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..dbt_business_process_tmp        ;
    QUIT;
    %put ######## Staging table: dbt_business_process_tmp         Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..dbt_business_process;
      DROP TABLE work.dbt_business_process;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table dbt_business_process;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..dbt_content) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..dbt_content_tmp                 ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..dbt_content_tmp                 ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=dbt_content, table_keys=%str(active_page_view_time,detail_dttm,entry_pages,exit_pages,pg_page_url,session_id), out_table=work.dbt_content);
 data &tmplib..dbt_content_tmp                 ;
     set work.dbt_content;
  if detail_dttm ne . then detail_dttm = tzoneu2s(detail_dttm,&timeZone_Value.);if detail_dttm_tz ne . then detail_dttm_tz = tzoneu2s(detail_dttm_tz,&timeZone_Value.);if session_complete_load_dttm ne . then session_complete_load_dttm = tzoneu2s(session_complete_load_dttm,&timeZone_Value.);if session_start_dttm ne . then session_start_dttm = tzoneu2s(session_start_dttm,&timeZone_Value.);if session_start_dttm_tz ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz,&timeZone_Value.) ;
  Hashed_pk_col = put(sha256(catx('|',active_page_view_time,detail_dttm,entry_pages,exit_pages,pg_page_url,session_id)), $hex64.);
  if pg_page_url='' then pg_page_url='-'; if session_id='' then session_id='-';
 run;
 %ErrCheck (Failed to Append Data to :dbt_content_tmp                 , dbt_content);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..dbt_content using &tmpdbschema..dbt_content_tmp                 
         ON (dbt_content.Hashed_pk_col = dbt_content_tmp.Hashed_pk_col)
        WHEN MATCHED THEN  
        UPDATE SET bouncer = dbt_content_tmp.bouncer , bouncers = dbt_content_tmp.bouncers , class1_id = dbt_content_tmp.class1_id , class2_id = dbt_content_tmp.class2_id , cu_customer_id = dbt_content_tmp.cu_customer_id , detail_dttm_tz = dbt_content_tmp.detail_dttm_tz , device_name = dbt_content_tmp.device_name , device_type = dbt_content_tmp.device_type , pg_domain_name = dbt_content_tmp.pg_domain_name , pg_page = dbt_content_tmp.pg_page , session_complete_load_dttm = dbt_content_tmp.session_complete_load_dttm , session_start_dttm = dbt_content_tmp.session_start_dttm , session_start_dttm_tz = dbt_content_tmp.session_start_dttm_tz , total_page_view_time = dbt_content_tmp.total_page_view_time , views = dbt_content_tmp.views , visit_id = dbt_content_tmp.visit_id , visit_origination_creative = dbt_content_tmp.visit_origination_creative , visit_origination_name = dbt_content_tmp.visit_origination_name , visit_origination_placement = dbt_content_tmp.visit_origination_placement , visit_origination_tracking_code = dbt_content_tmp.visit_origination_tracking_code , visit_origination_type = dbt_content_tmp.visit_origination_type , visitor_id = dbt_content_tmp.visitor_id , visitor_type = dbt_content_tmp.visitor_type , visits = dbt_content_tmp.visits
        WHEN NOT MATCHED THEN INSERT ( 
        active_page_view_time,bouncer,bouncers,class1_id,class2_id,cu_customer_id,detail_dttm,detail_dttm_tz,device_name,device_type,entry_pages,exit_pages,pg_domain_name,pg_page,pg_page_url,session_complete_load_dttm,session_id,session_start_dttm,session_start_dttm_tz,total_page_view_time,views,visit_id,visit_origination_creative,visit_origination_name,visit_origination_placement,visit_origination_tracking_code,visit_origination_type,visitor_id,visitor_type,visits
        ,Hashed_pk_col ) VALUES ( 
        dbt_content_tmp.active_page_view_time,dbt_content_tmp.bouncer,dbt_content_tmp.bouncers,dbt_content_tmp.class1_id,dbt_content_tmp.class2_id,dbt_content_tmp.cu_customer_id,dbt_content_tmp.detail_dttm,dbt_content_tmp.detail_dttm_tz,dbt_content_tmp.device_name,dbt_content_tmp.device_type,dbt_content_tmp.entry_pages,dbt_content_tmp.exit_pages,dbt_content_tmp.pg_domain_name,dbt_content_tmp.pg_page,dbt_content_tmp.pg_page_url,dbt_content_tmp.session_complete_load_dttm,dbt_content_tmp.session_id,dbt_content_tmp.session_start_dttm,dbt_content_tmp.session_start_dttm_tz,dbt_content_tmp.total_page_view_time,dbt_content_tmp.views,dbt_content_tmp.visit_id,dbt_content_tmp.visit_origination_creative,dbt_content_tmp.visit_origination_name,dbt_content_tmp.visit_origination_placement,dbt_content_tmp.visit_origination_tracking_code,dbt_content_tmp.visit_origination_type,dbt_content_tmp.visitor_id,dbt_content_tmp.visitor_type,dbt_content_tmp.visits,dbt_content_tmp.Hashed_pk_col
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :dbt_content_tmp                 , dbt_content, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..dbt_content_tmp                 ;
    QUIT;
    %put ######## Staging table: dbt_content_tmp                  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..dbt_content;
      DROP TABLE work.dbt_content;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table dbt_content;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..dbt_documents) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..dbt_documents_tmp               ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..dbt_documents_tmp               ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=dbt_documents, table_keys=%str(document_download_dttm,session_id), out_table=work.dbt_documents);
 data &tmplib..dbt_documents_tmp               ;
     set work.dbt_documents;
  if document_download_dttm ne . then document_download_dttm = tzoneu2s(document_download_dttm,&timeZone_Value.);if document_download_dttm_tz ne . then document_download_dttm_tz = tzoneu2s(document_download_dttm_tz,&timeZone_Value.);if session_complete_load_dttm ne . then session_complete_load_dttm = tzoneu2s(session_complete_load_dttm,&timeZone_Value.);if session_start_dttm ne . then session_start_dttm = tzoneu2s(session_start_dttm,&timeZone_Value.);if session_start_dttm_tz ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz,&timeZone_Value.) ;
  if session_id='' then session_id='-';
 run;
 %ErrCheck (Failed to Append Data to :dbt_documents_tmp               , dbt_documents);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..dbt_documents using &tmpdbschema..dbt_documents_tmp               
         ON (dbt_documents.document_download_dttm=dbt_documents_tmp.document_download_dttm and dbt_documents.session_id=dbt_documents_tmp.session_id)
        WHEN MATCHED THEN  
        UPDATE SET bouncer = dbt_documents_tmp.bouncer , class1_id = dbt_documents_tmp.class1_id , class2_id = dbt_documents_tmp.class2_id , cu_customer_id = dbt_documents_tmp.cu_customer_id , device_name = dbt_documents_tmp.device_name , device_type = dbt_documents_tmp.device_type , do_page_description = dbt_documents_tmp.do_page_description , do_page_url = dbt_documents_tmp.do_page_url , document_download_dttm_tz = dbt_documents_tmp.document_download_dttm_tz , document_downloads = dbt_documents_tmp.document_downloads , session_complete_load_dttm = dbt_documents_tmp.session_complete_load_dttm , session_start_dttm = dbt_documents_tmp.session_start_dttm , session_start_dttm_tz = dbt_documents_tmp.session_start_dttm_tz , visit_id = dbt_documents_tmp.visit_id , visit_origination_creative = dbt_documents_tmp.visit_origination_creative , visit_origination_name = dbt_documents_tmp.visit_origination_name , visit_origination_placement = dbt_documents_tmp.visit_origination_placement , visit_origination_tracking_code = dbt_documents_tmp.visit_origination_tracking_code , visit_origination_type = dbt_documents_tmp.visit_origination_type , visitor_id = dbt_documents_tmp.visitor_id , visitor_type = dbt_documents_tmp.visitor_type
        WHEN NOT MATCHED THEN INSERT ( 
        bouncer,class1_id,class2_id,cu_customer_id,device_name,device_type,do_page_description,do_page_url,document_download_dttm,document_download_dttm_tz,document_downloads,session_complete_load_dttm,session_id,session_start_dttm,session_start_dttm_tz,visit_id,visit_origination_creative,visit_origination_name,visit_origination_placement,visit_origination_tracking_code,visit_origination_type,visitor_id,visitor_type
         ) values ( 
        dbt_documents_tmp.bouncer,dbt_documents_tmp.class1_id,dbt_documents_tmp.class2_id,dbt_documents_tmp.cu_customer_id,dbt_documents_tmp.device_name,dbt_documents_tmp.device_type,dbt_documents_tmp.do_page_description,dbt_documents_tmp.do_page_url,dbt_documents_tmp.document_download_dttm,dbt_documents_tmp.document_download_dttm_tz,dbt_documents_tmp.document_downloads,dbt_documents_tmp.session_complete_load_dttm,dbt_documents_tmp.session_id,dbt_documents_tmp.session_start_dttm,dbt_documents_tmp.session_start_dttm_tz,dbt_documents_tmp.visit_id,dbt_documents_tmp.visit_origination_creative,dbt_documents_tmp.visit_origination_name,dbt_documents_tmp.visit_origination_placement,dbt_documents_tmp.visit_origination_tracking_code,dbt_documents_tmp.visit_origination_type,dbt_documents_tmp.visitor_id,dbt_documents_tmp.visitor_type
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :dbt_documents_tmp               , dbt_documents, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..dbt_documents_tmp               ;
    QUIT;
    %put ######## Staging table: dbt_documents_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..dbt_documents;
      DROP TABLE work.dbt_documents;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table dbt_documents;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..dbt_ecommerce) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..dbt_ecommerce_tmp               ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..dbt_ecommerce_tmp               ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=dbt_ecommerce, table_keys=%str(product_activity_dttm,product_id,session_id), out_table=work.dbt_ecommerce);
 data &tmplib..dbt_ecommerce_tmp               ;
     set work.dbt_ecommerce;
  if product_activity_dttm ne . then product_activity_dttm = tzoneu2s(product_activity_dttm,&timeZone_Value.);if product_activity_dttm_tz ne . then product_activity_dttm_tz = tzoneu2s(product_activity_dttm_tz,&timeZone_Value.);if session_complete_load_dttm ne . then session_complete_load_dttm = tzoneu2s(session_complete_load_dttm,&timeZone_Value.);if session_start_dttm ne . then session_start_dttm = tzoneu2s(session_start_dttm,&timeZone_Value.);if session_start_dttm_tz ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz,&timeZone_Value.) ;
  if product_id='' then product_id='-'; if session_id='' then session_id='-';
 run;
 %ErrCheck (Failed to Append Data to :dbt_ecommerce_tmp               , dbt_ecommerce);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..dbt_ecommerce using &tmpdbschema..dbt_ecommerce_tmp               
         ON (dbt_ecommerce.product_activity_dttm=dbt_ecommerce_tmp.product_activity_dttm and dbt_ecommerce.product_id=dbt_ecommerce_tmp.product_id and dbt_ecommerce.session_id=dbt_ecommerce_tmp.session_id)
        WHEN MATCHED THEN  
        UPDATE SET basket_adds = dbt_ecommerce_tmp.basket_adds , basket_adds_revenue = dbt_ecommerce_tmp.basket_adds_revenue , basket_adds_units = dbt_ecommerce_tmp.basket_adds_units , basket_id = dbt_ecommerce_tmp.basket_id , basket_removes = dbt_ecommerce_tmp.basket_removes , basket_removes_revenue = dbt_ecommerce_tmp.basket_removes_revenue , basket_removes_units = dbt_ecommerce_tmp.basket_removes_units , baskets_abandoned = dbt_ecommerce_tmp.baskets_abandoned , baskets_completed = dbt_ecommerce_tmp.baskets_completed , baskets_started = dbt_ecommerce_tmp.baskets_started , bouncer = dbt_ecommerce_tmp.bouncer , cu_customer_id = dbt_ecommerce_tmp.cu_customer_id , device_name = dbt_ecommerce_tmp.device_name , device_type = dbt_ecommerce_tmp.device_type , product_activity_dttm_tz = dbt_ecommerce_tmp.product_activity_dttm_tz , product_group_name = dbt_ecommerce_tmp.product_group_name , product_name = dbt_ecommerce_tmp.product_name , product_purchase_revenues = dbt_ecommerce_tmp.product_purchase_revenues , product_purchase_units = dbt_ecommerce_tmp.product_purchase_units , product_purchases = dbt_ecommerce_tmp.product_purchases , product_sku = dbt_ecommerce_tmp.product_sku , product_views = dbt_ecommerce_tmp.product_views , session_complete_load_dttm = dbt_ecommerce_tmp.session_complete_load_dttm , session_start_dttm = dbt_ecommerce_tmp.session_start_dttm , session_start_dttm_tz = dbt_ecommerce_tmp.session_start_dttm_tz , visit_id = dbt_ecommerce_tmp.visit_id , visit_origination_creative = dbt_ecommerce_tmp.visit_origination_creative , visit_origination_name = dbt_ecommerce_tmp.visit_origination_name , visit_origination_placement = dbt_ecommerce_tmp.visit_origination_placement , visit_origination_tracking_code = dbt_ecommerce_tmp.visit_origination_tracking_code , visit_origination_type = dbt_ecommerce_tmp.visit_origination_type , visitor_id = dbt_ecommerce_tmp.visitor_id , visitor_type = dbt_ecommerce_tmp.visitor_type
        WHEN NOT MATCHED THEN INSERT ( 
        basket_adds,basket_adds_revenue,basket_adds_units,basket_id,basket_removes,basket_removes_revenue,basket_removes_units,baskets_abandoned,baskets_completed,baskets_started,bouncer,cu_customer_id,device_name,device_type,product_activity_dttm,product_activity_dttm_tz,product_group_name,product_id,product_name,product_purchase_revenues,product_purchase_units,product_purchases,product_sku,product_views,session_complete_load_dttm,session_id,session_start_dttm,session_start_dttm_tz,visit_id,visit_origination_creative,visit_origination_name,visit_origination_placement,visit_origination_tracking_code,visit_origination_type,visitor_id,visitor_type
         ) values ( 
        dbt_ecommerce_tmp.basket_adds,dbt_ecommerce_tmp.basket_adds_revenue,dbt_ecommerce_tmp.basket_adds_units,dbt_ecommerce_tmp.basket_id,dbt_ecommerce_tmp.basket_removes,dbt_ecommerce_tmp.basket_removes_revenue,dbt_ecommerce_tmp.basket_removes_units,dbt_ecommerce_tmp.baskets_abandoned,dbt_ecommerce_tmp.baskets_completed,dbt_ecommerce_tmp.baskets_started,dbt_ecommerce_tmp.bouncer,dbt_ecommerce_tmp.cu_customer_id,dbt_ecommerce_tmp.device_name,dbt_ecommerce_tmp.device_type,dbt_ecommerce_tmp.product_activity_dttm,dbt_ecommerce_tmp.product_activity_dttm_tz,dbt_ecommerce_tmp.product_group_name,dbt_ecommerce_tmp.product_id,dbt_ecommerce_tmp.product_name,dbt_ecommerce_tmp.product_purchase_revenues,dbt_ecommerce_tmp.product_purchase_units,dbt_ecommerce_tmp.product_purchases,dbt_ecommerce_tmp.product_sku,dbt_ecommerce_tmp.product_views,dbt_ecommerce_tmp.session_complete_load_dttm,dbt_ecommerce_tmp.session_id,dbt_ecommerce_tmp.session_start_dttm,dbt_ecommerce_tmp.session_start_dttm_tz,dbt_ecommerce_tmp.visit_id,dbt_ecommerce_tmp.visit_origination_creative,dbt_ecommerce_tmp.visit_origination_name,dbt_ecommerce_tmp.visit_origination_placement,dbt_ecommerce_tmp.visit_origination_tracking_code,dbt_ecommerce_tmp.visit_origination_type,dbt_ecommerce_tmp.visitor_id,dbt_ecommerce_tmp.visitor_type
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :dbt_ecommerce_tmp               , dbt_ecommerce, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..dbt_ecommerce_tmp               ;
    QUIT;
    %put ######## Staging table: dbt_ecommerce_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..dbt_ecommerce;
      DROP TABLE work.dbt_ecommerce;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table dbt_ecommerce;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..dbt_forms) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..dbt_forms_tmp                   ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..dbt_forms_tmp                   ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=dbt_forms, table_keys=%str(form_attempt_dttm,form_nm,session_id), out_table=work.dbt_forms);
 data &tmplib..dbt_forms_tmp                   ;
     set work.dbt_forms;
  if form_attempt_dttm ne . then form_attempt_dttm = tzoneu2s(form_attempt_dttm,&timeZone_Value.);if form_attempt_dttm_tz ne . then form_attempt_dttm_tz = tzoneu2s(form_attempt_dttm_tz,&timeZone_Value.);if session_complete_load_dttm ne . then session_complete_load_dttm = tzoneu2s(session_complete_load_dttm,&timeZone_Value.);if session_start_dttm ne . then session_start_dttm = tzoneu2s(session_start_dttm,&timeZone_Value.);if session_start_dttm_tz ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz,&timeZone_Value.) ;
  if form_nm='' then form_nm='-'; if session_id='' then session_id='-';
 run;
 %ErrCheck (Failed to Append Data to :dbt_forms_tmp                   , dbt_forms);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..dbt_forms using &tmpdbschema..dbt_forms_tmp                   
         ON (dbt_forms.form_attempt_dttm=dbt_forms_tmp.form_attempt_dttm and dbt_forms.form_nm=dbt_forms_tmp.form_nm and dbt_forms.session_id=dbt_forms_tmp.session_id)
        WHEN MATCHED THEN  
        UPDATE SET attempts = dbt_forms_tmp.attempts , bouncer = dbt_forms_tmp.bouncer , cu_customer_id = dbt_forms_tmp.cu_customer_id , device_name = dbt_forms_tmp.device_name , device_type = dbt_forms_tmp.device_type , form_attempt_dttm_tz = dbt_forms_tmp.form_attempt_dttm_tz , forms_completed = dbt_forms_tmp.forms_completed , forms_not_submitted = dbt_forms_tmp.forms_not_submitted , forms_started = dbt_forms_tmp.forms_started , last_field = dbt_forms_tmp.last_field , session_complete_load_dttm = dbt_forms_tmp.session_complete_load_dttm , session_start_dttm = dbt_forms_tmp.session_start_dttm , session_start_dttm_tz = dbt_forms_tmp.session_start_dttm_tz , visit_id = dbt_forms_tmp.visit_id , visit_origination_creative = dbt_forms_tmp.visit_origination_creative , visit_origination_name = dbt_forms_tmp.visit_origination_name , visit_origination_placement = dbt_forms_tmp.visit_origination_placement , visit_origination_tracking_code = dbt_forms_tmp.visit_origination_tracking_code , visit_origination_type = dbt_forms_tmp.visit_origination_type , visitor_id = dbt_forms_tmp.visitor_id , visitor_type = dbt_forms_tmp.visitor_type
        WHEN NOT MATCHED THEN INSERT ( 
        attempts,bouncer,cu_customer_id,device_name,device_type,form_attempt_dttm,form_attempt_dttm_tz,form_nm,forms_completed,forms_not_submitted,forms_started,last_field,session_complete_load_dttm,session_id,session_start_dttm,session_start_dttm_tz,visit_id,visit_origination_creative,visit_origination_name,visit_origination_placement,visit_origination_tracking_code,visit_origination_type,visitor_id,visitor_type
         ) values ( 
        dbt_forms_tmp.attempts,dbt_forms_tmp.bouncer,dbt_forms_tmp.cu_customer_id,dbt_forms_tmp.device_name,dbt_forms_tmp.device_type,dbt_forms_tmp.form_attempt_dttm,dbt_forms_tmp.form_attempt_dttm_tz,dbt_forms_tmp.form_nm,dbt_forms_tmp.forms_completed,dbt_forms_tmp.forms_not_submitted,dbt_forms_tmp.forms_started,dbt_forms_tmp.last_field,dbt_forms_tmp.session_complete_load_dttm,dbt_forms_tmp.session_id,dbt_forms_tmp.session_start_dttm,dbt_forms_tmp.session_start_dttm_tz,dbt_forms_tmp.visit_id,dbt_forms_tmp.visit_origination_creative,dbt_forms_tmp.visit_origination_name,dbt_forms_tmp.visit_origination_placement,dbt_forms_tmp.visit_origination_tracking_code,dbt_forms_tmp.visit_origination_type,dbt_forms_tmp.visitor_id,dbt_forms_tmp.visitor_type
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :dbt_forms_tmp                   , dbt_forms, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..dbt_forms_tmp                   ;
    QUIT;
    %put ######## Staging table: dbt_forms_tmp                    Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..dbt_forms;
      DROP TABLE work.dbt_forms;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table dbt_forms;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..dbt_goals) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..dbt_goals_tmp                   ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..dbt_goals_tmp                   ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=dbt_goals, table_keys=%str(goal_reached_dttm,session_id), out_table=work.dbt_goals);
 data &tmplib..dbt_goals_tmp                   ;
     set work.dbt_goals;
  if goal_reached_dttm ne . then goal_reached_dttm = tzoneu2s(goal_reached_dttm,&timeZone_Value.);if goal_reached_dttm_tz ne . then goal_reached_dttm_tz = tzoneu2s(goal_reached_dttm_tz,&timeZone_Value.);if session_complete_load_dttm ne . then session_complete_load_dttm = tzoneu2s(session_complete_load_dttm,&timeZone_Value.);if session_start_dttm ne . then session_start_dttm = tzoneu2s(session_start_dttm,&timeZone_Value.);if session_start_dttm_tz ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz,&timeZone_Value.) ;
  if session_id='' then session_id='-';
 run;
 %ErrCheck (Failed to Append Data to :dbt_goals_tmp                   , dbt_goals);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..dbt_goals using &tmpdbschema..dbt_goals_tmp                   
         ON (dbt_goals.goal_reached_dttm=dbt_goals_tmp.goal_reached_dttm and dbt_goals.session_id=dbt_goals_tmp.session_id)
        WHEN MATCHED THEN  
        UPDATE SET bouncer = dbt_goals_tmp.bouncer , cu_customer_id = dbt_goals_tmp.cu_customer_id , device_name = dbt_goals_tmp.device_name , device_type = dbt_goals_tmp.device_type , goal_group_name = dbt_goals_tmp.goal_group_name , goal_name = dbt_goals_tmp.goal_name , goal_reached_dttm_tz = dbt_goals_tmp.goal_reached_dttm_tz , goal_revenue = dbt_goals_tmp.goal_revenue , goals = dbt_goals_tmp.goals , session_complete_load_dttm = dbt_goals_tmp.session_complete_load_dttm , session_start_dttm = dbt_goals_tmp.session_start_dttm , session_start_dttm_tz = dbt_goals_tmp.session_start_dttm_tz , visit_id = dbt_goals_tmp.visit_id , visit_origination_creative = dbt_goals_tmp.visit_origination_creative , visit_origination_name = dbt_goals_tmp.visit_origination_name , visit_origination_placement = dbt_goals_tmp.visit_origination_placement , visit_origination_tracking_code = dbt_goals_tmp.visit_origination_tracking_code , visit_origination_type = dbt_goals_tmp.visit_origination_type , visitor_id = dbt_goals_tmp.visitor_id , visitor_type = dbt_goals_tmp.visitor_type , visits = dbt_goals_tmp.visits
        WHEN NOT MATCHED THEN INSERT ( 
        bouncer,cu_customer_id,device_name,device_type,goal_group_name,goal_name,goal_reached_dttm,goal_reached_dttm_tz,goal_revenue,goals,session_complete_load_dttm,session_id,session_start_dttm,session_start_dttm_tz,visit_id,visit_origination_creative,visit_origination_name,visit_origination_placement,visit_origination_tracking_code,visit_origination_type,visitor_id,visitor_type,visits
         ) values ( 
        dbt_goals_tmp.bouncer,dbt_goals_tmp.cu_customer_id,dbt_goals_tmp.device_name,dbt_goals_tmp.device_type,dbt_goals_tmp.goal_group_name,dbt_goals_tmp.goal_name,dbt_goals_tmp.goal_reached_dttm,dbt_goals_tmp.goal_reached_dttm_tz,dbt_goals_tmp.goal_revenue,dbt_goals_tmp.goals,dbt_goals_tmp.session_complete_load_dttm,dbt_goals_tmp.session_id,dbt_goals_tmp.session_start_dttm,dbt_goals_tmp.session_start_dttm_tz,dbt_goals_tmp.visit_id,dbt_goals_tmp.visit_origination_creative,dbt_goals_tmp.visit_origination_name,dbt_goals_tmp.visit_origination_placement,dbt_goals_tmp.visit_origination_tracking_code,dbt_goals_tmp.visit_origination_type,dbt_goals_tmp.visitor_id,dbt_goals_tmp.visitor_type,dbt_goals_tmp.visits
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :dbt_goals_tmp                   , dbt_goals, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..dbt_goals_tmp                   ;
    QUIT;
    %put ######## Staging table: dbt_goals_tmp                    Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..dbt_goals;
      DROP TABLE work.dbt_goals;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table dbt_goals;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..dbt_media_consumption) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..dbt_media_consumption_tmp       ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..dbt_media_consumption_tmp       ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=dbt_media_consumption, table_keys=%str(detail_id,interactions_count,maximum_progress,media_completion_rate,media_section,visit_id), out_table=work.dbt_media_consumption);
 data &tmplib..dbt_media_consumption_tmp       ;
     set work.dbt_media_consumption;
  if media_start_dttm ne . then media_start_dttm = tzoneu2s(media_start_dttm,&timeZone_Value.);if media_start_dttm_tz ne . then media_start_dttm_tz = tzoneu2s(media_start_dttm_tz,&timeZone_Value.);if session_complete_load_dttm ne . then session_complete_load_dttm = tzoneu2s(session_complete_load_dttm,&timeZone_Value.);if session_start_dttm ne . then session_start_dttm = tzoneu2s(session_start_dttm,&timeZone_Value.);if session_start_dttm_tz ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz,&timeZone_Value.) ;
  if detail_id='' then detail_id='-'; if media_completion_rate='' then media_completion_rate='-'; if media_section='' then media_section='-'; if visit_id='' then visit_id='-';
 run;
 %ErrCheck (Failed to Append Data to :dbt_media_consumption_tmp       , dbt_media_consumption);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..dbt_media_consumption using &tmpdbschema..dbt_media_consumption_tmp       
         ON (dbt_media_consumption.detail_id=dbt_media_consumption_tmp.detail_id and dbt_media_consumption.interactions_count=dbt_media_consumption_tmp.interactions_count and dbt_media_consumption.maximum_progress=dbt_media_consumption_tmp.maximum_progress and dbt_media_consumption.media_completion_rate=dbt_media_consumption_tmp.media_completion_rate and dbt_media_consumption.media_section=dbt_media_consumption_tmp.media_section and dbt_media_consumption.visit_id=dbt_media_consumption_tmp.visit_id)
        WHEN MATCHED THEN  
        UPDATE SET bouncer = dbt_media_consumption_tmp.bouncer , content_viewed = dbt_media_consumption_tmp.content_viewed , counter = dbt_media_consumption_tmp.counter , cu_customer_id = dbt_media_consumption_tmp.cu_customer_id , device_name = dbt_media_consumption_tmp.device_name , device_type = dbt_media_consumption_tmp.device_type , duration = dbt_media_consumption_tmp.duration , media_name = dbt_media_consumption_tmp.media_name , media_section_view = dbt_media_consumption_tmp.media_section_view , media_start_dttm = dbt_media_consumption_tmp.media_start_dttm , media_start_dttm_tz = dbt_media_consumption_tmp.media_start_dttm_tz , media_uri_txt = dbt_media_consumption_tmp.media_uri_txt , session_complete_load_dttm = dbt_media_consumption_tmp.session_complete_load_dttm , session_id = dbt_media_consumption_tmp.session_id , session_start_dttm = dbt_media_consumption_tmp.session_start_dttm , session_start_dttm_tz = dbt_media_consumption_tmp.session_start_dttm_tz , time_viewing = dbt_media_consumption_tmp.time_viewing , views = dbt_media_consumption_tmp.views , views_completed = dbt_media_consumption_tmp.views_completed , views_started = dbt_media_consumption_tmp.views_started , visit_origination_creative = dbt_media_consumption_tmp.visit_origination_creative , visit_origination_name = dbt_media_consumption_tmp.visit_origination_name , visit_origination_placement = dbt_media_consumption_tmp.visit_origination_placement , visit_origination_tracking_code = dbt_media_consumption_tmp.visit_origination_tracking_code , visit_origination_type = dbt_media_consumption_tmp.visit_origination_type , visitor_id = dbt_media_consumption_tmp.visitor_id , visitor_type = dbt_media_consumption_tmp.visitor_type
        WHEN NOT MATCHED THEN INSERT ( 
        bouncer,content_viewed,counter,cu_customer_id,detail_id,device_name,device_type,duration,interactions_count,maximum_progress,media_completion_rate,media_name,media_section,media_section_view,media_start_dttm,media_start_dttm_tz,media_uri_txt,session_complete_load_dttm,session_id,session_start_dttm,session_start_dttm_tz,time_viewing,views,views_completed,views_started,visit_id,visit_origination_creative,visit_origination_name,visit_origination_placement,visit_origination_tracking_code,visit_origination_type,visitor_id,visitor_type
         ) values ( 
        dbt_media_consumption_tmp.bouncer,dbt_media_consumption_tmp.content_viewed,dbt_media_consumption_tmp.counter,dbt_media_consumption_tmp.cu_customer_id,dbt_media_consumption_tmp.detail_id,dbt_media_consumption_tmp.device_name,dbt_media_consumption_tmp.device_type,dbt_media_consumption_tmp.duration,dbt_media_consumption_tmp.interactions_count,dbt_media_consumption_tmp.maximum_progress,dbt_media_consumption_tmp.media_completion_rate,dbt_media_consumption_tmp.media_name,dbt_media_consumption_tmp.media_section,dbt_media_consumption_tmp.media_section_view,dbt_media_consumption_tmp.media_start_dttm,dbt_media_consumption_tmp.media_start_dttm_tz,dbt_media_consumption_tmp.media_uri_txt,dbt_media_consumption_tmp.session_complete_load_dttm,dbt_media_consumption_tmp.session_id,dbt_media_consumption_tmp.session_start_dttm,dbt_media_consumption_tmp.session_start_dttm_tz,dbt_media_consumption_tmp.time_viewing,dbt_media_consumption_tmp.views,dbt_media_consumption_tmp.views_completed,dbt_media_consumption_tmp.views_started,dbt_media_consumption_tmp.visit_id,dbt_media_consumption_tmp.visit_origination_creative,dbt_media_consumption_tmp.visit_origination_name,dbt_media_consumption_tmp.visit_origination_placement,dbt_media_consumption_tmp.visit_origination_tracking_code,dbt_media_consumption_tmp.visit_origination_type,dbt_media_consumption_tmp.visitor_id,dbt_media_consumption_tmp.visitor_type
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :dbt_media_consumption_tmp       , dbt_media_consumption, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..dbt_media_consumption_tmp       ;
    QUIT;
    %put ######## Staging table: dbt_media_consumption_tmp        Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..dbt_media_consumption;
      DROP TABLE work.dbt_media_consumption;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table dbt_media_consumption;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..dbt_promotions) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..dbt_promotions_tmp              ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..dbt_promotions_tmp              ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=dbt_promotions, table_keys=%str(promotion_name,promotion_shown_dttm,session_id), out_table=work.dbt_promotions);
 data &tmplib..dbt_promotions_tmp              ;
     set work.dbt_promotions;
  if promotion_shown_dttm ne . then promotion_shown_dttm = tzoneu2s(promotion_shown_dttm,&timeZone_Value.);if promotion_shown_dttm_tz ne . then promotion_shown_dttm_tz = tzoneu2s(promotion_shown_dttm_tz,&timeZone_Value.);if session_complete_load_dttm ne . then session_complete_load_dttm = tzoneu2s(session_complete_load_dttm,&timeZone_Value.);if session_start_dttm ne . then session_start_dttm = tzoneu2s(session_start_dttm,&timeZone_Value.);if session_start_dttm_tz ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz,&timeZone_Value.) ;
  if promotion_name='' then promotion_name='-'; if session_id='' then session_id='-';
 run;
 %ErrCheck (Failed to Append Data to :dbt_promotions_tmp              , dbt_promotions);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..dbt_promotions using &tmpdbschema..dbt_promotions_tmp              
         ON (dbt_promotions.promotion_name=dbt_promotions_tmp.promotion_name and dbt_promotions.promotion_shown_dttm=dbt_promotions_tmp.promotion_shown_dttm and dbt_promotions.session_id=dbt_promotions_tmp.session_id)
        WHEN MATCHED THEN  
        UPDATE SET bouncer = dbt_promotions_tmp.bouncer , click_throughs = dbt_promotions_tmp.click_throughs , cu_customer_id = dbt_promotions_tmp.cu_customer_id , device_name = dbt_promotions_tmp.device_name , device_type = dbt_promotions_tmp.device_type , displays = dbt_promotions_tmp.displays , promotion_creative = dbt_promotions_tmp.promotion_creative , promotion_placement = dbt_promotions_tmp.promotion_placement , promotion_shown_dttm_tz = dbt_promotions_tmp.promotion_shown_dttm_tz , promotion_tracking_code = dbt_promotions_tmp.promotion_tracking_code , promotion_type = dbt_promotions_tmp.promotion_type , session_complete_load_dttm = dbt_promotions_tmp.session_complete_load_dttm , session_start_dttm = dbt_promotions_tmp.session_start_dttm , session_start_dttm_tz = dbt_promotions_tmp.session_start_dttm_tz , visit_id = dbt_promotions_tmp.visit_id , visit_origination_creative = dbt_promotions_tmp.visit_origination_creative , visit_origination_name = dbt_promotions_tmp.visit_origination_name , visit_origination_placement = dbt_promotions_tmp.visit_origination_placement , visit_origination_tracking_code = dbt_promotions_tmp.visit_origination_tracking_code , visit_origination_type = dbt_promotions_tmp.visit_origination_type , visitor_id = dbt_promotions_tmp.visitor_id , visitor_type = dbt_promotions_tmp.visitor_type
        WHEN NOT MATCHED THEN INSERT ( 
        bouncer,click_throughs,cu_customer_id,device_name,device_type,displays,promotion_creative,promotion_name,promotion_placement,promotion_shown_dttm,promotion_shown_dttm_tz,promotion_tracking_code,promotion_type,session_complete_load_dttm,session_id,session_start_dttm,session_start_dttm_tz,visit_id,visit_origination_creative,visit_origination_name,visit_origination_placement,visit_origination_tracking_code,visit_origination_type,visitor_id,visitor_type
         ) values ( 
        dbt_promotions_tmp.bouncer,dbt_promotions_tmp.click_throughs,dbt_promotions_tmp.cu_customer_id,dbt_promotions_tmp.device_name,dbt_promotions_tmp.device_type,dbt_promotions_tmp.displays,dbt_promotions_tmp.promotion_creative,dbt_promotions_tmp.promotion_name,dbt_promotions_tmp.promotion_placement,dbt_promotions_tmp.promotion_shown_dttm,dbt_promotions_tmp.promotion_shown_dttm_tz,dbt_promotions_tmp.promotion_tracking_code,dbt_promotions_tmp.promotion_type,dbt_promotions_tmp.session_complete_load_dttm,dbt_promotions_tmp.session_id,dbt_promotions_tmp.session_start_dttm,dbt_promotions_tmp.session_start_dttm_tz,dbt_promotions_tmp.visit_id,dbt_promotions_tmp.visit_origination_creative,dbt_promotions_tmp.visit_origination_name,dbt_promotions_tmp.visit_origination_placement,dbt_promotions_tmp.visit_origination_tracking_code,dbt_promotions_tmp.visit_origination_type,dbt_promotions_tmp.visitor_id,dbt_promotions_tmp.visitor_type
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :dbt_promotions_tmp              , dbt_promotions, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..dbt_promotions_tmp              ;
    QUIT;
    %put ######## Staging table: dbt_promotions_tmp               Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..dbt_promotions;
      DROP TABLE work.dbt_promotions;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table dbt_promotions;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..dbt_search) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..dbt_search_tmp                  ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..dbt_search_tmp                  ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=dbt_search, table_keys=%str(search_name,search_results_dttm,session_id), out_table=work.dbt_search);
 data &tmplib..dbt_search_tmp                  ;
     set work.dbt_search;
  if search_results_dttm ne . then search_results_dttm = tzoneu2s(search_results_dttm,&timeZone_Value.);if search_results_dttm_tz ne . then search_results_dttm_tz = tzoneu2s(search_results_dttm_tz,&timeZone_Value.);if session_complete_load_dttm ne . then session_complete_load_dttm = tzoneu2s(session_complete_load_dttm,&timeZone_Value.);if session_start_dttm ne . then session_start_dttm = tzoneu2s(session_start_dttm,&timeZone_Value.);if session_start_dttm_tz ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz,&timeZone_Value.) ;
  if search_name='' then search_name='-'; if session_id='' then session_id='-';
 run;
 %ErrCheck (Failed to Append Data to :dbt_search_tmp                  , dbt_search);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..dbt_search using &tmpdbschema..dbt_search_tmp                  
         ON (dbt_search.search_name=dbt_search_tmp.search_name and dbt_search.search_results_dttm=dbt_search_tmp.search_results_dttm and dbt_search.session_id=dbt_search_tmp.session_id)
        WHEN MATCHED THEN  
        UPDATE SET bouncer = dbt_search_tmp.bouncer , cu_customer_id = dbt_search_tmp.cu_customer_id , device_name = dbt_search_tmp.device_name , device_type = dbt_search_tmp.device_type , exit_pages = dbt_search_tmp.exit_pages , internal_search_term = dbt_search_tmp.internal_search_term , num_additional_searches = dbt_search_tmp.num_additional_searches , num_pages_viewed_afterwards = dbt_search_tmp.num_pages_viewed_afterwards , search_no_results_returned = dbt_search_tmp.search_no_results_returned , search_results_dttm_tz = dbt_search_tmp.search_results_dttm_tz , search_returned_results = dbt_search_tmp.search_returned_results , search_unknown_results = dbt_search_tmp.search_unknown_results , searches = dbt_search_tmp.searches , session_complete_load_dttm = dbt_search_tmp.session_complete_load_dttm , session_start_dttm = dbt_search_tmp.session_start_dttm , session_start_dttm_tz = dbt_search_tmp.session_start_dttm_tz , visit_id = dbt_search_tmp.visit_id , visit_origination_creative = dbt_search_tmp.visit_origination_creative , visit_origination_name = dbt_search_tmp.visit_origination_name , visit_origination_placement = dbt_search_tmp.visit_origination_placement , visit_origination_tracking_code = dbt_search_tmp.visit_origination_tracking_code , visit_origination_type = dbt_search_tmp.visit_origination_type , visitor_id = dbt_search_tmp.visitor_id , visitor_type = dbt_search_tmp.visitor_type , visits = dbt_search_tmp.visits
        WHEN NOT MATCHED THEN INSERT ( 
        bouncer,cu_customer_id,device_name,device_type,exit_pages,internal_search_term,num_additional_searches,num_pages_viewed_afterwards,search_name,search_no_results_returned,search_results_dttm,search_results_dttm_tz,search_returned_results,search_unknown_results,searches,session_complete_load_dttm,session_id,session_start_dttm,session_start_dttm_tz,visit_id,visit_origination_creative,visit_origination_name,visit_origination_placement,visit_origination_tracking_code,visit_origination_type,visitor_id,visitor_type,visits
         ) values ( 
        dbt_search_tmp.bouncer,dbt_search_tmp.cu_customer_id,dbt_search_tmp.device_name,dbt_search_tmp.device_type,dbt_search_tmp.exit_pages,dbt_search_tmp.internal_search_term,dbt_search_tmp.num_additional_searches,dbt_search_tmp.num_pages_viewed_afterwards,dbt_search_tmp.search_name,dbt_search_tmp.search_no_results_returned,dbt_search_tmp.search_results_dttm,dbt_search_tmp.search_results_dttm_tz,dbt_search_tmp.search_returned_results,dbt_search_tmp.search_unknown_results,dbt_search_tmp.searches,dbt_search_tmp.session_complete_load_dttm,dbt_search_tmp.session_id,dbt_search_tmp.session_start_dttm,dbt_search_tmp.session_start_dttm_tz,dbt_search_tmp.visit_id,dbt_search_tmp.visit_origination_creative,dbt_search_tmp.visit_origination_name,dbt_search_tmp.visit_origination_placement,dbt_search_tmp.visit_origination_tracking_code,dbt_search_tmp.visit_origination_type,dbt_search_tmp.visitor_id,dbt_search_tmp.visitor_type,dbt_search_tmp.visits
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :dbt_search_tmp                  , dbt_search, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..dbt_search_tmp                  ;
    QUIT;
    %put ######## Staging table: dbt_search_tmp                   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..dbt_search;
      DROP TABLE work.dbt_search;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table dbt_search;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..direct_contact) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..direct_contact_tmp              ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..direct_contact_tmp              ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=direct_contact, table_keys=%str(event_id), out_table=work.direct_contact);
 data &tmplib..direct_contact_tmp              ;
     set work.direct_contact;
  if direct_contact_dttm ne . then direct_contact_dttm = tzoneu2s(direct_contact_dttm,&timeZone_Value.);if direct_contact_dttm_tz ne . then direct_contact_dttm_tz = tzoneu2s(direct_contact_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :direct_contact_tmp              , direct_contact);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..direct_contact using &tmpdbschema..direct_contact_tmp              
         ON (direct_contact.event_id=direct_contact_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET channel_nm = direct_contact_tmp.channel_nm , channel_user_id = direct_contact_tmp.channel_user_id , context_type_nm = direct_contact_tmp.context_type_nm , context_val = direct_contact_tmp.context_val , control_active_flg = direct_contact_tmp.control_active_flg , control_group_flg = direct_contact_tmp.control_group_flg , direct_contact_dttm = direct_contact_tmp.direct_contact_dttm , direct_contact_dttm_tz = direct_contact_tmp.direct_contact_dttm_tz , event_designed_id = direct_contact_tmp.event_designed_id , event_nm = direct_contact_tmp.event_nm , identity_id = direct_contact_tmp.identity_id , identity_type_nm = direct_contact_tmp.identity_type_nm , load_dttm = direct_contact_tmp.load_dttm , message_id = direct_contact_tmp.message_id , occurrence_id = direct_contact_tmp.occurrence_id , properties_map_doc = direct_contact_tmp.properties_map_doc , response_tracking_cd = direct_contact_tmp.response_tracking_cd , segment_id = direct_contact_tmp.segment_id , task_id = direct_contact_tmp.task_id , task_version_id = direct_contact_tmp.task_version_id
        WHEN NOT MATCHED THEN INSERT ( 
        channel_nm,channel_user_id,context_type_nm,context_val,control_active_flg,control_group_flg,direct_contact_dttm,direct_contact_dttm_tz,event_designed_id,event_id,event_nm,identity_id,identity_type_nm,load_dttm,message_id,occurrence_id,properties_map_doc,response_tracking_cd,segment_id,task_id,task_version_id
         ) values ( 
        direct_contact_tmp.channel_nm,direct_contact_tmp.channel_user_id,direct_contact_tmp.context_type_nm,direct_contact_tmp.context_val,direct_contact_tmp.control_active_flg,direct_contact_tmp.control_group_flg,direct_contact_tmp.direct_contact_dttm,direct_contact_tmp.direct_contact_dttm_tz,direct_contact_tmp.event_designed_id,direct_contact_tmp.event_id,direct_contact_tmp.event_nm,direct_contact_tmp.identity_id,direct_contact_tmp.identity_type_nm,direct_contact_tmp.load_dttm,direct_contact_tmp.message_id,direct_contact_tmp.occurrence_id,direct_contact_tmp.properties_map_doc,direct_contact_tmp.response_tracking_cd,direct_contact_tmp.segment_id,direct_contact_tmp.task_id,direct_contact_tmp.task_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :direct_contact_tmp              , direct_contact, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..direct_contact_tmp              ;
    QUIT;
    %put ######## Staging table: direct_contact_tmp               Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..direct_contact;
      DROP TABLE work.direct_contact;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table direct_contact;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..document_details) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..document_details_tmp            ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..document_details_tmp            ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=document_details, table_keys=%str(detail_id,link_event_dttm,uri_txt), out_table=work.document_details);
 data &tmplib..document_details_tmp            ;
     set work.document_details;
  if link_event_dttm ne . then link_event_dttm = tzoneu2s(link_event_dttm,&timeZone_Value.);if link_event_dttm_tz ne . then link_event_dttm_tz = tzoneu2s(link_event_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  Hashed_pk_col = put(sha256(catx('|',detail_id,link_event_dttm,uri_txt)), $hex64.);
  if detail_id='' then detail_id='-'; if uri_txt='' then uri_txt='-';
 run;
 %ErrCheck (Failed to Append Data to :document_details_tmp            , document_details);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..document_details using &tmpdbschema..document_details_tmp            
         ON (document_details.Hashed_pk_col = document_details_tmp.Hashed_pk_col)
        WHEN MATCHED THEN  
        UPDATE SET alt_txt = document_details_tmp.alt_txt , detail_id_hex = document_details_tmp.detail_id_hex , event_id = document_details_tmp.event_id , event_key_cd = document_details_tmp.event_key_cd , event_source_cd = document_details_tmp.event_source_cd , identity_id = document_details_tmp.identity_id , link_event_dttm_tz = document_details_tmp.link_event_dttm_tz , link_id = document_details_tmp.link_id , link_name = document_details_tmp.link_name , link_selector_path = document_details_tmp.link_selector_path , load_dttm = document_details_tmp.load_dttm , session_id = document_details_tmp.session_id , session_id_hex = document_details_tmp.session_id_hex , visit_id = document_details_tmp.visit_id , visit_id_hex = document_details_tmp.visit_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        alt_txt,detail_id,detail_id_hex,event_id,event_key_cd,event_source_cd,identity_id,link_event_dttm,link_event_dttm_tz,link_id,link_name,link_selector_path,load_dttm,session_id,session_id_hex,uri_txt,visit_id,visit_id_hex
        ,Hashed_pk_col ) VALUES ( 
        document_details_tmp.alt_txt,document_details_tmp.detail_id,document_details_tmp.detail_id_hex,document_details_tmp.event_id,document_details_tmp.event_key_cd,document_details_tmp.event_source_cd,document_details_tmp.identity_id,document_details_tmp.link_event_dttm,document_details_tmp.link_event_dttm_tz,document_details_tmp.link_id,document_details_tmp.link_name,document_details_tmp.link_selector_path,document_details_tmp.load_dttm,document_details_tmp.session_id,document_details_tmp.session_id_hex,document_details_tmp.uri_txt,document_details_tmp.visit_id,document_details_tmp.visit_id_hex,document_details_tmp.Hashed_pk_col
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :document_details_tmp            , document_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..document_details_tmp            ;
    QUIT;
    %put ######## Staging table: document_details_tmp             Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..document_details;
      DROP TABLE work.document_details;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table document_details;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..email_bounce) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..email_bounce_tmp                ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..email_bounce_tmp                ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=email_bounce, table_keys=%str(event_id), out_table=work.email_bounce);
 data &tmplib..email_bounce_tmp                ;
     set work.email_bounce;
  if email_bounce_dttm ne . then email_bounce_dttm = tzoneu2s(email_bounce_dttm,&timeZone_Value.);if email_bounce_dttm_tz ne . then email_bounce_dttm_tz = tzoneu2s(email_bounce_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :email_bounce_tmp                , email_bounce);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..email_bounce using &tmpdbschema..email_bounce_tmp                
         ON (email_bounce.event_id=email_bounce_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET analysis_group_id = email_bounce_tmp.analysis_group_id , aud_occurrence_id = email_bounce_tmp.aud_occurrence_id , audience_id = email_bounce_tmp.audience_id , bounce_class_cd = email_bounce_tmp.bounce_class_cd , channel_user_id = email_bounce_tmp.channel_user_id , context_type_nm = email_bounce_tmp.context_type_nm , context_val = email_bounce_tmp.context_val , email_bounce_dttm = email_bounce_tmp.email_bounce_dttm , email_bounce_dttm_tz = email_bounce_tmp.email_bounce_dttm_tz , event_designed_id = email_bounce_tmp.event_designed_id , event_nm = email_bounce_tmp.event_nm , identity_id = email_bounce_tmp.identity_id , imprint_id = email_bounce_tmp.imprint_id , journey_id = email_bounce_tmp.journey_id , journey_occurrence_id = email_bounce_tmp.journey_occurrence_id , load_dttm = email_bounce_tmp.load_dttm , occurrence_id = email_bounce_tmp.occurrence_id , program_id = email_bounce_tmp.program_id , properties_map_doc = email_bounce_tmp.properties_map_doc , raw_reason_txt = email_bounce_tmp.raw_reason_txt , reason_txt = email_bounce_tmp.reason_txt , recipient_domain_nm = email_bounce_tmp.recipient_domain_nm , response_tracking_cd = email_bounce_tmp.response_tracking_cd , segment_id = email_bounce_tmp.segment_id , segment_version_id = email_bounce_tmp.segment_version_id , subject_line_txt = email_bounce_tmp.subject_line_txt , task_id = email_bounce_tmp.task_id , task_version_id = email_bounce_tmp.task_version_id , test_flg = email_bounce_tmp.test_flg
        WHEN NOT MATCHED THEN INSERT ( 
        analysis_group_id,aud_occurrence_id,audience_id,bounce_class_cd,channel_user_id,context_type_nm,context_val,email_bounce_dttm,email_bounce_dttm_tz,event_designed_id,event_id,event_nm,identity_id,imprint_id,journey_id,journey_occurrence_id,load_dttm,occurrence_id,program_id,properties_map_doc,raw_reason_txt,reason_txt,recipient_domain_nm,response_tracking_cd,segment_id,segment_version_id,subject_line_txt,task_id,task_version_id,test_flg
         ) values ( 
        email_bounce_tmp.analysis_group_id,email_bounce_tmp.aud_occurrence_id,email_bounce_tmp.audience_id,email_bounce_tmp.bounce_class_cd,email_bounce_tmp.channel_user_id,email_bounce_tmp.context_type_nm,email_bounce_tmp.context_val,email_bounce_tmp.email_bounce_dttm,email_bounce_tmp.email_bounce_dttm_tz,email_bounce_tmp.event_designed_id,email_bounce_tmp.event_id,email_bounce_tmp.event_nm,email_bounce_tmp.identity_id,email_bounce_tmp.imprint_id,email_bounce_tmp.journey_id,email_bounce_tmp.journey_occurrence_id,email_bounce_tmp.load_dttm,email_bounce_tmp.occurrence_id,email_bounce_tmp.program_id,email_bounce_tmp.properties_map_doc,email_bounce_tmp.raw_reason_txt,email_bounce_tmp.reason_txt,email_bounce_tmp.recipient_domain_nm,email_bounce_tmp.response_tracking_cd,email_bounce_tmp.segment_id,email_bounce_tmp.segment_version_id,email_bounce_tmp.subject_line_txt,email_bounce_tmp.task_id,email_bounce_tmp.task_version_id,email_bounce_tmp.test_flg
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :email_bounce_tmp                , email_bounce, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..email_bounce_tmp                ;
    QUIT;
    %put ######## Staging table: email_bounce_tmp                 Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..email_bounce;
      DROP TABLE work.email_bounce;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table email_bounce;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..email_click) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..email_click_tmp                 ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..email_click_tmp                 ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=email_click, table_keys=%str(event_id), out_table=work.email_click);
 data &tmplib..email_click_tmp                 ;
     set work.email_click;
  if email_click_dttm ne . then email_click_dttm = tzoneu2s(email_click_dttm,&timeZone_Value.);if email_click_dttm_tz ne . then email_click_dttm_tz = tzoneu2s(email_click_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :email_click_tmp                 , email_click);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..email_click using &tmpdbschema..email_click_tmp                 
         ON (email_click.event_id=email_click_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET agent_family_nm = email_click_tmp.agent_family_nm , analysis_group_id = email_click_tmp.analysis_group_id , aud_occurrence_id = email_click_tmp.aud_occurrence_id , audience_id = email_click_tmp.audience_id , channel_user_id = email_click_tmp.channel_user_id , click_tracking_flg = email_click_tmp.click_tracking_flg , context_type_nm = email_click_tmp.context_type_nm , context_val = email_click_tmp.context_val , device_nm = email_click_tmp.device_nm , email_click_dttm = email_click_tmp.email_click_dttm , email_click_dttm_tz = email_click_tmp.email_click_dttm_tz , event_designed_id = email_click_tmp.event_designed_id , event_nm = email_click_tmp.event_nm , identity_id = email_click_tmp.identity_id , imprint_id = email_click_tmp.imprint_id , is_mobile_flg = email_click_tmp.is_mobile_flg , journey_id = email_click_tmp.journey_id , journey_occurrence_id = email_click_tmp.journey_occurrence_id , link_tracking_group_txt = email_click_tmp.link_tracking_group_txt , link_tracking_id = email_click_tmp.link_tracking_id , link_tracking_label_txt = email_click_tmp.link_tracking_label_txt , load_dttm = email_click_tmp.load_dttm , mailbox_provider_nm = email_click_tmp.mailbox_provider_nm , manufacturer_nm = email_click_tmp.manufacturer_nm , occurrence_id = email_click_tmp.occurrence_id , open_tracking_flg = email_click_tmp.open_tracking_flg , platform_desc = email_click_tmp.platform_desc , platform_version = email_click_tmp.platform_version , program_id = email_click_tmp.program_id , properties_map_doc = email_click_tmp.properties_map_doc , recipient_domain_nm = email_click_tmp.recipient_domain_nm , response_tracking_cd = email_click_tmp.response_tracking_cd , segment_id = email_click_tmp.segment_id , segment_version_id = email_click_tmp.segment_version_id , subject_line_txt = email_click_tmp.subject_line_txt , task_id = email_click_tmp.task_id , task_version_id = email_click_tmp.task_version_id , test_flg = email_click_tmp.test_flg , uri_txt = email_click_tmp.uri_txt , user_agent_nm = email_click_tmp.user_agent_nm
        WHEN NOT MATCHED THEN INSERT ( 
        agent_family_nm,analysis_group_id,aud_occurrence_id,audience_id,channel_user_id,click_tracking_flg,context_type_nm,context_val,device_nm,email_click_dttm,email_click_dttm_tz,event_designed_id,event_id,event_nm,identity_id,imprint_id,is_mobile_flg,journey_id,journey_occurrence_id,link_tracking_group_txt,link_tracking_id,link_tracking_label_txt,load_dttm,mailbox_provider_nm,manufacturer_nm,occurrence_id,open_tracking_flg,platform_desc,platform_version,program_id,properties_map_doc,recipient_domain_nm,response_tracking_cd,segment_id,segment_version_id,subject_line_txt,task_id,task_version_id,test_flg,uri_txt,user_agent_nm
         ) values ( 
        email_click_tmp.agent_family_nm,email_click_tmp.analysis_group_id,email_click_tmp.aud_occurrence_id,email_click_tmp.audience_id,email_click_tmp.channel_user_id,email_click_tmp.click_tracking_flg,email_click_tmp.context_type_nm,email_click_tmp.context_val,email_click_tmp.device_nm,email_click_tmp.email_click_dttm,email_click_tmp.email_click_dttm_tz,email_click_tmp.event_designed_id,email_click_tmp.event_id,email_click_tmp.event_nm,email_click_tmp.identity_id,email_click_tmp.imprint_id,email_click_tmp.is_mobile_flg,email_click_tmp.journey_id,email_click_tmp.journey_occurrence_id,email_click_tmp.link_tracking_group_txt,email_click_tmp.link_tracking_id,email_click_tmp.link_tracking_label_txt,email_click_tmp.load_dttm,email_click_tmp.mailbox_provider_nm,email_click_tmp.manufacturer_nm,email_click_tmp.occurrence_id,email_click_tmp.open_tracking_flg,email_click_tmp.platform_desc,email_click_tmp.platform_version,email_click_tmp.program_id,email_click_tmp.properties_map_doc,email_click_tmp.recipient_domain_nm,email_click_tmp.response_tracking_cd,email_click_tmp.segment_id,email_click_tmp.segment_version_id,email_click_tmp.subject_line_txt,email_click_tmp.task_id,email_click_tmp.task_version_id,email_click_tmp.test_flg,email_click_tmp.uri_txt,email_click_tmp.user_agent_nm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :email_click_tmp                 , email_click, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..email_click_tmp                 ;
    QUIT;
    %put ######## Staging table: email_click_tmp                  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..email_click;
      DROP TABLE work.email_click;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table email_click;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..email_complaint) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..email_complaint_tmp             ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..email_complaint_tmp             ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=email_complaint, table_keys=%str(event_id), out_table=work.email_complaint);
 data &tmplib..email_complaint_tmp             ;
     set work.email_complaint;
  if email_complaint_dttm ne . then email_complaint_dttm = tzoneu2s(email_complaint_dttm,&timeZone_Value.);if email_complaint_dttm_tz ne . then email_complaint_dttm_tz = tzoneu2s(email_complaint_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :email_complaint_tmp             , email_complaint);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..email_complaint using &tmpdbschema..email_complaint_tmp             
         ON (email_complaint.event_id=email_complaint_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET analysis_group_id = email_complaint_tmp.analysis_group_id , aud_occurrence_id = email_complaint_tmp.aud_occurrence_id , audience_id = email_complaint_tmp.audience_id , channel_user_id = email_complaint_tmp.channel_user_id , context_type_nm = email_complaint_tmp.context_type_nm , context_val = email_complaint_tmp.context_val , email_complaint_dttm = email_complaint_tmp.email_complaint_dttm , email_complaint_dttm_tz = email_complaint_tmp.email_complaint_dttm_tz , event_designed_id = email_complaint_tmp.event_designed_id , event_nm = email_complaint_tmp.event_nm , identity_id = email_complaint_tmp.identity_id , imprint_id = email_complaint_tmp.imprint_id , journey_id = email_complaint_tmp.journey_id , journey_occurrence_id = email_complaint_tmp.journey_occurrence_id , load_dttm = email_complaint_tmp.load_dttm , occurrence_id = email_complaint_tmp.occurrence_id , program_id = email_complaint_tmp.program_id , properties_map_doc = email_complaint_tmp.properties_map_doc , recipient_domain_nm = email_complaint_tmp.recipient_domain_nm , response_tracking_cd = email_complaint_tmp.response_tracking_cd , segment_id = email_complaint_tmp.segment_id , segment_version_id = email_complaint_tmp.segment_version_id , subject_line_txt = email_complaint_tmp.subject_line_txt , task_id = email_complaint_tmp.task_id , task_version_id = email_complaint_tmp.task_version_id , test_flg = email_complaint_tmp.test_flg
        WHEN NOT MATCHED THEN INSERT ( 
        analysis_group_id,aud_occurrence_id,audience_id,channel_user_id,context_type_nm,context_val,email_complaint_dttm,email_complaint_dttm_tz,event_designed_id,event_id,event_nm,identity_id,imprint_id,journey_id,journey_occurrence_id,load_dttm,occurrence_id,program_id,properties_map_doc,recipient_domain_nm,response_tracking_cd,segment_id,segment_version_id,subject_line_txt,task_id,task_version_id,test_flg
         ) values ( 
        email_complaint_tmp.analysis_group_id,email_complaint_tmp.aud_occurrence_id,email_complaint_tmp.audience_id,email_complaint_tmp.channel_user_id,email_complaint_tmp.context_type_nm,email_complaint_tmp.context_val,email_complaint_tmp.email_complaint_dttm,email_complaint_tmp.email_complaint_dttm_tz,email_complaint_tmp.event_designed_id,email_complaint_tmp.event_id,email_complaint_tmp.event_nm,email_complaint_tmp.identity_id,email_complaint_tmp.imprint_id,email_complaint_tmp.journey_id,email_complaint_tmp.journey_occurrence_id,email_complaint_tmp.load_dttm,email_complaint_tmp.occurrence_id,email_complaint_tmp.program_id,email_complaint_tmp.properties_map_doc,email_complaint_tmp.recipient_domain_nm,email_complaint_tmp.response_tracking_cd,email_complaint_tmp.segment_id,email_complaint_tmp.segment_version_id,email_complaint_tmp.subject_line_txt,email_complaint_tmp.task_id,email_complaint_tmp.task_version_id,email_complaint_tmp.test_flg
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :email_complaint_tmp             , email_complaint, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..email_complaint_tmp             ;
    QUIT;
    %put ######## Staging table: email_complaint_tmp              Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..email_complaint;
      DROP TABLE work.email_complaint;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table email_complaint;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..email_open) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..email_open_tmp                  ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..email_open_tmp                  ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=email_open, table_keys=%str(event_id), out_table=work.email_open);
 data &tmplib..email_open_tmp                  ;
     set work.email_open;
  if email_open_dttm ne . then email_open_dttm = tzoneu2s(email_open_dttm,&timeZone_Value.);if email_open_dttm_tz ne . then email_open_dttm_tz = tzoneu2s(email_open_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :email_open_tmp                  , email_open);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..email_open using &tmpdbschema..email_open_tmp                  
         ON (email_open.event_id=email_open_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET agent_family_nm = email_open_tmp.agent_family_nm , analysis_group_id = email_open_tmp.analysis_group_id , aud_occurrence_id = email_open_tmp.aud_occurrence_id , audience_id = email_open_tmp.audience_id , channel_user_id = email_open_tmp.channel_user_id , click_tracking_flg = email_open_tmp.click_tracking_flg , context_type_nm = email_open_tmp.context_type_nm , context_val = email_open_tmp.context_val , device_nm = email_open_tmp.device_nm , email_open_dttm = email_open_tmp.email_open_dttm , email_open_dttm_tz = email_open_tmp.email_open_dttm_tz , event_designed_id = email_open_tmp.event_designed_id , event_nm = email_open_tmp.event_nm , identity_id = email_open_tmp.identity_id , imprint_id = email_open_tmp.imprint_id , is_mobile_flg = email_open_tmp.is_mobile_flg , journey_id = email_open_tmp.journey_id , journey_occurrence_id = email_open_tmp.journey_occurrence_id , load_dttm = email_open_tmp.load_dttm , mailbox_provider_nm = email_open_tmp.mailbox_provider_nm , manufacturer_nm = email_open_tmp.manufacturer_nm , occurrence_id = email_open_tmp.occurrence_id , open_tracking_flg = email_open_tmp.open_tracking_flg , platform_desc = email_open_tmp.platform_desc , platform_version = email_open_tmp.platform_version , prefetched_flg = email_open_tmp.prefetched_flg , program_id = email_open_tmp.program_id , properties_map_doc = email_open_tmp.properties_map_doc , recipient_domain_nm = email_open_tmp.recipient_domain_nm , response_tracking_cd = email_open_tmp.response_tracking_cd , segment_id = email_open_tmp.segment_id , segment_version_id = email_open_tmp.segment_version_id , subject_line_txt = email_open_tmp.subject_line_txt , task_id = email_open_tmp.task_id , task_version_id = email_open_tmp.task_version_id , test_flg = email_open_tmp.test_flg , user_agent_nm = email_open_tmp.user_agent_nm
        WHEN NOT MATCHED THEN INSERT ( 
        agent_family_nm,analysis_group_id,aud_occurrence_id,audience_id,channel_user_id,click_tracking_flg,context_type_nm,context_val,device_nm,email_open_dttm,email_open_dttm_tz,event_designed_id,event_id,event_nm,identity_id,imprint_id,is_mobile_flg,journey_id,journey_occurrence_id,load_dttm,mailbox_provider_nm,manufacturer_nm,occurrence_id,open_tracking_flg,platform_desc,platform_version,prefetched_flg,program_id,properties_map_doc,recipient_domain_nm,response_tracking_cd,segment_id,segment_version_id,subject_line_txt,task_id,task_version_id,test_flg,user_agent_nm
         ) values ( 
        email_open_tmp.agent_family_nm,email_open_tmp.analysis_group_id,email_open_tmp.aud_occurrence_id,email_open_tmp.audience_id,email_open_tmp.channel_user_id,email_open_tmp.click_tracking_flg,email_open_tmp.context_type_nm,email_open_tmp.context_val,email_open_tmp.device_nm,email_open_tmp.email_open_dttm,email_open_tmp.email_open_dttm_tz,email_open_tmp.event_designed_id,email_open_tmp.event_id,email_open_tmp.event_nm,email_open_tmp.identity_id,email_open_tmp.imprint_id,email_open_tmp.is_mobile_flg,email_open_tmp.journey_id,email_open_tmp.journey_occurrence_id,email_open_tmp.load_dttm,email_open_tmp.mailbox_provider_nm,email_open_tmp.manufacturer_nm,email_open_tmp.occurrence_id,email_open_tmp.open_tracking_flg,email_open_tmp.platform_desc,email_open_tmp.platform_version,email_open_tmp.prefetched_flg,email_open_tmp.program_id,email_open_tmp.properties_map_doc,email_open_tmp.recipient_domain_nm,email_open_tmp.response_tracking_cd,email_open_tmp.segment_id,email_open_tmp.segment_version_id,email_open_tmp.subject_line_txt,email_open_tmp.task_id,email_open_tmp.task_version_id,email_open_tmp.test_flg,email_open_tmp.user_agent_nm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :email_open_tmp                  , email_open, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..email_open_tmp                  ;
    QUIT;
    %put ######## Staging table: email_open_tmp                   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..email_open;
      DROP TABLE work.email_open;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table email_open;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..email_optout) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..email_optout_tmp                ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..email_optout_tmp                ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=email_optout, table_keys=%str(event_id), out_table=work.email_optout);
 data &tmplib..email_optout_tmp                ;
     set work.email_optout;
  if email_optout_dttm ne . then email_optout_dttm = tzoneu2s(email_optout_dttm,&timeZone_Value.);if email_optout_dttm_tz ne . then email_optout_dttm_tz = tzoneu2s(email_optout_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :email_optout_tmp                , email_optout);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..email_optout using &tmpdbschema..email_optout_tmp                
         ON (email_optout.event_id=email_optout_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET analysis_group_id = email_optout_tmp.analysis_group_id , aud_occurrence_id = email_optout_tmp.aud_occurrence_id , audience_id = email_optout_tmp.audience_id , channel_user_id = email_optout_tmp.channel_user_id , context_type_nm = email_optout_tmp.context_type_nm , context_val = email_optout_tmp.context_val , email_optout_dttm = email_optout_tmp.email_optout_dttm , email_optout_dttm_tz = email_optout_tmp.email_optout_dttm_tz , event_designed_id = email_optout_tmp.event_designed_id , event_nm = email_optout_tmp.event_nm , identity_id = email_optout_tmp.identity_id , imprint_id = email_optout_tmp.imprint_id , journey_id = email_optout_tmp.journey_id , journey_occurrence_id = email_optout_tmp.journey_occurrence_id , link_tracking_group_txt = email_optout_tmp.link_tracking_group_txt , link_tracking_id = email_optout_tmp.link_tracking_id , link_tracking_label_txt = email_optout_tmp.link_tracking_label_txt , load_dttm = email_optout_tmp.load_dttm , occurrence_id = email_optout_tmp.occurrence_id , optout_type_nm = email_optout_tmp.optout_type_nm , program_id = email_optout_tmp.program_id , properties_map_doc = email_optout_tmp.properties_map_doc , recipient_domain_nm = email_optout_tmp.recipient_domain_nm , response_tracking_cd = email_optout_tmp.response_tracking_cd , segment_id = email_optout_tmp.segment_id , segment_version_id = email_optout_tmp.segment_version_id , subject_line_txt = email_optout_tmp.subject_line_txt , task_id = email_optout_tmp.task_id , task_version_id = email_optout_tmp.task_version_id , test_flg = email_optout_tmp.test_flg
        WHEN NOT MATCHED THEN INSERT ( 
        analysis_group_id,aud_occurrence_id,audience_id,channel_user_id,context_type_nm,context_val,email_optout_dttm,email_optout_dttm_tz,event_designed_id,event_id,event_nm,identity_id,imprint_id,journey_id,journey_occurrence_id,link_tracking_group_txt,link_tracking_id,link_tracking_label_txt,load_dttm,occurrence_id,optout_type_nm,program_id,properties_map_doc,recipient_domain_nm,response_tracking_cd,segment_id,segment_version_id,subject_line_txt,task_id,task_version_id,test_flg
         ) values ( 
        email_optout_tmp.analysis_group_id,email_optout_tmp.aud_occurrence_id,email_optout_tmp.audience_id,email_optout_tmp.channel_user_id,email_optout_tmp.context_type_nm,email_optout_tmp.context_val,email_optout_tmp.email_optout_dttm,email_optout_tmp.email_optout_dttm_tz,email_optout_tmp.event_designed_id,email_optout_tmp.event_id,email_optout_tmp.event_nm,email_optout_tmp.identity_id,email_optout_tmp.imprint_id,email_optout_tmp.journey_id,email_optout_tmp.journey_occurrence_id,email_optout_tmp.link_tracking_group_txt,email_optout_tmp.link_tracking_id,email_optout_tmp.link_tracking_label_txt,email_optout_tmp.load_dttm,email_optout_tmp.occurrence_id,email_optout_tmp.optout_type_nm,email_optout_tmp.program_id,email_optout_tmp.properties_map_doc,email_optout_tmp.recipient_domain_nm,email_optout_tmp.response_tracking_cd,email_optout_tmp.segment_id,email_optout_tmp.segment_version_id,email_optout_tmp.subject_line_txt,email_optout_tmp.task_id,email_optout_tmp.task_version_id,email_optout_tmp.test_flg
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :email_optout_tmp                , email_optout, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..email_optout_tmp                ;
    QUIT;
    %put ######## Staging table: email_optout_tmp                 Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..email_optout;
      DROP TABLE work.email_optout;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table email_optout;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..email_optout_details) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..email_optout_details_tmp        ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..email_optout_details_tmp        ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=email_optout_details, table_keys=%str(event_id), out_table=work.email_optout_details);
 data &tmplib..email_optout_details_tmp        ;
     set work.email_optout_details;
  if email_action_dttm ne . then email_action_dttm = tzoneu2s(email_action_dttm,&timeZone_Value.);if email_action_dttm_tz ne . then email_action_dttm_tz = tzoneu2s(email_action_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :email_optout_details_tmp        , email_optout_details);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..email_optout_details using &tmpdbschema..email_optout_details_tmp        
         ON (email_optout_details.event_id=email_optout_details_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET analysis_group_id = email_optout_details_tmp.analysis_group_id , aud_occurrence_id = email_optout_details_tmp.aud_occurrence_id , audience_id = email_optout_details_tmp.audience_id , context_type_nm = email_optout_details_tmp.context_type_nm , context_val = email_optout_details_tmp.context_val , email_action_dttm = email_optout_details_tmp.email_action_dttm , email_action_dttm_tz = email_optout_details_tmp.email_action_dttm_tz , email_address = email_optout_details_tmp.email_address , event_designed_id = email_optout_details_tmp.event_designed_id , event_nm = email_optout_details_tmp.event_nm , identity_id = email_optout_details_tmp.identity_id , imprint_id = email_optout_details_tmp.imprint_id , journey_id = email_optout_details_tmp.journey_id , journey_occurrence_id = email_optout_details_tmp.journey_occurrence_id , load_dttm = email_optout_details_tmp.load_dttm , occurrence_id = email_optout_details_tmp.occurrence_id , optout_type_nm = email_optout_details_tmp.optout_type_nm , program_id = email_optout_details_tmp.program_id , properties_map_doc = email_optout_details_tmp.properties_map_doc , recipient_domain_nm = email_optout_details_tmp.recipient_domain_nm , response_tracking_cd = email_optout_details_tmp.response_tracking_cd , segment_id = email_optout_details_tmp.segment_id , segment_version_id = email_optout_details_tmp.segment_version_id , subject_line_txt = email_optout_details_tmp.subject_line_txt , task_id = email_optout_details_tmp.task_id , task_version_id = email_optout_details_tmp.task_version_id , test_flg = email_optout_details_tmp.test_flg
        WHEN NOT MATCHED THEN INSERT ( 
        analysis_group_id,aud_occurrence_id,audience_id,context_type_nm,context_val,email_action_dttm,email_action_dttm_tz,email_address,event_designed_id,event_id,event_nm,identity_id,imprint_id,journey_id,journey_occurrence_id,load_dttm,occurrence_id,optout_type_nm,program_id,properties_map_doc,recipient_domain_nm,response_tracking_cd,segment_id,segment_version_id,subject_line_txt,task_id,task_version_id,test_flg
         ) values ( 
        email_optout_details_tmp.analysis_group_id,email_optout_details_tmp.aud_occurrence_id,email_optout_details_tmp.audience_id,email_optout_details_tmp.context_type_nm,email_optout_details_tmp.context_val,email_optout_details_tmp.email_action_dttm,email_optout_details_tmp.email_action_dttm_tz,email_optout_details_tmp.email_address,email_optout_details_tmp.event_designed_id,email_optout_details_tmp.event_id,email_optout_details_tmp.event_nm,email_optout_details_tmp.identity_id,email_optout_details_tmp.imprint_id,email_optout_details_tmp.journey_id,email_optout_details_tmp.journey_occurrence_id,email_optout_details_tmp.load_dttm,email_optout_details_tmp.occurrence_id,email_optout_details_tmp.optout_type_nm,email_optout_details_tmp.program_id,email_optout_details_tmp.properties_map_doc,email_optout_details_tmp.recipient_domain_nm,email_optout_details_tmp.response_tracking_cd,email_optout_details_tmp.segment_id,email_optout_details_tmp.segment_version_id,email_optout_details_tmp.subject_line_txt,email_optout_details_tmp.task_id,email_optout_details_tmp.task_version_id,email_optout_details_tmp.test_flg
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :email_optout_details_tmp        , email_optout_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..email_optout_details_tmp        ;
    QUIT;
    %put ######## Staging table: email_optout_details_tmp         Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..email_optout_details;
      DROP TABLE work.email_optout_details;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table email_optout_details;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..email_reply) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..email_reply_tmp                 ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..email_reply_tmp                 ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=email_reply, table_keys=%str(event_id), out_table=work.email_reply);
 data &tmplib..email_reply_tmp                 ;
     set work.email_reply;
  if email_reply_dttm ne . then email_reply_dttm = tzoneu2s(email_reply_dttm,&timeZone_Value.);if email_reply_dttm_tz ne . then email_reply_dttm_tz = tzoneu2s(email_reply_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :email_reply_tmp                 , email_reply);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..email_reply using &tmpdbschema..email_reply_tmp                 
         ON (email_reply.event_id=email_reply_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET analysis_group_id = email_reply_tmp.analysis_group_id , aud_occurrence_id = email_reply_tmp.aud_occurrence_id , audience_id = email_reply_tmp.audience_id , channel_user_id = email_reply_tmp.channel_user_id , context_type_nm = email_reply_tmp.context_type_nm , context_val = email_reply_tmp.context_val , email_reply_dttm = email_reply_tmp.email_reply_dttm , email_reply_dttm_tz = email_reply_tmp.email_reply_dttm_tz , event_designed_id = email_reply_tmp.event_designed_id , event_nm = email_reply_tmp.event_nm , identity_id = email_reply_tmp.identity_id , imprint_id = email_reply_tmp.imprint_id , journey_id = email_reply_tmp.journey_id , journey_occurrence_id = email_reply_tmp.journey_occurrence_id , load_dttm = email_reply_tmp.load_dttm , occurrence_id = email_reply_tmp.occurrence_id , program_id = email_reply_tmp.program_id , properties_map_doc = email_reply_tmp.properties_map_doc , recipient_domain_nm = email_reply_tmp.recipient_domain_nm , response_tracking_cd = email_reply_tmp.response_tracking_cd , segment_id = email_reply_tmp.segment_id , segment_version_id = email_reply_tmp.segment_version_id , subject_line_txt = email_reply_tmp.subject_line_txt , task_id = email_reply_tmp.task_id , task_version_id = email_reply_tmp.task_version_id , test_flg = email_reply_tmp.test_flg , uri_txt = email_reply_tmp.uri_txt
        WHEN NOT MATCHED THEN INSERT ( 
        analysis_group_id,aud_occurrence_id,audience_id,channel_user_id,context_type_nm,context_val,email_reply_dttm,email_reply_dttm_tz,event_designed_id,event_id,event_nm,identity_id,imprint_id,journey_id,journey_occurrence_id,load_dttm,occurrence_id,program_id,properties_map_doc,recipient_domain_nm,response_tracking_cd,segment_id,segment_version_id,subject_line_txt,task_id,task_version_id,test_flg,uri_txt
         ) values ( 
        email_reply_tmp.analysis_group_id,email_reply_tmp.aud_occurrence_id,email_reply_tmp.audience_id,email_reply_tmp.channel_user_id,email_reply_tmp.context_type_nm,email_reply_tmp.context_val,email_reply_tmp.email_reply_dttm,email_reply_tmp.email_reply_dttm_tz,email_reply_tmp.event_designed_id,email_reply_tmp.event_id,email_reply_tmp.event_nm,email_reply_tmp.identity_id,email_reply_tmp.imprint_id,email_reply_tmp.journey_id,email_reply_tmp.journey_occurrence_id,email_reply_tmp.load_dttm,email_reply_tmp.occurrence_id,email_reply_tmp.program_id,email_reply_tmp.properties_map_doc,email_reply_tmp.recipient_domain_nm,email_reply_tmp.response_tracking_cd,email_reply_tmp.segment_id,email_reply_tmp.segment_version_id,email_reply_tmp.subject_line_txt,email_reply_tmp.task_id,email_reply_tmp.task_version_id,email_reply_tmp.test_flg,email_reply_tmp.uri_txt
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :email_reply_tmp                 , email_reply, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..email_reply_tmp                 ;
    QUIT;
    %put ######## Staging table: email_reply_tmp                  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..email_reply;
      DROP TABLE work.email_reply;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table email_reply;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..email_send) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..email_send_tmp                  ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..email_send_tmp                  ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=email_send, table_keys=%str(event_id), out_table=work.email_send);
 data &tmplib..email_send_tmp                  ;
     set work.email_send;
  if email_send_dttm ne . then email_send_dttm = tzoneu2s(email_send_dttm,&timeZone_Value.);if email_send_dttm_tz ne . then email_send_dttm_tz = tzoneu2s(email_send_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :email_send_tmp                  , email_send);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..email_send using &tmpdbschema..email_send_tmp                  
         ON (email_send.event_id=email_send_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET analysis_group_id = email_send_tmp.analysis_group_id , aud_occurrence_id = email_send_tmp.aud_occurrence_id , audience_id = email_send_tmp.audience_id , channel_user_id = email_send_tmp.channel_user_id , context_type_nm = email_send_tmp.context_type_nm , context_val = email_send_tmp.context_val , email_send_dttm = email_send_tmp.email_send_dttm , email_send_dttm_tz = email_send_tmp.email_send_dttm_tz , event_designed_id = email_send_tmp.event_designed_id , event_nm = email_send_tmp.event_nm , identity_id = email_send_tmp.identity_id , imprint_id = email_send_tmp.imprint_id , imprint_url_txt = email_send_tmp.imprint_url_txt , journey_id = email_send_tmp.journey_id , journey_occurrence_id = email_send_tmp.journey_occurrence_id , load_dttm = email_send_tmp.load_dttm , occurrence_id = email_send_tmp.occurrence_id , program_id = email_send_tmp.program_id , properties_map_doc = email_send_tmp.properties_map_doc , recipient_domain_nm = email_send_tmp.recipient_domain_nm , response_tracking_cd = email_send_tmp.response_tracking_cd , segment_id = email_send_tmp.segment_id , segment_version_id = email_send_tmp.segment_version_id , subject_line_txt = email_send_tmp.subject_line_txt , task_id = email_send_tmp.task_id , task_version_id = email_send_tmp.task_version_id , test_flg = email_send_tmp.test_flg
        WHEN NOT MATCHED THEN INSERT ( 
        analysis_group_id,aud_occurrence_id,audience_id,channel_user_id,context_type_nm,context_val,email_send_dttm,email_send_dttm_tz,event_designed_id,event_id,event_nm,identity_id,imprint_id,imprint_url_txt,journey_id,journey_occurrence_id,load_dttm,occurrence_id,program_id,properties_map_doc,recipient_domain_nm,response_tracking_cd,segment_id,segment_version_id,subject_line_txt,task_id,task_version_id,test_flg
         ) values ( 
        email_send_tmp.analysis_group_id,email_send_tmp.aud_occurrence_id,email_send_tmp.audience_id,email_send_tmp.channel_user_id,email_send_tmp.context_type_nm,email_send_tmp.context_val,email_send_tmp.email_send_dttm,email_send_tmp.email_send_dttm_tz,email_send_tmp.event_designed_id,email_send_tmp.event_id,email_send_tmp.event_nm,email_send_tmp.identity_id,email_send_tmp.imprint_id,email_send_tmp.imprint_url_txt,email_send_tmp.journey_id,email_send_tmp.journey_occurrence_id,email_send_tmp.load_dttm,email_send_tmp.occurrence_id,email_send_tmp.program_id,email_send_tmp.properties_map_doc,email_send_tmp.recipient_domain_nm,email_send_tmp.response_tracking_cd,email_send_tmp.segment_id,email_send_tmp.segment_version_id,email_send_tmp.subject_line_txt,email_send_tmp.task_id,email_send_tmp.task_version_id,email_send_tmp.test_flg
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :email_send_tmp                  , email_send, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..email_send_tmp                  ;
    QUIT;
    %put ######## Staging table: email_send_tmp                   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..email_send;
      DROP TABLE work.email_send;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table email_send;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..email_view) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..email_view_tmp                  ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..email_view_tmp                  ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=email_view, table_keys=%str(event_id), out_table=work.email_view);
 data &tmplib..email_view_tmp                  ;
     set work.email_view;
  if email_view_dttm ne . then email_view_dttm = tzoneu2s(email_view_dttm,&timeZone_Value.);if email_view_dttm_tz ne . then email_view_dttm_tz = tzoneu2s(email_view_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :email_view_tmp                  , email_view);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..email_view using &tmpdbschema..email_view_tmp                  
         ON (email_view.event_id=email_view_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET analysis_group_id = email_view_tmp.analysis_group_id , aud_occurrence_id = email_view_tmp.aud_occurrence_id , audience_id = email_view_tmp.audience_id , channel_user_id = email_view_tmp.channel_user_id , context_type_nm = email_view_tmp.context_type_nm , context_val = email_view_tmp.context_val , email_view_dttm = email_view_tmp.email_view_dttm , email_view_dttm_tz = email_view_tmp.email_view_dttm_tz , event_designed_id = email_view_tmp.event_designed_id , event_nm = email_view_tmp.event_nm , identity_id = email_view_tmp.identity_id , imprint_id = email_view_tmp.imprint_id , link_tracking_group_txt = email_view_tmp.link_tracking_group_txt , link_tracking_id = email_view_tmp.link_tracking_id , link_tracking_label_txt = email_view_tmp.link_tracking_label_txt , load_dttm = email_view_tmp.load_dttm , occurrence_id = email_view_tmp.occurrence_id , program_id = email_view_tmp.program_id , properties_map_doc = email_view_tmp.properties_map_doc , recipient_domain_nm = email_view_tmp.recipient_domain_nm , response_tracking_cd = email_view_tmp.response_tracking_cd , segment_id = email_view_tmp.segment_id , segment_version_id = email_view_tmp.segment_version_id , subject_line_txt = email_view_tmp.subject_line_txt , task_id = email_view_tmp.task_id , task_version_id = email_view_tmp.task_version_id , test_flg = email_view_tmp.test_flg
        WHEN NOT MATCHED THEN INSERT ( 
        analysis_group_id,aud_occurrence_id,audience_id,channel_user_id,context_type_nm,context_val,email_view_dttm,email_view_dttm_tz,event_designed_id,event_id,event_nm,identity_id,imprint_id,link_tracking_group_txt,link_tracking_id,link_tracking_label_txt,load_dttm,occurrence_id,program_id,properties_map_doc,recipient_domain_nm,response_tracking_cd,segment_id,segment_version_id,subject_line_txt,task_id,task_version_id,test_flg
         ) values ( 
        email_view_tmp.analysis_group_id,email_view_tmp.aud_occurrence_id,email_view_tmp.audience_id,email_view_tmp.channel_user_id,email_view_tmp.context_type_nm,email_view_tmp.context_val,email_view_tmp.email_view_dttm,email_view_tmp.email_view_dttm_tz,email_view_tmp.event_designed_id,email_view_tmp.event_id,email_view_tmp.event_nm,email_view_tmp.identity_id,email_view_tmp.imprint_id,email_view_tmp.link_tracking_group_txt,email_view_tmp.link_tracking_id,email_view_tmp.link_tracking_label_txt,email_view_tmp.load_dttm,email_view_tmp.occurrence_id,email_view_tmp.program_id,email_view_tmp.properties_map_doc,email_view_tmp.recipient_domain_nm,email_view_tmp.response_tracking_cd,email_view_tmp.segment_id,email_view_tmp.segment_version_id,email_view_tmp.subject_line_txt,email_view_tmp.task_id,email_view_tmp.task_version_id,email_view_tmp.test_flg
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :email_view_tmp                  , email_view, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..email_view_tmp                  ;
    QUIT;
    %put ######## Staging table: email_view_tmp                   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..email_view;
      DROP TABLE work.email_view;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table email_view;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..external_event) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..external_event_tmp              ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..external_event_tmp              ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=external_event, table_keys=%str(event_id), out_table=work.external_event);
 data &tmplib..external_event_tmp              ;
     set work.external_event;
  if external_event_dttm ne . then external_event_dttm = tzoneu2s(external_event_dttm,&timeZone_Value.);if external_event_dttm_tz ne . then external_event_dttm_tz = tzoneu2s(external_event_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :external_event_tmp              , external_event);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..external_event using &tmpdbschema..external_event_tmp              
         ON (external_event.event_id=external_event_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET channel_nm = external_event_tmp.channel_nm , channel_user_id = external_event_tmp.channel_user_id , context_type_nm = external_event_tmp.context_type_nm , context_val = external_event_tmp.context_val , event_designed_id = external_event_tmp.event_designed_id , event_nm = external_event_tmp.event_nm , external_event_dttm = external_event_tmp.external_event_dttm , external_event_dttm_tz = external_event_tmp.external_event_dttm_tz , identity_id = external_event_tmp.identity_id , load_dttm = external_event_tmp.load_dttm , properties_map_doc = external_event_tmp.properties_map_doc , response_tracking_cd = external_event_tmp.response_tracking_cd
        WHEN NOT MATCHED THEN INSERT ( 
        channel_nm,channel_user_id,context_type_nm,context_val,event_designed_id,event_id,event_nm,external_event_dttm,external_event_dttm_tz,identity_id,load_dttm,properties_map_doc,response_tracking_cd
         ) values ( 
        external_event_tmp.channel_nm,external_event_tmp.channel_user_id,external_event_tmp.context_type_nm,external_event_tmp.context_val,external_event_tmp.event_designed_id,external_event_tmp.event_id,external_event_tmp.event_nm,external_event_tmp.external_event_dttm,external_event_tmp.external_event_dttm_tz,external_event_tmp.identity_id,external_event_tmp.load_dttm,external_event_tmp.properties_map_doc,external_event_tmp.response_tracking_cd
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :external_event_tmp              , external_event, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..external_event_tmp              ;
    QUIT;
    %put ######## Staging table: external_event_tmp               Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..external_event;
      DROP TABLE work.external_event;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table external_event;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..fiscal_cc_budget) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..fiscal_cc_budget_tmp            ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..fiscal_cc_budget_tmp            ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=fiscal_cc_budget, table_keys=%str(cost_center_id,fp_id), out_table=work.fiscal_cc_budget);
 data &tmplib..fiscal_cc_budget_tmp            ;
     set work.fiscal_cc_budget;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if cost_center_id='' then cost_center_id='-'; if fp_id='' then fp_id='-';
 run;
 %ErrCheck (Failed to Append Data to :fiscal_cc_budget_tmp            , fiscal_cc_budget);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..fiscal_cc_budget using &tmpdbschema..fiscal_cc_budget_tmp            
         ON (fiscal_cc_budget.cost_center_id=fiscal_cc_budget_tmp.cost_center_id and fiscal_cc_budget.fp_id=fiscal_cc_budget_tmp.fp_id)
        WHEN MATCHED THEN  
        UPDATE SET cc_bdgt_amt = fiscal_cc_budget_tmp.cc_bdgt_amt , cc_bdgt_budget_amt = fiscal_cc_budget_tmp.cc_bdgt_budget_amt , cc_bdgt_budget_desc = fiscal_cc_budget_tmp.cc_bdgt_budget_desc , cc_bdgt_cmtmnt_invoice_amt = fiscal_cc_budget_tmp.cc_bdgt_cmtmnt_invoice_amt , cc_bdgt_cmtmnt_invoice_cnt = fiscal_cc_budget_tmp.cc_bdgt_cmtmnt_invoice_cnt , cc_bdgt_cmtmnt_outstanding_amt = fiscal_cc_budget_tmp.cc_bdgt_cmtmnt_outstanding_amt , cc_bdgt_cmtmnt_overspent_amt = fiscal_cc_budget_tmp.cc_bdgt_cmtmnt_overspent_amt , cc_bdgt_committed_amt = fiscal_cc_budget_tmp.cc_bdgt_committed_amt , cc_bdgt_direct_invoice_amt = fiscal_cc_budget_tmp.cc_bdgt_direct_invoice_amt , cc_bdgt_invoiced_amt = fiscal_cc_budget_tmp.cc_bdgt_invoiced_amt , cc_desc = fiscal_cc_budget_tmp.cc_desc , cc_nm = fiscal_cc_budget_tmp.cc_nm , cc_number = fiscal_cc_budget_tmp.cc_number , cc_obsolete_flg = fiscal_cc_budget_tmp.cc_obsolete_flg , cc_owner_usernm = fiscal_cc_budget_tmp.cc_owner_usernm , created_by_usernm = fiscal_cc_budget_tmp.created_by_usernm , created_dttm = fiscal_cc_budget_tmp.created_dttm , fin_accnt_desc = fiscal_cc_budget_tmp.fin_accnt_desc , fin_accnt_nm = fiscal_cc_budget_tmp.fin_accnt_nm , fin_accnt_obsolete_flg = fiscal_cc_budget_tmp.fin_accnt_obsolete_flg , fp_cls_ver = fiscal_cc_budget_tmp.fp_cls_ver , fp_desc = fiscal_cc_budget_tmp.fp_desc , fp_end_dt = fiscal_cc_budget_tmp.fp_end_dt , fp_nm = fiscal_cc_budget_tmp.fp_nm , fp_obsolete_flg = fiscal_cc_budget_tmp.fp_obsolete_flg , fp_start_dt = fiscal_cc_budget_tmp.fp_start_dt , gen_ledger_cd = fiscal_cc_budget_tmp.gen_ledger_cd , last_modified_dttm = fiscal_cc_budget_tmp.last_modified_dttm , last_modified_usernm = fiscal_cc_budget_tmp.last_modified_usernm , load_dttm = fiscal_cc_budget_tmp.load_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        cc_bdgt_amt,cc_bdgt_budget_amt,cc_bdgt_budget_desc,cc_bdgt_cmtmnt_invoice_amt,cc_bdgt_cmtmnt_invoice_cnt,cc_bdgt_cmtmnt_outstanding_amt,cc_bdgt_cmtmnt_overspent_amt,cc_bdgt_committed_amt,cc_bdgt_direct_invoice_amt,cc_bdgt_invoiced_amt,cc_desc,cc_nm,cc_number,cc_obsolete_flg,cc_owner_usernm,cost_center_id,created_by_usernm,created_dttm,fin_accnt_desc,fin_accnt_nm,fin_accnt_obsolete_flg,fp_cls_ver,fp_desc,fp_end_dt,fp_id,fp_nm,fp_obsolete_flg,fp_start_dt,gen_ledger_cd,last_modified_dttm,last_modified_usernm,load_dttm
         ) values ( 
        fiscal_cc_budget_tmp.cc_bdgt_amt,fiscal_cc_budget_tmp.cc_bdgt_budget_amt,fiscal_cc_budget_tmp.cc_bdgt_budget_desc,fiscal_cc_budget_tmp.cc_bdgt_cmtmnt_invoice_amt,fiscal_cc_budget_tmp.cc_bdgt_cmtmnt_invoice_cnt,fiscal_cc_budget_tmp.cc_bdgt_cmtmnt_outstanding_amt,fiscal_cc_budget_tmp.cc_bdgt_cmtmnt_overspent_amt,fiscal_cc_budget_tmp.cc_bdgt_committed_amt,fiscal_cc_budget_tmp.cc_bdgt_direct_invoice_amt,fiscal_cc_budget_tmp.cc_bdgt_invoiced_amt,fiscal_cc_budget_tmp.cc_desc,fiscal_cc_budget_tmp.cc_nm,fiscal_cc_budget_tmp.cc_number,fiscal_cc_budget_tmp.cc_obsolete_flg,fiscal_cc_budget_tmp.cc_owner_usernm,fiscal_cc_budget_tmp.cost_center_id,fiscal_cc_budget_tmp.created_by_usernm,fiscal_cc_budget_tmp.created_dttm,fiscal_cc_budget_tmp.fin_accnt_desc,fiscal_cc_budget_tmp.fin_accnt_nm,fiscal_cc_budget_tmp.fin_accnt_obsolete_flg,fiscal_cc_budget_tmp.fp_cls_ver,fiscal_cc_budget_tmp.fp_desc,fiscal_cc_budget_tmp.fp_end_dt,fiscal_cc_budget_tmp.fp_id,fiscal_cc_budget_tmp.fp_nm,fiscal_cc_budget_tmp.fp_obsolete_flg,fiscal_cc_budget_tmp.fp_start_dt,fiscal_cc_budget_tmp.gen_ledger_cd,fiscal_cc_budget_tmp.last_modified_dttm,fiscal_cc_budget_tmp.last_modified_usernm,fiscal_cc_budget_tmp.load_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :fiscal_cc_budget_tmp            , fiscal_cc_budget, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..fiscal_cc_budget_tmp            ;
    QUIT;
    %put ######## Staging table: fiscal_cc_budget_tmp             Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..fiscal_cc_budget;
      DROP TABLE work.fiscal_cc_budget;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table fiscal_cc_budget;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..form_details) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..form_details_tmp                ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..form_details_tmp                ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=form_details, table_keys=%str(event_id), out_table=work.form_details);
 data &tmplib..form_details_tmp                ;
     set work.form_details;
  if form_field_detail_dttm ne . then form_field_detail_dttm = tzoneu2s(form_field_detail_dttm,&timeZone_Value.);if form_field_detail_dttm_tz ne . then form_field_detail_dttm_tz = tzoneu2s(form_field_detail_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :form_details_tmp                , form_details);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..form_details using &tmpdbschema..form_details_tmp                
         ON (form_details.event_id=form_details_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET attempt_index_cnt = form_details_tmp.attempt_index_cnt , attempt_status_cd = form_details_tmp.attempt_status_cd , change_index_no = form_details_tmp.change_index_no , detail_id = form_details_tmp.detail_id , detail_id_hex = form_details_tmp.detail_id_hex , event_key_cd = form_details_tmp.event_key_cd , event_source_cd = form_details_tmp.event_source_cd , form_field_detail_dttm = form_details_tmp.form_field_detail_dttm , form_field_detail_dttm_tz = form_details_tmp.form_field_detail_dttm_tz , form_field_id = form_details_tmp.form_field_id , form_field_nm = form_details_tmp.form_field_nm , form_field_value = form_details_tmp.form_field_value , form_nm = form_details_tmp.form_nm , identity_id = form_details_tmp.identity_id , load_dttm = form_details_tmp.load_dttm , session_id = form_details_tmp.session_id , session_id_hex = form_details_tmp.session_id_hex , submit_flg = form_details_tmp.submit_flg , visit_id = form_details_tmp.visit_id , visit_id_hex = form_details_tmp.visit_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        attempt_index_cnt,attempt_status_cd,change_index_no,detail_id,detail_id_hex,event_id,event_key_cd,event_source_cd,form_field_detail_dttm,form_field_detail_dttm_tz,form_field_id,form_field_nm,form_field_value,form_nm,identity_id,load_dttm,session_id,session_id_hex,submit_flg,visit_id,visit_id_hex
         ) values ( 
        form_details_tmp.attempt_index_cnt,form_details_tmp.attempt_status_cd,form_details_tmp.change_index_no,form_details_tmp.detail_id,form_details_tmp.detail_id_hex,form_details_tmp.event_id,form_details_tmp.event_key_cd,form_details_tmp.event_source_cd,form_details_tmp.form_field_detail_dttm,form_details_tmp.form_field_detail_dttm_tz,form_details_tmp.form_field_id,form_details_tmp.form_field_nm,form_details_tmp.form_field_value,form_details_tmp.form_nm,form_details_tmp.identity_id,form_details_tmp.load_dttm,form_details_tmp.session_id,form_details_tmp.session_id_hex,form_details_tmp.submit_flg,form_details_tmp.visit_id,form_details_tmp.visit_id_hex
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :form_details_tmp                , form_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..form_details_tmp                ;
    QUIT;
    %put ######## Staging table: form_details_tmp                 Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..form_details;
      DROP TABLE work.form_details;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table form_details;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..identity_attributes) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..identity_attributes_tmp         ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..identity_attributes_tmp         ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=identity_attributes, table_keys=%str(entrytime,identifier_type_id,identity_id), out_table=work.identity_attributes);
 data &tmplib..identity_attributes_tmp         ;
     set work.identity_attributes;
  if entrytime ne . then entrytime = tzoneu2s(entrytime,&timeZone_Value.);if processed_dttm ne . then processed_dttm = tzoneu2s(processed_dttm,&timeZone_Value.) ;
  if identifier_type_id='' then identifier_type_id='-'; if identity_id='' then identity_id='-';
 run;
 %ErrCheck (Failed to Append Data to :identity_attributes_tmp         , identity_attributes);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..identity_attributes using &tmpdbschema..identity_attributes_tmp         
         ON (identity_attributes.entrytime=identity_attributes_tmp.entrytime and identity_attributes.identifier_type_id=identity_attributes_tmp.identifier_type_id and identity_attributes.identity_id=identity_attributes_tmp.identity_id)
        WHEN MATCHED THEN  
        UPDATE SET processed_dttm = identity_attributes_tmp.processed_dttm , user_identifier_val = identity_attributes_tmp.user_identifier_val
        WHEN NOT MATCHED THEN INSERT ( 
        entrytime,identifier_type_id,identity_id,processed_dttm,user_identifier_val
         ) values ( 
        identity_attributes_tmp.entrytime,identity_attributes_tmp.identifier_type_id,identity_attributes_tmp.identity_id,identity_attributes_tmp.processed_dttm,identity_attributes_tmp.user_identifier_val
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :identity_attributes_tmp         , identity_attributes, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..identity_attributes_tmp         ;
    QUIT;
    %put ######## Staging table: identity_attributes_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..identity_attributes;
      DROP TABLE work.identity_attributes;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table identity_attributes;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..identity_map) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..identity_map_tmp                ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..identity_map_tmp                ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=identity_map, table_keys=%str(source_identity_id), out_table=work.identity_map);
 data &tmplib..identity_map_tmp                ;
     set work.identity_map;
  if entrytime ne . then entrytime = tzoneu2s(entrytime,&timeZone_Value.);if processed_dttm ne . then processed_dttm = tzoneu2s(processed_dttm,&timeZone_Value.) ;
  if source_identity_id='' then source_identity_id='-';
 run;
 %ErrCheck (Failed to Append Data to :identity_map_tmp                , identity_map);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..identity_map using &tmpdbschema..identity_map_tmp                
         ON (identity_map.source_identity_id=identity_map_tmp.source_identity_id)
        WHEN MATCHED THEN  
        UPDATE SET entrytime = identity_map_tmp.entrytime , processed_dttm = identity_map_tmp.processed_dttm , target_identity_id = identity_map_tmp.target_identity_id
        WHEN NOT MATCHED THEN INSERT ( 
        entrytime,processed_dttm,source_identity_id,target_identity_id
         ) values ( 
        identity_map_tmp.entrytime,identity_map_tmp.processed_dttm,identity_map_tmp.source_identity_id,identity_map_tmp.target_identity_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :identity_map_tmp                , identity_map, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..identity_map_tmp                ;
    QUIT;
    %put ######## Staging table: identity_map_tmp                 Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..identity_map;
      DROP TABLE work.identity_map;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table identity_map;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..impression_delivered) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..impression_delivered_tmp        ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..impression_delivered_tmp        ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=impression_delivered, table_keys=%str(event_id), out_table=work.impression_delivered);
 data &tmplib..impression_delivered_tmp        ;
     set work.impression_delivered;
  if impression_delivered_dttm ne . then impression_delivered_dttm = tzoneu2s(impression_delivered_dttm,&timeZone_Value.);if impression_delivered_dttm_tz ne . then impression_delivered_dttm_tz = tzoneu2s(impression_delivered_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :impression_delivered_tmp        , impression_delivered);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..impression_delivered using &tmpdbschema..impression_delivered_tmp        
         ON (impression_delivered.event_id=impression_delivered_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET aud_occurrence_id = impression_delivered_tmp.aud_occurrence_id , audience_id = impression_delivered_tmp.audience_id , channel_nm = impression_delivered_tmp.channel_nm , channel_user_id = impression_delivered_tmp.channel_user_id , context_type_nm = impression_delivered_tmp.context_type_nm , context_val = impression_delivered_tmp.context_val , control_group_flg = impression_delivered_tmp.control_group_flg , creative_id = impression_delivered_tmp.creative_id , creative_version_id = impression_delivered_tmp.creative_version_id , detail_id_hex = impression_delivered_tmp.detail_id_hex , event_designed_id = impression_delivered_tmp.event_designed_id , event_key_cd = impression_delivered_tmp.event_key_cd , event_nm = impression_delivered_tmp.event_nm , event_source_cd = impression_delivered_tmp.event_source_cd , identity_id = impression_delivered_tmp.identity_id , impression_delivered_dttm = impression_delivered_tmp.impression_delivered_dttm , impression_delivered_dttm_tz = impression_delivered_tmp.impression_delivered_dttm_tz , journey_id = impression_delivered_tmp.journey_id , journey_occurrence_id = impression_delivered_tmp.journey_occurrence_id , load_dttm = impression_delivered_tmp.load_dttm , message_id = impression_delivered_tmp.message_id , message_version_id = impression_delivered_tmp.message_version_id , mobile_app_id = impression_delivered_tmp.mobile_app_id , product_id = impression_delivered_tmp.product_id , product_nm = impression_delivered_tmp.product_nm , product_qty_no = impression_delivered_tmp.product_qty_no , product_sku_no = impression_delivered_tmp.product_sku_no , properties_map_doc = impression_delivered_tmp.properties_map_doc , rec_group_id = impression_delivered_tmp.rec_group_id , request_id = impression_delivered_tmp.request_id , reserved_1_txt = impression_delivered_tmp.reserved_1_txt , reserved_2_txt = impression_delivered_tmp.reserved_2_txt , response_tracking_cd = impression_delivered_tmp.response_tracking_cd , segment_id = impression_delivered_tmp.segment_id , segment_version_id = impression_delivered_tmp.segment_version_id , session_id_hex = impression_delivered_tmp.session_id_hex , spot_id = impression_delivered_tmp.spot_id , task_id = impression_delivered_tmp.task_id , task_version_id = impression_delivered_tmp.task_version_id , visit_id_hex = impression_delivered_tmp.visit_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        aud_occurrence_id,audience_id,channel_nm,channel_user_id,context_type_nm,context_val,control_group_flg,creative_id,creative_version_id,detail_id_hex,event_designed_id,event_id,event_key_cd,event_nm,event_source_cd,identity_id,impression_delivered_dttm,impression_delivered_dttm_tz,journey_id,journey_occurrence_id,load_dttm,message_id,message_version_id,mobile_app_id,product_id,product_nm,product_qty_no,product_sku_no,properties_map_doc,rec_group_id,request_id,reserved_1_txt,reserved_2_txt,response_tracking_cd,segment_id,segment_version_id,session_id_hex,spot_id,task_id,task_version_id,visit_id_hex
         ) values ( 
        impression_delivered_tmp.aud_occurrence_id,impression_delivered_tmp.audience_id,impression_delivered_tmp.channel_nm,impression_delivered_tmp.channel_user_id,impression_delivered_tmp.context_type_nm,impression_delivered_tmp.context_val,impression_delivered_tmp.control_group_flg,impression_delivered_tmp.creative_id,impression_delivered_tmp.creative_version_id,impression_delivered_tmp.detail_id_hex,impression_delivered_tmp.event_designed_id,impression_delivered_tmp.event_id,impression_delivered_tmp.event_key_cd,impression_delivered_tmp.event_nm,impression_delivered_tmp.event_source_cd,impression_delivered_tmp.identity_id,impression_delivered_tmp.impression_delivered_dttm,impression_delivered_tmp.impression_delivered_dttm_tz,impression_delivered_tmp.journey_id,impression_delivered_tmp.journey_occurrence_id,impression_delivered_tmp.load_dttm,impression_delivered_tmp.message_id,impression_delivered_tmp.message_version_id,impression_delivered_tmp.mobile_app_id,impression_delivered_tmp.product_id,impression_delivered_tmp.product_nm,impression_delivered_tmp.product_qty_no,impression_delivered_tmp.product_sku_no,impression_delivered_tmp.properties_map_doc,impression_delivered_tmp.rec_group_id,impression_delivered_tmp.request_id,impression_delivered_tmp.reserved_1_txt,impression_delivered_tmp.reserved_2_txt,impression_delivered_tmp.response_tracking_cd,impression_delivered_tmp.segment_id,impression_delivered_tmp.segment_version_id,impression_delivered_tmp.session_id_hex,impression_delivered_tmp.spot_id,impression_delivered_tmp.task_id,impression_delivered_tmp.task_version_id,impression_delivered_tmp.visit_id_hex
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :impression_delivered_tmp        , impression_delivered, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..impression_delivered_tmp        ;
    QUIT;
    %put ######## Staging table: impression_delivered_tmp         Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..impression_delivered;
      DROP TABLE work.impression_delivered;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table impression_delivered;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..impression_spot_viewable) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..impression_spot_viewable_tmp    ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..impression_spot_viewable_tmp    ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=impression_spot_viewable, table_keys=%str(event_id), out_table=work.impression_spot_viewable);
 data &tmplib..impression_spot_viewable_tmp    ;
     set work.impression_spot_viewable;
  if impression_viewable_dttm ne . then impression_viewable_dttm = tzoneu2s(impression_viewable_dttm,&timeZone_Value.);if impression_viewable_dttm_tz ne . then impression_viewable_dttm_tz = tzoneu2s(impression_viewable_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :impression_spot_viewable_tmp    , impression_spot_viewable);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..impression_spot_viewable using &tmpdbschema..impression_spot_viewable_tmp    
         ON (impression_spot_viewable.event_id=impression_spot_viewable_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET analysis_group_id = impression_spot_viewable_tmp.analysis_group_id , aud_occurrence_id = impression_spot_viewable_tmp.aud_occurrence_id , audience_id = impression_spot_viewable_tmp.audience_id , channel_nm = impression_spot_viewable_tmp.channel_nm , channel_user_id = impression_spot_viewable_tmp.channel_user_id , context_type_nm = impression_spot_viewable_tmp.context_type_nm , context_val = impression_spot_viewable_tmp.context_val , control_group_flg = impression_spot_viewable_tmp.control_group_flg , creative_id = impression_spot_viewable_tmp.creative_id , creative_version_id = impression_spot_viewable_tmp.creative_version_id , detail_id_hex = impression_spot_viewable_tmp.detail_id_hex , event_designed_id = impression_spot_viewable_tmp.event_designed_id , event_key_cd = impression_spot_viewable_tmp.event_key_cd , event_nm = impression_spot_viewable_tmp.event_nm , event_source_cd = impression_spot_viewable_tmp.event_source_cd , identity_id = impression_spot_viewable_tmp.identity_id , impression_viewable_dttm = impression_spot_viewable_tmp.impression_viewable_dttm , impression_viewable_dttm_tz = impression_spot_viewable_tmp.impression_viewable_dttm_tz , load_dttm = impression_spot_viewable_tmp.load_dttm , message_id = impression_spot_viewable_tmp.message_id , message_version_id = impression_spot_viewable_tmp.message_version_id , mobile_app_id = impression_spot_viewable_tmp.mobile_app_id , occurrence_id = impression_spot_viewable_tmp.occurrence_id , product_id = impression_spot_viewable_tmp.product_id , product_nm = impression_spot_viewable_tmp.product_nm , product_qty_no = impression_spot_viewable_tmp.product_qty_no , product_sku_no = impression_spot_viewable_tmp.product_sku_no , properties_map_doc = impression_spot_viewable_tmp.properties_map_doc , rec_group_id = impression_spot_viewable_tmp.rec_group_id , request_id = impression_spot_viewable_tmp.request_id , reserved_1_txt = impression_spot_viewable_tmp.reserved_1_txt , reserved_2_txt = impression_spot_viewable_tmp.reserved_2_txt , response_tracking_cd = impression_spot_viewable_tmp.response_tracking_cd , segment_id = impression_spot_viewable_tmp.segment_id , segment_version_id = impression_spot_viewable_tmp.segment_version_id , session_id_hex = impression_spot_viewable_tmp.session_id_hex , spot_id = impression_spot_viewable_tmp.spot_id , task_id = impression_spot_viewable_tmp.task_id , task_version_id = impression_spot_viewable_tmp.task_version_id , visit_id_hex = impression_spot_viewable_tmp.visit_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        analysis_group_id,aud_occurrence_id,audience_id,channel_nm,channel_user_id,context_type_nm,context_val,control_group_flg,creative_id,creative_version_id,detail_id_hex,event_designed_id,event_id,event_key_cd,event_nm,event_source_cd,identity_id,impression_viewable_dttm,impression_viewable_dttm_tz,load_dttm,message_id,message_version_id,mobile_app_id,occurrence_id,product_id,product_nm,product_qty_no,product_sku_no,properties_map_doc,rec_group_id,request_id,reserved_1_txt,reserved_2_txt,response_tracking_cd,segment_id,segment_version_id,session_id_hex,spot_id,task_id,task_version_id,visit_id_hex
         ) values ( 
        impression_spot_viewable_tmp.analysis_group_id,impression_spot_viewable_tmp.aud_occurrence_id,impression_spot_viewable_tmp.audience_id,impression_spot_viewable_tmp.channel_nm,impression_spot_viewable_tmp.channel_user_id,impression_spot_viewable_tmp.context_type_nm,impression_spot_viewable_tmp.context_val,impression_spot_viewable_tmp.control_group_flg,impression_spot_viewable_tmp.creative_id,impression_spot_viewable_tmp.creative_version_id,impression_spot_viewable_tmp.detail_id_hex,impression_spot_viewable_tmp.event_designed_id,impression_spot_viewable_tmp.event_id,impression_spot_viewable_tmp.event_key_cd,impression_spot_viewable_tmp.event_nm,impression_spot_viewable_tmp.event_source_cd,impression_spot_viewable_tmp.identity_id,impression_spot_viewable_tmp.impression_viewable_dttm,impression_spot_viewable_tmp.impression_viewable_dttm_tz,impression_spot_viewable_tmp.load_dttm,impression_spot_viewable_tmp.message_id,impression_spot_viewable_tmp.message_version_id,impression_spot_viewable_tmp.mobile_app_id,impression_spot_viewable_tmp.occurrence_id,impression_spot_viewable_tmp.product_id,impression_spot_viewable_tmp.product_nm,impression_spot_viewable_tmp.product_qty_no,impression_spot_viewable_tmp.product_sku_no,impression_spot_viewable_tmp.properties_map_doc,impression_spot_viewable_tmp.rec_group_id,impression_spot_viewable_tmp.request_id,impression_spot_viewable_tmp.reserved_1_txt,impression_spot_viewable_tmp.reserved_2_txt,impression_spot_viewable_tmp.response_tracking_cd,impression_spot_viewable_tmp.segment_id,impression_spot_viewable_tmp.segment_version_id,impression_spot_viewable_tmp.session_id_hex,impression_spot_viewable_tmp.spot_id,impression_spot_viewable_tmp.task_id,impression_spot_viewable_tmp.task_version_id,impression_spot_viewable_tmp.visit_id_hex
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :impression_spot_viewable_tmp    , impression_spot_viewable, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..impression_spot_viewable_tmp    ;
    QUIT;
    %put ######## Staging table: impression_spot_viewable_tmp     Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..impression_spot_viewable;
      DROP TABLE work.impression_spot_viewable;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table impression_spot_viewable;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..in_app_failed) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..in_app_failed_tmp               ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..in_app_failed_tmp               ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=in_app_failed, table_keys=%str(event_id), out_table=work.in_app_failed);
 data &tmplib..in_app_failed_tmp               ;
     set work.in_app_failed;
  if in_app_failed_dttm ne . then in_app_failed_dttm = tzoneu2s(in_app_failed_dttm,&timeZone_Value.);if in_app_failed_dttm_tz ne . then in_app_failed_dttm_tz = tzoneu2s(in_app_failed_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :in_app_failed_tmp               , in_app_failed);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..in_app_failed using &tmpdbschema..in_app_failed_tmp               
         ON (in_app_failed.event_id=in_app_failed_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET channel_nm = in_app_failed_tmp.channel_nm , channel_user_id = in_app_failed_tmp.channel_user_id , context_type_nm = in_app_failed_tmp.context_type_nm , context_val = in_app_failed_tmp.context_val , creative_id = in_app_failed_tmp.creative_id , creative_version_id = in_app_failed_tmp.creative_version_id , error_cd = in_app_failed_tmp.error_cd , error_message_txt = in_app_failed_tmp.error_message_txt , event_designed_id = in_app_failed_tmp.event_designed_id , event_nm = in_app_failed_tmp.event_nm , identity_id = in_app_failed_tmp.identity_id , in_app_failed_dttm = in_app_failed_tmp.in_app_failed_dttm , in_app_failed_dttm_tz = in_app_failed_tmp.in_app_failed_dttm_tz , load_dttm = in_app_failed_tmp.load_dttm , message_id = in_app_failed_tmp.message_id , message_version_id = in_app_failed_tmp.message_version_id , mobile_app_id = in_app_failed_tmp.mobile_app_id , occurrence_id = in_app_failed_tmp.occurrence_id , properties_map_doc = in_app_failed_tmp.properties_map_doc , reserved_1_txt = in_app_failed_tmp.reserved_1_txt , reserved_2_txt = in_app_failed_tmp.reserved_2_txt , response_tracking_cd = in_app_failed_tmp.response_tracking_cd , segment_id = in_app_failed_tmp.segment_id , segment_version_id = in_app_failed_tmp.segment_version_id , spot_id = in_app_failed_tmp.spot_id , task_id = in_app_failed_tmp.task_id , task_version_id = in_app_failed_tmp.task_version_id
        WHEN NOT MATCHED THEN INSERT ( 
        channel_nm,channel_user_id,context_type_nm,context_val,creative_id,creative_version_id,error_cd,error_message_txt,event_designed_id,event_id,event_nm,identity_id,in_app_failed_dttm,in_app_failed_dttm_tz,load_dttm,message_id,message_version_id,mobile_app_id,occurrence_id,properties_map_doc,reserved_1_txt,reserved_2_txt,response_tracking_cd,segment_id,segment_version_id,spot_id,task_id,task_version_id
         ) values ( 
        in_app_failed_tmp.channel_nm,in_app_failed_tmp.channel_user_id,in_app_failed_tmp.context_type_nm,in_app_failed_tmp.context_val,in_app_failed_tmp.creative_id,in_app_failed_tmp.creative_version_id,in_app_failed_tmp.error_cd,in_app_failed_tmp.error_message_txt,in_app_failed_tmp.event_designed_id,in_app_failed_tmp.event_id,in_app_failed_tmp.event_nm,in_app_failed_tmp.identity_id,in_app_failed_tmp.in_app_failed_dttm,in_app_failed_tmp.in_app_failed_dttm_tz,in_app_failed_tmp.load_dttm,in_app_failed_tmp.message_id,in_app_failed_tmp.message_version_id,in_app_failed_tmp.mobile_app_id,in_app_failed_tmp.occurrence_id,in_app_failed_tmp.properties_map_doc,in_app_failed_tmp.reserved_1_txt,in_app_failed_tmp.reserved_2_txt,in_app_failed_tmp.response_tracking_cd,in_app_failed_tmp.segment_id,in_app_failed_tmp.segment_version_id,in_app_failed_tmp.spot_id,in_app_failed_tmp.task_id,in_app_failed_tmp.task_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :in_app_failed_tmp               , in_app_failed, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..in_app_failed_tmp               ;
    QUIT;
    %put ######## Staging table: in_app_failed_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..in_app_failed;
      DROP TABLE work.in_app_failed;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table in_app_failed;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..in_app_message) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..in_app_message_tmp              ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..in_app_message_tmp              ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=in_app_message, table_keys=%str(event_id), out_table=work.in_app_message);
 data &tmplib..in_app_message_tmp              ;
     set work.in_app_message;
  if in_app_action_dttm ne . then in_app_action_dttm = tzoneu2s(in_app_action_dttm,&timeZone_Value.);if in_app_action_dttm_tz ne . then in_app_action_dttm_tz = tzoneu2s(in_app_action_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :in_app_message_tmp              , in_app_message);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..in_app_message using &tmpdbschema..in_app_message_tmp              
         ON (in_app_message.event_id=in_app_message_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET channel_nm = in_app_message_tmp.channel_nm , channel_user_id = in_app_message_tmp.channel_user_id , context_type_nm = in_app_message_tmp.context_type_nm , context_val = in_app_message_tmp.context_val , creative_id = in_app_message_tmp.creative_id , creative_version_id = in_app_message_tmp.creative_version_id , event_designed_id = in_app_message_tmp.event_designed_id , event_nm = in_app_message_tmp.event_nm , identity_id = in_app_message_tmp.identity_id , in_app_action_dttm = in_app_message_tmp.in_app_action_dttm , in_app_action_dttm_tz = in_app_message_tmp.in_app_action_dttm_tz , load_dttm = in_app_message_tmp.load_dttm , message_id = in_app_message_tmp.message_id , message_version_id = in_app_message_tmp.message_version_id , mobile_app_id = in_app_message_tmp.mobile_app_id , occurrence_id = in_app_message_tmp.occurrence_id , properties_map_doc = in_app_message_tmp.properties_map_doc , reserved_1_txt = in_app_message_tmp.reserved_1_txt , reserved_2_txt = in_app_message_tmp.reserved_2_txt , reserved_3_txt = in_app_message_tmp.reserved_3_txt , response_tracking_cd = in_app_message_tmp.response_tracking_cd , segment_id = in_app_message_tmp.segment_id , segment_version_id = in_app_message_tmp.segment_version_id , spot_id = in_app_message_tmp.spot_id , task_id = in_app_message_tmp.task_id , task_version_id = in_app_message_tmp.task_version_id
        WHEN NOT MATCHED THEN INSERT ( 
        channel_nm,channel_user_id,context_type_nm,context_val,creative_id,creative_version_id,event_designed_id,event_id,event_nm,identity_id,in_app_action_dttm,in_app_action_dttm_tz,load_dttm,message_id,message_version_id,mobile_app_id,occurrence_id,properties_map_doc,reserved_1_txt,reserved_2_txt,reserved_3_txt,response_tracking_cd,segment_id,segment_version_id,spot_id,task_id,task_version_id
         ) values ( 
        in_app_message_tmp.channel_nm,in_app_message_tmp.channel_user_id,in_app_message_tmp.context_type_nm,in_app_message_tmp.context_val,in_app_message_tmp.creative_id,in_app_message_tmp.creative_version_id,in_app_message_tmp.event_designed_id,in_app_message_tmp.event_id,in_app_message_tmp.event_nm,in_app_message_tmp.identity_id,in_app_message_tmp.in_app_action_dttm,in_app_message_tmp.in_app_action_dttm_tz,in_app_message_tmp.load_dttm,in_app_message_tmp.message_id,in_app_message_tmp.message_version_id,in_app_message_tmp.mobile_app_id,in_app_message_tmp.occurrence_id,in_app_message_tmp.properties_map_doc,in_app_message_tmp.reserved_1_txt,in_app_message_tmp.reserved_2_txt,in_app_message_tmp.reserved_3_txt,in_app_message_tmp.response_tracking_cd,in_app_message_tmp.segment_id,in_app_message_tmp.segment_version_id,in_app_message_tmp.spot_id,in_app_message_tmp.task_id,in_app_message_tmp.task_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :in_app_message_tmp              , in_app_message, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..in_app_message_tmp              ;
    QUIT;
    %put ######## Staging table: in_app_message_tmp               Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..in_app_message;
      DROP TABLE work.in_app_message;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table in_app_message;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..in_app_send) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..in_app_send_tmp                 ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..in_app_send_tmp                 ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=in_app_send, table_keys=%str(event_id), out_table=work.in_app_send);
 data &tmplib..in_app_send_tmp                 ;
     set work.in_app_send;
  if in_app_send_dttm ne . then in_app_send_dttm = tzoneu2s(in_app_send_dttm,&timeZone_Value.);if in_app_send_dttm_tz ne . then in_app_send_dttm_tz = tzoneu2s(in_app_send_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :in_app_send_tmp                 , in_app_send);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..in_app_send using &tmpdbschema..in_app_send_tmp                 
         ON (in_app_send.event_id=in_app_send_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET channel_nm = in_app_send_tmp.channel_nm , channel_user_id = in_app_send_tmp.channel_user_id , context_type_nm = in_app_send_tmp.context_type_nm , context_val = in_app_send_tmp.context_val , creative_id = in_app_send_tmp.creative_id , creative_version_id = in_app_send_tmp.creative_version_id , event_designed_id = in_app_send_tmp.event_designed_id , event_nm = in_app_send_tmp.event_nm , identity_id = in_app_send_tmp.identity_id , in_app_send_dttm = in_app_send_tmp.in_app_send_dttm , in_app_send_dttm_tz = in_app_send_tmp.in_app_send_dttm_tz , load_dttm = in_app_send_tmp.load_dttm , message_id = in_app_send_tmp.message_id , message_version_id = in_app_send_tmp.message_version_id , mobile_app_id = in_app_send_tmp.mobile_app_id , occurrence_id = in_app_send_tmp.occurrence_id , properties_map_doc = in_app_send_tmp.properties_map_doc , reserved_1_txt = in_app_send_tmp.reserved_1_txt , reserved_2_txt = in_app_send_tmp.reserved_2_txt , response_tracking_cd = in_app_send_tmp.response_tracking_cd , segment_id = in_app_send_tmp.segment_id , segment_version_id = in_app_send_tmp.segment_version_id , spot_id = in_app_send_tmp.spot_id , task_id = in_app_send_tmp.task_id , task_version_id = in_app_send_tmp.task_version_id
        WHEN NOT MATCHED THEN INSERT ( 
        channel_nm,channel_user_id,context_type_nm,context_val,creative_id,creative_version_id,event_designed_id,event_id,event_nm,identity_id,in_app_send_dttm,in_app_send_dttm_tz,load_dttm,message_id,message_version_id,mobile_app_id,occurrence_id,properties_map_doc,reserved_1_txt,reserved_2_txt,response_tracking_cd,segment_id,segment_version_id,spot_id,task_id,task_version_id
         ) values ( 
        in_app_send_tmp.channel_nm,in_app_send_tmp.channel_user_id,in_app_send_tmp.context_type_nm,in_app_send_tmp.context_val,in_app_send_tmp.creative_id,in_app_send_tmp.creative_version_id,in_app_send_tmp.event_designed_id,in_app_send_tmp.event_id,in_app_send_tmp.event_nm,in_app_send_tmp.identity_id,in_app_send_tmp.in_app_send_dttm,in_app_send_tmp.in_app_send_dttm_tz,in_app_send_tmp.load_dttm,in_app_send_tmp.message_id,in_app_send_tmp.message_version_id,in_app_send_tmp.mobile_app_id,in_app_send_tmp.occurrence_id,in_app_send_tmp.properties_map_doc,in_app_send_tmp.reserved_1_txt,in_app_send_tmp.reserved_2_txt,in_app_send_tmp.response_tracking_cd,in_app_send_tmp.segment_id,in_app_send_tmp.segment_version_id,in_app_send_tmp.spot_id,in_app_send_tmp.task_id,in_app_send_tmp.task_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :in_app_send_tmp                 , in_app_send, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..in_app_send_tmp                 ;
    QUIT;
    %put ######## Staging table: in_app_send_tmp                  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..in_app_send;
      DROP TABLE work.in_app_send;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table in_app_send;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..in_app_targeting_request) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..in_app_targeting_request_tmp    ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..in_app_targeting_request_tmp    ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=in_app_targeting_request, table_keys=%str(event_id), out_table=work.in_app_targeting_request);
 data &tmplib..in_app_targeting_request_tmp    ;
     set work.in_app_targeting_request;
  if in_app_tgt_request_dttm ne . then in_app_tgt_request_dttm = tzoneu2s(in_app_tgt_request_dttm,&timeZone_Value.);if in_app_tgt_request_dttm_tz ne . then in_app_tgt_request_dttm_tz = tzoneu2s(in_app_tgt_request_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :in_app_targeting_request_tmp    , in_app_targeting_request);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..in_app_targeting_request using &tmpdbschema..in_app_targeting_request_tmp    
         ON (in_app_targeting_request.event_id=in_app_targeting_request_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET channel_nm = in_app_targeting_request_tmp.channel_nm , channel_user_id = in_app_targeting_request_tmp.channel_user_id , context_type_nm = in_app_targeting_request_tmp.context_type_nm , context_val = in_app_targeting_request_tmp.context_val , eligibility_flg = in_app_targeting_request_tmp.eligibility_flg , event_designed_id = in_app_targeting_request_tmp.event_designed_id , event_nm = in_app_targeting_request_tmp.event_nm , identity_id = in_app_targeting_request_tmp.identity_id , in_app_tgt_request_dttm = in_app_targeting_request_tmp.in_app_tgt_request_dttm , in_app_tgt_request_dttm_tz = in_app_targeting_request_tmp.in_app_tgt_request_dttm_tz , load_dttm = in_app_targeting_request_tmp.load_dttm , mobile_app_id = in_app_targeting_request_tmp.mobile_app_id
        WHEN NOT MATCHED THEN INSERT ( 
        channel_nm,channel_user_id,context_type_nm,context_val,eligibility_flg,event_designed_id,event_id,event_nm,identity_id,in_app_tgt_request_dttm,in_app_tgt_request_dttm_tz,load_dttm,mobile_app_id
         ) values ( 
        in_app_targeting_request_tmp.channel_nm,in_app_targeting_request_tmp.channel_user_id,in_app_targeting_request_tmp.context_type_nm,in_app_targeting_request_tmp.context_val,in_app_targeting_request_tmp.eligibility_flg,in_app_targeting_request_tmp.event_designed_id,in_app_targeting_request_tmp.event_id,in_app_targeting_request_tmp.event_nm,in_app_targeting_request_tmp.identity_id,in_app_targeting_request_tmp.in_app_tgt_request_dttm,in_app_targeting_request_tmp.in_app_tgt_request_dttm_tz,in_app_targeting_request_tmp.load_dttm,in_app_targeting_request_tmp.mobile_app_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :in_app_targeting_request_tmp    , in_app_targeting_request, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..in_app_targeting_request_tmp    ;
    QUIT;
    %put ######## Staging table: in_app_targeting_request_tmp     Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..in_app_targeting_request;
      DROP TABLE work.in_app_targeting_request;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table in_app_targeting_request;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..invoice_details) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..invoice_details_tmp             ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..invoice_details_tmp             ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=invoice_details, table_keys=%str(cmtmnt_id,invoice_id,planning_id), out_table=work.invoice_details);
 data &tmplib..invoice_details_tmp             ;
     set work.invoice_details;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if invoice_created_dttm ne . then invoice_created_dttm = tzoneu2s(invoice_created_dttm,&timeZone_Value.);if invoice_reconciled_dttm ne . then invoice_reconciled_dttm = tzoneu2s(invoice_reconciled_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if payment_dttm ne . then payment_dttm = tzoneu2s(payment_dttm,&timeZone_Value.) ;
  if cmtmnt_id='' then cmtmnt_id='-'; if invoice_id='' then invoice_id='-'; if planning_id='' then planning_id='-';
 run;
 %ErrCheck (Failed to Append Data to :invoice_details_tmp             , invoice_details);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..invoice_details using &tmpdbschema..invoice_details_tmp             
         ON (invoice_details.cmtmnt_id=invoice_details_tmp.cmtmnt_id and invoice_details.invoice_id=invoice_details_tmp.invoice_id and invoice_details.planning_id=invoice_details_tmp.planning_id)
        WHEN MATCHED THEN  
        UPDATE SET cmtmnt_nm = invoice_details_tmp.cmtmnt_nm , created_by_usernm = invoice_details_tmp.created_by_usernm , created_dttm = invoice_details_tmp.created_dttm , invoice_amt = invoice_details_tmp.invoice_amt , invoice_created_dttm = invoice_details_tmp.invoice_created_dttm , invoice_desc = invoice_details_tmp.invoice_desc , invoice_nm = invoice_details_tmp.invoice_nm , invoice_number = invoice_details_tmp.invoice_number , invoice_reconciled_dttm = invoice_details_tmp.invoice_reconciled_dttm , invoice_status = invoice_details_tmp.invoice_status , last_modified_dttm = invoice_details_tmp.last_modified_dttm , last_modified_usernm = invoice_details_tmp.last_modified_usernm , load_dttm = invoice_details_tmp.load_dttm , payment_dttm = invoice_details_tmp.payment_dttm , plan_currency_cd = invoice_details_tmp.plan_currency_cd , planning_nm = invoice_details_tmp.planning_nm , reconcile_amt = invoice_details_tmp.reconcile_amt , reconcile_note = invoice_details_tmp.reconcile_note , vendor_amt = invoice_details_tmp.vendor_amt , vendor_currency_cd = invoice_details_tmp.vendor_currency_cd , vendor_desc = invoice_details_tmp.vendor_desc , vendor_id = invoice_details_tmp.vendor_id , vendor_nm = invoice_details_tmp.vendor_nm , vendor_number = invoice_details_tmp.vendor_number , vendor_obsolete_flg = invoice_details_tmp.vendor_obsolete_flg
        WHEN NOT MATCHED THEN INSERT ( 
        cmtmnt_id,cmtmnt_nm,created_by_usernm,created_dttm,invoice_amt,invoice_created_dttm,invoice_desc,invoice_id,invoice_nm,invoice_number,invoice_reconciled_dttm,invoice_status,last_modified_dttm,last_modified_usernm,load_dttm,payment_dttm,plan_currency_cd,planning_id,planning_nm,reconcile_amt,reconcile_note,vendor_amt,vendor_currency_cd,vendor_desc,vendor_id,vendor_nm,vendor_number,vendor_obsolete_flg
         ) values ( 
        invoice_details_tmp.cmtmnt_id,invoice_details_tmp.cmtmnt_nm,invoice_details_tmp.created_by_usernm,invoice_details_tmp.created_dttm,invoice_details_tmp.invoice_amt,invoice_details_tmp.invoice_created_dttm,invoice_details_tmp.invoice_desc,invoice_details_tmp.invoice_id,invoice_details_tmp.invoice_nm,invoice_details_tmp.invoice_number,invoice_details_tmp.invoice_reconciled_dttm,invoice_details_tmp.invoice_status,invoice_details_tmp.last_modified_dttm,invoice_details_tmp.last_modified_usernm,invoice_details_tmp.load_dttm,invoice_details_tmp.payment_dttm,invoice_details_tmp.plan_currency_cd,invoice_details_tmp.planning_id,invoice_details_tmp.planning_nm,invoice_details_tmp.reconcile_amt,invoice_details_tmp.reconcile_note,invoice_details_tmp.vendor_amt,invoice_details_tmp.vendor_currency_cd,invoice_details_tmp.vendor_desc,invoice_details_tmp.vendor_id,invoice_details_tmp.vendor_nm,invoice_details_tmp.vendor_number,invoice_details_tmp.vendor_obsolete_flg
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :invoice_details_tmp             , invoice_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..invoice_details_tmp             ;
    QUIT;
    %put ######## Staging table: invoice_details_tmp              Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..invoice_details;
      DROP TABLE work.invoice_details;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table invoice_details;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..invoice_line_items) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..invoice_line_items_tmp          ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..invoice_line_items_tmp          ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=invoice_line_items, table_keys=%str(cmtmnt_id,invoice_id,invoice_nm,invoice_number,planning_id), out_table=work.invoice_line_items);
 data &tmplib..invoice_line_items_tmp          ;
     set work.invoice_line_items;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if invoice_created_dttm ne . then invoice_created_dttm = tzoneu2s(invoice_created_dttm,&timeZone_Value.);if invoice_reconciled_dttm ne . then invoice_reconciled_dttm = tzoneu2s(invoice_reconciled_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if payment_dttm ne . then payment_dttm = tzoneu2s(payment_dttm,&timeZone_Value.) ;
  if cmtmnt_id='' then cmtmnt_id='-'; if invoice_id='' then invoice_id='-'; if invoice_nm='' then invoice_nm='-'; if invoice_number='' then invoice_number='-'; if planning_id='' then planning_id='-';
 run;
 %ErrCheck (Failed to Append Data to :invoice_line_items_tmp          , invoice_line_items);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..invoice_line_items using &tmpdbschema..invoice_line_items_tmp          
         ON (invoice_line_items.cmtmnt_id=invoice_line_items_tmp.cmtmnt_id and invoice_line_items.invoice_id=invoice_line_items_tmp.invoice_id and invoice_line_items.invoice_nm=invoice_line_items_tmp.invoice_nm and invoice_line_items.invoice_number=invoice_line_items_tmp.invoice_number and invoice_line_items.planning_id=invoice_line_items_tmp.planning_id)
        WHEN MATCHED THEN  
        UPDATE SET cc_allocated_amt = invoice_line_items_tmp.cc_allocated_amt , cc_available_amt = invoice_line_items_tmp.cc_available_amt , cc_desc = invoice_line_items_tmp.cc_desc , cc_nm = invoice_line_items_tmp.cc_nm , cc_owner_usernm = invoice_line_items_tmp.cc_owner_usernm , cc_recon_alloc_amt = invoice_line_items_tmp.cc_recon_alloc_amt , ccat_nm = invoice_line_items_tmp.ccat_nm , cmtmnt_nm = invoice_line_items_tmp.cmtmnt_nm , cost_center_id = invoice_line_items_tmp.cost_center_id , created_by_usernm = invoice_line_items_tmp.created_by_usernm , created_dttm = invoice_line_items_tmp.created_dttm , fin_acc_ccat_nm = invoice_line_items_tmp.fin_acc_ccat_nm , fin_acc_nm = invoice_line_items_tmp.fin_acc_nm , gen_ledger_cd = invoice_line_items_tmp.gen_ledger_cd , invoice_amt = invoice_line_items_tmp.invoice_amt , invoice_created_dttm = invoice_line_items_tmp.invoice_created_dttm , invoice_desc = invoice_line_items_tmp.invoice_desc , invoice_reconciled_dttm = invoice_line_items_tmp.invoice_reconciled_dttm , invoice_status = invoice_line_items_tmp.invoice_status , item_alloc_amt = invoice_line_items_tmp.item_alloc_amt , item_alloc_unit = invoice_line_items_tmp.item_alloc_unit , item_nm = invoice_line_items_tmp.item_nm , item_number = invoice_line_items_tmp.item_number , item_qty = invoice_line_items_tmp.item_qty , item_rate = invoice_line_items_tmp.item_rate , item_vend_alloc_amt = invoice_line_items_tmp.item_vend_alloc_amt , item_vend_alloc_unit = invoice_line_items_tmp.item_vend_alloc_unit , last_modified_dttm = invoice_line_items_tmp.last_modified_dttm , last_modified_usernm = invoice_line_items_tmp.last_modified_usernm , load_dttm = invoice_line_items_tmp.load_dttm , payment_dttm = invoice_line_items_tmp.payment_dttm , plan_currency_cd = invoice_line_items_tmp.plan_currency_cd , planning_nm = invoice_line_items_tmp.planning_nm , reconcile_amt = invoice_line_items_tmp.reconcile_amt , reconcile_note = invoice_line_items_tmp.reconcile_note , vendor_amt = invoice_line_items_tmp.vendor_amt , vendor_currency_cd = invoice_line_items_tmp.vendor_currency_cd , vendor_desc = invoice_line_items_tmp.vendor_desc , vendor_id = invoice_line_items_tmp.vendor_id , vendor_nm = invoice_line_items_tmp.vendor_nm , vendor_number = invoice_line_items_tmp.vendor_number , vendor_obsolete_flg = invoice_line_items_tmp.vendor_obsolete_flg
        WHEN NOT MATCHED THEN INSERT ( 
        cc_allocated_amt,cc_available_amt,cc_desc,cc_nm,cc_owner_usernm,cc_recon_alloc_amt,ccat_nm,cmtmnt_id,cmtmnt_nm,cost_center_id,created_by_usernm,created_dttm,fin_acc_ccat_nm,fin_acc_nm,gen_ledger_cd,invoice_amt,invoice_created_dttm,invoice_desc,invoice_id,invoice_nm,invoice_number,invoice_reconciled_dttm,invoice_status,item_alloc_amt,item_alloc_unit,item_nm,item_number,item_qty,item_rate,item_vend_alloc_amt,item_vend_alloc_unit,last_modified_dttm,last_modified_usernm,load_dttm,payment_dttm,plan_currency_cd,planning_id,planning_nm,reconcile_amt,reconcile_note,vendor_amt,vendor_currency_cd,vendor_desc,vendor_id,vendor_nm,vendor_number,vendor_obsolete_flg
         ) values ( 
        invoice_line_items_tmp.cc_allocated_amt,invoice_line_items_tmp.cc_available_amt,invoice_line_items_tmp.cc_desc,invoice_line_items_tmp.cc_nm,invoice_line_items_tmp.cc_owner_usernm,invoice_line_items_tmp.cc_recon_alloc_amt,invoice_line_items_tmp.ccat_nm,invoice_line_items_tmp.cmtmnt_id,invoice_line_items_tmp.cmtmnt_nm,invoice_line_items_tmp.cost_center_id,invoice_line_items_tmp.created_by_usernm,invoice_line_items_tmp.created_dttm,invoice_line_items_tmp.fin_acc_ccat_nm,invoice_line_items_tmp.fin_acc_nm,invoice_line_items_tmp.gen_ledger_cd,invoice_line_items_tmp.invoice_amt,invoice_line_items_tmp.invoice_created_dttm,invoice_line_items_tmp.invoice_desc,invoice_line_items_tmp.invoice_id,invoice_line_items_tmp.invoice_nm,invoice_line_items_tmp.invoice_number,invoice_line_items_tmp.invoice_reconciled_dttm,invoice_line_items_tmp.invoice_status,invoice_line_items_tmp.item_alloc_amt,invoice_line_items_tmp.item_alloc_unit,invoice_line_items_tmp.item_nm,invoice_line_items_tmp.item_number,invoice_line_items_tmp.item_qty,invoice_line_items_tmp.item_rate,invoice_line_items_tmp.item_vend_alloc_amt,invoice_line_items_tmp.item_vend_alloc_unit,invoice_line_items_tmp.last_modified_dttm,invoice_line_items_tmp.last_modified_usernm,invoice_line_items_tmp.load_dttm,invoice_line_items_tmp.payment_dttm,invoice_line_items_tmp.plan_currency_cd,invoice_line_items_tmp.planning_id,invoice_line_items_tmp.planning_nm,invoice_line_items_tmp.reconcile_amt,invoice_line_items_tmp.reconcile_note,invoice_line_items_tmp.vendor_amt,invoice_line_items_tmp.vendor_currency_cd,invoice_line_items_tmp.vendor_desc,invoice_line_items_tmp.vendor_id,invoice_line_items_tmp.vendor_nm,invoice_line_items_tmp.vendor_number,invoice_line_items_tmp.vendor_obsolete_flg
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :invoice_line_items_tmp          , invoice_line_items, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..invoice_line_items_tmp          ;
    QUIT;
    %put ######## Staging table: invoice_line_items_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..invoice_line_items;
      DROP TABLE work.invoice_line_items;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table invoice_line_items;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..invoice_line_items_ccbdgt) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..invoice_line_items_ccbdgt_tmp   ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..invoice_line_items_ccbdgt_tmp   ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=invoice_line_items_ccbdgt, table_keys=%str(invoice_id,item_number), out_table=work.invoice_line_items_ccbdgt);
 data &tmplib..invoice_line_items_ccbdgt_tmp   ;
     set work.invoice_line_items_ccbdgt;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if invoice_created_dttm ne . then invoice_created_dttm = tzoneu2s(invoice_created_dttm,&timeZone_Value.);if invoice_reconciled_dttm ne . then invoice_reconciled_dttm = tzoneu2s(invoice_reconciled_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if payment_dttm ne . then payment_dttm = tzoneu2s(payment_dttm,&timeZone_Value.) ;
  if invoice_id='' then invoice_id='-';
 run;
 %ErrCheck (Failed to Append Data to :invoice_line_items_ccbdgt_tmp   , invoice_line_items_ccbdgt);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..invoice_line_items_ccbdgt using &tmpdbschema..invoice_line_items_ccbdgt_tmp   
         ON (invoice_line_items_ccbdgt.invoice_id=invoice_line_items_ccbdgt_tmp.invoice_id and invoice_line_items_ccbdgt.item_number=invoice_line_items_ccbdgt_tmp.item_number)
        WHEN MATCHED THEN  
        UPDATE SET cc_allocated_amt = invoice_line_items_ccbdgt_tmp.cc_allocated_amt , cc_available_amt = invoice_line_items_ccbdgt_tmp.cc_available_amt , cc_bdgt_amt = invoice_line_items_ccbdgt_tmp.cc_bdgt_amt , cc_bdgt_budget_amt = invoice_line_items_ccbdgt_tmp.cc_bdgt_budget_amt , cc_bdgt_budget_desc = invoice_line_items_ccbdgt_tmp.cc_bdgt_budget_desc , cc_bdgt_cmtmnt_invoice_amt = invoice_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_invoice_amt , cc_bdgt_cmtmnt_invoice_cnt = invoice_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_invoice_cnt , cc_bdgt_cmtmnt_outstanding_amt = invoice_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_outstanding_amt , cc_bdgt_cmtmnt_overspent_amt = invoice_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_overspent_amt , cc_bdgt_committed_amt = invoice_line_items_ccbdgt_tmp.cc_bdgt_committed_amt , cc_bdgt_direct_invoice_amt = invoice_line_items_ccbdgt_tmp.cc_bdgt_direct_invoice_amt , cc_bdgt_invoiced_amt = invoice_line_items_ccbdgt_tmp.cc_bdgt_invoiced_amt , cc_desc = invoice_line_items_ccbdgt_tmp.cc_desc , cc_nm = invoice_line_items_ccbdgt_tmp.cc_nm , cc_number = invoice_line_items_ccbdgt_tmp.cc_number , cc_obsolete_flg = invoice_line_items_ccbdgt_tmp.cc_obsolete_flg , cc_owner_usernm = invoice_line_items_ccbdgt_tmp.cc_owner_usernm , cc_recon_alloc_amt = invoice_line_items_ccbdgt_tmp.cc_recon_alloc_amt , ccat_nm = invoice_line_items_ccbdgt_tmp.ccat_nm , cmtmnt_id = invoice_line_items_ccbdgt_tmp.cmtmnt_id , cmtmnt_nm = invoice_line_items_ccbdgt_tmp.cmtmnt_nm , cost_center_id = invoice_line_items_ccbdgt_tmp.cost_center_id , created_by_usernm = invoice_line_items_ccbdgt_tmp.created_by_usernm , created_dttm = invoice_line_items_ccbdgt_tmp.created_dttm , fin_acc_ccat_nm = invoice_line_items_ccbdgt_tmp.fin_acc_ccat_nm , fin_acc_nm = invoice_line_items_ccbdgt_tmp.fin_acc_nm , fp_cls_ver = invoice_line_items_ccbdgt_tmp.fp_cls_ver , fp_desc = invoice_line_items_ccbdgt_tmp.fp_desc , fp_end_dt = invoice_line_items_ccbdgt_tmp.fp_end_dt , fp_id = invoice_line_items_ccbdgt_tmp.fp_id , fp_nm = invoice_line_items_ccbdgt_tmp.fp_nm , fp_obsolete_flg = invoice_line_items_ccbdgt_tmp.fp_obsolete_flg , fp_start_dt = invoice_line_items_ccbdgt_tmp.fp_start_dt , gen_ledger_cd = invoice_line_items_ccbdgt_tmp.gen_ledger_cd , invoice_amt = invoice_line_items_ccbdgt_tmp.invoice_amt , invoice_created_dttm = invoice_line_items_ccbdgt_tmp.invoice_created_dttm , invoice_desc = invoice_line_items_ccbdgt_tmp.invoice_desc , invoice_nm = invoice_line_items_ccbdgt_tmp.invoice_nm , invoice_number = invoice_line_items_ccbdgt_tmp.invoice_number , invoice_reconciled_dttm = invoice_line_items_ccbdgt_tmp.invoice_reconciled_dttm , invoice_status = invoice_line_items_ccbdgt_tmp.invoice_status , item_alloc_amt = invoice_line_items_ccbdgt_tmp.item_alloc_amt , item_alloc_unit = invoice_line_items_ccbdgt_tmp.item_alloc_unit , item_nm = invoice_line_items_ccbdgt_tmp.item_nm , item_qty = invoice_line_items_ccbdgt_tmp.item_qty , item_rate = invoice_line_items_ccbdgt_tmp.item_rate , item_vend_alloc_amt = invoice_line_items_ccbdgt_tmp.item_vend_alloc_amt , item_vend_alloc_unit = invoice_line_items_ccbdgt_tmp.item_vend_alloc_unit , last_modified_dttm = invoice_line_items_ccbdgt_tmp.last_modified_dttm , last_modified_usernm = invoice_line_items_ccbdgt_tmp.last_modified_usernm , load_dttm = invoice_line_items_ccbdgt_tmp.load_dttm , payment_dttm = invoice_line_items_ccbdgt_tmp.payment_dttm , plan_currency_cd = invoice_line_items_ccbdgt_tmp.plan_currency_cd , planning_id = invoice_line_items_ccbdgt_tmp.planning_id , planning_nm = invoice_line_items_ccbdgt_tmp.planning_nm , reconcile_amt = invoice_line_items_ccbdgt_tmp.reconcile_amt , reconcile_note = invoice_line_items_ccbdgt_tmp.reconcile_note , vendor_amt = invoice_line_items_ccbdgt_tmp.vendor_amt , vendor_currency_cd = invoice_line_items_ccbdgt_tmp.vendor_currency_cd , vendor_desc = invoice_line_items_ccbdgt_tmp.vendor_desc , vendor_id = invoice_line_items_ccbdgt_tmp.vendor_id , vendor_nm = invoice_line_items_ccbdgt_tmp.vendor_nm , vendor_number = invoice_line_items_ccbdgt_tmp.vendor_number , vendor_obsolete_flg = invoice_line_items_ccbdgt_tmp.vendor_obsolete_flg
        WHEN NOT MATCHED THEN INSERT ( 
        cc_allocated_amt,cc_available_amt,cc_bdgt_amt,cc_bdgt_budget_amt,cc_bdgt_budget_desc,cc_bdgt_cmtmnt_invoice_amt,cc_bdgt_cmtmnt_invoice_cnt,cc_bdgt_cmtmnt_outstanding_amt,cc_bdgt_cmtmnt_overspent_amt,cc_bdgt_committed_amt,cc_bdgt_direct_invoice_amt,cc_bdgt_invoiced_amt,cc_desc,cc_nm,cc_number,cc_obsolete_flg,cc_owner_usernm,cc_recon_alloc_amt,ccat_nm,cmtmnt_id,cmtmnt_nm,cost_center_id,created_by_usernm,created_dttm,fin_acc_ccat_nm,fin_acc_nm,fp_cls_ver,fp_desc,fp_end_dt,fp_id,fp_nm,fp_obsolete_flg,fp_start_dt,gen_ledger_cd,invoice_amt,invoice_created_dttm,invoice_desc,invoice_id,invoice_nm,invoice_number,invoice_reconciled_dttm,invoice_status,item_alloc_amt,item_alloc_unit,item_nm,item_number,item_qty,item_rate,item_vend_alloc_amt,item_vend_alloc_unit,last_modified_dttm,last_modified_usernm,load_dttm,payment_dttm,plan_currency_cd,planning_id,planning_nm,reconcile_amt,reconcile_note,vendor_amt,vendor_currency_cd,vendor_desc,vendor_id,vendor_nm,vendor_number,vendor_obsolete_flg
         ) values ( 
        invoice_line_items_ccbdgt_tmp.cc_allocated_amt,invoice_line_items_ccbdgt_tmp.cc_available_amt,invoice_line_items_ccbdgt_tmp.cc_bdgt_amt,invoice_line_items_ccbdgt_tmp.cc_bdgt_budget_amt,invoice_line_items_ccbdgt_tmp.cc_bdgt_budget_desc,invoice_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_invoice_amt,invoice_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_invoice_cnt,invoice_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_outstanding_amt,invoice_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_overspent_amt,invoice_line_items_ccbdgt_tmp.cc_bdgt_committed_amt,invoice_line_items_ccbdgt_tmp.cc_bdgt_direct_invoice_amt,invoice_line_items_ccbdgt_tmp.cc_bdgt_invoiced_amt,invoice_line_items_ccbdgt_tmp.cc_desc,invoice_line_items_ccbdgt_tmp.cc_nm,invoice_line_items_ccbdgt_tmp.cc_number,invoice_line_items_ccbdgt_tmp.cc_obsolete_flg,invoice_line_items_ccbdgt_tmp.cc_owner_usernm,invoice_line_items_ccbdgt_tmp.cc_recon_alloc_amt,invoice_line_items_ccbdgt_tmp.ccat_nm,invoice_line_items_ccbdgt_tmp.cmtmnt_id,invoice_line_items_ccbdgt_tmp.cmtmnt_nm,invoice_line_items_ccbdgt_tmp.cost_center_id,invoice_line_items_ccbdgt_tmp.created_by_usernm,invoice_line_items_ccbdgt_tmp.created_dttm,invoice_line_items_ccbdgt_tmp.fin_acc_ccat_nm,invoice_line_items_ccbdgt_tmp.fin_acc_nm,invoice_line_items_ccbdgt_tmp.fp_cls_ver,invoice_line_items_ccbdgt_tmp.fp_desc,invoice_line_items_ccbdgt_tmp.fp_end_dt,invoice_line_items_ccbdgt_tmp.fp_id,invoice_line_items_ccbdgt_tmp.fp_nm,invoice_line_items_ccbdgt_tmp.fp_obsolete_flg,invoice_line_items_ccbdgt_tmp.fp_start_dt,invoice_line_items_ccbdgt_tmp.gen_ledger_cd,invoice_line_items_ccbdgt_tmp.invoice_amt,invoice_line_items_ccbdgt_tmp.invoice_created_dttm,invoice_line_items_ccbdgt_tmp.invoice_desc,invoice_line_items_ccbdgt_tmp.invoice_id,invoice_line_items_ccbdgt_tmp.invoice_nm,invoice_line_items_ccbdgt_tmp.invoice_number,invoice_line_items_ccbdgt_tmp.invoice_reconciled_dttm,invoice_line_items_ccbdgt_tmp.invoice_status,invoice_line_items_ccbdgt_tmp.item_alloc_amt,invoice_line_items_ccbdgt_tmp.item_alloc_unit,invoice_line_items_ccbdgt_tmp.item_nm,invoice_line_items_ccbdgt_tmp.item_number,invoice_line_items_ccbdgt_tmp.item_qty,invoice_line_items_ccbdgt_tmp.item_rate,invoice_line_items_ccbdgt_tmp.item_vend_alloc_amt,invoice_line_items_ccbdgt_tmp.item_vend_alloc_unit,invoice_line_items_ccbdgt_tmp.last_modified_dttm,invoice_line_items_ccbdgt_tmp.last_modified_usernm,invoice_line_items_ccbdgt_tmp.load_dttm,invoice_line_items_ccbdgt_tmp.payment_dttm,invoice_line_items_ccbdgt_tmp.plan_currency_cd,invoice_line_items_ccbdgt_tmp.planning_id,invoice_line_items_ccbdgt_tmp.planning_nm,invoice_line_items_ccbdgt_tmp.reconcile_amt,invoice_line_items_ccbdgt_tmp.reconcile_note,invoice_line_items_ccbdgt_tmp.vendor_amt,invoice_line_items_ccbdgt_tmp.vendor_currency_cd,invoice_line_items_ccbdgt_tmp.vendor_desc,invoice_line_items_ccbdgt_tmp.vendor_id,invoice_line_items_ccbdgt_tmp.vendor_nm,invoice_line_items_ccbdgt_tmp.vendor_number,invoice_line_items_ccbdgt_tmp.vendor_obsolete_flg
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :invoice_line_items_ccbdgt_tmp   , invoice_line_items_ccbdgt, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..invoice_line_items_ccbdgt_tmp   ;
    QUIT;
    %put ######## Staging table: invoice_line_items_ccbdgt_tmp    Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..invoice_line_items_ccbdgt;
      DROP TABLE work.invoice_line_items_ccbdgt;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table invoice_line_items_ccbdgt;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..journey_entry) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..journey_entry_tmp               ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..journey_entry_tmp               ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=journey_entry, table_keys=%str(event_id), out_table=work.journey_entry);
 data &tmplib..journey_entry_tmp               ;
     set work.journey_entry;
  if entry_dttm ne . then entry_dttm = tzoneu2s(entry_dttm,&timeZone_Value.);if entry_dttm_tz ne . then entry_dttm_tz = tzoneu2s(entry_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :journey_entry_tmp               , journey_entry);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..journey_entry using &tmpdbschema..journey_entry_tmp               
         ON (journey_entry.event_id=journey_entry_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET aud_occurrence_id = journey_entry_tmp.aud_occurrence_id , audience_id = journey_entry_tmp.audience_id , context_type_nm = journey_entry_tmp.context_type_nm , context_val = journey_entry_tmp.context_val , entry_dttm = journey_entry_tmp.entry_dttm , entry_dttm_tz = journey_entry_tmp.entry_dttm_tz , event_nm = journey_entry_tmp.event_nm , identity_id = journey_entry_tmp.identity_id , identity_type_nm = journey_entry_tmp.identity_type_nm , identity_type_val = journey_entry_tmp.identity_type_val , journey_id = journey_entry_tmp.journey_id , journey_occurrence_id = journey_entry_tmp.journey_occurrence_id , load_dttm = journey_entry_tmp.load_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        aud_occurrence_id,audience_id,context_type_nm,context_val,entry_dttm,entry_dttm_tz,event_id,event_nm,identity_id,identity_type_nm,identity_type_val,journey_id,journey_occurrence_id,load_dttm
         ) values ( 
        journey_entry_tmp.aud_occurrence_id,journey_entry_tmp.audience_id,journey_entry_tmp.context_type_nm,journey_entry_tmp.context_val,journey_entry_tmp.entry_dttm,journey_entry_tmp.entry_dttm_tz,journey_entry_tmp.event_id,journey_entry_tmp.event_nm,journey_entry_tmp.identity_id,journey_entry_tmp.identity_type_nm,journey_entry_tmp.identity_type_val,journey_entry_tmp.journey_id,journey_entry_tmp.journey_occurrence_id,journey_entry_tmp.load_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :journey_entry_tmp               , journey_entry, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..journey_entry_tmp               ;
    QUIT;
    %put ######## Staging table: journey_entry_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..journey_entry;
      DROP TABLE work.journey_entry;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table journey_entry;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..journey_exit) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..journey_exit_tmp                ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..journey_exit_tmp                ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=journey_exit, table_keys=%str(event_id), out_table=work.journey_exit);
 data &tmplib..journey_exit_tmp                ;
     set work.journey_exit;
  if exit_dttm ne . then exit_dttm = tzoneu2s(exit_dttm,&timeZone_Value.);if exit_dttm_tz ne . then exit_dttm_tz = tzoneu2s(exit_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :journey_exit_tmp                , journey_exit);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..journey_exit using &tmpdbschema..journey_exit_tmp                
         ON (journey_exit.event_id=journey_exit_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET aud_occurrence_id = journey_exit_tmp.aud_occurrence_id , audience_id = journey_exit_tmp.audience_id , context_type_nm = journey_exit_tmp.context_type_nm , context_val = journey_exit_tmp.context_val , event_nm = journey_exit_tmp.event_nm , exit_dttm = journey_exit_tmp.exit_dttm , exit_dttm_tz = journey_exit_tmp.exit_dttm_tz , identity_id = journey_exit_tmp.identity_id , identity_type_nm = journey_exit_tmp.identity_type_nm , identity_type_val = journey_exit_tmp.identity_type_val , journey_id = journey_exit_tmp.journey_id , journey_occurrence_id = journey_exit_tmp.journey_occurrence_id , last_node_id = journey_exit_tmp.last_node_id , load_dttm = journey_exit_tmp.load_dttm , reason_cd = journey_exit_tmp.reason_cd , reason_txt = journey_exit_tmp.reason_txt
        WHEN NOT MATCHED THEN INSERT ( 
        aud_occurrence_id,audience_id,context_type_nm,context_val,event_id,event_nm,exit_dttm,exit_dttm_tz,identity_id,identity_type_nm,identity_type_val,journey_id,journey_occurrence_id,last_node_id,load_dttm,reason_cd,reason_txt
         ) values ( 
        journey_exit_tmp.aud_occurrence_id,journey_exit_tmp.audience_id,journey_exit_tmp.context_type_nm,journey_exit_tmp.context_val,journey_exit_tmp.event_id,journey_exit_tmp.event_nm,journey_exit_tmp.exit_dttm,journey_exit_tmp.exit_dttm_tz,journey_exit_tmp.identity_id,journey_exit_tmp.identity_type_nm,journey_exit_tmp.identity_type_val,journey_exit_tmp.journey_id,journey_exit_tmp.journey_occurrence_id,journey_exit_tmp.last_node_id,journey_exit_tmp.load_dttm,journey_exit_tmp.reason_cd,journey_exit_tmp.reason_txt
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :journey_exit_tmp                , journey_exit, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..journey_exit_tmp                ;
    QUIT;
    %put ######## Staging table: journey_exit_tmp                 Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..journey_exit;
      DROP TABLE work.journey_exit;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table journey_exit;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..journey_holdout) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..journey_holdout_tmp             ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..journey_holdout_tmp             ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=journey_holdout, table_keys=%str(event_id), out_table=work.journey_holdout);
 data &tmplib..journey_holdout_tmp             ;
     set work.journey_holdout;
  if holdout_dttm ne . then holdout_dttm = tzoneu2s(holdout_dttm,&timeZone_Value.);if holdout_dttm_tz ne . then holdout_dttm_tz = tzoneu2s(holdout_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :journey_holdout_tmp             , journey_holdout);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..journey_holdout using &tmpdbschema..journey_holdout_tmp             
         ON (journey_holdout.event_id=journey_holdout_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET aud_occurrence_id = journey_holdout_tmp.aud_occurrence_id , audience_id = journey_holdout_tmp.audience_id , context_type_nm = journey_holdout_tmp.context_type_nm , context_val = journey_holdout_tmp.context_val , event_nm = journey_holdout_tmp.event_nm , holdout_dttm = journey_holdout_tmp.holdout_dttm , holdout_dttm_tz = journey_holdout_tmp.holdout_dttm_tz , identity_id = journey_holdout_tmp.identity_id , identity_type_nm = journey_holdout_tmp.identity_type_nm , identity_type_val = journey_holdout_tmp.identity_type_val , journey_id = journey_holdout_tmp.journey_id , journey_occurrence_id = journey_holdout_tmp.journey_occurrence_id , load_dttm = journey_holdout_tmp.load_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        aud_occurrence_id,audience_id,context_type_nm,context_val,event_id,event_nm,holdout_dttm,holdout_dttm_tz,identity_id,identity_type_nm,identity_type_val,journey_id,journey_occurrence_id,load_dttm
         ) values ( 
        journey_holdout_tmp.aud_occurrence_id,journey_holdout_tmp.audience_id,journey_holdout_tmp.context_type_nm,journey_holdout_tmp.context_val,journey_holdout_tmp.event_id,journey_holdout_tmp.event_nm,journey_holdout_tmp.holdout_dttm,journey_holdout_tmp.holdout_dttm_tz,journey_holdout_tmp.identity_id,journey_holdout_tmp.identity_type_nm,journey_holdout_tmp.identity_type_val,journey_holdout_tmp.journey_id,journey_holdout_tmp.journey_occurrence_id,journey_holdout_tmp.load_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :journey_holdout_tmp             , journey_holdout, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..journey_holdout_tmp             ;
    QUIT;
    %put ######## Staging table: journey_holdout_tmp              Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..journey_holdout;
      DROP TABLE work.journey_holdout;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table journey_holdout;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..journey_node_entry) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..journey_node_entry_tmp          ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..journey_node_entry_tmp          ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=journey_node_entry, table_keys=%str(event_id), out_table=work.journey_node_entry);
 data &tmplib..journey_node_entry_tmp          ;
     set work.journey_node_entry;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if node_entry_dttm ne . then node_entry_dttm = tzoneu2s(node_entry_dttm,&timeZone_Value.);if node_entry_dttm_tz ne . then node_entry_dttm_tz = tzoneu2s(node_entry_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :journey_node_entry_tmp          , journey_node_entry);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..journey_node_entry using &tmpdbschema..journey_node_entry_tmp          
         ON (journey_node_entry.event_id=journey_node_entry_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET aud_occurrence_id = journey_node_entry_tmp.aud_occurrence_id , audience_id = journey_node_entry_tmp.audience_id , context_type_nm = journey_node_entry_tmp.context_type_nm , context_val = journey_node_entry_tmp.context_val , event_nm = journey_node_entry_tmp.event_nm , identity_id = journey_node_entry_tmp.identity_id , identity_type_nm = journey_node_entry_tmp.identity_type_nm , identity_type_val = journey_node_entry_tmp.identity_type_val , journey_id = journey_node_entry_tmp.journey_id , journey_occurrence_id = journey_node_entry_tmp.journey_occurrence_id , load_dttm = journey_node_entry_tmp.load_dttm , node_entry_dttm = journey_node_entry_tmp.node_entry_dttm , node_entry_dttm_tz = journey_node_entry_tmp.node_entry_dttm_tz , node_id = journey_node_entry_tmp.node_id , node_type_nm = journey_node_entry_tmp.node_type_nm , previous_node_id = journey_node_entry_tmp.previous_node_id
        WHEN NOT MATCHED THEN INSERT ( 
        aud_occurrence_id,audience_id,context_type_nm,context_val,event_id,event_nm,identity_id,identity_type_nm,identity_type_val,journey_id,journey_occurrence_id,load_dttm,node_entry_dttm,node_entry_dttm_tz,node_id,node_type_nm,previous_node_id
         ) values ( 
        journey_node_entry_tmp.aud_occurrence_id,journey_node_entry_tmp.audience_id,journey_node_entry_tmp.context_type_nm,journey_node_entry_tmp.context_val,journey_node_entry_tmp.event_id,journey_node_entry_tmp.event_nm,journey_node_entry_tmp.identity_id,journey_node_entry_tmp.identity_type_nm,journey_node_entry_tmp.identity_type_val,journey_node_entry_tmp.journey_id,journey_node_entry_tmp.journey_occurrence_id,journey_node_entry_tmp.load_dttm,journey_node_entry_tmp.node_entry_dttm,journey_node_entry_tmp.node_entry_dttm_tz,journey_node_entry_tmp.node_id,journey_node_entry_tmp.node_type_nm,journey_node_entry_tmp.previous_node_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :journey_node_entry_tmp          , journey_node_entry, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..journey_node_entry_tmp          ;
    QUIT;
    %put ######## Staging table: journey_node_entry_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..journey_node_entry;
      DROP TABLE work.journey_node_entry;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table journey_node_entry;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..journey_success) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..journey_success_tmp             ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..journey_success_tmp             ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=journey_success, table_keys=%str(event_id), out_table=work.journey_success);
 data &tmplib..journey_success_tmp             ;
     set work.journey_success;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if success_dttm ne . then success_dttm = tzoneu2s(success_dttm,&timeZone_Value.);if success_dttm_tz ne . then success_dttm_tz = tzoneu2s(success_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :journey_success_tmp             , journey_success);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..journey_success using &tmpdbschema..journey_success_tmp             
         ON (journey_success.event_id=journey_success_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET aud_occurrence_id = journey_success_tmp.aud_occurrence_id , audience_id = journey_success_tmp.audience_id , context_type_nm = journey_success_tmp.context_type_nm , context_val = journey_success_tmp.context_val , event_nm = journey_success_tmp.event_nm , identity_id = journey_success_tmp.identity_id , identity_type_nm = journey_success_tmp.identity_type_nm , identity_type_val = journey_success_tmp.identity_type_val , journey_id = journey_success_tmp.journey_id , journey_occurrence_id = journey_success_tmp.journey_occurrence_id , load_dttm = journey_success_tmp.load_dttm , success_dttm = journey_success_tmp.success_dttm , success_dttm_tz = journey_success_tmp.success_dttm_tz , success_val = journey_success_tmp.success_val , unit_qty = journey_success_tmp.unit_qty
        WHEN NOT MATCHED THEN INSERT ( 
        aud_occurrence_id,audience_id,context_type_nm,context_val,event_id,event_nm,identity_id,identity_type_nm,identity_type_val,journey_id,journey_occurrence_id,load_dttm,success_dttm,success_dttm_tz,success_val,unit_qty
         ) values ( 
        journey_success_tmp.aud_occurrence_id,journey_success_tmp.audience_id,journey_success_tmp.context_type_nm,journey_success_tmp.context_val,journey_success_tmp.event_id,journey_success_tmp.event_nm,journey_success_tmp.identity_id,journey_success_tmp.identity_type_nm,journey_success_tmp.identity_type_val,journey_success_tmp.journey_id,journey_success_tmp.journey_occurrence_id,journey_success_tmp.load_dttm,journey_success_tmp.success_dttm,journey_success_tmp.success_dttm_tz,journey_success_tmp.success_val,journey_success_tmp.unit_qty
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :journey_success_tmp             , journey_success, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..journey_success_tmp             ;
    QUIT;
    %put ######## Staging table: journey_success_tmp              Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..journey_success;
      DROP TABLE work.journey_success;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table journey_success;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..journey_suppression) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..journey_suppression_tmp         ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..journey_suppression_tmp         ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=journey_suppression, table_keys=%str(event_id), out_table=work.journey_suppression);
 data &tmplib..journey_suppression_tmp         ;
     set work.journey_suppression;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if suppression_dttm ne . then suppression_dttm = tzoneu2s(suppression_dttm,&timeZone_Value.);if suppression_dttm_tz ne . then suppression_dttm_tz = tzoneu2s(suppression_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :journey_suppression_tmp         , journey_suppression);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..journey_suppression using &tmpdbschema..journey_suppression_tmp         
         ON (journey_suppression.event_id=journey_suppression_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET aud_occurrence_id = journey_suppression_tmp.aud_occurrence_id , audience_id = journey_suppression_tmp.audience_id , context_type_nm = journey_suppression_tmp.context_type_nm , context_val = journey_suppression_tmp.context_val , event_nm = journey_suppression_tmp.event_nm , identity_id = journey_suppression_tmp.identity_id , identity_type_nm = journey_suppression_tmp.identity_type_nm , identity_type_val = journey_suppression_tmp.identity_type_val , journey_id = journey_suppression_tmp.journey_id , journey_occurrence_id = journey_suppression_tmp.journey_occurrence_id , load_dttm = journey_suppression_tmp.load_dttm , reason_cd = journey_suppression_tmp.reason_cd , reason_txt = journey_suppression_tmp.reason_txt , suppression_dttm = journey_suppression_tmp.suppression_dttm , suppression_dttm_tz = journey_suppression_tmp.suppression_dttm_tz
        WHEN NOT MATCHED THEN INSERT ( 
        aud_occurrence_id,audience_id,context_type_nm,context_val,event_id,event_nm,identity_id,identity_type_nm,identity_type_val,journey_id,journey_occurrence_id,load_dttm,reason_cd,reason_txt,suppression_dttm,suppression_dttm_tz
         ) values ( 
        journey_suppression_tmp.aud_occurrence_id,journey_suppression_tmp.audience_id,journey_suppression_tmp.context_type_nm,journey_suppression_tmp.context_val,journey_suppression_tmp.event_id,journey_suppression_tmp.event_nm,journey_suppression_tmp.identity_id,journey_suppression_tmp.identity_type_nm,journey_suppression_tmp.identity_type_val,journey_suppression_tmp.journey_id,journey_suppression_tmp.journey_occurrence_id,journey_suppression_tmp.load_dttm,journey_suppression_tmp.reason_cd,journey_suppression_tmp.reason_txt,journey_suppression_tmp.suppression_dttm,journey_suppression_tmp.suppression_dttm_tz
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :journey_suppression_tmp         , journey_suppression, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..journey_suppression_tmp         ;
    QUIT;
    %put ######## Staging table: journey_suppression_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..journey_suppression;
      DROP TABLE work.journey_suppression;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table journey_suppression;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_activity) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_activity_tmp                 ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_activity_tmp                 ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_activity, table_keys=%str(activity_version_id), out_table=work.md_activity);
 data &tmplib..md_activity_tmp                 ;
     set work.md_activity;
  if last_published_dttm ne . then last_published_dttm = tzoneu2s(last_published_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if activity_version_id='' then activity_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_activity_tmp                 , md_activity);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_activity using &tmpdbschema..md_activity_tmp                 
         ON (md_activity.activity_version_id=md_activity_tmp.activity_version_id)
        WHEN MATCHED THEN  
        UPDATE SET activity_category_nm = md_activity_tmp.activity_category_nm , activity_cd = md_activity_tmp.activity_cd , activity_desc = md_activity_tmp.activity_desc , activity_id = md_activity_tmp.activity_id , activity_nm = md_activity_tmp.activity_nm , activity_status_cd = md_activity_tmp.activity_status_cd , business_context_id = md_activity_tmp.business_context_id , folder_path_nm = md_activity_tmp.folder_path_nm , last_published_dttm = md_activity_tmp.last_published_dttm , valid_from_dttm = md_activity_tmp.valid_from_dttm , valid_to_dttm = md_activity_tmp.valid_to_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        activity_category_nm,activity_cd,activity_desc,activity_id,activity_nm,activity_status_cd,activity_version_id,business_context_id,folder_path_nm,last_published_dttm,valid_from_dttm,valid_to_dttm
         ) values ( 
        md_activity_tmp.activity_category_nm,md_activity_tmp.activity_cd,md_activity_tmp.activity_desc,md_activity_tmp.activity_id,md_activity_tmp.activity_nm,md_activity_tmp.activity_status_cd,md_activity_tmp.activity_version_id,md_activity_tmp.business_context_id,md_activity_tmp.folder_path_nm,md_activity_tmp.last_published_dttm,md_activity_tmp.valid_from_dttm,md_activity_tmp.valid_to_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_activity_tmp                 , md_activity, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_activity_tmp                 ;
    QUIT;
    %put ######## Staging table: md_activity_tmp                  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_activity;
      DROP TABLE work.md_activity;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_activity;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_activity_abtestpath) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_activity_abtestpath_tmp      ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_activity_abtestpath_tmp      ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_activity_abtestpath, table_keys=%str(abtest_path_id,activity_node_id,activity_version_id), out_table=work.md_activity_abtestpath);
 data &tmplib..md_activity_abtestpath_tmp      ;
     set work.md_activity_abtestpath;
  if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if abtest_path_id='' then abtest_path_id='-'; if activity_node_id='' then activity_node_id='-'; if activity_version_id='' then activity_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_activity_abtestpath_tmp      , md_activity_abtestpath);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_activity_abtestpath using &tmpdbschema..md_activity_abtestpath_tmp      
         ON (md_activity_abtestpath.abtest_path_id=md_activity_abtestpath_tmp.abtest_path_id and md_activity_abtestpath.activity_node_id=md_activity_abtestpath_tmp.activity_node_id and md_activity_abtestpath.activity_version_id=md_activity_abtestpath_tmp.activity_version_id)
        WHEN MATCHED THEN  
        UPDATE SET abtest_dist_pct = md_activity_abtestpath_tmp.abtest_dist_pct , abtest_path_nm = md_activity_abtestpath_tmp.abtest_path_nm , activity_id = md_activity_abtestpath_tmp.activity_id , activity_status_cd = md_activity_abtestpath_tmp.activity_status_cd , control_flg = md_activity_abtestpath_tmp.control_flg , next_node_val = md_activity_abtestpath_tmp.next_node_val , valid_from_dttm = md_activity_abtestpath_tmp.valid_from_dttm , valid_to_dttm = md_activity_abtestpath_tmp.valid_to_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        abtest_dist_pct,abtest_path_id,abtest_path_nm,activity_id,activity_node_id,activity_status_cd,activity_version_id,control_flg,next_node_val,valid_from_dttm,valid_to_dttm
         ) values ( 
        md_activity_abtestpath_tmp.abtest_dist_pct,md_activity_abtestpath_tmp.abtest_path_id,md_activity_abtestpath_tmp.abtest_path_nm,md_activity_abtestpath_tmp.activity_id,md_activity_abtestpath_tmp.activity_node_id,md_activity_abtestpath_tmp.activity_status_cd,md_activity_abtestpath_tmp.activity_version_id,md_activity_abtestpath_tmp.control_flg,md_activity_abtestpath_tmp.next_node_val,md_activity_abtestpath_tmp.valid_from_dttm,md_activity_abtestpath_tmp.valid_to_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_activity_abtestpath_tmp      , md_activity_abtestpath, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_activity_abtestpath_tmp      ;
    QUIT;
    %put ######## Staging table: md_activity_abtestpath_tmp       Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_activity_abtestpath;
      DROP TABLE work.md_activity_abtestpath;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_activity_abtestpath;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_activity_custom_prop) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_activity_custom_prop_tmp     ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_activity_custom_prop_tmp     ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_activity_custom_prop, table_keys=%str(activity_version_id,property_datatype_cd,property_nm,property_val), out_table=work.md_activity_custom_prop);
 data &tmplib..md_activity_custom_prop_tmp     ;
     set work.md_activity_custom_prop;
  if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  Hashed_pk_col = put(sha256(catx('|',activity_version_id,property_datatype_cd,property_nm,property_val)), $hex64.);
  if activity_version_id='' then activity_version_id='-'; if property_datatype_cd='' then property_datatype_cd='-'; if property_nm='' then property_nm='-'; if property_val='' then property_val='-';
 run;
 %ErrCheck (Failed to Append Data to :md_activity_custom_prop_tmp     , md_activity_custom_prop);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_activity_custom_prop using &tmpdbschema..md_activity_custom_prop_tmp     
         ON (md_activity_custom_prop.Hashed_pk_col = md_activity_custom_prop_tmp.Hashed_pk_col)
        WHEN MATCHED THEN  
        UPDATE SET activity_id = md_activity_custom_prop_tmp.activity_id , activity_status_cd = md_activity_custom_prop_tmp.activity_status_cd , valid_from_dttm = md_activity_custom_prop_tmp.valid_from_dttm , valid_to_dttm = md_activity_custom_prop_tmp.valid_to_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        activity_id,activity_status_cd,activity_version_id,property_datatype_cd,property_nm,property_val,valid_from_dttm,valid_to_dttm
        ,Hashed_pk_col ) VALUES ( 
        md_activity_custom_prop_tmp.activity_id,md_activity_custom_prop_tmp.activity_status_cd,md_activity_custom_prop_tmp.activity_version_id,md_activity_custom_prop_tmp.property_datatype_cd,md_activity_custom_prop_tmp.property_nm,md_activity_custom_prop_tmp.property_val,md_activity_custom_prop_tmp.valid_from_dttm,md_activity_custom_prop_tmp.valid_to_dttm,md_activity_custom_prop_tmp.Hashed_pk_col
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_activity_custom_prop_tmp     , md_activity_custom_prop, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_activity_custom_prop_tmp     ;
    QUIT;
    %put ######## Staging table: md_activity_custom_prop_tmp      Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_activity_custom_prop;
      DROP TABLE work.md_activity_custom_prop;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_activity_custom_prop;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_activity_node) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_activity_node_tmp            ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_activity_node_tmp            ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_activity_node, table_keys=%str(activity_node_id,activity_version_id), out_table=work.md_activity_node);
 data &tmplib..md_activity_node_tmp            ;
     set work.md_activity_node;
  if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if activity_node_id='' then activity_node_id='-'; if activity_version_id='' then activity_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_activity_node_tmp            , md_activity_node);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_activity_node using &tmpdbschema..md_activity_node_tmp            
         ON (md_activity_node.activity_node_id=md_activity_node_tmp.activity_node_id and md_activity_node.activity_version_id=md_activity_node_tmp.activity_version_id)
        WHEN MATCHED THEN  
        UPDATE SET abtest_id = md_activity_node_tmp.abtest_id , activity_id = md_activity_node_tmp.activity_id , activity_node_nm = md_activity_node_tmp.activity_node_nm , activity_node_type_nm = md_activity_node_tmp.activity_node_type_nm , activity_status_cd = md_activity_node_tmp.activity_status_cd , end_node_flg = md_activity_node_tmp.end_node_flg , next_node_val = md_activity_node_tmp.next_node_val , node_sequence_no = md_activity_node_tmp.node_sequence_no , previous_node_val = md_activity_node_tmp.previous_node_val , specific_wait_flg = md_activity_node_tmp.specific_wait_flg , start_node_flg = md_activity_node_tmp.start_node_flg , time_boxed_flg = md_activity_node_tmp.time_boxed_flg , valid_from_dttm = md_activity_node_tmp.valid_from_dttm , valid_to_dttm = md_activity_node_tmp.valid_to_dttm , wait_tm = md_activity_node_tmp.wait_tm
        WHEN NOT MATCHED THEN INSERT ( 
        abtest_id,activity_id,activity_node_id,activity_node_nm,activity_node_type_nm,activity_status_cd,activity_version_id,end_node_flg,next_node_val,node_sequence_no,previous_node_val,specific_wait_flg,start_node_flg,time_boxed_flg,valid_from_dttm,valid_to_dttm,wait_tm
         ) values ( 
        md_activity_node_tmp.abtest_id,md_activity_node_tmp.activity_id,md_activity_node_tmp.activity_node_id,md_activity_node_tmp.activity_node_nm,md_activity_node_tmp.activity_node_type_nm,md_activity_node_tmp.activity_status_cd,md_activity_node_tmp.activity_version_id,md_activity_node_tmp.end_node_flg,md_activity_node_tmp.next_node_val,md_activity_node_tmp.node_sequence_no,md_activity_node_tmp.previous_node_val,md_activity_node_tmp.specific_wait_flg,md_activity_node_tmp.start_node_flg,md_activity_node_tmp.time_boxed_flg,md_activity_node_tmp.valid_from_dttm,md_activity_node_tmp.valid_to_dttm,md_activity_node_tmp.wait_tm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_activity_node_tmp            , md_activity_node, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_activity_node_tmp            ;
    QUIT;
    %put ######## Staging table: md_activity_node_tmp             Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_activity_node;
      DROP TABLE work.md_activity_node;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_activity_node;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_activity_x_activity_node) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_activity_x_activity_node_tmp ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_activity_x_activity_node_tmp ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_activity_x_activity_node, table_keys=%str(activity_node_id,activity_version_id), out_table=work.md_activity_x_activity_node);
 data &tmplib..md_activity_x_activity_node_tmp ;
     set work.md_activity_x_activity_node;
  if activity_node_id='' then activity_node_id='-'; if activity_version_id='' then activity_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_activity_x_activity_node_tmp , md_activity_x_activity_node);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_activity_x_activity_node using &tmpdbschema..md_activity_x_activity_node_tmp 
         ON (md_activity_x_activity_node.activity_node_id=md_activity_x_activity_node_tmp.activity_node_id and md_activity_x_activity_node.activity_version_id=md_activity_x_activity_node_tmp.activity_version_id)
        WHEN MATCHED THEN  
        UPDATE SET activity_id = md_activity_x_activity_node_tmp.activity_id , activity_status_cd = md_activity_x_activity_node_tmp.activity_status_cd
        WHEN NOT MATCHED THEN INSERT ( 
        activity_id,activity_node_id,activity_status_cd,activity_version_id
         ) values ( 
        md_activity_x_activity_node_tmp.activity_id,md_activity_x_activity_node_tmp.activity_node_id,md_activity_x_activity_node_tmp.activity_status_cd,md_activity_x_activity_node_tmp.activity_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_activity_x_activity_node_tmp , md_activity_x_activity_node, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_activity_x_activity_node_tmp ;
    QUIT;
    %put ######## Staging table: md_activity_x_activity_node_tmp  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_activity_x_activity_node;
      DROP TABLE work.md_activity_x_activity_node;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_activity_x_activity_node;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_activity_x_task) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_activity_x_task_tmp          ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_activity_x_task_tmp          ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_activity_x_task, table_keys=%str(activity_version_id,task_id), out_table=work.md_activity_x_task);
 data &tmplib..md_activity_x_task_tmp          ;
     set work.md_activity_x_task;
  if activity_version_id='' then activity_version_id='-'; if task_id='' then task_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_activity_x_task_tmp          , md_activity_x_task);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_activity_x_task using &tmpdbschema..md_activity_x_task_tmp          
         ON (md_activity_x_task.activity_version_id=md_activity_x_task_tmp.activity_version_id and md_activity_x_task.task_id=md_activity_x_task_tmp.task_id)
        WHEN MATCHED THEN  
        UPDATE SET activity_id = md_activity_x_task_tmp.activity_id , activity_status_cd = md_activity_x_task_tmp.activity_status_cd , task_version_id = md_activity_x_task_tmp.task_version_id
        WHEN NOT MATCHED THEN INSERT ( 
        activity_id,activity_status_cd,activity_version_id,task_id,task_version_id
         ) values ( 
        md_activity_x_task_tmp.activity_id,md_activity_x_task_tmp.activity_status_cd,md_activity_x_task_tmp.activity_version_id,md_activity_x_task_tmp.task_id,md_activity_x_task_tmp.task_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_activity_x_task_tmp          , md_activity_x_task, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_activity_x_task_tmp          ;
    QUIT;
    %put ######## Staging table: md_activity_x_task_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_activity_x_task;
      DROP TABLE work.md_activity_x_task;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_activity_x_task;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_asset) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_asset_tmp                    ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_asset_tmp                    ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_asset, table_keys=%str(asset_version_id), out_table=work.md_asset);
 data &tmplib..md_asset_tmp                    ;
     set work.md_asset;
  if last_published_dttm ne . then last_published_dttm = tzoneu2s(last_published_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if asset_version_id='' then asset_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_asset_tmp                    , md_asset);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_asset using &tmpdbschema..md_asset_tmp                    
         ON (md_asset.asset_version_id=md_asset_tmp.asset_version_id)
        WHEN MATCHED THEN  
        UPDATE SET asset_desc = md_asset_tmp.asset_desc , asset_id = md_asset_tmp.asset_id , asset_nm = md_asset_tmp.asset_nm , asset_status_cd = md_asset_tmp.asset_status_cd , asset_type_nm = md_asset_tmp.asset_type_nm , created_user_nm = md_asset_tmp.created_user_nm , last_published_dttm = md_asset_tmp.last_published_dttm , owner_nm = md_asset_tmp.owner_nm , valid_from_dttm = md_asset_tmp.valid_from_dttm , valid_to_dttm = md_asset_tmp.valid_to_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        asset_desc,asset_id,asset_nm,asset_status_cd,asset_type_nm,asset_version_id,created_user_nm,last_published_dttm,owner_nm,valid_from_dttm,valid_to_dttm
         ) values ( 
        md_asset_tmp.asset_desc,md_asset_tmp.asset_id,md_asset_tmp.asset_nm,md_asset_tmp.asset_status_cd,md_asset_tmp.asset_type_nm,md_asset_tmp.asset_version_id,md_asset_tmp.created_user_nm,md_asset_tmp.last_published_dttm,md_asset_tmp.owner_nm,md_asset_tmp.valid_from_dttm,md_asset_tmp.valid_to_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_asset_tmp                    , md_asset, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_asset_tmp                    ;
    QUIT;
    %put ######## Staging table: md_asset_tmp                     Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_asset;
      DROP TABLE work.md_asset;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_asset;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_audience) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_audience_tmp                 ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_audience_tmp                 ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_audience, table_keys=%str(audience_id), out_table=work.md_audience);
 data &tmplib..md_audience_tmp                 ;
     set work.md_audience;
  if create_dttm ne . then create_dttm = tzoneu2s(create_dttm,&timeZone_Value.);if delete_dttm ne . then delete_dttm = tzoneu2s(delete_dttm,&timeZone_Value.) ;
  if audience_id='' then audience_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_audience_tmp                 , md_audience);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_audience using &tmpdbschema..md_audience_tmp                 
         ON (md_audience.audience_id=md_audience_tmp.audience_id)
        WHEN MATCHED THEN  
        UPDATE SET audience_data_source_nm = md_audience_tmp.audience_data_source_nm , audience_desc = md_audience_tmp.audience_desc , audience_expiration_val = md_audience_tmp.audience_expiration_val , audience_nm = md_audience_tmp.audience_nm , audience_schedule_flg = md_audience_tmp.audience_schedule_flg , audience_source_nm = md_audience_tmp.audience_source_nm , create_dttm = md_audience_tmp.create_dttm , created_user_nm = md_audience_tmp.created_user_nm , delete_dttm = md_audience_tmp.delete_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        audience_data_source_nm,audience_desc,audience_expiration_val,audience_id,audience_nm,audience_schedule_flg,audience_source_nm,create_dttm,created_user_nm,delete_dttm
         ) values ( 
        md_audience_tmp.audience_data_source_nm,md_audience_tmp.audience_desc,md_audience_tmp.audience_expiration_val,md_audience_tmp.audience_id,md_audience_tmp.audience_nm,md_audience_tmp.audience_schedule_flg,md_audience_tmp.audience_source_nm,md_audience_tmp.create_dttm,md_audience_tmp.created_user_nm,md_audience_tmp.delete_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_audience_tmp                 , md_audience, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_audience_tmp                 ;
    QUIT;
    %put ######## Staging table: md_audience_tmp                  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_audience;
      DROP TABLE work.md_audience;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_audience;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_audience_occurrence) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_audience_occurrence_tmp      ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_audience_occurrence_tmp      ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_audience_occurrence, table_keys=%str(aud_occurrence_id), out_table=work.md_audience_occurrence);
 data &tmplib..md_audience_occurrence_tmp      ;
     set work.md_audience_occurrence;
  if end_tm ne . then end_tm = tzoneu2s(end_tm,&timeZone_Value.);if start_tm ne . then start_tm = tzoneu2s(start_tm,&timeZone_Value.) ;
  if aud_occurrence_id='' then aud_occurrence_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_audience_occurrence_tmp      , md_audience_occurrence);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_audience_occurrence using &tmpdbschema..md_audience_occurrence_tmp      
         ON (md_audience_occurrence.aud_occurrence_id=md_audience_occurrence_tmp.aud_occurrence_id)
        WHEN MATCHED THEN  
        UPDATE SET audience_id = md_audience_occurrence_tmp.audience_id , audience_size_val = md_audience_occurrence_tmp.audience_size_val , end_tm = md_audience_occurrence_tmp.end_tm , execution_status_cd = md_audience_occurrence_tmp.execution_status_cd , occurrence_type_nm = md_audience_occurrence_tmp.occurrence_type_nm , start_tm = md_audience_occurrence_tmp.start_tm , started_by_nm = md_audience_occurrence_tmp.started_by_nm
        WHEN NOT MATCHED THEN INSERT ( 
        aud_occurrence_id,audience_id,audience_size_val,end_tm,execution_status_cd,occurrence_type_nm,start_tm,started_by_nm
         ) values ( 
        md_audience_occurrence_tmp.aud_occurrence_id,md_audience_occurrence_tmp.audience_id,md_audience_occurrence_tmp.audience_size_val,md_audience_occurrence_tmp.end_tm,md_audience_occurrence_tmp.execution_status_cd,md_audience_occurrence_tmp.occurrence_type_nm,md_audience_occurrence_tmp.start_tm,md_audience_occurrence_tmp.started_by_nm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_audience_occurrence_tmp      , md_audience_occurrence, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_audience_occurrence_tmp      ;
    QUIT;
    %put ######## Staging table: md_audience_occurrence_tmp       Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_audience_occurrence;
      DROP TABLE work.md_audience_occurrence;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_audience_occurrence;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_audience_x_segment) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_audience_x_segment_tmp       ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_audience_x_segment_tmp       ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_audience_x_segment, table_keys=%str(audience_id), out_table=work.md_audience_x_segment);
 data &tmplib..md_audience_x_segment_tmp       ;
     set work.md_audience_x_segment;
  if audience_id='' then audience_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_audience_x_segment_tmp       , md_audience_x_segment);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_audience_x_segment using &tmpdbschema..md_audience_x_segment_tmp       
         ON (md_audience_x_segment.audience_id=md_audience_x_segment_tmp.audience_id)
        WHEN MATCHED THEN  
        UPDATE SET segment_id = md_audience_x_segment_tmp.segment_id
        WHEN NOT MATCHED THEN INSERT ( 
        audience_id,segment_id
         ) values ( 
        md_audience_x_segment_tmp.audience_id,md_audience_x_segment_tmp.segment_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_audience_x_segment_tmp       , md_audience_x_segment, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_audience_x_segment_tmp       ;
    QUIT;
    %put ######## Staging table: md_audience_x_segment_tmp        Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_audience_x_segment;
      DROP TABLE work.md_audience_x_segment;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_audience_x_segment;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_bu) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_bu_tmp                       ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_bu_tmp                       ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_bu, table_keys=%str(bu_id), out_table=work.md_bu);
 data &tmplib..md_bu_tmp                       ;
     set work.md_bu;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if bu_id='' then bu_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_bu_tmp                       , md_bu);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_bu using &tmpdbschema..md_bu_tmp                       
         ON (md_bu.bu_id=md_bu_tmp.bu_id)
        WHEN MATCHED THEN  
        UPDATE SET bu_currency_cd = md_bu_tmp.bu_currency_cd , bu_desc = md_bu_tmp.bu_desc , bu_nm = md_bu_tmp.bu_nm , bu_obsolete_flg = md_bu_tmp.bu_obsolete_flg , bu_owner_usernm = md_bu_tmp.bu_owner_usernm , bu_parentid = md_bu_tmp.bu_parentid , created_by_usernm = md_bu_tmp.created_by_usernm , created_dttm = md_bu_tmp.created_dttm , last_modified_dttm = md_bu_tmp.last_modified_dttm , last_modified_usernm = md_bu_tmp.last_modified_usernm , load_dttm = md_bu_tmp.load_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        bu_currency_cd,bu_desc,bu_id,bu_nm,bu_obsolete_flg,bu_owner_usernm,bu_parentid,created_by_usernm,created_dttm,last_modified_dttm,last_modified_usernm,load_dttm
         ) values ( 
        md_bu_tmp.bu_currency_cd,md_bu_tmp.bu_desc,md_bu_tmp.bu_id,md_bu_tmp.bu_nm,md_bu_tmp.bu_obsolete_flg,md_bu_tmp.bu_owner_usernm,md_bu_tmp.bu_parentid,md_bu_tmp.created_by_usernm,md_bu_tmp.created_dttm,md_bu_tmp.last_modified_dttm,md_bu_tmp.last_modified_usernm,md_bu_tmp.load_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_bu_tmp                       , md_bu, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_bu_tmp                       ;
    QUIT;
    %put ######## Staging table: md_bu_tmp                        Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_bu;
      DROP TABLE work.md_bu;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_bu;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_business_context) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_business_context_tmp         ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_business_context_tmp         ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_business_context, table_keys=%str(business_context_version_id), out_table=work.md_business_context);
 data &tmplib..md_business_context_tmp         ;
     set work.md_business_context;
  if last_published_dttm ne . then last_published_dttm = tzoneu2s(last_published_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if business_context_version_id='' then business_context_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_business_context_tmp         , md_business_context);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_business_context using &tmpdbschema..md_business_context_tmp         
         ON (md_business_context.business_context_version_id=md_business_context_tmp.business_context_version_id)
        WHEN MATCHED THEN  
        UPDATE SET business_context_desc = md_business_context_tmp.business_context_desc , business_context_id = md_business_context_tmp.business_context_id , business_context_nm = md_business_context_tmp.business_context_nm , business_context_src_cd = md_business_context_tmp.business_context_src_cd , business_context_status_cd = md_business_context_tmp.business_context_status_cd , created_user_nm = md_business_context_tmp.created_user_nm , information_map_nm = md_business_context_tmp.information_map_nm , last_published_dttm = md_business_context_tmp.last_published_dttm , locked_information_map_nm = md_business_context_tmp.locked_information_map_nm , owner_nm = md_business_context_tmp.owner_nm , valid_from_dttm = md_business_context_tmp.valid_from_dttm , valid_to_dttm = md_business_context_tmp.valid_to_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        business_context_desc,business_context_id,business_context_nm,business_context_src_cd,business_context_status_cd,business_context_version_id,created_user_nm,information_map_nm,last_published_dttm,locked_information_map_nm,owner_nm,valid_from_dttm,valid_to_dttm
         ) values ( 
        md_business_context_tmp.business_context_desc,md_business_context_tmp.business_context_id,md_business_context_tmp.business_context_nm,md_business_context_tmp.business_context_src_cd,md_business_context_tmp.business_context_status_cd,md_business_context_tmp.business_context_version_id,md_business_context_tmp.created_user_nm,md_business_context_tmp.information_map_nm,md_business_context_tmp.last_published_dttm,md_business_context_tmp.locked_information_map_nm,md_business_context_tmp.owner_nm,md_business_context_tmp.valid_from_dttm,md_business_context_tmp.valid_to_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_business_context_tmp         , md_business_context, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_business_context_tmp         ;
    QUIT;
    %put ######## Staging table: md_business_context_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_business_context;
      DROP TABLE work.md_business_context;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_business_context;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_cost_category) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_cost_category_tmp            ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_cost_category_tmp            ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_cost_category, table_keys=%str(ccat_id), out_table=work.md_cost_category);
 data &tmplib..md_cost_category_tmp            ;
     set work.md_cost_category;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if ccat_id='' then ccat_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_cost_category_tmp            , md_cost_category);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_cost_category using &tmpdbschema..md_cost_category_tmp            
         ON (md_cost_category.ccat_id=md_cost_category_tmp.ccat_id)
        WHEN MATCHED THEN  
        UPDATE SET ccat_desc = md_cost_category_tmp.ccat_desc , ccat_nm = md_cost_category_tmp.ccat_nm , ccat_obsolete_flg = md_cost_category_tmp.ccat_obsolete_flg , ccat_owner_usernm = md_cost_category_tmp.ccat_owner_usernm , created_by_usernm = md_cost_category_tmp.created_by_usernm , created_dttm = md_cost_category_tmp.created_dttm , fin_accnt_nm = md_cost_category_tmp.fin_accnt_nm , gen_ledger_cd = md_cost_category_tmp.gen_ledger_cd , last_modified_dttm = md_cost_category_tmp.last_modified_dttm , last_modified_usernm = md_cost_category_tmp.last_modified_usernm , load_dttm = md_cost_category_tmp.load_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        ccat_desc,ccat_id,ccat_nm,ccat_obsolete_flg,ccat_owner_usernm,created_by_usernm,created_dttm,fin_accnt_nm,gen_ledger_cd,last_modified_dttm,last_modified_usernm,load_dttm
         ) values ( 
        md_cost_category_tmp.ccat_desc,md_cost_category_tmp.ccat_id,md_cost_category_tmp.ccat_nm,md_cost_category_tmp.ccat_obsolete_flg,md_cost_category_tmp.ccat_owner_usernm,md_cost_category_tmp.created_by_usernm,md_cost_category_tmp.created_dttm,md_cost_category_tmp.fin_accnt_nm,md_cost_category_tmp.gen_ledger_cd,md_cost_category_tmp.last_modified_dttm,md_cost_category_tmp.last_modified_usernm,md_cost_category_tmp.load_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_cost_category_tmp            , md_cost_category, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_cost_category_tmp            ;
    QUIT;
    %put ######## Staging table: md_cost_category_tmp             Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_cost_category;
      DROP TABLE work.md_cost_category;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_cost_category;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_costcenter) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_costcenter_tmp               ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_costcenter_tmp               ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_costcenter, table_keys=%str(cost_center_id), out_table=work.md_costcenter);
 data &tmplib..md_costcenter_tmp               ;
     set work.md_costcenter;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if cost_center_id='' then cost_center_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_costcenter_tmp               , md_costcenter);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_costcenter using &tmpdbschema..md_costcenter_tmp               
         ON (md_costcenter.cost_center_id=md_costcenter_tmp.cost_center_id)
        WHEN MATCHED THEN  
        UPDATE SET cc_desc = md_costcenter_tmp.cc_desc , cc_nm = md_costcenter_tmp.cc_nm , cc_obsolete_flg = md_costcenter_tmp.cc_obsolete_flg , cc_owner_usernm = md_costcenter_tmp.cc_owner_usernm , created_by_usernm = md_costcenter_tmp.created_by_usernm , created_dttm = md_costcenter_tmp.created_dttm , fin_accnt_nm = md_costcenter_tmp.fin_accnt_nm , gen_ledger_cd = md_costcenter_tmp.gen_ledger_cd , last_modified_dttm = md_costcenter_tmp.last_modified_dttm , last_modified_usernm = md_costcenter_tmp.last_modified_usernm , load_dttm = md_costcenter_tmp.load_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        cc_desc,cc_nm,cc_obsolete_flg,cc_owner_usernm,cost_center_id,created_by_usernm,created_dttm,fin_accnt_nm,gen_ledger_cd,last_modified_dttm,last_modified_usernm,load_dttm
         ) values ( 
        md_costcenter_tmp.cc_desc,md_costcenter_tmp.cc_nm,md_costcenter_tmp.cc_obsolete_flg,md_costcenter_tmp.cc_owner_usernm,md_costcenter_tmp.cost_center_id,md_costcenter_tmp.created_by_usernm,md_costcenter_tmp.created_dttm,md_costcenter_tmp.fin_accnt_nm,md_costcenter_tmp.gen_ledger_cd,md_costcenter_tmp.last_modified_dttm,md_costcenter_tmp.last_modified_usernm,md_costcenter_tmp.load_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_costcenter_tmp               , md_costcenter, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_costcenter_tmp               ;
    QUIT;
    %put ######## Staging table: md_costcenter_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_costcenter;
      DROP TABLE work.md_costcenter;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_costcenter;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_creative) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_creative_tmp                 ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_creative_tmp                 ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_creative, table_keys=%str(creative_version_id), out_table=work.md_creative);
 data &tmplib..md_creative_tmp                 ;
     set work.md_creative;
  if last_published_dttm ne . then last_published_dttm = tzoneu2s(last_published_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if creative_version_id='' then creative_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_creative_tmp                 , md_creative);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_creative using &tmpdbschema..md_creative_tmp                 
         ON (md_creative.creative_version_id=md_creative_tmp.creative_version_id)
        WHEN MATCHED THEN  
        UPDATE SET business_context_id = md_creative_tmp.business_context_id , created_user_nm = md_creative_tmp.created_user_nm , creative_category_nm = md_creative_tmp.creative_category_nm , creative_cd = md_creative_tmp.creative_cd , creative_desc = md_creative_tmp.creative_desc , creative_id = md_creative_tmp.creative_id , creative_nm = md_creative_tmp.creative_nm , creative_status_cd = md_creative_tmp.creative_status_cd , creative_txt = md_creative_tmp.creative_txt , creative_type_nm = md_creative_tmp.creative_type_nm , folder_path_nm = md_creative_tmp.folder_path_nm , last_published_dttm = md_creative_tmp.last_published_dttm , owner_nm = md_creative_tmp.owner_nm , recommender_template_id = md_creative_tmp.recommender_template_id , recommender_template_nm = md_creative_tmp.recommender_template_nm , valid_from_dttm = md_creative_tmp.valid_from_dttm , valid_to_dttm = md_creative_tmp.valid_to_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        business_context_id,created_user_nm,creative_category_nm,creative_cd,creative_desc,creative_id,creative_nm,creative_status_cd,creative_txt,creative_type_nm,creative_version_id,folder_path_nm,last_published_dttm,owner_nm,recommender_template_id,recommender_template_nm,valid_from_dttm,valid_to_dttm
         ) values ( 
        md_creative_tmp.business_context_id,md_creative_tmp.created_user_nm,md_creative_tmp.creative_category_nm,md_creative_tmp.creative_cd,md_creative_tmp.creative_desc,md_creative_tmp.creative_id,md_creative_tmp.creative_nm,md_creative_tmp.creative_status_cd,md_creative_tmp.creative_txt,md_creative_tmp.creative_type_nm,md_creative_tmp.creative_version_id,md_creative_tmp.folder_path_nm,md_creative_tmp.last_published_dttm,md_creative_tmp.owner_nm,md_creative_tmp.recommender_template_id,md_creative_tmp.recommender_template_nm,md_creative_tmp.valid_from_dttm,md_creative_tmp.valid_to_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_creative_tmp                 , md_creative, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_creative_tmp                 ;
    QUIT;
    %put ######## Staging table: md_creative_tmp                  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_creative;
      DROP TABLE work.md_creative;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_creative;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_creative_custom_prop) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_creative_custom_prop_tmp     ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_creative_custom_prop_tmp     ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_creative_custom_prop, table_keys=%str(creative_version_id,property_datatype_cd,property_nm,property_val), out_table=work.md_creative_custom_prop);
 data &tmplib..md_creative_custom_prop_tmp     ;
     set work.md_creative_custom_prop;
  if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  Hashed_pk_col = put(sha256(catx('|',creative_version_id,property_datatype_cd,property_nm,property_val)), $hex64.);
  if creative_version_id='' then creative_version_id='-'; if property_datatype_cd='' then property_datatype_cd='-'; if property_nm='' then property_nm='-'; if property_val='' then property_val='-';
 run;
 %ErrCheck (Failed to Append Data to :md_creative_custom_prop_tmp     , md_creative_custom_prop);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_creative_custom_prop using &tmpdbschema..md_creative_custom_prop_tmp     
         ON (md_creative_custom_prop.Hashed_pk_col = md_creative_custom_prop_tmp.Hashed_pk_col)
        WHEN MATCHED THEN  
        UPDATE SET creative_id = md_creative_custom_prop_tmp.creative_id , creative_status_cd = md_creative_custom_prop_tmp.creative_status_cd , valid_from_dttm = md_creative_custom_prop_tmp.valid_from_dttm , valid_to_dttm = md_creative_custom_prop_tmp.valid_to_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        creative_id,creative_status_cd,creative_version_id,property_datatype_cd,property_nm,property_val,valid_from_dttm,valid_to_dttm
        ,Hashed_pk_col ) VALUES ( 
        md_creative_custom_prop_tmp.creative_id,md_creative_custom_prop_tmp.creative_status_cd,md_creative_custom_prop_tmp.creative_version_id,md_creative_custom_prop_tmp.property_datatype_cd,md_creative_custom_prop_tmp.property_nm,md_creative_custom_prop_tmp.property_val,md_creative_custom_prop_tmp.valid_from_dttm,md_creative_custom_prop_tmp.valid_to_dttm,md_creative_custom_prop_tmp.Hashed_pk_col
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_creative_custom_prop_tmp     , md_creative_custom_prop, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_creative_custom_prop_tmp     ;
    QUIT;
    %put ######## Staging table: md_creative_custom_prop_tmp      Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_creative_custom_prop;
      DROP TABLE work.md_creative_custom_prop;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_creative_custom_prop;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_creative_x_asset) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_creative_x_asset_tmp         ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_creative_x_asset_tmp         ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_creative_x_asset, table_keys=%str(asset_id,creative_version_id), out_table=work.md_creative_x_asset);
 data &tmplib..md_creative_x_asset_tmp         ;
     set work.md_creative_x_asset;
  if asset_id='' then asset_id='-'; if creative_version_id='' then creative_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_creative_x_asset_tmp         , md_creative_x_asset);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_creative_x_asset using &tmpdbschema..md_creative_x_asset_tmp         
         ON (md_creative_x_asset.asset_id=md_creative_x_asset_tmp.asset_id and md_creative_x_asset.creative_version_id=md_creative_x_asset_tmp.creative_version_id)
        WHEN MATCHED THEN  
        UPDATE SET creative_id = md_creative_x_asset_tmp.creative_id , creative_status_cd = md_creative_x_asset_tmp.creative_status_cd
        WHEN NOT MATCHED THEN INSERT ( 
        asset_id,creative_id,creative_status_cd,creative_version_id
         ) values ( 
        md_creative_x_asset_tmp.asset_id,md_creative_x_asset_tmp.creative_id,md_creative_x_asset_tmp.creative_status_cd,md_creative_x_asset_tmp.creative_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_creative_x_asset_tmp         , md_creative_x_asset, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_creative_x_asset_tmp         ;
    QUIT;
    %put ######## Staging table: md_creative_x_asset_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_creative_x_asset;
      DROP TABLE work.md_creative_x_asset;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_creative_x_asset;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_cust_attrib) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_cust_attrib_tmp              ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_cust_attrib_tmp              ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_cust_attrib, table_keys=%str(attr_group_id,attr_id), out_table=work.md_cust_attrib);
 data &tmplib..md_cust_attrib_tmp              ;
     set work.md_cust_attrib;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if attr_group_id='' then attr_group_id='-'; if attr_id='' then attr_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_cust_attrib_tmp              , md_cust_attrib);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_cust_attrib using &tmpdbschema..md_cust_attrib_tmp              
         ON (md_cust_attrib.attr_group_id=md_cust_attrib_tmp.attr_group_id and md_cust_attrib.attr_id=md_cust_attrib_tmp.attr_id)
        WHEN MATCHED THEN  
        UPDATE SET associated_grid = md_cust_attrib_tmp.associated_grid , attr_cd = md_cust_attrib_tmp.attr_cd , attr_group_cd = md_cust_attrib_tmp.attr_group_cd , attr_group_nm = md_cust_attrib_tmp.attr_group_nm , attr_nm = md_cust_attrib_tmp.attr_nm , created_by_usernm = md_cust_attrib_tmp.created_by_usernm , created_dttm = md_cust_attrib_tmp.created_dttm , data_formatter = md_cust_attrib_tmp.data_formatter , data_type = md_cust_attrib_tmp.data_type , is_grid_flg = md_cust_attrib_tmp.is_grid_flg , is_obsolete_flg = md_cust_attrib_tmp.is_obsolete_flg , last_modified_dttm = md_cust_attrib_tmp.last_modified_dttm , last_modified_usernm = md_cust_attrib_tmp.last_modified_usernm , load_dttm = md_cust_attrib_tmp.load_dttm , remote_pklist_tab_col = md_cust_attrib_tmp.remote_pklist_tab_col
        WHEN NOT MATCHED THEN INSERT ( 
        associated_grid,attr_cd,attr_group_cd,attr_group_id,attr_group_nm,attr_id,attr_nm,created_by_usernm,created_dttm,data_formatter,data_type,is_grid_flg,is_obsolete_flg,last_modified_dttm,last_modified_usernm,load_dttm,remote_pklist_tab_col
         ) values ( 
        md_cust_attrib_tmp.associated_grid,md_cust_attrib_tmp.attr_cd,md_cust_attrib_tmp.attr_group_cd,md_cust_attrib_tmp.attr_group_id,md_cust_attrib_tmp.attr_group_nm,md_cust_attrib_tmp.attr_id,md_cust_attrib_tmp.attr_nm,md_cust_attrib_tmp.created_by_usernm,md_cust_attrib_tmp.created_dttm,md_cust_attrib_tmp.data_formatter,md_cust_attrib_tmp.data_type,md_cust_attrib_tmp.is_grid_flg,md_cust_attrib_tmp.is_obsolete_flg,md_cust_attrib_tmp.last_modified_dttm,md_cust_attrib_tmp.last_modified_usernm,md_cust_attrib_tmp.load_dttm,md_cust_attrib_tmp.remote_pklist_tab_col
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_cust_attrib_tmp              , md_cust_attrib, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_cust_attrib_tmp              ;
    QUIT;
    %put ######## Staging table: md_cust_attrib_tmp               Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_cust_attrib;
      DROP TABLE work.md_cust_attrib;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_cust_attrib;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_custattrib_table_values) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_custattrib_table_values_tmp  ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_custattrib_table_values_tmp  ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_custattrib_table_values, table_keys=%str(attr_id,table_val), out_table=work.md_custattrib_table_values);
 data &tmplib..md_custattrib_table_values_tmp  ;
     set work.md_custattrib_table_values;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if attr_id='' then attr_id='-'; if table_val='' then table_val='-';
 run;
 %ErrCheck (Failed to Append Data to :md_custattrib_table_values_tmp  , md_custattrib_table_values);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_custattrib_table_values using &tmpdbschema..md_custattrib_table_values_tmp  
         ON (md_custattrib_table_values.attr_id=md_custattrib_table_values_tmp.attr_id and md_custattrib_table_values.table_val=md_custattrib_table_values_tmp.table_val)
        WHEN MATCHED THEN  
        UPDATE SET attr_cd = md_custattrib_table_values_tmp.attr_cd , attr_group_cd = md_custattrib_table_values_tmp.attr_group_cd , attr_group_id = md_custattrib_table_values_tmp.attr_group_id , attr_group_nm = md_custattrib_table_values_tmp.attr_group_nm , attr_nm = md_custattrib_table_values_tmp.attr_nm , created_by_usernm = md_custattrib_table_values_tmp.created_by_usernm , created_dttm = md_custattrib_table_values_tmp.created_dttm , data_formatter = md_custattrib_table_values_tmp.data_formatter , data_type = md_custattrib_table_values_tmp.data_type , is_obsolete_flg = md_custattrib_table_values_tmp.is_obsolete_flg , last_modified_dttm = md_custattrib_table_values_tmp.last_modified_dttm , last_modified_usernm = md_custattrib_table_values_tmp.last_modified_usernm , load_dttm = md_custattrib_table_values_tmp.load_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        attr_cd,attr_group_cd,attr_group_id,attr_group_nm,attr_id,attr_nm,created_by_usernm,created_dttm,data_formatter,data_type,is_obsolete_flg,last_modified_dttm,last_modified_usernm,load_dttm,table_val
         ) values ( 
        md_custattrib_table_values_tmp.attr_cd,md_custattrib_table_values_tmp.attr_group_cd,md_custattrib_table_values_tmp.attr_group_id,md_custattrib_table_values_tmp.attr_group_nm,md_custattrib_table_values_tmp.attr_id,md_custattrib_table_values_tmp.attr_nm,md_custattrib_table_values_tmp.created_by_usernm,md_custattrib_table_values_tmp.created_dttm,md_custattrib_table_values_tmp.data_formatter,md_custattrib_table_values_tmp.data_type,md_custattrib_table_values_tmp.is_obsolete_flg,md_custattrib_table_values_tmp.last_modified_dttm,md_custattrib_table_values_tmp.last_modified_usernm,md_custattrib_table_values_tmp.load_dttm,md_custattrib_table_values_tmp.table_val
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_custattrib_table_values_tmp  , md_custattrib_table_values, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_custattrib_table_values_tmp  ;
    QUIT;
    %put ######## Staging table: md_custattrib_table_values_tmp   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_custattrib_table_values;
      DROP TABLE work.md_custattrib_table_values;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_custattrib_table_values;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_dataview) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_dataview_tmp                 ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_dataview_tmp                 ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_dataview, table_keys=%str(dataview_version_id), out_table=work.md_dataview);
 data &tmplib..md_dataview_tmp                 ;
     set work.md_dataview;
  if last_published_dttm ne . then last_published_dttm = tzoneu2s(last_published_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if dataview_version_id='' then dataview_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_dataview_tmp                 , md_dataview);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_dataview using &tmpdbschema..md_dataview_tmp                 
         ON (md_dataview.dataview_version_id=md_dataview_tmp.dataview_version_id)
        WHEN MATCHED THEN  
        UPDATE SET analytic_active_flg = md_dataview_tmp.analytic_active_flg , analytics_period_type_nm = md_dataview_tmp.analytics_period_type_nm , analytics_period_val = md_dataview_tmp.analytics_period_val , created_user_nm = md_dataview_tmp.created_user_nm , custom_recent_cd = md_dataview_tmp.custom_recent_cd , custom_recent_exclude_cd = md_dataview_tmp.custom_recent_exclude_cd , dataview_desc = md_dataview_tmp.dataview_desc , dataview_id = md_dataview_tmp.dataview_id , dataview_nm = md_dataview_tmp.dataview_nm , dataview_status_cd = md_dataview_tmp.dataview_status_cd , half_life_time_val = md_dataview_tmp.half_life_time_val , include_external_flg = md_dataview_tmp.include_external_flg , include_internal_flg = md_dataview_tmp.include_internal_flg , last_published_dttm = md_dataview_tmp.last_published_dttm , max_path_length_val = md_dataview_tmp.max_path_length_val , max_path_time_type_nm = md_dataview_tmp.max_path_time_type_nm , max_path_time_val = md_dataview_tmp.max_path_time_val , owner_nm = md_dataview_tmp.owner_nm , selected_task_list = md_dataview_tmp.selected_task_list , valid_from_dttm = md_dataview_tmp.valid_from_dttm , valid_to_dttm = md_dataview_tmp.valid_to_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        analytic_active_flg,analytics_period_type_nm,analytics_period_val,created_user_nm,custom_recent_cd,custom_recent_exclude_cd,dataview_desc,dataview_id,dataview_nm,dataview_status_cd,dataview_version_id,half_life_time_val,include_external_flg,include_internal_flg,last_published_dttm,max_path_length_val,max_path_time_type_nm,max_path_time_val,owner_nm,selected_task_list,valid_from_dttm,valid_to_dttm
         ) values ( 
        md_dataview_tmp.analytic_active_flg,md_dataview_tmp.analytics_period_type_nm,md_dataview_tmp.analytics_period_val,md_dataview_tmp.created_user_nm,md_dataview_tmp.custom_recent_cd,md_dataview_tmp.custom_recent_exclude_cd,md_dataview_tmp.dataview_desc,md_dataview_tmp.dataview_id,md_dataview_tmp.dataview_nm,md_dataview_tmp.dataview_status_cd,md_dataview_tmp.dataview_version_id,md_dataview_tmp.half_life_time_val,md_dataview_tmp.include_external_flg,md_dataview_tmp.include_internal_flg,md_dataview_tmp.last_published_dttm,md_dataview_tmp.max_path_length_val,md_dataview_tmp.max_path_time_type_nm,md_dataview_tmp.max_path_time_val,md_dataview_tmp.owner_nm,md_dataview_tmp.selected_task_list,md_dataview_tmp.valid_from_dttm,md_dataview_tmp.valid_to_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_dataview_tmp                 , md_dataview, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_dataview_tmp                 ;
    QUIT;
    %put ######## Staging table: md_dataview_tmp                  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_dataview;
      DROP TABLE work.md_dataview;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_dataview;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_dataview_x_event) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_dataview_x_event_tmp         ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_dataview_x_event_tmp         ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_dataview_x_event, table_keys=%str(dataview_version_id,event_id), out_table=work.md_dataview_x_event);
 data &tmplib..md_dataview_x_event_tmp         ;
     set work.md_dataview_x_event;
  if dataview_version_id='' then dataview_version_id='-'; if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_dataview_x_event_tmp         , md_dataview_x_event);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_dataview_x_event using &tmpdbschema..md_dataview_x_event_tmp         
         ON (md_dataview_x_event.dataview_version_id=md_dataview_x_event_tmp.dataview_version_id and md_dataview_x_event.event_id=md_dataview_x_event_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET dataview_id = md_dataview_x_event_tmp.dataview_id , dataview_status_cd = md_dataview_x_event_tmp.dataview_status_cd
        WHEN NOT MATCHED THEN INSERT ( 
        dataview_id,dataview_status_cd,dataview_version_id,event_id
         ) values ( 
        md_dataview_x_event_tmp.dataview_id,md_dataview_x_event_tmp.dataview_status_cd,md_dataview_x_event_tmp.dataview_version_id,md_dataview_x_event_tmp.event_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_dataview_x_event_tmp         , md_dataview_x_event, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_dataview_x_event_tmp         ;
    QUIT;
    %put ######## Staging table: md_dataview_x_event_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_dataview_x_event;
      DROP TABLE work.md_dataview_x_event;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_dataview_x_event;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_event) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_event_tmp                    ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_event_tmp                    ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_event, table_keys=%str(event_version_id), out_table=work.md_event);
 data &tmplib..md_event_tmp                    ;
     set work.md_event;
  if last_published_dttm ne . then last_published_dttm = tzoneu2s(last_published_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if event_version_id='' then event_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_event_tmp                    , md_event);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_event using &tmpdbschema..md_event_tmp                    
         ON (md_event.event_version_id=md_event_tmp.event_version_id)
        WHEN MATCHED THEN  
        UPDATE SET channel_nm = md_event_tmp.channel_nm , created_user_nm = md_event_tmp.created_user_nm , event_desc = md_event_tmp.event_desc , event_id = md_event_tmp.event_id , event_nm = md_event_tmp.event_nm , event_status_cd = md_event_tmp.event_status_cd , event_subtype_nm = md_event_tmp.event_subtype_nm , event_type_nm = md_event_tmp.event_type_nm , last_published_dttm = md_event_tmp.last_published_dttm , owner_nm = md_event_tmp.owner_nm , valid_from_dttm = md_event_tmp.valid_from_dttm , valid_to_dttm = md_event_tmp.valid_to_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        channel_nm,created_user_nm,event_desc,event_id,event_nm,event_status_cd,event_subtype_nm,event_type_nm,event_version_id,last_published_dttm,owner_nm,valid_from_dttm,valid_to_dttm
         ) values ( 
        md_event_tmp.channel_nm,md_event_tmp.created_user_nm,md_event_tmp.event_desc,md_event_tmp.event_id,md_event_tmp.event_nm,md_event_tmp.event_status_cd,md_event_tmp.event_subtype_nm,md_event_tmp.event_type_nm,md_event_tmp.event_version_id,md_event_tmp.last_published_dttm,md_event_tmp.owner_nm,md_event_tmp.valid_from_dttm,md_event_tmp.valid_to_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_event_tmp                    , md_event, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_event_tmp                    ;
    QUIT;
    %put ######## Staging table: md_event_tmp                     Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_event;
      DROP TABLE work.md_event;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_event;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_fiscal_period) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_fiscal_period_tmp            ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_fiscal_period_tmp            ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_fiscal_period, table_keys=%str(fp_id), out_table=work.md_fiscal_period);
 data &tmplib..md_fiscal_period_tmp            ;
     set work.md_fiscal_period;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if fp_id='' then fp_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_fiscal_period_tmp            , md_fiscal_period);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_fiscal_period using &tmpdbschema..md_fiscal_period_tmp            
         ON (md_fiscal_period.fp_id=md_fiscal_period_tmp.fp_id)
        WHEN MATCHED THEN  
        UPDATE SET created_by_usernm = md_fiscal_period_tmp.created_by_usernm , created_dttm = md_fiscal_period_tmp.created_dttm , fp_cls_ver = md_fiscal_period_tmp.fp_cls_ver , fp_desc = md_fiscal_period_tmp.fp_desc , fp_end_dt = md_fiscal_period_tmp.fp_end_dt , fp_nm = md_fiscal_period_tmp.fp_nm , fp_obsolete_flg = md_fiscal_period_tmp.fp_obsolete_flg , fp_start_dt = md_fiscal_period_tmp.fp_start_dt , last_modified_dttm = md_fiscal_period_tmp.last_modified_dttm , last_modified_usernm = md_fiscal_period_tmp.last_modified_usernm , load_dttm = md_fiscal_period_tmp.load_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        created_by_usernm,created_dttm,fp_cls_ver,fp_desc,fp_end_dt,fp_id,fp_nm,fp_obsolete_flg,fp_start_dt,last_modified_dttm,last_modified_usernm,load_dttm
         ) values ( 
        md_fiscal_period_tmp.created_by_usernm,md_fiscal_period_tmp.created_dttm,md_fiscal_period_tmp.fp_cls_ver,md_fiscal_period_tmp.fp_desc,md_fiscal_period_tmp.fp_end_dt,md_fiscal_period_tmp.fp_id,md_fiscal_period_tmp.fp_nm,md_fiscal_period_tmp.fp_obsolete_flg,md_fiscal_period_tmp.fp_start_dt,md_fiscal_period_tmp.last_modified_dttm,md_fiscal_period_tmp.last_modified_usernm,md_fiscal_period_tmp.load_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_fiscal_period_tmp            , md_fiscal_period, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_fiscal_period_tmp            ;
    QUIT;
    %put ######## Staging table: md_fiscal_period_tmp             Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_fiscal_period;
      DROP TABLE work.md_fiscal_period;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_fiscal_period;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_grid_attr_defn) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_grid_attr_defn_tmp           ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_grid_attr_defn_tmp           ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_grid_attr_defn, table_keys=%str(attr_group_id,attr_id,grid_id), out_table=work.md_grid_attr_defn);
 data &tmplib..md_grid_attr_defn_tmp           ;
     set work.md_grid_attr_defn;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if attr_group_id='' then attr_group_id='-'; if attr_id='' then attr_id='-'; if grid_id='' then grid_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_grid_attr_defn_tmp           , md_grid_attr_defn);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_grid_attr_defn using &tmpdbschema..md_grid_attr_defn_tmp           
         ON (md_grid_attr_defn.attr_group_id=md_grid_attr_defn_tmp.attr_group_id and md_grid_attr_defn.attr_id=md_grid_attr_defn_tmp.attr_id and md_grid_attr_defn.grid_id=md_grid_attr_defn_tmp.grid_id)
        WHEN MATCHED THEN  
        UPDATE SET associated_grid = md_grid_attr_defn_tmp.associated_grid , attr_cd = md_grid_attr_defn_tmp.attr_cd , attr_desc = md_grid_attr_defn_tmp.attr_desc , attr_group_cd = md_grid_attr_defn_tmp.attr_group_cd , attr_group_nm = md_grid_attr_defn_tmp.attr_group_nm , attr_nm = md_grid_attr_defn_tmp.attr_nm , attr_obsolete_flg = md_grid_attr_defn_tmp.attr_obsolete_flg , attr_order_no = md_grid_attr_defn_tmp.attr_order_no , created_by_usernm = md_grid_attr_defn_tmp.created_by_usernm , created_dttm = md_grid_attr_defn_tmp.created_dttm , data_formatter = md_grid_attr_defn_tmp.data_formatter , data_type = md_grid_attr_defn_tmp.data_type , grid_cd = md_grid_attr_defn_tmp.grid_cd , grid_desc = md_grid_attr_defn_tmp.grid_desc , grid_mandatory_flg = md_grid_attr_defn_tmp.grid_mandatory_flg , grid_nm = md_grid_attr_defn_tmp.grid_nm , grid_obsolete_flg = md_grid_attr_defn_tmp.grid_obsolete_flg , last_modified_dttm = md_grid_attr_defn_tmp.last_modified_dttm , last_modified_usernm = md_grid_attr_defn_tmp.last_modified_usernm , load_dttm = md_grid_attr_defn_tmp.load_dttm , remote_pklist_tab_col = md_grid_attr_defn_tmp.remote_pklist_tab_col
        WHEN NOT MATCHED THEN INSERT ( 
        associated_grid,attr_cd,attr_desc,attr_group_cd,attr_group_id,attr_group_nm,attr_id,attr_nm,attr_obsolete_flg,attr_order_no,created_by_usernm,created_dttm,data_formatter,data_type,grid_cd,grid_desc,grid_id,grid_mandatory_flg,grid_nm,grid_obsolete_flg,last_modified_dttm,last_modified_usernm,load_dttm,remote_pklist_tab_col
         ) values ( 
        md_grid_attr_defn_tmp.associated_grid,md_grid_attr_defn_tmp.attr_cd,md_grid_attr_defn_tmp.attr_desc,md_grid_attr_defn_tmp.attr_group_cd,md_grid_attr_defn_tmp.attr_group_id,md_grid_attr_defn_tmp.attr_group_nm,md_grid_attr_defn_tmp.attr_id,md_grid_attr_defn_tmp.attr_nm,md_grid_attr_defn_tmp.attr_obsolete_flg,md_grid_attr_defn_tmp.attr_order_no,md_grid_attr_defn_tmp.created_by_usernm,md_grid_attr_defn_tmp.created_dttm,md_grid_attr_defn_tmp.data_formatter,md_grid_attr_defn_tmp.data_type,md_grid_attr_defn_tmp.grid_cd,md_grid_attr_defn_tmp.grid_desc,md_grid_attr_defn_tmp.grid_id,md_grid_attr_defn_tmp.grid_mandatory_flg,md_grid_attr_defn_tmp.grid_nm,md_grid_attr_defn_tmp.grid_obsolete_flg,md_grid_attr_defn_tmp.last_modified_dttm,md_grid_attr_defn_tmp.last_modified_usernm,md_grid_attr_defn_tmp.load_dttm,md_grid_attr_defn_tmp.remote_pklist_tab_col
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_grid_attr_defn_tmp           , md_grid_attr_defn, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_grid_attr_defn_tmp           ;
    QUIT;
    %put ######## Staging table: md_grid_attr_defn_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_grid_attr_defn;
      DROP TABLE work.md_grid_attr_defn;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_grid_attr_defn;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_journey) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_journey_tmp                  ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_journey_tmp                  ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_journey, table_keys=%str(journey_version_id), out_table=work.md_journey);
 data &tmplib..md_journey_tmp                  ;
     set work.md_journey;
  if last_activated_dttm ne . then last_activated_dttm = tzoneu2s(last_activated_dttm,&timeZone_Value.) ;
  if journey_version_id='' then journey_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_journey_tmp                  , md_journey);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_journey using &tmpdbschema..md_journey_tmp                  
         ON (md_journey.journey_version_id=md_journey_tmp.journey_version_id)
        WHEN MATCHED THEN  
        UPDATE SET activated_user_nm = md_journey_tmp.activated_user_nm , control_group_flg = md_journey_tmp.control_group_flg , created_user_nm = md_journey_tmp.created_user_nm , journey_id = md_journey_tmp.journey_id , journey_nm = md_journey_tmp.journey_nm , journey_status_cd = md_journey_tmp.journey_status_cd , last_activated_dttm = md_journey_tmp.last_activated_dttm , purpose_id = md_journey_tmp.purpose_id , target_goal_qty = md_journey_tmp.target_goal_qty , target_goal_type_nm = md_journey_tmp.target_goal_type_nm
        WHEN NOT MATCHED THEN INSERT ( 
        activated_user_nm,control_group_flg,created_user_nm,journey_id,journey_nm,journey_status_cd,journey_version_id,last_activated_dttm,purpose_id,target_goal_qty,target_goal_type_nm
         ) values ( 
        md_journey_tmp.activated_user_nm,md_journey_tmp.control_group_flg,md_journey_tmp.created_user_nm,md_journey_tmp.journey_id,md_journey_tmp.journey_nm,md_journey_tmp.journey_status_cd,md_journey_tmp.journey_version_id,md_journey_tmp.last_activated_dttm,md_journey_tmp.purpose_id,md_journey_tmp.target_goal_qty,md_journey_tmp.target_goal_type_nm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_journey_tmp                  , md_journey, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_journey_tmp                  ;
    QUIT;
    %put ######## Staging table: md_journey_tmp                   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_journey;
      DROP TABLE work.md_journey;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_journey;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_journey_node) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_journey_node_tmp             ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_journey_node_tmp             ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_journey_node, table_keys=%str(journey_node_id), out_table=work.md_journey_node);
 data &tmplib..md_journey_node_tmp             ;
     set work.md_journey_node;
  if journey_node_id='' then journey_node_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_journey_node_tmp             , md_journey_node);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_journey_node using &tmpdbschema..md_journey_node_tmp             
         ON (md_journey_node.journey_node_id=md_journey_node_tmp.journey_node_id)
        WHEN MATCHED THEN  
        UPDATE SET journey_id = md_journey_node_tmp.journey_id , journey_version_id = md_journey_node_tmp.journey_version_id , next_node_id = md_journey_node_tmp.next_node_id , node_nm = md_journey_node_tmp.node_nm , node_type = md_journey_node_tmp.node_type , previous_node_id = md_journey_node_tmp.previous_node_id
        WHEN NOT MATCHED THEN INSERT ( 
        journey_id,journey_node_id,journey_version_id,next_node_id,node_nm,node_type,previous_node_id
         ) values ( 
        md_journey_node_tmp.journey_id,md_journey_node_tmp.journey_node_id,md_journey_node_tmp.journey_version_id,md_journey_node_tmp.next_node_id,md_journey_node_tmp.node_nm,md_journey_node_tmp.node_type,md_journey_node_tmp.previous_node_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_journey_node_tmp             , md_journey_node, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_journey_node_tmp             ;
    QUIT;
    %put ######## Staging table: md_journey_node_tmp              Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_journey_node;
      DROP TABLE work.md_journey_node;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_journey_node;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_journey_node_occurrence) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_journey_node_occurrence_tmp  ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_journey_node_occurrence_tmp  ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_journey_node_occurrence, table_keys=%str(journey_node_occurrence_id), out_table=work.md_journey_node_occurrence);
 data &tmplib..md_journey_node_occurrence_tmp  ;
     set work.md_journey_node_occurrence;
  if end_dttm ne . then end_dttm = tzoneu2s(end_dttm,&timeZone_Value.);if start_dttm ne . then start_dttm = tzoneu2s(start_dttm,&timeZone_Value.) ;
  if journey_node_occurrence_id='' then journey_node_occurrence_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_journey_node_occurrence_tmp  , md_journey_node_occurrence);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_journey_node_occurrence using &tmpdbschema..md_journey_node_occurrence_tmp  
         ON (md_journey_node_occurrence.journey_node_occurrence_id=md_journey_node_occurrence_tmp.journey_node_occurrence_id)
        WHEN MATCHED THEN  
        UPDATE SET end_dttm = md_journey_node_occurrence_tmp.end_dttm , error_messages = md_journey_node_occurrence_tmp.error_messages , execution_status = md_journey_node_occurrence_tmp.execution_status , journey_id = md_journey_node_occurrence_tmp.journey_id , journey_node_id = md_journey_node_occurrence_tmp.journey_node_id , journey_occurrence_id = md_journey_node_occurrence_tmp.journey_occurrence_id , journey_version_id = md_journey_node_occurrence_tmp.journey_version_id , num_of_contacts_entered = md_journey_node_occurrence_tmp.num_of_contacts_entered , start_dttm = md_journey_node_occurrence_tmp.start_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        end_dttm,error_messages,execution_status,journey_id,journey_node_id,journey_node_occurrence_id,journey_occurrence_id,journey_version_id,num_of_contacts_entered,start_dttm
         ) values ( 
        md_journey_node_occurrence_tmp.end_dttm,md_journey_node_occurrence_tmp.error_messages,md_journey_node_occurrence_tmp.execution_status,md_journey_node_occurrence_tmp.journey_id,md_journey_node_occurrence_tmp.journey_node_id,md_journey_node_occurrence_tmp.journey_node_occurrence_id,md_journey_node_occurrence_tmp.journey_occurrence_id,md_journey_node_occurrence_tmp.journey_version_id,md_journey_node_occurrence_tmp.num_of_contacts_entered,md_journey_node_occurrence_tmp.start_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_journey_node_occurrence_tmp  , md_journey_node_occurrence, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_journey_node_occurrence_tmp  ;
    QUIT;
    %put ######## Staging table: md_journey_node_occurrence_tmp   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_journey_node_occurrence;
      DROP TABLE work.md_journey_node_occurrence;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_journey_node_occurrence;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_journey_occurrence) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_journey_occurrence_tmp       ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_journey_occurrence_tmp       ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_journey_occurrence, table_keys=%str(journey_occurrence_id), out_table=work.md_journey_occurrence);
 data &tmplib..md_journey_occurrence_tmp       ;
     set work.md_journey_occurrence;
  if end_dttm ne . then end_dttm = tzoneu2s(end_dttm,&timeZone_Value.);if start_dttm ne . then start_dttm = tzoneu2s(start_dttm,&timeZone_Value.) ;
  if journey_occurrence_id='' then journey_occurrence_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_journey_occurrence_tmp       , md_journey_occurrence);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_journey_occurrence using &tmpdbschema..md_journey_occurrence_tmp       
         ON (md_journey_occurrence.journey_occurrence_id=md_journey_occurrence_tmp.journey_occurrence_id)
        WHEN MATCHED THEN  
        UPDATE SET aud_occurrence_id = md_journey_occurrence_tmp.aud_occurrence_id , end_dttm = md_journey_occurrence_tmp.end_dttm , error_messages = md_journey_occurrence_tmp.error_messages , execution_status = md_journey_occurrence_tmp.execution_status , journey_id = md_journey_occurrence_tmp.journey_id , journey_occurrence_num = md_journey_occurrence_tmp.journey_occurrence_num , journey_version_id = md_journey_occurrence_tmp.journey_version_id , num_of_contacts_entered = md_journey_occurrence_tmp.num_of_contacts_entered , num_of_contacts_suppressed = md_journey_occurrence_tmp.num_of_contacts_suppressed , occurrence_type_nm = md_journey_occurrence_tmp.occurrence_type_nm , start_dttm = md_journey_occurrence_tmp.start_dttm , started_by_nm = md_journey_occurrence_tmp.started_by_nm
        WHEN NOT MATCHED THEN INSERT ( 
        aud_occurrence_id,end_dttm,error_messages,execution_status,journey_id,journey_occurrence_id,journey_occurrence_num,journey_version_id,num_of_contacts_entered,num_of_contacts_suppressed,occurrence_type_nm,start_dttm,started_by_nm
         ) values ( 
        md_journey_occurrence_tmp.aud_occurrence_id,md_journey_occurrence_tmp.end_dttm,md_journey_occurrence_tmp.error_messages,md_journey_occurrence_tmp.execution_status,md_journey_occurrence_tmp.journey_id,md_journey_occurrence_tmp.journey_occurrence_id,md_journey_occurrence_tmp.journey_occurrence_num,md_journey_occurrence_tmp.journey_version_id,md_journey_occurrence_tmp.num_of_contacts_entered,md_journey_occurrence_tmp.num_of_contacts_suppressed,md_journey_occurrence_tmp.occurrence_type_nm,md_journey_occurrence_tmp.start_dttm,md_journey_occurrence_tmp.started_by_nm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_journey_occurrence_tmp       , md_journey_occurrence, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_journey_occurrence_tmp       ;
    QUIT;
    %put ######## Staging table: md_journey_occurrence_tmp        Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_journey_occurrence;
      DROP TABLE work.md_journey_occurrence;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_journey_occurrence;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_journey_x_audience) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_journey_x_audience_tmp       ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_journey_x_audience_tmp       ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_journey_x_audience, table_keys=%str(audience_id,journey_version_id), out_table=work.md_journey_x_audience);
 data &tmplib..md_journey_x_audience_tmp       ;
     set work.md_journey_x_audience;
  if audience_id='' then audience_id='-'; if journey_version_id='' then journey_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_journey_x_audience_tmp       , md_journey_x_audience);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_journey_x_audience using &tmpdbschema..md_journey_x_audience_tmp       
         ON (md_journey_x_audience.audience_id=md_journey_x_audience_tmp.audience_id and md_journey_x_audience.journey_version_id=md_journey_x_audience_tmp.journey_version_id)
        WHEN MATCHED THEN  
        UPDATE SET aud_relationship_nm = md_journey_x_audience_tmp.aud_relationship_nm , journey_id = md_journey_x_audience_tmp.journey_id , journey_node_id = md_journey_x_audience_tmp.journey_node_id
        WHEN NOT MATCHED THEN INSERT ( 
        aud_relationship_nm,audience_id,journey_id,journey_node_id,journey_version_id
         ) values ( 
        md_journey_x_audience_tmp.aud_relationship_nm,md_journey_x_audience_tmp.audience_id,md_journey_x_audience_tmp.journey_id,md_journey_x_audience_tmp.journey_node_id,md_journey_x_audience_tmp.journey_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_journey_x_audience_tmp       , md_journey_x_audience, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_journey_x_audience_tmp       ;
    QUIT;
    %put ######## Staging table: md_journey_x_audience_tmp        Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_journey_x_audience;
      DROP TABLE work.md_journey_x_audience;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_journey_x_audience;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_journey_x_event) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_journey_x_event_tmp          ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_journey_x_event_tmp          ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_journey_x_event, table_keys=%str(event_id,journey_node_id), out_table=work.md_journey_x_event);
 data &tmplib..md_journey_x_event_tmp          ;
     set work.md_journey_x_event;
  if event_id='' then event_id='-'; if journey_node_id='' then journey_node_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_journey_x_event_tmp          , md_journey_x_event);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_journey_x_event using &tmpdbschema..md_journey_x_event_tmp          
         ON (md_journey_x_event.event_id=md_journey_x_event_tmp.event_id and md_journey_x_event.journey_node_id=md_journey_x_event_tmp.journey_node_id)
        WHEN MATCHED THEN  
        UPDATE SET event_relationship_nm = md_journey_x_event_tmp.event_relationship_nm , journey_id = md_journey_x_event_tmp.journey_id , journey_version_id = md_journey_x_event_tmp.journey_version_id
        WHEN NOT MATCHED THEN INSERT ( 
        event_id,event_relationship_nm,journey_id,journey_node_id,journey_version_id
         ) values ( 
        md_journey_x_event_tmp.event_id,md_journey_x_event_tmp.event_relationship_nm,md_journey_x_event_tmp.journey_id,md_journey_x_event_tmp.journey_node_id,md_journey_x_event_tmp.journey_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_journey_x_event_tmp          , md_journey_x_event, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_journey_x_event_tmp          ;
    QUIT;
    %put ######## Staging table: md_journey_x_event_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_journey_x_event;
      DROP TABLE work.md_journey_x_event;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_journey_x_event;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_journey_x_task) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_journey_x_task_tmp           ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_journey_x_task_tmp           ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_journey_x_task, table_keys=%str(journey_node_id), out_table=work.md_journey_x_task);
 data &tmplib..md_journey_x_task_tmp           ;
     set work.md_journey_x_task;
  if journey_node_id='' then journey_node_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_journey_x_task_tmp           , md_journey_x_task);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_journey_x_task using &tmpdbschema..md_journey_x_task_tmp           
         ON (md_journey_x_task.journey_node_id=md_journey_x_task_tmp.journey_node_id)
        WHEN MATCHED THEN  
        UPDATE SET journey_id = md_journey_x_task_tmp.journey_id , journey_version_id = md_journey_x_task_tmp.journey_version_id , task_id = md_journey_x_task_tmp.task_id , task_version_id = md_journey_x_task_tmp.task_version_id
        WHEN NOT MATCHED THEN INSERT ( 
        journey_id,journey_node_id,journey_version_id,task_id,task_version_id
         ) values ( 
        md_journey_x_task_tmp.journey_id,md_journey_x_task_tmp.journey_node_id,md_journey_x_task_tmp.journey_version_id,md_journey_x_task_tmp.task_id,md_journey_x_task_tmp.task_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_journey_x_task_tmp           , md_journey_x_task, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_journey_x_task_tmp           ;
    QUIT;
    %put ######## Staging table: md_journey_x_task_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_journey_x_task;
      DROP TABLE work.md_journey_x_task;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_journey_x_task;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_message) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_message_tmp                  ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_message_tmp                  ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_message, table_keys=%str(message_version_id), out_table=work.md_message);
 data &tmplib..md_message_tmp                  ;
     set work.md_message;
  if last_published_dttm ne . then last_published_dttm = tzoneu2s(last_published_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if message_version_id='' then message_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_message_tmp                  , md_message);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_message using &tmpdbschema..md_message_tmp                  
         ON (md_message.message_version_id=md_message_tmp.message_version_id)
        WHEN MATCHED THEN  
        UPDATE SET business_context_id = md_message_tmp.business_context_id , created_user_nm = md_message_tmp.created_user_nm , folder_path_nm = md_message_tmp.folder_path_nm , last_published_dttm = md_message_tmp.last_published_dttm , message_category_nm = md_message_tmp.message_category_nm , message_cd = md_message_tmp.message_cd , message_desc = md_message_tmp.message_desc , message_id = md_message_tmp.message_id , message_nm = md_message_tmp.message_nm , message_status_cd = md_message_tmp.message_status_cd , message_type_nm = md_message_tmp.message_type_nm , owner_nm = md_message_tmp.owner_nm , valid_from_dttm = md_message_tmp.valid_from_dttm , valid_to_dttm = md_message_tmp.valid_to_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        business_context_id,created_user_nm,folder_path_nm,last_published_dttm,message_category_nm,message_cd,message_desc,message_id,message_nm,message_status_cd,message_type_nm,message_version_id,owner_nm,valid_from_dttm,valid_to_dttm
         ) values ( 
        md_message_tmp.business_context_id,md_message_tmp.created_user_nm,md_message_tmp.folder_path_nm,md_message_tmp.last_published_dttm,md_message_tmp.message_category_nm,md_message_tmp.message_cd,md_message_tmp.message_desc,md_message_tmp.message_id,md_message_tmp.message_nm,md_message_tmp.message_status_cd,md_message_tmp.message_type_nm,md_message_tmp.message_version_id,md_message_tmp.owner_nm,md_message_tmp.valid_from_dttm,md_message_tmp.valid_to_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_message_tmp                  , md_message, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_message_tmp                  ;
    QUIT;
    %put ######## Staging table: md_message_tmp                   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_message;
      DROP TABLE work.md_message;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_message;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_message_custom_prop) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_message_custom_prop_tmp      ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_message_custom_prop_tmp      ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_message_custom_prop, table_keys=%str(message_version_id,property_datatype_cd,property_nm,property_val), out_table=work.md_message_custom_prop);
 data &tmplib..md_message_custom_prop_tmp      ;
     set work.md_message_custom_prop;
  if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  Hashed_pk_col = put(sha256(catx('|',message_version_id,property_datatype_cd,property_nm,property_val)), $hex64.);
  if message_version_id='' then message_version_id='-'; if property_datatype_cd='' then property_datatype_cd='-'; if property_nm='' then property_nm='-'; if property_val='' then property_val='-';
 run;
 %ErrCheck (Failed to Append Data to :md_message_custom_prop_tmp      , md_message_custom_prop);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_message_custom_prop using &tmpdbschema..md_message_custom_prop_tmp      
         ON (md_message_custom_prop.Hashed_pk_col = md_message_custom_prop_tmp.Hashed_pk_col)
        WHEN MATCHED THEN  
        UPDATE SET message_id = md_message_custom_prop_tmp.message_id , message_status_cd = md_message_custom_prop_tmp.message_status_cd , valid_from_dttm = md_message_custom_prop_tmp.valid_from_dttm , valid_to_dttm = md_message_custom_prop_tmp.valid_to_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        message_id,message_status_cd,message_version_id,property_datatype_cd,property_nm,property_val,valid_from_dttm,valid_to_dttm
        ,Hashed_pk_col ) VALUES ( 
        md_message_custom_prop_tmp.message_id,md_message_custom_prop_tmp.message_status_cd,md_message_custom_prop_tmp.message_version_id,md_message_custom_prop_tmp.property_datatype_cd,md_message_custom_prop_tmp.property_nm,md_message_custom_prop_tmp.property_val,md_message_custom_prop_tmp.valid_from_dttm,md_message_custom_prop_tmp.valid_to_dttm,md_message_custom_prop_tmp.Hashed_pk_col
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_message_custom_prop_tmp      , md_message_custom_prop, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_message_custom_prop_tmp      ;
    QUIT;
    %put ######## Staging table: md_message_custom_prop_tmp       Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_message_custom_prop;
      DROP TABLE work.md_message_custom_prop;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_message_custom_prop;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_message_x_creative) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_message_x_creative_tmp       ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_message_x_creative_tmp       ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_message_x_creative, table_keys=%str(creative_id,message_version_id), out_table=work.md_message_x_creative);
 data &tmplib..md_message_x_creative_tmp       ;
     set work.md_message_x_creative;
  if creative_id='' then creative_id='-'; if message_version_id='' then message_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_message_x_creative_tmp       , md_message_x_creative);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_message_x_creative using &tmpdbschema..md_message_x_creative_tmp       
         ON (md_message_x_creative.creative_id=md_message_x_creative_tmp.creative_id and md_message_x_creative.message_version_id=md_message_x_creative_tmp.message_version_id)
        WHEN MATCHED THEN  
        UPDATE SET message_id = md_message_x_creative_tmp.message_id , message_status_cd = md_message_x_creative_tmp.message_status_cd
        WHEN NOT MATCHED THEN INSERT ( 
        creative_id,message_id,message_status_cd,message_version_id
         ) values ( 
        md_message_x_creative_tmp.creative_id,md_message_x_creative_tmp.message_id,md_message_x_creative_tmp.message_status_cd,md_message_x_creative_tmp.message_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_message_x_creative_tmp       , md_message_x_creative, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_message_x_creative_tmp       ;
    QUIT;
    %put ######## Staging table: md_message_x_creative_tmp        Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_message_x_creative;
      DROP TABLE work.md_message_x_creative;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_message_x_creative;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_object_type) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_object_type_tmp              ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_object_type_tmp              ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_object_type, table_keys=%str(attr_group_id,attr_id,object_type), out_table=work.md_object_type);
 data &tmplib..md_object_type_tmp              ;
     set work.md_object_type;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if attr_group_id='' then attr_group_id='-'; if attr_id='' then attr_id='-'; if object_type='' then object_type='-';
 run;
 %ErrCheck (Failed to Append Data to :md_object_type_tmp              , md_object_type);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_object_type using &tmpdbschema..md_object_type_tmp              
         ON (md_object_type.attr_group_id=md_object_type_tmp.attr_group_id and md_object_type.attr_id=md_object_type_tmp.attr_id and md_object_type.object_type=md_object_type_tmp.object_type)
        WHEN MATCHED THEN  
        UPDATE SET attr_cd = md_object_type_tmp.attr_cd , attr_group_cd = md_object_type_tmp.attr_group_cd , attr_group_nm = md_object_type_tmp.attr_group_nm , attr_nm = md_object_type_tmp.attr_nm , created_by_usernm = md_object_type_tmp.created_by_usernm , created_dttm = md_object_type_tmp.created_dttm , data_formatter = md_object_type_tmp.data_formatter , data_type = md_object_type_tmp.data_type , is_obsolete_flg = md_object_type_tmp.is_obsolete_flg , last_modified_dttm = md_object_type_tmp.last_modified_dttm , last_modified_usernm = md_object_type_tmp.last_modified_usernm , load_dttm = md_object_type_tmp.load_dttm , object_category = md_object_type_tmp.object_category , remote_pklist_tab_col = md_object_type_tmp.remote_pklist_tab_col
        WHEN NOT MATCHED THEN INSERT ( 
        attr_cd,attr_group_cd,attr_group_id,attr_group_nm,attr_id,attr_nm,created_by_usernm,created_dttm,data_formatter,data_type,is_obsolete_flg,last_modified_dttm,last_modified_usernm,load_dttm,object_category,object_type,remote_pklist_tab_col
         ) values ( 
        md_object_type_tmp.attr_cd,md_object_type_tmp.attr_group_cd,md_object_type_tmp.attr_group_id,md_object_type_tmp.attr_group_nm,md_object_type_tmp.attr_id,md_object_type_tmp.attr_nm,md_object_type_tmp.created_by_usernm,md_object_type_tmp.created_dttm,md_object_type_tmp.data_formatter,md_object_type_tmp.data_type,md_object_type_tmp.is_obsolete_flg,md_object_type_tmp.last_modified_dttm,md_object_type_tmp.last_modified_usernm,md_object_type_tmp.load_dttm,md_object_type_tmp.object_category,md_object_type_tmp.object_type,md_object_type_tmp.remote_pklist_tab_col
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_object_type_tmp              , md_object_type, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_object_type_tmp              ;
    QUIT;
    %put ######## Staging table: md_object_type_tmp               Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_object_type;
      DROP TABLE work.md_object_type;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_object_type;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_occurrence) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_occurrence_tmp               ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_occurrence_tmp               ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_occurrence, table_keys=%str(occurrence_id), out_table=work.md_occurrence);
 data &tmplib..md_occurrence_tmp               ;
     set work.md_occurrence;
  if end_tm ne . then end_tm = tzoneu2s(end_tm,&timeZone_Value.);if start_tm ne . then start_tm = tzoneu2s(start_tm,&timeZone_Value.) ;
  if occurrence_id='' then occurrence_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_occurrence_tmp               , md_occurrence);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_occurrence using &tmpdbschema..md_occurrence_tmp               
         ON (md_occurrence.occurrence_id=md_occurrence_tmp.occurrence_id)
        WHEN MATCHED THEN  
        UPDATE SET end_tm = md_occurrence_tmp.end_tm , execution_status_cd = md_occurrence_tmp.execution_status_cd , object_id = md_occurrence_tmp.object_id , object_type_nm = md_occurrence_tmp.object_type_nm , object_version_id = md_occurrence_tmp.object_version_id , occurrence_no = md_occurrence_tmp.occurrence_no , occurrence_type_nm = md_occurrence_tmp.occurrence_type_nm , properties_map_doc = md_occurrence_tmp.properties_map_doc , start_tm = md_occurrence_tmp.start_tm , started_by_nm = md_occurrence_tmp.started_by_nm
        WHEN NOT MATCHED THEN INSERT ( 
        end_tm,execution_status_cd,object_id,object_type_nm,object_version_id,occurrence_id,occurrence_no,occurrence_type_nm,properties_map_doc,start_tm,started_by_nm
         ) values ( 
        md_occurrence_tmp.end_tm,md_occurrence_tmp.execution_status_cd,md_occurrence_tmp.object_id,md_occurrence_tmp.object_type_nm,md_occurrence_tmp.object_version_id,md_occurrence_tmp.occurrence_id,md_occurrence_tmp.occurrence_no,md_occurrence_tmp.occurrence_type_nm,md_occurrence_tmp.properties_map_doc,md_occurrence_tmp.start_tm,md_occurrence_tmp.started_by_nm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_occurrence_tmp               , md_occurrence, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_occurrence_tmp               ;
    QUIT;
    %put ######## Staging table: md_occurrence_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_occurrence;
      DROP TABLE work.md_occurrence;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_occurrence;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_picklist) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_picklist_tmp                 ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_picklist_tmp                 ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_picklist, table_keys=%str(attr_id,plist_id), out_table=work.md_picklist);
 data &tmplib..md_picklist_tmp                 ;
     set work.md_picklist;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if attr_id='' then attr_id='-'; if plist_id='' then plist_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_picklist_tmp                 , md_picklist);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_picklist using &tmpdbschema..md_picklist_tmp                 
         ON (md_picklist.attr_id=md_picklist_tmp.attr_id and md_picklist.plist_id=md_picklist_tmp.plist_id)
        WHEN MATCHED THEN  
        UPDATE SET attr_cd = md_picklist_tmp.attr_cd , attr_group_id = md_picklist_tmp.attr_group_id , attr_group_nm = md_picklist_tmp.attr_group_nm , attr_nm = md_picklist_tmp.attr_nm , created_by_usernm = md_picklist_tmp.created_by_usernm , created_dttm = md_picklist_tmp.created_dttm , is_obsolete_flg = md_picklist_tmp.is_obsolete_flg , last_modified_dttm = md_picklist_tmp.last_modified_dttm , last_modified_usernm = md_picklist_tmp.last_modified_usernm , load_dttm = md_picklist_tmp.load_dttm , plist_cd = md_picklist_tmp.plist_cd , plist_desc = md_picklist_tmp.plist_desc , plist_nm = md_picklist_tmp.plist_nm , plist_val = md_picklist_tmp.plist_val
        WHEN NOT MATCHED THEN INSERT ( 
        attr_cd,attr_group_id,attr_group_nm,attr_id,attr_nm,created_by_usernm,created_dttm,is_obsolete_flg,last_modified_dttm,last_modified_usernm,load_dttm,plist_cd,plist_desc,plist_id,plist_nm,plist_val
         ) values ( 
        md_picklist_tmp.attr_cd,md_picklist_tmp.attr_group_id,md_picklist_tmp.attr_group_nm,md_picklist_tmp.attr_id,md_picklist_tmp.attr_nm,md_picklist_tmp.created_by_usernm,md_picklist_tmp.created_dttm,md_picklist_tmp.is_obsolete_flg,md_picklist_tmp.last_modified_dttm,md_picklist_tmp.last_modified_usernm,md_picklist_tmp.load_dttm,md_picklist_tmp.plist_cd,md_picklist_tmp.plist_desc,md_picklist_tmp.plist_id,md_picklist_tmp.plist_nm,md_picklist_tmp.plist_val
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_picklist_tmp                 , md_picklist, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_picklist_tmp                 ;
    QUIT;
    %put ######## Staging table: md_picklist_tmp                  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_picklist;
      DROP TABLE work.md_picklist;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_picklist;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_purpose) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_purpose_tmp                  ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_purpose_tmp                  ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_purpose, table_keys=%str(purpose_id), out_table=work.md_purpose);
 data &tmplib..md_purpose_tmp                  ;
     set work.md_purpose;
  if purpose_id='' then purpose_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_purpose_tmp                  , md_purpose);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_purpose using &tmpdbschema..md_purpose_tmp                  
         ON (md_purpose.purpose_id=md_purpose_tmp.purpose_id)
        WHEN MATCHED THEN  
        UPDATE SET purpose_nm = md_purpose_tmp.purpose_nm
        WHEN NOT MATCHED THEN INSERT ( 
        purpose_id,purpose_nm
         ) values ( 
        md_purpose_tmp.purpose_id,md_purpose_tmp.purpose_nm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_purpose_tmp                  , md_purpose, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_purpose_tmp                  ;
    QUIT;
    %put ######## Staging table: md_purpose_tmp                   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_purpose;
      DROP TABLE work.md_purpose;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_purpose;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_rtc) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_rtc_tmp                      ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_rtc_tmp                      ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_rtc, table_keys=%str(rtc_id), out_table=work.md_rtc);
 data &tmplib..md_rtc_tmp                      ;
     set work.md_rtc;
  if rtc_dttm ne . then rtc_dttm = tzoneu2s(rtc_dttm,&timeZone_Value.) ;
  if rtc_id='' then rtc_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_rtc_tmp                      , md_rtc);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_rtc using &tmpdbschema..md_rtc_tmp                      
         ON (md_rtc.rtc_id=md_rtc_tmp.rtc_id)
        WHEN MATCHED THEN  
        UPDATE SET content_map_doc = md_rtc_tmp.content_map_doc , occurrence_id = md_rtc_tmp.occurrence_id , occurrence_no = md_rtc_tmp.occurrence_no , rtc_dttm = md_rtc_tmp.rtc_dttm , segment_id = md_rtc_tmp.segment_id , segment_version_id = md_rtc_tmp.segment_version_id , task_id = md_rtc_tmp.task_id , task_version_id = md_rtc_tmp.task_version_id
        WHEN NOT MATCHED THEN INSERT ( 
        content_map_doc,occurrence_id,occurrence_no,rtc_dttm,rtc_id,segment_id,segment_version_id,task_id,task_version_id
         ) values ( 
        md_rtc_tmp.content_map_doc,md_rtc_tmp.occurrence_id,md_rtc_tmp.occurrence_no,md_rtc_tmp.rtc_dttm,md_rtc_tmp.rtc_id,md_rtc_tmp.segment_id,md_rtc_tmp.segment_version_id,md_rtc_tmp.task_id,md_rtc_tmp.task_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_rtc_tmp                      , md_rtc, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_rtc_tmp                      ;
    QUIT;
    %put ######## Staging table: md_rtc_tmp                       Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_rtc;
      DROP TABLE work.md_rtc;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_rtc;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_segment) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_segment_tmp                  ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_segment_tmp                  ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_segment, table_keys=%str(segment_version_id), out_table=work.md_segment);
 data &tmplib..md_segment_tmp                  ;
     set work.md_segment;
  if last_published_dttm ne . then last_published_dttm = tzoneu2s(last_published_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if segment_version_id='' then segment_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_segment_tmp                  , md_segment);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_segment using &tmpdbschema..md_segment_tmp                  
         ON (md_segment.segment_version_id=md_segment_tmp.segment_version_id)
        WHEN MATCHED THEN  
        UPDATE SET business_context_id = md_segment_tmp.business_context_id , created_user_nm = md_segment_tmp.created_user_nm , folder_path_nm = md_segment_tmp.folder_path_nm , last_published_dttm = md_segment_tmp.last_published_dttm , owner_nm = md_segment_tmp.owner_nm , segment_category_nm = md_segment_tmp.segment_category_nm , segment_cd = md_segment_tmp.segment_cd , segment_desc = md_segment_tmp.segment_desc , segment_id = md_segment_tmp.segment_id , segment_map_id = md_segment_tmp.segment_map_id , segment_nm = md_segment_tmp.segment_nm , segment_src_cd = md_segment_tmp.segment_src_cd , segment_status_cd = md_segment_tmp.segment_status_cd , valid_from_dttm = md_segment_tmp.valid_from_dttm , valid_to_dttm = md_segment_tmp.valid_to_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        business_context_id,created_user_nm,folder_path_nm,last_published_dttm,owner_nm,segment_category_nm,segment_cd,segment_desc,segment_id,segment_map_id,segment_nm,segment_src_cd,segment_status_cd,segment_version_id,valid_from_dttm,valid_to_dttm
         ) values ( 
        md_segment_tmp.business_context_id,md_segment_tmp.created_user_nm,md_segment_tmp.folder_path_nm,md_segment_tmp.last_published_dttm,md_segment_tmp.owner_nm,md_segment_tmp.segment_category_nm,md_segment_tmp.segment_cd,md_segment_tmp.segment_desc,md_segment_tmp.segment_id,md_segment_tmp.segment_map_id,md_segment_tmp.segment_nm,md_segment_tmp.segment_src_cd,md_segment_tmp.segment_status_cd,md_segment_tmp.segment_version_id,md_segment_tmp.valid_from_dttm,md_segment_tmp.valid_to_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_segment_tmp                  , md_segment, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_segment_tmp                  ;
    QUIT;
    %put ######## Staging table: md_segment_tmp                   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_segment;
      DROP TABLE work.md_segment;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_segment;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_segment_custom_prop) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_segment_custom_prop_tmp      ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_segment_custom_prop_tmp      ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_segment_custom_prop, table_keys=%str(property_datatype_cd,property_nm,property_val,segment_version_id), out_table=work.md_segment_custom_prop);
 data &tmplib..md_segment_custom_prop_tmp      ;
     set work.md_segment_custom_prop;
  if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  Hashed_pk_col = put(sha256(catx('|',property_datatype_cd,property_nm,property_val,segment_version_id)), $hex64.);
  if property_datatype_cd='' then property_datatype_cd='-'; if property_nm='' then property_nm='-'; if property_val='' then property_val='-'; if segment_version_id='' then segment_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_segment_custom_prop_tmp      , md_segment_custom_prop);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_segment_custom_prop using &tmpdbschema..md_segment_custom_prop_tmp      
         ON (md_segment_custom_prop.Hashed_pk_col = md_segment_custom_prop_tmp.Hashed_pk_col)
        WHEN MATCHED THEN  
        UPDATE SET segment_id = md_segment_custom_prop_tmp.segment_id , segment_status_cd = md_segment_custom_prop_tmp.segment_status_cd , valid_from_dttm = md_segment_custom_prop_tmp.valid_from_dttm , valid_to_dttm = md_segment_custom_prop_tmp.valid_to_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        property_datatype_cd,property_nm,property_val,segment_id,segment_status_cd,segment_version_id,valid_from_dttm,valid_to_dttm
        ,Hashed_pk_col ) VALUES ( 
        md_segment_custom_prop_tmp.property_datatype_cd,md_segment_custom_prop_tmp.property_nm,md_segment_custom_prop_tmp.property_val,md_segment_custom_prop_tmp.segment_id,md_segment_custom_prop_tmp.segment_status_cd,md_segment_custom_prop_tmp.segment_version_id,md_segment_custom_prop_tmp.valid_from_dttm,md_segment_custom_prop_tmp.valid_to_dttm,md_segment_custom_prop_tmp.Hashed_pk_col
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_segment_custom_prop_tmp      , md_segment_custom_prop, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_segment_custom_prop_tmp      ;
    QUIT;
    %put ######## Staging table: md_segment_custom_prop_tmp       Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_segment_custom_prop;
      DROP TABLE work.md_segment_custom_prop;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_segment_custom_prop;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_segment_map) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_segment_map_tmp              ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_segment_map_tmp              ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_segment_map, table_keys=%str(segment_map_version_id), out_table=work.md_segment_map);
 data &tmplib..md_segment_map_tmp              ;
     set work.md_segment_map;
  if last_published_dttm ne . then last_published_dttm = tzoneu2s(last_published_dttm,&timeZone_Value.);if rec_scheduled_end_dttm ne . then rec_scheduled_end_dttm = tzoneu2s(rec_scheduled_end_dttm,&timeZone_Value.);if rec_scheduled_start_dttm ne . then rec_scheduled_start_dttm = tzoneu2s(rec_scheduled_start_dttm,&timeZone_Value.);if scheduled_end_dttm ne . then scheduled_end_dttm = tzoneu2s(scheduled_end_dttm,&timeZone_Value.);if scheduled_start_dttm ne . then scheduled_start_dttm = tzoneu2s(scheduled_start_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if segment_map_version_id='' then segment_map_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_segment_map_tmp              , md_segment_map);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_segment_map using &tmpdbschema..md_segment_map_tmp              
         ON (md_segment_map.segment_map_version_id=md_segment_map_tmp.segment_map_version_id)
        WHEN MATCHED THEN  
        UPDATE SET business_context_id = md_segment_map_tmp.business_context_id , created_user_nm = md_segment_map_tmp.created_user_nm , folder_path_nm = md_segment_map_tmp.folder_path_nm , last_published_dttm = md_segment_map_tmp.last_published_dttm , owner_nm = md_segment_map_tmp.owner_nm , rec_scheduled_end_dttm = md_segment_map_tmp.rec_scheduled_end_dttm , rec_scheduled_start_dttm = md_segment_map_tmp.rec_scheduled_start_dttm , rec_scheduled_start_tm = md_segment_map_tmp.rec_scheduled_start_tm , recurrence_day_of_month_no = md_segment_map_tmp.recurrence_day_of_month_no , recurrence_day_of_week_txt = md_segment_map_tmp.recurrence_day_of_week_txt , recurrence_day_of_wk_ordinal_no = md_segment_map_tmp.recurrence_day_of_wk_ordinal_no , recurrence_days_of_week_txt = md_segment_map_tmp.recurrence_days_of_week_txt , recurrence_frequency_cd = md_segment_map_tmp.recurrence_frequency_cd , recurrence_monthly_type_nm = md_segment_map_tmp.recurrence_monthly_type_nm , scheduled_end_dttm = md_segment_map_tmp.scheduled_end_dttm , scheduled_flg = md_segment_map_tmp.scheduled_flg , scheduled_start_dttm = md_segment_map_tmp.scheduled_start_dttm , segment_map_category_nm = md_segment_map_tmp.segment_map_category_nm , segment_map_cd = md_segment_map_tmp.segment_map_cd , segment_map_desc = md_segment_map_tmp.segment_map_desc , segment_map_id = md_segment_map_tmp.segment_map_id , segment_map_nm = md_segment_map_tmp.segment_map_nm , segment_map_src_cd = md_segment_map_tmp.segment_map_src_cd , segment_map_status_cd = md_segment_map_tmp.segment_map_status_cd , valid_from_dttm = md_segment_map_tmp.valid_from_dttm , valid_to_dttm = md_segment_map_tmp.valid_to_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        business_context_id,created_user_nm,folder_path_nm,last_published_dttm,owner_nm,rec_scheduled_end_dttm,rec_scheduled_start_dttm,rec_scheduled_start_tm,recurrence_day_of_month_no,recurrence_day_of_week_txt,recurrence_day_of_wk_ordinal_no,recurrence_days_of_week_txt,recurrence_frequency_cd,recurrence_monthly_type_nm,scheduled_end_dttm,scheduled_flg,scheduled_start_dttm,segment_map_category_nm,segment_map_cd,segment_map_desc,segment_map_id,segment_map_nm,segment_map_src_cd,segment_map_status_cd,segment_map_version_id,valid_from_dttm,valid_to_dttm
         ) values ( 
        md_segment_map_tmp.business_context_id,md_segment_map_tmp.created_user_nm,md_segment_map_tmp.folder_path_nm,md_segment_map_tmp.last_published_dttm,md_segment_map_tmp.owner_nm,md_segment_map_tmp.rec_scheduled_end_dttm,md_segment_map_tmp.rec_scheduled_start_dttm,md_segment_map_tmp.rec_scheduled_start_tm,md_segment_map_tmp.recurrence_day_of_month_no,md_segment_map_tmp.recurrence_day_of_week_txt,md_segment_map_tmp.recurrence_day_of_wk_ordinal_no,md_segment_map_tmp.recurrence_days_of_week_txt,md_segment_map_tmp.recurrence_frequency_cd,md_segment_map_tmp.recurrence_monthly_type_nm,md_segment_map_tmp.scheduled_end_dttm,md_segment_map_tmp.scheduled_flg,md_segment_map_tmp.scheduled_start_dttm,md_segment_map_tmp.segment_map_category_nm,md_segment_map_tmp.segment_map_cd,md_segment_map_tmp.segment_map_desc,md_segment_map_tmp.segment_map_id,md_segment_map_tmp.segment_map_nm,md_segment_map_tmp.segment_map_src_cd,md_segment_map_tmp.segment_map_status_cd,md_segment_map_tmp.segment_map_version_id,md_segment_map_tmp.valid_from_dttm,md_segment_map_tmp.valid_to_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_segment_map_tmp              , md_segment_map, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_segment_map_tmp              ;
    QUIT;
    %put ######## Staging table: md_segment_map_tmp               Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_segment_map;
      DROP TABLE work.md_segment_map;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_segment_map;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_segment_map_custom_prop) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_segment_map_custom_prop_tmp  ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_segment_map_custom_prop_tmp  ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_segment_map_custom_prop, table_keys=%str(property_datatype_cd,property_nm,property_val,segment_map_version_id), out_table=work.md_segment_map_custom_prop);
 data &tmplib..md_segment_map_custom_prop_tmp  ;
     set work.md_segment_map_custom_prop;
  if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  Hashed_pk_col = put(sha256(catx('|',property_datatype_cd,property_nm,property_val,segment_map_version_id)), $hex64.);
  if property_datatype_cd='' then property_datatype_cd='-'; if property_nm='' then property_nm='-'; if property_val='' then property_val='-'; if segment_map_version_id='' then segment_map_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_segment_map_custom_prop_tmp  , md_segment_map_custom_prop);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_segment_map_custom_prop using &tmpdbschema..md_segment_map_custom_prop_tmp  
         ON (md_segment_map_custom_prop.Hashed_pk_col = md_segment_map_custom_prop_tmp.Hashed_pk_col)
        WHEN MATCHED THEN  
        UPDATE SET segment_map_id = md_segment_map_custom_prop_tmp.segment_map_id , segment_map_status_cd = md_segment_map_custom_prop_tmp.segment_map_status_cd , valid_from_dttm = md_segment_map_custom_prop_tmp.valid_from_dttm , valid_to_dttm = md_segment_map_custom_prop_tmp.valid_to_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        property_datatype_cd,property_nm,property_val,segment_map_id,segment_map_status_cd,segment_map_version_id,valid_from_dttm,valid_to_dttm
        ,Hashed_pk_col ) VALUES ( 
        md_segment_map_custom_prop_tmp.property_datatype_cd,md_segment_map_custom_prop_tmp.property_nm,md_segment_map_custom_prop_tmp.property_val,md_segment_map_custom_prop_tmp.segment_map_id,md_segment_map_custom_prop_tmp.segment_map_status_cd,md_segment_map_custom_prop_tmp.segment_map_version_id,md_segment_map_custom_prop_tmp.valid_from_dttm,md_segment_map_custom_prop_tmp.valid_to_dttm,md_segment_map_custom_prop_tmp.Hashed_pk_col
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_segment_map_custom_prop_tmp  , md_segment_map_custom_prop, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_segment_map_custom_prop_tmp  ;
    QUIT;
    %put ######## Staging table: md_segment_map_custom_prop_tmp   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_segment_map_custom_prop;
      DROP TABLE work.md_segment_map_custom_prop;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_segment_map_custom_prop;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_segment_map_x_segment) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_segment_map_x_segment_tmp    ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_segment_map_x_segment_tmp    ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_segment_map_x_segment, table_keys=%str(segment_id,segment_map_version_id), out_table=work.md_segment_map_x_segment);
 data &tmplib..md_segment_map_x_segment_tmp    ;
     set work.md_segment_map_x_segment;
  if segment_id='' then segment_id='-'; if segment_map_version_id='' then segment_map_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_segment_map_x_segment_tmp    , md_segment_map_x_segment);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_segment_map_x_segment using &tmpdbschema..md_segment_map_x_segment_tmp    
         ON (md_segment_map_x_segment.segment_id=md_segment_map_x_segment_tmp.segment_id and md_segment_map_x_segment.segment_map_version_id=md_segment_map_x_segment_tmp.segment_map_version_id)
        WHEN MATCHED THEN  
        UPDATE SET segment_map_id = md_segment_map_x_segment_tmp.segment_map_id , segment_map_status_cd = md_segment_map_x_segment_tmp.segment_map_status_cd , segment_version_id = md_segment_map_x_segment_tmp.segment_version_id
        WHEN NOT MATCHED THEN INSERT ( 
        segment_id,segment_map_id,segment_map_status_cd,segment_map_version_id,segment_version_id
         ) values ( 
        md_segment_map_x_segment_tmp.segment_id,md_segment_map_x_segment_tmp.segment_map_id,md_segment_map_x_segment_tmp.segment_map_status_cd,md_segment_map_x_segment_tmp.segment_map_version_id,md_segment_map_x_segment_tmp.segment_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_segment_map_x_segment_tmp    , md_segment_map_x_segment, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_segment_map_x_segment_tmp    ;
    QUIT;
    %put ######## Staging table: md_segment_map_x_segment_tmp     Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_segment_map_x_segment;
      DROP TABLE work.md_segment_map_x_segment;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_segment_map_x_segment;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_segment_test) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_segment_test_tmp             ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_segment_test_tmp             ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_segment_test, table_keys=%str(task_version_id,test_cd), out_table=work.md_segment_test);
 data &tmplib..md_segment_test_tmp             ;
     set work.md_segment_test;
  if task_version_id='' then task_version_id='-'; if test_cd='' then test_cd='-';
 run;
 %ErrCheck (Failed to Append Data to :md_segment_test_tmp             , md_segment_test);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_segment_test using &tmpdbschema..md_segment_test_tmp             
         ON (md_segment_test.task_version_id=md_segment_test_tmp.task_version_id and md_segment_test.test_cd=md_segment_test_tmp.test_cd)
        WHEN MATCHED THEN  
        UPDATE SET stratified_samp_criteria_txt = md_segment_test_tmp.stratified_samp_criteria_txt , stratified_sampling_flg = md_segment_test_tmp.stratified_sampling_flg , task_id = md_segment_test_tmp.task_id , test_cnt = md_segment_test_tmp.test_cnt , test_enabled_flg = md_segment_test_tmp.test_enabled_flg , test_nm = md_segment_test_tmp.test_nm , test_pct = md_segment_test_tmp.test_pct , test_sizing_type_nm = md_segment_test_tmp.test_sizing_type_nm , test_type_nm = md_segment_test_tmp.test_type_nm
        WHEN NOT MATCHED THEN INSERT ( 
        stratified_samp_criteria_txt,stratified_sampling_flg,task_id,task_version_id,test_cd,test_cnt,test_enabled_flg,test_nm,test_pct,test_sizing_type_nm,test_type_nm
         ) values ( 
        md_segment_test_tmp.stratified_samp_criteria_txt,md_segment_test_tmp.stratified_sampling_flg,md_segment_test_tmp.task_id,md_segment_test_tmp.task_version_id,md_segment_test_tmp.test_cd,md_segment_test_tmp.test_cnt,md_segment_test_tmp.test_enabled_flg,md_segment_test_tmp.test_nm,md_segment_test_tmp.test_pct,md_segment_test_tmp.test_sizing_type_nm,md_segment_test_tmp.test_type_nm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_segment_test_tmp             , md_segment_test, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_segment_test_tmp             ;
    QUIT;
    %put ######## Staging table: md_segment_test_tmp              Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_segment_test;
      DROP TABLE work.md_segment_test;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_segment_test;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_segment_test_x_segment) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_segment_test_x_segment_tmp   ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_segment_test_x_segment_tmp   ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_segment_test_x_segment, table_keys=%str(segment_id,task_version_id,test_cd), out_table=work.md_segment_test_x_segment);
 data &tmplib..md_segment_test_x_segment_tmp   ;
     set work.md_segment_test_x_segment;
  if segment_id='' then segment_id='-'; if task_version_id='' then task_version_id='-'; if test_cd='' then test_cd='-';
 run;
 %ErrCheck (Failed to Append Data to :md_segment_test_x_segment_tmp   , md_segment_test_x_segment);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_segment_test_x_segment using &tmpdbschema..md_segment_test_x_segment_tmp   
         ON (md_segment_test_x_segment.segment_id=md_segment_test_x_segment_tmp.segment_id and md_segment_test_x_segment.task_version_id=md_segment_test_x_segment_tmp.task_version_id and md_segment_test_x_segment.test_cd=md_segment_test_x_segment_tmp.test_cd)
        WHEN MATCHED THEN  
        UPDATE SET task_id = md_segment_test_x_segment_tmp.task_id
        WHEN NOT MATCHED THEN INSERT ( 
        segment_id,task_id,task_version_id,test_cd
         ) values ( 
        md_segment_test_x_segment_tmp.segment_id,md_segment_test_x_segment_tmp.task_id,md_segment_test_x_segment_tmp.task_version_id,md_segment_test_x_segment_tmp.test_cd
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_segment_test_x_segment_tmp   , md_segment_test_x_segment, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_segment_test_x_segment_tmp   ;
    QUIT;
    %put ######## Staging table: md_segment_test_x_segment_tmp    Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_segment_test_x_segment;
      DROP TABLE work.md_segment_test_x_segment;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_segment_test_x_segment;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_segment_x_event) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_segment_x_event_tmp          ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_segment_x_event_tmp          ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_segment_x_event, table_keys=%str(event_id,segment_version_id), out_table=work.md_segment_x_event);
 data &tmplib..md_segment_x_event_tmp          ;
     set work.md_segment_x_event;
  if event_id='' then event_id='-'; if segment_version_id='' then segment_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_segment_x_event_tmp          , md_segment_x_event);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_segment_x_event using &tmpdbschema..md_segment_x_event_tmp          
         ON (md_segment_x_event.event_id=md_segment_x_event_tmp.event_id and md_segment_x_event.segment_version_id=md_segment_x_event_tmp.segment_version_id)
        WHEN MATCHED THEN  
        UPDATE SET segment_id = md_segment_x_event_tmp.segment_id , segment_status_cd = md_segment_x_event_tmp.segment_status_cd
        WHEN NOT MATCHED THEN INSERT ( 
        event_id,segment_id,segment_status_cd,segment_version_id
         ) values ( 
        md_segment_x_event_tmp.event_id,md_segment_x_event_tmp.segment_id,md_segment_x_event_tmp.segment_status_cd,md_segment_x_event_tmp.segment_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_segment_x_event_tmp          , md_segment_x_event, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_segment_x_event_tmp          ;
    QUIT;
    %put ######## Staging table: md_segment_x_event_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_segment_x_event;
      DROP TABLE work.md_segment_x_event;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_segment_x_event;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_spot) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_spot_tmp                     ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_spot_tmp                     ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_spot, table_keys=%str(spot_version_id), out_table=work.md_spot);
 data &tmplib..md_spot_tmp                     ;
     set work.md_spot;
  if last_published_dttm ne . then last_published_dttm = tzoneu2s(last_published_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if spot_version_id='' then spot_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_spot_tmp                     , md_spot);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_spot using &tmpdbschema..md_spot_tmp                     
         ON (md_spot.spot_version_id=md_spot_tmp.spot_version_id)
        WHEN MATCHED THEN  
        UPDATE SET channel_nm = md_spot_tmp.channel_nm , created_user_nm = md_spot_tmp.created_user_nm , dimension_label_txt = md_spot_tmp.dimension_label_txt , height_width_ratio_val_txt = md_spot_tmp.height_width_ratio_val_txt , last_published_dttm = md_spot_tmp.last_published_dttm , location_selector_flg = md_spot_tmp.location_selector_flg , multi_page_flg = md_spot_tmp.multi_page_flg , owner_nm = md_spot_tmp.owner_nm , spot_desc = md_spot_tmp.spot_desc , spot_height_val_no = md_spot_tmp.spot_height_val_no , spot_id = md_spot_tmp.spot_id , spot_key = md_spot_tmp.spot_key , spot_nm = md_spot_tmp.spot_nm , spot_status_cd = md_spot_tmp.spot_status_cd , spot_type_nm = md_spot_tmp.spot_type_nm , spot_width_val_no = md_spot_tmp.spot_width_val_no , valid_from_dttm = md_spot_tmp.valid_from_dttm , valid_to_dttm = md_spot_tmp.valid_to_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        channel_nm,created_user_nm,dimension_label_txt,height_width_ratio_val_txt,last_published_dttm,location_selector_flg,multi_page_flg,owner_nm,spot_desc,spot_height_val_no,spot_id,spot_key,spot_nm,spot_status_cd,spot_type_nm,spot_version_id,spot_width_val_no,valid_from_dttm,valid_to_dttm
         ) values ( 
        md_spot_tmp.channel_nm,md_spot_tmp.created_user_nm,md_spot_tmp.dimension_label_txt,md_spot_tmp.height_width_ratio_val_txt,md_spot_tmp.last_published_dttm,md_spot_tmp.location_selector_flg,md_spot_tmp.multi_page_flg,md_spot_tmp.owner_nm,md_spot_tmp.spot_desc,md_spot_tmp.spot_height_val_no,md_spot_tmp.spot_id,md_spot_tmp.spot_key,md_spot_tmp.spot_nm,md_spot_tmp.spot_status_cd,md_spot_tmp.spot_type_nm,md_spot_tmp.spot_version_id,md_spot_tmp.spot_width_val_no,md_spot_tmp.valid_from_dttm,md_spot_tmp.valid_to_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_spot_tmp                     , md_spot, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_spot_tmp                     ;
    QUIT;
    %put ######## Staging table: md_spot_tmp                      Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_spot;
      DROP TABLE work.md_spot;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_spot;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_target_assist) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_target_assist_tmp            ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_target_assist_tmp            ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_target_assist, table_keys=%str(task_id), out_table=work.md_target_assist);
 data &tmplib..md_target_assist_tmp            ;
     set work.md_target_assist;
  if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if model_available_dttm ne . then model_available_dttm = tzoneu2s(model_available_dttm,&timeZone_Value.) ;
  if task_id='' then task_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_target_assist_tmp            , md_target_assist);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_target_assist using &tmpdbschema..md_target_assist_tmp            
         ON (md_target_assist.task_id=md_target_assist_tmp.task_id)
        WHEN MATCHED THEN  
        UPDATE SET last_modified_dttm = md_target_assist_tmp.last_modified_dttm , model_available_dttm = md_target_assist_tmp.model_available_dttm , percent_target_population_size = md_target_assist_tmp.percent_target_population_size , threshold_type_nm = md_target_assist_tmp.threshold_type_nm , use_targeting_flg = md_target_assist_tmp.use_targeting_flg
        WHEN NOT MATCHED THEN INSERT ( 
        last_modified_dttm,model_available_dttm,percent_target_population_size,task_id,threshold_type_nm,use_targeting_flg
         ) values ( 
        md_target_assist_tmp.last_modified_dttm,md_target_assist_tmp.model_available_dttm,md_target_assist_tmp.percent_target_population_size,md_target_assist_tmp.task_id,md_target_assist_tmp.threshold_type_nm,md_target_assist_tmp.use_targeting_flg
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_target_assist_tmp            , md_target_assist, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_target_assist_tmp            ;
    QUIT;
    %put ######## Staging table: md_target_assist_tmp             Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_target_assist;
      DROP TABLE work.md_target_assist;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_target_assist;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_task) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_task_tmp                     ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_task_tmp                     ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_task, table_keys=%str(task_version_id), out_table=work.md_task);
 data &tmplib..md_task_tmp                     ;
     set work.md_task;
  if last_published_dttm ne . then last_published_dttm = tzoneu2s(last_published_dttm,&timeZone_Value.);if model_start_dttm ne . then model_start_dttm = tzoneu2s(model_start_dttm,&timeZone_Value.);if rec_scheduled_end_dttm ne . then rec_scheduled_end_dttm = tzoneu2s(rec_scheduled_end_dttm,&timeZone_Value.);if rec_scheduled_start_dttm ne . then rec_scheduled_start_dttm = tzoneu2s(rec_scheduled_start_dttm,&timeZone_Value.);if scheduled_end_dttm ne . then scheduled_end_dttm = tzoneu2s(scheduled_end_dttm,&timeZone_Value.);if scheduled_start_dttm ne . then scheduled_start_dttm = tzoneu2s(scheduled_start_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if task_version_id='' then task_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_task_tmp                     , md_task);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_task using &tmpdbschema..md_task_tmp                     
         ON (md_task.task_version_id=md_task_tmp.task_version_id)
        WHEN MATCHED THEN  
        UPDATE SET activity_flg = md_task_tmp.activity_flg , arbitration_method_cd = md_task_tmp.arbitration_method_cd , business_context_id = md_task_tmp.business_context_id , channel_nm = md_task_tmp.channel_nm , control_group_action_nm = md_task_tmp.control_group_action_nm , created_user_nm = md_task_tmp.created_user_nm , delivery_config_type_nm = md_task_tmp.delivery_config_type_nm , display_priority_no = md_task_tmp.display_priority_no , export_template_flg = md_task_tmp.export_template_flg , folder_path_nm = md_task_tmp.folder_path_nm , impressions_life_time_cnt = md_task_tmp.impressions_life_time_cnt , impressions_per_session_cnt = md_task_tmp.impressions_per_session_cnt , impressions_qty_period_cnt = md_task_tmp.impressions_qty_period_cnt , last_published_dttm = md_task_tmp.last_published_dttm , limit_period_unit_cnt = md_task_tmp.limit_period_unit_cnt , maximum_period_expression_cnt = md_task_tmp.maximum_period_expression_cnt , mobile_app_id = md_task_tmp.mobile_app_id , mobile_app_nm = md_task_tmp.mobile_app_nm , model_start_dttm = md_task_tmp.model_start_dttm , owner_nm = md_task_tmp.owner_nm , period_type_nm = md_task_tmp.period_type_nm , rec_scheduled_end_dttm = md_task_tmp.rec_scheduled_end_dttm , rec_scheduled_start_dttm = md_task_tmp.rec_scheduled_start_dttm , rec_scheduled_start_tm = md_task_tmp.rec_scheduled_start_tm , recurrence_day_of_month_no = md_task_tmp.recurrence_day_of_month_no , recurrence_day_of_week_txt = md_task_tmp.recurrence_day_of_week_txt , recurrence_day_of_wk_ordinal_no = md_task_tmp.recurrence_day_of_wk_ordinal_no , recurrence_days_of_week_txt = md_task_tmp.recurrence_days_of_week_txt , recurrence_frequency_cd = md_task_tmp.recurrence_frequency_cd , recurrence_monthly_type_nm = md_task_tmp.recurrence_monthly_type_nm , recurring_schedule_flg = md_task_tmp.recurring_schedule_flg , rtdm_flg = md_task_tmp.rtdm_flg , scheduled_end_dttm = md_task_tmp.scheduled_end_dttm , scheduled_flg = md_task_tmp.scheduled_flg , scheduled_start_dttm = md_task_tmp.scheduled_start_dttm , secondary_status = md_task_tmp.secondary_status , segment_tests_flg = md_task_tmp.segment_tests_flg , send_notification_locale_cd = md_task_tmp.send_notification_locale_cd , stratified_sampling_action_nm = md_task_tmp.stratified_sampling_action_nm , subject_line_source_nm = md_task_tmp.subject_line_source_nm , subject_line_txt = md_task_tmp.subject_line_txt , task_category_nm = md_task_tmp.task_category_nm , task_cd = md_task_tmp.task_cd , task_delivery_type_nm = md_task_tmp.task_delivery_type_nm , task_desc = md_task_tmp.task_desc , task_id = md_task_tmp.task_id , task_nm = md_task_tmp.task_nm , task_status_cd = md_task_tmp.task_status_cd , task_subtype_nm = md_task_tmp.task_subtype_nm , task_type_nm = md_task_tmp.task_type_nm , template_id = md_task_tmp.template_id , test_duration = md_task_tmp.test_duration , use_modeling_flg = md_task_tmp.use_modeling_flg , valid_from_dttm = md_task_tmp.valid_from_dttm , valid_to_dttm = md_task_tmp.valid_to_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        activity_flg,arbitration_method_cd,business_context_id,channel_nm,control_group_action_nm,created_user_nm,delivery_config_type_nm,display_priority_no,export_template_flg,folder_path_nm,impressions_life_time_cnt,impressions_per_session_cnt,impressions_qty_period_cnt,last_published_dttm,limit_period_unit_cnt,maximum_period_expression_cnt,mobile_app_id,mobile_app_nm,model_start_dttm,owner_nm,period_type_nm,rec_scheduled_end_dttm,rec_scheduled_start_dttm,rec_scheduled_start_tm,recurrence_day_of_month_no,recurrence_day_of_week_txt,recurrence_day_of_wk_ordinal_no,recurrence_days_of_week_txt,recurrence_frequency_cd,recurrence_monthly_type_nm,recurring_schedule_flg,rtdm_flg,scheduled_end_dttm,scheduled_flg,scheduled_start_dttm,secondary_status,segment_tests_flg,send_notification_locale_cd,stratified_sampling_action_nm,subject_line_source_nm,subject_line_txt,task_category_nm,task_cd,task_delivery_type_nm,task_desc,task_id,task_nm,task_status_cd,task_subtype_nm,task_type_nm,task_version_id,template_id,test_duration,use_modeling_flg,valid_from_dttm,valid_to_dttm
         ) values ( 
        md_task_tmp.activity_flg,md_task_tmp.arbitration_method_cd,md_task_tmp.business_context_id,md_task_tmp.channel_nm,md_task_tmp.control_group_action_nm,md_task_tmp.created_user_nm,md_task_tmp.delivery_config_type_nm,md_task_tmp.display_priority_no,md_task_tmp.export_template_flg,md_task_tmp.folder_path_nm,md_task_tmp.impressions_life_time_cnt,md_task_tmp.impressions_per_session_cnt,md_task_tmp.impressions_qty_period_cnt,md_task_tmp.last_published_dttm,md_task_tmp.limit_period_unit_cnt,md_task_tmp.maximum_period_expression_cnt,md_task_tmp.mobile_app_id,md_task_tmp.mobile_app_nm,md_task_tmp.model_start_dttm,md_task_tmp.owner_nm,md_task_tmp.period_type_nm,md_task_tmp.rec_scheduled_end_dttm,md_task_tmp.rec_scheduled_start_dttm,md_task_tmp.rec_scheduled_start_tm,md_task_tmp.recurrence_day_of_month_no,md_task_tmp.recurrence_day_of_week_txt,md_task_tmp.recurrence_day_of_wk_ordinal_no,md_task_tmp.recurrence_days_of_week_txt,md_task_tmp.recurrence_frequency_cd,md_task_tmp.recurrence_monthly_type_nm,md_task_tmp.recurring_schedule_flg,md_task_tmp.rtdm_flg,md_task_tmp.scheduled_end_dttm,md_task_tmp.scheduled_flg,md_task_tmp.scheduled_start_dttm,md_task_tmp.secondary_status,md_task_tmp.segment_tests_flg,md_task_tmp.send_notification_locale_cd,md_task_tmp.stratified_sampling_action_nm,md_task_tmp.subject_line_source_nm,md_task_tmp.subject_line_txt,md_task_tmp.task_category_nm,md_task_tmp.task_cd,md_task_tmp.task_delivery_type_nm,md_task_tmp.task_desc,md_task_tmp.task_id,md_task_tmp.task_nm,md_task_tmp.task_status_cd,md_task_tmp.task_subtype_nm,md_task_tmp.task_type_nm,md_task_tmp.task_version_id,md_task_tmp.template_id,md_task_tmp.test_duration,md_task_tmp.use_modeling_flg,md_task_tmp.valid_from_dttm,md_task_tmp.valid_to_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_task_tmp                     , md_task, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_task_tmp                     ;
    QUIT;
    %put ######## Staging table: md_task_tmp                      Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_task;
      DROP TABLE work.md_task;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_task;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_task_custom_prop) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_task_custom_prop_tmp         ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_task_custom_prop_tmp         ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_task_custom_prop, table_keys=%str(property_datatype_nm,property_nm,property_val,task_version_id), out_table=work.md_task_custom_prop);
 data &tmplib..md_task_custom_prop_tmp         ;
     set work.md_task_custom_prop;
  if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  Hashed_pk_col = put(sha256(catx('|',property_datatype_nm,property_nm,property_val,task_version_id)), $hex64.);
  if property_datatype_nm='' then property_datatype_nm='-'; if property_nm='' then property_nm='-'; if property_val='' then property_val='-'; if task_version_id='' then task_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_task_custom_prop_tmp         , md_task_custom_prop);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_task_custom_prop using &tmpdbschema..md_task_custom_prop_tmp         
         ON (md_task_custom_prop.Hashed_pk_col = md_task_custom_prop_tmp.Hashed_pk_col)
        WHEN MATCHED THEN  
        UPDATE SET task_id = md_task_custom_prop_tmp.task_id , task_status_cd = md_task_custom_prop_tmp.task_status_cd , valid_from_dttm = md_task_custom_prop_tmp.valid_from_dttm , valid_to_dttm = md_task_custom_prop_tmp.valid_to_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        property_datatype_nm,property_nm,property_val,task_id,task_status_cd,task_version_id,valid_from_dttm,valid_to_dttm
        ,Hashed_pk_col ) VALUES ( 
        md_task_custom_prop_tmp.property_datatype_nm,md_task_custom_prop_tmp.property_nm,md_task_custom_prop_tmp.property_val,md_task_custom_prop_tmp.task_id,md_task_custom_prop_tmp.task_status_cd,md_task_custom_prop_tmp.task_version_id,md_task_custom_prop_tmp.valid_from_dttm,md_task_custom_prop_tmp.valid_to_dttm,md_task_custom_prop_tmp.Hashed_pk_col
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_task_custom_prop_tmp         , md_task_custom_prop, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_task_custom_prop_tmp         ;
    QUIT;
    %put ######## Staging table: md_task_custom_prop_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_task_custom_prop;
      DROP TABLE work.md_task_custom_prop;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_task_custom_prop;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_task_x_audience) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_task_x_audience_tmp          ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_task_x_audience_tmp          ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_task_x_audience, table_keys=%str(audience_id,task_id), out_table=work.md_task_x_audience);
 data &tmplib..md_task_x_audience_tmp          ;
     set work.md_task_x_audience;
  if audience_id='' then audience_id='-'; if task_id='' then task_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_task_x_audience_tmp          , md_task_x_audience);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_task_x_audience using &tmpdbschema..md_task_x_audience_tmp          
         ON (md_task_x_audience.audience_id=md_task_x_audience_tmp.audience_id and md_task_x_audience.task_id=md_task_x_audience_tmp.task_id)
        WHEN NOT MATCHED THEN INSERT ( 
        audience_id,task_id
         ) values ( 
        md_task_x_audience_tmp.audience_id,md_task_x_audience_tmp.task_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_task_x_audience_tmp          , md_task_x_audience, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_task_x_audience_tmp          ;
    QUIT;
    %put ######## Staging table: md_task_x_audience_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_task_x_audience;
      DROP TABLE work.md_task_x_audience;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_task_x_audience;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_task_x_creative) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_task_x_creative_tmp          ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_task_x_creative_tmp          ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_task_x_creative, table_keys=%str(creative_id,spot_id,task_version_id), out_table=work.md_task_x_creative);
 data &tmplib..md_task_x_creative_tmp          ;
     set work.md_task_x_creative;
  if creative_id='' then creative_id='-'; if spot_id='' then spot_id='-'; if task_version_id='' then task_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_task_x_creative_tmp          , md_task_x_creative);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_task_x_creative using &tmpdbschema..md_task_x_creative_tmp          
         ON (md_task_x_creative.creative_id=md_task_x_creative_tmp.creative_id and md_task_x_creative.spot_id=md_task_x_creative_tmp.spot_id and md_task_x_creative.task_version_id=md_task_x_creative_tmp.task_version_id)
        WHEN MATCHED THEN  
        UPDATE SET arbitration_method_cd = md_task_x_creative_tmp.arbitration_method_cd , arbitration_method_val = md_task_x_creative_tmp.arbitration_method_val , task_id = md_task_x_creative_tmp.task_id , task_status_cd = md_task_x_creative_tmp.task_status_cd , variant_id = md_task_x_creative_tmp.variant_id , variant_nm = md_task_x_creative_tmp.variant_nm
        WHEN NOT MATCHED THEN INSERT ( 
        arbitration_method_cd,arbitration_method_val,creative_id,spot_id,task_id,task_status_cd,task_version_id,variant_id,variant_nm
         ) values ( 
        md_task_x_creative_tmp.arbitration_method_cd,md_task_x_creative_tmp.arbitration_method_val,md_task_x_creative_tmp.creative_id,md_task_x_creative_tmp.spot_id,md_task_x_creative_tmp.task_id,md_task_x_creative_tmp.task_status_cd,md_task_x_creative_tmp.task_version_id,md_task_x_creative_tmp.variant_id,md_task_x_creative_tmp.variant_nm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_task_x_creative_tmp          , md_task_x_creative, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_task_x_creative_tmp          ;
    QUIT;
    %put ######## Staging table: md_task_x_creative_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_task_x_creative;
      DROP TABLE work.md_task_x_creative;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_task_x_creative;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_task_x_dataview) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_task_x_dataview_tmp          ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_task_x_dataview_tmp          ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_task_x_dataview, table_keys=%str(dataview_id,task_version_id), out_table=work.md_task_x_dataview);
 data &tmplib..md_task_x_dataview_tmp          ;
     set work.md_task_x_dataview;
  if dataview_id='' then dataview_id='-'; if task_version_id='' then task_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_task_x_dataview_tmp          , md_task_x_dataview);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_task_x_dataview using &tmpdbschema..md_task_x_dataview_tmp          
         ON (md_task_x_dataview.dataview_id=md_task_x_dataview_tmp.dataview_id and md_task_x_dataview.task_version_id=md_task_x_dataview_tmp.task_version_id)
        WHEN MATCHED THEN  
        UPDATE SET primary_metric_flg = md_task_x_dataview_tmp.primary_metric_flg , secondary_metric_flg = md_task_x_dataview_tmp.secondary_metric_flg , targeting_flg = md_task_x_dataview_tmp.targeting_flg , task_id = md_task_x_dataview_tmp.task_id , task_status_cd = md_task_x_dataview_tmp.task_status_cd
        WHEN NOT MATCHED THEN INSERT ( 
        dataview_id,primary_metric_flg,secondary_metric_flg,targeting_flg,task_id,task_status_cd,task_version_id
         ) values ( 
        md_task_x_dataview_tmp.dataview_id,md_task_x_dataview_tmp.primary_metric_flg,md_task_x_dataview_tmp.secondary_metric_flg,md_task_x_dataview_tmp.targeting_flg,md_task_x_dataview_tmp.task_id,md_task_x_dataview_tmp.task_status_cd,md_task_x_dataview_tmp.task_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_task_x_dataview_tmp          , md_task_x_dataview, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_task_x_dataview_tmp          ;
    QUIT;
    %put ######## Staging table: md_task_x_dataview_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_task_x_dataview;
      DROP TABLE work.md_task_x_dataview;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_task_x_dataview;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_task_x_event) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_task_x_event_tmp             ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_task_x_event_tmp             ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_task_x_event, table_keys=%str(event_id,task_version_id), out_table=work.md_task_x_event);
 data &tmplib..md_task_x_event_tmp             ;
     set work.md_task_x_event;
  if event_id='' then event_id='-'; if task_version_id='' then task_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_task_x_event_tmp             , md_task_x_event);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_task_x_event using &tmpdbschema..md_task_x_event_tmp             
         ON (md_task_x_event.event_id=md_task_x_event_tmp.event_id and md_task_x_event.task_version_id=md_task_x_event_tmp.task_version_id)
        WHEN MATCHED THEN  
        UPDATE SET primary_metric_flg = md_task_x_event_tmp.primary_metric_flg , secondary_metric_flg = md_task_x_event_tmp.secondary_metric_flg , targeting_flg = md_task_x_event_tmp.targeting_flg , task_id = md_task_x_event_tmp.task_id , task_status_cd = md_task_x_event_tmp.task_status_cd
        WHEN NOT MATCHED THEN INSERT ( 
        event_id,primary_metric_flg,secondary_metric_flg,targeting_flg,task_id,task_status_cd,task_version_id
         ) values ( 
        md_task_x_event_tmp.event_id,md_task_x_event_tmp.primary_metric_flg,md_task_x_event_tmp.secondary_metric_flg,md_task_x_event_tmp.targeting_flg,md_task_x_event_tmp.task_id,md_task_x_event_tmp.task_status_cd,md_task_x_event_tmp.task_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_task_x_event_tmp             , md_task_x_event, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_task_x_event_tmp             ;
    QUIT;
    %put ######## Staging table: md_task_x_event_tmp              Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_task_x_event;
      DROP TABLE work.md_task_x_event;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_task_x_event;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_task_x_message) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_task_x_message_tmp           ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_task_x_message_tmp           ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_task_x_message, table_keys=%str(message_id,task_version_id), out_table=work.md_task_x_message);
 data &tmplib..md_task_x_message_tmp           ;
     set work.md_task_x_message;
  if message_id='' then message_id='-'; if task_version_id='' then task_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_task_x_message_tmp           , md_task_x_message);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_task_x_message using &tmpdbschema..md_task_x_message_tmp           
         ON (md_task_x_message.message_id=md_task_x_message_tmp.message_id and md_task_x_message.task_version_id=md_task_x_message_tmp.task_version_id)
        WHEN MATCHED THEN  
        UPDATE SET task_id = md_task_x_message_tmp.task_id , task_status_cd = md_task_x_message_tmp.task_status_cd
        WHEN NOT MATCHED THEN INSERT ( 
        message_id,task_id,task_status_cd,task_version_id
         ) values ( 
        md_task_x_message_tmp.message_id,md_task_x_message_tmp.task_id,md_task_x_message_tmp.task_status_cd,md_task_x_message_tmp.task_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_task_x_message_tmp           , md_task_x_message, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_task_x_message_tmp           ;
    QUIT;
    %put ######## Staging table: md_task_x_message_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_task_x_message;
      DROP TABLE work.md_task_x_message;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_task_x_message;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_task_x_segment) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_task_x_segment_tmp           ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_task_x_segment_tmp           ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_task_x_segment, table_keys=%str(segment_id,task_version_id), out_table=work.md_task_x_segment);
 data &tmplib..md_task_x_segment_tmp           ;
     set work.md_task_x_segment;
  if segment_id='' then segment_id='-'; if task_version_id='' then task_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_task_x_segment_tmp           , md_task_x_segment);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_task_x_segment using &tmpdbschema..md_task_x_segment_tmp           
         ON (md_task_x_segment.segment_id=md_task_x_segment_tmp.segment_id and md_task_x_segment.task_version_id=md_task_x_segment_tmp.task_version_id)
        WHEN MATCHED THEN  
        UPDATE SET task_id = md_task_x_segment_tmp.task_id , task_status_cd = md_task_x_segment_tmp.task_status_cd
        WHEN NOT MATCHED THEN INSERT ( 
        segment_id,task_id,task_status_cd,task_version_id
         ) values ( 
        md_task_x_segment_tmp.segment_id,md_task_x_segment_tmp.task_id,md_task_x_segment_tmp.task_status_cd,md_task_x_segment_tmp.task_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_task_x_segment_tmp           , md_task_x_segment, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_task_x_segment_tmp           ;
    QUIT;
    %put ######## Staging table: md_task_x_segment_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_task_x_segment;
      DROP TABLE work.md_task_x_segment;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_task_x_segment;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_task_x_spot) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_task_x_spot_tmp              ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_task_x_spot_tmp              ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_task_x_spot, table_keys=%str(spot_id,task_version_id), out_table=work.md_task_x_spot);
 data &tmplib..md_task_x_spot_tmp              ;
     set work.md_task_x_spot;
  if spot_id='' then spot_id='-'; if task_version_id='' then task_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_task_x_spot_tmp              , md_task_x_spot);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_task_x_spot using &tmpdbschema..md_task_x_spot_tmp              
         ON (md_task_x_spot.spot_id=md_task_x_spot_tmp.spot_id and md_task_x_spot.task_version_id=md_task_x_spot_tmp.task_version_id)
        WHEN MATCHED THEN  
        UPDATE SET task_id = md_task_x_spot_tmp.task_id , task_status_cd = md_task_x_spot_tmp.task_status_cd
        WHEN NOT MATCHED THEN INSERT ( 
        spot_id,task_id,task_status_cd,task_version_id
         ) values ( 
        md_task_x_spot_tmp.spot_id,md_task_x_spot_tmp.task_id,md_task_x_spot_tmp.task_status_cd,md_task_x_spot_tmp.task_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_task_x_spot_tmp              , md_task_x_spot, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_task_x_spot_tmp              ;
    QUIT;
    %put ######## Staging table: md_task_x_spot_tmp               Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_task_x_spot;
      DROP TABLE work.md_task_x_spot;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_task_x_spot;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_task_x_variant) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_task_x_variant_tmp           ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_task_x_variant_tmp           ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_task_x_variant, table_keys=%str(analysis_group_id,task_version_id), out_table=work.md_task_x_variant);
 data &tmplib..md_task_x_variant_tmp           ;
     set work.md_task_x_variant;
  if analysis_group_id='' then analysis_group_id='-'; if task_version_id='' then task_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_task_x_variant_tmp           , md_task_x_variant);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_task_x_variant using &tmpdbschema..md_task_x_variant_tmp           
         ON (md_task_x_variant.analysis_group_id=md_task_x_variant_tmp.analysis_group_id and md_task_x_variant.task_version_id=md_task_x_variant_tmp.task_version_id)
        WHEN MATCHED THEN  
        UPDATE SET task_id = md_task_x_variant_tmp.task_id , task_status_cd = md_task_x_variant_tmp.task_status_cd , variant_nm = md_task_x_variant_tmp.variant_nm , variant_source_nm = md_task_x_variant_tmp.variant_source_nm , variant_type_nm = md_task_x_variant_tmp.variant_type_nm , variant_val = md_task_x_variant_tmp.variant_val
        WHEN NOT MATCHED THEN INSERT ( 
        analysis_group_id,task_id,task_status_cd,task_version_id,variant_nm,variant_source_nm,variant_type_nm,variant_val
         ) values ( 
        md_task_x_variant_tmp.analysis_group_id,md_task_x_variant_tmp.task_id,md_task_x_variant_tmp.task_status_cd,md_task_x_variant_tmp.task_version_id,md_task_x_variant_tmp.variant_nm,md_task_x_variant_tmp.variant_source_nm,md_task_x_variant_tmp.variant_type_nm,md_task_x_variant_tmp.variant_val
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_task_x_variant_tmp           , md_task_x_variant, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_task_x_variant_tmp           ;
    QUIT;
    %put ######## Staging table: md_task_x_variant_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_task_x_variant;
      DROP TABLE work.md_task_x_variant;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_task_x_variant;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_vendor) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_vendor_tmp                   ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_vendor_tmp                   ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_vendor, table_keys=%str(vendor_id), out_table=work.md_vendor);
 data &tmplib..md_vendor_tmp                   ;
     set work.md_vendor;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if vendor_id='' then vendor_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_vendor_tmp                   , md_vendor);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_vendor using &tmpdbschema..md_vendor_tmp                   
         ON (md_vendor.vendor_id=md_vendor_tmp.vendor_id)
        WHEN MATCHED THEN  
        UPDATE SET created_by_usernm = md_vendor_tmp.created_by_usernm , created_dttm = md_vendor_tmp.created_dttm , is_obsolete_flg = md_vendor_tmp.is_obsolete_flg , last_modified_dttm = md_vendor_tmp.last_modified_dttm , last_modified_usernm = md_vendor_tmp.last_modified_usernm , load_dttm = md_vendor_tmp.load_dttm , owner_usernm = md_vendor_tmp.owner_usernm , vendor_currency_cd = md_vendor_tmp.vendor_currency_cd , vendor_desc = md_vendor_tmp.vendor_desc , vendor_nm = md_vendor_tmp.vendor_nm , vendor_number = md_vendor_tmp.vendor_number
        WHEN NOT MATCHED THEN INSERT ( 
        created_by_usernm,created_dttm,is_obsolete_flg,last_modified_dttm,last_modified_usernm,load_dttm,owner_usernm,vendor_currency_cd,vendor_desc,vendor_id,vendor_nm,vendor_number
         ) values ( 
        md_vendor_tmp.created_by_usernm,md_vendor_tmp.created_dttm,md_vendor_tmp.is_obsolete_flg,md_vendor_tmp.last_modified_dttm,md_vendor_tmp.last_modified_usernm,md_vendor_tmp.load_dttm,md_vendor_tmp.owner_usernm,md_vendor_tmp.vendor_currency_cd,md_vendor_tmp.vendor_desc,md_vendor_tmp.vendor_id,md_vendor_tmp.vendor_nm,md_vendor_tmp.vendor_number
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_vendor_tmp                   , md_vendor, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_vendor_tmp                   ;
    QUIT;
    %put ######## Staging table: md_vendor_tmp                    Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_vendor;
      DROP TABLE work.md_vendor;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_vendor;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_wf_process_def) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_wf_process_def_tmp           ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_wf_process_def_tmp           ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_wf_process_def, table_keys=%str(engine_pdef_id,pdef_id), out_table=work.md_wf_process_def);
 data &tmplib..md_wf_process_def_tmp           ;
     set work.md_wf_process_def;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if engine_pdef_id='' then engine_pdef_id='-'; if pdef_id='' then pdef_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_wf_process_def_tmp           , md_wf_process_def);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_wf_process_def using &tmpdbschema..md_wf_process_def_tmp           
         ON (md_wf_process_def.engine_pdef_id=md_wf_process_def_tmp.engine_pdef_id and md_wf_process_def.pdef_id=md_wf_process_def_tmp.pdef_id)
        WHEN MATCHED THEN  
        UPDATE SET associated_object_type = md_wf_process_def_tmp.associated_object_type , buildin_template_flg = md_wf_process_def_tmp.buildin_template_flg , created_by_usernm = md_wf_process_def_tmp.created_by_usernm , created_dttm = md_wf_process_def_tmp.created_dttm , default_approval_flg = md_wf_process_def_tmp.default_approval_flg , engine_pdef_key = md_wf_process_def_tmp.engine_pdef_key , file_tobecatlgd_flg = md_wf_process_def_tmp.file_tobecatlgd_flg , last_modified_dttm = md_wf_process_def_tmp.last_modified_dttm , last_modified_usernm = md_wf_process_def_tmp.last_modified_usernm , latest_version_flg = md_wf_process_def_tmp.latest_version_flg , load_dttm = md_wf_process_def_tmp.load_dttm , owner_usernm = md_wf_process_def_tmp.owner_usernm , pdef_desc = md_wf_process_def_tmp.pdef_desc , pdef_nm = md_wf_process_def_tmp.pdef_nm , pdef_state = md_wf_process_def_tmp.pdef_state , pdef_type = md_wf_process_def_tmp.pdef_type , version_num = md_wf_process_def_tmp.version_num
        WHEN NOT MATCHED THEN INSERT ( 
        associated_object_type,buildin_template_flg,created_by_usernm,created_dttm,default_approval_flg,engine_pdef_id,engine_pdef_key,file_tobecatlgd_flg,last_modified_dttm,last_modified_usernm,latest_version_flg,load_dttm,owner_usernm,pdef_desc,pdef_id,pdef_nm,pdef_state,pdef_type,version_num
         ) values ( 
        md_wf_process_def_tmp.associated_object_type,md_wf_process_def_tmp.buildin_template_flg,md_wf_process_def_tmp.created_by_usernm,md_wf_process_def_tmp.created_dttm,md_wf_process_def_tmp.default_approval_flg,md_wf_process_def_tmp.engine_pdef_id,md_wf_process_def_tmp.engine_pdef_key,md_wf_process_def_tmp.file_tobecatlgd_flg,md_wf_process_def_tmp.last_modified_dttm,md_wf_process_def_tmp.last_modified_usernm,md_wf_process_def_tmp.latest_version_flg,md_wf_process_def_tmp.load_dttm,md_wf_process_def_tmp.owner_usernm,md_wf_process_def_tmp.pdef_desc,md_wf_process_def_tmp.pdef_id,md_wf_process_def_tmp.pdef_nm,md_wf_process_def_tmp.pdef_state,md_wf_process_def_tmp.pdef_type,md_wf_process_def_tmp.version_num
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_wf_process_def_tmp           , md_wf_process_def, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_wf_process_def_tmp           ;
    QUIT;
    %put ######## Staging table: md_wf_process_def_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_wf_process_def;
      DROP TABLE work.md_wf_process_def;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_wf_process_def;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_wf_process_def_attr_grp) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_wf_process_def_attr_grp_tmp  ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_wf_process_def_attr_grp_tmp  ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_wf_process_def_attr_grp, table_keys=%str(attr_group_id,pdef_id), out_table=work.md_wf_process_def_attr_grp);
 data &tmplib..md_wf_process_def_attr_grp_tmp  ;
     set work.md_wf_process_def_attr_grp;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if attr_group_id='' then attr_group_id='-'; if pdef_id='' then pdef_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_wf_process_def_attr_grp_tmp  , md_wf_process_def_attr_grp);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_wf_process_def_attr_grp using &tmpdbschema..md_wf_process_def_attr_grp_tmp  
         ON (md_wf_process_def_attr_grp.attr_group_id=md_wf_process_def_attr_grp_tmp.attr_group_id and md_wf_process_def_attr_grp.pdef_id=md_wf_process_def_attr_grp_tmp.pdef_id)
        WHEN MATCHED THEN  
        UPDATE SET load_dttm = md_wf_process_def_attr_grp_tmp.load_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        attr_group_id,load_dttm,pdef_id
         ) values ( 
        md_wf_process_def_attr_grp_tmp.attr_group_id,md_wf_process_def_attr_grp_tmp.load_dttm,md_wf_process_def_attr_grp_tmp.pdef_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_wf_process_def_attr_grp_tmp  , md_wf_process_def_attr_grp, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_wf_process_def_attr_grp_tmp  ;
    QUIT;
    %put ######## Staging table: md_wf_process_def_attr_grp_tmp   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_wf_process_def_attr_grp;
      DROP TABLE work.md_wf_process_def_attr_grp;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_wf_process_def_attr_grp;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_wf_process_def_categories) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_wf_process_def_categories_tmp) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_wf_process_def_categories_tmp;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_wf_process_def_categories, table_keys=%str(category_id,pdef_id), out_table=work.md_wf_process_def_categories);
 data &tmplib..md_wf_process_def_categories_tmp;
     set work.md_wf_process_def_categories;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if category_id='' then category_id='-'; if pdef_id='' then pdef_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_wf_process_def_categories_tmp, md_wf_process_def_categories);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_wf_process_def_categories using &tmpdbschema..md_wf_process_def_categories_tmp
         ON (md_wf_process_def_categories.category_id=md_wf_process_def_categories_tmp.category_id and md_wf_process_def_categories.pdef_id=md_wf_process_def_categories_tmp.pdef_id)
        WHEN MATCHED THEN  
        UPDATE SET category_type = md_wf_process_def_categories_tmp.category_type , default_category_flg = md_wf_process_def_categories_tmp.default_category_flg , load_dttm = md_wf_process_def_categories_tmp.load_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        category_id,category_type,default_category_flg,load_dttm,pdef_id
         ) values ( 
        md_wf_process_def_categories_tmp.category_id,md_wf_process_def_categories_tmp.category_type,md_wf_process_def_categories_tmp.default_category_flg,md_wf_process_def_categories_tmp.load_dttm,md_wf_process_def_categories_tmp.pdef_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_wf_process_def_categories_tmp, md_wf_process_def_categories, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_wf_process_def_categories_tmp;
    QUIT;
    %put ######## Staging table: md_wf_process_def_categories_tmp Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_wf_process_def_categories;
      DROP TABLE work.md_wf_process_def_categories;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_wf_process_def_categories;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_wf_process_def_task_assg) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_wf_process_def_task_assg_tmp ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_wf_process_def_task_assg_tmp ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_wf_process_def_task_assg, table_keys=%str(assignee_id,assignee_type,pdef_id,task_id), out_table=work.md_wf_process_def_task_assg);
 data &tmplib..md_wf_process_def_task_assg_tmp ;
     set work.md_wf_process_def_task_assg;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if assignee_id='' then assignee_id='-'; if assignee_type='' then assignee_type='-'; if pdef_id='' then pdef_id='-'; if task_id='' then task_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_wf_process_def_task_assg_tmp , md_wf_process_def_task_assg);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_wf_process_def_task_assg using &tmpdbschema..md_wf_process_def_task_assg_tmp 
         ON (md_wf_process_def_task_assg.assignee_id=md_wf_process_def_task_assg_tmp.assignee_id and md_wf_process_def_task_assg.assignee_type=md_wf_process_def_task_assg_tmp.assignee_type and md_wf_process_def_task_assg.pdef_id=md_wf_process_def_task_assg_tmp.pdef_id and md_wf_process_def_task_assg.task_id=md_wf_process_def_task_assg_tmp.task_id)
        WHEN MATCHED THEN  
        UPDATE SET assignee_duration = md_wf_process_def_task_assg_tmp.assignee_duration , assignee_instruction = md_wf_process_def_task_assg_tmp.assignee_instruction , load_dttm = md_wf_process_def_task_assg_tmp.load_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        assignee_duration,assignee_id,assignee_instruction,assignee_type,load_dttm,pdef_id,task_id
         ) values ( 
        md_wf_process_def_task_assg_tmp.assignee_duration,md_wf_process_def_task_assg_tmp.assignee_id,md_wf_process_def_task_assg_tmp.assignee_instruction,md_wf_process_def_task_assg_tmp.assignee_type,md_wf_process_def_task_assg_tmp.load_dttm,md_wf_process_def_task_assg_tmp.pdef_id,md_wf_process_def_task_assg_tmp.task_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_wf_process_def_task_assg_tmp , md_wf_process_def_task_assg, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_wf_process_def_task_assg_tmp ;
    QUIT;
    %put ######## Staging table: md_wf_process_def_task_assg_tmp  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_wf_process_def_task_assg;
      DROP TABLE work.md_wf_process_def_task_assg;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_wf_process_def_task_assg;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_wf_process_def_tasks) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_wf_process_def_tasks_tmp     ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_wf_process_def_tasks_tmp     ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=md_wf_process_def_tasks, table_keys=%str(pdef_id,task_id), out_table=work.md_wf_process_def_tasks);
 data &tmplib..md_wf_process_def_tasks_tmp     ;
     set work.md_wf_process_def_tasks;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if pdef_id='' then pdef_id='-'; if task_id='' then task_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_wf_process_def_tasks_tmp     , md_wf_process_def_tasks);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..md_wf_process_def_tasks using &tmpdbschema..md_wf_process_def_tasks_tmp     
         ON (md_wf_process_def_tasks.pdef_id=md_wf_process_def_tasks_tmp.pdef_id and md_wf_process_def_tasks.task_id=md_wf_process_def_tasks_tmp.task_id)
        WHEN MATCHED THEN  
        UPDATE SET assignee_type = md_wf_process_def_tasks_tmp.assignee_type , ciobject_enabled_flg = md_wf_process_def_tasks_tmp.ciobject_enabled_flg , comment_enabled_flg = md_wf_process_def_tasks_tmp.comment_enabled_flg , comment_mandatory_flg = md_wf_process_def_tasks_tmp.comment_mandatory_flg , default_duration_perassignee = md_wf_process_def_tasks_tmp.default_duration_perassignee , file_enabled_flg = md_wf_process_def_tasks_tmp.file_enabled_flg , file_mandatory_flg = md_wf_process_def_tasks_tmp.file_mandatory_flg , is_sequential_flg = md_wf_process_def_tasks_tmp.is_sequential_flg , item_approval_state = md_wf_process_def_tasks_tmp.item_approval_state , load_dttm = md_wf_process_def_tasks_tmp.load_dttm , multiple_asgnsuprt_flg = md_wf_process_def_tasks_tmp.multiple_asgnsuprt_flg , outgoing_flow_flg = md_wf_process_def_tasks_tmp.outgoing_flow_flg , predecessor_task_id = md_wf_process_def_tasks_tmp.predecessor_task_id , res_mandatory_flg = md_wf_process_def_tasks_tmp.res_mandatory_flg , resp_enabled_flg = md_wf_process_def_tasks_tmp.resp_enabled_flg , resp_file_enabled_flg = md_wf_process_def_tasks_tmp.resp_file_enabled_flg , show_sourceitemlink_flg = md_wf_process_def_tasks_tmp.show_sourceitemlink_flg , show_workflowlink_flg = md_wf_process_def_tasks_tmp.show_workflowlink_flg , source_item_field = md_wf_process_def_tasks_tmp.source_item_field , task_desc = md_wf_process_def_tasks_tmp.task_desc , task_instruction = md_wf_process_def_tasks_tmp.task_instruction , task_nm = md_wf_process_def_tasks_tmp.task_nm , task_subtype = md_wf_process_def_tasks_tmp.task_subtype , task_type = md_wf_process_def_tasks_tmp.task_type , url_enabled_flg = md_wf_process_def_tasks_tmp.url_enabled_flg
        WHEN NOT MATCHED THEN INSERT ( 
        assignee_type,ciobject_enabled_flg,comment_enabled_flg,comment_mandatory_flg,default_duration_perassignee,file_enabled_flg,file_mandatory_flg,is_sequential_flg,item_approval_state,load_dttm,multiple_asgnsuprt_flg,outgoing_flow_flg,pdef_id,predecessor_task_id,res_mandatory_flg,resp_enabled_flg,resp_file_enabled_flg,show_sourceitemlink_flg,show_workflowlink_flg,source_item_field,task_desc,task_id,task_instruction,task_nm,task_subtype,task_type,url_enabled_flg
         ) values ( 
        md_wf_process_def_tasks_tmp.assignee_type,md_wf_process_def_tasks_tmp.ciobject_enabled_flg,md_wf_process_def_tasks_tmp.comment_enabled_flg,md_wf_process_def_tasks_tmp.comment_mandatory_flg,md_wf_process_def_tasks_tmp.default_duration_perassignee,md_wf_process_def_tasks_tmp.file_enabled_flg,md_wf_process_def_tasks_tmp.file_mandatory_flg,md_wf_process_def_tasks_tmp.is_sequential_flg,md_wf_process_def_tasks_tmp.item_approval_state,md_wf_process_def_tasks_tmp.load_dttm,md_wf_process_def_tasks_tmp.multiple_asgnsuprt_flg,md_wf_process_def_tasks_tmp.outgoing_flow_flg,md_wf_process_def_tasks_tmp.pdef_id,md_wf_process_def_tasks_tmp.predecessor_task_id,md_wf_process_def_tasks_tmp.res_mandatory_flg,md_wf_process_def_tasks_tmp.resp_enabled_flg,md_wf_process_def_tasks_tmp.resp_file_enabled_flg,md_wf_process_def_tasks_tmp.show_sourceitemlink_flg,md_wf_process_def_tasks_tmp.show_workflowlink_flg,md_wf_process_def_tasks_tmp.source_item_field,md_wf_process_def_tasks_tmp.task_desc,md_wf_process_def_tasks_tmp.task_id,md_wf_process_def_tasks_tmp.task_instruction,md_wf_process_def_tasks_tmp.task_nm,md_wf_process_def_tasks_tmp.task_subtype,md_wf_process_def_tasks_tmp.task_type,md_wf_process_def_tasks_tmp.url_enabled_flg
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :md_wf_process_def_tasks_tmp     , md_wf_process_def_tasks, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..md_wf_process_def_tasks_tmp     ;
    QUIT;
    %put ######## Staging table: md_wf_process_def_tasks_tmp      Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..md_wf_process_def_tasks;
      DROP TABLE work.md_wf_process_def_tasks;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_wf_process_def_tasks;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..media_activity_details) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..media_activity_details_tmp      ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..media_activity_details_tmp      ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=media_activity_details, table_keys=%str(action_dttm,detail_id,media_nm), out_table=work.media_activity_details);
 data &tmplib..media_activity_details_tmp      ;
     set work.media_activity_details;
  if action_dttm ne . then action_dttm = tzoneu2s(action_dttm,&timeZone_Value.);if action_dttm_tz ne . then action_dttm_tz = tzoneu2s(action_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if detail_id='' then detail_id='-'; if media_nm='' then media_nm='-';
 run;
 %ErrCheck (Failed to Append Data to :media_activity_details_tmp      , media_activity_details);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..media_activity_details using &tmpdbschema..media_activity_details_tmp      
         ON (media_activity_details.action_dttm=media_activity_details_tmp.action_dttm and media_activity_details.detail_id=media_activity_details_tmp.detail_id and media_activity_details.media_nm=media_activity_details_tmp.media_nm)
        WHEN MATCHED THEN  
        UPDATE SET action = media_activity_details_tmp.action , action_dttm_tz = media_activity_details_tmp.action_dttm_tz , detail_id_hex = media_activity_details_tmp.detail_id_hex , load_dttm = media_activity_details_tmp.load_dttm , media_uri_txt = media_activity_details_tmp.media_uri_txt , playhead_position = media_activity_details_tmp.playhead_position
        WHEN NOT MATCHED THEN INSERT ( 
        action,action_dttm,action_dttm_tz,detail_id,detail_id_hex,load_dttm,media_nm,media_uri_txt,playhead_position
         ) values ( 
        media_activity_details_tmp.action,media_activity_details_tmp.action_dttm,media_activity_details_tmp.action_dttm_tz,media_activity_details_tmp.detail_id,media_activity_details_tmp.detail_id_hex,media_activity_details_tmp.load_dttm,media_activity_details_tmp.media_nm,media_activity_details_tmp.media_uri_txt,media_activity_details_tmp.playhead_position
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :media_activity_details_tmp      , media_activity_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..media_activity_details_tmp      ;
    QUIT;
    %put ######## Staging table: media_activity_details_tmp       Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..media_activity_details;
      DROP TABLE work.media_activity_details;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table media_activity_details;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..media_details) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..media_details_tmp               ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..media_details_tmp               ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=media_details, table_keys=%str(event_id), out_table=work.media_details);
 data &tmplib..media_details_tmp               ;
     set work.media_details;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if play_start_dttm ne . then play_start_dttm = tzoneu2s(play_start_dttm,&timeZone_Value.);if play_start_dttm_tz ne . then play_start_dttm_tz = tzoneu2s(play_start_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :media_details_tmp               , media_details);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..media_details using &tmpdbschema..media_details_tmp               
         ON (media_details.event_id=media_details_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET detail_id = media_details_tmp.detail_id , detail_id_hex = media_details_tmp.detail_id_hex , event_key_cd = media_details_tmp.event_key_cd , event_source_cd = media_details_tmp.event_source_cd , identity_id = media_details_tmp.identity_id , load_dttm = media_details_tmp.load_dttm , media_duration_secs = media_details_tmp.media_duration_secs , media_nm = media_details_tmp.media_nm , media_player_nm = media_details_tmp.media_player_nm , media_player_version_txt = media_details_tmp.media_player_version_txt , media_uri_txt = media_details_tmp.media_uri_txt , play_start_dttm = media_details_tmp.play_start_dttm , play_start_dttm_tz = media_details_tmp.play_start_dttm_tz , session_id = media_details_tmp.session_id , session_id_hex = media_details_tmp.session_id_hex , visit_id = media_details_tmp.visit_id , visit_id_hex = media_details_tmp.visit_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        detail_id,detail_id_hex,event_id,event_key_cd,event_source_cd,identity_id,load_dttm,media_duration_secs,media_nm,media_player_nm,media_player_version_txt,media_uri_txt,play_start_dttm,play_start_dttm_tz,session_id,session_id_hex,visit_id,visit_id_hex
         ) values ( 
        media_details_tmp.detail_id,media_details_tmp.detail_id_hex,media_details_tmp.event_id,media_details_tmp.event_key_cd,media_details_tmp.event_source_cd,media_details_tmp.identity_id,media_details_tmp.load_dttm,media_details_tmp.media_duration_secs,media_details_tmp.media_nm,media_details_tmp.media_player_nm,media_details_tmp.media_player_version_txt,media_details_tmp.media_uri_txt,media_details_tmp.play_start_dttm,media_details_tmp.play_start_dttm_tz,media_details_tmp.session_id,media_details_tmp.session_id_hex,media_details_tmp.visit_id,media_details_tmp.visit_id_hex
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :media_details_tmp               , media_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..media_details_tmp               ;
    QUIT;
    %put ######## Staging table: media_details_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..media_details;
      DROP TABLE work.media_details;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table media_details;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..media_details_ext) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..media_details_ext_tmp           ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..media_details_ext_tmp           ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=media_details_ext, table_keys=%str(detail_id,media_nm,play_end_dttm), out_table=work.media_details_ext);
 data &tmplib..media_details_ext_tmp           ;
     set work.media_details_ext;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if play_end_dttm ne . then play_end_dttm = tzoneu2s(play_end_dttm,&timeZone_Value.);if play_end_dttm_tz ne . then play_end_dttm_tz = tzoneu2s(play_end_dttm_tz,&timeZone_Value.) ;
  if detail_id='' then detail_id='-'; if media_nm='' then media_nm='-';
 run;
 %ErrCheck (Failed to Append Data to :media_details_ext_tmp           , media_details_ext);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..media_details_ext using &tmpdbschema..media_details_ext_tmp           
         ON (media_details_ext.detail_id=media_details_ext_tmp.detail_id and media_details_ext.media_nm=media_details_ext_tmp.media_nm and media_details_ext.play_end_dttm=media_details_ext_tmp.play_end_dttm)
        WHEN MATCHED THEN  
        UPDATE SET detail_id_hex = media_details_ext_tmp.detail_id_hex , end_tm = media_details_ext_tmp.end_tm , exit_point_secs = media_details_ext_tmp.exit_point_secs , interaction_cnt = media_details_ext_tmp.interaction_cnt , load_dttm = media_details_ext_tmp.load_dttm , max_play_secs = media_details_ext_tmp.max_play_secs , media_display_duration_secs = media_details_ext_tmp.media_display_duration_secs , media_uri_txt = media_details_ext_tmp.media_uri_txt , play_end_dttm_tz = media_details_ext_tmp.play_end_dttm_tz , start_tm = media_details_ext_tmp.start_tm , view_duration_secs = media_details_ext_tmp.view_duration_secs
        WHEN NOT MATCHED THEN INSERT ( 
        detail_id,detail_id_hex,end_tm,exit_point_secs,interaction_cnt,load_dttm,max_play_secs,media_display_duration_secs,media_nm,media_uri_txt,play_end_dttm,play_end_dttm_tz,start_tm,view_duration_secs
         ) values ( 
        media_details_ext_tmp.detail_id,media_details_ext_tmp.detail_id_hex,media_details_ext_tmp.end_tm,media_details_ext_tmp.exit_point_secs,media_details_ext_tmp.interaction_cnt,media_details_ext_tmp.load_dttm,media_details_ext_tmp.max_play_secs,media_details_ext_tmp.media_display_duration_secs,media_details_ext_tmp.media_nm,media_details_ext_tmp.media_uri_txt,media_details_ext_tmp.play_end_dttm,media_details_ext_tmp.play_end_dttm_tz,media_details_ext_tmp.start_tm,media_details_ext_tmp.view_duration_secs
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :media_details_ext_tmp           , media_details_ext, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..media_details_ext_tmp           ;
    QUIT;
    %put ######## Staging table: media_details_ext_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..media_details_ext;
      DROP TABLE work.media_details_ext;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table media_details_ext;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..mobile_focus_defocus) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..mobile_focus_defocus_tmp        ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..mobile_focus_defocus_tmp        ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=mobile_focus_defocus, table_keys=%str(event_id), out_table=work.mobile_focus_defocus);
 data &tmplib..mobile_focus_defocus_tmp        ;
     set work.mobile_focus_defocus;
  if action_dttm ne . then action_dttm = tzoneu2s(action_dttm,&timeZone_Value.);if action_dttm_tz ne . then action_dttm_tz = tzoneu2s(action_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :mobile_focus_defocus_tmp        , mobile_focus_defocus);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..mobile_focus_defocus using &tmpdbschema..mobile_focus_defocus_tmp        
         ON (mobile_focus_defocus.event_id=mobile_focus_defocus_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET action_dttm = mobile_focus_defocus_tmp.action_dttm , action_dttm_tz = mobile_focus_defocus_tmp.action_dttm_tz , channel_user_id = mobile_focus_defocus_tmp.channel_user_id , detail_id_hex = mobile_focus_defocus_tmp.detail_id_hex , event_designed_id = mobile_focus_defocus_tmp.event_designed_id , event_nm = mobile_focus_defocus_tmp.event_nm , identity_id = mobile_focus_defocus_tmp.identity_id , load_dttm = mobile_focus_defocus_tmp.load_dttm , mobile_app_id = mobile_focus_defocus_tmp.mobile_app_id , reserved_1_txt = mobile_focus_defocus_tmp.reserved_1_txt , session_id_hex = mobile_focus_defocus_tmp.session_id_hex , visit_id_hex = mobile_focus_defocus_tmp.visit_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        action_dttm,action_dttm_tz,channel_user_id,detail_id_hex,event_designed_id,event_id,event_nm,identity_id,load_dttm,mobile_app_id,reserved_1_txt,session_id_hex,visit_id_hex
         ) values ( 
        mobile_focus_defocus_tmp.action_dttm,mobile_focus_defocus_tmp.action_dttm_tz,mobile_focus_defocus_tmp.channel_user_id,mobile_focus_defocus_tmp.detail_id_hex,mobile_focus_defocus_tmp.event_designed_id,mobile_focus_defocus_tmp.event_id,mobile_focus_defocus_tmp.event_nm,mobile_focus_defocus_tmp.identity_id,mobile_focus_defocus_tmp.load_dttm,mobile_focus_defocus_tmp.mobile_app_id,mobile_focus_defocus_tmp.reserved_1_txt,mobile_focus_defocus_tmp.session_id_hex,mobile_focus_defocus_tmp.visit_id_hex
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :mobile_focus_defocus_tmp        , mobile_focus_defocus, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..mobile_focus_defocus_tmp        ;
    QUIT;
    %put ######## Staging table: mobile_focus_defocus_tmp         Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..mobile_focus_defocus;
      DROP TABLE work.mobile_focus_defocus;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table mobile_focus_defocus;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..mobile_spots) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..mobile_spots_tmp                ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..mobile_spots_tmp                ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=mobile_spots, table_keys=%str(event_id), out_table=work.mobile_spots);
 data &tmplib..mobile_spots_tmp                ;
     set work.mobile_spots;
  if action_dttm ne . then action_dttm = tzoneu2s(action_dttm,&timeZone_Value.);if action_dttm_tz ne . then action_dttm_tz = tzoneu2s(action_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :mobile_spots_tmp                , mobile_spots);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..mobile_spots using &tmpdbschema..mobile_spots_tmp                
         ON (mobile_spots.event_id=mobile_spots_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET action_dttm = mobile_spots_tmp.action_dttm , action_dttm_tz = mobile_spots_tmp.action_dttm_tz , channel_user_id = mobile_spots_tmp.channel_user_id , context_type_nm = mobile_spots_tmp.context_type_nm , context_val = mobile_spots_tmp.context_val , creative_id = mobile_spots_tmp.creative_id , detail_id_hex = mobile_spots_tmp.detail_id_hex , event_designed_id = mobile_spots_tmp.event_designed_id , event_nm = mobile_spots_tmp.event_nm , identity_id = mobile_spots_tmp.identity_id , load_dttm = mobile_spots_tmp.load_dttm , mobile_app_id = mobile_spots_tmp.mobile_app_id , session_id_hex = mobile_spots_tmp.session_id_hex , spot_id = mobile_spots_tmp.spot_id , visit_id_hex = mobile_spots_tmp.visit_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        action_dttm,action_dttm_tz,channel_user_id,context_type_nm,context_val,creative_id,detail_id_hex,event_designed_id,event_id,event_nm,identity_id,load_dttm,mobile_app_id,session_id_hex,spot_id,visit_id_hex
         ) values ( 
        mobile_spots_tmp.action_dttm,mobile_spots_tmp.action_dttm_tz,mobile_spots_tmp.channel_user_id,mobile_spots_tmp.context_type_nm,mobile_spots_tmp.context_val,mobile_spots_tmp.creative_id,mobile_spots_tmp.detail_id_hex,mobile_spots_tmp.event_designed_id,mobile_spots_tmp.event_id,mobile_spots_tmp.event_nm,mobile_spots_tmp.identity_id,mobile_spots_tmp.load_dttm,mobile_spots_tmp.mobile_app_id,mobile_spots_tmp.session_id_hex,mobile_spots_tmp.spot_id,mobile_spots_tmp.visit_id_hex
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :mobile_spots_tmp                , mobile_spots, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..mobile_spots_tmp                ;
    QUIT;
    %put ######## Staging table: mobile_spots_tmp                 Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..mobile_spots;
      DROP TABLE work.mobile_spots;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table mobile_spots;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..monthly_usage) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..monthly_usage_tmp               ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..monthly_usage_tmp               ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=monthly_usage, table_keys=%str(event_month), out_table=work.monthly_usage);
 data &tmplib..monthly_usage_tmp               ;
     set work.monthly_usage;
  if event_month='' then event_month='-';
 run;
 %ErrCheck (Failed to Append Data to :monthly_usage_tmp               , monthly_usage);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..monthly_usage using &tmpdbschema..monthly_usage_tmp               
         ON (monthly_usage.event_month=monthly_usage_tmp.event_month)
        WHEN MATCHED THEN  
        UPDATE SET admin_user_cnt = monthly_usage_tmp.admin_user_cnt , api_usage_str = monthly_usage_tmp.api_usage_str , asset_size = monthly_usage_tmp.asset_size , audience_usage_cnt = monthly_usage_tmp.audience_usage_cnt , bc_subjcnt_str = monthly_usage_tmp.bc_subjcnt_str , customer_profiles_processed_str = monthly_usage_tmp.customer_profiles_processed_str , db_size = monthly_usage_tmp.db_size , email_preview_cnt = monthly_usage_tmp.email_preview_cnt , email_send_cnt = monthly_usage_tmp.email_send_cnt , facebook_ads_cnt = monthly_usage_tmp.facebook_ads_cnt , google_ads_cnt = monthly_usage_tmp.google_ads_cnt , linkedin_ads_cnt = monthly_usage_tmp.linkedin_ads_cnt , mob_impr_cnt = monthly_usage_tmp.mob_impr_cnt , mob_sesn_cnt = monthly_usage_tmp.mob_sesn_cnt , mobile_in_app_msg_cnt = monthly_usage_tmp.mobile_in_app_msg_cnt , mobile_push_cnt = monthly_usage_tmp.mobile_push_cnt , outbound_api_cnt = monthly_usage_tmp.outbound_api_cnt , plan_users_cnt = monthly_usage_tmp.plan_users_cnt , web_impr_cnt = monthly_usage_tmp.web_impr_cnt , web_sesn_cnt = monthly_usage_tmp.web_sesn_cnt
        WHEN NOT MATCHED THEN INSERT ( 
        admin_user_cnt,api_usage_str,asset_size,audience_usage_cnt,bc_subjcnt_str,customer_profiles_processed_str,db_size,email_preview_cnt,email_send_cnt,event_month,facebook_ads_cnt,google_ads_cnt,linkedin_ads_cnt,mob_impr_cnt,mob_sesn_cnt,mobile_in_app_msg_cnt,mobile_push_cnt,outbound_api_cnt,plan_users_cnt,web_impr_cnt,web_sesn_cnt
         ) values ( 
        monthly_usage_tmp.admin_user_cnt,monthly_usage_tmp.api_usage_str,monthly_usage_tmp.asset_size,monthly_usage_tmp.audience_usage_cnt,monthly_usage_tmp.bc_subjcnt_str,monthly_usage_tmp.customer_profiles_processed_str,monthly_usage_tmp.db_size,monthly_usage_tmp.email_preview_cnt,monthly_usage_tmp.email_send_cnt,monthly_usage_tmp.event_month,monthly_usage_tmp.facebook_ads_cnt,monthly_usage_tmp.google_ads_cnt,monthly_usage_tmp.linkedin_ads_cnt,monthly_usage_tmp.mob_impr_cnt,monthly_usage_tmp.mob_sesn_cnt,monthly_usage_tmp.mobile_in_app_msg_cnt,monthly_usage_tmp.mobile_push_cnt,monthly_usage_tmp.outbound_api_cnt,monthly_usage_tmp.plan_users_cnt,monthly_usage_tmp.web_impr_cnt,monthly_usage_tmp.web_sesn_cnt
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :monthly_usage_tmp               , monthly_usage, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..monthly_usage_tmp               ;
    QUIT;
    %put ######## Staging table: monthly_usage_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..monthly_usage;
      DROP TABLE work.monthly_usage;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table monthly_usage;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..notification_failed) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..notification_failed_tmp         ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..notification_failed_tmp         ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=notification_failed, table_keys=%str(event_id), out_table=work.notification_failed);
 data &tmplib..notification_failed_tmp         ;
     set work.notification_failed;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if notification_failed_dttm ne . then notification_failed_dttm = tzoneu2s(notification_failed_dttm,&timeZone_Value.);if notification_failed_dttm_tz ne . then notification_failed_dttm_tz = tzoneu2s(notification_failed_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :notification_failed_tmp         , notification_failed);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..notification_failed using &tmpdbschema..notification_failed_tmp         
         ON (notification_failed.event_id=notification_failed_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET aud_occurrence_id = notification_failed_tmp.aud_occurrence_id , audience_id = notification_failed_tmp.audience_id , channel_nm = notification_failed_tmp.channel_nm , channel_user_id = notification_failed_tmp.channel_user_id , context_type_nm = notification_failed_tmp.context_type_nm , context_val = notification_failed_tmp.context_val , creative_id = notification_failed_tmp.creative_id , creative_version_id = notification_failed_tmp.creative_version_id , error_cd = notification_failed_tmp.error_cd , error_message_txt = notification_failed_tmp.error_message_txt , event_designed_id = notification_failed_tmp.event_designed_id , event_nm = notification_failed_tmp.event_nm , identity_id = notification_failed_tmp.identity_id , journey_id = notification_failed_tmp.journey_id , journey_occurrence_id = notification_failed_tmp.journey_occurrence_id , load_dttm = notification_failed_tmp.load_dttm , message_id = notification_failed_tmp.message_id , message_version_id = notification_failed_tmp.message_version_id , mobile_app_id = notification_failed_tmp.mobile_app_id , notification_failed_dttm = notification_failed_tmp.notification_failed_dttm , notification_failed_dttm_tz = notification_failed_tmp.notification_failed_dttm_tz , occurrence_id = notification_failed_tmp.occurrence_id , properties_map_doc = notification_failed_tmp.properties_map_doc , reserved_1_txt = notification_failed_tmp.reserved_1_txt , reserved_2_txt = notification_failed_tmp.reserved_2_txt , response_tracking_cd = notification_failed_tmp.response_tracking_cd , segment_id = notification_failed_tmp.segment_id , segment_version_id = notification_failed_tmp.segment_version_id , spot_id = notification_failed_tmp.spot_id , task_id = notification_failed_tmp.task_id , task_version_id = notification_failed_tmp.task_version_id
        WHEN NOT MATCHED THEN INSERT ( 
        aud_occurrence_id,audience_id,channel_nm,channel_user_id,context_type_nm,context_val,creative_id,creative_version_id,error_cd,error_message_txt,event_designed_id,event_id,event_nm,identity_id,journey_id,journey_occurrence_id,load_dttm,message_id,message_version_id,mobile_app_id,notification_failed_dttm,notification_failed_dttm_tz,occurrence_id,properties_map_doc,reserved_1_txt,reserved_2_txt,response_tracking_cd,segment_id,segment_version_id,spot_id,task_id,task_version_id
         ) values ( 
        notification_failed_tmp.aud_occurrence_id,notification_failed_tmp.audience_id,notification_failed_tmp.channel_nm,notification_failed_tmp.channel_user_id,notification_failed_tmp.context_type_nm,notification_failed_tmp.context_val,notification_failed_tmp.creative_id,notification_failed_tmp.creative_version_id,notification_failed_tmp.error_cd,notification_failed_tmp.error_message_txt,notification_failed_tmp.event_designed_id,notification_failed_tmp.event_id,notification_failed_tmp.event_nm,notification_failed_tmp.identity_id,notification_failed_tmp.journey_id,notification_failed_tmp.journey_occurrence_id,notification_failed_tmp.load_dttm,notification_failed_tmp.message_id,notification_failed_tmp.message_version_id,notification_failed_tmp.mobile_app_id,notification_failed_tmp.notification_failed_dttm,notification_failed_tmp.notification_failed_dttm_tz,notification_failed_tmp.occurrence_id,notification_failed_tmp.properties_map_doc,notification_failed_tmp.reserved_1_txt,notification_failed_tmp.reserved_2_txt,notification_failed_tmp.response_tracking_cd,notification_failed_tmp.segment_id,notification_failed_tmp.segment_version_id,notification_failed_tmp.spot_id,notification_failed_tmp.task_id,notification_failed_tmp.task_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :notification_failed_tmp         , notification_failed, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..notification_failed_tmp         ;
    QUIT;
    %put ######## Staging table: notification_failed_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..notification_failed;
      DROP TABLE work.notification_failed;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table notification_failed;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..notification_opened) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..notification_opened_tmp         ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..notification_opened_tmp         ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=notification_opened, table_keys=%str(event_id), out_table=work.notification_opened);
 data &tmplib..notification_opened_tmp         ;
     set work.notification_opened;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if notification_opened_dttm ne . then notification_opened_dttm = tzoneu2s(notification_opened_dttm,&timeZone_Value.);if notification_opened_dttm_tz ne . then notification_opened_dttm_tz = tzoneu2s(notification_opened_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :notification_opened_tmp         , notification_opened);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..notification_opened using &tmpdbschema..notification_opened_tmp         
         ON (notification_opened.event_id=notification_opened_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET aud_occurrence_id = notification_opened_tmp.aud_occurrence_id , audience_id = notification_opened_tmp.audience_id , channel_nm = notification_opened_tmp.channel_nm , channel_user_id = notification_opened_tmp.channel_user_id , context_type_nm = notification_opened_tmp.context_type_nm , context_val = notification_opened_tmp.context_val , creative_id = notification_opened_tmp.creative_id , creative_version_id = notification_opened_tmp.creative_version_id , event_designed_id = notification_opened_tmp.event_designed_id , event_nm = notification_opened_tmp.event_nm , identity_id = notification_opened_tmp.identity_id , journey_id = notification_opened_tmp.journey_id , journey_occurrence_id = notification_opened_tmp.journey_occurrence_id , load_dttm = notification_opened_tmp.load_dttm , message_id = notification_opened_tmp.message_id , message_version_id = notification_opened_tmp.message_version_id , mobile_app_id = notification_opened_tmp.mobile_app_id , notification_opened_dttm = notification_opened_tmp.notification_opened_dttm , notification_opened_dttm_tz = notification_opened_tmp.notification_opened_dttm_tz , occurrence_id = notification_opened_tmp.occurrence_id , properties_map_doc = notification_opened_tmp.properties_map_doc , reserved_1_txt = notification_opened_tmp.reserved_1_txt , reserved_2_txt = notification_opened_tmp.reserved_2_txt , reserved_3_txt = notification_opened_tmp.reserved_3_txt , response_tracking_cd = notification_opened_tmp.response_tracking_cd , segment_id = notification_opened_tmp.segment_id , segment_version_id = notification_opened_tmp.segment_version_id , spot_id = notification_opened_tmp.spot_id , task_id = notification_opened_tmp.task_id , task_version_id = notification_opened_tmp.task_version_id
        WHEN NOT MATCHED THEN INSERT ( 
        aud_occurrence_id,audience_id,channel_nm,channel_user_id,context_type_nm,context_val,creative_id,creative_version_id,event_designed_id,event_id,event_nm,identity_id,journey_id,journey_occurrence_id,load_dttm,message_id,message_version_id,mobile_app_id,notification_opened_dttm,notification_opened_dttm_tz,occurrence_id,properties_map_doc,reserved_1_txt,reserved_2_txt,reserved_3_txt,response_tracking_cd,segment_id,segment_version_id,spot_id,task_id,task_version_id
         ) values ( 
        notification_opened_tmp.aud_occurrence_id,notification_opened_tmp.audience_id,notification_opened_tmp.channel_nm,notification_opened_tmp.channel_user_id,notification_opened_tmp.context_type_nm,notification_opened_tmp.context_val,notification_opened_tmp.creative_id,notification_opened_tmp.creative_version_id,notification_opened_tmp.event_designed_id,notification_opened_tmp.event_id,notification_opened_tmp.event_nm,notification_opened_tmp.identity_id,notification_opened_tmp.journey_id,notification_opened_tmp.journey_occurrence_id,notification_opened_tmp.load_dttm,notification_opened_tmp.message_id,notification_opened_tmp.message_version_id,notification_opened_tmp.mobile_app_id,notification_opened_tmp.notification_opened_dttm,notification_opened_tmp.notification_opened_dttm_tz,notification_opened_tmp.occurrence_id,notification_opened_tmp.properties_map_doc,notification_opened_tmp.reserved_1_txt,notification_opened_tmp.reserved_2_txt,notification_opened_tmp.reserved_3_txt,notification_opened_tmp.response_tracking_cd,notification_opened_tmp.segment_id,notification_opened_tmp.segment_version_id,notification_opened_tmp.spot_id,notification_opened_tmp.task_id,notification_opened_tmp.task_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :notification_opened_tmp         , notification_opened, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..notification_opened_tmp         ;
    QUIT;
    %put ######## Staging table: notification_opened_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..notification_opened;
      DROP TABLE work.notification_opened;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table notification_opened;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..notification_send) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..notification_send_tmp           ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..notification_send_tmp           ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=notification_send, table_keys=%str(event_id), out_table=work.notification_send);
 data &tmplib..notification_send_tmp           ;
     set work.notification_send;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if notification_send_dttm ne . then notification_send_dttm = tzoneu2s(notification_send_dttm,&timeZone_Value.);if notification_send_dttm_tz ne . then notification_send_dttm_tz = tzoneu2s(notification_send_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :notification_send_tmp           , notification_send);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..notification_send using &tmpdbschema..notification_send_tmp           
         ON (notification_send.event_id=notification_send_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET aud_occurrence_id = notification_send_tmp.aud_occurrence_id , audience_id = notification_send_tmp.audience_id , channel_nm = notification_send_tmp.channel_nm , channel_user_id = notification_send_tmp.channel_user_id , context_type_nm = notification_send_tmp.context_type_nm , context_val = notification_send_tmp.context_val , creative_id = notification_send_tmp.creative_id , creative_version_id = notification_send_tmp.creative_version_id , event_designed_id = notification_send_tmp.event_designed_id , event_nm = notification_send_tmp.event_nm , identity_id = notification_send_tmp.identity_id , journey_id = notification_send_tmp.journey_id , journey_occurrence_id = notification_send_tmp.journey_occurrence_id , load_dttm = notification_send_tmp.load_dttm , message_id = notification_send_tmp.message_id , message_version_id = notification_send_tmp.message_version_id , mobile_app_id = notification_send_tmp.mobile_app_id , notification_send_dttm = notification_send_tmp.notification_send_dttm , notification_send_dttm_tz = notification_send_tmp.notification_send_dttm_tz , occurrence_id = notification_send_tmp.occurrence_id , properties_map_doc = notification_send_tmp.properties_map_doc , reserved_1_txt = notification_send_tmp.reserved_1_txt , reserved_2_txt = notification_send_tmp.reserved_2_txt , response_tracking_cd = notification_send_tmp.response_tracking_cd , segment_id = notification_send_tmp.segment_id , segment_version_id = notification_send_tmp.segment_version_id , spot_id = notification_send_tmp.spot_id , task_id = notification_send_tmp.task_id , task_version_id = notification_send_tmp.task_version_id
        WHEN NOT MATCHED THEN INSERT ( 
        aud_occurrence_id,audience_id,channel_nm,channel_user_id,context_type_nm,context_val,creative_id,creative_version_id,event_designed_id,event_id,event_nm,identity_id,journey_id,journey_occurrence_id,load_dttm,message_id,message_version_id,mobile_app_id,notification_send_dttm,notification_send_dttm_tz,occurrence_id,properties_map_doc,reserved_1_txt,reserved_2_txt,response_tracking_cd,segment_id,segment_version_id,spot_id,task_id,task_version_id
         ) values ( 
        notification_send_tmp.aud_occurrence_id,notification_send_tmp.audience_id,notification_send_tmp.channel_nm,notification_send_tmp.channel_user_id,notification_send_tmp.context_type_nm,notification_send_tmp.context_val,notification_send_tmp.creative_id,notification_send_tmp.creative_version_id,notification_send_tmp.event_designed_id,notification_send_tmp.event_id,notification_send_tmp.event_nm,notification_send_tmp.identity_id,notification_send_tmp.journey_id,notification_send_tmp.journey_occurrence_id,notification_send_tmp.load_dttm,notification_send_tmp.message_id,notification_send_tmp.message_version_id,notification_send_tmp.mobile_app_id,notification_send_tmp.notification_send_dttm,notification_send_tmp.notification_send_dttm_tz,notification_send_tmp.occurrence_id,notification_send_tmp.properties_map_doc,notification_send_tmp.reserved_1_txt,notification_send_tmp.reserved_2_txt,notification_send_tmp.response_tracking_cd,notification_send_tmp.segment_id,notification_send_tmp.segment_version_id,notification_send_tmp.spot_id,notification_send_tmp.task_id,notification_send_tmp.task_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :notification_send_tmp           , notification_send, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..notification_send_tmp           ;
    QUIT;
    %put ######## Staging table: notification_send_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..notification_send;
      DROP TABLE work.notification_send;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table notification_send;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..notification_targeting_request) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..notification_targeting_reque_tmp) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..notification_targeting_reque_tmp;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=notification_targeting_request, table_keys=%str(event_id), out_table=work.notification_targeting_request);
 data &tmplib..notification_targeting_reque_tmp;
     set work.notification_targeting_request;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if notification_tgt_req_dttm ne . then notification_tgt_req_dttm = tzoneu2s(notification_tgt_req_dttm,&timeZone_Value.);if notification_tgt_req_dttm_tz ne . then notification_tgt_req_dttm_tz = tzoneu2s(notification_tgt_req_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :notification_targeting_reque_tmp, notification_targeting_request);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..notification_targeting_request using &tmpdbschema..notification_targeting_reque_tmp
         ON (notification_targeting_request.event_id=notification_targeting_reque_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET aud_occurrence_id = notification_targeting_reque_tmp.aud_occurrence_id , audience_id = notification_targeting_reque_tmp.audience_id , channel_nm = notification_targeting_reque_tmp.channel_nm , channel_user_id = notification_targeting_reque_tmp.channel_user_id , context_type_nm = notification_targeting_reque_tmp.context_type_nm , context_val = notification_targeting_reque_tmp.context_val , eligibility_flg = notification_targeting_reque_tmp.eligibility_flg , event_designed_id = notification_targeting_reque_tmp.event_designed_id , event_nm = notification_targeting_reque_tmp.event_nm , identity_id = notification_targeting_reque_tmp.identity_id , journey_id = notification_targeting_reque_tmp.journey_id , journey_occurrence_id = notification_targeting_reque_tmp.journey_occurrence_id , load_dttm = notification_targeting_reque_tmp.load_dttm , mobile_app_id = notification_targeting_reque_tmp.mobile_app_id , notification_tgt_req_dttm = notification_targeting_reque_tmp.notification_tgt_req_dttm , notification_tgt_req_dttm_tz = notification_targeting_reque_tmp.notification_tgt_req_dttm_tz , task_id = notification_targeting_reque_tmp.task_id
        WHEN NOT MATCHED THEN INSERT ( 
        aud_occurrence_id,audience_id,channel_nm,channel_user_id,context_type_nm,context_val,eligibility_flg,event_designed_id,event_id,event_nm,identity_id,journey_id,journey_occurrence_id,load_dttm,mobile_app_id,notification_tgt_req_dttm,notification_tgt_req_dttm_tz,task_id
         ) values ( 
        notification_targeting_reque_tmp.aud_occurrence_id,notification_targeting_reque_tmp.audience_id,notification_targeting_reque_tmp.channel_nm,notification_targeting_reque_tmp.channel_user_id,notification_targeting_reque_tmp.context_type_nm,notification_targeting_reque_tmp.context_val,notification_targeting_reque_tmp.eligibility_flg,notification_targeting_reque_tmp.event_designed_id,notification_targeting_reque_tmp.event_id,notification_targeting_reque_tmp.event_nm,notification_targeting_reque_tmp.identity_id,notification_targeting_reque_tmp.journey_id,notification_targeting_reque_tmp.journey_occurrence_id,notification_targeting_reque_tmp.load_dttm,notification_targeting_reque_tmp.mobile_app_id,notification_targeting_reque_tmp.notification_tgt_req_dttm,notification_targeting_reque_tmp.notification_tgt_req_dttm_tz,notification_targeting_reque_tmp.task_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :notification_targeting_reque_tmp, notification_targeting_request, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..notification_targeting_reque_tmp;
    QUIT;
    %put ######## Staging table: notification_targeting_reque_tmp Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..notification_targeting_request;
      DROP TABLE work.notification_targeting_request;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table notification_targeting_request;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..order_details) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..order_details_tmp               ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..order_details_tmp               ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=order_details, table_keys=%str(detail_id,event_designed_id,product_id,product_nm,product_sku,record_type), out_table=work.order_details);
 data &tmplib..order_details_tmp               ;
     set work.order_details;
  if activity_dttm ne . then activity_dttm = tzoneu2s(activity_dttm,&timeZone_Value.);if activity_dttm_tz ne . then activity_dttm_tz = tzoneu2s(activity_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if detail_id='' then detail_id='-'; if event_designed_id='' then event_designed_id='-'; if product_id='' then product_id='-'; if product_nm='' then product_nm='-'; if product_sku='' then product_sku='-'; if record_type='' then record_type='-';
 run;
 %ErrCheck (Failed to Append Data to :order_details_tmp               , order_details);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..order_details using &tmpdbschema..order_details_tmp               
         ON (order_details.detail_id=order_details_tmp.detail_id and order_details.event_designed_id=order_details_tmp.event_designed_id and order_details.product_id=order_details_tmp.product_id and order_details.product_nm=order_details_tmp.product_nm and order_details.product_sku=order_details_tmp.product_sku and order_details.record_type=order_details_tmp.record_type)
        WHEN MATCHED THEN  
        UPDATE SET activity_dttm = order_details_tmp.activity_dttm , activity_dttm_tz = order_details_tmp.activity_dttm_tz , availability_message_txt = order_details_tmp.availability_message_txt , cart_id = order_details_tmp.cart_id , cart_nm = order_details_tmp.cart_nm , channel_nm = order_details_tmp.channel_nm , currency_cd = order_details_tmp.currency_cd , detail_id_hex = order_details_tmp.detail_id_hex , event_id = order_details_tmp.event_id , event_key_cd = order_details_tmp.event_key_cd , event_nm = order_details_tmp.event_nm , event_source_cd = order_details_tmp.event_source_cd , identity_id = order_details_tmp.identity_id , load_dttm = order_details_tmp.load_dttm , mobile_app_id = order_details_tmp.mobile_app_id , order_id = order_details_tmp.order_id , product_group_nm = order_details_tmp.product_group_nm , properties_map_doc = order_details_tmp.properties_map_doc , quantity_amt = order_details_tmp.quantity_amt , reserved_1_txt = order_details_tmp.reserved_1_txt , saving_message_txt = order_details_tmp.saving_message_txt , session_id = order_details_tmp.session_id , session_id_hex = order_details_tmp.session_id_hex , shipping_message_txt = order_details_tmp.shipping_message_txt , unit_price_amt = order_details_tmp.unit_price_amt , visit_id = order_details_tmp.visit_id , visit_id_hex = order_details_tmp.visit_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        activity_dttm,activity_dttm_tz,availability_message_txt,cart_id,cart_nm,channel_nm,currency_cd,detail_id,detail_id_hex,event_designed_id,event_id,event_key_cd,event_nm,event_source_cd,identity_id,load_dttm,mobile_app_id,order_id,product_group_nm,product_id,product_nm,product_sku,properties_map_doc,quantity_amt,record_type,reserved_1_txt,saving_message_txt,session_id,session_id_hex,shipping_message_txt,unit_price_amt,visit_id,visit_id_hex
         ) values ( 
        order_details_tmp.activity_dttm,order_details_tmp.activity_dttm_tz,order_details_tmp.availability_message_txt,order_details_tmp.cart_id,order_details_tmp.cart_nm,order_details_tmp.channel_nm,order_details_tmp.currency_cd,order_details_tmp.detail_id,order_details_tmp.detail_id_hex,order_details_tmp.event_designed_id,order_details_tmp.event_id,order_details_tmp.event_key_cd,order_details_tmp.event_nm,order_details_tmp.event_source_cd,order_details_tmp.identity_id,order_details_tmp.load_dttm,order_details_tmp.mobile_app_id,order_details_tmp.order_id,order_details_tmp.product_group_nm,order_details_tmp.product_id,order_details_tmp.product_nm,order_details_tmp.product_sku,order_details_tmp.properties_map_doc,order_details_tmp.quantity_amt,order_details_tmp.record_type,order_details_tmp.reserved_1_txt,order_details_tmp.saving_message_txt,order_details_tmp.session_id,order_details_tmp.session_id_hex,order_details_tmp.shipping_message_txt,order_details_tmp.unit_price_amt,order_details_tmp.visit_id,order_details_tmp.visit_id_hex
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :order_details_tmp               , order_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..order_details_tmp               ;
    QUIT;
    %put ######## Staging table: order_details_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..order_details;
      DROP TABLE work.order_details;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table order_details;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..order_summary) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..order_summary_tmp               ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..order_summary_tmp               ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=order_summary, table_keys=%str(detail_id,event_designed_id,record_type), out_table=work.order_summary);
 data &tmplib..order_summary_tmp               ;
     set work.order_summary;
  if activity_dttm ne . then activity_dttm = tzoneu2s(activity_dttm,&timeZone_Value.);if activity_dttm_tz ne . then activity_dttm_tz = tzoneu2s(activity_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if detail_id='' then detail_id='-'; if event_designed_id='' then event_designed_id='-'; if record_type='' then record_type='-';
 run;
 %ErrCheck (Failed to Append Data to :order_summary_tmp               , order_summary);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..order_summary using &tmpdbschema..order_summary_tmp               
         ON (order_summary.detail_id=order_summary_tmp.detail_id and order_summary.event_designed_id=order_summary_tmp.event_designed_id and order_summary.record_type=order_summary_tmp.record_type)
        WHEN MATCHED THEN  
        UPDATE SET activity_dttm = order_summary_tmp.activity_dttm , activity_dttm_tz = order_summary_tmp.activity_dttm_tz , billing_city_nm = order_summary_tmp.billing_city_nm , billing_country_nm = order_summary_tmp.billing_country_nm , billing_postal_cd = order_summary_tmp.billing_postal_cd , billing_state_region_cd = order_summary_tmp.billing_state_region_cd , cart_id = order_summary_tmp.cart_id , cart_nm = order_summary_tmp.cart_nm , channel_nm = order_summary_tmp.channel_nm , currency_cd = order_summary_tmp.currency_cd , delivery_type_desc = order_summary_tmp.delivery_type_desc , detail_id_hex = order_summary_tmp.detail_id_hex , event_id = order_summary_tmp.event_id , event_key_cd = order_summary_tmp.event_key_cd , event_nm = order_summary_tmp.event_nm , event_source_cd = order_summary_tmp.event_source_cd , identity_id = order_summary_tmp.identity_id , load_dttm = order_summary_tmp.load_dttm , mobile_app_id = order_summary_tmp.mobile_app_id , order_id = order_summary_tmp.order_id , payment_type_desc = order_summary_tmp.payment_type_desc , properties_map_doc = order_summary_tmp.properties_map_doc , session_id = order_summary_tmp.session_id , session_id_hex = order_summary_tmp.session_id_hex , shipping_amt = order_summary_tmp.shipping_amt , shipping_city_nm = order_summary_tmp.shipping_city_nm , shipping_country_nm = order_summary_tmp.shipping_country_nm , shipping_postal_cd = order_summary_tmp.shipping_postal_cd , shipping_state_region_cd = order_summary_tmp.shipping_state_region_cd , total_price_amt = order_summary_tmp.total_price_amt , total_tax_amt = order_summary_tmp.total_tax_amt , total_unit_qty = order_summary_tmp.total_unit_qty , visit_id = order_summary_tmp.visit_id , visit_id_hex = order_summary_tmp.visit_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        activity_dttm,activity_dttm_tz,billing_city_nm,billing_country_nm,billing_postal_cd,billing_state_region_cd,cart_id,cart_nm,channel_nm,currency_cd,delivery_type_desc,detail_id,detail_id_hex,event_designed_id,event_id,event_key_cd,event_nm,event_source_cd,identity_id,load_dttm,mobile_app_id,order_id,payment_type_desc,properties_map_doc,record_type,session_id,session_id_hex,shipping_amt,shipping_city_nm,shipping_country_nm,shipping_postal_cd,shipping_state_region_cd,total_price_amt,total_tax_amt,total_unit_qty,visit_id,visit_id_hex
         ) values ( 
        order_summary_tmp.activity_dttm,order_summary_tmp.activity_dttm_tz,order_summary_tmp.billing_city_nm,order_summary_tmp.billing_country_nm,order_summary_tmp.billing_postal_cd,order_summary_tmp.billing_state_region_cd,order_summary_tmp.cart_id,order_summary_tmp.cart_nm,order_summary_tmp.channel_nm,order_summary_tmp.currency_cd,order_summary_tmp.delivery_type_desc,order_summary_tmp.detail_id,order_summary_tmp.detail_id_hex,order_summary_tmp.event_designed_id,order_summary_tmp.event_id,order_summary_tmp.event_key_cd,order_summary_tmp.event_nm,order_summary_tmp.event_source_cd,order_summary_tmp.identity_id,order_summary_tmp.load_dttm,order_summary_tmp.mobile_app_id,order_summary_tmp.order_id,order_summary_tmp.payment_type_desc,order_summary_tmp.properties_map_doc,order_summary_tmp.record_type,order_summary_tmp.session_id,order_summary_tmp.session_id_hex,order_summary_tmp.shipping_amt,order_summary_tmp.shipping_city_nm,order_summary_tmp.shipping_country_nm,order_summary_tmp.shipping_postal_cd,order_summary_tmp.shipping_state_region_cd,order_summary_tmp.total_price_amt,order_summary_tmp.total_tax_amt,order_summary_tmp.total_unit_qty,order_summary_tmp.visit_id,order_summary_tmp.visit_id_hex
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :order_summary_tmp               , order_summary, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..order_summary_tmp               ;
    QUIT;
    %put ######## Staging table: order_summary_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..order_summary;
      DROP TABLE work.order_summary;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table order_summary;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..outbound_system) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..outbound_system_tmp             ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..outbound_system_tmp             ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=outbound_system, table_keys=%str(event_id), out_table=work.outbound_system);
 data &tmplib..outbound_system_tmp             ;
     set work.outbound_system;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if outbound_system_dttm ne . then outbound_system_dttm = tzoneu2s(outbound_system_dttm,&timeZone_Value.);if outbound_system_dttm_tz ne . then outbound_system_dttm_tz = tzoneu2s(outbound_system_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :outbound_system_tmp             , outbound_system);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..outbound_system using &tmpdbschema..outbound_system_tmp             
         ON (outbound_system.event_id=outbound_system_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET aud_occurrence_id = outbound_system_tmp.aud_occurrence_id , audience_id = outbound_system_tmp.audience_id , channel_nm = outbound_system_tmp.channel_nm , channel_user_id = outbound_system_tmp.channel_user_id , context_type_nm = outbound_system_tmp.context_type_nm , context_val = outbound_system_tmp.context_val , creative_id = outbound_system_tmp.creative_id , creative_version_id = outbound_system_tmp.creative_version_id , detail_id_hex = outbound_system_tmp.detail_id_hex , event_designed_id = outbound_system_tmp.event_designed_id , event_nm = outbound_system_tmp.event_nm , identity_id = outbound_system_tmp.identity_id , journey_id = outbound_system_tmp.journey_id , journey_occurrence_id = outbound_system_tmp.journey_occurrence_id , load_dttm = outbound_system_tmp.load_dttm , message_id = outbound_system_tmp.message_id , message_version_id = outbound_system_tmp.message_version_id , mobile_app_id = outbound_system_tmp.mobile_app_id , occurrence_id = outbound_system_tmp.occurrence_id , outbound_system_dttm = outbound_system_tmp.outbound_system_dttm , outbound_system_dttm_tz = outbound_system_tmp.outbound_system_dttm_tz , parent_event_id = outbound_system_tmp.parent_event_id , properties_map_doc = outbound_system_tmp.properties_map_doc , reserved_1_txt = outbound_system_tmp.reserved_1_txt , reserved_2_txt = outbound_system_tmp.reserved_2_txt , response_tracking_cd = outbound_system_tmp.response_tracking_cd , segment_id = outbound_system_tmp.segment_id , segment_version_id = outbound_system_tmp.segment_version_id , session_id_hex = outbound_system_tmp.session_id_hex , spot_id = outbound_system_tmp.spot_id , task_id = outbound_system_tmp.task_id , task_version_id = outbound_system_tmp.task_version_id , visit_id_hex = outbound_system_tmp.visit_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        aud_occurrence_id,audience_id,channel_nm,channel_user_id,context_type_nm,context_val,creative_id,creative_version_id,detail_id_hex,event_designed_id,event_id,event_nm,identity_id,journey_id,journey_occurrence_id,load_dttm,message_id,message_version_id,mobile_app_id,occurrence_id,outbound_system_dttm,outbound_system_dttm_tz,parent_event_id,properties_map_doc,reserved_1_txt,reserved_2_txt,response_tracking_cd,segment_id,segment_version_id,session_id_hex,spot_id,task_id,task_version_id,visit_id_hex
         ) values ( 
        outbound_system_tmp.aud_occurrence_id,outbound_system_tmp.audience_id,outbound_system_tmp.channel_nm,outbound_system_tmp.channel_user_id,outbound_system_tmp.context_type_nm,outbound_system_tmp.context_val,outbound_system_tmp.creative_id,outbound_system_tmp.creative_version_id,outbound_system_tmp.detail_id_hex,outbound_system_tmp.event_designed_id,outbound_system_tmp.event_id,outbound_system_tmp.event_nm,outbound_system_tmp.identity_id,outbound_system_tmp.journey_id,outbound_system_tmp.journey_occurrence_id,outbound_system_tmp.load_dttm,outbound_system_tmp.message_id,outbound_system_tmp.message_version_id,outbound_system_tmp.mobile_app_id,outbound_system_tmp.occurrence_id,outbound_system_tmp.outbound_system_dttm,outbound_system_tmp.outbound_system_dttm_tz,outbound_system_tmp.parent_event_id,outbound_system_tmp.properties_map_doc,outbound_system_tmp.reserved_1_txt,outbound_system_tmp.reserved_2_txt,outbound_system_tmp.response_tracking_cd,outbound_system_tmp.segment_id,outbound_system_tmp.segment_version_id,outbound_system_tmp.session_id_hex,outbound_system_tmp.spot_id,outbound_system_tmp.task_id,outbound_system_tmp.task_version_id,outbound_system_tmp.visit_id_hex
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :outbound_system_tmp             , outbound_system, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..outbound_system_tmp             ;
    QUIT;
    %put ######## Staging table: outbound_system_tmp              Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..outbound_system;
      DROP TABLE work.outbound_system;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table outbound_system;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..page_details) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..page_details_tmp                ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..page_details_tmp                ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=page_details, table_keys=%str(event_id), out_table=work.page_details);
 data &tmplib..page_details_tmp                ;
     set work.page_details;
  if detail_dttm ne . then detail_dttm = tzoneu2s(detail_dttm,&timeZone_Value.);if detail_dttm_tz ne . then detail_dttm_tz = tzoneu2s(detail_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :page_details_tmp                , page_details);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..page_details using &tmpdbschema..page_details_tmp                
         ON (page_details.event_id=page_details_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET bytes_sent_cnt = page_details_tmp.bytes_sent_cnt , channel_nm = page_details_tmp.channel_nm , class10_id = page_details_tmp.class10_id , class11_id = page_details_tmp.class11_id , class12_id = page_details_tmp.class12_id , class13_id = page_details_tmp.class13_id , class14_id = page_details_tmp.class14_id , class15_id = page_details_tmp.class15_id , class1_id = page_details_tmp.class1_id , class2_id = page_details_tmp.class2_id , class3_id = page_details_tmp.class3_id , class4_id = page_details_tmp.class4_id , class5_id = page_details_tmp.class5_id , class6_id = page_details_tmp.class6_id , class7_id = page_details_tmp.class7_id , class8_id = page_details_tmp.class8_id , class9_id = page_details_tmp.class9_id , detail_dttm = page_details_tmp.detail_dttm , detail_dttm_tz = page_details_tmp.detail_dttm_tz , detail_id = page_details_tmp.detail_id , detail_id_hex = page_details_tmp.detail_id_hex , domain_nm = page_details_tmp.domain_nm , event_key_cd = page_details_tmp.event_key_cd , event_nm = page_details_tmp.event_nm , event_source_cd = page_details_tmp.event_source_cd , identity_id = page_details_tmp.identity_id , load_dttm = page_details_tmp.load_dttm , mobile_app_id = page_details_tmp.mobile_app_id , page_complete_sec_cnt = page_details_tmp.page_complete_sec_cnt , page_desc = page_details_tmp.page_desc , page_load_sec_cnt = page_details_tmp.page_load_sec_cnt , page_url_txt = page_details_tmp.page_url_txt , protocol_nm = page_details_tmp.protocol_nm , referrer_url_txt = page_details_tmp.referrer_url_txt , session_dt = page_details_tmp.session_dt , session_dt_tz = page_details_tmp.session_dt_tz , session_id = page_details_tmp.session_id , session_id_hex = page_details_tmp.session_id_hex , url_domain = page_details_tmp.url_domain , visit_id = page_details_tmp.visit_id , visit_id_hex = page_details_tmp.visit_id_hex , window_size_txt = page_details_tmp.window_size_txt
        WHEN NOT MATCHED THEN INSERT ( 
        bytes_sent_cnt,channel_nm,class10_id,class11_id,class12_id,class13_id,class14_id,class15_id,class1_id,class2_id,class3_id,class4_id,class5_id,class6_id,class7_id,class8_id,class9_id,detail_dttm,detail_dttm_tz,detail_id,detail_id_hex,domain_nm,event_id,event_key_cd,event_nm,event_source_cd,identity_id,load_dttm,mobile_app_id,page_complete_sec_cnt,page_desc,page_load_sec_cnt,page_url_txt,protocol_nm,referrer_url_txt,session_dt,session_dt_tz,session_id,session_id_hex,url_domain,visit_id,visit_id_hex,window_size_txt
         ) values ( 
        page_details_tmp.bytes_sent_cnt,page_details_tmp.channel_nm,page_details_tmp.class10_id,page_details_tmp.class11_id,page_details_tmp.class12_id,page_details_tmp.class13_id,page_details_tmp.class14_id,page_details_tmp.class15_id,page_details_tmp.class1_id,page_details_tmp.class2_id,page_details_tmp.class3_id,page_details_tmp.class4_id,page_details_tmp.class5_id,page_details_tmp.class6_id,page_details_tmp.class7_id,page_details_tmp.class8_id,page_details_tmp.class9_id,page_details_tmp.detail_dttm,page_details_tmp.detail_dttm_tz,page_details_tmp.detail_id,page_details_tmp.detail_id_hex,page_details_tmp.domain_nm,page_details_tmp.event_id,page_details_tmp.event_key_cd,page_details_tmp.event_nm,page_details_tmp.event_source_cd,page_details_tmp.identity_id,page_details_tmp.load_dttm,page_details_tmp.mobile_app_id,page_details_tmp.page_complete_sec_cnt,page_details_tmp.page_desc,page_details_tmp.page_load_sec_cnt,page_details_tmp.page_url_txt,page_details_tmp.protocol_nm,page_details_tmp.referrer_url_txt,page_details_tmp.session_dt,page_details_tmp.session_dt_tz,page_details_tmp.session_id,page_details_tmp.session_id_hex,page_details_tmp.url_domain,page_details_tmp.visit_id,page_details_tmp.visit_id_hex,page_details_tmp.window_size_txt
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :page_details_tmp                , page_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..page_details_tmp                ;
    QUIT;
    %put ######## Staging table: page_details_tmp                 Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..page_details;
      DROP TABLE work.page_details;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table page_details;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..page_details_ext) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..page_details_ext_tmp            ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..page_details_ext_tmp            ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=page_details_ext, table_keys=%str(detail_id,load_dttm,session_id), out_table=work.page_details_ext);
 data &tmplib..page_details_ext_tmp            ;
     set work.page_details_ext;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if detail_id='' then detail_id='-'; if session_id='' then session_id='-';
 run;
 %ErrCheck (Failed to Append Data to :page_details_ext_tmp            , page_details_ext);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..page_details_ext using &tmpdbschema..page_details_ext_tmp            
         ON (page_details_ext.detail_id=page_details_ext_tmp.detail_id and page_details_ext.load_dttm=page_details_ext_tmp.load_dttm and page_details_ext.session_id=page_details_ext_tmp.session_id)
        WHEN MATCHED THEN  
        UPDATE SET active_sec_spent_on_page_cnt = page_details_ext_tmp.active_sec_spent_on_page_cnt , detail_id_hex = page_details_ext_tmp.detail_id_hex , seconds_spent_on_page_cnt = page_details_ext_tmp.seconds_spent_on_page_cnt , session_id_hex = page_details_ext_tmp.session_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        active_sec_spent_on_page_cnt,detail_id,detail_id_hex,load_dttm,seconds_spent_on_page_cnt,session_id,session_id_hex
         ) values ( 
        page_details_ext_tmp.active_sec_spent_on_page_cnt,page_details_ext_tmp.detail_id,page_details_ext_tmp.detail_id_hex,page_details_ext_tmp.load_dttm,page_details_ext_tmp.seconds_spent_on_page_cnt,page_details_ext_tmp.session_id,page_details_ext_tmp.session_id_hex
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :page_details_ext_tmp            , page_details_ext, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..page_details_ext_tmp            ;
    QUIT;
    %put ######## Staging table: page_details_ext_tmp             Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..page_details_ext;
      DROP TABLE work.page_details_ext;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table page_details_ext;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..page_errors) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..page_errors_tmp                 ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..page_errors_tmp                 ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=page_errors, table_keys=%str(event_id), out_table=work.page_errors);
 data &tmplib..page_errors_tmp                 ;
     set work.page_errors;
  if in_page_error_dttm ne . then in_page_error_dttm = tzoneu2s(in_page_error_dttm,&timeZone_Value.);if in_page_error_dttm_tz ne . then in_page_error_dttm_tz = tzoneu2s(in_page_error_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :page_errors_tmp                 , page_errors);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..page_errors using &tmpdbschema..page_errors_tmp                 
         ON (page_errors.event_id=page_errors_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET detail_id = page_errors_tmp.detail_id , detail_id_hex = page_errors_tmp.detail_id_hex , error_location_txt = page_errors_tmp.error_location_txt , event_source_cd = page_errors_tmp.event_source_cd , identity_id = page_errors_tmp.identity_id , in_page_error_dttm = page_errors_tmp.in_page_error_dttm , in_page_error_dttm_tz = page_errors_tmp.in_page_error_dttm_tz , in_page_error_txt = page_errors_tmp.in_page_error_txt , load_dttm = page_errors_tmp.load_dttm , session_id = page_errors_tmp.session_id , session_id_hex = page_errors_tmp.session_id_hex , visit_id = page_errors_tmp.visit_id , visit_id_hex = page_errors_tmp.visit_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        detail_id,detail_id_hex,error_location_txt,event_id,event_source_cd,identity_id,in_page_error_dttm,in_page_error_dttm_tz,in_page_error_txt,load_dttm,session_id,session_id_hex,visit_id,visit_id_hex
         ) values ( 
        page_errors_tmp.detail_id,page_errors_tmp.detail_id_hex,page_errors_tmp.error_location_txt,page_errors_tmp.event_id,page_errors_tmp.event_source_cd,page_errors_tmp.identity_id,page_errors_tmp.in_page_error_dttm,page_errors_tmp.in_page_error_dttm_tz,page_errors_tmp.in_page_error_txt,page_errors_tmp.load_dttm,page_errors_tmp.session_id,page_errors_tmp.session_id_hex,page_errors_tmp.visit_id,page_errors_tmp.visit_id_hex
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :page_errors_tmp                 , page_errors, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..page_errors_tmp                 ;
    QUIT;
    %put ######## Staging table: page_errors_tmp                  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..page_errors;
      DROP TABLE work.page_errors;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table page_errors;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..planning_hierarchy_defn) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..planning_hierarchy_defn_tmp     ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..planning_hierarchy_defn_tmp     ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=planning_hierarchy_defn, table_keys=%str(hier_defn_id,level_nm,level_no), out_table=work.planning_hierarchy_defn);
 data &tmplib..planning_hierarchy_defn_tmp     ;
     set work.planning_hierarchy_defn;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if hier_defn_id='' then hier_defn_id='-'; if level_nm='' then level_nm='-';
 run;
 %ErrCheck (Failed to Append Data to :planning_hierarchy_defn_tmp     , planning_hierarchy_defn);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..planning_hierarchy_defn using &tmpdbschema..planning_hierarchy_defn_tmp     
         ON (planning_hierarchy_defn.hier_defn_id=planning_hierarchy_defn_tmp.hier_defn_id and planning_hierarchy_defn.level_nm=planning_hierarchy_defn_tmp.level_nm and planning_hierarchy_defn.level_no=planning_hierarchy_defn_tmp.level_no)
        WHEN MATCHED THEN  
        UPDATE SET created_by_usernm = planning_hierarchy_defn_tmp.created_by_usernm , created_dttm = planning_hierarchy_defn_tmp.created_dttm , hier_defn_desc = planning_hierarchy_defn_tmp.hier_defn_desc , hier_defn_nm = planning_hierarchy_defn_tmp.hier_defn_nm , hier_defn_subtype = planning_hierarchy_defn_tmp.hier_defn_subtype , hier_defn_type = planning_hierarchy_defn_tmp.hier_defn_type , last_modified_dttm = planning_hierarchy_defn_tmp.last_modified_dttm , last_modified_usernm = planning_hierarchy_defn_tmp.last_modified_usernm , level_desc = planning_hierarchy_defn_tmp.level_desc , load_dttm = planning_hierarchy_defn_tmp.load_dttm
        WHEN NOT MATCHED THEN INSERT ( 
        created_by_usernm,created_dttm,hier_defn_desc,hier_defn_id,hier_defn_nm,hier_defn_subtype,hier_defn_type,last_modified_dttm,last_modified_usernm,level_desc,level_nm,level_no,load_dttm
         ) values ( 
        planning_hierarchy_defn_tmp.created_by_usernm,planning_hierarchy_defn_tmp.created_dttm,planning_hierarchy_defn_tmp.hier_defn_desc,planning_hierarchy_defn_tmp.hier_defn_id,planning_hierarchy_defn_tmp.hier_defn_nm,planning_hierarchy_defn_tmp.hier_defn_subtype,planning_hierarchy_defn_tmp.hier_defn_type,planning_hierarchy_defn_tmp.last_modified_dttm,planning_hierarchy_defn_tmp.last_modified_usernm,planning_hierarchy_defn_tmp.level_desc,planning_hierarchy_defn_tmp.level_nm,planning_hierarchy_defn_tmp.level_no,planning_hierarchy_defn_tmp.load_dttm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :planning_hierarchy_defn_tmp     , planning_hierarchy_defn, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..planning_hierarchy_defn_tmp     ;
    QUIT;
    %put ######## Staging table: planning_hierarchy_defn_tmp      Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..planning_hierarchy_defn;
      DROP TABLE work.planning_hierarchy_defn;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table planning_hierarchy_defn;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..planning_info) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..planning_info_tmp               ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..planning_info_tmp               ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=planning_info, table_keys=%str(hier_defn_id,planning_id), out_table=work.planning_info);
 data &tmplib..planning_info_tmp               ;
     set work.planning_info;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if planned_end_dttm ne . then planned_end_dttm = tzoneu2s(planned_end_dttm,&timeZone_Value.);if planned_start_dttm ne . then planned_start_dttm = tzoneu2s(planned_start_dttm,&timeZone_Value.) ;
  if hier_defn_id='' then hier_defn_id='-'; if planning_id='' then planning_id='-';
 run;
 %ErrCheck (Failed to Append Data to :planning_info_tmp               , planning_info);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..planning_info using &tmpdbschema..planning_info_tmp               
         ON (planning_info.hier_defn_id=planning_info_tmp.hier_defn_id and planning_info.planning_id=planning_info_tmp.planning_id)
        WHEN MATCHED THEN  
        UPDATE SET activity_desc = planning_info_tmp.activity_desc , activity_id = planning_info_tmp.activity_id , activity_nm = planning_info_tmp.activity_nm , activity_status = planning_info_tmp.activity_status , all_msgs = planning_info_tmp.all_msgs , alloc_budget = planning_info_tmp.alloc_budget , available_budget = planning_info_tmp.available_budget , bu_currency_cd = planning_info_tmp.bu_currency_cd , bu_desc = planning_info_tmp.bu_desc , bu_id = planning_info_tmp.bu_id , bu_nm = planning_info_tmp.bu_nm , bu_obsolete_flg = planning_info_tmp.bu_obsolete_flg , category_nm = planning_info_tmp.category_nm , created_by_usernm = planning_info_tmp.created_by_usernm , created_dttm = planning_info_tmp.created_dttm , currency_cd = planning_info_tmp.currency_cd , hier_defn_nodeid = planning_info_tmp.hier_defn_nodeid , last_modified_dttm = planning_info_tmp.last_modified_dttm , last_modified_usernm = planning_info_tmp.last_modified_usernm , lev10_nm = planning_info_tmp.lev10_nm , lev1_nm = planning_info_tmp.lev1_nm , lev2_nm = planning_info_tmp.lev2_nm , lev3_nm = planning_info_tmp.lev3_nm , lev4_nm = planning_info_tmp.lev4_nm , lev5_nm = planning_info_tmp.lev5_nm , lev6_nm = planning_info_tmp.lev6_nm , lev7_nm = planning_info_tmp.lev7_nm , lev8_nm = planning_info_tmp.lev8_nm , lev9_nm = planning_info_tmp.lev9_nm , load_dttm = planning_info_tmp.load_dttm , parent_id = planning_info_tmp.parent_id , parent_nm = planning_info_tmp.parent_nm , planned_end_dttm = planning_info_tmp.planned_end_dttm , planned_start_dttm = planning_info_tmp.planned_start_dttm , planning_desc = planning_info_tmp.planning_desc , planning_item_path = planning_info_tmp.planning_item_path , planning_level_no = planning_info_tmp.planning_level_no , planning_level_type = planning_info_tmp.planning_level_type , planning_nm = planning_info_tmp.planning_nm , planning_number = planning_info_tmp.planning_number , planning_owner_usernm = planning_info_tmp.planning_owner_usernm , planning_status = planning_info_tmp.planning_status , planning_type = planning_info_tmp.planning_type , reserved_budget = planning_info_tmp.reserved_budget , reserved_budget_same_flg = planning_info_tmp.reserved_budget_same_flg , rolledup_budget = planning_info_tmp.rolledup_budget , task_channel = planning_info_tmp.task_channel , task_desc = planning_info_tmp.task_desc , task_id = planning_info_tmp.task_id , task_nm = planning_info_tmp.task_nm , task_status = planning_info_tmp.task_status , tot_cmtmnt_outstanding = planning_info_tmp.tot_cmtmnt_outstanding , tot_cmtmnt_overspent = planning_info_tmp.tot_cmtmnt_overspent , tot_committed = planning_info_tmp.tot_committed , tot_expenses = planning_info_tmp.tot_expenses , tot_invoiced = planning_info_tmp.tot_invoiced , total_budget = planning_info_tmp.total_budget
        WHEN NOT MATCHED THEN INSERT ( 
        activity_desc,activity_id,activity_nm,activity_status,all_msgs,alloc_budget,available_budget,bu_currency_cd,bu_desc,bu_id,bu_nm,bu_obsolete_flg,category_nm,created_by_usernm,created_dttm,currency_cd,hier_defn_id,hier_defn_nodeid,last_modified_dttm,last_modified_usernm,lev10_nm,lev1_nm,lev2_nm,lev3_nm,lev4_nm,lev5_nm,lev6_nm,lev7_nm,lev8_nm,lev9_nm,load_dttm,parent_id,parent_nm,planned_end_dttm,planned_start_dttm,planning_desc,planning_id,planning_item_path,planning_level_no,planning_level_type,planning_nm,planning_number,planning_owner_usernm,planning_status,planning_type,reserved_budget,reserved_budget_same_flg,rolledup_budget,task_channel,task_desc,task_id,task_nm,task_status,tot_cmtmnt_outstanding,tot_cmtmnt_overspent,tot_committed,tot_expenses,tot_invoiced,total_budget
         ) values ( 
        planning_info_tmp.activity_desc,planning_info_tmp.activity_id,planning_info_tmp.activity_nm,planning_info_tmp.activity_status,planning_info_tmp.all_msgs,planning_info_tmp.alloc_budget,planning_info_tmp.available_budget,planning_info_tmp.bu_currency_cd,planning_info_tmp.bu_desc,planning_info_tmp.bu_id,planning_info_tmp.bu_nm,planning_info_tmp.bu_obsolete_flg,planning_info_tmp.category_nm,planning_info_tmp.created_by_usernm,planning_info_tmp.created_dttm,planning_info_tmp.currency_cd,planning_info_tmp.hier_defn_id,planning_info_tmp.hier_defn_nodeid,planning_info_tmp.last_modified_dttm,planning_info_tmp.last_modified_usernm,planning_info_tmp.lev10_nm,planning_info_tmp.lev1_nm,planning_info_tmp.lev2_nm,planning_info_tmp.lev3_nm,planning_info_tmp.lev4_nm,planning_info_tmp.lev5_nm,planning_info_tmp.lev6_nm,planning_info_tmp.lev7_nm,planning_info_tmp.lev8_nm,planning_info_tmp.lev9_nm,planning_info_tmp.load_dttm,planning_info_tmp.parent_id,planning_info_tmp.parent_nm,planning_info_tmp.planned_end_dttm,planning_info_tmp.planned_start_dttm,planning_info_tmp.planning_desc,planning_info_tmp.planning_id,planning_info_tmp.planning_item_path,planning_info_tmp.planning_level_no,planning_info_tmp.planning_level_type,planning_info_tmp.planning_nm,planning_info_tmp.planning_number,planning_info_tmp.planning_owner_usernm,planning_info_tmp.planning_status,planning_info_tmp.planning_type,planning_info_tmp.reserved_budget,planning_info_tmp.reserved_budget_same_flg,planning_info_tmp.rolledup_budget,planning_info_tmp.task_channel,planning_info_tmp.task_desc,planning_info_tmp.task_id,planning_info_tmp.task_nm,planning_info_tmp.task_status,planning_info_tmp.tot_cmtmnt_outstanding,planning_info_tmp.tot_cmtmnt_overspent,planning_info_tmp.tot_committed,planning_info_tmp.tot_expenses,planning_info_tmp.tot_invoiced,planning_info_tmp.total_budget
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :planning_info_tmp               , planning_info, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..planning_info_tmp               ;
    QUIT;
    %put ######## Staging table: planning_info_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..planning_info;
      DROP TABLE work.planning_info;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table planning_info;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..planning_info_custom_prop) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..planning_info_custom_prop_tmp   ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..planning_info_custom_prop_tmp   ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=planning_info_custom_prop, table_keys=%str(attr_group_id,attr_id,planning_id), out_table=work.planning_info_custom_prop);
 data &tmplib..planning_info_custom_prop_tmp   ;
     set work.planning_info_custom_prop;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if attr_group_id='' then attr_group_id='-'; if attr_id='' then attr_id='-'; if planning_id='' then planning_id='-';
 run;
 %ErrCheck (Failed to Append Data to :planning_info_custom_prop_tmp   , planning_info_custom_prop);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..planning_info_custom_prop using &tmpdbschema..planning_info_custom_prop_tmp   
         ON (planning_info_custom_prop.attr_group_id=planning_info_custom_prop_tmp.attr_group_id and planning_info_custom_prop.attr_id=planning_info_custom_prop_tmp.attr_id and planning_info_custom_prop.planning_id=planning_info_custom_prop_tmp.planning_id)
        WHEN MATCHED THEN  
        UPDATE SET attr_cd = planning_info_custom_prop_tmp.attr_cd , attr_group_cd = planning_info_custom_prop_tmp.attr_group_cd , attr_group_nm = planning_info_custom_prop_tmp.attr_group_nm , attr_nm = planning_info_custom_prop_tmp.attr_nm , attr_val = planning_info_custom_prop_tmp.attr_val , created_by_usernm = planning_info_custom_prop_tmp.created_by_usernm , created_dttm = planning_info_custom_prop_tmp.created_dttm , data_formatter = planning_info_custom_prop_tmp.data_formatter , data_type = planning_info_custom_prop_tmp.data_type , is_grid_flg = planning_info_custom_prop_tmp.is_grid_flg , is_obsolete_flg = planning_info_custom_prop_tmp.is_obsolete_flg , last_modified_dttm = planning_info_custom_prop_tmp.last_modified_dttm , last_modified_usernm = planning_info_custom_prop_tmp.last_modified_usernm , load_dttm = planning_info_custom_prop_tmp.load_dttm , remote_pklist_tab_col = planning_info_custom_prop_tmp.remote_pklist_tab_col
        WHEN NOT MATCHED THEN INSERT ( 
        attr_cd,attr_group_cd,attr_group_id,attr_group_nm,attr_id,attr_nm,attr_val,created_by_usernm,created_dttm,data_formatter,data_type,is_grid_flg,is_obsolete_flg,last_modified_dttm,last_modified_usernm,load_dttm,planning_id,remote_pklist_tab_col
         ) values ( 
        planning_info_custom_prop_tmp.attr_cd,planning_info_custom_prop_tmp.attr_group_cd,planning_info_custom_prop_tmp.attr_group_id,planning_info_custom_prop_tmp.attr_group_nm,planning_info_custom_prop_tmp.attr_id,planning_info_custom_prop_tmp.attr_nm,planning_info_custom_prop_tmp.attr_val,planning_info_custom_prop_tmp.created_by_usernm,planning_info_custom_prop_tmp.created_dttm,planning_info_custom_prop_tmp.data_formatter,planning_info_custom_prop_tmp.data_type,planning_info_custom_prop_tmp.is_grid_flg,planning_info_custom_prop_tmp.is_obsolete_flg,planning_info_custom_prop_tmp.last_modified_dttm,planning_info_custom_prop_tmp.last_modified_usernm,planning_info_custom_prop_tmp.load_dttm,planning_info_custom_prop_tmp.planning_id,planning_info_custom_prop_tmp.remote_pklist_tab_col
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :planning_info_custom_prop_tmp   , planning_info_custom_prop, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..planning_info_custom_prop_tmp   ;
    QUIT;
    %put ######## Staging table: planning_info_custom_prop_tmp    Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..planning_info_custom_prop;
      DROP TABLE work.planning_info_custom_prop;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table planning_info_custom_prop;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..product_views) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..product_views_tmp               ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..product_views_tmp               ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=product_views, table_keys=%str(detail_id,product_id,product_nm,product_sku), out_table=work.product_views);
 data &tmplib..product_views_tmp               ;
     set work.product_views;
  if action_dttm ne . then action_dttm = tzoneu2s(action_dttm,&timeZone_Value.);if action_dttm_tz ne . then action_dttm_tz = tzoneu2s(action_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if detail_id='' then detail_id='-'; if product_id='' then product_id='-'; if product_nm='' then product_nm='-'; if product_sku='' then product_sku='-';
 run;
 %ErrCheck (Failed to Append Data to :product_views_tmp               , product_views);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..product_views using &tmpdbschema..product_views_tmp               
         ON (product_views.detail_id=product_views_tmp.detail_id and product_views.product_id=product_views_tmp.product_id and product_views.product_nm=product_views_tmp.product_nm and product_views.product_sku=product_views_tmp.product_sku)
        WHEN MATCHED THEN  
        UPDATE SET action_dttm = product_views_tmp.action_dttm , action_dttm_tz = product_views_tmp.action_dttm_tz , availability_message_txt = product_views_tmp.availability_message_txt , channel_nm = product_views_tmp.channel_nm , currency_cd = product_views_tmp.currency_cd , detail_id_hex = product_views_tmp.detail_id_hex , event_designed_id = product_views_tmp.event_designed_id , event_id = product_views_tmp.event_id , event_key_cd = product_views_tmp.event_key_cd , event_nm = product_views_tmp.event_nm , event_source_cd = product_views_tmp.event_source_cd , identity_id = product_views_tmp.identity_id , load_dttm = product_views_tmp.load_dttm , mobile_app_id = product_views_tmp.mobile_app_id , price_val = product_views_tmp.price_val , product_group_nm = product_views_tmp.product_group_nm , properties_map_doc = product_views_tmp.properties_map_doc , saving_message_txt = product_views_tmp.saving_message_txt , session_id = product_views_tmp.session_id , session_id_hex = product_views_tmp.session_id_hex , shipping_message_txt = product_views_tmp.shipping_message_txt , visit_id = product_views_tmp.visit_id , visit_id_hex = product_views_tmp.visit_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        action_dttm,action_dttm_tz,availability_message_txt,channel_nm,currency_cd,detail_id,detail_id_hex,event_designed_id,event_id,event_key_cd,event_nm,event_source_cd,identity_id,load_dttm,mobile_app_id,price_val,product_group_nm,product_id,product_nm,product_sku,properties_map_doc,saving_message_txt,session_id,session_id_hex,shipping_message_txt,visit_id,visit_id_hex
         ) values ( 
        product_views_tmp.action_dttm,product_views_tmp.action_dttm_tz,product_views_tmp.availability_message_txt,product_views_tmp.channel_nm,product_views_tmp.currency_cd,product_views_tmp.detail_id,product_views_tmp.detail_id_hex,product_views_tmp.event_designed_id,product_views_tmp.event_id,product_views_tmp.event_key_cd,product_views_tmp.event_nm,product_views_tmp.event_source_cd,product_views_tmp.identity_id,product_views_tmp.load_dttm,product_views_tmp.mobile_app_id,product_views_tmp.price_val,product_views_tmp.product_group_nm,product_views_tmp.product_id,product_views_tmp.product_nm,product_views_tmp.product_sku,product_views_tmp.properties_map_doc,product_views_tmp.saving_message_txt,product_views_tmp.session_id,product_views_tmp.session_id_hex,product_views_tmp.shipping_message_txt,product_views_tmp.visit_id,product_views_tmp.visit_id_hex
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :product_views_tmp               , product_views, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..product_views_tmp               ;
    QUIT;
    %put ######## Staging table: product_views_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..product_views;
      DROP TABLE work.product_views;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table product_views;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..promotion_displayed) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..promotion_displayed_tmp         ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..promotion_displayed_tmp         ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=promotion_displayed, table_keys=%str(event_id), out_table=work.promotion_displayed);
 data &tmplib..promotion_displayed_tmp         ;
     set work.promotion_displayed;
  if display_dttm ne . then display_dttm = tzoneu2s(display_dttm,&timeZone_Value.);if display_dttm_tz ne . then display_dttm_tz = tzoneu2s(display_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :promotion_displayed_tmp         , promotion_displayed);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..promotion_displayed using &tmpdbschema..promotion_displayed_tmp         
         ON (promotion_displayed.event_id=promotion_displayed_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET channel_nm = promotion_displayed_tmp.channel_nm , derived_display_flg = promotion_displayed_tmp.derived_display_flg , detail_id = promotion_displayed_tmp.detail_id , detail_id_hex = promotion_displayed_tmp.detail_id_hex , display_dttm = promotion_displayed_tmp.display_dttm , display_dttm_tz = promotion_displayed_tmp.display_dttm_tz , event_designed_id = promotion_displayed_tmp.event_designed_id , event_key_cd = promotion_displayed_tmp.event_key_cd , event_nm = promotion_displayed_tmp.event_nm , event_source_cd = promotion_displayed_tmp.event_source_cd , identity_id = promotion_displayed_tmp.identity_id , load_dttm = promotion_displayed_tmp.load_dttm , mobile_app_id = promotion_displayed_tmp.mobile_app_id , promotion_creative_nm = promotion_displayed_tmp.promotion_creative_nm , promotion_nm = promotion_displayed_tmp.promotion_nm , promotion_number = promotion_displayed_tmp.promotion_number , promotion_placement_nm = promotion_displayed_tmp.promotion_placement_nm , promotion_tracking_cd = promotion_displayed_tmp.promotion_tracking_cd , promotion_type_nm = promotion_displayed_tmp.promotion_type_nm , properties_map_doc = promotion_displayed_tmp.properties_map_doc , session_id = promotion_displayed_tmp.session_id , session_id_hex = promotion_displayed_tmp.session_id_hex , visit_id = promotion_displayed_tmp.visit_id , visit_id_hex = promotion_displayed_tmp.visit_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        channel_nm,derived_display_flg,detail_id,detail_id_hex,display_dttm,display_dttm_tz,event_designed_id,event_id,event_key_cd,event_nm,event_source_cd,identity_id,load_dttm,mobile_app_id,promotion_creative_nm,promotion_nm,promotion_number,promotion_placement_nm,promotion_tracking_cd,promotion_type_nm,properties_map_doc,session_id,session_id_hex,visit_id,visit_id_hex
         ) values ( 
        promotion_displayed_tmp.channel_nm,promotion_displayed_tmp.derived_display_flg,promotion_displayed_tmp.detail_id,promotion_displayed_tmp.detail_id_hex,promotion_displayed_tmp.display_dttm,promotion_displayed_tmp.display_dttm_tz,promotion_displayed_tmp.event_designed_id,promotion_displayed_tmp.event_id,promotion_displayed_tmp.event_key_cd,promotion_displayed_tmp.event_nm,promotion_displayed_tmp.event_source_cd,promotion_displayed_tmp.identity_id,promotion_displayed_tmp.load_dttm,promotion_displayed_tmp.mobile_app_id,promotion_displayed_tmp.promotion_creative_nm,promotion_displayed_tmp.promotion_nm,promotion_displayed_tmp.promotion_number,promotion_displayed_tmp.promotion_placement_nm,promotion_displayed_tmp.promotion_tracking_cd,promotion_displayed_tmp.promotion_type_nm,promotion_displayed_tmp.properties_map_doc,promotion_displayed_tmp.session_id,promotion_displayed_tmp.session_id_hex,promotion_displayed_tmp.visit_id,promotion_displayed_tmp.visit_id_hex
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :promotion_displayed_tmp         , promotion_displayed, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..promotion_displayed_tmp         ;
    QUIT;
    %put ######## Staging table: promotion_displayed_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..promotion_displayed;
      DROP TABLE work.promotion_displayed;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table promotion_displayed;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..promotion_used) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..promotion_used_tmp              ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..promotion_used_tmp              ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=promotion_used, table_keys=%str(event_id), out_table=work.promotion_used);
 data &tmplib..promotion_used_tmp              ;
     set work.promotion_used;
  if click_dttm ne . then click_dttm = tzoneu2s(click_dttm,&timeZone_Value.);if click_dttm_tz ne . then click_dttm_tz = tzoneu2s(click_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :promotion_used_tmp              , promotion_used);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..promotion_used using &tmpdbschema..promotion_used_tmp              
         ON (promotion_used.event_id=promotion_used_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET channel_nm = promotion_used_tmp.channel_nm , click_dttm = promotion_used_tmp.click_dttm , click_dttm_tz = promotion_used_tmp.click_dttm_tz , detail_id = promotion_used_tmp.detail_id , detail_id_hex = promotion_used_tmp.detail_id_hex , event_designed_id = promotion_used_tmp.event_designed_id , event_key_cd = promotion_used_tmp.event_key_cd , event_nm = promotion_used_tmp.event_nm , event_source_cd = promotion_used_tmp.event_source_cd , identity_id = promotion_used_tmp.identity_id , load_dttm = promotion_used_tmp.load_dttm , mobile_app_id = promotion_used_tmp.mobile_app_id , promotion_creative_nm = promotion_used_tmp.promotion_creative_nm , promotion_nm = promotion_used_tmp.promotion_nm , promotion_number = promotion_used_tmp.promotion_number , promotion_placement_nm = promotion_used_tmp.promotion_placement_nm , promotion_tracking_cd = promotion_used_tmp.promotion_tracking_cd , promotion_type_nm = promotion_used_tmp.promotion_type_nm , properties_map_doc = promotion_used_tmp.properties_map_doc , session_id = promotion_used_tmp.session_id , session_id_hex = promotion_used_tmp.session_id_hex , visit_id = promotion_used_tmp.visit_id , visit_id_hex = promotion_used_tmp.visit_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        channel_nm,click_dttm,click_dttm_tz,detail_id,detail_id_hex,event_designed_id,event_id,event_key_cd,event_nm,event_source_cd,identity_id,load_dttm,mobile_app_id,promotion_creative_nm,promotion_nm,promotion_number,promotion_placement_nm,promotion_tracking_cd,promotion_type_nm,properties_map_doc,session_id,session_id_hex,visit_id,visit_id_hex
         ) values ( 
        promotion_used_tmp.channel_nm,promotion_used_tmp.click_dttm,promotion_used_tmp.click_dttm_tz,promotion_used_tmp.detail_id,promotion_used_tmp.detail_id_hex,promotion_used_tmp.event_designed_id,promotion_used_tmp.event_id,promotion_used_tmp.event_key_cd,promotion_used_tmp.event_nm,promotion_used_tmp.event_source_cd,promotion_used_tmp.identity_id,promotion_used_tmp.load_dttm,promotion_used_tmp.mobile_app_id,promotion_used_tmp.promotion_creative_nm,promotion_used_tmp.promotion_nm,promotion_used_tmp.promotion_number,promotion_used_tmp.promotion_placement_nm,promotion_used_tmp.promotion_tracking_cd,promotion_used_tmp.promotion_type_nm,promotion_used_tmp.properties_map_doc,promotion_used_tmp.session_id,promotion_used_tmp.session_id_hex,promotion_used_tmp.visit_id,promotion_used_tmp.visit_id_hex
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :promotion_used_tmp              , promotion_used, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..promotion_used_tmp              ;
    QUIT;
    %put ######## Staging table: promotion_used_tmp               Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..promotion_used;
      DROP TABLE work.promotion_used;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table promotion_used;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..response_history) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..response_history_tmp            ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..response_history_tmp            ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=response_history, table_keys=%str(response_id), out_table=work.response_history);
 data &tmplib..response_history_tmp            ;
     set work.response_history;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if response_dttm ne . then response_dttm = tzoneu2s(response_dttm,&timeZone_Value.);if response_dttm_tz ne . then response_dttm_tz = tzoneu2s(response_dttm_tz,&timeZone_Value.) ;
  if response_id='' then response_id='-';
 run;
 %ErrCheck (Failed to Append Data to :response_history_tmp            , response_history);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..response_history using &tmpdbschema..response_history_tmp            
         ON (response_history.response_id=response_history_tmp.response_id)
        WHEN MATCHED THEN  
        UPDATE SET aud_occurrence_id = response_history_tmp.aud_occurrence_id , audience_id = response_history_tmp.audience_id , context_type_nm = response_history_tmp.context_type_nm , context_val = response_history_tmp.context_val , creative_id = response_history_tmp.creative_id , detail_id_hex = response_history_tmp.detail_id_hex , event_designed_id = response_history_tmp.event_designed_id , identity_id = response_history_tmp.identity_id , journey_id = response_history_tmp.journey_id , journey_occurrence_id = response_history_tmp.journey_occurrence_id , load_dttm = response_history_tmp.load_dttm , message_id = response_history_tmp.message_id , occurrence_id = response_history_tmp.occurrence_id , parent_event_designed_id = response_history_tmp.parent_event_designed_id , properties_map_doc = response_history_tmp.properties_map_doc , response_channel_nm = response_history_tmp.response_channel_nm , response_dttm = response_history_tmp.response_dttm , response_dttm_tz = response_history_tmp.response_dttm_tz , response_nm = response_history_tmp.response_nm , response_tracking_cd = response_history_tmp.response_tracking_cd , session_id_hex = response_history_tmp.session_id_hex , task_id = response_history_tmp.task_id , task_version_id = response_history_tmp.task_version_id , visit_id_hex = response_history_tmp.visit_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        aud_occurrence_id,audience_id,context_type_nm,context_val,creative_id,detail_id_hex,event_designed_id,identity_id,journey_id,journey_occurrence_id,load_dttm,message_id,occurrence_id,parent_event_designed_id,properties_map_doc,response_channel_nm,response_dttm,response_dttm_tz,response_id,response_nm,response_tracking_cd,session_id_hex,task_id,task_version_id,visit_id_hex
         ) values ( 
        response_history_tmp.aud_occurrence_id,response_history_tmp.audience_id,response_history_tmp.context_type_nm,response_history_tmp.context_val,response_history_tmp.creative_id,response_history_tmp.detail_id_hex,response_history_tmp.event_designed_id,response_history_tmp.identity_id,response_history_tmp.journey_id,response_history_tmp.journey_occurrence_id,response_history_tmp.load_dttm,response_history_tmp.message_id,response_history_tmp.occurrence_id,response_history_tmp.parent_event_designed_id,response_history_tmp.properties_map_doc,response_history_tmp.response_channel_nm,response_history_tmp.response_dttm,response_history_tmp.response_dttm_tz,response_history_tmp.response_id,response_history_tmp.response_nm,response_history_tmp.response_tracking_cd,response_history_tmp.session_id_hex,response_history_tmp.task_id,response_history_tmp.task_version_id,response_history_tmp.visit_id_hex
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :response_history_tmp            , response_history, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..response_history_tmp            ;
    QUIT;
    %put ######## Staging table: response_history_tmp             Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..response_history;
      DROP TABLE work.response_history;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table response_history;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..search_results) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..search_results_tmp              ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..search_results_tmp              ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=search_results, table_keys=%str(event_id), out_table=work.search_results);
 data &tmplib..search_results_tmp              ;
     set work.search_results;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if search_results_dttm ne . then search_results_dttm = tzoneu2s(search_results_dttm,&timeZone_Value.);if search_results_dttm_tz ne . then search_results_dttm_tz = tzoneu2s(search_results_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :search_results_tmp              , search_results);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..search_results using &tmpdbschema..search_results_tmp              
         ON (search_results.event_id=search_results_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET channel_nm = search_results_tmp.channel_nm , detail_id = search_results_tmp.detail_id , detail_id_hex = search_results_tmp.detail_id_hex , event_designed_id = search_results_tmp.event_designed_id , event_key_cd = search_results_tmp.event_key_cd , event_nm = search_results_tmp.event_nm , event_source_cd = search_results_tmp.event_source_cd , identity_id = search_results_tmp.identity_id , load_dttm = search_results_tmp.load_dttm , mobile_app_id = search_results_tmp.mobile_app_id , properties_map_doc = search_results_tmp.properties_map_doc , results_displayed_flg = search_results_tmp.results_displayed_flg , search_nm = search_results_tmp.search_nm , search_results_displayed = search_results_tmp.search_results_displayed , search_results_dttm = search_results_tmp.search_results_dttm , search_results_dttm_tz = search_results_tmp.search_results_dttm_tz , search_results_sk = search_results_tmp.search_results_sk , session_id = search_results_tmp.session_id , session_id_hex = search_results_tmp.session_id_hex , srch_field_id = search_results_tmp.srch_field_id , srch_field_name = search_results_tmp.srch_field_name , srch_phrase = search_results_tmp.srch_phrase , visit_id = search_results_tmp.visit_id , visit_id_hex = search_results_tmp.visit_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        channel_nm,detail_id,detail_id_hex,event_designed_id,event_id,event_key_cd,event_nm,event_source_cd,identity_id,load_dttm,mobile_app_id,properties_map_doc,results_displayed_flg,search_nm,search_results_displayed,search_results_dttm,search_results_dttm_tz,search_results_sk,session_id,session_id_hex,srch_field_id,srch_field_name,srch_phrase,visit_id,visit_id_hex
         ) values ( 
        search_results_tmp.channel_nm,search_results_tmp.detail_id,search_results_tmp.detail_id_hex,search_results_tmp.event_designed_id,search_results_tmp.event_id,search_results_tmp.event_key_cd,search_results_tmp.event_nm,search_results_tmp.event_source_cd,search_results_tmp.identity_id,search_results_tmp.load_dttm,search_results_tmp.mobile_app_id,search_results_tmp.properties_map_doc,search_results_tmp.results_displayed_flg,search_results_tmp.search_nm,search_results_tmp.search_results_displayed,search_results_tmp.search_results_dttm,search_results_tmp.search_results_dttm_tz,search_results_tmp.search_results_sk,search_results_tmp.session_id,search_results_tmp.session_id_hex,search_results_tmp.srch_field_id,search_results_tmp.srch_field_name,search_results_tmp.srch_phrase,search_results_tmp.visit_id,search_results_tmp.visit_id_hex
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :search_results_tmp              , search_results, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..search_results_tmp              ;
    QUIT;
    %put ######## Staging table: search_results_tmp               Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..search_results;
      DROP TABLE work.search_results;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table search_results;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..search_results_ext) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..search_results_ext_tmp          ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..search_results_ext_tmp          ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=search_results_ext, table_keys=%str(event_id), out_table=work.search_results_ext);
 data &tmplib..search_results_ext_tmp          ;
     set work.search_results_ext;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :search_results_ext_tmp          , search_results_ext);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..search_results_ext using &tmpdbschema..search_results_ext_tmp          
         ON (search_results_ext.event_id=search_results_ext_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET event_designed_id = search_results_ext_tmp.event_designed_id , load_dttm = search_results_ext_tmp.load_dttm , search_results_displayed = search_results_ext_tmp.search_results_displayed , search_results_sk = search_results_ext_tmp.search_results_sk
        WHEN NOT MATCHED THEN INSERT ( 
        event_designed_id,event_id,load_dttm,search_results_displayed,search_results_sk
         ) values ( 
        search_results_ext_tmp.event_designed_id,search_results_ext_tmp.event_id,search_results_ext_tmp.load_dttm,search_results_ext_tmp.search_results_displayed,search_results_ext_tmp.search_results_sk
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :search_results_ext_tmp          , search_results_ext, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..search_results_ext_tmp          ;
    QUIT;
    %put ######## Staging table: search_results_ext_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..search_results_ext;
      DROP TABLE work.search_results_ext;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table search_results_ext;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..session_details) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..session_details_tmp             ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..session_details_tmp             ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=session_details, table_keys=%str(session_id), out_table=work.session_details);
 data &tmplib..session_details_tmp             ;
     set work.session_details;
  if client_session_start_dttm ne . then client_session_start_dttm = tzoneu2s(client_session_start_dttm,&timeZone_Value.);if client_session_start_dttm_tz ne . then client_session_start_dttm_tz = tzoneu2s(client_session_start_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if session_start_dttm ne . then session_start_dttm = tzoneu2s(session_start_dttm,&timeZone_Value.);if session_start_dttm_tz ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz,&timeZone_Value.) ;
  if session_id='' then session_id='-';
 run;
 %ErrCheck (Failed to Append Data to :session_details_tmp             , session_details);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..session_details using &tmpdbschema..session_details_tmp             
         ON (session_details.session_id=session_details_tmp.session_id)
        WHEN MATCHED THEN  
        UPDATE SET app_id = session_details_tmp.app_id , app_version = session_details_tmp.app_version , browser_nm = session_details_tmp.browser_nm , browser_version_no = session_details_tmp.browser_version_no , carrier_name = session_details_tmp.carrier_name , channel_nm = session_details_tmp.channel_nm , city_nm = session_details_tmp.city_nm , client_session_start_dttm = session_details_tmp.client_session_start_dttm , client_session_start_dttm_tz = session_details_tmp.client_session_start_dttm_tz , cookies_enabled_flg = session_details_tmp.cookies_enabled_flg , country_cd = session_details_tmp.country_cd , country_nm = session_details_tmp.country_nm , device_language = session_details_tmp.device_language , device_nm = session_details_tmp.device_nm , device_type_nm = session_details_tmp.device_type_nm , event_id = session_details_tmp.event_id , flash_enabled_flg = session_details_tmp.flash_enabled_flg , flash_version_no = session_details_tmp.flash_version_no , identity_id = session_details_tmp.identity_id , ip_address = session_details_tmp.ip_address , is_portable_flag = session_details_tmp.is_portable_flag , java_enabled_flg = session_details_tmp.java_enabled_flg , java_script_enabled_flg = session_details_tmp.java_script_enabled_flg , java_version_no = session_details_tmp.java_version_no , latitude = session_details_tmp.latitude , load_dttm = session_details_tmp.load_dttm , longitude = session_details_tmp.longitude , manufacturer = session_details_tmp.manufacturer , metro_cd = session_details_tmp.metro_cd , mobile_country_code = session_details_tmp.mobile_country_code , network_code = session_details_tmp.network_code , new_visitor_flg = session_details_tmp.new_visitor_flg , organization_nm = session_details_tmp.organization_nm , parent_event_id = session_details_tmp.parent_event_id , platform_desc = session_details_tmp.platform_desc , platform_type_nm = session_details_tmp.platform_type_nm , platform_version = session_details_tmp.platform_version , postal_cd = session_details_tmp.postal_cd , previous_session_id = session_details_tmp.previous_session_id , previous_session_id_hex = session_details_tmp.previous_session_id_hex , profile_nm1 = session_details_tmp.profile_nm1 , profile_nm2 = session_details_tmp.profile_nm2 , profile_nm3 = session_details_tmp.profile_nm3 , profile_nm4 = session_details_tmp.profile_nm4 , profile_nm5 = session_details_tmp.profile_nm5 , region_nm = session_details_tmp.region_nm , screen_color_depth_no = session_details_tmp.screen_color_depth_no , screen_size_txt = session_details_tmp.screen_size_txt , sdk_version = session_details_tmp.sdk_version , session_dt = session_details_tmp.session_dt , session_dt_tz = session_details_tmp.session_dt_tz , session_id_hex = session_details_tmp.session_id_hex , session_start_dttm = session_details_tmp.session_start_dttm , session_start_dttm_tz = session_details_tmp.session_start_dttm_tz , session_timeout = session_details_tmp.session_timeout , state_region_cd = session_details_tmp.state_region_cd , user_agent_nm = session_details_tmp.user_agent_nm , user_language_cd = session_details_tmp.user_language_cd , visitor_id = session_details_tmp.visitor_id
        WHEN NOT MATCHED THEN INSERT ( 
        app_id,app_version,browser_nm,browser_version_no,carrier_name,channel_nm,city_nm,client_session_start_dttm,client_session_start_dttm_tz,cookies_enabled_flg,country_cd,country_nm,device_language,device_nm,device_type_nm,event_id,flash_enabled_flg,flash_version_no,identity_id,ip_address,is_portable_flag,java_enabled_flg,java_script_enabled_flg,java_version_no,latitude,load_dttm,longitude,manufacturer,metro_cd,mobile_country_code,network_code,new_visitor_flg,organization_nm,parent_event_id,platform_desc,platform_type_nm,platform_version,postal_cd,previous_session_id,previous_session_id_hex,profile_nm1,profile_nm2,profile_nm3,profile_nm4,profile_nm5,region_nm,screen_color_depth_no,screen_size_txt,sdk_version,session_dt,session_dt_tz,session_id,session_id_hex,session_start_dttm,session_start_dttm_tz,session_timeout,state_region_cd,user_agent_nm,user_language_cd,visitor_id
         ) values ( 
        session_details_tmp.app_id,session_details_tmp.app_version,session_details_tmp.browser_nm,session_details_tmp.browser_version_no,session_details_tmp.carrier_name,session_details_tmp.channel_nm,session_details_tmp.city_nm,session_details_tmp.client_session_start_dttm,session_details_tmp.client_session_start_dttm_tz,session_details_tmp.cookies_enabled_flg,session_details_tmp.country_cd,session_details_tmp.country_nm,session_details_tmp.device_language,session_details_tmp.device_nm,session_details_tmp.device_type_nm,session_details_tmp.event_id,session_details_tmp.flash_enabled_flg,session_details_tmp.flash_version_no,session_details_tmp.identity_id,session_details_tmp.ip_address,session_details_tmp.is_portable_flag,session_details_tmp.java_enabled_flg,session_details_tmp.java_script_enabled_flg,session_details_tmp.java_version_no,session_details_tmp.latitude,session_details_tmp.load_dttm,session_details_tmp.longitude,session_details_tmp.manufacturer,session_details_tmp.metro_cd,session_details_tmp.mobile_country_code,session_details_tmp.network_code,session_details_tmp.new_visitor_flg,session_details_tmp.organization_nm,session_details_tmp.parent_event_id,session_details_tmp.platform_desc,session_details_tmp.platform_type_nm,session_details_tmp.platform_version,session_details_tmp.postal_cd,session_details_tmp.previous_session_id,session_details_tmp.previous_session_id_hex,session_details_tmp.profile_nm1,session_details_tmp.profile_nm2,session_details_tmp.profile_nm3,session_details_tmp.profile_nm4,session_details_tmp.profile_nm5,session_details_tmp.region_nm,session_details_tmp.screen_color_depth_no,session_details_tmp.screen_size_txt,session_details_tmp.sdk_version,session_details_tmp.session_dt,session_details_tmp.session_dt_tz,session_details_tmp.session_id,session_details_tmp.session_id_hex,session_details_tmp.session_start_dttm,session_details_tmp.session_start_dttm_tz,session_details_tmp.session_timeout,session_details_tmp.state_region_cd,session_details_tmp.user_agent_nm,session_details_tmp.user_language_cd,session_details_tmp.visitor_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :session_details_tmp             , session_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..session_details_tmp             ;
    QUIT;
    %put ######## Staging table: session_details_tmp              Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..session_details;
      DROP TABLE work.session_details;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table session_details;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..session_details_ext) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..session_details_ext_tmp         ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..session_details_ext_tmp         ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=session_details_ext, table_keys=%str(last_session_activity_dttm,session_id), out_table=work.session_details_ext);
 data &tmplib..session_details_ext_tmp         ;
     set work.session_details_ext;
  if last_session_activity_dttm ne . then last_session_activity_dttm = tzoneu2s(last_session_activity_dttm,&timeZone_Value.);if last_session_activity_dttm_tz ne . then last_session_activity_dttm_tz = tzoneu2s(last_session_activity_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if session_expiration_dttm ne . then session_expiration_dttm = tzoneu2s(session_expiration_dttm,&timeZone_Value.);if session_expiration_dttm_tz ne . then session_expiration_dttm_tz = tzoneu2s(session_expiration_dttm_tz,&timeZone_Value.) ;
  if session_id='' then session_id='-';
 run;
 %ErrCheck (Failed to Append Data to :session_details_ext_tmp         , session_details_ext);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..session_details_ext using &tmpdbschema..session_details_ext_tmp         
         ON (session_details_ext.last_session_activity_dttm=session_details_ext_tmp.last_session_activity_dttm and session_details_ext.session_id=session_details_ext_tmp.session_id)
        WHEN MATCHED THEN  
        UPDATE SET active_sec_spent_in_sessn_cnt = session_details_ext_tmp.active_sec_spent_in_sessn_cnt , last_session_activity_dttm_tz = session_details_ext_tmp.last_session_activity_dttm_tz , load_dttm = session_details_ext_tmp.load_dttm , seconds_spent_in_session_cnt = session_details_ext_tmp.seconds_spent_in_session_cnt , session_expiration_dttm = session_details_ext_tmp.session_expiration_dttm , session_expiration_dttm_tz = session_details_ext_tmp.session_expiration_dttm_tz , session_id_hex = session_details_ext_tmp.session_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        active_sec_spent_in_sessn_cnt,last_session_activity_dttm,last_session_activity_dttm_tz,load_dttm,seconds_spent_in_session_cnt,session_expiration_dttm,session_expiration_dttm_tz,session_id,session_id_hex
         ) values ( 
        session_details_ext_tmp.active_sec_spent_in_sessn_cnt,session_details_ext_tmp.last_session_activity_dttm,session_details_ext_tmp.last_session_activity_dttm_tz,session_details_ext_tmp.load_dttm,session_details_ext_tmp.seconds_spent_in_session_cnt,session_details_ext_tmp.session_expiration_dttm,session_details_ext_tmp.session_expiration_dttm_tz,session_details_ext_tmp.session_id,session_details_ext_tmp.session_id_hex
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :session_details_ext_tmp         , session_details_ext, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..session_details_ext_tmp         ;
    QUIT;
    %put ######## Staging table: session_details_ext_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..session_details_ext;
      DROP TABLE work.session_details_ext;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table session_details_ext;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..sms_message_clicked) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..sms_message_clicked_tmp         ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..sms_message_clicked_tmp         ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=sms_message_clicked, table_keys=%str(event_id), out_table=work.sms_message_clicked);
 data &tmplib..sms_message_clicked_tmp         ;
     set work.sms_message_clicked;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if sms_click_dttm ne . then sms_click_dttm = tzoneu2s(sms_click_dttm,&timeZone_Value.);if sms_click_dttm_tz ne . then sms_click_dttm_tz = tzoneu2s(sms_click_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :sms_message_clicked_tmp         , sms_message_clicked);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..sms_message_clicked using &tmpdbschema..sms_message_clicked_tmp         
         ON (sms_message_clicked.event_id=sms_message_clicked_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET aud_occurrence_id = sms_message_clicked_tmp.aud_occurrence_id , audience_id = sms_message_clicked_tmp.audience_id , context_type_nm = sms_message_clicked_tmp.context_type_nm , context_val = sms_message_clicked_tmp.context_val , country_cd = sms_message_clicked_tmp.country_cd , creative_id = sms_message_clicked_tmp.creative_id , creative_version_id = sms_message_clicked_tmp.creative_version_id , event_designed_id = sms_message_clicked_tmp.event_designed_id , event_nm = sms_message_clicked_tmp.event_nm , identity_id = sms_message_clicked_tmp.identity_id , journey_id = sms_message_clicked_tmp.journey_id , journey_occurrence_id = sms_message_clicked_tmp.journey_occurrence_id , load_dttm = sms_message_clicked_tmp.load_dttm , occurrence_id = sms_message_clicked_tmp.occurrence_id , response_tracking_cd = sms_message_clicked_tmp.response_tracking_cd , sender_id = sms_message_clicked_tmp.sender_id , sms_click_dttm = sms_message_clicked_tmp.sms_click_dttm , sms_click_dttm_tz = sms_message_clicked_tmp.sms_click_dttm_tz , sms_message_id = sms_message_clicked_tmp.sms_message_id , task_id = sms_message_clicked_tmp.task_id , task_version_id = sms_message_clicked_tmp.task_version_id
        WHEN NOT MATCHED THEN INSERT ( 
        aud_occurrence_id,audience_id,context_type_nm,context_val,country_cd,creative_id,creative_version_id,event_designed_id,event_id,event_nm,identity_id,journey_id,journey_occurrence_id,load_dttm,occurrence_id,response_tracking_cd,sender_id,sms_click_dttm,sms_click_dttm_tz,sms_message_id,task_id,task_version_id
         ) values ( 
        sms_message_clicked_tmp.aud_occurrence_id,sms_message_clicked_tmp.audience_id,sms_message_clicked_tmp.context_type_nm,sms_message_clicked_tmp.context_val,sms_message_clicked_tmp.country_cd,sms_message_clicked_tmp.creative_id,sms_message_clicked_tmp.creative_version_id,sms_message_clicked_tmp.event_designed_id,sms_message_clicked_tmp.event_id,sms_message_clicked_tmp.event_nm,sms_message_clicked_tmp.identity_id,sms_message_clicked_tmp.journey_id,sms_message_clicked_tmp.journey_occurrence_id,sms_message_clicked_tmp.load_dttm,sms_message_clicked_tmp.occurrence_id,sms_message_clicked_tmp.response_tracking_cd,sms_message_clicked_tmp.sender_id,sms_message_clicked_tmp.sms_click_dttm,sms_message_clicked_tmp.sms_click_dttm_tz,sms_message_clicked_tmp.sms_message_id,sms_message_clicked_tmp.task_id,sms_message_clicked_tmp.task_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :sms_message_clicked_tmp         , sms_message_clicked, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..sms_message_clicked_tmp         ;
    QUIT;
    %put ######## Staging table: sms_message_clicked_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..sms_message_clicked;
      DROP TABLE work.sms_message_clicked;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table sms_message_clicked;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..sms_message_delivered) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..sms_message_delivered_tmp       ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..sms_message_delivered_tmp       ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=sms_message_delivered, table_keys=%str(event_id), out_table=work.sms_message_delivered);
 data &tmplib..sms_message_delivered_tmp       ;
     set work.sms_message_delivered;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if sms_delivered_dttm ne . then sms_delivered_dttm = tzoneu2s(sms_delivered_dttm,&timeZone_Value.);if sms_delivered_dttm_tz ne . then sms_delivered_dttm_tz = tzoneu2s(sms_delivered_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :sms_message_delivered_tmp       , sms_message_delivered);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..sms_message_delivered using &tmpdbschema..sms_message_delivered_tmp       
         ON (sms_message_delivered.event_id=sms_message_delivered_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET aud_occurrence_id = sms_message_delivered_tmp.aud_occurrence_id , audience_id = sms_message_delivered_tmp.audience_id , context_type_nm = sms_message_delivered_tmp.context_type_nm , context_val = sms_message_delivered_tmp.context_val , country_cd = sms_message_delivered_tmp.country_cd , creative_id = sms_message_delivered_tmp.creative_id , creative_version_id = sms_message_delivered_tmp.creative_version_id , event_designed_id = sms_message_delivered_tmp.event_designed_id , event_nm = sms_message_delivered_tmp.event_nm , identity_id = sms_message_delivered_tmp.identity_id , journey_id = sms_message_delivered_tmp.journey_id , journey_occurrence_id = sms_message_delivered_tmp.journey_occurrence_id , load_dttm = sms_message_delivered_tmp.load_dttm , occurrence_id = sms_message_delivered_tmp.occurrence_id , response_tracking_cd = sms_message_delivered_tmp.response_tracking_cd , sender_id = sms_message_delivered_tmp.sender_id , sms_delivered_dttm = sms_message_delivered_tmp.sms_delivered_dttm , sms_delivered_dttm_tz = sms_message_delivered_tmp.sms_delivered_dttm_tz , sms_message_id = sms_message_delivered_tmp.sms_message_id , task_id = sms_message_delivered_tmp.task_id , task_version_id = sms_message_delivered_tmp.task_version_id
        WHEN NOT MATCHED THEN INSERT ( 
        aud_occurrence_id,audience_id,context_type_nm,context_val,country_cd,creative_id,creative_version_id,event_designed_id,event_id,event_nm,identity_id,journey_id,journey_occurrence_id,load_dttm,occurrence_id,response_tracking_cd,sender_id,sms_delivered_dttm,sms_delivered_dttm_tz,sms_message_id,task_id,task_version_id
         ) values ( 
        sms_message_delivered_tmp.aud_occurrence_id,sms_message_delivered_tmp.audience_id,sms_message_delivered_tmp.context_type_nm,sms_message_delivered_tmp.context_val,sms_message_delivered_tmp.country_cd,sms_message_delivered_tmp.creative_id,sms_message_delivered_tmp.creative_version_id,sms_message_delivered_tmp.event_designed_id,sms_message_delivered_tmp.event_id,sms_message_delivered_tmp.event_nm,sms_message_delivered_tmp.identity_id,sms_message_delivered_tmp.journey_id,sms_message_delivered_tmp.journey_occurrence_id,sms_message_delivered_tmp.load_dttm,sms_message_delivered_tmp.occurrence_id,sms_message_delivered_tmp.response_tracking_cd,sms_message_delivered_tmp.sender_id,sms_message_delivered_tmp.sms_delivered_dttm,sms_message_delivered_tmp.sms_delivered_dttm_tz,sms_message_delivered_tmp.sms_message_id,sms_message_delivered_tmp.task_id,sms_message_delivered_tmp.task_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :sms_message_delivered_tmp       , sms_message_delivered, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..sms_message_delivered_tmp       ;
    QUIT;
    %put ######## Staging table: sms_message_delivered_tmp        Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..sms_message_delivered;
      DROP TABLE work.sms_message_delivered;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table sms_message_delivered;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..sms_message_failed) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..sms_message_failed_tmp          ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..sms_message_failed_tmp          ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=sms_message_failed, table_keys=%str(event_id), out_table=work.sms_message_failed);
 data &tmplib..sms_message_failed_tmp          ;
     set work.sms_message_failed;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if sms_failed_dttm ne . then sms_failed_dttm = tzoneu2s(sms_failed_dttm,&timeZone_Value.);if sms_failed_dttm_tz ne . then sms_failed_dttm_tz = tzoneu2s(sms_failed_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :sms_message_failed_tmp          , sms_message_failed);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..sms_message_failed using &tmpdbschema..sms_message_failed_tmp          
         ON (sms_message_failed.event_id=sms_message_failed_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET aud_occurrence_id = sms_message_failed_tmp.aud_occurrence_id , audience_id = sms_message_failed_tmp.audience_id , context_type_nm = sms_message_failed_tmp.context_type_nm , context_val = sms_message_failed_tmp.context_val , country_cd = sms_message_failed_tmp.country_cd , creative_id = sms_message_failed_tmp.creative_id , creative_version_id = sms_message_failed_tmp.creative_version_id , event_designed_id = sms_message_failed_tmp.event_designed_id , event_nm = sms_message_failed_tmp.event_nm , identity_id = sms_message_failed_tmp.identity_id , journey_id = sms_message_failed_tmp.journey_id , journey_occurrence_id = sms_message_failed_tmp.journey_occurrence_id , load_dttm = sms_message_failed_tmp.load_dttm , occurrence_id = sms_message_failed_tmp.occurrence_id , reason_cd = sms_message_failed_tmp.reason_cd , reason_description_txt = sms_message_failed_tmp.reason_description_txt , response_tracking_cd = sms_message_failed_tmp.response_tracking_cd , sender_id = sms_message_failed_tmp.sender_id , sms_failed_dttm = sms_message_failed_tmp.sms_failed_dttm , sms_failed_dttm_tz = sms_message_failed_tmp.sms_failed_dttm_tz , sms_message_id = sms_message_failed_tmp.sms_message_id , task_id = sms_message_failed_tmp.task_id , task_version_id = sms_message_failed_tmp.task_version_id
        WHEN NOT MATCHED THEN INSERT ( 
        aud_occurrence_id,audience_id,context_type_nm,context_val,country_cd,creative_id,creative_version_id,event_designed_id,event_id,event_nm,identity_id,journey_id,journey_occurrence_id,load_dttm,occurrence_id,reason_cd,reason_description_txt,response_tracking_cd,sender_id,sms_failed_dttm,sms_failed_dttm_tz,sms_message_id,task_id,task_version_id
         ) values ( 
        sms_message_failed_tmp.aud_occurrence_id,sms_message_failed_tmp.audience_id,sms_message_failed_tmp.context_type_nm,sms_message_failed_tmp.context_val,sms_message_failed_tmp.country_cd,sms_message_failed_tmp.creative_id,sms_message_failed_tmp.creative_version_id,sms_message_failed_tmp.event_designed_id,sms_message_failed_tmp.event_id,sms_message_failed_tmp.event_nm,sms_message_failed_tmp.identity_id,sms_message_failed_tmp.journey_id,sms_message_failed_tmp.journey_occurrence_id,sms_message_failed_tmp.load_dttm,sms_message_failed_tmp.occurrence_id,sms_message_failed_tmp.reason_cd,sms_message_failed_tmp.reason_description_txt,sms_message_failed_tmp.response_tracking_cd,sms_message_failed_tmp.sender_id,sms_message_failed_tmp.sms_failed_dttm,sms_message_failed_tmp.sms_failed_dttm_tz,sms_message_failed_tmp.sms_message_id,sms_message_failed_tmp.task_id,sms_message_failed_tmp.task_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :sms_message_failed_tmp          , sms_message_failed, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..sms_message_failed_tmp          ;
    QUIT;
    %put ######## Staging table: sms_message_failed_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..sms_message_failed;
      DROP TABLE work.sms_message_failed;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table sms_message_failed;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..sms_message_reply) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..sms_message_reply_tmp           ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..sms_message_reply_tmp           ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=sms_message_reply, table_keys=%str(event_id), out_table=work.sms_message_reply);
 data &tmplib..sms_message_reply_tmp           ;
     set work.sms_message_reply;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if sms_reply_dttm ne . then sms_reply_dttm = tzoneu2s(sms_reply_dttm,&timeZone_Value.);if sms_reply_dttm_tz ne . then sms_reply_dttm_tz = tzoneu2s(sms_reply_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :sms_message_reply_tmp           , sms_message_reply);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..sms_message_reply using &tmpdbschema..sms_message_reply_tmp           
         ON (sms_message_reply.event_id=sms_message_reply_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET aud_occurrence_id = sms_message_reply_tmp.aud_occurrence_id , audience_id = sms_message_reply_tmp.audience_id , context_type_nm = sms_message_reply_tmp.context_type_nm , context_val = sms_message_reply_tmp.context_val , country_cd = sms_message_reply_tmp.country_cd , event_designed_id = sms_message_reply_tmp.event_designed_id , event_nm = sms_message_reply_tmp.event_nm , identity_id = sms_message_reply_tmp.identity_id , journey_id = sms_message_reply_tmp.journey_id , journey_occurrence_id = sms_message_reply_tmp.journey_occurrence_id , load_dttm = sms_message_reply_tmp.load_dttm , occurrence_id = sms_message_reply_tmp.occurrence_id , response_tracking_cd = sms_message_reply_tmp.response_tracking_cd , sender_id = sms_message_reply_tmp.sender_id , sms_content = sms_message_reply_tmp.sms_content , sms_message_id = sms_message_reply_tmp.sms_message_id , sms_reply_dttm = sms_message_reply_tmp.sms_reply_dttm , sms_reply_dttm_tz = sms_message_reply_tmp.sms_reply_dttm_tz , task_id = sms_message_reply_tmp.task_id , task_version_id = sms_message_reply_tmp.task_version_id
        WHEN NOT MATCHED THEN INSERT ( 
        aud_occurrence_id,audience_id,context_type_nm,context_val,country_cd,event_designed_id,event_id,event_nm,identity_id,journey_id,journey_occurrence_id,load_dttm,occurrence_id,response_tracking_cd,sender_id,sms_content,sms_message_id,sms_reply_dttm,sms_reply_dttm_tz,task_id,task_version_id
         ) values ( 
        sms_message_reply_tmp.aud_occurrence_id,sms_message_reply_tmp.audience_id,sms_message_reply_tmp.context_type_nm,sms_message_reply_tmp.context_val,sms_message_reply_tmp.country_cd,sms_message_reply_tmp.event_designed_id,sms_message_reply_tmp.event_id,sms_message_reply_tmp.event_nm,sms_message_reply_tmp.identity_id,sms_message_reply_tmp.journey_id,sms_message_reply_tmp.journey_occurrence_id,sms_message_reply_tmp.load_dttm,sms_message_reply_tmp.occurrence_id,sms_message_reply_tmp.response_tracking_cd,sms_message_reply_tmp.sender_id,sms_message_reply_tmp.sms_content,sms_message_reply_tmp.sms_message_id,sms_message_reply_tmp.sms_reply_dttm,sms_message_reply_tmp.sms_reply_dttm_tz,sms_message_reply_tmp.task_id,sms_message_reply_tmp.task_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :sms_message_reply_tmp           , sms_message_reply, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..sms_message_reply_tmp           ;
    QUIT;
    %put ######## Staging table: sms_message_reply_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..sms_message_reply;
      DROP TABLE work.sms_message_reply;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table sms_message_reply;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..sms_message_send) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..sms_message_send_tmp            ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..sms_message_send_tmp            ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=sms_message_send, table_keys=%str(event_id), out_table=work.sms_message_send);
 data &tmplib..sms_message_send_tmp            ;
     set work.sms_message_send;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if sms_send_dttm ne . then sms_send_dttm = tzoneu2s(sms_send_dttm,&timeZone_Value.);if sms_send_dttm_tz ne . then sms_send_dttm_tz = tzoneu2s(sms_send_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :sms_message_send_tmp            , sms_message_send);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..sms_message_send using &tmpdbschema..sms_message_send_tmp            
         ON (sms_message_send.event_id=sms_message_send_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET aud_occurrence_id = sms_message_send_tmp.aud_occurrence_id , audience_id = sms_message_send_tmp.audience_id , context_type_nm = sms_message_send_tmp.context_type_nm , context_val = sms_message_send_tmp.context_val , country_cd = sms_message_send_tmp.country_cd , creative_id = sms_message_send_tmp.creative_id , creative_version_id = sms_message_send_tmp.creative_version_id , event_designed_id = sms_message_send_tmp.event_designed_id , event_nm = sms_message_send_tmp.event_nm , fragment_cnt = sms_message_send_tmp.fragment_cnt , identity_id = sms_message_send_tmp.identity_id , journey_id = sms_message_send_tmp.journey_id , journey_occurrence_id = sms_message_send_tmp.journey_occurrence_id , load_dttm = sms_message_send_tmp.load_dttm , occurrence_id = sms_message_send_tmp.occurrence_id , response_tracking_cd = sms_message_send_tmp.response_tracking_cd , sender_id = sms_message_send_tmp.sender_id , sms_message_id = sms_message_send_tmp.sms_message_id , sms_send_dttm = sms_message_send_tmp.sms_send_dttm , sms_send_dttm_tz = sms_message_send_tmp.sms_send_dttm_tz , task_id = sms_message_send_tmp.task_id , task_version_id = sms_message_send_tmp.task_version_id
        WHEN NOT MATCHED THEN INSERT ( 
        aud_occurrence_id,audience_id,context_type_nm,context_val,country_cd,creative_id,creative_version_id,event_designed_id,event_id,event_nm,fragment_cnt,identity_id,journey_id,journey_occurrence_id,load_dttm,occurrence_id,response_tracking_cd,sender_id,sms_message_id,sms_send_dttm,sms_send_dttm_tz,task_id,task_version_id
         ) values ( 
        sms_message_send_tmp.aud_occurrence_id,sms_message_send_tmp.audience_id,sms_message_send_tmp.context_type_nm,sms_message_send_tmp.context_val,sms_message_send_tmp.country_cd,sms_message_send_tmp.creative_id,sms_message_send_tmp.creative_version_id,sms_message_send_tmp.event_designed_id,sms_message_send_tmp.event_id,sms_message_send_tmp.event_nm,sms_message_send_tmp.fragment_cnt,sms_message_send_tmp.identity_id,sms_message_send_tmp.journey_id,sms_message_send_tmp.journey_occurrence_id,sms_message_send_tmp.load_dttm,sms_message_send_tmp.occurrence_id,sms_message_send_tmp.response_tracking_cd,sms_message_send_tmp.sender_id,sms_message_send_tmp.sms_message_id,sms_message_send_tmp.sms_send_dttm,sms_message_send_tmp.sms_send_dttm_tz,sms_message_send_tmp.task_id,sms_message_send_tmp.task_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :sms_message_send_tmp            , sms_message_send, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..sms_message_send_tmp            ;
    QUIT;
    %put ######## Staging table: sms_message_send_tmp             Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..sms_message_send;
      DROP TABLE work.sms_message_send;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table sms_message_send;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..sms_optout) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..sms_optout_tmp                  ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..sms_optout_tmp                  ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=sms_optout, table_keys=%str(event_id), out_table=work.sms_optout);
 data &tmplib..sms_optout_tmp                  ;
     set work.sms_optout;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if sms_optout_dttm ne . then sms_optout_dttm = tzoneu2s(sms_optout_dttm,&timeZone_Value.);if sms_optout_dttm_tz ne . then sms_optout_dttm_tz = tzoneu2s(sms_optout_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :sms_optout_tmp                  , sms_optout);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..sms_optout using &tmpdbschema..sms_optout_tmp                  
         ON (sms_optout.event_id=sms_optout_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET aud_occurrence_id = sms_optout_tmp.aud_occurrence_id , audience_id = sms_optout_tmp.audience_id , context_type_nm = sms_optout_tmp.context_type_nm , context_val = sms_optout_tmp.context_val , country_cd = sms_optout_tmp.country_cd , creative_id = sms_optout_tmp.creative_id , creative_version_id = sms_optout_tmp.creative_version_id , event_designed_id = sms_optout_tmp.event_designed_id , event_nm = sms_optout_tmp.event_nm , identity_id = sms_optout_tmp.identity_id , journey_id = sms_optout_tmp.journey_id , journey_occurrence_id = sms_optout_tmp.journey_occurrence_id , load_dttm = sms_optout_tmp.load_dttm , occurrence_id = sms_optout_tmp.occurrence_id , response_tracking_cd = sms_optout_tmp.response_tracking_cd , sender_id = sms_optout_tmp.sender_id , sms_message_id = sms_optout_tmp.sms_message_id , sms_optout_dttm = sms_optout_tmp.sms_optout_dttm , sms_optout_dttm_tz = sms_optout_tmp.sms_optout_dttm_tz , task_id = sms_optout_tmp.task_id , task_version_id = sms_optout_tmp.task_version_id
        WHEN NOT MATCHED THEN INSERT ( 
        aud_occurrence_id,audience_id,context_type_nm,context_val,country_cd,creative_id,creative_version_id,event_designed_id,event_id,event_nm,identity_id,journey_id,journey_occurrence_id,load_dttm,occurrence_id,response_tracking_cd,sender_id,sms_message_id,sms_optout_dttm,sms_optout_dttm_tz,task_id,task_version_id
         ) values ( 
        sms_optout_tmp.aud_occurrence_id,sms_optout_tmp.audience_id,sms_optout_tmp.context_type_nm,sms_optout_tmp.context_val,sms_optout_tmp.country_cd,sms_optout_tmp.creative_id,sms_optout_tmp.creative_version_id,sms_optout_tmp.event_designed_id,sms_optout_tmp.event_id,sms_optout_tmp.event_nm,sms_optout_tmp.identity_id,sms_optout_tmp.journey_id,sms_optout_tmp.journey_occurrence_id,sms_optout_tmp.load_dttm,sms_optout_tmp.occurrence_id,sms_optout_tmp.response_tracking_cd,sms_optout_tmp.sender_id,sms_optout_tmp.sms_message_id,sms_optout_tmp.sms_optout_dttm,sms_optout_tmp.sms_optout_dttm_tz,sms_optout_tmp.task_id,sms_optout_tmp.task_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :sms_optout_tmp                  , sms_optout, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..sms_optout_tmp                  ;
    QUIT;
    %put ######## Staging table: sms_optout_tmp                   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..sms_optout;
      DROP TABLE work.sms_optout;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table sms_optout;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..sms_optout_details) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..sms_optout_details_tmp          ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..sms_optout_details_tmp          ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=sms_optout_details, table_keys=%str(event_id), out_table=work.sms_optout_details);
 data &tmplib..sms_optout_details_tmp          ;
     set work.sms_optout_details;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if sms_optout_dttm ne . then sms_optout_dttm = tzoneu2s(sms_optout_dttm,&timeZone_Value.);if sms_optout_dttm_tz ne . then sms_optout_dttm_tz = tzoneu2s(sms_optout_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :sms_optout_details_tmp          , sms_optout_details);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..sms_optout_details using &tmpdbschema..sms_optout_details_tmp          
         ON (sms_optout_details.event_id=sms_optout_details_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET address_val = sms_optout_details_tmp.address_val , aud_occurrence_id = sms_optout_details_tmp.aud_occurrence_id , audience_id = sms_optout_details_tmp.audience_id , context_type_nm = sms_optout_details_tmp.context_type_nm , context_val = sms_optout_details_tmp.context_val , country_cd = sms_optout_details_tmp.country_cd , creative_id = sms_optout_details_tmp.creative_id , creative_version_id = sms_optout_details_tmp.creative_version_id , event_designed_id = sms_optout_details_tmp.event_designed_id , event_nm = sms_optout_details_tmp.event_nm , identity_id = sms_optout_details_tmp.identity_id , journey_id = sms_optout_details_tmp.journey_id , journey_occurrence_id = sms_optout_details_tmp.journey_occurrence_id , load_dttm = sms_optout_details_tmp.load_dttm , occurrence_id = sms_optout_details_tmp.occurrence_id , response_tracking_cd = sms_optout_details_tmp.response_tracking_cd , sender_id = sms_optout_details_tmp.sender_id , sms_message_id = sms_optout_details_tmp.sms_message_id , sms_optout_dttm = sms_optout_details_tmp.sms_optout_dttm , sms_optout_dttm_tz = sms_optout_details_tmp.sms_optout_dttm_tz , task_id = sms_optout_details_tmp.task_id , task_version_id = sms_optout_details_tmp.task_version_id
        WHEN NOT MATCHED THEN INSERT ( 
        address_val,aud_occurrence_id,audience_id,context_type_nm,context_val,country_cd,creative_id,creative_version_id,event_designed_id,event_id,event_nm,identity_id,journey_id,journey_occurrence_id,load_dttm,occurrence_id,response_tracking_cd,sender_id,sms_message_id,sms_optout_dttm,sms_optout_dttm_tz,task_id,task_version_id
         ) values ( 
        sms_optout_details_tmp.address_val,sms_optout_details_tmp.aud_occurrence_id,sms_optout_details_tmp.audience_id,sms_optout_details_tmp.context_type_nm,sms_optout_details_tmp.context_val,sms_optout_details_tmp.country_cd,sms_optout_details_tmp.creative_id,sms_optout_details_tmp.creative_version_id,sms_optout_details_tmp.event_designed_id,sms_optout_details_tmp.event_id,sms_optout_details_tmp.event_nm,sms_optout_details_tmp.identity_id,sms_optout_details_tmp.journey_id,sms_optout_details_tmp.journey_occurrence_id,sms_optout_details_tmp.load_dttm,sms_optout_details_tmp.occurrence_id,sms_optout_details_tmp.response_tracking_cd,sms_optout_details_tmp.sender_id,sms_optout_details_tmp.sms_message_id,sms_optout_details_tmp.sms_optout_dttm,sms_optout_details_tmp.sms_optout_dttm_tz,sms_optout_details_tmp.task_id,sms_optout_details_tmp.task_version_id
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :sms_optout_details_tmp          , sms_optout_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..sms_optout_details_tmp          ;
    QUIT;
    %put ######## Staging table: sms_optout_details_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..sms_optout_details;
      DROP TABLE work.sms_optout_details;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table sms_optout_details;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..spot_clicked) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..spot_clicked_tmp                ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..spot_clicked_tmp                ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=spot_clicked, table_keys=%str(event_id), out_table=work.spot_clicked);
 data &tmplib..spot_clicked_tmp                ;
     set work.spot_clicked;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if spot_clicked_dttm ne . then spot_clicked_dttm = tzoneu2s(spot_clicked_dttm,&timeZone_Value.);if spot_clicked_dttm_tz ne . then spot_clicked_dttm_tz = tzoneu2s(spot_clicked_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :spot_clicked_tmp                , spot_clicked);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..spot_clicked using &tmpdbschema..spot_clicked_tmp                
         ON (spot_clicked.event_id=spot_clicked_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET channel_nm = spot_clicked_tmp.channel_nm , channel_user_id = spot_clicked_tmp.channel_user_id , context_type_nm = spot_clicked_tmp.context_type_nm , context_val = spot_clicked_tmp.context_val , control_group_flg = spot_clicked_tmp.control_group_flg , creative_id = spot_clicked_tmp.creative_id , creative_version_id = spot_clicked_tmp.creative_version_id , detail_id_hex = spot_clicked_tmp.detail_id_hex , event_designed_id = spot_clicked_tmp.event_designed_id , event_key_cd = spot_clicked_tmp.event_key_cd , event_nm = spot_clicked_tmp.event_nm , event_source_cd = spot_clicked_tmp.event_source_cd , identity_id = spot_clicked_tmp.identity_id , load_dttm = spot_clicked_tmp.load_dttm , message_id = spot_clicked_tmp.message_id , message_version_id = spot_clicked_tmp.message_version_id , mobile_app_id = spot_clicked_tmp.mobile_app_id , occurrence_id = spot_clicked_tmp.occurrence_id , product_id = spot_clicked_tmp.product_id , product_nm = spot_clicked_tmp.product_nm , product_qty_no = spot_clicked_tmp.product_qty_no , product_sku_no = spot_clicked_tmp.product_sku_no , properties_map_doc = spot_clicked_tmp.properties_map_doc , rec_group_id = spot_clicked_tmp.rec_group_id , request_id = spot_clicked_tmp.request_id , reserved_1_txt = spot_clicked_tmp.reserved_1_txt , reserved_2_txt = spot_clicked_tmp.reserved_2_txt , response_tracking_cd = spot_clicked_tmp.response_tracking_cd , segment_id = spot_clicked_tmp.segment_id , segment_version_id = spot_clicked_tmp.segment_version_id , session_id_hex = spot_clicked_tmp.session_id_hex , spot_clicked_dttm = spot_clicked_tmp.spot_clicked_dttm , spot_clicked_dttm_tz = spot_clicked_tmp.spot_clicked_dttm_tz , spot_id = spot_clicked_tmp.spot_id , task_id = spot_clicked_tmp.task_id , task_version_id = spot_clicked_tmp.task_version_id , url_txt = spot_clicked_tmp.url_txt , visit_id_hex = spot_clicked_tmp.visit_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        channel_nm,channel_user_id,context_type_nm,context_val,control_group_flg,creative_id,creative_version_id,detail_id_hex,event_designed_id,event_id,event_key_cd,event_nm,event_source_cd,identity_id,load_dttm,message_id,message_version_id,mobile_app_id,occurrence_id,product_id,product_nm,product_qty_no,product_sku_no,properties_map_doc,rec_group_id,request_id,reserved_1_txt,reserved_2_txt,response_tracking_cd,segment_id,segment_version_id,session_id_hex,spot_clicked_dttm,spot_clicked_dttm_tz,spot_id,task_id,task_version_id,url_txt,visit_id_hex
         ) values ( 
        spot_clicked_tmp.channel_nm,spot_clicked_tmp.channel_user_id,spot_clicked_tmp.context_type_nm,spot_clicked_tmp.context_val,spot_clicked_tmp.control_group_flg,spot_clicked_tmp.creative_id,spot_clicked_tmp.creative_version_id,spot_clicked_tmp.detail_id_hex,spot_clicked_tmp.event_designed_id,spot_clicked_tmp.event_id,spot_clicked_tmp.event_key_cd,spot_clicked_tmp.event_nm,spot_clicked_tmp.event_source_cd,spot_clicked_tmp.identity_id,spot_clicked_tmp.load_dttm,spot_clicked_tmp.message_id,spot_clicked_tmp.message_version_id,spot_clicked_tmp.mobile_app_id,spot_clicked_tmp.occurrence_id,spot_clicked_tmp.product_id,spot_clicked_tmp.product_nm,spot_clicked_tmp.product_qty_no,spot_clicked_tmp.product_sku_no,spot_clicked_tmp.properties_map_doc,spot_clicked_tmp.rec_group_id,spot_clicked_tmp.request_id,spot_clicked_tmp.reserved_1_txt,spot_clicked_tmp.reserved_2_txt,spot_clicked_tmp.response_tracking_cd,spot_clicked_tmp.segment_id,spot_clicked_tmp.segment_version_id,spot_clicked_tmp.session_id_hex,spot_clicked_tmp.spot_clicked_dttm,spot_clicked_tmp.spot_clicked_dttm_tz,spot_clicked_tmp.spot_id,spot_clicked_tmp.task_id,spot_clicked_tmp.task_version_id,spot_clicked_tmp.url_txt,spot_clicked_tmp.visit_id_hex
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :spot_clicked_tmp                , spot_clicked, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..spot_clicked_tmp                ;
    QUIT;
    %put ######## Staging table: spot_clicked_tmp                 Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..spot_clicked;
      DROP TABLE work.spot_clicked;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table spot_clicked;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..spot_requested) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..spot_requested_tmp              ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..spot_requested_tmp              ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=spot_requested, table_keys=%str(event_id), out_table=work.spot_requested);
 data &tmplib..spot_requested_tmp              ;
     set work.spot_requested;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if spot_requested_dttm ne . then spot_requested_dttm = tzoneu2s(spot_requested_dttm,&timeZone_Value.);if spot_requested_dttm_tz ne . then spot_requested_dttm_tz = tzoneu2s(spot_requested_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :spot_requested_tmp              , spot_requested);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..spot_requested using &tmpdbschema..spot_requested_tmp              
         ON (spot_requested.event_id=spot_requested_tmp.event_id)
        WHEN MATCHED THEN  
        UPDATE SET channel_nm = spot_requested_tmp.channel_nm , channel_user_id = spot_requested_tmp.channel_user_id , context_type_nm = spot_requested_tmp.context_type_nm , context_val = spot_requested_tmp.context_val , detail_id_hex = spot_requested_tmp.detail_id_hex , event_designed_id = spot_requested_tmp.event_designed_id , event_nm = spot_requested_tmp.event_nm , event_source_cd = spot_requested_tmp.event_source_cd , identity_id = spot_requested_tmp.identity_id , load_dttm = spot_requested_tmp.load_dttm , mobile_app_id = spot_requested_tmp.mobile_app_id , properties_map_doc = spot_requested_tmp.properties_map_doc , request_id = spot_requested_tmp.request_id , session_id_hex = spot_requested_tmp.session_id_hex , spot_id = spot_requested_tmp.spot_id , spot_requested_dttm = spot_requested_tmp.spot_requested_dttm , spot_requested_dttm_tz = spot_requested_tmp.spot_requested_dttm_tz , visit_id_hex = spot_requested_tmp.visit_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        channel_nm,channel_user_id,context_type_nm,context_val,detail_id_hex,event_designed_id,event_id,event_nm,event_source_cd,identity_id,load_dttm,mobile_app_id,properties_map_doc,request_id,session_id_hex,spot_id,spot_requested_dttm,spot_requested_dttm_tz,visit_id_hex
         ) values ( 
        spot_requested_tmp.channel_nm,spot_requested_tmp.channel_user_id,spot_requested_tmp.context_type_nm,spot_requested_tmp.context_val,spot_requested_tmp.detail_id_hex,spot_requested_tmp.event_designed_id,spot_requested_tmp.event_id,spot_requested_tmp.event_nm,spot_requested_tmp.event_source_cd,spot_requested_tmp.identity_id,spot_requested_tmp.load_dttm,spot_requested_tmp.mobile_app_id,spot_requested_tmp.properties_map_doc,spot_requested_tmp.request_id,spot_requested_tmp.session_id_hex,spot_requested_tmp.spot_id,spot_requested_tmp.spot_requested_dttm,spot_requested_tmp.spot_requested_dttm_tz,spot_requested_tmp.visit_id_hex
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :spot_requested_tmp              , spot_requested, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..spot_requested_tmp              ;
    QUIT;
    %put ######## Staging table: spot_requested_tmp               Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..spot_requested;
      DROP TABLE work.spot_requested;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table spot_requested;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..tag_details) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..tag_details_tmp                 ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..tag_details_tmp                 ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=tag_details, table_keys=%str(component_id,component_type,tag_id), out_table=work.tag_details);
 data &tmplib..tag_details_tmp                 ;
     set work.tag_details;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if component_id='' then component_id='-'; if component_type='' then component_type='-'; if tag_id='' then tag_id='-';
 run;
 %ErrCheck (Failed to Append Data to :tag_details_tmp                 , tag_details);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..tag_details using &tmpdbschema..tag_details_tmp                 
         ON (tag_details.component_id=tag_details_tmp.component_id and tag_details.component_type=tag_details_tmp.component_type and tag_details.tag_id=tag_details_tmp.tag_id)
        WHEN MATCHED THEN  
        UPDATE SET created_by_usernm = tag_details_tmp.created_by_usernm , created_dttm = tag_details_tmp.created_dttm , identity_cd = tag_details_tmp.identity_cd , last_modified_dttm = tag_details_tmp.last_modified_dttm , last_modified_usernm = tag_details_tmp.last_modified_usernm , load_dttm = tag_details_tmp.load_dttm , tag_desc = tag_details_tmp.tag_desc , tag_nm = tag_details_tmp.tag_nm , tag_owner_usernm = tag_details_tmp.tag_owner_usernm
        WHEN NOT MATCHED THEN INSERT ( 
        component_id,component_type,created_by_usernm,created_dttm,identity_cd,last_modified_dttm,last_modified_usernm,load_dttm,tag_desc,tag_id,tag_nm,tag_owner_usernm
         ) values ( 
        tag_details_tmp.component_id,tag_details_tmp.component_type,tag_details_tmp.created_by_usernm,tag_details_tmp.created_dttm,tag_details_tmp.identity_cd,tag_details_tmp.last_modified_dttm,tag_details_tmp.last_modified_usernm,tag_details_tmp.load_dttm,tag_details_tmp.tag_desc,tag_details_tmp.tag_id,tag_details_tmp.tag_nm,tag_details_tmp.tag_owner_usernm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :tag_details_tmp                 , tag_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..tag_details_tmp                 ;
    QUIT;
    %put ######## Staging table: tag_details_tmp                  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..tag_details;
      DROP TABLE work.tag_details;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table tag_details;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..visit_details) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..visit_details_tmp               ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..visit_details_tmp               ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=visit_details, table_keys=%str(visit_id), out_table=work.visit_details);
 data &tmplib..visit_details_tmp               ;
     set work.visit_details;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if visit_dttm ne . then visit_dttm = tzoneu2s(visit_dttm,&timeZone_Value.);if visit_dttm_tz ne . then visit_dttm_tz = tzoneu2s(visit_dttm_tz,&timeZone_Value.) ;
  if visit_id='' then visit_id='-';
 run;
 %ErrCheck (Failed to Append Data to :visit_details_tmp               , visit_details);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..visit_details using &tmpdbschema..visit_details_tmp               
         ON (visit_details.visit_id=visit_details_tmp.visit_id)
        WHEN MATCHED THEN  
        UPDATE SET event_id = visit_details_tmp.event_id , identity_id = visit_details_tmp.identity_id , load_dttm = visit_details_tmp.load_dttm , origination_creative_nm = visit_details_tmp.origination_creative_nm , origination_nm = visit_details_tmp.origination_nm , origination_placement_nm = visit_details_tmp.origination_placement_nm , origination_tracking_cd = visit_details_tmp.origination_tracking_cd , origination_type_nm = visit_details_tmp.origination_type_nm , referrer_domain_nm = visit_details_tmp.referrer_domain_nm , referrer_query_string_txt = visit_details_tmp.referrer_query_string_txt , referrer_txt = visit_details_tmp.referrer_txt , search_engine_desc = visit_details_tmp.search_engine_desc , search_engine_domain_txt = visit_details_tmp.search_engine_domain_txt , search_term_txt = visit_details_tmp.search_term_txt , sequence_no = visit_details_tmp.sequence_no , session_id = visit_details_tmp.session_id , session_id_hex = visit_details_tmp.session_id_hex , visit_dttm = visit_details_tmp.visit_dttm , visit_dttm_tz = visit_details_tmp.visit_dttm_tz , visit_id_hex = visit_details_tmp.visit_id_hex
        WHEN NOT MATCHED THEN INSERT ( 
        event_id,identity_id,load_dttm,origination_creative_nm,origination_nm,origination_placement_nm,origination_tracking_cd,origination_type_nm,referrer_domain_nm,referrer_query_string_txt,referrer_txt,search_engine_desc,search_engine_domain_txt,search_term_txt,sequence_no,session_id,session_id_hex,visit_dttm,visit_dttm_tz,visit_id,visit_id_hex
         ) values ( 
        visit_details_tmp.event_id,visit_details_tmp.identity_id,visit_details_tmp.load_dttm,visit_details_tmp.origination_creative_nm,visit_details_tmp.origination_nm,visit_details_tmp.origination_placement_nm,visit_details_tmp.origination_tracking_cd,visit_details_tmp.origination_type_nm,visit_details_tmp.referrer_domain_nm,visit_details_tmp.referrer_query_string_txt,visit_details_tmp.referrer_txt,visit_details_tmp.search_engine_desc,visit_details_tmp.search_engine_domain_txt,visit_details_tmp.search_term_txt,visit_details_tmp.sequence_no,visit_details_tmp.session_id,visit_details_tmp.session_id_hex,visit_details_tmp.visit_dttm,visit_details_tmp.visit_dttm_tz,visit_details_tmp.visit_id,visit_details_tmp.visit_id_hex
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :visit_details_tmp               , visit_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..visit_details_tmp               ;
    QUIT;
    %put ######## Staging table: visit_details_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..visit_details;
      DROP TABLE work.visit_details;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table visit_details;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..wf_process_details) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..wf_process_details_tmp          ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..wf_process_details_tmp          ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=wf_process_details, table_keys=%str(pdef_id,process_id), out_table=work.wf_process_details);
 data &tmplib..wf_process_details_tmp          ;
     set work.wf_process_details;
  if completed_dttm ne . then completed_dttm = tzoneu2s(completed_dttm,&timeZone_Value.);if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if deleted_dttm ne . then deleted_dttm = tzoneu2s(deleted_dttm,&timeZone_Value.);if indexed_dttm ne . then indexed_dttm = tzoneu2s(indexed_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if planned_end_dttm ne . then planned_end_dttm = tzoneu2s(planned_end_dttm,&timeZone_Value.);if projected_end_dttm ne . then projected_end_dttm = tzoneu2s(projected_end_dttm,&timeZone_Value.);if published_dttm ne . then published_dttm = tzoneu2s(published_dttm,&timeZone_Value.);if start_dttm ne . then start_dttm = tzoneu2s(start_dttm,&timeZone_Value.);if submitted_dttm ne . then submitted_dttm = tzoneu2s(submitted_dttm,&timeZone_Value.);if timeline_calculated_dttm ne . then timeline_calculated_dttm = tzoneu2s(timeline_calculated_dttm,&timeZone_Value.) ;
  if pdef_id='' then pdef_id='-'; if process_id='' then process_id='-';
 run;
 %ErrCheck (Failed to Append Data to :wf_process_details_tmp          , wf_process_details);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..wf_process_details using &tmpdbschema..wf_process_details_tmp          
         ON (wf_process_details.pdef_id=wf_process_details_tmp.pdef_id and wf_process_details.process_id=wf_process_details_tmp.process_id)
        WHEN MATCHED THEN  
        UPDATE SET business_info_id = wf_process_details_tmp.business_info_id , business_info_nm = wf_process_details_tmp.business_info_nm , business_info_type = wf_process_details_tmp.business_info_type , completed_dttm = wf_process_details_tmp.completed_dttm , created_by_usernm = wf_process_details_tmp.created_by_usernm , created_dttm = wf_process_details_tmp.created_dttm , delayed_by_day = wf_process_details_tmp.delayed_by_day , deleted_by_usernm = wf_process_details_tmp.deleted_by_usernm , deleted_dttm = wf_process_details_tmp.deleted_dttm , indexed_dttm = wf_process_details_tmp.indexed_dttm , last_modified_dttm = wf_process_details_tmp.last_modified_dttm , last_modified_usernm = wf_process_details_tmp.last_modified_usernm , load_dttm = wf_process_details_tmp.load_dttm , modified_status_cd = wf_process_details_tmp.modified_status_cd , percent_complete = wf_process_details_tmp.percent_complete , planned_end_dttm = wf_process_details_tmp.planned_end_dttm , process_category = wf_process_details_tmp.process_category , process_comment = wf_process_details_tmp.process_comment , process_desc = wf_process_details_tmp.process_desc , process_instance_version = wf_process_details_tmp.process_instance_version , process_nm = wf_process_details_tmp.process_nm , process_owner_usernm = wf_process_details_tmp.process_owner_usernm , process_status = wf_process_details_tmp.process_status , process_type = wf_process_details_tmp.process_type , projected_end_dttm = wf_process_details_tmp.projected_end_dttm , published_by_usernm = wf_process_details_tmp.published_by_usernm , published_dttm = wf_process_details_tmp.published_dttm , start_dttm = wf_process_details_tmp.start_dttm , submitted_by_usernm = wf_process_details_tmp.submitted_by_usernm , submitted_dttm = wf_process_details_tmp.submitted_dttm , timeline_calculated_dttm = wf_process_details_tmp.timeline_calculated_dttm , user_tasks_cnt = wf_process_details_tmp.user_tasks_cnt
        WHEN NOT MATCHED THEN INSERT ( 
        business_info_id,business_info_nm,business_info_type,completed_dttm,created_by_usernm,created_dttm,delayed_by_day,deleted_by_usernm,deleted_dttm,indexed_dttm,last_modified_dttm,last_modified_usernm,load_dttm,modified_status_cd,pdef_id,percent_complete,planned_end_dttm,process_category,process_comment,process_desc,process_id,process_instance_version,process_nm,process_owner_usernm,process_status,process_type,projected_end_dttm,published_by_usernm,published_dttm,start_dttm,submitted_by_usernm,submitted_dttm,timeline_calculated_dttm,user_tasks_cnt
         ) values ( 
        wf_process_details_tmp.business_info_id,wf_process_details_tmp.business_info_nm,wf_process_details_tmp.business_info_type,wf_process_details_tmp.completed_dttm,wf_process_details_tmp.created_by_usernm,wf_process_details_tmp.created_dttm,wf_process_details_tmp.delayed_by_day,wf_process_details_tmp.deleted_by_usernm,wf_process_details_tmp.deleted_dttm,wf_process_details_tmp.indexed_dttm,wf_process_details_tmp.last_modified_dttm,wf_process_details_tmp.last_modified_usernm,wf_process_details_tmp.load_dttm,wf_process_details_tmp.modified_status_cd,wf_process_details_tmp.pdef_id,wf_process_details_tmp.percent_complete,wf_process_details_tmp.planned_end_dttm,wf_process_details_tmp.process_category,wf_process_details_tmp.process_comment,wf_process_details_tmp.process_desc,wf_process_details_tmp.process_id,wf_process_details_tmp.process_instance_version,wf_process_details_tmp.process_nm,wf_process_details_tmp.process_owner_usernm,wf_process_details_tmp.process_status,wf_process_details_tmp.process_type,wf_process_details_tmp.projected_end_dttm,wf_process_details_tmp.published_by_usernm,wf_process_details_tmp.published_dttm,wf_process_details_tmp.start_dttm,wf_process_details_tmp.submitted_by_usernm,wf_process_details_tmp.submitted_dttm,wf_process_details_tmp.timeline_calculated_dttm,wf_process_details_tmp.user_tasks_cnt
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :wf_process_details_tmp          , wf_process_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..wf_process_details_tmp          ;
    QUIT;
    %put ######## Staging table: wf_process_details_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..wf_process_details;
      DROP TABLE work.wf_process_details;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table wf_process_details;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..wf_process_details_custom_prop) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..wf_process_details_custom_pr_tmp) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..wf_process_details_custom_pr_tmp;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=wf_process_details_custom_prop, table_keys=%str(attr_group_id,attr_id,process_id), out_table=work.wf_process_details_custom_prop);
 data &tmplib..wf_process_details_custom_pr_tmp;
     set work.wf_process_details_custom_prop;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if attr_group_id='' then attr_group_id='-'; if attr_id='' then attr_id='-'; if process_id='' then process_id='-';
 run;
 %ErrCheck (Failed to Append Data to :wf_process_details_custom_pr_tmp, wf_process_details_custom_prop);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..wf_process_details_custom_prop using &tmpdbschema..wf_process_details_custom_pr_tmp
         ON (wf_process_details_custom_prop.attr_group_id=wf_process_details_custom_pr_tmp.attr_group_id and wf_process_details_custom_prop.attr_id=wf_process_details_custom_pr_tmp.attr_id and wf_process_details_custom_prop.process_id=wf_process_details_custom_pr_tmp.process_id)
        WHEN MATCHED THEN  
        UPDATE SET attr_cd = wf_process_details_custom_pr_tmp.attr_cd , attr_group_cd = wf_process_details_custom_pr_tmp.attr_group_cd , attr_group_nm = wf_process_details_custom_pr_tmp.attr_group_nm , attr_nm = wf_process_details_custom_pr_tmp.attr_nm , attr_val = wf_process_details_custom_pr_tmp.attr_val , created_by_usernm = wf_process_details_custom_pr_tmp.created_by_usernm , created_dttm = wf_process_details_custom_pr_tmp.created_dttm , data_formatter = wf_process_details_custom_pr_tmp.data_formatter , data_type = wf_process_details_custom_pr_tmp.data_type , is_grid_flg = wf_process_details_custom_pr_tmp.is_grid_flg , is_obsolete_flg = wf_process_details_custom_pr_tmp.is_obsolete_flg , last_modified_dttm = wf_process_details_custom_pr_tmp.last_modified_dttm , last_modified_usernm = wf_process_details_custom_pr_tmp.last_modified_usernm , load_dttm = wf_process_details_custom_pr_tmp.load_dttm , remote_pklist_tab_col = wf_process_details_custom_pr_tmp.remote_pklist_tab_col
        WHEN NOT MATCHED THEN INSERT ( 
        attr_cd,attr_group_cd,attr_group_id,attr_group_nm,attr_id,attr_nm,attr_val,created_by_usernm,created_dttm,data_formatter,data_type,is_grid_flg,is_obsolete_flg,last_modified_dttm,last_modified_usernm,load_dttm,process_id,remote_pklist_tab_col
         ) values ( 
        wf_process_details_custom_pr_tmp.attr_cd,wf_process_details_custom_pr_tmp.attr_group_cd,wf_process_details_custom_pr_tmp.attr_group_id,wf_process_details_custom_pr_tmp.attr_group_nm,wf_process_details_custom_pr_tmp.attr_id,wf_process_details_custom_pr_tmp.attr_nm,wf_process_details_custom_pr_tmp.attr_val,wf_process_details_custom_pr_tmp.created_by_usernm,wf_process_details_custom_pr_tmp.created_dttm,wf_process_details_custom_pr_tmp.data_formatter,wf_process_details_custom_pr_tmp.data_type,wf_process_details_custom_pr_tmp.is_grid_flg,wf_process_details_custom_pr_tmp.is_obsolete_flg,wf_process_details_custom_pr_tmp.last_modified_dttm,wf_process_details_custom_pr_tmp.last_modified_usernm,wf_process_details_custom_pr_tmp.load_dttm,wf_process_details_custom_pr_tmp.process_id,wf_process_details_custom_pr_tmp.remote_pklist_tab_col
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :wf_process_details_custom_pr_tmp, wf_process_details_custom_prop, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..wf_process_details_custom_pr_tmp;
    QUIT;
    %put ######## Staging table: wf_process_details_custom_pr_tmp Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..wf_process_details_custom_prop;
      DROP TABLE work.wf_process_details_custom_prop;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table wf_process_details_custom_prop;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..wf_process_tasks) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..wf_process_tasks_tmp            ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..wf_process_tasks_tmp            ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=wf_process_tasks, table_keys=%str(engine_taskdef_id,process_id,task_id), out_table=work.wf_process_tasks);
 data &tmplib..wf_process_tasks_tmp            ;
     set work.wf_process_tasks;
  if completed_dttm ne . then completed_dttm = tzoneu2s(completed_dttm,&timeZone_Value.);if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if deleted_dttm ne . then deleted_dttm = tzoneu2s(deleted_dttm,&timeZone_Value.);if due_dttm ne . then due_dttm = tzoneu2s(due_dttm,&timeZone_Value.);if engine_task_cancelled_dttm ne . then engine_task_cancelled_dttm = tzoneu2s(engine_task_cancelled_dttm,&timeZone_Value.);if indexed_dttm ne . then indexed_dttm = tzoneu2s(indexed_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if modified_dttm ne . then modified_dttm = tzoneu2s(modified_dttm,&timeZone_Value.);if projected_end_dttm ne . then projected_end_dttm = tzoneu2s(projected_end_dttm,&timeZone_Value.);if projected_start_dttm ne . then projected_start_dttm = tzoneu2s(projected_start_dttm,&timeZone_Value.);if published_dttm ne . then published_dttm = tzoneu2s(published_dttm,&timeZone_Value.);if started_dttm ne . then started_dttm = tzoneu2s(started_dttm,&timeZone_Value.) ;
  if engine_taskdef_id='' then engine_taskdef_id='-'; if process_id='' then process_id='-'; if task_id='' then task_id='-';
 run;
 %ErrCheck (Failed to Append Data to :wf_process_tasks_tmp            , wf_process_tasks);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..wf_process_tasks using &tmpdbschema..wf_process_tasks_tmp            
         ON (wf_process_tasks.engine_taskdef_id=wf_process_tasks_tmp.engine_taskdef_id and wf_process_tasks.process_id=wf_process_tasks_tmp.process_id and wf_process_tasks.task_id=wf_process_tasks_tmp.task_id)
        WHEN MATCHED THEN  
        UPDATE SET approval_task_flg = wf_process_tasks_tmp.approval_task_flg , cancelled_task_flg = wf_process_tasks_tmp.cancelled_task_flg , completed_dttm = wf_process_tasks_tmp.completed_dttm , created_by_usernm = wf_process_tasks_tmp.created_by_usernm , created_dttm = wf_process_tasks_tmp.created_dttm , delayed_by_day = wf_process_tasks_tmp.delayed_by_day , deleted_by_usernm = wf_process_tasks_tmp.deleted_by_usernm , deleted_dttm = wf_process_tasks_tmp.deleted_dttm , due_dttm = wf_process_tasks_tmp.due_dttm , duration_per_assignee = wf_process_tasks_tmp.duration_per_assignee , engine_task_cancelled_dttm = wf_process_tasks_tmp.engine_task_cancelled_dttm , existobj_update_flg = wf_process_tasks_tmp.existobj_update_flg , first_usertask_flg = wf_process_tasks_tmp.first_usertask_flg , indexed_dttm = wf_process_tasks_tmp.indexed_dttm , instance_version = wf_process_tasks_tmp.instance_version , is_sequential_flg = wf_process_tasks_tmp.is_sequential_flg , latest_flg = wf_process_tasks_tmp.latest_flg , load_dttm = wf_process_tasks_tmp.load_dttm , locally_updated_flg = wf_process_tasks_tmp.locally_updated_flg , modified_by_usernm = wf_process_tasks_tmp.modified_by_usernm , modified_dttm = wf_process_tasks_tmp.modified_dttm , modified_status_cd = wf_process_tasks_tmp.modified_status_cd , multi_assig_suprt_flg = wf_process_tasks_tmp.multi_assig_suprt_flg , owner_usernm = wf_process_tasks_tmp.owner_usernm , percent_complete = wf_process_tasks_tmp.percent_complete , projected_end_dttm = wf_process_tasks_tmp.projected_end_dttm , projected_start_dttm = wf_process_tasks_tmp.projected_start_dttm , published_by_usernm = wf_process_tasks_tmp.published_by_usernm , published_dttm = wf_process_tasks_tmp.published_dttm , skip_peerupdate_scanning_flg = wf_process_tasks_tmp.skip_peerupdate_scanning_flg , skip_update_scanning_flg = wf_process_tasks_tmp.skip_update_scanning_flg , started_dttm = wf_process_tasks_tmp.started_dttm , task_attachment = wf_process_tasks_tmp.task_attachment , task_comment = wf_process_tasks_tmp.task_comment , task_desc = wf_process_tasks_tmp.task_desc , task_instruction = wf_process_tasks_tmp.task_instruction , task_nm = wf_process_tasks_tmp.task_nm , task_status = wf_process_tasks_tmp.task_status , task_subtype = wf_process_tasks_tmp.task_subtype , task_type = wf_process_tasks_tmp.task_type , version_num = wf_process_tasks_tmp.version_num
        WHEN NOT MATCHED THEN INSERT ( 
        approval_task_flg,cancelled_task_flg,completed_dttm,created_by_usernm,created_dttm,delayed_by_day,deleted_by_usernm,deleted_dttm,due_dttm,duration_per_assignee,engine_task_cancelled_dttm,engine_taskdef_id,existobj_update_flg,first_usertask_flg,indexed_dttm,instance_version,is_sequential_flg,latest_flg,load_dttm,locally_updated_flg,modified_by_usernm,modified_dttm,modified_status_cd,multi_assig_suprt_flg,owner_usernm,percent_complete,process_id,projected_end_dttm,projected_start_dttm,published_by_usernm,published_dttm,skip_peerupdate_scanning_flg,skip_update_scanning_flg,started_dttm,task_attachment,task_comment,task_desc,task_id,task_instruction,task_nm,task_status,task_subtype,task_type,version_num
         ) values ( 
        wf_process_tasks_tmp.approval_task_flg,wf_process_tasks_tmp.cancelled_task_flg,wf_process_tasks_tmp.completed_dttm,wf_process_tasks_tmp.created_by_usernm,wf_process_tasks_tmp.created_dttm,wf_process_tasks_tmp.delayed_by_day,wf_process_tasks_tmp.deleted_by_usernm,wf_process_tasks_tmp.deleted_dttm,wf_process_tasks_tmp.due_dttm,wf_process_tasks_tmp.duration_per_assignee,wf_process_tasks_tmp.engine_task_cancelled_dttm,wf_process_tasks_tmp.engine_taskdef_id,wf_process_tasks_tmp.existobj_update_flg,wf_process_tasks_tmp.first_usertask_flg,wf_process_tasks_tmp.indexed_dttm,wf_process_tasks_tmp.instance_version,wf_process_tasks_tmp.is_sequential_flg,wf_process_tasks_tmp.latest_flg,wf_process_tasks_tmp.load_dttm,wf_process_tasks_tmp.locally_updated_flg,wf_process_tasks_tmp.modified_by_usernm,wf_process_tasks_tmp.modified_dttm,wf_process_tasks_tmp.modified_status_cd,wf_process_tasks_tmp.multi_assig_suprt_flg,wf_process_tasks_tmp.owner_usernm,wf_process_tasks_tmp.percent_complete,wf_process_tasks_tmp.process_id,wf_process_tasks_tmp.projected_end_dttm,wf_process_tasks_tmp.projected_start_dttm,wf_process_tasks_tmp.published_by_usernm,wf_process_tasks_tmp.published_dttm,wf_process_tasks_tmp.skip_peerupdate_scanning_flg,wf_process_tasks_tmp.skip_update_scanning_flg,wf_process_tasks_tmp.started_dttm,wf_process_tasks_tmp.task_attachment,wf_process_tasks_tmp.task_comment,wf_process_tasks_tmp.task_desc,wf_process_tasks_tmp.task_id,wf_process_tasks_tmp.task_instruction,wf_process_tasks_tmp.task_nm,wf_process_tasks_tmp.task_status,wf_process_tasks_tmp.task_subtype,wf_process_tasks_tmp.task_type,wf_process_tasks_tmp.version_num
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :wf_process_tasks_tmp            , wf_process_tasks, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..wf_process_tasks_tmp            ;
    QUIT;
    %put ######## Staging table: wf_process_tasks_tmp             Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..wf_process_tasks;
      DROP TABLE work.wf_process_tasks;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table wf_process_tasks;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..wf_tasks_user_assignment) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..wf_tasks_user_assignment_tmp    ) ) %then %do;
      PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..wf_tasks_user_assignment_tmp    ;
      QUIT;
 %end;
 %check_duplicate_from_source(table_nm=wf_tasks_user_assignment, table_keys=%str(process_id,task_id,user_assignment_id,user_id), out_table=work.wf_tasks_user_assignment);
 data &tmplib..wf_tasks_user_assignment_tmp    ;
     set work.wf_tasks_user_assignment;
  if completed_dttm ne . then completed_dttm = tzoneu2s(completed_dttm,&timeZone_Value.);if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if deleted_dttm ne . then deleted_dttm = tzoneu2s(deleted_dttm,&timeZone_Value.);if due_dttm ne . then due_dttm = tzoneu2s(due_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if modified_dttm ne . then modified_dttm = tzoneu2s(modified_dttm,&timeZone_Value.);if projected_end_dttm ne . then projected_end_dttm = tzoneu2s(projected_end_dttm,&timeZone_Value.);if projected_start_dttm ne . then projected_start_dttm = tzoneu2s(projected_start_dttm,&timeZone_Value.);if start_dttm ne . then start_dttm = tzoneu2s(start_dttm,&timeZone_Value.) ;
  if process_id='' then process_id='-'; if task_id='' then task_id='-'; if user_assignment_id='' then user_assignment_id='-'; if user_id='' then user_id='-';
 run;
 %ErrCheck (Failed to Append Data to :wf_tasks_user_assignment_tmp    , wf_tasks_user_assignment);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
    CONNECT TO REDSHIFT (&sql_passthru_connection.);
        execute (MERGE INTO &dbschema..wf_tasks_user_assignment using &tmpdbschema..wf_tasks_user_assignment_tmp    
         ON (wf_tasks_user_assignment.process_id=wf_tasks_user_assignment_tmp.process_id and wf_tasks_user_assignment.task_id=wf_tasks_user_assignment_tmp.task_id and wf_tasks_user_assignment.user_assignment_id=wf_tasks_user_assignment_tmp.user_assignment_id and wf_tasks_user_assignment.user_id=wf_tasks_user_assignment_tmp.user_id)
        WHEN MATCHED THEN  
        UPDATE SET activation_completed_flg = wf_tasks_user_assignment_tmp.activation_completed_flg , approval_status = wf_tasks_user_assignment_tmp.approval_status , assignee_id = wf_tasks_user_assignment_tmp.assignee_id , assignee_type = wf_tasks_user_assignment_tmp.assignee_type , completed_dttm = wf_tasks_user_assignment_tmp.completed_dttm , created_by_usernm = wf_tasks_user_assignment_tmp.created_by_usernm , created_dttm = wf_tasks_user_assignment_tmp.created_dttm , delayed_by_day = wf_tasks_user_assignment_tmp.delayed_by_day , deleted_by_usernm = wf_tasks_user_assignment_tmp.deleted_by_usernm , deleted_dttm = wf_tasks_user_assignment_tmp.deleted_dttm , due_dttm = wf_tasks_user_assignment_tmp.due_dttm , initiator_comment = wf_tasks_user_assignment_tmp.initiator_comment , instance_version = wf_tasks_user_assignment_tmp.instance_version , is_assigned_flg = wf_tasks_user_assignment_tmp.is_assigned_flg , is_latest_flg = wf_tasks_user_assignment_tmp.is_latest_flg , is_replaced_flg = wf_tasks_user_assignment_tmp.is_replaced_flg , load_dttm = wf_tasks_user_assignment_tmp.load_dttm , modified_by_usernm = wf_tasks_user_assignment_tmp.modified_by_usernm , modified_dttm = wf_tasks_user_assignment_tmp.modified_dttm , modified_status_cd = wf_tasks_user_assignment_tmp.modified_status_cd , owner_usernm = wf_tasks_user_assignment_tmp.owner_usernm , projected_end_dttm = wf_tasks_user_assignment_tmp.projected_end_dttm , projected_start_dttm = wf_tasks_user_assignment_tmp.projected_start_dttm , replacement_assignee_id = wf_tasks_user_assignment_tmp.replacement_assignee_id , replacement_reason = wf_tasks_user_assignment_tmp.replacement_reason , replacement_userid = wf_tasks_user_assignment_tmp.replacement_userid , start_dttm = wf_tasks_user_assignment_tmp.start_dttm , usan_comment = wf_tasks_user_assignment_tmp.usan_comment , usan_desc = wf_tasks_user_assignment_tmp.usan_desc , usan_duration_day = wf_tasks_user_assignment_tmp.usan_duration_day , usan_instruction = wf_tasks_user_assignment_tmp.usan_instruction , usan_status = wf_tasks_user_assignment_tmp.usan_status , user_nm = wf_tasks_user_assignment_tmp.user_nm
        WHEN NOT MATCHED THEN INSERT ( 
        activation_completed_flg,approval_status,assignee_id,assignee_type,completed_dttm,created_by_usernm,created_dttm,delayed_by_day,deleted_by_usernm,deleted_dttm,due_dttm,initiator_comment,instance_version,is_assigned_flg,is_latest_flg,is_replaced_flg,load_dttm,modified_by_usernm,modified_dttm,modified_status_cd,owner_usernm,process_id,projected_end_dttm,projected_start_dttm,replacement_assignee_id,replacement_reason,replacement_userid,start_dttm,task_id,usan_comment,usan_desc,usan_duration_day,usan_instruction,usan_status,user_assignment_id,user_id,user_nm
         ) values ( 
        wf_tasks_user_assignment_tmp.activation_completed_flg,wf_tasks_user_assignment_tmp.approval_status,wf_tasks_user_assignment_tmp.assignee_id,wf_tasks_user_assignment_tmp.assignee_type,wf_tasks_user_assignment_tmp.completed_dttm,wf_tasks_user_assignment_tmp.created_by_usernm,wf_tasks_user_assignment_tmp.created_dttm,wf_tasks_user_assignment_tmp.delayed_by_day,wf_tasks_user_assignment_tmp.deleted_by_usernm,wf_tasks_user_assignment_tmp.deleted_dttm,wf_tasks_user_assignment_tmp.due_dttm,wf_tasks_user_assignment_tmp.initiator_comment,wf_tasks_user_assignment_tmp.instance_version,wf_tasks_user_assignment_tmp.is_assigned_flg,wf_tasks_user_assignment_tmp.is_latest_flg,wf_tasks_user_assignment_tmp.is_replaced_flg,wf_tasks_user_assignment_tmp.load_dttm,wf_tasks_user_assignment_tmp.modified_by_usernm,wf_tasks_user_assignment_tmp.modified_dttm,wf_tasks_user_assignment_tmp.modified_status_cd,wf_tasks_user_assignment_tmp.owner_usernm,wf_tasks_user_assignment_tmp.process_id,wf_tasks_user_assignment_tmp.projected_end_dttm,wf_tasks_user_assignment_tmp.projected_start_dttm,wf_tasks_user_assignment_tmp.replacement_assignee_id,wf_tasks_user_assignment_tmp.replacement_reason,wf_tasks_user_assignment_tmp.replacement_userid,wf_tasks_user_assignment_tmp.start_dttm,wf_tasks_user_assignment_tmp.task_id,wf_tasks_user_assignment_tmp.usan_comment,wf_tasks_user_assignment_tmp.usan_desc,wf_tasks_user_assignment_tmp.usan_duration_day,wf_tasks_user_assignment_tmp.usan_instruction,wf_tasks_user_assignment_tmp.usan_status,wf_tasks_user_assignment_tmp.user_assignment_id,wf_tasks_user_assignment_tmp.user_id,wf_tasks_user_assignment_tmp.user_nm
     );) BY REDSHIFT;
    DISCONNECT FROM REDSHIFT;
    QUIT;
 %ErrCheck (Failed to Update/Insert into  :wf_tasks_user_assignment_tmp    , wf_tasks_user_assignment, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    PROC SQL NOERRORSTOP;
        DROP TABLE &tmplib..wf_tasks_user_assignment_tmp    ;
    QUIT;
    %put ######## Staging table: wf_tasks_user_assignment_tmp     Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  PROC SQL NOERRORSTOP;
      drop table &udmmart..wf_tasks_user_assignment;
      DROP TABLE work.wf_tasks_user_assignment;
  QUIT;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table wf_tasks_user_assignment;
%put------------------------------------------------------------------;
 %mend execute_REDSHIFT_code;
 %execute_REDSHIFT_code;
