/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%macro execute_AZURE_code;
%if %sysfunc(exist(&udmmart..ab_test_path_assignment) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..ab_test_path_assignment_tmp     ) ) %then %do;
      proc sql noerrorstop;
        drop table &tmplib..ab_test_path_assignment_tmp     ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=ab_test_path_assignment, table_keys=%str(event_id), out_table=work.ab_test_path_assignment);
 data &tmplib..ab_test_path_assignment_tmp     ;
     set work.ab_test_path_assignment;
  if abtestpath_assignment_dttm ne . then abtestpath_assignment_dttm = tzoneu2s(abtestpath_assignment_dttm,&timeZone_Value.);if abtestpath_assignment_dttm_tz ne . then abtestpath_assignment_dttm_tz = tzoneu2s(abtestpath_assignment_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :ab_test_path_assignment_tmp     , ab_test_path_assignment);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..ab_test_path_assignment using &tmpdbschema..ab_test_path_assignment_tmp     
         on (ab_test_path_assignment.event_id=ab_test_path_assignment_tmp.event_id)
        when matched then  
        update set ab_test_path_assignment.abtest_path_id = ab_test_path_assignment_tmp.abtest_path_id , ab_test_path_assignment.abtestpath_assignment_dttm = ab_test_path_assignment_tmp.abtestpath_assignment_dttm , ab_test_path_assignment.abtestpath_assignment_dttm_tz = ab_test_path_assignment_tmp.abtestpath_assignment_dttm_tz , ab_test_path_assignment.activity_id = ab_test_path_assignment_tmp.activity_id , ab_test_path_assignment.channel_nm = ab_test_path_assignment_tmp.channel_nm , ab_test_path_assignment.channel_user_id = ab_test_path_assignment_tmp.channel_user_id , ab_test_path_assignment.context_type_nm = ab_test_path_assignment_tmp.context_type_nm , ab_test_path_assignment.context_val = ab_test_path_assignment_tmp.context_val , ab_test_path_assignment.event_designed_id = ab_test_path_assignment_tmp.event_designed_id , ab_test_path_assignment.event_nm = ab_test_path_assignment_tmp.event_nm , ab_test_path_assignment.identity_id = ab_test_path_assignment_tmp.identity_id , ab_test_path_assignment.load_dttm = ab_test_path_assignment_tmp.load_dttm , ab_test_path_assignment.session_id_hex = ab_test_path_assignment_tmp.session_id_hex
        when not matched then insert ( 
        abtest_path_id,abtestpath_assignment_dttm,abtestpath_assignment_dttm_tz,activity_id,channel_nm,channel_user_id,context_type_nm,context_val,event_designed_id,event_id,event_nm,identity_id,load_dttm,session_id_hex
         ) values ( 
        ab_test_path_assignment_tmp.abtest_path_id,ab_test_path_assignment_tmp.abtestpath_assignment_dttm,ab_test_path_assignment_tmp.abtestpath_assignment_dttm_tz,ab_test_path_assignment_tmp.activity_id,ab_test_path_assignment_tmp.channel_nm,ab_test_path_assignment_tmp.channel_user_id,ab_test_path_assignment_tmp.context_type_nm,ab_test_path_assignment_tmp.context_val,ab_test_path_assignment_tmp.event_designed_id,ab_test_path_assignment_tmp.event_id,ab_test_path_assignment_tmp.event_nm,ab_test_path_assignment_tmp.identity_id,ab_test_path_assignment_tmp.load_dttm,ab_test_path_assignment_tmp.session_id_hex
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :ab_test_path_assignment_tmp     , ab_test_path_assignment, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..ab_test_path_assignment_tmp     ;
    quit;
    %put ######## Staging table: ab_test_path_assignment_tmp      Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..ab_test_path_assignment;
      drop table work.ab_test_path_assignment;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..abt_attribution_tmp             ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=abt_attribution, table_keys=%str(interaction_dttm,interaction_id), out_table=work.abt_attribution);
 data &tmplib..abt_attribution_tmp             ;
     set work.abt_attribution;
  if interaction_dttm ne . then interaction_dttm = tzoneu2s(interaction_dttm,&timeZone_Value.) ;
  if interaction_id='' then interaction_id='-';
 run;
 %ErrCheck (Failed to Append Data to :abt_attribution_tmp             , abt_attribution);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..abt_attribution using &tmpdbschema..abt_attribution_tmp             
         on (abt_attribution.interaction_dttm=abt_attribution_tmp.interaction_dttm and abt_attribution.interaction_id=abt_attribution_tmp.interaction_id)
        when matched then  
        update set abt_attribution.conversion_value = abt_attribution_tmp.conversion_value , abt_attribution.creative_id = abt_attribution_tmp.creative_id , abt_attribution.identity_id = abt_attribution_tmp.identity_id , abt_attribution.interaction = abt_attribution_tmp.interaction , abt_attribution.interaction_cost = abt_attribution_tmp.interaction_cost , abt_attribution.interaction_subtype = abt_attribution_tmp.interaction_subtype , abt_attribution.interaction_type = abt_attribution_tmp.interaction_type , abt_attribution.load_id = abt_attribution_tmp.load_id , abt_attribution.task_id = abt_attribution_tmp.task_id
        when not matched then insert ( 
        conversion_value,creative_id,identity_id,interaction,interaction_cost,interaction_dttm,interaction_id,interaction_subtype,interaction_type,load_id,task_id
         ) values ( 
        abt_attribution_tmp.conversion_value,abt_attribution_tmp.creative_id,abt_attribution_tmp.identity_id,abt_attribution_tmp.interaction,abt_attribution_tmp.interaction_cost,abt_attribution_tmp.interaction_dttm,abt_attribution_tmp.interaction_id,abt_attribution_tmp.interaction_subtype,abt_attribution_tmp.interaction_type,abt_attribution_tmp.load_id,abt_attribution_tmp.task_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :abt_attribution_tmp             , abt_attribution, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..abt_attribution_tmp             ;
    quit;
    %put ######## Staging table: abt_attribution_tmp              Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..abt_attribution;
      drop table work.abt_attribution;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..activity_conversion_tmp         ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=activity_conversion, table_keys=%str(event_id), out_table=work.activity_conversion);
 data &tmplib..activity_conversion_tmp         ;
     set work.activity_conversion;
  if activity_conversion_dttm ne . then activity_conversion_dttm = tzoneu2s(activity_conversion_dttm,&timeZone_Value.);if activity_conversion_dttm_tz ne . then activity_conversion_dttm_tz = tzoneu2s(activity_conversion_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :activity_conversion_tmp         , activity_conversion);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..activity_conversion using &tmpdbschema..activity_conversion_tmp         
         on (activity_conversion.event_id=activity_conversion_tmp.event_id)
        when matched then  
        update set activity_conversion.abtest_path_id = activity_conversion_tmp.abtest_path_id , activity_conversion.activity_conversion_dttm = activity_conversion_tmp.activity_conversion_dttm , activity_conversion.activity_conversion_dttm_tz = activity_conversion_tmp.activity_conversion_dttm_tz , activity_conversion.activity_id = activity_conversion_tmp.activity_id , activity_conversion.activity_node_id = activity_conversion_tmp.activity_node_id , activity_conversion.channel_nm = activity_conversion_tmp.channel_nm , activity_conversion.channel_user_id = activity_conversion_tmp.channel_user_id , activity_conversion.context_type_nm = activity_conversion_tmp.context_type_nm , activity_conversion.context_val = activity_conversion_tmp.context_val , activity_conversion.detail_id_hex = activity_conversion_tmp.detail_id_hex , activity_conversion.event_designed_id = activity_conversion_tmp.event_designed_id , activity_conversion.event_nm = activity_conversion_tmp.event_nm , activity_conversion.goal_id = activity_conversion_tmp.goal_id , activity_conversion.identity_id = activity_conversion_tmp.identity_id , activity_conversion.load_dttm = activity_conversion_tmp.load_dttm , activity_conversion.parent_event_designed_id = activity_conversion_tmp.parent_event_designed_id , activity_conversion.session_id_hex = activity_conversion_tmp.session_id_hex
        when not matched then insert ( 
        abtest_path_id,activity_conversion_dttm,activity_conversion_dttm_tz,activity_id,activity_node_id,channel_nm,channel_user_id,context_type_nm,context_val,detail_id_hex,event_designed_id,event_id,event_nm,goal_id,identity_id,load_dttm,parent_event_designed_id,session_id_hex
         ) values ( 
        activity_conversion_tmp.abtest_path_id,activity_conversion_tmp.activity_conversion_dttm,activity_conversion_tmp.activity_conversion_dttm_tz,activity_conversion_tmp.activity_id,activity_conversion_tmp.activity_node_id,activity_conversion_tmp.channel_nm,activity_conversion_tmp.channel_user_id,activity_conversion_tmp.context_type_nm,activity_conversion_tmp.context_val,activity_conversion_tmp.detail_id_hex,activity_conversion_tmp.event_designed_id,activity_conversion_tmp.event_id,activity_conversion_tmp.event_nm,activity_conversion_tmp.goal_id,activity_conversion_tmp.identity_id,activity_conversion_tmp.load_dttm,activity_conversion_tmp.parent_event_designed_id,activity_conversion_tmp.session_id_hex
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :activity_conversion_tmp         , activity_conversion, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..activity_conversion_tmp         ;
    quit;
    %put ######## Staging table: activity_conversion_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..activity_conversion;
      drop table work.activity_conversion;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..activity_flow_in_tmp            ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=activity_flow_in, table_keys=%str(event_id), out_table=work.activity_flow_in);
 data &tmplib..activity_flow_in_tmp            ;
     set work.activity_flow_in;
  if activity_flow_in_dttm ne . then activity_flow_in_dttm = tzoneu2s(activity_flow_in_dttm,&timeZone_Value.);if activity_flow_in_dttm_tz ne . then activity_flow_in_dttm_tz = tzoneu2s(activity_flow_in_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :activity_flow_in_tmp            , activity_flow_in);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..activity_flow_in using &tmpdbschema..activity_flow_in_tmp            
         on (activity_flow_in.event_id=activity_flow_in_tmp.event_id)
        when matched then  
        update set activity_flow_in.abtest_path_id = activity_flow_in_tmp.abtest_path_id , activity_flow_in.activity_flow_in_dttm = activity_flow_in_tmp.activity_flow_in_dttm , activity_flow_in.activity_flow_in_dttm_tz = activity_flow_in_tmp.activity_flow_in_dttm_tz , activity_flow_in.activity_id = activity_flow_in_tmp.activity_id , activity_flow_in.activity_node_id = activity_flow_in_tmp.activity_node_id , activity_flow_in.channel_nm = activity_flow_in_tmp.channel_nm , activity_flow_in.channel_user_id = activity_flow_in_tmp.channel_user_id , activity_flow_in.context_type_nm = activity_flow_in_tmp.context_type_nm , activity_flow_in.context_val = activity_flow_in_tmp.context_val , activity_flow_in.event_designed_id = activity_flow_in_tmp.event_designed_id , activity_flow_in.event_nm = activity_flow_in_tmp.event_nm , activity_flow_in.identity_id = activity_flow_in_tmp.identity_id , activity_flow_in.load_dttm = activity_flow_in_tmp.load_dttm , activity_flow_in.task_id = activity_flow_in_tmp.task_id
        when not matched then insert ( 
        abtest_path_id,activity_flow_in_dttm,activity_flow_in_dttm_tz,activity_id,activity_node_id,channel_nm,channel_user_id,context_type_nm,context_val,event_designed_id,event_id,event_nm,identity_id,load_dttm,task_id
         ) values ( 
        activity_flow_in_tmp.abtest_path_id,activity_flow_in_tmp.activity_flow_in_dttm,activity_flow_in_tmp.activity_flow_in_dttm_tz,activity_flow_in_tmp.activity_id,activity_flow_in_tmp.activity_node_id,activity_flow_in_tmp.channel_nm,activity_flow_in_tmp.channel_user_id,activity_flow_in_tmp.context_type_nm,activity_flow_in_tmp.context_val,activity_flow_in_tmp.event_designed_id,activity_flow_in_tmp.event_id,activity_flow_in_tmp.event_nm,activity_flow_in_tmp.identity_id,activity_flow_in_tmp.load_dttm,activity_flow_in_tmp.task_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :activity_flow_in_tmp            , activity_flow_in, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..activity_flow_in_tmp            ;
    quit;
    %put ######## Staging table: activity_flow_in_tmp             Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..activity_flow_in;
      drop table work.activity_flow_in;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..activity_start_tmp              ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=activity_start, table_keys=%str(event_id), out_table=work.activity_start);
 data &tmplib..activity_start_tmp              ;
     set work.activity_start;
  if activity_start_dttm ne . then activity_start_dttm = tzoneu2s(activity_start_dttm,&timeZone_Value.);if activity_start_dttm_tz ne . then activity_start_dttm_tz = tzoneu2s(activity_start_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :activity_start_tmp              , activity_start);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..activity_start using &tmpdbschema..activity_start_tmp              
         on (activity_start.event_id=activity_start_tmp.event_id)
        when matched then  
        update set activity_start.activity_id = activity_start_tmp.activity_id , activity_start.activity_start_dttm = activity_start_tmp.activity_start_dttm , activity_start.activity_start_dttm_tz = activity_start_tmp.activity_start_dttm_tz , activity_start.channel_nm = activity_start_tmp.channel_nm , activity_start.channel_user_id = activity_start_tmp.channel_user_id , activity_start.context_type_nm = activity_start_tmp.context_type_nm , activity_start.context_val = activity_start_tmp.context_val , activity_start.event_designed_id = activity_start_tmp.event_designed_id , activity_start.event_nm = activity_start_tmp.event_nm , activity_start.identity_id = activity_start_tmp.identity_id , activity_start.load_dttm = activity_start_tmp.load_dttm
        when not matched then insert ( 
        activity_id,activity_start_dttm,activity_start_dttm_tz,channel_nm,channel_user_id,context_type_nm,context_val,event_designed_id,event_id,event_nm,identity_id,load_dttm
         ) values ( 
        activity_start_tmp.activity_id,activity_start_tmp.activity_start_dttm,activity_start_tmp.activity_start_dttm_tz,activity_start_tmp.channel_nm,activity_start_tmp.channel_user_id,activity_start_tmp.context_type_nm,activity_start_tmp.context_val,activity_start_tmp.event_designed_id,activity_start_tmp.event_id,activity_start_tmp.event_nm,activity_start_tmp.identity_id,activity_start_tmp.load_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :activity_start_tmp              , activity_start, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..activity_start_tmp              ;
    quit;
    %put ######## Staging table: activity_start_tmp               Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..activity_start;
      drop table work.activity_start;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..advertising_contact_tmp         ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=advertising_contact, table_keys=%str(event_id), out_table=work.advertising_contact);
 data &tmplib..advertising_contact_tmp         ;
     set work.advertising_contact;
  if advertising_contact_dttm ne . then advertising_contact_dttm = tzoneu2s(advertising_contact_dttm,&timeZone_Value.);if advertising_contact_dttm_tz ne . then advertising_contact_dttm_tz = tzoneu2s(advertising_contact_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :advertising_contact_tmp         , advertising_contact);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..advertising_contact using &tmpdbschema..advertising_contact_tmp         
         on (advertising_contact.event_id=advertising_contact_tmp.event_id)
        when matched then  
        update set advertising_contact.advertising_contact_dttm = advertising_contact_tmp.advertising_contact_dttm , advertising_contact.advertising_contact_dttm_tz = advertising_contact_tmp.advertising_contact_dttm_tz , advertising_contact.advertising_platform_nm = advertising_contact_tmp.advertising_platform_nm , advertising_contact.aud_occurrence_id = advertising_contact_tmp.aud_occurrence_id , advertising_contact.audience_id = advertising_contact_tmp.audience_id , advertising_contact.channel_nm = advertising_contact_tmp.channel_nm , advertising_contact.context_type_nm = advertising_contact_tmp.context_type_nm , advertising_contact.context_val = advertising_contact_tmp.context_val , advertising_contact.event_designed_id = advertising_contact_tmp.event_designed_id , advertising_contact.event_nm = advertising_contact_tmp.event_nm , advertising_contact.identity_id = advertising_contact_tmp.identity_id , advertising_contact.journey_id = advertising_contact_tmp.journey_id , advertising_contact.journey_occurrence_id = advertising_contact_tmp.journey_occurrence_id , advertising_contact.load_dttm = advertising_contact_tmp.load_dttm , advertising_contact.occurrence_id = advertising_contact_tmp.occurrence_id , advertising_contact.response_tracking_cd = advertising_contact_tmp.response_tracking_cd , advertising_contact.segment_id = advertising_contact_tmp.segment_id , advertising_contact.segment_version_id = advertising_contact_tmp.segment_version_id , advertising_contact.task_action_nm = advertising_contact_tmp.task_action_nm , advertising_contact.task_id = advertising_contact_tmp.task_id , advertising_contact.task_version_id = advertising_contact_tmp.task_version_id
        when not matched then insert ( 
        advertising_contact_dttm,advertising_contact_dttm_tz,advertising_platform_nm,aud_occurrence_id,audience_id,channel_nm,context_type_nm,context_val,event_designed_id,event_id,event_nm,identity_id,journey_id,journey_occurrence_id,load_dttm,occurrence_id,response_tracking_cd,segment_id,segment_version_id,task_action_nm,task_id,task_version_id
         ) values ( 
        advertising_contact_tmp.advertising_contact_dttm,advertising_contact_tmp.advertising_contact_dttm_tz,advertising_contact_tmp.advertising_platform_nm,advertising_contact_tmp.aud_occurrence_id,advertising_contact_tmp.audience_id,advertising_contact_tmp.channel_nm,advertising_contact_tmp.context_type_nm,advertising_contact_tmp.context_val,advertising_contact_tmp.event_designed_id,advertising_contact_tmp.event_id,advertising_contact_tmp.event_nm,advertising_contact_tmp.identity_id,advertising_contact_tmp.journey_id,advertising_contact_tmp.journey_occurrence_id,advertising_contact_tmp.load_dttm,advertising_contact_tmp.occurrence_id,advertising_contact_tmp.response_tracking_cd,advertising_contact_tmp.segment_id,advertising_contact_tmp.segment_version_id,advertising_contact_tmp.task_action_nm,advertising_contact_tmp.task_id,advertising_contact_tmp.task_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :advertising_contact_tmp         , advertising_contact, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..advertising_contact_tmp         ;
    quit;
    %put ######## Staging table: advertising_contact_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..advertising_contact;
      drop table work.advertising_contact;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..asset_details_tmp               ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=asset_details, table_keys=%str(asset_id), out_table=work.asset_details);
 data &tmplib..asset_details_tmp               ;
     set work.asset_details;
  if asset_locked_dttm ne . then asset_locked_dttm = tzoneu2s(asset_locked_dttm,&timeZone_Value.);if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if download_disabled_dttm ne . then download_disabled_dttm = tzoneu2s(download_disabled_dttm,&timeZone_Value.);if expired_dttm ne . then expired_dttm = tzoneu2s(expired_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if recycled_dttm ne . then recycled_dttm = tzoneu2s(recycled_dttm,&timeZone_Value.) ;
  if asset_id='' then asset_id='-';
 run;
 %ErrCheck (Failed to Append Data to :asset_details_tmp               , asset_details);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..asset_details using &tmpdbschema..asset_details_tmp               
         on (asset_details.asset_id=asset_details_tmp.asset_id)
        when matched then  
        update set asset_details.asset_deleted_flg = asset_details_tmp.asset_deleted_flg , asset_details.asset_desc = asset_details_tmp.asset_desc , asset_details.asset_locked_by_usernm = asset_details_tmp.asset_locked_by_usernm , asset_details.asset_locked_dttm = asset_details_tmp.asset_locked_dttm , asset_details.asset_locked_flg = asset_details_tmp.asset_locked_flg , asset_details.asset_nm = asset_details_tmp.asset_nm , asset_details.asset_owner_usernm = asset_details_tmp.asset_owner_usernm , asset_details.asset_process_status = asset_details_tmp.asset_process_status , asset_details.asset_sk = asset_details_tmp.asset_sk , asset_details.asset_source_nm = asset_details_tmp.asset_source_nm , asset_details.asset_source_type = asset_details_tmp.asset_source_type , asset_details.average_user_rating_val = asset_details_tmp.average_user_rating_val , asset_details.created_by_usernm = asset_details_tmp.created_by_usernm , asset_details.created_dttm = asset_details_tmp.created_dttm , asset_details.download_disabled_by_usernm = asset_details_tmp.download_disabled_by_usernm , asset_details.download_disabled_dttm = asset_details_tmp.download_disabled_dttm , asset_details.download_disabled_flg = asset_details_tmp.download_disabled_flg , asset_details.entity_attribute_enabled_flg = asset_details_tmp.entity_attribute_enabled_flg , asset_details.entity_revision_enabled_flg = asset_details_tmp.entity_revision_enabled_flg , asset_details.entity_status_cd = asset_details_tmp.entity_status_cd , asset_details.entity_subtype_enabled_flg = asset_details_tmp.entity_subtype_enabled_flg , asset_details.entity_subtype_nm = asset_details_tmp.entity_subtype_nm , asset_details.entity_table_nm = asset_details_tmp.entity_table_nm , asset_details.entity_type_nm = asset_details_tmp.entity_type_nm , asset_details.entity_type_usage_cd = asset_details_tmp.entity_type_usage_cd , asset_details.expired_dttm = asset_details_tmp.expired_dttm , asset_details.expired_flg = asset_details_tmp.expired_flg , asset_details.external_sharing_error_dt = asset_details_tmp.external_sharing_error_dt , asset_details.external_sharing_error_msg = asset_details_tmp.external_sharing_error_msg , asset_details.folder_deleted_flg = asset_details_tmp.folder_deleted_flg , asset_details.folder_desc = asset_details_tmp.folder_desc , asset_details.folder_entity_status_cd = asset_details_tmp.folder_entity_status_cd , asset_details.folder_id = asset_details_tmp.folder_id , asset_details.folder_level = asset_details_tmp.folder_level , asset_details.folder_nm = asset_details_tmp.folder_nm , asset_details.folder_owner_usernm = asset_details_tmp.folder_owner_usernm , asset_details.folder_path = asset_details_tmp.folder_path , asset_details.folder_sk = asset_details_tmp.folder_sk , asset_details.last_modified_by_usernm = asset_details_tmp.last_modified_by_usernm , asset_details.last_modified_dttm = asset_details_tmp.last_modified_dttm , asset_details.load_dttm = asset_details_tmp.load_dttm , asset_details.process_id = asset_details_tmp.process_id , asset_details.process_task_id = asset_details_tmp.process_task_id , asset_details.public_link = asset_details_tmp.public_link , asset_details.public_media_id = asset_details_tmp.public_media_id , asset_details.public_url = asset_details_tmp.public_url , asset_details.recycled_by_usernm = asset_details_tmp.recycled_by_usernm , asset_details.recycled_dttm = asset_details_tmp.recycled_dttm , asset_details.total_user_rating_val = asset_details_tmp.total_user_rating_val , asset_details.user_rating_cnt = asset_details_tmp.user_rating_cnt
        when not matched then insert ( 
        asset_deleted_flg,asset_desc,asset_id,asset_locked_by_usernm,asset_locked_dttm,asset_locked_flg,asset_nm,asset_owner_usernm,asset_process_status,asset_sk,asset_source_nm,asset_source_type,average_user_rating_val,created_by_usernm,created_dttm,download_disabled_by_usernm,download_disabled_dttm,download_disabled_flg,entity_attribute_enabled_flg,entity_revision_enabled_flg,entity_status_cd,entity_subtype_enabled_flg,entity_subtype_nm,entity_table_nm,entity_type_nm,entity_type_usage_cd,expired_dttm,expired_flg,external_sharing_error_dt,external_sharing_error_msg,folder_deleted_flg,folder_desc,folder_entity_status_cd,folder_id,folder_level,folder_nm,folder_owner_usernm,folder_path,folder_sk,last_modified_by_usernm,last_modified_dttm,load_dttm,process_id,process_task_id,public_link,public_media_id,public_url,recycled_by_usernm,recycled_dttm,total_user_rating_val,user_rating_cnt
         ) values ( 
        asset_details_tmp.asset_deleted_flg,asset_details_tmp.asset_desc,asset_details_tmp.asset_id,asset_details_tmp.asset_locked_by_usernm,asset_details_tmp.asset_locked_dttm,asset_details_tmp.asset_locked_flg,asset_details_tmp.asset_nm,asset_details_tmp.asset_owner_usernm,asset_details_tmp.asset_process_status,asset_details_tmp.asset_sk,asset_details_tmp.asset_source_nm,asset_details_tmp.asset_source_type,asset_details_tmp.average_user_rating_val,asset_details_tmp.created_by_usernm,asset_details_tmp.created_dttm,asset_details_tmp.download_disabled_by_usernm,asset_details_tmp.download_disabled_dttm,asset_details_tmp.download_disabled_flg,asset_details_tmp.entity_attribute_enabled_flg,asset_details_tmp.entity_revision_enabled_flg,asset_details_tmp.entity_status_cd,asset_details_tmp.entity_subtype_enabled_flg,asset_details_tmp.entity_subtype_nm,asset_details_tmp.entity_table_nm,asset_details_tmp.entity_type_nm,asset_details_tmp.entity_type_usage_cd,asset_details_tmp.expired_dttm,asset_details_tmp.expired_flg,asset_details_tmp.external_sharing_error_dt,asset_details_tmp.external_sharing_error_msg,asset_details_tmp.folder_deleted_flg,asset_details_tmp.folder_desc,asset_details_tmp.folder_entity_status_cd,asset_details_tmp.folder_id,asset_details_tmp.folder_level,asset_details_tmp.folder_nm,asset_details_tmp.folder_owner_usernm,asset_details_tmp.folder_path,asset_details_tmp.folder_sk,asset_details_tmp.last_modified_by_usernm,asset_details_tmp.last_modified_dttm,asset_details_tmp.load_dttm,asset_details_tmp.process_id,asset_details_tmp.process_task_id,asset_details_tmp.public_link,asset_details_tmp.public_media_id,asset_details_tmp.public_url,asset_details_tmp.recycled_by_usernm,asset_details_tmp.recycled_dttm,asset_details_tmp.total_user_rating_val,asset_details_tmp.user_rating_cnt
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :asset_details_tmp               , asset_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..asset_details_tmp               ;
    quit;
    %put ######## Staging table: asset_details_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..asset_details;
      drop table work.asset_details;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..asset_details_custom_prop_tmp   ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=asset_details_custom_prop, table_keys=%str(asset_id,attr_id), out_table=work.asset_details_custom_prop);
 data &tmplib..asset_details_custom_prop_tmp   ;
     set work.asset_details_custom_prop;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if asset_id='' then asset_id='-'; if attr_id='' then attr_id='-';
 run;
 %ErrCheck (Failed to Append Data to :asset_details_custom_prop_tmp   , asset_details_custom_prop);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..asset_details_custom_prop using &tmpdbschema..asset_details_custom_prop_tmp   
         on (asset_details_custom_prop.asset_id=asset_details_custom_prop_tmp.asset_id and asset_details_custom_prop.attr_id=asset_details_custom_prop_tmp.attr_id)
        when matched then  
        update set asset_details_custom_prop.attr_cd = asset_details_custom_prop_tmp.attr_cd , asset_details_custom_prop.attr_group_cd = asset_details_custom_prop_tmp.attr_group_cd , asset_details_custom_prop.attr_group_id = asset_details_custom_prop_tmp.attr_group_id , asset_details_custom_prop.attr_group_nm = asset_details_custom_prop_tmp.attr_group_nm , asset_details_custom_prop.attr_nm = asset_details_custom_prop_tmp.attr_nm , asset_details_custom_prop.attr_val = asset_details_custom_prop_tmp.attr_val , asset_details_custom_prop.created_by_usernm = asset_details_custom_prop_tmp.created_by_usernm , asset_details_custom_prop.created_dttm = asset_details_custom_prop_tmp.created_dttm , asset_details_custom_prop.data_formatter = asset_details_custom_prop_tmp.data_formatter , asset_details_custom_prop.data_type = asset_details_custom_prop_tmp.data_type , asset_details_custom_prop.is_grid_flg = asset_details_custom_prop_tmp.is_grid_flg , asset_details_custom_prop.is_obsolete_flg = asset_details_custom_prop_tmp.is_obsolete_flg , asset_details_custom_prop.last_modified_dttm = asset_details_custom_prop_tmp.last_modified_dttm , asset_details_custom_prop.last_modified_usernm = asset_details_custom_prop_tmp.last_modified_usernm , asset_details_custom_prop.load_dttm = asset_details_custom_prop_tmp.load_dttm , asset_details_custom_prop.remote_pklist_tab_col = asset_details_custom_prop_tmp.remote_pklist_tab_col
        when not matched then insert ( 
        asset_id,attr_cd,attr_group_cd,attr_group_id,attr_group_nm,attr_id,attr_nm,attr_val,created_by_usernm,created_dttm,data_formatter,data_type,is_grid_flg,is_obsolete_flg,last_modified_dttm,last_modified_usernm,load_dttm,remote_pklist_tab_col
         ) values ( 
        asset_details_custom_prop_tmp.asset_id,asset_details_custom_prop_tmp.attr_cd,asset_details_custom_prop_tmp.attr_group_cd,asset_details_custom_prop_tmp.attr_group_id,asset_details_custom_prop_tmp.attr_group_nm,asset_details_custom_prop_tmp.attr_id,asset_details_custom_prop_tmp.attr_nm,asset_details_custom_prop_tmp.attr_val,asset_details_custom_prop_tmp.created_by_usernm,asset_details_custom_prop_tmp.created_dttm,asset_details_custom_prop_tmp.data_formatter,asset_details_custom_prop_tmp.data_type,asset_details_custom_prop_tmp.is_grid_flg,asset_details_custom_prop_tmp.is_obsolete_flg,asset_details_custom_prop_tmp.last_modified_dttm,asset_details_custom_prop_tmp.last_modified_usernm,asset_details_custom_prop_tmp.load_dttm,asset_details_custom_prop_tmp.remote_pklist_tab_col
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :asset_details_custom_prop_tmp   , asset_details_custom_prop, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..asset_details_custom_prop_tmp   ;
    quit;
    %put ######## Staging table: asset_details_custom_prop_tmp    Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..asset_details_custom_prop;
      drop table work.asset_details_custom_prop;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..asset_folder_details_tmp        ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=asset_folder_details, table_keys=%str(folder_id), out_table=work.asset_folder_details);
 data &tmplib..asset_folder_details_tmp        ;
     set work.asset_folder_details;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if folder_id='' then folder_id='-';
 run;
 %ErrCheck (Failed to Append Data to :asset_folder_details_tmp        , asset_folder_details);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..asset_folder_details using &tmpdbschema..asset_folder_details_tmp        
         on (asset_folder_details.folder_id=asset_folder_details_tmp.folder_id)
        when matched then  
        update set asset_folder_details.created_by_usernm = asset_folder_details_tmp.created_by_usernm , asset_folder_details.created_dttm = asset_folder_details_tmp.created_dttm , asset_folder_details.deleted_flg = asset_folder_details_tmp.deleted_flg , asset_folder_details.entity_status_cd = asset_folder_details_tmp.entity_status_cd , asset_folder_details.folder_desc = asset_folder_details_tmp.folder_desc , asset_folder_details.folder_level = asset_folder_details_tmp.folder_level , asset_folder_details.folder_nm = asset_folder_details_tmp.folder_nm , asset_folder_details.folder_owner_usernm = asset_folder_details_tmp.folder_owner_usernm , asset_folder_details.folder_path = asset_folder_details_tmp.folder_path , asset_folder_details.last_modified_by_usernm = asset_folder_details_tmp.last_modified_by_usernm , asset_folder_details.last_modified_dttm = asset_folder_details_tmp.last_modified_dttm , asset_folder_details.load_dttm = asset_folder_details_tmp.load_dttm
        when not matched then insert ( 
        created_by_usernm,created_dttm,deleted_flg,entity_status_cd,folder_desc,folder_id,folder_level,folder_nm,folder_owner_usernm,folder_path,last_modified_by_usernm,last_modified_dttm,load_dttm
         ) values ( 
        asset_folder_details_tmp.created_by_usernm,asset_folder_details_tmp.created_dttm,asset_folder_details_tmp.deleted_flg,asset_folder_details_tmp.entity_status_cd,asset_folder_details_tmp.folder_desc,asset_folder_details_tmp.folder_id,asset_folder_details_tmp.folder_level,asset_folder_details_tmp.folder_nm,asset_folder_details_tmp.folder_owner_usernm,asset_folder_details_tmp.folder_path,asset_folder_details_tmp.last_modified_by_usernm,asset_folder_details_tmp.last_modified_dttm,asset_folder_details_tmp.load_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :asset_folder_details_tmp        , asset_folder_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..asset_folder_details_tmp        ;
    quit;
    %put ######## Staging table: asset_folder_details_tmp         Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..asset_folder_details;
      drop table work.asset_folder_details;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..asset_rendition_details_tmp     ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=asset_rendition_details, table_keys=%str(asset_id,rendition_id,revision_id,revision_no), out_table=work.asset_rendition_details);
 data &tmplib..asset_rendition_details_tmp     ;
     set work.asset_rendition_details;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if asset_id='' then asset_id='-'; if rendition_id='' then rendition_id='-'; if revision_id='' then revision_id='-';
 run;
 %ErrCheck (Failed to Append Data to :asset_rendition_details_tmp     , asset_rendition_details);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..asset_rendition_details using &tmpdbschema..asset_rendition_details_tmp     
         on (asset_rendition_details.asset_id=asset_rendition_details_tmp.asset_id and asset_rendition_details.rendition_id=asset_rendition_details_tmp.rendition_id and asset_rendition_details.revision_id=asset_rendition_details_tmp.revision_id and asset_rendition_details.revision_no=asset_rendition_details_tmp.revision_no)
        when matched then  
        update set asset_rendition_details.created_by_usernm = asset_rendition_details_tmp.created_by_usernm , asset_rendition_details.created_dttm = asset_rendition_details_tmp.created_dttm , asset_rendition_details.current_revision_flg = asset_rendition_details_tmp.current_revision_flg , asset_rendition_details.download_cnt = asset_rendition_details_tmp.download_cnt , asset_rendition_details.entity_status_cd = asset_rendition_details_tmp.entity_status_cd , asset_rendition_details.file_format = asset_rendition_details_tmp.file_format , asset_rendition_details.file_nm = asset_rendition_details_tmp.file_nm , asset_rendition_details.file_size = asset_rendition_details_tmp.file_size , asset_rendition_details.last_modified_by_usernm = asset_rendition_details_tmp.last_modified_by_usernm , asset_rendition_details.last_modified_dttm = asset_rendition_details_tmp.last_modified_dttm , asset_rendition_details.last_modified_status_cd = asset_rendition_details_tmp.last_modified_status_cd , asset_rendition_details.load_dttm = asset_rendition_details_tmp.load_dttm , asset_rendition_details.media_depth = asset_rendition_details_tmp.media_depth , asset_rendition_details.media_dpi = asset_rendition_details_tmp.media_dpi , asset_rendition_details.media_height = asset_rendition_details_tmp.media_height , asset_rendition_details.media_width = asset_rendition_details_tmp.media_width , asset_rendition_details.rend_deleted_flg = asset_rendition_details_tmp.rend_deleted_flg , asset_rendition_details.rend_duration = asset_rendition_details_tmp.rend_duration , asset_rendition_details.rendition_generated_type_cd = asset_rendition_details_tmp.rendition_generated_type_cd , asset_rendition_details.rendition_nm = asset_rendition_details_tmp.rendition_nm , asset_rendition_details.rendition_type_cd = asset_rendition_details_tmp.rendition_type_cd , asset_rendition_details.rev_deleted_flg = asset_rendition_details_tmp.rev_deleted_flg , asset_rendition_details.revision_comment_txt = asset_rendition_details_tmp.revision_comment_txt
        when not matched then insert ( 
        asset_id,created_by_usernm,created_dttm,current_revision_flg,download_cnt,entity_status_cd,file_format,file_nm,file_size,last_modified_by_usernm,last_modified_dttm,last_modified_status_cd,load_dttm,media_depth,media_dpi,media_height,media_width,rend_deleted_flg,rend_duration,rendition_generated_type_cd,rendition_id,rendition_nm,rendition_type_cd,rev_deleted_flg,revision_comment_txt,revision_id,revision_no
         ) values ( 
        asset_rendition_details_tmp.asset_id,asset_rendition_details_tmp.created_by_usernm,asset_rendition_details_tmp.created_dttm,asset_rendition_details_tmp.current_revision_flg,asset_rendition_details_tmp.download_cnt,asset_rendition_details_tmp.entity_status_cd,asset_rendition_details_tmp.file_format,asset_rendition_details_tmp.file_nm,asset_rendition_details_tmp.file_size,asset_rendition_details_tmp.last_modified_by_usernm,asset_rendition_details_tmp.last_modified_dttm,asset_rendition_details_tmp.last_modified_status_cd,asset_rendition_details_tmp.load_dttm,asset_rendition_details_tmp.media_depth,asset_rendition_details_tmp.media_dpi,asset_rendition_details_tmp.media_height,asset_rendition_details_tmp.media_width,asset_rendition_details_tmp.rend_deleted_flg,asset_rendition_details_tmp.rend_duration,asset_rendition_details_tmp.rendition_generated_type_cd,asset_rendition_details_tmp.rendition_id,asset_rendition_details_tmp.rendition_nm,asset_rendition_details_tmp.rendition_type_cd,asset_rendition_details_tmp.rev_deleted_flg,asset_rendition_details_tmp.revision_comment_txt,asset_rendition_details_tmp.revision_id,asset_rendition_details_tmp.revision_no
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :asset_rendition_details_tmp     , asset_rendition_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..asset_rendition_details_tmp     ;
    quit;
    %put ######## Staging table: asset_rendition_details_tmp      Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..asset_rendition_details;
      drop table work.asset_rendition_details;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..asset_revision_tmp              ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=asset_revision, table_keys=%str(asset_id,revision_id,revision_no), out_table=work.asset_revision);
 data &tmplib..asset_revision_tmp              ;
     set work.asset_revision;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if asset_id='' then asset_id='-'; if revision_id='' then revision_id='-';
 run;
 %ErrCheck (Failed to Append Data to :asset_revision_tmp              , asset_revision);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..asset_revision using &tmpdbschema..asset_revision_tmp              
         on (asset_revision.asset_id=asset_revision_tmp.asset_id and asset_revision.revision_id=asset_revision_tmp.revision_id and asset_revision.revision_no=asset_revision_tmp.revision_no)
        when matched then  
        update set asset_revision.created_by_usernm = asset_revision_tmp.created_by_usernm , asset_revision.created_dttm = asset_revision_tmp.created_dttm , asset_revision.current_revision_flg = asset_revision_tmp.current_revision_flg , asset_revision.deleted_flg = asset_revision_tmp.deleted_flg , asset_revision.entity_status_cd = asset_revision_tmp.entity_status_cd , asset_revision.last_modified_by_usernm = asset_revision_tmp.last_modified_by_usernm , asset_revision.last_modified_dttm = asset_revision_tmp.last_modified_dttm , asset_revision.load_dttm = asset_revision_tmp.load_dttm , asset_revision.revision_comment_txt = asset_revision_tmp.revision_comment_txt
        when not matched then insert ( 
        asset_id,created_by_usernm,created_dttm,current_revision_flg,deleted_flg,entity_status_cd,last_modified_by_usernm,last_modified_dttm,load_dttm,revision_comment_txt,revision_id,revision_no
         ) values ( 
        asset_revision_tmp.asset_id,asset_revision_tmp.created_by_usernm,asset_revision_tmp.created_dttm,asset_revision_tmp.current_revision_flg,asset_revision_tmp.deleted_flg,asset_revision_tmp.entity_status_cd,asset_revision_tmp.last_modified_by_usernm,asset_revision_tmp.last_modified_dttm,asset_revision_tmp.load_dttm,asset_revision_tmp.revision_comment_txt,asset_revision_tmp.revision_id,asset_revision_tmp.revision_no
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :asset_revision_tmp              , asset_revision, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..asset_revision_tmp              ;
    quit;
    %put ######## Staging table: asset_revision_tmp               Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..asset_revision;
      drop table work.asset_revision;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..audience_membership_change_tmp  ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=audience_membership_change, table_keys=%str(event_id), out_table=work.audience_membership_change);
 data &tmplib..audience_membership_change_tmp  ;
     set work.audience_membership_change;
  if audience_change_dttm ne . then audience_change_dttm = tzoneu2s(audience_change_dttm,&timeZone_Value.);if audience_change_dttm_tz ne . then audience_change_dttm_tz = tzoneu2s(audience_change_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :audience_membership_change_tmp  , audience_membership_change);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..audience_membership_change using &tmpdbschema..audience_membership_change_tmp  
         on (audience_membership_change.event_id=audience_membership_change_tmp.event_id)
        when matched then  
        update set audience_membership_change.aud_occurrence_id = audience_membership_change_tmp.aud_occurrence_id , audience_membership_change.audience_change_dttm = audience_membership_change_tmp.audience_change_dttm , audience_membership_change.audience_change_dttm_tz = audience_membership_change_tmp.audience_change_dttm_tz , audience_membership_change.audience_id = audience_membership_change_tmp.audience_id , audience_membership_change.event_nm = audience_membership_change_tmp.event_nm , audience_membership_change.identity_id = audience_membership_change_tmp.identity_id , audience_membership_change.load_dttm = audience_membership_change_tmp.load_dttm
        when not matched then insert ( 
        aud_occurrence_id,audience_change_dttm,audience_change_dttm_tz,audience_id,event_id,event_nm,identity_id,load_dttm
         ) values ( 
        audience_membership_change_tmp.aud_occurrence_id,audience_membership_change_tmp.audience_change_dttm,audience_membership_change_tmp.audience_change_dttm_tz,audience_membership_change_tmp.audience_id,audience_membership_change_tmp.event_id,audience_membership_change_tmp.event_nm,audience_membership_change_tmp.identity_id,audience_membership_change_tmp.load_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :audience_membership_change_tmp  , audience_membership_change, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..audience_membership_change_tmp  ;
    quit;
    %put ######## Staging table: audience_membership_change_tmp   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..audience_membership_change;
      drop table work.audience_membership_change;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..business_process_details_tmp    ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=business_process_details, table_keys=%str(detail_id,event_designed_id,process_dttm,process_instance_no,process_nm,process_step_nm,step_order_no), out_table=work.business_process_details);
 data &tmplib..business_process_details_tmp    ;
     set work.business_process_details;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if process_dttm ne . then process_dttm = tzoneu2s(process_dttm,&timeZone_Value.);if process_dttm_tz ne . then process_dttm_tz = tzoneu2s(process_dttm_tz,&timeZone_Value.);if process_exception_dttm ne . then process_exception_dttm = tzoneu2s(process_exception_dttm,&timeZone_Value.);if process_exception_dttm_tz ne . then process_exception_dttm_tz = tzoneu2s(process_exception_dttm_tz,&timeZone_Value.) ;
  if detail_id='' then detail_id='-'; if event_designed_id='' then event_designed_id='-'; if process_nm='' then process_nm='-'; if process_step_nm='' then process_step_nm='-';
 run;
 %ErrCheck (Failed to Append Data to :business_process_details_tmp    , business_process_details);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..business_process_details using &tmpdbschema..business_process_details_tmp    
         on (business_process_details.detail_id=business_process_details_tmp.detail_id and business_process_details.event_designed_id=business_process_details_tmp.event_designed_id and business_process_details.process_dttm=business_process_details_tmp.process_dttm and business_process_details.process_instance_no=business_process_details_tmp.process_instance_no and business_process_details.process_nm=business_process_details_tmp.process_nm and business_process_details.process_step_nm=business_process_details_tmp.process_step_nm and business_process_details.step_order_no=business_process_details_tmp.step_order_no)
        when matched then  
        update set business_process_details.attribute1_txt = business_process_details_tmp.attribute1_txt , business_process_details.attribute2_txt = business_process_details_tmp.attribute2_txt , business_process_details.detail_id_hex = business_process_details_tmp.detail_id_hex , business_process_details.event_id = business_process_details_tmp.event_id , business_process_details.event_nm = business_process_details_tmp.event_nm , business_process_details.event_source_cd = business_process_details_tmp.event_source_cd , business_process_details.identity_id = business_process_details_tmp.identity_id , business_process_details.is_completion_flg = business_process_details_tmp.is_completion_flg , business_process_details.is_start_flg = business_process_details_tmp.is_start_flg , business_process_details.load_dttm = business_process_details_tmp.load_dttm , business_process_details.next_detail_id = business_process_details_tmp.next_detail_id , business_process_details.process_attempt_cnt = business_process_details_tmp.process_attempt_cnt , business_process_details.process_details_sk = business_process_details_tmp.process_details_sk , business_process_details.process_dttm_tz = business_process_details_tmp.process_dttm_tz , business_process_details.process_exception_dttm = business_process_details_tmp.process_exception_dttm , business_process_details.process_exception_dttm_tz = business_process_details_tmp.process_exception_dttm_tz , business_process_details.process_exception_txt = business_process_details_tmp.process_exception_txt , business_process_details.session_id = business_process_details_tmp.session_id , business_process_details.session_id_hex = business_process_details_tmp.session_id_hex , business_process_details.visit_id = business_process_details_tmp.visit_id , business_process_details.visit_id_hex = business_process_details_tmp.visit_id_hex
        when not matched then insert ( 
        attribute1_txt,attribute2_txt,detail_id,detail_id_hex,event_designed_id,event_id,event_nm,event_source_cd,identity_id,is_completion_flg,is_start_flg,load_dttm,next_detail_id,process_attempt_cnt,process_details_sk,process_dttm,process_dttm_tz,process_exception_dttm,process_exception_dttm_tz,process_exception_txt,process_instance_no,process_nm,process_step_nm,session_id,session_id_hex,step_order_no,visit_id,visit_id_hex
         ) values ( 
        business_process_details_tmp.attribute1_txt,business_process_details_tmp.attribute2_txt,business_process_details_tmp.detail_id,business_process_details_tmp.detail_id_hex,business_process_details_tmp.event_designed_id,business_process_details_tmp.event_id,business_process_details_tmp.event_nm,business_process_details_tmp.event_source_cd,business_process_details_tmp.identity_id,business_process_details_tmp.is_completion_flg,business_process_details_tmp.is_start_flg,business_process_details_tmp.load_dttm,business_process_details_tmp.next_detail_id,business_process_details_tmp.process_attempt_cnt,business_process_details_tmp.process_details_sk,business_process_details_tmp.process_dttm,business_process_details_tmp.process_dttm_tz,business_process_details_tmp.process_exception_dttm,business_process_details_tmp.process_exception_dttm_tz,business_process_details_tmp.process_exception_txt,business_process_details_tmp.process_instance_no,business_process_details_tmp.process_nm,business_process_details_tmp.process_step_nm,business_process_details_tmp.session_id,business_process_details_tmp.session_id_hex,business_process_details_tmp.step_order_no,business_process_details_tmp.visit_id,business_process_details_tmp.visit_id_hex
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :business_process_details_tmp    , business_process_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..business_process_details_tmp    ;
    quit;
    %put ######## Staging table: business_process_details_tmp     Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..business_process_details;
      drop table work.business_process_details;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cart_activity_details_tmp       ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cart_activity_details, table_keys=%str(activity_dttm,detail_id,product_id,product_nm,product_sku), out_table=work.cart_activity_details);
 data &tmplib..cart_activity_details_tmp       ;
     set work.cart_activity_details;
  if activity_dttm ne . then activity_dttm = tzoneu2s(activity_dttm,&timeZone_Value.);if activity_dttm_tz ne . then activity_dttm_tz = tzoneu2s(activity_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if detail_id='' then detail_id='-'; if product_id='' then product_id='-'; if product_nm='' then product_nm='-'; if product_sku='' then product_sku='-';
 run;
 %ErrCheck (Failed to Append Data to :cart_activity_details_tmp       , cart_activity_details);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cart_activity_details using &tmpdbschema..cart_activity_details_tmp       
         on (cart_activity_details.activity_dttm=cart_activity_details_tmp.activity_dttm and cart_activity_details.detail_id=cart_activity_details_tmp.detail_id and cart_activity_details.product_id=cart_activity_details_tmp.product_id and cart_activity_details.product_nm=cart_activity_details_tmp.product_nm and cart_activity_details.product_sku=cart_activity_details_tmp.product_sku)
        when matched then  
        update set cart_activity_details.activity_cd = cart_activity_details_tmp.activity_cd , cart_activity_details.activity_dttm_tz = cart_activity_details_tmp.activity_dttm_tz , cart_activity_details.availability_message_txt = cart_activity_details_tmp.availability_message_txt , cart_activity_details.cart_activity_sk = cart_activity_details_tmp.cart_activity_sk , cart_activity_details.cart_id = cart_activity_details_tmp.cart_id , cart_activity_details.cart_nm = cart_activity_details_tmp.cart_nm , cart_activity_details.channel_nm = cart_activity_details_tmp.channel_nm , cart_activity_details.currency_cd = cart_activity_details_tmp.currency_cd , cart_activity_details.detail_id_hex = cart_activity_details_tmp.detail_id_hex , cart_activity_details.displayed_cart_amt = cart_activity_details_tmp.displayed_cart_amt , cart_activity_details.displayed_cart_items_no = cart_activity_details_tmp.displayed_cart_items_no , cart_activity_details.event_designed_id = cart_activity_details_tmp.event_designed_id , cart_activity_details.event_id = cart_activity_details_tmp.event_id , cart_activity_details.event_key_cd = cart_activity_details_tmp.event_key_cd , cart_activity_details.event_nm = cart_activity_details_tmp.event_nm , cart_activity_details.event_source_cd = cart_activity_details_tmp.event_source_cd , cart_activity_details.identity_id = cart_activity_details_tmp.identity_id , cart_activity_details.load_dttm = cart_activity_details_tmp.load_dttm , cart_activity_details.mobile_app_id = cart_activity_details_tmp.mobile_app_id , cart_activity_details.product_group_nm = cart_activity_details_tmp.product_group_nm , cart_activity_details.properties_map_doc = cart_activity_details_tmp.properties_map_doc , cart_activity_details.quantity_val = cart_activity_details_tmp.quantity_val , cart_activity_details.saving_message_txt = cart_activity_details_tmp.saving_message_txt , cart_activity_details.session_id = cart_activity_details_tmp.session_id , cart_activity_details.session_id_hex = cart_activity_details_tmp.session_id_hex , cart_activity_details.shipping_message_txt = cart_activity_details_tmp.shipping_message_txt , cart_activity_details.unit_price_amt = cart_activity_details_tmp.unit_price_amt , cart_activity_details.visit_id = cart_activity_details_tmp.visit_id , cart_activity_details.visit_id_hex = cart_activity_details_tmp.visit_id_hex
        when not matched then insert ( 
        activity_cd,activity_dttm,activity_dttm_tz,availability_message_txt,cart_activity_sk,cart_id,cart_nm,channel_nm,currency_cd,detail_id,detail_id_hex,displayed_cart_amt,displayed_cart_items_no,event_designed_id,event_id,event_key_cd,event_nm,event_source_cd,identity_id,load_dttm,mobile_app_id,product_group_nm,product_id,product_nm,product_sku,properties_map_doc,quantity_val,saving_message_txt,session_id,session_id_hex,shipping_message_txt,unit_price_amt,visit_id,visit_id_hex
         ) values ( 
        cart_activity_details_tmp.activity_cd,cart_activity_details_tmp.activity_dttm,cart_activity_details_tmp.activity_dttm_tz,cart_activity_details_tmp.availability_message_txt,cart_activity_details_tmp.cart_activity_sk,cart_activity_details_tmp.cart_id,cart_activity_details_tmp.cart_nm,cart_activity_details_tmp.channel_nm,cart_activity_details_tmp.currency_cd,cart_activity_details_tmp.detail_id,cart_activity_details_tmp.detail_id_hex,cart_activity_details_tmp.displayed_cart_amt,cart_activity_details_tmp.displayed_cart_items_no,cart_activity_details_tmp.event_designed_id,cart_activity_details_tmp.event_id,cart_activity_details_tmp.event_key_cd,cart_activity_details_tmp.event_nm,cart_activity_details_tmp.event_source_cd,cart_activity_details_tmp.identity_id,cart_activity_details_tmp.load_dttm,cart_activity_details_tmp.mobile_app_id,cart_activity_details_tmp.product_group_nm,cart_activity_details_tmp.product_id,cart_activity_details_tmp.product_nm,cart_activity_details_tmp.product_sku,cart_activity_details_tmp.properties_map_doc,cart_activity_details_tmp.quantity_val,cart_activity_details_tmp.saving_message_txt,cart_activity_details_tmp.session_id,cart_activity_details_tmp.session_id_hex,cart_activity_details_tmp.shipping_message_txt,cart_activity_details_tmp.unit_price_amt,cart_activity_details_tmp.visit_id,cart_activity_details_tmp.visit_id_hex
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cart_activity_details_tmp       , cart_activity_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cart_activity_details_tmp       ;
    quit;
    %put ######## Staging table: cart_activity_details_tmp        Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cart_activity_details;
      drop table work.cart_activity_details;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cc_budget_breakup_tmp           ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cc_budget_breakup, table_keys=%str(cost_center_id,planning_id), out_table=work.cc_budget_breakup);
 data &tmplib..cc_budget_breakup_tmp           ;
     set work.cc_budget_breakup;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if cost_center_id='' then cost_center_id='-'; if planning_id='' then planning_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cc_budget_breakup_tmp           , cc_budget_breakup);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cc_budget_breakup using &tmpdbschema..cc_budget_breakup_tmp           
         on (cc_budget_breakup.cost_center_id=cc_budget_breakup_tmp.cost_center_id and cc_budget_breakup.planning_id=cc_budget_breakup_tmp.planning_id)
        when matched then  
        update set cc_budget_breakup.cc_budget_distribution = cc_budget_breakup_tmp.cc_budget_distribution , cc_budget_breakup.cc_desc = cc_budget_breakup_tmp.cc_desc , cc_budget_breakup.cc_nm = cc_budget_breakup_tmp.cc_nm , cc_budget_breakup.cc_obsolete_flg = cc_budget_breakup_tmp.cc_obsolete_flg , cc_budget_breakup.cc_owner_usernm = cc_budget_breakup_tmp.cc_owner_usernm , cc_budget_breakup.created_by_usernm = cc_budget_breakup_tmp.created_by_usernm , cc_budget_breakup.created_dttm = cc_budget_breakup_tmp.created_dttm , cc_budget_breakup.fin_accnt_desc = cc_budget_breakup_tmp.fin_accnt_desc , cc_budget_breakup.fin_accnt_nm = cc_budget_breakup_tmp.fin_accnt_nm , cc_budget_breakup.fin_accnt_obsolete_flg = cc_budget_breakup_tmp.fin_accnt_obsolete_flg , cc_budget_breakup.gen_ledger_cd = cc_budget_breakup_tmp.gen_ledger_cd , cc_budget_breakup.last_modified_dttm = cc_budget_breakup_tmp.last_modified_dttm , cc_budget_breakup.last_modified_usernm = cc_budget_breakup_tmp.last_modified_usernm , cc_budget_breakup.load_dttm = cc_budget_breakup_tmp.load_dttm , cc_budget_breakup.planning_nm = cc_budget_breakup_tmp.planning_nm
        when not matched then insert ( 
        cc_budget_distribution,cc_desc,cc_nm,cc_obsolete_flg,cc_owner_usernm,cost_center_id,created_by_usernm,created_dttm,fin_accnt_desc,fin_accnt_nm,fin_accnt_obsolete_flg,gen_ledger_cd,last_modified_dttm,last_modified_usernm,load_dttm,planning_id,planning_nm
         ) values ( 
        cc_budget_breakup_tmp.cc_budget_distribution,cc_budget_breakup_tmp.cc_desc,cc_budget_breakup_tmp.cc_nm,cc_budget_breakup_tmp.cc_obsolete_flg,cc_budget_breakup_tmp.cc_owner_usernm,cc_budget_breakup_tmp.cost_center_id,cc_budget_breakup_tmp.created_by_usernm,cc_budget_breakup_tmp.created_dttm,cc_budget_breakup_tmp.fin_accnt_desc,cc_budget_breakup_tmp.fin_accnt_nm,cc_budget_breakup_tmp.fin_accnt_obsolete_flg,cc_budget_breakup_tmp.gen_ledger_cd,cc_budget_breakup_tmp.last_modified_dttm,cc_budget_breakup_tmp.last_modified_usernm,cc_budget_breakup_tmp.load_dttm,cc_budget_breakup_tmp.planning_id,cc_budget_breakup_tmp.planning_nm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cc_budget_breakup_tmp           , cc_budget_breakup, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cc_budget_breakup_tmp           ;
    quit;
    %put ######## Staging table: cc_budget_breakup_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cc_budget_breakup;
      drop table work.cc_budget_breakup;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cc_budget_breakup_ccbdgt_tmp    ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cc_budget_breakup_ccbdgt, table_keys=%str(cost_center_id,fp_id,planning_id), out_table=work.cc_budget_breakup_ccbdgt);
 data &tmplib..cc_budget_breakup_ccbdgt_tmp    ;
     set work.cc_budget_breakup_ccbdgt;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if cost_center_id='' then cost_center_id='-'; if fp_id='' then fp_id='-'; if planning_id='' then planning_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cc_budget_breakup_ccbdgt_tmp    , cc_budget_breakup_ccbdgt);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cc_budget_breakup_ccbdgt using &tmpdbschema..cc_budget_breakup_ccbdgt_tmp    
         on (cc_budget_breakup_ccbdgt.cost_center_id=cc_budget_breakup_ccbdgt_tmp.cost_center_id and cc_budget_breakup_ccbdgt.fp_id=cc_budget_breakup_ccbdgt_tmp.fp_id and cc_budget_breakup_ccbdgt.planning_id=cc_budget_breakup_ccbdgt_tmp.planning_id)
        when matched then  
        update set cc_budget_breakup_ccbdgt.cc_bdgt_amt = cc_budget_breakup_ccbdgt_tmp.cc_bdgt_amt , cc_budget_breakup_ccbdgt.cc_bdgt_budget_amt = cc_budget_breakup_ccbdgt_tmp.cc_bdgt_budget_amt , cc_budget_breakup_ccbdgt.cc_bdgt_budget_desc = cc_budget_breakup_ccbdgt_tmp.cc_bdgt_budget_desc , cc_budget_breakup_ccbdgt.cc_bdgt_cmtmnt_invoice_amt = cc_budget_breakup_ccbdgt_tmp.cc_bdgt_cmtmnt_invoice_amt , cc_budget_breakup_ccbdgt.cc_bdgt_cmtmnt_invoice_cnt = cc_budget_breakup_ccbdgt_tmp.cc_bdgt_cmtmnt_invoice_cnt , cc_budget_breakup_ccbdgt.cc_bdgt_cmtmnt_outstanding_amt = cc_budget_breakup_ccbdgt_tmp.cc_bdgt_cmtmnt_outstanding_amt , cc_budget_breakup_ccbdgt.cc_bdgt_cmtmnt_overspent_amt = cc_budget_breakup_ccbdgt_tmp.cc_bdgt_cmtmnt_overspent_amt , cc_budget_breakup_ccbdgt.cc_bdgt_committed_amt = cc_budget_breakup_ccbdgt_tmp.cc_bdgt_committed_amt , cc_budget_breakup_ccbdgt.cc_bdgt_direct_invoice_amt = cc_budget_breakup_ccbdgt_tmp.cc_bdgt_direct_invoice_amt , cc_budget_breakup_ccbdgt.cc_bdgt_invoiced_amt = cc_budget_breakup_ccbdgt_tmp.cc_bdgt_invoiced_amt , cc_budget_breakup_ccbdgt.cc_budget_distribution = cc_budget_breakup_ccbdgt_tmp.cc_budget_distribution , cc_budget_breakup_ccbdgt.cc_desc = cc_budget_breakup_ccbdgt_tmp.cc_desc , cc_budget_breakup_ccbdgt.cc_level_expense = cc_budget_breakup_ccbdgt_tmp.cc_level_expense , cc_budget_breakup_ccbdgt.cc_lvl_distribution = cc_budget_breakup_ccbdgt_tmp.cc_lvl_distribution , cc_budget_breakup_ccbdgt.cc_nm = cc_budget_breakup_ccbdgt_tmp.cc_nm , cc_budget_breakup_ccbdgt.cc_number = cc_budget_breakup_ccbdgt_tmp.cc_number , cc_budget_breakup_ccbdgt.cc_obsolete_flg = cc_budget_breakup_ccbdgt_tmp.cc_obsolete_flg , cc_budget_breakup_ccbdgt.cc_owner_usernm = cc_budget_breakup_ccbdgt_tmp.cc_owner_usernm , cc_budget_breakup_ccbdgt.cc_rldup_child_bdgt = cc_budget_breakup_ccbdgt_tmp.cc_rldup_child_bdgt , cc_budget_breakup_ccbdgt.cc_rldup_total_expense = cc_budget_breakup_ccbdgt_tmp.cc_rldup_total_expense , cc_budget_breakup_ccbdgt.created_by_usernm = cc_budget_breakup_ccbdgt_tmp.created_by_usernm , cc_budget_breakup_ccbdgt.created_dttm = cc_budget_breakup_ccbdgt_tmp.created_dttm , cc_budget_breakup_ccbdgt.fin_accnt_desc = cc_budget_breakup_ccbdgt_tmp.fin_accnt_desc , cc_budget_breakup_ccbdgt.fin_accnt_nm = cc_budget_breakup_ccbdgt_tmp.fin_accnt_nm , cc_budget_breakup_ccbdgt.fin_accnt_obsolete_flg = cc_budget_breakup_ccbdgt_tmp.fin_accnt_obsolete_flg , cc_budget_breakup_ccbdgt.fp_cls_ver = cc_budget_breakup_ccbdgt_tmp.fp_cls_ver , cc_budget_breakup_ccbdgt.fp_desc = cc_budget_breakup_ccbdgt_tmp.fp_desc , cc_budget_breakup_ccbdgt.fp_end_dt = cc_budget_breakup_ccbdgt_tmp.fp_end_dt , cc_budget_breakup_ccbdgt.fp_nm = cc_budget_breakup_ccbdgt_tmp.fp_nm , cc_budget_breakup_ccbdgt.fp_obsolete_flg = cc_budget_breakup_ccbdgt_tmp.fp_obsolete_flg , cc_budget_breakup_ccbdgt.fp_start_dt = cc_budget_breakup_ccbdgt_tmp.fp_start_dt , cc_budget_breakup_ccbdgt.gen_ledger_cd = cc_budget_breakup_ccbdgt_tmp.gen_ledger_cd , cc_budget_breakup_ccbdgt.last_modified_dttm = cc_budget_breakup_ccbdgt_tmp.last_modified_dttm , cc_budget_breakup_ccbdgt.last_modified_usernm = cc_budget_breakup_ccbdgt_tmp.last_modified_usernm , cc_budget_breakup_ccbdgt.load_dttm = cc_budget_breakup_ccbdgt_tmp.load_dttm , cc_budget_breakup_ccbdgt.planning_nm = cc_budget_breakup_ccbdgt_tmp.planning_nm
        when not matched then insert ( 
        cc_bdgt_amt,cc_bdgt_budget_amt,cc_bdgt_budget_desc,cc_bdgt_cmtmnt_invoice_amt,cc_bdgt_cmtmnt_invoice_cnt,cc_bdgt_cmtmnt_outstanding_amt,cc_bdgt_cmtmnt_overspent_amt,cc_bdgt_committed_amt,cc_bdgt_direct_invoice_amt,cc_bdgt_invoiced_amt,cc_budget_distribution,cc_desc,cc_level_expense,cc_lvl_distribution,cc_nm,cc_number,cc_obsolete_flg,cc_owner_usernm,cc_rldup_child_bdgt,cc_rldup_total_expense,cost_center_id,created_by_usernm,created_dttm,fin_accnt_desc,fin_accnt_nm,fin_accnt_obsolete_flg,fp_cls_ver,fp_desc,fp_end_dt,fp_id,fp_nm,fp_obsolete_flg,fp_start_dt,gen_ledger_cd,last_modified_dttm,last_modified_usernm,load_dttm,planning_id,planning_nm
         ) values ( 
        cc_budget_breakup_ccbdgt_tmp.cc_bdgt_amt,cc_budget_breakup_ccbdgt_tmp.cc_bdgt_budget_amt,cc_budget_breakup_ccbdgt_tmp.cc_bdgt_budget_desc,cc_budget_breakup_ccbdgt_tmp.cc_bdgt_cmtmnt_invoice_amt,cc_budget_breakup_ccbdgt_tmp.cc_bdgt_cmtmnt_invoice_cnt,cc_budget_breakup_ccbdgt_tmp.cc_bdgt_cmtmnt_outstanding_amt,cc_budget_breakup_ccbdgt_tmp.cc_bdgt_cmtmnt_overspent_amt,cc_budget_breakup_ccbdgt_tmp.cc_bdgt_committed_amt,cc_budget_breakup_ccbdgt_tmp.cc_bdgt_direct_invoice_amt,cc_budget_breakup_ccbdgt_tmp.cc_bdgt_invoiced_amt,cc_budget_breakup_ccbdgt_tmp.cc_budget_distribution,cc_budget_breakup_ccbdgt_tmp.cc_desc,cc_budget_breakup_ccbdgt_tmp.cc_level_expense,cc_budget_breakup_ccbdgt_tmp.cc_lvl_distribution,cc_budget_breakup_ccbdgt_tmp.cc_nm,cc_budget_breakup_ccbdgt_tmp.cc_number,cc_budget_breakup_ccbdgt_tmp.cc_obsolete_flg,cc_budget_breakup_ccbdgt_tmp.cc_owner_usernm,cc_budget_breakup_ccbdgt_tmp.cc_rldup_child_bdgt,cc_budget_breakup_ccbdgt_tmp.cc_rldup_total_expense,cc_budget_breakup_ccbdgt_tmp.cost_center_id,cc_budget_breakup_ccbdgt_tmp.created_by_usernm,cc_budget_breakup_ccbdgt_tmp.created_dttm,cc_budget_breakup_ccbdgt_tmp.fin_accnt_desc,cc_budget_breakup_ccbdgt_tmp.fin_accnt_nm,cc_budget_breakup_ccbdgt_tmp.fin_accnt_obsolete_flg,cc_budget_breakup_ccbdgt_tmp.fp_cls_ver,cc_budget_breakup_ccbdgt_tmp.fp_desc,cc_budget_breakup_ccbdgt_tmp.fp_end_dt,cc_budget_breakup_ccbdgt_tmp.fp_id,cc_budget_breakup_ccbdgt_tmp.fp_nm,cc_budget_breakup_ccbdgt_tmp.fp_obsolete_flg,cc_budget_breakup_ccbdgt_tmp.fp_start_dt,cc_budget_breakup_ccbdgt_tmp.gen_ledger_cd,cc_budget_breakup_ccbdgt_tmp.last_modified_dttm,cc_budget_breakup_ccbdgt_tmp.last_modified_usernm,cc_budget_breakup_ccbdgt_tmp.load_dttm,cc_budget_breakup_ccbdgt_tmp.planning_id,cc_budget_breakup_ccbdgt_tmp.planning_nm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cc_budget_breakup_ccbdgt_tmp    , cc_budget_breakup_ccbdgt, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cc_budget_breakup_ccbdgt_tmp    ;
    quit;
    %put ######## Staging table: cc_budget_breakup_ccbdgt_tmp     Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cc_budget_breakup_ccbdgt;
      drop table work.cc_budget_breakup_ccbdgt;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_activity_custom_attr_tmp    ;
      quit;
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
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_activity_custom_attr using &tmpdbschema..cdm_activity_custom_attr_tmp    
         on (cdm_activity_custom_attr.Hashed_pk_col = cdm_activity_custom_attr_tmp.Hashed_pk_col)
        when matched then  
        update set cdm_activity_custom_attr.activity_id = cdm_activity_custom_attr_tmp.activity_id , cdm_activity_custom_attr.attribute_character_val = cdm_activity_custom_attr_tmp.attribute_character_val , cdm_activity_custom_attr.attribute_dttm_val = cdm_activity_custom_attr_tmp.attribute_dttm_val , cdm_activity_custom_attr.attribute_numeric_val = cdm_activity_custom_attr_tmp.attribute_numeric_val , cdm_activity_custom_attr.updated_by_nm = cdm_activity_custom_attr_tmp.updated_by_nm , cdm_activity_custom_attr.updated_dttm = cdm_activity_custom_attr_tmp.updated_dttm
        when not matched then insert ( 
        activity_id,activity_version_id,attribute_character_val,attribute_data_type_cd,attribute_dttm_val,attribute_nm,attribute_numeric_val,attribute_val,updated_by_nm,updated_dttm
        ,Hashed_pk_col ) values ( 
        cdm_activity_custom_attr_tmp.activity_id,cdm_activity_custom_attr_tmp.activity_version_id,cdm_activity_custom_attr_tmp.attribute_character_val,cdm_activity_custom_attr_tmp.attribute_data_type_cd,cdm_activity_custom_attr_tmp.attribute_dttm_val,cdm_activity_custom_attr_tmp.attribute_nm,cdm_activity_custom_attr_tmp.attribute_numeric_val,cdm_activity_custom_attr_tmp.attribute_val,cdm_activity_custom_attr_tmp.updated_by_nm,cdm_activity_custom_attr_tmp.updated_dttm,cdm_activity_custom_attr_tmp.Hashed_pk_col
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_activity_custom_attr_tmp    , cdm_activity_custom_attr, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_activity_custom_attr_tmp    ;
    quit;
    %put ######## Staging table: cdm_activity_custom_attr_tmp     Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_activity_custom_attr;
      drop table work.cdm_activity_custom_attr;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_activity_detail_tmp         ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cdm_activity_detail, table_keys=%str(activity_version_id), out_table=work.cdm_activity_detail);
 data &tmplib..cdm_activity_detail_tmp         ;
     set work.cdm_activity_detail;
  if last_published_dttm ne . then last_published_dttm = tzoneu2s(last_published_dttm,&timeZone_Value.);if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if activity_version_id='' then activity_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_activity_detail_tmp         , cdm_activity_detail);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_activity_detail using &tmpdbschema..cdm_activity_detail_tmp         
         on (cdm_activity_detail.activity_version_id=cdm_activity_detail_tmp.activity_version_id)
        when matched then  
        update set cdm_activity_detail.activity_category_nm = cdm_activity_detail_tmp.activity_category_nm , cdm_activity_detail.activity_cd = cdm_activity_detail_tmp.activity_cd , cdm_activity_detail.activity_desc = cdm_activity_detail_tmp.activity_desc , cdm_activity_detail.activity_id = cdm_activity_detail_tmp.activity_id , cdm_activity_detail.activity_nm = cdm_activity_detail_tmp.activity_nm , cdm_activity_detail.last_published_dttm = cdm_activity_detail_tmp.last_published_dttm , cdm_activity_detail.source_system_cd = cdm_activity_detail_tmp.source_system_cd , cdm_activity_detail.status_cd = cdm_activity_detail_tmp.status_cd , cdm_activity_detail.updated_by_nm = cdm_activity_detail_tmp.updated_by_nm , cdm_activity_detail.updated_dttm = cdm_activity_detail_tmp.updated_dttm , cdm_activity_detail.valid_from_dttm = cdm_activity_detail_tmp.valid_from_dttm , cdm_activity_detail.valid_to_dttm = cdm_activity_detail_tmp.valid_to_dttm
        when not matched then insert ( 
        activity_category_nm,activity_cd,activity_desc,activity_id,activity_nm,activity_version_id,last_published_dttm,source_system_cd,status_cd,updated_by_nm,updated_dttm,valid_from_dttm,valid_to_dttm
         ) values ( 
        cdm_activity_detail_tmp.activity_category_nm,cdm_activity_detail_tmp.activity_cd,cdm_activity_detail_tmp.activity_desc,cdm_activity_detail_tmp.activity_id,cdm_activity_detail_tmp.activity_nm,cdm_activity_detail_tmp.activity_version_id,cdm_activity_detail_tmp.last_published_dttm,cdm_activity_detail_tmp.source_system_cd,cdm_activity_detail_tmp.status_cd,cdm_activity_detail_tmp.updated_by_nm,cdm_activity_detail_tmp.updated_dttm,cdm_activity_detail_tmp.valid_from_dttm,cdm_activity_detail_tmp.valid_to_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_activity_detail_tmp         , cdm_activity_detail, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_activity_detail_tmp         ;
    quit;
    %put ######## Staging table: cdm_activity_detail_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_activity_detail;
      drop table work.cdm_activity_detail;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_activity_x_task_tmp         ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cdm_activity_x_task, table_keys=%str(activity_version_id,task_version_id), out_table=work.cdm_activity_x_task);
 data &tmplib..cdm_activity_x_task_tmp         ;
     set work.cdm_activity_x_task;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if activity_version_id='' then activity_version_id='-'; if task_version_id='' then task_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_activity_x_task_tmp         , cdm_activity_x_task);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_activity_x_task using &tmpdbschema..cdm_activity_x_task_tmp         
         on (cdm_activity_x_task.activity_version_id=cdm_activity_x_task_tmp.activity_version_id and cdm_activity_x_task.task_version_id=cdm_activity_x_task_tmp.task_version_id)
        when matched then  
        update set cdm_activity_x_task.activity_id = cdm_activity_x_task_tmp.activity_id , cdm_activity_x_task.task_id = cdm_activity_x_task_tmp.task_id , cdm_activity_x_task.updated_by_nm = cdm_activity_x_task_tmp.updated_by_nm , cdm_activity_x_task.updated_dttm = cdm_activity_x_task_tmp.updated_dttm
        when not matched then insert ( 
        activity_id,activity_version_id,task_id,task_version_id,updated_by_nm,updated_dttm
         ) values ( 
        cdm_activity_x_task_tmp.activity_id,cdm_activity_x_task_tmp.activity_version_id,cdm_activity_x_task_tmp.task_id,cdm_activity_x_task_tmp.task_version_id,cdm_activity_x_task_tmp.updated_by_nm,cdm_activity_x_task_tmp.updated_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_activity_x_task_tmp         , cdm_activity_x_task, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_activity_x_task_tmp         ;
    quit;
    %put ######## Staging table: cdm_activity_x_task_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_activity_x_task;
      drop table work.cdm_activity_x_task;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_audience_detail_tmp         ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cdm_audience_detail, table_keys=%str(audience_id), out_table=work.cdm_audience_detail);
 data &tmplib..cdm_audience_detail_tmp         ;
     set work.cdm_audience_detail;
  if create_dttm ne . then create_dttm = tzoneu2s(create_dttm,&timeZone_Value.);if delete_dttm ne . then delete_dttm = tzoneu2s(delete_dttm,&timeZone_Value.);if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if audience_id='' then audience_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_audience_detail_tmp         , cdm_audience_detail);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_audience_detail using &tmpdbschema..cdm_audience_detail_tmp         
         on (cdm_audience_detail.audience_id=cdm_audience_detail_tmp.audience_id)
        when matched then  
        update set cdm_audience_detail.audience_data_source_nm = cdm_audience_detail_tmp.audience_data_source_nm , cdm_audience_detail.audience_desc = cdm_audience_detail_tmp.audience_desc , cdm_audience_detail.audience_nm = cdm_audience_detail_tmp.audience_nm , cdm_audience_detail.audience_schedule_flg = cdm_audience_detail_tmp.audience_schedule_flg , cdm_audience_detail.audience_source_nm = cdm_audience_detail_tmp.audience_source_nm , cdm_audience_detail.create_dttm = cdm_audience_detail_tmp.create_dttm , cdm_audience_detail.created_user_nm = cdm_audience_detail_tmp.created_user_nm , cdm_audience_detail.delete_dttm = cdm_audience_detail_tmp.delete_dttm , cdm_audience_detail.updated_dttm = cdm_audience_detail_tmp.updated_dttm
        when not matched then insert ( 
        audience_data_source_nm,audience_desc,audience_id,audience_nm,audience_schedule_flg,audience_source_nm,create_dttm,created_user_nm,delete_dttm,updated_dttm
         ) values ( 
        cdm_audience_detail_tmp.audience_data_source_nm,cdm_audience_detail_tmp.audience_desc,cdm_audience_detail_tmp.audience_id,cdm_audience_detail_tmp.audience_nm,cdm_audience_detail_tmp.audience_schedule_flg,cdm_audience_detail_tmp.audience_source_nm,cdm_audience_detail_tmp.create_dttm,cdm_audience_detail_tmp.created_user_nm,cdm_audience_detail_tmp.delete_dttm,cdm_audience_detail_tmp.updated_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_audience_detail_tmp         , cdm_audience_detail, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_audience_detail_tmp         ;
    quit;
    %put ######## Staging table: cdm_audience_detail_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_audience_detail;
      drop table work.cdm_audience_detail;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_audience_occur_detail_tmp   ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cdm_audience_occur_detail, table_keys=%str(audience_occur_id), out_table=work.cdm_audience_occur_detail);
 data &tmplib..cdm_audience_occur_detail_tmp   ;
     set work.cdm_audience_occur_detail;
  if end_dttm ne . then end_dttm = tzoneu2s(end_dttm,&timeZone_Value.);if start_dttm ne . then start_dttm = tzoneu2s(start_dttm,&timeZone_Value.);if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if audience_occur_id='' then audience_occur_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_audience_occur_detail_tmp   , cdm_audience_occur_detail);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_audience_occur_detail using &tmpdbschema..cdm_audience_occur_detail_tmp   
         on (cdm_audience_occur_detail.audience_occur_id=cdm_audience_occur_detail_tmp.audience_occur_id)
        when matched then  
        update set cdm_audience_occur_detail.audience_id = cdm_audience_occur_detail_tmp.audience_id , cdm_audience_occur_detail.audience_size_cnt = cdm_audience_occur_detail_tmp.audience_size_cnt , cdm_audience_occur_detail.end_dttm = cdm_audience_occur_detail_tmp.end_dttm , cdm_audience_occur_detail.execution_status_cd = cdm_audience_occur_detail_tmp.execution_status_cd , cdm_audience_occur_detail.occurrence_type_nm = cdm_audience_occur_detail_tmp.occurrence_type_nm , cdm_audience_occur_detail.start_dttm = cdm_audience_occur_detail_tmp.start_dttm , cdm_audience_occur_detail.started_by_nm = cdm_audience_occur_detail_tmp.started_by_nm , cdm_audience_occur_detail.updated_dttm = cdm_audience_occur_detail_tmp.updated_dttm
        when not matched then insert ( 
        audience_id,audience_occur_id,audience_size_cnt,end_dttm,execution_status_cd,occurrence_type_nm,start_dttm,started_by_nm,updated_dttm
         ) values ( 
        cdm_audience_occur_detail_tmp.audience_id,cdm_audience_occur_detail_tmp.audience_occur_id,cdm_audience_occur_detail_tmp.audience_size_cnt,cdm_audience_occur_detail_tmp.end_dttm,cdm_audience_occur_detail_tmp.execution_status_cd,cdm_audience_occur_detail_tmp.occurrence_type_nm,cdm_audience_occur_detail_tmp.start_dttm,cdm_audience_occur_detail_tmp.started_by_nm,cdm_audience_occur_detail_tmp.updated_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_audience_occur_detail_tmp   , cdm_audience_occur_detail, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_audience_occur_detail_tmp   ;
    quit;
    %put ######## Staging table: cdm_audience_occur_detail_tmp    Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_audience_occur_detail;
      drop table work.cdm_audience_occur_detail;
  quit;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table cdm_audience_occur_detail;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..cdm_business_context) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..cdm_business_context_tmp        ) ) %then %do;
      proc sql noerrorstop;
        drop table &tmplib..cdm_business_context_tmp        ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cdm_business_context, table_keys=%str(business_context_id), out_table=work.cdm_business_context);
 data &tmplib..cdm_business_context_tmp        ;
     set work.cdm_business_context;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if business_context_id='' then business_context_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_business_context_tmp        , cdm_business_context);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_business_context using &tmpdbschema..cdm_business_context_tmp        
         on (cdm_business_context.business_context_id=cdm_business_context_tmp.business_context_id)
        when matched then  
        update set cdm_business_context.business_context_nm = cdm_business_context_tmp.business_context_nm , cdm_business_context.business_context_type_cd = cdm_business_context_tmp.business_context_type_cd , cdm_business_context.source_system_cd = cdm_business_context_tmp.source_system_cd , cdm_business_context.updated_by_nm = cdm_business_context_tmp.updated_by_nm , cdm_business_context.updated_dttm = cdm_business_context_tmp.updated_dttm
        when not matched then insert ( 
        business_context_id,business_context_nm,business_context_type_cd,source_system_cd,updated_by_nm,updated_dttm
         ) values ( 
        cdm_business_context_tmp.business_context_id,cdm_business_context_tmp.business_context_nm,cdm_business_context_tmp.business_context_type_cd,cdm_business_context_tmp.source_system_cd,cdm_business_context_tmp.updated_by_nm,cdm_business_context_tmp.updated_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_business_context_tmp        , cdm_business_context, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_business_context_tmp        ;
    quit;
    %put ######## Staging table: cdm_business_context_tmp         Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_business_context;
      drop table work.cdm_business_context;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_campaign_custom_attr_tmp    ;
      quit;
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
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_campaign_custom_attr using &tmpdbschema..cdm_campaign_custom_attr_tmp    
         on (cdm_campaign_custom_attr.Hashed_pk_col = cdm_campaign_custom_attr_tmp.Hashed_pk_col)
        when matched then  
        update set cdm_campaign_custom_attr.attribute_character_val = cdm_campaign_custom_attr_tmp.attribute_character_val , cdm_campaign_custom_attr.attribute_dttm_val = cdm_campaign_custom_attr_tmp.attribute_dttm_val , cdm_campaign_custom_attr.attribute_numeric_val = cdm_campaign_custom_attr_tmp.attribute_numeric_val , cdm_campaign_custom_attr.extension_attribute_nm = cdm_campaign_custom_attr_tmp.extension_attribute_nm , cdm_campaign_custom_attr.updated_by_nm = cdm_campaign_custom_attr_tmp.updated_by_nm , cdm_campaign_custom_attr.updated_dttm = cdm_campaign_custom_attr_tmp.updated_dttm
        when not matched then insert ( 
        attribute_character_val,attribute_data_type_cd,attribute_dttm_val,attribute_nm,attribute_numeric_val,attribute_val,campaign_id,extension_attribute_nm,page_nm,updated_by_nm,updated_dttm
        ,Hashed_pk_col ) values ( 
        cdm_campaign_custom_attr_tmp.attribute_character_val,cdm_campaign_custom_attr_tmp.attribute_data_type_cd,cdm_campaign_custom_attr_tmp.attribute_dttm_val,cdm_campaign_custom_attr_tmp.attribute_nm,cdm_campaign_custom_attr_tmp.attribute_numeric_val,cdm_campaign_custom_attr_tmp.attribute_val,cdm_campaign_custom_attr_tmp.campaign_id,cdm_campaign_custom_attr_tmp.extension_attribute_nm,cdm_campaign_custom_attr_tmp.page_nm,cdm_campaign_custom_attr_tmp.updated_by_nm,cdm_campaign_custom_attr_tmp.updated_dttm,cdm_campaign_custom_attr_tmp.Hashed_pk_col
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_campaign_custom_attr_tmp    , cdm_campaign_custom_attr, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_campaign_custom_attr_tmp    ;
    quit;
    %put ######## Staging table: cdm_campaign_custom_attr_tmp     Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_campaign_custom_attr;
      drop table work.cdm_campaign_custom_attr;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_campaign_detail_tmp         ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cdm_campaign_detail, table_keys=%str(campaign_id), out_table=work.cdm_campaign_detail);
 data &tmplib..cdm_campaign_detail_tmp         ;
     set work.cdm_campaign_detail;
  if approval_dttm ne . then approval_dttm = tzoneu2s(approval_dttm,&timeZone_Value.);if end_dttm ne . then end_dttm = tzoneu2s(end_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if run_dttm ne . then run_dttm = tzoneu2s(run_dttm,&timeZone_Value.);if start_dttm ne . then start_dttm = tzoneu2s(start_dttm,&timeZone_Value.);if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if campaign_id='' then campaign_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_campaign_detail_tmp         , cdm_campaign_detail);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_campaign_detail using &tmpdbschema..cdm_campaign_detail_tmp         
         on (cdm_campaign_detail.campaign_id=cdm_campaign_detail_tmp.campaign_id)
        when matched then  
        update set cdm_campaign_detail.approval_dttm = cdm_campaign_detail_tmp.approval_dttm , cdm_campaign_detail.approval_given_by_nm = cdm_campaign_detail_tmp.approval_given_by_nm , cdm_campaign_detail.campaign_cd = cdm_campaign_detail_tmp.campaign_cd , cdm_campaign_detail.campaign_desc = cdm_campaign_detail_tmp.campaign_desc , cdm_campaign_detail.campaign_folder_txt = cdm_campaign_detail_tmp.campaign_folder_txt , cdm_campaign_detail.campaign_group_sk = cdm_campaign_detail_tmp.campaign_group_sk , cdm_campaign_detail.campaign_nm = cdm_campaign_detail_tmp.campaign_nm , cdm_campaign_detail.campaign_owner_nm = cdm_campaign_detail_tmp.campaign_owner_nm , cdm_campaign_detail.campaign_status_cd = cdm_campaign_detail_tmp.campaign_status_cd , cdm_campaign_detail.campaign_type_cd = cdm_campaign_detail_tmp.campaign_type_cd , cdm_campaign_detail.campaign_version_no = cdm_campaign_detail_tmp.campaign_version_no , cdm_campaign_detail.current_version_flg = cdm_campaign_detail_tmp.current_version_flg , cdm_campaign_detail.deleted_flg = cdm_campaign_detail_tmp.deleted_flg , cdm_campaign_detail.deployment_version_no = cdm_campaign_detail_tmp.deployment_version_no , cdm_campaign_detail.end_dttm = cdm_campaign_detail_tmp.end_dttm , cdm_campaign_detail.last_modified_by_user_nm = cdm_campaign_detail_tmp.last_modified_by_user_nm , cdm_campaign_detail.last_modified_dttm = cdm_campaign_detail_tmp.last_modified_dttm , cdm_campaign_detail.max_budget_amt = cdm_campaign_detail_tmp.max_budget_amt , cdm_campaign_detail.max_budget_offer_amt = cdm_campaign_detail_tmp.max_budget_offer_amt , cdm_campaign_detail.min_budget_amt = cdm_campaign_detail_tmp.min_budget_amt , cdm_campaign_detail.min_budget_offer_amt = cdm_campaign_detail_tmp.min_budget_offer_amt , cdm_campaign_detail.run_dttm = cdm_campaign_detail_tmp.run_dttm , cdm_campaign_detail.source_system_cd = cdm_campaign_detail_tmp.source_system_cd , cdm_campaign_detail.start_dttm = cdm_campaign_detail_tmp.start_dttm , cdm_campaign_detail.updated_by_nm = cdm_campaign_detail_tmp.updated_by_nm , cdm_campaign_detail.updated_dttm = cdm_campaign_detail_tmp.updated_dttm , cdm_campaign_detail.valid_from_dttm = cdm_campaign_detail_tmp.valid_from_dttm , cdm_campaign_detail.valid_to_dttm = cdm_campaign_detail_tmp.valid_to_dttm
        when not matched then insert ( 
        approval_dttm,approval_given_by_nm,campaign_cd,campaign_desc,campaign_folder_txt,campaign_group_sk,campaign_id,campaign_nm,campaign_owner_nm,campaign_status_cd,campaign_type_cd,campaign_version_no,current_version_flg,deleted_flg,deployment_version_no,end_dttm,last_modified_by_user_nm,last_modified_dttm,max_budget_amt,max_budget_offer_amt,min_budget_amt,min_budget_offer_amt,run_dttm,source_system_cd,start_dttm,updated_by_nm,updated_dttm,valid_from_dttm,valid_to_dttm
         ) values ( 
        cdm_campaign_detail_tmp.approval_dttm,cdm_campaign_detail_tmp.approval_given_by_nm,cdm_campaign_detail_tmp.campaign_cd,cdm_campaign_detail_tmp.campaign_desc,cdm_campaign_detail_tmp.campaign_folder_txt,cdm_campaign_detail_tmp.campaign_group_sk,cdm_campaign_detail_tmp.campaign_id,cdm_campaign_detail_tmp.campaign_nm,cdm_campaign_detail_tmp.campaign_owner_nm,cdm_campaign_detail_tmp.campaign_status_cd,cdm_campaign_detail_tmp.campaign_type_cd,cdm_campaign_detail_tmp.campaign_version_no,cdm_campaign_detail_tmp.current_version_flg,cdm_campaign_detail_tmp.deleted_flg,cdm_campaign_detail_tmp.deployment_version_no,cdm_campaign_detail_tmp.end_dttm,cdm_campaign_detail_tmp.last_modified_by_user_nm,cdm_campaign_detail_tmp.last_modified_dttm,cdm_campaign_detail_tmp.max_budget_amt,cdm_campaign_detail_tmp.max_budget_offer_amt,cdm_campaign_detail_tmp.min_budget_amt,cdm_campaign_detail_tmp.min_budget_offer_amt,cdm_campaign_detail_tmp.run_dttm,cdm_campaign_detail_tmp.source_system_cd,cdm_campaign_detail_tmp.start_dttm,cdm_campaign_detail_tmp.updated_by_nm,cdm_campaign_detail_tmp.updated_dttm,cdm_campaign_detail_tmp.valid_from_dttm,cdm_campaign_detail_tmp.valid_to_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_campaign_detail_tmp         , cdm_campaign_detail, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_campaign_detail_tmp         ;
    quit;
    %put ######## Staging table: cdm_campaign_detail_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_campaign_detail;
      drop table work.cdm_campaign_detail;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_contact_channel_tmp         ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cdm_contact_channel, table_keys=%str(contact_channel_cd), out_table=work.cdm_contact_channel);
 data &tmplib..cdm_contact_channel_tmp         ;
     set work.cdm_contact_channel;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if contact_channel_cd='' then contact_channel_cd='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_contact_channel_tmp         , cdm_contact_channel);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_contact_channel using &tmpdbschema..cdm_contact_channel_tmp         
         on (cdm_contact_channel.contact_channel_cd=cdm_contact_channel_tmp.contact_channel_cd)
        when matched then  
        update set cdm_contact_channel.contact_channel_nm = cdm_contact_channel_tmp.contact_channel_nm , cdm_contact_channel.updated_by_nm = cdm_contact_channel_tmp.updated_by_nm , cdm_contact_channel.updated_dttm = cdm_contact_channel_tmp.updated_dttm
        when not matched then insert ( 
        contact_channel_cd,contact_channel_nm,updated_by_nm,updated_dttm
         ) values ( 
        cdm_contact_channel_tmp.contact_channel_cd,cdm_contact_channel_tmp.contact_channel_nm,cdm_contact_channel_tmp.updated_by_nm,cdm_contact_channel_tmp.updated_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_contact_channel_tmp         , cdm_contact_channel, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_contact_channel_tmp         ;
    quit;
    %put ######## Staging table: cdm_contact_channel_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_contact_channel;
      drop table work.cdm_contact_channel;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_contact_history_tmp         ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cdm_contact_history, table_keys=%str(contact_id), out_table=work.cdm_contact_history);
 data &tmplib..cdm_contact_history_tmp         ;
     set work.cdm_contact_history;
  if contact_dttm ne . then contact_dttm = tzoneu2s(contact_dttm,&timeZone_Value.);if contact_dttm_tz ne . then contact_dttm_tz = tzoneu2s(contact_dttm_tz,&timeZone_Value.);if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if contact_id='' then contact_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_contact_history_tmp         , cdm_contact_history);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_contact_history using &tmpdbschema..cdm_contact_history_tmp         
         on (cdm_contact_history.contact_id=cdm_contact_history_tmp.contact_id)
        when matched then  
        update set cdm_contact_history.audience_id = cdm_contact_history_tmp.audience_id , cdm_contact_history.audience_occur_id = cdm_contact_history_tmp.audience_occur_id , cdm_contact_history.contact_dt = cdm_contact_history_tmp.contact_dt , cdm_contact_history.contact_dttm = cdm_contact_history_tmp.contact_dttm , cdm_contact_history.contact_dttm_tz = cdm_contact_history_tmp.contact_dttm_tz , cdm_contact_history.contact_nm = cdm_contact_history_tmp.contact_nm , cdm_contact_history.contact_status_cd = cdm_contact_history_tmp.contact_status_cd , cdm_contact_history.context_type_nm = cdm_contact_history_tmp.context_type_nm , cdm_contact_history.context_val = cdm_contact_history_tmp.context_val , cdm_contact_history.control_group_flg = cdm_contact_history_tmp.control_group_flg , cdm_contact_history.external_contact_info_1_id = cdm_contact_history_tmp.external_contact_info_1_id , cdm_contact_history.external_contact_info_2_id = cdm_contact_history_tmp.external_contact_info_2_id , cdm_contact_history.identity_id = cdm_contact_history_tmp.identity_id , cdm_contact_history.optimization_backfill_flg = cdm_contact_history_tmp.optimization_backfill_flg , cdm_contact_history.rtc_id = cdm_contact_history_tmp.rtc_id , cdm_contact_history.source_system_cd = cdm_contact_history_tmp.source_system_cd , cdm_contact_history.updated_by_nm = cdm_contact_history_tmp.updated_by_nm , cdm_contact_history.updated_dttm = cdm_contact_history_tmp.updated_dttm
        when not matched then insert ( 
        audience_id,audience_occur_id,contact_dt,contact_dttm,contact_dttm_tz,contact_id,contact_nm,contact_status_cd,context_type_nm,context_val,control_group_flg,external_contact_info_1_id,external_contact_info_2_id,identity_id,optimization_backfill_flg,rtc_id,source_system_cd,updated_by_nm,updated_dttm
         ) values ( 
        cdm_contact_history_tmp.audience_id,cdm_contact_history_tmp.audience_occur_id,cdm_contact_history_tmp.contact_dt,cdm_contact_history_tmp.contact_dttm,cdm_contact_history_tmp.contact_dttm_tz,cdm_contact_history_tmp.contact_id,cdm_contact_history_tmp.contact_nm,cdm_contact_history_tmp.contact_status_cd,cdm_contact_history_tmp.context_type_nm,cdm_contact_history_tmp.context_val,cdm_contact_history_tmp.control_group_flg,cdm_contact_history_tmp.external_contact_info_1_id,cdm_contact_history_tmp.external_contact_info_2_id,cdm_contact_history_tmp.identity_id,cdm_contact_history_tmp.optimization_backfill_flg,cdm_contact_history_tmp.rtc_id,cdm_contact_history_tmp.source_system_cd,cdm_contact_history_tmp.updated_by_nm,cdm_contact_history_tmp.updated_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_contact_history_tmp         , cdm_contact_history, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_contact_history_tmp         ;
    quit;
    %put ######## Staging table: cdm_contact_history_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_contact_history;
      drop table work.cdm_contact_history;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_contact_status_tmp          ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cdm_contact_status, table_keys=%str(contact_status_cd), out_table=work.cdm_contact_status);
 data &tmplib..cdm_contact_status_tmp          ;
     set work.cdm_contact_status;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if contact_status_cd='' then contact_status_cd='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_contact_status_tmp          , cdm_contact_status);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_contact_status using &tmpdbschema..cdm_contact_status_tmp          
         on (cdm_contact_status.contact_status_cd=cdm_contact_status_tmp.contact_status_cd)
        when matched then  
        update set cdm_contact_status.contact_status_desc = cdm_contact_status_tmp.contact_status_desc , cdm_contact_status.updated_by_nm = cdm_contact_status_tmp.updated_by_nm , cdm_contact_status.updated_dttm = cdm_contact_status_tmp.updated_dttm
        when not matched then insert ( 
        contact_status_cd,contact_status_desc,updated_by_nm,updated_dttm
         ) values ( 
        cdm_contact_status_tmp.contact_status_cd,cdm_contact_status_tmp.contact_status_desc,cdm_contact_status_tmp.updated_by_nm,cdm_contact_status_tmp.updated_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_contact_status_tmp          , cdm_contact_status, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_contact_status_tmp          ;
    quit;
    %put ######## Staging table: cdm_contact_status_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_contact_status;
      drop table work.cdm_contact_status;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_content_custom_attr_tmp     ;
      quit;
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
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_content_custom_attr using &tmpdbschema..cdm_content_custom_attr_tmp     
         on (cdm_content_custom_attr.Hashed_pk_col = cdm_content_custom_attr_tmp.Hashed_pk_col)
        when matched then  
        update set cdm_content_custom_attr.content_id = cdm_content_custom_attr_tmp.content_id , cdm_content_custom_attr.extension_attribute_nm = cdm_content_custom_attr_tmp.extension_attribute_nm , cdm_content_custom_attr.updated_by_nm = cdm_content_custom_attr_tmp.updated_by_nm , cdm_content_custom_attr.updated_dttm = cdm_content_custom_attr_tmp.updated_dttm
        when not matched then insert ( 
        attribute_character_val,attribute_data_type_cd,attribute_dttm_val,attribute_nm,attribute_numeric_val,attribute_val,content_id,content_version_id,extension_attribute_nm,updated_by_nm,updated_dttm
        ,Hashed_pk_col ) values ( 
        cdm_content_custom_attr_tmp.attribute_character_val,cdm_content_custom_attr_tmp.attribute_data_type_cd,cdm_content_custom_attr_tmp.attribute_dttm_val,cdm_content_custom_attr_tmp.attribute_nm,cdm_content_custom_attr_tmp.attribute_numeric_val,cdm_content_custom_attr_tmp.attribute_val,cdm_content_custom_attr_tmp.content_id,cdm_content_custom_attr_tmp.content_version_id,cdm_content_custom_attr_tmp.extension_attribute_nm,cdm_content_custom_attr_tmp.updated_by_nm,cdm_content_custom_attr_tmp.updated_dttm,cdm_content_custom_attr_tmp.Hashed_pk_col
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_content_custom_attr_tmp     , cdm_content_custom_attr, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_content_custom_attr_tmp     ;
    quit;
    %put ######## Staging table: cdm_content_custom_attr_tmp      Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_content_custom_attr;
      drop table work.cdm_content_custom_attr;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_content_detail_tmp          ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cdm_content_detail, table_keys=%str(content_version_id), out_table=work.cdm_content_detail);
 data &tmplib..cdm_content_detail_tmp          ;
     set work.cdm_content_detail;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if content_version_id='' then content_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_content_detail_tmp          , cdm_content_detail);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_content_detail using &tmpdbschema..cdm_content_detail_tmp          
         on (cdm_content_detail.content_version_id=cdm_content_detail_tmp.content_version_id)
        when matched then  
        update set cdm_content_detail.active_flg = cdm_content_detail_tmp.active_flg , cdm_content_detail.contact_content_category_nm = cdm_content_detail_tmp.contact_content_category_nm , cdm_content_detail.contact_content_cd = cdm_content_detail_tmp.contact_content_cd , cdm_content_detail.contact_content_class_nm = cdm_content_detail_tmp.contact_content_class_nm , cdm_content_detail.contact_content_desc = cdm_content_detail_tmp.contact_content_desc , cdm_content_detail.contact_content_nm = cdm_content_detail_tmp.contact_content_nm , cdm_content_detail.contact_content_status_cd = cdm_content_detail_tmp.contact_content_status_cd , cdm_content_detail.contact_content_type_nm = cdm_content_detail_tmp.contact_content_type_nm , cdm_content_detail.content_id = cdm_content_detail_tmp.content_id , cdm_content_detail.created_dt = cdm_content_detail_tmp.created_dt , cdm_content_detail.created_user_nm = cdm_content_detail_tmp.created_user_nm , cdm_content_detail.external_reference_txt = cdm_content_detail_tmp.external_reference_txt , cdm_content_detail.external_reference_url_txt = cdm_content_detail_tmp.external_reference_url_txt , cdm_content_detail.owner_nm = cdm_content_detail_tmp.owner_nm , cdm_content_detail.source_system_cd = cdm_content_detail_tmp.source_system_cd , cdm_content_detail.updated_by_nm = cdm_content_detail_tmp.updated_by_nm , cdm_content_detail.updated_dttm = cdm_content_detail_tmp.updated_dttm , cdm_content_detail.valid_from_dttm = cdm_content_detail_tmp.valid_from_dttm , cdm_content_detail.valid_to_dttm = cdm_content_detail_tmp.valid_to_dttm
        when not matched then insert ( 
        active_flg,contact_content_category_nm,contact_content_cd,contact_content_class_nm,contact_content_desc,contact_content_nm,contact_content_status_cd,contact_content_type_nm,content_id,content_version_id,created_dt,created_user_nm,external_reference_txt,external_reference_url_txt,owner_nm,source_system_cd,updated_by_nm,updated_dttm,valid_from_dttm,valid_to_dttm
         ) values ( 
        cdm_content_detail_tmp.active_flg,cdm_content_detail_tmp.contact_content_category_nm,cdm_content_detail_tmp.contact_content_cd,cdm_content_detail_tmp.contact_content_class_nm,cdm_content_detail_tmp.contact_content_desc,cdm_content_detail_tmp.contact_content_nm,cdm_content_detail_tmp.contact_content_status_cd,cdm_content_detail_tmp.contact_content_type_nm,cdm_content_detail_tmp.content_id,cdm_content_detail_tmp.content_version_id,cdm_content_detail_tmp.created_dt,cdm_content_detail_tmp.created_user_nm,cdm_content_detail_tmp.external_reference_txt,cdm_content_detail_tmp.external_reference_url_txt,cdm_content_detail_tmp.owner_nm,cdm_content_detail_tmp.source_system_cd,cdm_content_detail_tmp.updated_by_nm,cdm_content_detail_tmp.updated_dttm,cdm_content_detail_tmp.valid_from_dttm,cdm_content_detail_tmp.valid_to_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_content_detail_tmp          , cdm_content_detail, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_content_detail_tmp          ;
    quit;
    %put ######## Staging table: cdm_content_detail_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_content_detail;
      drop table work.cdm_content_detail;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_dyn_content_custom_attr_tmp ;
      quit;
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
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_dyn_content_custom_attr using &tmpdbschema..cdm_dyn_content_custom_attr_tmp 
         on (cdm_dyn_content_custom_attr.Hashed_pk_col = cdm_dyn_content_custom_attr_tmp.Hashed_pk_col)
        when matched then  
        update set cdm_dyn_content_custom_attr.attribute_character_val = cdm_dyn_content_custom_attr_tmp.attribute_character_val , cdm_dyn_content_custom_attr.attribute_dttm_val = cdm_dyn_content_custom_attr_tmp.attribute_dttm_val , cdm_dyn_content_custom_attr.attribute_numeric_val = cdm_dyn_content_custom_attr_tmp.attribute_numeric_val , cdm_dyn_content_custom_attr.content_id = cdm_dyn_content_custom_attr_tmp.content_id , cdm_dyn_content_custom_attr.extension_attribute_nm = cdm_dyn_content_custom_attr_tmp.extension_attribute_nm , cdm_dyn_content_custom_attr.updated_by_nm = cdm_dyn_content_custom_attr_tmp.updated_by_nm , cdm_dyn_content_custom_attr.updated_dttm = cdm_dyn_content_custom_attr_tmp.updated_dttm
        when not matched then insert ( 
        attribute_character_val,attribute_data_type_cd,attribute_dttm_val,attribute_nm,attribute_numeric_val,attribute_val,content_hash_val,content_id,content_version_id,extension_attribute_nm,updated_by_nm,updated_dttm
        ,Hashed_pk_col ) values ( 
        cdm_dyn_content_custom_attr_tmp.attribute_character_val,cdm_dyn_content_custom_attr_tmp.attribute_data_type_cd,cdm_dyn_content_custom_attr_tmp.attribute_dttm_val,cdm_dyn_content_custom_attr_tmp.attribute_nm,cdm_dyn_content_custom_attr_tmp.attribute_numeric_val,cdm_dyn_content_custom_attr_tmp.attribute_val,cdm_dyn_content_custom_attr_tmp.content_hash_val,cdm_dyn_content_custom_attr_tmp.content_id,cdm_dyn_content_custom_attr_tmp.content_version_id,cdm_dyn_content_custom_attr_tmp.extension_attribute_nm,cdm_dyn_content_custom_attr_tmp.updated_by_nm,cdm_dyn_content_custom_attr_tmp.updated_dttm,cdm_dyn_content_custom_attr_tmp.Hashed_pk_col
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_dyn_content_custom_attr_tmp , cdm_dyn_content_custom_attr, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_dyn_content_custom_attr_tmp ;
    quit;
    %put ######## Staging table: cdm_dyn_content_custom_attr_tmp  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_dyn_content_custom_attr;
      drop table work.cdm_dyn_content_custom_attr;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_identifier_type_tmp         ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cdm_identifier_type, table_keys=%str(identifier_type_id), out_table=work.cdm_identifier_type);
 data &tmplib..cdm_identifier_type_tmp         ;
     set work.cdm_identifier_type;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if identifier_type_id='' then identifier_type_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_identifier_type_tmp         , cdm_identifier_type);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_identifier_type using &tmpdbschema..cdm_identifier_type_tmp         
         on (cdm_identifier_type.identifier_type_id=cdm_identifier_type_tmp.identifier_type_id)
        when matched then  
        update set cdm_identifier_type.identifier_type_desc = cdm_identifier_type_tmp.identifier_type_desc , cdm_identifier_type.updated_by_nm = cdm_identifier_type_tmp.updated_by_nm , cdm_identifier_type.updated_dttm = cdm_identifier_type_tmp.updated_dttm
        when not matched then insert ( 
        identifier_type_desc,identifier_type_id,updated_by_nm,updated_dttm
         ) values ( 
        cdm_identifier_type_tmp.identifier_type_desc,cdm_identifier_type_tmp.identifier_type_id,cdm_identifier_type_tmp.updated_by_nm,cdm_identifier_type_tmp.updated_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_identifier_type_tmp         , cdm_identifier_type, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_identifier_type_tmp         ;
    quit;
    %put ######## Staging table: cdm_identifier_type_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_identifier_type;
      drop table work.cdm_identifier_type;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_identity_attr_tmp           ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cdm_identity_attr, table_keys=%str(identifier_type_id,identity_id), out_table=work.cdm_identity_attr);
 data &tmplib..cdm_identity_attr_tmp           ;
     set work.cdm_identity_attr;
  if entry_dttm ne . then entry_dttm = tzoneu2s(entry_dttm,&timeZone_Value.);if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if identifier_type_id='' then identifier_type_id='-'; if identity_id='' then identity_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_identity_attr_tmp           , cdm_identity_attr);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_identity_attr using &tmpdbschema..cdm_identity_attr_tmp           
         on (cdm_identity_attr.identifier_type_id=cdm_identity_attr_tmp.identifier_type_id and cdm_identity_attr.identity_id=cdm_identity_attr_tmp.identity_id)
        when matched then  
        update set cdm_identity_attr.entry_dttm = cdm_identity_attr_tmp.entry_dttm , cdm_identity_attr.source_system_cd = cdm_identity_attr_tmp.source_system_cd , cdm_identity_attr.updated_by_nm = cdm_identity_attr_tmp.updated_by_nm , cdm_identity_attr.updated_dttm = cdm_identity_attr_tmp.updated_dttm , cdm_identity_attr.user_identifier_val = cdm_identity_attr_tmp.user_identifier_val , cdm_identity_attr.valid_from_dttm = cdm_identity_attr_tmp.valid_from_dttm , cdm_identity_attr.valid_to_dttm = cdm_identity_attr_tmp.valid_to_dttm
        when not matched then insert ( 
        entry_dttm,identifier_type_id,identity_id,source_system_cd,updated_by_nm,updated_dttm,user_identifier_val,valid_from_dttm,valid_to_dttm
         ) values ( 
        cdm_identity_attr_tmp.entry_dttm,cdm_identity_attr_tmp.identifier_type_id,cdm_identity_attr_tmp.identity_id,cdm_identity_attr_tmp.source_system_cd,cdm_identity_attr_tmp.updated_by_nm,cdm_identity_attr_tmp.updated_dttm,cdm_identity_attr_tmp.user_identifier_val,cdm_identity_attr_tmp.valid_from_dttm,cdm_identity_attr_tmp.valid_to_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_identity_attr_tmp           , cdm_identity_attr, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_identity_attr_tmp           ;
    quit;
    %put ######## Staging table: cdm_identity_attr_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_identity_attr;
      drop table work.cdm_identity_attr;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_identity_map_tmp            ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cdm_identity_map, table_keys=%str(identity_id), out_table=work.cdm_identity_map);
 data &tmplib..cdm_identity_map_tmp            ;
     set work.cdm_identity_map;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if identity_id='' then identity_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_identity_map_tmp            , cdm_identity_map);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_identity_map using &tmpdbschema..cdm_identity_map_tmp            
         on (cdm_identity_map.identity_id=cdm_identity_map_tmp.identity_id)
        when matched then  
        update set cdm_identity_map.identity_type_cd = cdm_identity_map_tmp.identity_type_cd , cdm_identity_map.updated_by_nm = cdm_identity_map_tmp.updated_by_nm , cdm_identity_map.updated_dttm = cdm_identity_map_tmp.updated_dttm
        when not matched then insert ( 
        identity_id,identity_type_cd,updated_by_nm,updated_dttm
         ) values ( 
        cdm_identity_map_tmp.identity_id,cdm_identity_map_tmp.identity_type_cd,cdm_identity_map_tmp.updated_by_nm,cdm_identity_map_tmp.updated_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_identity_map_tmp            , cdm_identity_map, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_identity_map_tmp            ;
    quit;
    %put ######## Staging table: cdm_identity_map_tmp             Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_identity_map;
      drop table work.cdm_identity_map;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_identity_type_tmp           ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cdm_identity_type, table_keys=%str(identity_type_cd), out_table=work.cdm_identity_type);
 data &tmplib..cdm_identity_type_tmp           ;
     set work.cdm_identity_type;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if identity_type_cd='' then identity_type_cd='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_identity_type_tmp           , cdm_identity_type);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_identity_type using &tmpdbschema..cdm_identity_type_tmp           
         on (cdm_identity_type.identity_type_cd=cdm_identity_type_tmp.identity_type_cd)
        when matched then  
        update set cdm_identity_type.identity_type_desc = cdm_identity_type_tmp.identity_type_desc , cdm_identity_type.updated_by_nm = cdm_identity_type_tmp.updated_by_nm , cdm_identity_type.updated_dttm = cdm_identity_type_tmp.updated_dttm
        when not matched then insert ( 
        identity_type_cd,identity_type_desc,updated_by_nm,updated_dttm
         ) values ( 
        cdm_identity_type_tmp.identity_type_cd,cdm_identity_type_tmp.identity_type_desc,cdm_identity_type_tmp.updated_by_nm,cdm_identity_type_tmp.updated_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_identity_type_tmp           , cdm_identity_type, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_identity_type_tmp           ;
    quit;
    %put ######## Staging table: cdm_identity_type_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_identity_type;
      drop table work.cdm_identity_type;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_occurrence_detail_tmp       ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cdm_occurrence_detail, table_keys=%str(occurrence_id), out_table=work.cdm_occurrence_detail);
 data &tmplib..cdm_occurrence_detail_tmp       ;
     set work.cdm_occurrence_detail;
  if end_dttm ne . then end_dttm = tzoneu2s(end_dttm,&timeZone_Value.);if start_dttm ne . then start_dttm = tzoneu2s(start_dttm,&timeZone_Value.);if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if occurrence_id='' then occurrence_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_occurrence_detail_tmp       , cdm_occurrence_detail);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_occurrence_detail using &tmpdbschema..cdm_occurrence_detail_tmp       
         on (cdm_occurrence_detail.occurrence_id=cdm_occurrence_detail_tmp.occurrence_id)
        when matched then  
        update set cdm_occurrence_detail.end_dttm = cdm_occurrence_detail_tmp.end_dttm , cdm_occurrence_detail.execution_status_cd = cdm_occurrence_detail_tmp.execution_status_cd , cdm_occurrence_detail.occurrence_no = cdm_occurrence_detail_tmp.occurrence_no , cdm_occurrence_detail.occurrence_object_id = cdm_occurrence_detail_tmp.occurrence_object_id , cdm_occurrence_detail.occurrence_object_type_cd = cdm_occurrence_detail_tmp.occurrence_object_type_cd , cdm_occurrence_detail.occurrence_type_cd = cdm_occurrence_detail_tmp.occurrence_type_cd , cdm_occurrence_detail.source_system_cd = cdm_occurrence_detail_tmp.source_system_cd , cdm_occurrence_detail.start_dttm = cdm_occurrence_detail_tmp.start_dttm , cdm_occurrence_detail.updated_by_nm = cdm_occurrence_detail_tmp.updated_by_nm , cdm_occurrence_detail.updated_dttm = cdm_occurrence_detail_tmp.updated_dttm
        when not matched then insert ( 
        end_dttm,execution_status_cd,occurrence_id,occurrence_no,occurrence_object_id,occurrence_object_type_cd,occurrence_type_cd,source_system_cd,start_dttm,updated_by_nm,updated_dttm
         ) values ( 
        cdm_occurrence_detail_tmp.end_dttm,cdm_occurrence_detail_tmp.execution_status_cd,cdm_occurrence_detail_tmp.occurrence_id,cdm_occurrence_detail_tmp.occurrence_no,cdm_occurrence_detail_tmp.occurrence_object_id,cdm_occurrence_detail_tmp.occurrence_object_type_cd,cdm_occurrence_detail_tmp.occurrence_type_cd,cdm_occurrence_detail_tmp.source_system_cd,cdm_occurrence_detail_tmp.start_dttm,cdm_occurrence_detail_tmp.updated_by_nm,cdm_occurrence_detail_tmp.updated_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_occurrence_detail_tmp       , cdm_occurrence_detail, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_occurrence_detail_tmp       ;
    quit;
    %put ######## Staging table: cdm_occurrence_detail_tmp        Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_occurrence_detail;
      drop table work.cdm_occurrence_detail;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_response_channel_tmp        ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cdm_response_channel, table_keys=%str(response_channel_cd), out_table=work.cdm_response_channel);
 data &tmplib..cdm_response_channel_tmp        ;
     set work.cdm_response_channel;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if response_channel_cd='' then response_channel_cd='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_response_channel_tmp        , cdm_response_channel);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_response_channel using &tmpdbschema..cdm_response_channel_tmp        
         on (cdm_response_channel.response_channel_cd=cdm_response_channel_tmp.response_channel_cd)
        when matched then  
        update set cdm_response_channel.response_channel_nm = cdm_response_channel_tmp.response_channel_nm , cdm_response_channel.updated_by_nm = cdm_response_channel_tmp.updated_by_nm , cdm_response_channel.updated_dttm = cdm_response_channel_tmp.updated_dttm
        when not matched then insert ( 
        response_channel_cd,response_channel_nm,updated_by_nm,updated_dttm
         ) values ( 
        cdm_response_channel_tmp.response_channel_cd,cdm_response_channel_tmp.response_channel_nm,cdm_response_channel_tmp.updated_by_nm,cdm_response_channel_tmp.updated_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_response_channel_tmp        , cdm_response_channel, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_response_channel_tmp        ;
    quit;
    %put ######## Staging table: cdm_response_channel_tmp         Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_response_channel;
      drop table work.cdm_response_channel;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_response_extended_attr_tmp  ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cdm_response_extended_attr, table_keys=%str(attribute_nm,response_attribute_type_cd,response_id), out_table=work.cdm_response_extended_attr);
 data &tmplib..cdm_response_extended_attr_tmp  ;
     set work.cdm_response_extended_attr;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if attribute_nm='' then attribute_nm='-'; if response_attribute_type_cd='' then response_attribute_type_cd='-'; if response_id='' then response_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_response_extended_attr_tmp  , cdm_response_extended_attr);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_response_extended_attr using &tmpdbschema..cdm_response_extended_attr_tmp  
         on (cdm_response_extended_attr.attribute_nm=cdm_response_extended_attr_tmp.attribute_nm and cdm_response_extended_attr.response_attribute_type_cd=cdm_response_extended_attr_tmp.response_attribute_type_cd and cdm_response_extended_attr.response_id=cdm_response_extended_attr_tmp.response_id)
        when matched then  
        update set cdm_response_extended_attr.attribute_data_type_cd = cdm_response_extended_attr_tmp.attribute_data_type_cd , cdm_response_extended_attr.attribute_val = cdm_response_extended_attr_tmp.attribute_val , cdm_response_extended_attr.updated_by_nm = cdm_response_extended_attr_tmp.updated_by_nm , cdm_response_extended_attr.updated_dttm = cdm_response_extended_attr_tmp.updated_dttm
        when not matched then insert ( 
        attribute_data_type_cd,attribute_nm,attribute_val,response_attribute_type_cd,response_id,updated_by_nm,updated_dttm
         ) values ( 
        cdm_response_extended_attr_tmp.attribute_data_type_cd,cdm_response_extended_attr_tmp.attribute_nm,cdm_response_extended_attr_tmp.attribute_val,cdm_response_extended_attr_tmp.response_attribute_type_cd,cdm_response_extended_attr_tmp.response_id,cdm_response_extended_attr_tmp.updated_by_nm,cdm_response_extended_attr_tmp.updated_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_response_extended_attr_tmp  , cdm_response_extended_attr, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_response_extended_attr_tmp  ;
    quit;
    %put ######## Staging table: cdm_response_extended_attr_tmp   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_response_extended_attr;
      drop table work.cdm_response_extended_attr;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_response_history_tmp        ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cdm_response_history, table_keys=%str(response_id), out_table=work.cdm_response_history);
 data &tmplib..cdm_response_history_tmp        ;
     set work.cdm_response_history;
  if response_dttm ne . then response_dttm = tzoneu2s(response_dttm,&timeZone_Value.);if response_dttm_tz ne . then response_dttm_tz = tzoneu2s(response_dttm_tz,&timeZone_Value.);if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if response_id='' then response_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_response_history_tmp        , cdm_response_history);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_response_history using &tmpdbschema..cdm_response_history_tmp        
         on (cdm_response_history.response_id=cdm_response_history_tmp.response_id)
        when matched then  
        update set cdm_response_history.audience_id = cdm_response_history_tmp.audience_id , cdm_response_history.audience_occur_id = cdm_response_history_tmp.audience_occur_id , cdm_response_history.contact_id = cdm_response_history_tmp.contact_id , cdm_response_history.content_hash_val = cdm_response_history_tmp.content_hash_val , cdm_response_history.content_id = cdm_response_history_tmp.content_id , cdm_response_history.content_version_id = cdm_response_history_tmp.content_version_id , cdm_response_history.context_type_nm = cdm_response_history_tmp.context_type_nm , cdm_response_history.context_val = cdm_response_history_tmp.context_val , cdm_response_history.conversion_flg = cdm_response_history_tmp.conversion_flg , cdm_response_history.external_contact_info_1_id = cdm_response_history_tmp.external_contact_info_1_id , cdm_response_history.external_contact_info_2_id = cdm_response_history_tmp.external_contact_info_2_id , cdm_response_history.identity_id = cdm_response_history_tmp.identity_id , cdm_response_history.inferred_response_flg = cdm_response_history_tmp.inferred_response_flg , cdm_response_history.properties_map_doc = cdm_response_history_tmp.properties_map_doc , cdm_response_history.response_cd = cdm_response_history_tmp.response_cd , cdm_response_history.response_channel_cd = cdm_response_history_tmp.response_channel_cd , cdm_response_history.response_dt = cdm_response_history_tmp.response_dt , cdm_response_history.response_dttm = cdm_response_history_tmp.response_dttm , cdm_response_history.response_dttm_tz = cdm_response_history_tmp.response_dttm_tz , cdm_response_history.response_type_cd = cdm_response_history_tmp.response_type_cd , cdm_response_history.response_val_amt = cdm_response_history_tmp.response_val_amt , cdm_response_history.rtc_id = cdm_response_history_tmp.rtc_id , cdm_response_history.source_system_cd = cdm_response_history_tmp.source_system_cd , cdm_response_history.updated_by_nm = cdm_response_history_tmp.updated_by_nm , cdm_response_history.updated_dttm = cdm_response_history_tmp.updated_dttm
        when not matched then insert ( 
        audience_id,audience_occur_id,contact_id,content_hash_val,content_id,content_version_id,context_type_nm,context_val,conversion_flg,external_contact_info_1_id,external_contact_info_2_id,identity_id,inferred_response_flg,properties_map_doc,response_cd,response_channel_cd,response_dt,response_dttm,response_dttm_tz,response_id,response_type_cd,response_val_amt,rtc_id,source_system_cd,updated_by_nm,updated_dttm
         ) values ( 
        cdm_response_history_tmp.audience_id,cdm_response_history_tmp.audience_occur_id,cdm_response_history_tmp.contact_id,cdm_response_history_tmp.content_hash_val,cdm_response_history_tmp.content_id,cdm_response_history_tmp.content_version_id,cdm_response_history_tmp.context_type_nm,cdm_response_history_tmp.context_val,cdm_response_history_tmp.conversion_flg,cdm_response_history_tmp.external_contact_info_1_id,cdm_response_history_tmp.external_contact_info_2_id,cdm_response_history_tmp.identity_id,cdm_response_history_tmp.inferred_response_flg,cdm_response_history_tmp.properties_map_doc,cdm_response_history_tmp.response_cd,cdm_response_history_tmp.response_channel_cd,cdm_response_history_tmp.response_dt,cdm_response_history_tmp.response_dttm,cdm_response_history_tmp.response_dttm_tz,cdm_response_history_tmp.response_id,cdm_response_history_tmp.response_type_cd,cdm_response_history_tmp.response_val_amt,cdm_response_history_tmp.rtc_id,cdm_response_history_tmp.source_system_cd,cdm_response_history_tmp.updated_by_nm,cdm_response_history_tmp.updated_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_response_history_tmp        , cdm_response_history, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_response_history_tmp        ;
    quit;
    %put ######## Staging table: cdm_response_history_tmp         Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_response_history;
      drop table work.cdm_response_history;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_response_lookup_tmp         ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cdm_response_lookup, table_keys=%str(response_cd), out_table=work.cdm_response_lookup);
 data &tmplib..cdm_response_lookup_tmp         ;
     set work.cdm_response_lookup;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if response_cd='' then response_cd='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_response_lookup_tmp         , cdm_response_lookup);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_response_lookup using &tmpdbschema..cdm_response_lookup_tmp         
         on (cdm_response_lookup.response_cd=cdm_response_lookup_tmp.response_cd)
        when matched then  
        update set cdm_response_lookup.response_nm = cdm_response_lookup_tmp.response_nm , cdm_response_lookup.updated_by_nm = cdm_response_lookup_tmp.updated_by_nm , cdm_response_lookup.updated_dttm = cdm_response_lookup_tmp.updated_dttm
        when not matched then insert ( 
        response_cd,response_nm,updated_by_nm,updated_dttm
         ) values ( 
        cdm_response_lookup_tmp.response_cd,cdm_response_lookup_tmp.response_nm,cdm_response_lookup_tmp.updated_by_nm,cdm_response_lookup_tmp.updated_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_response_lookup_tmp         , cdm_response_lookup, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_response_lookup_tmp         ;
    quit;
    %put ######## Staging table: cdm_response_lookup_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_response_lookup;
      drop table work.cdm_response_lookup;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_response_type_tmp           ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cdm_response_type, table_keys=%str(response_type_cd), out_table=work.cdm_response_type);
 data &tmplib..cdm_response_type_tmp           ;
     set work.cdm_response_type;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if response_type_cd='' then response_type_cd='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_response_type_tmp           , cdm_response_type);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_response_type using &tmpdbschema..cdm_response_type_tmp           
         on (cdm_response_type.response_type_cd=cdm_response_type_tmp.response_type_cd)
        when matched then  
        update set cdm_response_type.response_type_desc = cdm_response_type_tmp.response_type_desc , cdm_response_type.updated_by_nm = cdm_response_type_tmp.updated_by_nm , cdm_response_type.updated_dttm = cdm_response_type_tmp.updated_dttm
        when not matched then insert ( 
        response_type_cd,response_type_desc,updated_by_nm,updated_dttm
         ) values ( 
        cdm_response_type_tmp.response_type_cd,cdm_response_type_tmp.response_type_desc,cdm_response_type_tmp.updated_by_nm,cdm_response_type_tmp.updated_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_response_type_tmp           , cdm_response_type, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_response_type_tmp           ;
    quit;
    %put ######## Staging table: cdm_response_type_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_response_type;
      drop table work.cdm_response_type;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_rtc_detail_tmp              ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cdm_rtc_detail, table_keys=%str(rtc_id), out_table=work.cdm_rtc_detail);
 data &tmplib..cdm_rtc_detail_tmp              ;
     set work.cdm_rtc_detail;
  if processed_dttm ne . then processed_dttm = tzoneu2s(processed_dttm,&timeZone_Value.);if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if rtc_id='' then rtc_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_rtc_detail_tmp              , cdm_rtc_detail);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_rtc_detail using &tmpdbschema..cdm_rtc_detail_tmp              
         on (cdm_rtc_detail.rtc_id=cdm_rtc_detail_tmp.rtc_id)
        when matched then  
        update set cdm_rtc_detail.deleted_flg = cdm_rtc_detail_tmp.deleted_flg , cdm_rtc_detail.execution_status_cd = cdm_rtc_detail_tmp.execution_status_cd , cdm_rtc_detail.occurrence_id = cdm_rtc_detail_tmp.occurrence_id , cdm_rtc_detail.processed_dttm = cdm_rtc_detail_tmp.processed_dttm , cdm_rtc_detail.response_tracking_flg = cdm_rtc_detail_tmp.response_tracking_flg , cdm_rtc_detail.segment_id = cdm_rtc_detail_tmp.segment_id , cdm_rtc_detail.segment_version_id = cdm_rtc_detail_tmp.segment_version_id , cdm_rtc_detail.source_system_cd = cdm_rtc_detail_tmp.source_system_cd , cdm_rtc_detail.task_id = cdm_rtc_detail_tmp.task_id , cdm_rtc_detail.task_occurrence_no = cdm_rtc_detail_tmp.task_occurrence_no , cdm_rtc_detail.task_version_id = cdm_rtc_detail_tmp.task_version_id , cdm_rtc_detail.updated_by_nm = cdm_rtc_detail_tmp.updated_by_nm , cdm_rtc_detail.updated_dttm = cdm_rtc_detail_tmp.updated_dttm
        when not matched then insert ( 
        deleted_flg,execution_status_cd,occurrence_id,processed_dttm,response_tracking_flg,rtc_id,segment_id,segment_version_id,source_system_cd,task_id,task_occurrence_no,task_version_id,updated_by_nm,updated_dttm
         ) values ( 
        cdm_rtc_detail_tmp.deleted_flg,cdm_rtc_detail_tmp.execution_status_cd,cdm_rtc_detail_tmp.occurrence_id,cdm_rtc_detail_tmp.processed_dttm,cdm_rtc_detail_tmp.response_tracking_flg,cdm_rtc_detail_tmp.rtc_id,cdm_rtc_detail_tmp.segment_id,cdm_rtc_detail_tmp.segment_version_id,cdm_rtc_detail_tmp.source_system_cd,cdm_rtc_detail_tmp.task_id,cdm_rtc_detail_tmp.task_occurrence_no,cdm_rtc_detail_tmp.task_version_id,cdm_rtc_detail_tmp.updated_by_nm,cdm_rtc_detail_tmp.updated_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_rtc_detail_tmp              , cdm_rtc_detail, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_rtc_detail_tmp              ;
    quit;
    %put ######## Staging table: cdm_rtc_detail_tmp               Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_rtc_detail;
      drop table work.cdm_rtc_detail;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_rtc_x_content_tmp           ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cdm_rtc_x_content, table_keys=%str(content_version_id,rtc_id), out_table=work.cdm_rtc_x_content);
 data &tmplib..cdm_rtc_x_content_tmp           ;
     set work.cdm_rtc_x_content;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if content_version_id='' then content_version_id='-'; if rtc_id='' then rtc_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_rtc_x_content_tmp           , cdm_rtc_x_content);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_rtc_x_content using &tmpdbschema..cdm_rtc_x_content_tmp           
         on (cdm_rtc_x_content.content_version_id=cdm_rtc_x_content_tmp.content_version_id and cdm_rtc_x_content.rtc_id=cdm_rtc_x_content_tmp.rtc_id)
        when matched then  
        update set cdm_rtc_x_content.content_hash_val = cdm_rtc_x_content_tmp.content_hash_val , cdm_rtc_x_content.content_id = cdm_rtc_x_content_tmp.content_id , cdm_rtc_x_content.rtc_x_content_sk = cdm_rtc_x_content_tmp.rtc_x_content_sk , cdm_rtc_x_content.sequence_no = cdm_rtc_x_content_tmp.sequence_no , cdm_rtc_x_content.updated_by_nm = cdm_rtc_x_content_tmp.updated_by_nm , cdm_rtc_x_content.updated_dttm = cdm_rtc_x_content_tmp.updated_dttm
        when not matched then insert ( 
        content_hash_val,content_id,content_version_id,rtc_id,rtc_x_content_sk,sequence_no,updated_by_nm,updated_dttm
         ) values ( 
        cdm_rtc_x_content_tmp.content_hash_val,cdm_rtc_x_content_tmp.content_id,cdm_rtc_x_content_tmp.content_version_id,cdm_rtc_x_content_tmp.rtc_id,cdm_rtc_x_content_tmp.rtc_x_content_sk,cdm_rtc_x_content_tmp.sequence_no,cdm_rtc_x_content_tmp.updated_by_nm,cdm_rtc_x_content_tmp.updated_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_rtc_x_content_tmp           , cdm_rtc_x_content, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_rtc_x_content_tmp           ;
    quit;
    %put ######## Staging table: cdm_rtc_x_content_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_rtc_x_content;
      drop table work.cdm_rtc_x_content;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_segment_custom_attr_tmp     ;
      quit;
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
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_segment_custom_attr using &tmpdbschema..cdm_segment_custom_attr_tmp     
         on (cdm_segment_custom_attr.Hashed_pk_col = cdm_segment_custom_attr_tmp.Hashed_pk_col)
        when matched then  
        update set cdm_segment_custom_attr.attribute_character_val = cdm_segment_custom_attr_tmp.attribute_character_val , cdm_segment_custom_attr.attribute_dttm_val = cdm_segment_custom_attr_tmp.attribute_dttm_val , cdm_segment_custom_attr.attribute_numeric_val = cdm_segment_custom_attr_tmp.attribute_numeric_val , cdm_segment_custom_attr.segment_id = cdm_segment_custom_attr_tmp.segment_id , cdm_segment_custom_attr.updated_by_nm = cdm_segment_custom_attr_tmp.updated_by_nm , cdm_segment_custom_attr.updated_dttm = cdm_segment_custom_attr_tmp.updated_dttm
        when not matched then insert ( 
        attribute_character_val,attribute_data_type_cd,attribute_dttm_val,attribute_nm,attribute_numeric_val,attribute_val,segment_id,segment_version_id,updated_by_nm,updated_dttm
        ,Hashed_pk_col ) values ( 
        cdm_segment_custom_attr_tmp.attribute_character_val,cdm_segment_custom_attr_tmp.attribute_data_type_cd,cdm_segment_custom_attr_tmp.attribute_dttm_val,cdm_segment_custom_attr_tmp.attribute_nm,cdm_segment_custom_attr_tmp.attribute_numeric_val,cdm_segment_custom_attr_tmp.attribute_val,cdm_segment_custom_attr_tmp.segment_id,cdm_segment_custom_attr_tmp.segment_version_id,cdm_segment_custom_attr_tmp.updated_by_nm,cdm_segment_custom_attr_tmp.updated_dttm,cdm_segment_custom_attr_tmp.Hashed_pk_col
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_segment_custom_attr_tmp     , cdm_segment_custom_attr, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_segment_custom_attr_tmp     ;
    quit;
    %put ######## Staging table: cdm_segment_custom_attr_tmp      Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_segment_custom_attr;
      drop table work.cdm_segment_custom_attr;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_segment_detail_tmp          ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cdm_segment_detail, table_keys=%str(segment_version_id), out_table=work.cdm_segment_detail);
 data &tmplib..cdm_segment_detail_tmp          ;
     set work.cdm_segment_detail;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if segment_version_id='' then segment_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_segment_detail_tmp          , cdm_segment_detail);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_segment_detail using &tmpdbschema..cdm_segment_detail_tmp          
         on (cdm_segment_detail.segment_version_id=cdm_segment_detail_tmp.segment_version_id)
        when matched then  
        update set cdm_segment_detail.segment_category_nm = cdm_segment_detail_tmp.segment_category_nm , cdm_segment_detail.segment_cd = cdm_segment_detail_tmp.segment_cd , cdm_segment_detail.segment_desc = cdm_segment_detail_tmp.segment_desc , cdm_segment_detail.segment_id = cdm_segment_detail_tmp.segment_id , cdm_segment_detail.segment_map_id = cdm_segment_detail_tmp.segment_map_id , cdm_segment_detail.segment_map_version_id = cdm_segment_detail_tmp.segment_map_version_id , cdm_segment_detail.segment_nm = cdm_segment_detail_tmp.segment_nm , cdm_segment_detail.segment_src_nm = cdm_segment_detail_tmp.segment_src_nm , cdm_segment_detail.segment_status_cd = cdm_segment_detail_tmp.segment_status_cd , cdm_segment_detail.source_system_cd = cdm_segment_detail_tmp.source_system_cd , cdm_segment_detail.updated_by_nm = cdm_segment_detail_tmp.updated_by_nm , cdm_segment_detail.updated_dttm = cdm_segment_detail_tmp.updated_dttm , cdm_segment_detail.valid_from_dttm = cdm_segment_detail_tmp.valid_from_dttm , cdm_segment_detail.valid_to_dttm = cdm_segment_detail_tmp.valid_to_dttm
        when not matched then insert ( 
        segment_category_nm,segment_cd,segment_desc,segment_id,segment_map_id,segment_map_version_id,segment_nm,segment_src_nm,segment_status_cd,segment_version_id,source_system_cd,updated_by_nm,updated_dttm,valid_from_dttm,valid_to_dttm
         ) values ( 
        cdm_segment_detail_tmp.segment_category_nm,cdm_segment_detail_tmp.segment_cd,cdm_segment_detail_tmp.segment_desc,cdm_segment_detail_tmp.segment_id,cdm_segment_detail_tmp.segment_map_id,cdm_segment_detail_tmp.segment_map_version_id,cdm_segment_detail_tmp.segment_nm,cdm_segment_detail_tmp.segment_src_nm,cdm_segment_detail_tmp.segment_status_cd,cdm_segment_detail_tmp.segment_version_id,cdm_segment_detail_tmp.source_system_cd,cdm_segment_detail_tmp.updated_by_nm,cdm_segment_detail_tmp.updated_dttm,cdm_segment_detail_tmp.valid_from_dttm,cdm_segment_detail_tmp.valid_to_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_segment_detail_tmp          , cdm_segment_detail, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_segment_detail_tmp          ;
    quit;
    %put ######## Staging table: cdm_segment_detail_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_segment_detail;
      drop table work.cdm_segment_detail;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_segment_map_tmp             ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cdm_segment_map, table_keys=%str(segment_map_version_id), out_table=work.cdm_segment_map);
 data &tmplib..cdm_segment_map_tmp             ;
     set work.cdm_segment_map;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if segment_map_version_id='' then segment_map_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_segment_map_tmp             , cdm_segment_map);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_segment_map using &tmpdbschema..cdm_segment_map_tmp             
         on (cdm_segment_map.segment_map_version_id=cdm_segment_map_tmp.segment_map_version_id)
        when matched then  
        update set cdm_segment_map.segment_map_category_nm = cdm_segment_map_tmp.segment_map_category_nm , cdm_segment_map.segment_map_cd = cdm_segment_map_tmp.segment_map_cd , cdm_segment_map.segment_map_desc = cdm_segment_map_tmp.segment_map_desc , cdm_segment_map.segment_map_id = cdm_segment_map_tmp.segment_map_id , cdm_segment_map.segment_map_nm = cdm_segment_map_tmp.segment_map_nm , cdm_segment_map.segment_map_src_nm = cdm_segment_map_tmp.segment_map_src_nm , cdm_segment_map.segment_map_status_cd = cdm_segment_map_tmp.segment_map_status_cd , cdm_segment_map.source_system_cd = cdm_segment_map_tmp.source_system_cd , cdm_segment_map.updated_by_nm = cdm_segment_map_tmp.updated_by_nm , cdm_segment_map.updated_dttm = cdm_segment_map_tmp.updated_dttm , cdm_segment_map.valid_from_dttm = cdm_segment_map_tmp.valid_from_dttm , cdm_segment_map.valid_to_dttm = cdm_segment_map_tmp.valid_to_dttm
        when not matched then insert ( 
        segment_map_category_nm,segment_map_cd,segment_map_desc,segment_map_id,segment_map_nm,segment_map_src_nm,segment_map_status_cd,segment_map_version_id,source_system_cd,updated_by_nm,updated_dttm,valid_from_dttm,valid_to_dttm
         ) values ( 
        cdm_segment_map_tmp.segment_map_category_nm,cdm_segment_map_tmp.segment_map_cd,cdm_segment_map_tmp.segment_map_desc,cdm_segment_map_tmp.segment_map_id,cdm_segment_map_tmp.segment_map_nm,cdm_segment_map_tmp.segment_map_src_nm,cdm_segment_map_tmp.segment_map_status_cd,cdm_segment_map_tmp.segment_map_version_id,cdm_segment_map_tmp.source_system_cd,cdm_segment_map_tmp.updated_by_nm,cdm_segment_map_tmp.updated_dttm,cdm_segment_map_tmp.valid_from_dttm,cdm_segment_map_tmp.valid_to_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_segment_map_tmp             , cdm_segment_map, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_segment_map_tmp             ;
    quit;
    %put ######## Staging table: cdm_segment_map_tmp              Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_segment_map;
      drop table work.cdm_segment_map;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_segment_map_custom_attr_tmp ;
      quit;
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
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_segment_map_custom_attr using &tmpdbschema..cdm_segment_map_custom_attr_tmp 
         on (cdm_segment_map_custom_attr.Hashed_pk_col = cdm_segment_map_custom_attr_tmp.Hashed_pk_col)
        when matched then  
        update set cdm_segment_map_custom_attr.attribute_character_val = cdm_segment_map_custom_attr_tmp.attribute_character_val , cdm_segment_map_custom_attr.attribute_dttm_val = cdm_segment_map_custom_attr_tmp.attribute_dttm_val , cdm_segment_map_custom_attr.attribute_numeric_val = cdm_segment_map_custom_attr_tmp.attribute_numeric_val , cdm_segment_map_custom_attr.segment_map_id = cdm_segment_map_custom_attr_tmp.segment_map_id , cdm_segment_map_custom_attr.updated_by_nm = cdm_segment_map_custom_attr_tmp.updated_by_nm , cdm_segment_map_custom_attr.updated_dttm = cdm_segment_map_custom_attr_tmp.updated_dttm
        when not matched then insert ( 
        attribute_character_val,attribute_data_type_cd,attribute_dttm_val,attribute_nm,attribute_numeric_val,attribute_val,segment_map_id,segment_map_version_id,updated_by_nm,updated_dttm
        ,Hashed_pk_col ) values ( 
        cdm_segment_map_custom_attr_tmp.attribute_character_val,cdm_segment_map_custom_attr_tmp.attribute_data_type_cd,cdm_segment_map_custom_attr_tmp.attribute_dttm_val,cdm_segment_map_custom_attr_tmp.attribute_nm,cdm_segment_map_custom_attr_tmp.attribute_numeric_val,cdm_segment_map_custom_attr_tmp.attribute_val,cdm_segment_map_custom_attr_tmp.segment_map_id,cdm_segment_map_custom_attr_tmp.segment_map_version_id,cdm_segment_map_custom_attr_tmp.updated_by_nm,cdm_segment_map_custom_attr_tmp.updated_dttm,cdm_segment_map_custom_attr_tmp.Hashed_pk_col
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_segment_map_custom_attr_tmp , cdm_segment_map_custom_attr, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_segment_map_custom_attr_tmp ;
    quit;
    %put ######## Staging table: cdm_segment_map_custom_attr_tmp  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_segment_map_custom_attr;
      drop table work.cdm_segment_map_custom_attr;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_segment_test_tmp            ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cdm_segment_test, table_keys=%str(task_version_id,test_cd), out_table=work.cdm_segment_test);
 data &tmplib..cdm_segment_test_tmp            ;
     set work.cdm_segment_test;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if task_version_id='' then task_version_id='-'; if test_cd='' then test_cd='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_segment_test_tmp            , cdm_segment_test);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_segment_test using &tmpdbschema..cdm_segment_test_tmp            
         on (cdm_segment_test.task_version_id=cdm_segment_test_tmp.task_version_id and cdm_segment_test.test_cd=cdm_segment_test_tmp.test_cd)
        when matched then  
        update set cdm_segment_test.stratified_samp_criteria_txt = cdm_segment_test_tmp.stratified_samp_criteria_txt , cdm_segment_test.stratified_sampling_flg = cdm_segment_test_tmp.stratified_sampling_flg , cdm_segment_test.task_id = cdm_segment_test_tmp.task_id , cdm_segment_test.test_cnt = cdm_segment_test_tmp.test_cnt , cdm_segment_test.test_enabled_flg = cdm_segment_test_tmp.test_enabled_flg , cdm_segment_test.test_nm = cdm_segment_test_tmp.test_nm , cdm_segment_test.test_pct = cdm_segment_test_tmp.test_pct , cdm_segment_test.test_sizing_type_nm = cdm_segment_test_tmp.test_sizing_type_nm , cdm_segment_test.test_type_nm = cdm_segment_test_tmp.test_type_nm , cdm_segment_test.updated_dttm = cdm_segment_test_tmp.updated_dttm
        when not matched then insert ( 
        stratified_samp_criteria_txt,stratified_sampling_flg,task_id,task_version_id,test_cd,test_cnt,test_enabled_flg,test_nm,test_pct,test_sizing_type_nm,test_type_nm,updated_dttm
         ) values ( 
        cdm_segment_test_tmp.stratified_samp_criteria_txt,cdm_segment_test_tmp.stratified_sampling_flg,cdm_segment_test_tmp.task_id,cdm_segment_test_tmp.task_version_id,cdm_segment_test_tmp.test_cd,cdm_segment_test_tmp.test_cnt,cdm_segment_test_tmp.test_enabled_flg,cdm_segment_test_tmp.test_nm,cdm_segment_test_tmp.test_pct,cdm_segment_test_tmp.test_sizing_type_nm,cdm_segment_test_tmp.test_type_nm,cdm_segment_test_tmp.updated_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_segment_test_tmp            , cdm_segment_test, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_segment_test_tmp            ;
    quit;
    %put ######## Staging table: cdm_segment_test_tmp             Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_segment_test;
      drop table work.cdm_segment_test;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_segment_test_x_segment_tmp  ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cdm_segment_test_x_segment, table_keys=%str(segment_id,task_version_id,test_cd), out_table=work.cdm_segment_test_x_segment);
 data &tmplib..cdm_segment_test_x_segment_tmp  ;
     set work.cdm_segment_test_x_segment;
  if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.) ;
  if segment_id='' then segment_id='-'; if task_version_id='' then task_version_id='-'; if test_cd='' then test_cd='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_segment_test_x_segment_tmp  , cdm_segment_test_x_segment);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_segment_test_x_segment using &tmpdbschema..cdm_segment_test_x_segment_tmp  
         on (cdm_segment_test_x_segment.segment_id=cdm_segment_test_x_segment_tmp.segment_id and cdm_segment_test_x_segment.task_version_id=cdm_segment_test_x_segment_tmp.task_version_id and cdm_segment_test_x_segment.test_cd=cdm_segment_test_x_segment_tmp.test_cd)
        when matched then  
        update set cdm_segment_test_x_segment.task_id = cdm_segment_test_x_segment_tmp.task_id , cdm_segment_test_x_segment.updated_dttm = cdm_segment_test_x_segment_tmp.updated_dttm
        when not matched then insert ( 
        segment_id,task_id,task_version_id,test_cd,updated_dttm
         ) values ( 
        cdm_segment_test_x_segment_tmp.segment_id,cdm_segment_test_x_segment_tmp.task_id,cdm_segment_test_x_segment_tmp.task_version_id,cdm_segment_test_x_segment_tmp.test_cd,cdm_segment_test_x_segment_tmp.updated_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_segment_test_x_segment_tmp  , cdm_segment_test_x_segment, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_segment_test_x_segment_tmp  ;
    quit;
    %put ######## Staging table: cdm_segment_test_x_segment_tmp   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_segment_test_x_segment;
      drop table work.cdm_segment_test_x_segment;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_task_custom_attr_tmp        ;
      quit;
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
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_task_custom_attr using &tmpdbschema..cdm_task_custom_attr_tmp        
         on (cdm_task_custom_attr.Hashed_pk_col = cdm_task_custom_attr_tmp.Hashed_pk_col)
        when matched then  
        update set cdm_task_custom_attr.attribute_character_val = cdm_task_custom_attr_tmp.attribute_character_val , cdm_task_custom_attr.attribute_dttm_val = cdm_task_custom_attr_tmp.attribute_dttm_val , cdm_task_custom_attr.attribute_numeric_val = cdm_task_custom_attr_tmp.attribute_numeric_val , cdm_task_custom_attr.extension_attribute_nm = cdm_task_custom_attr_tmp.extension_attribute_nm , cdm_task_custom_attr.task_id = cdm_task_custom_attr_tmp.task_id , cdm_task_custom_attr.updated_by_nm = cdm_task_custom_attr_tmp.updated_by_nm , cdm_task_custom_attr.updated_dttm = cdm_task_custom_attr_tmp.updated_dttm
        when not matched then insert ( 
        attribute_character_val,attribute_data_type_cd,attribute_dttm_val,attribute_nm,attribute_numeric_val,attribute_val,extension_attribute_nm,task_id,task_version_id,updated_by_nm,updated_dttm
        ,Hashed_pk_col ) values ( 
        cdm_task_custom_attr_tmp.attribute_character_val,cdm_task_custom_attr_tmp.attribute_data_type_cd,cdm_task_custom_attr_tmp.attribute_dttm_val,cdm_task_custom_attr_tmp.attribute_nm,cdm_task_custom_attr_tmp.attribute_numeric_val,cdm_task_custom_attr_tmp.attribute_val,cdm_task_custom_attr_tmp.extension_attribute_nm,cdm_task_custom_attr_tmp.task_id,cdm_task_custom_attr_tmp.task_version_id,cdm_task_custom_attr_tmp.updated_by_nm,cdm_task_custom_attr_tmp.updated_dttm,cdm_task_custom_attr_tmp.Hashed_pk_col
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_task_custom_attr_tmp        , cdm_task_custom_attr, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_task_custom_attr_tmp        ;
    quit;
    %put ######## Staging table: cdm_task_custom_attr_tmp         Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_task_custom_attr;
      drop table work.cdm_task_custom_attr;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..cdm_task_detail_tmp             ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=cdm_task_detail, table_keys=%str(task_version_id), out_table=work.cdm_task_detail);
 data &tmplib..cdm_task_detail_tmp             ;
     set work.cdm_task_detail;
  if export_dttm ne . then export_dttm = tzoneu2s(export_dttm,&timeZone_Value.);if scheduled_end_dttm ne . then scheduled_end_dttm = tzoneu2s(scheduled_end_dttm,&timeZone_Value.);if scheduled_start_dttm ne . then scheduled_start_dttm = tzoneu2s(scheduled_start_dttm,&timeZone_Value.);if updated_dttm ne . then updated_dttm = tzoneu2s(updated_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if task_version_id='' then task_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :cdm_task_detail_tmp             , cdm_task_detail);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..cdm_task_detail using &tmpdbschema..cdm_task_detail_tmp             
         on (cdm_task_detail.task_version_id=cdm_task_detail_tmp.task_version_id)
        when matched then  
        update set cdm_task_detail.active_flg = cdm_task_detail_tmp.active_flg , cdm_task_detail.budget_unit_cost_amt = cdm_task_detail_tmp.budget_unit_cost_amt , cdm_task_detail.budget_unit_usage_amt = cdm_task_detail_tmp.budget_unit_usage_amt , cdm_task_detail.business_context_id = cdm_task_detail_tmp.business_context_id , cdm_task_detail.campaign_id = cdm_task_detail_tmp.campaign_id , cdm_task_detail.contact_channel_cd = cdm_task_detail_tmp.contact_channel_cd , cdm_task_detail.control_group_action_nm = cdm_task_detail_tmp.control_group_action_nm , cdm_task_detail.created_dt = cdm_task_detail_tmp.created_dt , cdm_task_detail.created_user_nm = cdm_task_detail_tmp.created_user_nm , cdm_task_detail.export_dttm = cdm_task_detail_tmp.export_dttm , cdm_task_detail.limit_by_total_impression_flg = cdm_task_detail_tmp.limit_by_total_impression_flg , cdm_task_detail.limit_period_unit_cnt = cdm_task_detail_tmp.limit_period_unit_cnt , cdm_task_detail.max_budget_amt = cdm_task_detail_tmp.max_budget_amt , cdm_task_detail.max_budget_offer_amt = cdm_task_detail_tmp.max_budget_offer_amt , cdm_task_detail.maximum_period_expression_cnt = cdm_task_detail_tmp.maximum_period_expression_cnt , cdm_task_detail.min_budget_amt = cdm_task_detail_tmp.min_budget_amt , cdm_task_detail.min_budget_offer_amt = cdm_task_detail_tmp.min_budget_offer_amt , cdm_task_detail.modified_status_cd = cdm_task_detail_tmp.modified_status_cd , cdm_task_detail.owner_nm = cdm_task_detail_tmp.owner_nm , cdm_task_detail.published_flg = cdm_task_detail_tmp.published_flg , cdm_task_detail.recurr_type_cd = cdm_task_detail_tmp.recurr_type_cd , cdm_task_detail.recurring_schedule_flg = cdm_task_detail_tmp.recurring_schedule_flg , cdm_task_detail.saved_flg = cdm_task_detail_tmp.saved_flg , cdm_task_detail.scheduled_end_dttm = cdm_task_detail_tmp.scheduled_end_dttm , cdm_task_detail.scheduled_flg = cdm_task_detail_tmp.scheduled_flg , cdm_task_detail.scheduled_start_dttm = cdm_task_detail_tmp.scheduled_start_dttm , cdm_task_detail.segment_tests_flg = cdm_task_detail_tmp.segment_tests_flg , cdm_task_detail.source_system_cd = cdm_task_detail_tmp.source_system_cd , cdm_task_detail.staged_flg = cdm_task_detail_tmp.staged_flg , cdm_task_detail.standard_reply_flg = cdm_task_detail_tmp.standard_reply_flg , cdm_task_detail.stratified_sampling_action_nm = cdm_task_detail_tmp.stratified_sampling_action_nm , cdm_task_detail.subject_type_nm = cdm_task_detail_tmp.subject_type_nm , cdm_task_detail.task_cd = cdm_task_detail_tmp.task_cd , cdm_task_detail.task_delivery_type_nm = cdm_task_detail_tmp.task_delivery_type_nm , cdm_task_detail.task_desc = cdm_task_detail_tmp.task_desc , cdm_task_detail.task_id = cdm_task_detail_tmp.task_id , cdm_task_detail.task_nm = cdm_task_detail_tmp.task_nm , cdm_task_detail.task_status_cd = cdm_task_detail_tmp.task_status_cd , cdm_task_detail.task_subtype_nm = cdm_task_detail_tmp.task_subtype_nm , cdm_task_detail.task_type_nm = cdm_task_detail_tmp.task_type_nm , cdm_task_detail.update_contact_history_flg = cdm_task_detail_tmp.update_contact_history_flg , cdm_task_detail.updated_by_nm = cdm_task_detail_tmp.updated_by_nm , cdm_task_detail.updated_dttm = cdm_task_detail_tmp.updated_dttm , cdm_task_detail.valid_from_dttm = cdm_task_detail_tmp.valid_from_dttm , cdm_task_detail.valid_to_dttm = cdm_task_detail_tmp.valid_to_dttm
        when not matched then insert ( 
        active_flg,budget_unit_cost_amt,budget_unit_usage_amt,business_context_id,campaign_id,contact_channel_cd,control_group_action_nm,created_dt,created_user_nm,export_dttm,limit_by_total_impression_flg,limit_period_unit_cnt,max_budget_amt,max_budget_offer_amt,maximum_period_expression_cnt,min_budget_amt,min_budget_offer_amt,modified_status_cd,owner_nm,published_flg,recurr_type_cd,recurring_schedule_flg,saved_flg,scheduled_end_dttm,scheduled_flg,scheduled_start_dttm,segment_tests_flg,source_system_cd,staged_flg,standard_reply_flg,stratified_sampling_action_nm,subject_type_nm,task_cd,task_delivery_type_nm,task_desc,task_id,task_nm,task_status_cd,task_subtype_nm,task_type_nm,task_version_id,update_contact_history_flg,updated_by_nm,updated_dttm,valid_from_dttm,valid_to_dttm
         ) values ( 
        cdm_task_detail_tmp.active_flg,cdm_task_detail_tmp.budget_unit_cost_amt,cdm_task_detail_tmp.budget_unit_usage_amt,cdm_task_detail_tmp.business_context_id,cdm_task_detail_tmp.campaign_id,cdm_task_detail_tmp.contact_channel_cd,cdm_task_detail_tmp.control_group_action_nm,cdm_task_detail_tmp.created_dt,cdm_task_detail_tmp.created_user_nm,cdm_task_detail_tmp.export_dttm,cdm_task_detail_tmp.limit_by_total_impression_flg,cdm_task_detail_tmp.limit_period_unit_cnt,cdm_task_detail_tmp.max_budget_amt,cdm_task_detail_tmp.max_budget_offer_amt,cdm_task_detail_tmp.maximum_period_expression_cnt,cdm_task_detail_tmp.min_budget_amt,cdm_task_detail_tmp.min_budget_offer_amt,cdm_task_detail_tmp.modified_status_cd,cdm_task_detail_tmp.owner_nm,cdm_task_detail_tmp.published_flg,cdm_task_detail_tmp.recurr_type_cd,cdm_task_detail_tmp.recurring_schedule_flg,cdm_task_detail_tmp.saved_flg,cdm_task_detail_tmp.scheduled_end_dttm,cdm_task_detail_tmp.scheduled_flg,cdm_task_detail_tmp.scheduled_start_dttm,cdm_task_detail_tmp.segment_tests_flg,cdm_task_detail_tmp.source_system_cd,cdm_task_detail_tmp.staged_flg,cdm_task_detail_tmp.standard_reply_flg,cdm_task_detail_tmp.stratified_sampling_action_nm,cdm_task_detail_tmp.subject_type_nm,cdm_task_detail_tmp.task_cd,cdm_task_detail_tmp.task_delivery_type_nm,cdm_task_detail_tmp.task_desc,cdm_task_detail_tmp.task_id,cdm_task_detail_tmp.task_nm,cdm_task_detail_tmp.task_status_cd,cdm_task_detail_tmp.task_subtype_nm,cdm_task_detail_tmp.task_type_nm,cdm_task_detail_tmp.task_version_id,cdm_task_detail_tmp.update_contact_history_flg,cdm_task_detail_tmp.updated_by_nm,cdm_task_detail_tmp.updated_dttm,cdm_task_detail_tmp.valid_from_dttm,cdm_task_detail_tmp.valid_to_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :cdm_task_detail_tmp             , cdm_task_detail, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..cdm_task_detail_tmp             ;
    quit;
    %put ######## Staging table: cdm_task_detail_tmp              Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..cdm_task_detail;
      drop table work.cdm_task_detail;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..commitment_details_tmp          ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=commitment_details, table_keys=%str(cmtmnt_id,planning_id), out_table=work.commitment_details);
 data &tmplib..commitment_details_tmp          ;
     set work.commitment_details;
  if cmtmnt_created_dttm ne . then cmtmnt_created_dttm = tzoneu2s(cmtmnt_created_dttm,&timeZone_Value.);if cmtmnt_payment_dttm ne . then cmtmnt_payment_dttm = tzoneu2s(cmtmnt_payment_dttm,&timeZone_Value.);if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if cmtmnt_id='' then cmtmnt_id='-'; if planning_id='' then planning_id='-';
 run;
 %ErrCheck (Failed to Append Data to :commitment_details_tmp          , commitment_details);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..commitment_details using &tmpdbschema..commitment_details_tmp          
         on (commitment_details.cmtmnt_id=commitment_details_tmp.cmtmnt_id and commitment_details.planning_id=commitment_details_tmp.planning_id)
        when matched then  
        update set commitment_details.cmtmnt_amt = commitment_details_tmp.cmtmnt_amt , commitment_details.cmtmnt_closure_note = commitment_details_tmp.cmtmnt_closure_note , commitment_details.cmtmnt_created_dttm = commitment_details_tmp.cmtmnt_created_dttm , commitment_details.cmtmnt_desc = commitment_details_tmp.cmtmnt_desc , commitment_details.cmtmnt_nm = commitment_details_tmp.cmtmnt_nm , commitment_details.cmtmnt_no = commitment_details_tmp.cmtmnt_no , commitment_details.cmtmnt_outstanding_amt = commitment_details_tmp.cmtmnt_outstanding_amt , commitment_details.cmtmnt_overspent_amt = commitment_details_tmp.cmtmnt_overspent_amt , commitment_details.cmtmnt_payment_dttm = commitment_details_tmp.cmtmnt_payment_dttm , commitment_details.cmtmnt_status = commitment_details_tmp.cmtmnt_status , commitment_details.created_by_usernm = commitment_details_tmp.created_by_usernm , commitment_details.created_dttm = commitment_details_tmp.created_dttm , commitment_details.last_modified_dttm = commitment_details_tmp.last_modified_dttm , commitment_details.last_modified_usernm = commitment_details_tmp.last_modified_usernm , commitment_details.load_dttm = commitment_details_tmp.load_dttm , commitment_details.planning_currency_cd = commitment_details_tmp.planning_currency_cd , commitment_details.planning_nm = commitment_details_tmp.planning_nm , commitment_details.vendor_amt = commitment_details_tmp.vendor_amt , commitment_details.vendor_currency_cd = commitment_details_tmp.vendor_currency_cd , commitment_details.vendor_id = commitment_details_tmp.vendor_id , commitment_details.vendor_nm = commitment_details_tmp.vendor_nm , commitment_details.vendor_number = commitment_details_tmp.vendor_number , commitment_details.vendor_obsolete_flg = commitment_details_tmp.vendor_obsolete_flg
        when not matched then insert ( 
        cmtmnt_amt,cmtmnt_closure_note,cmtmnt_created_dttm,cmtmnt_desc,cmtmnt_id,cmtmnt_nm,cmtmnt_no,cmtmnt_outstanding_amt,cmtmnt_overspent_amt,cmtmnt_payment_dttm,cmtmnt_status,created_by_usernm,created_dttm,last_modified_dttm,last_modified_usernm,load_dttm,planning_currency_cd,planning_id,planning_nm,vendor_amt,vendor_currency_cd,vendor_id,vendor_nm,vendor_number,vendor_obsolete_flg
         ) values ( 
        commitment_details_tmp.cmtmnt_amt,commitment_details_tmp.cmtmnt_closure_note,commitment_details_tmp.cmtmnt_created_dttm,commitment_details_tmp.cmtmnt_desc,commitment_details_tmp.cmtmnt_id,commitment_details_tmp.cmtmnt_nm,commitment_details_tmp.cmtmnt_no,commitment_details_tmp.cmtmnt_outstanding_amt,commitment_details_tmp.cmtmnt_overspent_amt,commitment_details_tmp.cmtmnt_payment_dttm,commitment_details_tmp.cmtmnt_status,commitment_details_tmp.created_by_usernm,commitment_details_tmp.created_dttm,commitment_details_tmp.last_modified_dttm,commitment_details_tmp.last_modified_usernm,commitment_details_tmp.load_dttm,commitment_details_tmp.planning_currency_cd,commitment_details_tmp.planning_id,commitment_details_tmp.planning_nm,commitment_details_tmp.vendor_amt,commitment_details_tmp.vendor_currency_cd,commitment_details_tmp.vendor_id,commitment_details_tmp.vendor_nm,commitment_details_tmp.vendor_number,commitment_details_tmp.vendor_obsolete_flg
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :commitment_details_tmp          , commitment_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..commitment_details_tmp          ;
    quit;
    %put ######## Staging table: commitment_details_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..commitment_details;
      drop table work.commitment_details;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..commitment_line_items_tmp       ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=commitment_line_items, table_keys=%str(cmtmnt_id,item_nm,item_number,planning_id), out_table=work.commitment_line_items);
 data &tmplib..commitment_line_items_tmp       ;
     set work.commitment_line_items;
  if cmtmnt_created_dttm ne . then cmtmnt_created_dttm = tzoneu2s(cmtmnt_created_dttm,&timeZone_Value.);if cmtmnt_payment_dttm ne . then cmtmnt_payment_dttm = tzoneu2s(cmtmnt_payment_dttm,&timeZone_Value.);if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if cmtmnt_id='' then cmtmnt_id='-'; if item_nm='' then item_nm='-'; if planning_id='' then planning_id='-';
 run;
 %ErrCheck (Failed to Append Data to :commitment_line_items_tmp       , commitment_line_items);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..commitment_line_items using &tmpdbschema..commitment_line_items_tmp       
         on (commitment_line_items.cmtmnt_id=commitment_line_items_tmp.cmtmnt_id and commitment_line_items.item_nm=commitment_line_items_tmp.item_nm and commitment_line_items.item_number=commitment_line_items_tmp.item_number and commitment_line_items.planning_id=commitment_line_items_tmp.planning_id)
        when matched then  
        update set commitment_line_items.cc_allocated_amt = commitment_line_items_tmp.cc_allocated_amt , commitment_line_items.cc_available_amt = commitment_line_items_tmp.cc_available_amt , commitment_line_items.cc_desc = commitment_line_items_tmp.cc_desc , commitment_line_items.cc_nm = commitment_line_items_tmp.cc_nm , commitment_line_items.cc_owner_usernm = commitment_line_items_tmp.cc_owner_usernm , commitment_line_items.cc_recon_alloc_amt = commitment_line_items_tmp.cc_recon_alloc_amt , commitment_line_items.ccat_nm = commitment_line_items_tmp.ccat_nm , commitment_line_items.cmtmnt_amt = commitment_line_items_tmp.cmtmnt_amt , commitment_line_items.cmtmnt_closure_note = commitment_line_items_tmp.cmtmnt_closure_note , commitment_line_items.cmtmnt_created_dttm = commitment_line_items_tmp.cmtmnt_created_dttm , commitment_line_items.cmtmnt_desc = commitment_line_items_tmp.cmtmnt_desc , commitment_line_items.cmtmnt_nm = commitment_line_items_tmp.cmtmnt_nm , commitment_line_items.cmtmnt_no = commitment_line_items_tmp.cmtmnt_no , commitment_line_items.cmtmnt_outstanding_amt = commitment_line_items_tmp.cmtmnt_outstanding_amt , commitment_line_items.cmtmnt_overspent_amt = commitment_line_items_tmp.cmtmnt_overspent_amt , commitment_line_items.cmtmnt_payment_dttm = commitment_line_items_tmp.cmtmnt_payment_dttm , commitment_line_items.cmtmnt_status = commitment_line_items_tmp.cmtmnt_status , commitment_line_items.cost_center_id = commitment_line_items_tmp.cost_center_id , commitment_line_items.created_by_usernm = commitment_line_items_tmp.created_by_usernm , commitment_line_items.created_dttm = commitment_line_items_tmp.created_dttm , commitment_line_items.fin_acc_nm = commitment_line_items_tmp.fin_acc_nm , commitment_line_items.gen_ledger_cd = commitment_line_items_tmp.gen_ledger_cd , commitment_line_items.item_alloc_amt = commitment_line_items_tmp.item_alloc_amt , commitment_line_items.item_alloc_unit = commitment_line_items_tmp.item_alloc_unit , commitment_line_items.item_qty = commitment_line_items_tmp.item_qty , commitment_line_items.item_rate = commitment_line_items_tmp.item_rate , commitment_line_items.item_vend_alloc_amt = commitment_line_items_tmp.item_vend_alloc_amt , commitment_line_items.item_vend_alloc_unit = commitment_line_items_tmp.item_vend_alloc_unit , commitment_line_items.last_modified_dttm = commitment_line_items_tmp.last_modified_dttm , commitment_line_items.last_modified_usernm = commitment_line_items_tmp.last_modified_usernm , commitment_line_items.load_dttm = commitment_line_items_tmp.load_dttm , commitment_line_items.planning_currency_cd = commitment_line_items_tmp.planning_currency_cd , commitment_line_items.planning_nm = commitment_line_items_tmp.planning_nm , commitment_line_items.vendor_amt = commitment_line_items_tmp.vendor_amt , commitment_line_items.vendor_currency_cd = commitment_line_items_tmp.vendor_currency_cd , commitment_line_items.vendor_id = commitment_line_items_tmp.vendor_id , commitment_line_items.vendor_nm = commitment_line_items_tmp.vendor_nm , commitment_line_items.vendor_number = commitment_line_items_tmp.vendor_number , commitment_line_items.vendor_obsolete_flg = commitment_line_items_tmp.vendor_obsolete_flg
        when not matched then insert ( 
        cc_allocated_amt,cc_available_amt,cc_desc,cc_nm,cc_owner_usernm,cc_recon_alloc_amt,ccat_nm,cmtmnt_amt,cmtmnt_closure_note,cmtmnt_created_dttm,cmtmnt_desc,cmtmnt_id,cmtmnt_nm,cmtmnt_no,cmtmnt_outstanding_amt,cmtmnt_overspent_amt,cmtmnt_payment_dttm,cmtmnt_status,cost_center_id,created_by_usernm,created_dttm,fin_acc_nm,gen_ledger_cd,item_alloc_amt,item_alloc_unit,item_nm,item_number,item_qty,item_rate,item_vend_alloc_amt,item_vend_alloc_unit,last_modified_dttm,last_modified_usernm,load_dttm,planning_currency_cd,planning_id,planning_nm,vendor_amt,vendor_currency_cd,vendor_id,vendor_nm,vendor_number,vendor_obsolete_flg
         ) values ( 
        commitment_line_items_tmp.cc_allocated_amt,commitment_line_items_tmp.cc_available_amt,commitment_line_items_tmp.cc_desc,commitment_line_items_tmp.cc_nm,commitment_line_items_tmp.cc_owner_usernm,commitment_line_items_tmp.cc_recon_alloc_amt,commitment_line_items_tmp.ccat_nm,commitment_line_items_tmp.cmtmnt_amt,commitment_line_items_tmp.cmtmnt_closure_note,commitment_line_items_tmp.cmtmnt_created_dttm,commitment_line_items_tmp.cmtmnt_desc,commitment_line_items_tmp.cmtmnt_id,commitment_line_items_tmp.cmtmnt_nm,commitment_line_items_tmp.cmtmnt_no,commitment_line_items_tmp.cmtmnt_outstanding_amt,commitment_line_items_tmp.cmtmnt_overspent_amt,commitment_line_items_tmp.cmtmnt_payment_dttm,commitment_line_items_tmp.cmtmnt_status,commitment_line_items_tmp.cost_center_id,commitment_line_items_tmp.created_by_usernm,commitment_line_items_tmp.created_dttm,commitment_line_items_tmp.fin_acc_nm,commitment_line_items_tmp.gen_ledger_cd,commitment_line_items_tmp.item_alloc_amt,commitment_line_items_tmp.item_alloc_unit,commitment_line_items_tmp.item_nm,commitment_line_items_tmp.item_number,commitment_line_items_tmp.item_qty,commitment_line_items_tmp.item_rate,commitment_line_items_tmp.item_vend_alloc_amt,commitment_line_items_tmp.item_vend_alloc_unit,commitment_line_items_tmp.last_modified_dttm,commitment_line_items_tmp.last_modified_usernm,commitment_line_items_tmp.load_dttm,commitment_line_items_tmp.planning_currency_cd,commitment_line_items_tmp.planning_id,commitment_line_items_tmp.planning_nm,commitment_line_items_tmp.vendor_amt,commitment_line_items_tmp.vendor_currency_cd,commitment_line_items_tmp.vendor_id,commitment_line_items_tmp.vendor_nm,commitment_line_items_tmp.vendor_number,commitment_line_items_tmp.vendor_obsolete_flg
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :commitment_line_items_tmp       , commitment_line_items, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..commitment_line_items_tmp       ;
    quit;
    %put ######## Staging table: commitment_line_items_tmp        Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..commitment_line_items;
      drop table work.commitment_line_items;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..commitment_line_items_ccbdgt_tmp;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=commitment_line_items_ccbdgt, table_keys=%str(cmtmnt_id,item_number), out_table=work.commitment_line_items_ccbdgt);
 data &tmplib..commitment_line_items_ccbdgt_tmp;
     set work.commitment_line_items_ccbdgt;
  if cmtmnt_created_dttm ne . then cmtmnt_created_dttm = tzoneu2s(cmtmnt_created_dttm,&timeZone_Value.);if cmtmnt_payment_dttm ne . then cmtmnt_payment_dttm = tzoneu2s(cmtmnt_payment_dttm,&timeZone_Value.);if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if cmtmnt_id='' then cmtmnt_id='-';
 run;
 %ErrCheck (Failed to Append Data to :commitment_line_items_ccbdgt_tmp, commitment_line_items_ccbdgt);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..commitment_line_items_ccbdgt using &tmpdbschema..commitment_line_items_ccbdgt_tmp
         on (commitment_line_items_ccbdgt.cmtmnt_id=commitment_line_items_ccbdgt_tmp.cmtmnt_id and commitment_line_items_ccbdgt.item_number=commitment_line_items_ccbdgt_tmp.item_number)
        when matched then  
        update set commitment_line_items_ccbdgt.cc_allocated_amt = commitment_line_items_ccbdgt_tmp.cc_allocated_amt , commitment_line_items_ccbdgt.cc_available_amt = commitment_line_items_ccbdgt_tmp.cc_available_amt , commitment_line_items_ccbdgt.cc_bdgt_amt = commitment_line_items_ccbdgt_tmp.cc_bdgt_amt , commitment_line_items_ccbdgt.cc_bdgt_budget_amt = commitment_line_items_ccbdgt_tmp.cc_bdgt_budget_amt , commitment_line_items_ccbdgt.cc_bdgt_budget_desc = commitment_line_items_ccbdgt_tmp.cc_bdgt_budget_desc , commitment_line_items_ccbdgt.cc_bdgt_cmtmnt_invoice_amt = commitment_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_invoice_amt , commitment_line_items_ccbdgt.cc_bdgt_cmtmnt_invoice_cnt = commitment_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_invoice_cnt , commitment_line_items_ccbdgt.cc_bdgt_cmtmnt_outstanding_amt = commitment_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_outstanding_amt , commitment_line_items_ccbdgt.cc_bdgt_cmtmnt_overspent_amt = commitment_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_overspent_amt , commitment_line_items_ccbdgt.cc_bdgt_committed_amt = commitment_line_items_ccbdgt_tmp.cc_bdgt_committed_amt , commitment_line_items_ccbdgt.cc_bdgt_direct_invoice_amt = commitment_line_items_ccbdgt_tmp.cc_bdgt_direct_invoice_amt , commitment_line_items_ccbdgt.cc_bdgt_invoiced_amt = commitment_line_items_ccbdgt_tmp.cc_bdgt_invoiced_amt , commitment_line_items_ccbdgt.cc_desc = commitment_line_items_ccbdgt_tmp.cc_desc , commitment_line_items_ccbdgt.cc_nm = commitment_line_items_ccbdgt_tmp.cc_nm , commitment_line_items_ccbdgt.cc_number = commitment_line_items_ccbdgt_tmp.cc_number , commitment_line_items_ccbdgt.cc_obsolete_flg = commitment_line_items_ccbdgt_tmp.cc_obsolete_flg , commitment_line_items_ccbdgt.cc_owner_usernm = commitment_line_items_ccbdgt_tmp.cc_owner_usernm , commitment_line_items_ccbdgt.cc_recon_alloc_amt = commitment_line_items_ccbdgt_tmp.cc_recon_alloc_amt , commitment_line_items_ccbdgt.ccat_nm = commitment_line_items_ccbdgt_tmp.ccat_nm , commitment_line_items_ccbdgt.cmtmnt_amt = commitment_line_items_ccbdgt_tmp.cmtmnt_amt , commitment_line_items_ccbdgt.cmtmnt_closure_note = commitment_line_items_ccbdgt_tmp.cmtmnt_closure_note , commitment_line_items_ccbdgt.cmtmnt_created_dttm = commitment_line_items_ccbdgt_tmp.cmtmnt_created_dttm , commitment_line_items_ccbdgt.cmtmnt_desc = commitment_line_items_ccbdgt_tmp.cmtmnt_desc , commitment_line_items_ccbdgt.cmtmnt_nm = commitment_line_items_ccbdgt_tmp.cmtmnt_nm , commitment_line_items_ccbdgt.cmtmnt_no = commitment_line_items_ccbdgt_tmp.cmtmnt_no , commitment_line_items_ccbdgt.cmtmnt_outstanding_amt = commitment_line_items_ccbdgt_tmp.cmtmnt_outstanding_amt , commitment_line_items_ccbdgt.cmtmnt_overspent_amt = commitment_line_items_ccbdgt_tmp.cmtmnt_overspent_amt , commitment_line_items_ccbdgt.cmtmnt_payment_dttm = commitment_line_items_ccbdgt_tmp.cmtmnt_payment_dttm , commitment_line_items_ccbdgt.cmtmnt_status = commitment_line_items_ccbdgt_tmp.cmtmnt_status , commitment_line_items_ccbdgt.cost_center_id = commitment_line_items_ccbdgt_tmp.cost_center_id , commitment_line_items_ccbdgt.created_by_usernm = commitment_line_items_ccbdgt_tmp.created_by_usernm , commitment_line_items_ccbdgt.created_dttm = commitment_line_items_ccbdgt_tmp.created_dttm , commitment_line_items_ccbdgt.fin_acc_nm = commitment_line_items_ccbdgt_tmp.fin_acc_nm , commitment_line_items_ccbdgt.fp_cls_ver = commitment_line_items_ccbdgt_tmp.fp_cls_ver , commitment_line_items_ccbdgt.fp_desc = commitment_line_items_ccbdgt_tmp.fp_desc , commitment_line_items_ccbdgt.fp_end_dt = commitment_line_items_ccbdgt_tmp.fp_end_dt , commitment_line_items_ccbdgt.fp_id = commitment_line_items_ccbdgt_tmp.fp_id , commitment_line_items_ccbdgt.fp_nm = commitment_line_items_ccbdgt_tmp.fp_nm , commitment_line_items_ccbdgt.fp_obsolete_flg = commitment_line_items_ccbdgt_tmp.fp_obsolete_flg , commitment_line_items_ccbdgt.fp_start_dt = commitment_line_items_ccbdgt_tmp.fp_start_dt , commitment_line_items_ccbdgt.gen_ledger_cd = commitment_line_items_ccbdgt_tmp.gen_ledger_cd , commitment_line_items_ccbdgt.item_alloc_amt = commitment_line_items_ccbdgt_tmp.item_alloc_amt , commitment_line_items_ccbdgt.item_alloc_unit = commitment_line_items_ccbdgt_tmp.item_alloc_unit , commitment_line_items_ccbdgt.item_nm = commitment_line_items_ccbdgt_tmp.item_nm , commitment_line_items_ccbdgt.item_qty = commitment_line_items_ccbdgt_tmp.item_qty , commitment_line_items_ccbdgt.item_rate = commitment_line_items_ccbdgt_tmp.item_rate , commitment_line_items_ccbdgt.item_vend_alloc_amt = commitment_line_items_ccbdgt_tmp.item_vend_alloc_amt , commitment_line_items_ccbdgt.item_vend_alloc_unit = commitment_line_items_ccbdgt_tmp.item_vend_alloc_unit , commitment_line_items_ccbdgt.last_modified_dttm = commitment_line_items_ccbdgt_tmp.last_modified_dttm , commitment_line_items_ccbdgt.last_modified_usernm = commitment_line_items_ccbdgt_tmp.last_modified_usernm , commitment_line_items_ccbdgt.load_dttm = commitment_line_items_ccbdgt_tmp.load_dttm , commitment_line_items_ccbdgt.planning_currency_cd = commitment_line_items_ccbdgt_tmp.planning_currency_cd , commitment_line_items_ccbdgt.planning_id = commitment_line_items_ccbdgt_tmp.planning_id , commitment_line_items_ccbdgt.planning_nm = commitment_line_items_ccbdgt_tmp.planning_nm , commitment_line_items_ccbdgt.vendor_amt = commitment_line_items_ccbdgt_tmp.vendor_amt , commitment_line_items_ccbdgt.vendor_currency_cd = commitment_line_items_ccbdgt_tmp.vendor_currency_cd , commitment_line_items_ccbdgt.vendor_id = commitment_line_items_ccbdgt_tmp.vendor_id , commitment_line_items_ccbdgt.vendor_nm = commitment_line_items_ccbdgt_tmp.vendor_nm , commitment_line_items_ccbdgt.vendor_number = commitment_line_items_ccbdgt_tmp.vendor_number , commitment_line_items_ccbdgt.vendor_obsolete_flg = commitment_line_items_ccbdgt_tmp.vendor_obsolete_flg
        when not matched then insert ( 
        cc_allocated_amt,cc_available_amt,cc_bdgt_amt,cc_bdgt_budget_amt,cc_bdgt_budget_desc,cc_bdgt_cmtmnt_invoice_amt,cc_bdgt_cmtmnt_invoice_cnt,cc_bdgt_cmtmnt_outstanding_amt,cc_bdgt_cmtmnt_overspent_amt,cc_bdgt_committed_amt,cc_bdgt_direct_invoice_amt,cc_bdgt_invoiced_amt,cc_desc,cc_nm,cc_number,cc_obsolete_flg,cc_owner_usernm,cc_recon_alloc_amt,ccat_nm,cmtmnt_amt,cmtmnt_closure_note,cmtmnt_created_dttm,cmtmnt_desc,cmtmnt_id,cmtmnt_nm,cmtmnt_no,cmtmnt_outstanding_amt,cmtmnt_overspent_amt,cmtmnt_payment_dttm,cmtmnt_status,cost_center_id,created_by_usernm,created_dttm,fin_acc_nm,fp_cls_ver,fp_desc,fp_end_dt,fp_id,fp_nm,fp_obsolete_flg,fp_start_dt,gen_ledger_cd,item_alloc_amt,item_alloc_unit,item_nm,item_number,item_qty,item_rate,item_vend_alloc_amt,item_vend_alloc_unit,last_modified_dttm,last_modified_usernm,load_dttm,planning_currency_cd,planning_id,planning_nm,vendor_amt,vendor_currency_cd,vendor_id,vendor_nm,vendor_number,vendor_obsolete_flg
         ) values ( 
        commitment_line_items_ccbdgt_tmp.cc_allocated_amt,commitment_line_items_ccbdgt_tmp.cc_available_amt,commitment_line_items_ccbdgt_tmp.cc_bdgt_amt,commitment_line_items_ccbdgt_tmp.cc_bdgt_budget_amt,commitment_line_items_ccbdgt_tmp.cc_bdgt_budget_desc,commitment_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_invoice_amt,commitment_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_invoice_cnt,commitment_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_outstanding_amt,commitment_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_overspent_amt,commitment_line_items_ccbdgt_tmp.cc_bdgt_committed_amt,commitment_line_items_ccbdgt_tmp.cc_bdgt_direct_invoice_amt,commitment_line_items_ccbdgt_tmp.cc_bdgt_invoiced_amt,commitment_line_items_ccbdgt_tmp.cc_desc,commitment_line_items_ccbdgt_tmp.cc_nm,commitment_line_items_ccbdgt_tmp.cc_number,commitment_line_items_ccbdgt_tmp.cc_obsolete_flg,commitment_line_items_ccbdgt_tmp.cc_owner_usernm,commitment_line_items_ccbdgt_tmp.cc_recon_alloc_amt,commitment_line_items_ccbdgt_tmp.ccat_nm,commitment_line_items_ccbdgt_tmp.cmtmnt_amt,commitment_line_items_ccbdgt_tmp.cmtmnt_closure_note,commitment_line_items_ccbdgt_tmp.cmtmnt_created_dttm,commitment_line_items_ccbdgt_tmp.cmtmnt_desc,commitment_line_items_ccbdgt_tmp.cmtmnt_id,commitment_line_items_ccbdgt_tmp.cmtmnt_nm,commitment_line_items_ccbdgt_tmp.cmtmnt_no,commitment_line_items_ccbdgt_tmp.cmtmnt_outstanding_amt,commitment_line_items_ccbdgt_tmp.cmtmnt_overspent_amt,commitment_line_items_ccbdgt_tmp.cmtmnt_payment_dttm,commitment_line_items_ccbdgt_tmp.cmtmnt_status,commitment_line_items_ccbdgt_tmp.cost_center_id,commitment_line_items_ccbdgt_tmp.created_by_usernm,commitment_line_items_ccbdgt_tmp.created_dttm,commitment_line_items_ccbdgt_tmp.fin_acc_nm,commitment_line_items_ccbdgt_tmp.fp_cls_ver,commitment_line_items_ccbdgt_tmp.fp_desc,commitment_line_items_ccbdgt_tmp.fp_end_dt,commitment_line_items_ccbdgt_tmp.fp_id,commitment_line_items_ccbdgt_tmp.fp_nm,commitment_line_items_ccbdgt_tmp.fp_obsolete_flg,commitment_line_items_ccbdgt_tmp.fp_start_dt,commitment_line_items_ccbdgt_tmp.gen_ledger_cd,commitment_line_items_ccbdgt_tmp.item_alloc_amt,commitment_line_items_ccbdgt_tmp.item_alloc_unit,commitment_line_items_ccbdgt_tmp.item_nm,commitment_line_items_ccbdgt_tmp.item_number,commitment_line_items_ccbdgt_tmp.item_qty,commitment_line_items_ccbdgt_tmp.item_rate,commitment_line_items_ccbdgt_tmp.item_vend_alloc_amt,commitment_line_items_ccbdgt_tmp.item_vend_alloc_unit,commitment_line_items_ccbdgt_tmp.last_modified_dttm,commitment_line_items_ccbdgt_tmp.last_modified_usernm,commitment_line_items_ccbdgt_tmp.load_dttm,commitment_line_items_ccbdgt_tmp.planning_currency_cd,commitment_line_items_ccbdgt_tmp.planning_id,commitment_line_items_ccbdgt_tmp.planning_nm,commitment_line_items_ccbdgt_tmp.vendor_amt,commitment_line_items_ccbdgt_tmp.vendor_currency_cd,commitment_line_items_ccbdgt_tmp.vendor_id,commitment_line_items_ccbdgt_tmp.vendor_nm,commitment_line_items_ccbdgt_tmp.vendor_number,commitment_line_items_ccbdgt_tmp.vendor_obsolete_flg
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :commitment_line_items_ccbdgt_tmp, commitment_line_items_ccbdgt, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..commitment_line_items_ccbdgt_tmp;
    quit;
    %put ######## Staging table: commitment_line_items_ccbdgt_tmp Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..commitment_line_items_ccbdgt;
      drop table work.commitment_line_items_ccbdgt;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..contact_history_tmp             ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=contact_history, table_keys=%str(contact_id), out_table=work.contact_history);
 data &tmplib..contact_history_tmp             ;
     set work.contact_history;
  if contact_dttm ne . then contact_dttm = tzoneu2s(contact_dttm,&timeZone_Value.);if contact_dttm_tz ne . then contact_dttm_tz = tzoneu2s(contact_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if contact_id='' then contact_id='-';
 run;
 %ErrCheck (Failed to Append Data to :contact_history_tmp             , contact_history);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..contact_history using &tmpdbschema..contact_history_tmp             
         on (contact_history.contact_id=contact_history_tmp.contact_id)
        when matched then  
        update set contact_history.aud_occurrence_id = contact_history_tmp.aud_occurrence_id , contact_history.audience_id = contact_history_tmp.audience_id , contact_history.contact_channel_nm = contact_history_tmp.contact_channel_nm , contact_history.contact_dttm = contact_history_tmp.contact_dttm , contact_history.contact_dttm_tz = contact_history_tmp.contact_dttm_tz , contact_history.contact_nm = contact_history_tmp.contact_nm , contact_history.context_type_nm = contact_history_tmp.context_type_nm , contact_history.context_val = contact_history_tmp.context_val , contact_history.control_group_flg = contact_history_tmp.control_group_flg , contact_history.creative_id = contact_history_tmp.creative_id , contact_history.detail_id_hex = contact_history_tmp.detail_id_hex , contact_history.event_designed_id = contact_history_tmp.event_designed_id , contact_history.identity_id = contact_history_tmp.identity_id , contact_history.journey_id = contact_history_tmp.journey_id , contact_history.journey_occurrence_id = contact_history_tmp.journey_occurrence_id , contact_history.load_dttm = contact_history_tmp.load_dttm , contact_history.message_id = contact_history_tmp.message_id , contact_history.occurrence_id = contact_history_tmp.occurrence_id , contact_history.parent_event_designed_id = contact_history_tmp.parent_event_designed_id , contact_history.properties_map_doc = contact_history_tmp.properties_map_doc , contact_history.response_tracking_cd = contact_history_tmp.response_tracking_cd , contact_history.session_id_hex = contact_history_tmp.session_id_hex , contact_history.task_id = contact_history_tmp.task_id , contact_history.task_version_id = contact_history_tmp.task_version_id , contact_history.visit_id_hex = contact_history_tmp.visit_id_hex
        when not matched then insert ( 
        aud_occurrence_id,audience_id,contact_channel_nm,contact_dttm,contact_dttm_tz,contact_id,contact_nm,context_type_nm,context_val,control_group_flg,creative_id,detail_id_hex,event_designed_id,identity_id,journey_id,journey_occurrence_id,load_dttm,message_id,occurrence_id,parent_event_designed_id,properties_map_doc,response_tracking_cd,session_id_hex,task_id,task_version_id,visit_id_hex
         ) values ( 
        contact_history_tmp.aud_occurrence_id,contact_history_tmp.audience_id,contact_history_tmp.contact_channel_nm,contact_history_tmp.contact_dttm,contact_history_tmp.contact_dttm_tz,contact_history_tmp.contact_id,contact_history_tmp.contact_nm,contact_history_tmp.context_type_nm,contact_history_tmp.context_val,contact_history_tmp.control_group_flg,contact_history_tmp.creative_id,contact_history_tmp.detail_id_hex,contact_history_tmp.event_designed_id,contact_history_tmp.identity_id,contact_history_tmp.journey_id,contact_history_tmp.journey_occurrence_id,contact_history_tmp.load_dttm,contact_history_tmp.message_id,contact_history_tmp.occurrence_id,contact_history_tmp.parent_event_designed_id,contact_history_tmp.properties_map_doc,contact_history_tmp.response_tracking_cd,contact_history_tmp.session_id_hex,contact_history_tmp.task_id,contact_history_tmp.task_version_id,contact_history_tmp.visit_id_hex
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :contact_history_tmp             , contact_history, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..contact_history_tmp             ;
    quit;
    %put ######## Staging table: contact_history_tmp              Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..contact_history;
      drop table work.contact_history;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..conversion_milestone_tmp        ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=conversion_milestone, table_keys=%str(event_id), out_table=work.conversion_milestone);
 data &tmplib..conversion_milestone_tmp        ;
     set work.conversion_milestone;
  if conversion_milestone_dttm ne . then conversion_milestone_dttm = tzoneu2s(conversion_milestone_dttm,&timeZone_Value.);if conversion_milestone_dttm_tz ne . then conversion_milestone_dttm_tz = tzoneu2s(conversion_milestone_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :conversion_milestone_tmp        , conversion_milestone);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..conversion_milestone using &tmpdbschema..conversion_milestone_tmp        
         on (conversion_milestone.event_id=conversion_milestone_tmp.event_id)
        when matched then  
        update set conversion_milestone.activity_id = conversion_milestone_tmp.activity_id , conversion_milestone.analysis_group_id = conversion_milestone_tmp.analysis_group_id , conversion_milestone.aud_occurrence_id = conversion_milestone_tmp.aud_occurrence_id , conversion_milestone.audience_id = conversion_milestone_tmp.audience_id , conversion_milestone.channel_nm = conversion_milestone_tmp.channel_nm , conversion_milestone.channel_user_id = conversion_milestone_tmp.channel_user_id , conversion_milestone.context_type_nm = conversion_milestone_tmp.context_type_nm , conversion_milestone.context_val = conversion_milestone_tmp.context_val , conversion_milestone.control_group_flg = conversion_milestone_tmp.control_group_flg , conversion_milestone.conversion_milestone_dttm = conversion_milestone_tmp.conversion_milestone_dttm , conversion_milestone.conversion_milestone_dttm_tz = conversion_milestone_tmp.conversion_milestone_dttm_tz , conversion_milestone.creative_id = conversion_milestone_tmp.creative_id , conversion_milestone.creative_version_id = conversion_milestone_tmp.creative_version_id , conversion_milestone.detail_id_hex = conversion_milestone_tmp.detail_id_hex , conversion_milestone.event_designed_id = conversion_milestone_tmp.event_designed_id , conversion_milestone.event_nm = conversion_milestone_tmp.event_nm , conversion_milestone.goal_id = conversion_milestone_tmp.goal_id , conversion_milestone.identity_id = conversion_milestone_tmp.identity_id , conversion_milestone.journey_id = conversion_milestone_tmp.journey_id , conversion_milestone.journey_occurrence_id = conversion_milestone_tmp.journey_occurrence_id , conversion_milestone.load_dttm = conversion_milestone_tmp.load_dttm , conversion_milestone.message_id = conversion_milestone_tmp.message_id , conversion_milestone.message_version_id = conversion_milestone_tmp.message_version_id , conversion_milestone.mobile_app_id = conversion_milestone_tmp.mobile_app_id , conversion_milestone.occurrence_id = conversion_milestone_tmp.occurrence_id , conversion_milestone.parent_event_designed_id = conversion_milestone_tmp.parent_event_designed_id , conversion_milestone.properties_map_doc = conversion_milestone_tmp.properties_map_doc , conversion_milestone.rec_group_id = conversion_milestone_tmp.rec_group_id , conversion_milestone.reserved_1_txt = conversion_milestone_tmp.reserved_1_txt , conversion_milestone.reserved_2_txt = conversion_milestone_tmp.reserved_2_txt , conversion_milestone.response_tracking_cd = conversion_milestone_tmp.response_tracking_cd , conversion_milestone.segment_id = conversion_milestone_tmp.segment_id , conversion_milestone.segment_version_id = conversion_milestone_tmp.segment_version_id , conversion_milestone.session_id_hex = conversion_milestone_tmp.session_id_hex , conversion_milestone.spot_id = conversion_milestone_tmp.spot_id , conversion_milestone.subject_line_txt = conversion_milestone_tmp.subject_line_txt , conversion_milestone.task_id = conversion_milestone_tmp.task_id , conversion_milestone.task_version_id = conversion_milestone_tmp.task_version_id , conversion_milestone.test_flg = conversion_milestone_tmp.test_flg , conversion_milestone.total_cost_amt = conversion_milestone_tmp.total_cost_amt , conversion_milestone.visit_id_hex = conversion_milestone_tmp.visit_id_hex
        when not matched then insert ( 
        activity_id,analysis_group_id,aud_occurrence_id,audience_id,channel_nm,channel_user_id,context_type_nm,context_val,control_group_flg,conversion_milestone_dttm,conversion_milestone_dttm_tz,creative_id,creative_version_id,detail_id_hex,event_designed_id,event_id,event_nm,goal_id,identity_id,journey_id,journey_occurrence_id,load_dttm,message_id,message_version_id,mobile_app_id,occurrence_id,parent_event_designed_id,properties_map_doc,rec_group_id,reserved_1_txt,reserved_2_txt,response_tracking_cd,segment_id,segment_version_id,session_id_hex,spot_id,subject_line_txt,task_id,task_version_id,test_flg,total_cost_amt,visit_id_hex
         ) values ( 
        conversion_milestone_tmp.activity_id,conversion_milestone_tmp.analysis_group_id,conversion_milestone_tmp.aud_occurrence_id,conversion_milestone_tmp.audience_id,conversion_milestone_tmp.channel_nm,conversion_milestone_tmp.channel_user_id,conversion_milestone_tmp.context_type_nm,conversion_milestone_tmp.context_val,conversion_milestone_tmp.control_group_flg,conversion_milestone_tmp.conversion_milestone_dttm,conversion_milestone_tmp.conversion_milestone_dttm_tz,conversion_milestone_tmp.creative_id,conversion_milestone_tmp.creative_version_id,conversion_milestone_tmp.detail_id_hex,conversion_milestone_tmp.event_designed_id,conversion_milestone_tmp.event_id,conversion_milestone_tmp.event_nm,conversion_milestone_tmp.goal_id,conversion_milestone_tmp.identity_id,conversion_milestone_tmp.journey_id,conversion_milestone_tmp.journey_occurrence_id,conversion_milestone_tmp.load_dttm,conversion_milestone_tmp.message_id,conversion_milestone_tmp.message_version_id,conversion_milestone_tmp.mobile_app_id,conversion_milestone_tmp.occurrence_id,conversion_milestone_tmp.parent_event_designed_id,conversion_milestone_tmp.properties_map_doc,conversion_milestone_tmp.rec_group_id,conversion_milestone_tmp.reserved_1_txt,conversion_milestone_tmp.reserved_2_txt,conversion_milestone_tmp.response_tracking_cd,conversion_milestone_tmp.segment_id,conversion_milestone_tmp.segment_version_id,conversion_milestone_tmp.session_id_hex,conversion_milestone_tmp.spot_id,conversion_milestone_tmp.subject_line_txt,conversion_milestone_tmp.task_id,conversion_milestone_tmp.task_version_id,conversion_milestone_tmp.test_flg,conversion_milestone_tmp.total_cost_amt,conversion_milestone_tmp.visit_id_hex
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :conversion_milestone_tmp        , conversion_milestone, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..conversion_milestone_tmp        ;
    quit;
    %put ######## Staging table: conversion_milestone_tmp         Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..conversion_milestone;
      drop table work.conversion_milestone;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..custom_events_tmp               ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=custom_events, table_keys=%str(event_id), out_table=work.custom_events);
 data &tmplib..custom_events_tmp               ;
     set work.custom_events;
  if custom_event_dttm ne . then custom_event_dttm = tzoneu2s(custom_event_dttm,&timeZone_Value.);if custom_event_dttm_tz ne . then custom_event_dttm_tz = tzoneu2s(custom_event_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :custom_events_tmp               , custom_events);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..custom_events using &tmpdbschema..custom_events_tmp               
         on (custom_events.event_id=custom_events_tmp.event_id)
        when matched then  
        update set custom_events.channel_nm = custom_events_tmp.channel_nm , custom_events.channel_user_id = custom_events_tmp.channel_user_id , custom_events.custom_event_dttm = custom_events_tmp.custom_event_dttm , custom_events.custom_event_dttm_tz = custom_events_tmp.custom_event_dttm_tz , custom_events.custom_event_group_nm = custom_events_tmp.custom_event_group_nm , custom_events.custom_event_nm = custom_events_tmp.custom_event_nm , custom_events.custom_events_sk = custom_events_tmp.custom_events_sk , custom_events.custom_revenue_amt = custom_events_tmp.custom_revenue_amt , custom_events.detail_id = custom_events_tmp.detail_id , custom_events.detail_id_hex = custom_events_tmp.detail_id_hex , custom_events.event_designed_id = custom_events_tmp.event_designed_id , custom_events.event_key_cd = custom_events_tmp.event_key_cd , custom_events.event_nm = custom_events_tmp.event_nm , custom_events.event_source_cd = custom_events_tmp.event_source_cd , custom_events.event_type_nm = custom_events_tmp.event_type_nm , custom_events.identity_id = custom_events_tmp.identity_id , custom_events.load_dttm = custom_events_tmp.load_dttm , custom_events.mobile_app_id = custom_events_tmp.mobile_app_id , custom_events.page_id = custom_events_tmp.page_id , custom_events.properties_map_doc = custom_events_tmp.properties_map_doc , custom_events.reserved_1_txt = custom_events_tmp.reserved_1_txt , custom_events.reserved_2_txt = custom_events_tmp.reserved_2_txt , custom_events.session_id = custom_events_tmp.session_id , custom_events.session_id_hex = custom_events_tmp.session_id_hex , custom_events.visit_id = custom_events_tmp.visit_id , custom_events.visit_id_hex = custom_events_tmp.visit_id_hex
        when not matched then insert ( 
        channel_nm,channel_user_id,custom_event_dttm,custom_event_dttm_tz,custom_event_group_nm,custom_event_nm,custom_events_sk,custom_revenue_amt,detail_id,detail_id_hex,event_designed_id,event_id,event_key_cd,event_nm,event_source_cd,event_type_nm,identity_id,load_dttm,mobile_app_id,page_id,properties_map_doc,reserved_1_txt,reserved_2_txt,session_id,session_id_hex,visit_id,visit_id_hex
         ) values ( 
        custom_events_tmp.channel_nm,custom_events_tmp.channel_user_id,custom_events_tmp.custom_event_dttm,custom_events_tmp.custom_event_dttm_tz,custom_events_tmp.custom_event_group_nm,custom_events_tmp.custom_event_nm,custom_events_tmp.custom_events_sk,custom_events_tmp.custom_revenue_amt,custom_events_tmp.detail_id,custom_events_tmp.detail_id_hex,custom_events_tmp.event_designed_id,custom_events_tmp.event_id,custom_events_tmp.event_key_cd,custom_events_tmp.event_nm,custom_events_tmp.event_source_cd,custom_events_tmp.event_type_nm,custom_events_tmp.identity_id,custom_events_tmp.load_dttm,custom_events_tmp.mobile_app_id,custom_events_tmp.page_id,custom_events_tmp.properties_map_doc,custom_events_tmp.reserved_1_txt,custom_events_tmp.reserved_2_txt,custom_events_tmp.session_id,custom_events_tmp.session_id_hex,custom_events_tmp.visit_id,custom_events_tmp.visit_id_hex
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :custom_events_tmp               , custom_events, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..custom_events_tmp               ;
    quit;
    %put ######## Staging table: custom_events_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..custom_events;
      drop table work.custom_events;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..custom_events_ext_tmp           ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=custom_events_ext, table_keys=%str(custom_events_sk), out_table=work.custom_events_ext);
 data &tmplib..custom_events_ext_tmp           ;
     set work.custom_events_ext;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if custom_events_sk='' then custom_events_sk='-';
 run;
 %ErrCheck (Failed to Append Data to :custom_events_ext_tmp           , custom_events_ext);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..custom_events_ext using &tmpdbschema..custom_events_ext_tmp           
         on (custom_events_ext.custom_events_sk=custom_events_ext_tmp.custom_events_sk)
        when matched then  
        update set custom_events_ext.custom_revenue_amt = custom_events_ext_tmp.custom_revenue_amt , custom_events_ext.event_designed_id = custom_events_ext_tmp.event_designed_id , custom_events_ext.load_dttm = custom_events_ext_tmp.load_dttm
        when not matched then insert ( 
        custom_events_sk,custom_revenue_amt,event_designed_id,load_dttm
         ) values ( 
        custom_events_ext_tmp.custom_events_sk,custom_events_ext_tmp.custom_revenue_amt,custom_events_ext_tmp.event_designed_id,custom_events_ext_tmp.load_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :custom_events_ext_tmp           , custom_events_ext, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..custom_events_ext_tmp           ;
    quit;
    %put ######## Staging table: custom_events_ext_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..custom_events_ext;
      drop table work.custom_events_ext;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..daily_usage_tmp                 ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=daily_usage, table_keys=%str(event_day), out_table=work.daily_usage);
 data &tmplib..daily_usage_tmp                 ;
     set work.daily_usage;
  if event_day='' then event_day='-';
 run;
 %ErrCheck (Failed to Append Data to :daily_usage_tmp                 , daily_usage);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..daily_usage using &tmpdbschema..daily_usage_tmp                 
         on (daily_usage.event_day=daily_usage_tmp.event_day)
        when matched then  
        update set daily_usage.admin_user_cnt = daily_usage_tmp.admin_user_cnt , daily_usage.api_usage_str = daily_usage_tmp.api_usage_str , daily_usage.asset_size = daily_usage_tmp.asset_size , daily_usage.audience_usage_cnt = daily_usage_tmp.audience_usage_cnt , daily_usage.bc_subjcnt_str = daily_usage_tmp.bc_subjcnt_str , daily_usage.db_size = daily_usage_tmp.db_size , daily_usage.email_preview_cnt = daily_usage_tmp.email_preview_cnt , daily_usage.email_send_cnt = daily_usage_tmp.email_send_cnt , daily_usage.facebook_ads_cnt = daily_usage_tmp.facebook_ads_cnt , daily_usage.google_ads_cnt = daily_usage_tmp.google_ads_cnt , daily_usage.linkedin_ads_cnt = daily_usage_tmp.linkedin_ads_cnt , daily_usage.mob_impr_cnt = daily_usage_tmp.mob_impr_cnt , daily_usage.mob_sesn_cnt = daily_usage_tmp.mob_sesn_cnt , daily_usage.mobile_in_app_msg_cnt = daily_usage_tmp.mobile_in_app_msg_cnt , daily_usage.mobile_push_cnt = daily_usage_tmp.mobile_push_cnt , daily_usage.outbound_api_cnt = daily_usage_tmp.outbound_api_cnt , daily_usage.plan_users_cnt = daily_usage_tmp.plan_users_cnt , daily_usage.web_impr_cnt = daily_usage_tmp.web_impr_cnt , daily_usage.web_sesn_cnt = daily_usage_tmp.web_sesn_cnt
        when not matched then insert ( 
        admin_user_cnt,api_usage_str,asset_size,audience_usage_cnt,bc_subjcnt_str,db_size,email_preview_cnt,email_send_cnt,event_day,facebook_ads_cnt,google_ads_cnt,linkedin_ads_cnt,mob_impr_cnt,mob_sesn_cnt,mobile_in_app_msg_cnt,mobile_push_cnt,outbound_api_cnt,plan_users_cnt,web_impr_cnt,web_sesn_cnt
         ) values ( 
        daily_usage_tmp.admin_user_cnt,daily_usage_tmp.api_usage_str,daily_usage_tmp.asset_size,daily_usage_tmp.audience_usage_cnt,daily_usage_tmp.bc_subjcnt_str,daily_usage_tmp.db_size,daily_usage_tmp.email_preview_cnt,daily_usage_tmp.email_send_cnt,daily_usage_tmp.event_day,daily_usage_tmp.facebook_ads_cnt,daily_usage_tmp.google_ads_cnt,daily_usage_tmp.linkedin_ads_cnt,daily_usage_tmp.mob_impr_cnt,daily_usage_tmp.mob_sesn_cnt,daily_usage_tmp.mobile_in_app_msg_cnt,daily_usage_tmp.mobile_push_cnt,daily_usage_tmp.outbound_api_cnt,daily_usage_tmp.plan_users_cnt,daily_usage_tmp.web_impr_cnt,daily_usage_tmp.web_sesn_cnt
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :daily_usage_tmp                 , daily_usage, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..daily_usage_tmp                 ;
    quit;
    %put ######## Staging table: daily_usage_tmp                  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..daily_usage;
      drop table work.daily_usage;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..data_view_details_tmp           ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=data_view_details, table_keys=%str(detail_id_hex,event_id), out_table=work.data_view_details);
 data &tmplib..data_view_details_tmp           ;
     set work.data_view_details;
  if data_view_dttm ne . then data_view_dttm = tzoneu2s(data_view_dttm,&timeZone_Value.);if data_view_dttm_tz ne . then data_view_dttm_tz = tzoneu2s(data_view_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if detail_id_hex='' then detail_id_hex='-'; if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :data_view_details_tmp           , data_view_details);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..data_view_details using &tmpdbschema..data_view_details_tmp           
         on (data_view_details.detail_id_hex=data_view_details_tmp.detail_id_hex and data_view_details.event_id=data_view_details_tmp.event_id)
        when matched then  
        update set data_view_details.channel_user_id = data_view_details_tmp.channel_user_id , data_view_details.data_view_dttm = data_view_details_tmp.data_view_dttm , data_view_details.data_view_dttm_tz = data_view_details_tmp.data_view_dttm_tz , data_view_details.detail_id = data_view_details_tmp.detail_id , data_view_details.event_designed_id = data_view_details_tmp.event_designed_id , data_view_details.event_nm = data_view_details_tmp.event_nm , data_view_details.identity_id = data_view_details_tmp.identity_id , data_view_details.load_dttm = data_view_details_tmp.load_dttm , data_view_details.parent_event_designed_id = data_view_details_tmp.parent_event_designed_id , data_view_details.properties_map_doc = data_view_details_tmp.properties_map_doc , data_view_details.reserved_1_txt = data_view_details_tmp.reserved_1_txt , data_view_details.reserved_2_txt = data_view_details_tmp.reserved_2_txt , data_view_details.session_id = data_view_details_tmp.session_id , data_view_details.session_id_hex = data_view_details_tmp.session_id_hex , data_view_details.total_cost_amt = data_view_details_tmp.total_cost_amt , data_view_details.visit_id = data_view_details_tmp.visit_id , data_view_details.visit_id_hex = data_view_details_tmp.visit_id_hex
        when not matched then insert ( 
        channel_user_id,data_view_dttm,data_view_dttm_tz,detail_id,detail_id_hex,event_designed_id,event_id,event_nm,identity_id,load_dttm,parent_event_designed_id,properties_map_doc,reserved_1_txt,reserved_2_txt,session_id,session_id_hex,total_cost_amt,visit_id,visit_id_hex
         ) values ( 
        data_view_details_tmp.channel_user_id,data_view_details_tmp.data_view_dttm,data_view_details_tmp.data_view_dttm_tz,data_view_details_tmp.detail_id,data_view_details_tmp.detail_id_hex,data_view_details_tmp.event_designed_id,data_view_details_tmp.event_id,data_view_details_tmp.event_nm,data_view_details_tmp.identity_id,data_view_details_tmp.load_dttm,data_view_details_tmp.parent_event_designed_id,data_view_details_tmp.properties_map_doc,data_view_details_tmp.reserved_1_txt,data_view_details_tmp.reserved_2_txt,data_view_details_tmp.session_id,data_view_details_tmp.session_id_hex,data_view_details_tmp.total_cost_amt,data_view_details_tmp.visit_id,data_view_details_tmp.visit_id_hex
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :data_view_details_tmp           , data_view_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..data_view_details_tmp           ;
    quit;
    %put ######## Staging table: data_view_details_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..data_view_details;
      drop table work.data_view_details;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..dbt_adv_campaign_visitors_tmp   ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=dbt_adv_campaign_visitors, table_keys=%str(session_id,visit_id), out_table=work.dbt_adv_campaign_visitors);
 data &tmplib..dbt_adv_campaign_visitors_tmp   ;
     set work.dbt_adv_campaign_visitors;
  if session_complete_load_dttm ne . then session_complete_load_dttm = tzoneu2s(session_complete_load_dttm,&timeZone_Value.);if session_start_dttm ne . then session_start_dttm = tzoneu2s(session_start_dttm,&timeZone_Value.);if session_start_dttm_tz ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz,&timeZone_Value.);if visit_dttm ne . then visit_dttm = tzoneu2s(visit_dttm,&timeZone_Value.);if visit_dttm_tz ne . then visit_dttm_tz = tzoneu2s(visit_dttm_tz,&timeZone_Value.) ;
  if session_id='' then session_id='-'; if visit_id='' then visit_id='-';
 run;
 %ErrCheck (Failed to Append Data to :dbt_adv_campaign_visitors_tmp   , dbt_adv_campaign_visitors);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..dbt_adv_campaign_visitors using &tmpdbschema..dbt_adv_campaign_visitors_tmp   
         on (dbt_adv_campaign_visitors.session_id=dbt_adv_campaign_visitors_tmp.session_id and dbt_adv_campaign_visitors.visit_id=dbt_adv_campaign_visitors_tmp.visit_id)
        when matched then  
        update set dbt_adv_campaign_visitors.average_visit_duration = dbt_adv_campaign_visitors_tmp.average_visit_duration , dbt_adv_campaign_visitors.bouncer = dbt_adv_campaign_visitors_tmp.bouncer , dbt_adv_campaign_visitors.bouncers = dbt_adv_campaign_visitors_tmp.bouncers , dbt_adv_campaign_visitors.br_browser_name = dbt_adv_campaign_visitors_tmp.br_browser_name , dbt_adv_campaign_visitors.br_browser_version = dbt_adv_campaign_visitors_tmp.br_browser_version , dbt_adv_campaign_visitors.co_conversions = dbt_adv_campaign_visitors_tmp.co_conversions , dbt_adv_campaign_visitors.cu_customer_id = dbt_adv_campaign_visitors_tmp.cu_customer_id , dbt_adv_campaign_visitors.device_name = dbt_adv_campaign_visitors_tmp.device_name , dbt_adv_campaign_visitors.device_type = dbt_adv_campaign_visitors_tmp.device_type , dbt_adv_campaign_visitors.ge_city = dbt_adv_campaign_visitors_tmp.ge_city , dbt_adv_campaign_visitors.ge_country = dbt_adv_campaign_visitors_tmp.ge_country , dbt_adv_campaign_visitors.ge_latitude = dbt_adv_campaign_visitors_tmp.ge_latitude , dbt_adv_campaign_visitors.ge_longitude = dbt_adv_campaign_visitors_tmp.ge_longitude , dbt_adv_campaign_visitors.ge_state_region = dbt_adv_campaign_visitors_tmp.ge_state_region , dbt_adv_campaign_visitors.landing_page = dbt_adv_campaign_visitors_tmp.landing_page , dbt_adv_campaign_visitors.landing_page_url = dbt_adv_campaign_visitors_tmp.landing_page_url , dbt_adv_campaign_visitors.landing_page_url_domain = dbt_adv_campaign_visitors_tmp.landing_page_url_domain , dbt_adv_campaign_visitors.new_visitors = dbt_adv_campaign_visitors_tmp.new_visitors , dbt_adv_campaign_visitors.page_views = dbt_adv_campaign_visitors_tmp.page_views , dbt_adv_campaign_visitors.pl_device_operating_system = dbt_adv_campaign_visitors_tmp.pl_device_operating_system , dbt_adv_campaign_visitors.return_visitors = dbt_adv_campaign_visitors_tmp.return_visitors , dbt_adv_campaign_visitors.rv_revenue = dbt_adv_campaign_visitors_tmp.rv_revenue , dbt_adv_campaign_visitors.se_external_search_engine = dbt_adv_campaign_visitors_tmp.se_external_search_engine , dbt_adv_campaign_visitors.se_external_search_engine_domain = dbt_adv_campaign_visitors_tmp.se_external_search_engine_domain , dbt_adv_campaign_visitors.se_external_search_engine_phrase = dbt_adv_campaign_visitors_tmp.se_external_search_engine_phrase , dbt_adv_campaign_visitors.session_complete_load_dttm = dbt_adv_campaign_visitors_tmp.session_complete_load_dttm , dbt_adv_campaign_visitors.session_start_dttm = dbt_adv_campaign_visitors_tmp.session_start_dttm , dbt_adv_campaign_visitors.session_start_dttm_tz = dbt_adv_campaign_visitors_tmp.session_start_dttm_tz , dbt_adv_campaign_visitors.visit_dttm = dbt_adv_campaign_visitors_tmp.visit_dttm , dbt_adv_campaign_visitors.visit_dttm_tz = dbt_adv_campaign_visitors_tmp.visit_dttm_tz , dbt_adv_campaign_visitors.visit_origination_creative = dbt_adv_campaign_visitors_tmp.visit_origination_creative , dbt_adv_campaign_visitors.visit_origination_name = dbt_adv_campaign_visitors_tmp.visit_origination_name , dbt_adv_campaign_visitors.visit_origination_placement = dbt_adv_campaign_visitors_tmp.visit_origination_placement , dbt_adv_campaign_visitors.visit_origination_tracking_code = dbt_adv_campaign_visitors_tmp.visit_origination_tracking_code , dbt_adv_campaign_visitors.visit_origination_type = dbt_adv_campaign_visitors_tmp.visit_origination_type , dbt_adv_campaign_visitors.visitor_id = dbt_adv_campaign_visitors_tmp.visitor_id , dbt_adv_campaign_visitors.visitor_type = dbt_adv_campaign_visitors_tmp.visitor_type , dbt_adv_campaign_visitors.visits = dbt_adv_campaign_visitors_tmp.visits
        when not matched then insert ( 
        average_visit_duration,bouncer,bouncers,br_browser_name,br_browser_version,co_conversions,cu_customer_id,device_name,device_type,ge_city,ge_country,ge_latitude,ge_longitude,ge_state_region,landing_page,landing_page_url,landing_page_url_domain,new_visitors,page_views,pl_device_operating_system,return_visitors,rv_revenue,se_external_search_engine,se_external_search_engine_domain,se_external_search_engine_phrase,session_complete_load_dttm,session_id,session_start_dttm,session_start_dttm_tz,visit_dttm,visit_dttm_tz,visit_id,visit_origination_creative,visit_origination_name,visit_origination_placement,visit_origination_tracking_code,visit_origination_type,visitor_id,visitor_type,visits
         ) values ( 
        dbt_adv_campaign_visitors_tmp.average_visit_duration,dbt_adv_campaign_visitors_tmp.bouncer,dbt_adv_campaign_visitors_tmp.bouncers,dbt_adv_campaign_visitors_tmp.br_browser_name,dbt_adv_campaign_visitors_tmp.br_browser_version,dbt_adv_campaign_visitors_tmp.co_conversions,dbt_adv_campaign_visitors_tmp.cu_customer_id,dbt_adv_campaign_visitors_tmp.device_name,dbt_adv_campaign_visitors_tmp.device_type,dbt_adv_campaign_visitors_tmp.ge_city,dbt_adv_campaign_visitors_tmp.ge_country,dbt_adv_campaign_visitors_tmp.ge_latitude,dbt_adv_campaign_visitors_tmp.ge_longitude,dbt_adv_campaign_visitors_tmp.ge_state_region,dbt_adv_campaign_visitors_tmp.landing_page,dbt_adv_campaign_visitors_tmp.landing_page_url,dbt_adv_campaign_visitors_tmp.landing_page_url_domain,dbt_adv_campaign_visitors_tmp.new_visitors,dbt_adv_campaign_visitors_tmp.page_views,dbt_adv_campaign_visitors_tmp.pl_device_operating_system,dbt_adv_campaign_visitors_tmp.return_visitors,dbt_adv_campaign_visitors_tmp.rv_revenue,dbt_adv_campaign_visitors_tmp.se_external_search_engine,dbt_adv_campaign_visitors_tmp.se_external_search_engine_domain,dbt_adv_campaign_visitors_tmp.se_external_search_engine_phrase,dbt_adv_campaign_visitors_tmp.session_complete_load_dttm,dbt_adv_campaign_visitors_tmp.session_id,dbt_adv_campaign_visitors_tmp.session_start_dttm,dbt_adv_campaign_visitors_tmp.session_start_dttm_tz,dbt_adv_campaign_visitors_tmp.visit_dttm,dbt_adv_campaign_visitors_tmp.visit_dttm_tz,dbt_adv_campaign_visitors_tmp.visit_id,dbt_adv_campaign_visitors_tmp.visit_origination_creative,dbt_adv_campaign_visitors_tmp.visit_origination_name,dbt_adv_campaign_visitors_tmp.visit_origination_placement,dbt_adv_campaign_visitors_tmp.visit_origination_tracking_code,dbt_adv_campaign_visitors_tmp.visit_origination_type,dbt_adv_campaign_visitors_tmp.visitor_id,dbt_adv_campaign_visitors_tmp.visitor_type,dbt_adv_campaign_visitors_tmp.visits
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :dbt_adv_campaign_visitors_tmp   , dbt_adv_campaign_visitors, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..dbt_adv_campaign_visitors_tmp   ;
    quit;
    %put ######## Staging table: dbt_adv_campaign_visitors_tmp    Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..dbt_adv_campaign_visitors;
      drop table work.dbt_adv_campaign_visitors;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..dbt_business_process_tmp        ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=dbt_business_process, table_keys=%str(bus_process_started_dttm,business_process_name,business_process_step_name,session_id), out_table=work.dbt_business_process);
 data &tmplib..dbt_business_process_tmp        ;
     set work.dbt_business_process;
  if bus_process_started_dttm ne . then bus_process_started_dttm = tzoneu2s(bus_process_started_dttm,&timeZone_Value.);if bus_process_started_dttm_tz ne . then bus_process_started_dttm_tz = tzoneu2s(bus_process_started_dttm_tz,&timeZone_Value.);if session_complete_load_dttm ne . then session_complete_load_dttm = tzoneu2s(session_complete_load_dttm,&timeZone_Value.);if session_start_dttm ne . then session_start_dttm = tzoneu2s(session_start_dttm,&timeZone_Value.);if session_start_dttm_tz ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz,&timeZone_Value.) ;
  if business_process_name='' then business_process_name='-'; if business_process_step_name='' then business_process_step_name='-'; if session_id='' then session_id='-';
 run;
 %ErrCheck (Failed to Append Data to :dbt_business_process_tmp        , dbt_business_process);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..dbt_business_process using &tmpdbschema..dbt_business_process_tmp        
         on (dbt_business_process.bus_process_started_dttm=dbt_business_process_tmp.bus_process_started_dttm and dbt_business_process.business_process_name=dbt_business_process_tmp.business_process_name and dbt_business_process.business_process_step_name=dbt_business_process_tmp.business_process_step_name and dbt_business_process.session_id=dbt_business_process_tmp.session_id)
        when matched then  
        update set dbt_business_process.bouncer = dbt_business_process_tmp.bouncer , dbt_business_process.bus_process_started_dttm_tz = dbt_business_process_tmp.bus_process_started_dttm_tz , dbt_business_process.business_process_attribute_1 = dbt_business_process_tmp.business_process_attribute_1 , dbt_business_process.business_process_attribute_2 = dbt_business_process_tmp.business_process_attribute_2 , dbt_business_process.cu_customer_id = dbt_business_process_tmp.cu_customer_id , dbt_business_process.device_name = dbt_business_process_tmp.device_name , dbt_business_process.device_type = dbt_business_process_tmp.device_type , dbt_business_process.last_step = dbt_business_process_tmp.last_step , dbt_business_process.processes = dbt_business_process_tmp.processes , dbt_business_process.processes_abandoned = dbt_business_process_tmp.processes_abandoned , dbt_business_process.processes_completed = dbt_business_process_tmp.processes_completed , dbt_business_process.session_complete_load_dttm = dbt_business_process_tmp.session_complete_load_dttm , dbt_business_process.session_start_dttm = dbt_business_process_tmp.session_start_dttm , dbt_business_process.session_start_dttm_tz = dbt_business_process_tmp.session_start_dttm_tz , dbt_business_process.step_count = dbt_business_process_tmp.step_count , dbt_business_process.steps = dbt_business_process_tmp.steps , dbt_business_process.steps_abandoned = dbt_business_process_tmp.steps_abandoned , dbt_business_process.steps_completed = dbt_business_process_tmp.steps_completed , dbt_business_process.visit_id = dbt_business_process_tmp.visit_id , dbt_business_process.visit_origination_creative = dbt_business_process_tmp.visit_origination_creative , dbt_business_process.visit_origination_name = dbt_business_process_tmp.visit_origination_name , dbt_business_process.visit_origination_placement = dbt_business_process_tmp.visit_origination_placement , dbt_business_process.visit_origination_tracking_code = dbt_business_process_tmp.visit_origination_tracking_code , dbt_business_process.visit_origination_type = dbt_business_process_tmp.visit_origination_type , dbt_business_process.visitor_id = dbt_business_process_tmp.visitor_id , dbt_business_process.visitor_type = dbt_business_process_tmp.visitor_type
        when not matched then insert ( 
        bouncer,bus_process_started_dttm,bus_process_started_dttm_tz,business_process_attribute_1,business_process_attribute_2,business_process_name,business_process_step_name,cu_customer_id,device_name,device_type,last_step,processes,processes_abandoned,processes_completed,session_complete_load_dttm,session_id,session_start_dttm,session_start_dttm_tz,step_count,steps,steps_abandoned,steps_completed,visit_id,visit_origination_creative,visit_origination_name,visit_origination_placement,visit_origination_tracking_code,visit_origination_type,visitor_id,visitor_type
         ) values ( 
        dbt_business_process_tmp.bouncer,dbt_business_process_tmp.bus_process_started_dttm,dbt_business_process_tmp.bus_process_started_dttm_tz,dbt_business_process_tmp.business_process_attribute_1,dbt_business_process_tmp.business_process_attribute_2,dbt_business_process_tmp.business_process_name,dbt_business_process_tmp.business_process_step_name,dbt_business_process_tmp.cu_customer_id,dbt_business_process_tmp.device_name,dbt_business_process_tmp.device_type,dbt_business_process_tmp.last_step,dbt_business_process_tmp.processes,dbt_business_process_tmp.processes_abandoned,dbt_business_process_tmp.processes_completed,dbt_business_process_tmp.session_complete_load_dttm,dbt_business_process_tmp.session_id,dbt_business_process_tmp.session_start_dttm,dbt_business_process_tmp.session_start_dttm_tz,dbt_business_process_tmp.step_count,dbt_business_process_tmp.steps,dbt_business_process_tmp.steps_abandoned,dbt_business_process_tmp.steps_completed,dbt_business_process_tmp.visit_id,dbt_business_process_tmp.visit_origination_creative,dbt_business_process_tmp.visit_origination_name,dbt_business_process_tmp.visit_origination_placement,dbt_business_process_tmp.visit_origination_tracking_code,dbt_business_process_tmp.visit_origination_type,dbt_business_process_tmp.visitor_id,dbt_business_process_tmp.visitor_type
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :dbt_business_process_tmp        , dbt_business_process, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..dbt_business_process_tmp        ;
    quit;
    %put ######## Staging table: dbt_business_process_tmp         Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..dbt_business_process;
      drop table work.dbt_business_process;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..dbt_content_tmp                 ;
      quit;
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
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..dbt_content using &tmpdbschema..dbt_content_tmp                 
         on (dbt_content.Hashed_pk_col = dbt_content_tmp.Hashed_pk_col)
        when matched then  
        update set dbt_content.bouncer = dbt_content_tmp.bouncer , dbt_content.bouncers = dbt_content_tmp.bouncers , dbt_content.class1_id = dbt_content_tmp.class1_id , dbt_content.class2_id = dbt_content_tmp.class2_id , dbt_content.cu_customer_id = dbt_content_tmp.cu_customer_id , dbt_content.detail_dttm_tz = dbt_content_tmp.detail_dttm_tz , dbt_content.device_name = dbt_content_tmp.device_name , dbt_content.device_type = dbt_content_tmp.device_type , dbt_content.pg_domain_name = dbt_content_tmp.pg_domain_name , dbt_content.pg_page = dbt_content_tmp.pg_page , dbt_content.session_complete_load_dttm = dbt_content_tmp.session_complete_load_dttm , dbt_content.session_start_dttm = dbt_content_tmp.session_start_dttm , dbt_content.session_start_dttm_tz = dbt_content_tmp.session_start_dttm_tz , dbt_content.total_page_view_time = dbt_content_tmp.total_page_view_time , dbt_content.views = dbt_content_tmp.views , dbt_content.visit_id = dbt_content_tmp.visit_id , dbt_content.visit_origination_creative = dbt_content_tmp.visit_origination_creative , dbt_content.visit_origination_name = dbt_content_tmp.visit_origination_name , dbt_content.visit_origination_placement = dbt_content_tmp.visit_origination_placement , dbt_content.visit_origination_tracking_code = dbt_content_tmp.visit_origination_tracking_code , dbt_content.visit_origination_type = dbt_content_tmp.visit_origination_type , dbt_content.visitor_id = dbt_content_tmp.visitor_id , dbt_content.visitor_type = dbt_content_tmp.visitor_type , dbt_content.visits = dbt_content_tmp.visits
        when not matched then insert ( 
        active_page_view_time,bouncer,bouncers,class1_id,class2_id,cu_customer_id,detail_dttm,detail_dttm_tz,device_name,device_type,entry_pages,exit_pages,pg_domain_name,pg_page,pg_page_url,session_complete_load_dttm,session_id,session_start_dttm,session_start_dttm_tz,total_page_view_time,views,visit_id,visit_origination_creative,visit_origination_name,visit_origination_placement,visit_origination_tracking_code,visit_origination_type,visitor_id,visitor_type,visits
        ,Hashed_pk_col ) values ( 
        dbt_content_tmp.active_page_view_time,dbt_content_tmp.bouncer,dbt_content_tmp.bouncers,dbt_content_tmp.class1_id,dbt_content_tmp.class2_id,dbt_content_tmp.cu_customer_id,dbt_content_tmp.detail_dttm,dbt_content_tmp.detail_dttm_tz,dbt_content_tmp.device_name,dbt_content_tmp.device_type,dbt_content_tmp.entry_pages,dbt_content_tmp.exit_pages,dbt_content_tmp.pg_domain_name,dbt_content_tmp.pg_page,dbt_content_tmp.pg_page_url,dbt_content_tmp.session_complete_load_dttm,dbt_content_tmp.session_id,dbt_content_tmp.session_start_dttm,dbt_content_tmp.session_start_dttm_tz,dbt_content_tmp.total_page_view_time,dbt_content_tmp.views,dbt_content_tmp.visit_id,dbt_content_tmp.visit_origination_creative,dbt_content_tmp.visit_origination_name,dbt_content_tmp.visit_origination_placement,dbt_content_tmp.visit_origination_tracking_code,dbt_content_tmp.visit_origination_type,dbt_content_tmp.visitor_id,dbt_content_tmp.visitor_type,dbt_content_tmp.visits,dbt_content_tmp.Hashed_pk_col
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :dbt_content_tmp                 , dbt_content, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..dbt_content_tmp                 ;
    quit;
    %put ######## Staging table: dbt_content_tmp                  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..dbt_content;
      drop table work.dbt_content;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..dbt_documents_tmp               ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=dbt_documents, table_keys=%str(document_download_dttm,session_id), out_table=work.dbt_documents);
 data &tmplib..dbt_documents_tmp               ;
     set work.dbt_documents;
  if document_download_dttm ne . then document_download_dttm = tzoneu2s(document_download_dttm,&timeZone_Value.);if document_download_dttm_tz ne . then document_download_dttm_tz = tzoneu2s(document_download_dttm_tz,&timeZone_Value.);if session_complete_load_dttm ne . then session_complete_load_dttm = tzoneu2s(session_complete_load_dttm,&timeZone_Value.);if session_start_dttm ne . then session_start_dttm = tzoneu2s(session_start_dttm,&timeZone_Value.);if session_start_dttm_tz ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz,&timeZone_Value.) ;
  if session_id='' then session_id='-';
 run;
 %ErrCheck (Failed to Append Data to :dbt_documents_tmp               , dbt_documents);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..dbt_documents using &tmpdbschema..dbt_documents_tmp               
         on (dbt_documents.document_download_dttm=dbt_documents_tmp.document_download_dttm and dbt_documents.session_id=dbt_documents_tmp.session_id)
        when matched then  
        update set dbt_documents.bouncer = dbt_documents_tmp.bouncer , dbt_documents.class1_id = dbt_documents_tmp.class1_id , dbt_documents.class2_id = dbt_documents_tmp.class2_id , dbt_documents.cu_customer_id = dbt_documents_tmp.cu_customer_id , dbt_documents.device_name = dbt_documents_tmp.device_name , dbt_documents.device_type = dbt_documents_tmp.device_type , dbt_documents.do_page_description = dbt_documents_tmp.do_page_description , dbt_documents.do_page_url = dbt_documents_tmp.do_page_url , dbt_documents.document_download_dttm_tz = dbt_documents_tmp.document_download_dttm_tz , dbt_documents.document_downloads = dbt_documents_tmp.document_downloads , dbt_documents.session_complete_load_dttm = dbt_documents_tmp.session_complete_load_dttm , dbt_documents.session_start_dttm = dbt_documents_tmp.session_start_dttm , dbt_documents.session_start_dttm_tz = dbt_documents_tmp.session_start_dttm_tz , dbt_documents.visit_id = dbt_documents_tmp.visit_id , dbt_documents.visit_origination_creative = dbt_documents_tmp.visit_origination_creative , dbt_documents.visit_origination_name = dbt_documents_tmp.visit_origination_name , dbt_documents.visit_origination_placement = dbt_documents_tmp.visit_origination_placement , dbt_documents.visit_origination_tracking_code = dbt_documents_tmp.visit_origination_tracking_code , dbt_documents.visit_origination_type = dbt_documents_tmp.visit_origination_type , dbt_documents.visitor_id = dbt_documents_tmp.visitor_id , dbt_documents.visitor_type = dbt_documents_tmp.visitor_type
        when not matched then insert ( 
        bouncer,class1_id,class2_id,cu_customer_id,device_name,device_type,do_page_description,do_page_url,document_download_dttm,document_download_dttm_tz,document_downloads,session_complete_load_dttm,session_id,session_start_dttm,session_start_dttm_tz,visit_id,visit_origination_creative,visit_origination_name,visit_origination_placement,visit_origination_tracking_code,visit_origination_type,visitor_id,visitor_type
         ) values ( 
        dbt_documents_tmp.bouncer,dbt_documents_tmp.class1_id,dbt_documents_tmp.class2_id,dbt_documents_tmp.cu_customer_id,dbt_documents_tmp.device_name,dbt_documents_tmp.device_type,dbt_documents_tmp.do_page_description,dbt_documents_tmp.do_page_url,dbt_documents_tmp.document_download_dttm,dbt_documents_tmp.document_download_dttm_tz,dbt_documents_tmp.document_downloads,dbt_documents_tmp.session_complete_load_dttm,dbt_documents_tmp.session_id,dbt_documents_tmp.session_start_dttm,dbt_documents_tmp.session_start_dttm_tz,dbt_documents_tmp.visit_id,dbt_documents_tmp.visit_origination_creative,dbt_documents_tmp.visit_origination_name,dbt_documents_tmp.visit_origination_placement,dbt_documents_tmp.visit_origination_tracking_code,dbt_documents_tmp.visit_origination_type,dbt_documents_tmp.visitor_id,dbt_documents_tmp.visitor_type
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :dbt_documents_tmp               , dbt_documents, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..dbt_documents_tmp               ;
    quit;
    %put ######## Staging table: dbt_documents_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..dbt_documents;
      drop table work.dbt_documents;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..dbt_ecommerce_tmp               ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=dbt_ecommerce, table_keys=%str(product_activity_dttm,product_id,session_id), out_table=work.dbt_ecommerce);
 data &tmplib..dbt_ecommerce_tmp               ;
     set work.dbt_ecommerce;
  if product_activity_dttm ne . then product_activity_dttm = tzoneu2s(product_activity_dttm,&timeZone_Value.);if product_activity_dttm_tz ne . then product_activity_dttm_tz = tzoneu2s(product_activity_dttm_tz,&timeZone_Value.);if session_complete_load_dttm ne . then session_complete_load_dttm = tzoneu2s(session_complete_load_dttm,&timeZone_Value.);if session_start_dttm ne . then session_start_dttm = tzoneu2s(session_start_dttm,&timeZone_Value.);if session_start_dttm_tz ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz,&timeZone_Value.) ;
  if product_id='' then product_id='-'; if session_id='' then session_id='-';
 run;
 %ErrCheck (Failed to Append Data to :dbt_ecommerce_tmp               , dbt_ecommerce);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..dbt_ecommerce using &tmpdbschema..dbt_ecommerce_tmp               
         on (dbt_ecommerce.product_activity_dttm=dbt_ecommerce_tmp.product_activity_dttm and dbt_ecommerce.product_id=dbt_ecommerce_tmp.product_id and dbt_ecommerce.session_id=dbt_ecommerce_tmp.session_id)
        when matched then  
        update set dbt_ecommerce.basket_adds = dbt_ecommerce_tmp.basket_adds , dbt_ecommerce.basket_adds_revenue = dbt_ecommerce_tmp.basket_adds_revenue , dbt_ecommerce.basket_adds_units = dbt_ecommerce_tmp.basket_adds_units , dbt_ecommerce.basket_id = dbt_ecommerce_tmp.basket_id , dbt_ecommerce.basket_removes = dbt_ecommerce_tmp.basket_removes , dbt_ecommerce.basket_removes_revenue = dbt_ecommerce_tmp.basket_removes_revenue , dbt_ecommerce.basket_removes_units = dbt_ecommerce_tmp.basket_removes_units , dbt_ecommerce.baskets_abandoned = dbt_ecommerce_tmp.baskets_abandoned , dbt_ecommerce.baskets_completed = dbt_ecommerce_tmp.baskets_completed , dbt_ecommerce.baskets_started = dbt_ecommerce_tmp.baskets_started , dbt_ecommerce.bouncer = dbt_ecommerce_tmp.bouncer , dbt_ecommerce.cu_customer_id = dbt_ecommerce_tmp.cu_customer_id , dbt_ecommerce.device_name = dbt_ecommerce_tmp.device_name , dbt_ecommerce.device_type = dbt_ecommerce_tmp.device_type , dbt_ecommerce.product_activity_dttm_tz = dbt_ecommerce_tmp.product_activity_dttm_tz , dbt_ecommerce.product_group_name = dbt_ecommerce_tmp.product_group_name , dbt_ecommerce.product_name = dbt_ecommerce_tmp.product_name , dbt_ecommerce.product_purchase_revenues = dbt_ecommerce_tmp.product_purchase_revenues , dbt_ecommerce.product_purchase_units = dbt_ecommerce_tmp.product_purchase_units , dbt_ecommerce.product_purchases = dbt_ecommerce_tmp.product_purchases , dbt_ecommerce.product_sku = dbt_ecommerce_tmp.product_sku , dbt_ecommerce.product_views = dbt_ecommerce_tmp.product_views , dbt_ecommerce.session_complete_load_dttm = dbt_ecommerce_tmp.session_complete_load_dttm , dbt_ecommerce.session_start_dttm = dbt_ecommerce_tmp.session_start_dttm , dbt_ecommerce.session_start_dttm_tz = dbt_ecommerce_tmp.session_start_dttm_tz , dbt_ecommerce.visit_id = dbt_ecommerce_tmp.visit_id , dbt_ecommerce.visit_origination_creative = dbt_ecommerce_tmp.visit_origination_creative , dbt_ecommerce.visit_origination_name = dbt_ecommerce_tmp.visit_origination_name , dbt_ecommerce.visit_origination_placement = dbt_ecommerce_tmp.visit_origination_placement , dbt_ecommerce.visit_origination_tracking_code = dbt_ecommerce_tmp.visit_origination_tracking_code , dbt_ecommerce.visit_origination_type = dbt_ecommerce_tmp.visit_origination_type , dbt_ecommerce.visitor_id = dbt_ecommerce_tmp.visitor_id , dbt_ecommerce.visitor_type = dbt_ecommerce_tmp.visitor_type
        when not matched then insert ( 
        basket_adds,basket_adds_revenue,basket_adds_units,basket_id,basket_removes,basket_removes_revenue,basket_removes_units,baskets_abandoned,baskets_completed,baskets_started,bouncer,cu_customer_id,device_name,device_type,product_activity_dttm,product_activity_dttm_tz,product_group_name,product_id,product_name,product_purchase_revenues,product_purchase_units,product_purchases,product_sku,product_views,session_complete_load_dttm,session_id,session_start_dttm,session_start_dttm_tz,visit_id,visit_origination_creative,visit_origination_name,visit_origination_placement,visit_origination_tracking_code,visit_origination_type,visitor_id,visitor_type
         ) values ( 
        dbt_ecommerce_tmp.basket_adds,dbt_ecommerce_tmp.basket_adds_revenue,dbt_ecommerce_tmp.basket_adds_units,dbt_ecommerce_tmp.basket_id,dbt_ecommerce_tmp.basket_removes,dbt_ecommerce_tmp.basket_removes_revenue,dbt_ecommerce_tmp.basket_removes_units,dbt_ecommerce_tmp.baskets_abandoned,dbt_ecommerce_tmp.baskets_completed,dbt_ecommerce_tmp.baskets_started,dbt_ecommerce_tmp.bouncer,dbt_ecommerce_tmp.cu_customer_id,dbt_ecommerce_tmp.device_name,dbt_ecommerce_tmp.device_type,dbt_ecommerce_tmp.product_activity_dttm,dbt_ecommerce_tmp.product_activity_dttm_tz,dbt_ecommerce_tmp.product_group_name,dbt_ecommerce_tmp.product_id,dbt_ecommerce_tmp.product_name,dbt_ecommerce_tmp.product_purchase_revenues,dbt_ecommerce_tmp.product_purchase_units,dbt_ecommerce_tmp.product_purchases,dbt_ecommerce_tmp.product_sku,dbt_ecommerce_tmp.product_views,dbt_ecommerce_tmp.session_complete_load_dttm,dbt_ecommerce_tmp.session_id,dbt_ecommerce_tmp.session_start_dttm,dbt_ecommerce_tmp.session_start_dttm_tz,dbt_ecommerce_tmp.visit_id,dbt_ecommerce_tmp.visit_origination_creative,dbt_ecommerce_tmp.visit_origination_name,dbt_ecommerce_tmp.visit_origination_placement,dbt_ecommerce_tmp.visit_origination_tracking_code,dbt_ecommerce_tmp.visit_origination_type,dbt_ecommerce_tmp.visitor_id,dbt_ecommerce_tmp.visitor_type
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :dbt_ecommerce_tmp               , dbt_ecommerce, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..dbt_ecommerce_tmp               ;
    quit;
    %put ######## Staging table: dbt_ecommerce_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..dbt_ecommerce;
      drop table work.dbt_ecommerce;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..dbt_forms_tmp                   ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=dbt_forms, table_keys=%str(form_attempt_dttm,form_nm,session_id), out_table=work.dbt_forms);
 data &tmplib..dbt_forms_tmp                   ;
     set work.dbt_forms;
  if form_attempt_dttm ne . then form_attempt_dttm = tzoneu2s(form_attempt_dttm,&timeZone_Value.);if form_attempt_dttm_tz ne . then form_attempt_dttm_tz = tzoneu2s(form_attempt_dttm_tz,&timeZone_Value.);if session_complete_load_dttm ne . then session_complete_load_dttm = tzoneu2s(session_complete_load_dttm,&timeZone_Value.);if session_start_dttm ne . then session_start_dttm = tzoneu2s(session_start_dttm,&timeZone_Value.);if session_start_dttm_tz ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz,&timeZone_Value.) ;
  if form_nm='' then form_nm='-'; if session_id='' then session_id='-';
 run;
 %ErrCheck (Failed to Append Data to :dbt_forms_tmp                   , dbt_forms);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..dbt_forms using &tmpdbschema..dbt_forms_tmp                   
         on (dbt_forms.form_attempt_dttm=dbt_forms_tmp.form_attempt_dttm and dbt_forms.form_nm=dbt_forms_tmp.form_nm and dbt_forms.session_id=dbt_forms_tmp.session_id)
        when matched then  
        update set dbt_forms.attempts = dbt_forms_tmp.attempts , dbt_forms.bouncer = dbt_forms_tmp.bouncer , dbt_forms.cu_customer_id = dbt_forms_tmp.cu_customer_id , dbt_forms.device_name = dbt_forms_tmp.device_name , dbt_forms.device_type = dbt_forms_tmp.device_type , dbt_forms.form_attempt_dttm_tz = dbt_forms_tmp.form_attempt_dttm_tz , dbt_forms.forms_completed = dbt_forms_tmp.forms_completed , dbt_forms.forms_not_submitted = dbt_forms_tmp.forms_not_submitted , dbt_forms.forms_started = dbt_forms_tmp.forms_started , dbt_forms.last_field = dbt_forms_tmp.last_field , dbt_forms.session_complete_load_dttm = dbt_forms_tmp.session_complete_load_dttm , dbt_forms.session_start_dttm = dbt_forms_tmp.session_start_dttm , dbt_forms.session_start_dttm_tz = dbt_forms_tmp.session_start_dttm_tz , dbt_forms.visit_id = dbt_forms_tmp.visit_id , dbt_forms.visit_origination_creative = dbt_forms_tmp.visit_origination_creative , dbt_forms.visit_origination_name = dbt_forms_tmp.visit_origination_name , dbt_forms.visit_origination_placement = dbt_forms_tmp.visit_origination_placement , dbt_forms.visit_origination_tracking_code = dbt_forms_tmp.visit_origination_tracking_code , dbt_forms.visit_origination_type = dbt_forms_tmp.visit_origination_type , dbt_forms.visitor_id = dbt_forms_tmp.visitor_id , dbt_forms.visitor_type = dbt_forms_tmp.visitor_type
        when not matched then insert ( 
        attempts,bouncer,cu_customer_id,device_name,device_type,form_attempt_dttm,form_attempt_dttm_tz,form_nm,forms_completed,forms_not_submitted,forms_started,last_field,session_complete_load_dttm,session_id,session_start_dttm,session_start_dttm_tz,visit_id,visit_origination_creative,visit_origination_name,visit_origination_placement,visit_origination_tracking_code,visit_origination_type,visitor_id,visitor_type
         ) values ( 
        dbt_forms_tmp.attempts,dbt_forms_tmp.bouncer,dbt_forms_tmp.cu_customer_id,dbt_forms_tmp.device_name,dbt_forms_tmp.device_type,dbt_forms_tmp.form_attempt_dttm,dbt_forms_tmp.form_attempt_dttm_tz,dbt_forms_tmp.form_nm,dbt_forms_tmp.forms_completed,dbt_forms_tmp.forms_not_submitted,dbt_forms_tmp.forms_started,dbt_forms_tmp.last_field,dbt_forms_tmp.session_complete_load_dttm,dbt_forms_tmp.session_id,dbt_forms_tmp.session_start_dttm,dbt_forms_tmp.session_start_dttm_tz,dbt_forms_tmp.visit_id,dbt_forms_tmp.visit_origination_creative,dbt_forms_tmp.visit_origination_name,dbt_forms_tmp.visit_origination_placement,dbt_forms_tmp.visit_origination_tracking_code,dbt_forms_tmp.visit_origination_type,dbt_forms_tmp.visitor_id,dbt_forms_tmp.visitor_type
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :dbt_forms_tmp                   , dbt_forms, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..dbt_forms_tmp                   ;
    quit;
    %put ######## Staging table: dbt_forms_tmp                    Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..dbt_forms;
      drop table work.dbt_forms;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..dbt_goals_tmp                   ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=dbt_goals, table_keys=%str(goal_reached_dttm,session_id), out_table=work.dbt_goals);
 data &tmplib..dbt_goals_tmp                   ;
     set work.dbt_goals;
  if goal_reached_dttm ne . then goal_reached_dttm = tzoneu2s(goal_reached_dttm,&timeZone_Value.);if goal_reached_dttm_tz ne . then goal_reached_dttm_tz = tzoneu2s(goal_reached_dttm_tz,&timeZone_Value.);if session_complete_load_dttm ne . then session_complete_load_dttm = tzoneu2s(session_complete_load_dttm,&timeZone_Value.);if session_start_dttm ne . then session_start_dttm = tzoneu2s(session_start_dttm,&timeZone_Value.);if session_start_dttm_tz ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz,&timeZone_Value.) ;
  if session_id='' then session_id='-';
 run;
 %ErrCheck (Failed to Append Data to :dbt_goals_tmp                   , dbt_goals);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..dbt_goals using &tmpdbschema..dbt_goals_tmp                   
         on (dbt_goals.goal_reached_dttm=dbt_goals_tmp.goal_reached_dttm and dbt_goals.session_id=dbt_goals_tmp.session_id)
        when matched then  
        update set dbt_goals.bouncer = dbt_goals_tmp.bouncer , dbt_goals.cu_customer_id = dbt_goals_tmp.cu_customer_id , dbt_goals.device_name = dbt_goals_tmp.device_name , dbt_goals.device_type = dbt_goals_tmp.device_type , dbt_goals.goal_group_name = dbt_goals_tmp.goal_group_name , dbt_goals.goal_name = dbt_goals_tmp.goal_name , dbt_goals.goal_reached_dttm_tz = dbt_goals_tmp.goal_reached_dttm_tz , dbt_goals.goal_revenue = dbt_goals_tmp.goal_revenue , dbt_goals.goals = dbt_goals_tmp.goals , dbt_goals.session_complete_load_dttm = dbt_goals_tmp.session_complete_load_dttm , dbt_goals.session_start_dttm = dbt_goals_tmp.session_start_dttm , dbt_goals.session_start_dttm_tz = dbt_goals_tmp.session_start_dttm_tz , dbt_goals.visit_id = dbt_goals_tmp.visit_id , dbt_goals.visit_origination_creative = dbt_goals_tmp.visit_origination_creative , dbt_goals.visit_origination_name = dbt_goals_tmp.visit_origination_name , dbt_goals.visit_origination_placement = dbt_goals_tmp.visit_origination_placement , dbt_goals.visit_origination_tracking_code = dbt_goals_tmp.visit_origination_tracking_code , dbt_goals.visit_origination_type = dbt_goals_tmp.visit_origination_type , dbt_goals.visitor_id = dbt_goals_tmp.visitor_id , dbt_goals.visitor_type = dbt_goals_tmp.visitor_type , dbt_goals.visits = dbt_goals_tmp.visits
        when not matched then insert ( 
        bouncer,cu_customer_id,device_name,device_type,goal_group_name,goal_name,goal_reached_dttm,goal_reached_dttm_tz,goal_revenue,goals,session_complete_load_dttm,session_id,session_start_dttm,session_start_dttm_tz,visit_id,visit_origination_creative,visit_origination_name,visit_origination_placement,visit_origination_tracking_code,visit_origination_type,visitor_id,visitor_type,visits
         ) values ( 
        dbt_goals_tmp.bouncer,dbt_goals_tmp.cu_customer_id,dbt_goals_tmp.device_name,dbt_goals_tmp.device_type,dbt_goals_tmp.goal_group_name,dbt_goals_tmp.goal_name,dbt_goals_tmp.goal_reached_dttm,dbt_goals_tmp.goal_reached_dttm_tz,dbt_goals_tmp.goal_revenue,dbt_goals_tmp.goals,dbt_goals_tmp.session_complete_load_dttm,dbt_goals_tmp.session_id,dbt_goals_tmp.session_start_dttm,dbt_goals_tmp.session_start_dttm_tz,dbt_goals_tmp.visit_id,dbt_goals_tmp.visit_origination_creative,dbt_goals_tmp.visit_origination_name,dbt_goals_tmp.visit_origination_placement,dbt_goals_tmp.visit_origination_tracking_code,dbt_goals_tmp.visit_origination_type,dbt_goals_tmp.visitor_id,dbt_goals_tmp.visitor_type,dbt_goals_tmp.visits
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :dbt_goals_tmp                   , dbt_goals, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..dbt_goals_tmp                   ;
    quit;
    %put ######## Staging table: dbt_goals_tmp                    Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..dbt_goals;
      drop table work.dbt_goals;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..dbt_media_consumption_tmp       ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=dbt_media_consumption, table_keys=%str(detail_id,interactions_count,maximum_progress,media_completion_rate,media_section,visit_id), out_table=work.dbt_media_consumption);
 data &tmplib..dbt_media_consumption_tmp       ;
     set work.dbt_media_consumption;
  if media_start_dttm ne . then media_start_dttm = tzoneu2s(media_start_dttm,&timeZone_Value.);if media_start_dttm_tz ne . then media_start_dttm_tz = tzoneu2s(media_start_dttm_tz,&timeZone_Value.);if session_complete_load_dttm ne . then session_complete_load_dttm = tzoneu2s(session_complete_load_dttm,&timeZone_Value.);if session_start_dttm ne . then session_start_dttm = tzoneu2s(session_start_dttm,&timeZone_Value.);if session_start_dttm_tz ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz,&timeZone_Value.) ;
  if detail_id='' then detail_id='-'; if media_completion_rate='' then media_completion_rate='-'; if media_section='' then media_section='-'; if visit_id='' then visit_id='-';
 run;
 %ErrCheck (Failed to Append Data to :dbt_media_consumption_tmp       , dbt_media_consumption);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..dbt_media_consumption using &tmpdbschema..dbt_media_consumption_tmp       
         on (dbt_media_consumption.detail_id=dbt_media_consumption_tmp.detail_id and dbt_media_consumption.interactions_count=dbt_media_consumption_tmp.interactions_count and dbt_media_consumption.maximum_progress=dbt_media_consumption_tmp.maximum_progress and dbt_media_consumption.media_completion_rate=dbt_media_consumption_tmp.media_completion_rate and dbt_media_consumption.media_section=dbt_media_consumption_tmp.media_section and dbt_media_consumption.visit_id=dbt_media_consumption_tmp.visit_id)
        when matched then  
        update set dbt_media_consumption.bouncer = dbt_media_consumption_tmp.bouncer , dbt_media_consumption.content_viewed = dbt_media_consumption_tmp.content_viewed , dbt_media_consumption.counter = dbt_media_consumption_tmp.counter , dbt_media_consumption.cu_customer_id = dbt_media_consumption_tmp.cu_customer_id , dbt_media_consumption.device_name = dbt_media_consumption_tmp.device_name , dbt_media_consumption.device_type = dbt_media_consumption_tmp.device_type , dbt_media_consumption.duration = dbt_media_consumption_tmp.duration , dbt_media_consumption.media_name = dbt_media_consumption_tmp.media_name , dbt_media_consumption.media_section_view = dbt_media_consumption_tmp.media_section_view , dbt_media_consumption.media_start_dttm = dbt_media_consumption_tmp.media_start_dttm , dbt_media_consumption.media_start_dttm_tz = dbt_media_consumption_tmp.media_start_dttm_tz , dbt_media_consumption.media_uri_txt = dbt_media_consumption_tmp.media_uri_txt , dbt_media_consumption.session_complete_load_dttm = dbt_media_consumption_tmp.session_complete_load_dttm , dbt_media_consumption.session_id = dbt_media_consumption_tmp.session_id , dbt_media_consumption.session_start_dttm = dbt_media_consumption_tmp.session_start_dttm , dbt_media_consumption.session_start_dttm_tz = dbt_media_consumption_tmp.session_start_dttm_tz , dbt_media_consumption.time_viewing = dbt_media_consumption_tmp.time_viewing , dbt_media_consumption.views = dbt_media_consumption_tmp.views , dbt_media_consumption.views_completed = dbt_media_consumption_tmp.views_completed , dbt_media_consumption.views_started = dbt_media_consumption_tmp.views_started , dbt_media_consumption.visit_origination_creative = dbt_media_consumption_tmp.visit_origination_creative , dbt_media_consumption.visit_origination_name = dbt_media_consumption_tmp.visit_origination_name , dbt_media_consumption.visit_origination_placement = dbt_media_consumption_tmp.visit_origination_placement , dbt_media_consumption.visit_origination_tracking_code = dbt_media_consumption_tmp.visit_origination_tracking_code , dbt_media_consumption.visit_origination_type = dbt_media_consumption_tmp.visit_origination_type , dbt_media_consumption.visitor_id = dbt_media_consumption_tmp.visitor_id , dbt_media_consumption.visitor_type = dbt_media_consumption_tmp.visitor_type
        when not matched then insert ( 
        bouncer,content_viewed,counter,cu_customer_id,detail_id,device_name,device_type,duration,interactions_count,maximum_progress,media_completion_rate,media_name,media_section,media_section_view,media_start_dttm,media_start_dttm_tz,media_uri_txt,session_complete_load_dttm,session_id,session_start_dttm,session_start_dttm_tz,time_viewing,views,views_completed,views_started,visit_id,visit_origination_creative,visit_origination_name,visit_origination_placement,visit_origination_tracking_code,visit_origination_type,visitor_id,visitor_type
         ) values ( 
        dbt_media_consumption_tmp.bouncer,dbt_media_consumption_tmp.content_viewed,dbt_media_consumption_tmp.counter,dbt_media_consumption_tmp.cu_customer_id,dbt_media_consumption_tmp.detail_id,dbt_media_consumption_tmp.device_name,dbt_media_consumption_tmp.device_type,dbt_media_consumption_tmp.duration,dbt_media_consumption_tmp.interactions_count,dbt_media_consumption_tmp.maximum_progress,dbt_media_consumption_tmp.media_completion_rate,dbt_media_consumption_tmp.media_name,dbt_media_consumption_tmp.media_section,dbt_media_consumption_tmp.media_section_view,dbt_media_consumption_tmp.media_start_dttm,dbt_media_consumption_tmp.media_start_dttm_tz,dbt_media_consumption_tmp.media_uri_txt,dbt_media_consumption_tmp.session_complete_load_dttm,dbt_media_consumption_tmp.session_id,dbt_media_consumption_tmp.session_start_dttm,dbt_media_consumption_tmp.session_start_dttm_tz,dbt_media_consumption_tmp.time_viewing,dbt_media_consumption_tmp.views,dbt_media_consumption_tmp.views_completed,dbt_media_consumption_tmp.views_started,dbt_media_consumption_tmp.visit_id,dbt_media_consumption_tmp.visit_origination_creative,dbt_media_consumption_tmp.visit_origination_name,dbt_media_consumption_tmp.visit_origination_placement,dbt_media_consumption_tmp.visit_origination_tracking_code,dbt_media_consumption_tmp.visit_origination_type,dbt_media_consumption_tmp.visitor_id,dbt_media_consumption_tmp.visitor_type
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :dbt_media_consumption_tmp       , dbt_media_consumption, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..dbt_media_consumption_tmp       ;
    quit;
    %put ######## Staging table: dbt_media_consumption_tmp        Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..dbt_media_consumption;
      drop table work.dbt_media_consumption;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..dbt_promotions_tmp              ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=dbt_promotions, table_keys=%str(promotion_name,promotion_shown_dttm,session_id), out_table=work.dbt_promotions);
 data &tmplib..dbt_promotions_tmp              ;
     set work.dbt_promotions;
  if promotion_shown_dttm ne . then promotion_shown_dttm = tzoneu2s(promotion_shown_dttm,&timeZone_Value.);if promotion_shown_dttm_tz ne . then promotion_shown_dttm_tz = tzoneu2s(promotion_shown_dttm_tz,&timeZone_Value.);if session_complete_load_dttm ne . then session_complete_load_dttm = tzoneu2s(session_complete_load_dttm,&timeZone_Value.);if session_start_dttm ne . then session_start_dttm = tzoneu2s(session_start_dttm,&timeZone_Value.);if session_start_dttm_tz ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz,&timeZone_Value.) ;
  if promotion_name='' then promotion_name='-'; if session_id='' then session_id='-';
 run;
 %ErrCheck (Failed to Append Data to :dbt_promotions_tmp              , dbt_promotions);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..dbt_promotions using &tmpdbschema..dbt_promotions_tmp              
         on (dbt_promotions.promotion_name=dbt_promotions_tmp.promotion_name and dbt_promotions.promotion_shown_dttm=dbt_promotions_tmp.promotion_shown_dttm and dbt_promotions.session_id=dbt_promotions_tmp.session_id)
        when matched then  
        update set dbt_promotions.bouncer = dbt_promotions_tmp.bouncer , dbt_promotions.click_throughs = dbt_promotions_tmp.click_throughs , dbt_promotions.cu_customer_id = dbt_promotions_tmp.cu_customer_id , dbt_promotions.device_name = dbt_promotions_tmp.device_name , dbt_promotions.device_type = dbt_promotions_tmp.device_type , dbt_promotions.displays = dbt_promotions_tmp.displays , dbt_promotions.promotion_creative = dbt_promotions_tmp.promotion_creative , dbt_promotions.promotion_placement = dbt_promotions_tmp.promotion_placement , dbt_promotions.promotion_shown_dttm_tz = dbt_promotions_tmp.promotion_shown_dttm_tz , dbt_promotions.promotion_tracking_code = dbt_promotions_tmp.promotion_tracking_code , dbt_promotions.promotion_type = dbt_promotions_tmp.promotion_type , dbt_promotions.session_complete_load_dttm = dbt_promotions_tmp.session_complete_load_dttm , dbt_promotions.session_start_dttm = dbt_promotions_tmp.session_start_dttm , dbt_promotions.session_start_dttm_tz = dbt_promotions_tmp.session_start_dttm_tz , dbt_promotions.visit_id = dbt_promotions_tmp.visit_id , dbt_promotions.visit_origination_creative = dbt_promotions_tmp.visit_origination_creative , dbt_promotions.visit_origination_name = dbt_promotions_tmp.visit_origination_name , dbt_promotions.visit_origination_placement = dbt_promotions_tmp.visit_origination_placement , dbt_promotions.visit_origination_tracking_code = dbt_promotions_tmp.visit_origination_tracking_code , dbt_promotions.visit_origination_type = dbt_promotions_tmp.visit_origination_type , dbt_promotions.visitor_id = dbt_promotions_tmp.visitor_id , dbt_promotions.visitor_type = dbt_promotions_tmp.visitor_type
        when not matched then insert ( 
        bouncer,click_throughs,cu_customer_id,device_name,device_type,displays,promotion_creative,promotion_name,promotion_placement,promotion_shown_dttm,promotion_shown_dttm_tz,promotion_tracking_code,promotion_type,session_complete_load_dttm,session_id,session_start_dttm,session_start_dttm_tz,visit_id,visit_origination_creative,visit_origination_name,visit_origination_placement,visit_origination_tracking_code,visit_origination_type,visitor_id,visitor_type
         ) values ( 
        dbt_promotions_tmp.bouncer,dbt_promotions_tmp.click_throughs,dbt_promotions_tmp.cu_customer_id,dbt_promotions_tmp.device_name,dbt_promotions_tmp.device_type,dbt_promotions_tmp.displays,dbt_promotions_tmp.promotion_creative,dbt_promotions_tmp.promotion_name,dbt_promotions_tmp.promotion_placement,dbt_promotions_tmp.promotion_shown_dttm,dbt_promotions_tmp.promotion_shown_dttm_tz,dbt_promotions_tmp.promotion_tracking_code,dbt_promotions_tmp.promotion_type,dbt_promotions_tmp.session_complete_load_dttm,dbt_promotions_tmp.session_id,dbt_promotions_tmp.session_start_dttm,dbt_promotions_tmp.session_start_dttm_tz,dbt_promotions_tmp.visit_id,dbt_promotions_tmp.visit_origination_creative,dbt_promotions_tmp.visit_origination_name,dbt_promotions_tmp.visit_origination_placement,dbt_promotions_tmp.visit_origination_tracking_code,dbt_promotions_tmp.visit_origination_type,dbt_promotions_tmp.visitor_id,dbt_promotions_tmp.visitor_type
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :dbt_promotions_tmp              , dbt_promotions, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..dbt_promotions_tmp              ;
    quit;
    %put ######## Staging table: dbt_promotions_tmp               Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..dbt_promotions;
      drop table work.dbt_promotions;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..dbt_search_tmp                  ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=dbt_search, table_keys=%str(search_name,search_results_dttm,session_id), out_table=work.dbt_search);
 data &tmplib..dbt_search_tmp                  ;
     set work.dbt_search;
  if search_results_dttm ne . then search_results_dttm = tzoneu2s(search_results_dttm,&timeZone_Value.);if search_results_dttm_tz ne . then search_results_dttm_tz = tzoneu2s(search_results_dttm_tz,&timeZone_Value.);if session_complete_load_dttm ne . then session_complete_load_dttm = tzoneu2s(session_complete_load_dttm,&timeZone_Value.);if session_start_dttm ne . then session_start_dttm = tzoneu2s(session_start_dttm,&timeZone_Value.);if session_start_dttm_tz ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz,&timeZone_Value.) ;
  if search_name='' then search_name='-'; if session_id='' then session_id='-';
 run;
 %ErrCheck (Failed to Append Data to :dbt_search_tmp                  , dbt_search);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..dbt_search using &tmpdbschema..dbt_search_tmp                  
         on (dbt_search.search_name=dbt_search_tmp.search_name and dbt_search.search_results_dttm=dbt_search_tmp.search_results_dttm and dbt_search.session_id=dbt_search_tmp.session_id)
        when matched then  
        update set dbt_search.bouncer = dbt_search_tmp.bouncer , dbt_search.cu_customer_id = dbt_search_tmp.cu_customer_id , dbt_search.device_name = dbt_search_tmp.device_name , dbt_search.device_type = dbt_search_tmp.device_type , dbt_search.exit_pages = dbt_search_tmp.exit_pages , dbt_search.internal_search_term = dbt_search_tmp.internal_search_term , dbt_search.num_additional_searches = dbt_search_tmp.num_additional_searches , dbt_search.num_pages_viewed_afterwards = dbt_search_tmp.num_pages_viewed_afterwards , dbt_search.search_no_results_returned = dbt_search_tmp.search_no_results_returned , dbt_search.search_results_dttm_tz = dbt_search_tmp.search_results_dttm_tz , dbt_search.search_returned_results = dbt_search_tmp.search_returned_results , dbt_search.search_unknown_results = dbt_search_tmp.search_unknown_results , dbt_search.searches = dbt_search_tmp.searches , dbt_search.session_complete_load_dttm = dbt_search_tmp.session_complete_load_dttm , dbt_search.session_start_dttm = dbt_search_tmp.session_start_dttm , dbt_search.session_start_dttm_tz = dbt_search_tmp.session_start_dttm_tz , dbt_search.visit_id = dbt_search_tmp.visit_id , dbt_search.visit_origination_creative = dbt_search_tmp.visit_origination_creative , dbt_search.visit_origination_name = dbt_search_tmp.visit_origination_name , dbt_search.visit_origination_placement = dbt_search_tmp.visit_origination_placement , dbt_search.visit_origination_tracking_code = dbt_search_tmp.visit_origination_tracking_code , dbt_search.visit_origination_type = dbt_search_tmp.visit_origination_type , dbt_search.visitor_id = dbt_search_tmp.visitor_id , dbt_search.visitor_type = dbt_search_tmp.visitor_type , dbt_search.visits = dbt_search_tmp.visits
        when not matched then insert ( 
        bouncer,cu_customer_id,device_name,device_type,exit_pages,internal_search_term,num_additional_searches,num_pages_viewed_afterwards,search_name,search_no_results_returned,search_results_dttm,search_results_dttm_tz,search_returned_results,search_unknown_results,searches,session_complete_load_dttm,session_id,session_start_dttm,session_start_dttm_tz,visit_id,visit_origination_creative,visit_origination_name,visit_origination_placement,visit_origination_tracking_code,visit_origination_type,visitor_id,visitor_type,visits
         ) values ( 
        dbt_search_tmp.bouncer,dbt_search_tmp.cu_customer_id,dbt_search_tmp.device_name,dbt_search_tmp.device_type,dbt_search_tmp.exit_pages,dbt_search_tmp.internal_search_term,dbt_search_tmp.num_additional_searches,dbt_search_tmp.num_pages_viewed_afterwards,dbt_search_tmp.search_name,dbt_search_tmp.search_no_results_returned,dbt_search_tmp.search_results_dttm,dbt_search_tmp.search_results_dttm_tz,dbt_search_tmp.search_returned_results,dbt_search_tmp.search_unknown_results,dbt_search_tmp.searches,dbt_search_tmp.session_complete_load_dttm,dbt_search_tmp.session_id,dbt_search_tmp.session_start_dttm,dbt_search_tmp.session_start_dttm_tz,dbt_search_tmp.visit_id,dbt_search_tmp.visit_origination_creative,dbt_search_tmp.visit_origination_name,dbt_search_tmp.visit_origination_placement,dbt_search_tmp.visit_origination_tracking_code,dbt_search_tmp.visit_origination_type,dbt_search_tmp.visitor_id,dbt_search_tmp.visitor_type,dbt_search_tmp.visits
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :dbt_search_tmp                  , dbt_search, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..dbt_search_tmp                  ;
    quit;
    %put ######## Staging table: dbt_search_tmp                   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..dbt_search;
      drop table work.dbt_search;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..direct_contact_tmp              ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=direct_contact, table_keys=%str(event_id), out_table=work.direct_contact);
 data &tmplib..direct_contact_tmp              ;
     set work.direct_contact;
  if direct_contact_dttm ne . then direct_contact_dttm = tzoneu2s(direct_contact_dttm,&timeZone_Value.);if direct_contact_dttm_tz ne . then direct_contact_dttm_tz = tzoneu2s(direct_contact_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :direct_contact_tmp              , direct_contact);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..direct_contact using &tmpdbschema..direct_contact_tmp              
         on (direct_contact.event_id=direct_contact_tmp.event_id)
        when matched then  
        update set direct_contact.channel_nm = direct_contact_tmp.channel_nm , direct_contact.channel_user_id = direct_contact_tmp.channel_user_id , direct_contact.context_type_nm = direct_contact_tmp.context_type_nm , direct_contact.context_val = direct_contact_tmp.context_val , direct_contact.control_active_flg = direct_contact_tmp.control_active_flg , direct_contact.control_group_flg = direct_contact_tmp.control_group_flg , direct_contact.direct_contact_dttm = direct_contact_tmp.direct_contact_dttm , direct_contact.direct_contact_dttm_tz = direct_contact_tmp.direct_contact_dttm_tz , direct_contact.event_designed_id = direct_contact_tmp.event_designed_id , direct_contact.event_nm = direct_contact_tmp.event_nm , direct_contact.identity_id = direct_contact_tmp.identity_id , direct_contact.identity_type_nm = direct_contact_tmp.identity_type_nm , direct_contact.load_dttm = direct_contact_tmp.load_dttm , direct_contact.message_id = direct_contact_tmp.message_id , direct_contact.occurrence_id = direct_contact_tmp.occurrence_id , direct_contact.properties_map_doc = direct_contact_tmp.properties_map_doc , direct_contact.response_tracking_cd = direct_contact_tmp.response_tracking_cd , direct_contact.segment_id = direct_contact_tmp.segment_id , direct_contact.task_id = direct_contact_tmp.task_id , direct_contact.task_version_id = direct_contact_tmp.task_version_id
        when not matched then insert ( 
        channel_nm,channel_user_id,context_type_nm,context_val,control_active_flg,control_group_flg,direct_contact_dttm,direct_contact_dttm_tz,event_designed_id,event_id,event_nm,identity_id,identity_type_nm,load_dttm,message_id,occurrence_id,properties_map_doc,response_tracking_cd,segment_id,task_id,task_version_id
         ) values ( 
        direct_contact_tmp.channel_nm,direct_contact_tmp.channel_user_id,direct_contact_tmp.context_type_nm,direct_contact_tmp.context_val,direct_contact_tmp.control_active_flg,direct_contact_tmp.control_group_flg,direct_contact_tmp.direct_contact_dttm,direct_contact_tmp.direct_contact_dttm_tz,direct_contact_tmp.event_designed_id,direct_contact_tmp.event_id,direct_contact_tmp.event_nm,direct_contact_tmp.identity_id,direct_contact_tmp.identity_type_nm,direct_contact_tmp.load_dttm,direct_contact_tmp.message_id,direct_contact_tmp.occurrence_id,direct_contact_tmp.properties_map_doc,direct_contact_tmp.response_tracking_cd,direct_contact_tmp.segment_id,direct_contact_tmp.task_id,direct_contact_tmp.task_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :direct_contact_tmp              , direct_contact, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..direct_contact_tmp              ;
    quit;
    %put ######## Staging table: direct_contact_tmp               Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..direct_contact;
      drop table work.direct_contact;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..document_details_tmp            ;
      quit;
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
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..document_details using &tmpdbschema..document_details_tmp            
         on (document_details.Hashed_pk_col = document_details_tmp.Hashed_pk_col)
        when matched then  
        update set document_details.alt_txt = document_details_tmp.alt_txt , document_details.detail_id_hex = document_details_tmp.detail_id_hex , document_details.event_id = document_details_tmp.event_id , document_details.event_key_cd = document_details_tmp.event_key_cd , document_details.event_source_cd = document_details_tmp.event_source_cd , document_details.identity_id = document_details_tmp.identity_id , document_details.link_event_dttm_tz = document_details_tmp.link_event_dttm_tz , document_details.link_id = document_details_tmp.link_id , document_details.link_name = document_details_tmp.link_name , document_details.link_selector_path = document_details_tmp.link_selector_path , document_details.load_dttm = document_details_tmp.load_dttm , document_details.session_id = document_details_tmp.session_id , document_details.session_id_hex = document_details_tmp.session_id_hex , document_details.visit_id = document_details_tmp.visit_id , document_details.visit_id_hex = document_details_tmp.visit_id_hex
        when not matched then insert ( 
        alt_txt,detail_id,detail_id_hex,event_id,event_key_cd,event_source_cd,identity_id,link_event_dttm,link_event_dttm_tz,link_id,link_name,link_selector_path,load_dttm,session_id,session_id_hex,uri_txt,visit_id,visit_id_hex
        ,Hashed_pk_col ) values ( 
        document_details_tmp.alt_txt,document_details_tmp.detail_id,document_details_tmp.detail_id_hex,document_details_tmp.event_id,document_details_tmp.event_key_cd,document_details_tmp.event_source_cd,document_details_tmp.identity_id,document_details_tmp.link_event_dttm,document_details_tmp.link_event_dttm_tz,document_details_tmp.link_id,document_details_tmp.link_name,document_details_tmp.link_selector_path,document_details_tmp.load_dttm,document_details_tmp.session_id,document_details_tmp.session_id_hex,document_details_tmp.uri_txt,document_details_tmp.visit_id,document_details_tmp.visit_id_hex,document_details_tmp.Hashed_pk_col
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :document_details_tmp            , document_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..document_details_tmp            ;
    quit;
    %put ######## Staging table: document_details_tmp             Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..document_details;
      drop table work.document_details;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..email_bounce_tmp                ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=email_bounce, table_keys=%str(event_id), out_table=work.email_bounce);
 data &tmplib..email_bounce_tmp                ;
     set work.email_bounce;
  if email_bounce_dttm ne . then email_bounce_dttm = tzoneu2s(email_bounce_dttm,&timeZone_Value.);if email_bounce_dttm_tz ne . then email_bounce_dttm_tz = tzoneu2s(email_bounce_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :email_bounce_tmp                , email_bounce);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..email_bounce using &tmpdbschema..email_bounce_tmp                
         on (email_bounce.event_id=email_bounce_tmp.event_id)
        when matched then  
        update set email_bounce.analysis_group_id = email_bounce_tmp.analysis_group_id , email_bounce.aud_occurrence_id = email_bounce_tmp.aud_occurrence_id , email_bounce.audience_id = email_bounce_tmp.audience_id , email_bounce.bounce_class_cd = email_bounce_tmp.bounce_class_cd , email_bounce.channel_user_id = email_bounce_tmp.channel_user_id , email_bounce.context_type_nm = email_bounce_tmp.context_type_nm , email_bounce.context_val = email_bounce_tmp.context_val , email_bounce.email_bounce_dttm = email_bounce_tmp.email_bounce_dttm , email_bounce.email_bounce_dttm_tz = email_bounce_tmp.email_bounce_dttm_tz , email_bounce.event_designed_id = email_bounce_tmp.event_designed_id , email_bounce.event_nm = email_bounce_tmp.event_nm , email_bounce.identity_id = email_bounce_tmp.identity_id , email_bounce.imprint_id = email_bounce_tmp.imprint_id , email_bounce.journey_id = email_bounce_tmp.journey_id , email_bounce.journey_occurrence_id = email_bounce_tmp.journey_occurrence_id , email_bounce.load_dttm = email_bounce_tmp.load_dttm , email_bounce.occurrence_id = email_bounce_tmp.occurrence_id , email_bounce.program_id = email_bounce_tmp.program_id , email_bounce.properties_map_doc = email_bounce_tmp.properties_map_doc , email_bounce.raw_reason_txt = email_bounce_tmp.raw_reason_txt , email_bounce.reason_txt = email_bounce_tmp.reason_txt , email_bounce.recipient_domain_nm = email_bounce_tmp.recipient_domain_nm , email_bounce.response_tracking_cd = email_bounce_tmp.response_tracking_cd , email_bounce.segment_id = email_bounce_tmp.segment_id , email_bounce.segment_version_id = email_bounce_tmp.segment_version_id , email_bounce.subject_line_txt = email_bounce_tmp.subject_line_txt , email_bounce.task_id = email_bounce_tmp.task_id , email_bounce.task_version_id = email_bounce_tmp.task_version_id , email_bounce.test_flg = email_bounce_tmp.test_flg
        when not matched then insert ( 
        analysis_group_id,aud_occurrence_id,audience_id,bounce_class_cd,channel_user_id,context_type_nm,context_val,email_bounce_dttm,email_bounce_dttm_tz,event_designed_id,event_id,event_nm,identity_id,imprint_id,journey_id,journey_occurrence_id,load_dttm,occurrence_id,program_id,properties_map_doc,raw_reason_txt,reason_txt,recipient_domain_nm,response_tracking_cd,segment_id,segment_version_id,subject_line_txt,task_id,task_version_id,test_flg
         ) values ( 
        email_bounce_tmp.analysis_group_id,email_bounce_tmp.aud_occurrence_id,email_bounce_tmp.audience_id,email_bounce_tmp.bounce_class_cd,email_bounce_tmp.channel_user_id,email_bounce_tmp.context_type_nm,email_bounce_tmp.context_val,email_bounce_tmp.email_bounce_dttm,email_bounce_tmp.email_bounce_dttm_tz,email_bounce_tmp.event_designed_id,email_bounce_tmp.event_id,email_bounce_tmp.event_nm,email_bounce_tmp.identity_id,email_bounce_tmp.imprint_id,email_bounce_tmp.journey_id,email_bounce_tmp.journey_occurrence_id,email_bounce_tmp.load_dttm,email_bounce_tmp.occurrence_id,email_bounce_tmp.program_id,email_bounce_tmp.properties_map_doc,email_bounce_tmp.raw_reason_txt,email_bounce_tmp.reason_txt,email_bounce_tmp.recipient_domain_nm,email_bounce_tmp.response_tracking_cd,email_bounce_tmp.segment_id,email_bounce_tmp.segment_version_id,email_bounce_tmp.subject_line_txt,email_bounce_tmp.task_id,email_bounce_tmp.task_version_id,email_bounce_tmp.test_flg
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :email_bounce_tmp                , email_bounce, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..email_bounce_tmp                ;
    quit;
    %put ######## Staging table: email_bounce_tmp                 Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..email_bounce;
      drop table work.email_bounce;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..email_click_tmp                 ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=email_click, table_keys=%str(event_id), out_table=work.email_click);
 data &tmplib..email_click_tmp                 ;
     set work.email_click;
  if email_click_dttm ne . then email_click_dttm = tzoneu2s(email_click_dttm,&timeZone_Value.);if email_click_dttm_tz ne . then email_click_dttm_tz = tzoneu2s(email_click_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :email_click_tmp                 , email_click);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..email_click using &tmpdbschema..email_click_tmp                 
         on (email_click.event_id=email_click_tmp.event_id)
        when matched then  
        update set email_click.agent_family_nm = email_click_tmp.agent_family_nm , email_click.analysis_group_id = email_click_tmp.analysis_group_id , email_click.aud_occurrence_id = email_click_tmp.aud_occurrence_id , email_click.audience_id = email_click_tmp.audience_id , email_click.channel_user_id = email_click_tmp.channel_user_id , email_click.click_tracking_flg = email_click_tmp.click_tracking_flg , email_click.context_type_nm = email_click_tmp.context_type_nm , email_click.context_val = email_click_tmp.context_val , email_click.device_nm = email_click_tmp.device_nm , email_click.email_click_dttm = email_click_tmp.email_click_dttm , email_click.email_click_dttm_tz = email_click_tmp.email_click_dttm_tz , email_click.event_designed_id = email_click_tmp.event_designed_id , email_click.event_nm = email_click_tmp.event_nm , email_click.identity_id = email_click_tmp.identity_id , email_click.imprint_id = email_click_tmp.imprint_id , email_click.is_mobile_flg = email_click_tmp.is_mobile_flg , email_click.journey_id = email_click_tmp.journey_id , email_click.journey_occurrence_id = email_click_tmp.journey_occurrence_id , email_click.link_tracking_group_txt = email_click_tmp.link_tracking_group_txt , email_click.link_tracking_id = email_click_tmp.link_tracking_id , email_click.link_tracking_label_txt = email_click_tmp.link_tracking_label_txt , email_click.load_dttm = email_click_tmp.load_dttm , email_click.mailbox_provider_nm = email_click_tmp.mailbox_provider_nm , email_click.manufacturer_nm = email_click_tmp.manufacturer_nm , email_click.occurrence_id = email_click_tmp.occurrence_id , email_click.open_tracking_flg = email_click_tmp.open_tracking_flg , email_click.platform_desc = email_click_tmp.platform_desc , email_click.platform_version = email_click_tmp.platform_version , email_click.program_id = email_click_tmp.program_id , email_click.properties_map_doc = email_click_tmp.properties_map_doc , email_click.recipient_domain_nm = email_click_tmp.recipient_domain_nm , email_click.response_tracking_cd = email_click_tmp.response_tracking_cd , email_click.segment_id = email_click_tmp.segment_id , email_click.segment_version_id = email_click_tmp.segment_version_id , email_click.subject_line_txt = email_click_tmp.subject_line_txt , email_click.task_id = email_click_tmp.task_id , email_click.task_version_id = email_click_tmp.task_version_id , email_click.test_flg = email_click_tmp.test_flg , email_click.uri_txt = email_click_tmp.uri_txt , email_click.user_agent_nm = email_click_tmp.user_agent_nm
        when not matched then insert ( 
        agent_family_nm,analysis_group_id,aud_occurrence_id,audience_id,channel_user_id,click_tracking_flg,context_type_nm,context_val,device_nm,email_click_dttm,email_click_dttm_tz,event_designed_id,event_id,event_nm,identity_id,imprint_id,is_mobile_flg,journey_id,journey_occurrence_id,link_tracking_group_txt,link_tracking_id,link_tracking_label_txt,load_dttm,mailbox_provider_nm,manufacturer_nm,occurrence_id,open_tracking_flg,platform_desc,platform_version,program_id,properties_map_doc,recipient_domain_nm,response_tracking_cd,segment_id,segment_version_id,subject_line_txt,task_id,task_version_id,test_flg,uri_txt,user_agent_nm
         ) values ( 
        email_click_tmp.agent_family_nm,email_click_tmp.analysis_group_id,email_click_tmp.aud_occurrence_id,email_click_tmp.audience_id,email_click_tmp.channel_user_id,email_click_tmp.click_tracking_flg,email_click_tmp.context_type_nm,email_click_tmp.context_val,email_click_tmp.device_nm,email_click_tmp.email_click_dttm,email_click_tmp.email_click_dttm_tz,email_click_tmp.event_designed_id,email_click_tmp.event_id,email_click_tmp.event_nm,email_click_tmp.identity_id,email_click_tmp.imprint_id,email_click_tmp.is_mobile_flg,email_click_tmp.journey_id,email_click_tmp.journey_occurrence_id,email_click_tmp.link_tracking_group_txt,email_click_tmp.link_tracking_id,email_click_tmp.link_tracking_label_txt,email_click_tmp.load_dttm,email_click_tmp.mailbox_provider_nm,email_click_tmp.manufacturer_nm,email_click_tmp.occurrence_id,email_click_tmp.open_tracking_flg,email_click_tmp.platform_desc,email_click_tmp.platform_version,email_click_tmp.program_id,email_click_tmp.properties_map_doc,email_click_tmp.recipient_domain_nm,email_click_tmp.response_tracking_cd,email_click_tmp.segment_id,email_click_tmp.segment_version_id,email_click_tmp.subject_line_txt,email_click_tmp.task_id,email_click_tmp.task_version_id,email_click_tmp.test_flg,email_click_tmp.uri_txt,email_click_tmp.user_agent_nm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :email_click_tmp                 , email_click, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..email_click_tmp                 ;
    quit;
    %put ######## Staging table: email_click_tmp                  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..email_click;
      drop table work.email_click;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..email_complaint_tmp             ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=email_complaint, table_keys=%str(event_id), out_table=work.email_complaint);
 data &tmplib..email_complaint_tmp             ;
     set work.email_complaint;
  if email_complaint_dttm ne . then email_complaint_dttm = tzoneu2s(email_complaint_dttm,&timeZone_Value.);if email_complaint_dttm_tz ne . then email_complaint_dttm_tz = tzoneu2s(email_complaint_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :email_complaint_tmp             , email_complaint);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..email_complaint using &tmpdbschema..email_complaint_tmp             
         on (email_complaint.event_id=email_complaint_tmp.event_id)
        when matched then  
        update set email_complaint.analysis_group_id = email_complaint_tmp.analysis_group_id , email_complaint.aud_occurrence_id = email_complaint_tmp.aud_occurrence_id , email_complaint.audience_id = email_complaint_tmp.audience_id , email_complaint.channel_user_id = email_complaint_tmp.channel_user_id , email_complaint.context_type_nm = email_complaint_tmp.context_type_nm , email_complaint.context_val = email_complaint_tmp.context_val , email_complaint.email_complaint_dttm = email_complaint_tmp.email_complaint_dttm , email_complaint.email_complaint_dttm_tz = email_complaint_tmp.email_complaint_dttm_tz , email_complaint.event_designed_id = email_complaint_tmp.event_designed_id , email_complaint.event_nm = email_complaint_tmp.event_nm , email_complaint.identity_id = email_complaint_tmp.identity_id , email_complaint.imprint_id = email_complaint_tmp.imprint_id , email_complaint.journey_id = email_complaint_tmp.journey_id , email_complaint.journey_occurrence_id = email_complaint_tmp.journey_occurrence_id , email_complaint.load_dttm = email_complaint_tmp.load_dttm , email_complaint.occurrence_id = email_complaint_tmp.occurrence_id , email_complaint.program_id = email_complaint_tmp.program_id , email_complaint.properties_map_doc = email_complaint_tmp.properties_map_doc , email_complaint.recipient_domain_nm = email_complaint_tmp.recipient_domain_nm , email_complaint.response_tracking_cd = email_complaint_tmp.response_tracking_cd , email_complaint.segment_id = email_complaint_tmp.segment_id , email_complaint.segment_version_id = email_complaint_tmp.segment_version_id , email_complaint.subject_line_txt = email_complaint_tmp.subject_line_txt , email_complaint.task_id = email_complaint_tmp.task_id , email_complaint.task_version_id = email_complaint_tmp.task_version_id , email_complaint.test_flg = email_complaint_tmp.test_flg
        when not matched then insert ( 
        analysis_group_id,aud_occurrence_id,audience_id,channel_user_id,context_type_nm,context_val,email_complaint_dttm,email_complaint_dttm_tz,event_designed_id,event_id,event_nm,identity_id,imprint_id,journey_id,journey_occurrence_id,load_dttm,occurrence_id,program_id,properties_map_doc,recipient_domain_nm,response_tracking_cd,segment_id,segment_version_id,subject_line_txt,task_id,task_version_id,test_flg
         ) values ( 
        email_complaint_tmp.analysis_group_id,email_complaint_tmp.aud_occurrence_id,email_complaint_tmp.audience_id,email_complaint_tmp.channel_user_id,email_complaint_tmp.context_type_nm,email_complaint_tmp.context_val,email_complaint_tmp.email_complaint_dttm,email_complaint_tmp.email_complaint_dttm_tz,email_complaint_tmp.event_designed_id,email_complaint_tmp.event_id,email_complaint_tmp.event_nm,email_complaint_tmp.identity_id,email_complaint_tmp.imprint_id,email_complaint_tmp.journey_id,email_complaint_tmp.journey_occurrence_id,email_complaint_tmp.load_dttm,email_complaint_tmp.occurrence_id,email_complaint_tmp.program_id,email_complaint_tmp.properties_map_doc,email_complaint_tmp.recipient_domain_nm,email_complaint_tmp.response_tracking_cd,email_complaint_tmp.segment_id,email_complaint_tmp.segment_version_id,email_complaint_tmp.subject_line_txt,email_complaint_tmp.task_id,email_complaint_tmp.task_version_id,email_complaint_tmp.test_flg
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :email_complaint_tmp             , email_complaint, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..email_complaint_tmp             ;
    quit;
    %put ######## Staging table: email_complaint_tmp              Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..email_complaint;
      drop table work.email_complaint;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..email_open_tmp                  ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=email_open, table_keys=%str(event_id), out_table=work.email_open);
 data &tmplib..email_open_tmp                  ;
     set work.email_open;
  if email_open_dttm ne . then email_open_dttm = tzoneu2s(email_open_dttm,&timeZone_Value.);if email_open_dttm_tz ne . then email_open_dttm_tz = tzoneu2s(email_open_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :email_open_tmp                  , email_open);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..email_open using &tmpdbschema..email_open_tmp                  
         on (email_open.event_id=email_open_tmp.event_id)
        when matched then  
        update set email_open.agent_family_nm = email_open_tmp.agent_family_nm , email_open.analysis_group_id = email_open_tmp.analysis_group_id , email_open.aud_occurrence_id = email_open_tmp.aud_occurrence_id , email_open.audience_id = email_open_tmp.audience_id , email_open.channel_user_id = email_open_tmp.channel_user_id , email_open.click_tracking_flg = email_open_tmp.click_tracking_flg , email_open.context_type_nm = email_open_tmp.context_type_nm , email_open.context_val = email_open_tmp.context_val , email_open.device_nm = email_open_tmp.device_nm , email_open.email_open_dttm = email_open_tmp.email_open_dttm , email_open.email_open_dttm_tz = email_open_tmp.email_open_dttm_tz , email_open.event_designed_id = email_open_tmp.event_designed_id , email_open.event_nm = email_open_tmp.event_nm , email_open.identity_id = email_open_tmp.identity_id , email_open.imprint_id = email_open_tmp.imprint_id , email_open.is_mobile_flg = email_open_tmp.is_mobile_flg , email_open.journey_id = email_open_tmp.journey_id , email_open.journey_occurrence_id = email_open_tmp.journey_occurrence_id , email_open.load_dttm = email_open_tmp.load_dttm , email_open.mailbox_provider_nm = email_open_tmp.mailbox_provider_nm , email_open.manufacturer_nm = email_open_tmp.manufacturer_nm , email_open.occurrence_id = email_open_tmp.occurrence_id , email_open.open_tracking_flg = email_open_tmp.open_tracking_flg , email_open.platform_desc = email_open_tmp.platform_desc , email_open.platform_version = email_open_tmp.platform_version , email_open.prefetched_flg = email_open_tmp.prefetched_flg , email_open.program_id = email_open_tmp.program_id , email_open.properties_map_doc = email_open_tmp.properties_map_doc , email_open.recipient_domain_nm = email_open_tmp.recipient_domain_nm , email_open.response_tracking_cd = email_open_tmp.response_tracking_cd , email_open.segment_id = email_open_tmp.segment_id , email_open.segment_version_id = email_open_tmp.segment_version_id , email_open.subject_line_txt = email_open_tmp.subject_line_txt , email_open.task_id = email_open_tmp.task_id , email_open.task_version_id = email_open_tmp.task_version_id , email_open.test_flg = email_open_tmp.test_flg , email_open.user_agent_nm = email_open_tmp.user_agent_nm
        when not matched then insert ( 
        agent_family_nm,analysis_group_id,aud_occurrence_id,audience_id,channel_user_id,click_tracking_flg,context_type_nm,context_val,device_nm,email_open_dttm,email_open_dttm_tz,event_designed_id,event_id,event_nm,identity_id,imprint_id,is_mobile_flg,journey_id,journey_occurrence_id,load_dttm,mailbox_provider_nm,manufacturer_nm,occurrence_id,open_tracking_flg,platform_desc,platform_version,prefetched_flg,program_id,properties_map_doc,recipient_domain_nm,response_tracking_cd,segment_id,segment_version_id,subject_line_txt,task_id,task_version_id,test_flg,user_agent_nm
         ) values ( 
        email_open_tmp.agent_family_nm,email_open_tmp.analysis_group_id,email_open_tmp.aud_occurrence_id,email_open_tmp.audience_id,email_open_tmp.channel_user_id,email_open_tmp.click_tracking_flg,email_open_tmp.context_type_nm,email_open_tmp.context_val,email_open_tmp.device_nm,email_open_tmp.email_open_dttm,email_open_tmp.email_open_dttm_tz,email_open_tmp.event_designed_id,email_open_tmp.event_id,email_open_tmp.event_nm,email_open_tmp.identity_id,email_open_tmp.imprint_id,email_open_tmp.is_mobile_flg,email_open_tmp.journey_id,email_open_tmp.journey_occurrence_id,email_open_tmp.load_dttm,email_open_tmp.mailbox_provider_nm,email_open_tmp.manufacturer_nm,email_open_tmp.occurrence_id,email_open_tmp.open_tracking_flg,email_open_tmp.platform_desc,email_open_tmp.platform_version,email_open_tmp.prefetched_flg,email_open_tmp.program_id,email_open_tmp.properties_map_doc,email_open_tmp.recipient_domain_nm,email_open_tmp.response_tracking_cd,email_open_tmp.segment_id,email_open_tmp.segment_version_id,email_open_tmp.subject_line_txt,email_open_tmp.task_id,email_open_tmp.task_version_id,email_open_tmp.test_flg,email_open_tmp.user_agent_nm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :email_open_tmp                  , email_open, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..email_open_tmp                  ;
    quit;
    %put ######## Staging table: email_open_tmp                   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..email_open;
      drop table work.email_open;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..email_optout_tmp                ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=email_optout, table_keys=%str(event_id), out_table=work.email_optout);
 data &tmplib..email_optout_tmp                ;
     set work.email_optout;
  if email_optout_dttm ne . then email_optout_dttm = tzoneu2s(email_optout_dttm,&timeZone_Value.);if email_optout_dttm_tz ne . then email_optout_dttm_tz = tzoneu2s(email_optout_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :email_optout_tmp                , email_optout);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..email_optout using &tmpdbschema..email_optout_tmp                
         on (email_optout.event_id=email_optout_tmp.event_id)
        when matched then  
        update set email_optout.analysis_group_id = email_optout_tmp.analysis_group_id , email_optout.aud_occurrence_id = email_optout_tmp.aud_occurrence_id , email_optout.audience_id = email_optout_tmp.audience_id , email_optout.channel_user_id = email_optout_tmp.channel_user_id , email_optout.context_type_nm = email_optout_tmp.context_type_nm , email_optout.context_val = email_optout_tmp.context_val , email_optout.email_optout_dttm = email_optout_tmp.email_optout_dttm , email_optout.email_optout_dttm_tz = email_optout_tmp.email_optout_dttm_tz , email_optout.event_designed_id = email_optout_tmp.event_designed_id , email_optout.event_nm = email_optout_tmp.event_nm , email_optout.identity_id = email_optout_tmp.identity_id , email_optout.imprint_id = email_optout_tmp.imprint_id , email_optout.journey_id = email_optout_tmp.journey_id , email_optout.journey_occurrence_id = email_optout_tmp.journey_occurrence_id , email_optout.link_tracking_group_txt = email_optout_tmp.link_tracking_group_txt , email_optout.link_tracking_id = email_optout_tmp.link_tracking_id , email_optout.link_tracking_label_txt = email_optout_tmp.link_tracking_label_txt , email_optout.load_dttm = email_optout_tmp.load_dttm , email_optout.occurrence_id = email_optout_tmp.occurrence_id , email_optout.optout_type_nm = email_optout_tmp.optout_type_nm , email_optout.program_id = email_optout_tmp.program_id , email_optout.properties_map_doc = email_optout_tmp.properties_map_doc , email_optout.recipient_domain_nm = email_optout_tmp.recipient_domain_nm , email_optout.response_tracking_cd = email_optout_tmp.response_tracking_cd , email_optout.segment_id = email_optout_tmp.segment_id , email_optout.segment_version_id = email_optout_tmp.segment_version_id , email_optout.subject_line_txt = email_optout_tmp.subject_line_txt , email_optout.task_id = email_optout_tmp.task_id , email_optout.task_version_id = email_optout_tmp.task_version_id , email_optout.test_flg = email_optout_tmp.test_flg
        when not matched then insert ( 
        analysis_group_id,aud_occurrence_id,audience_id,channel_user_id,context_type_nm,context_val,email_optout_dttm,email_optout_dttm_tz,event_designed_id,event_id,event_nm,identity_id,imprint_id,journey_id,journey_occurrence_id,link_tracking_group_txt,link_tracking_id,link_tracking_label_txt,load_dttm,occurrence_id,optout_type_nm,program_id,properties_map_doc,recipient_domain_nm,response_tracking_cd,segment_id,segment_version_id,subject_line_txt,task_id,task_version_id,test_flg
         ) values ( 
        email_optout_tmp.analysis_group_id,email_optout_tmp.aud_occurrence_id,email_optout_tmp.audience_id,email_optout_tmp.channel_user_id,email_optout_tmp.context_type_nm,email_optout_tmp.context_val,email_optout_tmp.email_optout_dttm,email_optout_tmp.email_optout_dttm_tz,email_optout_tmp.event_designed_id,email_optout_tmp.event_id,email_optout_tmp.event_nm,email_optout_tmp.identity_id,email_optout_tmp.imprint_id,email_optout_tmp.journey_id,email_optout_tmp.journey_occurrence_id,email_optout_tmp.link_tracking_group_txt,email_optout_tmp.link_tracking_id,email_optout_tmp.link_tracking_label_txt,email_optout_tmp.load_dttm,email_optout_tmp.occurrence_id,email_optout_tmp.optout_type_nm,email_optout_tmp.program_id,email_optout_tmp.properties_map_doc,email_optout_tmp.recipient_domain_nm,email_optout_tmp.response_tracking_cd,email_optout_tmp.segment_id,email_optout_tmp.segment_version_id,email_optout_tmp.subject_line_txt,email_optout_tmp.task_id,email_optout_tmp.task_version_id,email_optout_tmp.test_flg
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :email_optout_tmp                , email_optout, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..email_optout_tmp                ;
    quit;
    %put ######## Staging table: email_optout_tmp                 Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..email_optout;
      drop table work.email_optout;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..email_optout_details_tmp        ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=email_optout_details, table_keys=%str(event_id), out_table=work.email_optout_details);
 data &tmplib..email_optout_details_tmp        ;
     set work.email_optout_details;
  if email_action_dttm ne . then email_action_dttm = tzoneu2s(email_action_dttm,&timeZone_Value.);if email_action_dttm_tz ne . then email_action_dttm_tz = tzoneu2s(email_action_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :email_optout_details_tmp        , email_optout_details);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..email_optout_details using &tmpdbschema..email_optout_details_tmp        
         on (email_optout_details.event_id=email_optout_details_tmp.event_id)
        when matched then  
        update set email_optout_details.analysis_group_id = email_optout_details_tmp.analysis_group_id , email_optout_details.aud_occurrence_id = email_optout_details_tmp.aud_occurrence_id , email_optout_details.audience_id = email_optout_details_tmp.audience_id , email_optout_details.context_type_nm = email_optout_details_tmp.context_type_nm , email_optout_details.context_val = email_optout_details_tmp.context_val , email_optout_details.email_action_dttm = email_optout_details_tmp.email_action_dttm , email_optout_details.email_action_dttm_tz = email_optout_details_tmp.email_action_dttm_tz , email_optout_details.email_address = email_optout_details_tmp.email_address , email_optout_details.event_designed_id = email_optout_details_tmp.event_designed_id , email_optout_details.event_nm = email_optout_details_tmp.event_nm , email_optout_details.identity_id = email_optout_details_tmp.identity_id , email_optout_details.imprint_id = email_optout_details_tmp.imprint_id , email_optout_details.journey_id = email_optout_details_tmp.journey_id , email_optout_details.journey_occurrence_id = email_optout_details_tmp.journey_occurrence_id , email_optout_details.load_dttm = email_optout_details_tmp.load_dttm , email_optout_details.occurrence_id = email_optout_details_tmp.occurrence_id , email_optout_details.optout_type_nm = email_optout_details_tmp.optout_type_nm , email_optout_details.program_id = email_optout_details_tmp.program_id , email_optout_details.properties_map_doc = email_optout_details_tmp.properties_map_doc , email_optout_details.recipient_domain_nm = email_optout_details_tmp.recipient_domain_nm , email_optout_details.response_tracking_cd = email_optout_details_tmp.response_tracking_cd , email_optout_details.segment_id = email_optout_details_tmp.segment_id , email_optout_details.segment_version_id = email_optout_details_tmp.segment_version_id , email_optout_details.subject_line_txt = email_optout_details_tmp.subject_line_txt , email_optout_details.task_id = email_optout_details_tmp.task_id , email_optout_details.task_version_id = email_optout_details_tmp.task_version_id , email_optout_details.test_flg = email_optout_details_tmp.test_flg
        when not matched then insert ( 
        analysis_group_id,aud_occurrence_id,audience_id,context_type_nm,context_val,email_action_dttm,email_action_dttm_tz,email_address,event_designed_id,event_id,event_nm,identity_id,imprint_id,journey_id,journey_occurrence_id,load_dttm,occurrence_id,optout_type_nm,program_id,properties_map_doc,recipient_domain_nm,response_tracking_cd,segment_id,segment_version_id,subject_line_txt,task_id,task_version_id,test_flg
         ) values ( 
        email_optout_details_tmp.analysis_group_id,email_optout_details_tmp.aud_occurrence_id,email_optout_details_tmp.audience_id,email_optout_details_tmp.context_type_nm,email_optout_details_tmp.context_val,email_optout_details_tmp.email_action_dttm,email_optout_details_tmp.email_action_dttm_tz,email_optout_details_tmp.email_address,email_optout_details_tmp.event_designed_id,email_optout_details_tmp.event_id,email_optout_details_tmp.event_nm,email_optout_details_tmp.identity_id,email_optout_details_tmp.imprint_id,email_optout_details_tmp.journey_id,email_optout_details_tmp.journey_occurrence_id,email_optout_details_tmp.load_dttm,email_optout_details_tmp.occurrence_id,email_optout_details_tmp.optout_type_nm,email_optout_details_tmp.program_id,email_optout_details_tmp.properties_map_doc,email_optout_details_tmp.recipient_domain_nm,email_optout_details_tmp.response_tracking_cd,email_optout_details_tmp.segment_id,email_optout_details_tmp.segment_version_id,email_optout_details_tmp.subject_line_txt,email_optout_details_tmp.task_id,email_optout_details_tmp.task_version_id,email_optout_details_tmp.test_flg
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :email_optout_details_tmp        , email_optout_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..email_optout_details_tmp        ;
    quit;
    %put ######## Staging table: email_optout_details_tmp         Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..email_optout_details;
      drop table work.email_optout_details;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..email_reply_tmp                 ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=email_reply, table_keys=%str(event_id), out_table=work.email_reply);
 data &tmplib..email_reply_tmp                 ;
     set work.email_reply;
  if email_reply_dttm ne . then email_reply_dttm = tzoneu2s(email_reply_dttm,&timeZone_Value.);if email_reply_dttm_tz ne . then email_reply_dttm_tz = tzoneu2s(email_reply_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :email_reply_tmp                 , email_reply);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..email_reply using &tmpdbschema..email_reply_tmp                 
         on (email_reply.event_id=email_reply_tmp.event_id)
        when matched then  
        update set email_reply.analysis_group_id = email_reply_tmp.analysis_group_id , email_reply.aud_occurrence_id = email_reply_tmp.aud_occurrence_id , email_reply.audience_id = email_reply_tmp.audience_id , email_reply.channel_user_id = email_reply_tmp.channel_user_id , email_reply.context_type_nm = email_reply_tmp.context_type_nm , email_reply.context_val = email_reply_tmp.context_val , email_reply.email_reply_dttm = email_reply_tmp.email_reply_dttm , email_reply.email_reply_dttm_tz = email_reply_tmp.email_reply_dttm_tz , email_reply.event_designed_id = email_reply_tmp.event_designed_id , email_reply.event_nm = email_reply_tmp.event_nm , email_reply.identity_id = email_reply_tmp.identity_id , email_reply.imprint_id = email_reply_tmp.imprint_id , email_reply.journey_id = email_reply_tmp.journey_id , email_reply.journey_occurrence_id = email_reply_tmp.journey_occurrence_id , email_reply.load_dttm = email_reply_tmp.load_dttm , email_reply.occurrence_id = email_reply_tmp.occurrence_id , email_reply.program_id = email_reply_tmp.program_id , email_reply.properties_map_doc = email_reply_tmp.properties_map_doc , email_reply.recipient_domain_nm = email_reply_tmp.recipient_domain_nm , email_reply.response_tracking_cd = email_reply_tmp.response_tracking_cd , email_reply.segment_id = email_reply_tmp.segment_id , email_reply.segment_version_id = email_reply_tmp.segment_version_id , email_reply.subject_line_txt = email_reply_tmp.subject_line_txt , email_reply.task_id = email_reply_tmp.task_id , email_reply.task_version_id = email_reply_tmp.task_version_id , email_reply.test_flg = email_reply_tmp.test_flg , email_reply.uri_txt = email_reply_tmp.uri_txt
        when not matched then insert ( 
        analysis_group_id,aud_occurrence_id,audience_id,channel_user_id,context_type_nm,context_val,email_reply_dttm,email_reply_dttm_tz,event_designed_id,event_id,event_nm,identity_id,imprint_id,journey_id,journey_occurrence_id,load_dttm,occurrence_id,program_id,properties_map_doc,recipient_domain_nm,response_tracking_cd,segment_id,segment_version_id,subject_line_txt,task_id,task_version_id,test_flg,uri_txt
         ) values ( 
        email_reply_tmp.analysis_group_id,email_reply_tmp.aud_occurrence_id,email_reply_tmp.audience_id,email_reply_tmp.channel_user_id,email_reply_tmp.context_type_nm,email_reply_tmp.context_val,email_reply_tmp.email_reply_dttm,email_reply_tmp.email_reply_dttm_tz,email_reply_tmp.event_designed_id,email_reply_tmp.event_id,email_reply_tmp.event_nm,email_reply_tmp.identity_id,email_reply_tmp.imprint_id,email_reply_tmp.journey_id,email_reply_tmp.journey_occurrence_id,email_reply_tmp.load_dttm,email_reply_tmp.occurrence_id,email_reply_tmp.program_id,email_reply_tmp.properties_map_doc,email_reply_tmp.recipient_domain_nm,email_reply_tmp.response_tracking_cd,email_reply_tmp.segment_id,email_reply_tmp.segment_version_id,email_reply_tmp.subject_line_txt,email_reply_tmp.task_id,email_reply_tmp.task_version_id,email_reply_tmp.test_flg,email_reply_tmp.uri_txt
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :email_reply_tmp                 , email_reply, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..email_reply_tmp                 ;
    quit;
    %put ######## Staging table: email_reply_tmp                  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..email_reply;
      drop table work.email_reply;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..email_send_tmp                  ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=email_send, table_keys=%str(event_id), out_table=work.email_send);
 data &tmplib..email_send_tmp                  ;
     set work.email_send;
  if email_send_dttm ne . then email_send_dttm = tzoneu2s(email_send_dttm,&timeZone_Value.);if email_send_dttm_tz ne . then email_send_dttm_tz = tzoneu2s(email_send_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :email_send_tmp                  , email_send);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..email_send using &tmpdbschema..email_send_tmp                  
         on (email_send.event_id=email_send_tmp.event_id)
        when matched then  
        update set email_send.analysis_group_id = email_send_tmp.analysis_group_id , email_send.aud_occurrence_id = email_send_tmp.aud_occurrence_id , email_send.audience_id = email_send_tmp.audience_id , email_send.channel_user_id = email_send_tmp.channel_user_id , email_send.context_type_nm = email_send_tmp.context_type_nm , email_send.context_val = email_send_tmp.context_val , email_send.email_send_dttm = email_send_tmp.email_send_dttm , email_send.email_send_dttm_tz = email_send_tmp.email_send_dttm_tz , email_send.event_designed_id = email_send_tmp.event_designed_id , email_send.event_nm = email_send_tmp.event_nm , email_send.identity_id = email_send_tmp.identity_id , email_send.imprint_id = email_send_tmp.imprint_id , email_send.imprint_url_txt = email_send_tmp.imprint_url_txt , email_send.journey_id = email_send_tmp.journey_id , email_send.journey_occurrence_id = email_send_tmp.journey_occurrence_id , email_send.load_dttm = email_send_tmp.load_dttm , email_send.occurrence_id = email_send_tmp.occurrence_id , email_send.program_id = email_send_tmp.program_id , email_send.properties_map_doc = email_send_tmp.properties_map_doc , email_send.recipient_domain_nm = email_send_tmp.recipient_domain_nm , email_send.response_tracking_cd = email_send_tmp.response_tracking_cd , email_send.segment_id = email_send_tmp.segment_id , email_send.segment_version_id = email_send_tmp.segment_version_id , email_send.subject_line_txt = email_send_tmp.subject_line_txt , email_send.task_id = email_send_tmp.task_id , email_send.task_version_id = email_send_tmp.task_version_id , email_send.test_flg = email_send_tmp.test_flg
        when not matched then insert ( 
        analysis_group_id,aud_occurrence_id,audience_id,channel_user_id,context_type_nm,context_val,email_send_dttm,email_send_dttm_tz,event_designed_id,event_id,event_nm,identity_id,imprint_id,imprint_url_txt,journey_id,journey_occurrence_id,load_dttm,occurrence_id,program_id,properties_map_doc,recipient_domain_nm,response_tracking_cd,segment_id,segment_version_id,subject_line_txt,task_id,task_version_id,test_flg
         ) values ( 
        email_send_tmp.analysis_group_id,email_send_tmp.aud_occurrence_id,email_send_tmp.audience_id,email_send_tmp.channel_user_id,email_send_tmp.context_type_nm,email_send_tmp.context_val,email_send_tmp.email_send_dttm,email_send_tmp.email_send_dttm_tz,email_send_tmp.event_designed_id,email_send_tmp.event_id,email_send_tmp.event_nm,email_send_tmp.identity_id,email_send_tmp.imprint_id,email_send_tmp.imprint_url_txt,email_send_tmp.journey_id,email_send_tmp.journey_occurrence_id,email_send_tmp.load_dttm,email_send_tmp.occurrence_id,email_send_tmp.program_id,email_send_tmp.properties_map_doc,email_send_tmp.recipient_domain_nm,email_send_tmp.response_tracking_cd,email_send_tmp.segment_id,email_send_tmp.segment_version_id,email_send_tmp.subject_line_txt,email_send_tmp.task_id,email_send_tmp.task_version_id,email_send_tmp.test_flg
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :email_send_tmp                  , email_send, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..email_send_tmp                  ;
    quit;
    %put ######## Staging table: email_send_tmp                   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..email_send;
      drop table work.email_send;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..email_view_tmp                  ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=email_view, table_keys=%str(event_id), out_table=work.email_view);
 data &tmplib..email_view_tmp                  ;
     set work.email_view;
  if email_view_dttm ne . then email_view_dttm = tzoneu2s(email_view_dttm,&timeZone_Value.);if email_view_dttm_tz ne . then email_view_dttm_tz = tzoneu2s(email_view_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :email_view_tmp                  , email_view);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..email_view using &tmpdbschema..email_view_tmp                  
         on (email_view.event_id=email_view_tmp.event_id)
        when matched then  
        update set email_view.analysis_group_id = email_view_tmp.analysis_group_id , email_view.aud_occurrence_id = email_view_tmp.aud_occurrence_id , email_view.audience_id = email_view_tmp.audience_id , email_view.channel_user_id = email_view_tmp.channel_user_id , email_view.context_type_nm = email_view_tmp.context_type_nm , email_view.context_val = email_view_tmp.context_val , email_view.email_view_dttm = email_view_tmp.email_view_dttm , email_view.email_view_dttm_tz = email_view_tmp.email_view_dttm_tz , email_view.event_designed_id = email_view_tmp.event_designed_id , email_view.event_nm = email_view_tmp.event_nm , email_view.identity_id = email_view_tmp.identity_id , email_view.imprint_id = email_view_tmp.imprint_id , email_view.link_tracking_group_txt = email_view_tmp.link_tracking_group_txt , email_view.link_tracking_id = email_view_tmp.link_tracking_id , email_view.link_tracking_label_txt = email_view_tmp.link_tracking_label_txt , email_view.load_dttm = email_view_tmp.load_dttm , email_view.occurrence_id = email_view_tmp.occurrence_id , email_view.program_id = email_view_tmp.program_id , email_view.properties_map_doc = email_view_tmp.properties_map_doc , email_view.recipient_domain_nm = email_view_tmp.recipient_domain_nm , email_view.response_tracking_cd = email_view_tmp.response_tracking_cd , email_view.segment_id = email_view_tmp.segment_id , email_view.segment_version_id = email_view_tmp.segment_version_id , email_view.subject_line_txt = email_view_tmp.subject_line_txt , email_view.task_id = email_view_tmp.task_id , email_view.task_version_id = email_view_tmp.task_version_id , email_view.test_flg = email_view_tmp.test_flg
        when not matched then insert ( 
        analysis_group_id,aud_occurrence_id,audience_id,channel_user_id,context_type_nm,context_val,email_view_dttm,email_view_dttm_tz,event_designed_id,event_id,event_nm,identity_id,imprint_id,link_tracking_group_txt,link_tracking_id,link_tracking_label_txt,load_dttm,occurrence_id,program_id,properties_map_doc,recipient_domain_nm,response_tracking_cd,segment_id,segment_version_id,subject_line_txt,task_id,task_version_id,test_flg
         ) values ( 
        email_view_tmp.analysis_group_id,email_view_tmp.aud_occurrence_id,email_view_tmp.audience_id,email_view_tmp.channel_user_id,email_view_tmp.context_type_nm,email_view_tmp.context_val,email_view_tmp.email_view_dttm,email_view_tmp.email_view_dttm_tz,email_view_tmp.event_designed_id,email_view_tmp.event_id,email_view_tmp.event_nm,email_view_tmp.identity_id,email_view_tmp.imprint_id,email_view_tmp.link_tracking_group_txt,email_view_tmp.link_tracking_id,email_view_tmp.link_tracking_label_txt,email_view_tmp.load_dttm,email_view_tmp.occurrence_id,email_view_tmp.program_id,email_view_tmp.properties_map_doc,email_view_tmp.recipient_domain_nm,email_view_tmp.response_tracking_cd,email_view_tmp.segment_id,email_view_tmp.segment_version_id,email_view_tmp.subject_line_txt,email_view_tmp.task_id,email_view_tmp.task_version_id,email_view_tmp.test_flg
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :email_view_tmp                  , email_view, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..email_view_tmp                  ;
    quit;
    %put ######## Staging table: email_view_tmp                   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..email_view;
      drop table work.email_view;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..external_event_tmp              ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=external_event, table_keys=%str(event_id), out_table=work.external_event);
 data &tmplib..external_event_tmp              ;
     set work.external_event;
  if external_event_dttm ne . then external_event_dttm = tzoneu2s(external_event_dttm,&timeZone_Value.);if external_event_dttm_tz ne . then external_event_dttm_tz = tzoneu2s(external_event_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :external_event_tmp              , external_event);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..external_event using &tmpdbschema..external_event_tmp              
         on (external_event.event_id=external_event_tmp.event_id)
        when matched then  
        update set external_event.channel_nm = external_event_tmp.channel_nm , external_event.channel_user_id = external_event_tmp.channel_user_id , external_event.context_type_nm = external_event_tmp.context_type_nm , external_event.context_val = external_event_tmp.context_val , external_event.event_designed_id = external_event_tmp.event_designed_id , external_event.event_nm = external_event_tmp.event_nm , external_event.external_event_dttm = external_event_tmp.external_event_dttm , external_event.external_event_dttm_tz = external_event_tmp.external_event_dttm_tz , external_event.identity_id = external_event_tmp.identity_id , external_event.load_dttm = external_event_tmp.load_dttm , external_event.properties_map_doc = external_event_tmp.properties_map_doc , external_event.response_tracking_cd = external_event_tmp.response_tracking_cd
        when not matched then insert ( 
        channel_nm,channel_user_id,context_type_nm,context_val,event_designed_id,event_id,event_nm,external_event_dttm,external_event_dttm_tz,identity_id,load_dttm,properties_map_doc,response_tracking_cd
         ) values ( 
        external_event_tmp.channel_nm,external_event_tmp.channel_user_id,external_event_tmp.context_type_nm,external_event_tmp.context_val,external_event_tmp.event_designed_id,external_event_tmp.event_id,external_event_tmp.event_nm,external_event_tmp.external_event_dttm,external_event_tmp.external_event_dttm_tz,external_event_tmp.identity_id,external_event_tmp.load_dttm,external_event_tmp.properties_map_doc,external_event_tmp.response_tracking_cd
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :external_event_tmp              , external_event, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..external_event_tmp              ;
    quit;
    %put ######## Staging table: external_event_tmp               Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..external_event;
      drop table work.external_event;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..fiscal_cc_budget_tmp            ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=fiscal_cc_budget, table_keys=%str(cost_center_id,fp_id), out_table=work.fiscal_cc_budget);
 data &tmplib..fiscal_cc_budget_tmp            ;
     set work.fiscal_cc_budget;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if cost_center_id='' then cost_center_id='-'; if fp_id='' then fp_id='-';
 run;
 %ErrCheck (Failed to Append Data to :fiscal_cc_budget_tmp            , fiscal_cc_budget);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..fiscal_cc_budget using &tmpdbschema..fiscal_cc_budget_tmp            
         on (fiscal_cc_budget.cost_center_id=fiscal_cc_budget_tmp.cost_center_id and fiscal_cc_budget.fp_id=fiscal_cc_budget_tmp.fp_id)
        when matched then  
        update set fiscal_cc_budget.cc_bdgt_amt = fiscal_cc_budget_tmp.cc_bdgt_amt , fiscal_cc_budget.cc_bdgt_budget_amt = fiscal_cc_budget_tmp.cc_bdgt_budget_amt , fiscal_cc_budget.cc_bdgt_budget_desc = fiscal_cc_budget_tmp.cc_bdgt_budget_desc , fiscal_cc_budget.cc_bdgt_cmtmnt_invoice_amt = fiscal_cc_budget_tmp.cc_bdgt_cmtmnt_invoice_amt , fiscal_cc_budget.cc_bdgt_cmtmnt_invoice_cnt = fiscal_cc_budget_tmp.cc_bdgt_cmtmnt_invoice_cnt , fiscal_cc_budget.cc_bdgt_cmtmnt_outstanding_amt = fiscal_cc_budget_tmp.cc_bdgt_cmtmnt_outstanding_amt , fiscal_cc_budget.cc_bdgt_cmtmnt_overspent_amt = fiscal_cc_budget_tmp.cc_bdgt_cmtmnt_overspent_amt , fiscal_cc_budget.cc_bdgt_committed_amt = fiscal_cc_budget_tmp.cc_bdgt_committed_amt , fiscal_cc_budget.cc_bdgt_direct_invoice_amt = fiscal_cc_budget_tmp.cc_bdgt_direct_invoice_amt , fiscal_cc_budget.cc_bdgt_invoiced_amt = fiscal_cc_budget_tmp.cc_bdgt_invoiced_amt , fiscal_cc_budget.cc_desc = fiscal_cc_budget_tmp.cc_desc , fiscal_cc_budget.cc_nm = fiscal_cc_budget_tmp.cc_nm , fiscal_cc_budget.cc_number = fiscal_cc_budget_tmp.cc_number , fiscal_cc_budget.cc_obsolete_flg = fiscal_cc_budget_tmp.cc_obsolete_flg , fiscal_cc_budget.cc_owner_usernm = fiscal_cc_budget_tmp.cc_owner_usernm , fiscal_cc_budget.created_by_usernm = fiscal_cc_budget_tmp.created_by_usernm , fiscal_cc_budget.created_dttm = fiscal_cc_budget_tmp.created_dttm , fiscal_cc_budget.fin_accnt_desc = fiscal_cc_budget_tmp.fin_accnt_desc , fiscal_cc_budget.fin_accnt_nm = fiscal_cc_budget_tmp.fin_accnt_nm , fiscal_cc_budget.fin_accnt_obsolete_flg = fiscal_cc_budget_tmp.fin_accnt_obsolete_flg , fiscal_cc_budget.fp_cls_ver = fiscal_cc_budget_tmp.fp_cls_ver , fiscal_cc_budget.fp_desc = fiscal_cc_budget_tmp.fp_desc , fiscal_cc_budget.fp_end_dt = fiscal_cc_budget_tmp.fp_end_dt , fiscal_cc_budget.fp_nm = fiscal_cc_budget_tmp.fp_nm , fiscal_cc_budget.fp_obsolete_flg = fiscal_cc_budget_tmp.fp_obsolete_flg , fiscal_cc_budget.fp_start_dt = fiscal_cc_budget_tmp.fp_start_dt , fiscal_cc_budget.gen_ledger_cd = fiscal_cc_budget_tmp.gen_ledger_cd , fiscal_cc_budget.last_modified_dttm = fiscal_cc_budget_tmp.last_modified_dttm , fiscal_cc_budget.last_modified_usernm = fiscal_cc_budget_tmp.last_modified_usernm , fiscal_cc_budget.load_dttm = fiscal_cc_budget_tmp.load_dttm
        when not matched then insert ( 
        cc_bdgt_amt,cc_bdgt_budget_amt,cc_bdgt_budget_desc,cc_bdgt_cmtmnt_invoice_amt,cc_bdgt_cmtmnt_invoice_cnt,cc_bdgt_cmtmnt_outstanding_amt,cc_bdgt_cmtmnt_overspent_amt,cc_bdgt_committed_amt,cc_bdgt_direct_invoice_amt,cc_bdgt_invoiced_amt,cc_desc,cc_nm,cc_number,cc_obsolete_flg,cc_owner_usernm,cost_center_id,created_by_usernm,created_dttm,fin_accnt_desc,fin_accnt_nm,fin_accnt_obsolete_flg,fp_cls_ver,fp_desc,fp_end_dt,fp_id,fp_nm,fp_obsolete_flg,fp_start_dt,gen_ledger_cd,last_modified_dttm,last_modified_usernm,load_dttm
         ) values ( 
        fiscal_cc_budget_tmp.cc_bdgt_amt,fiscal_cc_budget_tmp.cc_bdgt_budget_amt,fiscal_cc_budget_tmp.cc_bdgt_budget_desc,fiscal_cc_budget_tmp.cc_bdgt_cmtmnt_invoice_amt,fiscal_cc_budget_tmp.cc_bdgt_cmtmnt_invoice_cnt,fiscal_cc_budget_tmp.cc_bdgt_cmtmnt_outstanding_amt,fiscal_cc_budget_tmp.cc_bdgt_cmtmnt_overspent_amt,fiscal_cc_budget_tmp.cc_bdgt_committed_amt,fiscal_cc_budget_tmp.cc_bdgt_direct_invoice_amt,fiscal_cc_budget_tmp.cc_bdgt_invoiced_amt,fiscal_cc_budget_tmp.cc_desc,fiscal_cc_budget_tmp.cc_nm,fiscal_cc_budget_tmp.cc_number,fiscal_cc_budget_tmp.cc_obsolete_flg,fiscal_cc_budget_tmp.cc_owner_usernm,fiscal_cc_budget_tmp.cost_center_id,fiscal_cc_budget_tmp.created_by_usernm,fiscal_cc_budget_tmp.created_dttm,fiscal_cc_budget_tmp.fin_accnt_desc,fiscal_cc_budget_tmp.fin_accnt_nm,fiscal_cc_budget_tmp.fin_accnt_obsolete_flg,fiscal_cc_budget_tmp.fp_cls_ver,fiscal_cc_budget_tmp.fp_desc,fiscal_cc_budget_tmp.fp_end_dt,fiscal_cc_budget_tmp.fp_id,fiscal_cc_budget_tmp.fp_nm,fiscal_cc_budget_tmp.fp_obsolete_flg,fiscal_cc_budget_tmp.fp_start_dt,fiscal_cc_budget_tmp.gen_ledger_cd,fiscal_cc_budget_tmp.last_modified_dttm,fiscal_cc_budget_tmp.last_modified_usernm,fiscal_cc_budget_tmp.load_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :fiscal_cc_budget_tmp            , fiscal_cc_budget, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..fiscal_cc_budget_tmp            ;
    quit;
    %put ######## Staging table: fiscal_cc_budget_tmp             Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..fiscal_cc_budget;
      drop table work.fiscal_cc_budget;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..form_details_tmp                ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=form_details, table_keys=%str(attempt_index_cnt,attempt_status_cd,detail_id,form_field_detail_dttm,submit_flg), out_table=work.form_details);
 data &tmplib..form_details_tmp                ;
     set work.form_details;
  if form_field_detail_dttm ne . then form_field_detail_dttm = tzoneu2s(form_field_detail_dttm,&timeZone_Value.);if form_field_detail_dttm_tz ne . then form_field_detail_dttm_tz = tzoneu2s(form_field_detail_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if attempt_status_cd='' then attempt_status_cd='-'; if detail_id='' then detail_id='-'; if submit_flg='' then submit_flg='-';
 run;
 %ErrCheck (Failed to Append Data to :form_details_tmp                , form_details);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..form_details using &tmpdbschema..form_details_tmp                
         on (form_details.attempt_index_cnt=form_details_tmp.attempt_index_cnt and form_details.attempt_status_cd=form_details_tmp.attempt_status_cd and form_details.detail_id=form_details_tmp.detail_id and form_details.form_field_detail_dttm=form_details_tmp.form_field_detail_dttm and form_details.submit_flg=form_details_tmp.submit_flg)
        when matched then  
        update set form_details.change_index_no = form_details_tmp.change_index_no , form_details.detail_id_hex = form_details_tmp.detail_id_hex , form_details.event_id = form_details_tmp.event_id , form_details.event_key_cd = form_details_tmp.event_key_cd , form_details.event_source_cd = form_details_tmp.event_source_cd , form_details.form_field_detail_dttm_tz = form_details_tmp.form_field_detail_dttm_tz , form_details.form_field_id = form_details_tmp.form_field_id , form_details.form_field_nm = form_details_tmp.form_field_nm , form_details.form_field_value = form_details_tmp.form_field_value , form_details.form_nm = form_details_tmp.form_nm , form_details.identity_id = form_details_tmp.identity_id , form_details.load_dttm = form_details_tmp.load_dttm , form_details.session_id = form_details_tmp.session_id , form_details.session_id_hex = form_details_tmp.session_id_hex , form_details.visit_id = form_details_tmp.visit_id , form_details.visit_id_hex = form_details_tmp.visit_id_hex
        when not matched then insert ( 
        attempt_index_cnt,attempt_status_cd,change_index_no,detail_id,detail_id_hex,event_id,event_key_cd,event_source_cd,form_field_detail_dttm,form_field_detail_dttm_tz,form_field_id,form_field_nm,form_field_value,form_nm,identity_id,load_dttm,session_id,session_id_hex,submit_flg,visit_id,visit_id_hex
         ) values ( 
        form_details_tmp.attempt_index_cnt,form_details_tmp.attempt_status_cd,form_details_tmp.change_index_no,form_details_tmp.detail_id,form_details_tmp.detail_id_hex,form_details_tmp.event_id,form_details_tmp.event_key_cd,form_details_tmp.event_source_cd,form_details_tmp.form_field_detail_dttm,form_details_tmp.form_field_detail_dttm_tz,form_details_tmp.form_field_id,form_details_tmp.form_field_nm,form_details_tmp.form_field_value,form_details_tmp.form_nm,form_details_tmp.identity_id,form_details_tmp.load_dttm,form_details_tmp.session_id,form_details_tmp.session_id_hex,form_details_tmp.submit_flg,form_details_tmp.visit_id,form_details_tmp.visit_id_hex
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :form_details_tmp                , form_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..form_details_tmp                ;
    quit;
    %put ######## Staging table: form_details_tmp                 Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..form_details;
      drop table work.form_details;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..identity_attributes_tmp         ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=identity_attributes, table_keys=%str(entrytime,identifier_type_id,identity_id), out_table=work.identity_attributes);
 data &tmplib..identity_attributes_tmp         ;
     set work.identity_attributes;
  if entrytime ne . then entrytime = tzoneu2s(entrytime,&timeZone_Value.);if processed_dttm ne . then processed_dttm = tzoneu2s(processed_dttm,&timeZone_Value.) ;
  if identifier_type_id='' then identifier_type_id='-'; if identity_id='' then identity_id='-';
 run;
 %ErrCheck (Failed to Append Data to :identity_attributes_tmp         , identity_attributes);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..identity_attributes using &tmpdbschema..identity_attributes_tmp         
         on (identity_attributes.entrytime=identity_attributes_tmp.entrytime and identity_attributes.identifier_type_id=identity_attributes_tmp.identifier_type_id and identity_attributes.identity_id=identity_attributes_tmp.identity_id)
        when matched then  
        update set identity_attributes.processed_dttm = identity_attributes_tmp.processed_dttm , identity_attributes.user_identifier_val = identity_attributes_tmp.user_identifier_val
        when not matched then insert ( 
        entrytime,identifier_type_id,identity_id,processed_dttm,user_identifier_val
         ) values ( 
        identity_attributes_tmp.entrytime,identity_attributes_tmp.identifier_type_id,identity_attributes_tmp.identity_id,identity_attributes_tmp.processed_dttm,identity_attributes_tmp.user_identifier_val
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :identity_attributes_tmp         , identity_attributes, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..identity_attributes_tmp         ;
    quit;
    %put ######## Staging table: identity_attributes_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..identity_attributes;
      drop table work.identity_attributes;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..identity_map_tmp                ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=identity_map, table_keys=%str(source_identity_id), out_table=work.identity_map);
 data &tmplib..identity_map_tmp                ;
     set work.identity_map;
  if entrytime ne . then entrytime = tzoneu2s(entrytime,&timeZone_Value.);if processed_dttm ne . then processed_dttm = tzoneu2s(processed_dttm,&timeZone_Value.) ;
  if source_identity_id='' then source_identity_id='-';
 run;
 %ErrCheck (Failed to Append Data to :identity_map_tmp                , identity_map);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..identity_map using &tmpdbschema..identity_map_tmp                
         on (identity_map.source_identity_id=identity_map_tmp.source_identity_id)
        when matched then  
        update set identity_map.entrytime = identity_map_tmp.entrytime , identity_map.processed_dttm = identity_map_tmp.processed_dttm , identity_map.target_identity_id = identity_map_tmp.target_identity_id
        when not matched then insert ( 
        entrytime,processed_dttm,source_identity_id,target_identity_id
         ) values ( 
        identity_map_tmp.entrytime,identity_map_tmp.processed_dttm,identity_map_tmp.source_identity_id,identity_map_tmp.target_identity_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :identity_map_tmp                , identity_map, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..identity_map_tmp                ;
    quit;
    %put ######## Staging table: identity_map_tmp                 Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..identity_map;
      drop table work.identity_map;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..impression_delivered_tmp        ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=impression_delivered, table_keys=%str(event_id), out_table=work.impression_delivered);
 data &tmplib..impression_delivered_tmp        ;
     set work.impression_delivered;
  if impression_delivered_dttm ne . then impression_delivered_dttm = tzoneu2s(impression_delivered_dttm,&timeZone_Value.);if impression_delivered_dttm_tz ne . then impression_delivered_dttm_tz = tzoneu2s(impression_delivered_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :impression_delivered_tmp        , impression_delivered);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..impression_delivered using &tmpdbschema..impression_delivered_tmp        
         on (impression_delivered.event_id=impression_delivered_tmp.event_id)
        when matched then  
        update set impression_delivered.aud_occurrence_id = impression_delivered_tmp.aud_occurrence_id , impression_delivered.audience_id = impression_delivered_tmp.audience_id , impression_delivered.channel_nm = impression_delivered_tmp.channel_nm , impression_delivered.channel_user_id = impression_delivered_tmp.channel_user_id , impression_delivered.context_type_nm = impression_delivered_tmp.context_type_nm , impression_delivered.context_val = impression_delivered_tmp.context_val , impression_delivered.control_group_flg = impression_delivered_tmp.control_group_flg , impression_delivered.creative_id = impression_delivered_tmp.creative_id , impression_delivered.creative_version_id = impression_delivered_tmp.creative_version_id , impression_delivered.detail_id_hex = impression_delivered_tmp.detail_id_hex , impression_delivered.event_designed_id = impression_delivered_tmp.event_designed_id , impression_delivered.event_key_cd = impression_delivered_tmp.event_key_cd , impression_delivered.event_nm = impression_delivered_tmp.event_nm , impression_delivered.event_source_cd = impression_delivered_tmp.event_source_cd , impression_delivered.identity_id = impression_delivered_tmp.identity_id , impression_delivered.impression_delivered_dttm = impression_delivered_tmp.impression_delivered_dttm , impression_delivered.impression_delivered_dttm_tz = impression_delivered_tmp.impression_delivered_dttm_tz , impression_delivered.load_dttm = impression_delivered_tmp.load_dttm , impression_delivered.message_id = impression_delivered_tmp.message_id , impression_delivered.message_version_id = impression_delivered_tmp.message_version_id , impression_delivered.mobile_app_id = impression_delivered_tmp.mobile_app_id , impression_delivered.product_id = impression_delivered_tmp.product_id , impression_delivered.product_nm = impression_delivered_tmp.product_nm , impression_delivered.product_qty_no = impression_delivered_tmp.product_qty_no , impression_delivered.product_sku_no = impression_delivered_tmp.product_sku_no , impression_delivered.properties_map_doc = impression_delivered_tmp.properties_map_doc , impression_delivered.rec_group_id = impression_delivered_tmp.rec_group_id , impression_delivered.request_id = impression_delivered_tmp.request_id , impression_delivered.reserved_1_txt = impression_delivered_tmp.reserved_1_txt , impression_delivered.reserved_2_txt = impression_delivered_tmp.reserved_2_txt , impression_delivered.response_tracking_cd = impression_delivered_tmp.response_tracking_cd , impression_delivered.segment_id = impression_delivered_tmp.segment_id , impression_delivered.segment_version_id = impression_delivered_tmp.segment_version_id , impression_delivered.session_id_hex = impression_delivered_tmp.session_id_hex , impression_delivered.spot_id = impression_delivered_tmp.spot_id , impression_delivered.task_id = impression_delivered_tmp.task_id , impression_delivered.task_version_id = impression_delivered_tmp.task_version_id , impression_delivered.visit_id_hex = impression_delivered_tmp.visit_id_hex
        when not matched then insert ( 
        aud_occurrence_id,audience_id,channel_nm,channel_user_id,context_type_nm,context_val,control_group_flg,creative_id,creative_version_id,detail_id_hex,event_designed_id,event_id,event_key_cd,event_nm,event_source_cd,identity_id,impression_delivered_dttm,impression_delivered_dttm_tz,load_dttm,message_id,message_version_id,mobile_app_id,product_id,product_nm,product_qty_no,product_sku_no,properties_map_doc,rec_group_id,request_id,reserved_1_txt,reserved_2_txt,response_tracking_cd,segment_id,segment_version_id,session_id_hex,spot_id,task_id,task_version_id,visit_id_hex
         ) values ( 
        impression_delivered_tmp.aud_occurrence_id,impression_delivered_tmp.audience_id,impression_delivered_tmp.channel_nm,impression_delivered_tmp.channel_user_id,impression_delivered_tmp.context_type_nm,impression_delivered_tmp.context_val,impression_delivered_tmp.control_group_flg,impression_delivered_tmp.creative_id,impression_delivered_tmp.creative_version_id,impression_delivered_tmp.detail_id_hex,impression_delivered_tmp.event_designed_id,impression_delivered_tmp.event_id,impression_delivered_tmp.event_key_cd,impression_delivered_tmp.event_nm,impression_delivered_tmp.event_source_cd,impression_delivered_tmp.identity_id,impression_delivered_tmp.impression_delivered_dttm,impression_delivered_tmp.impression_delivered_dttm_tz,impression_delivered_tmp.load_dttm,impression_delivered_tmp.message_id,impression_delivered_tmp.message_version_id,impression_delivered_tmp.mobile_app_id,impression_delivered_tmp.product_id,impression_delivered_tmp.product_nm,impression_delivered_tmp.product_qty_no,impression_delivered_tmp.product_sku_no,impression_delivered_tmp.properties_map_doc,impression_delivered_tmp.rec_group_id,impression_delivered_tmp.request_id,impression_delivered_tmp.reserved_1_txt,impression_delivered_tmp.reserved_2_txt,impression_delivered_tmp.response_tracking_cd,impression_delivered_tmp.segment_id,impression_delivered_tmp.segment_version_id,impression_delivered_tmp.session_id_hex,impression_delivered_tmp.spot_id,impression_delivered_tmp.task_id,impression_delivered_tmp.task_version_id,impression_delivered_tmp.visit_id_hex
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :impression_delivered_tmp        , impression_delivered, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..impression_delivered_tmp        ;
    quit;
    %put ######## Staging table: impression_delivered_tmp         Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..impression_delivered;
      drop table work.impression_delivered;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..impression_spot_viewable_tmp    ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=impression_spot_viewable, table_keys=%str(event_id), out_table=work.impression_spot_viewable);
 data &tmplib..impression_spot_viewable_tmp    ;
     set work.impression_spot_viewable;
  if impression_viewable_dttm ne . then impression_viewable_dttm = tzoneu2s(impression_viewable_dttm,&timeZone_Value.);if impression_viewable_dttm_tz ne . then impression_viewable_dttm_tz = tzoneu2s(impression_viewable_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :impression_spot_viewable_tmp    , impression_spot_viewable);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..impression_spot_viewable using &tmpdbschema..impression_spot_viewable_tmp    
         on (impression_spot_viewable.event_id=impression_spot_viewable_tmp.event_id)
        when matched then  
        update set impression_spot_viewable.analysis_group_id = impression_spot_viewable_tmp.analysis_group_id , impression_spot_viewable.aud_occurrence_id = impression_spot_viewable_tmp.aud_occurrence_id , impression_spot_viewable.audience_id = impression_spot_viewable_tmp.audience_id , impression_spot_viewable.channel_nm = impression_spot_viewable_tmp.channel_nm , impression_spot_viewable.channel_user_id = impression_spot_viewable_tmp.channel_user_id , impression_spot_viewable.context_type_nm = impression_spot_viewable_tmp.context_type_nm , impression_spot_viewable.context_val = impression_spot_viewable_tmp.context_val , impression_spot_viewable.control_group_flg = impression_spot_viewable_tmp.control_group_flg , impression_spot_viewable.creative_id = impression_spot_viewable_tmp.creative_id , impression_spot_viewable.creative_version_id = impression_spot_viewable_tmp.creative_version_id , impression_spot_viewable.detail_id_hex = impression_spot_viewable_tmp.detail_id_hex , impression_spot_viewable.event_designed_id = impression_spot_viewable_tmp.event_designed_id , impression_spot_viewable.event_key_cd = impression_spot_viewable_tmp.event_key_cd , impression_spot_viewable.event_nm = impression_spot_viewable_tmp.event_nm , impression_spot_viewable.event_source_cd = impression_spot_viewable_tmp.event_source_cd , impression_spot_viewable.identity_id = impression_spot_viewable_tmp.identity_id , impression_spot_viewable.impression_viewable_dttm = impression_spot_viewable_tmp.impression_viewable_dttm , impression_spot_viewable.impression_viewable_dttm_tz = impression_spot_viewable_tmp.impression_viewable_dttm_tz , impression_spot_viewable.load_dttm = impression_spot_viewable_tmp.load_dttm , impression_spot_viewable.message_id = impression_spot_viewable_tmp.message_id , impression_spot_viewable.message_version_id = impression_spot_viewable_tmp.message_version_id , impression_spot_viewable.mobile_app_id = impression_spot_viewable_tmp.mobile_app_id , impression_spot_viewable.occurrence_id = impression_spot_viewable_tmp.occurrence_id , impression_spot_viewable.product_id = impression_spot_viewable_tmp.product_id , impression_spot_viewable.product_nm = impression_spot_viewable_tmp.product_nm , impression_spot_viewable.product_qty_no = impression_spot_viewable_tmp.product_qty_no , impression_spot_viewable.product_sku_no = impression_spot_viewable_tmp.product_sku_no , impression_spot_viewable.properties_map_doc = impression_spot_viewable_tmp.properties_map_doc , impression_spot_viewable.rec_group_id = impression_spot_viewable_tmp.rec_group_id , impression_spot_viewable.request_id = impression_spot_viewable_tmp.request_id , impression_spot_viewable.reserved_1_txt = impression_spot_viewable_tmp.reserved_1_txt , impression_spot_viewable.reserved_2_txt = impression_spot_viewable_tmp.reserved_2_txt , impression_spot_viewable.response_tracking_cd = impression_spot_viewable_tmp.response_tracking_cd , impression_spot_viewable.segment_id = impression_spot_viewable_tmp.segment_id , impression_spot_viewable.segment_version_id = impression_spot_viewable_tmp.segment_version_id , impression_spot_viewable.session_id_hex = impression_spot_viewable_tmp.session_id_hex , impression_spot_viewable.spot_id = impression_spot_viewable_tmp.spot_id , impression_spot_viewable.task_id = impression_spot_viewable_tmp.task_id , impression_spot_viewable.task_version_id = impression_spot_viewable_tmp.task_version_id , impression_spot_viewable.visit_id_hex = impression_spot_viewable_tmp.visit_id_hex
        when not matched then insert ( 
        analysis_group_id,aud_occurrence_id,audience_id,channel_nm,channel_user_id,context_type_nm,context_val,control_group_flg,creative_id,creative_version_id,detail_id_hex,event_designed_id,event_id,event_key_cd,event_nm,event_source_cd,identity_id,impression_viewable_dttm,impression_viewable_dttm_tz,load_dttm,message_id,message_version_id,mobile_app_id,occurrence_id,product_id,product_nm,product_qty_no,product_sku_no,properties_map_doc,rec_group_id,request_id,reserved_1_txt,reserved_2_txt,response_tracking_cd,segment_id,segment_version_id,session_id_hex,spot_id,task_id,task_version_id,visit_id_hex
         ) values ( 
        impression_spot_viewable_tmp.analysis_group_id,impression_spot_viewable_tmp.aud_occurrence_id,impression_spot_viewable_tmp.audience_id,impression_spot_viewable_tmp.channel_nm,impression_spot_viewable_tmp.channel_user_id,impression_spot_viewable_tmp.context_type_nm,impression_spot_viewable_tmp.context_val,impression_spot_viewable_tmp.control_group_flg,impression_spot_viewable_tmp.creative_id,impression_spot_viewable_tmp.creative_version_id,impression_spot_viewable_tmp.detail_id_hex,impression_spot_viewable_tmp.event_designed_id,impression_spot_viewable_tmp.event_id,impression_spot_viewable_tmp.event_key_cd,impression_spot_viewable_tmp.event_nm,impression_spot_viewable_tmp.event_source_cd,impression_spot_viewable_tmp.identity_id,impression_spot_viewable_tmp.impression_viewable_dttm,impression_spot_viewable_tmp.impression_viewable_dttm_tz,impression_spot_viewable_tmp.load_dttm,impression_spot_viewable_tmp.message_id,impression_spot_viewable_tmp.message_version_id,impression_spot_viewable_tmp.mobile_app_id,impression_spot_viewable_tmp.occurrence_id,impression_spot_viewable_tmp.product_id,impression_spot_viewable_tmp.product_nm,impression_spot_viewable_tmp.product_qty_no,impression_spot_viewable_tmp.product_sku_no,impression_spot_viewable_tmp.properties_map_doc,impression_spot_viewable_tmp.rec_group_id,impression_spot_viewable_tmp.request_id,impression_spot_viewable_tmp.reserved_1_txt,impression_spot_viewable_tmp.reserved_2_txt,impression_spot_viewable_tmp.response_tracking_cd,impression_spot_viewable_tmp.segment_id,impression_spot_viewable_tmp.segment_version_id,impression_spot_viewable_tmp.session_id_hex,impression_spot_viewable_tmp.spot_id,impression_spot_viewable_tmp.task_id,impression_spot_viewable_tmp.task_version_id,impression_spot_viewable_tmp.visit_id_hex
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :impression_spot_viewable_tmp    , impression_spot_viewable, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..impression_spot_viewable_tmp    ;
    quit;
    %put ######## Staging table: impression_spot_viewable_tmp     Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..impression_spot_viewable;
      drop table work.impression_spot_viewable;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..in_app_failed_tmp               ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=in_app_failed, table_keys=%str(event_id), out_table=work.in_app_failed);
 data &tmplib..in_app_failed_tmp               ;
     set work.in_app_failed;
  if in_app_failed_dttm ne . then in_app_failed_dttm = tzoneu2s(in_app_failed_dttm,&timeZone_Value.);if in_app_failed_dttm_tz ne . then in_app_failed_dttm_tz = tzoneu2s(in_app_failed_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :in_app_failed_tmp               , in_app_failed);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..in_app_failed using &tmpdbschema..in_app_failed_tmp               
         on (in_app_failed.event_id=in_app_failed_tmp.event_id)
        when matched then  
        update set in_app_failed.channel_nm = in_app_failed_tmp.channel_nm , in_app_failed.channel_user_id = in_app_failed_tmp.channel_user_id , in_app_failed.context_type_nm = in_app_failed_tmp.context_type_nm , in_app_failed.context_val = in_app_failed_tmp.context_val , in_app_failed.creative_id = in_app_failed_tmp.creative_id , in_app_failed.creative_version_id = in_app_failed_tmp.creative_version_id , in_app_failed.error_cd = in_app_failed_tmp.error_cd , in_app_failed.error_message_txt = in_app_failed_tmp.error_message_txt , in_app_failed.event_designed_id = in_app_failed_tmp.event_designed_id , in_app_failed.event_nm = in_app_failed_tmp.event_nm , in_app_failed.identity_id = in_app_failed_tmp.identity_id , in_app_failed.in_app_failed_dttm = in_app_failed_tmp.in_app_failed_dttm , in_app_failed.in_app_failed_dttm_tz = in_app_failed_tmp.in_app_failed_dttm_tz , in_app_failed.load_dttm = in_app_failed_tmp.load_dttm , in_app_failed.message_id = in_app_failed_tmp.message_id , in_app_failed.message_version_id = in_app_failed_tmp.message_version_id , in_app_failed.mobile_app_id = in_app_failed_tmp.mobile_app_id , in_app_failed.occurrence_id = in_app_failed_tmp.occurrence_id , in_app_failed.properties_map_doc = in_app_failed_tmp.properties_map_doc , in_app_failed.reserved_1_txt = in_app_failed_tmp.reserved_1_txt , in_app_failed.reserved_2_txt = in_app_failed_tmp.reserved_2_txt , in_app_failed.response_tracking_cd = in_app_failed_tmp.response_tracking_cd , in_app_failed.segment_id = in_app_failed_tmp.segment_id , in_app_failed.segment_version_id = in_app_failed_tmp.segment_version_id , in_app_failed.spot_id = in_app_failed_tmp.spot_id , in_app_failed.task_id = in_app_failed_tmp.task_id , in_app_failed.task_version_id = in_app_failed_tmp.task_version_id
        when not matched then insert ( 
        channel_nm,channel_user_id,context_type_nm,context_val,creative_id,creative_version_id,error_cd,error_message_txt,event_designed_id,event_id,event_nm,identity_id,in_app_failed_dttm,in_app_failed_dttm_tz,load_dttm,message_id,message_version_id,mobile_app_id,occurrence_id,properties_map_doc,reserved_1_txt,reserved_2_txt,response_tracking_cd,segment_id,segment_version_id,spot_id,task_id,task_version_id
         ) values ( 
        in_app_failed_tmp.channel_nm,in_app_failed_tmp.channel_user_id,in_app_failed_tmp.context_type_nm,in_app_failed_tmp.context_val,in_app_failed_tmp.creative_id,in_app_failed_tmp.creative_version_id,in_app_failed_tmp.error_cd,in_app_failed_tmp.error_message_txt,in_app_failed_tmp.event_designed_id,in_app_failed_tmp.event_id,in_app_failed_tmp.event_nm,in_app_failed_tmp.identity_id,in_app_failed_tmp.in_app_failed_dttm,in_app_failed_tmp.in_app_failed_dttm_tz,in_app_failed_tmp.load_dttm,in_app_failed_tmp.message_id,in_app_failed_tmp.message_version_id,in_app_failed_tmp.mobile_app_id,in_app_failed_tmp.occurrence_id,in_app_failed_tmp.properties_map_doc,in_app_failed_tmp.reserved_1_txt,in_app_failed_tmp.reserved_2_txt,in_app_failed_tmp.response_tracking_cd,in_app_failed_tmp.segment_id,in_app_failed_tmp.segment_version_id,in_app_failed_tmp.spot_id,in_app_failed_tmp.task_id,in_app_failed_tmp.task_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :in_app_failed_tmp               , in_app_failed, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..in_app_failed_tmp               ;
    quit;
    %put ######## Staging table: in_app_failed_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..in_app_failed;
      drop table work.in_app_failed;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..in_app_message_tmp              ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=in_app_message, table_keys=%str(event_id), out_table=work.in_app_message);
 data &tmplib..in_app_message_tmp              ;
     set work.in_app_message;
  if in_app_action_dttm ne . then in_app_action_dttm = tzoneu2s(in_app_action_dttm,&timeZone_Value.);if in_app_action_dttm_tz ne . then in_app_action_dttm_tz = tzoneu2s(in_app_action_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :in_app_message_tmp              , in_app_message);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..in_app_message using &tmpdbschema..in_app_message_tmp              
         on (in_app_message.event_id=in_app_message_tmp.event_id)
        when matched then  
        update set in_app_message.channel_nm = in_app_message_tmp.channel_nm , in_app_message.channel_user_id = in_app_message_tmp.channel_user_id , in_app_message.context_type_nm = in_app_message_tmp.context_type_nm , in_app_message.context_val = in_app_message_tmp.context_val , in_app_message.creative_id = in_app_message_tmp.creative_id , in_app_message.creative_version_id = in_app_message_tmp.creative_version_id , in_app_message.event_designed_id = in_app_message_tmp.event_designed_id , in_app_message.event_nm = in_app_message_tmp.event_nm , in_app_message.identity_id = in_app_message_tmp.identity_id , in_app_message.in_app_action_dttm = in_app_message_tmp.in_app_action_dttm , in_app_message.in_app_action_dttm_tz = in_app_message_tmp.in_app_action_dttm_tz , in_app_message.load_dttm = in_app_message_tmp.load_dttm , in_app_message.message_id = in_app_message_tmp.message_id , in_app_message.message_version_id = in_app_message_tmp.message_version_id , in_app_message.mobile_app_id = in_app_message_tmp.mobile_app_id , in_app_message.occurrence_id = in_app_message_tmp.occurrence_id , in_app_message.properties_map_doc = in_app_message_tmp.properties_map_doc , in_app_message.reserved_1_txt = in_app_message_tmp.reserved_1_txt , in_app_message.reserved_2_txt = in_app_message_tmp.reserved_2_txt , in_app_message.reserved_3_txt = in_app_message_tmp.reserved_3_txt , in_app_message.response_tracking_cd = in_app_message_tmp.response_tracking_cd , in_app_message.segment_id = in_app_message_tmp.segment_id , in_app_message.segment_version_id = in_app_message_tmp.segment_version_id , in_app_message.spot_id = in_app_message_tmp.spot_id , in_app_message.task_id = in_app_message_tmp.task_id , in_app_message.task_version_id = in_app_message_tmp.task_version_id
        when not matched then insert ( 
        channel_nm,channel_user_id,context_type_nm,context_val,creative_id,creative_version_id,event_designed_id,event_id,event_nm,identity_id,in_app_action_dttm,in_app_action_dttm_tz,load_dttm,message_id,message_version_id,mobile_app_id,occurrence_id,properties_map_doc,reserved_1_txt,reserved_2_txt,reserved_3_txt,response_tracking_cd,segment_id,segment_version_id,spot_id,task_id,task_version_id
         ) values ( 
        in_app_message_tmp.channel_nm,in_app_message_tmp.channel_user_id,in_app_message_tmp.context_type_nm,in_app_message_tmp.context_val,in_app_message_tmp.creative_id,in_app_message_tmp.creative_version_id,in_app_message_tmp.event_designed_id,in_app_message_tmp.event_id,in_app_message_tmp.event_nm,in_app_message_tmp.identity_id,in_app_message_tmp.in_app_action_dttm,in_app_message_tmp.in_app_action_dttm_tz,in_app_message_tmp.load_dttm,in_app_message_tmp.message_id,in_app_message_tmp.message_version_id,in_app_message_tmp.mobile_app_id,in_app_message_tmp.occurrence_id,in_app_message_tmp.properties_map_doc,in_app_message_tmp.reserved_1_txt,in_app_message_tmp.reserved_2_txt,in_app_message_tmp.reserved_3_txt,in_app_message_tmp.response_tracking_cd,in_app_message_tmp.segment_id,in_app_message_tmp.segment_version_id,in_app_message_tmp.spot_id,in_app_message_tmp.task_id,in_app_message_tmp.task_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :in_app_message_tmp              , in_app_message, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..in_app_message_tmp              ;
    quit;
    %put ######## Staging table: in_app_message_tmp               Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..in_app_message;
      drop table work.in_app_message;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..in_app_send_tmp                 ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=in_app_send, table_keys=%str(event_id), out_table=work.in_app_send);
 data &tmplib..in_app_send_tmp                 ;
     set work.in_app_send;
  if in_app_send_dttm ne . then in_app_send_dttm = tzoneu2s(in_app_send_dttm,&timeZone_Value.);if in_app_send_dttm_tz ne . then in_app_send_dttm_tz = tzoneu2s(in_app_send_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :in_app_send_tmp                 , in_app_send);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..in_app_send using &tmpdbschema..in_app_send_tmp                 
         on (in_app_send.event_id=in_app_send_tmp.event_id)
        when matched then  
        update set in_app_send.channel_nm = in_app_send_tmp.channel_nm , in_app_send.channel_user_id = in_app_send_tmp.channel_user_id , in_app_send.context_type_nm = in_app_send_tmp.context_type_nm , in_app_send.context_val = in_app_send_tmp.context_val , in_app_send.creative_id = in_app_send_tmp.creative_id , in_app_send.creative_version_id = in_app_send_tmp.creative_version_id , in_app_send.event_designed_id = in_app_send_tmp.event_designed_id , in_app_send.event_nm = in_app_send_tmp.event_nm , in_app_send.identity_id = in_app_send_tmp.identity_id , in_app_send.in_app_send_dttm = in_app_send_tmp.in_app_send_dttm , in_app_send.in_app_send_dttm_tz = in_app_send_tmp.in_app_send_dttm_tz , in_app_send.load_dttm = in_app_send_tmp.load_dttm , in_app_send.message_id = in_app_send_tmp.message_id , in_app_send.message_version_id = in_app_send_tmp.message_version_id , in_app_send.mobile_app_id = in_app_send_tmp.mobile_app_id , in_app_send.occurrence_id = in_app_send_tmp.occurrence_id , in_app_send.properties_map_doc = in_app_send_tmp.properties_map_doc , in_app_send.reserved_1_txt = in_app_send_tmp.reserved_1_txt , in_app_send.reserved_2_txt = in_app_send_tmp.reserved_2_txt , in_app_send.response_tracking_cd = in_app_send_tmp.response_tracking_cd , in_app_send.segment_id = in_app_send_tmp.segment_id , in_app_send.segment_version_id = in_app_send_tmp.segment_version_id , in_app_send.spot_id = in_app_send_tmp.spot_id , in_app_send.task_id = in_app_send_tmp.task_id , in_app_send.task_version_id = in_app_send_tmp.task_version_id
        when not matched then insert ( 
        channel_nm,channel_user_id,context_type_nm,context_val,creative_id,creative_version_id,event_designed_id,event_id,event_nm,identity_id,in_app_send_dttm,in_app_send_dttm_tz,load_dttm,message_id,message_version_id,mobile_app_id,occurrence_id,properties_map_doc,reserved_1_txt,reserved_2_txt,response_tracking_cd,segment_id,segment_version_id,spot_id,task_id,task_version_id
         ) values ( 
        in_app_send_tmp.channel_nm,in_app_send_tmp.channel_user_id,in_app_send_tmp.context_type_nm,in_app_send_tmp.context_val,in_app_send_tmp.creative_id,in_app_send_tmp.creative_version_id,in_app_send_tmp.event_designed_id,in_app_send_tmp.event_id,in_app_send_tmp.event_nm,in_app_send_tmp.identity_id,in_app_send_tmp.in_app_send_dttm,in_app_send_tmp.in_app_send_dttm_tz,in_app_send_tmp.load_dttm,in_app_send_tmp.message_id,in_app_send_tmp.message_version_id,in_app_send_tmp.mobile_app_id,in_app_send_tmp.occurrence_id,in_app_send_tmp.properties_map_doc,in_app_send_tmp.reserved_1_txt,in_app_send_tmp.reserved_2_txt,in_app_send_tmp.response_tracking_cd,in_app_send_tmp.segment_id,in_app_send_tmp.segment_version_id,in_app_send_tmp.spot_id,in_app_send_tmp.task_id,in_app_send_tmp.task_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :in_app_send_tmp                 , in_app_send, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..in_app_send_tmp                 ;
    quit;
    %put ######## Staging table: in_app_send_tmp                  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..in_app_send;
      drop table work.in_app_send;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..in_app_targeting_request_tmp    ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=in_app_targeting_request, table_keys=%str(event_id), out_table=work.in_app_targeting_request);
 data &tmplib..in_app_targeting_request_tmp    ;
     set work.in_app_targeting_request;
  if in_app_tgt_request_dttm ne . then in_app_tgt_request_dttm = tzoneu2s(in_app_tgt_request_dttm,&timeZone_Value.);if in_app_tgt_request_dttm_tz ne . then in_app_tgt_request_dttm_tz = tzoneu2s(in_app_tgt_request_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :in_app_targeting_request_tmp    , in_app_targeting_request);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..in_app_targeting_request using &tmpdbschema..in_app_targeting_request_tmp    
         on (in_app_targeting_request.event_id=in_app_targeting_request_tmp.event_id)
        when matched then  
        update set in_app_targeting_request.channel_nm = in_app_targeting_request_tmp.channel_nm , in_app_targeting_request.channel_user_id = in_app_targeting_request_tmp.channel_user_id , in_app_targeting_request.context_type_nm = in_app_targeting_request_tmp.context_type_nm , in_app_targeting_request.context_val = in_app_targeting_request_tmp.context_val , in_app_targeting_request.eligibility_flg = in_app_targeting_request_tmp.eligibility_flg , in_app_targeting_request.event_designed_id = in_app_targeting_request_tmp.event_designed_id , in_app_targeting_request.event_nm = in_app_targeting_request_tmp.event_nm , in_app_targeting_request.identity_id = in_app_targeting_request_tmp.identity_id , in_app_targeting_request.in_app_tgt_request_dttm = in_app_targeting_request_tmp.in_app_tgt_request_dttm , in_app_targeting_request.in_app_tgt_request_dttm_tz = in_app_targeting_request_tmp.in_app_tgt_request_dttm_tz , in_app_targeting_request.load_dttm = in_app_targeting_request_tmp.load_dttm , in_app_targeting_request.mobile_app_id = in_app_targeting_request_tmp.mobile_app_id
        when not matched then insert ( 
        channel_nm,channel_user_id,context_type_nm,context_val,eligibility_flg,event_designed_id,event_id,event_nm,identity_id,in_app_tgt_request_dttm,in_app_tgt_request_dttm_tz,load_dttm,mobile_app_id
         ) values ( 
        in_app_targeting_request_tmp.channel_nm,in_app_targeting_request_tmp.channel_user_id,in_app_targeting_request_tmp.context_type_nm,in_app_targeting_request_tmp.context_val,in_app_targeting_request_tmp.eligibility_flg,in_app_targeting_request_tmp.event_designed_id,in_app_targeting_request_tmp.event_id,in_app_targeting_request_tmp.event_nm,in_app_targeting_request_tmp.identity_id,in_app_targeting_request_tmp.in_app_tgt_request_dttm,in_app_targeting_request_tmp.in_app_tgt_request_dttm_tz,in_app_targeting_request_tmp.load_dttm,in_app_targeting_request_tmp.mobile_app_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :in_app_targeting_request_tmp    , in_app_targeting_request, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..in_app_targeting_request_tmp    ;
    quit;
    %put ######## Staging table: in_app_targeting_request_tmp     Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..in_app_targeting_request;
      drop table work.in_app_targeting_request;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..invoice_details_tmp             ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=invoice_details, table_keys=%str(cmtmnt_id,invoice_id,planning_id), out_table=work.invoice_details);
 data &tmplib..invoice_details_tmp             ;
     set work.invoice_details;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if invoice_created_dttm ne . then invoice_created_dttm = tzoneu2s(invoice_created_dttm,&timeZone_Value.);if invoice_reconciled_dttm ne . then invoice_reconciled_dttm = tzoneu2s(invoice_reconciled_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if payment_dttm ne . then payment_dttm = tzoneu2s(payment_dttm,&timeZone_Value.) ;
  if cmtmnt_id='' then cmtmnt_id='-'; if invoice_id='' then invoice_id='-'; if planning_id='' then planning_id='-';
 run;
 %ErrCheck (Failed to Append Data to :invoice_details_tmp             , invoice_details);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..invoice_details using &tmpdbschema..invoice_details_tmp             
         on (invoice_details.cmtmnt_id=invoice_details_tmp.cmtmnt_id and invoice_details.invoice_id=invoice_details_tmp.invoice_id and invoice_details.planning_id=invoice_details_tmp.planning_id)
        when matched then  
        update set invoice_details.cmtmnt_nm = invoice_details_tmp.cmtmnt_nm , invoice_details.created_by_usernm = invoice_details_tmp.created_by_usernm , invoice_details.created_dttm = invoice_details_tmp.created_dttm , invoice_details.invoice_amt = invoice_details_tmp.invoice_amt , invoice_details.invoice_created_dttm = invoice_details_tmp.invoice_created_dttm , invoice_details.invoice_desc = invoice_details_tmp.invoice_desc , invoice_details.invoice_nm = invoice_details_tmp.invoice_nm , invoice_details.invoice_number = invoice_details_tmp.invoice_number , invoice_details.invoice_reconciled_dttm = invoice_details_tmp.invoice_reconciled_dttm , invoice_details.invoice_status = invoice_details_tmp.invoice_status , invoice_details.last_modified_dttm = invoice_details_tmp.last_modified_dttm , invoice_details.last_modified_usernm = invoice_details_tmp.last_modified_usernm , invoice_details.load_dttm = invoice_details_tmp.load_dttm , invoice_details.payment_dttm = invoice_details_tmp.payment_dttm , invoice_details.plan_currency_cd = invoice_details_tmp.plan_currency_cd , invoice_details.planning_nm = invoice_details_tmp.planning_nm , invoice_details.reconcile_amt = invoice_details_tmp.reconcile_amt , invoice_details.reconcile_note = invoice_details_tmp.reconcile_note , invoice_details.vendor_amt = invoice_details_tmp.vendor_amt , invoice_details.vendor_currency_cd = invoice_details_tmp.vendor_currency_cd , invoice_details.vendor_desc = invoice_details_tmp.vendor_desc , invoice_details.vendor_id = invoice_details_tmp.vendor_id , invoice_details.vendor_nm = invoice_details_tmp.vendor_nm , invoice_details.vendor_number = invoice_details_tmp.vendor_number , invoice_details.vendor_obsolete_flg = invoice_details_tmp.vendor_obsolete_flg
        when not matched then insert ( 
        cmtmnt_id,cmtmnt_nm,created_by_usernm,created_dttm,invoice_amt,invoice_created_dttm,invoice_desc,invoice_id,invoice_nm,invoice_number,invoice_reconciled_dttm,invoice_status,last_modified_dttm,last_modified_usernm,load_dttm,payment_dttm,plan_currency_cd,planning_id,planning_nm,reconcile_amt,reconcile_note,vendor_amt,vendor_currency_cd,vendor_desc,vendor_id,vendor_nm,vendor_number,vendor_obsolete_flg
         ) values ( 
        invoice_details_tmp.cmtmnt_id,invoice_details_tmp.cmtmnt_nm,invoice_details_tmp.created_by_usernm,invoice_details_tmp.created_dttm,invoice_details_tmp.invoice_amt,invoice_details_tmp.invoice_created_dttm,invoice_details_tmp.invoice_desc,invoice_details_tmp.invoice_id,invoice_details_tmp.invoice_nm,invoice_details_tmp.invoice_number,invoice_details_tmp.invoice_reconciled_dttm,invoice_details_tmp.invoice_status,invoice_details_tmp.last_modified_dttm,invoice_details_tmp.last_modified_usernm,invoice_details_tmp.load_dttm,invoice_details_tmp.payment_dttm,invoice_details_tmp.plan_currency_cd,invoice_details_tmp.planning_id,invoice_details_tmp.planning_nm,invoice_details_tmp.reconcile_amt,invoice_details_tmp.reconcile_note,invoice_details_tmp.vendor_amt,invoice_details_tmp.vendor_currency_cd,invoice_details_tmp.vendor_desc,invoice_details_tmp.vendor_id,invoice_details_tmp.vendor_nm,invoice_details_tmp.vendor_number,invoice_details_tmp.vendor_obsolete_flg
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :invoice_details_tmp             , invoice_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..invoice_details_tmp             ;
    quit;
    %put ######## Staging table: invoice_details_tmp              Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..invoice_details;
      drop table work.invoice_details;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..invoice_line_items_tmp          ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=invoice_line_items, table_keys=%str(cmtmnt_id,invoice_id,invoice_nm,invoice_number,planning_id), out_table=work.invoice_line_items);
 data &tmplib..invoice_line_items_tmp          ;
     set work.invoice_line_items;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if invoice_created_dttm ne . then invoice_created_dttm = tzoneu2s(invoice_created_dttm,&timeZone_Value.);if invoice_reconciled_dttm ne . then invoice_reconciled_dttm = tzoneu2s(invoice_reconciled_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if payment_dttm ne . then payment_dttm = tzoneu2s(payment_dttm,&timeZone_Value.) ;
  if cmtmnt_id='' then cmtmnt_id='-'; if invoice_id='' then invoice_id='-'; if invoice_nm='' then invoice_nm='-'; if invoice_number='' then invoice_number='-'; if planning_id='' then planning_id='-';
 run;
 %ErrCheck (Failed to Append Data to :invoice_line_items_tmp          , invoice_line_items);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..invoice_line_items using &tmpdbschema..invoice_line_items_tmp          
         on (invoice_line_items.cmtmnt_id=invoice_line_items_tmp.cmtmnt_id and invoice_line_items.invoice_id=invoice_line_items_tmp.invoice_id and invoice_line_items.invoice_nm=invoice_line_items_tmp.invoice_nm and invoice_line_items.invoice_number=invoice_line_items_tmp.invoice_number and invoice_line_items.planning_id=invoice_line_items_tmp.planning_id)
        when matched then  
        update set invoice_line_items.cc_allocated_amt = invoice_line_items_tmp.cc_allocated_amt , invoice_line_items.cc_available_amt = invoice_line_items_tmp.cc_available_amt , invoice_line_items.cc_desc = invoice_line_items_tmp.cc_desc , invoice_line_items.cc_nm = invoice_line_items_tmp.cc_nm , invoice_line_items.cc_owner_usernm = invoice_line_items_tmp.cc_owner_usernm , invoice_line_items.cc_recon_alloc_amt = invoice_line_items_tmp.cc_recon_alloc_amt , invoice_line_items.ccat_nm = invoice_line_items_tmp.ccat_nm , invoice_line_items.cmtmnt_nm = invoice_line_items_tmp.cmtmnt_nm , invoice_line_items.cost_center_id = invoice_line_items_tmp.cost_center_id , invoice_line_items.created_by_usernm = invoice_line_items_tmp.created_by_usernm , invoice_line_items.created_dttm = invoice_line_items_tmp.created_dttm , invoice_line_items.fin_acc_ccat_nm = invoice_line_items_tmp.fin_acc_ccat_nm , invoice_line_items.fin_acc_nm = invoice_line_items_tmp.fin_acc_nm , invoice_line_items.gen_ledger_cd = invoice_line_items_tmp.gen_ledger_cd , invoice_line_items.invoice_amt = invoice_line_items_tmp.invoice_amt , invoice_line_items.invoice_created_dttm = invoice_line_items_tmp.invoice_created_dttm , invoice_line_items.invoice_desc = invoice_line_items_tmp.invoice_desc , invoice_line_items.invoice_reconciled_dttm = invoice_line_items_tmp.invoice_reconciled_dttm , invoice_line_items.invoice_status = invoice_line_items_tmp.invoice_status , invoice_line_items.item_alloc_amt = invoice_line_items_tmp.item_alloc_amt , invoice_line_items.item_alloc_unit = invoice_line_items_tmp.item_alloc_unit , invoice_line_items.item_nm = invoice_line_items_tmp.item_nm , invoice_line_items.item_number = invoice_line_items_tmp.item_number , invoice_line_items.item_qty = invoice_line_items_tmp.item_qty , invoice_line_items.item_rate = invoice_line_items_tmp.item_rate , invoice_line_items.item_vend_alloc_amt = invoice_line_items_tmp.item_vend_alloc_amt , invoice_line_items.item_vend_alloc_unit = invoice_line_items_tmp.item_vend_alloc_unit , invoice_line_items.last_modified_dttm = invoice_line_items_tmp.last_modified_dttm , invoice_line_items.last_modified_usernm = invoice_line_items_tmp.last_modified_usernm , invoice_line_items.load_dttm = invoice_line_items_tmp.load_dttm , invoice_line_items.payment_dttm = invoice_line_items_tmp.payment_dttm , invoice_line_items.plan_currency_cd = invoice_line_items_tmp.plan_currency_cd , invoice_line_items.planning_nm = invoice_line_items_tmp.planning_nm , invoice_line_items.reconcile_amt = invoice_line_items_tmp.reconcile_amt , invoice_line_items.reconcile_note = invoice_line_items_tmp.reconcile_note , invoice_line_items.vendor_amt = invoice_line_items_tmp.vendor_amt , invoice_line_items.vendor_currency_cd = invoice_line_items_tmp.vendor_currency_cd , invoice_line_items.vendor_desc = invoice_line_items_tmp.vendor_desc , invoice_line_items.vendor_id = invoice_line_items_tmp.vendor_id , invoice_line_items.vendor_nm = invoice_line_items_tmp.vendor_nm , invoice_line_items.vendor_number = invoice_line_items_tmp.vendor_number , invoice_line_items.vendor_obsolete_flg = invoice_line_items_tmp.vendor_obsolete_flg
        when not matched then insert ( 
        cc_allocated_amt,cc_available_amt,cc_desc,cc_nm,cc_owner_usernm,cc_recon_alloc_amt,ccat_nm,cmtmnt_id,cmtmnt_nm,cost_center_id,created_by_usernm,created_dttm,fin_acc_ccat_nm,fin_acc_nm,gen_ledger_cd,invoice_amt,invoice_created_dttm,invoice_desc,invoice_id,invoice_nm,invoice_number,invoice_reconciled_dttm,invoice_status,item_alloc_amt,item_alloc_unit,item_nm,item_number,item_qty,item_rate,item_vend_alloc_amt,item_vend_alloc_unit,last_modified_dttm,last_modified_usernm,load_dttm,payment_dttm,plan_currency_cd,planning_id,planning_nm,reconcile_amt,reconcile_note,vendor_amt,vendor_currency_cd,vendor_desc,vendor_id,vendor_nm,vendor_number,vendor_obsolete_flg
         ) values ( 
        invoice_line_items_tmp.cc_allocated_amt,invoice_line_items_tmp.cc_available_amt,invoice_line_items_tmp.cc_desc,invoice_line_items_tmp.cc_nm,invoice_line_items_tmp.cc_owner_usernm,invoice_line_items_tmp.cc_recon_alloc_amt,invoice_line_items_tmp.ccat_nm,invoice_line_items_tmp.cmtmnt_id,invoice_line_items_tmp.cmtmnt_nm,invoice_line_items_tmp.cost_center_id,invoice_line_items_tmp.created_by_usernm,invoice_line_items_tmp.created_dttm,invoice_line_items_tmp.fin_acc_ccat_nm,invoice_line_items_tmp.fin_acc_nm,invoice_line_items_tmp.gen_ledger_cd,invoice_line_items_tmp.invoice_amt,invoice_line_items_tmp.invoice_created_dttm,invoice_line_items_tmp.invoice_desc,invoice_line_items_tmp.invoice_id,invoice_line_items_tmp.invoice_nm,invoice_line_items_tmp.invoice_number,invoice_line_items_tmp.invoice_reconciled_dttm,invoice_line_items_tmp.invoice_status,invoice_line_items_tmp.item_alloc_amt,invoice_line_items_tmp.item_alloc_unit,invoice_line_items_tmp.item_nm,invoice_line_items_tmp.item_number,invoice_line_items_tmp.item_qty,invoice_line_items_tmp.item_rate,invoice_line_items_tmp.item_vend_alloc_amt,invoice_line_items_tmp.item_vend_alloc_unit,invoice_line_items_tmp.last_modified_dttm,invoice_line_items_tmp.last_modified_usernm,invoice_line_items_tmp.load_dttm,invoice_line_items_tmp.payment_dttm,invoice_line_items_tmp.plan_currency_cd,invoice_line_items_tmp.planning_id,invoice_line_items_tmp.planning_nm,invoice_line_items_tmp.reconcile_amt,invoice_line_items_tmp.reconcile_note,invoice_line_items_tmp.vendor_amt,invoice_line_items_tmp.vendor_currency_cd,invoice_line_items_tmp.vendor_desc,invoice_line_items_tmp.vendor_id,invoice_line_items_tmp.vendor_nm,invoice_line_items_tmp.vendor_number,invoice_line_items_tmp.vendor_obsolete_flg
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :invoice_line_items_tmp          , invoice_line_items, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..invoice_line_items_tmp          ;
    quit;
    %put ######## Staging table: invoice_line_items_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..invoice_line_items;
      drop table work.invoice_line_items;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..invoice_line_items_ccbdgt_tmp   ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=invoice_line_items_ccbdgt, table_keys=%str(invoice_id,item_number), out_table=work.invoice_line_items_ccbdgt);
 data &tmplib..invoice_line_items_ccbdgt_tmp   ;
     set work.invoice_line_items_ccbdgt;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if invoice_created_dttm ne . then invoice_created_dttm = tzoneu2s(invoice_created_dttm,&timeZone_Value.);if invoice_reconciled_dttm ne . then invoice_reconciled_dttm = tzoneu2s(invoice_reconciled_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if payment_dttm ne . then payment_dttm = tzoneu2s(payment_dttm,&timeZone_Value.) ;
  if invoice_id='' then invoice_id='-';
 run;
 %ErrCheck (Failed to Append Data to :invoice_line_items_ccbdgt_tmp   , invoice_line_items_ccbdgt);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..invoice_line_items_ccbdgt using &tmpdbschema..invoice_line_items_ccbdgt_tmp   
         on (invoice_line_items_ccbdgt.invoice_id=invoice_line_items_ccbdgt_tmp.invoice_id and invoice_line_items_ccbdgt.item_number=invoice_line_items_ccbdgt_tmp.item_number)
        when matched then  
        update set invoice_line_items_ccbdgt.cc_allocated_amt = invoice_line_items_ccbdgt_tmp.cc_allocated_amt , invoice_line_items_ccbdgt.cc_available_amt = invoice_line_items_ccbdgt_tmp.cc_available_amt , invoice_line_items_ccbdgt.cc_bdgt_amt = invoice_line_items_ccbdgt_tmp.cc_bdgt_amt , invoice_line_items_ccbdgt.cc_bdgt_budget_amt = invoice_line_items_ccbdgt_tmp.cc_bdgt_budget_amt , invoice_line_items_ccbdgt.cc_bdgt_budget_desc = invoice_line_items_ccbdgt_tmp.cc_bdgt_budget_desc , invoice_line_items_ccbdgt.cc_bdgt_cmtmnt_invoice_amt = invoice_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_invoice_amt , invoice_line_items_ccbdgt.cc_bdgt_cmtmnt_invoice_cnt = invoice_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_invoice_cnt , invoice_line_items_ccbdgt.cc_bdgt_cmtmnt_outstanding_amt = invoice_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_outstanding_amt , invoice_line_items_ccbdgt.cc_bdgt_cmtmnt_overspent_amt = invoice_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_overspent_amt , invoice_line_items_ccbdgt.cc_bdgt_committed_amt = invoice_line_items_ccbdgt_tmp.cc_bdgt_committed_amt , invoice_line_items_ccbdgt.cc_bdgt_direct_invoice_amt = invoice_line_items_ccbdgt_tmp.cc_bdgt_direct_invoice_amt , invoice_line_items_ccbdgt.cc_bdgt_invoiced_amt = invoice_line_items_ccbdgt_tmp.cc_bdgt_invoiced_amt , invoice_line_items_ccbdgt.cc_desc = invoice_line_items_ccbdgt_tmp.cc_desc , invoice_line_items_ccbdgt.cc_nm = invoice_line_items_ccbdgt_tmp.cc_nm , invoice_line_items_ccbdgt.cc_number = invoice_line_items_ccbdgt_tmp.cc_number , invoice_line_items_ccbdgt.cc_obsolete_flg = invoice_line_items_ccbdgt_tmp.cc_obsolete_flg , invoice_line_items_ccbdgt.cc_owner_usernm = invoice_line_items_ccbdgt_tmp.cc_owner_usernm , invoice_line_items_ccbdgt.cc_recon_alloc_amt = invoice_line_items_ccbdgt_tmp.cc_recon_alloc_amt , invoice_line_items_ccbdgt.ccat_nm = invoice_line_items_ccbdgt_tmp.ccat_nm , invoice_line_items_ccbdgt.cmtmnt_id = invoice_line_items_ccbdgt_tmp.cmtmnt_id , invoice_line_items_ccbdgt.cmtmnt_nm = invoice_line_items_ccbdgt_tmp.cmtmnt_nm , invoice_line_items_ccbdgt.cost_center_id = invoice_line_items_ccbdgt_tmp.cost_center_id , invoice_line_items_ccbdgt.created_by_usernm = invoice_line_items_ccbdgt_tmp.created_by_usernm , invoice_line_items_ccbdgt.created_dttm = invoice_line_items_ccbdgt_tmp.created_dttm , invoice_line_items_ccbdgt.fin_acc_ccat_nm = invoice_line_items_ccbdgt_tmp.fin_acc_ccat_nm , invoice_line_items_ccbdgt.fin_acc_nm = invoice_line_items_ccbdgt_tmp.fin_acc_nm , invoice_line_items_ccbdgt.fp_cls_ver = invoice_line_items_ccbdgt_tmp.fp_cls_ver , invoice_line_items_ccbdgt.fp_desc = invoice_line_items_ccbdgt_tmp.fp_desc , invoice_line_items_ccbdgt.fp_end_dt = invoice_line_items_ccbdgt_tmp.fp_end_dt , invoice_line_items_ccbdgt.fp_id = invoice_line_items_ccbdgt_tmp.fp_id , invoice_line_items_ccbdgt.fp_nm = invoice_line_items_ccbdgt_tmp.fp_nm , invoice_line_items_ccbdgt.fp_obsolete_flg = invoice_line_items_ccbdgt_tmp.fp_obsolete_flg , invoice_line_items_ccbdgt.fp_start_dt = invoice_line_items_ccbdgt_tmp.fp_start_dt , invoice_line_items_ccbdgt.gen_ledger_cd = invoice_line_items_ccbdgt_tmp.gen_ledger_cd , invoice_line_items_ccbdgt.invoice_amt = invoice_line_items_ccbdgt_tmp.invoice_amt , invoice_line_items_ccbdgt.invoice_created_dttm = invoice_line_items_ccbdgt_tmp.invoice_created_dttm , invoice_line_items_ccbdgt.invoice_desc = invoice_line_items_ccbdgt_tmp.invoice_desc , invoice_line_items_ccbdgt.invoice_nm = invoice_line_items_ccbdgt_tmp.invoice_nm , invoice_line_items_ccbdgt.invoice_number = invoice_line_items_ccbdgt_tmp.invoice_number , invoice_line_items_ccbdgt.invoice_reconciled_dttm = invoice_line_items_ccbdgt_tmp.invoice_reconciled_dttm , invoice_line_items_ccbdgt.invoice_status = invoice_line_items_ccbdgt_tmp.invoice_status , invoice_line_items_ccbdgt.item_alloc_amt = invoice_line_items_ccbdgt_tmp.item_alloc_amt , invoice_line_items_ccbdgt.item_alloc_unit = invoice_line_items_ccbdgt_tmp.item_alloc_unit , invoice_line_items_ccbdgt.item_nm = invoice_line_items_ccbdgt_tmp.item_nm , invoice_line_items_ccbdgt.item_qty = invoice_line_items_ccbdgt_tmp.item_qty , invoice_line_items_ccbdgt.item_rate = invoice_line_items_ccbdgt_tmp.item_rate , invoice_line_items_ccbdgt.item_vend_alloc_amt = invoice_line_items_ccbdgt_tmp.item_vend_alloc_amt , invoice_line_items_ccbdgt.item_vend_alloc_unit = invoice_line_items_ccbdgt_tmp.item_vend_alloc_unit , invoice_line_items_ccbdgt.last_modified_dttm = invoice_line_items_ccbdgt_tmp.last_modified_dttm , invoice_line_items_ccbdgt.last_modified_usernm = invoice_line_items_ccbdgt_tmp.last_modified_usernm , invoice_line_items_ccbdgt.load_dttm = invoice_line_items_ccbdgt_tmp.load_dttm , invoice_line_items_ccbdgt.payment_dttm = invoice_line_items_ccbdgt_tmp.payment_dttm , invoice_line_items_ccbdgt.plan_currency_cd = invoice_line_items_ccbdgt_tmp.plan_currency_cd , invoice_line_items_ccbdgt.planning_id = invoice_line_items_ccbdgt_tmp.planning_id , invoice_line_items_ccbdgt.planning_nm = invoice_line_items_ccbdgt_tmp.planning_nm , invoice_line_items_ccbdgt.reconcile_amt = invoice_line_items_ccbdgt_tmp.reconcile_amt , invoice_line_items_ccbdgt.reconcile_note = invoice_line_items_ccbdgt_tmp.reconcile_note , invoice_line_items_ccbdgt.vendor_amt = invoice_line_items_ccbdgt_tmp.vendor_amt , invoice_line_items_ccbdgt.vendor_currency_cd = invoice_line_items_ccbdgt_tmp.vendor_currency_cd , invoice_line_items_ccbdgt.vendor_desc = invoice_line_items_ccbdgt_tmp.vendor_desc , invoice_line_items_ccbdgt.vendor_id = invoice_line_items_ccbdgt_tmp.vendor_id , invoice_line_items_ccbdgt.vendor_nm = invoice_line_items_ccbdgt_tmp.vendor_nm , invoice_line_items_ccbdgt.vendor_number = invoice_line_items_ccbdgt_tmp.vendor_number , invoice_line_items_ccbdgt.vendor_obsolete_flg = invoice_line_items_ccbdgt_tmp.vendor_obsolete_flg
        when not matched then insert ( 
        cc_allocated_amt,cc_available_amt,cc_bdgt_amt,cc_bdgt_budget_amt,cc_bdgt_budget_desc,cc_bdgt_cmtmnt_invoice_amt,cc_bdgt_cmtmnt_invoice_cnt,cc_bdgt_cmtmnt_outstanding_amt,cc_bdgt_cmtmnt_overspent_amt,cc_bdgt_committed_amt,cc_bdgt_direct_invoice_amt,cc_bdgt_invoiced_amt,cc_desc,cc_nm,cc_number,cc_obsolete_flg,cc_owner_usernm,cc_recon_alloc_amt,ccat_nm,cmtmnt_id,cmtmnt_nm,cost_center_id,created_by_usernm,created_dttm,fin_acc_ccat_nm,fin_acc_nm,fp_cls_ver,fp_desc,fp_end_dt,fp_id,fp_nm,fp_obsolete_flg,fp_start_dt,gen_ledger_cd,invoice_amt,invoice_created_dttm,invoice_desc,invoice_id,invoice_nm,invoice_number,invoice_reconciled_dttm,invoice_status,item_alloc_amt,item_alloc_unit,item_nm,item_number,item_qty,item_rate,item_vend_alloc_amt,item_vend_alloc_unit,last_modified_dttm,last_modified_usernm,load_dttm,payment_dttm,plan_currency_cd,planning_id,planning_nm,reconcile_amt,reconcile_note,vendor_amt,vendor_currency_cd,vendor_desc,vendor_id,vendor_nm,vendor_number,vendor_obsolete_flg
         ) values ( 
        invoice_line_items_ccbdgt_tmp.cc_allocated_amt,invoice_line_items_ccbdgt_tmp.cc_available_amt,invoice_line_items_ccbdgt_tmp.cc_bdgt_amt,invoice_line_items_ccbdgt_tmp.cc_bdgt_budget_amt,invoice_line_items_ccbdgt_tmp.cc_bdgt_budget_desc,invoice_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_invoice_amt,invoice_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_invoice_cnt,invoice_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_outstanding_amt,invoice_line_items_ccbdgt_tmp.cc_bdgt_cmtmnt_overspent_amt,invoice_line_items_ccbdgt_tmp.cc_bdgt_committed_amt,invoice_line_items_ccbdgt_tmp.cc_bdgt_direct_invoice_amt,invoice_line_items_ccbdgt_tmp.cc_bdgt_invoiced_amt,invoice_line_items_ccbdgt_tmp.cc_desc,invoice_line_items_ccbdgt_tmp.cc_nm,invoice_line_items_ccbdgt_tmp.cc_number,invoice_line_items_ccbdgt_tmp.cc_obsolete_flg,invoice_line_items_ccbdgt_tmp.cc_owner_usernm,invoice_line_items_ccbdgt_tmp.cc_recon_alloc_amt,invoice_line_items_ccbdgt_tmp.ccat_nm,invoice_line_items_ccbdgt_tmp.cmtmnt_id,invoice_line_items_ccbdgt_tmp.cmtmnt_nm,invoice_line_items_ccbdgt_tmp.cost_center_id,invoice_line_items_ccbdgt_tmp.created_by_usernm,invoice_line_items_ccbdgt_tmp.created_dttm,invoice_line_items_ccbdgt_tmp.fin_acc_ccat_nm,invoice_line_items_ccbdgt_tmp.fin_acc_nm,invoice_line_items_ccbdgt_tmp.fp_cls_ver,invoice_line_items_ccbdgt_tmp.fp_desc,invoice_line_items_ccbdgt_tmp.fp_end_dt,invoice_line_items_ccbdgt_tmp.fp_id,invoice_line_items_ccbdgt_tmp.fp_nm,invoice_line_items_ccbdgt_tmp.fp_obsolete_flg,invoice_line_items_ccbdgt_tmp.fp_start_dt,invoice_line_items_ccbdgt_tmp.gen_ledger_cd,invoice_line_items_ccbdgt_tmp.invoice_amt,invoice_line_items_ccbdgt_tmp.invoice_created_dttm,invoice_line_items_ccbdgt_tmp.invoice_desc,invoice_line_items_ccbdgt_tmp.invoice_id,invoice_line_items_ccbdgt_tmp.invoice_nm,invoice_line_items_ccbdgt_tmp.invoice_number,invoice_line_items_ccbdgt_tmp.invoice_reconciled_dttm,invoice_line_items_ccbdgt_tmp.invoice_status,invoice_line_items_ccbdgt_tmp.item_alloc_amt,invoice_line_items_ccbdgt_tmp.item_alloc_unit,invoice_line_items_ccbdgt_tmp.item_nm,invoice_line_items_ccbdgt_tmp.item_number,invoice_line_items_ccbdgt_tmp.item_qty,invoice_line_items_ccbdgt_tmp.item_rate,invoice_line_items_ccbdgt_tmp.item_vend_alloc_amt,invoice_line_items_ccbdgt_tmp.item_vend_alloc_unit,invoice_line_items_ccbdgt_tmp.last_modified_dttm,invoice_line_items_ccbdgt_tmp.last_modified_usernm,invoice_line_items_ccbdgt_tmp.load_dttm,invoice_line_items_ccbdgt_tmp.payment_dttm,invoice_line_items_ccbdgt_tmp.plan_currency_cd,invoice_line_items_ccbdgt_tmp.planning_id,invoice_line_items_ccbdgt_tmp.planning_nm,invoice_line_items_ccbdgt_tmp.reconcile_amt,invoice_line_items_ccbdgt_tmp.reconcile_note,invoice_line_items_ccbdgt_tmp.vendor_amt,invoice_line_items_ccbdgt_tmp.vendor_currency_cd,invoice_line_items_ccbdgt_tmp.vendor_desc,invoice_line_items_ccbdgt_tmp.vendor_id,invoice_line_items_ccbdgt_tmp.vendor_nm,invoice_line_items_ccbdgt_tmp.vendor_number,invoice_line_items_ccbdgt_tmp.vendor_obsolete_flg
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :invoice_line_items_ccbdgt_tmp   , invoice_line_items_ccbdgt, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..invoice_line_items_ccbdgt_tmp   ;
    quit;
    %put ######## Staging table: invoice_line_items_ccbdgt_tmp    Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..invoice_line_items_ccbdgt;
      drop table work.invoice_line_items_ccbdgt;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..journey_entry_tmp               ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=journey_entry, table_keys=%str(event_id), out_table=work.journey_entry);
 data &tmplib..journey_entry_tmp               ;
     set work.journey_entry;
  if entry_dttm ne . then entry_dttm = tzoneu2s(entry_dttm,&timeZone_Value.);if entry_dttm_tz ne . then entry_dttm_tz = tzoneu2s(entry_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :journey_entry_tmp               , journey_entry);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..journey_entry using &tmpdbschema..journey_entry_tmp               
         on (journey_entry.event_id=journey_entry_tmp.event_id)
        when matched then  
        update set journey_entry.aud_occurrence_id = journey_entry_tmp.aud_occurrence_id , journey_entry.audience_id = journey_entry_tmp.audience_id , journey_entry.entry_dttm = journey_entry_tmp.entry_dttm , journey_entry.entry_dttm_tz = journey_entry_tmp.entry_dttm_tz , journey_entry.event_nm = journey_entry_tmp.event_nm , journey_entry.identity_id = journey_entry_tmp.identity_id , journey_entry.identity_type_nm = journey_entry_tmp.identity_type_nm , journey_entry.identity_type_val = journey_entry_tmp.identity_type_val , journey_entry.journey_id = journey_entry_tmp.journey_id , journey_entry.journey_occurrence_id = journey_entry_tmp.journey_occurrence_id , journey_entry.load_dttm = journey_entry_tmp.load_dttm
        when not matched then insert ( 
        aud_occurrence_id,audience_id,entry_dttm,entry_dttm_tz,event_id,event_nm,identity_id,identity_type_nm,identity_type_val,journey_id,journey_occurrence_id,load_dttm
         ) values ( 
        journey_entry_tmp.aud_occurrence_id,journey_entry_tmp.audience_id,journey_entry_tmp.entry_dttm,journey_entry_tmp.entry_dttm_tz,journey_entry_tmp.event_id,journey_entry_tmp.event_nm,journey_entry_tmp.identity_id,journey_entry_tmp.identity_type_nm,journey_entry_tmp.identity_type_val,journey_entry_tmp.journey_id,journey_entry_tmp.journey_occurrence_id,journey_entry_tmp.load_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :journey_entry_tmp               , journey_entry, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..journey_entry_tmp               ;
    quit;
    %put ######## Staging table: journey_entry_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..journey_entry;
      drop table work.journey_entry;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..journey_exit_tmp                ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=journey_exit, table_keys=%str(event_id), out_table=work.journey_exit);
 data &tmplib..journey_exit_tmp                ;
     set work.journey_exit;
  if exit_dttm ne . then exit_dttm = tzoneu2s(exit_dttm,&timeZone_Value.);if exit_dttm_tz ne . then exit_dttm_tz = tzoneu2s(exit_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :journey_exit_tmp                , journey_exit);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..journey_exit using &tmpdbschema..journey_exit_tmp                
         on (journey_exit.event_id=journey_exit_tmp.event_id)
        when matched then  
        update set journey_exit.aud_occurrence_id = journey_exit_tmp.aud_occurrence_id , journey_exit.audience_id = journey_exit_tmp.audience_id , journey_exit.event_nm = journey_exit_tmp.event_nm , journey_exit.exit_dttm = journey_exit_tmp.exit_dttm , journey_exit.exit_dttm_tz = journey_exit_tmp.exit_dttm_tz , journey_exit.identity_id = journey_exit_tmp.identity_id , journey_exit.identity_type_nm = journey_exit_tmp.identity_type_nm , journey_exit.identity_type_val = journey_exit_tmp.identity_type_val , journey_exit.journey_id = journey_exit_tmp.journey_id , journey_exit.journey_occurrence_id = journey_exit_tmp.journey_occurrence_id , journey_exit.last_node_id = journey_exit_tmp.last_node_id , journey_exit.load_dttm = journey_exit_tmp.load_dttm , journey_exit.reason_cd = journey_exit_tmp.reason_cd , journey_exit.reason_txt = journey_exit_tmp.reason_txt
        when not matched then insert ( 
        aud_occurrence_id,audience_id,event_id,event_nm,exit_dttm,exit_dttm_tz,identity_id,identity_type_nm,identity_type_val,journey_id,journey_occurrence_id,last_node_id,load_dttm,reason_cd,reason_txt
         ) values ( 
        journey_exit_tmp.aud_occurrence_id,journey_exit_tmp.audience_id,journey_exit_tmp.event_id,journey_exit_tmp.event_nm,journey_exit_tmp.exit_dttm,journey_exit_tmp.exit_dttm_tz,journey_exit_tmp.identity_id,journey_exit_tmp.identity_type_nm,journey_exit_tmp.identity_type_val,journey_exit_tmp.journey_id,journey_exit_tmp.journey_occurrence_id,journey_exit_tmp.last_node_id,journey_exit_tmp.load_dttm,journey_exit_tmp.reason_cd,journey_exit_tmp.reason_txt
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :journey_exit_tmp                , journey_exit, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..journey_exit_tmp                ;
    quit;
    %put ######## Staging table: journey_exit_tmp                 Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..journey_exit;
      drop table work.journey_exit;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..journey_holdout_tmp             ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=journey_holdout, table_keys=%str(event_id), out_table=work.journey_holdout);
 data &tmplib..journey_holdout_tmp             ;
     set work.journey_holdout;
  if holdout_dttm ne . then holdout_dttm = tzoneu2s(holdout_dttm,&timeZone_Value.);if holdout_dttm_tz ne . then holdout_dttm_tz = tzoneu2s(holdout_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :journey_holdout_tmp             , journey_holdout);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..journey_holdout using &tmpdbschema..journey_holdout_tmp             
         on (journey_holdout.event_id=journey_holdout_tmp.event_id)
        when matched then  
        update set journey_holdout.aud_occurrence_id = journey_holdout_tmp.aud_occurrence_id , journey_holdout.audience_id = journey_holdout_tmp.audience_id , journey_holdout.event_nm = journey_holdout_tmp.event_nm , journey_holdout.holdout_dttm = journey_holdout_tmp.holdout_dttm , journey_holdout.holdout_dttm_tz = journey_holdout_tmp.holdout_dttm_tz , journey_holdout.identity_id = journey_holdout_tmp.identity_id , journey_holdout.identity_type_nm = journey_holdout_tmp.identity_type_nm , journey_holdout.identity_type_val = journey_holdout_tmp.identity_type_val , journey_holdout.journey_id = journey_holdout_tmp.journey_id , journey_holdout.journey_occurrence_id = journey_holdout_tmp.journey_occurrence_id , journey_holdout.load_dttm = journey_holdout_tmp.load_dttm
        when not matched then insert ( 
        aud_occurrence_id,audience_id,event_id,event_nm,holdout_dttm,holdout_dttm_tz,identity_id,identity_type_nm,identity_type_val,journey_id,journey_occurrence_id,load_dttm
         ) values ( 
        journey_holdout_tmp.aud_occurrence_id,journey_holdout_tmp.audience_id,journey_holdout_tmp.event_id,journey_holdout_tmp.event_nm,journey_holdout_tmp.holdout_dttm,journey_holdout_tmp.holdout_dttm_tz,journey_holdout_tmp.identity_id,journey_holdout_tmp.identity_type_nm,journey_holdout_tmp.identity_type_val,journey_holdout_tmp.journey_id,journey_holdout_tmp.journey_occurrence_id,journey_holdout_tmp.load_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :journey_holdout_tmp             , journey_holdout, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..journey_holdout_tmp             ;
    quit;
    %put ######## Staging table: journey_holdout_tmp              Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..journey_holdout;
      drop table work.journey_holdout;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..journey_node_entry_tmp          ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=journey_node_entry, table_keys=%str(event_id), out_table=work.journey_node_entry);
 data &tmplib..journey_node_entry_tmp          ;
     set work.journey_node_entry;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if node_entry_dttm ne . then node_entry_dttm = tzoneu2s(node_entry_dttm,&timeZone_Value.);if node_entry_dttm_tz ne . then node_entry_dttm_tz = tzoneu2s(node_entry_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :journey_node_entry_tmp          , journey_node_entry);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..journey_node_entry using &tmpdbschema..journey_node_entry_tmp          
         on (journey_node_entry.event_id=journey_node_entry_tmp.event_id)
        when matched then  
        update set journey_node_entry.aud_occurrence_id = journey_node_entry_tmp.aud_occurrence_id , journey_node_entry.audience_id = journey_node_entry_tmp.audience_id , journey_node_entry.event_nm = journey_node_entry_tmp.event_nm , journey_node_entry.identity_id = journey_node_entry_tmp.identity_id , journey_node_entry.identity_type_nm = journey_node_entry_tmp.identity_type_nm , journey_node_entry.identity_type_val = journey_node_entry_tmp.identity_type_val , journey_node_entry.journey_id = journey_node_entry_tmp.journey_id , journey_node_entry.journey_occurrence_id = journey_node_entry_tmp.journey_occurrence_id , journey_node_entry.load_dttm = journey_node_entry_tmp.load_dttm , journey_node_entry.node_entry_dttm = journey_node_entry_tmp.node_entry_dttm , journey_node_entry.node_entry_dttm_tz = journey_node_entry_tmp.node_entry_dttm_tz , journey_node_entry.node_id = journey_node_entry_tmp.node_id , journey_node_entry.node_type_nm = journey_node_entry_tmp.node_type_nm , journey_node_entry.previous_node_id = journey_node_entry_tmp.previous_node_id
        when not matched then insert ( 
        aud_occurrence_id,audience_id,event_id,event_nm,identity_id,identity_type_nm,identity_type_val,journey_id,journey_occurrence_id,load_dttm,node_entry_dttm,node_entry_dttm_tz,node_id,node_type_nm,previous_node_id
         ) values ( 
        journey_node_entry_tmp.aud_occurrence_id,journey_node_entry_tmp.audience_id,journey_node_entry_tmp.event_id,journey_node_entry_tmp.event_nm,journey_node_entry_tmp.identity_id,journey_node_entry_tmp.identity_type_nm,journey_node_entry_tmp.identity_type_val,journey_node_entry_tmp.journey_id,journey_node_entry_tmp.journey_occurrence_id,journey_node_entry_tmp.load_dttm,journey_node_entry_tmp.node_entry_dttm,journey_node_entry_tmp.node_entry_dttm_tz,journey_node_entry_tmp.node_id,journey_node_entry_tmp.node_type_nm,journey_node_entry_tmp.previous_node_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :journey_node_entry_tmp          , journey_node_entry, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..journey_node_entry_tmp          ;
    quit;
    %put ######## Staging table: journey_node_entry_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..journey_node_entry;
      drop table work.journey_node_entry;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..journey_success_tmp             ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=journey_success, table_keys=%str(event_id), out_table=work.journey_success);
 data &tmplib..journey_success_tmp             ;
     set work.journey_success;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if success_dttm ne . then success_dttm = tzoneu2s(success_dttm,&timeZone_Value.);if success_dttm_tz ne . then success_dttm_tz = tzoneu2s(success_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :journey_success_tmp             , journey_success);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..journey_success using &tmpdbschema..journey_success_tmp             
         on (journey_success.event_id=journey_success_tmp.event_id)
        when matched then  
        update set journey_success.aud_occurrence_id = journey_success_tmp.aud_occurrence_id , journey_success.audience_id = journey_success_tmp.audience_id , journey_success.event_nm = journey_success_tmp.event_nm , journey_success.identity_id = journey_success_tmp.identity_id , journey_success.identity_type_nm = journey_success_tmp.identity_type_nm , journey_success.identity_type_val = journey_success_tmp.identity_type_val , journey_success.journey_id = journey_success_tmp.journey_id , journey_success.journey_occurrence_id = journey_success_tmp.journey_occurrence_id , journey_success.load_dttm = journey_success_tmp.load_dttm , journey_success.success_dttm = journey_success_tmp.success_dttm , journey_success.success_dttm_tz = journey_success_tmp.success_dttm_tz , journey_success.success_val = journey_success_tmp.success_val , journey_success.unit_qty = journey_success_tmp.unit_qty
        when not matched then insert ( 
        aud_occurrence_id,audience_id,event_id,event_nm,identity_id,identity_type_nm,identity_type_val,journey_id,journey_occurrence_id,load_dttm,success_dttm,success_dttm_tz,success_val,unit_qty
         ) values ( 
        journey_success_tmp.aud_occurrence_id,journey_success_tmp.audience_id,journey_success_tmp.event_id,journey_success_tmp.event_nm,journey_success_tmp.identity_id,journey_success_tmp.identity_type_nm,journey_success_tmp.identity_type_val,journey_success_tmp.journey_id,journey_success_tmp.journey_occurrence_id,journey_success_tmp.load_dttm,journey_success_tmp.success_dttm,journey_success_tmp.success_dttm_tz,journey_success_tmp.success_val,journey_success_tmp.unit_qty
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :journey_success_tmp             , journey_success, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..journey_success_tmp             ;
    quit;
    %put ######## Staging table: journey_success_tmp              Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..journey_success;
      drop table work.journey_success;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..journey_suppression_tmp         ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=journey_suppression, table_keys=%str(event_id), out_table=work.journey_suppression);
 data &tmplib..journey_suppression_tmp         ;
     set work.journey_suppression;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if suppression_dttm ne . then suppression_dttm = tzoneu2s(suppression_dttm,&timeZone_Value.);if suppression_dttm_tz ne . then suppression_dttm_tz = tzoneu2s(suppression_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :journey_suppression_tmp         , journey_suppression);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..journey_suppression using &tmpdbschema..journey_suppression_tmp         
         on (journey_suppression.event_id=journey_suppression_tmp.event_id)
        when matched then  
        update set journey_suppression.aud_occurrence_id = journey_suppression_tmp.aud_occurrence_id , journey_suppression.audience_id = journey_suppression_tmp.audience_id , journey_suppression.event_nm = journey_suppression_tmp.event_nm , journey_suppression.identity_id = journey_suppression_tmp.identity_id , journey_suppression.identity_type_nm = journey_suppression_tmp.identity_type_nm , journey_suppression.identity_type_val = journey_suppression_tmp.identity_type_val , journey_suppression.journey_id = journey_suppression_tmp.journey_id , journey_suppression.journey_occurrence_id = journey_suppression_tmp.journey_occurrence_id , journey_suppression.load_dttm = journey_suppression_tmp.load_dttm , journey_suppression.reason_cd = journey_suppression_tmp.reason_cd , journey_suppression.reason_txt = journey_suppression_tmp.reason_txt , journey_suppression.suppression_dttm = journey_suppression_tmp.suppression_dttm , journey_suppression.suppression_dttm_tz = journey_suppression_tmp.suppression_dttm_tz
        when not matched then insert ( 
        aud_occurrence_id,audience_id,event_id,event_nm,identity_id,identity_type_nm,identity_type_val,journey_id,journey_occurrence_id,load_dttm,reason_cd,reason_txt,suppression_dttm,suppression_dttm_tz
         ) values ( 
        journey_suppression_tmp.aud_occurrence_id,journey_suppression_tmp.audience_id,journey_suppression_tmp.event_id,journey_suppression_tmp.event_nm,journey_suppression_tmp.identity_id,journey_suppression_tmp.identity_type_nm,journey_suppression_tmp.identity_type_val,journey_suppression_tmp.journey_id,journey_suppression_tmp.journey_occurrence_id,journey_suppression_tmp.load_dttm,journey_suppression_tmp.reason_cd,journey_suppression_tmp.reason_txt,journey_suppression_tmp.suppression_dttm,journey_suppression_tmp.suppression_dttm_tz
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :journey_suppression_tmp         , journey_suppression, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..journey_suppression_tmp         ;
    quit;
    %put ######## Staging table: journey_suppression_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..journey_suppression;
      drop table work.journey_suppression;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_activity_tmp                 ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_activity, table_keys=%str(activity_version_id), out_table=work.md_activity);
 data &tmplib..md_activity_tmp                 ;
     set work.md_activity;
  if last_published_dttm ne . then last_published_dttm = tzoneu2s(last_published_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if activity_version_id='' then activity_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_activity_tmp                 , md_activity);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_activity using &tmpdbschema..md_activity_tmp                 
         on (md_activity.activity_version_id=md_activity_tmp.activity_version_id)
        when matched then  
        update set md_activity.activity_category_nm = md_activity_tmp.activity_category_nm , md_activity.activity_cd = md_activity_tmp.activity_cd , md_activity.activity_desc = md_activity_tmp.activity_desc , md_activity.activity_id = md_activity_tmp.activity_id , md_activity.activity_nm = md_activity_tmp.activity_nm , md_activity.activity_status_cd = md_activity_tmp.activity_status_cd , md_activity.business_context_id = md_activity_tmp.business_context_id , md_activity.folder_path_nm = md_activity_tmp.folder_path_nm , md_activity.last_published_dttm = md_activity_tmp.last_published_dttm , md_activity.valid_from_dttm = md_activity_tmp.valid_from_dttm , md_activity.valid_to_dttm = md_activity_tmp.valid_to_dttm
        when not matched then insert ( 
        activity_category_nm,activity_cd,activity_desc,activity_id,activity_nm,activity_status_cd,activity_version_id,business_context_id,folder_path_nm,last_published_dttm,valid_from_dttm,valid_to_dttm
         ) values ( 
        md_activity_tmp.activity_category_nm,md_activity_tmp.activity_cd,md_activity_tmp.activity_desc,md_activity_tmp.activity_id,md_activity_tmp.activity_nm,md_activity_tmp.activity_status_cd,md_activity_tmp.activity_version_id,md_activity_tmp.business_context_id,md_activity_tmp.folder_path_nm,md_activity_tmp.last_published_dttm,md_activity_tmp.valid_from_dttm,md_activity_tmp.valid_to_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_activity_tmp                 , md_activity, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_activity_tmp                 ;
    quit;
    %put ######## Staging table: md_activity_tmp                  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_activity;
      drop table work.md_activity;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_activity_abtestpath_tmp      ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_activity_abtestpath, table_keys=%str(abtest_path_id,activity_node_id,activity_version_id), out_table=work.md_activity_abtestpath);
 data &tmplib..md_activity_abtestpath_tmp      ;
     set work.md_activity_abtestpath;
  if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if abtest_path_id='' then abtest_path_id='-'; if activity_node_id='' then activity_node_id='-'; if activity_version_id='' then activity_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_activity_abtestpath_tmp      , md_activity_abtestpath);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_activity_abtestpath using &tmpdbschema..md_activity_abtestpath_tmp      
         on (md_activity_abtestpath.abtest_path_id=md_activity_abtestpath_tmp.abtest_path_id and md_activity_abtestpath.activity_node_id=md_activity_abtestpath_tmp.activity_node_id and md_activity_abtestpath.activity_version_id=md_activity_abtestpath_tmp.activity_version_id)
        when matched then  
        update set md_activity_abtestpath.abtest_dist_pct = md_activity_abtestpath_tmp.abtest_dist_pct , md_activity_abtestpath.abtest_path_nm = md_activity_abtestpath_tmp.abtest_path_nm , md_activity_abtestpath.activity_id = md_activity_abtestpath_tmp.activity_id , md_activity_abtestpath.activity_status_cd = md_activity_abtestpath_tmp.activity_status_cd , md_activity_abtestpath.control_flg = md_activity_abtestpath_tmp.control_flg , md_activity_abtestpath.next_node_val = md_activity_abtestpath_tmp.next_node_val , md_activity_abtestpath.valid_from_dttm = md_activity_abtestpath_tmp.valid_from_dttm , md_activity_abtestpath.valid_to_dttm = md_activity_abtestpath_tmp.valid_to_dttm
        when not matched then insert ( 
        abtest_dist_pct,abtest_path_id,abtest_path_nm,activity_id,activity_node_id,activity_status_cd,activity_version_id,control_flg,next_node_val,valid_from_dttm,valid_to_dttm
         ) values ( 
        md_activity_abtestpath_tmp.abtest_dist_pct,md_activity_abtestpath_tmp.abtest_path_id,md_activity_abtestpath_tmp.abtest_path_nm,md_activity_abtestpath_tmp.activity_id,md_activity_abtestpath_tmp.activity_node_id,md_activity_abtestpath_tmp.activity_status_cd,md_activity_abtestpath_tmp.activity_version_id,md_activity_abtestpath_tmp.control_flg,md_activity_abtestpath_tmp.next_node_val,md_activity_abtestpath_tmp.valid_from_dttm,md_activity_abtestpath_tmp.valid_to_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_activity_abtestpath_tmp      , md_activity_abtestpath, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_activity_abtestpath_tmp      ;
    quit;
    %put ######## Staging table: md_activity_abtestpath_tmp       Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_activity_abtestpath;
      drop table work.md_activity_abtestpath;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_activity_custom_prop_tmp     ;
      quit;
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
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_activity_custom_prop using &tmpdbschema..md_activity_custom_prop_tmp     
         on (md_activity_custom_prop.Hashed_pk_col = md_activity_custom_prop_tmp.Hashed_pk_col)
        when matched then  
        update set md_activity_custom_prop.activity_id = md_activity_custom_prop_tmp.activity_id , md_activity_custom_prop.activity_status_cd = md_activity_custom_prop_tmp.activity_status_cd , md_activity_custom_prop.valid_from_dttm = md_activity_custom_prop_tmp.valid_from_dttm , md_activity_custom_prop.valid_to_dttm = md_activity_custom_prop_tmp.valid_to_dttm
        when not matched then insert ( 
        activity_id,activity_status_cd,activity_version_id,property_datatype_cd,property_nm,property_val,valid_from_dttm,valid_to_dttm
        ,Hashed_pk_col ) values ( 
        md_activity_custom_prop_tmp.activity_id,md_activity_custom_prop_tmp.activity_status_cd,md_activity_custom_prop_tmp.activity_version_id,md_activity_custom_prop_tmp.property_datatype_cd,md_activity_custom_prop_tmp.property_nm,md_activity_custom_prop_tmp.property_val,md_activity_custom_prop_tmp.valid_from_dttm,md_activity_custom_prop_tmp.valid_to_dttm,md_activity_custom_prop_tmp.Hashed_pk_col
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_activity_custom_prop_tmp     , md_activity_custom_prop, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_activity_custom_prop_tmp     ;
    quit;
    %put ######## Staging table: md_activity_custom_prop_tmp      Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_activity_custom_prop;
      drop table work.md_activity_custom_prop;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_activity_node_tmp            ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_activity_node, table_keys=%str(activity_node_id,activity_version_id), out_table=work.md_activity_node);
 data &tmplib..md_activity_node_tmp            ;
     set work.md_activity_node;
  if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if activity_node_id='' then activity_node_id='-'; if activity_version_id='' then activity_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_activity_node_tmp            , md_activity_node);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_activity_node using &tmpdbschema..md_activity_node_tmp            
         on (md_activity_node.activity_node_id=md_activity_node_tmp.activity_node_id and md_activity_node.activity_version_id=md_activity_node_tmp.activity_version_id)
        when matched then  
        update set md_activity_node.abtest_id = md_activity_node_tmp.abtest_id , md_activity_node.activity_id = md_activity_node_tmp.activity_id , md_activity_node.activity_node_nm = md_activity_node_tmp.activity_node_nm , md_activity_node.activity_node_type_nm = md_activity_node_tmp.activity_node_type_nm , md_activity_node.activity_status_cd = md_activity_node_tmp.activity_status_cd , md_activity_node.end_node_flg = md_activity_node_tmp.end_node_flg , md_activity_node.next_node_val = md_activity_node_tmp.next_node_val , md_activity_node.node_sequence_no = md_activity_node_tmp.node_sequence_no , md_activity_node.previous_node_val = md_activity_node_tmp.previous_node_val , md_activity_node.specific_wait_flg = md_activity_node_tmp.specific_wait_flg , md_activity_node.start_node_flg = md_activity_node_tmp.start_node_flg , md_activity_node.time_boxed_flg = md_activity_node_tmp.time_boxed_flg , md_activity_node.valid_from_dttm = md_activity_node_tmp.valid_from_dttm , md_activity_node.valid_to_dttm = md_activity_node_tmp.valid_to_dttm , md_activity_node.wait_tm = md_activity_node_tmp.wait_tm
        when not matched then insert ( 
        abtest_id,activity_id,activity_node_id,activity_node_nm,activity_node_type_nm,activity_status_cd,activity_version_id,end_node_flg,next_node_val,node_sequence_no,previous_node_val,specific_wait_flg,start_node_flg,time_boxed_flg,valid_from_dttm,valid_to_dttm,wait_tm
         ) values ( 
        md_activity_node_tmp.abtest_id,md_activity_node_tmp.activity_id,md_activity_node_tmp.activity_node_id,md_activity_node_tmp.activity_node_nm,md_activity_node_tmp.activity_node_type_nm,md_activity_node_tmp.activity_status_cd,md_activity_node_tmp.activity_version_id,md_activity_node_tmp.end_node_flg,md_activity_node_tmp.next_node_val,md_activity_node_tmp.node_sequence_no,md_activity_node_tmp.previous_node_val,md_activity_node_tmp.specific_wait_flg,md_activity_node_tmp.start_node_flg,md_activity_node_tmp.time_boxed_flg,md_activity_node_tmp.valid_from_dttm,md_activity_node_tmp.valid_to_dttm,md_activity_node_tmp.wait_tm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_activity_node_tmp            , md_activity_node, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_activity_node_tmp            ;
    quit;
    %put ######## Staging table: md_activity_node_tmp             Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_activity_node;
      drop table work.md_activity_node;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_activity_x_activity_node_tmp ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_activity_x_activity_node, table_keys=%str(activity_node_id,activity_version_id), out_table=work.md_activity_x_activity_node);
 data &tmplib..md_activity_x_activity_node_tmp ;
     set work.md_activity_x_activity_node;
  if activity_node_id='' then activity_node_id='-'; if activity_version_id='' then activity_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_activity_x_activity_node_tmp , md_activity_x_activity_node);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_activity_x_activity_node using &tmpdbschema..md_activity_x_activity_node_tmp 
         on (md_activity_x_activity_node.activity_node_id=md_activity_x_activity_node_tmp.activity_node_id and md_activity_x_activity_node.activity_version_id=md_activity_x_activity_node_tmp.activity_version_id)
        when matched then  
        update set md_activity_x_activity_node.activity_id = md_activity_x_activity_node_tmp.activity_id , md_activity_x_activity_node.activity_status_cd = md_activity_x_activity_node_tmp.activity_status_cd
        when not matched then insert ( 
        activity_id,activity_node_id,activity_status_cd,activity_version_id
         ) values ( 
        md_activity_x_activity_node_tmp.activity_id,md_activity_x_activity_node_tmp.activity_node_id,md_activity_x_activity_node_tmp.activity_status_cd,md_activity_x_activity_node_tmp.activity_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_activity_x_activity_node_tmp , md_activity_x_activity_node, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_activity_x_activity_node_tmp ;
    quit;
    %put ######## Staging table: md_activity_x_activity_node_tmp  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_activity_x_activity_node;
      drop table work.md_activity_x_activity_node;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_activity_x_task_tmp          ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_activity_x_task, table_keys=%str(activity_version_id,task_id), out_table=work.md_activity_x_task);
 data &tmplib..md_activity_x_task_tmp          ;
     set work.md_activity_x_task;
  if activity_version_id='' then activity_version_id='-'; if task_id='' then task_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_activity_x_task_tmp          , md_activity_x_task);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_activity_x_task using &tmpdbschema..md_activity_x_task_tmp          
         on (md_activity_x_task.activity_version_id=md_activity_x_task_tmp.activity_version_id and md_activity_x_task.task_id=md_activity_x_task_tmp.task_id)
        when matched then  
        update set md_activity_x_task.activity_id = md_activity_x_task_tmp.activity_id , md_activity_x_task.activity_status_cd = md_activity_x_task_tmp.activity_status_cd , md_activity_x_task.task_version_id = md_activity_x_task_tmp.task_version_id
        when not matched then insert ( 
        activity_id,activity_status_cd,activity_version_id,task_id,task_version_id
         ) values ( 
        md_activity_x_task_tmp.activity_id,md_activity_x_task_tmp.activity_status_cd,md_activity_x_task_tmp.activity_version_id,md_activity_x_task_tmp.task_id,md_activity_x_task_tmp.task_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_activity_x_task_tmp          , md_activity_x_task, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_activity_x_task_tmp          ;
    quit;
    %put ######## Staging table: md_activity_x_task_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_activity_x_task;
      drop table work.md_activity_x_task;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_asset_tmp                    ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_asset, table_keys=%str(asset_version_id), out_table=work.md_asset);
 data &tmplib..md_asset_tmp                    ;
     set work.md_asset;
  if last_published_dttm ne . then last_published_dttm = tzoneu2s(last_published_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if asset_version_id='' then asset_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_asset_tmp                    , md_asset);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_asset using &tmpdbschema..md_asset_tmp                    
         on (md_asset.asset_version_id=md_asset_tmp.asset_version_id)
        when matched then  
        update set md_asset.asset_desc = md_asset_tmp.asset_desc , md_asset.asset_id = md_asset_tmp.asset_id , md_asset.asset_nm = md_asset_tmp.asset_nm , md_asset.asset_status_cd = md_asset_tmp.asset_status_cd , md_asset.asset_type_nm = md_asset_tmp.asset_type_nm , md_asset.created_user_nm = md_asset_tmp.created_user_nm , md_asset.last_published_dttm = md_asset_tmp.last_published_dttm , md_asset.owner_nm = md_asset_tmp.owner_nm , md_asset.valid_from_dttm = md_asset_tmp.valid_from_dttm , md_asset.valid_to_dttm = md_asset_tmp.valid_to_dttm
        when not matched then insert ( 
        asset_desc,asset_id,asset_nm,asset_status_cd,asset_type_nm,asset_version_id,created_user_nm,last_published_dttm,owner_nm,valid_from_dttm,valid_to_dttm
         ) values ( 
        md_asset_tmp.asset_desc,md_asset_tmp.asset_id,md_asset_tmp.asset_nm,md_asset_tmp.asset_status_cd,md_asset_tmp.asset_type_nm,md_asset_tmp.asset_version_id,md_asset_tmp.created_user_nm,md_asset_tmp.last_published_dttm,md_asset_tmp.owner_nm,md_asset_tmp.valid_from_dttm,md_asset_tmp.valid_to_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_asset_tmp                    , md_asset, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_asset_tmp                    ;
    quit;
    %put ######## Staging table: md_asset_tmp                     Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_asset;
      drop table work.md_asset;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_audience_tmp                 ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_audience, table_keys=%str(audience_id), out_table=work.md_audience);
 data &tmplib..md_audience_tmp                 ;
     set work.md_audience;
  if create_dttm ne . then create_dttm = tzoneu2s(create_dttm,&timeZone_Value.);if delete_dttm ne . then delete_dttm = tzoneu2s(delete_dttm,&timeZone_Value.) ;
  if audience_id='' then audience_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_audience_tmp                 , md_audience);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_audience using &tmpdbschema..md_audience_tmp                 
         on (md_audience.audience_id=md_audience_tmp.audience_id)
        when matched then  
        update set md_audience.audience_data_source_nm = md_audience_tmp.audience_data_source_nm , md_audience.audience_desc = md_audience_tmp.audience_desc , md_audience.audience_expiration_val = md_audience_tmp.audience_expiration_val , md_audience.audience_nm = md_audience_tmp.audience_nm , md_audience.audience_schedule_flg = md_audience_tmp.audience_schedule_flg , md_audience.audience_source_nm = md_audience_tmp.audience_source_nm , md_audience.create_dttm = md_audience_tmp.create_dttm , md_audience.created_user_nm = md_audience_tmp.created_user_nm , md_audience.delete_dttm = md_audience_tmp.delete_dttm
        when not matched then insert ( 
        audience_data_source_nm,audience_desc,audience_expiration_val,audience_id,audience_nm,audience_schedule_flg,audience_source_nm,create_dttm,created_user_nm,delete_dttm
         ) values ( 
        md_audience_tmp.audience_data_source_nm,md_audience_tmp.audience_desc,md_audience_tmp.audience_expiration_val,md_audience_tmp.audience_id,md_audience_tmp.audience_nm,md_audience_tmp.audience_schedule_flg,md_audience_tmp.audience_source_nm,md_audience_tmp.create_dttm,md_audience_tmp.created_user_nm,md_audience_tmp.delete_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_audience_tmp                 , md_audience, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_audience_tmp                 ;
    quit;
    %put ######## Staging table: md_audience_tmp                  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_audience;
      drop table work.md_audience;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_audience_occurrence_tmp      ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_audience_occurrence, table_keys=%str(aud_occurrence_id), out_table=work.md_audience_occurrence);
 data &tmplib..md_audience_occurrence_tmp      ;
     set work.md_audience_occurrence;
  if end_tm ne . then end_tm = tzoneu2s(end_tm,&timeZone_Value.);if start_tm ne . then start_tm = tzoneu2s(start_tm,&timeZone_Value.) ;
  if aud_occurrence_id='' then aud_occurrence_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_audience_occurrence_tmp      , md_audience_occurrence);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_audience_occurrence using &tmpdbschema..md_audience_occurrence_tmp      
         on (md_audience_occurrence.aud_occurrence_id=md_audience_occurrence_tmp.aud_occurrence_id)
        when matched then  
        update set md_audience_occurrence.audience_id = md_audience_occurrence_tmp.audience_id , md_audience_occurrence.audience_size_val = md_audience_occurrence_tmp.audience_size_val , md_audience_occurrence.end_tm = md_audience_occurrence_tmp.end_tm , md_audience_occurrence.execution_status_cd = md_audience_occurrence_tmp.execution_status_cd , md_audience_occurrence.occurrence_type_nm = md_audience_occurrence_tmp.occurrence_type_nm , md_audience_occurrence.start_tm = md_audience_occurrence_tmp.start_tm , md_audience_occurrence.started_by_nm = md_audience_occurrence_tmp.started_by_nm
        when not matched then insert ( 
        aud_occurrence_id,audience_id,audience_size_val,end_tm,execution_status_cd,occurrence_type_nm,start_tm,started_by_nm
         ) values ( 
        md_audience_occurrence_tmp.aud_occurrence_id,md_audience_occurrence_tmp.audience_id,md_audience_occurrence_tmp.audience_size_val,md_audience_occurrence_tmp.end_tm,md_audience_occurrence_tmp.execution_status_cd,md_audience_occurrence_tmp.occurrence_type_nm,md_audience_occurrence_tmp.start_tm,md_audience_occurrence_tmp.started_by_nm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_audience_occurrence_tmp      , md_audience_occurrence, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_audience_occurrence_tmp      ;
    quit;
    %put ######## Staging table: md_audience_occurrence_tmp       Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_audience_occurrence;
      drop table work.md_audience_occurrence;
  quit;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table md_audience_occurrence;
%put------------------------------------------------------------------;
%if %sysfunc(exist(&udmmart..md_bu) ) %then %do;
 %let errFlag=0;
 %let nrows=0;
 %if %sysfunc(exist(&tmplib..md_bu_tmp                       ) ) %then %do;
      proc sql noerrorstop;
        drop table &tmplib..md_bu_tmp                       ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_bu, table_keys=%str(bu_id), out_table=work.md_bu);
 data &tmplib..md_bu_tmp                       ;
     set work.md_bu;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if bu_id='' then bu_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_bu_tmp                       , md_bu);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_bu using &tmpdbschema..md_bu_tmp                       
         on (md_bu.bu_id=md_bu_tmp.bu_id)
        when matched then  
        update set md_bu.bu_currency_cd = md_bu_tmp.bu_currency_cd , md_bu.bu_desc = md_bu_tmp.bu_desc , md_bu.bu_nm = md_bu_tmp.bu_nm , md_bu.bu_obsolete_flg = md_bu_tmp.bu_obsolete_flg , md_bu.bu_owner_usernm = md_bu_tmp.bu_owner_usernm , md_bu.bu_parentid = md_bu_tmp.bu_parentid , md_bu.created_by_usernm = md_bu_tmp.created_by_usernm , md_bu.created_dttm = md_bu_tmp.created_dttm , md_bu.last_modified_dttm = md_bu_tmp.last_modified_dttm , md_bu.last_modified_usernm = md_bu_tmp.last_modified_usernm , md_bu.load_dttm = md_bu_tmp.load_dttm
        when not matched then insert ( 
        bu_currency_cd,bu_desc,bu_id,bu_nm,bu_obsolete_flg,bu_owner_usernm,bu_parentid,created_by_usernm,created_dttm,last_modified_dttm,last_modified_usernm,load_dttm
         ) values ( 
        md_bu_tmp.bu_currency_cd,md_bu_tmp.bu_desc,md_bu_tmp.bu_id,md_bu_tmp.bu_nm,md_bu_tmp.bu_obsolete_flg,md_bu_tmp.bu_owner_usernm,md_bu_tmp.bu_parentid,md_bu_tmp.created_by_usernm,md_bu_tmp.created_dttm,md_bu_tmp.last_modified_dttm,md_bu_tmp.last_modified_usernm,md_bu_tmp.load_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_bu_tmp                       , md_bu, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_bu_tmp                       ;
    quit;
    %put ######## Staging table: md_bu_tmp                        Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_bu;
      drop table work.md_bu;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_business_context_tmp         ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_business_context, table_keys=%str(business_context_version_id), out_table=work.md_business_context);
 data &tmplib..md_business_context_tmp         ;
     set work.md_business_context;
  if last_published_dttm ne . then last_published_dttm = tzoneu2s(last_published_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if business_context_version_id='' then business_context_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_business_context_tmp         , md_business_context);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_business_context using &tmpdbschema..md_business_context_tmp         
         on (md_business_context.business_context_version_id=md_business_context_tmp.business_context_version_id)
        when matched then  
        update set md_business_context.business_context_desc = md_business_context_tmp.business_context_desc , md_business_context.business_context_id = md_business_context_tmp.business_context_id , md_business_context.business_context_nm = md_business_context_tmp.business_context_nm , md_business_context.business_context_src_cd = md_business_context_tmp.business_context_src_cd , md_business_context.business_context_status_cd = md_business_context_tmp.business_context_status_cd , md_business_context.created_user_nm = md_business_context_tmp.created_user_nm , md_business_context.information_map_nm = md_business_context_tmp.information_map_nm , md_business_context.last_published_dttm = md_business_context_tmp.last_published_dttm , md_business_context.locked_information_map_nm = md_business_context_tmp.locked_information_map_nm , md_business_context.owner_nm = md_business_context_tmp.owner_nm , md_business_context.valid_from_dttm = md_business_context_tmp.valid_from_dttm , md_business_context.valid_to_dttm = md_business_context_tmp.valid_to_dttm
        when not matched then insert ( 
        business_context_desc,business_context_id,business_context_nm,business_context_src_cd,business_context_status_cd,business_context_version_id,created_user_nm,information_map_nm,last_published_dttm,locked_information_map_nm,owner_nm,valid_from_dttm,valid_to_dttm
         ) values ( 
        md_business_context_tmp.business_context_desc,md_business_context_tmp.business_context_id,md_business_context_tmp.business_context_nm,md_business_context_tmp.business_context_src_cd,md_business_context_tmp.business_context_status_cd,md_business_context_tmp.business_context_version_id,md_business_context_tmp.created_user_nm,md_business_context_tmp.information_map_nm,md_business_context_tmp.last_published_dttm,md_business_context_tmp.locked_information_map_nm,md_business_context_tmp.owner_nm,md_business_context_tmp.valid_from_dttm,md_business_context_tmp.valid_to_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_business_context_tmp         , md_business_context, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_business_context_tmp         ;
    quit;
    %put ######## Staging table: md_business_context_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_business_context;
      drop table work.md_business_context;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_cost_category_tmp            ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_cost_category, table_keys=%str(ccat_id), out_table=work.md_cost_category);
 data &tmplib..md_cost_category_tmp            ;
     set work.md_cost_category;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if ccat_id='' then ccat_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_cost_category_tmp            , md_cost_category);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_cost_category using &tmpdbschema..md_cost_category_tmp            
         on (md_cost_category.ccat_id=md_cost_category_tmp.ccat_id)
        when matched then  
        update set md_cost_category.ccat_desc = md_cost_category_tmp.ccat_desc , md_cost_category.ccat_nm = md_cost_category_tmp.ccat_nm , md_cost_category.ccat_obsolete_flg = md_cost_category_tmp.ccat_obsolete_flg , md_cost_category.ccat_owner_usernm = md_cost_category_tmp.ccat_owner_usernm , md_cost_category.created_by_usernm = md_cost_category_tmp.created_by_usernm , md_cost_category.created_dttm = md_cost_category_tmp.created_dttm , md_cost_category.fin_accnt_nm = md_cost_category_tmp.fin_accnt_nm , md_cost_category.gen_ledger_cd = md_cost_category_tmp.gen_ledger_cd , md_cost_category.last_modified_dttm = md_cost_category_tmp.last_modified_dttm , md_cost_category.last_modified_usernm = md_cost_category_tmp.last_modified_usernm , md_cost_category.load_dttm = md_cost_category_tmp.load_dttm
        when not matched then insert ( 
        ccat_desc,ccat_id,ccat_nm,ccat_obsolete_flg,ccat_owner_usernm,created_by_usernm,created_dttm,fin_accnt_nm,gen_ledger_cd,last_modified_dttm,last_modified_usernm,load_dttm
         ) values ( 
        md_cost_category_tmp.ccat_desc,md_cost_category_tmp.ccat_id,md_cost_category_tmp.ccat_nm,md_cost_category_tmp.ccat_obsolete_flg,md_cost_category_tmp.ccat_owner_usernm,md_cost_category_tmp.created_by_usernm,md_cost_category_tmp.created_dttm,md_cost_category_tmp.fin_accnt_nm,md_cost_category_tmp.gen_ledger_cd,md_cost_category_tmp.last_modified_dttm,md_cost_category_tmp.last_modified_usernm,md_cost_category_tmp.load_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_cost_category_tmp            , md_cost_category, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_cost_category_tmp            ;
    quit;
    %put ######## Staging table: md_cost_category_tmp             Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_cost_category;
      drop table work.md_cost_category;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_costcenter_tmp               ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_costcenter, table_keys=%str(cost_center_id), out_table=work.md_costcenter);
 data &tmplib..md_costcenter_tmp               ;
     set work.md_costcenter;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if cost_center_id='' then cost_center_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_costcenter_tmp               , md_costcenter);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_costcenter using &tmpdbschema..md_costcenter_tmp               
         on (md_costcenter.cost_center_id=md_costcenter_tmp.cost_center_id)
        when matched then  
        update set md_costcenter.cc_desc = md_costcenter_tmp.cc_desc , md_costcenter.cc_nm = md_costcenter_tmp.cc_nm , md_costcenter.cc_obsolete_flg = md_costcenter_tmp.cc_obsolete_flg , md_costcenter.cc_owner_usernm = md_costcenter_tmp.cc_owner_usernm , md_costcenter.created_by_usernm = md_costcenter_tmp.created_by_usernm , md_costcenter.created_dttm = md_costcenter_tmp.created_dttm , md_costcenter.fin_accnt_nm = md_costcenter_tmp.fin_accnt_nm , md_costcenter.gen_ledger_cd = md_costcenter_tmp.gen_ledger_cd , md_costcenter.last_modified_dttm = md_costcenter_tmp.last_modified_dttm , md_costcenter.last_modified_usernm = md_costcenter_tmp.last_modified_usernm , md_costcenter.load_dttm = md_costcenter_tmp.load_dttm
        when not matched then insert ( 
        cc_desc,cc_nm,cc_obsolete_flg,cc_owner_usernm,cost_center_id,created_by_usernm,created_dttm,fin_accnt_nm,gen_ledger_cd,last_modified_dttm,last_modified_usernm,load_dttm
         ) values ( 
        md_costcenter_tmp.cc_desc,md_costcenter_tmp.cc_nm,md_costcenter_tmp.cc_obsolete_flg,md_costcenter_tmp.cc_owner_usernm,md_costcenter_tmp.cost_center_id,md_costcenter_tmp.created_by_usernm,md_costcenter_tmp.created_dttm,md_costcenter_tmp.fin_accnt_nm,md_costcenter_tmp.gen_ledger_cd,md_costcenter_tmp.last_modified_dttm,md_costcenter_tmp.last_modified_usernm,md_costcenter_tmp.load_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_costcenter_tmp               , md_costcenter, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_costcenter_tmp               ;
    quit;
    %put ######## Staging table: md_costcenter_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_costcenter;
      drop table work.md_costcenter;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_creative_tmp                 ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_creative, table_keys=%str(creative_version_id), out_table=work.md_creative);
 data &tmplib..md_creative_tmp                 ;
     set work.md_creative;
  if last_published_dttm ne . then last_published_dttm = tzoneu2s(last_published_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if creative_version_id='' then creative_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_creative_tmp                 , md_creative);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_creative using &tmpdbschema..md_creative_tmp                 
         on (md_creative.creative_version_id=md_creative_tmp.creative_version_id)
        when matched then  
        update set md_creative.business_context_id = md_creative_tmp.business_context_id , md_creative.created_user_nm = md_creative_tmp.created_user_nm , md_creative.creative_category_nm = md_creative_tmp.creative_category_nm , md_creative.creative_cd = md_creative_tmp.creative_cd , md_creative.creative_desc = md_creative_tmp.creative_desc , md_creative.creative_id = md_creative_tmp.creative_id , md_creative.creative_nm = md_creative_tmp.creative_nm , md_creative.creative_status_cd = md_creative_tmp.creative_status_cd , md_creative.creative_txt = md_creative_tmp.creative_txt , md_creative.creative_type_nm = md_creative_tmp.creative_type_nm , md_creative.folder_path_nm = md_creative_tmp.folder_path_nm , md_creative.last_published_dttm = md_creative_tmp.last_published_dttm , md_creative.owner_nm = md_creative_tmp.owner_nm , md_creative.recommender_template_id = md_creative_tmp.recommender_template_id , md_creative.recommender_template_nm = md_creative_tmp.recommender_template_nm , md_creative.valid_from_dttm = md_creative_tmp.valid_from_dttm , md_creative.valid_to_dttm = md_creative_tmp.valid_to_dttm
        when not matched then insert ( 
        business_context_id,created_user_nm,creative_category_nm,creative_cd,creative_desc,creative_id,creative_nm,creative_status_cd,creative_txt,creative_type_nm,creative_version_id,folder_path_nm,last_published_dttm,owner_nm,recommender_template_id,recommender_template_nm,valid_from_dttm,valid_to_dttm
         ) values ( 
        md_creative_tmp.business_context_id,md_creative_tmp.created_user_nm,md_creative_tmp.creative_category_nm,md_creative_tmp.creative_cd,md_creative_tmp.creative_desc,md_creative_tmp.creative_id,md_creative_tmp.creative_nm,md_creative_tmp.creative_status_cd,md_creative_tmp.creative_txt,md_creative_tmp.creative_type_nm,md_creative_tmp.creative_version_id,md_creative_tmp.folder_path_nm,md_creative_tmp.last_published_dttm,md_creative_tmp.owner_nm,md_creative_tmp.recommender_template_id,md_creative_tmp.recommender_template_nm,md_creative_tmp.valid_from_dttm,md_creative_tmp.valid_to_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_creative_tmp                 , md_creative, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_creative_tmp                 ;
    quit;
    %put ######## Staging table: md_creative_tmp                  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_creative;
      drop table work.md_creative;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_creative_custom_prop_tmp     ;
      quit;
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
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_creative_custom_prop using &tmpdbschema..md_creative_custom_prop_tmp     
         on (md_creative_custom_prop.Hashed_pk_col = md_creative_custom_prop_tmp.Hashed_pk_col)
        when matched then  
        update set md_creative_custom_prop.creative_id = md_creative_custom_prop_tmp.creative_id , md_creative_custom_prop.creative_status_cd = md_creative_custom_prop_tmp.creative_status_cd , md_creative_custom_prop.valid_from_dttm = md_creative_custom_prop_tmp.valid_from_dttm , md_creative_custom_prop.valid_to_dttm = md_creative_custom_prop_tmp.valid_to_dttm
        when not matched then insert ( 
        creative_id,creative_status_cd,creative_version_id,property_datatype_cd,property_nm,property_val,valid_from_dttm,valid_to_dttm
        ,Hashed_pk_col ) values ( 
        md_creative_custom_prop_tmp.creative_id,md_creative_custom_prop_tmp.creative_status_cd,md_creative_custom_prop_tmp.creative_version_id,md_creative_custom_prop_tmp.property_datatype_cd,md_creative_custom_prop_tmp.property_nm,md_creative_custom_prop_tmp.property_val,md_creative_custom_prop_tmp.valid_from_dttm,md_creative_custom_prop_tmp.valid_to_dttm,md_creative_custom_prop_tmp.Hashed_pk_col
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_creative_custom_prop_tmp     , md_creative_custom_prop, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_creative_custom_prop_tmp     ;
    quit;
    %put ######## Staging table: md_creative_custom_prop_tmp      Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_creative_custom_prop;
      drop table work.md_creative_custom_prop;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_creative_x_asset_tmp         ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_creative_x_asset, table_keys=%str(asset_id,creative_version_id), out_table=work.md_creative_x_asset);
 data &tmplib..md_creative_x_asset_tmp         ;
     set work.md_creative_x_asset;
  if asset_id='' then asset_id='-'; if creative_version_id='' then creative_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_creative_x_asset_tmp         , md_creative_x_asset);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_creative_x_asset using &tmpdbschema..md_creative_x_asset_tmp         
         on (md_creative_x_asset.asset_id=md_creative_x_asset_tmp.asset_id and md_creative_x_asset.creative_version_id=md_creative_x_asset_tmp.creative_version_id)
        when matched then  
        update set md_creative_x_asset.creative_id = md_creative_x_asset_tmp.creative_id , md_creative_x_asset.creative_status_cd = md_creative_x_asset_tmp.creative_status_cd
        when not matched then insert ( 
        asset_id,creative_id,creative_status_cd,creative_version_id
         ) values ( 
        md_creative_x_asset_tmp.asset_id,md_creative_x_asset_tmp.creative_id,md_creative_x_asset_tmp.creative_status_cd,md_creative_x_asset_tmp.creative_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_creative_x_asset_tmp         , md_creative_x_asset, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_creative_x_asset_tmp         ;
    quit;
    %put ######## Staging table: md_creative_x_asset_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_creative_x_asset;
      drop table work.md_creative_x_asset;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_cust_attrib_tmp              ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_cust_attrib, table_keys=%str(attr_group_id,attr_id), out_table=work.md_cust_attrib);
 data &tmplib..md_cust_attrib_tmp              ;
     set work.md_cust_attrib;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if attr_group_id='' then attr_group_id='-'; if attr_id='' then attr_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_cust_attrib_tmp              , md_cust_attrib);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_cust_attrib using &tmpdbschema..md_cust_attrib_tmp              
         on (md_cust_attrib.attr_group_id=md_cust_attrib_tmp.attr_group_id and md_cust_attrib.attr_id=md_cust_attrib_tmp.attr_id)
        when matched then  
        update set md_cust_attrib.associated_grid = md_cust_attrib_tmp.associated_grid , md_cust_attrib.attr_cd = md_cust_attrib_tmp.attr_cd , md_cust_attrib.attr_group_cd = md_cust_attrib_tmp.attr_group_cd , md_cust_attrib.attr_group_nm = md_cust_attrib_tmp.attr_group_nm , md_cust_attrib.attr_nm = md_cust_attrib_tmp.attr_nm , md_cust_attrib.created_by_usernm = md_cust_attrib_tmp.created_by_usernm , md_cust_attrib.created_dttm = md_cust_attrib_tmp.created_dttm , md_cust_attrib.data_formatter = md_cust_attrib_tmp.data_formatter , md_cust_attrib.data_type = md_cust_attrib_tmp.data_type , md_cust_attrib.is_grid_flg = md_cust_attrib_tmp.is_grid_flg , md_cust_attrib.is_obsolete_flg = md_cust_attrib_tmp.is_obsolete_flg , md_cust_attrib.last_modified_dttm = md_cust_attrib_tmp.last_modified_dttm , md_cust_attrib.last_modified_usernm = md_cust_attrib_tmp.last_modified_usernm , md_cust_attrib.load_dttm = md_cust_attrib_tmp.load_dttm , md_cust_attrib.remote_pklist_tab_col = md_cust_attrib_tmp.remote_pklist_tab_col
        when not matched then insert ( 
        associated_grid,attr_cd,attr_group_cd,attr_group_id,attr_group_nm,attr_id,attr_nm,created_by_usernm,created_dttm,data_formatter,data_type,is_grid_flg,is_obsolete_flg,last_modified_dttm,last_modified_usernm,load_dttm,remote_pklist_tab_col
         ) values ( 
        md_cust_attrib_tmp.associated_grid,md_cust_attrib_tmp.attr_cd,md_cust_attrib_tmp.attr_group_cd,md_cust_attrib_tmp.attr_group_id,md_cust_attrib_tmp.attr_group_nm,md_cust_attrib_tmp.attr_id,md_cust_attrib_tmp.attr_nm,md_cust_attrib_tmp.created_by_usernm,md_cust_attrib_tmp.created_dttm,md_cust_attrib_tmp.data_formatter,md_cust_attrib_tmp.data_type,md_cust_attrib_tmp.is_grid_flg,md_cust_attrib_tmp.is_obsolete_flg,md_cust_attrib_tmp.last_modified_dttm,md_cust_attrib_tmp.last_modified_usernm,md_cust_attrib_tmp.load_dttm,md_cust_attrib_tmp.remote_pklist_tab_col
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_cust_attrib_tmp              , md_cust_attrib, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_cust_attrib_tmp              ;
    quit;
    %put ######## Staging table: md_cust_attrib_tmp               Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_cust_attrib;
      drop table work.md_cust_attrib;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_custattrib_table_values_tmp  ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_custattrib_table_values, table_keys=%str(attr_id,table_val), out_table=work.md_custattrib_table_values);
 data &tmplib..md_custattrib_table_values_tmp  ;
     set work.md_custattrib_table_values;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if attr_id='' then attr_id='-'; if table_val='' then table_val='-';
 run;
 %ErrCheck (Failed to Append Data to :md_custattrib_table_values_tmp  , md_custattrib_table_values);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_custattrib_table_values using &tmpdbschema..md_custattrib_table_values_tmp  
         on (md_custattrib_table_values.attr_id=md_custattrib_table_values_tmp.attr_id and md_custattrib_table_values.table_val=md_custattrib_table_values_tmp.table_val)
        when matched then  
        update set md_custattrib_table_values.attr_cd = md_custattrib_table_values_tmp.attr_cd , md_custattrib_table_values.attr_group_cd = md_custattrib_table_values_tmp.attr_group_cd , md_custattrib_table_values.attr_group_id = md_custattrib_table_values_tmp.attr_group_id , md_custattrib_table_values.attr_group_nm = md_custattrib_table_values_tmp.attr_group_nm , md_custattrib_table_values.attr_nm = md_custattrib_table_values_tmp.attr_nm , md_custattrib_table_values.created_by_usernm = md_custattrib_table_values_tmp.created_by_usernm , md_custattrib_table_values.created_dttm = md_custattrib_table_values_tmp.created_dttm , md_custattrib_table_values.data_formatter = md_custattrib_table_values_tmp.data_formatter , md_custattrib_table_values.data_type = md_custattrib_table_values_tmp.data_type , md_custattrib_table_values.is_obsolete_flg = md_custattrib_table_values_tmp.is_obsolete_flg , md_custattrib_table_values.last_modified_dttm = md_custattrib_table_values_tmp.last_modified_dttm , md_custattrib_table_values.last_modified_usernm = md_custattrib_table_values_tmp.last_modified_usernm , md_custattrib_table_values.load_dttm = md_custattrib_table_values_tmp.load_dttm
        when not matched then insert ( 
        attr_cd,attr_group_cd,attr_group_id,attr_group_nm,attr_id,attr_nm,created_by_usernm,created_dttm,data_formatter,data_type,is_obsolete_flg,last_modified_dttm,last_modified_usernm,load_dttm,table_val
         ) values ( 
        md_custattrib_table_values_tmp.attr_cd,md_custattrib_table_values_tmp.attr_group_cd,md_custattrib_table_values_tmp.attr_group_id,md_custattrib_table_values_tmp.attr_group_nm,md_custattrib_table_values_tmp.attr_id,md_custattrib_table_values_tmp.attr_nm,md_custattrib_table_values_tmp.created_by_usernm,md_custattrib_table_values_tmp.created_dttm,md_custattrib_table_values_tmp.data_formatter,md_custattrib_table_values_tmp.data_type,md_custattrib_table_values_tmp.is_obsolete_flg,md_custattrib_table_values_tmp.last_modified_dttm,md_custattrib_table_values_tmp.last_modified_usernm,md_custattrib_table_values_tmp.load_dttm,md_custattrib_table_values_tmp.table_val
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_custattrib_table_values_tmp  , md_custattrib_table_values, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_custattrib_table_values_tmp  ;
    quit;
    %put ######## Staging table: md_custattrib_table_values_tmp   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_custattrib_table_values;
      drop table work.md_custattrib_table_values;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_dataview_tmp                 ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_dataview, table_keys=%str(dataview_version_id), out_table=work.md_dataview);
 data &tmplib..md_dataview_tmp                 ;
     set work.md_dataview;
  if last_published_dttm ne . then last_published_dttm = tzoneu2s(last_published_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if dataview_version_id='' then dataview_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_dataview_tmp                 , md_dataview);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_dataview using &tmpdbschema..md_dataview_tmp                 
         on (md_dataview.dataview_version_id=md_dataview_tmp.dataview_version_id)
        when matched then  
        update set md_dataview.analytic_active_flg = md_dataview_tmp.analytic_active_flg , md_dataview.analytics_period_type_nm = md_dataview_tmp.analytics_period_type_nm , md_dataview.analytics_period_val = md_dataview_tmp.analytics_period_val , md_dataview.created_user_nm = md_dataview_tmp.created_user_nm , md_dataview.custom_recent_cd = md_dataview_tmp.custom_recent_cd , md_dataview.custom_recent_exclude_cd = md_dataview_tmp.custom_recent_exclude_cd , md_dataview.dataview_desc = md_dataview_tmp.dataview_desc , md_dataview.dataview_id = md_dataview_tmp.dataview_id , md_dataview.dataview_nm = md_dataview_tmp.dataview_nm , md_dataview.dataview_status_cd = md_dataview_tmp.dataview_status_cd , md_dataview.half_life_time_val = md_dataview_tmp.half_life_time_val , md_dataview.include_external_flg = md_dataview_tmp.include_external_flg , md_dataview.include_internal_flg = md_dataview_tmp.include_internal_flg , md_dataview.last_published_dttm = md_dataview_tmp.last_published_dttm , md_dataview.max_path_length_val = md_dataview_tmp.max_path_length_val , md_dataview.max_path_time_type_nm = md_dataview_tmp.max_path_time_type_nm , md_dataview.max_path_time_val = md_dataview_tmp.max_path_time_val , md_dataview.owner_nm = md_dataview_tmp.owner_nm , md_dataview.selected_task_list = md_dataview_tmp.selected_task_list , md_dataview.valid_from_dttm = md_dataview_tmp.valid_from_dttm , md_dataview.valid_to_dttm = md_dataview_tmp.valid_to_dttm
        when not matched then insert ( 
        analytic_active_flg,analytics_period_type_nm,analytics_period_val,created_user_nm,custom_recent_cd,custom_recent_exclude_cd,dataview_desc,dataview_id,dataview_nm,dataview_status_cd,dataview_version_id,half_life_time_val,include_external_flg,include_internal_flg,last_published_dttm,max_path_length_val,max_path_time_type_nm,max_path_time_val,owner_nm,selected_task_list,valid_from_dttm,valid_to_dttm
         ) values ( 
        md_dataview_tmp.analytic_active_flg,md_dataview_tmp.analytics_period_type_nm,md_dataview_tmp.analytics_period_val,md_dataview_tmp.created_user_nm,md_dataview_tmp.custom_recent_cd,md_dataview_tmp.custom_recent_exclude_cd,md_dataview_tmp.dataview_desc,md_dataview_tmp.dataview_id,md_dataview_tmp.dataview_nm,md_dataview_tmp.dataview_status_cd,md_dataview_tmp.dataview_version_id,md_dataview_tmp.half_life_time_val,md_dataview_tmp.include_external_flg,md_dataview_tmp.include_internal_flg,md_dataview_tmp.last_published_dttm,md_dataview_tmp.max_path_length_val,md_dataview_tmp.max_path_time_type_nm,md_dataview_tmp.max_path_time_val,md_dataview_tmp.owner_nm,md_dataview_tmp.selected_task_list,md_dataview_tmp.valid_from_dttm,md_dataview_tmp.valid_to_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_dataview_tmp                 , md_dataview, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_dataview_tmp                 ;
    quit;
    %put ######## Staging table: md_dataview_tmp                  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_dataview;
      drop table work.md_dataview;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_dataview_x_event_tmp         ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_dataview_x_event, table_keys=%str(dataview_version_id,event_id), out_table=work.md_dataview_x_event);
 data &tmplib..md_dataview_x_event_tmp         ;
     set work.md_dataview_x_event;
  if dataview_version_id='' then dataview_version_id='-'; if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_dataview_x_event_tmp         , md_dataview_x_event);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_dataview_x_event using &tmpdbschema..md_dataview_x_event_tmp         
         on (md_dataview_x_event.dataview_version_id=md_dataview_x_event_tmp.dataview_version_id and md_dataview_x_event.event_id=md_dataview_x_event_tmp.event_id)
        when matched then  
        update set md_dataview_x_event.dataview_id = md_dataview_x_event_tmp.dataview_id , md_dataview_x_event.dataview_status_cd = md_dataview_x_event_tmp.dataview_status_cd
        when not matched then insert ( 
        dataview_id,dataview_status_cd,dataview_version_id,event_id
         ) values ( 
        md_dataview_x_event_tmp.dataview_id,md_dataview_x_event_tmp.dataview_status_cd,md_dataview_x_event_tmp.dataview_version_id,md_dataview_x_event_tmp.event_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_dataview_x_event_tmp         , md_dataview_x_event, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_dataview_x_event_tmp         ;
    quit;
    %put ######## Staging table: md_dataview_x_event_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_dataview_x_event;
      drop table work.md_dataview_x_event;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_event_tmp                    ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_event, table_keys=%str(event_version_id), out_table=work.md_event);
 data &tmplib..md_event_tmp                    ;
     set work.md_event;
  if last_published_dttm ne . then last_published_dttm = tzoneu2s(last_published_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if event_version_id='' then event_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_event_tmp                    , md_event);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_event using &tmpdbschema..md_event_tmp                    
         on (md_event.event_version_id=md_event_tmp.event_version_id)
        when matched then  
        update set md_event.channel_nm = md_event_tmp.channel_nm , md_event.created_user_nm = md_event_tmp.created_user_nm , md_event.event_desc = md_event_tmp.event_desc , md_event.event_id = md_event_tmp.event_id , md_event.event_nm = md_event_tmp.event_nm , md_event.event_status_cd = md_event_tmp.event_status_cd , md_event.event_subtype_nm = md_event_tmp.event_subtype_nm , md_event.event_type_nm = md_event_tmp.event_type_nm , md_event.last_published_dttm = md_event_tmp.last_published_dttm , md_event.owner_nm = md_event_tmp.owner_nm , md_event.valid_from_dttm = md_event_tmp.valid_from_dttm , md_event.valid_to_dttm = md_event_tmp.valid_to_dttm
        when not matched then insert ( 
        channel_nm,created_user_nm,event_desc,event_id,event_nm,event_status_cd,event_subtype_nm,event_type_nm,event_version_id,last_published_dttm,owner_nm,valid_from_dttm,valid_to_dttm
         ) values ( 
        md_event_tmp.channel_nm,md_event_tmp.created_user_nm,md_event_tmp.event_desc,md_event_tmp.event_id,md_event_tmp.event_nm,md_event_tmp.event_status_cd,md_event_tmp.event_subtype_nm,md_event_tmp.event_type_nm,md_event_tmp.event_version_id,md_event_tmp.last_published_dttm,md_event_tmp.owner_nm,md_event_tmp.valid_from_dttm,md_event_tmp.valid_to_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_event_tmp                    , md_event, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_event_tmp                    ;
    quit;
    %put ######## Staging table: md_event_tmp                     Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_event;
      drop table work.md_event;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_fiscal_period_tmp            ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_fiscal_period, table_keys=%str(fp_id), out_table=work.md_fiscal_period);
 data &tmplib..md_fiscal_period_tmp            ;
     set work.md_fiscal_period;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if fp_id='' then fp_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_fiscal_period_tmp            , md_fiscal_period);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_fiscal_period using &tmpdbschema..md_fiscal_period_tmp            
         on (md_fiscal_period.fp_id=md_fiscal_period_tmp.fp_id)
        when matched then  
        update set md_fiscal_period.created_by_usernm = md_fiscal_period_tmp.created_by_usernm , md_fiscal_period.created_dttm = md_fiscal_period_tmp.created_dttm , md_fiscal_period.fp_cls_ver = md_fiscal_period_tmp.fp_cls_ver , md_fiscal_period.fp_desc = md_fiscal_period_tmp.fp_desc , md_fiscal_period.fp_end_dt = md_fiscal_period_tmp.fp_end_dt , md_fiscal_period.fp_nm = md_fiscal_period_tmp.fp_nm , md_fiscal_period.fp_obsolete_flg = md_fiscal_period_tmp.fp_obsolete_flg , md_fiscal_period.fp_start_dt = md_fiscal_period_tmp.fp_start_dt , md_fiscal_period.last_modified_dttm = md_fiscal_period_tmp.last_modified_dttm , md_fiscal_period.last_modified_usernm = md_fiscal_period_tmp.last_modified_usernm , md_fiscal_period.load_dttm = md_fiscal_period_tmp.load_dttm
        when not matched then insert ( 
        created_by_usernm,created_dttm,fp_cls_ver,fp_desc,fp_end_dt,fp_id,fp_nm,fp_obsolete_flg,fp_start_dt,last_modified_dttm,last_modified_usernm,load_dttm
         ) values ( 
        md_fiscal_period_tmp.created_by_usernm,md_fiscal_period_tmp.created_dttm,md_fiscal_period_tmp.fp_cls_ver,md_fiscal_period_tmp.fp_desc,md_fiscal_period_tmp.fp_end_dt,md_fiscal_period_tmp.fp_id,md_fiscal_period_tmp.fp_nm,md_fiscal_period_tmp.fp_obsolete_flg,md_fiscal_period_tmp.fp_start_dt,md_fiscal_period_tmp.last_modified_dttm,md_fiscal_period_tmp.last_modified_usernm,md_fiscal_period_tmp.load_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_fiscal_period_tmp            , md_fiscal_period, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_fiscal_period_tmp            ;
    quit;
    %put ######## Staging table: md_fiscal_period_tmp             Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_fiscal_period;
      drop table work.md_fiscal_period;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_grid_attr_defn_tmp           ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_grid_attr_defn, table_keys=%str(attr_group_id,attr_id,grid_id), out_table=work.md_grid_attr_defn);
 data &tmplib..md_grid_attr_defn_tmp           ;
     set work.md_grid_attr_defn;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if attr_group_id='' then attr_group_id='-'; if attr_id='' then attr_id='-'; if grid_id='' then grid_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_grid_attr_defn_tmp           , md_grid_attr_defn);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_grid_attr_defn using &tmpdbschema..md_grid_attr_defn_tmp           
         on (md_grid_attr_defn.attr_group_id=md_grid_attr_defn_tmp.attr_group_id and md_grid_attr_defn.attr_id=md_grid_attr_defn_tmp.attr_id and md_grid_attr_defn.grid_id=md_grid_attr_defn_tmp.grid_id)
        when matched then  
        update set md_grid_attr_defn.associated_grid = md_grid_attr_defn_tmp.associated_grid , md_grid_attr_defn.attr_cd = md_grid_attr_defn_tmp.attr_cd , md_grid_attr_defn.attr_desc = md_grid_attr_defn_tmp.attr_desc , md_grid_attr_defn.attr_group_cd = md_grid_attr_defn_tmp.attr_group_cd , md_grid_attr_defn.attr_group_nm = md_grid_attr_defn_tmp.attr_group_nm , md_grid_attr_defn.attr_nm = md_grid_attr_defn_tmp.attr_nm , md_grid_attr_defn.attr_obsolete_flg = md_grid_attr_defn_tmp.attr_obsolete_flg , md_grid_attr_defn.attr_order_no = md_grid_attr_defn_tmp.attr_order_no , md_grid_attr_defn.created_by_usernm = md_grid_attr_defn_tmp.created_by_usernm , md_grid_attr_defn.created_dttm = md_grid_attr_defn_tmp.created_dttm , md_grid_attr_defn.data_formatter = md_grid_attr_defn_tmp.data_formatter , md_grid_attr_defn.data_type = md_grid_attr_defn_tmp.data_type , md_grid_attr_defn.grid_cd = md_grid_attr_defn_tmp.grid_cd , md_grid_attr_defn.grid_desc = md_grid_attr_defn_tmp.grid_desc , md_grid_attr_defn.grid_mandatory_flg = md_grid_attr_defn_tmp.grid_mandatory_flg , md_grid_attr_defn.grid_nm = md_grid_attr_defn_tmp.grid_nm , md_grid_attr_defn.grid_obsolete_flg = md_grid_attr_defn_tmp.grid_obsolete_flg , md_grid_attr_defn.last_modified_dttm = md_grid_attr_defn_tmp.last_modified_dttm , md_grid_attr_defn.last_modified_usernm = md_grid_attr_defn_tmp.last_modified_usernm , md_grid_attr_defn.load_dttm = md_grid_attr_defn_tmp.load_dttm , md_grid_attr_defn.remote_pklist_tab_col = md_grid_attr_defn_tmp.remote_pklist_tab_col
        when not matched then insert ( 
        associated_grid,attr_cd,attr_desc,attr_group_cd,attr_group_id,attr_group_nm,attr_id,attr_nm,attr_obsolete_flg,attr_order_no,created_by_usernm,created_dttm,data_formatter,data_type,grid_cd,grid_desc,grid_id,grid_mandatory_flg,grid_nm,grid_obsolete_flg,last_modified_dttm,last_modified_usernm,load_dttm,remote_pklist_tab_col
         ) values ( 
        md_grid_attr_defn_tmp.associated_grid,md_grid_attr_defn_tmp.attr_cd,md_grid_attr_defn_tmp.attr_desc,md_grid_attr_defn_tmp.attr_group_cd,md_grid_attr_defn_tmp.attr_group_id,md_grid_attr_defn_tmp.attr_group_nm,md_grid_attr_defn_tmp.attr_id,md_grid_attr_defn_tmp.attr_nm,md_grid_attr_defn_tmp.attr_obsolete_flg,md_grid_attr_defn_tmp.attr_order_no,md_grid_attr_defn_tmp.created_by_usernm,md_grid_attr_defn_tmp.created_dttm,md_grid_attr_defn_tmp.data_formatter,md_grid_attr_defn_tmp.data_type,md_grid_attr_defn_tmp.grid_cd,md_grid_attr_defn_tmp.grid_desc,md_grid_attr_defn_tmp.grid_id,md_grid_attr_defn_tmp.grid_mandatory_flg,md_grid_attr_defn_tmp.grid_nm,md_grid_attr_defn_tmp.grid_obsolete_flg,md_grid_attr_defn_tmp.last_modified_dttm,md_grid_attr_defn_tmp.last_modified_usernm,md_grid_attr_defn_tmp.load_dttm,md_grid_attr_defn_tmp.remote_pklist_tab_col
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_grid_attr_defn_tmp           , md_grid_attr_defn, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_grid_attr_defn_tmp           ;
    quit;
    %put ######## Staging table: md_grid_attr_defn_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_grid_attr_defn;
      drop table work.md_grid_attr_defn;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_journey_tmp                  ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_journey, table_keys=%str(journey_version_id), out_table=work.md_journey);
 data &tmplib..md_journey_tmp                  ;
     set work.md_journey;
  if last_activated_dttm ne . then last_activated_dttm = tzoneu2s(last_activated_dttm,&timeZone_Value.) ;
  if journey_version_id='' then journey_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_journey_tmp                  , md_journey);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_journey using &tmpdbschema..md_journey_tmp                  
         on (md_journey.journey_version_id=md_journey_tmp.journey_version_id)
        when matched then  
        update set md_journey.activated_user_nm = md_journey_tmp.activated_user_nm , md_journey.control_group_flg = md_journey_tmp.control_group_flg , md_journey.created_user_nm = md_journey_tmp.created_user_nm , md_journey.journey_id = md_journey_tmp.journey_id , md_journey.journey_nm = md_journey_tmp.journey_nm , md_journey.journey_status_cd = md_journey_tmp.journey_status_cd , md_journey.last_activated_dttm = md_journey_tmp.last_activated_dttm , md_journey.purpose_id = md_journey_tmp.purpose_id , md_journey.target_goal_qty = md_journey_tmp.target_goal_qty , md_journey.target_goal_type_nm = md_journey_tmp.target_goal_type_nm
        when not matched then insert ( 
        activated_user_nm,control_group_flg,created_user_nm,journey_id,journey_nm,journey_status_cd,journey_version_id,last_activated_dttm,purpose_id,target_goal_qty,target_goal_type_nm
         ) values ( 
        md_journey_tmp.activated_user_nm,md_journey_tmp.control_group_flg,md_journey_tmp.created_user_nm,md_journey_tmp.journey_id,md_journey_tmp.journey_nm,md_journey_tmp.journey_status_cd,md_journey_tmp.journey_version_id,md_journey_tmp.last_activated_dttm,md_journey_tmp.purpose_id,md_journey_tmp.target_goal_qty,md_journey_tmp.target_goal_type_nm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_journey_tmp                  , md_journey, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_journey_tmp                  ;
    quit;
    %put ######## Staging table: md_journey_tmp                   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_journey;
      drop table work.md_journey;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_journey_node_tmp             ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_journey_node, table_keys=%str(journey_node_id), out_table=work.md_journey_node);
 data &tmplib..md_journey_node_tmp             ;
     set work.md_journey_node;
  if journey_node_id='' then journey_node_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_journey_node_tmp             , md_journey_node);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_journey_node using &tmpdbschema..md_journey_node_tmp             
         on (md_journey_node.journey_node_id=md_journey_node_tmp.journey_node_id)
        when matched then  
        update set md_journey_node.journey_id = md_journey_node_tmp.journey_id , md_journey_node.journey_version_id = md_journey_node_tmp.journey_version_id , md_journey_node.next_node_id = md_journey_node_tmp.next_node_id , md_journey_node.node_nm = md_journey_node_tmp.node_nm , md_journey_node.node_type = md_journey_node_tmp.node_type , md_journey_node.previous_node_id = md_journey_node_tmp.previous_node_id
        when not matched then insert ( 
        journey_id,journey_node_id,journey_version_id,next_node_id,node_nm,node_type,previous_node_id
         ) values ( 
        md_journey_node_tmp.journey_id,md_journey_node_tmp.journey_node_id,md_journey_node_tmp.journey_version_id,md_journey_node_tmp.next_node_id,md_journey_node_tmp.node_nm,md_journey_node_tmp.node_type,md_journey_node_tmp.previous_node_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_journey_node_tmp             , md_journey_node, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_journey_node_tmp             ;
    quit;
    %put ######## Staging table: md_journey_node_tmp              Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_journey_node;
      drop table work.md_journey_node;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_journey_node_occurrence_tmp  ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_journey_node_occurrence, table_keys=%str(journey_node_occurrence_id), out_table=work.md_journey_node_occurrence);
 data &tmplib..md_journey_node_occurrence_tmp  ;
     set work.md_journey_node_occurrence;
  if end_dttm ne . then end_dttm = tzoneu2s(end_dttm,&timeZone_Value.);if start_dttm ne . then start_dttm = tzoneu2s(start_dttm,&timeZone_Value.) ;
  if journey_node_occurrence_id='' then journey_node_occurrence_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_journey_node_occurrence_tmp  , md_journey_node_occurrence);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_journey_node_occurrence using &tmpdbschema..md_journey_node_occurrence_tmp  
         on (md_journey_node_occurrence.journey_node_occurrence_id=md_journey_node_occurrence_tmp.journey_node_occurrence_id)
        when matched then  
        update set md_journey_node_occurrence.end_dttm = md_journey_node_occurrence_tmp.end_dttm , md_journey_node_occurrence.error_messages = md_journey_node_occurrence_tmp.error_messages , md_journey_node_occurrence.execution_status = md_journey_node_occurrence_tmp.execution_status , md_journey_node_occurrence.journey_id = md_journey_node_occurrence_tmp.journey_id , md_journey_node_occurrence.journey_node_id = md_journey_node_occurrence_tmp.journey_node_id , md_journey_node_occurrence.journey_occurrence_id = md_journey_node_occurrence_tmp.journey_occurrence_id , md_journey_node_occurrence.journey_version_id = md_journey_node_occurrence_tmp.journey_version_id , md_journey_node_occurrence.num_of_contacts_entered = md_journey_node_occurrence_tmp.num_of_contacts_entered , md_journey_node_occurrence.start_dttm = md_journey_node_occurrence_tmp.start_dttm
        when not matched then insert ( 
        end_dttm,error_messages,execution_status,journey_id,journey_node_id,journey_node_occurrence_id,journey_occurrence_id,journey_version_id,num_of_contacts_entered,start_dttm
         ) values ( 
        md_journey_node_occurrence_tmp.end_dttm,md_journey_node_occurrence_tmp.error_messages,md_journey_node_occurrence_tmp.execution_status,md_journey_node_occurrence_tmp.journey_id,md_journey_node_occurrence_tmp.journey_node_id,md_journey_node_occurrence_tmp.journey_node_occurrence_id,md_journey_node_occurrence_tmp.journey_occurrence_id,md_journey_node_occurrence_tmp.journey_version_id,md_journey_node_occurrence_tmp.num_of_contacts_entered,md_journey_node_occurrence_tmp.start_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_journey_node_occurrence_tmp  , md_journey_node_occurrence, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_journey_node_occurrence_tmp  ;
    quit;
    %put ######## Staging table: md_journey_node_occurrence_tmp   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_journey_node_occurrence;
      drop table work.md_journey_node_occurrence;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_journey_occurrence_tmp       ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_journey_occurrence, table_keys=%str(journey_occurrence_id), out_table=work.md_journey_occurrence);
 data &tmplib..md_journey_occurrence_tmp       ;
     set work.md_journey_occurrence;
  if end_dttm ne . then end_dttm = tzoneu2s(end_dttm,&timeZone_Value.);if start_dttm ne . then start_dttm = tzoneu2s(start_dttm,&timeZone_Value.) ;
  if journey_occurrence_id='' then journey_occurrence_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_journey_occurrence_tmp       , md_journey_occurrence);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_journey_occurrence using &tmpdbschema..md_journey_occurrence_tmp       
         on (md_journey_occurrence.journey_occurrence_id=md_journey_occurrence_tmp.journey_occurrence_id)
        when matched then  
        update set md_journey_occurrence.aud_occurrence_id = md_journey_occurrence_tmp.aud_occurrence_id , md_journey_occurrence.end_dttm = md_journey_occurrence_tmp.end_dttm , md_journey_occurrence.error_messages = md_journey_occurrence_tmp.error_messages , md_journey_occurrence.execution_status = md_journey_occurrence_tmp.execution_status , md_journey_occurrence.journey_id = md_journey_occurrence_tmp.journey_id , md_journey_occurrence.journey_occurrence_num = md_journey_occurrence_tmp.journey_occurrence_num , md_journey_occurrence.journey_version_id = md_journey_occurrence_tmp.journey_version_id , md_journey_occurrence.num_of_contacts_entered = md_journey_occurrence_tmp.num_of_contacts_entered , md_journey_occurrence.num_of_contacts_suppressed = md_journey_occurrence_tmp.num_of_contacts_suppressed , md_journey_occurrence.occurrence_type_nm = md_journey_occurrence_tmp.occurrence_type_nm , md_journey_occurrence.start_dttm = md_journey_occurrence_tmp.start_dttm , md_journey_occurrence.started_by_nm = md_journey_occurrence_tmp.started_by_nm
        when not matched then insert ( 
        aud_occurrence_id,end_dttm,error_messages,execution_status,journey_id,journey_occurrence_id,journey_occurrence_num,journey_version_id,num_of_contacts_entered,num_of_contacts_suppressed,occurrence_type_nm,start_dttm,started_by_nm
         ) values ( 
        md_journey_occurrence_tmp.aud_occurrence_id,md_journey_occurrence_tmp.end_dttm,md_journey_occurrence_tmp.error_messages,md_journey_occurrence_tmp.execution_status,md_journey_occurrence_tmp.journey_id,md_journey_occurrence_tmp.journey_occurrence_id,md_journey_occurrence_tmp.journey_occurrence_num,md_journey_occurrence_tmp.journey_version_id,md_journey_occurrence_tmp.num_of_contacts_entered,md_journey_occurrence_tmp.num_of_contacts_suppressed,md_journey_occurrence_tmp.occurrence_type_nm,md_journey_occurrence_tmp.start_dttm,md_journey_occurrence_tmp.started_by_nm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_journey_occurrence_tmp       , md_journey_occurrence, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_journey_occurrence_tmp       ;
    quit;
    %put ######## Staging table: md_journey_occurrence_tmp        Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_journey_occurrence;
      drop table work.md_journey_occurrence;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_journey_x_audience_tmp       ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_journey_x_audience, table_keys=%str(audience_id,journey_version_id), out_table=work.md_journey_x_audience);
 data &tmplib..md_journey_x_audience_tmp       ;
     set work.md_journey_x_audience;
  if audience_id='' then audience_id='-'; if journey_version_id='' then journey_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_journey_x_audience_tmp       , md_journey_x_audience);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_journey_x_audience using &tmpdbschema..md_journey_x_audience_tmp       
         on (md_journey_x_audience.audience_id=md_journey_x_audience_tmp.audience_id and md_journey_x_audience.journey_version_id=md_journey_x_audience_tmp.journey_version_id)
        when matched then  
        update set md_journey_x_audience.aud_relationship_nm = md_journey_x_audience_tmp.aud_relationship_nm , md_journey_x_audience.journey_id = md_journey_x_audience_tmp.journey_id , md_journey_x_audience.journey_node_id = md_journey_x_audience_tmp.journey_node_id
        when not matched then insert ( 
        aud_relationship_nm,audience_id,journey_id,journey_node_id,journey_version_id
         ) values ( 
        md_journey_x_audience_tmp.aud_relationship_nm,md_journey_x_audience_tmp.audience_id,md_journey_x_audience_tmp.journey_id,md_journey_x_audience_tmp.journey_node_id,md_journey_x_audience_tmp.journey_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_journey_x_audience_tmp       , md_journey_x_audience, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_journey_x_audience_tmp       ;
    quit;
    %put ######## Staging table: md_journey_x_audience_tmp        Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_journey_x_audience;
      drop table work.md_journey_x_audience;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_journey_x_event_tmp          ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_journey_x_event, table_keys=%str(event_id,journey_node_id), out_table=work.md_journey_x_event);
 data &tmplib..md_journey_x_event_tmp          ;
     set work.md_journey_x_event;
  if event_id='' then event_id='-'; if journey_node_id='' then journey_node_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_journey_x_event_tmp          , md_journey_x_event);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_journey_x_event using &tmpdbschema..md_journey_x_event_tmp          
         on (md_journey_x_event.event_id=md_journey_x_event_tmp.event_id and md_journey_x_event.journey_node_id=md_journey_x_event_tmp.journey_node_id)
        when matched then  
        update set md_journey_x_event.event_relationship_nm = md_journey_x_event_tmp.event_relationship_nm , md_journey_x_event.journey_id = md_journey_x_event_tmp.journey_id , md_journey_x_event.journey_version_id = md_journey_x_event_tmp.journey_version_id
        when not matched then insert ( 
        event_id,event_relationship_nm,journey_id,journey_node_id,journey_version_id
         ) values ( 
        md_journey_x_event_tmp.event_id,md_journey_x_event_tmp.event_relationship_nm,md_journey_x_event_tmp.journey_id,md_journey_x_event_tmp.journey_node_id,md_journey_x_event_tmp.journey_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_journey_x_event_tmp          , md_journey_x_event, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_journey_x_event_tmp          ;
    quit;
    %put ######## Staging table: md_journey_x_event_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_journey_x_event;
      drop table work.md_journey_x_event;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_journey_x_task_tmp           ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_journey_x_task, table_keys=%str(journey_node_id), out_table=work.md_journey_x_task);
 data &tmplib..md_journey_x_task_tmp           ;
     set work.md_journey_x_task;
  if journey_node_id='' then journey_node_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_journey_x_task_tmp           , md_journey_x_task);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_journey_x_task using &tmpdbschema..md_journey_x_task_tmp           
         on (md_journey_x_task.journey_node_id=md_journey_x_task_tmp.journey_node_id)
        when matched then  
        update set md_journey_x_task.journey_id = md_journey_x_task_tmp.journey_id , md_journey_x_task.journey_version_id = md_journey_x_task_tmp.journey_version_id , md_journey_x_task.task_id = md_journey_x_task_tmp.task_id , md_journey_x_task.task_version_id = md_journey_x_task_tmp.task_version_id
        when not matched then insert ( 
        journey_id,journey_node_id,journey_version_id,task_id,task_version_id
         ) values ( 
        md_journey_x_task_tmp.journey_id,md_journey_x_task_tmp.journey_node_id,md_journey_x_task_tmp.journey_version_id,md_journey_x_task_tmp.task_id,md_journey_x_task_tmp.task_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_journey_x_task_tmp           , md_journey_x_task, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_journey_x_task_tmp           ;
    quit;
    %put ######## Staging table: md_journey_x_task_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_journey_x_task;
      drop table work.md_journey_x_task;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_message_tmp                  ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_message, table_keys=%str(message_version_id), out_table=work.md_message);
 data &tmplib..md_message_tmp                  ;
     set work.md_message;
  if last_published_dttm ne . then last_published_dttm = tzoneu2s(last_published_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if message_version_id='' then message_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_message_tmp                  , md_message);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_message using &tmpdbschema..md_message_tmp                  
         on (md_message.message_version_id=md_message_tmp.message_version_id)
        when matched then  
        update set md_message.business_context_id = md_message_tmp.business_context_id , md_message.created_user_nm = md_message_tmp.created_user_nm , md_message.folder_path_nm = md_message_tmp.folder_path_nm , md_message.last_published_dttm = md_message_tmp.last_published_dttm , md_message.message_category_nm = md_message_tmp.message_category_nm , md_message.message_cd = md_message_tmp.message_cd , md_message.message_desc = md_message_tmp.message_desc , md_message.message_id = md_message_tmp.message_id , md_message.message_nm = md_message_tmp.message_nm , md_message.message_status_cd = md_message_tmp.message_status_cd , md_message.message_type_nm = md_message_tmp.message_type_nm , md_message.owner_nm = md_message_tmp.owner_nm , md_message.valid_from_dttm = md_message_tmp.valid_from_dttm , md_message.valid_to_dttm = md_message_tmp.valid_to_dttm
        when not matched then insert ( 
        business_context_id,created_user_nm,folder_path_nm,last_published_dttm,message_category_nm,message_cd,message_desc,message_id,message_nm,message_status_cd,message_type_nm,message_version_id,owner_nm,valid_from_dttm,valid_to_dttm
         ) values ( 
        md_message_tmp.business_context_id,md_message_tmp.created_user_nm,md_message_tmp.folder_path_nm,md_message_tmp.last_published_dttm,md_message_tmp.message_category_nm,md_message_tmp.message_cd,md_message_tmp.message_desc,md_message_tmp.message_id,md_message_tmp.message_nm,md_message_tmp.message_status_cd,md_message_tmp.message_type_nm,md_message_tmp.message_version_id,md_message_tmp.owner_nm,md_message_tmp.valid_from_dttm,md_message_tmp.valid_to_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_message_tmp                  , md_message, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_message_tmp                  ;
    quit;
    %put ######## Staging table: md_message_tmp                   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_message;
      drop table work.md_message;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_message_custom_prop_tmp      ;
      quit;
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
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_message_custom_prop using &tmpdbschema..md_message_custom_prop_tmp      
         on (md_message_custom_prop.Hashed_pk_col = md_message_custom_prop_tmp.Hashed_pk_col)
        when matched then  
        update set md_message_custom_prop.message_id = md_message_custom_prop_tmp.message_id , md_message_custom_prop.message_status_cd = md_message_custom_prop_tmp.message_status_cd , md_message_custom_prop.valid_from_dttm = md_message_custom_prop_tmp.valid_from_dttm , md_message_custom_prop.valid_to_dttm = md_message_custom_prop_tmp.valid_to_dttm
        when not matched then insert ( 
        message_id,message_status_cd,message_version_id,property_datatype_cd,property_nm,property_val,valid_from_dttm,valid_to_dttm
        ,Hashed_pk_col ) values ( 
        md_message_custom_prop_tmp.message_id,md_message_custom_prop_tmp.message_status_cd,md_message_custom_prop_tmp.message_version_id,md_message_custom_prop_tmp.property_datatype_cd,md_message_custom_prop_tmp.property_nm,md_message_custom_prop_tmp.property_val,md_message_custom_prop_tmp.valid_from_dttm,md_message_custom_prop_tmp.valid_to_dttm,md_message_custom_prop_tmp.Hashed_pk_col
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_message_custom_prop_tmp      , md_message_custom_prop, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_message_custom_prop_tmp      ;
    quit;
    %put ######## Staging table: md_message_custom_prop_tmp       Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_message_custom_prop;
      drop table work.md_message_custom_prop;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_message_x_creative_tmp       ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_message_x_creative, table_keys=%str(creative_id,message_version_id), out_table=work.md_message_x_creative);
 data &tmplib..md_message_x_creative_tmp       ;
     set work.md_message_x_creative;
  if creative_id='' then creative_id='-'; if message_version_id='' then message_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_message_x_creative_tmp       , md_message_x_creative);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_message_x_creative using &tmpdbschema..md_message_x_creative_tmp       
         on (md_message_x_creative.creative_id=md_message_x_creative_tmp.creative_id and md_message_x_creative.message_version_id=md_message_x_creative_tmp.message_version_id)
        when matched then  
        update set md_message_x_creative.message_id = md_message_x_creative_tmp.message_id , md_message_x_creative.message_status_cd = md_message_x_creative_tmp.message_status_cd
        when not matched then insert ( 
        creative_id,message_id,message_status_cd,message_version_id
         ) values ( 
        md_message_x_creative_tmp.creative_id,md_message_x_creative_tmp.message_id,md_message_x_creative_tmp.message_status_cd,md_message_x_creative_tmp.message_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_message_x_creative_tmp       , md_message_x_creative, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_message_x_creative_tmp       ;
    quit;
    %put ######## Staging table: md_message_x_creative_tmp        Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_message_x_creative;
      drop table work.md_message_x_creative;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_object_type_tmp              ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_object_type, table_keys=%str(attr_group_id,attr_id,object_type), out_table=work.md_object_type);
 data &tmplib..md_object_type_tmp              ;
     set work.md_object_type;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if attr_group_id='' then attr_group_id='-'; if attr_id='' then attr_id='-'; if object_type='' then object_type='-';
 run;
 %ErrCheck (Failed to Append Data to :md_object_type_tmp              , md_object_type);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_object_type using &tmpdbschema..md_object_type_tmp              
         on (md_object_type.attr_group_id=md_object_type_tmp.attr_group_id and md_object_type.attr_id=md_object_type_tmp.attr_id and md_object_type.object_type=md_object_type_tmp.object_type)
        when matched then  
        update set md_object_type.attr_cd = md_object_type_tmp.attr_cd , md_object_type.attr_group_cd = md_object_type_tmp.attr_group_cd , md_object_type.attr_group_nm = md_object_type_tmp.attr_group_nm , md_object_type.attr_nm = md_object_type_tmp.attr_nm , md_object_type.created_by_usernm = md_object_type_tmp.created_by_usernm , md_object_type.created_dttm = md_object_type_tmp.created_dttm , md_object_type.data_formatter = md_object_type_tmp.data_formatter , md_object_type.data_type = md_object_type_tmp.data_type , md_object_type.is_obsolete_flg = md_object_type_tmp.is_obsolete_flg , md_object_type.last_modified_dttm = md_object_type_tmp.last_modified_dttm , md_object_type.last_modified_usernm = md_object_type_tmp.last_modified_usernm , md_object_type.load_dttm = md_object_type_tmp.load_dttm , md_object_type.object_category = md_object_type_tmp.object_category , md_object_type.remote_pklist_tab_col = md_object_type_tmp.remote_pklist_tab_col
        when not matched then insert ( 
        attr_cd,attr_group_cd,attr_group_id,attr_group_nm,attr_id,attr_nm,created_by_usernm,created_dttm,data_formatter,data_type,is_obsolete_flg,last_modified_dttm,last_modified_usernm,load_dttm,object_category,object_type,remote_pklist_tab_col
         ) values ( 
        md_object_type_tmp.attr_cd,md_object_type_tmp.attr_group_cd,md_object_type_tmp.attr_group_id,md_object_type_tmp.attr_group_nm,md_object_type_tmp.attr_id,md_object_type_tmp.attr_nm,md_object_type_tmp.created_by_usernm,md_object_type_tmp.created_dttm,md_object_type_tmp.data_formatter,md_object_type_tmp.data_type,md_object_type_tmp.is_obsolete_flg,md_object_type_tmp.last_modified_dttm,md_object_type_tmp.last_modified_usernm,md_object_type_tmp.load_dttm,md_object_type_tmp.object_category,md_object_type_tmp.object_type,md_object_type_tmp.remote_pklist_tab_col
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_object_type_tmp              , md_object_type, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_object_type_tmp              ;
    quit;
    %put ######## Staging table: md_object_type_tmp               Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_object_type;
      drop table work.md_object_type;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_occurrence_tmp               ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_occurrence, table_keys=%str(occurrence_id), out_table=work.md_occurrence);
 data &tmplib..md_occurrence_tmp               ;
     set work.md_occurrence;
  if end_tm ne . then end_tm = tzoneu2s(end_tm,&timeZone_Value.);if start_tm ne . then start_tm = tzoneu2s(start_tm,&timeZone_Value.) ;
  if occurrence_id='' then occurrence_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_occurrence_tmp               , md_occurrence);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_occurrence using &tmpdbschema..md_occurrence_tmp               
         on (md_occurrence.occurrence_id=md_occurrence_tmp.occurrence_id)
        when matched then  
        update set md_occurrence.end_tm = md_occurrence_tmp.end_tm , md_occurrence.execution_status_cd = md_occurrence_tmp.execution_status_cd , md_occurrence.object_id = md_occurrence_tmp.object_id , md_occurrence.object_type_nm = md_occurrence_tmp.object_type_nm , md_occurrence.object_version_id = md_occurrence_tmp.object_version_id , md_occurrence.occurrence_no = md_occurrence_tmp.occurrence_no , md_occurrence.occurrence_type_nm = md_occurrence_tmp.occurrence_type_nm , md_occurrence.properties_map_doc = md_occurrence_tmp.properties_map_doc , md_occurrence.start_tm = md_occurrence_tmp.start_tm , md_occurrence.started_by_nm = md_occurrence_tmp.started_by_nm
        when not matched then insert ( 
        end_tm,execution_status_cd,object_id,object_type_nm,object_version_id,occurrence_id,occurrence_no,occurrence_type_nm,properties_map_doc,start_tm,started_by_nm
         ) values ( 
        md_occurrence_tmp.end_tm,md_occurrence_tmp.execution_status_cd,md_occurrence_tmp.object_id,md_occurrence_tmp.object_type_nm,md_occurrence_tmp.object_version_id,md_occurrence_tmp.occurrence_id,md_occurrence_tmp.occurrence_no,md_occurrence_tmp.occurrence_type_nm,md_occurrence_tmp.properties_map_doc,md_occurrence_tmp.start_tm,md_occurrence_tmp.started_by_nm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_occurrence_tmp               , md_occurrence, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_occurrence_tmp               ;
    quit;
    %put ######## Staging table: md_occurrence_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_occurrence;
      drop table work.md_occurrence;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_picklist_tmp                 ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_picklist, table_keys=%str(attr_id,plist_id), out_table=work.md_picklist);
 data &tmplib..md_picklist_tmp                 ;
     set work.md_picklist;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if attr_id='' then attr_id='-'; if plist_id='' then plist_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_picklist_tmp                 , md_picklist);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_picklist using &tmpdbschema..md_picklist_tmp                 
         on (md_picklist.attr_id=md_picklist_tmp.attr_id and md_picklist.plist_id=md_picklist_tmp.plist_id)
        when matched then  
        update set md_picklist.attr_cd = md_picklist_tmp.attr_cd , md_picklist.attr_group_id = md_picklist_tmp.attr_group_id , md_picklist.attr_group_nm = md_picklist_tmp.attr_group_nm , md_picklist.attr_nm = md_picklist_tmp.attr_nm , md_picklist.created_by_usernm = md_picklist_tmp.created_by_usernm , md_picklist.created_dttm = md_picklist_tmp.created_dttm , md_picklist.is_obsolete_flg = md_picklist_tmp.is_obsolete_flg , md_picklist.last_modified_dttm = md_picklist_tmp.last_modified_dttm , md_picklist.last_modified_usernm = md_picklist_tmp.last_modified_usernm , md_picklist.load_dttm = md_picklist_tmp.load_dttm , md_picklist.plist_cd = md_picklist_tmp.plist_cd , md_picklist.plist_desc = md_picklist_tmp.plist_desc , md_picklist.plist_nm = md_picklist_tmp.plist_nm , md_picklist.plist_val = md_picklist_tmp.plist_val
        when not matched then insert ( 
        attr_cd,attr_group_id,attr_group_nm,attr_id,attr_nm,created_by_usernm,created_dttm,is_obsolete_flg,last_modified_dttm,last_modified_usernm,load_dttm,plist_cd,plist_desc,plist_id,plist_nm,plist_val
         ) values ( 
        md_picklist_tmp.attr_cd,md_picklist_tmp.attr_group_id,md_picklist_tmp.attr_group_nm,md_picklist_tmp.attr_id,md_picklist_tmp.attr_nm,md_picklist_tmp.created_by_usernm,md_picklist_tmp.created_dttm,md_picklist_tmp.is_obsolete_flg,md_picklist_tmp.last_modified_dttm,md_picklist_tmp.last_modified_usernm,md_picklist_tmp.load_dttm,md_picklist_tmp.plist_cd,md_picklist_tmp.plist_desc,md_picklist_tmp.plist_id,md_picklist_tmp.plist_nm,md_picklist_tmp.plist_val
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_picklist_tmp                 , md_picklist, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_picklist_tmp                 ;
    quit;
    %put ######## Staging table: md_picklist_tmp                  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_picklist;
      drop table work.md_picklist;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_purpose_tmp                  ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_purpose, table_keys=%str(purpose_id), out_table=work.md_purpose);
 data &tmplib..md_purpose_tmp                  ;
     set work.md_purpose;
  if purpose_id='' then purpose_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_purpose_tmp                  , md_purpose);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_purpose using &tmpdbschema..md_purpose_tmp                  
         on (md_purpose.purpose_id=md_purpose_tmp.purpose_id)
        when matched then  
        update set md_purpose.purpose_nm = md_purpose_tmp.purpose_nm
        when not matched then insert ( 
        purpose_id,purpose_nm
         ) values ( 
        md_purpose_tmp.purpose_id,md_purpose_tmp.purpose_nm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_purpose_tmp                  , md_purpose, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_purpose_tmp                  ;
    quit;
    %put ######## Staging table: md_purpose_tmp                   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_purpose;
      drop table work.md_purpose;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_rtc_tmp                      ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_rtc, table_keys=%str(rtc_id), out_table=work.md_rtc);
 data &tmplib..md_rtc_tmp                      ;
     set work.md_rtc;
  if rtc_dttm ne . then rtc_dttm = tzoneu2s(rtc_dttm,&timeZone_Value.) ;
  if rtc_id='' then rtc_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_rtc_tmp                      , md_rtc);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_rtc using &tmpdbschema..md_rtc_tmp                      
         on (md_rtc.rtc_id=md_rtc_tmp.rtc_id)
        when matched then  
        update set md_rtc.content_map_doc = md_rtc_tmp.content_map_doc , md_rtc.occurrence_id = md_rtc_tmp.occurrence_id , md_rtc.occurrence_no = md_rtc_tmp.occurrence_no , md_rtc.rtc_dttm = md_rtc_tmp.rtc_dttm , md_rtc.segment_id = md_rtc_tmp.segment_id , md_rtc.segment_version_id = md_rtc_tmp.segment_version_id , md_rtc.task_id = md_rtc_tmp.task_id , md_rtc.task_version_id = md_rtc_tmp.task_version_id
        when not matched then insert ( 
        content_map_doc,occurrence_id,occurrence_no,rtc_dttm,rtc_id,segment_id,segment_version_id,task_id,task_version_id
         ) values ( 
        md_rtc_tmp.content_map_doc,md_rtc_tmp.occurrence_id,md_rtc_tmp.occurrence_no,md_rtc_tmp.rtc_dttm,md_rtc_tmp.rtc_id,md_rtc_tmp.segment_id,md_rtc_tmp.segment_version_id,md_rtc_tmp.task_id,md_rtc_tmp.task_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_rtc_tmp                      , md_rtc, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_rtc_tmp                      ;
    quit;
    %put ######## Staging table: md_rtc_tmp                       Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_rtc;
      drop table work.md_rtc;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_segment_tmp                  ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_segment, table_keys=%str(segment_version_id), out_table=work.md_segment);
 data &tmplib..md_segment_tmp                  ;
     set work.md_segment;
  if last_published_dttm ne . then last_published_dttm = tzoneu2s(last_published_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if segment_version_id='' then segment_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_segment_tmp                  , md_segment);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_segment using &tmpdbschema..md_segment_tmp                  
         on (md_segment.segment_version_id=md_segment_tmp.segment_version_id)
        when matched then  
        update set md_segment.business_context_id = md_segment_tmp.business_context_id , md_segment.created_user_nm = md_segment_tmp.created_user_nm , md_segment.folder_path_nm = md_segment_tmp.folder_path_nm , md_segment.last_published_dttm = md_segment_tmp.last_published_dttm , md_segment.owner_nm = md_segment_tmp.owner_nm , md_segment.segment_category_nm = md_segment_tmp.segment_category_nm , md_segment.segment_cd = md_segment_tmp.segment_cd , md_segment.segment_desc = md_segment_tmp.segment_desc , md_segment.segment_id = md_segment_tmp.segment_id , md_segment.segment_map_id = md_segment_tmp.segment_map_id , md_segment.segment_nm = md_segment_tmp.segment_nm , md_segment.segment_src_cd = md_segment_tmp.segment_src_cd , md_segment.segment_status_cd = md_segment_tmp.segment_status_cd , md_segment.valid_from_dttm = md_segment_tmp.valid_from_dttm , md_segment.valid_to_dttm = md_segment_tmp.valid_to_dttm
        when not matched then insert ( 
        business_context_id,created_user_nm,folder_path_nm,last_published_dttm,owner_nm,segment_category_nm,segment_cd,segment_desc,segment_id,segment_map_id,segment_nm,segment_src_cd,segment_status_cd,segment_version_id,valid_from_dttm,valid_to_dttm
         ) values ( 
        md_segment_tmp.business_context_id,md_segment_tmp.created_user_nm,md_segment_tmp.folder_path_nm,md_segment_tmp.last_published_dttm,md_segment_tmp.owner_nm,md_segment_tmp.segment_category_nm,md_segment_tmp.segment_cd,md_segment_tmp.segment_desc,md_segment_tmp.segment_id,md_segment_tmp.segment_map_id,md_segment_tmp.segment_nm,md_segment_tmp.segment_src_cd,md_segment_tmp.segment_status_cd,md_segment_tmp.segment_version_id,md_segment_tmp.valid_from_dttm,md_segment_tmp.valid_to_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_segment_tmp                  , md_segment, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_segment_tmp                  ;
    quit;
    %put ######## Staging table: md_segment_tmp                   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_segment;
      drop table work.md_segment;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_segment_custom_prop_tmp      ;
      quit;
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
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_segment_custom_prop using &tmpdbschema..md_segment_custom_prop_tmp      
         on (md_segment_custom_prop.Hashed_pk_col = md_segment_custom_prop_tmp.Hashed_pk_col)
        when matched then  
        update set md_segment_custom_prop.segment_id = md_segment_custom_prop_tmp.segment_id , md_segment_custom_prop.segment_status_cd = md_segment_custom_prop_tmp.segment_status_cd , md_segment_custom_prop.valid_from_dttm = md_segment_custom_prop_tmp.valid_from_dttm , md_segment_custom_prop.valid_to_dttm = md_segment_custom_prop_tmp.valid_to_dttm
        when not matched then insert ( 
        property_datatype_cd,property_nm,property_val,segment_id,segment_status_cd,segment_version_id,valid_from_dttm,valid_to_dttm
        ,Hashed_pk_col ) values ( 
        md_segment_custom_prop_tmp.property_datatype_cd,md_segment_custom_prop_tmp.property_nm,md_segment_custom_prop_tmp.property_val,md_segment_custom_prop_tmp.segment_id,md_segment_custom_prop_tmp.segment_status_cd,md_segment_custom_prop_tmp.segment_version_id,md_segment_custom_prop_tmp.valid_from_dttm,md_segment_custom_prop_tmp.valid_to_dttm,md_segment_custom_prop_tmp.Hashed_pk_col
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_segment_custom_prop_tmp      , md_segment_custom_prop, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_segment_custom_prop_tmp      ;
    quit;
    %put ######## Staging table: md_segment_custom_prop_tmp       Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_segment_custom_prop;
      drop table work.md_segment_custom_prop;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_segment_map_tmp              ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_segment_map, table_keys=%str(segment_map_version_id), out_table=work.md_segment_map);
 data &tmplib..md_segment_map_tmp              ;
     set work.md_segment_map;
  if last_published_dttm ne . then last_published_dttm = tzoneu2s(last_published_dttm,&timeZone_Value.);if rec_scheduled_end_dttm ne . then rec_scheduled_end_dttm = tzoneu2s(rec_scheduled_end_dttm,&timeZone_Value.);if rec_scheduled_start_dttm ne . then rec_scheduled_start_dttm = tzoneu2s(rec_scheduled_start_dttm,&timeZone_Value.);if scheduled_end_dttm ne . then scheduled_end_dttm = tzoneu2s(scheduled_end_dttm,&timeZone_Value.);if scheduled_start_dttm ne . then scheduled_start_dttm = tzoneu2s(scheduled_start_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if segment_map_version_id='' then segment_map_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_segment_map_tmp              , md_segment_map);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_segment_map using &tmpdbschema..md_segment_map_tmp              
         on (md_segment_map.segment_map_version_id=md_segment_map_tmp.segment_map_version_id)
        when matched then  
        update set md_segment_map.business_context_id = md_segment_map_tmp.business_context_id , md_segment_map.created_user_nm = md_segment_map_tmp.created_user_nm , md_segment_map.folder_path_nm = md_segment_map_tmp.folder_path_nm , md_segment_map.last_published_dttm = md_segment_map_tmp.last_published_dttm , md_segment_map.owner_nm = md_segment_map_tmp.owner_nm , md_segment_map.rec_scheduled_end_dttm = md_segment_map_tmp.rec_scheduled_end_dttm , md_segment_map.rec_scheduled_start_dttm = md_segment_map_tmp.rec_scheduled_start_dttm , md_segment_map.rec_scheduled_start_tm = md_segment_map_tmp.rec_scheduled_start_tm , md_segment_map.recurrence_day_of_month_no = md_segment_map_tmp.recurrence_day_of_month_no , md_segment_map.recurrence_day_of_week_txt = md_segment_map_tmp.recurrence_day_of_week_txt , md_segment_map.recurrence_day_of_wk_ordinal_no = md_segment_map_tmp.recurrence_day_of_wk_ordinal_no , md_segment_map.recurrence_days_of_week_txt = md_segment_map_tmp.recurrence_days_of_week_txt , md_segment_map.recurrence_frequency_cd = md_segment_map_tmp.recurrence_frequency_cd , md_segment_map.recurrence_monthly_type_nm = md_segment_map_tmp.recurrence_monthly_type_nm , md_segment_map.scheduled_end_dttm = md_segment_map_tmp.scheduled_end_dttm , md_segment_map.scheduled_flg = md_segment_map_tmp.scheduled_flg , md_segment_map.scheduled_start_dttm = md_segment_map_tmp.scheduled_start_dttm , md_segment_map.segment_map_category_nm = md_segment_map_tmp.segment_map_category_nm , md_segment_map.segment_map_cd = md_segment_map_tmp.segment_map_cd , md_segment_map.segment_map_desc = md_segment_map_tmp.segment_map_desc , md_segment_map.segment_map_id = md_segment_map_tmp.segment_map_id , md_segment_map.segment_map_nm = md_segment_map_tmp.segment_map_nm , md_segment_map.segment_map_src_cd = md_segment_map_tmp.segment_map_src_cd , md_segment_map.segment_map_status_cd = md_segment_map_tmp.segment_map_status_cd , md_segment_map.valid_from_dttm = md_segment_map_tmp.valid_from_dttm , md_segment_map.valid_to_dttm = md_segment_map_tmp.valid_to_dttm
        when not matched then insert ( 
        business_context_id,created_user_nm,folder_path_nm,last_published_dttm,owner_nm,rec_scheduled_end_dttm,rec_scheduled_start_dttm,rec_scheduled_start_tm,recurrence_day_of_month_no,recurrence_day_of_week_txt,recurrence_day_of_wk_ordinal_no,recurrence_days_of_week_txt,recurrence_frequency_cd,recurrence_monthly_type_nm,scheduled_end_dttm,scheduled_flg,scheduled_start_dttm,segment_map_category_nm,segment_map_cd,segment_map_desc,segment_map_id,segment_map_nm,segment_map_src_cd,segment_map_status_cd,segment_map_version_id,valid_from_dttm,valid_to_dttm
         ) values ( 
        md_segment_map_tmp.business_context_id,md_segment_map_tmp.created_user_nm,md_segment_map_tmp.folder_path_nm,md_segment_map_tmp.last_published_dttm,md_segment_map_tmp.owner_nm,md_segment_map_tmp.rec_scheduled_end_dttm,md_segment_map_tmp.rec_scheduled_start_dttm,md_segment_map_tmp.rec_scheduled_start_tm,md_segment_map_tmp.recurrence_day_of_month_no,md_segment_map_tmp.recurrence_day_of_week_txt,md_segment_map_tmp.recurrence_day_of_wk_ordinal_no,md_segment_map_tmp.recurrence_days_of_week_txt,md_segment_map_tmp.recurrence_frequency_cd,md_segment_map_tmp.recurrence_monthly_type_nm,md_segment_map_tmp.scheduled_end_dttm,md_segment_map_tmp.scheduled_flg,md_segment_map_tmp.scheduled_start_dttm,md_segment_map_tmp.segment_map_category_nm,md_segment_map_tmp.segment_map_cd,md_segment_map_tmp.segment_map_desc,md_segment_map_tmp.segment_map_id,md_segment_map_tmp.segment_map_nm,md_segment_map_tmp.segment_map_src_cd,md_segment_map_tmp.segment_map_status_cd,md_segment_map_tmp.segment_map_version_id,md_segment_map_tmp.valid_from_dttm,md_segment_map_tmp.valid_to_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_segment_map_tmp              , md_segment_map, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_segment_map_tmp              ;
    quit;
    %put ######## Staging table: md_segment_map_tmp               Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_segment_map;
      drop table work.md_segment_map;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_segment_map_custom_prop_tmp  ;
      quit;
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
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_segment_map_custom_prop using &tmpdbschema..md_segment_map_custom_prop_tmp  
         on (md_segment_map_custom_prop.Hashed_pk_col = md_segment_map_custom_prop_tmp.Hashed_pk_col)
        when matched then  
        update set md_segment_map_custom_prop.segment_map_id = md_segment_map_custom_prop_tmp.segment_map_id , md_segment_map_custom_prop.segment_map_status_cd = md_segment_map_custom_prop_tmp.segment_map_status_cd , md_segment_map_custom_prop.valid_from_dttm = md_segment_map_custom_prop_tmp.valid_from_dttm , md_segment_map_custom_prop.valid_to_dttm = md_segment_map_custom_prop_tmp.valid_to_dttm
        when not matched then insert ( 
        property_datatype_cd,property_nm,property_val,segment_map_id,segment_map_status_cd,segment_map_version_id,valid_from_dttm,valid_to_dttm
        ,Hashed_pk_col ) values ( 
        md_segment_map_custom_prop_tmp.property_datatype_cd,md_segment_map_custom_prop_tmp.property_nm,md_segment_map_custom_prop_tmp.property_val,md_segment_map_custom_prop_tmp.segment_map_id,md_segment_map_custom_prop_tmp.segment_map_status_cd,md_segment_map_custom_prop_tmp.segment_map_version_id,md_segment_map_custom_prop_tmp.valid_from_dttm,md_segment_map_custom_prop_tmp.valid_to_dttm,md_segment_map_custom_prop_tmp.Hashed_pk_col
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_segment_map_custom_prop_tmp  , md_segment_map_custom_prop, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_segment_map_custom_prop_tmp  ;
    quit;
    %put ######## Staging table: md_segment_map_custom_prop_tmp   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_segment_map_custom_prop;
      drop table work.md_segment_map_custom_prop;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_segment_map_x_segment_tmp    ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_segment_map_x_segment, table_keys=%str(segment_id,segment_map_version_id), out_table=work.md_segment_map_x_segment);
 data &tmplib..md_segment_map_x_segment_tmp    ;
     set work.md_segment_map_x_segment;
  if segment_id='' then segment_id='-'; if segment_map_version_id='' then segment_map_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_segment_map_x_segment_tmp    , md_segment_map_x_segment);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_segment_map_x_segment using &tmpdbschema..md_segment_map_x_segment_tmp    
         on (md_segment_map_x_segment.segment_id=md_segment_map_x_segment_tmp.segment_id and md_segment_map_x_segment.segment_map_version_id=md_segment_map_x_segment_tmp.segment_map_version_id)
        when matched then  
        update set md_segment_map_x_segment.segment_map_id = md_segment_map_x_segment_tmp.segment_map_id , md_segment_map_x_segment.segment_map_status_cd = md_segment_map_x_segment_tmp.segment_map_status_cd , md_segment_map_x_segment.segment_version_id = md_segment_map_x_segment_tmp.segment_version_id
        when not matched then insert ( 
        segment_id,segment_map_id,segment_map_status_cd,segment_map_version_id,segment_version_id
         ) values ( 
        md_segment_map_x_segment_tmp.segment_id,md_segment_map_x_segment_tmp.segment_map_id,md_segment_map_x_segment_tmp.segment_map_status_cd,md_segment_map_x_segment_tmp.segment_map_version_id,md_segment_map_x_segment_tmp.segment_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_segment_map_x_segment_tmp    , md_segment_map_x_segment, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_segment_map_x_segment_tmp    ;
    quit;
    %put ######## Staging table: md_segment_map_x_segment_tmp     Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_segment_map_x_segment;
      drop table work.md_segment_map_x_segment;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_segment_test_tmp             ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_segment_test, table_keys=%str(task_version_id,test_cd), out_table=work.md_segment_test);
 data &tmplib..md_segment_test_tmp             ;
     set work.md_segment_test;
  if task_version_id='' then task_version_id='-'; if test_cd='' then test_cd='-';
 run;
 %ErrCheck (Failed to Append Data to :md_segment_test_tmp             , md_segment_test);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_segment_test using &tmpdbschema..md_segment_test_tmp             
         on (md_segment_test.task_version_id=md_segment_test_tmp.task_version_id and md_segment_test.test_cd=md_segment_test_tmp.test_cd)
        when matched then  
        update set md_segment_test.stratified_samp_criteria_txt = md_segment_test_tmp.stratified_samp_criteria_txt , md_segment_test.stratified_sampling_flg = md_segment_test_tmp.stratified_sampling_flg , md_segment_test.task_id = md_segment_test_tmp.task_id , md_segment_test.test_cnt = md_segment_test_tmp.test_cnt , md_segment_test.test_enabled_flg = md_segment_test_tmp.test_enabled_flg , md_segment_test.test_nm = md_segment_test_tmp.test_nm , md_segment_test.test_pct = md_segment_test_tmp.test_pct , md_segment_test.test_sizing_type_nm = md_segment_test_tmp.test_sizing_type_nm , md_segment_test.test_type_nm = md_segment_test_tmp.test_type_nm
        when not matched then insert ( 
        stratified_samp_criteria_txt,stratified_sampling_flg,task_id,task_version_id,test_cd,test_cnt,test_enabled_flg,test_nm,test_pct,test_sizing_type_nm,test_type_nm
         ) values ( 
        md_segment_test_tmp.stratified_samp_criteria_txt,md_segment_test_tmp.stratified_sampling_flg,md_segment_test_tmp.task_id,md_segment_test_tmp.task_version_id,md_segment_test_tmp.test_cd,md_segment_test_tmp.test_cnt,md_segment_test_tmp.test_enabled_flg,md_segment_test_tmp.test_nm,md_segment_test_tmp.test_pct,md_segment_test_tmp.test_sizing_type_nm,md_segment_test_tmp.test_type_nm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_segment_test_tmp             , md_segment_test, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_segment_test_tmp             ;
    quit;
    %put ######## Staging table: md_segment_test_tmp              Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_segment_test;
      drop table work.md_segment_test;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_segment_test_x_segment_tmp   ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_segment_test_x_segment, table_keys=%str(segment_id,task_version_id,test_cd), out_table=work.md_segment_test_x_segment);
 data &tmplib..md_segment_test_x_segment_tmp   ;
     set work.md_segment_test_x_segment;
  if segment_id='' then segment_id='-'; if task_version_id='' then task_version_id='-'; if test_cd='' then test_cd='-';
 run;
 %ErrCheck (Failed to Append Data to :md_segment_test_x_segment_tmp   , md_segment_test_x_segment);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_segment_test_x_segment using &tmpdbschema..md_segment_test_x_segment_tmp   
         on (md_segment_test_x_segment.segment_id=md_segment_test_x_segment_tmp.segment_id and md_segment_test_x_segment.task_version_id=md_segment_test_x_segment_tmp.task_version_id and md_segment_test_x_segment.test_cd=md_segment_test_x_segment_tmp.test_cd)
        when matched then  
        update set md_segment_test_x_segment.task_id = md_segment_test_x_segment_tmp.task_id
        when not matched then insert ( 
        segment_id,task_id,task_version_id,test_cd
         ) values ( 
        md_segment_test_x_segment_tmp.segment_id,md_segment_test_x_segment_tmp.task_id,md_segment_test_x_segment_tmp.task_version_id,md_segment_test_x_segment_tmp.test_cd
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_segment_test_x_segment_tmp   , md_segment_test_x_segment, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_segment_test_x_segment_tmp   ;
    quit;
    %put ######## Staging table: md_segment_test_x_segment_tmp    Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_segment_test_x_segment;
      drop table work.md_segment_test_x_segment;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_segment_x_event_tmp          ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_segment_x_event, table_keys=%str(event_id,segment_version_id), out_table=work.md_segment_x_event);
 data &tmplib..md_segment_x_event_tmp          ;
     set work.md_segment_x_event;
  if event_id='' then event_id='-'; if segment_version_id='' then segment_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_segment_x_event_tmp          , md_segment_x_event);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_segment_x_event using &tmpdbschema..md_segment_x_event_tmp          
         on (md_segment_x_event.event_id=md_segment_x_event_tmp.event_id and md_segment_x_event.segment_version_id=md_segment_x_event_tmp.segment_version_id)
        when matched then  
        update set md_segment_x_event.segment_id = md_segment_x_event_tmp.segment_id , md_segment_x_event.segment_status_cd = md_segment_x_event_tmp.segment_status_cd
        when not matched then insert ( 
        event_id,segment_id,segment_status_cd,segment_version_id
         ) values ( 
        md_segment_x_event_tmp.event_id,md_segment_x_event_tmp.segment_id,md_segment_x_event_tmp.segment_status_cd,md_segment_x_event_tmp.segment_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_segment_x_event_tmp          , md_segment_x_event, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_segment_x_event_tmp          ;
    quit;
    %put ######## Staging table: md_segment_x_event_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_segment_x_event;
      drop table work.md_segment_x_event;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_spot_tmp                     ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_spot, table_keys=%str(spot_version_id), out_table=work.md_spot);
 data &tmplib..md_spot_tmp                     ;
     set work.md_spot;
  if last_published_dttm ne . then last_published_dttm = tzoneu2s(last_published_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if spot_version_id='' then spot_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_spot_tmp                     , md_spot);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_spot using &tmpdbschema..md_spot_tmp                     
         on (md_spot.spot_version_id=md_spot_tmp.spot_version_id)
        when matched then  
        update set md_spot.channel_nm = md_spot_tmp.channel_nm , md_spot.created_user_nm = md_spot_tmp.created_user_nm , md_spot.dimension_label_txt = md_spot_tmp.dimension_label_txt , md_spot.height_width_ratio_val_txt = md_spot_tmp.height_width_ratio_val_txt , md_spot.last_published_dttm = md_spot_tmp.last_published_dttm , md_spot.location_selector_flg = md_spot_tmp.location_selector_flg , md_spot.multi_page_flg = md_spot_tmp.multi_page_flg , md_spot.owner_nm = md_spot_tmp.owner_nm , md_spot.spot_desc = md_spot_tmp.spot_desc , md_spot.spot_height_val_no = md_spot_tmp.spot_height_val_no , md_spot.spot_id = md_spot_tmp.spot_id , md_spot.spot_key = md_spot_tmp.spot_key , md_spot.spot_nm = md_spot_tmp.spot_nm , md_spot.spot_status_cd = md_spot_tmp.spot_status_cd , md_spot.spot_type_nm = md_spot_tmp.spot_type_nm , md_spot.spot_width_val_no = md_spot_tmp.spot_width_val_no , md_spot.valid_from_dttm = md_spot_tmp.valid_from_dttm , md_spot.valid_to_dttm = md_spot_tmp.valid_to_dttm
        when not matched then insert ( 
        channel_nm,created_user_nm,dimension_label_txt,height_width_ratio_val_txt,last_published_dttm,location_selector_flg,multi_page_flg,owner_nm,spot_desc,spot_height_val_no,spot_id,spot_key,spot_nm,spot_status_cd,spot_type_nm,spot_version_id,spot_width_val_no,valid_from_dttm,valid_to_dttm
         ) values ( 
        md_spot_tmp.channel_nm,md_spot_tmp.created_user_nm,md_spot_tmp.dimension_label_txt,md_spot_tmp.height_width_ratio_val_txt,md_spot_tmp.last_published_dttm,md_spot_tmp.location_selector_flg,md_spot_tmp.multi_page_flg,md_spot_tmp.owner_nm,md_spot_tmp.spot_desc,md_spot_tmp.spot_height_val_no,md_spot_tmp.spot_id,md_spot_tmp.spot_key,md_spot_tmp.spot_nm,md_spot_tmp.spot_status_cd,md_spot_tmp.spot_type_nm,md_spot_tmp.spot_version_id,md_spot_tmp.spot_width_val_no,md_spot_tmp.valid_from_dttm,md_spot_tmp.valid_to_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_spot_tmp                     , md_spot, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_spot_tmp                     ;
    quit;
    %put ######## Staging table: md_spot_tmp                      Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_spot;
      drop table work.md_spot;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_target_assist_tmp            ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_target_assist, table_keys=%str(task_id), out_table=work.md_target_assist);
 data &tmplib..md_target_assist_tmp            ;
     set work.md_target_assist;
  if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if model_available_dttm ne . then model_available_dttm = tzoneu2s(model_available_dttm,&timeZone_Value.) ;
  if task_id='' then task_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_target_assist_tmp            , md_target_assist);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_target_assist using &tmpdbschema..md_target_assist_tmp            
         on (md_target_assist.task_id=md_target_assist_tmp.task_id)
        when matched then  
        update set md_target_assist.last_modified_dttm = md_target_assist_tmp.last_modified_dttm , md_target_assist.model_available_dttm = md_target_assist_tmp.model_available_dttm , md_target_assist.percent_target_population_size = md_target_assist_tmp.percent_target_population_size , md_target_assist.threshold_type_nm = md_target_assist_tmp.threshold_type_nm , md_target_assist.use_targeting_flg = md_target_assist_tmp.use_targeting_flg
        when not matched then insert ( 
        last_modified_dttm,model_available_dttm,percent_target_population_size,task_id,threshold_type_nm,use_targeting_flg
         ) values ( 
        md_target_assist_tmp.last_modified_dttm,md_target_assist_tmp.model_available_dttm,md_target_assist_tmp.percent_target_population_size,md_target_assist_tmp.task_id,md_target_assist_tmp.threshold_type_nm,md_target_assist_tmp.use_targeting_flg
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_target_assist_tmp            , md_target_assist, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_target_assist_tmp            ;
    quit;
    %put ######## Staging table: md_target_assist_tmp             Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_target_assist;
      drop table work.md_target_assist;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_task_tmp                     ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_task, table_keys=%str(task_version_id), out_table=work.md_task);
 data &tmplib..md_task_tmp                     ;
     set work.md_task;
  if last_published_dttm ne . then last_published_dttm = tzoneu2s(last_published_dttm,&timeZone_Value.);if model_start_dttm ne . then model_start_dttm = tzoneu2s(model_start_dttm,&timeZone_Value.);if rec_scheduled_end_dttm ne . then rec_scheduled_end_dttm = tzoneu2s(rec_scheduled_end_dttm,&timeZone_Value.);if rec_scheduled_start_dttm ne . then rec_scheduled_start_dttm = tzoneu2s(rec_scheduled_start_dttm,&timeZone_Value.);if scheduled_end_dttm ne . then scheduled_end_dttm = tzoneu2s(scheduled_end_dttm,&timeZone_Value.);if scheduled_start_dttm ne . then scheduled_start_dttm = tzoneu2s(scheduled_start_dttm,&timeZone_Value.);if valid_from_dttm ne . then valid_from_dttm = tzoneu2s(valid_from_dttm,&timeZone_Value.);if valid_to_dttm ne . then valid_to_dttm = tzoneu2s(valid_to_dttm,&timeZone_Value.) ;
  if task_version_id='' then task_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_task_tmp                     , md_task);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_task using &tmpdbschema..md_task_tmp                     
         on (md_task.task_version_id=md_task_tmp.task_version_id)
        when matched then  
        update set md_task.activity_flg = md_task_tmp.activity_flg , md_task.arbitration_method_cd = md_task_tmp.arbitration_method_cd , md_task.business_context_id = md_task_tmp.business_context_id , md_task.channel_nm = md_task_tmp.channel_nm , md_task.control_group_action_nm = md_task_tmp.control_group_action_nm , md_task.created_user_nm = md_task_tmp.created_user_nm , md_task.delivery_config_type_nm = md_task_tmp.delivery_config_type_nm , md_task.display_priority_no = md_task_tmp.display_priority_no , md_task.export_template_flg = md_task_tmp.export_template_flg , md_task.folder_path_nm = md_task_tmp.folder_path_nm , md_task.impressions_life_time_cnt = md_task_tmp.impressions_life_time_cnt , md_task.impressions_per_session_cnt = md_task_tmp.impressions_per_session_cnt , md_task.impressions_qty_period_cnt = md_task_tmp.impressions_qty_period_cnt , md_task.last_published_dttm = md_task_tmp.last_published_dttm , md_task.limit_period_unit_cnt = md_task_tmp.limit_period_unit_cnt , md_task.maximum_period_expression_cnt = md_task_tmp.maximum_period_expression_cnt , md_task.mobile_app_id = md_task_tmp.mobile_app_id , md_task.mobile_app_nm = md_task_tmp.mobile_app_nm , md_task.model_start_dttm = md_task_tmp.model_start_dttm , md_task.owner_nm = md_task_tmp.owner_nm , md_task.period_type_nm = md_task_tmp.period_type_nm , md_task.rec_scheduled_end_dttm = md_task_tmp.rec_scheduled_end_dttm , md_task.rec_scheduled_start_dttm = md_task_tmp.rec_scheduled_start_dttm , md_task.rec_scheduled_start_tm = md_task_tmp.rec_scheduled_start_tm , md_task.recurrence_day_of_month_no = md_task_tmp.recurrence_day_of_month_no , md_task.recurrence_day_of_week_txt = md_task_tmp.recurrence_day_of_week_txt , md_task.recurrence_day_of_wk_ordinal_no = md_task_tmp.recurrence_day_of_wk_ordinal_no , md_task.recurrence_days_of_week_txt = md_task_tmp.recurrence_days_of_week_txt , md_task.recurrence_frequency_cd = md_task_tmp.recurrence_frequency_cd , md_task.recurrence_monthly_type_nm = md_task_tmp.recurrence_monthly_type_nm , md_task.recurring_schedule_flg = md_task_tmp.recurring_schedule_flg , md_task.rtdm_flg = md_task_tmp.rtdm_flg , md_task.scheduled_end_dttm = md_task_tmp.scheduled_end_dttm , md_task.scheduled_flg = md_task_tmp.scheduled_flg , md_task.scheduled_start_dttm = md_task_tmp.scheduled_start_dttm , md_task.secondary_status = md_task_tmp.secondary_status , md_task.segment_tests_flg = md_task_tmp.segment_tests_flg , md_task.send_notification_locale_cd = md_task_tmp.send_notification_locale_cd , md_task.stratified_sampling_action_nm = md_task_tmp.stratified_sampling_action_nm , md_task.subject_line_source_nm = md_task_tmp.subject_line_source_nm , md_task.subject_line_txt = md_task_tmp.subject_line_txt , md_task.task_category_nm = md_task_tmp.task_category_nm , md_task.task_cd = md_task_tmp.task_cd , md_task.task_delivery_type_nm = md_task_tmp.task_delivery_type_nm , md_task.task_desc = md_task_tmp.task_desc , md_task.task_id = md_task_tmp.task_id , md_task.task_nm = md_task_tmp.task_nm , md_task.task_status_cd = md_task_tmp.task_status_cd , md_task.task_subtype_nm = md_task_tmp.task_subtype_nm , md_task.task_type_nm = md_task_tmp.task_type_nm , md_task.template_id = md_task_tmp.template_id , md_task.test_duration = md_task_tmp.test_duration , md_task.use_modeling_flg = md_task_tmp.use_modeling_flg , md_task.valid_from_dttm = md_task_tmp.valid_from_dttm , md_task.valid_to_dttm = md_task_tmp.valid_to_dttm
        when not matched then insert ( 
        activity_flg,arbitration_method_cd,business_context_id,channel_nm,control_group_action_nm,created_user_nm,delivery_config_type_nm,display_priority_no,export_template_flg,folder_path_nm,impressions_life_time_cnt,impressions_per_session_cnt,impressions_qty_period_cnt,last_published_dttm,limit_period_unit_cnt,maximum_period_expression_cnt,mobile_app_id,mobile_app_nm,model_start_dttm,owner_nm,period_type_nm,rec_scheduled_end_dttm,rec_scheduled_start_dttm,rec_scheduled_start_tm,recurrence_day_of_month_no,recurrence_day_of_week_txt,recurrence_day_of_wk_ordinal_no,recurrence_days_of_week_txt,recurrence_frequency_cd,recurrence_monthly_type_nm,recurring_schedule_flg,rtdm_flg,scheduled_end_dttm,scheduled_flg,scheduled_start_dttm,secondary_status,segment_tests_flg,send_notification_locale_cd,stratified_sampling_action_nm,subject_line_source_nm,subject_line_txt,task_category_nm,task_cd,task_delivery_type_nm,task_desc,task_id,task_nm,task_status_cd,task_subtype_nm,task_type_nm,task_version_id,template_id,test_duration,use_modeling_flg,valid_from_dttm,valid_to_dttm
         ) values ( 
        md_task_tmp.activity_flg,md_task_tmp.arbitration_method_cd,md_task_tmp.business_context_id,md_task_tmp.channel_nm,md_task_tmp.control_group_action_nm,md_task_tmp.created_user_nm,md_task_tmp.delivery_config_type_nm,md_task_tmp.display_priority_no,md_task_tmp.export_template_flg,md_task_tmp.folder_path_nm,md_task_tmp.impressions_life_time_cnt,md_task_tmp.impressions_per_session_cnt,md_task_tmp.impressions_qty_period_cnt,md_task_tmp.last_published_dttm,md_task_tmp.limit_period_unit_cnt,md_task_tmp.maximum_period_expression_cnt,md_task_tmp.mobile_app_id,md_task_tmp.mobile_app_nm,md_task_tmp.model_start_dttm,md_task_tmp.owner_nm,md_task_tmp.period_type_nm,md_task_tmp.rec_scheduled_end_dttm,md_task_tmp.rec_scheduled_start_dttm,md_task_tmp.rec_scheduled_start_tm,md_task_tmp.recurrence_day_of_month_no,md_task_tmp.recurrence_day_of_week_txt,md_task_tmp.recurrence_day_of_wk_ordinal_no,md_task_tmp.recurrence_days_of_week_txt,md_task_tmp.recurrence_frequency_cd,md_task_tmp.recurrence_monthly_type_nm,md_task_tmp.recurring_schedule_flg,md_task_tmp.rtdm_flg,md_task_tmp.scheduled_end_dttm,md_task_tmp.scheduled_flg,md_task_tmp.scheduled_start_dttm,md_task_tmp.secondary_status,md_task_tmp.segment_tests_flg,md_task_tmp.send_notification_locale_cd,md_task_tmp.stratified_sampling_action_nm,md_task_tmp.subject_line_source_nm,md_task_tmp.subject_line_txt,md_task_tmp.task_category_nm,md_task_tmp.task_cd,md_task_tmp.task_delivery_type_nm,md_task_tmp.task_desc,md_task_tmp.task_id,md_task_tmp.task_nm,md_task_tmp.task_status_cd,md_task_tmp.task_subtype_nm,md_task_tmp.task_type_nm,md_task_tmp.task_version_id,md_task_tmp.template_id,md_task_tmp.test_duration,md_task_tmp.use_modeling_flg,md_task_tmp.valid_from_dttm,md_task_tmp.valid_to_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_task_tmp                     , md_task, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_task_tmp                     ;
    quit;
    %put ######## Staging table: md_task_tmp                      Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_task;
      drop table work.md_task;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_task_custom_prop_tmp         ;
      quit;
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
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_task_custom_prop using &tmpdbschema..md_task_custom_prop_tmp         
         on (md_task_custom_prop.Hashed_pk_col = md_task_custom_prop_tmp.Hashed_pk_col)
        when matched then  
        update set md_task_custom_prop.task_id = md_task_custom_prop_tmp.task_id , md_task_custom_prop.task_status_cd = md_task_custom_prop_tmp.task_status_cd , md_task_custom_prop.valid_from_dttm = md_task_custom_prop_tmp.valid_from_dttm , md_task_custom_prop.valid_to_dttm = md_task_custom_prop_tmp.valid_to_dttm
        when not matched then insert ( 
        property_datatype_nm,property_nm,property_val,task_id,task_status_cd,task_version_id,valid_from_dttm,valid_to_dttm
        ,Hashed_pk_col ) values ( 
        md_task_custom_prop_tmp.property_datatype_nm,md_task_custom_prop_tmp.property_nm,md_task_custom_prop_tmp.property_val,md_task_custom_prop_tmp.task_id,md_task_custom_prop_tmp.task_status_cd,md_task_custom_prop_tmp.task_version_id,md_task_custom_prop_tmp.valid_from_dttm,md_task_custom_prop_tmp.valid_to_dttm,md_task_custom_prop_tmp.Hashed_pk_col
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_task_custom_prop_tmp         , md_task_custom_prop, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_task_custom_prop_tmp         ;
    quit;
    %put ######## Staging table: md_task_custom_prop_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_task_custom_prop;
      drop table work.md_task_custom_prop;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_task_x_audience_tmp          ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_task_x_audience, table_keys=%str(audience_id,task_id), out_table=work.md_task_x_audience);
 data &tmplib..md_task_x_audience_tmp          ;
     set work.md_task_x_audience;
  if audience_id='' then audience_id='-'; if task_id='' then task_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_task_x_audience_tmp          , md_task_x_audience);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_task_x_audience using &tmpdbschema..md_task_x_audience_tmp          
         on (md_task_x_audience.audience_id=md_task_x_audience_tmp.audience_id and md_task_x_audience.task_id=md_task_x_audience_tmp.task_id)
        when not matched then insert ( 
        audience_id,task_id
         ) values ( 
        md_task_x_audience_tmp.audience_id,md_task_x_audience_tmp.task_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_task_x_audience_tmp          , md_task_x_audience, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_task_x_audience_tmp          ;
    quit;
    %put ######## Staging table: md_task_x_audience_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_task_x_audience;
      drop table work.md_task_x_audience;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_task_x_creative_tmp          ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_task_x_creative, table_keys=%str(creative_id,spot_id,task_version_id), out_table=work.md_task_x_creative);
 data &tmplib..md_task_x_creative_tmp          ;
     set work.md_task_x_creative;
  if creative_id='' then creative_id='-'; if spot_id='' then spot_id='-'; if task_version_id='' then task_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_task_x_creative_tmp          , md_task_x_creative);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_task_x_creative using &tmpdbschema..md_task_x_creative_tmp          
         on (md_task_x_creative.creative_id=md_task_x_creative_tmp.creative_id and md_task_x_creative.spot_id=md_task_x_creative_tmp.spot_id and md_task_x_creative.task_version_id=md_task_x_creative_tmp.task_version_id)
        when matched then  
        update set md_task_x_creative.arbitration_method_cd = md_task_x_creative_tmp.arbitration_method_cd , md_task_x_creative.arbitration_method_val = md_task_x_creative_tmp.arbitration_method_val , md_task_x_creative.task_id = md_task_x_creative_tmp.task_id , md_task_x_creative.task_status_cd = md_task_x_creative_tmp.task_status_cd , md_task_x_creative.variant_id = md_task_x_creative_tmp.variant_id , md_task_x_creative.variant_nm = md_task_x_creative_tmp.variant_nm
        when not matched then insert ( 
        arbitration_method_cd,arbitration_method_val,creative_id,spot_id,task_id,task_status_cd,task_version_id,variant_id,variant_nm
         ) values ( 
        md_task_x_creative_tmp.arbitration_method_cd,md_task_x_creative_tmp.arbitration_method_val,md_task_x_creative_tmp.creative_id,md_task_x_creative_tmp.spot_id,md_task_x_creative_tmp.task_id,md_task_x_creative_tmp.task_status_cd,md_task_x_creative_tmp.task_version_id,md_task_x_creative_tmp.variant_id,md_task_x_creative_tmp.variant_nm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_task_x_creative_tmp          , md_task_x_creative, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_task_x_creative_tmp          ;
    quit;
    %put ######## Staging table: md_task_x_creative_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_task_x_creative;
      drop table work.md_task_x_creative;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_task_x_dataview_tmp          ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_task_x_dataview, table_keys=%str(dataview_id,task_version_id), out_table=work.md_task_x_dataview);
 data &tmplib..md_task_x_dataview_tmp          ;
     set work.md_task_x_dataview;
  if dataview_id='' then dataview_id='-'; if task_version_id='' then task_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_task_x_dataview_tmp          , md_task_x_dataview);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_task_x_dataview using &tmpdbschema..md_task_x_dataview_tmp          
         on (md_task_x_dataview.dataview_id=md_task_x_dataview_tmp.dataview_id and md_task_x_dataview.task_version_id=md_task_x_dataview_tmp.task_version_id)
        when matched then  
        update set md_task_x_dataview.primary_metric_flg = md_task_x_dataview_tmp.primary_metric_flg , md_task_x_dataview.secondary_metric_flg = md_task_x_dataview_tmp.secondary_metric_flg , md_task_x_dataview.targeting_flg = md_task_x_dataview_tmp.targeting_flg , md_task_x_dataview.task_id = md_task_x_dataview_tmp.task_id , md_task_x_dataview.task_status_cd = md_task_x_dataview_tmp.task_status_cd
        when not matched then insert ( 
        dataview_id,primary_metric_flg,secondary_metric_flg,targeting_flg,task_id,task_status_cd,task_version_id
         ) values ( 
        md_task_x_dataview_tmp.dataview_id,md_task_x_dataview_tmp.primary_metric_flg,md_task_x_dataview_tmp.secondary_metric_flg,md_task_x_dataview_tmp.targeting_flg,md_task_x_dataview_tmp.task_id,md_task_x_dataview_tmp.task_status_cd,md_task_x_dataview_tmp.task_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_task_x_dataview_tmp          , md_task_x_dataview, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_task_x_dataview_tmp          ;
    quit;
    %put ######## Staging table: md_task_x_dataview_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_task_x_dataview;
      drop table work.md_task_x_dataview;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_task_x_event_tmp             ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_task_x_event, table_keys=%str(event_id,task_version_id), out_table=work.md_task_x_event);
 data &tmplib..md_task_x_event_tmp             ;
     set work.md_task_x_event;
  if event_id='' then event_id='-'; if task_version_id='' then task_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_task_x_event_tmp             , md_task_x_event);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_task_x_event using &tmpdbschema..md_task_x_event_tmp             
         on (md_task_x_event.event_id=md_task_x_event_tmp.event_id and md_task_x_event.task_version_id=md_task_x_event_tmp.task_version_id)
        when matched then  
        update set md_task_x_event.primary_metric_flg = md_task_x_event_tmp.primary_metric_flg , md_task_x_event.secondary_metric_flg = md_task_x_event_tmp.secondary_metric_flg , md_task_x_event.targeting_flg = md_task_x_event_tmp.targeting_flg , md_task_x_event.task_id = md_task_x_event_tmp.task_id , md_task_x_event.task_status_cd = md_task_x_event_tmp.task_status_cd
        when not matched then insert ( 
        event_id,primary_metric_flg,secondary_metric_flg,targeting_flg,task_id,task_status_cd,task_version_id
         ) values ( 
        md_task_x_event_tmp.event_id,md_task_x_event_tmp.primary_metric_flg,md_task_x_event_tmp.secondary_metric_flg,md_task_x_event_tmp.targeting_flg,md_task_x_event_tmp.task_id,md_task_x_event_tmp.task_status_cd,md_task_x_event_tmp.task_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_task_x_event_tmp             , md_task_x_event, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_task_x_event_tmp             ;
    quit;
    %put ######## Staging table: md_task_x_event_tmp              Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_task_x_event;
      drop table work.md_task_x_event;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_task_x_message_tmp           ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_task_x_message, table_keys=%str(message_id,task_version_id), out_table=work.md_task_x_message);
 data &tmplib..md_task_x_message_tmp           ;
     set work.md_task_x_message;
  if message_id='' then message_id='-'; if task_version_id='' then task_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_task_x_message_tmp           , md_task_x_message);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_task_x_message using &tmpdbschema..md_task_x_message_tmp           
         on (md_task_x_message.message_id=md_task_x_message_tmp.message_id and md_task_x_message.task_version_id=md_task_x_message_tmp.task_version_id)
        when matched then  
        update set md_task_x_message.task_id = md_task_x_message_tmp.task_id , md_task_x_message.task_status_cd = md_task_x_message_tmp.task_status_cd
        when not matched then insert ( 
        message_id,task_id,task_status_cd,task_version_id
         ) values ( 
        md_task_x_message_tmp.message_id,md_task_x_message_tmp.task_id,md_task_x_message_tmp.task_status_cd,md_task_x_message_tmp.task_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_task_x_message_tmp           , md_task_x_message, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_task_x_message_tmp           ;
    quit;
    %put ######## Staging table: md_task_x_message_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_task_x_message;
      drop table work.md_task_x_message;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_task_x_segment_tmp           ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_task_x_segment, table_keys=%str(segment_id,task_version_id), out_table=work.md_task_x_segment);
 data &tmplib..md_task_x_segment_tmp           ;
     set work.md_task_x_segment;
  if segment_id='' then segment_id='-'; if task_version_id='' then task_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_task_x_segment_tmp           , md_task_x_segment);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_task_x_segment using &tmpdbschema..md_task_x_segment_tmp           
         on (md_task_x_segment.segment_id=md_task_x_segment_tmp.segment_id and md_task_x_segment.task_version_id=md_task_x_segment_tmp.task_version_id)
        when matched then  
        update set md_task_x_segment.task_id = md_task_x_segment_tmp.task_id , md_task_x_segment.task_status_cd = md_task_x_segment_tmp.task_status_cd
        when not matched then insert ( 
        segment_id,task_id,task_status_cd,task_version_id
         ) values ( 
        md_task_x_segment_tmp.segment_id,md_task_x_segment_tmp.task_id,md_task_x_segment_tmp.task_status_cd,md_task_x_segment_tmp.task_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_task_x_segment_tmp           , md_task_x_segment, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_task_x_segment_tmp           ;
    quit;
    %put ######## Staging table: md_task_x_segment_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_task_x_segment;
      drop table work.md_task_x_segment;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_task_x_spot_tmp              ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_task_x_spot, table_keys=%str(spot_id,task_version_id), out_table=work.md_task_x_spot);
 data &tmplib..md_task_x_spot_tmp              ;
     set work.md_task_x_spot;
  if spot_id='' then spot_id='-'; if task_version_id='' then task_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_task_x_spot_tmp              , md_task_x_spot);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_task_x_spot using &tmpdbschema..md_task_x_spot_tmp              
         on (md_task_x_spot.spot_id=md_task_x_spot_tmp.spot_id and md_task_x_spot.task_version_id=md_task_x_spot_tmp.task_version_id)
        when matched then  
        update set md_task_x_spot.task_id = md_task_x_spot_tmp.task_id , md_task_x_spot.task_status_cd = md_task_x_spot_tmp.task_status_cd
        when not matched then insert ( 
        spot_id,task_id,task_status_cd,task_version_id
         ) values ( 
        md_task_x_spot_tmp.spot_id,md_task_x_spot_tmp.task_id,md_task_x_spot_tmp.task_status_cd,md_task_x_spot_tmp.task_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_task_x_spot_tmp              , md_task_x_spot, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_task_x_spot_tmp              ;
    quit;
    %put ######## Staging table: md_task_x_spot_tmp               Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_task_x_spot;
      drop table work.md_task_x_spot;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_task_x_variant_tmp           ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_task_x_variant, table_keys=%str(analysis_group_id,task_version_id), out_table=work.md_task_x_variant);
 data &tmplib..md_task_x_variant_tmp           ;
     set work.md_task_x_variant;
  if analysis_group_id='' then analysis_group_id='-'; if task_version_id='' then task_version_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_task_x_variant_tmp           , md_task_x_variant);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_task_x_variant using &tmpdbschema..md_task_x_variant_tmp           
         on (md_task_x_variant.analysis_group_id=md_task_x_variant_tmp.analysis_group_id and md_task_x_variant.task_version_id=md_task_x_variant_tmp.task_version_id)
        when matched then  
        update set md_task_x_variant.task_id = md_task_x_variant_tmp.task_id , md_task_x_variant.task_status_cd = md_task_x_variant_tmp.task_status_cd , md_task_x_variant.variant_nm = md_task_x_variant_tmp.variant_nm , md_task_x_variant.variant_source_nm = md_task_x_variant_tmp.variant_source_nm , md_task_x_variant.variant_type_nm = md_task_x_variant_tmp.variant_type_nm , md_task_x_variant.variant_val = md_task_x_variant_tmp.variant_val
        when not matched then insert ( 
        analysis_group_id,task_id,task_status_cd,task_version_id,variant_nm,variant_source_nm,variant_type_nm,variant_val
         ) values ( 
        md_task_x_variant_tmp.analysis_group_id,md_task_x_variant_tmp.task_id,md_task_x_variant_tmp.task_status_cd,md_task_x_variant_tmp.task_version_id,md_task_x_variant_tmp.variant_nm,md_task_x_variant_tmp.variant_source_nm,md_task_x_variant_tmp.variant_type_nm,md_task_x_variant_tmp.variant_val
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_task_x_variant_tmp           , md_task_x_variant, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_task_x_variant_tmp           ;
    quit;
    %put ######## Staging table: md_task_x_variant_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_task_x_variant;
      drop table work.md_task_x_variant;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_vendor_tmp                   ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_vendor, table_keys=%str(vendor_id), out_table=work.md_vendor);
 data &tmplib..md_vendor_tmp                   ;
     set work.md_vendor;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if vendor_id='' then vendor_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_vendor_tmp                   , md_vendor);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_vendor using &tmpdbschema..md_vendor_tmp                   
         on (md_vendor.vendor_id=md_vendor_tmp.vendor_id)
        when matched then  
        update set md_vendor.created_by_usernm = md_vendor_tmp.created_by_usernm , md_vendor.created_dttm = md_vendor_tmp.created_dttm , md_vendor.is_obsolete_flg = md_vendor_tmp.is_obsolete_flg , md_vendor.last_modified_dttm = md_vendor_tmp.last_modified_dttm , md_vendor.last_modified_usernm = md_vendor_tmp.last_modified_usernm , md_vendor.load_dttm = md_vendor_tmp.load_dttm , md_vendor.owner_usernm = md_vendor_tmp.owner_usernm , md_vendor.vendor_currency_cd = md_vendor_tmp.vendor_currency_cd , md_vendor.vendor_desc = md_vendor_tmp.vendor_desc , md_vendor.vendor_nm = md_vendor_tmp.vendor_nm , md_vendor.vendor_number = md_vendor_tmp.vendor_number
        when not matched then insert ( 
        created_by_usernm,created_dttm,is_obsolete_flg,last_modified_dttm,last_modified_usernm,load_dttm,owner_usernm,vendor_currency_cd,vendor_desc,vendor_id,vendor_nm,vendor_number
         ) values ( 
        md_vendor_tmp.created_by_usernm,md_vendor_tmp.created_dttm,md_vendor_tmp.is_obsolete_flg,md_vendor_tmp.last_modified_dttm,md_vendor_tmp.last_modified_usernm,md_vendor_tmp.load_dttm,md_vendor_tmp.owner_usernm,md_vendor_tmp.vendor_currency_cd,md_vendor_tmp.vendor_desc,md_vendor_tmp.vendor_id,md_vendor_tmp.vendor_nm,md_vendor_tmp.vendor_number
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_vendor_tmp                   , md_vendor, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_vendor_tmp                   ;
    quit;
    %put ######## Staging table: md_vendor_tmp                    Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_vendor;
      drop table work.md_vendor;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_wf_process_def_tmp           ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_wf_process_def, table_keys=%str(engine_pdef_id,pdef_id), out_table=work.md_wf_process_def);
 data &tmplib..md_wf_process_def_tmp           ;
     set work.md_wf_process_def;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if engine_pdef_id='' then engine_pdef_id='-'; if pdef_id='' then pdef_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_wf_process_def_tmp           , md_wf_process_def);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_wf_process_def using &tmpdbschema..md_wf_process_def_tmp           
         on (md_wf_process_def.engine_pdef_id=md_wf_process_def_tmp.engine_pdef_id and md_wf_process_def.pdef_id=md_wf_process_def_tmp.pdef_id)
        when matched then  
        update set md_wf_process_def.associated_object_type = md_wf_process_def_tmp.associated_object_type , md_wf_process_def.buildin_template_flg = md_wf_process_def_tmp.buildin_template_flg , md_wf_process_def.created_by_usernm = md_wf_process_def_tmp.created_by_usernm , md_wf_process_def.created_dttm = md_wf_process_def_tmp.created_dttm , md_wf_process_def.default_approval_flg = md_wf_process_def_tmp.default_approval_flg , md_wf_process_def.engine_pdef_key = md_wf_process_def_tmp.engine_pdef_key , md_wf_process_def.file_tobecatlgd_flg = md_wf_process_def_tmp.file_tobecatlgd_flg , md_wf_process_def.last_modified_dttm = md_wf_process_def_tmp.last_modified_dttm , md_wf_process_def.last_modified_usernm = md_wf_process_def_tmp.last_modified_usernm , md_wf_process_def.latest_version_flg = md_wf_process_def_tmp.latest_version_flg , md_wf_process_def.load_dttm = md_wf_process_def_tmp.load_dttm , md_wf_process_def.owner_usernm = md_wf_process_def_tmp.owner_usernm , md_wf_process_def.pdef_desc = md_wf_process_def_tmp.pdef_desc , md_wf_process_def.pdef_nm = md_wf_process_def_tmp.pdef_nm , md_wf_process_def.pdef_state = md_wf_process_def_tmp.pdef_state , md_wf_process_def.pdef_type = md_wf_process_def_tmp.pdef_type , md_wf_process_def.version_num = md_wf_process_def_tmp.version_num
        when not matched then insert ( 
        associated_object_type,buildin_template_flg,created_by_usernm,created_dttm,default_approval_flg,engine_pdef_id,engine_pdef_key,file_tobecatlgd_flg,last_modified_dttm,last_modified_usernm,latest_version_flg,load_dttm,owner_usernm,pdef_desc,pdef_id,pdef_nm,pdef_state,pdef_type,version_num
         ) values ( 
        md_wf_process_def_tmp.associated_object_type,md_wf_process_def_tmp.buildin_template_flg,md_wf_process_def_tmp.created_by_usernm,md_wf_process_def_tmp.created_dttm,md_wf_process_def_tmp.default_approval_flg,md_wf_process_def_tmp.engine_pdef_id,md_wf_process_def_tmp.engine_pdef_key,md_wf_process_def_tmp.file_tobecatlgd_flg,md_wf_process_def_tmp.last_modified_dttm,md_wf_process_def_tmp.last_modified_usernm,md_wf_process_def_tmp.latest_version_flg,md_wf_process_def_tmp.load_dttm,md_wf_process_def_tmp.owner_usernm,md_wf_process_def_tmp.pdef_desc,md_wf_process_def_tmp.pdef_id,md_wf_process_def_tmp.pdef_nm,md_wf_process_def_tmp.pdef_state,md_wf_process_def_tmp.pdef_type,md_wf_process_def_tmp.version_num
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_wf_process_def_tmp           , md_wf_process_def, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_wf_process_def_tmp           ;
    quit;
    %put ######## Staging table: md_wf_process_def_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_wf_process_def;
      drop table work.md_wf_process_def;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_wf_process_def_attr_grp_tmp  ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_wf_process_def_attr_grp, table_keys=%str(attr_group_id,pdef_id), out_table=work.md_wf_process_def_attr_grp);
 data &tmplib..md_wf_process_def_attr_grp_tmp  ;
     set work.md_wf_process_def_attr_grp;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if attr_group_id='' then attr_group_id='-'; if pdef_id='' then pdef_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_wf_process_def_attr_grp_tmp  , md_wf_process_def_attr_grp);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_wf_process_def_attr_grp using &tmpdbschema..md_wf_process_def_attr_grp_tmp  
         on (md_wf_process_def_attr_grp.attr_group_id=md_wf_process_def_attr_grp_tmp.attr_group_id and md_wf_process_def_attr_grp.pdef_id=md_wf_process_def_attr_grp_tmp.pdef_id)
        when matched then  
        update set md_wf_process_def_attr_grp.load_dttm = md_wf_process_def_attr_grp_tmp.load_dttm
        when not matched then insert ( 
        attr_group_id,load_dttm,pdef_id
         ) values ( 
        md_wf_process_def_attr_grp_tmp.attr_group_id,md_wf_process_def_attr_grp_tmp.load_dttm,md_wf_process_def_attr_grp_tmp.pdef_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_wf_process_def_attr_grp_tmp  , md_wf_process_def_attr_grp, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_wf_process_def_attr_grp_tmp  ;
    quit;
    %put ######## Staging table: md_wf_process_def_attr_grp_tmp   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_wf_process_def_attr_grp;
      drop table work.md_wf_process_def_attr_grp;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_wf_process_def_categories_tmp;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_wf_process_def_categories, table_keys=%str(category_id,pdef_id), out_table=work.md_wf_process_def_categories);
 data &tmplib..md_wf_process_def_categories_tmp;
     set work.md_wf_process_def_categories;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if category_id='' then category_id='-'; if pdef_id='' then pdef_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_wf_process_def_categories_tmp, md_wf_process_def_categories);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_wf_process_def_categories using &tmpdbschema..md_wf_process_def_categories_tmp
         on (md_wf_process_def_categories.category_id=md_wf_process_def_categories_tmp.category_id and md_wf_process_def_categories.pdef_id=md_wf_process_def_categories_tmp.pdef_id)
        when matched then  
        update set md_wf_process_def_categories.category_type = md_wf_process_def_categories_tmp.category_type , md_wf_process_def_categories.default_category_flg = md_wf_process_def_categories_tmp.default_category_flg , md_wf_process_def_categories.load_dttm = md_wf_process_def_categories_tmp.load_dttm
        when not matched then insert ( 
        category_id,category_type,default_category_flg,load_dttm,pdef_id
         ) values ( 
        md_wf_process_def_categories_tmp.category_id,md_wf_process_def_categories_tmp.category_type,md_wf_process_def_categories_tmp.default_category_flg,md_wf_process_def_categories_tmp.load_dttm,md_wf_process_def_categories_tmp.pdef_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_wf_process_def_categories_tmp, md_wf_process_def_categories, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_wf_process_def_categories_tmp;
    quit;
    %put ######## Staging table: md_wf_process_def_categories_tmp Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_wf_process_def_categories;
      drop table work.md_wf_process_def_categories;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_wf_process_def_task_assg_tmp ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_wf_process_def_task_assg, table_keys=%str(assignee_id,assignee_type,pdef_id,task_id), out_table=work.md_wf_process_def_task_assg);
 data &tmplib..md_wf_process_def_task_assg_tmp ;
     set work.md_wf_process_def_task_assg;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if assignee_id='' then assignee_id='-'; if assignee_type='' then assignee_type='-'; if pdef_id='' then pdef_id='-'; if task_id='' then task_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_wf_process_def_task_assg_tmp , md_wf_process_def_task_assg);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_wf_process_def_task_assg using &tmpdbschema..md_wf_process_def_task_assg_tmp 
         on (md_wf_process_def_task_assg.assignee_id=md_wf_process_def_task_assg_tmp.assignee_id and md_wf_process_def_task_assg.assignee_type=md_wf_process_def_task_assg_tmp.assignee_type and md_wf_process_def_task_assg.pdef_id=md_wf_process_def_task_assg_tmp.pdef_id and md_wf_process_def_task_assg.task_id=md_wf_process_def_task_assg_tmp.task_id)
        when matched then  
        update set md_wf_process_def_task_assg.assignee_duration = md_wf_process_def_task_assg_tmp.assignee_duration , md_wf_process_def_task_assg.assignee_instruction = md_wf_process_def_task_assg_tmp.assignee_instruction , md_wf_process_def_task_assg.load_dttm = md_wf_process_def_task_assg_tmp.load_dttm
        when not matched then insert ( 
        assignee_duration,assignee_id,assignee_instruction,assignee_type,load_dttm,pdef_id,task_id
         ) values ( 
        md_wf_process_def_task_assg_tmp.assignee_duration,md_wf_process_def_task_assg_tmp.assignee_id,md_wf_process_def_task_assg_tmp.assignee_instruction,md_wf_process_def_task_assg_tmp.assignee_type,md_wf_process_def_task_assg_tmp.load_dttm,md_wf_process_def_task_assg_tmp.pdef_id,md_wf_process_def_task_assg_tmp.task_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_wf_process_def_task_assg_tmp , md_wf_process_def_task_assg, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_wf_process_def_task_assg_tmp ;
    quit;
    %put ######## Staging table: md_wf_process_def_task_assg_tmp  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_wf_process_def_task_assg;
      drop table work.md_wf_process_def_task_assg;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..md_wf_process_def_tasks_tmp     ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=md_wf_process_def_tasks, table_keys=%str(pdef_id,task_id), out_table=work.md_wf_process_def_tasks);
 data &tmplib..md_wf_process_def_tasks_tmp     ;
     set work.md_wf_process_def_tasks;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if pdef_id='' then pdef_id='-'; if task_id='' then task_id='-';
 run;
 %ErrCheck (Failed to Append Data to :md_wf_process_def_tasks_tmp     , md_wf_process_def_tasks);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..md_wf_process_def_tasks using &tmpdbschema..md_wf_process_def_tasks_tmp     
         on (md_wf_process_def_tasks.pdef_id=md_wf_process_def_tasks_tmp.pdef_id and md_wf_process_def_tasks.task_id=md_wf_process_def_tasks_tmp.task_id)
        when matched then  
        update set md_wf_process_def_tasks.assignee_type = md_wf_process_def_tasks_tmp.assignee_type , md_wf_process_def_tasks.ciobject_enabled_flg = md_wf_process_def_tasks_tmp.ciobject_enabled_flg , md_wf_process_def_tasks.comment_enabled_flg = md_wf_process_def_tasks_tmp.comment_enabled_flg , md_wf_process_def_tasks.comment_mandatory_flg = md_wf_process_def_tasks_tmp.comment_mandatory_flg , md_wf_process_def_tasks.default_duration_perassignee = md_wf_process_def_tasks_tmp.default_duration_perassignee , md_wf_process_def_tasks.file_enabled_flg = md_wf_process_def_tasks_tmp.file_enabled_flg , md_wf_process_def_tasks.file_mandatory_flg = md_wf_process_def_tasks_tmp.file_mandatory_flg , md_wf_process_def_tasks.is_sequential_flg = md_wf_process_def_tasks_tmp.is_sequential_flg , md_wf_process_def_tasks.item_approval_state = md_wf_process_def_tasks_tmp.item_approval_state , md_wf_process_def_tasks.load_dttm = md_wf_process_def_tasks_tmp.load_dttm , md_wf_process_def_tasks.multiple_asgnsuprt_flg = md_wf_process_def_tasks_tmp.multiple_asgnsuprt_flg , md_wf_process_def_tasks.outgoing_flow_flg = md_wf_process_def_tasks_tmp.outgoing_flow_flg , md_wf_process_def_tasks.predecessor_task_id = md_wf_process_def_tasks_tmp.predecessor_task_id , md_wf_process_def_tasks.res_mandatory_flg = md_wf_process_def_tasks_tmp.res_mandatory_flg , md_wf_process_def_tasks.resp_enabled_flg = md_wf_process_def_tasks_tmp.resp_enabled_flg , md_wf_process_def_tasks.resp_file_enabled_flg = md_wf_process_def_tasks_tmp.resp_file_enabled_flg , md_wf_process_def_tasks.show_sourceitemlink_flg = md_wf_process_def_tasks_tmp.show_sourceitemlink_flg , md_wf_process_def_tasks.show_workflowlink_flg = md_wf_process_def_tasks_tmp.show_workflowlink_flg , md_wf_process_def_tasks.source_item_field = md_wf_process_def_tasks_tmp.source_item_field , md_wf_process_def_tasks.task_desc = md_wf_process_def_tasks_tmp.task_desc , md_wf_process_def_tasks.task_instruction = md_wf_process_def_tasks_tmp.task_instruction , md_wf_process_def_tasks.task_nm = md_wf_process_def_tasks_tmp.task_nm , md_wf_process_def_tasks.task_subtype = md_wf_process_def_tasks_tmp.task_subtype , md_wf_process_def_tasks.task_type = md_wf_process_def_tasks_tmp.task_type , md_wf_process_def_tasks.url_enabled_flg = md_wf_process_def_tasks_tmp.url_enabled_flg
        when not matched then insert ( 
        assignee_type,ciobject_enabled_flg,comment_enabled_flg,comment_mandatory_flg,default_duration_perassignee,file_enabled_flg,file_mandatory_flg,is_sequential_flg,item_approval_state,load_dttm,multiple_asgnsuprt_flg,outgoing_flow_flg,pdef_id,predecessor_task_id,res_mandatory_flg,resp_enabled_flg,resp_file_enabled_flg,show_sourceitemlink_flg,show_workflowlink_flg,source_item_field,task_desc,task_id,task_instruction,task_nm,task_subtype,task_type,url_enabled_flg
         ) values ( 
        md_wf_process_def_tasks_tmp.assignee_type,md_wf_process_def_tasks_tmp.ciobject_enabled_flg,md_wf_process_def_tasks_tmp.comment_enabled_flg,md_wf_process_def_tasks_tmp.comment_mandatory_flg,md_wf_process_def_tasks_tmp.default_duration_perassignee,md_wf_process_def_tasks_tmp.file_enabled_flg,md_wf_process_def_tasks_tmp.file_mandatory_flg,md_wf_process_def_tasks_tmp.is_sequential_flg,md_wf_process_def_tasks_tmp.item_approval_state,md_wf_process_def_tasks_tmp.load_dttm,md_wf_process_def_tasks_tmp.multiple_asgnsuprt_flg,md_wf_process_def_tasks_tmp.outgoing_flow_flg,md_wf_process_def_tasks_tmp.pdef_id,md_wf_process_def_tasks_tmp.predecessor_task_id,md_wf_process_def_tasks_tmp.res_mandatory_flg,md_wf_process_def_tasks_tmp.resp_enabled_flg,md_wf_process_def_tasks_tmp.resp_file_enabled_flg,md_wf_process_def_tasks_tmp.show_sourceitemlink_flg,md_wf_process_def_tasks_tmp.show_workflowlink_flg,md_wf_process_def_tasks_tmp.source_item_field,md_wf_process_def_tasks_tmp.task_desc,md_wf_process_def_tasks_tmp.task_id,md_wf_process_def_tasks_tmp.task_instruction,md_wf_process_def_tasks_tmp.task_nm,md_wf_process_def_tasks_tmp.task_subtype,md_wf_process_def_tasks_tmp.task_type,md_wf_process_def_tasks_tmp.url_enabled_flg
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :md_wf_process_def_tasks_tmp     , md_wf_process_def_tasks, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..md_wf_process_def_tasks_tmp     ;
    quit;
    %put ######## Staging table: md_wf_process_def_tasks_tmp      Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..md_wf_process_def_tasks;
      drop table work.md_wf_process_def_tasks;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..media_activity_details_tmp      ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=media_activity_details, table_keys=%str(action_dttm,detail_id,media_nm), out_table=work.media_activity_details);
 data &tmplib..media_activity_details_tmp      ;
     set work.media_activity_details;
  if action_dttm ne . then action_dttm = tzoneu2s(action_dttm,&timeZone_Value.);if action_dttm_tz ne . then action_dttm_tz = tzoneu2s(action_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if detail_id='' then detail_id='-'; if media_nm='' then media_nm='-';
 run;
 %ErrCheck (Failed to Append Data to :media_activity_details_tmp      , media_activity_details);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..media_activity_details using &tmpdbschema..media_activity_details_tmp      
         on (media_activity_details.action_dttm=media_activity_details_tmp.action_dttm and media_activity_details.detail_id=media_activity_details_tmp.detail_id and media_activity_details.media_nm=media_activity_details_tmp.media_nm)
        when matched then  
        update set media_activity_details.action = media_activity_details_tmp.action , media_activity_details.action_dttm_tz = media_activity_details_tmp.action_dttm_tz , media_activity_details.detail_id_hex = media_activity_details_tmp.detail_id_hex , media_activity_details.load_dttm = media_activity_details_tmp.load_dttm , media_activity_details.media_uri_txt = media_activity_details_tmp.media_uri_txt , media_activity_details.playhead_position = media_activity_details_tmp.playhead_position
        when not matched then insert ( 
        action,action_dttm,action_dttm_tz,detail_id,detail_id_hex,load_dttm,media_nm,media_uri_txt,playhead_position
         ) values ( 
        media_activity_details_tmp.action,media_activity_details_tmp.action_dttm,media_activity_details_tmp.action_dttm_tz,media_activity_details_tmp.detail_id,media_activity_details_tmp.detail_id_hex,media_activity_details_tmp.load_dttm,media_activity_details_tmp.media_nm,media_activity_details_tmp.media_uri_txt,media_activity_details_tmp.playhead_position
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :media_activity_details_tmp      , media_activity_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..media_activity_details_tmp      ;
    quit;
    %put ######## Staging table: media_activity_details_tmp       Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..media_activity_details;
      drop table work.media_activity_details;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..media_details_tmp               ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=media_details, table_keys=%str(detail_id,media_nm,play_start_dttm), out_table=work.media_details);
 data &tmplib..media_details_tmp               ;
     set work.media_details;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if play_start_dttm ne . then play_start_dttm = tzoneu2s(play_start_dttm,&timeZone_Value.);if play_start_dttm_tz ne . then play_start_dttm_tz = tzoneu2s(play_start_dttm_tz,&timeZone_Value.) ;
  if detail_id='' then detail_id='-'; if media_nm='' then media_nm='-';
 run;
 %ErrCheck (Failed to Append Data to :media_details_tmp               , media_details);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..media_details using &tmpdbschema..media_details_tmp               
         on (media_details.detail_id=media_details_tmp.detail_id and media_details.media_nm=media_details_tmp.media_nm and media_details.play_start_dttm=media_details_tmp.play_start_dttm)
        when matched then  
        update set media_details.detail_id_hex = media_details_tmp.detail_id_hex , media_details.event_id = media_details_tmp.event_id , media_details.event_key_cd = media_details_tmp.event_key_cd , media_details.event_source_cd = media_details_tmp.event_source_cd , media_details.identity_id = media_details_tmp.identity_id , media_details.load_dttm = media_details_tmp.load_dttm , media_details.media_duration_secs = media_details_tmp.media_duration_secs , media_details.media_player_nm = media_details_tmp.media_player_nm , media_details.media_player_version_txt = media_details_tmp.media_player_version_txt , media_details.media_uri_txt = media_details_tmp.media_uri_txt , media_details.play_start_dttm_tz = media_details_tmp.play_start_dttm_tz , media_details.session_id = media_details_tmp.session_id , media_details.session_id_hex = media_details_tmp.session_id_hex , media_details.visit_id = media_details_tmp.visit_id , media_details.visit_id_hex = media_details_tmp.visit_id_hex
        when not matched then insert ( 
        detail_id,detail_id_hex,event_id,event_key_cd,event_source_cd,identity_id,load_dttm,media_duration_secs,media_nm,media_player_nm,media_player_version_txt,media_uri_txt,play_start_dttm,play_start_dttm_tz,session_id,session_id_hex,visit_id,visit_id_hex
         ) values ( 
        media_details_tmp.detail_id,media_details_tmp.detail_id_hex,media_details_tmp.event_id,media_details_tmp.event_key_cd,media_details_tmp.event_source_cd,media_details_tmp.identity_id,media_details_tmp.load_dttm,media_details_tmp.media_duration_secs,media_details_tmp.media_nm,media_details_tmp.media_player_nm,media_details_tmp.media_player_version_txt,media_details_tmp.media_uri_txt,media_details_tmp.play_start_dttm,media_details_tmp.play_start_dttm_tz,media_details_tmp.session_id,media_details_tmp.session_id_hex,media_details_tmp.visit_id,media_details_tmp.visit_id_hex
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :media_details_tmp               , media_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..media_details_tmp               ;
    quit;
    %put ######## Staging table: media_details_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..media_details;
      drop table work.media_details;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..media_details_ext_tmp           ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=media_details_ext, table_keys=%str(detail_id,media_nm,play_end_dttm), out_table=work.media_details_ext);
 data &tmplib..media_details_ext_tmp           ;
     set work.media_details_ext;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if play_end_dttm ne . then play_end_dttm = tzoneu2s(play_end_dttm,&timeZone_Value.);if play_end_dttm_tz ne . then play_end_dttm_tz = tzoneu2s(play_end_dttm_tz,&timeZone_Value.) ;
  if detail_id='' then detail_id='-'; if media_nm='' then media_nm='-';
 run;
 %ErrCheck (Failed to Append Data to :media_details_ext_tmp           , media_details_ext);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..media_details_ext using &tmpdbschema..media_details_ext_tmp           
         on (media_details_ext.detail_id=media_details_ext_tmp.detail_id and media_details_ext.media_nm=media_details_ext_tmp.media_nm and media_details_ext.play_end_dttm=media_details_ext_tmp.play_end_dttm)
        when matched then  
        update set media_details_ext.detail_id_hex = media_details_ext_tmp.detail_id_hex , media_details_ext.end_tm = media_details_ext_tmp.end_tm , media_details_ext.exit_point_secs = media_details_ext_tmp.exit_point_secs , media_details_ext.interaction_cnt = media_details_ext_tmp.interaction_cnt , media_details_ext.load_dttm = media_details_ext_tmp.load_dttm , media_details_ext.max_play_secs = media_details_ext_tmp.max_play_secs , media_details_ext.media_display_duration_secs = media_details_ext_tmp.media_display_duration_secs , media_details_ext.media_uri_txt = media_details_ext_tmp.media_uri_txt , media_details_ext.play_end_dttm_tz = media_details_ext_tmp.play_end_dttm_tz , media_details_ext.start_tm = media_details_ext_tmp.start_tm , media_details_ext.view_duration_secs = media_details_ext_tmp.view_duration_secs
        when not matched then insert ( 
        detail_id,detail_id_hex,end_tm,exit_point_secs,interaction_cnt,load_dttm,max_play_secs,media_display_duration_secs,media_nm,media_uri_txt,play_end_dttm,play_end_dttm_tz,start_tm,view_duration_secs
         ) values ( 
        media_details_ext_tmp.detail_id,media_details_ext_tmp.detail_id_hex,media_details_ext_tmp.end_tm,media_details_ext_tmp.exit_point_secs,media_details_ext_tmp.interaction_cnt,media_details_ext_tmp.load_dttm,media_details_ext_tmp.max_play_secs,media_details_ext_tmp.media_display_duration_secs,media_details_ext_tmp.media_nm,media_details_ext_tmp.media_uri_txt,media_details_ext_tmp.play_end_dttm,media_details_ext_tmp.play_end_dttm_tz,media_details_ext_tmp.start_tm,media_details_ext_tmp.view_duration_secs
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :media_details_ext_tmp           , media_details_ext, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..media_details_ext_tmp           ;
    quit;
    %put ######## Staging table: media_details_ext_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..media_details_ext;
      drop table work.media_details_ext;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..mobile_focus_defocus_tmp        ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=mobile_focus_defocus, table_keys=%str(event_id), out_table=work.mobile_focus_defocus);
 data &tmplib..mobile_focus_defocus_tmp        ;
     set work.mobile_focus_defocus;
  if action_dttm ne . then action_dttm = tzoneu2s(action_dttm,&timeZone_Value.);if action_dttm_tz ne . then action_dttm_tz = tzoneu2s(action_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :mobile_focus_defocus_tmp        , mobile_focus_defocus);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..mobile_focus_defocus using &tmpdbschema..mobile_focus_defocus_tmp        
         on (mobile_focus_defocus.event_id=mobile_focus_defocus_tmp.event_id)
        when matched then  
        update set mobile_focus_defocus.action_dttm = mobile_focus_defocus_tmp.action_dttm , mobile_focus_defocus.action_dttm_tz = mobile_focus_defocus_tmp.action_dttm_tz , mobile_focus_defocus.channel_user_id = mobile_focus_defocus_tmp.channel_user_id , mobile_focus_defocus.detail_id_hex = mobile_focus_defocus_tmp.detail_id_hex , mobile_focus_defocus.event_designed_id = mobile_focus_defocus_tmp.event_designed_id , mobile_focus_defocus.event_nm = mobile_focus_defocus_tmp.event_nm , mobile_focus_defocus.identity_id = mobile_focus_defocus_tmp.identity_id , mobile_focus_defocus.load_dttm = mobile_focus_defocus_tmp.load_dttm , mobile_focus_defocus.mobile_app_id = mobile_focus_defocus_tmp.mobile_app_id , mobile_focus_defocus.reserved_1_txt = mobile_focus_defocus_tmp.reserved_1_txt , mobile_focus_defocus.session_id_hex = mobile_focus_defocus_tmp.session_id_hex , mobile_focus_defocus.visit_id_hex = mobile_focus_defocus_tmp.visit_id_hex
        when not matched then insert ( 
        action_dttm,action_dttm_tz,channel_user_id,detail_id_hex,event_designed_id,event_id,event_nm,identity_id,load_dttm,mobile_app_id,reserved_1_txt,session_id_hex,visit_id_hex
         ) values ( 
        mobile_focus_defocus_tmp.action_dttm,mobile_focus_defocus_tmp.action_dttm_tz,mobile_focus_defocus_tmp.channel_user_id,mobile_focus_defocus_tmp.detail_id_hex,mobile_focus_defocus_tmp.event_designed_id,mobile_focus_defocus_tmp.event_id,mobile_focus_defocus_tmp.event_nm,mobile_focus_defocus_tmp.identity_id,mobile_focus_defocus_tmp.load_dttm,mobile_focus_defocus_tmp.mobile_app_id,mobile_focus_defocus_tmp.reserved_1_txt,mobile_focus_defocus_tmp.session_id_hex,mobile_focus_defocus_tmp.visit_id_hex
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :mobile_focus_defocus_tmp        , mobile_focus_defocus, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..mobile_focus_defocus_tmp        ;
    quit;
    %put ######## Staging table: mobile_focus_defocus_tmp         Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..mobile_focus_defocus;
      drop table work.mobile_focus_defocus;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..mobile_spots_tmp                ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=mobile_spots, table_keys=%str(event_id), out_table=work.mobile_spots);
 data &tmplib..mobile_spots_tmp                ;
     set work.mobile_spots;
  if action_dttm ne . then action_dttm = tzoneu2s(action_dttm,&timeZone_Value.);if action_dttm_tz ne . then action_dttm_tz = tzoneu2s(action_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :mobile_spots_tmp                , mobile_spots);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..mobile_spots using &tmpdbschema..mobile_spots_tmp                
         on (mobile_spots.event_id=mobile_spots_tmp.event_id)
        when matched then  
        update set mobile_spots.action_dttm = mobile_spots_tmp.action_dttm , mobile_spots.action_dttm_tz = mobile_spots_tmp.action_dttm_tz , mobile_spots.channel_user_id = mobile_spots_tmp.channel_user_id , mobile_spots.context_type_nm = mobile_spots_tmp.context_type_nm , mobile_spots.context_val = mobile_spots_tmp.context_val , mobile_spots.creative_id = mobile_spots_tmp.creative_id , mobile_spots.detail_id_hex = mobile_spots_tmp.detail_id_hex , mobile_spots.event_designed_id = mobile_spots_tmp.event_designed_id , mobile_spots.event_nm = mobile_spots_tmp.event_nm , mobile_spots.identity_id = mobile_spots_tmp.identity_id , mobile_spots.load_dttm = mobile_spots_tmp.load_dttm , mobile_spots.mobile_app_id = mobile_spots_tmp.mobile_app_id , mobile_spots.session_id_hex = mobile_spots_tmp.session_id_hex , mobile_spots.spot_id = mobile_spots_tmp.spot_id , mobile_spots.visit_id_hex = mobile_spots_tmp.visit_id_hex
        when not matched then insert ( 
        action_dttm,action_dttm_tz,channel_user_id,context_type_nm,context_val,creative_id,detail_id_hex,event_designed_id,event_id,event_nm,identity_id,load_dttm,mobile_app_id,session_id_hex,spot_id,visit_id_hex
         ) values ( 
        mobile_spots_tmp.action_dttm,mobile_spots_tmp.action_dttm_tz,mobile_spots_tmp.channel_user_id,mobile_spots_tmp.context_type_nm,mobile_spots_tmp.context_val,mobile_spots_tmp.creative_id,mobile_spots_tmp.detail_id_hex,mobile_spots_tmp.event_designed_id,mobile_spots_tmp.event_id,mobile_spots_tmp.event_nm,mobile_spots_tmp.identity_id,mobile_spots_tmp.load_dttm,mobile_spots_tmp.mobile_app_id,mobile_spots_tmp.session_id_hex,mobile_spots_tmp.spot_id,mobile_spots_tmp.visit_id_hex
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :mobile_spots_tmp                , mobile_spots, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..mobile_spots_tmp                ;
    quit;
    %put ######## Staging table: mobile_spots_tmp                 Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..mobile_spots;
      drop table work.mobile_spots;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..monthly_usage_tmp               ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=monthly_usage, table_keys=%str(event_month), out_table=work.monthly_usage);
 data &tmplib..monthly_usage_tmp               ;
     set work.monthly_usage;
  if event_month='' then event_month='-';
 run;
 %ErrCheck (Failed to Append Data to :monthly_usage_tmp               , monthly_usage);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..monthly_usage using &tmpdbschema..monthly_usage_tmp               
         on (monthly_usage.event_month=monthly_usage_tmp.event_month)
        when matched then  
        update set monthly_usage.admin_user_cnt = monthly_usage_tmp.admin_user_cnt , monthly_usage.api_usage_str = monthly_usage_tmp.api_usage_str , monthly_usage.asset_size = monthly_usage_tmp.asset_size , monthly_usage.audience_usage_cnt = monthly_usage_tmp.audience_usage_cnt , monthly_usage.bc_subjcnt_str = monthly_usage_tmp.bc_subjcnt_str , monthly_usage.db_size = monthly_usage_tmp.db_size , monthly_usage.email_preview_cnt = monthly_usage_tmp.email_preview_cnt , monthly_usage.email_send_cnt = monthly_usage_tmp.email_send_cnt , monthly_usage.facebook_ads_cnt = monthly_usage_tmp.facebook_ads_cnt , monthly_usage.google_ads_cnt = monthly_usage_tmp.google_ads_cnt , monthly_usage.linkedin_ads_cnt = monthly_usage_tmp.linkedin_ads_cnt , monthly_usage.mob_impr_cnt = monthly_usage_tmp.mob_impr_cnt , monthly_usage.mob_sesn_cnt = monthly_usage_tmp.mob_sesn_cnt , monthly_usage.mobile_in_app_msg_cnt = monthly_usage_tmp.mobile_in_app_msg_cnt , monthly_usage.mobile_push_cnt = monthly_usage_tmp.mobile_push_cnt , monthly_usage.outbound_api_cnt = monthly_usage_tmp.outbound_api_cnt , monthly_usage.plan_users_cnt = monthly_usage_tmp.plan_users_cnt , monthly_usage.web_impr_cnt = monthly_usage_tmp.web_impr_cnt , monthly_usage.web_sesn_cnt = monthly_usage_tmp.web_sesn_cnt
        when not matched then insert ( 
        admin_user_cnt,api_usage_str,asset_size,audience_usage_cnt,bc_subjcnt_str,db_size,email_preview_cnt,email_send_cnt,event_month,facebook_ads_cnt,google_ads_cnt,linkedin_ads_cnt,mob_impr_cnt,mob_sesn_cnt,mobile_in_app_msg_cnt,mobile_push_cnt,outbound_api_cnt,plan_users_cnt,web_impr_cnt,web_sesn_cnt
         ) values ( 
        monthly_usage_tmp.admin_user_cnt,monthly_usage_tmp.api_usage_str,monthly_usage_tmp.asset_size,monthly_usage_tmp.audience_usage_cnt,monthly_usage_tmp.bc_subjcnt_str,monthly_usage_tmp.db_size,monthly_usage_tmp.email_preview_cnt,monthly_usage_tmp.email_send_cnt,monthly_usage_tmp.event_month,monthly_usage_tmp.facebook_ads_cnt,monthly_usage_tmp.google_ads_cnt,monthly_usage_tmp.linkedin_ads_cnt,monthly_usage_tmp.mob_impr_cnt,monthly_usage_tmp.mob_sesn_cnt,monthly_usage_tmp.mobile_in_app_msg_cnt,monthly_usage_tmp.mobile_push_cnt,monthly_usage_tmp.outbound_api_cnt,monthly_usage_tmp.plan_users_cnt,monthly_usage_tmp.web_impr_cnt,monthly_usage_tmp.web_sesn_cnt
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :monthly_usage_tmp               , monthly_usage, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..monthly_usage_tmp               ;
    quit;
    %put ######## Staging table: monthly_usage_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..monthly_usage;
      drop table work.monthly_usage;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..notification_failed_tmp         ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=notification_failed, table_keys=%str(event_id), out_table=work.notification_failed);
 data &tmplib..notification_failed_tmp         ;
     set work.notification_failed;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if notification_failed_dttm ne . then notification_failed_dttm = tzoneu2s(notification_failed_dttm,&timeZone_Value.);if notification_failed_dttm_tz ne . then notification_failed_dttm_tz = tzoneu2s(notification_failed_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :notification_failed_tmp         , notification_failed);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..notification_failed using &tmpdbschema..notification_failed_tmp         
         on (notification_failed.event_id=notification_failed_tmp.event_id)
        when matched then  
        update set notification_failed.aud_occurrence_id = notification_failed_tmp.aud_occurrence_id , notification_failed.audience_id = notification_failed_tmp.audience_id , notification_failed.channel_nm = notification_failed_tmp.channel_nm , notification_failed.channel_user_id = notification_failed_tmp.channel_user_id , notification_failed.context_type_nm = notification_failed_tmp.context_type_nm , notification_failed.context_val = notification_failed_tmp.context_val , notification_failed.creative_id = notification_failed_tmp.creative_id , notification_failed.creative_version_id = notification_failed_tmp.creative_version_id , notification_failed.error_cd = notification_failed_tmp.error_cd , notification_failed.error_message_txt = notification_failed_tmp.error_message_txt , notification_failed.event_designed_id = notification_failed_tmp.event_designed_id , notification_failed.event_nm = notification_failed_tmp.event_nm , notification_failed.identity_id = notification_failed_tmp.identity_id , notification_failed.journey_id = notification_failed_tmp.journey_id , notification_failed.journey_occurrence_id = notification_failed_tmp.journey_occurrence_id , notification_failed.load_dttm = notification_failed_tmp.load_dttm , notification_failed.message_id = notification_failed_tmp.message_id , notification_failed.message_version_id = notification_failed_tmp.message_version_id , notification_failed.mobile_app_id = notification_failed_tmp.mobile_app_id , notification_failed.notification_failed_dttm = notification_failed_tmp.notification_failed_dttm , notification_failed.notification_failed_dttm_tz = notification_failed_tmp.notification_failed_dttm_tz , notification_failed.occurrence_id = notification_failed_tmp.occurrence_id , notification_failed.properties_map_doc = notification_failed_tmp.properties_map_doc , notification_failed.reserved_1_txt = notification_failed_tmp.reserved_1_txt , notification_failed.reserved_2_txt = notification_failed_tmp.reserved_2_txt , notification_failed.response_tracking_cd = notification_failed_tmp.response_tracking_cd , notification_failed.segment_id = notification_failed_tmp.segment_id , notification_failed.segment_version_id = notification_failed_tmp.segment_version_id , notification_failed.spot_id = notification_failed_tmp.spot_id , notification_failed.task_id = notification_failed_tmp.task_id , notification_failed.task_version_id = notification_failed_tmp.task_version_id
        when not matched then insert ( 
        aud_occurrence_id,audience_id,channel_nm,channel_user_id,context_type_nm,context_val,creative_id,creative_version_id,error_cd,error_message_txt,event_designed_id,event_id,event_nm,identity_id,journey_id,journey_occurrence_id,load_dttm,message_id,message_version_id,mobile_app_id,notification_failed_dttm,notification_failed_dttm_tz,occurrence_id,properties_map_doc,reserved_1_txt,reserved_2_txt,response_tracking_cd,segment_id,segment_version_id,spot_id,task_id,task_version_id
         ) values ( 
        notification_failed_tmp.aud_occurrence_id,notification_failed_tmp.audience_id,notification_failed_tmp.channel_nm,notification_failed_tmp.channel_user_id,notification_failed_tmp.context_type_nm,notification_failed_tmp.context_val,notification_failed_tmp.creative_id,notification_failed_tmp.creative_version_id,notification_failed_tmp.error_cd,notification_failed_tmp.error_message_txt,notification_failed_tmp.event_designed_id,notification_failed_tmp.event_id,notification_failed_tmp.event_nm,notification_failed_tmp.identity_id,notification_failed_tmp.journey_id,notification_failed_tmp.journey_occurrence_id,notification_failed_tmp.load_dttm,notification_failed_tmp.message_id,notification_failed_tmp.message_version_id,notification_failed_tmp.mobile_app_id,notification_failed_tmp.notification_failed_dttm,notification_failed_tmp.notification_failed_dttm_tz,notification_failed_tmp.occurrence_id,notification_failed_tmp.properties_map_doc,notification_failed_tmp.reserved_1_txt,notification_failed_tmp.reserved_2_txt,notification_failed_tmp.response_tracking_cd,notification_failed_tmp.segment_id,notification_failed_tmp.segment_version_id,notification_failed_tmp.spot_id,notification_failed_tmp.task_id,notification_failed_tmp.task_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :notification_failed_tmp         , notification_failed, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..notification_failed_tmp         ;
    quit;
    %put ######## Staging table: notification_failed_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..notification_failed;
      drop table work.notification_failed;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..notification_opened_tmp         ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=notification_opened, table_keys=%str(event_id), out_table=work.notification_opened);
 data &tmplib..notification_opened_tmp         ;
     set work.notification_opened;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if notification_opened_dttm ne . then notification_opened_dttm = tzoneu2s(notification_opened_dttm,&timeZone_Value.);if notification_opened_dttm_tz ne . then notification_opened_dttm_tz = tzoneu2s(notification_opened_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :notification_opened_tmp         , notification_opened);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..notification_opened using &tmpdbschema..notification_opened_tmp         
         on (notification_opened.event_id=notification_opened_tmp.event_id)
        when matched then  
        update set notification_opened.aud_occurrence_id = notification_opened_tmp.aud_occurrence_id , notification_opened.audience_id = notification_opened_tmp.audience_id , notification_opened.channel_nm = notification_opened_tmp.channel_nm , notification_opened.channel_user_id = notification_opened_tmp.channel_user_id , notification_opened.context_type_nm = notification_opened_tmp.context_type_nm , notification_opened.context_val = notification_opened_tmp.context_val , notification_opened.creative_id = notification_opened_tmp.creative_id , notification_opened.creative_version_id = notification_opened_tmp.creative_version_id , notification_opened.event_designed_id = notification_opened_tmp.event_designed_id , notification_opened.event_nm = notification_opened_tmp.event_nm , notification_opened.identity_id = notification_opened_tmp.identity_id , notification_opened.journey_id = notification_opened_tmp.journey_id , notification_opened.journey_occurrence_id = notification_opened_tmp.journey_occurrence_id , notification_opened.load_dttm = notification_opened_tmp.load_dttm , notification_opened.message_id = notification_opened_tmp.message_id , notification_opened.message_version_id = notification_opened_tmp.message_version_id , notification_opened.mobile_app_id = notification_opened_tmp.mobile_app_id , notification_opened.notification_opened_dttm = notification_opened_tmp.notification_opened_dttm , notification_opened.notification_opened_dttm_tz = notification_opened_tmp.notification_opened_dttm_tz , notification_opened.occurrence_id = notification_opened_tmp.occurrence_id , notification_opened.properties_map_doc = notification_opened_tmp.properties_map_doc , notification_opened.reserved_1_txt = notification_opened_tmp.reserved_1_txt , notification_opened.reserved_2_txt = notification_opened_tmp.reserved_2_txt , notification_opened.reserved_3_txt = notification_opened_tmp.reserved_3_txt , notification_opened.response_tracking_cd = notification_opened_tmp.response_tracking_cd , notification_opened.segment_id = notification_opened_tmp.segment_id , notification_opened.segment_version_id = notification_opened_tmp.segment_version_id , notification_opened.spot_id = notification_opened_tmp.spot_id , notification_opened.task_id = notification_opened_tmp.task_id , notification_opened.task_version_id = notification_opened_tmp.task_version_id
        when not matched then insert ( 
        aud_occurrence_id,audience_id,channel_nm,channel_user_id,context_type_nm,context_val,creative_id,creative_version_id,event_designed_id,event_id,event_nm,identity_id,journey_id,journey_occurrence_id,load_dttm,message_id,message_version_id,mobile_app_id,notification_opened_dttm,notification_opened_dttm_tz,occurrence_id,properties_map_doc,reserved_1_txt,reserved_2_txt,reserved_3_txt,response_tracking_cd,segment_id,segment_version_id,spot_id,task_id,task_version_id
         ) values ( 
        notification_opened_tmp.aud_occurrence_id,notification_opened_tmp.audience_id,notification_opened_tmp.channel_nm,notification_opened_tmp.channel_user_id,notification_opened_tmp.context_type_nm,notification_opened_tmp.context_val,notification_opened_tmp.creative_id,notification_opened_tmp.creative_version_id,notification_opened_tmp.event_designed_id,notification_opened_tmp.event_id,notification_opened_tmp.event_nm,notification_opened_tmp.identity_id,notification_opened_tmp.journey_id,notification_opened_tmp.journey_occurrence_id,notification_opened_tmp.load_dttm,notification_opened_tmp.message_id,notification_opened_tmp.message_version_id,notification_opened_tmp.mobile_app_id,notification_opened_tmp.notification_opened_dttm,notification_opened_tmp.notification_opened_dttm_tz,notification_opened_tmp.occurrence_id,notification_opened_tmp.properties_map_doc,notification_opened_tmp.reserved_1_txt,notification_opened_tmp.reserved_2_txt,notification_opened_tmp.reserved_3_txt,notification_opened_tmp.response_tracking_cd,notification_opened_tmp.segment_id,notification_opened_tmp.segment_version_id,notification_opened_tmp.spot_id,notification_opened_tmp.task_id,notification_opened_tmp.task_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :notification_opened_tmp         , notification_opened, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..notification_opened_tmp         ;
    quit;
    %put ######## Staging table: notification_opened_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..notification_opened;
      drop table work.notification_opened;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..notification_send_tmp           ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=notification_send, table_keys=%str(event_id), out_table=work.notification_send);
 data &tmplib..notification_send_tmp           ;
     set work.notification_send;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if notification_send_dttm ne . then notification_send_dttm = tzoneu2s(notification_send_dttm,&timeZone_Value.);if notification_send_dttm_tz ne . then notification_send_dttm_tz = tzoneu2s(notification_send_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :notification_send_tmp           , notification_send);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..notification_send using &tmpdbschema..notification_send_tmp           
         on (notification_send.event_id=notification_send_tmp.event_id)
        when matched then  
        update set notification_send.aud_occurrence_id = notification_send_tmp.aud_occurrence_id , notification_send.audience_id = notification_send_tmp.audience_id , notification_send.channel_nm = notification_send_tmp.channel_nm , notification_send.channel_user_id = notification_send_tmp.channel_user_id , notification_send.context_type_nm = notification_send_tmp.context_type_nm , notification_send.context_val = notification_send_tmp.context_val , notification_send.creative_id = notification_send_tmp.creative_id , notification_send.creative_version_id = notification_send_tmp.creative_version_id , notification_send.event_designed_id = notification_send_tmp.event_designed_id , notification_send.event_nm = notification_send_tmp.event_nm , notification_send.identity_id = notification_send_tmp.identity_id , notification_send.journey_id = notification_send_tmp.journey_id , notification_send.journey_occurrence_id = notification_send_tmp.journey_occurrence_id , notification_send.load_dttm = notification_send_tmp.load_dttm , notification_send.message_id = notification_send_tmp.message_id , notification_send.message_version_id = notification_send_tmp.message_version_id , notification_send.mobile_app_id = notification_send_tmp.mobile_app_id , notification_send.notification_send_dttm = notification_send_tmp.notification_send_dttm , notification_send.notification_send_dttm_tz = notification_send_tmp.notification_send_dttm_tz , notification_send.occurrence_id = notification_send_tmp.occurrence_id , notification_send.properties_map_doc = notification_send_tmp.properties_map_doc , notification_send.reserved_1_txt = notification_send_tmp.reserved_1_txt , notification_send.reserved_2_txt = notification_send_tmp.reserved_2_txt , notification_send.response_tracking_cd = notification_send_tmp.response_tracking_cd , notification_send.segment_id = notification_send_tmp.segment_id , notification_send.segment_version_id = notification_send_tmp.segment_version_id , notification_send.spot_id = notification_send_tmp.spot_id , notification_send.task_id = notification_send_tmp.task_id , notification_send.task_version_id = notification_send_tmp.task_version_id
        when not matched then insert ( 
        aud_occurrence_id,audience_id,channel_nm,channel_user_id,context_type_nm,context_val,creative_id,creative_version_id,event_designed_id,event_id,event_nm,identity_id,journey_id,journey_occurrence_id,load_dttm,message_id,message_version_id,mobile_app_id,notification_send_dttm,notification_send_dttm_tz,occurrence_id,properties_map_doc,reserved_1_txt,reserved_2_txt,response_tracking_cd,segment_id,segment_version_id,spot_id,task_id,task_version_id
         ) values ( 
        notification_send_tmp.aud_occurrence_id,notification_send_tmp.audience_id,notification_send_tmp.channel_nm,notification_send_tmp.channel_user_id,notification_send_tmp.context_type_nm,notification_send_tmp.context_val,notification_send_tmp.creative_id,notification_send_tmp.creative_version_id,notification_send_tmp.event_designed_id,notification_send_tmp.event_id,notification_send_tmp.event_nm,notification_send_tmp.identity_id,notification_send_tmp.journey_id,notification_send_tmp.journey_occurrence_id,notification_send_tmp.load_dttm,notification_send_tmp.message_id,notification_send_tmp.message_version_id,notification_send_tmp.mobile_app_id,notification_send_tmp.notification_send_dttm,notification_send_tmp.notification_send_dttm_tz,notification_send_tmp.occurrence_id,notification_send_tmp.properties_map_doc,notification_send_tmp.reserved_1_txt,notification_send_tmp.reserved_2_txt,notification_send_tmp.response_tracking_cd,notification_send_tmp.segment_id,notification_send_tmp.segment_version_id,notification_send_tmp.spot_id,notification_send_tmp.task_id,notification_send_tmp.task_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :notification_send_tmp           , notification_send, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..notification_send_tmp           ;
    quit;
    %put ######## Staging table: notification_send_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..notification_send;
      drop table work.notification_send;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..notification_targeting_reque_tmp;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=notification_targeting_request, table_keys=%str(event_id), out_table=work.notification_targeting_request);
 data &tmplib..notification_targeting_reque_tmp;
     set work.notification_targeting_request;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if notification_tgt_req_dttm ne . then notification_tgt_req_dttm = tzoneu2s(notification_tgt_req_dttm,&timeZone_Value.);if notification_tgt_req_dttm_tz ne . then notification_tgt_req_dttm_tz = tzoneu2s(notification_tgt_req_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :notification_targeting_reque_tmp, notification_targeting_request);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..notification_targeting_request using &tmpdbschema..notification_targeting_reque_tmp
         on (notification_targeting_request.event_id=notification_targeting_reque_tmp.event_id)
        when matched then  
        update set notification_targeting_request.aud_occurrence_id = notification_targeting_reque_tmp.aud_occurrence_id , notification_targeting_request.audience_id = notification_targeting_reque_tmp.audience_id , notification_targeting_request.channel_nm = notification_targeting_reque_tmp.channel_nm , notification_targeting_request.channel_user_id = notification_targeting_reque_tmp.channel_user_id , notification_targeting_request.context_type_nm = notification_targeting_reque_tmp.context_type_nm , notification_targeting_request.context_val = notification_targeting_reque_tmp.context_val , notification_targeting_request.eligibility_flg = notification_targeting_reque_tmp.eligibility_flg , notification_targeting_request.event_designed_id = notification_targeting_reque_tmp.event_designed_id , notification_targeting_request.event_nm = notification_targeting_reque_tmp.event_nm , notification_targeting_request.identity_id = notification_targeting_reque_tmp.identity_id , notification_targeting_request.journey_id = notification_targeting_reque_tmp.journey_id , notification_targeting_request.journey_occurrence_id = notification_targeting_reque_tmp.journey_occurrence_id , notification_targeting_request.load_dttm = notification_targeting_reque_tmp.load_dttm , notification_targeting_request.mobile_app_id = notification_targeting_reque_tmp.mobile_app_id , notification_targeting_request.notification_tgt_req_dttm = notification_targeting_reque_tmp.notification_tgt_req_dttm , notification_targeting_request.notification_tgt_req_dttm_tz = notification_targeting_reque_tmp.notification_tgt_req_dttm_tz
        when not matched then insert ( 
        aud_occurrence_id,audience_id,channel_nm,channel_user_id,context_type_nm,context_val,eligibility_flg,event_designed_id,event_id,event_nm,identity_id,journey_id,journey_occurrence_id,load_dttm,mobile_app_id,notification_tgt_req_dttm,notification_tgt_req_dttm_tz
         ) values ( 
        notification_targeting_reque_tmp.aud_occurrence_id,notification_targeting_reque_tmp.audience_id,notification_targeting_reque_tmp.channel_nm,notification_targeting_reque_tmp.channel_user_id,notification_targeting_reque_tmp.context_type_nm,notification_targeting_reque_tmp.context_val,notification_targeting_reque_tmp.eligibility_flg,notification_targeting_reque_tmp.event_designed_id,notification_targeting_reque_tmp.event_id,notification_targeting_reque_tmp.event_nm,notification_targeting_reque_tmp.identity_id,notification_targeting_reque_tmp.journey_id,notification_targeting_reque_tmp.journey_occurrence_id,notification_targeting_reque_tmp.load_dttm,notification_targeting_reque_tmp.mobile_app_id,notification_targeting_reque_tmp.notification_tgt_req_dttm,notification_targeting_reque_tmp.notification_tgt_req_dttm_tz
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :notification_targeting_reque_tmp, notification_targeting_request, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..notification_targeting_reque_tmp;
    quit;
    %put ######## Staging table: notification_targeting_reque_tmp Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..notification_targeting_request;
      drop table work.notification_targeting_request;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..order_details_tmp               ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=order_details, table_keys=%str(detail_id,event_designed_id,product_id,product_nm,product_sku,record_type), out_table=work.order_details);
 data &tmplib..order_details_tmp               ;
     set work.order_details;
  if activity_dttm ne . then activity_dttm = tzoneu2s(activity_dttm,&timeZone_Value.);if activity_dttm_tz ne . then activity_dttm_tz = tzoneu2s(activity_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if detail_id='' then detail_id='-'; if event_designed_id='' then event_designed_id='-'; if product_id='' then product_id='-'; if product_nm='' then product_nm='-'; if product_sku='' then product_sku='-'; if record_type='' then record_type='-';
 run;
 %ErrCheck (Failed to Append Data to :order_details_tmp               , order_details);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..order_details using &tmpdbschema..order_details_tmp               
         on (order_details.detail_id=order_details_tmp.detail_id and order_details.event_designed_id=order_details_tmp.event_designed_id and order_details.product_id=order_details_tmp.product_id and order_details.product_nm=order_details_tmp.product_nm and order_details.product_sku=order_details_tmp.product_sku and order_details.record_type=order_details_tmp.record_type)
        when matched then  
        update set order_details.activity_dttm = order_details_tmp.activity_dttm , order_details.activity_dttm_tz = order_details_tmp.activity_dttm_tz , order_details.availability_message_txt = order_details_tmp.availability_message_txt , order_details.cart_id = order_details_tmp.cart_id , order_details.cart_nm = order_details_tmp.cart_nm , order_details.channel_nm = order_details_tmp.channel_nm , order_details.currency_cd = order_details_tmp.currency_cd , order_details.detail_id_hex = order_details_tmp.detail_id_hex , order_details.event_id = order_details_tmp.event_id , order_details.event_key_cd = order_details_tmp.event_key_cd , order_details.event_nm = order_details_tmp.event_nm , order_details.event_source_cd = order_details_tmp.event_source_cd , order_details.identity_id = order_details_tmp.identity_id , order_details.load_dttm = order_details_tmp.load_dttm , order_details.mobile_app_id = order_details_tmp.mobile_app_id , order_details.order_id = order_details_tmp.order_id , order_details.product_group_nm = order_details_tmp.product_group_nm , order_details.properties_map_doc = order_details_tmp.properties_map_doc , order_details.quantity_amt = order_details_tmp.quantity_amt , order_details.reserved_1_txt = order_details_tmp.reserved_1_txt , order_details.saving_message_txt = order_details_tmp.saving_message_txt , order_details.session_id = order_details_tmp.session_id , order_details.session_id_hex = order_details_tmp.session_id_hex , order_details.shipping_message_txt = order_details_tmp.shipping_message_txt , order_details.unit_price_amt = order_details_tmp.unit_price_amt , order_details.visit_id = order_details_tmp.visit_id , order_details.visit_id_hex = order_details_tmp.visit_id_hex
        when not matched then insert ( 
        activity_dttm,activity_dttm_tz,availability_message_txt,cart_id,cart_nm,channel_nm,currency_cd,detail_id,detail_id_hex,event_designed_id,event_id,event_key_cd,event_nm,event_source_cd,identity_id,load_dttm,mobile_app_id,order_id,product_group_nm,product_id,product_nm,product_sku,properties_map_doc,quantity_amt,record_type,reserved_1_txt,saving_message_txt,session_id,session_id_hex,shipping_message_txt,unit_price_amt,visit_id,visit_id_hex
         ) values ( 
        order_details_tmp.activity_dttm,order_details_tmp.activity_dttm_tz,order_details_tmp.availability_message_txt,order_details_tmp.cart_id,order_details_tmp.cart_nm,order_details_tmp.channel_nm,order_details_tmp.currency_cd,order_details_tmp.detail_id,order_details_tmp.detail_id_hex,order_details_tmp.event_designed_id,order_details_tmp.event_id,order_details_tmp.event_key_cd,order_details_tmp.event_nm,order_details_tmp.event_source_cd,order_details_tmp.identity_id,order_details_tmp.load_dttm,order_details_tmp.mobile_app_id,order_details_tmp.order_id,order_details_tmp.product_group_nm,order_details_tmp.product_id,order_details_tmp.product_nm,order_details_tmp.product_sku,order_details_tmp.properties_map_doc,order_details_tmp.quantity_amt,order_details_tmp.record_type,order_details_tmp.reserved_1_txt,order_details_tmp.saving_message_txt,order_details_tmp.session_id,order_details_tmp.session_id_hex,order_details_tmp.shipping_message_txt,order_details_tmp.unit_price_amt,order_details_tmp.visit_id,order_details_tmp.visit_id_hex
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :order_details_tmp               , order_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..order_details_tmp               ;
    quit;
    %put ######## Staging table: order_details_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..order_details;
      drop table work.order_details;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..order_summary_tmp               ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=order_summary, table_keys=%str(detail_id,event_designed_id,record_type), out_table=work.order_summary);
 data &tmplib..order_summary_tmp               ;
     set work.order_summary;
  if activity_dttm ne . then activity_dttm = tzoneu2s(activity_dttm,&timeZone_Value.);if activity_dttm_tz ne . then activity_dttm_tz = tzoneu2s(activity_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if detail_id='' then detail_id='-'; if event_designed_id='' then event_designed_id='-'; if record_type='' then record_type='-';
 run;
 %ErrCheck (Failed to Append Data to :order_summary_tmp               , order_summary);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..order_summary using &tmpdbschema..order_summary_tmp               
         on (order_summary.detail_id=order_summary_tmp.detail_id and order_summary.event_designed_id=order_summary_tmp.event_designed_id and order_summary.record_type=order_summary_tmp.record_type)
        when matched then  
        update set order_summary.activity_dttm = order_summary_tmp.activity_dttm , order_summary.activity_dttm_tz = order_summary_tmp.activity_dttm_tz , order_summary.billing_city_nm = order_summary_tmp.billing_city_nm , order_summary.billing_country_nm = order_summary_tmp.billing_country_nm , order_summary.billing_postal_cd = order_summary_tmp.billing_postal_cd , order_summary.billing_state_region_cd = order_summary_tmp.billing_state_region_cd , order_summary.cart_id = order_summary_tmp.cart_id , order_summary.cart_nm = order_summary_tmp.cart_nm , order_summary.channel_nm = order_summary_tmp.channel_nm , order_summary.currency_cd = order_summary_tmp.currency_cd , order_summary.delivery_type_desc = order_summary_tmp.delivery_type_desc , order_summary.detail_id_hex = order_summary_tmp.detail_id_hex , order_summary.event_id = order_summary_tmp.event_id , order_summary.event_key_cd = order_summary_tmp.event_key_cd , order_summary.event_nm = order_summary_tmp.event_nm , order_summary.event_source_cd = order_summary_tmp.event_source_cd , order_summary.identity_id = order_summary_tmp.identity_id , order_summary.load_dttm = order_summary_tmp.load_dttm , order_summary.mobile_app_id = order_summary_tmp.mobile_app_id , order_summary.order_id = order_summary_tmp.order_id , order_summary.payment_type_desc = order_summary_tmp.payment_type_desc , order_summary.properties_map_doc = order_summary_tmp.properties_map_doc , order_summary.session_id = order_summary_tmp.session_id , order_summary.session_id_hex = order_summary_tmp.session_id_hex , order_summary.shipping_amt = order_summary_tmp.shipping_amt , order_summary.shipping_city_nm = order_summary_tmp.shipping_city_nm , order_summary.shipping_country_nm = order_summary_tmp.shipping_country_nm , order_summary.shipping_postal_cd = order_summary_tmp.shipping_postal_cd , order_summary.shipping_state_region_cd = order_summary_tmp.shipping_state_region_cd , order_summary.total_price_amt = order_summary_tmp.total_price_amt , order_summary.total_tax_amt = order_summary_tmp.total_tax_amt , order_summary.total_unit_qty = order_summary_tmp.total_unit_qty , order_summary.visit_id = order_summary_tmp.visit_id , order_summary.visit_id_hex = order_summary_tmp.visit_id_hex
        when not matched then insert ( 
        activity_dttm,activity_dttm_tz,billing_city_nm,billing_country_nm,billing_postal_cd,billing_state_region_cd,cart_id,cart_nm,channel_nm,currency_cd,delivery_type_desc,detail_id,detail_id_hex,event_designed_id,event_id,event_key_cd,event_nm,event_source_cd,identity_id,load_dttm,mobile_app_id,order_id,payment_type_desc,properties_map_doc,record_type,session_id,session_id_hex,shipping_amt,shipping_city_nm,shipping_country_nm,shipping_postal_cd,shipping_state_region_cd,total_price_amt,total_tax_amt,total_unit_qty,visit_id,visit_id_hex
         ) values ( 
        order_summary_tmp.activity_dttm,order_summary_tmp.activity_dttm_tz,order_summary_tmp.billing_city_nm,order_summary_tmp.billing_country_nm,order_summary_tmp.billing_postal_cd,order_summary_tmp.billing_state_region_cd,order_summary_tmp.cart_id,order_summary_tmp.cart_nm,order_summary_tmp.channel_nm,order_summary_tmp.currency_cd,order_summary_tmp.delivery_type_desc,order_summary_tmp.detail_id,order_summary_tmp.detail_id_hex,order_summary_tmp.event_designed_id,order_summary_tmp.event_id,order_summary_tmp.event_key_cd,order_summary_tmp.event_nm,order_summary_tmp.event_source_cd,order_summary_tmp.identity_id,order_summary_tmp.load_dttm,order_summary_tmp.mobile_app_id,order_summary_tmp.order_id,order_summary_tmp.payment_type_desc,order_summary_tmp.properties_map_doc,order_summary_tmp.record_type,order_summary_tmp.session_id,order_summary_tmp.session_id_hex,order_summary_tmp.shipping_amt,order_summary_tmp.shipping_city_nm,order_summary_tmp.shipping_country_nm,order_summary_tmp.shipping_postal_cd,order_summary_tmp.shipping_state_region_cd,order_summary_tmp.total_price_amt,order_summary_tmp.total_tax_amt,order_summary_tmp.total_unit_qty,order_summary_tmp.visit_id,order_summary_tmp.visit_id_hex
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :order_summary_tmp               , order_summary, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..order_summary_tmp               ;
    quit;
    %put ######## Staging table: order_summary_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..order_summary;
      drop table work.order_summary;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..outbound_system_tmp             ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=outbound_system, table_keys=%str(event_id), out_table=work.outbound_system);
 data &tmplib..outbound_system_tmp             ;
     set work.outbound_system;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if outbound_system_dttm ne . then outbound_system_dttm = tzoneu2s(outbound_system_dttm,&timeZone_Value.);if outbound_system_dttm_tz ne . then outbound_system_dttm_tz = tzoneu2s(outbound_system_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :outbound_system_tmp             , outbound_system);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..outbound_system using &tmpdbschema..outbound_system_tmp             
         on (outbound_system.event_id=outbound_system_tmp.event_id)
        when matched then  
        update set outbound_system.aud_occurrence_id = outbound_system_tmp.aud_occurrence_id , outbound_system.audience_id = outbound_system_tmp.audience_id , outbound_system.channel_nm = outbound_system_tmp.channel_nm , outbound_system.channel_user_id = outbound_system_tmp.channel_user_id , outbound_system.context_type_nm = outbound_system_tmp.context_type_nm , outbound_system.context_val = outbound_system_tmp.context_val , outbound_system.creative_id = outbound_system_tmp.creative_id , outbound_system.creative_version_id = outbound_system_tmp.creative_version_id , outbound_system.detail_id_hex = outbound_system_tmp.detail_id_hex , outbound_system.event_designed_id = outbound_system_tmp.event_designed_id , outbound_system.event_nm = outbound_system_tmp.event_nm , outbound_system.identity_id = outbound_system_tmp.identity_id , outbound_system.journey_id = outbound_system_tmp.journey_id , outbound_system.journey_occurrence_id = outbound_system_tmp.journey_occurrence_id , outbound_system.load_dttm = outbound_system_tmp.load_dttm , outbound_system.message_id = outbound_system_tmp.message_id , outbound_system.message_version_id = outbound_system_tmp.message_version_id , outbound_system.mobile_app_id = outbound_system_tmp.mobile_app_id , outbound_system.occurrence_id = outbound_system_tmp.occurrence_id , outbound_system.outbound_system_dttm = outbound_system_tmp.outbound_system_dttm , outbound_system.outbound_system_dttm_tz = outbound_system_tmp.outbound_system_dttm_tz , outbound_system.parent_event_id = outbound_system_tmp.parent_event_id , outbound_system.properties_map_doc = outbound_system_tmp.properties_map_doc , outbound_system.reserved_1_txt = outbound_system_tmp.reserved_1_txt , outbound_system.reserved_2_txt = outbound_system_tmp.reserved_2_txt , outbound_system.response_tracking_cd = outbound_system_tmp.response_tracking_cd , outbound_system.segment_id = outbound_system_tmp.segment_id , outbound_system.segment_version_id = outbound_system_tmp.segment_version_id , outbound_system.session_id_hex = outbound_system_tmp.session_id_hex , outbound_system.spot_id = outbound_system_tmp.spot_id , outbound_system.task_id = outbound_system_tmp.task_id , outbound_system.task_version_id = outbound_system_tmp.task_version_id , outbound_system.visit_id_hex = outbound_system_tmp.visit_id_hex
        when not matched then insert ( 
        aud_occurrence_id,audience_id,channel_nm,channel_user_id,context_type_nm,context_val,creative_id,creative_version_id,detail_id_hex,event_designed_id,event_id,event_nm,identity_id,journey_id,journey_occurrence_id,load_dttm,message_id,message_version_id,mobile_app_id,occurrence_id,outbound_system_dttm,outbound_system_dttm_tz,parent_event_id,properties_map_doc,reserved_1_txt,reserved_2_txt,response_tracking_cd,segment_id,segment_version_id,session_id_hex,spot_id,task_id,task_version_id,visit_id_hex
         ) values ( 
        outbound_system_tmp.aud_occurrence_id,outbound_system_tmp.audience_id,outbound_system_tmp.channel_nm,outbound_system_tmp.channel_user_id,outbound_system_tmp.context_type_nm,outbound_system_tmp.context_val,outbound_system_tmp.creative_id,outbound_system_tmp.creative_version_id,outbound_system_tmp.detail_id_hex,outbound_system_tmp.event_designed_id,outbound_system_tmp.event_id,outbound_system_tmp.event_nm,outbound_system_tmp.identity_id,outbound_system_tmp.journey_id,outbound_system_tmp.journey_occurrence_id,outbound_system_tmp.load_dttm,outbound_system_tmp.message_id,outbound_system_tmp.message_version_id,outbound_system_tmp.mobile_app_id,outbound_system_tmp.occurrence_id,outbound_system_tmp.outbound_system_dttm,outbound_system_tmp.outbound_system_dttm_tz,outbound_system_tmp.parent_event_id,outbound_system_tmp.properties_map_doc,outbound_system_tmp.reserved_1_txt,outbound_system_tmp.reserved_2_txt,outbound_system_tmp.response_tracking_cd,outbound_system_tmp.segment_id,outbound_system_tmp.segment_version_id,outbound_system_tmp.session_id_hex,outbound_system_tmp.spot_id,outbound_system_tmp.task_id,outbound_system_tmp.task_version_id,outbound_system_tmp.visit_id_hex
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :outbound_system_tmp             , outbound_system, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..outbound_system_tmp             ;
    quit;
    %put ######## Staging table: outbound_system_tmp              Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..outbound_system;
      drop table work.outbound_system;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..page_details_tmp                ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=page_details, table_keys=%str(detail_id), out_table=work.page_details);
 data &tmplib..page_details_tmp                ;
     set work.page_details;
  if detail_dttm ne . then detail_dttm = tzoneu2s(detail_dttm,&timeZone_Value.);if detail_dttm_tz ne . then detail_dttm_tz = tzoneu2s(detail_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if detail_id='' then detail_id='-';
 run;
 %ErrCheck (Failed to Append Data to :page_details_tmp                , page_details);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..page_details using &tmpdbschema..page_details_tmp                
         on (page_details.detail_id=page_details_tmp.detail_id)
        when matched then  
        update set page_details.bytes_sent_cnt = page_details_tmp.bytes_sent_cnt , page_details.channel_nm = page_details_tmp.channel_nm , page_details.class10_id = page_details_tmp.class10_id , page_details.class11_id = page_details_tmp.class11_id , page_details.class12_id = page_details_tmp.class12_id , page_details.class13_id = page_details_tmp.class13_id , page_details.class14_id = page_details_tmp.class14_id , page_details.class15_id = page_details_tmp.class15_id , page_details.class1_id = page_details_tmp.class1_id , page_details.class2_id = page_details_tmp.class2_id , page_details.class3_id = page_details_tmp.class3_id , page_details.class4_id = page_details_tmp.class4_id , page_details.class5_id = page_details_tmp.class5_id , page_details.class6_id = page_details_tmp.class6_id , page_details.class7_id = page_details_tmp.class7_id , page_details.class8_id = page_details_tmp.class8_id , page_details.class9_id = page_details_tmp.class9_id , page_details.detail_dttm = page_details_tmp.detail_dttm , page_details.detail_dttm_tz = page_details_tmp.detail_dttm_tz , page_details.detail_id_hex = page_details_tmp.detail_id_hex , page_details.domain_nm = page_details_tmp.domain_nm , page_details.event_id = page_details_tmp.event_id , page_details.event_key_cd = page_details_tmp.event_key_cd , page_details.event_nm = page_details_tmp.event_nm , page_details.event_source_cd = page_details_tmp.event_source_cd , page_details.identity_id = page_details_tmp.identity_id , page_details.load_dttm = page_details_tmp.load_dttm , page_details.mobile_app_id = page_details_tmp.mobile_app_id , page_details.page_complete_sec_cnt = page_details_tmp.page_complete_sec_cnt , page_details.page_desc = page_details_tmp.page_desc , page_details.page_load_sec_cnt = page_details_tmp.page_load_sec_cnt , page_details.page_url_txt = page_details_tmp.page_url_txt , page_details.protocol_nm = page_details_tmp.protocol_nm , page_details.referrer_url_txt = page_details_tmp.referrer_url_txt , page_details.session_dt = page_details_tmp.session_dt , page_details.session_dt_tz = page_details_tmp.session_dt_tz , page_details.session_id = page_details_tmp.session_id , page_details.session_id_hex = page_details_tmp.session_id_hex , page_details.url_domain = page_details_tmp.url_domain , page_details.visit_id = page_details_tmp.visit_id , page_details.visit_id_hex = page_details_tmp.visit_id_hex , page_details.window_size_txt = page_details_tmp.window_size_txt
        when not matched then insert ( 
        bytes_sent_cnt,channel_nm,class10_id,class11_id,class12_id,class13_id,class14_id,class15_id,class1_id,class2_id,class3_id,class4_id,class5_id,class6_id,class7_id,class8_id,class9_id,detail_dttm,detail_dttm_tz,detail_id,detail_id_hex,domain_nm,event_id,event_key_cd,event_nm,event_source_cd,identity_id,load_dttm,mobile_app_id,page_complete_sec_cnt,page_desc,page_load_sec_cnt,page_url_txt,protocol_nm,referrer_url_txt,session_dt,session_dt_tz,session_id,session_id_hex,url_domain,visit_id,visit_id_hex,window_size_txt
         ) values ( 
        page_details_tmp.bytes_sent_cnt,page_details_tmp.channel_nm,page_details_tmp.class10_id,page_details_tmp.class11_id,page_details_tmp.class12_id,page_details_tmp.class13_id,page_details_tmp.class14_id,page_details_tmp.class15_id,page_details_tmp.class1_id,page_details_tmp.class2_id,page_details_tmp.class3_id,page_details_tmp.class4_id,page_details_tmp.class5_id,page_details_tmp.class6_id,page_details_tmp.class7_id,page_details_tmp.class8_id,page_details_tmp.class9_id,page_details_tmp.detail_dttm,page_details_tmp.detail_dttm_tz,page_details_tmp.detail_id,page_details_tmp.detail_id_hex,page_details_tmp.domain_nm,page_details_tmp.event_id,page_details_tmp.event_key_cd,page_details_tmp.event_nm,page_details_tmp.event_source_cd,page_details_tmp.identity_id,page_details_tmp.load_dttm,page_details_tmp.mobile_app_id,page_details_tmp.page_complete_sec_cnt,page_details_tmp.page_desc,page_details_tmp.page_load_sec_cnt,page_details_tmp.page_url_txt,page_details_tmp.protocol_nm,page_details_tmp.referrer_url_txt,page_details_tmp.session_dt,page_details_tmp.session_dt_tz,page_details_tmp.session_id,page_details_tmp.session_id_hex,page_details_tmp.url_domain,page_details_tmp.visit_id,page_details_tmp.visit_id_hex,page_details_tmp.window_size_txt
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :page_details_tmp                , page_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..page_details_tmp                ;
    quit;
    %put ######## Staging table: page_details_tmp                 Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..page_details;
      drop table work.page_details;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..page_details_ext_tmp            ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=page_details_ext, table_keys=%str(detail_id,load_dttm,session_id), out_table=work.page_details_ext);
 data &tmplib..page_details_ext_tmp            ;
     set work.page_details_ext;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if detail_id='' then detail_id='-'; if session_id='' then session_id='-';
 run;
 %ErrCheck (Failed to Append Data to :page_details_ext_tmp            , page_details_ext);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..page_details_ext using &tmpdbschema..page_details_ext_tmp            
         on (page_details_ext.detail_id=page_details_ext_tmp.detail_id and page_details_ext.load_dttm=page_details_ext_tmp.load_dttm and page_details_ext.session_id=page_details_ext_tmp.session_id)
        when matched then  
        update set page_details_ext.active_sec_spent_on_page_cnt = page_details_ext_tmp.active_sec_spent_on_page_cnt , page_details_ext.detail_id_hex = page_details_ext_tmp.detail_id_hex , page_details_ext.seconds_spent_on_page_cnt = page_details_ext_tmp.seconds_spent_on_page_cnt , page_details_ext.session_id_hex = page_details_ext_tmp.session_id_hex
        when not matched then insert ( 
        active_sec_spent_on_page_cnt,detail_id,detail_id_hex,load_dttm,seconds_spent_on_page_cnt,session_id,session_id_hex
         ) values ( 
        page_details_ext_tmp.active_sec_spent_on_page_cnt,page_details_ext_tmp.detail_id,page_details_ext_tmp.detail_id_hex,page_details_ext_tmp.load_dttm,page_details_ext_tmp.seconds_spent_on_page_cnt,page_details_ext_tmp.session_id,page_details_ext_tmp.session_id_hex
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :page_details_ext_tmp            , page_details_ext, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..page_details_ext_tmp            ;
    quit;
    %put ######## Staging table: page_details_ext_tmp             Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..page_details_ext;
      drop table work.page_details_ext;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..page_errors_tmp                 ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=page_errors, table_keys=%str(detail_id,error_location_txt,in_page_error_txt), out_table=work.page_errors);
 data &tmplib..page_errors_tmp                 ;
     set work.page_errors;
  if in_page_error_dttm ne . then in_page_error_dttm = tzoneu2s(in_page_error_dttm,&timeZone_Value.);if in_page_error_dttm_tz ne . then in_page_error_dttm_tz = tzoneu2s(in_page_error_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if detail_id='' then detail_id='-'; if error_location_txt='' then error_location_txt='-'; if in_page_error_txt='' then in_page_error_txt='-';
 run;
 %ErrCheck (Failed to Append Data to :page_errors_tmp                 , page_errors);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..page_errors using &tmpdbschema..page_errors_tmp                 
         on (page_errors.detail_id=page_errors_tmp.detail_id and page_errors.error_location_txt=page_errors_tmp.error_location_txt and page_errors.in_page_error_txt=page_errors_tmp.in_page_error_txt)
        when matched then  
        update set page_errors.detail_id_hex = page_errors_tmp.detail_id_hex , page_errors.event_id = page_errors_tmp.event_id , page_errors.event_source_cd = page_errors_tmp.event_source_cd , page_errors.identity_id = page_errors_tmp.identity_id , page_errors.in_page_error_dttm = page_errors_tmp.in_page_error_dttm , page_errors.in_page_error_dttm_tz = page_errors_tmp.in_page_error_dttm_tz , page_errors.load_dttm = page_errors_tmp.load_dttm , page_errors.session_id = page_errors_tmp.session_id , page_errors.session_id_hex = page_errors_tmp.session_id_hex , page_errors.visit_id = page_errors_tmp.visit_id , page_errors.visit_id_hex = page_errors_tmp.visit_id_hex
        when not matched then insert ( 
        detail_id,detail_id_hex,error_location_txt,event_id,event_source_cd,identity_id,in_page_error_dttm,in_page_error_dttm_tz,in_page_error_txt,load_dttm,session_id,session_id_hex,visit_id,visit_id_hex
         ) values ( 
        page_errors_tmp.detail_id,page_errors_tmp.detail_id_hex,page_errors_tmp.error_location_txt,page_errors_tmp.event_id,page_errors_tmp.event_source_cd,page_errors_tmp.identity_id,page_errors_tmp.in_page_error_dttm,page_errors_tmp.in_page_error_dttm_tz,page_errors_tmp.in_page_error_txt,page_errors_tmp.load_dttm,page_errors_tmp.session_id,page_errors_tmp.session_id_hex,page_errors_tmp.visit_id,page_errors_tmp.visit_id_hex
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :page_errors_tmp                 , page_errors, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..page_errors_tmp                 ;
    quit;
    %put ######## Staging table: page_errors_tmp                  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..page_errors;
      drop table work.page_errors;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..planning_hierarchy_defn_tmp     ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=planning_hierarchy_defn, table_keys=%str(hier_defn_id,level_nm,level_no), out_table=work.planning_hierarchy_defn);
 data &tmplib..planning_hierarchy_defn_tmp     ;
     set work.planning_hierarchy_defn;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if hier_defn_id='' then hier_defn_id='-'; if level_nm='' then level_nm='-';
 run;
 %ErrCheck (Failed to Append Data to :planning_hierarchy_defn_tmp     , planning_hierarchy_defn);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..planning_hierarchy_defn using &tmpdbschema..planning_hierarchy_defn_tmp     
         on (planning_hierarchy_defn.hier_defn_id=planning_hierarchy_defn_tmp.hier_defn_id and planning_hierarchy_defn.level_nm=planning_hierarchy_defn_tmp.level_nm and planning_hierarchy_defn.level_no=planning_hierarchy_defn_tmp.level_no)
        when matched then  
        update set planning_hierarchy_defn.created_by_usernm = planning_hierarchy_defn_tmp.created_by_usernm , planning_hierarchy_defn.created_dttm = planning_hierarchy_defn_tmp.created_dttm , planning_hierarchy_defn.hier_defn_desc = planning_hierarchy_defn_tmp.hier_defn_desc , planning_hierarchy_defn.hier_defn_nm = planning_hierarchy_defn_tmp.hier_defn_nm , planning_hierarchy_defn.hier_defn_subtype = planning_hierarchy_defn_tmp.hier_defn_subtype , planning_hierarchy_defn.hier_defn_type = planning_hierarchy_defn_tmp.hier_defn_type , planning_hierarchy_defn.last_modified_dttm = planning_hierarchy_defn_tmp.last_modified_dttm , planning_hierarchy_defn.last_modified_usernm = planning_hierarchy_defn_tmp.last_modified_usernm , planning_hierarchy_defn.level_desc = planning_hierarchy_defn_tmp.level_desc , planning_hierarchy_defn.load_dttm = planning_hierarchy_defn_tmp.load_dttm
        when not matched then insert ( 
        created_by_usernm,created_dttm,hier_defn_desc,hier_defn_id,hier_defn_nm,hier_defn_subtype,hier_defn_type,last_modified_dttm,last_modified_usernm,level_desc,level_nm,level_no,load_dttm
         ) values ( 
        planning_hierarchy_defn_tmp.created_by_usernm,planning_hierarchy_defn_tmp.created_dttm,planning_hierarchy_defn_tmp.hier_defn_desc,planning_hierarchy_defn_tmp.hier_defn_id,planning_hierarchy_defn_tmp.hier_defn_nm,planning_hierarchy_defn_tmp.hier_defn_subtype,planning_hierarchy_defn_tmp.hier_defn_type,planning_hierarchy_defn_tmp.last_modified_dttm,planning_hierarchy_defn_tmp.last_modified_usernm,planning_hierarchy_defn_tmp.level_desc,planning_hierarchy_defn_tmp.level_nm,planning_hierarchy_defn_tmp.level_no,planning_hierarchy_defn_tmp.load_dttm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :planning_hierarchy_defn_tmp     , planning_hierarchy_defn, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..planning_hierarchy_defn_tmp     ;
    quit;
    %put ######## Staging table: planning_hierarchy_defn_tmp      Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..planning_hierarchy_defn;
      drop table work.planning_hierarchy_defn;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..planning_info_tmp               ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=planning_info, table_keys=%str(hier_defn_id,planning_id), out_table=work.planning_info);
 data &tmplib..planning_info_tmp               ;
     set work.planning_info;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if planned_end_dttm ne . then planned_end_dttm = tzoneu2s(planned_end_dttm,&timeZone_Value.);if planned_start_dttm ne . then planned_start_dttm = tzoneu2s(planned_start_dttm,&timeZone_Value.) ;
  if hier_defn_id='' then hier_defn_id='-'; if planning_id='' then planning_id='-';
 run;
 %ErrCheck (Failed to Append Data to :planning_info_tmp               , planning_info);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..planning_info using &tmpdbschema..planning_info_tmp               
         on (planning_info.hier_defn_id=planning_info_tmp.hier_defn_id and planning_info.planning_id=planning_info_tmp.planning_id)
        when matched then  
        update set planning_info.activity_desc = planning_info_tmp.activity_desc , planning_info.activity_id = planning_info_tmp.activity_id , planning_info.activity_nm = planning_info_tmp.activity_nm , planning_info.activity_status = planning_info_tmp.activity_status , planning_info.all_msgs = planning_info_tmp.all_msgs , planning_info.alloc_budget = planning_info_tmp.alloc_budget , planning_info.available_budget = planning_info_tmp.available_budget , planning_info.bu_currency_cd = planning_info_tmp.bu_currency_cd , planning_info.bu_desc = planning_info_tmp.bu_desc , planning_info.bu_id = planning_info_tmp.bu_id , planning_info.bu_nm = planning_info_tmp.bu_nm , planning_info.bu_obsolete_flg = planning_info_tmp.bu_obsolete_flg , planning_info.category_nm = planning_info_tmp.category_nm , planning_info.created_by_usernm = planning_info_tmp.created_by_usernm , planning_info.created_dttm = planning_info_tmp.created_dttm , planning_info.currency_cd = planning_info_tmp.currency_cd , planning_info.hier_defn_nodeid = planning_info_tmp.hier_defn_nodeid , planning_info.last_modified_dttm = planning_info_tmp.last_modified_dttm , planning_info.last_modified_usernm = planning_info_tmp.last_modified_usernm , planning_info.lev10_nm = planning_info_tmp.lev10_nm , planning_info.lev1_nm = planning_info_tmp.lev1_nm , planning_info.lev2_nm = planning_info_tmp.lev2_nm , planning_info.lev3_nm = planning_info_tmp.lev3_nm , planning_info.lev4_nm = planning_info_tmp.lev4_nm , planning_info.lev5_nm = planning_info_tmp.lev5_nm , planning_info.lev6_nm = planning_info_tmp.lev6_nm , planning_info.lev7_nm = planning_info_tmp.lev7_nm , planning_info.lev8_nm = planning_info_tmp.lev8_nm , planning_info.lev9_nm = planning_info_tmp.lev9_nm , planning_info.load_dttm = planning_info_tmp.load_dttm , planning_info.parent_id = planning_info_tmp.parent_id , planning_info.parent_nm = planning_info_tmp.parent_nm , planning_info.planned_end_dttm = planning_info_tmp.planned_end_dttm , planning_info.planned_start_dttm = planning_info_tmp.planned_start_dttm , planning_info.planning_desc = planning_info_tmp.planning_desc , planning_info.planning_item_path = planning_info_tmp.planning_item_path , planning_info.planning_level_no = planning_info_tmp.planning_level_no , planning_info.planning_level_type = planning_info_tmp.planning_level_type , planning_info.planning_nm = planning_info_tmp.planning_nm , planning_info.planning_number = planning_info_tmp.planning_number , planning_info.planning_owner_usernm = planning_info_tmp.planning_owner_usernm , planning_info.planning_status = planning_info_tmp.planning_status , planning_info.planning_type = planning_info_tmp.planning_type , planning_info.reserved_budget = planning_info_tmp.reserved_budget , planning_info.reserved_budget_same_flg = planning_info_tmp.reserved_budget_same_flg , planning_info.rolledup_budget = planning_info_tmp.rolledup_budget , planning_info.task_channel = planning_info_tmp.task_channel , planning_info.task_desc = planning_info_tmp.task_desc , planning_info.task_id = planning_info_tmp.task_id , planning_info.task_nm = planning_info_tmp.task_nm , planning_info.task_status = planning_info_tmp.task_status , planning_info.tot_cmtmnt_outstanding = planning_info_tmp.tot_cmtmnt_outstanding , planning_info.tot_cmtmnt_overspent = planning_info_tmp.tot_cmtmnt_overspent , planning_info.tot_committed = planning_info_tmp.tot_committed , planning_info.tot_expenses = planning_info_tmp.tot_expenses , planning_info.tot_invoiced = planning_info_tmp.tot_invoiced , planning_info.total_budget = planning_info_tmp.total_budget
        when not matched then insert ( 
        activity_desc,activity_id,activity_nm,activity_status,all_msgs,alloc_budget,available_budget,bu_currency_cd,bu_desc,bu_id,bu_nm,bu_obsolete_flg,category_nm,created_by_usernm,created_dttm,currency_cd,hier_defn_id,hier_defn_nodeid,last_modified_dttm,last_modified_usernm,lev10_nm,lev1_nm,lev2_nm,lev3_nm,lev4_nm,lev5_nm,lev6_nm,lev7_nm,lev8_nm,lev9_nm,load_dttm,parent_id,parent_nm,planned_end_dttm,planned_start_dttm,planning_desc,planning_id,planning_item_path,planning_level_no,planning_level_type,planning_nm,planning_number,planning_owner_usernm,planning_status,planning_type,reserved_budget,reserved_budget_same_flg,rolledup_budget,task_channel,task_desc,task_id,task_nm,task_status,tot_cmtmnt_outstanding,tot_cmtmnt_overspent,tot_committed,tot_expenses,tot_invoiced,total_budget
         ) values ( 
        planning_info_tmp.activity_desc,planning_info_tmp.activity_id,planning_info_tmp.activity_nm,planning_info_tmp.activity_status,planning_info_tmp.all_msgs,planning_info_tmp.alloc_budget,planning_info_tmp.available_budget,planning_info_tmp.bu_currency_cd,planning_info_tmp.bu_desc,planning_info_tmp.bu_id,planning_info_tmp.bu_nm,planning_info_tmp.bu_obsolete_flg,planning_info_tmp.category_nm,planning_info_tmp.created_by_usernm,planning_info_tmp.created_dttm,planning_info_tmp.currency_cd,planning_info_tmp.hier_defn_id,planning_info_tmp.hier_defn_nodeid,planning_info_tmp.last_modified_dttm,planning_info_tmp.last_modified_usernm,planning_info_tmp.lev10_nm,planning_info_tmp.lev1_nm,planning_info_tmp.lev2_nm,planning_info_tmp.lev3_nm,planning_info_tmp.lev4_nm,planning_info_tmp.lev5_nm,planning_info_tmp.lev6_nm,planning_info_tmp.lev7_nm,planning_info_tmp.lev8_nm,planning_info_tmp.lev9_nm,planning_info_tmp.load_dttm,planning_info_tmp.parent_id,planning_info_tmp.parent_nm,planning_info_tmp.planned_end_dttm,planning_info_tmp.planned_start_dttm,planning_info_tmp.planning_desc,planning_info_tmp.planning_id,planning_info_tmp.planning_item_path,planning_info_tmp.planning_level_no,planning_info_tmp.planning_level_type,planning_info_tmp.planning_nm,planning_info_tmp.planning_number,planning_info_tmp.planning_owner_usernm,planning_info_tmp.planning_status,planning_info_tmp.planning_type,planning_info_tmp.reserved_budget,planning_info_tmp.reserved_budget_same_flg,planning_info_tmp.rolledup_budget,planning_info_tmp.task_channel,planning_info_tmp.task_desc,planning_info_tmp.task_id,planning_info_tmp.task_nm,planning_info_tmp.task_status,planning_info_tmp.tot_cmtmnt_outstanding,planning_info_tmp.tot_cmtmnt_overspent,planning_info_tmp.tot_committed,planning_info_tmp.tot_expenses,planning_info_tmp.tot_invoiced,planning_info_tmp.total_budget
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :planning_info_tmp               , planning_info, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..planning_info_tmp               ;
    quit;
    %put ######## Staging table: planning_info_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..planning_info;
      drop table work.planning_info;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..planning_info_custom_prop_tmp   ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=planning_info_custom_prop, table_keys=%str(attr_group_id,attr_id,planning_id), out_table=work.planning_info_custom_prop);
 data &tmplib..planning_info_custom_prop_tmp   ;
     set work.planning_info_custom_prop;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if attr_group_id='' then attr_group_id='-'; if attr_id='' then attr_id='-'; if planning_id='' then planning_id='-';
 run;
 %ErrCheck (Failed to Append Data to :planning_info_custom_prop_tmp   , planning_info_custom_prop);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..planning_info_custom_prop using &tmpdbschema..planning_info_custom_prop_tmp   
         on (planning_info_custom_prop.attr_group_id=planning_info_custom_prop_tmp.attr_group_id and planning_info_custom_prop.attr_id=planning_info_custom_prop_tmp.attr_id and planning_info_custom_prop.planning_id=planning_info_custom_prop_tmp.planning_id)
        when matched then  
        update set planning_info_custom_prop.attr_cd = planning_info_custom_prop_tmp.attr_cd , planning_info_custom_prop.attr_group_cd = planning_info_custom_prop_tmp.attr_group_cd , planning_info_custom_prop.attr_group_nm = planning_info_custom_prop_tmp.attr_group_nm , planning_info_custom_prop.attr_nm = planning_info_custom_prop_tmp.attr_nm , planning_info_custom_prop.attr_val = planning_info_custom_prop_tmp.attr_val , planning_info_custom_prop.created_by_usernm = planning_info_custom_prop_tmp.created_by_usernm , planning_info_custom_prop.created_dttm = planning_info_custom_prop_tmp.created_dttm , planning_info_custom_prop.data_formatter = planning_info_custom_prop_tmp.data_formatter , planning_info_custom_prop.data_type = planning_info_custom_prop_tmp.data_type , planning_info_custom_prop.is_grid_flg = planning_info_custom_prop_tmp.is_grid_flg , planning_info_custom_prop.is_obsolete_flg = planning_info_custom_prop_tmp.is_obsolete_flg , planning_info_custom_prop.last_modified_dttm = planning_info_custom_prop_tmp.last_modified_dttm , planning_info_custom_prop.last_modified_usernm = planning_info_custom_prop_tmp.last_modified_usernm , planning_info_custom_prop.load_dttm = planning_info_custom_prop_tmp.load_dttm , planning_info_custom_prop.remote_pklist_tab_col = planning_info_custom_prop_tmp.remote_pklist_tab_col
        when not matched then insert ( 
        attr_cd,attr_group_cd,attr_group_id,attr_group_nm,attr_id,attr_nm,attr_val,created_by_usernm,created_dttm,data_formatter,data_type,is_grid_flg,is_obsolete_flg,last_modified_dttm,last_modified_usernm,load_dttm,planning_id,remote_pklist_tab_col
         ) values ( 
        planning_info_custom_prop_tmp.attr_cd,planning_info_custom_prop_tmp.attr_group_cd,planning_info_custom_prop_tmp.attr_group_id,planning_info_custom_prop_tmp.attr_group_nm,planning_info_custom_prop_tmp.attr_id,planning_info_custom_prop_tmp.attr_nm,planning_info_custom_prop_tmp.attr_val,planning_info_custom_prop_tmp.created_by_usernm,planning_info_custom_prop_tmp.created_dttm,planning_info_custom_prop_tmp.data_formatter,planning_info_custom_prop_tmp.data_type,planning_info_custom_prop_tmp.is_grid_flg,planning_info_custom_prop_tmp.is_obsolete_flg,planning_info_custom_prop_tmp.last_modified_dttm,planning_info_custom_prop_tmp.last_modified_usernm,planning_info_custom_prop_tmp.load_dttm,planning_info_custom_prop_tmp.planning_id,planning_info_custom_prop_tmp.remote_pklist_tab_col
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :planning_info_custom_prop_tmp   , planning_info_custom_prop, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..planning_info_custom_prop_tmp   ;
    quit;
    %put ######## Staging table: planning_info_custom_prop_tmp    Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..planning_info_custom_prop;
      drop table work.planning_info_custom_prop;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..product_views_tmp               ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=product_views, table_keys=%str(detail_id,product_id,product_nm,product_sku), out_table=work.product_views);
 data &tmplib..product_views_tmp               ;
     set work.product_views;
  if action_dttm ne . then action_dttm = tzoneu2s(action_dttm,&timeZone_Value.);if action_dttm_tz ne . then action_dttm_tz = tzoneu2s(action_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if detail_id='' then detail_id='-'; if product_id='' then product_id='-'; if product_nm='' then product_nm='-'; if product_sku='' then product_sku='-';
 run;
 %ErrCheck (Failed to Append Data to :product_views_tmp               , product_views);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..product_views using &tmpdbschema..product_views_tmp               
         on (product_views.detail_id=product_views_tmp.detail_id and product_views.product_id=product_views_tmp.product_id and product_views.product_nm=product_views_tmp.product_nm and product_views.product_sku=product_views_tmp.product_sku)
        when matched then  
        update set product_views.action_dttm = product_views_tmp.action_dttm , product_views.action_dttm_tz = product_views_tmp.action_dttm_tz , product_views.availability_message_txt = product_views_tmp.availability_message_txt , product_views.channel_nm = product_views_tmp.channel_nm , product_views.currency_cd = product_views_tmp.currency_cd , product_views.detail_id_hex = product_views_tmp.detail_id_hex , product_views.event_designed_id = product_views_tmp.event_designed_id , product_views.event_id = product_views_tmp.event_id , product_views.event_key_cd = product_views_tmp.event_key_cd , product_views.event_nm = product_views_tmp.event_nm , product_views.event_source_cd = product_views_tmp.event_source_cd , product_views.identity_id = product_views_tmp.identity_id , product_views.load_dttm = product_views_tmp.load_dttm , product_views.mobile_app_id = product_views_tmp.mobile_app_id , product_views.price_val = product_views_tmp.price_val , product_views.product_group_nm = product_views_tmp.product_group_nm , product_views.properties_map_doc = product_views_tmp.properties_map_doc , product_views.saving_message_txt = product_views_tmp.saving_message_txt , product_views.session_id = product_views_tmp.session_id , product_views.session_id_hex = product_views_tmp.session_id_hex , product_views.shipping_message_txt = product_views_tmp.shipping_message_txt , product_views.visit_id = product_views_tmp.visit_id , product_views.visit_id_hex = product_views_tmp.visit_id_hex
        when not matched then insert ( 
        action_dttm,action_dttm_tz,availability_message_txt,channel_nm,currency_cd,detail_id,detail_id_hex,event_designed_id,event_id,event_key_cd,event_nm,event_source_cd,identity_id,load_dttm,mobile_app_id,price_val,product_group_nm,product_id,product_nm,product_sku,properties_map_doc,saving_message_txt,session_id,session_id_hex,shipping_message_txt,visit_id,visit_id_hex
         ) values ( 
        product_views_tmp.action_dttm,product_views_tmp.action_dttm_tz,product_views_tmp.availability_message_txt,product_views_tmp.channel_nm,product_views_tmp.currency_cd,product_views_tmp.detail_id,product_views_tmp.detail_id_hex,product_views_tmp.event_designed_id,product_views_tmp.event_id,product_views_tmp.event_key_cd,product_views_tmp.event_nm,product_views_tmp.event_source_cd,product_views_tmp.identity_id,product_views_tmp.load_dttm,product_views_tmp.mobile_app_id,product_views_tmp.price_val,product_views_tmp.product_group_nm,product_views_tmp.product_id,product_views_tmp.product_nm,product_views_tmp.product_sku,product_views_tmp.properties_map_doc,product_views_tmp.saving_message_txt,product_views_tmp.session_id,product_views_tmp.session_id_hex,product_views_tmp.shipping_message_txt,product_views_tmp.visit_id,product_views_tmp.visit_id_hex
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :product_views_tmp               , product_views, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..product_views_tmp               ;
    quit;
    %put ######## Staging table: product_views_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..product_views;
      drop table work.product_views;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..promotion_displayed_tmp         ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=promotion_displayed, table_keys=%str(detail_id,display_dttm,promotion_nm), out_table=work.promotion_displayed);
 data &tmplib..promotion_displayed_tmp         ;
     set work.promotion_displayed;
  if display_dttm ne . then display_dttm = tzoneu2s(display_dttm,&timeZone_Value.);if display_dttm_tz ne . then display_dttm_tz = tzoneu2s(display_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if detail_id='' then detail_id='-'; if promotion_nm='' then promotion_nm='-';
 run;
 %ErrCheck (Failed to Append Data to :promotion_displayed_tmp         , promotion_displayed);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..promotion_displayed using &tmpdbschema..promotion_displayed_tmp         
         on (promotion_displayed.detail_id=promotion_displayed_tmp.detail_id and promotion_displayed.display_dttm=promotion_displayed_tmp.display_dttm and promotion_displayed.promotion_nm=promotion_displayed_tmp.promotion_nm)
        when matched then  
        update set promotion_displayed.channel_nm = promotion_displayed_tmp.channel_nm , promotion_displayed.derived_display_flg = promotion_displayed_tmp.derived_display_flg , promotion_displayed.detail_id_hex = promotion_displayed_tmp.detail_id_hex , promotion_displayed.display_dttm_tz = promotion_displayed_tmp.display_dttm_tz , promotion_displayed.event_designed_id = promotion_displayed_tmp.event_designed_id , promotion_displayed.event_id = promotion_displayed_tmp.event_id , promotion_displayed.event_key_cd = promotion_displayed_tmp.event_key_cd , promotion_displayed.event_nm = promotion_displayed_tmp.event_nm , promotion_displayed.event_source_cd = promotion_displayed_tmp.event_source_cd , promotion_displayed.identity_id = promotion_displayed_tmp.identity_id , promotion_displayed.load_dttm = promotion_displayed_tmp.load_dttm , promotion_displayed.mobile_app_id = promotion_displayed_tmp.mobile_app_id , promotion_displayed.promotion_creative_nm = promotion_displayed_tmp.promotion_creative_nm , promotion_displayed.promotion_number = promotion_displayed_tmp.promotion_number , promotion_displayed.promotion_placement_nm = promotion_displayed_tmp.promotion_placement_nm , promotion_displayed.promotion_tracking_cd = promotion_displayed_tmp.promotion_tracking_cd , promotion_displayed.promotion_type_nm = promotion_displayed_tmp.promotion_type_nm , promotion_displayed.properties_map_doc = promotion_displayed_tmp.properties_map_doc , promotion_displayed.session_id = promotion_displayed_tmp.session_id , promotion_displayed.session_id_hex = promotion_displayed_tmp.session_id_hex , promotion_displayed.visit_id = promotion_displayed_tmp.visit_id , promotion_displayed.visit_id_hex = promotion_displayed_tmp.visit_id_hex
        when not matched then insert ( 
        channel_nm,derived_display_flg,detail_id,detail_id_hex,display_dttm,display_dttm_tz,event_designed_id,event_id,event_key_cd,event_nm,event_source_cd,identity_id,load_dttm,mobile_app_id,promotion_creative_nm,promotion_nm,promotion_number,promotion_placement_nm,promotion_tracking_cd,promotion_type_nm,properties_map_doc,session_id,session_id_hex,visit_id,visit_id_hex
         ) values ( 
        promotion_displayed_tmp.channel_nm,promotion_displayed_tmp.derived_display_flg,promotion_displayed_tmp.detail_id,promotion_displayed_tmp.detail_id_hex,promotion_displayed_tmp.display_dttm,promotion_displayed_tmp.display_dttm_tz,promotion_displayed_tmp.event_designed_id,promotion_displayed_tmp.event_id,promotion_displayed_tmp.event_key_cd,promotion_displayed_tmp.event_nm,promotion_displayed_tmp.event_source_cd,promotion_displayed_tmp.identity_id,promotion_displayed_tmp.load_dttm,promotion_displayed_tmp.mobile_app_id,promotion_displayed_tmp.promotion_creative_nm,promotion_displayed_tmp.promotion_nm,promotion_displayed_tmp.promotion_number,promotion_displayed_tmp.promotion_placement_nm,promotion_displayed_tmp.promotion_tracking_cd,promotion_displayed_tmp.promotion_type_nm,promotion_displayed_tmp.properties_map_doc,promotion_displayed_tmp.session_id,promotion_displayed_tmp.session_id_hex,promotion_displayed_tmp.visit_id,promotion_displayed_tmp.visit_id_hex
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :promotion_displayed_tmp         , promotion_displayed, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..promotion_displayed_tmp         ;
    quit;
    %put ######## Staging table: promotion_displayed_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..promotion_displayed;
      drop table work.promotion_displayed;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..promotion_used_tmp              ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=promotion_used, table_keys=%str(click_dttm,detail_id,promotion_nm), out_table=work.promotion_used);
 data &tmplib..promotion_used_tmp              ;
     set work.promotion_used;
  if click_dttm ne . then click_dttm = tzoneu2s(click_dttm,&timeZone_Value.);if click_dttm_tz ne . then click_dttm_tz = tzoneu2s(click_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if detail_id='' then detail_id='-'; if promotion_nm='' then promotion_nm='-';
 run;
 %ErrCheck (Failed to Append Data to :promotion_used_tmp              , promotion_used);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..promotion_used using &tmpdbschema..promotion_used_tmp              
         on (promotion_used.click_dttm=promotion_used_tmp.click_dttm and promotion_used.detail_id=promotion_used_tmp.detail_id and promotion_used.promotion_nm=promotion_used_tmp.promotion_nm)
        when matched then  
        update set promotion_used.channel_nm = promotion_used_tmp.channel_nm , promotion_used.click_dttm_tz = promotion_used_tmp.click_dttm_tz , promotion_used.detail_id_hex = promotion_used_tmp.detail_id_hex , promotion_used.event_designed_id = promotion_used_tmp.event_designed_id , promotion_used.event_id = promotion_used_tmp.event_id , promotion_used.event_key_cd = promotion_used_tmp.event_key_cd , promotion_used.event_nm = promotion_used_tmp.event_nm , promotion_used.event_source_cd = promotion_used_tmp.event_source_cd , promotion_used.identity_id = promotion_used_tmp.identity_id , promotion_used.load_dttm = promotion_used_tmp.load_dttm , promotion_used.mobile_app_id = promotion_used_tmp.mobile_app_id , promotion_used.promotion_creative_nm = promotion_used_tmp.promotion_creative_nm , promotion_used.promotion_number = promotion_used_tmp.promotion_number , promotion_used.promotion_placement_nm = promotion_used_tmp.promotion_placement_nm , promotion_used.promotion_tracking_cd = promotion_used_tmp.promotion_tracking_cd , promotion_used.promotion_type_nm = promotion_used_tmp.promotion_type_nm , promotion_used.properties_map_doc = promotion_used_tmp.properties_map_doc , promotion_used.session_id = promotion_used_tmp.session_id , promotion_used.session_id_hex = promotion_used_tmp.session_id_hex , promotion_used.visit_id = promotion_used_tmp.visit_id , promotion_used.visit_id_hex = promotion_used_tmp.visit_id_hex
        when not matched then insert ( 
        channel_nm,click_dttm,click_dttm_tz,detail_id,detail_id_hex,event_designed_id,event_id,event_key_cd,event_nm,event_source_cd,identity_id,load_dttm,mobile_app_id,promotion_creative_nm,promotion_nm,promotion_number,promotion_placement_nm,promotion_tracking_cd,promotion_type_nm,properties_map_doc,session_id,session_id_hex,visit_id,visit_id_hex
         ) values ( 
        promotion_used_tmp.channel_nm,promotion_used_tmp.click_dttm,promotion_used_tmp.click_dttm_tz,promotion_used_tmp.detail_id,promotion_used_tmp.detail_id_hex,promotion_used_tmp.event_designed_id,promotion_used_tmp.event_id,promotion_used_tmp.event_key_cd,promotion_used_tmp.event_nm,promotion_used_tmp.event_source_cd,promotion_used_tmp.identity_id,promotion_used_tmp.load_dttm,promotion_used_tmp.mobile_app_id,promotion_used_tmp.promotion_creative_nm,promotion_used_tmp.promotion_nm,promotion_used_tmp.promotion_number,promotion_used_tmp.promotion_placement_nm,promotion_used_tmp.promotion_tracking_cd,promotion_used_tmp.promotion_type_nm,promotion_used_tmp.properties_map_doc,promotion_used_tmp.session_id,promotion_used_tmp.session_id_hex,promotion_used_tmp.visit_id,promotion_used_tmp.visit_id_hex
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :promotion_used_tmp              , promotion_used, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..promotion_used_tmp              ;
    quit;
    %put ######## Staging table: promotion_used_tmp               Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..promotion_used;
      drop table work.promotion_used;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..response_history_tmp            ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=response_history, table_keys=%str(response_id), out_table=work.response_history);
 data &tmplib..response_history_tmp            ;
     set work.response_history;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if response_dttm ne . then response_dttm = tzoneu2s(response_dttm,&timeZone_Value.);if response_dttm_tz ne . then response_dttm_tz = tzoneu2s(response_dttm_tz,&timeZone_Value.) ;
  if response_id='' then response_id='-';
 run;
 %ErrCheck (Failed to Append Data to :response_history_tmp            , response_history);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..response_history using &tmpdbschema..response_history_tmp            
         on (response_history.response_id=response_history_tmp.response_id)
        when matched then  
        update set response_history.aud_occurrence_id = response_history_tmp.aud_occurrence_id , response_history.audience_id = response_history_tmp.audience_id , response_history.context_type_nm = response_history_tmp.context_type_nm , response_history.context_val = response_history_tmp.context_val , response_history.creative_id = response_history_tmp.creative_id , response_history.detail_id_hex = response_history_tmp.detail_id_hex , response_history.event_designed_id = response_history_tmp.event_designed_id , response_history.identity_id = response_history_tmp.identity_id , response_history.journey_id = response_history_tmp.journey_id , response_history.journey_occurrence_id = response_history_tmp.journey_occurrence_id , response_history.load_dttm = response_history_tmp.load_dttm , response_history.message_id = response_history_tmp.message_id , response_history.occurrence_id = response_history_tmp.occurrence_id , response_history.parent_event_designed_id = response_history_tmp.parent_event_designed_id , response_history.properties_map_doc = response_history_tmp.properties_map_doc , response_history.response_channel_nm = response_history_tmp.response_channel_nm , response_history.response_dttm = response_history_tmp.response_dttm , response_history.response_dttm_tz = response_history_tmp.response_dttm_tz , response_history.response_nm = response_history_tmp.response_nm , response_history.response_tracking_cd = response_history_tmp.response_tracking_cd , response_history.session_id_hex = response_history_tmp.session_id_hex , response_history.task_id = response_history_tmp.task_id , response_history.task_version_id = response_history_tmp.task_version_id , response_history.visit_id_hex = response_history_tmp.visit_id_hex
        when not matched then insert ( 
        aud_occurrence_id,audience_id,context_type_nm,context_val,creative_id,detail_id_hex,event_designed_id,identity_id,journey_id,journey_occurrence_id,load_dttm,message_id,occurrence_id,parent_event_designed_id,properties_map_doc,response_channel_nm,response_dttm,response_dttm_tz,response_id,response_nm,response_tracking_cd,session_id_hex,task_id,task_version_id,visit_id_hex
         ) values ( 
        response_history_tmp.aud_occurrence_id,response_history_tmp.audience_id,response_history_tmp.context_type_nm,response_history_tmp.context_val,response_history_tmp.creative_id,response_history_tmp.detail_id_hex,response_history_tmp.event_designed_id,response_history_tmp.identity_id,response_history_tmp.journey_id,response_history_tmp.journey_occurrence_id,response_history_tmp.load_dttm,response_history_tmp.message_id,response_history_tmp.occurrence_id,response_history_tmp.parent_event_designed_id,response_history_tmp.properties_map_doc,response_history_tmp.response_channel_nm,response_history_tmp.response_dttm,response_history_tmp.response_dttm_tz,response_history_tmp.response_id,response_history_tmp.response_nm,response_history_tmp.response_tracking_cd,response_history_tmp.session_id_hex,response_history_tmp.task_id,response_history_tmp.task_version_id,response_history_tmp.visit_id_hex
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :response_history_tmp            , response_history, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..response_history_tmp            ;
    quit;
    %put ######## Staging table: response_history_tmp             Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..response_history;
      drop table work.response_history;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..search_results_tmp              ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=search_results, table_keys=%str(detail_id,search_results_dttm), out_table=work.search_results);
 data &tmplib..search_results_tmp              ;
     set work.search_results;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if search_results_dttm ne . then search_results_dttm = tzoneu2s(search_results_dttm,&timeZone_Value.);if search_results_dttm_tz ne . then search_results_dttm_tz = tzoneu2s(search_results_dttm_tz,&timeZone_Value.) ;
  if detail_id='' then detail_id='-';
 run;
 %ErrCheck (Failed to Append Data to :search_results_tmp              , search_results);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..search_results using &tmpdbschema..search_results_tmp              
         on (search_results.detail_id=search_results_tmp.detail_id and search_results.search_results_dttm=search_results_tmp.search_results_dttm)
        when matched then  
        update set search_results.channel_nm = search_results_tmp.channel_nm , search_results.detail_id_hex = search_results_tmp.detail_id_hex , search_results.event_designed_id = search_results_tmp.event_designed_id , search_results.event_id = search_results_tmp.event_id , search_results.event_key_cd = search_results_tmp.event_key_cd , search_results.event_nm = search_results_tmp.event_nm , search_results.event_source_cd = search_results_tmp.event_source_cd , search_results.identity_id = search_results_tmp.identity_id , search_results.load_dttm = search_results_tmp.load_dttm , search_results.mobile_app_id = search_results_tmp.mobile_app_id , search_results.properties_map_doc = search_results_tmp.properties_map_doc , search_results.results_displayed_flg = search_results_tmp.results_displayed_flg , search_results.search_nm = search_results_tmp.search_nm , search_results.search_results_displayed = search_results_tmp.search_results_displayed , search_results.search_results_dttm_tz = search_results_tmp.search_results_dttm_tz , search_results.search_results_sk = search_results_tmp.search_results_sk , search_results.session_id = search_results_tmp.session_id , search_results.session_id_hex = search_results_tmp.session_id_hex , search_results.srch_field_id = search_results_tmp.srch_field_id , search_results.srch_field_name = search_results_tmp.srch_field_name , search_results.srch_phrase = search_results_tmp.srch_phrase , search_results.visit_id = search_results_tmp.visit_id , search_results.visit_id_hex = search_results_tmp.visit_id_hex
        when not matched then insert ( 
        channel_nm,detail_id,detail_id_hex,event_designed_id,event_id,event_key_cd,event_nm,event_source_cd,identity_id,load_dttm,mobile_app_id,properties_map_doc,results_displayed_flg,search_nm,search_results_displayed,search_results_dttm,search_results_dttm_tz,search_results_sk,session_id,session_id_hex,srch_field_id,srch_field_name,srch_phrase,visit_id,visit_id_hex
         ) values ( 
        search_results_tmp.channel_nm,search_results_tmp.detail_id,search_results_tmp.detail_id_hex,search_results_tmp.event_designed_id,search_results_tmp.event_id,search_results_tmp.event_key_cd,search_results_tmp.event_nm,search_results_tmp.event_source_cd,search_results_tmp.identity_id,search_results_tmp.load_dttm,search_results_tmp.mobile_app_id,search_results_tmp.properties_map_doc,search_results_tmp.results_displayed_flg,search_results_tmp.search_nm,search_results_tmp.search_results_displayed,search_results_tmp.search_results_dttm,search_results_tmp.search_results_dttm_tz,search_results_tmp.search_results_sk,search_results_tmp.session_id,search_results_tmp.session_id_hex,search_results_tmp.srch_field_id,search_results_tmp.srch_field_name,search_results_tmp.srch_phrase,search_results_tmp.visit_id,search_results_tmp.visit_id_hex
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :search_results_tmp              , search_results, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..search_results_tmp              ;
    quit;
    %put ######## Staging table: search_results_tmp               Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..search_results;
      drop table work.search_results;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..search_results_ext_tmp          ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=search_results_ext, table_keys=%str(search_results_sk), out_table=work.search_results_ext);
 data &tmplib..search_results_ext_tmp          ;
     set work.search_results_ext;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if search_results_sk='' then search_results_sk='-';
 run;
 %ErrCheck (Failed to Append Data to :search_results_ext_tmp          , search_results_ext);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..search_results_ext using &tmpdbschema..search_results_ext_tmp          
         on (search_results_ext.search_results_sk=search_results_ext_tmp.search_results_sk)
        when matched then  
        update set search_results_ext.event_designed_id = search_results_ext_tmp.event_designed_id , search_results_ext.load_dttm = search_results_ext_tmp.load_dttm , search_results_ext.search_results_displayed = search_results_ext_tmp.search_results_displayed
        when not matched then insert ( 
        event_designed_id,load_dttm,search_results_displayed,search_results_sk
         ) values ( 
        search_results_ext_tmp.event_designed_id,search_results_ext_tmp.load_dttm,search_results_ext_tmp.search_results_displayed,search_results_ext_tmp.search_results_sk
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :search_results_ext_tmp          , search_results_ext, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..search_results_ext_tmp          ;
    quit;
    %put ######## Staging table: search_results_ext_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..search_results_ext;
      drop table work.search_results_ext;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..session_details_tmp             ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=session_details, table_keys=%str(session_id), out_table=work.session_details);
 data &tmplib..session_details_tmp             ;
     set work.session_details;
  if client_session_start_dttm ne . then client_session_start_dttm = tzoneu2s(client_session_start_dttm,&timeZone_Value.);if client_session_start_dttm_tz ne . then client_session_start_dttm_tz = tzoneu2s(client_session_start_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if session_start_dttm ne . then session_start_dttm = tzoneu2s(session_start_dttm,&timeZone_Value.);if session_start_dttm_tz ne . then session_start_dttm_tz = tzoneu2s(session_start_dttm_tz,&timeZone_Value.) ;
  if session_id='' then session_id='-';
 run;
 %ErrCheck (Failed to Append Data to :session_details_tmp             , session_details);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..session_details using &tmpdbschema..session_details_tmp             
         on (session_details.session_id=session_details_tmp.session_id)
        when matched then  
        update set session_details.app_id = session_details_tmp.app_id , session_details.app_version = session_details_tmp.app_version , session_details.browser_nm = session_details_tmp.browser_nm , session_details.browser_version_no = session_details_tmp.browser_version_no , session_details.carrier_name = session_details_tmp.carrier_name , session_details.channel_nm = session_details_tmp.channel_nm , session_details.city_nm = session_details_tmp.city_nm , session_details.client_session_start_dttm = session_details_tmp.client_session_start_dttm , session_details.client_session_start_dttm_tz = session_details_tmp.client_session_start_dttm_tz , session_details.cookies_enabled_flg = session_details_tmp.cookies_enabled_flg , session_details.country_cd = session_details_tmp.country_cd , session_details.country_nm = session_details_tmp.country_nm , session_details.device_language = session_details_tmp.device_language , session_details.device_nm = session_details_tmp.device_nm , session_details.device_type_nm = session_details_tmp.device_type_nm , session_details.event_id = session_details_tmp.event_id , session_details.flash_enabled_flg = session_details_tmp.flash_enabled_flg , session_details.flash_version_no = session_details_tmp.flash_version_no , session_details.identity_id = session_details_tmp.identity_id , session_details.ip_address = session_details_tmp.ip_address , session_details.is_portable_flag = session_details_tmp.is_portable_flag , session_details.java_enabled_flg = session_details_tmp.java_enabled_flg , session_details.java_script_enabled_flg = session_details_tmp.java_script_enabled_flg , session_details.java_version_no = session_details_tmp.java_version_no , session_details.latitude = session_details_tmp.latitude , session_details.load_dttm = session_details_tmp.load_dttm , session_details.longitude = session_details_tmp.longitude , session_details.manufacturer = session_details_tmp.manufacturer , session_details.metro_cd = session_details_tmp.metro_cd , session_details.mobile_country_code = session_details_tmp.mobile_country_code , session_details.network_code = session_details_tmp.network_code , session_details.new_visitor_flg = session_details_tmp.new_visitor_flg , session_details.organization_nm = session_details_tmp.organization_nm , session_details.parent_event_id = session_details_tmp.parent_event_id , session_details.platform_desc = session_details_tmp.platform_desc , session_details.platform_type_nm = session_details_tmp.platform_type_nm , session_details.platform_version = session_details_tmp.platform_version , session_details.postal_cd = session_details_tmp.postal_cd , session_details.previous_session_id = session_details_tmp.previous_session_id , session_details.previous_session_id_hex = session_details_tmp.previous_session_id_hex , session_details.profile_nm1 = session_details_tmp.profile_nm1 , session_details.profile_nm2 = session_details_tmp.profile_nm2 , session_details.profile_nm3 = session_details_tmp.profile_nm3 , session_details.profile_nm4 = session_details_tmp.profile_nm4 , session_details.profile_nm5 = session_details_tmp.profile_nm5 , session_details.region_nm = session_details_tmp.region_nm , session_details.screen_color_depth_no = session_details_tmp.screen_color_depth_no , session_details.screen_size_txt = session_details_tmp.screen_size_txt , session_details.sdk_version = session_details_tmp.sdk_version , session_details.session_dt = session_details_tmp.session_dt , session_details.session_dt_tz = session_details_tmp.session_dt_tz , session_details.session_id_hex = session_details_tmp.session_id_hex , session_details.session_start_dttm = session_details_tmp.session_start_dttm , session_details.session_start_dttm_tz = session_details_tmp.session_start_dttm_tz , session_details.session_timeout = session_details_tmp.session_timeout , session_details.state_region_cd = session_details_tmp.state_region_cd , session_details.user_agent_nm = session_details_tmp.user_agent_nm , session_details.user_language_cd = session_details_tmp.user_language_cd , session_details.visitor_id = session_details_tmp.visitor_id
        when not matched then insert ( 
        app_id,app_version,browser_nm,browser_version_no,carrier_name,channel_nm,city_nm,client_session_start_dttm,client_session_start_dttm_tz,cookies_enabled_flg,country_cd,country_nm,device_language,device_nm,device_type_nm,event_id,flash_enabled_flg,flash_version_no,identity_id,ip_address,is_portable_flag,java_enabled_flg,java_script_enabled_flg,java_version_no,latitude,load_dttm,longitude,manufacturer,metro_cd,mobile_country_code,network_code,new_visitor_flg,organization_nm,parent_event_id,platform_desc,platform_type_nm,platform_version,postal_cd,previous_session_id,previous_session_id_hex,profile_nm1,profile_nm2,profile_nm3,profile_nm4,profile_nm5,region_nm,screen_color_depth_no,screen_size_txt,sdk_version,session_dt,session_dt_tz,session_id,session_id_hex,session_start_dttm,session_start_dttm_tz,session_timeout,state_region_cd,user_agent_nm,user_language_cd,visitor_id
         ) values ( 
        session_details_tmp.app_id,session_details_tmp.app_version,session_details_tmp.browser_nm,session_details_tmp.browser_version_no,session_details_tmp.carrier_name,session_details_tmp.channel_nm,session_details_tmp.city_nm,session_details_tmp.client_session_start_dttm,session_details_tmp.client_session_start_dttm_tz,session_details_tmp.cookies_enabled_flg,session_details_tmp.country_cd,session_details_tmp.country_nm,session_details_tmp.device_language,session_details_tmp.device_nm,session_details_tmp.device_type_nm,session_details_tmp.event_id,session_details_tmp.flash_enabled_flg,session_details_tmp.flash_version_no,session_details_tmp.identity_id,session_details_tmp.ip_address,session_details_tmp.is_portable_flag,session_details_tmp.java_enabled_flg,session_details_tmp.java_script_enabled_flg,session_details_tmp.java_version_no,session_details_tmp.latitude,session_details_tmp.load_dttm,session_details_tmp.longitude,session_details_tmp.manufacturer,session_details_tmp.metro_cd,session_details_tmp.mobile_country_code,session_details_tmp.network_code,session_details_tmp.new_visitor_flg,session_details_tmp.organization_nm,session_details_tmp.parent_event_id,session_details_tmp.platform_desc,session_details_tmp.platform_type_nm,session_details_tmp.platform_version,session_details_tmp.postal_cd,session_details_tmp.previous_session_id,session_details_tmp.previous_session_id_hex,session_details_tmp.profile_nm1,session_details_tmp.profile_nm2,session_details_tmp.profile_nm3,session_details_tmp.profile_nm4,session_details_tmp.profile_nm5,session_details_tmp.region_nm,session_details_tmp.screen_color_depth_no,session_details_tmp.screen_size_txt,session_details_tmp.sdk_version,session_details_tmp.session_dt,session_details_tmp.session_dt_tz,session_details_tmp.session_id,session_details_tmp.session_id_hex,session_details_tmp.session_start_dttm,session_details_tmp.session_start_dttm_tz,session_details_tmp.session_timeout,session_details_tmp.state_region_cd,session_details_tmp.user_agent_nm,session_details_tmp.user_language_cd,session_details_tmp.visitor_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :session_details_tmp             , session_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..session_details_tmp             ;
    quit;
    %put ######## Staging table: session_details_tmp              Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..session_details;
      drop table work.session_details;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..session_details_ext_tmp         ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=session_details_ext, table_keys=%str(session_id,session_id_hex), out_table=work.session_details_ext);
 data &tmplib..session_details_ext_tmp         ;
     set work.session_details_ext;
  if last_session_activity_dttm ne . then last_session_activity_dttm = tzoneu2s(last_session_activity_dttm,&timeZone_Value.);if last_session_activity_dttm_tz ne . then last_session_activity_dttm_tz = tzoneu2s(last_session_activity_dttm_tz,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if session_expiration_dttm ne . then session_expiration_dttm = tzoneu2s(session_expiration_dttm,&timeZone_Value.);if session_expiration_dttm_tz ne . then session_expiration_dttm_tz = tzoneu2s(session_expiration_dttm_tz,&timeZone_Value.) ;
  if session_id='' then session_id='-'; if session_id_hex='' then session_id_hex='-';
 run;
 %ErrCheck (Failed to Append Data to :session_details_ext_tmp         , session_details_ext);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..session_details_ext using &tmpdbschema..session_details_ext_tmp         
         on (session_details_ext.session_id=session_details_ext_tmp.session_id and session_details_ext.session_id_hex=session_details_ext_tmp.session_id_hex)
        when matched then  
        update set session_details_ext.active_sec_spent_in_sessn_cnt = session_details_ext_tmp.active_sec_spent_in_sessn_cnt , session_details_ext.last_session_activity_dttm = session_details_ext_tmp.last_session_activity_dttm , session_details_ext.last_session_activity_dttm_tz = session_details_ext_tmp.last_session_activity_dttm_tz , session_details_ext.load_dttm = session_details_ext_tmp.load_dttm , session_details_ext.seconds_spent_in_session_cnt = session_details_ext_tmp.seconds_spent_in_session_cnt , session_details_ext.session_expiration_dttm = session_details_ext_tmp.session_expiration_dttm , session_details_ext.session_expiration_dttm_tz = session_details_ext_tmp.session_expiration_dttm_tz
        when not matched then insert ( 
        active_sec_spent_in_sessn_cnt,last_session_activity_dttm,last_session_activity_dttm_tz,load_dttm,seconds_spent_in_session_cnt,session_expiration_dttm,session_expiration_dttm_tz,session_id,session_id_hex
         ) values ( 
        session_details_ext_tmp.active_sec_spent_in_sessn_cnt,session_details_ext_tmp.last_session_activity_dttm,session_details_ext_tmp.last_session_activity_dttm_tz,session_details_ext_tmp.load_dttm,session_details_ext_tmp.seconds_spent_in_session_cnt,session_details_ext_tmp.session_expiration_dttm,session_details_ext_tmp.session_expiration_dttm_tz,session_details_ext_tmp.session_id,session_details_ext_tmp.session_id_hex
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :session_details_ext_tmp         , session_details_ext, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..session_details_ext_tmp         ;
    quit;
    %put ######## Staging table: session_details_ext_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..session_details_ext;
      drop table work.session_details_ext;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..sms_message_clicked_tmp         ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=sms_message_clicked, table_keys=%str(event_id), out_table=work.sms_message_clicked);
 data &tmplib..sms_message_clicked_tmp         ;
     set work.sms_message_clicked;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if sms_click_dttm ne . then sms_click_dttm = tzoneu2s(sms_click_dttm,&timeZone_Value.);if sms_click_dttm_tz ne . then sms_click_dttm_tz = tzoneu2s(sms_click_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :sms_message_clicked_tmp         , sms_message_clicked);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..sms_message_clicked using &tmpdbschema..sms_message_clicked_tmp         
         on (sms_message_clicked.event_id=sms_message_clicked_tmp.event_id)
        when matched then  
        update set sms_message_clicked.aud_occurrence_id = sms_message_clicked_tmp.aud_occurrence_id , sms_message_clicked.audience_id = sms_message_clicked_tmp.audience_id , sms_message_clicked.context_type_nm = sms_message_clicked_tmp.context_type_nm , sms_message_clicked.context_val = sms_message_clicked_tmp.context_val , sms_message_clicked.country_cd = sms_message_clicked_tmp.country_cd , sms_message_clicked.creative_id = sms_message_clicked_tmp.creative_id , sms_message_clicked.creative_version_id = sms_message_clicked_tmp.creative_version_id , sms_message_clicked.event_designed_id = sms_message_clicked_tmp.event_designed_id , sms_message_clicked.event_nm = sms_message_clicked_tmp.event_nm , sms_message_clicked.identity_id = sms_message_clicked_tmp.identity_id , sms_message_clicked.load_dttm = sms_message_clicked_tmp.load_dttm , sms_message_clicked.occurrence_id = sms_message_clicked_tmp.occurrence_id , sms_message_clicked.response_tracking_cd = sms_message_clicked_tmp.response_tracking_cd , sms_message_clicked.sender_id = sms_message_clicked_tmp.sender_id , sms_message_clicked.sms_click_dttm = sms_message_clicked_tmp.sms_click_dttm , sms_message_clicked.sms_click_dttm_tz = sms_message_clicked_tmp.sms_click_dttm_tz , sms_message_clicked.sms_message_id = sms_message_clicked_tmp.sms_message_id , sms_message_clicked.task_id = sms_message_clicked_tmp.task_id , sms_message_clicked.task_version_id = sms_message_clicked_tmp.task_version_id
        when not matched then insert ( 
        aud_occurrence_id,audience_id,context_type_nm,context_val,country_cd,creative_id,creative_version_id,event_designed_id,event_id,event_nm,identity_id,load_dttm,occurrence_id,response_tracking_cd,sender_id,sms_click_dttm,sms_click_dttm_tz,sms_message_id,task_id,task_version_id
         ) values ( 
        sms_message_clicked_tmp.aud_occurrence_id,sms_message_clicked_tmp.audience_id,sms_message_clicked_tmp.context_type_nm,sms_message_clicked_tmp.context_val,sms_message_clicked_tmp.country_cd,sms_message_clicked_tmp.creative_id,sms_message_clicked_tmp.creative_version_id,sms_message_clicked_tmp.event_designed_id,sms_message_clicked_tmp.event_id,sms_message_clicked_tmp.event_nm,sms_message_clicked_tmp.identity_id,sms_message_clicked_tmp.load_dttm,sms_message_clicked_tmp.occurrence_id,sms_message_clicked_tmp.response_tracking_cd,sms_message_clicked_tmp.sender_id,sms_message_clicked_tmp.sms_click_dttm,sms_message_clicked_tmp.sms_click_dttm_tz,sms_message_clicked_tmp.sms_message_id,sms_message_clicked_tmp.task_id,sms_message_clicked_tmp.task_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :sms_message_clicked_tmp         , sms_message_clicked, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..sms_message_clicked_tmp         ;
    quit;
    %put ######## Staging table: sms_message_clicked_tmp          Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..sms_message_clicked;
      drop table work.sms_message_clicked;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..sms_message_delivered_tmp       ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=sms_message_delivered, table_keys=%str(event_id), out_table=work.sms_message_delivered);
 data &tmplib..sms_message_delivered_tmp       ;
     set work.sms_message_delivered;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if sms_delivered_dttm ne . then sms_delivered_dttm = tzoneu2s(sms_delivered_dttm,&timeZone_Value.);if sms_delivered_dttm_tz ne . then sms_delivered_dttm_tz = tzoneu2s(sms_delivered_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :sms_message_delivered_tmp       , sms_message_delivered);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..sms_message_delivered using &tmpdbschema..sms_message_delivered_tmp       
         on (sms_message_delivered.event_id=sms_message_delivered_tmp.event_id)
        when matched then  
        update set sms_message_delivered.aud_occurrence_id = sms_message_delivered_tmp.aud_occurrence_id , sms_message_delivered.audience_id = sms_message_delivered_tmp.audience_id , sms_message_delivered.context_type_nm = sms_message_delivered_tmp.context_type_nm , sms_message_delivered.context_val = sms_message_delivered_tmp.context_val , sms_message_delivered.country_cd = sms_message_delivered_tmp.country_cd , sms_message_delivered.creative_id = sms_message_delivered_tmp.creative_id , sms_message_delivered.creative_version_id = sms_message_delivered_tmp.creative_version_id , sms_message_delivered.event_designed_id = sms_message_delivered_tmp.event_designed_id , sms_message_delivered.event_nm = sms_message_delivered_tmp.event_nm , sms_message_delivered.identity_id = sms_message_delivered_tmp.identity_id , sms_message_delivered.load_dttm = sms_message_delivered_tmp.load_dttm , sms_message_delivered.occurrence_id = sms_message_delivered_tmp.occurrence_id , sms_message_delivered.response_tracking_cd = sms_message_delivered_tmp.response_tracking_cd , sms_message_delivered.sender_id = sms_message_delivered_tmp.sender_id , sms_message_delivered.sms_delivered_dttm = sms_message_delivered_tmp.sms_delivered_dttm , sms_message_delivered.sms_delivered_dttm_tz = sms_message_delivered_tmp.sms_delivered_dttm_tz , sms_message_delivered.sms_message_id = sms_message_delivered_tmp.sms_message_id , sms_message_delivered.task_id = sms_message_delivered_tmp.task_id , sms_message_delivered.task_version_id = sms_message_delivered_tmp.task_version_id
        when not matched then insert ( 
        aud_occurrence_id,audience_id,context_type_nm,context_val,country_cd,creative_id,creative_version_id,event_designed_id,event_id,event_nm,identity_id,load_dttm,occurrence_id,response_tracking_cd,sender_id,sms_delivered_dttm,sms_delivered_dttm_tz,sms_message_id,task_id,task_version_id
         ) values ( 
        sms_message_delivered_tmp.aud_occurrence_id,sms_message_delivered_tmp.audience_id,sms_message_delivered_tmp.context_type_nm,sms_message_delivered_tmp.context_val,sms_message_delivered_tmp.country_cd,sms_message_delivered_tmp.creative_id,sms_message_delivered_tmp.creative_version_id,sms_message_delivered_tmp.event_designed_id,sms_message_delivered_tmp.event_id,sms_message_delivered_tmp.event_nm,sms_message_delivered_tmp.identity_id,sms_message_delivered_tmp.load_dttm,sms_message_delivered_tmp.occurrence_id,sms_message_delivered_tmp.response_tracking_cd,sms_message_delivered_tmp.sender_id,sms_message_delivered_tmp.sms_delivered_dttm,sms_message_delivered_tmp.sms_delivered_dttm_tz,sms_message_delivered_tmp.sms_message_id,sms_message_delivered_tmp.task_id,sms_message_delivered_tmp.task_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :sms_message_delivered_tmp       , sms_message_delivered, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..sms_message_delivered_tmp       ;
    quit;
    %put ######## Staging table: sms_message_delivered_tmp        Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..sms_message_delivered;
      drop table work.sms_message_delivered;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..sms_message_failed_tmp          ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=sms_message_failed, table_keys=%str(event_id), out_table=work.sms_message_failed);
 data &tmplib..sms_message_failed_tmp          ;
     set work.sms_message_failed;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if sms_failed_dttm ne . then sms_failed_dttm = tzoneu2s(sms_failed_dttm,&timeZone_Value.);if sms_failed_dttm_tz ne . then sms_failed_dttm_tz = tzoneu2s(sms_failed_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :sms_message_failed_tmp          , sms_message_failed);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..sms_message_failed using &tmpdbschema..sms_message_failed_tmp          
         on (sms_message_failed.event_id=sms_message_failed_tmp.event_id)
        when matched then  
        update set sms_message_failed.aud_occurrence_id = sms_message_failed_tmp.aud_occurrence_id , sms_message_failed.audience_id = sms_message_failed_tmp.audience_id , sms_message_failed.context_type_nm = sms_message_failed_tmp.context_type_nm , sms_message_failed.context_val = sms_message_failed_tmp.context_val , sms_message_failed.country_cd = sms_message_failed_tmp.country_cd , sms_message_failed.creative_id = sms_message_failed_tmp.creative_id , sms_message_failed.creative_version_id = sms_message_failed_tmp.creative_version_id , sms_message_failed.event_designed_id = sms_message_failed_tmp.event_designed_id , sms_message_failed.event_nm = sms_message_failed_tmp.event_nm , sms_message_failed.identity_id = sms_message_failed_tmp.identity_id , sms_message_failed.load_dttm = sms_message_failed_tmp.load_dttm , sms_message_failed.occurrence_id = sms_message_failed_tmp.occurrence_id , sms_message_failed.reason_cd = sms_message_failed_tmp.reason_cd , sms_message_failed.reason_description_txt = sms_message_failed_tmp.reason_description_txt , sms_message_failed.response_tracking_cd = sms_message_failed_tmp.response_tracking_cd , sms_message_failed.sender_id = sms_message_failed_tmp.sender_id , sms_message_failed.sms_failed_dttm = sms_message_failed_tmp.sms_failed_dttm , sms_message_failed.sms_failed_dttm_tz = sms_message_failed_tmp.sms_failed_dttm_tz , sms_message_failed.sms_message_id = sms_message_failed_tmp.sms_message_id , sms_message_failed.task_id = sms_message_failed_tmp.task_id , sms_message_failed.task_version_id = sms_message_failed_tmp.task_version_id
        when not matched then insert ( 
        aud_occurrence_id,audience_id,context_type_nm,context_val,country_cd,creative_id,creative_version_id,event_designed_id,event_id,event_nm,identity_id,load_dttm,occurrence_id,reason_cd,reason_description_txt,response_tracking_cd,sender_id,sms_failed_dttm,sms_failed_dttm_tz,sms_message_id,task_id,task_version_id
         ) values ( 
        sms_message_failed_tmp.aud_occurrence_id,sms_message_failed_tmp.audience_id,sms_message_failed_tmp.context_type_nm,sms_message_failed_tmp.context_val,sms_message_failed_tmp.country_cd,sms_message_failed_tmp.creative_id,sms_message_failed_tmp.creative_version_id,sms_message_failed_tmp.event_designed_id,sms_message_failed_tmp.event_id,sms_message_failed_tmp.event_nm,sms_message_failed_tmp.identity_id,sms_message_failed_tmp.load_dttm,sms_message_failed_tmp.occurrence_id,sms_message_failed_tmp.reason_cd,sms_message_failed_tmp.reason_description_txt,sms_message_failed_tmp.response_tracking_cd,sms_message_failed_tmp.sender_id,sms_message_failed_tmp.sms_failed_dttm,sms_message_failed_tmp.sms_failed_dttm_tz,sms_message_failed_tmp.sms_message_id,sms_message_failed_tmp.task_id,sms_message_failed_tmp.task_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :sms_message_failed_tmp          , sms_message_failed, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..sms_message_failed_tmp          ;
    quit;
    %put ######## Staging table: sms_message_failed_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..sms_message_failed;
      drop table work.sms_message_failed;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..sms_message_reply_tmp           ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=sms_message_reply, table_keys=%str(event_id), out_table=work.sms_message_reply);
 data &tmplib..sms_message_reply_tmp           ;
     set work.sms_message_reply;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if sms_reply_dttm ne . then sms_reply_dttm = tzoneu2s(sms_reply_dttm,&timeZone_Value.);if sms_reply_dttm_tz ne . then sms_reply_dttm_tz = tzoneu2s(sms_reply_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :sms_message_reply_tmp           , sms_message_reply);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..sms_message_reply using &tmpdbschema..sms_message_reply_tmp           
         on (sms_message_reply.event_id=sms_message_reply_tmp.event_id)
        when matched then  
        update set sms_message_reply.aud_occurrence_id = sms_message_reply_tmp.aud_occurrence_id , sms_message_reply.audience_id = sms_message_reply_tmp.audience_id , sms_message_reply.context_type_nm = sms_message_reply_tmp.context_type_nm , sms_message_reply.context_val = sms_message_reply_tmp.context_val , sms_message_reply.country_cd = sms_message_reply_tmp.country_cd , sms_message_reply.event_designed_id = sms_message_reply_tmp.event_designed_id , sms_message_reply.event_nm = sms_message_reply_tmp.event_nm , sms_message_reply.identity_id = sms_message_reply_tmp.identity_id , sms_message_reply.load_dttm = sms_message_reply_tmp.load_dttm , sms_message_reply.occurrence_id = sms_message_reply_tmp.occurrence_id , sms_message_reply.response_tracking_cd = sms_message_reply_tmp.response_tracking_cd , sms_message_reply.sender_id = sms_message_reply_tmp.sender_id , sms_message_reply.sms_content = sms_message_reply_tmp.sms_content , sms_message_reply.sms_message_id = sms_message_reply_tmp.sms_message_id , sms_message_reply.sms_reply_dttm = sms_message_reply_tmp.sms_reply_dttm , sms_message_reply.sms_reply_dttm_tz = sms_message_reply_tmp.sms_reply_dttm_tz , sms_message_reply.task_id = sms_message_reply_tmp.task_id , sms_message_reply.task_version_id = sms_message_reply_tmp.task_version_id
        when not matched then insert ( 
        aud_occurrence_id,audience_id,context_type_nm,context_val,country_cd,event_designed_id,event_id,event_nm,identity_id,load_dttm,occurrence_id,response_tracking_cd,sender_id,sms_content,sms_message_id,sms_reply_dttm,sms_reply_dttm_tz,task_id,task_version_id
         ) values ( 
        sms_message_reply_tmp.aud_occurrence_id,sms_message_reply_tmp.audience_id,sms_message_reply_tmp.context_type_nm,sms_message_reply_tmp.context_val,sms_message_reply_tmp.country_cd,sms_message_reply_tmp.event_designed_id,sms_message_reply_tmp.event_id,sms_message_reply_tmp.event_nm,sms_message_reply_tmp.identity_id,sms_message_reply_tmp.load_dttm,sms_message_reply_tmp.occurrence_id,sms_message_reply_tmp.response_tracking_cd,sms_message_reply_tmp.sender_id,sms_message_reply_tmp.sms_content,sms_message_reply_tmp.sms_message_id,sms_message_reply_tmp.sms_reply_dttm,sms_message_reply_tmp.sms_reply_dttm_tz,sms_message_reply_tmp.task_id,sms_message_reply_tmp.task_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :sms_message_reply_tmp           , sms_message_reply, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..sms_message_reply_tmp           ;
    quit;
    %put ######## Staging table: sms_message_reply_tmp            Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..sms_message_reply;
      drop table work.sms_message_reply;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..sms_message_send_tmp            ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=sms_message_send, table_keys=%str(event_id), out_table=work.sms_message_send);
 data &tmplib..sms_message_send_tmp            ;
     set work.sms_message_send;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if sms_send_dttm ne . then sms_send_dttm = tzoneu2s(sms_send_dttm,&timeZone_Value.);if sms_send_dttm_tz ne . then sms_send_dttm_tz = tzoneu2s(sms_send_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :sms_message_send_tmp            , sms_message_send);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..sms_message_send using &tmpdbschema..sms_message_send_tmp            
         on (sms_message_send.event_id=sms_message_send_tmp.event_id)
        when matched then  
        update set sms_message_send.aud_occurrence_id = sms_message_send_tmp.aud_occurrence_id , sms_message_send.audience_id = sms_message_send_tmp.audience_id , sms_message_send.context_type_nm = sms_message_send_tmp.context_type_nm , sms_message_send.context_val = sms_message_send_tmp.context_val , sms_message_send.country_cd = sms_message_send_tmp.country_cd , sms_message_send.creative_id = sms_message_send_tmp.creative_id , sms_message_send.creative_version_id = sms_message_send_tmp.creative_version_id , sms_message_send.event_designed_id = sms_message_send_tmp.event_designed_id , sms_message_send.event_nm = sms_message_send_tmp.event_nm , sms_message_send.fragment_cnt = sms_message_send_tmp.fragment_cnt , sms_message_send.identity_id = sms_message_send_tmp.identity_id , sms_message_send.load_dttm = sms_message_send_tmp.load_dttm , sms_message_send.occurrence_id = sms_message_send_tmp.occurrence_id , sms_message_send.response_tracking_cd = sms_message_send_tmp.response_tracking_cd , sms_message_send.sender_id = sms_message_send_tmp.sender_id , sms_message_send.sms_message_id = sms_message_send_tmp.sms_message_id , sms_message_send.sms_send_dttm = sms_message_send_tmp.sms_send_dttm , sms_message_send.sms_send_dttm_tz = sms_message_send_tmp.sms_send_dttm_tz , sms_message_send.task_id = sms_message_send_tmp.task_id , sms_message_send.task_version_id = sms_message_send_tmp.task_version_id
        when not matched then insert ( 
        aud_occurrence_id,audience_id,context_type_nm,context_val,country_cd,creative_id,creative_version_id,event_designed_id,event_id,event_nm,fragment_cnt,identity_id,load_dttm,occurrence_id,response_tracking_cd,sender_id,sms_message_id,sms_send_dttm,sms_send_dttm_tz,task_id,task_version_id
         ) values ( 
        sms_message_send_tmp.aud_occurrence_id,sms_message_send_tmp.audience_id,sms_message_send_tmp.context_type_nm,sms_message_send_tmp.context_val,sms_message_send_tmp.country_cd,sms_message_send_tmp.creative_id,sms_message_send_tmp.creative_version_id,sms_message_send_tmp.event_designed_id,sms_message_send_tmp.event_id,sms_message_send_tmp.event_nm,sms_message_send_tmp.fragment_cnt,sms_message_send_tmp.identity_id,sms_message_send_tmp.load_dttm,sms_message_send_tmp.occurrence_id,sms_message_send_tmp.response_tracking_cd,sms_message_send_tmp.sender_id,sms_message_send_tmp.sms_message_id,sms_message_send_tmp.sms_send_dttm,sms_message_send_tmp.sms_send_dttm_tz,sms_message_send_tmp.task_id,sms_message_send_tmp.task_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :sms_message_send_tmp            , sms_message_send, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..sms_message_send_tmp            ;
    quit;
    %put ######## Staging table: sms_message_send_tmp             Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..sms_message_send;
      drop table work.sms_message_send;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..sms_optout_tmp                  ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=sms_optout, table_keys=%str(event_id), out_table=work.sms_optout);
 data &tmplib..sms_optout_tmp                  ;
     set work.sms_optout;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if sms_optout_dttm ne . then sms_optout_dttm = tzoneu2s(sms_optout_dttm,&timeZone_Value.);if sms_optout_dttm_tz ne . then sms_optout_dttm_tz = tzoneu2s(sms_optout_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :sms_optout_tmp                  , sms_optout);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..sms_optout using &tmpdbschema..sms_optout_tmp                  
         on (sms_optout.event_id=sms_optout_tmp.event_id)
        when matched then  
        update set sms_optout.aud_occurrence_id = sms_optout_tmp.aud_occurrence_id , sms_optout.audience_id = sms_optout_tmp.audience_id , sms_optout.context_type_nm = sms_optout_tmp.context_type_nm , sms_optout.context_val = sms_optout_tmp.context_val , sms_optout.country_cd = sms_optout_tmp.country_cd , sms_optout.creative_id = sms_optout_tmp.creative_id , sms_optout.creative_version_id = sms_optout_tmp.creative_version_id , sms_optout.event_designed_id = sms_optout_tmp.event_designed_id , sms_optout.event_nm = sms_optout_tmp.event_nm , sms_optout.identity_id = sms_optout_tmp.identity_id , sms_optout.load_dttm = sms_optout_tmp.load_dttm , sms_optout.occurrence_id = sms_optout_tmp.occurrence_id , sms_optout.response_tracking_cd = sms_optout_tmp.response_tracking_cd , sms_optout.sender_id = sms_optout_tmp.sender_id , sms_optout.sms_message_id = sms_optout_tmp.sms_message_id , sms_optout.sms_optout_dttm = sms_optout_tmp.sms_optout_dttm , sms_optout.sms_optout_dttm_tz = sms_optout_tmp.sms_optout_dttm_tz , sms_optout.task_id = sms_optout_tmp.task_id , sms_optout.task_version_id = sms_optout_tmp.task_version_id
        when not matched then insert ( 
        aud_occurrence_id,audience_id,context_type_nm,context_val,country_cd,creative_id,creative_version_id,event_designed_id,event_id,event_nm,identity_id,load_dttm,occurrence_id,response_tracking_cd,sender_id,sms_message_id,sms_optout_dttm,sms_optout_dttm_tz,task_id,task_version_id
         ) values ( 
        sms_optout_tmp.aud_occurrence_id,sms_optout_tmp.audience_id,sms_optout_tmp.context_type_nm,sms_optout_tmp.context_val,sms_optout_tmp.country_cd,sms_optout_tmp.creative_id,sms_optout_tmp.creative_version_id,sms_optout_tmp.event_designed_id,sms_optout_tmp.event_id,sms_optout_tmp.event_nm,sms_optout_tmp.identity_id,sms_optout_tmp.load_dttm,sms_optout_tmp.occurrence_id,sms_optout_tmp.response_tracking_cd,sms_optout_tmp.sender_id,sms_optout_tmp.sms_message_id,sms_optout_tmp.sms_optout_dttm,sms_optout_tmp.sms_optout_dttm_tz,sms_optout_tmp.task_id,sms_optout_tmp.task_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :sms_optout_tmp                  , sms_optout, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..sms_optout_tmp                  ;
    quit;
    %put ######## Staging table: sms_optout_tmp                   Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..sms_optout;
      drop table work.sms_optout;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..sms_optout_details_tmp          ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=sms_optout_details, table_keys=%str(event_id), out_table=work.sms_optout_details);
 data &tmplib..sms_optout_details_tmp          ;
     set work.sms_optout_details;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if sms_optout_dttm ne . then sms_optout_dttm = tzoneu2s(sms_optout_dttm,&timeZone_Value.);if sms_optout_dttm_tz ne . then sms_optout_dttm_tz = tzoneu2s(sms_optout_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :sms_optout_details_tmp          , sms_optout_details);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..sms_optout_details using &tmpdbschema..sms_optout_details_tmp          
         on (sms_optout_details.event_id=sms_optout_details_tmp.event_id)
        when matched then  
        update set sms_optout_details.address_val = sms_optout_details_tmp.address_val , sms_optout_details.aud_occurrence_id = sms_optout_details_tmp.aud_occurrence_id , sms_optout_details.audience_id = sms_optout_details_tmp.audience_id , sms_optout_details.context_type_nm = sms_optout_details_tmp.context_type_nm , sms_optout_details.context_val = sms_optout_details_tmp.context_val , sms_optout_details.country_cd = sms_optout_details_tmp.country_cd , sms_optout_details.creative_id = sms_optout_details_tmp.creative_id , sms_optout_details.creative_version_id = sms_optout_details_tmp.creative_version_id , sms_optout_details.event_designed_id = sms_optout_details_tmp.event_designed_id , sms_optout_details.event_nm = sms_optout_details_tmp.event_nm , sms_optout_details.identity_id = sms_optout_details_tmp.identity_id , sms_optout_details.load_dttm = sms_optout_details_tmp.load_dttm , sms_optout_details.occurrence_id = sms_optout_details_tmp.occurrence_id , sms_optout_details.response_tracking_cd = sms_optout_details_tmp.response_tracking_cd , sms_optout_details.sender_id = sms_optout_details_tmp.sender_id , sms_optout_details.sms_message_id = sms_optout_details_tmp.sms_message_id , sms_optout_details.sms_optout_dttm = sms_optout_details_tmp.sms_optout_dttm , sms_optout_details.sms_optout_dttm_tz = sms_optout_details_tmp.sms_optout_dttm_tz , sms_optout_details.task_id = sms_optout_details_tmp.task_id , sms_optout_details.task_version_id = sms_optout_details_tmp.task_version_id
        when not matched then insert ( 
        address_val,aud_occurrence_id,audience_id,context_type_nm,context_val,country_cd,creative_id,creative_version_id,event_designed_id,event_id,event_nm,identity_id,load_dttm,occurrence_id,response_tracking_cd,sender_id,sms_message_id,sms_optout_dttm,sms_optout_dttm_tz,task_id,task_version_id
         ) values ( 
        sms_optout_details_tmp.address_val,sms_optout_details_tmp.aud_occurrence_id,sms_optout_details_tmp.audience_id,sms_optout_details_tmp.context_type_nm,sms_optout_details_tmp.context_val,sms_optout_details_tmp.country_cd,sms_optout_details_tmp.creative_id,sms_optout_details_tmp.creative_version_id,sms_optout_details_tmp.event_designed_id,sms_optout_details_tmp.event_id,sms_optout_details_tmp.event_nm,sms_optout_details_tmp.identity_id,sms_optout_details_tmp.load_dttm,sms_optout_details_tmp.occurrence_id,sms_optout_details_tmp.response_tracking_cd,sms_optout_details_tmp.sender_id,sms_optout_details_tmp.sms_message_id,sms_optout_details_tmp.sms_optout_dttm,sms_optout_details_tmp.sms_optout_dttm_tz,sms_optout_details_tmp.task_id,sms_optout_details_tmp.task_version_id
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :sms_optout_details_tmp          , sms_optout_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..sms_optout_details_tmp          ;
    quit;
    %put ######## Staging table: sms_optout_details_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..sms_optout_details;
      drop table work.sms_optout_details;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..spot_clicked_tmp                ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=spot_clicked, table_keys=%str(event_id), out_table=work.spot_clicked);
 data &tmplib..spot_clicked_tmp                ;
     set work.spot_clicked;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if spot_clicked_dttm ne . then spot_clicked_dttm = tzoneu2s(spot_clicked_dttm,&timeZone_Value.);if spot_clicked_dttm_tz ne . then spot_clicked_dttm_tz = tzoneu2s(spot_clicked_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :spot_clicked_tmp                , spot_clicked);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..spot_clicked using &tmpdbschema..spot_clicked_tmp                
         on (spot_clicked.event_id=spot_clicked_tmp.event_id)
        when matched then  
        update set spot_clicked.channel_nm = spot_clicked_tmp.channel_nm , spot_clicked.channel_user_id = spot_clicked_tmp.channel_user_id , spot_clicked.context_type_nm = spot_clicked_tmp.context_type_nm , spot_clicked.context_val = spot_clicked_tmp.context_val , spot_clicked.control_group_flg = spot_clicked_tmp.control_group_flg , spot_clicked.creative_id = spot_clicked_tmp.creative_id , spot_clicked.creative_version_id = spot_clicked_tmp.creative_version_id , spot_clicked.detail_id_hex = spot_clicked_tmp.detail_id_hex , spot_clicked.event_designed_id = spot_clicked_tmp.event_designed_id , spot_clicked.event_key_cd = spot_clicked_tmp.event_key_cd , spot_clicked.event_nm = spot_clicked_tmp.event_nm , spot_clicked.event_source_cd = spot_clicked_tmp.event_source_cd , spot_clicked.identity_id = spot_clicked_tmp.identity_id , spot_clicked.load_dttm = spot_clicked_tmp.load_dttm , spot_clicked.message_id = spot_clicked_tmp.message_id , spot_clicked.message_version_id = spot_clicked_tmp.message_version_id , spot_clicked.mobile_app_id = spot_clicked_tmp.mobile_app_id , spot_clicked.occurrence_id = spot_clicked_tmp.occurrence_id , spot_clicked.product_id = spot_clicked_tmp.product_id , spot_clicked.product_nm = spot_clicked_tmp.product_nm , spot_clicked.product_qty_no = spot_clicked_tmp.product_qty_no , spot_clicked.product_sku_no = spot_clicked_tmp.product_sku_no , spot_clicked.properties_map_doc = spot_clicked_tmp.properties_map_doc , spot_clicked.rec_group_id = spot_clicked_tmp.rec_group_id , spot_clicked.request_id = spot_clicked_tmp.request_id , spot_clicked.reserved_1_txt = spot_clicked_tmp.reserved_1_txt , spot_clicked.reserved_2_txt = spot_clicked_tmp.reserved_2_txt , spot_clicked.response_tracking_cd = spot_clicked_tmp.response_tracking_cd , spot_clicked.segment_id = spot_clicked_tmp.segment_id , spot_clicked.segment_version_id = spot_clicked_tmp.segment_version_id , spot_clicked.session_id_hex = spot_clicked_tmp.session_id_hex , spot_clicked.spot_clicked_dttm = spot_clicked_tmp.spot_clicked_dttm , spot_clicked.spot_clicked_dttm_tz = spot_clicked_tmp.spot_clicked_dttm_tz , spot_clicked.spot_id = spot_clicked_tmp.spot_id , spot_clicked.task_id = spot_clicked_tmp.task_id , spot_clicked.task_version_id = spot_clicked_tmp.task_version_id , spot_clicked.url_txt = spot_clicked_tmp.url_txt , spot_clicked.visit_id_hex = spot_clicked_tmp.visit_id_hex
        when not matched then insert ( 
        channel_nm,channel_user_id,context_type_nm,context_val,control_group_flg,creative_id,creative_version_id,detail_id_hex,event_designed_id,event_id,event_key_cd,event_nm,event_source_cd,identity_id,load_dttm,message_id,message_version_id,mobile_app_id,occurrence_id,product_id,product_nm,product_qty_no,product_sku_no,properties_map_doc,rec_group_id,request_id,reserved_1_txt,reserved_2_txt,response_tracking_cd,segment_id,segment_version_id,session_id_hex,spot_clicked_dttm,spot_clicked_dttm_tz,spot_id,task_id,task_version_id,url_txt,visit_id_hex
         ) values ( 
        spot_clicked_tmp.channel_nm,spot_clicked_tmp.channel_user_id,spot_clicked_tmp.context_type_nm,spot_clicked_tmp.context_val,spot_clicked_tmp.control_group_flg,spot_clicked_tmp.creative_id,spot_clicked_tmp.creative_version_id,spot_clicked_tmp.detail_id_hex,spot_clicked_tmp.event_designed_id,spot_clicked_tmp.event_id,spot_clicked_tmp.event_key_cd,spot_clicked_tmp.event_nm,spot_clicked_tmp.event_source_cd,spot_clicked_tmp.identity_id,spot_clicked_tmp.load_dttm,spot_clicked_tmp.message_id,spot_clicked_tmp.message_version_id,spot_clicked_tmp.mobile_app_id,spot_clicked_tmp.occurrence_id,spot_clicked_tmp.product_id,spot_clicked_tmp.product_nm,spot_clicked_tmp.product_qty_no,spot_clicked_tmp.product_sku_no,spot_clicked_tmp.properties_map_doc,spot_clicked_tmp.rec_group_id,spot_clicked_tmp.request_id,spot_clicked_tmp.reserved_1_txt,spot_clicked_tmp.reserved_2_txt,spot_clicked_tmp.response_tracking_cd,spot_clicked_tmp.segment_id,spot_clicked_tmp.segment_version_id,spot_clicked_tmp.session_id_hex,spot_clicked_tmp.spot_clicked_dttm,spot_clicked_tmp.spot_clicked_dttm_tz,spot_clicked_tmp.spot_id,spot_clicked_tmp.task_id,spot_clicked_tmp.task_version_id,spot_clicked_tmp.url_txt,spot_clicked_tmp.visit_id_hex
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :spot_clicked_tmp                , spot_clicked, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..spot_clicked_tmp                ;
    quit;
    %put ######## Staging table: spot_clicked_tmp                 Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..spot_clicked;
      drop table work.spot_clicked;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..spot_requested_tmp              ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=spot_requested, table_keys=%str(event_id), out_table=work.spot_requested);
 data &tmplib..spot_requested_tmp              ;
     set work.spot_requested;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if spot_requested_dttm ne . then spot_requested_dttm = tzoneu2s(spot_requested_dttm,&timeZone_Value.);if spot_requested_dttm_tz ne . then spot_requested_dttm_tz = tzoneu2s(spot_requested_dttm_tz,&timeZone_Value.) ;
  if event_id='' then event_id='-';
 run;
 %ErrCheck (Failed to Append Data to :spot_requested_tmp              , spot_requested);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..spot_requested using &tmpdbschema..spot_requested_tmp              
         on (spot_requested.event_id=spot_requested_tmp.event_id)
        when matched then  
        update set spot_requested.channel_nm = spot_requested_tmp.channel_nm , spot_requested.channel_user_id = spot_requested_tmp.channel_user_id , spot_requested.context_type_nm = spot_requested_tmp.context_type_nm , spot_requested.context_val = spot_requested_tmp.context_val , spot_requested.detail_id_hex = spot_requested_tmp.detail_id_hex , spot_requested.event_designed_id = spot_requested_tmp.event_designed_id , spot_requested.event_nm = spot_requested_tmp.event_nm , spot_requested.event_source_cd = spot_requested_tmp.event_source_cd , spot_requested.identity_id = spot_requested_tmp.identity_id , spot_requested.load_dttm = spot_requested_tmp.load_dttm , spot_requested.mobile_app_id = spot_requested_tmp.mobile_app_id , spot_requested.properties_map_doc = spot_requested_tmp.properties_map_doc , spot_requested.request_id = spot_requested_tmp.request_id , spot_requested.session_id_hex = spot_requested_tmp.session_id_hex , spot_requested.spot_id = spot_requested_tmp.spot_id , spot_requested.spot_requested_dttm = spot_requested_tmp.spot_requested_dttm , spot_requested.spot_requested_dttm_tz = spot_requested_tmp.spot_requested_dttm_tz , spot_requested.visit_id_hex = spot_requested_tmp.visit_id_hex
        when not matched then insert ( 
        channel_nm,channel_user_id,context_type_nm,context_val,detail_id_hex,event_designed_id,event_id,event_nm,event_source_cd,identity_id,load_dttm,mobile_app_id,properties_map_doc,request_id,session_id_hex,spot_id,spot_requested_dttm,spot_requested_dttm_tz,visit_id_hex
         ) values ( 
        spot_requested_tmp.channel_nm,spot_requested_tmp.channel_user_id,spot_requested_tmp.context_type_nm,spot_requested_tmp.context_val,spot_requested_tmp.detail_id_hex,spot_requested_tmp.event_designed_id,spot_requested_tmp.event_id,spot_requested_tmp.event_nm,spot_requested_tmp.event_source_cd,spot_requested_tmp.identity_id,spot_requested_tmp.load_dttm,spot_requested_tmp.mobile_app_id,spot_requested_tmp.properties_map_doc,spot_requested_tmp.request_id,spot_requested_tmp.session_id_hex,spot_requested_tmp.spot_id,spot_requested_tmp.spot_requested_dttm,spot_requested_tmp.spot_requested_dttm_tz,spot_requested_tmp.visit_id_hex
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :spot_requested_tmp              , spot_requested, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..spot_requested_tmp              ;
    quit;
    %put ######## Staging table: spot_requested_tmp               Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..spot_requested;
      drop table work.spot_requested;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..tag_details_tmp                 ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=tag_details, table_keys=%str(component_id,component_type,tag_id), out_table=work.tag_details);
 data &tmplib..tag_details_tmp                 ;
     set work.tag_details;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if component_id='' then component_id='-'; if component_type='' then component_type='-'; if tag_id='' then tag_id='-';
 run;
 %ErrCheck (Failed to Append Data to :tag_details_tmp                 , tag_details);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..tag_details using &tmpdbschema..tag_details_tmp                 
         on (tag_details.component_id=tag_details_tmp.component_id and tag_details.component_type=tag_details_tmp.component_type and tag_details.tag_id=tag_details_tmp.tag_id)
        when matched then  
        update set tag_details.created_by_usernm = tag_details_tmp.created_by_usernm , tag_details.created_dttm = tag_details_tmp.created_dttm , tag_details.identity_cd = tag_details_tmp.identity_cd , tag_details.last_modified_dttm = tag_details_tmp.last_modified_dttm , tag_details.last_modified_usernm = tag_details_tmp.last_modified_usernm , tag_details.load_dttm = tag_details_tmp.load_dttm , tag_details.tag_desc = tag_details_tmp.tag_desc , tag_details.tag_nm = tag_details_tmp.tag_nm , tag_details.tag_owner_usernm = tag_details_tmp.tag_owner_usernm
        when not matched then insert ( 
        component_id,component_type,created_by_usernm,created_dttm,identity_cd,last_modified_dttm,last_modified_usernm,load_dttm,tag_desc,tag_id,tag_nm,tag_owner_usernm
         ) values ( 
        tag_details_tmp.component_id,tag_details_tmp.component_type,tag_details_tmp.created_by_usernm,tag_details_tmp.created_dttm,tag_details_tmp.identity_cd,tag_details_tmp.last_modified_dttm,tag_details_tmp.last_modified_usernm,tag_details_tmp.load_dttm,tag_details_tmp.tag_desc,tag_details_tmp.tag_id,tag_details_tmp.tag_nm,tag_details_tmp.tag_owner_usernm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :tag_details_tmp                 , tag_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..tag_details_tmp                 ;
    quit;
    %put ######## Staging table: tag_details_tmp                  Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..tag_details;
      drop table work.tag_details;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..visit_details_tmp               ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=visit_details, table_keys=%str(visit_id), out_table=work.visit_details);
 data &tmplib..visit_details_tmp               ;
     set work.visit_details;
  if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if visit_dttm ne . then visit_dttm = tzoneu2s(visit_dttm,&timeZone_Value.);if visit_dttm_tz ne . then visit_dttm_tz = tzoneu2s(visit_dttm_tz,&timeZone_Value.) ;
  if visit_id='' then visit_id='-';
 run;
 %ErrCheck (Failed to Append Data to :visit_details_tmp               , visit_details);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..visit_details using &tmpdbschema..visit_details_tmp               
         on (visit_details.visit_id=visit_details_tmp.visit_id)
        when matched then  
        update set visit_details.event_id = visit_details_tmp.event_id , visit_details.identity_id = visit_details_tmp.identity_id , visit_details.load_dttm = visit_details_tmp.load_dttm , visit_details.origination_creative_nm = visit_details_tmp.origination_creative_nm , visit_details.origination_nm = visit_details_tmp.origination_nm , visit_details.origination_placement_nm = visit_details_tmp.origination_placement_nm , visit_details.origination_tracking_cd = visit_details_tmp.origination_tracking_cd , visit_details.origination_type_nm = visit_details_tmp.origination_type_nm , visit_details.referrer_domain_nm = visit_details_tmp.referrer_domain_nm , visit_details.referrer_query_string_txt = visit_details_tmp.referrer_query_string_txt , visit_details.referrer_txt = visit_details_tmp.referrer_txt , visit_details.search_engine_desc = visit_details_tmp.search_engine_desc , visit_details.search_engine_domain_txt = visit_details_tmp.search_engine_domain_txt , visit_details.search_term_txt = visit_details_tmp.search_term_txt , visit_details.sequence_no = visit_details_tmp.sequence_no , visit_details.session_id = visit_details_tmp.session_id , visit_details.session_id_hex = visit_details_tmp.session_id_hex , visit_details.visit_dttm = visit_details_tmp.visit_dttm , visit_details.visit_dttm_tz = visit_details_tmp.visit_dttm_tz , visit_details.visit_id_hex = visit_details_tmp.visit_id_hex
        when not matched then insert ( 
        event_id,identity_id,load_dttm,origination_creative_nm,origination_nm,origination_placement_nm,origination_tracking_cd,origination_type_nm,referrer_domain_nm,referrer_query_string_txt,referrer_txt,search_engine_desc,search_engine_domain_txt,search_term_txt,sequence_no,session_id,session_id_hex,visit_dttm,visit_dttm_tz,visit_id,visit_id_hex
         ) values ( 
        visit_details_tmp.event_id,visit_details_tmp.identity_id,visit_details_tmp.load_dttm,visit_details_tmp.origination_creative_nm,visit_details_tmp.origination_nm,visit_details_tmp.origination_placement_nm,visit_details_tmp.origination_tracking_cd,visit_details_tmp.origination_type_nm,visit_details_tmp.referrer_domain_nm,visit_details_tmp.referrer_query_string_txt,visit_details_tmp.referrer_txt,visit_details_tmp.search_engine_desc,visit_details_tmp.search_engine_domain_txt,visit_details_tmp.search_term_txt,visit_details_tmp.sequence_no,visit_details_tmp.session_id,visit_details_tmp.session_id_hex,visit_details_tmp.visit_dttm,visit_details_tmp.visit_dttm_tz,visit_details_tmp.visit_id,visit_details_tmp.visit_id_hex
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :visit_details_tmp               , visit_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..visit_details_tmp               ;
    quit;
    %put ######## Staging table: visit_details_tmp                Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..visit_details;
      drop table work.visit_details;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..wf_process_details_tmp          ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=wf_process_details, table_keys=%str(pdef_id,process_id), out_table=work.wf_process_details);
 data &tmplib..wf_process_details_tmp          ;
     set work.wf_process_details;
  if completed_dttm ne . then completed_dttm = tzoneu2s(completed_dttm,&timeZone_Value.);if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if deleted_dttm ne . then deleted_dttm = tzoneu2s(deleted_dttm,&timeZone_Value.);if indexed_dttm ne . then indexed_dttm = tzoneu2s(indexed_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if planned_end_dttm ne . then planned_end_dttm = tzoneu2s(planned_end_dttm,&timeZone_Value.);if projected_end_dttm ne . then projected_end_dttm = tzoneu2s(projected_end_dttm,&timeZone_Value.);if published_dttm ne . then published_dttm = tzoneu2s(published_dttm,&timeZone_Value.);if start_dttm ne . then start_dttm = tzoneu2s(start_dttm,&timeZone_Value.);if submitted_dttm ne . then submitted_dttm = tzoneu2s(submitted_dttm,&timeZone_Value.);if timeline_calculated_dttm ne . then timeline_calculated_dttm = tzoneu2s(timeline_calculated_dttm,&timeZone_Value.) ;
  if pdef_id='' then pdef_id='-'; if process_id='' then process_id='-';
 run;
 %ErrCheck (Failed to Append Data to :wf_process_details_tmp          , wf_process_details);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..wf_process_details using &tmpdbschema..wf_process_details_tmp          
         on (wf_process_details.pdef_id=wf_process_details_tmp.pdef_id and wf_process_details.process_id=wf_process_details_tmp.process_id)
        when matched then  
        update set wf_process_details.business_info_id = wf_process_details_tmp.business_info_id , wf_process_details.business_info_nm = wf_process_details_tmp.business_info_nm , wf_process_details.business_info_type = wf_process_details_tmp.business_info_type , wf_process_details.completed_dttm = wf_process_details_tmp.completed_dttm , wf_process_details.created_by_usernm = wf_process_details_tmp.created_by_usernm , wf_process_details.created_dttm = wf_process_details_tmp.created_dttm , wf_process_details.delayed_by_day = wf_process_details_tmp.delayed_by_day , wf_process_details.deleted_by_usernm = wf_process_details_tmp.deleted_by_usernm , wf_process_details.deleted_dttm = wf_process_details_tmp.deleted_dttm , wf_process_details.indexed_dttm = wf_process_details_tmp.indexed_dttm , wf_process_details.last_modified_dttm = wf_process_details_tmp.last_modified_dttm , wf_process_details.last_modified_usernm = wf_process_details_tmp.last_modified_usernm , wf_process_details.load_dttm = wf_process_details_tmp.load_dttm , wf_process_details.modified_status_cd = wf_process_details_tmp.modified_status_cd , wf_process_details.percent_complete = wf_process_details_tmp.percent_complete , wf_process_details.planned_end_dttm = wf_process_details_tmp.planned_end_dttm , wf_process_details.process_category = wf_process_details_tmp.process_category , wf_process_details.process_comment = wf_process_details_tmp.process_comment , wf_process_details.process_desc = wf_process_details_tmp.process_desc , wf_process_details.process_instance_version = wf_process_details_tmp.process_instance_version , wf_process_details.process_nm = wf_process_details_tmp.process_nm , wf_process_details.process_owner_usernm = wf_process_details_tmp.process_owner_usernm , wf_process_details.process_status = wf_process_details_tmp.process_status , wf_process_details.process_type = wf_process_details_tmp.process_type , wf_process_details.projected_end_dttm = wf_process_details_tmp.projected_end_dttm , wf_process_details.published_by_usernm = wf_process_details_tmp.published_by_usernm , wf_process_details.published_dttm = wf_process_details_tmp.published_dttm , wf_process_details.start_dttm = wf_process_details_tmp.start_dttm , wf_process_details.submitted_by_usernm = wf_process_details_tmp.submitted_by_usernm , wf_process_details.submitted_dttm = wf_process_details_tmp.submitted_dttm , wf_process_details.timeline_calculated_dttm = wf_process_details_tmp.timeline_calculated_dttm , wf_process_details.user_tasks_cnt = wf_process_details_tmp.user_tasks_cnt
        when not matched then insert ( 
        business_info_id,business_info_nm,business_info_type,completed_dttm,created_by_usernm,created_dttm,delayed_by_day,deleted_by_usernm,deleted_dttm,indexed_dttm,last_modified_dttm,last_modified_usernm,load_dttm,modified_status_cd,pdef_id,percent_complete,planned_end_dttm,process_category,process_comment,process_desc,process_id,process_instance_version,process_nm,process_owner_usernm,process_status,process_type,projected_end_dttm,published_by_usernm,published_dttm,start_dttm,submitted_by_usernm,submitted_dttm,timeline_calculated_dttm,user_tasks_cnt
         ) values ( 
        wf_process_details_tmp.business_info_id,wf_process_details_tmp.business_info_nm,wf_process_details_tmp.business_info_type,wf_process_details_tmp.completed_dttm,wf_process_details_tmp.created_by_usernm,wf_process_details_tmp.created_dttm,wf_process_details_tmp.delayed_by_day,wf_process_details_tmp.deleted_by_usernm,wf_process_details_tmp.deleted_dttm,wf_process_details_tmp.indexed_dttm,wf_process_details_tmp.last_modified_dttm,wf_process_details_tmp.last_modified_usernm,wf_process_details_tmp.load_dttm,wf_process_details_tmp.modified_status_cd,wf_process_details_tmp.pdef_id,wf_process_details_tmp.percent_complete,wf_process_details_tmp.planned_end_dttm,wf_process_details_tmp.process_category,wf_process_details_tmp.process_comment,wf_process_details_tmp.process_desc,wf_process_details_tmp.process_id,wf_process_details_tmp.process_instance_version,wf_process_details_tmp.process_nm,wf_process_details_tmp.process_owner_usernm,wf_process_details_tmp.process_status,wf_process_details_tmp.process_type,wf_process_details_tmp.projected_end_dttm,wf_process_details_tmp.published_by_usernm,wf_process_details_tmp.published_dttm,wf_process_details_tmp.start_dttm,wf_process_details_tmp.submitted_by_usernm,wf_process_details_tmp.submitted_dttm,wf_process_details_tmp.timeline_calculated_dttm,wf_process_details_tmp.user_tasks_cnt
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :wf_process_details_tmp          , wf_process_details, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..wf_process_details_tmp          ;
    quit;
    %put ######## Staging table: wf_process_details_tmp           Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..wf_process_details;
      drop table work.wf_process_details;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..wf_process_details_custom_pr_tmp;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=wf_process_details_custom_prop, table_keys=%str(attr_group_id,attr_id,process_id), out_table=work.wf_process_details_custom_prop);
 data &tmplib..wf_process_details_custom_pr_tmp;
     set work.wf_process_details_custom_prop;
  if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if last_modified_dttm ne . then last_modified_dttm = tzoneu2s(last_modified_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.) ;
  if attr_group_id='' then attr_group_id='-'; if attr_id='' then attr_id='-'; if process_id='' then process_id='-';
 run;
 %ErrCheck (Failed to Append Data to :wf_process_details_custom_pr_tmp, wf_process_details_custom_prop);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..wf_process_details_custom_prop using &tmpdbschema..wf_process_details_custom_pr_tmp
         on (wf_process_details_custom_prop.attr_group_id=wf_process_details_custom_pr_tmp.attr_group_id and wf_process_details_custom_prop.attr_id=wf_process_details_custom_pr_tmp.attr_id and wf_process_details_custom_prop.process_id=wf_process_details_custom_pr_tmp.process_id)
        when matched then  
        update set wf_process_details_custom_prop.attr_cd = wf_process_details_custom_pr_tmp.attr_cd , wf_process_details_custom_prop.attr_group_cd = wf_process_details_custom_pr_tmp.attr_group_cd , wf_process_details_custom_prop.attr_group_nm = wf_process_details_custom_pr_tmp.attr_group_nm , wf_process_details_custom_prop.attr_nm = wf_process_details_custom_pr_tmp.attr_nm , wf_process_details_custom_prop.attr_val = wf_process_details_custom_pr_tmp.attr_val , wf_process_details_custom_prop.created_by_usernm = wf_process_details_custom_pr_tmp.created_by_usernm , wf_process_details_custom_prop.created_dttm = wf_process_details_custom_pr_tmp.created_dttm , wf_process_details_custom_prop.data_formatter = wf_process_details_custom_pr_tmp.data_formatter , wf_process_details_custom_prop.data_type = wf_process_details_custom_pr_tmp.data_type , wf_process_details_custom_prop.is_grid_flg = wf_process_details_custom_pr_tmp.is_grid_flg , wf_process_details_custom_prop.is_obsolete_flg = wf_process_details_custom_pr_tmp.is_obsolete_flg , wf_process_details_custom_prop.last_modified_dttm = wf_process_details_custom_pr_tmp.last_modified_dttm , wf_process_details_custom_prop.last_modified_usernm = wf_process_details_custom_pr_tmp.last_modified_usernm , wf_process_details_custom_prop.load_dttm = wf_process_details_custom_pr_tmp.load_dttm , wf_process_details_custom_prop.remote_pklist_tab_col = wf_process_details_custom_pr_tmp.remote_pklist_tab_col
        when not matched then insert ( 
        attr_cd,attr_group_cd,attr_group_id,attr_group_nm,attr_id,attr_nm,attr_val,created_by_usernm,created_dttm,data_formatter,data_type,is_grid_flg,is_obsolete_flg,last_modified_dttm,last_modified_usernm,load_dttm,process_id,remote_pklist_tab_col
         ) values ( 
        wf_process_details_custom_pr_tmp.attr_cd,wf_process_details_custom_pr_tmp.attr_group_cd,wf_process_details_custom_pr_tmp.attr_group_id,wf_process_details_custom_pr_tmp.attr_group_nm,wf_process_details_custom_pr_tmp.attr_id,wf_process_details_custom_pr_tmp.attr_nm,wf_process_details_custom_pr_tmp.attr_val,wf_process_details_custom_pr_tmp.created_by_usernm,wf_process_details_custom_pr_tmp.created_dttm,wf_process_details_custom_pr_tmp.data_formatter,wf_process_details_custom_pr_tmp.data_type,wf_process_details_custom_pr_tmp.is_grid_flg,wf_process_details_custom_pr_tmp.is_obsolete_flg,wf_process_details_custom_pr_tmp.last_modified_dttm,wf_process_details_custom_pr_tmp.last_modified_usernm,wf_process_details_custom_pr_tmp.load_dttm,wf_process_details_custom_pr_tmp.process_id,wf_process_details_custom_pr_tmp.remote_pklist_tab_col
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :wf_process_details_custom_pr_tmp, wf_process_details_custom_prop, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..wf_process_details_custom_pr_tmp;
    quit;
    %put ######## Staging table: wf_process_details_custom_pr_tmp Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..wf_process_details_custom_prop;
      drop table work.wf_process_details_custom_prop;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..wf_process_tasks_tmp            ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=wf_process_tasks, table_keys=%str(engine_taskdef_id,process_id,task_id), out_table=work.wf_process_tasks);
 data &tmplib..wf_process_tasks_tmp            ;
     set work.wf_process_tasks;
  if completed_dttm ne . then completed_dttm = tzoneu2s(completed_dttm,&timeZone_Value.);if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if deleted_dttm ne . then deleted_dttm = tzoneu2s(deleted_dttm,&timeZone_Value.);if due_dttm ne . then due_dttm = tzoneu2s(due_dttm,&timeZone_Value.);if engine_task_cancelled_dttm ne . then engine_task_cancelled_dttm = tzoneu2s(engine_task_cancelled_dttm,&timeZone_Value.);if indexed_dttm ne . then indexed_dttm = tzoneu2s(indexed_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if modified_dttm ne . then modified_dttm = tzoneu2s(modified_dttm,&timeZone_Value.);if projected_end_dttm ne . then projected_end_dttm = tzoneu2s(projected_end_dttm,&timeZone_Value.);if projected_start_dttm ne . then projected_start_dttm = tzoneu2s(projected_start_dttm,&timeZone_Value.);if published_dttm ne . then published_dttm = tzoneu2s(published_dttm,&timeZone_Value.);if started_dttm ne . then started_dttm = tzoneu2s(started_dttm,&timeZone_Value.) ;
  if engine_taskdef_id='' then engine_taskdef_id='-'; if process_id='' then process_id='-'; if task_id='' then task_id='-';
 run;
 %ErrCheck (Failed to Append Data to :wf_process_tasks_tmp            , wf_process_tasks);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..wf_process_tasks using &tmpdbschema..wf_process_tasks_tmp            
         on (wf_process_tasks.engine_taskdef_id=wf_process_tasks_tmp.engine_taskdef_id and wf_process_tasks.process_id=wf_process_tasks_tmp.process_id and wf_process_tasks.task_id=wf_process_tasks_tmp.task_id)
        when matched then  
        update set wf_process_tasks.approval_task_flg = wf_process_tasks_tmp.approval_task_flg , wf_process_tasks.cancelled_task_flg = wf_process_tasks_tmp.cancelled_task_flg , wf_process_tasks.completed_dttm = wf_process_tasks_tmp.completed_dttm , wf_process_tasks.created_by_usernm = wf_process_tasks_tmp.created_by_usernm , wf_process_tasks.created_dttm = wf_process_tasks_tmp.created_dttm , wf_process_tasks.delayed_by_day = wf_process_tasks_tmp.delayed_by_day , wf_process_tasks.deleted_by_usernm = wf_process_tasks_tmp.deleted_by_usernm , wf_process_tasks.deleted_dttm = wf_process_tasks_tmp.deleted_dttm , wf_process_tasks.due_dttm = wf_process_tasks_tmp.due_dttm , wf_process_tasks.duration_per_assignee = wf_process_tasks_tmp.duration_per_assignee , wf_process_tasks.engine_task_cancelled_dttm = wf_process_tasks_tmp.engine_task_cancelled_dttm , wf_process_tasks.existobj_update_flg = wf_process_tasks_tmp.existobj_update_flg , wf_process_tasks.first_usertask_flg = wf_process_tasks_tmp.first_usertask_flg , wf_process_tasks.indexed_dttm = wf_process_tasks_tmp.indexed_dttm , wf_process_tasks.instance_version = wf_process_tasks_tmp.instance_version , wf_process_tasks.is_sequential_flg = wf_process_tasks_tmp.is_sequential_flg , wf_process_tasks.latest_flg = wf_process_tasks_tmp.latest_flg , wf_process_tasks.load_dttm = wf_process_tasks_tmp.load_dttm , wf_process_tasks.locally_updated_flg = wf_process_tasks_tmp.locally_updated_flg , wf_process_tasks.modified_by_usernm = wf_process_tasks_tmp.modified_by_usernm , wf_process_tasks.modified_dttm = wf_process_tasks_tmp.modified_dttm , wf_process_tasks.modified_status_cd = wf_process_tasks_tmp.modified_status_cd , wf_process_tasks.multi_assig_suprt_flg = wf_process_tasks_tmp.multi_assig_suprt_flg , wf_process_tasks.owner_usernm = wf_process_tasks_tmp.owner_usernm , wf_process_tasks.percent_complete = wf_process_tasks_tmp.percent_complete , wf_process_tasks.projected_end_dttm = wf_process_tasks_tmp.projected_end_dttm , wf_process_tasks.projected_start_dttm = wf_process_tasks_tmp.projected_start_dttm , wf_process_tasks.published_by_usernm = wf_process_tasks_tmp.published_by_usernm , wf_process_tasks.published_dttm = wf_process_tasks_tmp.published_dttm , wf_process_tasks.skip_peerupdate_scanning_flg = wf_process_tasks_tmp.skip_peerupdate_scanning_flg , wf_process_tasks.skip_update_scanning_flg = wf_process_tasks_tmp.skip_update_scanning_flg , wf_process_tasks.started_dttm = wf_process_tasks_tmp.started_dttm , wf_process_tasks.task_attachment = wf_process_tasks_tmp.task_attachment , wf_process_tasks.task_comment = wf_process_tasks_tmp.task_comment , wf_process_tasks.task_desc = wf_process_tasks_tmp.task_desc , wf_process_tasks.task_instruction = wf_process_tasks_tmp.task_instruction , wf_process_tasks.task_nm = wf_process_tasks_tmp.task_nm , wf_process_tasks.task_status = wf_process_tasks_tmp.task_status , wf_process_tasks.task_subtype = wf_process_tasks_tmp.task_subtype , wf_process_tasks.task_type = wf_process_tasks_tmp.task_type , wf_process_tasks.version_num = wf_process_tasks_tmp.version_num
        when not matched then insert ( 
        approval_task_flg,cancelled_task_flg,completed_dttm,created_by_usernm,created_dttm,delayed_by_day,deleted_by_usernm,deleted_dttm,due_dttm,duration_per_assignee,engine_task_cancelled_dttm,engine_taskdef_id,existobj_update_flg,first_usertask_flg,indexed_dttm,instance_version,is_sequential_flg,latest_flg,load_dttm,locally_updated_flg,modified_by_usernm,modified_dttm,modified_status_cd,multi_assig_suprt_flg,owner_usernm,percent_complete,process_id,projected_end_dttm,projected_start_dttm,published_by_usernm,published_dttm,skip_peerupdate_scanning_flg,skip_update_scanning_flg,started_dttm,task_attachment,task_comment,task_desc,task_id,task_instruction,task_nm,task_status,task_subtype,task_type,version_num
         ) values ( 
        wf_process_tasks_tmp.approval_task_flg,wf_process_tasks_tmp.cancelled_task_flg,wf_process_tasks_tmp.completed_dttm,wf_process_tasks_tmp.created_by_usernm,wf_process_tasks_tmp.created_dttm,wf_process_tasks_tmp.delayed_by_day,wf_process_tasks_tmp.deleted_by_usernm,wf_process_tasks_tmp.deleted_dttm,wf_process_tasks_tmp.due_dttm,wf_process_tasks_tmp.duration_per_assignee,wf_process_tasks_tmp.engine_task_cancelled_dttm,wf_process_tasks_tmp.engine_taskdef_id,wf_process_tasks_tmp.existobj_update_flg,wf_process_tasks_tmp.first_usertask_flg,wf_process_tasks_tmp.indexed_dttm,wf_process_tasks_tmp.instance_version,wf_process_tasks_tmp.is_sequential_flg,wf_process_tasks_tmp.latest_flg,wf_process_tasks_tmp.load_dttm,wf_process_tasks_tmp.locally_updated_flg,wf_process_tasks_tmp.modified_by_usernm,wf_process_tasks_tmp.modified_dttm,wf_process_tasks_tmp.modified_status_cd,wf_process_tasks_tmp.multi_assig_suprt_flg,wf_process_tasks_tmp.owner_usernm,wf_process_tasks_tmp.percent_complete,wf_process_tasks_tmp.process_id,wf_process_tasks_tmp.projected_end_dttm,wf_process_tasks_tmp.projected_start_dttm,wf_process_tasks_tmp.published_by_usernm,wf_process_tasks_tmp.published_dttm,wf_process_tasks_tmp.skip_peerupdate_scanning_flg,wf_process_tasks_tmp.skip_update_scanning_flg,wf_process_tasks_tmp.started_dttm,wf_process_tasks_tmp.task_attachment,wf_process_tasks_tmp.task_comment,wf_process_tasks_tmp.task_desc,wf_process_tasks_tmp.task_id,wf_process_tasks_tmp.task_instruction,wf_process_tasks_tmp.task_nm,wf_process_tasks_tmp.task_status,wf_process_tasks_tmp.task_subtype,wf_process_tasks_tmp.task_type,wf_process_tasks_tmp.version_num
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :wf_process_tasks_tmp            , wf_process_tasks, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..wf_process_tasks_tmp            ;
    quit;
    %put ######## Staging table: wf_process_tasks_tmp             Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..wf_process_tasks;
      drop table work.wf_process_tasks;
  quit;
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
      proc sql noerrorstop;
        drop table &tmplib..wf_tasks_user_assignment_tmp    ;
      quit;
 %end;
 %check_duplicate_from_source(table_nm=wf_tasks_user_assignment, table_keys=%str(process_id,task_id,user_assignment_id,user_id), out_table=work.wf_tasks_user_assignment);
 data &tmplib..wf_tasks_user_assignment_tmp    ;
     set work.wf_tasks_user_assignment;
  if completed_dttm ne . then completed_dttm = tzoneu2s(completed_dttm,&timeZone_Value.);if created_dttm ne . then created_dttm = tzoneu2s(created_dttm,&timeZone_Value.);if deleted_dttm ne . then deleted_dttm = tzoneu2s(deleted_dttm,&timeZone_Value.);if due_dttm ne . then due_dttm = tzoneu2s(due_dttm,&timeZone_Value.);if load_dttm ne . then load_dttm = tzoneu2s(load_dttm,&timeZone_Value.);if modified_dttm ne . then modified_dttm = tzoneu2s(modified_dttm,&timeZone_Value.);if projected_end_dttm ne . then projected_end_dttm = tzoneu2s(projected_end_dttm,&timeZone_Value.);if projected_start_dttm ne . then projected_start_dttm = tzoneu2s(projected_start_dttm,&timeZone_Value.);if start_dttm ne . then start_dttm = tzoneu2s(start_dttm,&timeZone_Value.) ;
  if process_id='' then process_id='-'; if task_id='' then task_id='-'; if user_assignment_id='' then user_assignment_id='-'; if user_id='' then user_id='-';
 run;
 %ErrCheck (Failed to Append Data to :wf_tasks_user_assignment_tmp    , wf_tasks_user_assignment);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
    connect to SQLSVR (&sql_passthru_connection.);
        execute (merge into &dbschema..wf_tasks_user_assignment using &tmpdbschema..wf_tasks_user_assignment_tmp    
         on (wf_tasks_user_assignment.process_id=wf_tasks_user_assignment_tmp.process_id and wf_tasks_user_assignment.task_id=wf_tasks_user_assignment_tmp.task_id and wf_tasks_user_assignment.user_assignment_id=wf_tasks_user_assignment_tmp.user_assignment_id and wf_tasks_user_assignment.user_id=wf_tasks_user_assignment_tmp.user_id)
        when matched then  
        update set wf_tasks_user_assignment.activation_completed_flg = wf_tasks_user_assignment_tmp.activation_completed_flg , wf_tasks_user_assignment.approval_status = wf_tasks_user_assignment_tmp.approval_status , wf_tasks_user_assignment.assignee_id = wf_tasks_user_assignment_tmp.assignee_id , wf_tasks_user_assignment.assignee_type = wf_tasks_user_assignment_tmp.assignee_type , wf_tasks_user_assignment.completed_dttm = wf_tasks_user_assignment_tmp.completed_dttm , wf_tasks_user_assignment.created_by_usernm = wf_tasks_user_assignment_tmp.created_by_usernm , wf_tasks_user_assignment.created_dttm = wf_tasks_user_assignment_tmp.created_dttm , wf_tasks_user_assignment.delayed_by_day = wf_tasks_user_assignment_tmp.delayed_by_day , wf_tasks_user_assignment.deleted_by_usernm = wf_tasks_user_assignment_tmp.deleted_by_usernm , wf_tasks_user_assignment.deleted_dttm = wf_tasks_user_assignment_tmp.deleted_dttm , wf_tasks_user_assignment.due_dttm = wf_tasks_user_assignment_tmp.due_dttm , wf_tasks_user_assignment.initiator_comment = wf_tasks_user_assignment_tmp.initiator_comment , wf_tasks_user_assignment.instance_version = wf_tasks_user_assignment_tmp.instance_version , wf_tasks_user_assignment.is_assigned_flg = wf_tasks_user_assignment_tmp.is_assigned_flg , wf_tasks_user_assignment.is_latest_flg = wf_tasks_user_assignment_tmp.is_latest_flg , wf_tasks_user_assignment.is_replaced_flg = wf_tasks_user_assignment_tmp.is_replaced_flg , wf_tasks_user_assignment.load_dttm = wf_tasks_user_assignment_tmp.load_dttm , wf_tasks_user_assignment.modified_by_usernm = wf_tasks_user_assignment_tmp.modified_by_usernm , wf_tasks_user_assignment.modified_dttm = wf_tasks_user_assignment_tmp.modified_dttm , wf_tasks_user_assignment.modified_status_cd = wf_tasks_user_assignment_tmp.modified_status_cd , wf_tasks_user_assignment.owner_usernm = wf_tasks_user_assignment_tmp.owner_usernm , wf_tasks_user_assignment.projected_end_dttm = wf_tasks_user_assignment_tmp.projected_end_dttm , wf_tasks_user_assignment.projected_start_dttm = wf_tasks_user_assignment_tmp.projected_start_dttm , wf_tasks_user_assignment.replacement_assignee_id = wf_tasks_user_assignment_tmp.replacement_assignee_id , wf_tasks_user_assignment.replacement_reason = wf_tasks_user_assignment_tmp.replacement_reason , wf_tasks_user_assignment.replacement_userid = wf_tasks_user_assignment_tmp.replacement_userid , wf_tasks_user_assignment.start_dttm = wf_tasks_user_assignment_tmp.start_dttm , wf_tasks_user_assignment.usan_comment = wf_tasks_user_assignment_tmp.usan_comment , wf_tasks_user_assignment.usan_desc = wf_tasks_user_assignment_tmp.usan_desc , wf_tasks_user_assignment.usan_duration_day = wf_tasks_user_assignment_tmp.usan_duration_day , wf_tasks_user_assignment.usan_instruction = wf_tasks_user_assignment_tmp.usan_instruction , wf_tasks_user_assignment.usan_status = wf_tasks_user_assignment_tmp.usan_status , wf_tasks_user_assignment.user_nm = wf_tasks_user_assignment_tmp.user_nm
        when not matched then insert ( 
        activation_completed_flg,approval_status,assignee_id,assignee_type,completed_dttm,created_by_usernm,created_dttm,delayed_by_day,deleted_by_usernm,deleted_dttm,due_dttm,initiator_comment,instance_version,is_assigned_flg,is_latest_flg,is_replaced_flg,load_dttm,modified_by_usernm,modified_dttm,modified_status_cd,owner_usernm,process_id,projected_end_dttm,projected_start_dttm,replacement_assignee_id,replacement_reason,replacement_userid,start_dttm,task_id,usan_comment,usan_desc,usan_duration_day,usan_instruction,usan_status,user_assignment_id,user_id,user_nm
         ) values ( 
        wf_tasks_user_assignment_tmp.activation_completed_flg,wf_tasks_user_assignment_tmp.approval_status,wf_tasks_user_assignment_tmp.assignee_id,wf_tasks_user_assignment_tmp.assignee_type,wf_tasks_user_assignment_tmp.completed_dttm,wf_tasks_user_assignment_tmp.created_by_usernm,wf_tasks_user_assignment_tmp.created_dttm,wf_tasks_user_assignment_tmp.delayed_by_day,wf_tasks_user_assignment_tmp.deleted_by_usernm,wf_tasks_user_assignment_tmp.deleted_dttm,wf_tasks_user_assignment_tmp.due_dttm,wf_tasks_user_assignment_tmp.initiator_comment,wf_tasks_user_assignment_tmp.instance_version,wf_tasks_user_assignment_tmp.is_assigned_flg,wf_tasks_user_assignment_tmp.is_latest_flg,wf_tasks_user_assignment_tmp.is_replaced_flg,wf_tasks_user_assignment_tmp.load_dttm,wf_tasks_user_assignment_tmp.modified_by_usernm,wf_tasks_user_assignment_tmp.modified_dttm,wf_tasks_user_assignment_tmp.modified_status_cd,wf_tasks_user_assignment_tmp.owner_usernm,wf_tasks_user_assignment_tmp.process_id,wf_tasks_user_assignment_tmp.projected_end_dttm,wf_tasks_user_assignment_tmp.projected_start_dttm,wf_tasks_user_assignment_tmp.replacement_assignee_id,wf_tasks_user_assignment_tmp.replacement_reason,wf_tasks_user_assignment_tmp.replacement_userid,wf_tasks_user_assignment_tmp.start_dttm,wf_tasks_user_assignment_tmp.task_id,wf_tasks_user_assignment_tmp.usan_comment,wf_tasks_user_assignment_tmp.usan_desc,wf_tasks_user_assignment_tmp.usan_duration_day,wf_tasks_user_assignment_tmp.usan_instruction,wf_tasks_user_assignment_tmp.usan_status,wf_tasks_user_assignment_tmp.user_assignment_id,wf_tasks_user_assignment_tmp.user_id,wf_tasks_user_assignment_tmp.user_nm
     );) by SQLSVR;
    disconnect from SQLSVR;
    quit;
 %ErrCheck (Failed to Update/Insert into  :wf_tasks_user_assignment_tmp    , wf_tasks_user_assignment, err_macro=SYSDBRC);
 %if &errFlag = 0 %then %do;
    proc sql noerrorstop;
        drop table &tmplib..wf_tasks_user_assignment_tmp    ;
    quit;
    %put ######## Staging table: wf_tasks_user_assignment_tmp     Deleted ############;
      %end;
    %end;
 %if &errFlag = 0 %then %do;
  proc sql noerrorstop;
      drop table &udmmart..wf_tasks_user_assignment;
      drop table work.wf_tasks_user_assignment;
  quit;
 %end;
 %else %do;
    %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;
 %end;
 %end;
 %put %sysfunc(datetime(),E8601DT25.) --- Processing table wf_tasks_user_assignment;
%put------------------------------------------------------------------;
 %mend execute_AZURE_code;
 %execute_AZURE_code;
