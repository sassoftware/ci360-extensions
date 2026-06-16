/*******************************************************************************/
/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                         */
/* *****************************************************************************/
%macro execute_POSTGRES_etl;
%if %sysfunc(exist(&udmmart..ABT_ATTRIBUTION)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid=%sysfunc(open(&udmmart..ABT_ATTRIBUTION));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..ABT_ATTRIBUTION_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..ABT_ATTRIBUTION_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=ABT_ATTRIBUTION , table_keys=%str(INTERACTION_DTTM,INTERACTION_ID), out_table=work.ABT_ATTRIBUTION );
   DATA work.ABT_ATTRIBUTION_tmp /VIEW=work.ABT_ATTRIBUTION_tmp ;
      SET work.ABT_ATTRIBUTION ;
   RUN;
   %err_check (Failed to add time zone adaptation :ABT_ATTRIBUTION_tmp , ABT_ATTRIBUTION );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..ABT_ATTRIBUTION_tmp ;
            SET work.ABT_ATTRIBUTION_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.ABT_ATTRIBUTION_tmp  BASE=&tmplib..ABT_ATTRIBUTION_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..ABT_ATTRIBUTION_tmp ;
            SET work.ABT_ATTRIBUTION_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :ABT_ATTRIBUTION_tmp , ABT_ATTRIBUTION );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..ABT_ATTRIBUTION AS b USING &tmpdbschema..ABT_ATTRIBUTION_tmp AS d ON (
            b.interaction_dttm = d.interaction_dttm AND 
            b.interaction_id = d.interaction_id )
         WHEN MATCHED THEN
         UPDATE SET
            interaction_cost = d.interaction_cost, 
            conversion_value = d.conversion_value, task_id = d.task_id, 
            load_id = d.load_id, interaction_type = d.interaction_type, 
            interaction_subtype = d.interaction_subtype, interaction = d.interaction, 
            identity_id = d.identity_id, creative_id = d.creative_id
         WHEN NOT MATCHED AND INTERACTION_DTTM IS NOT NULL AND INTERACTION_ID IS NOT NULL THEN INSERT (
            interaction_cost, conversion_value, interaction_dttm, 
            task_id, load_id, interaction_type, interaction_subtype, 
            interaction_id, interaction, identity_id, creative_id
         ) VALUES (
            d.interaction_cost, d.conversion_value, d.interaction_dttm, 
            d.task_id, d.load_id, d.interaction_type, d.interaction_subtype, 
            d.interaction_id, d.interaction, d.identity_id, d.creative_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :ABT_ATTRIBUTION_tmp , ABT_ATTRIBUTION , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..ABT_ATTRIBUTION_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..ABT_ATTRIBUTION_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..ABT_ATTRIBUTION;
         DROP TABLE work.ABT_ATTRIBUTION;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..AB_TEST_PATH_ASSIGNMENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..AB_TEST_PATH_ASSIGNMENT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..AB_TEST_PATH_ASSIGNMENT_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=AB_TEST_PATH_ASSIGNMENT , table_keys=%str(EVENT_ID), out_table=work.AB_TEST_PATH_ASSIGNMENT );
   DATA work.AB_TEST_PATH_ASSIGNMENT_tmp /VIEW=work.AB_TEST_PATH_ASSIGNMENT_tmp ;
      SET work.AB_TEST_PATH_ASSIGNMENT ;
      IF abtestpath_assignment_dttm_tz  NE . THEN abtestpath_assignment_dttm_tz =tzoneu2s(abtestpath_assignment_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :AB_TEST_PATH_ASSIGNMENT_tmp , AB_TEST_PATH_ASSIGNMENT );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..AB_TEST_PATH_ASSIGNMENT_tmp ;
            SET work.AB_TEST_PATH_ASSIGNMENT_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.AB_TEST_PATH_ASSIGNMENT_tmp  BASE=&tmplib..AB_TEST_PATH_ASSIGNMENT_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..AB_TEST_PATH_ASSIGNMENT_tmp ;
            SET work.AB_TEST_PATH_ASSIGNMENT_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :AB_TEST_PATH_ASSIGNMENT_tmp , AB_TEST_PATH_ASSIGNMENT );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..AB_TEST_PATH_ASSIGNMENT AS b USING &tmpdbschema..AB_TEST_PATH_ASSIGNMENT_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            load_dttm = d.load_dttm, 
            abtestpath_assignment_dttm_tz = d.abtestpath_assignment_dttm_tz, abtestpath_assignment_dttm = d.abtestpath_assignment_dttm, 
            session_id_hex = d.session_id_hex, context_type_nm = d.context_type_nm, 
            channel_user_id = d.channel_user_id, identity_id = d.identity_id, 
            event_nm = d.event_nm, channel_nm = d.channel_nm, 
            event_designed_id = d.event_designed_id, abtest_path_id = d.abtest_path_id, 
            activity_id = d.activity_id, context_val = d.context_val
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            load_dttm, abtestpath_assignment_dttm_tz, abtestpath_assignment_dttm, 
            session_id_hex, context_type_nm, channel_user_id, identity_id, 
            event_nm, channel_nm, event_id, event_designed_id, 
            abtest_path_id, activity_id, context_val
         ) VALUES (
            d.load_dttm, d.abtestpath_assignment_dttm_tz, d.abtestpath_assignment_dttm, 
            d.session_id_hex, d.context_type_nm, d.channel_user_id, d.identity_id, 
            d.event_nm, d.channel_nm, d.event_id, d.event_designed_id, 
            d.abtest_path_id, d.activity_id, d.context_val  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :AB_TEST_PATH_ASSIGNMENT_tmp , AB_TEST_PATH_ASSIGNMENT , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..AB_TEST_PATH_ASSIGNMENT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..AB_TEST_PATH_ASSIGNMENT_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..AB_TEST_PATH_ASSIGNMENT;
         DROP TABLE work.AB_TEST_PATH_ASSIGNMENT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..ACTIVITY_CONVERSION));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..ACTIVITY_CONVERSION_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..ACTIVITY_CONVERSION_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=ACTIVITY_CONVERSION , table_keys=%str(EVENT_ID), out_table=work.ACTIVITY_CONVERSION );
   DATA work.ACTIVITY_CONVERSION_tmp /VIEW=work.ACTIVITY_CONVERSION_tmp ;
      SET work.ACTIVITY_CONVERSION ;
      IF activity_conversion_dttm_tz  NE . THEN activity_conversion_dttm_tz =tzoneu2s(activity_conversion_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :ACTIVITY_CONVERSION_tmp , ACTIVITY_CONVERSION );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..ACTIVITY_CONVERSION_tmp ;
            SET work.ACTIVITY_CONVERSION_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.ACTIVITY_CONVERSION_tmp  BASE=&tmplib..ACTIVITY_CONVERSION_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..ACTIVITY_CONVERSION_tmp ;
            SET work.ACTIVITY_CONVERSION_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :ACTIVITY_CONVERSION_tmp , ACTIVITY_CONVERSION );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..ACTIVITY_CONVERSION AS b USING &tmpdbschema..ACTIVITY_CONVERSION_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            activity_conversion_dttm_tz = d.activity_conversion_dttm_tz, 
            load_dttm = d.load_dttm, activity_conversion_dttm = d.activity_conversion_dttm, 
            abtest_path_id = d.abtest_path_id, activity_id = d.activity_id, 
            activity_node_id = d.activity_node_id, session_id_hex = d.session_id_hex, 
            parent_event_designed_id = d.parent_event_designed_id, identity_id = d.identity_id, 
            goal_id = d.goal_id, event_nm = d.event_nm, 
            event_designed_id = d.event_designed_id, detail_id_hex = d.detail_id_hex, 
            context_val = d.context_val, channel_nm = d.channel_nm, 
            context_type_nm = d.context_type_nm, channel_user_id = d.channel_user_id
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            activity_conversion_dttm_tz, load_dttm, activity_conversion_dttm, 
            abtest_path_id, activity_id, activity_node_id, session_id_hex, 
            parent_event_designed_id, identity_id, goal_id, event_nm, 
            event_id, event_designed_id, detail_id_hex, context_val, 
            channel_nm, context_type_nm, channel_user_id
         ) VALUES (
            d.activity_conversion_dttm_tz, d.load_dttm, d.activity_conversion_dttm, 
            d.abtest_path_id, d.activity_id, d.activity_node_id, d.session_id_hex, 
            d.parent_event_designed_id, d.identity_id, d.goal_id, d.event_nm, 
            d.event_id, d.event_designed_id, d.detail_id_hex, d.context_val, 
            d.channel_nm, d.context_type_nm, d.channel_user_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :ACTIVITY_CONVERSION_tmp , ACTIVITY_CONVERSION , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..ACTIVITY_CONVERSION_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..ACTIVITY_CONVERSION_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..ACTIVITY_CONVERSION;
         DROP TABLE work.ACTIVITY_CONVERSION;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..ACTIVITY_FLOW_IN));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..ACTIVITY_FLOW_IN_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..ACTIVITY_FLOW_IN_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=ACTIVITY_FLOW_IN , table_keys=%str(EVENT_ID), out_table=work.ACTIVITY_FLOW_IN );
   DATA work.ACTIVITY_FLOW_IN_tmp /VIEW=work.ACTIVITY_FLOW_IN_tmp ;
      SET work.ACTIVITY_FLOW_IN ;
      IF activity_flow_in_dttm_tz  NE . THEN activity_flow_in_dttm_tz =tzoneu2s(activity_flow_in_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :ACTIVITY_FLOW_IN_tmp , ACTIVITY_FLOW_IN );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..ACTIVITY_FLOW_IN_tmp ;
            SET work.ACTIVITY_FLOW_IN_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.ACTIVITY_FLOW_IN_tmp  BASE=&tmplib..ACTIVITY_FLOW_IN_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..ACTIVITY_FLOW_IN_tmp ;
            SET work.ACTIVITY_FLOW_IN_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :ACTIVITY_FLOW_IN_tmp , ACTIVITY_FLOW_IN );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..ACTIVITY_FLOW_IN AS b USING &tmpdbschema..ACTIVITY_FLOW_IN_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            activity_flow_in_dttm = d.activity_flow_in_dttm, 
            activity_flow_in_dttm_tz = d.activity_flow_in_dttm_tz, load_dttm = d.load_dttm, 
            task_id = d.task_id, identity_id = d.identity_id, 
            context_val = d.context_val, event_designed_id = d.event_designed_id, 
            channel_user_id = d.channel_user_id, activity_node_id = d.activity_node_id, 
            activity_id = d.activity_id, abtest_path_id = d.abtest_path_id, 
            channel_nm = d.channel_nm, context_type_nm = d.context_type_nm, 
            event_nm = d.event_nm
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            activity_flow_in_dttm, activity_flow_in_dttm_tz, load_dttm, 
            task_id, identity_id, context_val, event_designed_id, 
            event_id, channel_user_id, activity_node_id, activity_id, 
            abtest_path_id, channel_nm, context_type_nm, event_nm
         ) VALUES (
            d.activity_flow_in_dttm, d.activity_flow_in_dttm_tz, d.load_dttm, 
            d.task_id, d.identity_id, d.context_val, d.event_designed_id, 
            d.event_id, d.channel_user_id, d.activity_node_id, d.activity_id, 
            d.abtest_path_id, d.channel_nm, d.context_type_nm, d.event_nm  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :ACTIVITY_FLOW_IN_tmp , ACTIVITY_FLOW_IN , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..ACTIVITY_FLOW_IN_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..ACTIVITY_FLOW_IN_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..ACTIVITY_FLOW_IN;
         DROP TABLE work.ACTIVITY_FLOW_IN;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..ACTIVITY_START));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..ACTIVITY_START_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..ACTIVITY_START_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=ACTIVITY_START , table_keys=%str(EVENT_ID), out_table=work.ACTIVITY_START );
   DATA work.ACTIVITY_START_tmp /VIEW=work.ACTIVITY_START_tmp ;
      SET work.ACTIVITY_START ;
      IF activity_start_dttm_tz  NE . THEN activity_start_dttm_tz =tzoneu2s(activity_start_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :ACTIVITY_START_tmp , ACTIVITY_START );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..ACTIVITY_START_tmp ;
            SET work.ACTIVITY_START_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.ACTIVITY_START_tmp  BASE=&tmplib..ACTIVITY_START_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..ACTIVITY_START_tmp ;
            SET work.ACTIVITY_START_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :ACTIVITY_START_tmp , ACTIVITY_START );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..ACTIVITY_START AS b USING &tmpdbschema..ACTIVITY_START_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            activity_start_dttm_tz = d.activity_start_dttm_tz, 
            load_dttm = d.load_dttm, activity_start_dttm = d.activity_start_dttm, 
            channel_nm = d.channel_nm, activity_id = d.activity_id, 
            identity_id = d.identity_id, event_nm = d.event_nm, 
            channel_user_id = d.channel_user_id, event_designed_id = d.event_designed_id, 
            context_val = d.context_val, context_type_nm = d.context_type_nm
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            activity_start_dttm_tz, load_dttm, activity_start_dttm, 
            channel_nm, activity_id, identity_id, event_nm, 
            event_id, channel_user_id, event_designed_id, context_val, 
            context_type_nm
         ) VALUES (
            d.activity_start_dttm_tz, d.load_dttm, d.activity_start_dttm, 
            d.channel_nm, d.activity_id, d.identity_id, d.event_nm, 
            d.event_id, d.channel_user_id, d.event_designed_id, d.context_val, 
            d.context_type_nm  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :ACTIVITY_START_tmp , ACTIVITY_START , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..ACTIVITY_START_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..ACTIVITY_START_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..ACTIVITY_START;
         DROP TABLE work.ACTIVITY_START;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..ADVERTISING_CONTACT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..ADVERTISING_CONTACT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..ADVERTISING_CONTACT_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=ADVERTISING_CONTACT , table_keys=%str(EVENT_ID), out_table=work.ADVERTISING_CONTACT );
   DATA work.ADVERTISING_CONTACT_tmp /VIEW=work.ADVERTISING_CONTACT_tmp ;
      SET work.ADVERTISING_CONTACT ;
      IF advertising_contact_dttm_tz  NE . THEN advertising_contact_dttm_tz =tzoneu2s(advertising_contact_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :ADVERTISING_CONTACT_tmp , ADVERTISING_CONTACT );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..ADVERTISING_CONTACT_tmp ;
            SET work.ADVERTISING_CONTACT_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.ADVERTISING_CONTACT_tmp  BASE=&tmplib..ADVERTISING_CONTACT_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..ADVERTISING_CONTACT_tmp ;
            SET work.ADVERTISING_CONTACT_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :ADVERTISING_CONTACT_tmp , ADVERTISING_CONTACT );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..ADVERTISING_CONTACT AS b USING &tmpdbschema..ADVERTISING_CONTACT_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            load_dttm, advertising_contact_dttm_tz, advertising_contact_dttm, 
            task_version_id, task_id, task_action_nm, segment_version_id, 
            segment_id, response_tracking_cd, occurrence_id, journey_occurrence_id, 
            journey_id, identity_id, event_nm, event_id, 
            event_designed_id, context_val, context_type_nm, channel_nm, 
            audience_id, aud_occurrence_id, advertising_platform_nm
         ) VALUES (
            d.load_dttm, d.advertising_contact_dttm_tz, d.advertising_contact_dttm, 
            d.task_version_id, d.task_id, d.task_action_nm, d.segment_version_id, 
            d.segment_id, d.response_tracking_cd, d.occurrence_id, d.journey_occurrence_id, 
            d.journey_id, d.identity_id, d.event_nm, d.event_id, 
            d.event_designed_id, d.context_val, d.context_type_nm, d.channel_nm, 
            d.audience_id, d.aud_occurrence_id, d.advertising_platform_nm  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :ADVERTISING_CONTACT_tmp , ADVERTISING_CONTACT , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..ADVERTISING_CONTACT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..ADVERTISING_CONTACT_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..ADVERTISING_CONTACT;
         DROP TABLE work.ADVERTISING_CONTACT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..ASSET_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..ASSET_DETAILS) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate ASSET_DETAILS , ASSET_DETAILS );
   PROC APPEND DATA=&udmmart..ASSET_DETAILS  BASE=&trglib..ASSET_DETAILS (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to ASSET_DETAILS , ASSET_DETAILS );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..ASSET_DETAILS;
         DROP TABLE work.ASSET_DETAILS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..ASSET_DETAILS_CUSTOM_PROP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..ASSET_DETAILS_CUSTOM_PROP) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate ASSET_DETAILS_CUSTOM_PROP , ASSET_DETAILS_CUSTOM_PROP );
   PROC APPEND DATA=&udmmart..ASSET_DETAILS_CUSTOM_PROP  BASE=&trglib..ASSET_DETAILS_CUSTOM_PROP (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to ASSET_DETAILS_CUSTOM_PROP , ASSET_DETAILS_CUSTOM_PROP );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..ASSET_DETAILS_CUSTOM_PROP;
         DROP TABLE work.ASSET_DETAILS_CUSTOM_PROP;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..ASSET_FOLDER_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..ASSET_FOLDER_DETAILS) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate ASSET_FOLDER_DETAILS , ASSET_FOLDER_DETAILS );
   PROC APPEND DATA=&udmmart..ASSET_FOLDER_DETAILS  BASE=&trglib..ASSET_FOLDER_DETAILS (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to ASSET_FOLDER_DETAILS , ASSET_FOLDER_DETAILS );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..ASSET_FOLDER_DETAILS;
         DROP TABLE work.ASSET_FOLDER_DETAILS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..ASSET_RENDITION_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..ASSET_RENDITION_DETAILS) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate ASSET_RENDITION_DETAILS , ASSET_RENDITION_DETAILS );
   PROC APPEND DATA=&udmmart..ASSET_RENDITION_DETAILS  BASE=&trglib..ASSET_RENDITION_DETAILS (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to ASSET_RENDITION_DETAILS , ASSET_RENDITION_DETAILS );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..ASSET_RENDITION_DETAILS;
         DROP TABLE work.ASSET_RENDITION_DETAILS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..ASSET_REVISION));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..ASSET_REVISION) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate ASSET_REVISION , ASSET_REVISION );
   PROC APPEND DATA=&udmmart..ASSET_REVISION  BASE=&trglib..ASSET_REVISION (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to ASSET_REVISION , ASSET_REVISION );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..ASSET_REVISION;
         DROP TABLE work.ASSET_REVISION;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..AUDIENCE_MEMBERSHIP_CHANGE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..AUDIENCE_MEMBERSHIP_CHANGE_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..AUDIENCE_MEMBERSHIP_CHANGE_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=AUDIENCE_MEMBERSHIP_CHANGE , table_keys=%str(EVENT_ID), out_table=work.AUDIENCE_MEMBERSHIP_CHANGE );
   DATA work.AUDIENCE_MEMBERSHIP_CHANGE_tmp /VIEW=work.AUDIENCE_MEMBERSHIP_CHANGE_tmp ;
      SET work.AUDIENCE_MEMBERSHIP_CHANGE ;
      IF audience_change_dttm_tz  NE . THEN audience_change_dttm_tz =tzoneu2s(audience_change_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :AUDIENCE_MEMBERSHIP_CHANGE_tmp , AUDIENCE_MEMBERSHIP_CHANGE );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..AUDIENCE_MEMBERSHIP_CHANGE_tmp ;
            SET work.AUDIENCE_MEMBERSHIP_CHANGE_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.AUDIENCE_MEMBERSHIP_CHANGE_tmp  BASE=&tmplib..AUDIENCE_MEMBERSHIP_CHANGE_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..AUDIENCE_MEMBERSHIP_CHANGE_tmp ;
            SET work.AUDIENCE_MEMBERSHIP_CHANGE_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :AUDIENCE_MEMBERSHIP_CHANGE_tmp , AUDIENCE_MEMBERSHIP_CHANGE );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..AUDIENCE_MEMBERSHIP_CHANGE AS b USING &tmpdbschema..AUDIENCE_MEMBERSHIP_CHANGE_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            audience_change_dttm = d.audience_change_dttm, 
            load_dttm = d.load_dttm, audience_change_dttm_tz = d.audience_change_dttm_tz, 
            identity_id = d.identity_id, aud_occurrence_id = d.aud_occurrence_id, 
            audience_id = d.audience_id, event_nm = d.event_nm
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            audience_change_dttm, load_dttm, audience_change_dttm_tz, 
            identity_id, aud_occurrence_id, event_id, audience_id, 
            event_nm
         ) VALUES (
            d.audience_change_dttm, d.load_dttm, d.audience_change_dttm_tz, 
            d.identity_id, d.aud_occurrence_id, d.event_id, d.audience_id, 
            d.event_nm  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :AUDIENCE_MEMBERSHIP_CHANGE_tmp , AUDIENCE_MEMBERSHIP_CHANGE , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..AUDIENCE_MEMBERSHIP_CHANGE_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..AUDIENCE_MEMBERSHIP_CHANGE_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..AUDIENCE_MEMBERSHIP_CHANGE;
         DROP TABLE work.AUDIENCE_MEMBERSHIP_CHANGE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..BUSINESS_PROCESS_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..BUSINESS_PROCESS_DETAILS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..BUSINESS_PROCESS_DETAILS_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=BUSINESS_PROCESS_DETAILS , table_keys=%str(EVENT_ID), out_table=work.BUSINESS_PROCESS_DETAILS );
   DATA work.BUSINESS_PROCESS_DETAILS_tmp /VIEW=work.BUSINESS_PROCESS_DETAILS_tmp ;
      SET work.BUSINESS_PROCESS_DETAILS ;
      IF process_dttm_tz  NE . THEN process_dttm_tz =tzoneu2s(process_dttm_tz ,&timeZone_Value.);
      IF process_exception_dttm_tz  NE . THEN process_exception_dttm_tz =tzoneu2s(process_exception_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :BUSINESS_PROCESS_DETAILS_tmp , BUSINESS_PROCESS_DETAILS );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..BUSINESS_PROCESS_DETAILS_tmp ;
            SET work.BUSINESS_PROCESS_DETAILS_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.BUSINESS_PROCESS_DETAILS_tmp  BASE=&tmplib..BUSINESS_PROCESS_DETAILS_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..BUSINESS_PROCESS_DETAILS_tmp ;
            SET work.BUSINESS_PROCESS_DETAILS_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :BUSINESS_PROCESS_DETAILS_tmp , BUSINESS_PROCESS_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..BUSINESS_PROCESS_DETAILS AS b USING &tmpdbschema..BUSINESS_PROCESS_DETAILS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            is_start_flg, is_completion_flg, process_attempt_cnt, 
            step_order_no, process_instance_no, process_dttm_tz, process_exception_dttm_tz, 
            load_dttm, process_dttm, process_exception_dttm, visit_id, 
            process_step_nm, process_details_sk, identity_id, event_nm, 
            detail_id, attribute1_txt, detail_id_hex, event_designed_id, 
            event_id, next_detail_id, process_exception_txt, session_id, 
            session_id_hex, visit_id_hex, attribute2_txt, event_source_cd, 
            process_nm
         ) VALUES (
            d.is_start_flg, d.is_completion_flg, d.process_attempt_cnt, 
            d.step_order_no, d.process_instance_no, d.process_dttm_tz, d.process_exception_dttm_tz, 
            d.load_dttm, d.process_dttm, d.process_exception_dttm, d.visit_id, 
            d.process_step_nm, d.process_details_sk, d.identity_id, d.event_nm, 
            d.detail_id, d.attribute1_txt, d.detail_id_hex, d.event_designed_id, 
            d.event_id, d.next_detail_id, d.process_exception_txt, d.session_id, 
            d.session_id_hex, d.visit_id_hex, d.attribute2_txt, d.event_source_cd, 
            d.process_nm  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :BUSINESS_PROCESS_DETAILS_tmp , BUSINESS_PROCESS_DETAILS , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..BUSINESS_PROCESS_DETAILS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..BUSINESS_PROCESS_DETAILS_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..BUSINESS_PROCESS_DETAILS;
         DROP TABLE work.BUSINESS_PROCESS_DETAILS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CART_ACTIVITY_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..CART_ACTIVITY_DETAILS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..CART_ACTIVITY_DETAILS_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=CART_ACTIVITY_DETAILS , table_keys=%str(EVENT_ID), out_table=work.CART_ACTIVITY_DETAILS );
   DATA work.CART_ACTIVITY_DETAILS_tmp /VIEW=work.CART_ACTIVITY_DETAILS_tmp ;
      SET work.CART_ACTIVITY_DETAILS ;
      IF activity_dttm_tz  NE . THEN activity_dttm_tz =tzoneu2s(activity_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :CART_ACTIVITY_DETAILS_tmp , CART_ACTIVITY_DETAILS );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..CART_ACTIVITY_DETAILS_tmp ;
            SET work.CART_ACTIVITY_DETAILS_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.CART_ACTIVITY_DETAILS_tmp  BASE=&tmplib..CART_ACTIVITY_DETAILS_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..CART_ACTIVITY_DETAILS_tmp ;
            SET work.CART_ACTIVITY_DETAILS_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :CART_ACTIVITY_DETAILS_tmp , CART_ACTIVITY_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..CART_ACTIVITY_DETAILS AS b USING &tmpdbschema..CART_ACTIVITY_DETAILS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            unit_price_amt, displayed_cart_amt, quantity_val, 
            displayed_cart_items_no, properties_map_doc, activity_dttm, load_dttm, 
            activity_dttm_tz, cart_activity_sk, activity_cd, visit_id_hex, 
            visit_id, shipping_message_txt, session_id_hex, session_id, 
            saving_message_txt, product_sku, product_nm, product_id, 
            product_group_nm, mobile_app_id, identity_id, event_source_cd, 
            event_nm, availability_message_txt, cart_id, event_id, 
            event_designed_id, detail_id_hex, detail_id, currency_cd, 
            event_key_cd, channel_nm, cart_nm
         ) VALUES (
            d.unit_price_amt, d.displayed_cart_amt, d.quantity_val, 
            d.displayed_cart_items_no, d.properties_map_doc, d.activity_dttm, d.load_dttm, 
            d.activity_dttm_tz, d.cart_activity_sk, d.activity_cd, d.visit_id_hex, 
            d.visit_id, d.shipping_message_txt, d.session_id_hex, d.session_id, 
            d.saving_message_txt, d.product_sku, d.product_nm, d.product_id, 
            d.product_group_nm, d.mobile_app_id, d.identity_id, d.event_source_cd, 
            d.event_nm, d.availability_message_txt, d.cart_id, d.event_id, 
            d.event_designed_id, d.detail_id_hex, d.detail_id, d.currency_cd, 
            d.event_key_cd, d.channel_nm, d.cart_nm  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :CART_ACTIVITY_DETAILS_tmp , CART_ACTIVITY_DETAILS , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..CART_ACTIVITY_DETAILS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..CART_ACTIVITY_DETAILS_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CART_ACTIVITY_DETAILS;
         DROP TABLE work.CART_ACTIVITY_DETAILS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CC_BUDGET_BREAKUP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CC_BUDGET_BREAKUP) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CC_BUDGET_BREAKUP , CC_BUDGET_BREAKUP );
   PROC APPEND DATA=&udmmart..CC_BUDGET_BREAKUP  BASE=&trglib..CC_BUDGET_BREAKUP (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CC_BUDGET_BREAKUP , CC_BUDGET_BREAKUP );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CC_BUDGET_BREAKUP;
         DROP TABLE work.CC_BUDGET_BREAKUP;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CC_BUDGET_BREAKUP_CCBDGT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CC_BUDGET_BREAKUP_CCBDGT) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CC_BUDGET_BREAKUP_CCBDGT , CC_BUDGET_BREAKUP_CCBDGT );
   PROC APPEND DATA=&udmmart..CC_BUDGET_BREAKUP_CCBDGT  BASE=&trglib..CC_BUDGET_BREAKUP_CCBDGT (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CC_BUDGET_BREAKUP_CCBDGT , CC_BUDGET_BREAKUP_CCBDGT );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CC_BUDGET_BREAKUP_CCBDGT;
         DROP TABLE work.CC_BUDGET_BREAKUP_CCBDGT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_ACTIVITY_CUSTOM_ATTR));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_ACTIVITY_CUSTOM_ATTR) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_ACTIVITY_CUSTOM_ATTR , CDM_ACTIVITY_CUSTOM_ATTR );
   PROC APPEND DATA=&udmmart..CDM_ACTIVITY_CUSTOM_ATTR  BASE=&trglib..CDM_ACTIVITY_CUSTOM_ATTR (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_ACTIVITY_CUSTOM_ATTR , CDM_ACTIVITY_CUSTOM_ATTR );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_ACTIVITY_CUSTOM_ATTR;
         DROP TABLE work.CDM_ACTIVITY_CUSTOM_ATTR;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_ACTIVITY_DETAIL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_ACTIVITY_DETAIL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_ACTIVITY_DETAIL , CDM_ACTIVITY_DETAIL );
   PROC APPEND DATA=&udmmart..CDM_ACTIVITY_DETAIL  BASE=&trglib..CDM_ACTIVITY_DETAIL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_ACTIVITY_DETAIL , CDM_ACTIVITY_DETAIL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_ACTIVITY_DETAIL;
         DROP TABLE work.CDM_ACTIVITY_DETAIL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_ACTIVITY_X_TASK));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_ACTIVITY_X_TASK) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_ACTIVITY_X_TASK , CDM_ACTIVITY_X_TASK );
   PROC APPEND DATA=&udmmart..CDM_ACTIVITY_X_TASK  BASE=&trglib..CDM_ACTIVITY_X_TASK (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_ACTIVITY_X_TASK , CDM_ACTIVITY_X_TASK );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_ACTIVITY_X_TASK;
         DROP TABLE work.CDM_ACTIVITY_X_TASK;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_AUDIENCE_DETAIL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_AUDIENCE_DETAIL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_AUDIENCE_DETAIL , CDM_AUDIENCE_DETAIL );
   PROC APPEND DATA=&udmmart..CDM_AUDIENCE_DETAIL  BASE=&trglib..CDM_AUDIENCE_DETAIL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_AUDIENCE_DETAIL , CDM_AUDIENCE_DETAIL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_AUDIENCE_DETAIL;
         DROP TABLE work.CDM_AUDIENCE_DETAIL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_AUDIENCE_OCCUR_DETAIL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_AUDIENCE_OCCUR_DETAIL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_AUDIENCE_OCCUR_DETAIL , CDM_AUDIENCE_OCCUR_DETAIL );
   PROC APPEND DATA=&udmmart..CDM_AUDIENCE_OCCUR_DETAIL  BASE=&trglib..CDM_AUDIENCE_OCCUR_DETAIL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_AUDIENCE_OCCUR_DETAIL , CDM_AUDIENCE_OCCUR_DETAIL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_AUDIENCE_OCCUR_DETAIL;
         DROP TABLE work.CDM_AUDIENCE_OCCUR_DETAIL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_AUDIENCE_X_SEGMENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_AUDIENCE_X_SEGMENT) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_AUDIENCE_X_SEGMENT , CDM_AUDIENCE_X_SEGMENT );
   PROC APPEND DATA=&udmmart..CDM_AUDIENCE_X_SEGMENT  BASE=&trglib..CDM_AUDIENCE_X_SEGMENT (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_AUDIENCE_X_SEGMENT , CDM_AUDIENCE_X_SEGMENT );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_AUDIENCE_X_SEGMENT;
         DROP TABLE work.CDM_AUDIENCE_X_SEGMENT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_BUSINESS_CONTEXT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_BUSINESS_CONTEXT) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_BUSINESS_CONTEXT , CDM_BUSINESS_CONTEXT );
   PROC APPEND DATA=&udmmart..CDM_BUSINESS_CONTEXT  BASE=&trglib..CDM_BUSINESS_CONTEXT (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_BUSINESS_CONTEXT , CDM_BUSINESS_CONTEXT );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_BUSINESS_CONTEXT;
         DROP TABLE work.CDM_BUSINESS_CONTEXT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_CAMPAIGN_CUSTOM_ATTR));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_CAMPAIGN_CUSTOM_ATTR) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_CAMPAIGN_CUSTOM_ATTR , CDM_CAMPAIGN_CUSTOM_ATTR );
   PROC APPEND DATA=&udmmart..CDM_CAMPAIGN_CUSTOM_ATTR  BASE=&trglib..CDM_CAMPAIGN_CUSTOM_ATTR (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_CAMPAIGN_CUSTOM_ATTR , CDM_CAMPAIGN_CUSTOM_ATTR );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_CAMPAIGN_CUSTOM_ATTR;
         DROP TABLE work.CDM_CAMPAIGN_CUSTOM_ATTR;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_CAMPAIGN_DETAIL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_CAMPAIGN_DETAIL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_CAMPAIGN_DETAIL , CDM_CAMPAIGN_DETAIL );
   PROC APPEND DATA=&udmmart..CDM_CAMPAIGN_DETAIL  BASE=&trglib..CDM_CAMPAIGN_DETAIL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_CAMPAIGN_DETAIL , CDM_CAMPAIGN_DETAIL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_CAMPAIGN_DETAIL;
         DROP TABLE work.CDM_CAMPAIGN_DETAIL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_CONTACT_CHANNEL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_CONTACT_CHANNEL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_CONTACT_CHANNEL , CDM_CONTACT_CHANNEL );
   PROC APPEND DATA=&udmmart..CDM_CONTACT_CHANNEL  BASE=&trglib..CDM_CONTACT_CHANNEL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_CONTACT_CHANNEL , CDM_CONTACT_CHANNEL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_CONTACT_CHANNEL;
         DROP TABLE work.CDM_CONTACT_CHANNEL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_CONTACT_HISTORY));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..CDM_CONTACT_HISTORY_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..CDM_CONTACT_HISTORY_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=CDM_CONTACT_HISTORY , table_keys=%str(CONTACT_ID), out_table=work.CDM_CONTACT_HISTORY );
   DATA work.CDM_CONTACT_HISTORY_tmp /VIEW=work.CDM_CONTACT_HISTORY_tmp ;
      SET work.CDM_CONTACT_HISTORY ;
      IF contact_dttm_tz  NE . THEN contact_dttm_tz =tzoneu2s(contact_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :CDM_CONTACT_HISTORY_tmp , CDM_CONTACT_HISTORY );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..CDM_CONTACT_HISTORY_tmp ;
            SET work.CDM_CONTACT_HISTORY_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.CDM_CONTACT_HISTORY_tmp  BASE=&tmplib..CDM_CONTACT_HISTORY_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..CDM_CONTACT_HISTORY_tmp ;
            SET work.CDM_CONTACT_HISTORY_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :CDM_CONTACT_HISTORY_tmp , CDM_CONTACT_HISTORY );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..CDM_CONTACT_HISTORY AS b USING &tmpdbschema..CDM_CONTACT_HISTORY_tmp AS d ON (
            b.contact_id = d.contact_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND CONTACT_ID IS NOT NULL THEN INSERT (
            optimization_backfill_flg, control_group_flg, contact_dt, 
            updated_dttm, contact_dttm_tz, contact_dttm, source_system_cd, 
            external_contact_info_2_id, context_type_nm, audience_id, contact_nm, 
            identity_id, audience_occur_id, contact_id, contact_status_cd, 
            context_val, external_contact_info_1_id, rtc_id, updated_by_nm
         ) VALUES (
            d.optimization_backfill_flg, d.control_group_flg, d.contact_dt, 
            d.updated_dttm, d.contact_dttm_tz, d.contact_dttm, d.source_system_cd, 
            d.external_contact_info_2_id, d.context_type_nm, d.audience_id, d.contact_nm, 
            d.identity_id, d.audience_occur_id, d.contact_id, d.contact_status_cd, 
            d.context_val, d.external_contact_info_1_id, d.rtc_id, d.updated_by_nm  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :CDM_CONTACT_HISTORY_tmp , CDM_CONTACT_HISTORY , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..CDM_CONTACT_HISTORY_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..CDM_CONTACT_HISTORY_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_CONTACT_HISTORY;
         DROP TABLE work.CDM_CONTACT_HISTORY;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_CONTACT_STATUS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_CONTACT_STATUS) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_CONTACT_STATUS , CDM_CONTACT_STATUS );
   PROC APPEND DATA=&udmmart..CDM_CONTACT_STATUS  BASE=&trglib..CDM_CONTACT_STATUS (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_CONTACT_STATUS , CDM_CONTACT_STATUS );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_CONTACT_STATUS;
         DROP TABLE work.CDM_CONTACT_STATUS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_CONTENT_CUSTOM_ATTR));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_CONTENT_CUSTOM_ATTR) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_CONTENT_CUSTOM_ATTR , CDM_CONTENT_CUSTOM_ATTR );
   PROC APPEND DATA=&udmmart..CDM_CONTENT_CUSTOM_ATTR  BASE=&trglib..CDM_CONTENT_CUSTOM_ATTR (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_CONTENT_CUSTOM_ATTR , CDM_CONTENT_CUSTOM_ATTR );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_CONTENT_CUSTOM_ATTR;
         DROP TABLE work.CDM_CONTENT_CUSTOM_ATTR;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_CONTENT_DETAIL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_CONTENT_DETAIL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_CONTENT_DETAIL , CDM_CONTENT_DETAIL );
   PROC APPEND DATA=&udmmart..CDM_CONTENT_DETAIL  BASE=&trglib..CDM_CONTENT_DETAIL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_CONTENT_DETAIL , CDM_CONTENT_DETAIL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_CONTENT_DETAIL;
         DROP TABLE work.CDM_CONTENT_DETAIL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_DYN_CONTENT_CUSTOM_ATTR));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_DYN_CONTENT_CUSTOM_ATTR) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_DYN_CONTENT_CUSTOM_ATTR , CDM_DYN_CONTENT_CUSTOM_ATTR );
   PROC APPEND DATA=&udmmart..CDM_DYN_CONTENT_CUSTOM_ATTR  BASE=&trglib..CDM_DYN_CONTENT_CUSTOM_ATTR (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_DYN_CONTENT_CUSTOM_ATTR , CDM_DYN_CONTENT_CUSTOM_ATTR );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_DYN_CONTENT_CUSTOM_ATTR;
         DROP TABLE work.CDM_DYN_CONTENT_CUSTOM_ATTR;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_IDENTIFIER_TYPE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_IDENTIFIER_TYPE) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_IDENTIFIER_TYPE , CDM_IDENTIFIER_TYPE );
   PROC APPEND DATA=&udmmart..CDM_IDENTIFIER_TYPE  BASE=&trglib..CDM_IDENTIFIER_TYPE (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_IDENTIFIER_TYPE , CDM_IDENTIFIER_TYPE );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_IDENTIFIER_TYPE;
         DROP TABLE work.CDM_IDENTIFIER_TYPE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_IDENTITY_ATTR));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_IDENTITY_ATTR) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_IDENTITY_ATTR , CDM_IDENTITY_ATTR );
   PROC APPEND DATA=&udmmart..CDM_IDENTITY_ATTR  BASE=&trglib..CDM_IDENTITY_ATTR (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_IDENTITY_ATTR , CDM_IDENTITY_ATTR );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_IDENTITY_ATTR;
         DROP TABLE work.CDM_IDENTITY_ATTR;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_IDENTITY_MAP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_IDENTITY_MAP) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_IDENTITY_MAP , CDM_IDENTITY_MAP );
   PROC APPEND DATA=&udmmart..CDM_IDENTITY_MAP  BASE=&trglib..CDM_IDENTITY_MAP (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_IDENTITY_MAP , CDM_IDENTITY_MAP );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_IDENTITY_MAP;
         DROP TABLE work.CDM_IDENTITY_MAP;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_IDENTITY_TYPE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_IDENTITY_TYPE) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_IDENTITY_TYPE , CDM_IDENTITY_TYPE );
   PROC APPEND DATA=&udmmart..CDM_IDENTITY_TYPE  BASE=&trglib..CDM_IDENTITY_TYPE (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_IDENTITY_TYPE , CDM_IDENTITY_TYPE );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_IDENTITY_TYPE;
         DROP TABLE work.CDM_IDENTITY_TYPE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_OCCURRENCE_DETAIL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_OCCURRENCE_DETAIL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_OCCURRENCE_DETAIL , CDM_OCCURRENCE_DETAIL );
   PROC APPEND DATA=&udmmart..CDM_OCCURRENCE_DETAIL  BASE=&trglib..CDM_OCCURRENCE_DETAIL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_OCCURRENCE_DETAIL , CDM_OCCURRENCE_DETAIL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_OCCURRENCE_DETAIL;
         DROP TABLE work.CDM_OCCURRENCE_DETAIL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_RESPONSE_CHANNEL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_RESPONSE_CHANNEL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_RESPONSE_CHANNEL , CDM_RESPONSE_CHANNEL );
   PROC APPEND DATA=&udmmart..CDM_RESPONSE_CHANNEL  BASE=&trglib..CDM_RESPONSE_CHANNEL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_RESPONSE_CHANNEL , CDM_RESPONSE_CHANNEL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_RESPONSE_CHANNEL;
         DROP TABLE work.CDM_RESPONSE_CHANNEL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_RESPONSE_EXTENDED_ATTR));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..CDM_RESPONSE_EXTENDED_ATTR_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..CDM_RESPONSE_EXTENDED_ATTR_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=CDM_RESPONSE_EXTENDED_ATTR , table_keys=%str(ATTRIBUTE_NM,RESPONSE_ATTRIBUTE_TYPE_CD,RESPONSE_ID), out_table=work.CDM_RESPONSE_EXTENDED_ATTR );
   DATA work.CDM_RESPONSE_EXTENDED_ATTR_tmp /VIEW=work.CDM_RESPONSE_EXTENDED_ATTR_tmp ;
      SET work.CDM_RESPONSE_EXTENDED_ATTR ;
   RUN;
   %err_check (Failed to add time zone adaptation :CDM_RESPONSE_EXTENDED_ATTR_tmp , CDM_RESPONSE_EXTENDED_ATTR );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..CDM_RESPONSE_EXTENDED_ATTR_tmp ;
            SET work.CDM_RESPONSE_EXTENDED_ATTR_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.CDM_RESPONSE_EXTENDED_ATTR_tmp  BASE=&tmplib..CDM_RESPONSE_EXTENDED_ATTR_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..CDM_RESPONSE_EXTENDED_ATTR_tmp ;
            SET work.CDM_RESPONSE_EXTENDED_ATTR_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :CDM_RESPONSE_EXTENDED_ATTR_tmp , CDM_RESPONSE_EXTENDED_ATTR );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..CDM_RESPONSE_EXTENDED_ATTR AS b USING &tmpdbschema..CDM_RESPONSE_EXTENDED_ATTR_tmp AS d ON (
            b.response_id = d.response_id AND 
            b.response_attribute_type_cd = d.response_attribute_type_cd AND b.attribute_nm = d.attribute_nm )
         WHEN MATCHED THEN
         UPDATE SET
            updated_dttm = d.updated_dttm, 
            updated_by_nm = d.updated_by_nm, attribute_val = d.attribute_val, 
            attribute_data_type_cd = d.attribute_data_type_cd
         WHEN NOT MATCHED AND ATTRIBUTE_NM IS NOT NULL AND RESPONSE_ATTRIBUTE_TYPE_CD IS NOT NULL AND RESPONSE_ID IS NOT NULL THEN INSERT (
            updated_dttm, updated_by_nm, response_id, 
            response_attribute_type_cd, attribute_val, attribute_nm, attribute_data_type_cd
         ) VALUES (
            d.updated_dttm, d.updated_by_nm, d.response_id, 
            d.response_attribute_type_cd, d.attribute_val, d.attribute_nm, d.attribute_data_type_cd  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :CDM_RESPONSE_EXTENDED_ATTR_tmp , CDM_RESPONSE_EXTENDED_ATTR , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..CDM_RESPONSE_EXTENDED_ATTR_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..CDM_RESPONSE_EXTENDED_ATTR_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_RESPONSE_EXTENDED_ATTR;
         DROP TABLE work.CDM_RESPONSE_EXTENDED_ATTR;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_RESPONSE_HISTORY));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..CDM_RESPONSE_HISTORY_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..CDM_RESPONSE_HISTORY_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=CDM_RESPONSE_HISTORY , table_keys=%str(RESPONSE_ID), out_table=work.CDM_RESPONSE_HISTORY );
   DATA work.CDM_RESPONSE_HISTORY_tmp /VIEW=work.CDM_RESPONSE_HISTORY_tmp ;
      SET work.CDM_RESPONSE_HISTORY ;
      IF response_dttm_tz  NE . THEN response_dttm_tz =tzoneu2s(response_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :CDM_RESPONSE_HISTORY_tmp , CDM_RESPONSE_HISTORY );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..CDM_RESPONSE_HISTORY_tmp ;
            SET work.CDM_RESPONSE_HISTORY_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.CDM_RESPONSE_HISTORY_tmp  BASE=&tmplib..CDM_RESPONSE_HISTORY_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..CDM_RESPONSE_HISTORY_tmp ;
            SET work.CDM_RESPONSE_HISTORY_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :CDM_RESPONSE_HISTORY_tmp , CDM_RESPONSE_HISTORY );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..CDM_RESPONSE_HISTORY AS b USING &tmpdbschema..CDM_RESPONSE_HISTORY_tmp AS d ON (
            b.response_id = d.response_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND RESPONSE_ID IS NOT NULL THEN INSERT (
            conversion_flg, inferred_response_flg, response_dt, 
            response_val_amt, properties_map_doc, updated_dttm, response_dttm, 
            response_dttm_tz, updated_by_nm, source_system_cd, rtc_id, 
            response_type_cd, response_id, response_channel_cd, response_cd, 
            identity_id, external_contact_info_2_id, external_contact_info_1_id, context_val, 
            context_type_nm, content_version_id, content_id, content_hash_val, 
            contact_id, audience_occur_id, audience_id
         ) VALUES (
            d.conversion_flg, d.inferred_response_flg, d.response_dt, 
            d.response_val_amt, d.properties_map_doc, d.updated_dttm, d.response_dttm, 
            d.response_dttm_tz, d.updated_by_nm, d.source_system_cd, d.rtc_id, 
            d.response_type_cd, d.response_id, d.response_channel_cd, d.response_cd, 
            d.identity_id, d.external_contact_info_2_id, d.external_contact_info_1_id, d.context_val, 
            d.context_type_nm, d.content_version_id, d.content_id, d.content_hash_val, 
            d.contact_id, d.audience_occur_id, d.audience_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :CDM_RESPONSE_HISTORY_tmp , CDM_RESPONSE_HISTORY , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..CDM_RESPONSE_HISTORY_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..CDM_RESPONSE_HISTORY_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_RESPONSE_HISTORY;
         DROP TABLE work.CDM_RESPONSE_HISTORY;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_RESPONSE_LOOKUP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_RESPONSE_LOOKUP) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_RESPONSE_LOOKUP , CDM_RESPONSE_LOOKUP );
   PROC APPEND DATA=&udmmart..CDM_RESPONSE_LOOKUP  BASE=&trglib..CDM_RESPONSE_LOOKUP (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_RESPONSE_LOOKUP , CDM_RESPONSE_LOOKUP );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_RESPONSE_LOOKUP;
         DROP TABLE work.CDM_RESPONSE_LOOKUP;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_RESPONSE_TYPE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_RESPONSE_TYPE) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_RESPONSE_TYPE , CDM_RESPONSE_TYPE );
   PROC APPEND DATA=&udmmart..CDM_RESPONSE_TYPE  BASE=&trglib..CDM_RESPONSE_TYPE (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_RESPONSE_TYPE , CDM_RESPONSE_TYPE );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_RESPONSE_TYPE;
         DROP TABLE work.CDM_RESPONSE_TYPE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_RTC_DETAIL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_RTC_DETAIL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_RTC_DETAIL , CDM_RTC_DETAIL );
   PROC APPEND DATA=&udmmart..CDM_RTC_DETAIL  BASE=&trglib..CDM_RTC_DETAIL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_RTC_DETAIL , CDM_RTC_DETAIL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_RTC_DETAIL;
         DROP TABLE work.CDM_RTC_DETAIL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_RTC_X_CONTENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_RTC_X_CONTENT) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_RTC_X_CONTENT , CDM_RTC_X_CONTENT );
   PROC APPEND DATA=&udmmart..CDM_RTC_X_CONTENT  BASE=&trglib..CDM_RTC_X_CONTENT (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_RTC_X_CONTENT , CDM_RTC_X_CONTENT );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_RTC_X_CONTENT;
         DROP TABLE work.CDM_RTC_X_CONTENT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_SEGMENT_CUSTOM_ATTR));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_SEGMENT_CUSTOM_ATTR) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_SEGMENT_CUSTOM_ATTR , CDM_SEGMENT_CUSTOM_ATTR );
   PROC APPEND DATA=&udmmart..CDM_SEGMENT_CUSTOM_ATTR  BASE=&trglib..CDM_SEGMENT_CUSTOM_ATTR (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_SEGMENT_CUSTOM_ATTR , CDM_SEGMENT_CUSTOM_ATTR );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_SEGMENT_CUSTOM_ATTR;
         DROP TABLE work.CDM_SEGMENT_CUSTOM_ATTR;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_SEGMENT_DETAIL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_SEGMENT_DETAIL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_SEGMENT_DETAIL , CDM_SEGMENT_DETAIL );
   PROC APPEND DATA=&udmmart..CDM_SEGMENT_DETAIL  BASE=&trglib..CDM_SEGMENT_DETAIL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_SEGMENT_DETAIL , CDM_SEGMENT_DETAIL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_SEGMENT_DETAIL;
         DROP TABLE work.CDM_SEGMENT_DETAIL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_SEGMENT_MAP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_SEGMENT_MAP) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_SEGMENT_MAP , CDM_SEGMENT_MAP );
   PROC APPEND DATA=&udmmart..CDM_SEGMENT_MAP  BASE=&trglib..CDM_SEGMENT_MAP (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_SEGMENT_MAP , CDM_SEGMENT_MAP );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_SEGMENT_MAP;
         DROP TABLE work.CDM_SEGMENT_MAP;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_SEGMENT_MAP_CUSTOM_ATTR));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_SEGMENT_MAP_CUSTOM_ATTR) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_SEGMENT_MAP_CUSTOM_ATTR , CDM_SEGMENT_MAP_CUSTOM_ATTR );
   PROC APPEND DATA=&udmmart..CDM_SEGMENT_MAP_CUSTOM_ATTR  BASE=&trglib..CDM_SEGMENT_MAP_CUSTOM_ATTR (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_SEGMENT_MAP_CUSTOM_ATTR , CDM_SEGMENT_MAP_CUSTOM_ATTR );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_SEGMENT_MAP_CUSTOM_ATTR;
         DROP TABLE work.CDM_SEGMENT_MAP_CUSTOM_ATTR;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_SEGMENT_TEST));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_SEGMENT_TEST) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_SEGMENT_TEST , CDM_SEGMENT_TEST );
   PROC APPEND DATA=&udmmart..CDM_SEGMENT_TEST  BASE=&trglib..CDM_SEGMENT_TEST (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_SEGMENT_TEST , CDM_SEGMENT_TEST );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_SEGMENT_TEST;
         DROP TABLE work.CDM_SEGMENT_TEST;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_SEGMENT_TEST_X_SEGMENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_SEGMENT_TEST_X_SEGMENT) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_SEGMENT_TEST_X_SEGMENT , CDM_SEGMENT_TEST_X_SEGMENT );
   PROC APPEND DATA=&udmmart..CDM_SEGMENT_TEST_X_SEGMENT  BASE=&trglib..CDM_SEGMENT_TEST_X_SEGMENT (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_SEGMENT_TEST_X_SEGMENT , CDM_SEGMENT_TEST_X_SEGMENT );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_SEGMENT_TEST_X_SEGMENT;
         DROP TABLE work.CDM_SEGMENT_TEST_X_SEGMENT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_TASK_CUSTOM_ATTR));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_TASK_CUSTOM_ATTR) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_TASK_CUSTOM_ATTR , CDM_TASK_CUSTOM_ATTR );
   PROC APPEND DATA=&udmmart..CDM_TASK_CUSTOM_ATTR  BASE=&trglib..CDM_TASK_CUSTOM_ATTR (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_TASK_CUSTOM_ATTR , CDM_TASK_CUSTOM_ATTR );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_TASK_CUSTOM_ATTR;
         DROP TABLE work.CDM_TASK_CUSTOM_ATTR;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CDM_TASK_DETAIL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..CDM_TASK_DETAIL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate CDM_TASK_DETAIL , CDM_TASK_DETAIL );
   PROC APPEND DATA=&udmmart..CDM_TASK_DETAIL  BASE=&trglib..CDM_TASK_DETAIL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to CDM_TASK_DETAIL , CDM_TASK_DETAIL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CDM_TASK_DETAIL;
         DROP TABLE work.CDM_TASK_DETAIL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..COMMITMENT_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..COMMITMENT_DETAILS) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate COMMITMENT_DETAILS , COMMITMENT_DETAILS );
   PROC APPEND DATA=&udmmart..COMMITMENT_DETAILS  BASE=&trglib..COMMITMENT_DETAILS (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to COMMITMENT_DETAILS , COMMITMENT_DETAILS );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..COMMITMENT_DETAILS;
         DROP TABLE work.COMMITMENT_DETAILS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..COMMITMENT_LINE_ITEMS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..COMMITMENT_LINE_ITEMS) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate COMMITMENT_LINE_ITEMS , COMMITMENT_LINE_ITEMS );
   PROC APPEND DATA=&udmmart..COMMITMENT_LINE_ITEMS  BASE=&trglib..COMMITMENT_LINE_ITEMS (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to COMMITMENT_LINE_ITEMS , COMMITMENT_LINE_ITEMS );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..COMMITMENT_LINE_ITEMS;
         DROP TABLE work.COMMITMENT_LINE_ITEMS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..COMMITMENT_LINE_ITEMS_CCBDGT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..COMMITMENT_LINE_ITEMS_CCBDGT) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate COMMITMENT_LINE_ITEMS_CCBDGT , COMMITMENT_LINE_ITEMS_CCBDGT );
   PROC APPEND DATA=&udmmart..COMMITMENT_LINE_ITEMS_CCBDGT  BASE=&trglib..COMMITMENT_LINE_ITEMS_CCBDGT (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to COMMITMENT_LINE_ITEMS_CCBDGT , COMMITMENT_LINE_ITEMS_CCBDGT );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..COMMITMENT_LINE_ITEMS_CCBDGT;
         DROP TABLE work.COMMITMENT_LINE_ITEMS_CCBDGT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CONTACT_HISTORY));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..CONTACT_HISTORY_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..CONTACT_HISTORY_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=CONTACT_HISTORY , table_keys=%str(CONTACT_ID), out_table=work.CONTACT_HISTORY );
   DATA work.CONTACT_HISTORY_tmp /VIEW=work.CONTACT_HISTORY_tmp ;
      SET work.CONTACT_HISTORY ;
      IF contact_dttm_tz  NE . THEN contact_dttm_tz =tzoneu2s(contact_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :CONTACT_HISTORY_tmp , CONTACT_HISTORY );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..CONTACT_HISTORY_tmp ;
            SET work.CONTACT_HISTORY_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.CONTACT_HISTORY_tmp  BASE=&tmplib..CONTACT_HISTORY_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..CONTACT_HISTORY_tmp ;
            SET work.CONTACT_HISTORY_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :CONTACT_HISTORY_tmp , CONTACT_HISTORY );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..CONTACT_HISTORY AS b USING &tmpdbschema..CONTACT_HISTORY_tmp AS d ON (
            b.contact_id = d.contact_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND CONTACT_ID IS NOT NULL THEN INSERT (
            control_group_flg, properties_map_doc, contact_dttm_tz, 
            load_dttm, contact_dttm, task_id, parent_event_designed_id, 
            journey_occurrence_id, detail_id_hex, context_type_nm, audience_id, 
            contact_id, identity_id, message_id, response_tracking_cd, 
            visit_id_hex, aud_occurrence_id, contact_channel_nm, contact_nm, 
            context_val, creative_id, event_designed_id, journey_id, 
            occurrence_id, session_id_hex, task_version_id
         ) VALUES (
            d.control_group_flg, d.properties_map_doc, d.contact_dttm_tz, 
            d.load_dttm, d.contact_dttm, d.task_id, d.parent_event_designed_id, 
            d.journey_occurrence_id, d.detail_id_hex, d.context_type_nm, d.audience_id, 
            d.contact_id, d.identity_id, d.message_id, d.response_tracking_cd, 
            d.visit_id_hex, d.aud_occurrence_id, d.contact_channel_nm, d.contact_nm, 
            d.context_val, d.creative_id, d.event_designed_id, d.journey_id, 
            d.occurrence_id, d.session_id_hex, d.task_version_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :CONTACT_HISTORY_tmp , CONTACT_HISTORY , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..CONTACT_HISTORY_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..CONTACT_HISTORY_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CONTACT_HISTORY;
         DROP TABLE work.CONTACT_HISTORY;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CONVERSION_MILESTONE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..CONVERSION_MILESTONE_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..CONVERSION_MILESTONE_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=CONVERSION_MILESTONE , table_keys=%str(EVENT_ID), out_table=work.CONVERSION_MILESTONE );
   DATA work.CONVERSION_MILESTONE_tmp /VIEW=work.CONVERSION_MILESTONE_tmp ;
      SET work.CONVERSION_MILESTONE ;
      IF conversion_milestone_dttm_tz  NE . THEN conversion_milestone_dttm_tz =tzoneu2s(conversion_milestone_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :CONVERSION_MILESTONE_tmp , CONVERSION_MILESTONE );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..CONVERSION_MILESTONE_tmp ;
            SET work.CONVERSION_MILESTONE_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.CONVERSION_MILESTONE_tmp  BASE=&tmplib..CONVERSION_MILESTONE_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..CONVERSION_MILESTONE_tmp ;
            SET work.CONVERSION_MILESTONE_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :CONVERSION_MILESTONE_tmp , CONVERSION_MILESTONE );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..CONVERSION_MILESTONE AS b USING &tmpdbschema..CONVERSION_MILESTONE_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
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
         ) VALUES (
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
            d.session_id_hex, d.subject_line_txt, d.task_version_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :CONVERSION_MILESTONE_tmp , CONVERSION_MILESTONE , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..CONVERSION_MILESTONE_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..CONVERSION_MILESTONE_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CONVERSION_MILESTONE;
         DROP TABLE work.CONVERSION_MILESTONE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CUSTOM_EVENTS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..CUSTOM_EVENTS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..CUSTOM_EVENTS_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=CUSTOM_EVENTS , table_keys=%str(EVENT_ID), out_table=work.CUSTOM_EVENTS );
   DATA work.CUSTOM_EVENTS_tmp /VIEW=work.CUSTOM_EVENTS_tmp ;
      SET work.CUSTOM_EVENTS ;
      IF custom_event_dttm_tz  NE . THEN custom_event_dttm_tz =tzoneu2s(custom_event_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :CUSTOM_EVENTS_tmp , CUSTOM_EVENTS );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..CUSTOM_EVENTS_tmp ;
            SET work.CUSTOM_EVENTS_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.CUSTOM_EVENTS_tmp  BASE=&tmplib..CUSTOM_EVENTS_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..CUSTOM_EVENTS_tmp ;
            SET work.CUSTOM_EVENTS_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :CUSTOM_EVENTS_tmp , CUSTOM_EVENTS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..CUSTOM_EVENTS AS b USING &tmpdbschema..CUSTOM_EVENTS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            custom_revenue_amt, properties_map_doc, custom_event_dttm, 
            custom_event_dttm_tz, load_dttm, session_id, page_id, 
            event_type_nm, event_id, channel_user_id, custom_event_nm, 
            detail_id_hex, event_nm, reserved_1_txt, reserved_2_txt, 
            visit_id, channel_nm, custom_event_group_nm, custom_events_sk, 
            detail_id, event_designed_id, event_key_cd, event_source_cd, 
            identity_id, mobile_app_id, session_id_hex, visit_id_hex
         ) VALUES (
            d.custom_revenue_amt, d.properties_map_doc, d.custom_event_dttm, 
            d.custom_event_dttm_tz, d.load_dttm, d.session_id, d.page_id, 
            d.event_type_nm, d.event_id, d.channel_user_id, d.custom_event_nm, 
            d.detail_id_hex, d.event_nm, d.reserved_1_txt, d.reserved_2_txt, 
            d.visit_id, d.channel_nm, d.custom_event_group_nm, d.custom_events_sk, 
            d.detail_id, d.event_designed_id, d.event_key_cd, d.event_source_cd, 
            d.identity_id, d.mobile_app_id, d.session_id_hex, d.visit_id_hex  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :CUSTOM_EVENTS_tmp , CUSTOM_EVENTS , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..CUSTOM_EVENTS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..CUSTOM_EVENTS_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CUSTOM_EVENTS;
         DROP TABLE work.CUSTOM_EVENTS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..CUSTOM_EVENTS_EXT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..CUSTOM_EVENTS_EXT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..CUSTOM_EVENTS_EXT_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=CUSTOM_EVENTS_EXT , table_keys=%str(CUSTOM_EVENTS_SK), out_table=work.CUSTOM_EVENTS_EXT );
   DATA work.CUSTOM_EVENTS_EXT_tmp /VIEW=work.CUSTOM_EVENTS_EXT_tmp ;
      SET work.CUSTOM_EVENTS_EXT ;
   RUN;
   %err_check (Failed to add time zone adaptation :CUSTOM_EVENTS_EXT_tmp , CUSTOM_EVENTS_EXT );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..CUSTOM_EVENTS_EXT_tmp ;
            SET work.CUSTOM_EVENTS_EXT_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.CUSTOM_EVENTS_EXT_tmp  BASE=&tmplib..CUSTOM_EVENTS_EXT_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..CUSTOM_EVENTS_EXT_tmp ;
            SET work.CUSTOM_EVENTS_EXT_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :CUSTOM_EVENTS_EXT_tmp , CUSTOM_EVENTS_EXT );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..CUSTOM_EVENTS_EXT AS b USING &tmpdbschema..CUSTOM_EVENTS_EXT_tmp AS d ON (
            b.custom_events_sk = d.custom_events_sk )
         WHEN MATCHED THEN
         UPDATE SET
            custom_revenue_amt = d.custom_revenue_amt, 
            load_dttm = d.load_dttm, event_designed_id = d.event_designed_id
         WHEN NOT MATCHED AND CUSTOM_EVENTS_SK IS NOT NULL THEN INSERT (
            custom_revenue_amt, load_dttm, event_designed_id, 
            custom_events_sk
         ) VALUES (
            d.custom_revenue_amt, d.load_dttm, d.event_designed_id, 
            d.custom_events_sk  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :CUSTOM_EVENTS_EXT_tmp , CUSTOM_EVENTS_EXT , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..CUSTOM_EVENTS_EXT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..CUSTOM_EVENTS_EXT_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CUSTOM_EVENTS_EXT;
         DROP TABLE work.CUSTOM_EVENTS_EXT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..DAILY_USAGE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..DAILY_USAGE_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DAILY_USAGE_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=DAILY_USAGE , table_keys=%str(EVENT_DAY), out_table=work.DAILY_USAGE );
   DATA work.DAILY_USAGE_tmp /VIEW=work.DAILY_USAGE_tmp ;
      SET work.DAILY_USAGE ;
   RUN;
   %err_check (Failed to add time zone adaptation :DAILY_USAGE_tmp , DAILY_USAGE );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..DAILY_USAGE_tmp ;
            SET work.DAILY_USAGE_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.DAILY_USAGE_tmp  BASE=&tmplib..DAILY_USAGE_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..DAILY_USAGE_tmp ;
            SET work.DAILY_USAGE_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :DAILY_USAGE_tmp , DAILY_USAGE );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..DAILY_USAGE AS b USING &tmpdbschema..DAILY_USAGE_tmp AS d ON (
            b.event_day = d.event_day )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_DAY IS NOT NULL THEN INSERT (
            bc_subjcnt_str, customer_profiles_processed_str, api_usage_str, 
            mob_impr_cnt, dm_destinations_total_row_cnt, google_ads_cnt, mob_sesn_cnt, 
            audience_usage_cnt, mobile_in_app_msg_cnt, mobile_push_cnt, email_preview_cnt, 
            facebook_ads_cnt, web_sesn_cnt, plan_users_cnt, outbound_api_cnt, 
            web_impr_cnt, email_send_cnt, linkedin_ads_cnt, dm_destinations_total_id_cnt, 
            asset_size, db_size, admin_user_cnt, event_day
         ) VALUES (
            d.bc_subjcnt_str, d.customer_profiles_processed_str, d.api_usage_str, 
            d.mob_impr_cnt, d.dm_destinations_total_row_cnt, d.google_ads_cnt, d.mob_sesn_cnt, 
            d.audience_usage_cnt, d.mobile_in_app_msg_cnt, d.mobile_push_cnt, d.email_preview_cnt, 
            d.facebook_ads_cnt, d.web_sesn_cnt, d.plan_users_cnt, d.outbound_api_cnt, 
            d.web_impr_cnt, d.email_send_cnt, d.linkedin_ads_cnt, d.dm_destinations_total_id_cnt, 
            d.asset_size, d.db_size, d.admin_user_cnt, d.event_day  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :DAILY_USAGE_tmp , DAILY_USAGE , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..DAILY_USAGE_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DAILY_USAGE_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DAILY_USAGE;
         DROP TABLE work.DAILY_USAGE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..DATA_VIEW_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..DATA_VIEW_DETAILS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DATA_VIEW_DETAILS_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=DATA_VIEW_DETAILS , table_keys=%str(EVENT_ID), out_table=work.DATA_VIEW_DETAILS );
   DATA work.DATA_VIEW_DETAILS_tmp /VIEW=work.DATA_VIEW_DETAILS_tmp ;
      SET work.DATA_VIEW_DETAILS ;
      IF data_view_dttm_tz  NE . THEN data_view_dttm_tz =tzoneu2s(data_view_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :DATA_VIEW_DETAILS_tmp , DATA_VIEW_DETAILS );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..DATA_VIEW_DETAILS_tmp ;
            SET work.DATA_VIEW_DETAILS_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.DATA_VIEW_DETAILS_tmp  BASE=&tmplib..DATA_VIEW_DETAILS_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..DATA_VIEW_DETAILS_tmp ;
            SET work.DATA_VIEW_DETAILS_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :DATA_VIEW_DETAILS_tmp , DATA_VIEW_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..DATA_VIEW_DETAILS AS b USING &tmpdbschema..DATA_VIEW_DETAILS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            total_cost_amt, properties_map_doc, data_view_dttm, 
            data_view_dttm_tz, load_dttm, visit_id, reserved_2_txt, 
            event_designed_id, channel_user_id, detail_id, event_nm, 
            session_id_hex, detail_id_hex, event_id, identity_id, 
            parent_event_designed_id, reserved_1_txt, session_id, visit_id_hex
         ) VALUES (
            d.total_cost_amt, d.properties_map_doc, d.data_view_dttm, 
            d.data_view_dttm_tz, d.load_dttm, d.visit_id, d.reserved_2_txt, 
            d.event_designed_id, d.channel_user_id, d.detail_id, d.event_nm, 
            d.session_id_hex, d.detail_id_hex, d.event_id, d.identity_id, 
            d.parent_event_designed_id, d.reserved_1_txt, d.session_id, d.visit_id_hex  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :DATA_VIEW_DETAILS_tmp , DATA_VIEW_DETAILS , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..DATA_VIEW_DETAILS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DATA_VIEW_DETAILS_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DATA_VIEW_DETAILS;
         DROP TABLE work.DATA_VIEW_DETAILS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..DBT_ADV_CAMPAIGN_VISITORS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..DBT_ADV_CAMPAIGN_VISITORS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_ADV_CAMPAIGN_VISITORS_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=DBT_ADV_CAMPAIGN_VISITORS , table_keys=%str(SESSION_ID,VISIT_ID), out_table=work.DBT_ADV_CAMPAIGN_VISITORS );
   DATA work.DBT_ADV_CAMPAIGN_VISITORS_tmp /VIEW=work.DBT_ADV_CAMPAIGN_VISITORS_tmp ;
      SET work.DBT_ADV_CAMPAIGN_VISITORS ;
      IF visit_dttm_tz  NE . THEN visit_dttm_tz =tzoneu2s(visit_dttm_tz ,&timeZone_Value.);
      IF session_start_dttm_tz  NE . THEN session_start_dttm_tz =tzoneu2s(session_start_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :DBT_ADV_CAMPAIGN_VISITORS_tmp , DBT_ADV_CAMPAIGN_VISITORS );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..DBT_ADV_CAMPAIGN_VISITORS_tmp ;
            SET work.DBT_ADV_CAMPAIGN_VISITORS_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.DBT_ADV_CAMPAIGN_VISITORS_tmp  BASE=&tmplib..DBT_ADV_CAMPAIGN_VISITORS_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..DBT_ADV_CAMPAIGN_VISITORS_tmp ;
            SET work.DBT_ADV_CAMPAIGN_VISITORS_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :DBT_ADV_CAMPAIGN_VISITORS_tmp , DBT_ADV_CAMPAIGN_VISITORS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..DBT_ADV_CAMPAIGN_VISITORS AS b USING &tmpdbschema..DBT_ADV_CAMPAIGN_VISITORS_tmp AS d ON (
            b.visit_id = d.visit_id AND 
            b.session_id = d.session_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND SESSION_ID IS NOT NULL AND VISIT_ID IS NOT NULL THEN INSERT (
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
         ) VALUES (
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
            d.session_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :DBT_ADV_CAMPAIGN_VISITORS_tmp , DBT_ADV_CAMPAIGN_VISITORS , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..DBT_ADV_CAMPAIGN_VISITORS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_ADV_CAMPAIGN_VISITORS_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DBT_ADV_CAMPAIGN_VISITORS;
         DROP TABLE work.DBT_ADV_CAMPAIGN_VISITORS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..DBT_BUSINESS_PROCESS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..DBT_BUSINESS_PROCESS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_BUSINESS_PROCESS_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=DBT_BUSINESS_PROCESS , table_keys=%str(BUSINESS_PROCESS_NAME,BUSINESS_PROCESS_STEP_NAME,BUS_PROCESS_STARTED_DTTM,SESSION_ID), out_table=work.DBT_BUSINESS_PROCESS );
   DATA work.DBT_BUSINESS_PROCESS_tmp /VIEW=work.DBT_BUSINESS_PROCESS_tmp ;
      SET work.DBT_BUSINESS_PROCESS ;
      IF bus_process_started_dttm_tz  NE . THEN bus_process_started_dttm_tz =tzoneu2s(bus_process_started_dttm_tz ,&timeZone_Value.);
      IF session_start_dttm_tz  NE . THEN session_start_dttm_tz =tzoneu2s(session_start_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :DBT_BUSINESS_PROCESS_tmp , DBT_BUSINESS_PROCESS );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..DBT_BUSINESS_PROCESS_tmp ;
            SET work.DBT_BUSINESS_PROCESS_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.DBT_BUSINESS_PROCESS_tmp  BASE=&tmplib..DBT_BUSINESS_PROCESS_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..DBT_BUSINESS_PROCESS_tmp ;
            SET work.DBT_BUSINESS_PROCESS_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :DBT_BUSINESS_PROCESS_tmp , DBT_BUSINESS_PROCESS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..DBT_BUSINESS_PROCESS AS b USING &tmpdbschema..DBT_BUSINESS_PROCESS_tmp AS d ON (
            b.bus_process_started_dttm = d.bus_process_started_dttm AND 
            b.session_id = d.session_id AND b.business_process_step_name = d.business_process_step_name AND 
            b.business_process_name = d.business_process_name )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND BUSINESS_PROCESS_NAME IS NOT NULL AND BUSINESS_PROCESS_STEP_NAME IS NOT NULL AND BUS_PROCESS_STARTED_DTTM IS NOT NULL AND SESSION_ID IS NOT NULL THEN INSERT (
            processes, steps_completed, step_count, 
            processes_completed, steps_abandoned, last_step, processes_abandoned, 
            steps, bus_process_started_dttm_tz, session_start_dttm_tz, session_start_dttm, 
            bus_process_started_dttm, session_complete_load_dttm, visitor_id, visit_origination_tracking_code, 
            visit_origination_name, visit_id, session_id, device_name, 
            cu_customer_id, business_process_step_name, business_process_attribute_2, bouncer, 
            business_process_attribute_1, business_process_name, device_type, visit_origination_creative, 
            visit_origination_placement, visit_origination_type, visitor_type
         ) VALUES (
            d.processes, d.steps_completed, d.step_count, 
            d.processes_completed, d.steps_abandoned, d.last_step, d.processes_abandoned, 
            d.steps, d.bus_process_started_dttm_tz, d.session_start_dttm_tz, d.session_start_dttm, 
            d.bus_process_started_dttm, d.session_complete_load_dttm, d.visitor_id, d.visit_origination_tracking_code, 
            d.visit_origination_name, d.visit_id, d.session_id, d.device_name, 
            d.cu_customer_id, d.business_process_step_name, d.business_process_attribute_2, d.bouncer, 
            d.business_process_attribute_1, d.business_process_name, d.device_type, d.visit_origination_creative, 
            d.visit_origination_placement, d.visit_origination_type, d.visitor_type  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :DBT_BUSINESS_PROCESS_tmp , DBT_BUSINESS_PROCESS , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..DBT_BUSINESS_PROCESS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_BUSINESS_PROCESS_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DBT_BUSINESS_PROCESS;
         DROP TABLE work.DBT_BUSINESS_PROCESS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..DBT_CONTENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..DBT_CONTENT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_CONTENT_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=DBT_CONTENT , table_keys=%str(DETAIL_ID), out_table=work.DBT_CONTENT );
   DATA work.DBT_CONTENT_tmp /VIEW=work.DBT_CONTENT_tmp ;
      SET work.DBT_CONTENT ;
      IF session_start_dttm_tz  NE . THEN session_start_dttm_tz =tzoneu2s(session_start_dttm_tz ,&timeZone_Value.);
      IF detail_dttm_tz  NE . THEN detail_dttm_tz =tzoneu2s(detail_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :DBT_CONTENT_tmp , DBT_CONTENT );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..DBT_CONTENT_tmp ;
            SET work.DBT_CONTENT_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.DBT_CONTENT_tmp  BASE=&tmplib..DBT_CONTENT_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..DBT_CONTENT_tmp ;
            SET work.DBT_CONTENT_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :DBT_CONTENT_tmp , DBT_CONTENT );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..DBT_CONTENT AS b USING &tmpdbschema..DBT_CONTENT_tmp AS d ON (
            b.detail_id = d.detail_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND DETAIL_ID IS NOT NULL THEN INSERT (
            total_page_view_time, entry_pages, active_page_view_time, 
            views, exit_pages, visits, bouncers, 
            session_start_dttm, session_complete_load_dttm, session_start_dttm_tz, detail_dttm_tz, 
            detail_dttm, visitor_type, visitor_id, visit_origination_type, 
            visit_origination_tracking_code, visit_origination_placement, visit_origination_name, visit_origination_creative, 
            visit_id, session_id, pg_page_url, pg_page, 
            pg_domain_name, device_type, device_name, detail_id, 
            cu_customer_id, class2_id, bouncer, class1_id
         ) VALUES (
            d.total_page_view_time, d.entry_pages, d.active_page_view_time, 
            d.views, d.exit_pages, d.visits, d.bouncers, 
            d.session_start_dttm, d.session_complete_load_dttm, d.session_start_dttm_tz, d.detail_dttm_tz, 
            d.detail_dttm, d.visitor_type, d.visitor_id, d.visit_origination_type, 
            d.visit_origination_tracking_code, d.visit_origination_placement, d.visit_origination_name, d.visit_origination_creative, 
            d.visit_id, d.session_id, d.pg_page_url, d.pg_page, 
            d.pg_domain_name, d.device_type, d.device_name, d.detail_id, 
            d.cu_customer_id, d.class2_id, d.bouncer, d.class1_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :DBT_CONTENT_tmp , DBT_CONTENT , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..DBT_CONTENT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_CONTENT_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DBT_CONTENT;
         DROP TABLE work.DBT_CONTENT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..DBT_DOCUMENTS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..DBT_DOCUMENTS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_DOCUMENTS_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=DBT_DOCUMENTS , table_keys=%str(DETAIL_ID), out_table=work.DBT_DOCUMENTS );
   DATA work.DBT_DOCUMENTS_tmp /VIEW=work.DBT_DOCUMENTS_tmp ;
      SET work.DBT_DOCUMENTS ;
      IF document_download_dttm_tz  NE . THEN document_download_dttm_tz =tzoneu2s(document_download_dttm_tz ,&timeZone_Value.);
      IF session_start_dttm_tz  NE . THEN session_start_dttm_tz =tzoneu2s(session_start_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :DBT_DOCUMENTS_tmp , DBT_DOCUMENTS );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..DBT_DOCUMENTS_tmp ;
            SET work.DBT_DOCUMENTS_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.DBT_DOCUMENTS_tmp  BASE=&tmplib..DBT_DOCUMENTS_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..DBT_DOCUMENTS_tmp ;
            SET work.DBT_DOCUMENTS_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :DBT_DOCUMENTS_tmp , DBT_DOCUMENTS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..DBT_DOCUMENTS AS b USING &tmpdbschema..DBT_DOCUMENTS_tmp AS d ON (
            b.detail_id = d.detail_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND DETAIL_ID IS NOT NULL THEN INSERT (
            document_downloads, document_download_dttm_tz, session_start_dttm_tz, 
            session_start_dttm, session_complete_load_dttm, document_download_dttm, visitor_type, 
            visitor_id, visit_origination_type, visit_origination_tracking_code, visit_origination_placement, 
            visit_origination_name, visit_origination_creative, visit_id, session_id, 
            do_page_url, do_page_description, device_type, device_name, 
            detail_id, cu_customer_id, class2_id, class1_id, 
            bouncer
         ) VALUES (
            d.document_downloads, d.document_download_dttm_tz, d.session_start_dttm_tz, 
            d.session_start_dttm, d.session_complete_load_dttm, d.document_download_dttm, d.visitor_type, 
            d.visitor_id, d.visit_origination_type, d.visit_origination_tracking_code, d.visit_origination_placement, 
            d.visit_origination_name, d.visit_origination_creative, d.visit_id, d.session_id, 
            d.do_page_url, d.do_page_description, d.device_type, d.device_name, 
            d.detail_id, d.cu_customer_id, d.class2_id, d.class1_id, 
            d.bouncer  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :DBT_DOCUMENTS_tmp , DBT_DOCUMENTS , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..DBT_DOCUMENTS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_DOCUMENTS_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DBT_DOCUMENTS;
         DROP TABLE work.DBT_DOCUMENTS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..DBT_ECOMMERCE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..DBT_ECOMMERCE_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_ECOMMERCE_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=DBT_ECOMMERCE , table_keys=%str(BASKET_ID,PRODUCT_ACTIVITY_DTTM,PRODUCT_ID,PRODUCT_NAME,PRODUCT_SKU,VISIT_ID), out_table=work.DBT_ECOMMERCE );
   DATA work.DBT_ECOMMERCE_tmp /VIEW=work.DBT_ECOMMERCE_tmp ;
      SET work.DBT_ECOMMERCE ;
      IF session_start_dttm_tz  NE . THEN session_start_dttm_tz =tzoneu2s(session_start_dttm_tz ,&timeZone_Value.);
      IF product_activity_dttm_tz  NE . THEN product_activity_dttm_tz =tzoneu2s(product_activity_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :DBT_ECOMMERCE_tmp , DBT_ECOMMERCE );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..DBT_ECOMMERCE_tmp ;
            SET work.DBT_ECOMMERCE_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.DBT_ECOMMERCE_tmp  BASE=&tmplib..DBT_ECOMMERCE_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..DBT_ECOMMERCE_tmp ;
            SET work.DBT_ECOMMERCE_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :DBT_ECOMMERCE_tmp , DBT_ECOMMERCE );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..DBT_ECOMMERCE AS b USING &tmpdbschema..DBT_ECOMMERCE_tmp AS d ON (
            b.product_activity_dttm = d.product_activity_dttm AND 
            b.visit_id = d.visit_id AND b.product_sku = d.product_sku AND 
            b.product_name = d.product_name AND b.product_id = d.product_id AND 
            b.basket_id = d.basket_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND BASKET_ID IS NOT NULL AND PRODUCT_ACTIVITY_DTTM IS NOT NULL AND PRODUCT_ID IS NOT NULL AND PRODUCT_NAME IS NOT NULL AND PRODUCT_SKU IS NOT NULL AND VISIT_ID IS NOT NULL THEN INSERT (
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
         ) VALUES (
            d.product_purchase_revenues, d.basket_adds_revenue, d.basket_removes_revenue, 
            d.product_views, d.basket_adds, d.basket_adds_units, d.product_purchases, 
            d.product_purchase_units, d.basket_removes_units, d.basket_removes, d.baskets_abandoned, 
            d.baskets_completed, d.baskets_started, d.session_complete_load_dttm, d.session_start_dttm_tz, 
            d.product_activity_dttm_tz, d.product_activity_dttm, d.session_start_dttm, d.visitor_type, 
            d.visitor_id, d.visit_origination_type, d.visit_origination_tracking_code, d.visit_origination_placement, 
            d.visit_origination_name, d.visit_origination_creative, d.visit_id, d.session_id, 
            d.product_sku, d.product_name, d.product_id, d.product_group_name, 
            d.device_type, d.device_name, d.cu_customer_id, d.bouncer, 
            d.basket_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :DBT_ECOMMERCE_tmp , DBT_ECOMMERCE , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..DBT_ECOMMERCE_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_ECOMMERCE_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DBT_ECOMMERCE;
         DROP TABLE work.DBT_ECOMMERCE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..DBT_FORMS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..DBT_FORMS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_FORMS_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=DBT_FORMS , table_keys=%str(DETAIL_ID), out_table=work.DBT_FORMS );
   DATA work.DBT_FORMS_tmp /VIEW=work.DBT_FORMS_tmp ;
      SET work.DBT_FORMS ;
      IF form_attempt_dttm_tz  NE . THEN form_attempt_dttm_tz =tzoneu2s(form_attempt_dttm_tz ,&timeZone_Value.);
      IF session_start_dttm_tz  NE . THEN session_start_dttm_tz =tzoneu2s(session_start_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :DBT_FORMS_tmp , DBT_FORMS );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..DBT_FORMS_tmp ;
            SET work.DBT_FORMS_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.DBT_FORMS_tmp  BASE=&tmplib..DBT_FORMS_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..DBT_FORMS_tmp ;
            SET work.DBT_FORMS_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :DBT_FORMS_tmp , DBT_FORMS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..DBT_FORMS AS b USING &tmpdbschema..DBT_FORMS_tmp AS d ON (
            b.detail_id = d.detail_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND DETAIL_ID IS NOT NULL THEN INSERT (
            attempts, forms_completed, forms_not_submitted, 
            forms_started, form_attempt_dttm, session_start_dttm, form_attempt_dttm_tz, 
            session_complete_load_dttm, session_start_dttm_tz, visitor_type, visitor_id, 
            visit_origination_type, visit_origination_tracking_code, visit_origination_placement, visit_origination_name, 
            visit_origination_creative, visit_id, session_id, last_field, 
            form_nm, device_type, device_name, detail_id, 
            cu_customer_id, bouncer
         ) VALUES (
            d.attempts, d.forms_completed, d.forms_not_submitted, 
            d.forms_started, d.form_attempt_dttm, d.session_start_dttm, d.form_attempt_dttm_tz, 
            d.session_complete_load_dttm, d.session_start_dttm_tz, d.visitor_type, d.visitor_id, 
            d.visit_origination_type, d.visit_origination_tracking_code, d.visit_origination_placement, d.visit_origination_name, 
            d.visit_origination_creative, d.visit_id, d.session_id, d.last_field, 
            d.form_nm, d.device_type, d.device_name, d.detail_id, 
            d.cu_customer_id, d.bouncer  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :DBT_FORMS_tmp , DBT_FORMS , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..DBT_FORMS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_FORMS_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DBT_FORMS;
         DROP TABLE work.DBT_FORMS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..DBT_GOALS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..DBT_GOALS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_GOALS_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=DBT_GOALS , table_keys=%str(DETAIL_ID,GOAL_GROUP_NAME,GOAL_NAME), out_table=work.DBT_GOALS );
   DATA work.DBT_GOALS_tmp /VIEW=work.DBT_GOALS_tmp ;
      SET work.DBT_GOALS ;
      IF goal_reached_dttm_tz  NE . THEN goal_reached_dttm_tz =tzoneu2s(goal_reached_dttm_tz ,&timeZone_Value.);
      IF session_start_dttm_tz  NE . THEN session_start_dttm_tz =tzoneu2s(session_start_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :DBT_GOALS_tmp , DBT_GOALS );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..DBT_GOALS_tmp ;
            SET work.DBT_GOALS_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.DBT_GOALS_tmp  BASE=&tmplib..DBT_GOALS_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..DBT_GOALS_tmp ;
            SET work.DBT_GOALS_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :DBT_GOALS_tmp , DBT_GOALS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..DBT_GOALS AS b USING &tmpdbschema..DBT_GOALS_tmp AS d ON (
            b.goal_name = d.goal_name AND 
            b.goal_group_name = d.goal_group_name AND b.detail_id = d.detail_id )
         WHEN MATCHED THEN
         UPDATE SET
            goal_revenue = d.goal_revenue, 
            visits = d.visits, session_start_dttm = d.session_start_dttm, 
            goal_reached_dttm_tz = d.goal_reached_dttm_tz, goal_reached_dttm = d.goal_reached_dttm, 
            session_complete_load_dttm = d.session_complete_load_dttm, session_start_dttm_tz = d.session_start_dttm_tz, 
            goals = d.goals, visitor_type = d.visitor_type, 
            visitor_id = d.visitor_id, visit_origination_type = d.visit_origination_type, 
            visit_origination_tracking_code = d.visit_origination_tracking_code, visit_origination_placement = d.visit_origination_placement, 
            visit_origination_name = d.visit_origination_name, visit_origination_creative = d.visit_origination_creative, 
            visit_id = d.visit_id, session_id = d.session_id, 
            device_type = d.device_type, device_name = d.device_name, 
            cu_customer_id = d.cu_customer_id, bouncer = d.bouncer
         WHEN NOT MATCHED AND DETAIL_ID IS NOT NULL AND GOAL_GROUP_NAME IS NOT NULL AND GOAL_NAME IS NOT NULL THEN INSERT (
            goal_revenue, visits, session_start_dttm, 
            goal_reached_dttm_tz, goal_reached_dttm, session_complete_load_dttm, session_start_dttm_tz, 
            goals, visitor_type, visitor_id, visit_origination_type, 
            visit_origination_tracking_code, visit_origination_placement, visit_origination_name, visit_origination_creative, 
            visit_id, session_id, goal_name, goal_group_name, 
            device_type, device_name, detail_id, cu_customer_id, 
            bouncer
         ) VALUES (
            d.goal_revenue, d.visits, d.session_start_dttm, 
            d.goal_reached_dttm_tz, d.goal_reached_dttm, d.session_complete_load_dttm, d.session_start_dttm_tz, 
            d.goals, d.visitor_type, d.visitor_id, d.visit_origination_type, 
            d.visit_origination_tracking_code, d.visit_origination_placement, d.visit_origination_name, d.visit_origination_creative, 
            d.visit_id, d.session_id, d.goal_name, d.goal_group_name, 
            d.device_type, d.device_name, d.detail_id, d.cu_customer_id, 
            d.bouncer  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :DBT_GOALS_tmp , DBT_GOALS , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..DBT_GOALS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_GOALS_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DBT_GOALS;
         DROP TABLE work.DBT_GOALS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..DBT_MEDIA_CONSUMPTION));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..DBT_MEDIA_CONSUMPTION_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_MEDIA_CONSUMPTION_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=DBT_MEDIA_CONSUMPTION , table_keys=%str(DETAIL_ID,INTERACTIONS_COUNT,MAXIMUM_PROGRESS,MEDIA_COMPLETION_RATE,MEDIA_SECTION,VISIT_ID), out_table=work.DBT_MEDIA_CONSUMPTION );
   DATA work.DBT_MEDIA_CONSUMPTION_tmp /VIEW=work.DBT_MEDIA_CONSUMPTION_tmp ;
      SET work.DBT_MEDIA_CONSUMPTION ;
      IF session_start_dttm_tz  NE . THEN session_start_dttm_tz =tzoneu2s(session_start_dttm_tz ,&timeZone_Value.);
      IF media_start_dttm_tz  NE . THEN media_start_dttm_tz =tzoneu2s(media_start_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :DBT_MEDIA_CONSUMPTION_tmp , DBT_MEDIA_CONSUMPTION );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..DBT_MEDIA_CONSUMPTION_tmp ;
            SET work.DBT_MEDIA_CONSUMPTION_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.DBT_MEDIA_CONSUMPTION_tmp  BASE=&tmplib..DBT_MEDIA_CONSUMPTION_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..DBT_MEDIA_CONSUMPTION_tmp ;
            SET work.DBT_MEDIA_CONSUMPTION_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :DBT_MEDIA_CONSUMPTION_tmp , DBT_MEDIA_CONSUMPTION );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..DBT_MEDIA_CONSUMPTION AS b USING &tmpdbschema..DBT_MEDIA_CONSUMPTION_tmp AS d ON (
            b.maximum_progress = d.maximum_progress AND 
            b.interactions_count = d.interactions_count AND b.visit_id = d.visit_id AND 
            b.media_section = d.media_section AND b.media_completion_rate = d.media_completion_rate AND 
            b.detail_id = d.detail_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND DETAIL_ID IS NOT NULL AND INTERACTIONS_COUNT IS NOT NULL AND MAXIMUM_PROGRESS IS NOT NULL AND MEDIA_COMPLETION_RATE IS NOT NULL AND MEDIA_SECTION IS NOT NULL AND VISIT_ID IS NOT NULL THEN INSERT (
            time_viewing, duration, maximum_progress, 
            content_viewed, counter, interactions_count, session_start_dttm_tz, 
            session_start_dttm, session_complete_load_dttm, media_start_dttm, media_start_dttm_tz, 
            views_started, views_completed, views, media_section_view, 
            visitor_type, visitor_id, visit_origination_type, visit_origination_tracking_code, 
            visit_origination_placement, visit_origination_name, visit_origination_creative, visit_id, 
            session_id, media_uri_txt, media_section, media_name, 
            media_completion_rate, device_type, device_name, detail_id, 
            cu_customer_id, bouncer
         ) VALUES (
            d.time_viewing, d.duration, d.maximum_progress, 
            d.content_viewed, d.counter, d.interactions_count, d.session_start_dttm_tz, 
            d.session_start_dttm, d.session_complete_load_dttm, d.media_start_dttm, d.media_start_dttm_tz, 
            d.views_started, d.views_completed, d.views, d.media_section_view, 
            d.visitor_type, d.visitor_id, d.visit_origination_type, d.visit_origination_tracking_code, 
            d.visit_origination_placement, d.visit_origination_name, d.visit_origination_creative, d.visit_id, 
            d.session_id, d.media_uri_txt, d.media_section, d.media_name, 
            d.media_completion_rate, d.device_type, d.device_name, d.detail_id, 
            d.cu_customer_id, d.bouncer  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :DBT_MEDIA_CONSUMPTION_tmp , DBT_MEDIA_CONSUMPTION , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..DBT_MEDIA_CONSUMPTION_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_MEDIA_CONSUMPTION_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DBT_MEDIA_CONSUMPTION;
         DROP TABLE work.DBT_MEDIA_CONSUMPTION;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..DBT_PROMOTIONS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..DBT_PROMOTIONS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_PROMOTIONS_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=DBT_PROMOTIONS , table_keys=%str(DETAIL_ID), out_table=work.DBT_PROMOTIONS );
   DATA work.DBT_PROMOTIONS_tmp /VIEW=work.DBT_PROMOTIONS_tmp ;
      SET work.DBT_PROMOTIONS ;
      IF session_start_dttm_tz  NE . THEN session_start_dttm_tz =tzoneu2s(session_start_dttm_tz ,&timeZone_Value.);
      IF promotion_shown_dttm_tz  NE . THEN promotion_shown_dttm_tz =tzoneu2s(promotion_shown_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :DBT_PROMOTIONS_tmp , DBT_PROMOTIONS );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..DBT_PROMOTIONS_tmp ;
            SET work.DBT_PROMOTIONS_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.DBT_PROMOTIONS_tmp  BASE=&tmplib..DBT_PROMOTIONS_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..DBT_PROMOTIONS_tmp ;
            SET work.DBT_PROMOTIONS_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :DBT_PROMOTIONS_tmp , DBT_PROMOTIONS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..DBT_PROMOTIONS AS b USING &tmpdbschema..DBT_PROMOTIONS_tmp AS d ON (
            b.detail_id = d.detail_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND DETAIL_ID IS NOT NULL THEN INSERT (
            click_throughs, displays, session_start_dttm_tz, 
            promotion_shown_dttm_tz, promotion_shown_dttm, session_complete_load_dttm, session_start_dttm, 
            visitor_type, visitor_id, visit_origination_type, visit_origination_tracking_code, 
            visit_origination_placement, visit_origination_name, visit_origination_creative, visit_id, 
            session_id, promotion_type, promotion_tracking_code, promotion_placement, 
            promotion_name, promotion_creative, device_type, device_name, 
            detail_id, cu_customer_id, bouncer
         ) VALUES (
            d.click_throughs, d.displays, d.session_start_dttm_tz, 
            d.promotion_shown_dttm_tz, d.promotion_shown_dttm, d.session_complete_load_dttm, d.session_start_dttm, 
            d.visitor_type, d.visitor_id, d.visit_origination_type, d.visit_origination_tracking_code, 
            d.visit_origination_placement, d.visit_origination_name, d.visit_origination_creative, d.visit_id, 
            d.session_id, d.promotion_type, d.promotion_tracking_code, d.promotion_placement, 
            d.promotion_name, d.promotion_creative, d.device_type, d.device_name, 
            d.detail_id, d.cu_customer_id, d.bouncer  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :DBT_PROMOTIONS_tmp , DBT_PROMOTIONS , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..DBT_PROMOTIONS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_PROMOTIONS_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DBT_PROMOTIONS;
         DROP TABLE work.DBT_PROMOTIONS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..DBT_SEARCH));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..DBT_SEARCH_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_SEARCH_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=DBT_SEARCH , table_keys=%str(DETAIL_ID), out_table=work.DBT_SEARCH );
   DATA work.DBT_SEARCH_tmp /VIEW=work.DBT_SEARCH_tmp ;
      SET work.DBT_SEARCH ;
      IF search_results_dttm_tz  NE . THEN search_results_dttm_tz =tzoneu2s(search_results_dttm_tz ,&timeZone_Value.);
      IF session_start_dttm_tz  NE . THEN session_start_dttm_tz =tzoneu2s(session_start_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :DBT_SEARCH_tmp , DBT_SEARCH );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..DBT_SEARCH_tmp ;
            SET work.DBT_SEARCH_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.DBT_SEARCH_tmp  BASE=&tmplib..DBT_SEARCH_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..DBT_SEARCH_tmp ;
            SET work.DBT_SEARCH_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :DBT_SEARCH_tmp , DBT_SEARCH );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..DBT_SEARCH AS b USING &tmpdbschema..DBT_SEARCH_tmp AS d ON (
            b.detail_id = d.detail_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND DETAIL_ID IS NOT NULL THEN INSERT (
            num_additional_searches, num_pages_viewed_afterwards, searches, 
            visits, search_unknown_results, search_returned_results, exit_pages, 
            search_no_results_returned, search_results_dttm_tz, session_start_dttm, session_start_dttm_tz, 
            session_complete_load_dttm, search_results_dttm, visitor_type, visitor_id, 
            visit_origination_type, visit_origination_tracking_code, visit_origination_placement, visit_origination_name, 
            visit_origination_creative, visit_id, session_id, search_name, 
            internal_search_term, device_type, device_name, detail_id, 
            cu_customer_id, bouncer
         ) VALUES (
            d.num_additional_searches, d.num_pages_viewed_afterwards, d.searches, 
            d.visits, d.search_unknown_results, d.search_returned_results, d.exit_pages, 
            d.search_no_results_returned, d.search_results_dttm_tz, d.session_start_dttm, d.session_start_dttm_tz, 
            d.session_complete_load_dttm, d.search_results_dttm, d.visitor_type, d.visitor_id, 
            d.visit_origination_type, d.visit_origination_tracking_code, d.visit_origination_placement, d.visit_origination_name, 
            d.visit_origination_creative, d.visit_id, d.session_id, d.search_name, 
            d.internal_search_term, d.device_type, d.device_name, d.detail_id, 
            d.cu_customer_id, d.bouncer  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :DBT_SEARCH_tmp , DBT_SEARCH , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..DBT_SEARCH_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_SEARCH_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DBT_SEARCH;
         DROP TABLE work.DBT_SEARCH;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..DIRECT_CONTACT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..DIRECT_CONTACT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DIRECT_CONTACT_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=DIRECT_CONTACT , table_keys=%str(EVENT_ID), out_table=work.DIRECT_CONTACT );
   DATA work.DIRECT_CONTACT_tmp /VIEW=work.DIRECT_CONTACT_tmp ;
      SET work.DIRECT_CONTACT ;
      IF direct_contact_dttm_tz  NE . THEN direct_contact_dttm_tz =tzoneu2s(direct_contact_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :DIRECT_CONTACT_tmp , DIRECT_CONTACT );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..DIRECT_CONTACT_tmp ;
            SET work.DIRECT_CONTACT_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.DIRECT_CONTACT_tmp  BASE=&tmplib..DIRECT_CONTACT_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..DIRECT_CONTACT_tmp ;
            SET work.DIRECT_CONTACT_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :DIRECT_CONTACT_tmp , DIRECT_CONTACT );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..DIRECT_CONTACT AS b USING &tmpdbschema..DIRECT_CONTACT_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            control_active_flg, control_group_flg, properties_map_doc, 
            load_dttm, direct_contact_dttm, direct_contact_dttm_tz, task_version_id, 
            task_id, segment_id, response_tracking_cd, occurrence_id, 
            message_id, identity_type_nm, identity_id, event_nm, 
            event_id, event_designed_id, context_val, context_type_nm, 
            channel_user_id, channel_nm
         ) VALUES (
            d.control_active_flg, d.control_group_flg, d.properties_map_doc, 
            d.load_dttm, d.direct_contact_dttm, d.direct_contact_dttm_tz, d.task_version_id, 
            d.task_id, d.segment_id, d.response_tracking_cd, d.occurrence_id, 
            d.message_id, d.identity_type_nm, d.identity_id, d.event_nm, 
            d.event_id, d.event_designed_id, d.context_val, d.context_type_nm, 
            d.channel_user_id, d.channel_nm  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :DIRECT_CONTACT_tmp , DIRECT_CONTACT , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..DIRECT_CONTACT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DIRECT_CONTACT_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DIRECT_CONTACT;
         DROP TABLE work.DIRECT_CONTACT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..DOCUMENT_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..DOCUMENT_DETAILS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DOCUMENT_DETAILS_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=DOCUMENT_DETAILS , table_keys=%str(EVENT_ID), out_table=work.DOCUMENT_DETAILS );
   DATA work.DOCUMENT_DETAILS_tmp /VIEW=work.DOCUMENT_DETAILS_tmp ;
      SET work.DOCUMENT_DETAILS ;
      IF link_event_dttm_tz  NE . THEN link_event_dttm_tz =tzoneu2s(link_event_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :DOCUMENT_DETAILS_tmp , DOCUMENT_DETAILS );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..DOCUMENT_DETAILS_tmp ;
            SET work.DOCUMENT_DETAILS_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.DOCUMENT_DETAILS_tmp  BASE=&tmplib..DOCUMENT_DETAILS_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..DOCUMENT_DETAILS_tmp ;
            SET work.DOCUMENT_DETAILS_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :DOCUMENT_DETAILS_tmp , DOCUMENT_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..DOCUMENT_DETAILS AS b USING &tmpdbschema..DOCUMENT_DETAILS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            load_dttm = d.load_dttm, 
            link_event_dttm = d.link_event_dttm, link_event_dttm_tz = d.link_event_dttm_tz, 
            visit_id_hex = d.visit_id_hex, uri_txt = d.uri_txt, 
            session_id = d.session_id, link_selector_path = d.link_selector_path, 
            link_id = d.link_id, link_name = d.link_name, 
            identity_id = d.identity_id, event_source_cd = d.event_source_cd, 
            session_id_hex = d.session_id_hex, event_key_cd = d.event_key_cd, 
            visit_id = d.visit_id, detail_id_hex = d.detail_id_hex, 
            detail_id = d.detail_id, alt_txt = d.alt_txt
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            load_dttm, link_event_dttm, link_event_dttm_tz, 
            visit_id_hex, uri_txt, session_id, link_selector_path, 
            link_id, link_name, identity_id, event_source_cd, 
            session_id_hex, event_key_cd, visit_id, event_id, 
            detail_id_hex, detail_id, alt_txt
         ) VALUES (
            d.load_dttm, d.link_event_dttm, d.link_event_dttm_tz, 
            d.visit_id_hex, d.uri_txt, d.session_id, d.link_selector_path, 
            d.link_id, d.link_name, d.identity_id, d.event_source_cd, 
            d.session_id_hex, d.event_key_cd, d.visit_id, d.event_id, 
            d.detail_id_hex, d.detail_id, d.alt_txt  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :DOCUMENT_DETAILS_tmp , DOCUMENT_DETAILS , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..DOCUMENT_DETAILS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DOCUMENT_DETAILS_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DOCUMENT_DETAILS;
         DROP TABLE work.DOCUMENT_DETAILS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..EMAIL_BOUNCE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..EMAIL_BOUNCE_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EMAIL_BOUNCE_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=EMAIL_BOUNCE , table_keys=%str(EVENT_ID), out_table=work.EMAIL_BOUNCE );
   DATA work.EMAIL_BOUNCE_tmp /VIEW=work.EMAIL_BOUNCE_tmp ;
      SET work.EMAIL_BOUNCE ;
      IF email_bounce_dttm_tz  NE . THEN email_bounce_dttm_tz =tzoneu2s(email_bounce_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :EMAIL_BOUNCE_tmp , EMAIL_BOUNCE );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..EMAIL_BOUNCE_tmp ;
            SET work.EMAIL_BOUNCE_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.EMAIL_BOUNCE_tmp  BASE=&tmplib..EMAIL_BOUNCE_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..EMAIL_BOUNCE_tmp ;
            SET work.EMAIL_BOUNCE_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :EMAIL_BOUNCE_tmp , EMAIL_BOUNCE );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..EMAIL_BOUNCE AS b USING &tmpdbschema..EMAIL_BOUNCE_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            test_flg, properties_map_doc, load_dttm, 
            email_bounce_dttm_tz, email_bounce_dttm, task_id, subject_line_txt, 
            segment_version_id, response_tracking_cd, reason_txt, raw_reason_txt, 
            occurrence_id, journey_occurrence_id, imprint_id, event_nm, 
            event_designed_id, context_type_nm, bounce_class_cd, aud_occurrence_id, 
            analysis_group_id, audience_id, channel_user_id, context_val, 
            event_id, identity_id, journey_id, program_id, 
            recipient_domain_nm, segment_id, task_version_id
         ) VALUES (
            d.test_flg, d.properties_map_doc, d.load_dttm, 
            d.email_bounce_dttm_tz, d.email_bounce_dttm, d.task_id, d.subject_line_txt, 
            d.segment_version_id, d.response_tracking_cd, d.reason_txt, d.raw_reason_txt, 
            d.occurrence_id, d.journey_occurrence_id, d.imprint_id, d.event_nm, 
            d.event_designed_id, d.context_type_nm, d.bounce_class_cd, d.aud_occurrence_id, 
            d.analysis_group_id, d.audience_id, d.channel_user_id, d.context_val, 
            d.event_id, d.identity_id, d.journey_id, d.program_id, 
            d.recipient_domain_nm, d.segment_id, d.task_version_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :EMAIL_BOUNCE_tmp , EMAIL_BOUNCE , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..EMAIL_BOUNCE_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EMAIL_BOUNCE_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..EMAIL_BOUNCE;
         DROP TABLE work.EMAIL_BOUNCE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..EMAIL_CLICK));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..EMAIL_CLICK_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EMAIL_CLICK_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=EMAIL_CLICK , table_keys=%str(EVENT_ID), out_table=work.EMAIL_CLICK );
   DATA work.EMAIL_CLICK_tmp /VIEW=work.EMAIL_CLICK_tmp ;
      SET work.EMAIL_CLICK ;
      IF email_click_dttm_tz  NE . THEN email_click_dttm_tz =tzoneu2s(email_click_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :EMAIL_CLICK_tmp , EMAIL_CLICK );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..EMAIL_CLICK_tmp ;
            SET work.EMAIL_CLICK_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.EMAIL_CLICK_tmp  BASE=&tmplib..EMAIL_CLICK_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..EMAIL_CLICK_tmp ;
            SET work.EMAIL_CLICK_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :EMAIL_CLICK_tmp , EMAIL_CLICK );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..EMAIL_CLICK AS b USING &tmpdbschema..EMAIL_CLICK_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
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
         ) VALUES (
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
            d.segment_version_id, d.user_agent_nm  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :EMAIL_CLICK_tmp , EMAIL_CLICK , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..EMAIL_CLICK_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EMAIL_CLICK_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..EMAIL_CLICK;
         DROP TABLE work.EMAIL_CLICK;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..EMAIL_COMPLAINT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..EMAIL_COMPLAINT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EMAIL_COMPLAINT_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=EMAIL_COMPLAINT , table_keys=%str(EVENT_ID), out_table=work.EMAIL_COMPLAINT );
   DATA work.EMAIL_COMPLAINT_tmp /VIEW=work.EMAIL_COMPLAINT_tmp ;
      SET work.EMAIL_COMPLAINT ;
      IF email_complaint_dttm_tz  NE . THEN email_complaint_dttm_tz =tzoneu2s(email_complaint_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :EMAIL_COMPLAINT_tmp , EMAIL_COMPLAINT );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..EMAIL_COMPLAINT_tmp ;
            SET work.EMAIL_COMPLAINT_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.EMAIL_COMPLAINT_tmp  BASE=&tmplib..EMAIL_COMPLAINT_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..EMAIL_COMPLAINT_tmp ;
            SET work.EMAIL_COMPLAINT_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :EMAIL_COMPLAINT_tmp , EMAIL_COMPLAINT );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..EMAIL_COMPLAINT AS b USING &tmpdbschema..EMAIL_COMPLAINT_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            test_flg, properties_map_doc, load_dttm, 
            email_complaint_dttm, email_complaint_dttm_tz, task_id, segment_version_id, 
            response_tracking_cd, recipient_domain_nm, occurrence_id, journey_occurrence_id, 
            imprint_id, event_nm, event_id, event_designed_id, 
            context_type_nm, audience_id, analysis_group_id, aud_occurrence_id, 
            channel_user_id, context_val, identity_id, journey_id, 
            program_id, segment_id, subject_line_txt, task_version_id
         ) VALUES (
            d.test_flg, d.properties_map_doc, d.load_dttm, 
            d.email_complaint_dttm, d.email_complaint_dttm_tz, d.task_id, d.segment_version_id, 
            d.response_tracking_cd, d.recipient_domain_nm, d.occurrence_id, d.journey_occurrence_id, 
            d.imprint_id, d.event_nm, d.event_id, d.event_designed_id, 
            d.context_type_nm, d.audience_id, d.analysis_group_id, d.aud_occurrence_id, 
            d.channel_user_id, d.context_val, d.identity_id, d.journey_id, 
            d.program_id, d.segment_id, d.subject_line_txt, d.task_version_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :EMAIL_COMPLAINT_tmp , EMAIL_COMPLAINT , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..EMAIL_COMPLAINT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EMAIL_COMPLAINT_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..EMAIL_COMPLAINT;
         DROP TABLE work.EMAIL_COMPLAINT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..EMAIL_OPEN));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..EMAIL_OPEN_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EMAIL_OPEN_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=EMAIL_OPEN , table_keys=%str(EVENT_ID), out_table=work.EMAIL_OPEN );
   DATA work.EMAIL_OPEN_tmp /VIEW=work.EMAIL_OPEN_tmp ;
      SET work.EMAIL_OPEN ;
      IF email_open_dttm_tz  NE . THEN email_open_dttm_tz =tzoneu2s(email_open_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :EMAIL_OPEN_tmp , EMAIL_OPEN );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..EMAIL_OPEN_tmp ;
            SET work.EMAIL_OPEN_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.EMAIL_OPEN_tmp  BASE=&tmplib..EMAIL_OPEN_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..EMAIL_OPEN_tmp ;
            SET work.EMAIL_OPEN_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :EMAIL_OPEN_tmp , EMAIL_OPEN );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..EMAIL_OPEN AS b USING &tmpdbschema..EMAIL_OPEN_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
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
         ) VALUES (
            d.prefetched_flg, d.click_tracking_flg, d.open_tracking_flg, 
            d.is_mobile_flg, d.test_flg, d.properties_map_doc, d.email_open_dttm, 
            d.email_open_dttm_tz, d.load_dttm, d.user_agent_nm, d.task_version_id, 
            d.subject_line_txt, d.segment_version_id, d.segment_id, d.recipient_domain_nm, 
            d.program_id, d.platform_version, d.occurrence_id, d.manufacturer_nm, 
            d.journey_id, d.imprint_id, d.event_nm, d.event_designed_id, 
            d.context_val, d.audience_id, d.analysis_group_id, d.agent_family_nm, 
            d.aud_occurrence_id, d.channel_user_id, d.context_type_nm, d.device_nm, 
            d.event_id, d.identity_id, d.journey_occurrence_id, d.mailbox_provider_nm, 
            d.platform_desc, d.response_tracking_cd, d.task_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :EMAIL_OPEN_tmp , EMAIL_OPEN , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..EMAIL_OPEN_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EMAIL_OPEN_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..EMAIL_OPEN;
         DROP TABLE work.EMAIL_OPEN;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..EMAIL_OPTOUT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..EMAIL_OPTOUT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EMAIL_OPTOUT_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=EMAIL_OPTOUT , table_keys=%str(EVENT_ID), out_table=work.EMAIL_OPTOUT );
   DATA work.EMAIL_OPTOUT_tmp /VIEW=work.EMAIL_OPTOUT_tmp ;
      SET work.EMAIL_OPTOUT ;
      IF email_optout_dttm_tz  NE . THEN email_optout_dttm_tz =tzoneu2s(email_optout_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :EMAIL_OPTOUT_tmp , EMAIL_OPTOUT );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..EMAIL_OPTOUT_tmp ;
            SET work.EMAIL_OPTOUT_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.EMAIL_OPTOUT_tmp  BASE=&tmplib..EMAIL_OPTOUT_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..EMAIL_OPTOUT_tmp ;
            SET work.EMAIL_OPTOUT_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :EMAIL_OPTOUT_tmp , EMAIL_OPTOUT );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..EMAIL_OPTOUT AS b USING &tmpdbschema..EMAIL_OPTOUT_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            test_flg, properties_map_doc, email_optout_dttm_tz, 
            email_optout_dttm, load_dttm, task_version_id, subject_line_txt, 
            segment_id, recipient_domain_nm, program_id, optout_type_nm, 
            occurrence_id, link_tracking_label_txt, link_tracking_group_txt, journey_id, 
            identity_id, event_nm, event_id, context_val, 
            channel_user_id, audience_id, aud_occurrence_id, analysis_group_id, 
            context_type_nm, event_designed_id, imprint_id, journey_occurrence_id, 
            link_tracking_id, response_tracking_cd, segment_version_id, task_id
         ) VALUES (
            d.test_flg, d.properties_map_doc, d.email_optout_dttm_tz, 
            d.email_optout_dttm, d.load_dttm, d.task_version_id, d.subject_line_txt, 
            d.segment_id, d.recipient_domain_nm, d.program_id, d.optout_type_nm, 
            d.occurrence_id, d.link_tracking_label_txt, d.link_tracking_group_txt, d.journey_id, 
            d.identity_id, d.event_nm, d.event_id, d.context_val, 
            d.channel_user_id, d.audience_id, d.aud_occurrence_id, d.analysis_group_id, 
            d.context_type_nm, d.event_designed_id, d.imprint_id, d.journey_occurrence_id, 
            d.link_tracking_id, d.response_tracking_cd, d.segment_version_id, d.task_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :EMAIL_OPTOUT_tmp , EMAIL_OPTOUT , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..EMAIL_OPTOUT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EMAIL_OPTOUT_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..EMAIL_OPTOUT;
         DROP TABLE work.EMAIL_OPTOUT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..EMAIL_OPTOUT_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..EMAIL_OPTOUT_DETAILS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EMAIL_OPTOUT_DETAILS_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=EMAIL_OPTOUT_DETAILS , table_keys=%str(EVENT_ID), out_table=work.EMAIL_OPTOUT_DETAILS );
   DATA work.EMAIL_OPTOUT_DETAILS_tmp /VIEW=work.EMAIL_OPTOUT_DETAILS_tmp ;
      SET work.EMAIL_OPTOUT_DETAILS ;
      IF email_action_dttm_tz  NE . THEN email_action_dttm_tz =tzoneu2s(email_action_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :EMAIL_OPTOUT_DETAILS_tmp , EMAIL_OPTOUT_DETAILS );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..EMAIL_OPTOUT_DETAILS_tmp ;
            SET work.EMAIL_OPTOUT_DETAILS_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.EMAIL_OPTOUT_DETAILS_tmp  BASE=&tmplib..EMAIL_OPTOUT_DETAILS_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..EMAIL_OPTOUT_DETAILS_tmp ;
            SET work.EMAIL_OPTOUT_DETAILS_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :EMAIL_OPTOUT_DETAILS_tmp , EMAIL_OPTOUT_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..EMAIL_OPTOUT_DETAILS AS b USING &tmpdbschema..EMAIL_OPTOUT_DETAILS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            test_flg, properties_map_doc, email_action_dttm_tz, 
            email_action_dttm, load_dttm, task_version_id, subject_line_txt, 
            segment_id, recipient_domain_nm, program_id, optout_type_nm, 
            occurrence_id, journey_occurrence_id, imprint_id, event_nm, 
            event_designed_id, email_address, context_val, audience_id, 
            analysis_group_id, aud_occurrence_id, context_type_nm, event_id, 
            identity_id, journey_id, response_tracking_cd, segment_version_id, 
            task_id
         ) VALUES (
            d.test_flg, d.properties_map_doc, d.email_action_dttm_tz, 
            d.email_action_dttm, d.load_dttm, d.task_version_id, d.subject_line_txt, 
            d.segment_id, d.recipient_domain_nm, d.program_id, d.optout_type_nm, 
            d.occurrence_id, d.journey_occurrence_id, d.imprint_id, d.event_nm, 
            d.event_designed_id, d.email_address, d.context_val, d.audience_id, 
            d.analysis_group_id, d.aud_occurrence_id, d.context_type_nm, d.event_id, 
            d.identity_id, d.journey_id, d.response_tracking_cd, d.segment_version_id, 
            d.task_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :EMAIL_OPTOUT_DETAILS_tmp , EMAIL_OPTOUT_DETAILS , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..EMAIL_OPTOUT_DETAILS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EMAIL_OPTOUT_DETAILS_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..EMAIL_OPTOUT_DETAILS;
         DROP TABLE work.EMAIL_OPTOUT_DETAILS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..EMAIL_REPLY));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..EMAIL_REPLY_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EMAIL_REPLY_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=EMAIL_REPLY , table_keys=%str(EVENT_ID), out_table=work.EMAIL_REPLY );
   DATA work.EMAIL_REPLY_tmp /VIEW=work.EMAIL_REPLY_tmp ;
      SET work.EMAIL_REPLY ;
      IF email_reply_dttm_tz  NE . THEN email_reply_dttm_tz =tzoneu2s(email_reply_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :EMAIL_REPLY_tmp , EMAIL_REPLY );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..EMAIL_REPLY_tmp ;
            SET work.EMAIL_REPLY_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.EMAIL_REPLY_tmp  BASE=&tmplib..EMAIL_REPLY_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..EMAIL_REPLY_tmp ;
            SET work.EMAIL_REPLY_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :EMAIL_REPLY_tmp , EMAIL_REPLY );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..EMAIL_REPLY AS b USING &tmpdbschema..EMAIL_REPLY_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            test_flg, properties_map_doc, email_reply_dttm, 
            email_reply_dttm_tz, load_dttm, uri_txt, task_id, 
            subject_line_txt, segment_version_id, response_tracking_cd, occurrence_id, 
            journey_occurrence_id, imprint_id, event_nm, event_designed_id, 
            context_type_nm, audience_id, analysis_group_id, aud_occurrence_id, 
            channel_user_id, context_val, event_id, identity_id, 
            journey_id, program_id, recipient_domain_nm, segment_id, 
            task_version_id
         ) VALUES (
            d.test_flg, d.properties_map_doc, d.email_reply_dttm, 
            d.email_reply_dttm_tz, d.load_dttm, d.uri_txt, d.task_id, 
            d.subject_line_txt, d.segment_version_id, d.response_tracking_cd, d.occurrence_id, 
            d.journey_occurrence_id, d.imprint_id, d.event_nm, d.event_designed_id, 
            d.context_type_nm, d.audience_id, d.analysis_group_id, d.aud_occurrence_id, 
            d.channel_user_id, d.context_val, d.event_id, d.identity_id, 
            d.journey_id, d.program_id, d.recipient_domain_nm, d.segment_id, 
            d.task_version_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :EMAIL_REPLY_tmp , EMAIL_REPLY , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..EMAIL_REPLY_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EMAIL_REPLY_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..EMAIL_REPLY;
         DROP TABLE work.EMAIL_REPLY;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..EMAIL_SEND));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..EMAIL_SEND_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EMAIL_SEND_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=EMAIL_SEND , table_keys=%str(EVENT_ID), out_table=work.EMAIL_SEND );
   DATA work.EMAIL_SEND_tmp /VIEW=work.EMAIL_SEND_tmp ;
      SET work.EMAIL_SEND ;
      IF email_send_dttm_tz  NE . THEN email_send_dttm_tz =tzoneu2s(email_send_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :EMAIL_SEND_tmp , EMAIL_SEND );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..EMAIL_SEND_tmp ;
            SET work.EMAIL_SEND_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.EMAIL_SEND_tmp  BASE=&tmplib..EMAIL_SEND_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..EMAIL_SEND_tmp ;
            SET work.EMAIL_SEND_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :EMAIL_SEND_tmp , EMAIL_SEND );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..EMAIL_SEND AS b USING &tmpdbschema..EMAIL_SEND_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            test_flg, properties_map_doc, load_dttm, 
            email_send_dttm_tz, email_send_dttm, task_version_id, subject_line_txt, 
            segment_id, recipient_domain_nm, program_id, journey_id, 
            imprint_id, event_nm, event_designed_id, context_type_nm, 
            channel_user_id, audience_id, analysis_group_id, aud_occurrence_id, 
            context_val, event_id, identity_id, imprint_url_txt, 
            journey_occurrence_id, occurrence_id, response_tracking_cd, segment_version_id, 
            task_id
         ) VALUES (
            d.test_flg, d.properties_map_doc, d.load_dttm, 
            d.email_send_dttm_tz, d.email_send_dttm, d.task_version_id, d.subject_line_txt, 
            d.segment_id, d.recipient_domain_nm, d.program_id, d.journey_id, 
            d.imprint_id, d.event_nm, d.event_designed_id, d.context_type_nm, 
            d.channel_user_id, d.audience_id, d.analysis_group_id, d.aud_occurrence_id, 
            d.context_val, d.event_id, d.identity_id, d.imprint_url_txt, 
            d.journey_occurrence_id, d.occurrence_id, d.response_tracking_cd, d.segment_version_id, 
            d.task_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :EMAIL_SEND_tmp , EMAIL_SEND , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..EMAIL_SEND_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EMAIL_SEND_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..EMAIL_SEND;
         DROP TABLE work.EMAIL_SEND;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..EMAIL_VIEW));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..EMAIL_VIEW_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EMAIL_VIEW_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=EMAIL_VIEW , table_keys=%str(EVENT_ID), out_table=work.EMAIL_VIEW );
   DATA work.EMAIL_VIEW_tmp /VIEW=work.EMAIL_VIEW_tmp ;
      SET work.EMAIL_VIEW ;
      IF email_view_dttm_tz  NE . THEN email_view_dttm_tz =tzoneu2s(email_view_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :EMAIL_VIEW_tmp , EMAIL_VIEW );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..EMAIL_VIEW_tmp ;
            SET work.EMAIL_VIEW_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.EMAIL_VIEW_tmp  BASE=&tmplib..EMAIL_VIEW_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..EMAIL_VIEW_tmp ;
            SET work.EMAIL_VIEW_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :EMAIL_VIEW_tmp , EMAIL_VIEW );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..EMAIL_VIEW AS b USING &tmpdbschema..EMAIL_VIEW_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            test_flg, properties_map_doc, load_dttm, 
            email_view_dttm, email_view_dttm_tz, task_version_id, task_id, 
            subject_line_txt, segment_version_id, segment_id, response_tracking_cd, 
            recipient_domain_nm, program_id, occurrence_id, link_tracking_id, 
            link_tracking_group_txt, journey_occurrence_id, imprint_id, event_nm, 
            event_designed_id, context_type_nm, audience_id, analysis_group_id, 
            aud_occurrence_id, channel_user_id, context_val, event_id, 
            identity_id, journey_id, link_tracking_label_txt
         ) VALUES (
            d.test_flg, d.properties_map_doc, d.load_dttm, 
            d.email_view_dttm, d.email_view_dttm_tz, d.task_version_id, d.task_id, 
            d.subject_line_txt, d.segment_version_id, d.segment_id, d.response_tracking_cd, 
            d.recipient_domain_nm, d.program_id, d.occurrence_id, d.link_tracking_id, 
            d.link_tracking_group_txt, d.journey_occurrence_id, d.imprint_id, d.event_nm, 
            d.event_designed_id, d.context_type_nm, d.audience_id, d.analysis_group_id, 
            d.aud_occurrence_id, d.channel_user_id, d.context_val, d.event_id, 
            d.identity_id, d.journey_id, d.link_tracking_label_txt  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :EMAIL_VIEW_tmp , EMAIL_VIEW , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..EMAIL_VIEW_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EMAIL_VIEW_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..EMAIL_VIEW;
         DROP TABLE work.EMAIL_VIEW;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..EVENT_ERRORS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..EVENT_ERRORS) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate EVENT_ERRORS , EVENT_ERRORS );
   PROC APPEND DATA=&udmmart..EVENT_ERRORS  BASE=&trglib..EVENT_ERRORS (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to EVENT_ERRORS , EVENT_ERRORS );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..EVENT_ERRORS;
         DROP TABLE work.EVENT_ERRORS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..EXTERNAL_EVENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..EXTERNAL_EVENT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EXTERNAL_EVENT_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=EXTERNAL_EVENT , table_keys=%str(EVENT_ID), out_table=work.EXTERNAL_EVENT );
   DATA work.EXTERNAL_EVENT_tmp /VIEW=work.EXTERNAL_EVENT_tmp ;
      SET work.EXTERNAL_EVENT ;
      IF external_event_dttm_tz  NE . THEN external_event_dttm_tz =tzoneu2s(external_event_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :EXTERNAL_EVENT_tmp , EXTERNAL_EVENT );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..EXTERNAL_EVENT_tmp ;
            SET work.EXTERNAL_EVENT_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.EXTERNAL_EVENT_tmp  BASE=&tmplib..EXTERNAL_EVENT_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..EXTERNAL_EVENT_tmp ;
            SET work.EXTERNAL_EVENT_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :EXTERNAL_EVENT_tmp , EXTERNAL_EVENT );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..EXTERNAL_EVENT AS b USING &tmpdbschema..EXTERNAL_EVENT_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            properties_map_doc = d.properties_map_doc, 
            external_event_dttm_tz = d.external_event_dttm_tz, load_dttm = d.load_dttm, 
            external_event_dttm = d.external_event_dttm, response_tracking_cd = d.response_tracking_cd, 
            identity_id = d.identity_id, event_nm = d.event_nm, 
            event_designed_id = d.event_designed_id, context_type_nm = d.context_type_nm, 
            channel_nm = d.channel_nm, channel_user_id = d.channel_user_id, 
            context_val = d.context_val
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            properties_map_doc, external_event_dttm_tz, load_dttm, 
            external_event_dttm, response_tracking_cd, identity_id, event_nm, 
            event_designed_id, context_type_nm, channel_nm, channel_user_id, 
            context_val, event_id
         ) VALUES (
            d.properties_map_doc, d.external_event_dttm_tz, d.load_dttm, 
            d.external_event_dttm, d.response_tracking_cd, d.identity_id, d.event_nm, 
            d.event_designed_id, d.context_type_nm, d.channel_nm, d.channel_user_id, 
            d.context_val, d.event_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :EXTERNAL_EVENT_tmp , EXTERNAL_EVENT , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..EXTERNAL_EVENT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EXTERNAL_EVENT_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..EXTERNAL_EVENT;
         DROP TABLE work.EXTERNAL_EVENT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..FISCAL_CC_BUDGET));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..FISCAL_CC_BUDGET) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate FISCAL_CC_BUDGET , FISCAL_CC_BUDGET );
   PROC APPEND DATA=&udmmart..FISCAL_CC_BUDGET  BASE=&trglib..FISCAL_CC_BUDGET (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to FISCAL_CC_BUDGET , FISCAL_CC_BUDGET );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..FISCAL_CC_BUDGET;
         DROP TABLE work.FISCAL_CC_BUDGET;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..FORM_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..FORM_DETAILS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..FORM_DETAILS_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=FORM_DETAILS , table_keys=%str(EVENT_ID), out_table=work.FORM_DETAILS );
   DATA work.FORM_DETAILS_tmp /VIEW=work.FORM_DETAILS_tmp ;
      SET work.FORM_DETAILS ;
      IF form_field_detail_dttm_tz  NE . THEN form_field_detail_dttm_tz =tzoneu2s(form_field_detail_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :FORM_DETAILS_tmp , FORM_DETAILS );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..FORM_DETAILS_tmp ;
            SET work.FORM_DETAILS_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.FORM_DETAILS_tmp  BASE=&tmplib..FORM_DETAILS_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..FORM_DETAILS_tmp ;
            SET work.FORM_DETAILS_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :FORM_DETAILS_tmp , FORM_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..FORM_DETAILS AS b USING &tmpdbschema..FORM_DETAILS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            submit_flg, change_index_no, attempt_index_cnt, 
            load_dttm, form_field_detail_dttm_tz, form_field_detail_dttm, visit_id, 
            form_field_nm, event_source_cd, detail_id, attempt_status_cd, 
            event_id, form_field_value, form_nm, session_id_hex, 
            detail_id_hex, event_key_cd, form_field_id, identity_id, 
            session_id, visit_id_hex
         ) VALUES (
            d.submit_flg, d.change_index_no, d.attempt_index_cnt, 
            d.load_dttm, d.form_field_detail_dttm_tz, d.form_field_detail_dttm, d.visit_id, 
            d.form_field_nm, d.event_source_cd, d.detail_id, d.attempt_status_cd, 
            d.event_id, d.form_field_value, d.form_nm, d.session_id_hex, 
            d.detail_id_hex, d.event_key_cd, d.form_field_id, d.identity_id, 
            d.session_id, d.visit_id_hex  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :FORM_DETAILS_tmp , FORM_DETAILS , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..FORM_DETAILS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..FORM_DETAILS_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..FORM_DETAILS;
         DROP TABLE work.FORM_DETAILS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..IDENTITY_ATTRIBUTES));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..IDENTITY_ATTRIBUTES_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..IDENTITY_ATTRIBUTES_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=IDENTITY_ATTRIBUTES , table_keys=%str(ENTRYTIME,IDENTIFIER_TYPE_ID,USER_IDENTIFIER_VAL), out_table=work.IDENTITY_ATTRIBUTES );
   DATA work.IDENTITY_ATTRIBUTES_tmp /VIEW=work.IDENTITY_ATTRIBUTES_tmp ;
      SET work.IDENTITY_ATTRIBUTES ;
   RUN;
   %err_check (Failed to add time zone adaptation :IDENTITY_ATTRIBUTES_tmp , IDENTITY_ATTRIBUTES );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..IDENTITY_ATTRIBUTES_tmp ;
            SET work.IDENTITY_ATTRIBUTES_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.IDENTITY_ATTRIBUTES_tmp  BASE=&tmplib..IDENTITY_ATTRIBUTES_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..IDENTITY_ATTRIBUTES_tmp ;
            SET work.IDENTITY_ATTRIBUTES_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :IDENTITY_ATTRIBUTES_tmp , IDENTITY_ATTRIBUTES );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..IDENTITY_ATTRIBUTES AS b USING &tmpdbschema..IDENTITY_ATTRIBUTES_tmp AS d ON (
            b.entrytime = d.entrytime AND 
            b.user_identifier_val = d.user_identifier_val AND b.identifier_type_id = d.identifier_type_id )
         WHEN MATCHED THEN
         UPDATE SET
            processed_dttm = d.processed_dttm, 
            identity_id = d.identity_id
         WHEN NOT MATCHED AND ENTRYTIME IS NOT NULL AND IDENTIFIER_TYPE_ID IS NOT NULL AND USER_IDENTIFIER_VAL IS NOT NULL THEN INSERT (
            processed_dttm, entrytime, identity_id, 
            user_identifier_val, identifier_type_id
         ) VALUES (
            d.processed_dttm, d.entrytime, d.identity_id, 
            d.user_identifier_val, d.identifier_type_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :IDENTITY_ATTRIBUTES_tmp , IDENTITY_ATTRIBUTES , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..IDENTITY_ATTRIBUTES_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..IDENTITY_ATTRIBUTES_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..IDENTITY_ATTRIBUTES;
         DROP TABLE work.IDENTITY_ATTRIBUTES;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..IDENTITY_MAP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..IDENTITY_MAP_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..IDENTITY_MAP_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=IDENTITY_MAP , table_keys=%str(SOURCE_IDENTITY_ID), out_table=work.IDENTITY_MAP );
   DATA work.IDENTITY_MAP_tmp /VIEW=work.IDENTITY_MAP_tmp ;
      SET work.IDENTITY_MAP ;
   RUN;
   %err_check (Failed to add time zone adaptation :IDENTITY_MAP_tmp , IDENTITY_MAP );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..IDENTITY_MAP_tmp ;
            SET work.IDENTITY_MAP_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.IDENTITY_MAP_tmp  BASE=&tmplib..IDENTITY_MAP_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..IDENTITY_MAP_tmp ;
            SET work.IDENTITY_MAP_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :IDENTITY_MAP_tmp , IDENTITY_MAP );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..IDENTITY_MAP AS b USING &tmpdbschema..IDENTITY_MAP_tmp AS d ON (
            b.source_identity_id = d.source_identity_id )
         WHEN MATCHED THEN
         UPDATE SET
            processed_dttm = d.processed_dttm, 
            entrytime = d.entrytime, target_identity_id = d.target_identity_id
         WHEN NOT MATCHED AND SOURCE_IDENTITY_ID IS NOT NULL THEN INSERT (
            processed_dttm, entrytime, target_identity_id, 
            source_identity_id
         ) VALUES (
            d.processed_dttm, d.entrytime, d.target_identity_id, 
            d.source_identity_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :IDENTITY_MAP_tmp , IDENTITY_MAP , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..IDENTITY_MAP_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..IDENTITY_MAP_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..IDENTITY_MAP;
         DROP TABLE work.IDENTITY_MAP;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..IMPRESSION_DELIVERED));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..IMPRESSION_DELIVERED_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..IMPRESSION_DELIVERED_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=IMPRESSION_DELIVERED , table_keys=%str(EVENT_ID), out_table=work.IMPRESSION_DELIVERED );
   DATA work.IMPRESSION_DELIVERED_tmp /VIEW=work.IMPRESSION_DELIVERED_tmp ;
      SET work.IMPRESSION_DELIVERED ;
      IF impression_delivered_dttm_tz  NE . THEN impression_delivered_dttm_tz =tzoneu2s(impression_delivered_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :IMPRESSION_DELIVERED_tmp , IMPRESSION_DELIVERED );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..IMPRESSION_DELIVERED_tmp ;
            SET work.IMPRESSION_DELIVERED_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.IMPRESSION_DELIVERED_tmp  BASE=&tmplib..IMPRESSION_DELIVERED_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..IMPRESSION_DELIVERED_tmp ;
            SET work.IMPRESSION_DELIVERED_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :IMPRESSION_DELIVERED_tmp , IMPRESSION_DELIVERED );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..IMPRESSION_DELIVERED AS b USING &tmpdbschema..IMPRESSION_DELIVERED_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
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
         ) VALUES (
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
            d.session_id_hex, d.task_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :IMPRESSION_DELIVERED_tmp , IMPRESSION_DELIVERED , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..IMPRESSION_DELIVERED_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..IMPRESSION_DELIVERED_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..IMPRESSION_DELIVERED;
         DROP TABLE work.IMPRESSION_DELIVERED;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..IMPRESSION_SPOT_VIEWABLE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..IMPRESSION_SPOT_VIEWABLE_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..IMPRESSION_SPOT_VIEWABLE_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=IMPRESSION_SPOT_VIEWABLE , table_keys=%str(EVENT_ID), out_table=work.IMPRESSION_SPOT_VIEWABLE );
   DATA work.IMPRESSION_SPOT_VIEWABLE_tmp /VIEW=work.IMPRESSION_SPOT_VIEWABLE_tmp ;
      SET work.IMPRESSION_SPOT_VIEWABLE ;
      IF impression_viewable_dttm_tz  NE . THEN impression_viewable_dttm_tz =tzoneu2s(impression_viewable_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :IMPRESSION_SPOT_VIEWABLE_tmp , IMPRESSION_SPOT_VIEWABLE );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..IMPRESSION_SPOT_VIEWABLE_tmp ;
            SET work.IMPRESSION_SPOT_VIEWABLE_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.IMPRESSION_SPOT_VIEWABLE_tmp  BASE=&tmplib..IMPRESSION_SPOT_VIEWABLE_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..IMPRESSION_SPOT_VIEWABLE_tmp ;
            SET work.IMPRESSION_SPOT_VIEWABLE_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :IMPRESSION_SPOT_VIEWABLE_tmp , IMPRESSION_SPOT_VIEWABLE );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..IMPRESSION_SPOT_VIEWABLE AS b USING &tmpdbschema..IMPRESSION_SPOT_VIEWABLE_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
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
         ) VALUES (
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
            d.spot_id, d.task_version_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :IMPRESSION_SPOT_VIEWABLE_tmp , IMPRESSION_SPOT_VIEWABLE , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..IMPRESSION_SPOT_VIEWABLE_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..IMPRESSION_SPOT_VIEWABLE_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..IMPRESSION_SPOT_VIEWABLE;
         DROP TABLE work.IMPRESSION_SPOT_VIEWABLE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..INVOICE_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..INVOICE_DETAILS) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate INVOICE_DETAILS , INVOICE_DETAILS );
   PROC APPEND DATA=&udmmart..INVOICE_DETAILS  BASE=&trglib..INVOICE_DETAILS (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to INVOICE_DETAILS , INVOICE_DETAILS );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..INVOICE_DETAILS;
         DROP TABLE work.INVOICE_DETAILS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..INVOICE_LINE_ITEMS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..INVOICE_LINE_ITEMS) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate INVOICE_LINE_ITEMS , INVOICE_LINE_ITEMS );
   PROC APPEND DATA=&udmmart..INVOICE_LINE_ITEMS  BASE=&trglib..INVOICE_LINE_ITEMS (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to INVOICE_LINE_ITEMS , INVOICE_LINE_ITEMS );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..INVOICE_LINE_ITEMS;
         DROP TABLE work.INVOICE_LINE_ITEMS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..INVOICE_LINE_ITEMS_CCBDGT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..INVOICE_LINE_ITEMS_CCBDGT) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate INVOICE_LINE_ITEMS_CCBDGT , INVOICE_LINE_ITEMS_CCBDGT );
   PROC APPEND DATA=&udmmart..INVOICE_LINE_ITEMS_CCBDGT  BASE=&trglib..INVOICE_LINE_ITEMS_CCBDGT (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to INVOICE_LINE_ITEMS_CCBDGT , INVOICE_LINE_ITEMS_CCBDGT );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..INVOICE_LINE_ITEMS_CCBDGT;
         DROP TABLE work.INVOICE_LINE_ITEMS_CCBDGT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..IN_APP_FAILED));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..IN_APP_FAILED_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..IN_APP_FAILED_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=IN_APP_FAILED , table_keys=%str(EVENT_ID), out_table=work.IN_APP_FAILED );
   DATA work.IN_APP_FAILED_tmp /VIEW=work.IN_APP_FAILED_tmp ;
      SET work.IN_APP_FAILED ;
      IF in_app_failed_dttm_tz  NE . THEN in_app_failed_dttm_tz =tzoneu2s(in_app_failed_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :IN_APP_FAILED_tmp , IN_APP_FAILED );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..IN_APP_FAILED_tmp ;
            SET work.IN_APP_FAILED_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.IN_APP_FAILED_tmp  BASE=&tmplib..IN_APP_FAILED_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..IN_APP_FAILED_tmp ;
            SET work.IN_APP_FAILED_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :IN_APP_FAILED_tmp , IN_APP_FAILED );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..IN_APP_FAILED AS b USING &tmpdbschema..IN_APP_FAILED_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            properties_map_doc, in_app_failed_dttm, in_app_failed_dttm_tz, 
            load_dttm, task_version_id, segment_id, message_id, 
            identity_id, error_message_txt, context_val, channel_user_id, 
            context_type_nm, creative_version_id, event_id, mobile_app_id, 
            reserved_2_txt, spot_id, channel_nm, creative_id, 
            error_cd, event_designed_id, event_nm, message_version_id, 
            occurrence_id, reserved_1_txt, response_tracking_cd, segment_version_id, 
            task_id
         ) VALUES (
            d.properties_map_doc, d.in_app_failed_dttm, d.in_app_failed_dttm_tz, 
            d.load_dttm, d.task_version_id, d.segment_id, d.message_id, 
            d.identity_id, d.error_message_txt, d.context_val, d.channel_user_id, 
            d.context_type_nm, d.creative_version_id, d.event_id, d.mobile_app_id, 
            d.reserved_2_txt, d.spot_id, d.channel_nm, d.creative_id, 
            d.error_cd, d.event_designed_id, d.event_nm, d.message_version_id, 
            d.occurrence_id, d.reserved_1_txt, d.response_tracking_cd, d.segment_version_id, 
            d.task_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :IN_APP_FAILED_tmp , IN_APP_FAILED , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..IN_APP_FAILED_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..IN_APP_FAILED_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..IN_APP_FAILED;
         DROP TABLE work.IN_APP_FAILED;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..IN_APP_MESSAGE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..IN_APP_MESSAGE_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..IN_APP_MESSAGE_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=IN_APP_MESSAGE , table_keys=%str(EVENT_ID), out_table=work.IN_APP_MESSAGE );
   DATA work.IN_APP_MESSAGE_tmp /VIEW=work.IN_APP_MESSAGE_tmp ;
      SET work.IN_APP_MESSAGE ;
      IF in_app_action_dttm_tz  NE . THEN in_app_action_dttm_tz =tzoneu2s(in_app_action_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :IN_APP_MESSAGE_tmp , IN_APP_MESSAGE );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..IN_APP_MESSAGE_tmp ;
            SET work.IN_APP_MESSAGE_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.IN_APP_MESSAGE_tmp  BASE=&tmplib..IN_APP_MESSAGE_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..IN_APP_MESSAGE_tmp ;
            SET work.IN_APP_MESSAGE_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :IN_APP_MESSAGE_tmp , IN_APP_MESSAGE );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..IN_APP_MESSAGE AS b USING &tmpdbschema..IN_APP_MESSAGE_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            properties_map_doc, in_app_action_dttm_tz, load_dttm, 
            in_app_action_dttm, segment_version_id, reserved_2_txt, mobile_app_id, 
            event_id, context_val, channel_user_id, creative_version_id, 
            identity_id, message_id, response_tracking_cd, task_id, 
            channel_nm, context_type_nm, creative_id, event_designed_id, 
            event_nm, message_version_id, occurrence_id, reserved_1_txt, 
            reserved_3_txt, segment_id, spot_id, task_version_id
         ) VALUES (
            d.properties_map_doc, d.in_app_action_dttm_tz, d.load_dttm, 
            d.in_app_action_dttm, d.segment_version_id, d.reserved_2_txt, d.mobile_app_id, 
            d.event_id, d.context_val, d.channel_user_id, d.creative_version_id, 
            d.identity_id, d.message_id, d.response_tracking_cd, d.task_id, 
            d.channel_nm, d.context_type_nm, d.creative_id, d.event_designed_id, 
            d.event_nm, d.message_version_id, d.occurrence_id, d.reserved_1_txt, 
            d.reserved_3_txt, d.segment_id, d.spot_id, d.task_version_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :IN_APP_MESSAGE_tmp , IN_APP_MESSAGE , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..IN_APP_MESSAGE_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..IN_APP_MESSAGE_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..IN_APP_MESSAGE;
         DROP TABLE work.IN_APP_MESSAGE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..IN_APP_SEND));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..IN_APP_SEND_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..IN_APP_SEND_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=IN_APP_SEND , table_keys=%str(EVENT_ID), out_table=work.IN_APP_SEND );
   DATA work.IN_APP_SEND_tmp /VIEW=work.IN_APP_SEND_tmp ;
      SET work.IN_APP_SEND ;
      IF in_app_send_dttm_tz  NE . THEN in_app_send_dttm_tz =tzoneu2s(in_app_send_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :IN_APP_SEND_tmp , IN_APP_SEND );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..IN_APP_SEND_tmp ;
            SET work.IN_APP_SEND_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.IN_APP_SEND_tmp  BASE=&tmplib..IN_APP_SEND_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..IN_APP_SEND_tmp ;
            SET work.IN_APP_SEND_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :IN_APP_SEND_tmp , IN_APP_SEND );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..IN_APP_SEND AS b USING &tmpdbschema..IN_APP_SEND_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            properties_map_doc, load_dttm, in_app_send_dttm_tz, 
            in_app_send_dttm, task_id, response_tracking_cd, occurrence_id, 
            message_id, event_nm, creative_id, channel_nm, 
            context_type_nm, creative_version_id, event_designed_id, message_version_id, 
            reserved_1_txt, segment_version_id, channel_user_id, context_val, 
            event_id, identity_id, mobile_app_id, reserved_2_txt, 
            segment_id, spot_id, task_version_id
         ) VALUES (
            d.properties_map_doc, d.load_dttm, d.in_app_send_dttm_tz, 
            d.in_app_send_dttm, d.task_id, d.response_tracking_cd, d.occurrence_id, 
            d.message_id, d.event_nm, d.creative_id, d.channel_nm, 
            d.context_type_nm, d.creative_version_id, d.event_designed_id, d.message_version_id, 
            d.reserved_1_txt, d.segment_version_id, d.channel_user_id, d.context_val, 
            d.event_id, d.identity_id, d.mobile_app_id, d.reserved_2_txt, 
            d.segment_id, d.spot_id, d.task_version_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :IN_APP_SEND_tmp , IN_APP_SEND , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..IN_APP_SEND_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..IN_APP_SEND_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..IN_APP_SEND;
         DROP TABLE work.IN_APP_SEND;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..IN_APP_TARGETING_REQUEST));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..IN_APP_TARGETING_REQUEST_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..IN_APP_TARGETING_REQUEST_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=IN_APP_TARGETING_REQUEST , table_keys=%str(EVENT_ID), out_table=work.IN_APP_TARGETING_REQUEST );
   DATA work.IN_APP_TARGETING_REQUEST_tmp /VIEW=work.IN_APP_TARGETING_REQUEST_tmp ;
      SET work.IN_APP_TARGETING_REQUEST ;
      IF in_app_tgt_request_dttm_tz  NE . THEN in_app_tgt_request_dttm_tz =tzoneu2s(in_app_tgt_request_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :IN_APP_TARGETING_REQUEST_tmp , IN_APP_TARGETING_REQUEST );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..IN_APP_TARGETING_REQUEST_tmp ;
            SET work.IN_APP_TARGETING_REQUEST_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.IN_APP_TARGETING_REQUEST_tmp  BASE=&tmplib..IN_APP_TARGETING_REQUEST_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..IN_APP_TARGETING_REQUEST_tmp ;
            SET work.IN_APP_TARGETING_REQUEST_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :IN_APP_TARGETING_REQUEST_tmp , IN_APP_TARGETING_REQUEST );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..IN_APP_TARGETING_REQUEST AS b USING &tmpdbschema..IN_APP_TARGETING_REQUEST_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            eligibility_flg = d.eligibility_flg, 
            in_app_tgt_request_dttm = d.in_app_tgt_request_dttm, load_dttm = d.load_dttm, 
            in_app_tgt_request_dttm_tz = d.in_app_tgt_request_dttm_tz, context_type_nm = d.context_type_nm, 
            channel_nm = d.channel_nm, event_designed_id = d.event_designed_id, 
            identity_id = d.identity_id, mobile_app_id = d.mobile_app_id, 
            channel_user_id = d.channel_user_id, context_val = d.context_val, 
            event_nm = d.event_nm
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            eligibility_flg, in_app_tgt_request_dttm, load_dttm, 
            in_app_tgt_request_dttm_tz, event_id, context_type_nm, channel_nm, 
            event_designed_id, identity_id, mobile_app_id, channel_user_id, 
            context_val, event_nm
         ) VALUES (
            d.eligibility_flg, d.in_app_tgt_request_dttm, d.load_dttm, 
            d.in_app_tgt_request_dttm_tz, d.event_id, d.context_type_nm, d.channel_nm, 
            d.event_designed_id, d.identity_id, d.mobile_app_id, d.channel_user_id, 
            d.context_val, d.event_nm  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :IN_APP_TARGETING_REQUEST_tmp , IN_APP_TARGETING_REQUEST , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..IN_APP_TARGETING_REQUEST_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..IN_APP_TARGETING_REQUEST_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..IN_APP_TARGETING_REQUEST;
         DROP TABLE work.IN_APP_TARGETING_REQUEST;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..JOURNEY_ENTRY));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..JOURNEY_ENTRY_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..JOURNEY_ENTRY_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=JOURNEY_ENTRY , table_keys=%str(EVENT_ID), out_table=work.JOURNEY_ENTRY );
   DATA work.JOURNEY_ENTRY_tmp /VIEW=work.JOURNEY_ENTRY_tmp ;
      SET work.JOURNEY_ENTRY ;
      IF entry_dttm_tz  NE . THEN entry_dttm_tz =tzoneu2s(entry_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :JOURNEY_ENTRY_tmp , JOURNEY_ENTRY );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..JOURNEY_ENTRY_tmp ;
            SET work.JOURNEY_ENTRY_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.JOURNEY_ENTRY_tmp  BASE=&tmplib..JOURNEY_ENTRY_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..JOURNEY_ENTRY_tmp ;
            SET work.JOURNEY_ENTRY_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :JOURNEY_ENTRY_tmp , JOURNEY_ENTRY );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..JOURNEY_ENTRY AS b USING &tmpdbschema..JOURNEY_ENTRY_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            entry_dttm = d.entry_dttm, 
            entry_dttm_tz = d.entry_dttm_tz, load_dttm = d.load_dttm, 
            journey_occurrence_id = d.journey_occurrence_id, identity_id = d.identity_id, 
            aud_occurrence_id = d.aud_occurrence_id, audience_id = d.audience_id, 
            context_type_nm = d.context_type_nm, identity_type_val = d.identity_type_val, 
            context_val = d.context_val, event_nm = d.event_nm, 
            identity_type_nm = d.identity_type_nm, journey_id = d.journey_id
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            entry_dttm, entry_dttm_tz, load_dttm, 
            journey_occurrence_id, identity_id, aud_occurrence_id, audience_id, 
            context_type_nm, event_id, identity_type_val, context_val, 
            event_nm, identity_type_nm, journey_id
         ) VALUES (
            d.entry_dttm, d.entry_dttm_tz, d.load_dttm, 
            d.journey_occurrence_id, d.identity_id, d.aud_occurrence_id, d.audience_id, 
            d.context_type_nm, d.event_id, d.identity_type_val, d.context_val, 
            d.event_nm, d.identity_type_nm, d.journey_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :JOURNEY_ENTRY_tmp , JOURNEY_ENTRY , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..JOURNEY_ENTRY_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..JOURNEY_ENTRY_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..JOURNEY_ENTRY;
         DROP TABLE work.JOURNEY_ENTRY;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..JOURNEY_EXIT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..JOURNEY_EXIT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..JOURNEY_EXIT_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=JOURNEY_EXIT , table_keys=%str(EVENT_ID), out_table=work.JOURNEY_EXIT );
   DATA work.JOURNEY_EXIT_tmp /VIEW=work.JOURNEY_EXIT_tmp ;
      SET work.JOURNEY_EXIT ;
      IF exit_dttm_tz  NE . THEN exit_dttm_tz =tzoneu2s(exit_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :JOURNEY_EXIT_tmp , JOURNEY_EXIT );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..JOURNEY_EXIT_tmp ;
            SET work.JOURNEY_EXIT_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.JOURNEY_EXIT_tmp  BASE=&tmplib..JOURNEY_EXIT_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..JOURNEY_EXIT_tmp ;
            SET work.JOURNEY_EXIT_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :JOURNEY_EXIT_tmp , JOURNEY_EXIT );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..JOURNEY_EXIT AS b USING &tmpdbschema..JOURNEY_EXIT_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            exit_dttm = d.exit_dttm, 
            exit_dttm_tz = d.exit_dttm_tz, load_dttm = d.load_dttm, 
            last_node_id = d.last_node_id, identity_type_nm = d.identity_type_nm, 
            context_type_nm = d.context_type_nm, aud_occurrence_id = d.aud_occurrence_id, 
            group_id = d.group_id, journey_id = d.journey_id, 
            reason_cd = d.reason_cd, audience_id = d.audience_id, 
            context_val = d.context_val, event_nm = d.event_nm, 
            identity_id = d.identity_id, identity_type_val = d.identity_type_val, 
            journey_occurrence_id = d.journey_occurrence_id, reason_txt = d.reason_txt
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            exit_dttm, exit_dttm_tz, load_dttm, 
            last_node_id, identity_type_nm, context_type_nm, aud_occurrence_id, 
            event_id, group_id, journey_id, reason_cd, 
            audience_id, context_val, event_nm, identity_id, 
            identity_type_val, journey_occurrence_id, reason_txt
         ) VALUES (
            d.exit_dttm, d.exit_dttm_tz, d.load_dttm, 
            d.last_node_id, d.identity_type_nm, d.context_type_nm, d.aud_occurrence_id, 
            d.event_id, d.group_id, d.journey_id, d.reason_cd, 
            d.audience_id, d.context_val, d.event_nm, d.identity_id, 
            d.identity_type_val, d.journey_occurrence_id, d.reason_txt  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :JOURNEY_EXIT_tmp , JOURNEY_EXIT , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..JOURNEY_EXIT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..JOURNEY_EXIT_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..JOURNEY_EXIT;
         DROP TABLE work.JOURNEY_EXIT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..JOURNEY_HOLDOUT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..JOURNEY_HOLDOUT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..JOURNEY_HOLDOUT_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=JOURNEY_HOLDOUT , table_keys=%str(EVENT_ID), out_table=work.JOURNEY_HOLDOUT );
   DATA work.JOURNEY_HOLDOUT_tmp /VIEW=work.JOURNEY_HOLDOUT_tmp ;
      SET work.JOURNEY_HOLDOUT ;
      IF holdout_dttm_tz  NE . THEN holdout_dttm_tz =tzoneu2s(holdout_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :JOURNEY_HOLDOUT_tmp , JOURNEY_HOLDOUT );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..JOURNEY_HOLDOUT_tmp ;
            SET work.JOURNEY_HOLDOUT_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.JOURNEY_HOLDOUT_tmp  BASE=&tmplib..JOURNEY_HOLDOUT_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..JOURNEY_HOLDOUT_tmp ;
            SET work.JOURNEY_HOLDOUT_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :JOURNEY_HOLDOUT_tmp , JOURNEY_HOLDOUT );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..JOURNEY_HOLDOUT AS b USING &tmpdbschema..JOURNEY_HOLDOUT_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            holdout_dttm_tz = d.holdout_dttm_tz, 
            load_dttm = d.load_dttm, holdout_dttm = d.holdout_dttm, 
            journey_occurrence_id = d.journey_occurrence_id, journey_id = d.journey_id, 
            identity_type_val = d.identity_type_val, identity_type_nm = d.identity_type_nm, 
            identity_id = d.identity_id, event_nm = d.event_nm, 
            context_val = d.context_val, context_type_nm = d.context_type_nm, 
            audience_id = d.audience_id, aud_occurrence_id = d.aud_occurrence_id
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            holdout_dttm_tz, load_dttm, holdout_dttm, 
            journey_occurrence_id, journey_id, identity_type_val, identity_type_nm, 
            identity_id, event_nm, event_id, context_val, 
            context_type_nm, audience_id, aud_occurrence_id
         ) VALUES (
            d.holdout_dttm_tz, d.load_dttm, d.holdout_dttm, 
            d.journey_occurrence_id, d.journey_id, d.identity_type_val, d.identity_type_nm, 
            d.identity_id, d.event_nm, d.event_id, d.context_val, 
            d.context_type_nm, d.audience_id, d.aud_occurrence_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :JOURNEY_HOLDOUT_tmp , JOURNEY_HOLDOUT , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..JOURNEY_HOLDOUT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..JOURNEY_HOLDOUT_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..JOURNEY_HOLDOUT;
         DROP TABLE work.JOURNEY_HOLDOUT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..JOURNEY_NODE_ENTRY));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..JOURNEY_NODE_ENTRY_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..JOURNEY_NODE_ENTRY_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=JOURNEY_NODE_ENTRY , table_keys=%str(EVENT_ID), out_table=work.JOURNEY_NODE_ENTRY );
   DATA work.JOURNEY_NODE_ENTRY_tmp /VIEW=work.JOURNEY_NODE_ENTRY_tmp ;
      SET work.JOURNEY_NODE_ENTRY ;
      IF node_entry_dttm_tz  NE . THEN node_entry_dttm_tz =tzoneu2s(node_entry_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :JOURNEY_NODE_ENTRY_tmp , JOURNEY_NODE_ENTRY );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..JOURNEY_NODE_ENTRY_tmp ;
            SET work.JOURNEY_NODE_ENTRY_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.JOURNEY_NODE_ENTRY_tmp  BASE=&tmplib..JOURNEY_NODE_ENTRY_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..JOURNEY_NODE_ENTRY_tmp ;
            SET work.JOURNEY_NODE_ENTRY_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :JOURNEY_NODE_ENTRY_tmp , JOURNEY_NODE_ENTRY );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..JOURNEY_NODE_ENTRY AS b USING &tmpdbschema..JOURNEY_NODE_ENTRY_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            node_entry_dttm = d.node_entry_dttm, 
            load_dttm = d.load_dttm, node_entry_dttm_tz = d.node_entry_dttm_tz, 
            node_type_nm = d.node_type_nm, node_id = d.node_id, 
            previous_node_id = d.previous_node_id, journey_occurrence_id = d.journey_occurrence_id, 
            journey_id = d.journey_id, identity_type_val = d.identity_type_val, 
            identity_type_nm = d.identity_type_nm, identity_id = d.identity_id, 
            group_id = d.group_id, event_nm = d.event_nm, 
            context_val = d.context_val, context_type_nm = d.context_type_nm, 
            audience_id = d.audience_id, aud_occurrence_id = d.aud_occurrence_id
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            node_entry_dttm, load_dttm, node_entry_dttm_tz, 
            node_type_nm, node_id, previous_node_id, journey_occurrence_id, 
            journey_id, identity_type_val, identity_type_nm, identity_id, 
            group_id, event_nm, event_id, context_val, 
            context_type_nm, audience_id, aud_occurrence_id
         ) VALUES (
            d.node_entry_dttm, d.load_dttm, d.node_entry_dttm_tz, 
            d.node_type_nm, d.node_id, d.previous_node_id, d.journey_occurrence_id, 
            d.journey_id, d.identity_type_val, d.identity_type_nm, d.identity_id, 
            d.group_id, d.event_nm, d.event_id, d.context_val, 
            d.context_type_nm, d.audience_id, d.aud_occurrence_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :JOURNEY_NODE_ENTRY_tmp , JOURNEY_NODE_ENTRY , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..JOURNEY_NODE_ENTRY_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..JOURNEY_NODE_ENTRY_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..JOURNEY_NODE_ENTRY;
         DROP TABLE work.JOURNEY_NODE_ENTRY;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..JOURNEY_SUCCESS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..JOURNEY_SUCCESS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..JOURNEY_SUCCESS_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=JOURNEY_SUCCESS , table_keys=%str(EVENT_ID), out_table=work.JOURNEY_SUCCESS );
   DATA work.JOURNEY_SUCCESS_tmp /VIEW=work.JOURNEY_SUCCESS_tmp ;
      SET work.JOURNEY_SUCCESS ;
      IF success_dttm_tz  NE . THEN success_dttm_tz =tzoneu2s(success_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :JOURNEY_SUCCESS_tmp , JOURNEY_SUCCESS );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..JOURNEY_SUCCESS_tmp ;
            SET work.JOURNEY_SUCCESS_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.JOURNEY_SUCCESS_tmp  BASE=&tmplib..JOURNEY_SUCCESS_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..JOURNEY_SUCCESS_tmp ;
            SET work.JOURNEY_SUCCESS_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :JOURNEY_SUCCESS_tmp , JOURNEY_SUCCESS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..JOURNEY_SUCCESS AS b USING &tmpdbschema..JOURNEY_SUCCESS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            unit_qty = d.unit_qty, 
            success_val = d.success_val, success_dttm = d.success_dttm, 
            load_dttm = d.load_dttm, success_dttm_tz = d.success_dttm_tz, 
            parent_event_designed_id = d.parent_event_designed_id, journey_id = d.journey_id, 
            identity_type_nm = d.identity_type_nm, group_id = d.group_id, 
            context_type_nm = d.context_type_nm, audience_id = d.audience_id, 
            aud_occurrence_id = d.aud_occurrence_id, context_val = d.context_val, 
            event_nm = d.event_nm, identity_id = d.identity_id, 
            identity_type_val = d.identity_type_val, journey_occurrence_id = d.journey_occurrence_id
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            unit_qty, success_val, success_dttm, 
            load_dttm, success_dttm_tz, parent_event_designed_id, journey_id, 
            identity_type_nm, group_id, event_id, context_type_nm, 
            audience_id, aud_occurrence_id, context_val, event_nm, 
            identity_id, identity_type_val, journey_occurrence_id
         ) VALUES (
            d.unit_qty, d.success_val, d.success_dttm, 
            d.load_dttm, d.success_dttm_tz, d.parent_event_designed_id, d.journey_id, 
            d.identity_type_nm, d.group_id, d.event_id, d.context_type_nm, 
            d.audience_id, d.aud_occurrence_id, d.context_val, d.event_nm, 
            d.identity_id, d.identity_type_val, d.journey_occurrence_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :JOURNEY_SUCCESS_tmp , JOURNEY_SUCCESS , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..JOURNEY_SUCCESS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..JOURNEY_SUCCESS_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..JOURNEY_SUCCESS;
         DROP TABLE work.JOURNEY_SUCCESS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..JOURNEY_SUPPRESSION));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..JOURNEY_SUPPRESSION_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..JOURNEY_SUPPRESSION_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=JOURNEY_SUPPRESSION , table_keys=%str(EVENT_ID), out_table=work.JOURNEY_SUPPRESSION );
   DATA work.JOURNEY_SUPPRESSION_tmp /VIEW=work.JOURNEY_SUPPRESSION_tmp ;
      SET work.JOURNEY_SUPPRESSION ;
      IF suppression_dttm_tz  NE . THEN suppression_dttm_tz =tzoneu2s(suppression_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :JOURNEY_SUPPRESSION_tmp , JOURNEY_SUPPRESSION );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..JOURNEY_SUPPRESSION_tmp ;
            SET work.JOURNEY_SUPPRESSION_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.JOURNEY_SUPPRESSION_tmp  BASE=&tmplib..JOURNEY_SUPPRESSION_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..JOURNEY_SUPPRESSION_tmp ;
            SET work.JOURNEY_SUPPRESSION_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :JOURNEY_SUPPRESSION_tmp , JOURNEY_SUPPRESSION );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..JOURNEY_SUPPRESSION AS b USING &tmpdbschema..JOURNEY_SUPPRESSION_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            load_dttm = d.load_dttm, 
            suppression_dttm = d.suppression_dttm, suppression_dttm_tz = d.suppression_dttm_tz, 
            reason_txt = d.reason_txt, reason_cd = d.reason_cd, 
            journey_occurrence_id = d.journey_occurrence_id, identity_type_val = d.identity_type_val, 
            identity_type_nm = d.identity_type_nm, identity_id = d.identity_id, 
            context_type_nm = d.context_type_nm, audience_id = d.audience_id, 
            aud_occurrence_id = d.aud_occurrence_id, context_val = d.context_val, 
            event_nm = d.event_nm, journey_id = d.journey_id
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            load_dttm, suppression_dttm, suppression_dttm_tz, 
            reason_txt, reason_cd, journey_occurrence_id, identity_type_val, 
            identity_type_nm, identity_id, event_id, context_type_nm, 
            audience_id, aud_occurrence_id, context_val, event_nm, 
            journey_id
         ) VALUES (
            d.load_dttm, d.suppression_dttm, d.suppression_dttm_tz, 
            d.reason_txt, d.reason_cd, d.journey_occurrence_id, d.identity_type_val, 
            d.identity_type_nm, d.identity_id, d.event_id, d.context_type_nm, 
            d.audience_id, d.aud_occurrence_id, d.context_val, d.event_nm, 
            d.journey_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :JOURNEY_SUPPRESSION_tmp , JOURNEY_SUPPRESSION , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..JOURNEY_SUPPRESSION_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..JOURNEY_SUPPRESSION_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..JOURNEY_SUPPRESSION;
         DROP TABLE work.JOURNEY_SUPPRESSION;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..JOURNEY_TEST_SUCCESS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..JOURNEY_TEST_SUCCESS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..JOURNEY_TEST_SUCCESS_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=JOURNEY_TEST_SUCCESS , table_keys=%str(EVENT_ID), out_table=work.JOURNEY_TEST_SUCCESS );
   DATA work.JOURNEY_TEST_SUCCESS_tmp /VIEW=work.JOURNEY_TEST_SUCCESS_tmp ;
      SET work.JOURNEY_TEST_SUCCESS ;
      IF success_dttm_tz  NE . THEN success_dttm_tz =tzoneu2s(success_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :JOURNEY_TEST_SUCCESS_tmp , JOURNEY_TEST_SUCCESS );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..JOURNEY_TEST_SUCCESS_tmp ;
            SET work.JOURNEY_TEST_SUCCESS_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.JOURNEY_TEST_SUCCESS_tmp  BASE=&tmplib..JOURNEY_TEST_SUCCESS_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..JOURNEY_TEST_SUCCESS_tmp ;
            SET work.JOURNEY_TEST_SUCCESS_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :JOURNEY_TEST_SUCCESS_tmp , JOURNEY_TEST_SUCCESS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..JOURNEY_TEST_SUCCESS AS b USING &tmpdbschema..JOURNEY_TEST_SUCCESS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            success_dttm_tz = d.success_dttm_tz, 
            success_dttm = d.success_dttm, parent_event_designed_id = d.parent_event_designed_id, 
            journey_id = d.journey_id, group_id = d.group_id, 
            event_nm = d.event_nm, context_type_nm = d.context_type_nm, 
            context_val = d.context_val, identity_id = d.identity_id, 
            journey_occurrence_id = d.journey_occurrence_id
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            success_dttm_tz, success_dttm, parent_event_designed_id, 
            journey_id, group_id, event_nm, event_id, 
            context_type_nm, context_val, identity_id, journey_occurrence_id
         ) VALUES (
            d.success_dttm_tz, d.success_dttm, d.parent_event_designed_id, 
            d.journey_id, d.group_id, d.event_nm, d.event_id, 
            d.context_type_nm, d.context_val, d.identity_id, d.journey_occurrence_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :JOURNEY_TEST_SUCCESS_tmp , JOURNEY_TEST_SUCCESS , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..JOURNEY_TEST_SUCCESS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..JOURNEY_TEST_SUCCESS_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..JOURNEY_TEST_SUCCESS;
         DROP TABLE work.JOURNEY_TEST_SUCCESS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_ACTIVITY));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_ACTIVITY) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_ACTIVITY , MD_ACTIVITY );
   PROC APPEND DATA=&udmmart..MD_ACTIVITY  BASE=&trglib..MD_ACTIVITY (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_ACTIVITY , MD_ACTIVITY );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_ACTIVITY;
         DROP TABLE work.MD_ACTIVITY;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_ACTIVITY_ABTESTPATH));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_ACTIVITY_ABTESTPATH) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_ACTIVITY_ABTESTPATH , MD_ACTIVITY_ABTESTPATH );
   PROC APPEND DATA=&udmmart..MD_ACTIVITY_ABTESTPATH  BASE=&trglib..MD_ACTIVITY_ABTESTPATH (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_ACTIVITY_ABTESTPATH , MD_ACTIVITY_ABTESTPATH );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_ACTIVITY_ABTESTPATH;
         DROP TABLE work.MD_ACTIVITY_ABTESTPATH;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_ACTIVITY_ABTESTPATH_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_ACTIVITY_ABTESTPATH_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_ACTIVITY_ABTESTPATH_ALL , MD_ACTIVITY_ABTESTPATH_ALL );
   PROC APPEND DATA=&udmmart..MD_ACTIVITY_ABTESTPATH_ALL  BASE=&trglib..MD_ACTIVITY_ABTESTPATH_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_ACTIVITY_ABTESTPATH_ALL , MD_ACTIVITY_ABTESTPATH_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_ACTIVITY_ABTESTPATH_ALL;
         DROP TABLE work.MD_ACTIVITY_ABTESTPATH_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_ACTIVITY_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_ACTIVITY_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_ACTIVITY_ALL , MD_ACTIVITY_ALL );
   PROC APPEND DATA=&udmmart..MD_ACTIVITY_ALL  BASE=&trglib..MD_ACTIVITY_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_ACTIVITY_ALL , MD_ACTIVITY_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_ACTIVITY_ALL;
         DROP TABLE work.MD_ACTIVITY_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_ACTIVITY_CUSTOM_PROP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_ACTIVITY_CUSTOM_PROP) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_ACTIVITY_CUSTOM_PROP , MD_ACTIVITY_CUSTOM_PROP );
   PROC APPEND DATA=&udmmart..MD_ACTIVITY_CUSTOM_PROP  BASE=&trglib..MD_ACTIVITY_CUSTOM_PROP (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_ACTIVITY_CUSTOM_PROP , MD_ACTIVITY_CUSTOM_PROP );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_ACTIVITY_CUSTOM_PROP;
         DROP TABLE work.MD_ACTIVITY_CUSTOM_PROP;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_ACTIVITY_CUSTOM_PROP_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_ACTIVITY_CUSTOM_PROP_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_ACTIVITY_CUSTOM_PROP_ALL , MD_ACTIVITY_CUSTOM_PROP_ALL );
   PROC APPEND DATA=&udmmart..MD_ACTIVITY_CUSTOM_PROP_ALL  BASE=&trglib..MD_ACTIVITY_CUSTOM_PROP_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_ACTIVITY_CUSTOM_PROP_ALL , MD_ACTIVITY_CUSTOM_PROP_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_ACTIVITY_CUSTOM_PROP_ALL;
         DROP TABLE work.MD_ACTIVITY_CUSTOM_PROP_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_ACTIVITY_NODE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_ACTIVITY_NODE) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_ACTIVITY_NODE , MD_ACTIVITY_NODE );
   PROC APPEND DATA=&udmmart..MD_ACTIVITY_NODE  BASE=&trglib..MD_ACTIVITY_NODE (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_ACTIVITY_NODE , MD_ACTIVITY_NODE );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_ACTIVITY_NODE;
         DROP TABLE work.MD_ACTIVITY_NODE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_ACTIVITY_NODE_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_ACTIVITY_NODE_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_ACTIVITY_NODE_ALL , MD_ACTIVITY_NODE_ALL );
   PROC APPEND DATA=&udmmart..MD_ACTIVITY_NODE_ALL  BASE=&trglib..MD_ACTIVITY_NODE_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_ACTIVITY_NODE_ALL , MD_ACTIVITY_NODE_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_ACTIVITY_NODE_ALL;
         DROP TABLE work.MD_ACTIVITY_NODE_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_ACTIVITY_X_ACTIVITY_NODE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_ACTIVITY_X_ACTIVITY_NODE) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_ACTIVITY_X_ACTIVITY_NODE , MD_ACTIVITY_X_ACTIVITY_NODE );
   PROC APPEND DATA=&udmmart..MD_ACTIVITY_X_ACTIVITY_NODE  BASE=&trglib..MD_ACTIVITY_X_ACTIVITY_NODE (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_ACTIVITY_X_ACTIVITY_NODE , MD_ACTIVITY_X_ACTIVITY_NODE );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_ACTIVITY_X_ACTIVITY_NODE;
         DROP TABLE work.MD_ACTIVITY_X_ACTIVITY_NODE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_ACTIVITY_X_ACTIVITY_NODE_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_ACTIVITY_X_ACTIVITY_NODE_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_ACTIVITY_X_ACTIVITY_NODE_ALL , MD_ACTIVITY_X_ACTIVITY_NODE_ALL );
   PROC APPEND DATA=&udmmart..MD_ACTIVITY_X_ACTIVITY_NODE_ALL  BASE=&trglib..MD_ACTIVITY_X_ACTIVITY_NODE_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_ACTIVITY_X_ACTIVITY_NODE_ALL , MD_ACTIVITY_X_ACTIVITY_NODE_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_ACTIVITY_X_ACTIVITY_NODE_ALL;
         DROP TABLE work.MD_ACTIVITY_X_ACTIVITY_NODE_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_ACTIVITY_X_TASK));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_ACTIVITY_X_TASK) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_ACTIVITY_X_TASK , MD_ACTIVITY_X_TASK );
   PROC APPEND DATA=&udmmart..MD_ACTIVITY_X_TASK  BASE=&trglib..MD_ACTIVITY_X_TASK (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_ACTIVITY_X_TASK , MD_ACTIVITY_X_TASK );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_ACTIVITY_X_TASK;
         DROP TABLE work.MD_ACTIVITY_X_TASK;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_ACTIVITY_X_TASK_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_ACTIVITY_X_TASK_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_ACTIVITY_X_TASK_ALL , MD_ACTIVITY_X_TASK_ALL );
   PROC APPEND DATA=&udmmart..MD_ACTIVITY_X_TASK_ALL  BASE=&trglib..MD_ACTIVITY_X_TASK_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_ACTIVITY_X_TASK_ALL , MD_ACTIVITY_X_TASK_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_ACTIVITY_X_TASK_ALL;
         DROP TABLE work.MD_ACTIVITY_X_TASK_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_ASSET));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_ASSET) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_ASSET , MD_ASSET );
   PROC APPEND DATA=&udmmart..MD_ASSET  BASE=&trglib..MD_ASSET (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_ASSET , MD_ASSET );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_ASSET;
         DROP TABLE work.MD_ASSET;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_ASSET_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_ASSET_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_ASSET_ALL , MD_ASSET_ALL );
   PROC APPEND DATA=&udmmart..MD_ASSET_ALL  BASE=&trglib..MD_ASSET_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_ASSET_ALL , MD_ASSET_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_ASSET_ALL;
         DROP TABLE work.MD_ASSET_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_AUDIENCE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_AUDIENCE) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_AUDIENCE , MD_AUDIENCE );
   PROC APPEND DATA=&udmmart..MD_AUDIENCE  BASE=&trglib..MD_AUDIENCE (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_AUDIENCE , MD_AUDIENCE );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_AUDIENCE;
         DROP TABLE work.MD_AUDIENCE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_AUDIENCE_OCCURRENCE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_AUDIENCE_OCCURRENCE) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_AUDIENCE_OCCURRENCE , MD_AUDIENCE_OCCURRENCE );
   PROC APPEND DATA=&udmmart..MD_AUDIENCE_OCCURRENCE  BASE=&trglib..MD_AUDIENCE_OCCURRENCE (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_AUDIENCE_OCCURRENCE , MD_AUDIENCE_OCCURRENCE );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_AUDIENCE_OCCURRENCE;
         DROP TABLE work.MD_AUDIENCE_OCCURRENCE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_AUDIENCE_X_SEGMENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_AUDIENCE_X_SEGMENT) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_AUDIENCE_X_SEGMENT , MD_AUDIENCE_X_SEGMENT );
   PROC APPEND DATA=&udmmart..MD_AUDIENCE_X_SEGMENT  BASE=&trglib..MD_AUDIENCE_X_SEGMENT (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_AUDIENCE_X_SEGMENT , MD_AUDIENCE_X_SEGMENT );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_AUDIENCE_X_SEGMENT;
         DROP TABLE work.MD_AUDIENCE_X_SEGMENT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_BU));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_BU) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_BU , MD_BU );
   PROC APPEND DATA=&udmmart..MD_BU  BASE=&trglib..MD_BU (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_BU , MD_BU );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_BU;
         DROP TABLE work.MD_BU;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_BUSINESS_CONTEXT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_BUSINESS_CONTEXT) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_BUSINESS_CONTEXT , MD_BUSINESS_CONTEXT );
   PROC APPEND DATA=&udmmart..MD_BUSINESS_CONTEXT  BASE=&trglib..MD_BUSINESS_CONTEXT (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_BUSINESS_CONTEXT , MD_BUSINESS_CONTEXT );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_BUSINESS_CONTEXT;
         DROP TABLE work.MD_BUSINESS_CONTEXT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_BUSINESS_CONTEXT_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_BUSINESS_CONTEXT_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_BUSINESS_CONTEXT_ALL , MD_BUSINESS_CONTEXT_ALL );
   PROC APPEND DATA=&udmmart..MD_BUSINESS_CONTEXT_ALL  BASE=&trglib..MD_BUSINESS_CONTEXT_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_BUSINESS_CONTEXT_ALL , MD_BUSINESS_CONTEXT_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_BUSINESS_CONTEXT_ALL;
         DROP TABLE work.MD_BUSINESS_CONTEXT_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_COSTCENTER));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_COSTCENTER) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_COSTCENTER , MD_COSTCENTER );
   PROC APPEND DATA=&udmmart..MD_COSTCENTER  BASE=&trglib..MD_COSTCENTER (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_COSTCENTER , MD_COSTCENTER );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_COSTCENTER;
         DROP TABLE work.MD_COSTCENTER;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_COST_CATEGORY));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_COST_CATEGORY) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_COST_CATEGORY , MD_COST_CATEGORY );
   PROC APPEND DATA=&udmmart..MD_COST_CATEGORY  BASE=&trglib..MD_COST_CATEGORY (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_COST_CATEGORY , MD_COST_CATEGORY );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_COST_CATEGORY;
         DROP TABLE work.MD_COST_CATEGORY;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_CREATIVE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_CREATIVE) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_CREATIVE , MD_CREATIVE );
   PROC APPEND DATA=&udmmart..MD_CREATIVE  BASE=&trglib..MD_CREATIVE (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_CREATIVE , MD_CREATIVE );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_CREATIVE;
         DROP TABLE work.MD_CREATIVE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_CREATIVE_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_CREATIVE_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_CREATIVE_ALL , MD_CREATIVE_ALL );
   PROC APPEND DATA=&udmmart..MD_CREATIVE_ALL  BASE=&trglib..MD_CREATIVE_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_CREATIVE_ALL , MD_CREATIVE_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_CREATIVE_ALL;
         DROP TABLE work.MD_CREATIVE_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_CREATIVE_CUSTOM_PROP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_CREATIVE_CUSTOM_PROP) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_CREATIVE_CUSTOM_PROP , MD_CREATIVE_CUSTOM_PROP );
   PROC APPEND DATA=&udmmart..MD_CREATIVE_CUSTOM_PROP  BASE=&trglib..MD_CREATIVE_CUSTOM_PROP (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_CREATIVE_CUSTOM_PROP , MD_CREATIVE_CUSTOM_PROP );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_CREATIVE_CUSTOM_PROP;
         DROP TABLE work.MD_CREATIVE_CUSTOM_PROP;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_CREATIVE_CUSTOM_PROP_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_CREATIVE_CUSTOM_PROP_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_CREATIVE_CUSTOM_PROP_ALL , MD_CREATIVE_CUSTOM_PROP_ALL );
   PROC APPEND DATA=&udmmart..MD_CREATIVE_CUSTOM_PROP_ALL  BASE=&trglib..MD_CREATIVE_CUSTOM_PROP_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_CREATIVE_CUSTOM_PROP_ALL , MD_CREATIVE_CUSTOM_PROP_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_CREATIVE_CUSTOM_PROP_ALL;
         DROP TABLE work.MD_CREATIVE_CUSTOM_PROP_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_CREATIVE_X_ASSET));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_CREATIVE_X_ASSET) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_CREATIVE_X_ASSET , MD_CREATIVE_X_ASSET );
   PROC APPEND DATA=&udmmart..MD_CREATIVE_X_ASSET  BASE=&trglib..MD_CREATIVE_X_ASSET (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_CREATIVE_X_ASSET , MD_CREATIVE_X_ASSET );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_CREATIVE_X_ASSET;
         DROP TABLE work.MD_CREATIVE_X_ASSET;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_CREATIVE_X_ASSET_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_CREATIVE_X_ASSET_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_CREATIVE_X_ASSET_ALL , MD_CREATIVE_X_ASSET_ALL );
   PROC APPEND DATA=&udmmart..MD_CREATIVE_X_ASSET_ALL  BASE=&trglib..MD_CREATIVE_X_ASSET_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_CREATIVE_X_ASSET_ALL , MD_CREATIVE_X_ASSET_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_CREATIVE_X_ASSET_ALL;
         DROP TABLE work.MD_CREATIVE_X_ASSET_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_CUSTATTRIB_TABLE_VALUES));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_CUSTATTRIB_TABLE_VALUES) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_CUSTATTRIB_TABLE_VALUES , MD_CUSTATTRIB_TABLE_VALUES );
   PROC APPEND DATA=&udmmart..MD_CUSTATTRIB_TABLE_VALUES  BASE=&trglib..MD_CUSTATTRIB_TABLE_VALUES (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_CUSTATTRIB_TABLE_VALUES , MD_CUSTATTRIB_TABLE_VALUES );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_CUSTATTRIB_TABLE_VALUES;
         DROP TABLE work.MD_CUSTATTRIB_TABLE_VALUES;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_CUST_ATTRIB));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_CUST_ATTRIB) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_CUST_ATTRIB , MD_CUST_ATTRIB );
   PROC APPEND DATA=&udmmart..MD_CUST_ATTRIB  BASE=&trglib..MD_CUST_ATTRIB (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_CUST_ATTRIB , MD_CUST_ATTRIB );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_CUST_ATTRIB;
         DROP TABLE work.MD_CUST_ATTRIB;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_DATAVIEW));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_DATAVIEW) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_DATAVIEW , MD_DATAVIEW );
   PROC APPEND DATA=&udmmart..MD_DATAVIEW  BASE=&trglib..MD_DATAVIEW (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_DATAVIEW , MD_DATAVIEW );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_DATAVIEW;
         DROP TABLE work.MD_DATAVIEW;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_DATAVIEW_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_DATAVIEW_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_DATAVIEW_ALL , MD_DATAVIEW_ALL );
   PROC APPEND DATA=&udmmart..MD_DATAVIEW_ALL  BASE=&trglib..MD_DATAVIEW_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_DATAVIEW_ALL , MD_DATAVIEW_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_DATAVIEW_ALL;
         DROP TABLE work.MD_DATAVIEW_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_DATAVIEW_X_EVENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_DATAVIEW_X_EVENT) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_DATAVIEW_X_EVENT , MD_DATAVIEW_X_EVENT );
   PROC APPEND DATA=&udmmart..MD_DATAVIEW_X_EVENT  BASE=&trglib..MD_DATAVIEW_X_EVENT (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_DATAVIEW_X_EVENT , MD_DATAVIEW_X_EVENT );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_DATAVIEW_X_EVENT;
         DROP TABLE work.MD_DATAVIEW_X_EVENT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_DATAVIEW_X_EVENT_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_DATAVIEW_X_EVENT_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_DATAVIEW_X_EVENT_ALL , MD_DATAVIEW_X_EVENT_ALL );
   PROC APPEND DATA=&udmmart..MD_DATAVIEW_X_EVENT_ALL  BASE=&trglib..MD_DATAVIEW_X_EVENT_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_DATAVIEW_X_EVENT_ALL , MD_DATAVIEW_X_EVENT_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_DATAVIEW_X_EVENT_ALL;
         DROP TABLE work.MD_DATAVIEW_X_EVENT_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_EVENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_EVENT) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_EVENT , MD_EVENT );
   PROC APPEND DATA=&udmmart..MD_EVENT  BASE=&trglib..MD_EVENT (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_EVENT , MD_EVENT );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_EVENT;
         DROP TABLE work.MD_EVENT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_EVENT_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_EVENT_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_EVENT_ALL , MD_EVENT_ALL );
   PROC APPEND DATA=&udmmart..MD_EVENT_ALL  BASE=&trglib..MD_EVENT_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_EVENT_ALL , MD_EVENT_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_EVENT_ALL;
         DROP TABLE work.MD_EVENT_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_FISCAL_PERIOD));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_FISCAL_PERIOD) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_FISCAL_PERIOD , MD_FISCAL_PERIOD );
   PROC APPEND DATA=&udmmart..MD_FISCAL_PERIOD  BASE=&trglib..MD_FISCAL_PERIOD (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_FISCAL_PERIOD , MD_FISCAL_PERIOD );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_FISCAL_PERIOD;
         DROP TABLE work.MD_FISCAL_PERIOD;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_GRID_ATTR_DEFN));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_GRID_ATTR_DEFN) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_GRID_ATTR_DEFN , MD_GRID_ATTR_DEFN );
   PROC APPEND DATA=&udmmart..MD_GRID_ATTR_DEFN  BASE=&trglib..MD_GRID_ATTR_DEFN (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_GRID_ATTR_DEFN , MD_GRID_ATTR_DEFN );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_GRID_ATTR_DEFN;
         DROP TABLE work.MD_GRID_ATTR_DEFN;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_JOURNEY));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_JOURNEY) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_JOURNEY , MD_JOURNEY );
   PROC APPEND DATA=&udmmart..MD_JOURNEY  BASE=&trglib..MD_JOURNEY (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_JOURNEY , MD_JOURNEY );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_JOURNEY;
         DROP TABLE work.MD_JOURNEY;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_JOURNEY_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_JOURNEY_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_JOURNEY_ALL , MD_JOURNEY_ALL );
   PROC APPEND DATA=&udmmart..MD_JOURNEY_ALL  BASE=&trglib..MD_JOURNEY_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_JOURNEY_ALL , MD_JOURNEY_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_JOURNEY_ALL;
         DROP TABLE work.MD_JOURNEY_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_JOURNEY_NODE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_JOURNEY_NODE) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_JOURNEY_NODE , MD_JOURNEY_NODE );
   PROC APPEND DATA=&udmmart..MD_JOURNEY_NODE  BASE=&trglib..MD_JOURNEY_NODE (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_JOURNEY_NODE , MD_JOURNEY_NODE );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_JOURNEY_NODE;
         DROP TABLE work.MD_JOURNEY_NODE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_JOURNEY_NODE_OCCURRENCE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_JOURNEY_NODE_OCCURRENCE) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_JOURNEY_NODE_OCCURRENCE , MD_JOURNEY_NODE_OCCURRENCE );
   PROC APPEND DATA=&udmmart..MD_JOURNEY_NODE_OCCURRENCE  BASE=&trglib..MD_JOURNEY_NODE_OCCURRENCE (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_JOURNEY_NODE_OCCURRENCE , MD_JOURNEY_NODE_OCCURRENCE );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_JOURNEY_NODE_OCCURRENCE;
         DROP TABLE work.MD_JOURNEY_NODE_OCCURRENCE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_JOURNEY_NODE_X_NEXT_NODE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_JOURNEY_NODE_X_NEXT_NODE) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_JOURNEY_NODE_X_NEXT_NODE , MD_JOURNEY_NODE_X_NEXT_NODE );
   PROC APPEND DATA=&udmmart..MD_JOURNEY_NODE_X_NEXT_NODE  BASE=&trglib..MD_JOURNEY_NODE_X_NEXT_NODE (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_JOURNEY_NODE_X_NEXT_NODE , MD_JOURNEY_NODE_X_NEXT_NODE );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_JOURNEY_NODE_X_NEXT_NODE;
         DROP TABLE work.MD_JOURNEY_NODE_X_NEXT_NODE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_JOURNEY_NODE_X_PREVIOUS_NODE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_JOURNEY_NODE_X_PREVIOUS_NODE) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_JOURNEY_NODE_X_PREVIOUS_NODE , MD_JOURNEY_NODE_X_PREVIOUS_NODE );
   PROC APPEND DATA=&udmmart..MD_JOURNEY_NODE_X_PREVIOUS_NODE  BASE=&trglib..MD_JOURNEY_NODE_X_PREVIOUS_NODE (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_JOURNEY_NODE_X_PREVIOUS_NODE , MD_JOURNEY_NODE_X_PREVIOUS_NODE );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_JOURNEY_NODE_X_PREVIOUS_NODE;
         DROP TABLE work.MD_JOURNEY_NODE_X_PREVIOUS_NODE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_JOURNEY_NODE_X_VARIANT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_JOURNEY_NODE_X_VARIANT) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_JOURNEY_NODE_X_VARIANT , MD_JOURNEY_NODE_X_VARIANT );
   PROC APPEND DATA=&udmmart..MD_JOURNEY_NODE_X_VARIANT  BASE=&trglib..MD_JOURNEY_NODE_X_VARIANT (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_JOURNEY_NODE_X_VARIANT , MD_JOURNEY_NODE_X_VARIANT );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_JOURNEY_NODE_X_VARIANT;
         DROP TABLE work.MD_JOURNEY_NODE_X_VARIANT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_JOURNEY_OCCURRENCE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_JOURNEY_OCCURRENCE) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_JOURNEY_OCCURRENCE , MD_JOURNEY_OCCURRENCE );
   PROC APPEND DATA=&udmmart..MD_JOURNEY_OCCURRENCE  BASE=&trglib..MD_JOURNEY_OCCURRENCE (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_JOURNEY_OCCURRENCE , MD_JOURNEY_OCCURRENCE );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_JOURNEY_OCCURRENCE;
         DROP TABLE work.MD_JOURNEY_OCCURRENCE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_JOURNEY_X_AUDIENCE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_JOURNEY_X_AUDIENCE) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_JOURNEY_X_AUDIENCE , MD_JOURNEY_X_AUDIENCE );
   PROC APPEND DATA=&udmmart..MD_JOURNEY_X_AUDIENCE  BASE=&trglib..MD_JOURNEY_X_AUDIENCE (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_JOURNEY_X_AUDIENCE , MD_JOURNEY_X_AUDIENCE );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_JOURNEY_X_AUDIENCE;
         DROP TABLE work.MD_JOURNEY_X_AUDIENCE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_JOURNEY_X_EVENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_JOURNEY_X_EVENT) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_JOURNEY_X_EVENT , MD_JOURNEY_X_EVENT );
   PROC APPEND DATA=&udmmart..MD_JOURNEY_X_EVENT  BASE=&trglib..MD_JOURNEY_X_EVENT (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_JOURNEY_X_EVENT , MD_JOURNEY_X_EVENT );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_JOURNEY_X_EVENT;
         DROP TABLE work.MD_JOURNEY_X_EVENT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_JOURNEY_X_TASK));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_JOURNEY_X_TASK) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_JOURNEY_X_TASK , MD_JOURNEY_X_TASK );
   PROC APPEND DATA=&udmmart..MD_JOURNEY_X_TASK  BASE=&trglib..MD_JOURNEY_X_TASK (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_JOURNEY_X_TASK , MD_JOURNEY_X_TASK );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_JOURNEY_X_TASK;
         DROP TABLE work.MD_JOURNEY_X_TASK;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_MESSAGE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_MESSAGE) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_MESSAGE , MD_MESSAGE );
   PROC APPEND DATA=&udmmart..MD_MESSAGE  BASE=&trglib..MD_MESSAGE (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_MESSAGE , MD_MESSAGE );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_MESSAGE;
         DROP TABLE work.MD_MESSAGE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_MESSAGE_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_MESSAGE_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_MESSAGE_ALL , MD_MESSAGE_ALL );
   PROC APPEND DATA=&udmmart..MD_MESSAGE_ALL  BASE=&trglib..MD_MESSAGE_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_MESSAGE_ALL , MD_MESSAGE_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_MESSAGE_ALL;
         DROP TABLE work.MD_MESSAGE_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_MESSAGE_CUSTOM_PROP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_MESSAGE_CUSTOM_PROP) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_MESSAGE_CUSTOM_PROP , MD_MESSAGE_CUSTOM_PROP );
   PROC APPEND DATA=&udmmart..MD_MESSAGE_CUSTOM_PROP  BASE=&trglib..MD_MESSAGE_CUSTOM_PROP (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_MESSAGE_CUSTOM_PROP , MD_MESSAGE_CUSTOM_PROP );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_MESSAGE_CUSTOM_PROP;
         DROP TABLE work.MD_MESSAGE_CUSTOM_PROP;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_MESSAGE_CUSTOM_PROP_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_MESSAGE_CUSTOM_PROP_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_MESSAGE_CUSTOM_PROP_ALL , MD_MESSAGE_CUSTOM_PROP_ALL );
   PROC APPEND DATA=&udmmart..MD_MESSAGE_CUSTOM_PROP_ALL  BASE=&trglib..MD_MESSAGE_CUSTOM_PROP_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_MESSAGE_CUSTOM_PROP_ALL , MD_MESSAGE_CUSTOM_PROP_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_MESSAGE_CUSTOM_PROP_ALL;
         DROP TABLE work.MD_MESSAGE_CUSTOM_PROP_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_MESSAGE_X_CREATIVE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_MESSAGE_X_CREATIVE) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_MESSAGE_X_CREATIVE , MD_MESSAGE_X_CREATIVE );
   PROC APPEND DATA=&udmmart..MD_MESSAGE_X_CREATIVE  BASE=&trglib..MD_MESSAGE_X_CREATIVE (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_MESSAGE_X_CREATIVE , MD_MESSAGE_X_CREATIVE );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_MESSAGE_X_CREATIVE;
         DROP TABLE work.MD_MESSAGE_X_CREATIVE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_MESSAGE_X_CREATIVE_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_MESSAGE_X_CREATIVE_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_MESSAGE_X_CREATIVE_ALL , MD_MESSAGE_X_CREATIVE_ALL );
   PROC APPEND DATA=&udmmart..MD_MESSAGE_X_CREATIVE_ALL  BASE=&trglib..MD_MESSAGE_X_CREATIVE_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_MESSAGE_X_CREATIVE_ALL , MD_MESSAGE_X_CREATIVE_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_MESSAGE_X_CREATIVE_ALL;
         DROP TABLE work.MD_MESSAGE_X_CREATIVE_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_OBJECT_TYPE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_OBJECT_TYPE) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_OBJECT_TYPE , MD_OBJECT_TYPE );
   PROC APPEND DATA=&udmmart..MD_OBJECT_TYPE  BASE=&trglib..MD_OBJECT_TYPE (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_OBJECT_TYPE , MD_OBJECT_TYPE );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_OBJECT_TYPE;
         DROP TABLE work.MD_OBJECT_TYPE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_OCCURRENCE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_OCCURRENCE) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_OCCURRENCE , MD_OCCURRENCE );
   PROC APPEND DATA=&udmmart..MD_OCCURRENCE  BASE=&trglib..MD_OCCURRENCE (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_OCCURRENCE , MD_OCCURRENCE );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_OCCURRENCE;
         DROP TABLE work.MD_OCCURRENCE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_PICKLIST));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_PICKLIST) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_PICKLIST , MD_PICKLIST );
   PROC APPEND DATA=&udmmart..MD_PICKLIST  BASE=&trglib..MD_PICKLIST (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_PICKLIST , MD_PICKLIST );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_PICKLIST;
         DROP TABLE work.MD_PICKLIST;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_PURPOSE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_PURPOSE) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_PURPOSE , MD_PURPOSE );
   PROC APPEND DATA=&udmmart..MD_PURPOSE  BASE=&trglib..MD_PURPOSE (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_PURPOSE , MD_PURPOSE );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_PURPOSE;
         DROP TABLE work.MD_PURPOSE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_RTC));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_RTC) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_RTC , MD_RTC );
   PROC APPEND DATA=&udmmart..MD_RTC  BASE=&trglib..MD_RTC (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_RTC , MD_RTC );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_RTC;
         DROP TABLE work.MD_RTC;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_SEGMENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_SEGMENT , MD_SEGMENT );
   PROC APPEND DATA=&udmmart..MD_SEGMENT  BASE=&trglib..MD_SEGMENT (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_SEGMENT , MD_SEGMENT );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT;
         DROP TABLE work.MD_SEGMENT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_SEGMENT_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_SEGMENT_ALL , MD_SEGMENT_ALL );
   PROC APPEND DATA=&udmmart..MD_SEGMENT_ALL  BASE=&trglib..MD_SEGMENT_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_SEGMENT_ALL , MD_SEGMENT_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_ALL;
         DROP TABLE work.MD_SEGMENT_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_SEGMENT_CUSTOM_PROP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_CUSTOM_PROP) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_SEGMENT_CUSTOM_PROP , MD_SEGMENT_CUSTOM_PROP );
   PROC APPEND DATA=&udmmart..MD_SEGMENT_CUSTOM_PROP  BASE=&trglib..MD_SEGMENT_CUSTOM_PROP (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_SEGMENT_CUSTOM_PROP , MD_SEGMENT_CUSTOM_PROP );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_CUSTOM_PROP;
         DROP TABLE work.MD_SEGMENT_CUSTOM_PROP;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_SEGMENT_CUSTOM_PROP_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_CUSTOM_PROP_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_SEGMENT_CUSTOM_PROP_ALL , MD_SEGMENT_CUSTOM_PROP_ALL );
   PROC APPEND DATA=&udmmart..MD_SEGMENT_CUSTOM_PROP_ALL  BASE=&trglib..MD_SEGMENT_CUSTOM_PROP_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_SEGMENT_CUSTOM_PROP_ALL , MD_SEGMENT_CUSTOM_PROP_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_CUSTOM_PROP_ALL;
         DROP TABLE work.MD_SEGMENT_CUSTOM_PROP_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_SEGMENT_MAP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_MAP) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_SEGMENT_MAP , MD_SEGMENT_MAP );
   PROC APPEND DATA=&udmmart..MD_SEGMENT_MAP  BASE=&trglib..MD_SEGMENT_MAP (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_SEGMENT_MAP , MD_SEGMENT_MAP );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_MAP;
         DROP TABLE work.MD_SEGMENT_MAP;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_SEGMENT_MAP_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_MAP_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_SEGMENT_MAP_ALL , MD_SEGMENT_MAP_ALL );
   PROC APPEND DATA=&udmmart..MD_SEGMENT_MAP_ALL  BASE=&trglib..MD_SEGMENT_MAP_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_SEGMENT_MAP_ALL , MD_SEGMENT_MAP_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_MAP_ALL;
         DROP TABLE work.MD_SEGMENT_MAP_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_SEGMENT_MAP_CUSTOM_PROP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_MAP_CUSTOM_PROP) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_SEGMENT_MAP_CUSTOM_PROP , MD_SEGMENT_MAP_CUSTOM_PROP );
   PROC APPEND DATA=&udmmart..MD_SEGMENT_MAP_CUSTOM_PROP  BASE=&trglib..MD_SEGMENT_MAP_CUSTOM_PROP (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_SEGMENT_MAP_CUSTOM_PROP , MD_SEGMENT_MAP_CUSTOM_PROP );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_MAP_CUSTOM_PROP;
         DROP TABLE work.MD_SEGMENT_MAP_CUSTOM_PROP;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_SEGMENT_MAP_CUSTOM_PROP_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_MAP_CUSTOM_PROP_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_SEGMENT_MAP_CUSTOM_PROP_ALL , MD_SEGMENT_MAP_CUSTOM_PROP_ALL );
   PROC APPEND DATA=&udmmart..MD_SEGMENT_MAP_CUSTOM_PROP_ALL  BASE=&trglib..MD_SEGMENT_MAP_CUSTOM_PROP_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_SEGMENT_MAP_CUSTOM_PROP_ALL , MD_SEGMENT_MAP_CUSTOM_PROP_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_MAP_CUSTOM_PROP_ALL;
         DROP TABLE work.MD_SEGMENT_MAP_CUSTOM_PROP_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_SEGMENT_MAP_X_SEGMENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_MAP_X_SEGMENT) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_SEGMENT_MAP_X_SEGMENT , MD_SEGMENT_MAP_X_SEGMENT );
   PROC APPEND DATA=&udmmart..MD_SEGMENT_MAP_X_SEGMENT  BASE=&trglib..MD_SEGMENT_MAP_X_SEGMENT (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_SEGMENT_MAP_X_SEGMENT , MD_SEGMENT_MAP_X_SEGMENT );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_MAP_X_SEGMENT;
         DROP TABLE work.MD_SEGMENT_MAP_X_SEGMENT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_SEGMENT_MAP_X_SEGMENT_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_MAP_X_SEGMENT_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_SEGMENT_MAP_X_SEGMENT_ALL , MD_SEGMENT_MAP_X_SEGMENT_ALL );
   PROC APPEND DATA=&udmmart..MD_SEGMENT_MAP_X_SEGMENT_ALL  BASE=&trglib..MD_SEGMENT_MAP_X_SEGMENT_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_SEGMENT_MAP_X_SEGMENT_ALL , MD_SEGMENT_MAP_X_SEGMENT_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_MAP_X_SEGMENT_ALL;
         DROP TABLE work.MD_SEGMENT_MAP_X_SEGMENT_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_SEGMENT_TEST));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_TEST) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_SEGMENT_TEST , MD_SEGMENT_TEST );
   PROC APPEND DATA=&udmmart..MD_SEGMENT_TEST  BASE=&trglib..MD_SEGMENT_TEST (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_SEGMENT_TEST , MD_SEGMENT_TEST );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_TEST;
         DROP TABLE work.MD_SEGMENT_TEST;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_SEGMENT_TEST_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_TEST_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_SEGMENT_TEST_ALL , MD_SEGMENT_TEST_ALL );
   PROC APPEND DATA=&udmmart..MD_SEGMENT_TEST_ALL  BASE=&trglib..MD_SEGMENT_TEST_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_SEGMENT_TEST_ALL , MD_SEGMENT_TEST_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_TEST_ALL;
         DROP TABLE work.MD_SEGMENT_TEST_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_SEGMENT_TEST_X_SEGMENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_TEST_X_SEGMENT) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_SEGMENT_TEST_X_SEGMENT , MD_SEGMENT_TEST_X_SEGMENT );
   PROC APPEND DATA=&udmmart..MD_SEGMENT_TEST_X_SEGMENT  BASE=&trglib..MD_SEGMENT_TEST_X_SEGMENT (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_SEGMENT_TEST_X_SEGMENT , MD_SEGMENT_TEST_X_SEGMENT );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_TEST_X_SEGMENT;
         DROP TABLE work.MD_SEGMENT_TEST_X_SEGMENT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_SEGMENT_TEST_X_SEGMENT_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_TEST_X_SEGMENT_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_SEGMENT_TEST_X_SEGMENT_ALL , MD_SEGMENT_TEST_X_SEGMENT_ALL );
   PROC APPEND DATA=&udmmart..MD_SEGMENT_TEST_X_SEGMENT_ALL  BASE=&trglib..MD_SEGMENT_TEST_X_SEGMENT_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_SEGMENT_TEST_X_SEGMENT_ALL , MD_SEGMENT_TEST_X_SEGMENT_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_TEST_X_SEGMENT_ALL;
         DROP TABLE work.MD_SEGMENT_TEST_X_SEGMENT_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_SEGMENT_X_EVENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_X_EVENT) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_SEGMENT_X_EVENT , MD_SEGMENT_X_EVENT );
   PROC APPEND DATA=&udmmart..MD_SEGMENT_X_EVENT  BASE=&trglib..MD_SEGMENT_X_EVENT (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_SEGMENT_X_EVENT , MD_SEGMENT_X_EVENT );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_X_EVENT;
         DROP TABLE work.MD_SEGMENT_X_EVENT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_SEGMENT_X_EVENT_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_X_EVENT_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_SEGMENT_X_EVENT_ALL , MD_SEGMENT_X_EVENT_ALL );
   PROC APPEND DATA=&udmmart..MD_SEGMENT_X_EVENT_ALL  BASE=&trglib..MD_SEGMENT_X_EVENT_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_SEGMENT_X_EVENT_ALL , MD_SEGMENT_X_EVENT_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_X_EVENT_ALL;
         DROP TABLE work.MD_SEGMENT_X_EVENT_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_SPOT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_SPOT) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_SPOT , MD_SPOT );
   PROC APPEND DATA=&udmmart..MD_SPOT  BASE=&trglib..MD_SPOT (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_SPOT , MD_SPOT );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SPOT;
         DROP TABLE work.MD_SPOT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_SPOT_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_SPOT_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_SPOT_ALL , MD_SPOT_ALL );
   PROC APPEND DATA=&udmmart..MD_SPOT_ALL  BASE=&trglib..MD_SPOT_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_SPOT_ALL , MD_SPOT_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SPOT_ALL;
         DROP TABLE work.MD_SPOT_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_TARGET_ASSIST));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_TARGET_ASSIST) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_TARGET_ASSIST , MD_TARGET_ASSIST );
   PROC APPEND DATA=&udmmart..MD_TARGET_ASSIST  BASE=&trglib..MD_TARGET_ASSIST (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_TARGET_ASSIST , MD_TARGET_ASSIST );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TARGET_ASSIST;
         DROP TABLE work.MD_TARGET_ASSIST;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_TASK));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_TASK , MD_TASK );
   PROC APPEND DATA=&udmmart..MD_TASK  BASE=&trglib..MD_TASK (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_TASK , MD_TASK );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK;
         DROP TABLE work.MD_TASK;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_TASK_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_TASK_ALL , MD_TASK_ALL );
   PROC APPEND DATA=&udmmart..MD_TASK_ALL  BASE=&trglib..MD_TASK_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_TASK_ALL , MD_TASK_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_ALL;
         DROP TABLE work.MD_TASK_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_TASK_CUSTOM_PROP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_CUSTOM_PROP) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_TASK_CUSTOM_PROP , MD_TASK_CUSTOM_PROP );
   PROC APPEND DATA=&udmmart..MD_TASK_CUSTOM_PROP  BASE=&trglib..MD_TASK_CUSTOM_PROP (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_TASK_CUSTOM_PROP , MD_TASK_CUSTOM_PROP );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_CUSTOM_PROP;
         DROP TABLE work.MD_TASK_CUSTOM_PROP;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_TASK_CUSTOM_PROP_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_CUSTOM_PROP_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_TASK_CUSTOM_PROP_ALL , MD_TASK_CUSTOM_PROP_ALL );
   PROC APPEND DATA=&udmmart..MD_TASK_CUSTOM_PROP_ALL  BASE=&trglib..MD_TASK_CUSTOM_PROP_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_TASK_CUSTOM_PROP_ALL , MD_TASK_CUSTOM_PROP_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_CUSTOM_PROP_ALL;
         DROP TABLE work.MD_TASK_CUSTOM_PROP_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_TASK_X_AUDIENCE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_AUDIENCE) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_TASK_X_AUDIENCE , MD_TASK_X_AUDIENCE );
   PROC APPEND DATA=&udmmart..MD_TASK_X_AUDIENCE  BASE=&trglib..MD_TASK_X_AUDIENCE (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_TASK_X_AUDIENCE , MD_TASK_X_AUDIENCE );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_AUDIENCE;
         DROP TABLE work.MD_TASK_X_AUDIENCE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_TASK_X_CREATIVE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_CREATIVE) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_TASK_X_CREATIVE , MD_TASK_X_CREATIVE );
   PROC APPEND DATA=&udmmart..MD_TASK_X_CREATIVE  BASE=&trglib..MD_TASK_X_CREATIVE (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_TASK_X_CREATIVE , MD_TASK_X_CREATIVE );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_CREATIVE;
         DROP TABLE work.MD_TASK_X_CREATIVE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_TASK_X_CREATIVE_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_CREATIVE_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_TASK_X_CREATIVE_ALL , MD_TASK_X_CREATIVE_ALL );
   PROC APPEND DATA=&udmmart..MD_TASK_X_CREATIVE_ALL  BASE=&trglib..MD_TASK_X_CREATIVE_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_TASK_X_CREATIVE_ALL , MD_TASK_X_CREATIVE_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_CREATIVE_ALL;
         DROP TABLE work.MD_TASK_X_CREATIVE_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_TASK_X_DATAVIEW));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_DATAVIEW) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_TASK_X_DATAVIEW , MD_TASK_X_DATAVIEW );
   PROC APPEND DATA=&udmmart..MD_TASK_X_DATAVIEW  BASE=&trglib..MD_TASK_X_DATAVIEW (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_TASK_X_DATAVIEW , MD_TASK_X_DATAVIEW );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_DATAVIEW;
         DROP TABLE work.MD_TASK_X_DATAVIEW;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_TASK_X_DATAVIEW_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_DATAVIEW_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_TASK_X_DATAVIEW_ALL , MD_TASK_X_DATAVIEW_ALL );
   PROC APPEND DATA=&udmmart..MD_TASK_X_DATAVIEW_ALL  BASE=&trglib..MD_TASK_X_DATAVIEW_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_TASK_X_DATAVIEW_ALL , MD_TASK_X_DATAVIEW_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_DATAVIEW_ALL;
         DROP TABLE work.MD_TASK_X_DATAVIEW_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_TASK_X_EVENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_EVENT) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_TASK_X_EVENT , MD_TASK_X_EVENT );
   PROC APPEND DATA=&udmmart..MD_TASK_X_EVENT  BASE=&trglib..MD_TASK_X_EVENT (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_TASK_X_EVENT , MD_TASK_X_EVENT );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_EVENT;
         DROP TABLE work.MD_TASK_X_EVENT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_TASK_X_EVENT_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_EVENT_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_TASK_X_EVENT_ALL , MD_TASK_X_EVENT_ALL );
   PROC APPEND DATA=&udmmart..MD_TASK_X_EVENT_ALL  BASE=&trglib..MD_TASK_X_EVENT_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_TASK_X_EVENT_ALL , MD_TASK_X_EVENT_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_EVENT_ALL;
         DROP TABLE work.MD_TASK_X_EVENT_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_TASK_X_MESSAGE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_MESSAGE) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_TASK_X_MESSAGE , MD_TASK_X_MESSAGE );
   PROC APPEND DATA=&udmmart..MD_TASK_X_MESSAGE  BASE=&trglib..MD_TASK_X_MESSAGE (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_TASK_X_MESSAGE , MD_TASK_X_MESSAGE );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_MESSAGE;
         DROP TABLE work.MD_TASK_X_MESSAGE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_TASK_X_MESSAGE_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_MESSAGE_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_TASK_X_MESSAGE_ALL , MD_TASK_X_MESSAGE_ALL );
   PROC APPEND DATA=&udmmart..MD_TASK_X_MESSAGE_ALL  BASE=&trglib..MD_TASK_X_MESSAGE_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_TASK_X_MESSAGE_ALL , MD_TASK_X_MESSAGE_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_MESSAGE_ALL;
         DROP TABLE work.MD_TASK_X_MESSAGE_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_TASK_X_SEGMENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_SEGMENT) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_TASK_X_SEGMENT , MD_TASK_X_SEGMENT );
   PROC APPEND DATA=&udmmart..MD_TASK_X_SEGMENT  BASE=&trglib..MD_TASK_X_SEGMENT (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_TASK_X_SEGMENT , MD_TASK_X_SEGMENT );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_SEGMENT;
         DROP TABLE work.MD_TASK_X_SEGMENT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_TASK_X_SEGMENT_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_SEGMENT_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_TASK_X_SEGMENT_ALL , MD_TASK_X_SEGMENT_ALL );
   PROC APPEND DATA=&udmmart..MD_TASK_X_SEGMENT_ALL  BASE=&trglib..MD_TASK_X_SEGMENT_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_TASK_X_SEGMENT_ALL , MD_TASK_X_SEGMENT_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_SEGMENT_ALL;
         DROP TABLE work.MD_TASK_X_SEGMENT_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_TASK_X_SPOT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_SPOT) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_TASK_X_SPOT , MD_TASK_X_SPOT );
   PROC APPEND DATA=&udmmart..MD_TASK_X_SPOT  BASE=&trglib..MD_TASK_X_SPOT (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_TASK_X_SPOT , MD_TASK_X_SPOT );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_SPOT;
         DROP TABLE work.MD_TASK_X_SPOT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_TASK_X_SPOT_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_SPOT_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_TASK_X_SPOT_ALL , MD_TASK_X_SPOT_ALL );
   PROC APPEND DATA=&udmmart..MD_TASK_X_SPOT_ALL  BASE=&trglib..MD_TASK_X_SPOT_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_TASK_X_SPOT_ALL , MD_TASK_X_SPOT_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_SPOT_ALL;
         DROP TABLE work.MD_TASK_X_SPOT_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_TASK_X_VARIANT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_VARIANT) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_TASK_X_VARIANT , MD_TASK_X_VARIANT );
   PROC APPEND DATA=&udmmart..MD_TASK_X_VARIANT  BASE=&trglib..MD_TASK_X_VARIANT (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_TASK_X_VARIANT , MD_TASK_X_VARIANT );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_VARIANT;
         DROP TABLE work.MD_TASK_X_VARIANT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_TASK_X_VARIANT_ALL));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_VARIANT_ALL) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_TASK_X_VARIANT_ALL , MD_TASK_X_VARIANT_ALL );
   PROC APPEND DATA=&udmmart..MD_TASK_X_VARIANT_ALL  BASE=&trglib..MD_TASK_X_VARIANT_ALL (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_TASK_X_VARIANT_ALL , MD_TASK_X_VARIANT_ALL );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_VARIANT_ALL;
         DROP TABLE work.MD_TASK_X_VARIANT_ALL;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_VENDOR));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_VENDOR) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_VENDOR , MD_VENDOR );
   PROC APPEND DATA=&udmmart..MD_VENDOR  BASE=&trglib..MD_VENDOR (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_VENDOR , MD_VENDOR );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_VENDOR;
         DROP TABLE work.MD_VENDOR;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_WF_PROCESS_DEF));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_WF_PROCESS_DEF) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_WF_PROCESS_DEF , MD_WF_PROCESS_DEF );
   PROC APPEND DATA=&udmmart..MD_WF_PROCESS_DEF  BASE=&trglib..MD_WF_PROCESS_DEF (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_WF_PROCESS_DEF , MD_WF_PROCESS_DEF );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_WF_PROCESS_DEF;
         DROP TABLE work.MD_WF_PROCESS_DEF;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_WF_PROCESS_DEF_ATTR_GRP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_WF_PROCESS_DEF_ATTR_GRP) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_WF_PROCESS_DEF_ATTR_GRP , MD_WF_PROCESS_DEF_ATTR_GRP );
   PROC APPEND DATA=&udmmart..MD_WF_PROCESS_DEF_ATTR_GRP  BASE=&trglib..MD_WF_PROCESS_DEF_ATTR_GRP (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_WF_PROCESS_DEF_ATTR_GRP , MD_WF_PROCESS_DEF_ATTR_GRP );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_WF_PROCESS_DEF_ATTR_GRP;
         DROP TABLE work.MD_WF_PROCESS_DEF_ATTR_GRP;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_WF_PROCESS_DEF_CATEGORIES));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_WF_PROCESS_DEF_CATEGORIES) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_WF_PROCESS_DEF_CATEGORIES , MD_WF_PROCESS_DEF_CATEGORIES );
   PROC APPEND DATA=&udmmart..MD_WF_PROCESS_DEF_CATEGORIES  BASE=&trglib..MD_WF_PROCESS_DEF_CATEGORIES (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_WF_PROCESS_DEF_CATEGORIES , MD_WF_PROCESS_DEF_CATEGORIES );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_WF_PROCESS_DEF_CATEGORIES;
         DROP TABLE work.MD_WF_PROCESS_DEF_CATEGORIES;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_WF_PROCESS_DEF_TASKS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_WF_PROCESS_DEF_TASKS) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_WF_PROCESS_DEF_TASKS , MD_WF_PROCESS_DEF_TASKS );
   PROC APPEND DATA=&udmmart..MD_WF_PROCESS_DEF_TASKS  BASE=&trglib..MD_WF_PROCESS_DEF_TASKS (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_WF_PROCESS_DEF_TASKS , MD_WF_PROCESS_DEF_TASKS );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_WF_PROCESS_DEF_TASKS;
         DROP TABLE work.MD_WF_PROCESS_DEF_TASKS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MD_WF_PROCESS_DEF_TASK_ASSG));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..MD_WF_PROCESS_DEF_TASK_ASSG) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate MD_WF_PROCESS_DEF_TASK_ASSG , MD_WF_PROCESS_DEF_TASK_ASSG );
   PROC APPEND DATA=&udmmart..MD_WF_PROCESS_DEF_TASK_ASSG  BASE=&trglib..MD_WF_PROCESS_DEF_TASK_ASSG (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to MD_WF_PROCESS_DEF_TASK_ASSG , MD_WF_PROCESS_DEF_TASK_ASSG );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_WF_PROCESS_DEF_TASK_ASSG;
         DROP TABLE work.MD_WF_PROCESS_DEF_TASK_ASSG;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MEDIA_ACTIVITY_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..MEDIA_ACTIVITY_DETAILS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..MEDIA_ACTIVITY_DETAILS_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=MEDIA_ACTIVITY_DETAILS , table_keys=%str(EVENT_ID), out_table=work.MEDIA_ACTIVITY_DETAILS );
   DATA work.MEDIA_ACTIVITY_DETAILS_tmp /VIEW=work.MEDIA_ACTIVITY_DETAILS_tmp ;
      SET work.MEDIA_ACTIVITY_DETAILS ;
      IF action_dttm_tz  NE . THEN action_dttm_tz =tzoneu2s(action_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :MEDIA_ACTIVITY_DETAILS_tmp , MEDIA_ACTIVITY_DETAILS );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..MEDIA_ACTIVITY_DETAILS_tmp ;
            SET work.MEDIA_ACTIVITY_DETAILS_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.MEDIA_ACTIVITY_DETAILS_tmp  BASE=&tmplib..MEDIA_ACTIVITY_DETAILS_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..MEDIA_ACTIVITY_DETAILS_tmp ;
            SET work.MEDIA_ACTIVITY_DETAILS_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :MEDIA_ACTIVITY_DETAILS_tmp , MEDIA_ACTIVITY_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..MEDIA_ACTIVITY_DETAILS AS b USING &tmpdbschema..MEDIA_ACTIVITY_DETAILS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            action_dttm = d.action_dttm, 
            action_dttm_tz = d.action_dttm_tz, load_dttm = d.load_dttm, 
            playhead_position = d.playhead_position, media_nm = d.media_nm, 
            detail_id = d.detail_id, action = d.action, 
            detail_id_hex = d.detail_id_hex, media_uri_txt = d.media_uri_txt
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            action_dttm, action_dttm_tz, load_dttm, 
            playhead_position, media_nm, event_id, detail_id, 
            action, detail_id_hex, media_uri_txt
         ) VALUES (
            d.action_dttm, d.action_dttm_tz, d.load_dttm, 
            d.playhead_position, d.media_nm, d.event_id, d.detail_id, 
            d.action, d.detail_id_hex, d.media_uri_txt  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :MEDIA_ACTIVITY_DETAILS_tmp , MEDIA_ACTIVITY_DETAILS , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..MEDIA_ACTIVITY_DETAILS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..MEDIA_ACTIVITY_DETAILS_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MEDIA_ACTIVITY_DETAILS;
         DROP TABLE work.MEDIA_ACTIVITY_DETAILS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MEDIA_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..MEDIA_DETAILS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..MEDIA_DETAILS_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=MEDIA_DETAILS , table_keys=%str(EVENT_ID), out_table=work.MEDIA_DETAILS );
   DATA work.MEDIA_DETAILS_tmp /VIEW=work.MEDIA_DETAILS_tmp ;
      SET work.MEDIA_DETAILS ;
      IF play_start_dttm_tz  NE . THEN play_start_dttm_tz =tzoneu2s(play_start_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :MEDIA_DETAILS_tmp , MEDIA_DETAILS );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..MEDIA_DETAILS_tmp ;
            SET work.MEDIA_DETAILS_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.MEDIA_DETAILS_tmp  BASE=&tmplib..MEDIA_DETAILS_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..MEDIA_DETAILS_tmp ;
            SET work.MEDIA_DETAILS_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :MEDIA_DETAILS_tmp , MEDIA_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..MEDIA_DETAILS AS b USING &tmpdbschema..MEDIA_DETAILS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            media_duration_secs = d.media_duration_secs, 
            load_dttm = d.load_dttm, play_start_dttm_tz = d.play_start_dttm_tz, 
            play_start_dttm = d.play_start_dttm, visit_id_hex = d.visit_id_hex, 
            visit_id = d.visit_id, session_id_hex = d.session_id_hex, 
            session_id = d.session_id, media_uri_txt = d.media_uri_txt, 
            media_player_nm = d.media_player_nm, media_nm = d.media_nm, 
            identity_id = d.identity_id, event_key_cd = d.event_key_cd, 
            detail_id_hex = d.detail_id_hex, detail_id = d.detail_id, 
            event_source_cd = d.event_source_cd, media_player_version_txt = d.media_player_version_txt
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            media_duration_secs, load_dttm, play_start_dttm_tz, 
            play_start_dttm, visit_id_hex, visit_id, session_id_hex, 
            session_id, media_uri_txt, media_player_nm, media_nm, 
            identity_id, event_key_cd, detail_id_hex, detail_id, 
            event_id, event_source_cd, media_player_version_txt
         ) VALUES (
            d.media_duration_secs, d.load_dttm, d.play_start_dttm_tz, 
            d.play_start_dttm, d.visit_id_hex, d.visit_id, d.session_id_hex, 
            d.session_id, d.media_uri_txt, d.media_player_nm, d.media_nm, 
            d.identity_id, d.event_key_cd, d.detail_id_hex, d.detail_id, 
            d.event_id, d.event_source_cd, d.media_player_version_txt  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :MEDIA_DETAILS_tmp , MEDIA_DETAILS , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..MEDIA_DETAILS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..MEDIA_DETAILS_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MEDIA_DETAILS;
         DROP TABLE work.MEDIA_DETAILS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MEDIA_DETAILS_EXT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..MEDIA_DETAILS_EXT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..MEDIA_DETAILS_EXT_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=MEDIA_DETAILS_EXT , table_keys=%str(EVENT_ID), out_table=work.MEDIA_DETAILS_EXT );
   DATA work.MEDIA_DETAILS_EXT_tmp /VIEW=work.MEDIA_DETAILS_EXT_tmp ;
      SET work.MEDIA_DETAILS_EXT ;
      IF play_end_dttm_tz  NE . THEN play_end_dttm_tz =tzoneu2s(play_end_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :MEDIA_DETAILS_EXT_tmp , MEDIA_DETAILS_EXT );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..MEDIA_DETAILS_EXT_tmp ;
            SET work.MEDIA_DETAILS_EXT_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.MEDIA_DETAILS_EXT_tmp  BASE=&tmplib..MEDIA_DETAILS_EXT_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..MEDIA_DETAILS_EXT_tmp ;
            SET work.MEDIA_DETAILS_EXT_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :MEDIA_DETAILS_EXT_tmp , MEDIA_DETAILS_EXT );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..MEDIA_DETAILS_EXT AS b USING &tmpdbschema..MEDIA_DETAILS_EXT_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            media_display_duration_secs = d.media_display_duration_secs, 
            view_duration_secs = d.view_duration_secs, end_tm = d.end_tm, 
            start_tm = d.start_tm, exit_point_secs = d.exit_point_secs, 
            max_play_secs = d.max_play_secs, interaction_cnt = d.interaction_cnt, 
            play_end_dttm = d.play_end_dttm, play_end_dttm_tz = d.play_end_dttm_tz, 
            load_dttm = d.load_dttm, media_uri_txt = d.media_uri_txt, 
            media_nm = d.media_nm, detail_id_hex = d.detail_id_hex, 
            detail_id = d.detail_id
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            media_display_duration_secs, view_duration_secs, end_tm, 
            start_tm, exit_point_secs, max_play_secs, interaction_cnt, 
            play_end_dttm, play_end_dttm_tz, load_dttm, media_uri_txt, 
            media_nm, event_id, detail_id_hex, detail_id
         ) VALUES (
            d.media_display_duration_secs, d.view_duration_secs, d.end_tm, 
            d.start_tm, d.exit_point_secs, d.max_play_secs, d.interaction_cnt, 
            d.play_end_dttm, d.play_end_dttm_tz, d.load_dttm, d.media_uri_txt, 
            d.media_nm, d.event_id, d.detail_id_hex, d.detail_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :MEDIA_DETAILS_EXT_tmp , MEDIA_DETAILS_EXT , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..MEDIA_DETAILS_EXT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..MEDIA_DETAILS_EXT_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MEDIA_DETAILS_EXT;
         DROP TABLE work.MEDIA_DETAILS_EXT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MOBILE_FOCUS_DEFOCUS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..MOBILE_FOCUS_DEFOCUS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..MOBILE_FOCUS_DEFOCUS_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=MOBILE_FOCUS_DEFOCUS , table_keys=%str(EVENT_ID), out_table=work.MOBILE_FOCUS_DEFOCUS );
   DATA work.MOBILE_FOCUS_DEFOCUS_tmp /VIEW=work.MOBILE_FOCUS_DEFOCUS_tmp ;
      SET work.MOBILE_FOCUS_DEFOCUS ;
      IF action_dttm_tz  NE . THEN action_dttm_tz =tzoneu2s(action_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :MOBILE_FOCUS_DEFOCUS_tmp , MOBILE_FOCUS_DEFOCUS );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..MOBILE_FOCUS_DEFOCUS_tmp ;
            SET work.MOBILE_FOCUS_DEFOCUS_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.MOBILE_FOCUS_DEFOCUS_tmp  BASE=&tmplib..MOBILE_FOCUS_DEFOCUS_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..MOBILE_FOCUS_DEFOCUS_tmp ;
            SET work.MOBILE_FOCUS_DEFOCUS_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :MOBILE_FOCUS_DEFOCUS_tmp , MOBILE_FOCUS_DEFOCUS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..MOBILE_FOCUS_DEFOCUS AS b USING &tmpdbschema..MOBILE_FOCUS_DEFOCUS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            action_dttm_tz = d.action_dttm_tz, 
            action_dttm = d.action_dttm, load_dttm = d.load_dttm, 
            visit_id_hex = d.visit_id_hex, session_id_hex = d.session_id_hex, 
            reserved_1_txt = d.reserved_1_txt, mobile_app_id = d.mobile_app_id, 
            identity_id = d.identity_id, event_nm = d.event_nm, 
            event_designed_id = d.event_designed_id, detail_id_hex = d.detail_id_hex, 
            channel_user_id = d.channel_user_id
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            action_dttm_tz, action_dttm, load_dttm, 
            visit_id_hex, session_id_hex, reserved_1_txt, mobile_app_id, 
            identity_id, event_nm, event_designed_id, detail_id_hex, 
            channel_user_id, event_id
         ) VALUES (
            d.action_dttm_tz, d.action_dttm, d.load_dttm, 
            d.visit_id_hex, d.session_id_hex, d.reserved_1_txt, d.mobile_app_id, 
            d.identity_id, d.event_nm, d.event_designed_id, d.detail_id_hex, 
            d.channel_user_id, d.event_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :MOBILE_FOCUS_DEFOCUS_tmp , MOBILE_FOCUS_DEFOCUS , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..MOBILE_FOCUS_DEFOCUS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..MOBILE_FOCUS_DEFOCUS_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MOBILE_FOCUS_DEFOCUS;
         DROP TABLE work.MOBILE_FOCUS_DEFOCUS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MOBILE_SPOTS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..MOBILE_SPOTS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..MOBILE_SPOTS_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=MOBILE_SPOTS , table_keys=%str(EVENT_ID), out_table=work.MOBILE_SPOTS );
   DATA work.MOBILE_SPOTS_tmp /VIEW=work.MOBILE_SPOTS_tmp ;
      SET work.MOBILE_SPOTS ;
      IF action_dttm_tz  NE . THEN action_dttm_tz =tzoneu2s(action_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :MOBILE_SPOTS_tmp , MOBILE_SPOTS );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..MOBILE_SPOTS_tmp ;
            SET work.MOBILE_SPOTS_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.MOBILE_SPOTS_tmp  BASE=&tmplib..MOBILE_SPOTS_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..MOBILE_SPOTS_tmp ;
            SET work.MOBILE_SPOTS_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :MOBILE_SPOTS_tmp , MOBILE_SPOTS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..MOBILE_SPOTS AS b USING &tmpdbschema..MOBILE_SPOTS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            action_dttm_tz = d.action_dttm_tz, 
            action_dttm = d.action_dttm, load_dttm = d.load_dttm, 
            visit_id_hex = d.visit_id_hex, spot_id = d.spot_id, 
            session_id_hex = d.session_id_hex, mobile_app_id = d.mobile_app_id, 
            event_nm = d.event_nm, event_designed_id = d.event_designed_id, 
            detail_id_hex = d.detail_id_hex, creative_id = d.creative_id, 
            context_type_nm = d.context_type_nm, channel_user_id = d.channel_user_id, 
            context_val = d.context_val, identity_id = d.identity_id
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            action_dttm_tz, action_dttm, load_dttm, 
            visit_id_hex, spot_id, session_id_hex, mobile_app_id, 
            event_nm, event_designed_id, detail_id_hex, creative_id, 
            context_type_nm, channel_user_id, context_val, event_id, 
            identity_id
         ) VALUES (
            d.action_dttm_tz, d.action_dttm, d.load_dttm, 
            d.visit_id_hex, d.spot_id, d.session_id_hex, d.mobile_app_id, 
            d.event_nm, d.event_designed_id, d.detail_id_hex, d.creative_id, 
            d.context_type_nm, d.channel_user_id, d.context_val, d.event_id, 
            d.identity_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :MOBILE_SPOTS_tmp , MOBILE_SPOTS , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..MOBILE_SPOTS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..MOBILE_SPOTS_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MOBILE_SPOTS;
         DROP TABLE work.MOBILE_SPOTS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..MONTHLY_USAGE));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..MONTHLY_USAGE_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..MONTHLY_USAGE_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=MONTHLY_USAGE , table_keys=%str(EVENT_MONTH), out_table=work.MONTHLY_USAGE );
   DATA work.MONTHLY_USAGE_tmp /VIEW=work.MONTHLY_USAGE_tmp ;
      SET work.MONTHLY_USAGE ;
   RUN;
   %err_check (Failed to add time zone adaptation :MONTHLY_USAGE_tmp , MONTHLY_USAGE );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..MONTHLY_USAGE_tmp ;
            SET work.MONTHLY_USAGE_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.MONTHLY_USAGE_tmp  BASE=&tmplib..MONTHLY_USAGE_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..MONTHLY_USAGE_tmp ;
            SET work.MONTHLY_USAGE_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :MONTHLY_USAGE_tmp , MONTHLY_USAGE );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..MONTHLY_USAGE AS b USING &tmpdbschema..MONTHLY_USAGE_tmp AS d ON (
            b.event_month = d.event_month )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_MONTH IS NOT NULL THEN INSERT (
            api_usage_str, bc_subjcnt_str, customer_profiles_processed_str, 
            web_impr_cnt, web_sesn_cnt, mob_sesn_cnt, email_preview_cnt, 
            outbound_api_cnt, facebook_ads_cnt, mobile_push_cnt, google_ads_cnt, 
            audience_usage_cnt, plan_users_cnt, email_send_cnt, linkedin_ads_cnt, 
            dm_destinations_total_row_cnt, mob_impr_cnt, dm_destinations_total_id_cnt, mobile_in_app_msg_cnt, 
            asset_size, db_size, admin_user_cnt, event_month
         ) VALUES (
            d.api_usage_str, d.bc_subjcnt_str, d.customer_profiles_processed_str, 
            d.web_impr_cnt, d.web_sesn_cnt, d.mob_sesn_cnt, d.email_preview_cnt, 
            d.outbound_api_cnt, d.facebook_ads_cnt, d.mobile_push_cnt, d.google_ads_cnt, 
            d.audience_usage_cnt, d.plan_users_cnt, d.email_send_cnt, d.linkedin_ads_cnt, 
            d.dm_destinations_total_row_cnt, d.mob_impr_cnt, d.dm_destinations_total_id_cnt, d.mobile_in_app_msg_cnt, 
            d.asset_size, d.db_size, d.admin_user_cnt, d.event_month  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :MONTHLY_USAGE_tmp , MONTHLY_USAGE , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..MONTHLY_USAGE_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..MONTHLY_USAGE_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MONTHLY_USAGE;
         DROP TABLE work.MONTHLY_USAGE;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..NOTIFICATION_FAILED));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..NOTIFICATION_FAILED_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..NOTIFICATION_FAILED_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=NOTIFICATION_FAILED , table_keys=%str(EVENT_ID), out_table=work.NOTIFICATION_FAILED );
   DATA work.NOTIFICATION_FAILED_tmp /VIEW=work.NOTIFICATION_FAILED_tmp ;
      SET work.NOTIFICATION_FAILED ;
      IF notification_failed_dttm_tz  NE . THEN notification_failed_dttm_tz =tzoneu2s(notification_failed_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :NOTIFICATION_FAILED_tmp , NOTIFICATION_FAILED );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..NOTIFICATION_FAILED_tmp ;
            SET work.NOTIFICATION_FAILED_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.NOTIFICATION_FAILED_tmp  BASE=&tmplib..NOTIFICATION_FAILED_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..NOTIFICATION_FAILED_tmp ;
            SET work.NOTIFICATION_FAILED_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :NOTIFICATION_FAILED_tmp , NOTIFICATION_FAILED );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..NOTIFICATION_FAILED AS b USING &tmpdbschema..NOTIFICATION_FAILED_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            properties_map_doc, notification_failed_dttm, notification_failed_dttm_tz, 
            load_dttm, task_id, segment_version_id, segment_id, 
            response_tracking_cd, occurrence_id, message_version_id, journey_id, 
            event_designed_id, creative_id, channel_user_id, channel_nm, 
            aud_occurrence_id, context_type_nm, error_cd, event_id, 
            event_nm, message_id, mobile_app_id, reserved_1_txt, 
            audience_id, context_val, creative_version_id, error_message_txt, 
            identity_id, journey_occurrence_id, reserved_2_txt, spot_id, 
            task_version_id
         ) VALUES (
            d.properties_map_doc, d.notification_failed_dttm, d.notification_failed_dttm_tz, 
            d.load_dttm, d.task_id, d.segment_version_id, d.segment_id, 
            d.response_tracking_cd, d.occurrence_id, d.message_version_id, d.journey_id, 
            d.event_designed_id, d.creative_id, d.channel_user_id, d.channel_nm, 
            d.aud_occurrence_id, d.context_type_nm, d.error_cd, d.event_id, 
            d.event_nm, d.message_id, d.mobile_app_id, d.reserved_1_txt, 
            d.audience_id, d.context_val, d.creative_version_id, d.error_message_txt, 
            d.identity_id, d.journey_occurrence_id, d.reserved_2_txt, d.spot_id, 
            d.task_version_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :NOTIFICATION_FAILED_tmp , NOTIFICATION_FAILED , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..NOTIFICATION_FAILED_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..NOTIFICATION_FAILED_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..NOTIFICATION_FAILED;
         DROP TABLE work.NOTIFICATION_FAILED;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..NOTIFICATION_OPENED));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..NOTIFICATION_OPENED_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..NOTIFICATION_OPENED_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=NOTIFICATION_OPENED , table_keys=%str(EVENT_ID), out_table=work.NOTIFICATION_OPENED );
   DATA work.NOTIFICATION_OPENED_tmp /VIEW=work.NOTIFICATION_OPENED_tmp ;
      SET work.NOTIFICATION_OPENED ;
      IF notification_opened_dttm_tz  NE . THEN notification_opened_dttm_tz =tzoneu2s(notification_opened_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :NOTIFICATION_OPENED_tmp , NOTIFICATION_OPENED );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..NOTIFICATION_OPENED_tmp ;
            SET work.NOTIFICATION_OPENED_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.NOTIFICATION_OPENED_tmp  BASE=&tmplib..NOTIFICATION_OPENED_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..NOTIFICATION_OPENED_tmp ;
            SET work.NOTIFICATION_OPENED_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :NOTIFICATION_OPENED_tmp , NOTIFICATION_OPENED );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..NOTIFICATION_OPENED AS b USING &tmpdbschema..NOTIFICATION_OPENED_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            properties_map_doc, load_dttm, notification_opened_dttm_tz, 
            notification_opened_dttm, task_version_id, segment_version_id, segment_id, 
            reserved_1_txt, message_id, identity_id, event_nm, 
            creative_id, channel_user_id, channel_nm, aud_occurrence_id, 
            context_type_nm, event_designed_id, journey_id, message_version_id, 
            occurrence_id, reserved_3_txt, spot_id, audience_id, 
            context_val, creative_version_id, event_id, journey_occurrence_id, 
            mobile_app_id, reserved_2_txt, response_tracking_cd, task_id
         ) VALUES (
            d.properties_map_doc, d.load_dttm, d.notification_opened_dttm_tz, 
            d.notification_opened_dttm, d.task_version_id, d.segment_version_id, d.segment_id, 
            d.reserved_1_txt, d.message_id, d.identity_id, d.event_nm, 
            d.creative_id, d.channel_user_id, d.channel_nm, d.aud_occurrence_id, 
            d.context_type_nm, d.event_designed_id, d.journey_id, d.message_version_id, 
            d.occurrence_id, d.reserved_3_txt, d.spot_id, d.audience_id, 
            d.context_val, d.creative_version_id, d.event_id, d.journey_occurrence_id, 
            d.mobile_app_id, d.reserved_2_txt, d.response_tracking_cd, d.task_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :NOTIFICATION_OPENED_tmp , NOTIFICATION_OPENED , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..NOTIFICATION_OPENED_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..NOTIFICATION_OPENED_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..NOTIFICATION_OPENED;
         DROP TABLE work.NOTIFICATION_OPENED;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..NOTIFICATION_SEND));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..NOTIFICATION_SEND_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..NOTIFICATION_SEND_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=NOTIFICATION_SEND , table_keys=%str(EVENT_ID), out_table=work.NOTIFICATION_SEND );
   DATA work.NOTIFICATION_SEND_tmp /VIEW=work.NOTIFICATION_SEND_tmp ;
      SET work.NOTIFICATION_SEND ;
      IF notification_send_dttm_tz  NE . THEN notification_send_dttm_tz =tzoneu2s(notification_send_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :NOTIFICATION_SEND_tmp , NOTIFICATION_SEND );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..NOTIFICATION_SEND_tmp ;
            SET work.NOTIFICATION_SEND_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.NOTIFICATION_SEND_tmp  BASE=&tmplib..NOTIFICATION_SEND_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..NOTIFICATION_SEND_tmp ;
            SET work.NOTIFICATION_SEND_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :NOTIFICATION_SEND_tmp , NOTIFICATION_SEND );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..NOTIFICATION_SEND AS b USING &tmpdbschema..NOTIFICATION_SEND_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            properties_map_doc, load_dttm, notification_send_dttm_tz, 
            notification_send_dttm, task_id, spot_id, reserved_2_txt, 
            occurrence_id, message_id, identity_id, creative_version_id, 
            channel_user_id, audience_id, context_val, event_id, 
            journey_id, journey_occurrence_id, mobile_app_id, reserved_1_txt, 
            segment_id, task_version_id, aud_occurrence_id, channel_nm, 
            context_type_nm, creative_id, event_designed_id, event_nm, 
            message_version_id, response_tracking_cd, segment_version_id
         ) VALUES (
            d.properties_map_doc, d.load_dttm, d.notification_send_dttm_tz, 
            d.notification_send_dttm, d.task_id, d.spot_id, d.reserved_2_txt, 
            d.occurrence_id, d.message_id, d.identity_id, d.creative_version_id, 
            d.channel_user_id, d.audience_id, d.context_val, d.event_id, 
            d.journey_id, d.journey_occurrence_id, d.mobile_app_id, d.reserved_1_txt, 
            d.segment_id, d.task_version_id, d.aud_occurrence_id, d.channel_nm, 
            d.context_type_nm, d.creative_id, d.event_designed_id, d.event_nm, 
            d.message_version_id, d.response_tracking_cd, d.segment_version_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :NOTIFICATION_SEND_tmp , NOTIFICATION_SEND , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..NOTIFICATION_SEND_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..NOTIFICATION_SEND_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..NOTIFICATION_SEND;
         DROP TABLE work.NOTIFICATION_SEND;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..NOTIFICATION_TARGETING_REQUEST));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..NOTIFICATION_TARGETING_REQUE_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..NOTIFICATION_TARGETING_REQUE_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=NOTIFICATION_TARGETING_REQUEST , table_keys=%str(EVENT_ID), out_table=work.NOTIFICATION_TARGETING_REQUEST );
   DATA work.NOTIFICATION_TARGETING_REQUE_tmp /VIEW=work.NOTIFICATION_TARGETING_REQUE_tmp ;
      SET work.NOTIFICATION_TARGETING_REQUEST ;
      IF notification_tgt_req_dttm_tz  NE . THEN notification_tgt_req_dttm_tz =tzoneu2s(notification_tgt_req_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :NOTIFICATION_TARGETING_REQUE_tmp , NOTIFICATION_TARGETING_REQUEST );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..NOTIFICATION_TARGETING_REQUE_tmp ;
            SET work.NOTIFICATION_TARGETING_REQUE_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.NOTIFICATION_TARGETING_REQUE_tmp  BASE=&tmplib..NOTIFICATION_TARGETING_REQUE_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..NOTIFICATION_TARGETING_REQUE_tmp ;
            SET work.NOTIFICATION_TARGETING_REQUE_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :NOTIFICATION_TARGETING_REQUE_tmp , NOTIFICATION_TARGETING_REQUEST );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..NOTIFICATION_TARGETING_REQUEST AS b USING &tmpdbschema..NOTIFICATION_TARGETING_REQUE_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            eligibility_flg = d.eligibility_flg, 
            notification_tgt_req_dttm = d.notification_tgt_req_dttm, load_dttm = d.load_dttm, 
            notification_tgt_req_dttm_tz = d.notification_tgt_req_dttm_tz, task_id = d.task_id, 
            mobile_app_id = d.mobile_app_id, event_nm = d.event_nm, 
            context_val = d.context_val, audience_id = d.audience_id, 
            channel_user_id = d.channel_user_id, event_designed_id = d.event_designed_id, 
            journey_id = d.journey_id, aud_occurrence_id = d.aud_occurrence_id, 
            channel_nm = d.channel_nm, context_type_nm = d.context_type_nm, 
            identity_id = d.identity_id, journey_occurrence_id = d.journey_occurrence_id
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            eligibility_flg, notification_tgt_req_dttm, load_dttm, 
            notification_tgt_req_dttm_tz, task_id, mobile_app_id, event_nm, 
            context_val, audience_id, channel_user_id, event_designed_id, 
            journey_id, aud_occurrence_id, channel_nm, context_type_nm, 
            event_id, identity_id, journey_occurrence_id
         ) VALUES (
            d.eligibility_flg, d.notification_tgt_req_dttm, d.load_dttm, 
            d.notification_tgt_req_dttm_tz, d.task_id, d.mobile_app_id, d.event_nm, 
            d.context_val, d.audience_id, d.channel_user_id, d.event_designed_id, 
            d.journey_id, d.aud_occurrence_id, d.channel_nm, d.context_type_nm, 
            d.event_id, d.identity_id, d.journey_occurrence_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :NOTIFICATION_TARGETING_REQUE_tmp , NOTIFICATION_TARGETING_REQUEST , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..NOTIFICATION_TARGETING_REQUE_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..NOTIFICATION_TARGETING_REQUE_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..NOTIFICATION_TARGETING_REQUEST;
         DROP TABLE work.NOTIFICATION_TARGETING_REQUEST;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..ORDER_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..ORDER_DETAILS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..ORDER_DETAILS_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=ORDER_DETAILS , table_keys=%str(EVENT_ID), out_table=work.ORDER_DETAILS );
   DATA work.ORDER_DETAILS_tmp /VIEW=work.ORDER_DETAILS_tmp ;
      SET work.ORDER_DETAILS ;
      IF activity_dttm_tz  NE . THEN activity_dttm_tz =tzoneu2s(activity_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :ORDER_DETAILS_tmp , ORDER_DETAILS );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..ORDER_DETAILS_tmp ;
            SET work.ORDER_DETAILS_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.ORDER_DETAILS_tmp  BASE=&tmplib..ORDER_DETAILS_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..ORDER_DETAILS_tmp ;
            SET work.ORDER_DETAILS_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :ORDER_DETAILS_tmp , ORDER_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..ORDER_DETAILS AS b USING &tmpdbschema..ORDER_DETAILS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            unit_price_amt, quantity_amt, properties_map_doc, 
            load_dttm, activity_dttm, activity_dttm_tz, visit_id, 
            session_id, record_type, product_id, mobile_app_id, 
            event_nm, event_key_cd, detail_id, cart_id, 
            availability_message_txt, channel_nm, event_designed_id, event_source_cd, 
            order_id, product_nm, product_sku, reserved_1_txt, 
            session_id_hex, shipping_message_txt, cart_nm, currency_cd, 
            detail_id_hex, event_id, identity_id, product_group_nm, 
            saving_message_txt, visit_id_hex
         ) VALUES (
            d.unit_price_amt, d.quantity_amt, d.properties_map_doc, 
            d.load_dttm, d.activity_dttm, d.activity_dttm_tz, d.visit_id, 
            d.session_id, d.record_type, d.product_id, d.mobile_app_id, 
            d.event_nm, d.event_key_cd, d.detail_id, d.cart_id, 
            d.availability_message_txt, d.channel_nm, d.event_designed_id, d.event_source_cd, 
            d.order_id, d.product_nm, d.product_sku, d.reserved_1_txt, 
            d.session_id_hex, d.shipping_message_txt, d.cart_nm, d.currency_cd, 
            d.detail_id_hex, d.event_id, d.identity_id, d.product_group_nm, 
            d.saving_message_txt, d.visit_id_hex  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :ORDER_DETAILS_tmp , ORDER_DETAILS , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..ORDER_DETAILS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..ORDER_DETAILS_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..ORDER_DETAILS;
         DROP TABLE work.ORDER_DETAILS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..ORDER_SUMMARY));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..ORDER_SUMMARY_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..ORDER_SUMMARY_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=ORDER_SUMMARY , table_keys=%str(EVENT_ID), out_table=work.ORDER_SUMMARY );
   DATA work.ORDER_SUMMARY_tmp /VIEW=work.ORDER_SUMMARY_tmp ;
      SET work.ORDER_SUMMARY ;
      IF activity_dttm_tz  NE . THEN activity_dttm_tz =tzoneu2s(activity_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :ORDER_SUMMARY_tmp , ORDER_SUMMARY );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..ORDER_SUMMARY_tmp ;
            SET work.ORDER_SUMMARY_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.ORDER_SUMMARY_tmp  BASE=&tmplib..ORDER_SUMMARY_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..ORDER_SUMMARY_tmp ;
            SET work.ORDER_SUMMARY_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :ORDER_SUMMARY_tmp , ORDER_SUMMARY );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..ORDER_SUMMARY AS b USING &tmpdbschema..ORDER_SUMMARY_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
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
         ) VALUES (
            d.total_price_amt, d.shipping_amt, d.total_tax_amt, 
            d.total_unit_qty, d.properties_map_doc, d.load_dttm, d.activity_dttm_tz, 
            d.activity_dttm, d.visit_id, d.shipping_postal_cd, d.session_id_hex, 
            d.payment_type_desc, d.identity_id, d.event_id, d.delivery_type_desc, 
            d.cart_id, d.billing_city_nm, d.billing_postal_cd, d.channel_nm, 
            d.detail_id_hex, d.event_nm, d.mobile_app_id, d.record_type, 
            d.shipping_city_nm, d.visit_id_hex, d.billing_country_nm, d.billing_state_region_cd, 
            d.cart_nm, d.currency_cd, d.detail_id, d.event_designed_id, 
            d.event_key_cd, d.event_source_cd, d.order_id, d.session_id, 
            d.shipping_country_nm, d.shipping_state_region_cd  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :ORDER_SUMMARY_tmp , ORDER_SUMMARY , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..ORDER_SUMMARY_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..ORDER_SUMMARY_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..ORDER_SUMMARY;
         DROP TABLE work.ORDER_SUMMARY;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..OUTBOUND_SYSTEM));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..OUTBOUND_SYSTEM_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..OUTBOUND_SYSTEM_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=OUTBOUND_SYSTEM , table_keys=%str(EVENT_ID), out_table=work.OUTBOUND_SYSTEM );
   DATA work.OUTBOUND_SYSTEM_tmp /VIEW=work.OUTBOUND_SYSTEM_tmp ;
      SET work.OUTBOUND_SYSTEM ;
      IF outbound_system_dttm_tz  NE . THEN outbound_system_dttm_tz =tzoneu2s(outbound_system_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :OUTBOUND_SYSTEM_tmp , OUTBOUND_SYSTEM );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..OUTBOUND_SYSTEM_tmp ;
            SET work.OUTBOUND_SYSTEM_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.OUTBOUND_SYSTEM_tmp  BASE=&tmplib..OUTBOUND_SYSTEM_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..OUTBOUND_SYSTEM_tmp ;
            SET work.OUTBOUND_SYSTEM_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :OUTBOUND_SYSTEM_tmp , OUTBOUND_SYSTEM );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..OUTBOUND_SYSTEM AS b USING &tmpdbschema..OUTBOUND_SYSTEM_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            properties_map_doc, outbound_system_dttm_tz, outbound_system_dttm, 
            load_dttm, visit_id_hex, session_id_hex, reserved_2_txt, 
            reserved_1_txt, parent_event_id, message_version_id, journey_id, 
            event_designed_id, context_val, audience_id, channel_nm, 
            channel_user_id, creative_id, creative_version_id, event_nm, 
            message_id, mobile_app_id, occurrence_id, segment_id, 
            task_id, aud_occurrence_id, context_type_nm, detail_id_hex, 
            event_id, identity_id, journey_occurrence_id, response_tracking_cd, 
            segment_version_id, spot_id, task_version_id
         ) VALUES (
            d.properties_map_doc, d.outbound_system_dttm_tz, d.outbound_system_dttm, 
            d.load_dttm, d.visit_id_hex, d.session_id_hex, d.reserved_2_txt, 
            d.reserved_1_txt, d.parent_event_id, d.message_version_id, d.journey_id, 
            d.event_designed_id, d.context_val, d.audience_id, d.channel_nm, 
            d.channel_user_id, d.creative_id, d.creative_version_id, d.event_nm, 
            d.message_id, d.mobile_app_id, d.occurrence_id, d.segment_id, 
            d.task_id, d.aud_occurrence_id, d.context_type_nm, d.detail_id_hex, 
            d.event_id, d.identity_id, d.journey_occurrence_id, d.response_tracking_cd, 
            d.segment_version_id, d.spot_id, d.task_version_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :OUTBOUND_SYSTEM_tmp , OUTBOUND_SYSTEM , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..OUTBOUND_SYSTEM_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..OUTBOUND_SYSTEM_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..OUTBOUND_SYSTEM;
         DROP TABLE work.OUTBOUND_SYSTEM;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..PAGE_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..PAGE_DETAILS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..PAGE_DETAILS_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=PAGE_DETAILS , table_keys=%str(EVENT_ID), out_table=work.PAGE_DETAILS );
   DATA work.PAGE_DETAILS_tmp /VIEW=work.PAGE_DETAILS_tmp ;
      SET work.PAGE_DETAILS ;
      IF detail_dttm_tz  NE . THEN detail_dttm_tz =tzoneu2s(detail_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :PAGE_DETAILS_tmp , PAGE_DETAILS );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..PAGE_DETAILS_tmp ;
            SET work.PAGE_DETAILS_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.PAGE_DETAILS_tmp  BASE=&tmplib..PAGE_DETAILS_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..PAGE_DETAILS_tmp ;
            SET work.PAGE_DETAILS_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :PAGE_DETAILS_tmp , PAGE_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..PAGE_DETAILS AS b USING &tmpdbschema..PAGE_DETAILS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
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
         ) VALUES (
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
            d.event_nm, d.identity_id, d.referrer_url_txt, d.window_size_txt  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :PAGE_DETAILS_tmp , PAGE_DETAILS , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..PAGE_DETAILS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..PAGE_DETAILS_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..PAGE_DETAILS;
         DROP TABLE work.PAGE_DETAILS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..PAGE_DETAILS_EXT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..PAGE_DETAILS_EXT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..PAGE_DETAILS_EXT_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=PAGE_DETAILS_EXT , table_keys=%str(DETAIL_ID,LOAD_DTTM,SESSION_ID), out_table=work.PAGE_DETAILS_EXT );
   DATA work.PAGE_DETAILS_EXT_tmp /VIEW=work.PAGE_DETAILS_EXT_tmp ;
      SET work.PAGE_DETAILS_EXT ;
   RUN;
   %err_check (Failed to add time zone adaptation :PAGE_DETAILS_EXT_tmp , PAGE_DETAILS_EXT );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..PAGE_DETAILS_EXT_tmp ;
            SET work.PAGE_DETAILS_EXT_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.PAGE_DETAILS_EXT_tmp  BASE=&tmplib..PAGE_DETAILS_EXT_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..PAGE_DETAILS_EXT_tmp ;
            SET work.PAGE_DETAILS_EXT_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :PAGE_DETAILS_EXT_tmp , PAGE_DETAILS_EXT );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..PAGE_DETAILS_EXT AS b USING &tmpdbschema..PAGE_DETAILS_EXT_tmp AS d ON (
            b.load_dttm = d.load_dttm AND 
            b.session_id = d.session_id AND b.detail_id = d.detail_id )
         WHEN MATCHED THEN
         UPDATE SET
            active_sec_spent_on_page_cnt = d.active_sec_spent_on_page_cnt, 
            seconds_spent_on_page_cnt = d.seconds_spent_on_page_cnt, detail_id_hex = d.detail_id_hex, 
            session_id_hex = d.session_id_hex
         WHEN NOT MATCHED AND DETAIL_ID IS NOT NULL AND LOAD_DTTM IS NOT NULL AND SESSION_ID IS NOT NULL THEN INSERT (
            active_sec_spent_on_page_cnt, seconds_spent_on_page_cnt, load_dttm, 
            session_id, detail_id, detail_id_hex, session_id_hex
         ) VALUES (
            d.active_sec_spent_on_page_cnt, d.seconds_spent_on_page_cnt, d.load_dttm, 
            d.session_id, d.detail_id, d.detail_id_hex, d.session_id_hex  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :PAGE_DETAILS_EXT_tmp , PAGE_DETAILS_EXT , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..PAGE_DETAILS_EXT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..PAGE_DETAILS_EXT_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..PAGE_DETAILS_EXT;
         DROP TABLE work.PAGE_DETAILS_EXT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..PAGE_ERRORS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..PAGE_ERRORS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..PAGE_ERRORS_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=PAGE_ERRORS , table_keys=%str(EVENT_ID), out_table=work.PAGE_ERRORS );
   DATA work.PAGE_ERRORS_tmp /VIEW=work.PAGE_ERRORS_tmp ;
      SET work.PAGE_ERRORS ;
      IF in_page_error_dttm_tz  NE . THEN in_page_error_dttm_tz =tzoneu2s(in_page_error_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :PAGE_ERRORS_tmp , PAGE_ERRORS );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..PAGE_ERRORS_tmp ;
            SET work.PAGE_ERRORS_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.PAGE_ERRORS_tmp  BASE=&tmplib..PAGE_ERRORS_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..PAGE_ERRORS_tmp ;
            SET work.PAGE_ERRORS_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :PAGE_ERRORS_tmp , PAGE_ERRORS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..PAGE_ERRORS AS b USING &tmpdbschema..PAGE_ERRORS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            in_page_error_dttm = d.in_page_error_dttm, 
            in_page_error_dttm_tz = d.in_page_error_dttm_tz, load_dttm = d.load_dttm, 
            visit_id_hex = d.visit_id_hex, session_id = d.session_id, 
            identity_id = d.identity_id, error_location_txt = d.error_location_txt, 
            detail_id_hex = d.detail_id_hex, in_page_error_txt = d.in_page_error_txt, 
            session_id_hex = d.session_id_hex, detail_id = d.detail_id, 
            event_source_cd = d.event_source_cd, visit_id = d.visit_id
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            in_page_error_dttm, in_page_error_dttm_tz, load_dttm, 
            visit_id_hex, session_id, identity_id, error_location_txt, 
            detail_id_hex, event_id, in_page_error_txt, session_id_hex, 
            detail_id, event_source_cd, visit_id
         ) VALUES (
            d.in_page_error_dttm, d.in_page_error_dttm_tz, d.load_dttm, 
            d.visit_id_hex, d.session_id, d.identity_id, d.error_location_txt, 
            d.detail_id_hex, d.event_id, d.in_page_error_txt, d.session_id_hex, 
            d.detail_id, d.event_source_cd, d.visit_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :PAGE_ERRORS_tmp , PAGE_ERRORS , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..PAGE_ERRORS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..PAGE_ERRORS_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..PAGE_ERRORS;
         DROP TABLE work.PAGE_ERRORS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..PLANNING_HIERARCHY_DEFN));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..PLANNING_HIERARCHY_DEFN) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate PLANNING_HIERARCHY_DEFN , PLANNING_HIERARCHY_DEFN );
   PROC APPEND DATA=&udmmart..PLANNING_HIERARCHY_DEFN  BASE=&trglib..PLANNING_HIERARCHY_DEFN (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to PLANNING_HIERARCHY_DEFN , PLANNING_HIERARCHY_DEFN );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..PLANNING_HIERARCHY_DEFN;
         DROP TABLE work.PLANNING_HIERARCHY_DEFN;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..PLANNING_INFO));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..PLANNING_INFO) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate PLANNING_INFO , PLANNING_INFO );
   PROC APPEND DATA=&udmmart..PLANNING_INFO  BASE=&trglib..PLANNING_INFO (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to PLANNING_INFO , PLANNING_INFO );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..PLANNING_INFO;
         DROP TABLE work.PLANNING_INFO;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..PLANNING_INFO_CUSTOM_PROP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..PLANNING_INFO_CUSTOM_PROP) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate PLANNING_INFO_CUSTOM_PROP , PLANNING_INFO_CUSTOM_PROP );
   PROC APPEND DATA=&udmmart..PLANNING_INFO_CUSTOM_PROP  BASE=&trglib..PLANNING_INFO_CUSTOM_PROP (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to PLANNING_INFO_CUSTOM_PROP , PLANNING_INFO_CUSTOM_PROP );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..PLANNING_INFO_CUSTOM_PROP;
         DROP TABLE work.PLANNING_INFO_CUSTOM_PROP;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..PRODUCT_VIEWS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..PRODUCT_VIEWS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..PRODUCT_VIEWS_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=PRODUCT_VIEWS , table_keys=%str(EVENT_ID), out_table=work.PRODUCT_VIEWS );
   DATA work.PRODUCT_VIEWS_tmp /VIEW=work.PRODUCT_VIEWS_tmp ;
      SET work.PRODUCT_VIEWS ;
      IF action_dttm_tz  NE . THEN action_dttm_tz =tzoneu2s(action_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :PRODUCT_VIEWS_tmp , PRODUCT_VIEWS );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..PRODUCT_VIEWS_tmp ;
            SET work.PRODUCT_VIEWS_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.PRODUCT_VIEWS_tmp  BASE=&tmplib..PRODUCT_VIEWS_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..PRODUCT_VIEWS_tmp ;
            SET work.PRODUCT_VIEWS_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :PRODUCT_VIEWS_tmp , PRODUCT_VIEWS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..PRODUCT_VIEWS AS b USING &tmpdbschema..PRODUCT_VIEWS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            price_val, properties_map_doc, action_dttm_tz, 
            load_dttm, action_dttm, visit_id_hex, visit_id, 
            saving_message_txt, product_id, mobile_app_id, event_nm, 
            event_key_cd, detail_id, availability_message_txt, channel_nm, 
            event_designed_id, event_source_cd, product_group_nm, product_sku, 
            session_id_hex, currency_cd, detail_id_hex, event_id, 
            identity_id, product_nm, session_id, shipping_message_txt
         ) VALUES (
            d.price_val, d.properties_map_doc, d.action_dttm_tz, 
            d.load_dttm, d.action_dttm, d.visit_id_hex, d.visit_id, 
            d.saving_message_txt, d.product_id, d.mobile_app_id, d.event_nm, 
            d.event_key_cd, d.detail_id, d.availability_message_txt, d.channel_nm, 
            d.event_designed_id, d.event_source_cd, d.product_group_nm, d.product_sku, 
            d.session_id_hex, d.currency_cd, d.detail_id_hex, d.event_id, 
            d.identity_id, d.product_nm, d.session_id, d.shipping_message_txt  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :PRODUCT_VIEWS_tmp , PRODUCT_VIEWS , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..PRODUCT_VIEWS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..PRODUCT_VIEWS_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..PRODUCT_VIEWS;
         DROP TABLE work.PRODUCT_VIEWS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..PROMOTION_DISPLAYED));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..PROMOTION_DISPLAYED_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..PROMOTION_DISPLAYED_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=PROMOTION_DISPLAYED , table_keys=%str(EVENT_ID), out_table=work.PROMOTION_DISPLAYED );
   DATA work.PROMOTION_DISPLAYED_tmp /VIEW=work.PROMOTION_DISPLAYED_tmp ;
      SET work.PROMOTION_DISPLAYED ;
      IF display_dttm_tz  NE . THEN display_dttm_tz =tzoneu2s(display_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :PROMOTION_DISPLAYED_tmp , PROMOTION_DISPLAYED );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..PROMOTION_DISPLAYED_tmp ;
            SET work.PROMOTION_DISPLAYED_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.PROMOTION_DISPLAYED_tmp  BASE=&tmplib..PROMOTION_DISPLAYED_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..PROMOTION_DISPLAYED_tmp ;
            SET work.PROMOTION_DISPLAYED_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :PROMOTION_DISPLAYED_tmp , PROMOTION_DISPLAYED );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..PROMOTION_DISPLAYED AS b USING &tmpdbschema..PROMOTION_DISPLAYED_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            derived_display_flg, promotion_number, properties_map_doc, 
            display_dttm_tz, load_dttm, display_dttm, session_id_hex, 
            promotion_tracking_cd, promotion_nm, promotion_creative_nm, event_source_cd, 
            event_designed_id, detail_id, channel_nm, detail_id_hex, 
            event_key_cd, mobile_app_id, promotion_placement_nm, session_id, 
            visit_id_hex, event_id, event_nm, identity_id, 
            promotion_type_nm, visit_id
         ) VALUES (
            d.derived_display_flg, d.promotion_number, d.properties_map_doc, 
            d.display_dttm_tz, d.load_dttm, d.display_dttm, d.session_id_hex, 
            d.promotion_tracking_cd, d.promotion_nm, d.promotion_creative_nm, d.event_source_cd, 
            d.event_designed_id, d.detail_id, d.channel_nm, d.detail_id_hex, 
            d.event_key_cd, d.mobile_app_id, d.promotion_placement_nm, d.session_id, 
            d.visit_id_hex, d.event_id, d.event_nm, d.identity_id, 
            d.promotion_type_nm, d.visit_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :PROMOTION_DISPLAYED_tmp , PROMOTION_DISPLAYED , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..PROMOTION_DISPLAYED_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..PROMOTION_DISPLAYED_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..PROMOTION_DISPLAYED;
         DROP TABLE work.PROMOTION_DISPLAYED;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..PROMOTION_USED));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..PROMOTION_USED_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..PROMOTION_USED_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=PROMOTION_USED , table_keys=%str(EVENT_ID), out_table=work.PROMOTION_USED );
   DATA work.PROMOTION_USED_tmp /VIEW=work.PROMOTION_USED_tmp ;
      SET work.PROMOTION_USED ;
      IF click_dttm_tz  NE . THEN click_dttm_tz =tzoneu2s(click_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :PROMOTION_USED_tmp , PROMOTION_USED );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..PROMOTION_USED_tmp ;
            SET work.PROMOTION_USED_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.PROMOTION_USED_tmp  BASE=&tmplib..PROMOTION_USED_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..PROMOTION_USED_tmp ;
            SET work.PROMOTION_USED_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :PROMOTION_USED_tmp , PROMOTION_USED );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..PROMOTION_USED AS b USING &tmpdbschema..PROMOTION_USED_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            promotion_number, properties_map_doc, click_dttm_tz, 
            click_dttm, load_dttm, session_id_hex, promotion_tracking_cd, 
            promotion_creative_nm, event_source_cd, event_id, event_designed_id, 
            detail_id, detail_id_hex, event_key_cd, mobile_app_id, 
            promotion_nm, promotion_placement_nm, session_id, visit_id_hex, 
            channel_nm, event_nm, identity_id, promotion_type_nm, 
            visit_id
         ) VALUES (
            d.promotion_number, d.properties_map_doc, d.click_dttm_tz, 
            d.click_dttm, d.load_dttm, d.session_id_hex, d.promotion_tracking_cd, 
            d.promotion_creative_nm, d.event_source_cd, d.event_id, d.event_designed_id, 
            d.detail_id, d.detail_id_hex, d.event_key_cd, d.mobile_app_id, 
            d.promotion_nm, d.promotion_placement_nm, d.session_id, d.visit_id_hex, 
            d.channel_nm, d.event_nm, d.identity_id, d.promotion_type_nm, 
            d.visit_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :PROMOTION_USED_tmp , PROMOTION_USED , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..PROMOTION_USED_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..PROMOTION_USED_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..PROMOTION_USED;
         DROP TABLE work.PROMOTION_USED;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..RESPONSE_HISTORY));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..RESPONSE_HISTORY_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..RESPONSE_HISTORY_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=RESPONSE_HISTORY , table_keys=%str(RESPONSE_ID), out_table=work.RESPONSE_HISTORY );
   DATA work.RESPONSE_HISTORY_tmp /VIEW=work.RESPONSE_HISTORY_tmp ;
      SET work.RESPONSE_HISTORY ;
      IF response_dttm_tz  NE . THEN response_dttm_tz =tzoneu2s(response_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :RESPONSE_HISTORY_tmp , RESPONSE_HISTORY );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..RESPONSE_HISTORY_tmp ;
            SET work.RESPONSE_HISTORY_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.RESPONSE_HISTORY_tmp  BASE=&tmplib..RESPONSE_HISTORY_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..RESPONSE_HISTORY_tmp ;
            SET work.RESPONSE_HISTORY_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :RESPONSE_HISTORY_tmp , RESPONSE_HISTORY );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..RESPONSE_HISTORY AS b USING &tmpdbschema..RESPONSE_HISTORY_tmp AS d ON (
            b.response_id = d.response_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND RESPONSE_ID IS NOT NULL THEN INSERT (
            properties_map_doc, load_dttm, response_dttm, 
            response_dttm_tz, session_id_hex, response_id, response_channel_nm, 
            parent_event_designed_id, journey_occurrence_id, detail_id_hex, audience_id, 
            context_type_nm, context_val, identity_id, message_id, 
            response_nm, task_version_id, aud_occurrence_id, creative_id, 
            event_designed_id, journey_id, occurrence_id, response_tracking_cd, 
            task_id, visit_id_hex
         ) VALUES (
            d.properties_map_doc, d.load_dttm, d.response_dttm, 
            d.response_dttm_tz, d.session_id_hex, d.response_id, d.response_channel_nm, 
            d.parent_event_designed_id, d.journey_occurrence_id, d.detail_id_hex, d.audience_id, 
            d.context_type_nm, d.context_val, d.identity_id, d.message_id, 
            d.response_nm, d.task_version_id, d.aud_occurrence_id, d.creative_id, 
            d.event_designed_id, d.journey_id, d.occurrence_id, d.response_tracking_cd, 
            d.task_id, d.visit_id_hex  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :RESPONSE_HISTORY_tmp , RESPONSE_HISTORY , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..RESPONSE_HISTORY_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..RESPONSE_HISTORY_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..RESPONSE_HISTORY;
         DROP TABLE work.RESPONSE_HISTORY;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..SEARCH_RESULTS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..SEARCH_RESULTS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SEARCH_RESULTS_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=SEARCH_RESULTS , table_keys=%str(EVENT_ID), out_table=work.SEARCH_RESULTS );
   DATA work.SEARCH_RESULTS_tmp /VIEW=work.SEARCH_RESULTS_tmp ;
      SET work.SEARCH_RESULTS ;
      IF search_results_dttm_tz  NE . THEN search_results_dttm_tz =tzoneu2s(search_results_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :SEARCH_RESULTS_tmp , SEARCH_RESULTS );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..SEARCH_RESULTS_tmp ;
            SET work.SEARCH_RESULTS_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.SEARCH_RESULTS_tmp  BASE=&tmplib..SEARCH_RESULTS_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..SEARCH_RESULTS_tmp ;
            SET work.SEARCH_RESULTS_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :SEARCH_RESULTS_tmp , SEARCH_RESULTS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..SEARCH_RESULTS AS b USING &tmpdbschema..SEARCH_RESULTS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            results_displayed_flg, search_results_displayed, properties_map_doc, 
            load_dttm, search_results_dttm, search_results_dttm_tz, visit_id_hex, 
            srch_field_name, srch_field_id, search_results_sk, search_nm, 
            identity_id, event_key_cd, event_id, channel_nm, 
            detail_id, detail_id_hex, event_nm, mobile_app_id, 
            session_id, srch_phrase, event_designed_id, event_source_cd, 
            session_id_hex, visit_id
         ) VALUES (
            d.results_displayed_flg, d.search_results_displayed, d.properties_map_doc, 
            d.load_dttm, d.search_results_dttm, d.search_results_dttm_tz, d.visit_id_hex, 
            d.srch_field_name, d.srch_field_id, d.search_results_sk, d.search_nm, 
            d.identity_id, d.event_key_cd, d.event_id, d.channel_nm, 
            d.detail_id, d.detail_id_hex, d.event_nm, d.mobile_app_id, 
            d.session_id, d.srch_phrase, d.event_designed_id, d.event_source_cd, 
            d.session_id_hex, d.visit_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :SEARCH_RESULTS_tmp , SEARCH_RESULTS , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..SEARCH_RESULTS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SEARCH_RESULTS_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..SEARCH_RESULTS;
         DROP TABLE work.SEARCH_RESULTS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..SEARCH_RESULTS_EXT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..SEARCH_RESULTS_EXT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SEARCH_RESULTS_EXT_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=SEARCH_RESULTS_EXT , table_keys=%str(EVENT_ID), out_table=work.SEARCH_RESULTS_EXT );
   DATA work.SEARCH_RESULTS_EXT_tmp /VIEW=work.SEARCH_RESULTS_EXT_tmp ;
      SET work.SEARCH_RESULTS_EXT ;
   RUN;
   %err_check (Failed to add time zone adaptation :SEARCH_RESULTS_EXT_tmp , SEARCH_RESULTS_EXT );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..SEARCH_RESULTS_EXT_tmp ;
            SET work.SEARCH_RESULTS_EXT_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.SEARCH_RESULTS_EXT_tmp  BASE=&tmplib..SEARCH_RESULTS_EXT_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..SEARCH_RESULTS_EXT_tmp ;
            SET work.SEARCH_RESULTS_EXT_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :SEARCH_RESULTS_EXT_tmp , SEARCH_RESULTS_EXT );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..SEARCH_RESULTS_EXT AS b USING &tmpdbschema..SEARCH_RESULTS_EXT_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            search_results_displayed = d.search_results_displayed, 
            load_dttm = d.load_dttm, search_results_sk = d.search_results_sk, 
            event_designed_id = d.event_designed_id
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            search_results_displayed, load_dttm, search_results_sk, 
            event_designed_id, event_id
         ) VALUES (
            d.search_results_displayed, d.load_dttm, d.search_results_sk, 
            d.event_designed_id, d.event_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :SEARCH_RESULTS_EXT_tmp , SEARCH_RESULTS_EXT , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..SEARCH_RESULTS_EXT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SEARCH_RESULTS_EXT_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..SEARCH_RESULTS_EXT;
         DROP TABLE work.SEARCH_RESULTS_EXT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..SESSION_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..SESSION_DETAILS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SESSION_DETAILS_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=SESSION_DETAILS , table_keys=%str(EVENT_ID), out_table=work.SESSION_DETAILS );
   DATA work.SESSION_DETAILS_tmp /VIEW=work.SESSION_DETAILS_tmp ;
      SET work.SESSION_DETAILS ;
      IF session_start_dttm_tz  NE . THEN session_start_dttm_tz =tzoneu2s(session_start_dttm_tz ,&timeZone_Value.);
      IF client_session_start_dttm_tz  NE . THEN client_session_start_dttm_tz =tzoneu2s(client_session_start_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :SESSION_DETAILS_tmp , SESSION_DETAILS );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..SESSION_DETAILS_tmp ;
            SET work.SESSION_DETAILS_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.SESSION_DETAILS_tmp  BASE=&tmplib..SESSION_DETAILS_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..SESSION_DETAILS_tmp ;
            SET work.SESSION_DETAILS_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :SESSION_DETAILS_tmp , SESSION_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..SESSION_DETAILS AS b USING &tmpdbschema..SESSION_DETAILS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
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
         ) VALUES (
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
            d.session_id_hex, d.user_language_cd  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :SESSION_DETAILS_tmp , SESSION_DETAILS , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..SESSION_DETAILS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SESSION_DETAILS_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..SESSION_DETAILS;
         DROP TABLE work.SESSION_DETAILS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..SESSION_DETAILS_EXT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..SESSION_DETAILS_EXT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SESSION_DETAILS_EXT_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=SESSION_DETAILS_EXT , table_keys=%str(LAST_SESSION_ACTIVITY_DTTM,SESSION_ID), out_table=work.SESSION_DETAILS_EXT );
   DATA work.SESSION_DETAILS_EXT_tmp /VIEW=work.SESSION_DETAILS_EXT_tmp ;
      SET work.SESSION_DETAILS_EXT ;
      IF last_session_activity_dttm_tz  NE . THEN last_session_activity_dttm_tz =tzoneu2s(last_session_activity_dttm_tz ,&timeZone_Value.);
      IF session_expiration_dttm_tz  NE . THEN session_expiration_dttm_tz =tzoneu2s(session_expiration_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :SESSION_DETAILS_EXT_tmp , SESSION_DETAILS_EXT );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..SESSION_DETAILS_EXT_tmp ;
            SET work.SESSION_DETAILS_EXT_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.SESSION_DETAILS_EXT_tmp  BASE=&tmplib..SESSION_DETAILS_EXT_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..SESSION_DETAILS_EXT_tmp ;
            SET work.SESSION_DETAILS_EXT_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :SESSION_DETAILS_EXT_tmp , SESSION_DETAILS_EXT );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..SESSION_DETAILS_EXT AS b USING &tmpdbschema..SESSION_DETAILS_EXT_tmp AS d ON (
            b.last_session_activity_dttm = d.last_session_activity_dttm AND 
            b.session_id = d.session_id )
         WHEN MATCHED THEN
         UPDATE SET
            active_sec_spent_in_sessn_cnt = d.active_sec_spent_in_sessn_cnt, 
            seconds_spent_in_session_cnt = d.seconds_spent_in_session_cnt, load_dttm = d.load_dttm, 
            session_expiration_dttm = d.session_expiration_dttm, last_session_activity_dttm_tz = d.last_session_activity_dttm_tz, 
            session_expiration_dttm_tz = d.session_expiration_dttm_tz, session_id_hex = d.session_id_hex
         WHEN NOT MATCHED AND LAST_SESSION_ACTIVITY_DTTM IS NOT NULL AND SESSION_ID IS NOT NULL THEN INSERT (
            active_sec_spent_in_sessn_cnt, seconds_spent_in_session_cnt, load_dttm, 
            last_session_activity_dttm, session_expiration_dttm, last_session_activity_dttm_tz, session_expiration_dttm_tz, 
            session_id, session_id_hex
         ) VALUES (
            d.active_sec_spent_in_sessn_cnt, d.seconds_spent_in_session_cnt, d.load_dttm, 
            d.last_session_activity_dttm, d.session_expiration_dttm, d.last_session_activity_dttm_tz, d.session_expiration_dttm_tz, 
            d.session_id, d.session_id_hex  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :SESSION_DETAILS_EXT_tmp , SESSION_DETAILS_EXT , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..SESSION_DETAILS_EXT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SESSION_DETAILS_EXT_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..SESSION_DETAILS_EXT;
         DROP TABLE work.SESSION_DETAILS_EXT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..SMS_MESSAGE_CLICKED));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..SMS_MESSAGE_CLICKED_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SMS_MESSAGE_CLICKED_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=SMS_MESSAGE_CLICKED , table_keys=%str(EVENT_ID), out_table=work.SMS_MESSAGE_CLICKED );
   DATA work.SMS_MESSAGE_CLICKED_tmp /VIEW=work.SMS_MESSAGE_CLICKED_tmp ;
      SET work.SMS_MESSAGE_CLICKED ;
      IF sms_click_dttm_tz  NE . THEN sms_click_dttm_tz =tzoneu2s(sms_click_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :SMS_MESSAGE_CLICKED_tmp , SMS_MESSAGE_CLICKED );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..SMS_MESSAGE_CLICKED_tmp ;
            SET work.SMS_MESSAGE_CLICKED_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.SMS_MESSAGE_CLICKED_tmp  BASE=&tmplib..SMS_MESSAGE_CLICKED_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..SMS_MESSAGE_CLICKED_tmp ;
            SET work.SMS_MESSAGE_CLICKED_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :SMS_MESSAGE_CLICKED_tmp , SMS_MESSAGE_CLICKED );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..SMS_MESSAGE_CLICKED AS b USING &tmpdbschema..SMS_MESSAGE_CLICKED_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            sms_click_dttm_tz, sms_click_dttm, load_dttm, 
            task_id, sms_message_id, sender_id, journey_occurrence_id, 
            event_nm, event_id, country_cd, audience_id, 
            aud_occurrence_id, context_type_nm, creative_version_id, identity_id, 
            occurrence_id, context_val, creative_id, event_designed_id, 
            journey_id, response_tracking_cd, task_version_id
         ) VALUES (
            d.sms_click_dttm_tz, d.sms_click_dttm, d.load_dttm, 
            d.task_id, d.sms_message_id, d.sender_id, d.journey_occurrence_id, 
            d.event_nm, d.event_id, d.country_cd, d.audience_id, 
            d.aud_occurrence_id, d.context_type_nm, d.creative_version_id, d.identity_id, 
            d.occurrence_id, d.context_val, d.creative_id, d.event_designed_id, 
            d.journey_id, d.response_tracking_cd, d.task_version_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :SMS_MESSAGE_CLICKED_tmp , SMS_MESSAGE_CLICKED , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..SMS_MESSAGE_CLICKED_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SMS_MESSAGE_CLICKED_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..SMS_MESSAGE_CLICKED;
         DROP TABLE work.SMS_MESSAGE_CLICKED;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..SMS_MESSAGE_DELIVERED));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..SMS_MESSAGE_DELIVERED_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SMS_MESSAGE_DELIVERED_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=SMS_MESSAGE_DELIVERED , table_keys=%str(EVENT_ID), out_table=work.SMS_MESSAGE_DELIVERED );
   DATA work.SMS_MESSAGE_DELIVERED_tmp /VIEW=work.SMS_MESSAGE_DELIVERED_tmp ;
      SET work.SMS_MESSAGE_DELIVERED ;
      IF sms_delivered_dttm_tz  NE . THEN sms_delivered_dttm_tz =tzoneu2s(sms_delivered_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :SMS_MESSAGE_DELIVERED_tmp , SMS_MESSAGE_DELIVERED );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..SMS_MESSAGE_DELIVERED_tmp ;
            SET work.SMS_MESSAGE_DELIVERED_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.SMS_MESSAGE_DELIVERED_tmp  BASE=&tmplib..SMS_MESSAGE_DELIVERED_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..SMS_MESSAGE_DELIVERED_tmp ;
            SET work.SMS_MESSAGE_DELIVERED_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :SMS_MESSAGE_DELIVERED_tmp , SMS_MESSAGE_DELIVERED );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..SMS_MESSAGE_DELIVERED AS b USING &tmpdbschema..SMS_MESSAGE_DELIVERED_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            sms_delivered_dttm_tz, sms_delivered_dttm, load_dttm, 
            sms_message_id, occurrence_id, journey_id, identity_id, 
            creative_version_id, context_type_nm, aud_occurrence_id, country_cd, 
            event_id, journey_occurrence_id, sender_id, task_id, 
            audience_id, context_val, creative_id, event_designed_id, 
            event_nm, response_tracking_cd, task_version_id
         ) VALUES (
            d.sms_delivered_dttm_tz, d.sms_delivered_dttm, d.load_dttm, 
            d.sms_message_id, d.occurrence_id, d.journey_id, d.identity_id, 
            d.creative_version_id, d.context_type_nm, d.aud_occurrence_id, d.country_cd, 
            d.event_id, d.journey_occurrence_id, d.sender_id, d.task_id, 
            d.audience_id, d.context_val, d.creative_id, d.event_designed_id, 
            d.event_nm, d.response_tracking_cd, d.task_version_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :SMS_MESSAGE_DELIVERED_tmp , SMS_MESSAGE_DELIVERED , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..SMS_MESSAGE_DELIVERED_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SMS_MESSAGE_DELIVERED_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..SMS_MESSAGE_DELIVERED;
         DROP TABLE work.SMS_MESSAGE_DELIVERED;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..SMS_MESSAGE_FAILED));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..SMS_MESSAGE_FAILED_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SMS_MESSAGE_FAILED_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=SMS_MESSAGE_FAILED , table_keys=%str(EVENT_ID), out_table=work.SMS_MESSAGE_FAILED );
   DATA work.SMS_MESSAGE_FAILED_tmp /VIEW=work.SMS_MESSAGE_FAILED_tmp ;
      SET work.SMS_MESSAGE_FAILED ;
      IF sms_failed_dttm_tz  NE . THEN sms_failed_dttm_tz =tzoneu2s(sms_failed_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :SMS_MESSAGE_FAILED_tmp , SMS_MESSAGE_FAILED );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..SMS_MESSAGE_FAILED_tmp ;
            SET work.SMS_MESSAGE_FAILED_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.SMS_MESSAGE_FAILED_tmp  BASE=&tmplib..SMS_MESSAGE_FAILED_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..SMS_MESSAGE_FAILED_tmp ;
            SET work.SMS_MESSAGE_FAILED_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :SMS_MESSAGE_FAILED_tmp , SMS_MESSAGE_FAILED );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..SMS_MESSAGE_FAILED AS b USING &tmpdbschema..SMS_MESSAGE_FAILED_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            sms_failed_dttm_tz, load_dttm, sms_failed_dttm, 
            task_version_id, task_id, sms_message_id, reason_description_txt, 
            journey_occurrence_id, event_id, creative_id, country_cd, 
            aud_occurrence_id, context_type_nm, creative_version_id, event_nm, 
            identity_id, occurrence_id, response_tracking_cd, sender_id, 
            audience_id, context_val, event_designed_id, journey_id, 
            reason_cd
         ) VALUES (
            d.sms_failed_dttm_tz, d.load_dttm, d.sms_failed_dttm, 
            d.task_version_id, d.task_id, d.sms_message_id, d.reason_description_txt, 
            d.journey_occurrence_id, d.event_id, d.creative_id, d.country_cd, 
            d.aud_occurrence_id, d.context_type_nm, d.creative_version_id, d.event_nm, 
            d.identity_id, d.occurrence_id, d.response_tracking_cd, d.sender_id, 
            d.audience_id, d.context_val, d.event_designed_id, d.journey_id, 
            d.reason_cd  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :SMS_MESSAGE_FAILED_tmp , SMS_MESSAGE_FAILED , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..SMS_MESSAGE_FAILED_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SMS_MESSAGE_FAILED_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..SMS_MESSAGE_FAILED;
         DROP TABLE work.SMS_MESSAGE_FAILED;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..SMS_MESSAGE_REPLY));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..SMS_MESSAGE_REPLY_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SMS_MESSAGE_REPLY_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=SMS_MESSAGE_REPLY , table_keys=%str(EVENT_ID), out_table=work.SMS_MESSAGE_REPLY );
   DATA work.SMS_MESSAGE_REPLY_tmp /VIEW=work.SMS_MESSAGE_REPLY_tmp ;
      SET work.SMS_MESSAGE_REPLY ;
      IF sms_reply_dttm_tz  NE . THEN sms_reply_dttm_tz =tzoneu2s(sms_reply_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :SMS_MESSAGE_REPLY_tmp , SMS_MESSAGE_REPLY );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..SMS_MESSAGE_REPLY_tmp ;
            SET work.SMS_MESSAGE_REPLY_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.SMS_MESSAGE_REPLY_tmp  BASE=&tmplib..SMS_MESSAGE_REPLY_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..SMS_MESSAGE_REPLY_tmp ;
            SET work.SMS_MESSAGE_REPLY_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :SMS_MESSAGE_REPLY_tmp , SMS_MESSAGE_REPLY );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..SMS_MESSAGE_REPLY AS b USING &tmpdbschema..SMS_MESSAGE_REPLY_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            load_dttm, sms_reply_dttm_tz, sms_reply_dttm, 
            task_version_id, sms_message_id, response_tracking_cd, occurrence_id, 
            identity_id, country_cd, aud_occurrence_id, context_type_nm, 
            event_id, journey_id, journey_occurrence_id, sender_id, 
            task_id, audience_id, context_val, event_designed_id, 
            event_nm, sms_content
         ) VALUES (
            d.load_dttm, d.sms_reply_dttm_tz, d.sms_reply_dttm, 
            d.task_version_id, d.sms_message_id, d.response_tracking_cd, d.occurrence_id, 
            d.identity_id, d.country_cd, d.aud_occurrence_id, d.context_type_nm, 
            d.event_id, d.journey_id, d.journey_occurrence_id, d.sender_id, 
            d.task_id, d.audience_id, d.context_val, d.event_designed_id, 
            d.event_nm, d.sms_content  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :SMS_MESSAGE_REPLY_tmp , SMS_MESSAGE_REPLY , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..SMS_MESSAGE_REPLY_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SMS_MESSAGE_REPLY_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..SMS_MESSAGE_REPLY;
         DROP TABLE work.SMS_MESSAGE_REPLY;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..SMS_MESSAGE_SEND));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..SMS_MESSAGE_SEND_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SMS_MESSAGE_SEND_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=SMS_MESSAGE_SEND , table_keys=%str(EVENT_ID), out_table=work.SMS_MESSAGE_SEND );
   DATA work.SMS_MESSAGE_SEND_tmp /VIEW=work.SMS_MESSAGE_SEND_tmp ;
      SET work.SMS_MESSAGE_SEND ;
      IF sms_send_dttm_tz  NE . THEN sms_send_dttm_tz =tzoneu2s(sms_send_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :SMS_MESSAGE_SEND_tmp , SMS_MESSAGE_SEND );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..SMS_MESSAGE_SEND_tmp ;
            SET work.SMS_MESSAGE_SEND_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.SMS_MESSAGE_SEND_tmp  BASE=&tmplib..SMS_MESSAGE_SEND_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..SMS_MESSAGE_SEND_tmp ;
            SET work.SMS_MESSAGE_SEND_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :SMS_MESSAGE_SEND_tmp , SMS_MESSAGE_SEND );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..SMS_MESSAGE_SEND AS b USING &tmpdbschema..SMS_MESSAGE_SEND_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            fragment_cnt, sms_send_dttm, sms_send_dttm_tz, 
            load_dttm, occurrence_id, identity_id, event_id, 
            event_designed_id, context_val, aud_occurrence_id, audience_id, 
            country_cd, creative_id, event_nm, journey_id, 
            journey_occurrence_id, sender_id, task_id, context_type_nm, 
            creative_version_id, response_tracking_cd, sms_message_id, task_version_id
         ) VALUES (
            d.fragment_cnt, d.sms_send_dttm, d.sms_send_dttm_tz, 
            d.load_dttm, d.occurrence_id, d.identity_id, d.event_id, 
            d.event_designed_id, d.context_val, d.aud_occurrence_id, d.audience_id, 
            d.country_cd, d.creative_id, d.event_nm, d.journey_id, 
            d.journey_occurrence_id, d.sender_id, d.task_id, d.context_type_nm, 
            d.creative_version_id, d.response_tracking_cd, d.sms_message_id, d.task_version_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :SMS_MESSAGE_SEND_tmp , SMS_MESSAGE_SEND , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..SMS_MESSAGE_SEND_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SMS_MESSAGE_SEND_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..SMS_MESSAGE_SEND;
         DROP TABLE work.SMS_MESSAGE_SEND;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..SMS_OPTOUT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..SMS_OPTOUT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SMS_OPTOUT_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=SMS_OPTOUT , table_keys=%str(EVENT_ID), out_table=work.SMS_OPTOUT );
   DATA work.SMS_OPTOUT_tmp /VIEW=work.SMS_OPTOUT_tmp ;
      SET work.SMS_OPTOUT ;
      IF sms_optout_dttm_tz  NE . THEN sms_optout_dttm_tz =tzoneu2s(sms_optout_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :SMS_OPTOUT_tmp , SMS_OPTOUT );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..SMS_OPTOUT_tmp ;
            SET work.SMS_OPTOUT_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.SMS_OPTOUT_tmp  BASE=&tmplib..SMS_OPTOUT_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..SMS_OPTOUT_tmp ;
            SET work.SMS_OPTOUT_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :SMS_OPTOUT_tmp , SMS_OPTOUT );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..SMS_OPTOUT AS b USING &tmpdbschema..SMS_OPTOUT_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            load_dttm, sms_optout_dttm, sms_optout_dttm_tz, 
            task_id, sms_message_id, sender_id, journey_occurrence_id, 
            event_id, country_cd, aud_occurrence_id, context_type_nm, 
            creative_version_id, identity_id, occurrence_id, audience_id, 
            context_val, creative_id, event_designed_id, event_nm, 
            journey_id, response_tracking_cd, task_version_id
         ) VALUES (
            d.load_dttm, d.sms_optout_dttm, d.sms_optout_dttm_tz, 
            d.task_id, d.sms_message_id, d.sender_id, d.journey_occurrence_id, 
            d.event_id, d.country_cd, d.aud_occurrence_id, d.context_type_nm, 
            d.creative_version_id, d.identity_id, d.occurrence_id, d.audience_id, 
            d.context_val, d.creative_id, d.event_designed_id, d.event_nm, 
            d.journey_id, d.response_tracking_cd, d.task_version_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :SMS_OPTOUT_tmp , SMS_OPTOUT , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..SMS_OPTOUT_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SMS_OPTOUT_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..SMS_OPTOUT;
         DROP TABLE work.SMS_OPTOUT;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..SMS_OPTOUT_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..SMS_OPTOUT_DETAILS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SMS_OPTOUT_DETAILS_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=SMS_OPTOUT_DETAILS , table_keys=%str(EVENT_ID), out_table=work.SMS_OPTOUT_DETAILS );
   DATA work.SMS_OPTOUT_DETAILS_tmp /VIEW=work.SMS_OPTOUT_DETAILS_tmp ;
      SET work.SMS_OPTOUT_DETAILS ;
      IF sms_optout_dttm_tz  NE . THEN sms_optout_dttm_tz =tzoneu2s(sms_optout_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :SMS_OPTOUT_DETAILS_tmp , SMS_OPTOUT_DETAILS );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..SMS_OPTOUT_DETAILS_tmp ;
            SET work.SMS_OPTOUT_DETAILS_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.SMS_OPTOUT_DETAILS_tmp  BASE=&tmplib..SMS_OPTOUT_DETAILS_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..SMS_OPTOUT_DETAILS_tmp ;
            SET work.SMS_OPTOUT_DETAILS_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :SMS_OPTOUT_DETAILS_tmp , SMS_OPTOUT_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..SMS_OPTOUT_DETAILS AS b USING &tmpdbschema..SMS_OPTOUT_DETAILS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            load_dttm, sms_optout_dttm, sms_optout_dttm_tz, 
            task_version_id, sms_message_id, occurrence_id, event_nm, 
            creative_id, context_type_nm, audience_id, address_val, 
            context_val, event_designed_id, journey_id, response_tracking_cd, 
            task_id, aud_occurrence_id, country_cd, creative_version_id, 
            event_id, identity_id, journey_occurrence_id, sender_id
         ) VALUES (
            d.load_dttm, d.sms_optout_dttm, d.sms_optout_dttm_tz, 
            d.task_version_id, d.sms_message_id, d.occurrence_id, d.event_nm, 
            d.creative_id, d.context_type_nm, d.audience_id, d.address_val, 
            d.context_val, d.event_designed_id, d.journey_id, d.response_tracking_cd, 
            d.task_id, d.aud_occurrence_id, d.country_cd, d.creative_version_id, 
            d.event_id, d.identity_id, d.journey_occurrence_id, d.sender_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :SMS_OPTOUT_DETAILS_tmp , SMS_OPTOUT_DETAILS , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..SMS_OPTOUT_DETAILS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SMS_OPTOUT_DETAILS_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..SMS_OPTOUT_DETAILS;
         DROP TABLE work.SMS_OPTOUT_DETAILS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..SPOT_CLICKED));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..SPOT_CLICKED_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SPOT_CLICKED_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=SPOT_CLICKED , table_keys=%str(EVENT_ID), out_table=work.SPOT_CLICKED );
   DATA work.SPOT_CLICKED_tmp /VIEW=work.SPOT_CLICKED_tmp ;
      SET work.SPOT_CLICKED ;
      IF spot_clicked_dttm_tz  NE . THEN spot_clicked_dttm_tz =tzoneu2s(spot_clicked_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :SPOT_CLICKED_tmp , SPOT_CLICKED );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..SPOT_CLICKED_tmp ;
            SET work.SPOT_CLICKED_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.SPOT_CLICKED_tmp  BASE=&tmplib..SPOT_CLICKED_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..SPOT_CLICKED_tmp ;
            SET work.SPOT_CLICKED_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :SPOT_CLICKED_tmp , SPOT_CLICKED );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..SPOT_CLICKED AS b USING &tmpdbschema..SPOT_CLICKED_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
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
         ) VALUES (
            d.control_group_flg, d.product_qty_no, d.properties_map_doc, 
            d.spot_clicked_dttm, d.load_dttm, d.spot_clicked_dttm_tz, d.session_id_hex, 
            d.reserved_2_txt, d.rec_group_id, d.product_id, d.message_id, 
            d.event_source_cd, d.event_nm, d.detail_id_hex, d.context_val, 
            d.channel_user_id, d.creative_id, d.event_id, d.identity_id, 
            d.mobile_app_id, d.product_nm, d.product_sku_no, d.request_id, 
            d.segment_id, d.spot_id, d.channel_nm, d.context_type_nm, 
            d.creative_version_id, d.event_designed_id, d.event_key_cd, d.message_version_id, 
            d.occurrence_id, d.reserved_1_txt, d.response_tracking_cd, d.segment_version_id, 
            d.visit_id_hex, d.url_txt, d.task_version_id, d.task_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :SPOT_CLICKED_tmp , SPOT_CLICKED , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..SPOT_CLICKED_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SPOT_CLICKED_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..SPOT_CLICKED;
         DROP TABLE work.SPOT_CLICKED;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..SPOT_REQUESTED));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..SPOT_REQUESTED_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SPOT_REQUESTED_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=SPOT_REQUESTED , table_keys=%str(EVENT_ID), out_table=work.SPOT_REQUESTED );
   DATA work.SPOT_REQUESTED_tmp /VIEW=work.SPOT_REQUESTED_tmp ;
      SET work.SPOT_REQUESTED ;
      IF spot_requested_dttm_tz  NE . THEN spot_requested_dttm_tz =tzoneu2s(spot_requested_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :SPOT_REQUESTED_tmp , SPOT_REQUESTED );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..SPOT_REQUESTED_tmp ;
            SET work.SPOT_REQUESTED_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.SPOT_REQUESTED_tmp  BASE=&tmplib..SPOT_REQUESTED_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..SPOT_REQUESTED_tmp ;
            SET work.SPOT_REQUESTED_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :SPOT_REQUESTED_tmp , SPOT_REQUESTED );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..SPOT_REQUESTED AS b USING &tmpdbschema..SPOT_REQUESTED_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            properties_map_doc, load_dttm, spot_requested_dttm_tz, 
            spot_requested_dttm, visit_id_hex, spot_id, session_id_hex, 
            request_id, mobile_app_id, identity_id, event_source_cd, 
            event_nm, event_id, event_designed_id, detail_id_hex, 
            context_val, context_type_nm, channel_user_id, channel_nm
         ) VALUES (
            d.properties_map_doc, d.load_dttm, d.spot_requested_dttm_tz, 
            d.spot_requested_dttm, d.visit_id_hex, d.spot_id, d.session_id_hex, 
            d.request_id, d.mobile_app_id, d.identity_id, d.event_source_cd, 
            d.event_nm, d.event_id, d.event_designed_id, d.detail_id_hex, 
            d.context_val, d.context_type_nm, d.channel_user_id, d.channel_nm  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :SPOT_REQUESTED_tmp , SPOT_REQUESTED , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..SPOT_REQUESTED_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SPOT_REQUESTED_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..SPOT_REQUESTED;
         DROP TABLE work.SPOT_REQUESTED;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..TAG_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..TAG_DETAILS) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate TAG_DETAILS , TAG_DETAILS );
   PROC APPEND DATA=&udmmart..TAG_DETAILS  BASE=&trglib..TAG_DETAILS (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to TAG_DETAILS , TAG_DETAILS );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..TAG_DETAILS;
         DROP TABLE work.TAG_DETAILS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..VISIT_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if %sysfunc(exist(&tmplib..VISIT_DETAILS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..VISIT_DETAILS_tmp ;
      QUIT;
   %end;
   %check_duplicate_from_source(table_nm=VISIT_DETAILS , table_keys=%str(EVENT_ID), out_table=work.VISIT_DETAILS );
   DATA work.VISIT_DETAILS_tmp /VIEW=work.VISIT_DETAILS_tmp ;
      SET work.VISIT_DETAILS ;
      IF visit_dttm_tz  NE . THEN visit_dttm_tz =tzoneu2s(visit_dttm_tz ,&timeZone_Value.);
   RUN;
   %err_check (Failed to add time zone adaptation :VISIT_DETAILS_tmp , VISIT_DETAILS );
   %if &errFlag = 0 %then %do;
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         DATA &tmplib..VISIT_DETAILS_tmp ;
            SET work.VISIT_DETAILS_tmp ;
            STOP;
         RUN;
         PROC APPEND DATA=work.VISIT_DETAILS_tmp  BASE=&tmplib..VISIT_DETAILS_tmp (&DB_BL_OPTS) FORCE;
         RUN;
      %end;
      %else %do;
         DATA &tmplib..VISIT_DETAILS_tmp ;
            SET work.VISIT_DETAILS_tmp ;
         RUN;
      %end;
      %err_check (Failed to upload to temp location in DB :VISIT_DETAILS_tmp , VISIT_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..VISIT_DETAILS AS b USING &tmpdbschema..VISIT_DETAILS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
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
         WHEN NOT MATCHED AND EVENT_ID IS NOT NULL THEN INSERT (
            sequence_no, visit_dttm_tz, load_dttm, 
            visit_dttm, visit_id_hex, visit_id, session_id_hex, 
            session_id, search_term_txt, search_engine_domain_txt, search_engine_desc, 
            referrer_txt, referrer_query_string_txt, referrer_domain_nm, origination_type_nm, 
            origination_tracking_cd, origination_placement_nm, origination_nm, origination_creative_nm, 
            identity_id, event_id
         ) VALUES (
            d.sequence_no, d.visit_dttm_tz, d.load_dttm, 
            d.visit_dttm, d.visit_id_hex, d.visit_id, d.session_id_hex, 
            d.session_id, d.search_term_txt, d.search_engine_domain_txt, d.search_engine_desc, 
            d.referrer_txt, d.referrer_query_string_txt, d.referrer_domain_nm, d.origination_type_nm, 
            d.origination_tracking_cd, d.origination_placement_nm, d.origination_nm, d.origination_creative_nm, 
            d.identity_id, d.event_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into :VISIT_DETAILS_tmp , VISIT_DETAILS , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..VISIT_DETAILS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..VISIT_DETAILS_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..VISIT_DETAILS;
         DROP TABLE work.VISIT_DETAILS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..WF_PROCESS_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..WF_PROCESS_DETAILS) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate WF_PROCESS_DETAILS , WF_PROCESS_DETAILS );
   PROC APPEND DATA=&udmmart..WF_PROCESS_DETAILS  BASE=&trglib..WF_PROCESS_DETAILS (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to WF_PROCESS_DETAILS , WF_PROCESS_DETAILS );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..WF_PROCESS_DETAILS;
         DROP TABLE work.WF_PROCESS_DETAILS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..WF_PROCESS_DETAILS_CUSTOM_PROP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..WF_PROCESS_DETAILS_CUSTOM_PROP) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate WF_PROCESS_DETAILS_CUSTOM_PROP , WF_PROCESS_DETAILS_CUSTOM_PROP );
   PROC APPEND DATA=&udmmart..WF_PROCESS_DETAILS_CUSTOM_PROP  BASE=&trglib..WF_PROCESS_DETAILS_CUSTOM_PROP (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to WF_PROCESS_DETAILS_CUSTOM_PROP , WF_PROCESS_DETAILS_CUSTOM_PROP );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..WF_PROCESS_DETAILS_CUSTOM_PROP;
         DROP TABLE work.WF_PROCESS_DETAILS_CUSTOM_PROP;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..WF_PROCESS_TASKS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..WF_PROCESS_TASKS) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate WF_PROCESS_TASKS , WF_PROCESS_TASKS );
   PROC APPEND DATA=&udmmart..WF_PROCESS_TASKS  BASE=&trglib..WF_PROCESS_TASKS (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to WF_PROCESS_TASKS , WF_PROCESS_TASKS );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..WF_PROCESS_TASKS;
         DROP TABLE work.WF_PROCESS_TASKS;
      QUIT;
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
   %let dsid=%sysfunc(open(&udmmart..WF_TASKS_USER_ASSIGNMENT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   PROC SQL NOERRORSTOP;
      CONNECT TO &database. (&sql_passthru_connection.);
      EXECUTE (TRUNCATE TABLE &dbschema..WF_TASKS_USER_ASSIGNMENT) BY &database.;
      DISCONNECT FROM &database.;
   QUIT;
   %err_check (Failed to truncate WF_TASKS_USER_ASSIGNMENT , WF_TASKS_USER_ASSIGNMENT );
   PROC APPEND DATA=&udmmart..WF_TASKS_USER_ASSIGNMENT  BASE=&trglib..WF_TASKS_USER_ASSIGNMENT (
      %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
         &DB_BL_OPTS.
      %end;
      %else %do;
         &DB_LD_OPTS.
      %end;
      ) FORCE;
   RUN;
   %err_check (Failed to append to WF_TASKS_USER_ASSIGNMENT , WF_TASKS_USER_ASSIGNMENT );
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..WF_TASKS_USER_ASSIGNMENT;
         DROP TABLE work.WF_TASKS_USER_ASSIGNMENT;
      QUIT;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table WF_TASKS_USER_ASSIGNMENT;
   %put------------------------------------------------------------------;
%end;
%mend;
%execute_POSTGRES_etl;
