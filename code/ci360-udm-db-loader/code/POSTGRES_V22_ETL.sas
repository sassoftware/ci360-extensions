/*******************************************************************************/
/* Copyright(c) 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                         */
/* *****************************************************************************/
%macro execute_POSTGRES_etl;
%if %sysfunc(exist(&udmmart..ABT_ATTRIBUTION)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid=%sysfunc(open(&udmmart..ABT_ATTRIBUTION));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if &nrows = 0 %then %do;
      %put NOTE: ABT_ATTRIBUTION has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..ABT_ATTRIBUTION;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..ABT_ATTRIBUTION_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table ABT_ATTRIBUTION_tmp , ABT_ATTRIBUTION_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=ABT_ATTRIBUTION , table_keys=%str(INTERACTION_DTTM,INTERACTION_ID), out_table=work.ABT_ATTRIBUTION );
      DATA work.ABT_ATTRIBUTION_tmp ;
         SET work.ABT_ATTRIBUTION ;
         WHERE 1=1 AND INTERACTION_DTTM IS NOT NULL AND INTERACTION_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : ABT_ATTRIBUTION_tmp , ABT_ATTRIBUTION_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..ABT_ATTRIBUTION_tmp as select * from &dbschema..ABT_ATTRIBUTION where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : ABT_ATTRIBUTION_tmp , ABT_ATTRIBUTION , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.ABT_ATTRIBUTION_tmp  base=&tmplib..ABT_ATTRIBUTION_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : ABT_ATTRIBUTION_tmp , ABT_ATTRIBUTION );
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
            conversion_value = d.conversion_value, load_id = d.load_id, 
            interaction_subtype = d.interaction_subtype, interaction_type = d.interaction_type, 
            task_id = d.task_id, interaction = d.interaction, 
            identity_id = d.identity_id, creative_id = d.creative_id
         WHEN NOT MATCHED THEN INSERT (
            interaction_cost, conversion_value, interaction_dttm, 
            load_id, interaction_subtype, interaction_id, interaction_type, 
            task_id, interaction, identity_id, creative_id
         ) VALUES (
            d.interaction_cost, d.conversion_value, d.interaction_dttm, 
            d.load_id, d.interaction_subtype, d.interaction_id, d.interaction_type, 
            d.task_id, d.interaction, d.identity_id, d.creative_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : ABT_ATTRIBUTION_tmp , ABT_ATTRIBUTION , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: AB_TEST_PATH_ASSIGNMENT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..AB_TEST_PATH_ASSIGNMENT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..AB_TEST_PATH_ASSIGNMENT_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table AB_TEST_PATH_ASSIGNMENT_tmp , AB_TEST_PATH_ASSIGNMENT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=AB_TEST_PATH_ASSIGNMENT , table_keys=%str(EVENT_ID), out_table=work.AB_TEST_PATH_ASSIGNMENT );
      DATA work.AB_TEST_PATH_ASSIGNMENT_tmp ;
         SET work.AB_TEST_PATH_ASSIGNMENT ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : AB_TEST_PATH_ASSIGNMENT_tmp , AB_TEST_PATH_ASSIGNMENT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..AB_TEST_PATH_ASSIGNMENT_tmp as select * from &dbschema..AB_TEST_PATH_ASSIGNMENT where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : AB_TEST_PATH_ASSIGNMENT_tmp , AB_TEST_PATH_ASSIGNMENT , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.AB_TEST_PATH_ASSIGNMENT_tmp  base=&tmplib..AB_TEST_PATH_ASSIGNMENT_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : AB_TEST_PATH_ASSIGNMENT_tmp , AB_TEST_PATH_ASSIGNMENT );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..AB_TEST_PATH_ASSIGNMENT AS b USING &tmpdbschema..AB_TEST_PATH_ASSIGNMENT_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            abtestpath_assignment_dttm_tz = d.abtestpath_assignment_dttm_tz, 
            load_dttm = d.load_dttm, abtestpath_assignment_dttm = d.abtestpath_assignment_dttm, 
            context_type_nm = d.context_type_nm, session_id_hex = d.session_id_hex, 
            channel_user_id = d.channel_user_id, identity_id = d.identity_id, 
            event_nm = d.event_nm, channel_nm = d.channel_nm, 
            event_designed_id = d.event_designed_id, abtest_path_id = d.abtest_path_id, 
            activity_id = d.activity_id, context_val = d.context_val
         WHEN NOT MATCHED THEN INSERT (
            abtestpath_assignment_dttm_tz, load_dttm, abtestpath_assignment_dttm, 
            context_type_nm, session_id_hex, channel_user_id, identity_id, 
            event_nm, channel_nm, event_id, event_designed_id, 
            abtest_path_id, activity_id, context_val
         ) VALUES (
            d.abtestpath_assignment_dttm_tz, d.load_dttm, d.abtestpath_assignment_dttm, 
            d.context_type_nm, d.session_id_hex, d.channel_user_id, d.identity_id, 
            d.event_nm, d.channel_nm, d.event_id, d.event_designed_id, 
            d.abtest_path_id, d.activity_id, d.context_val  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : AB_TEST_PATH_ASSIGNMENT_tmp , AB_TEST_PATH_ASSIGNMENT , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: ACTIVITY_CONVERSION has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..ACTIVITY_CONVERSION;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..ACTIVITY_CONVERSION_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table ACTIVITY_CONVERSION_tmp , ACTIVITY_CONVERSION_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=ACTIVITY_CONVERSION , table_keys=%str(EVENT_ID), out_table=work.ACTIVITY_CONVERSION );
      DATA work.ACTIVITY_CONVERSION_tmp ;
         SET work.ACTIVITY_CONVERSION ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : ACTIVITY_CONVERSION_tmp , ACTIVITY_CONVERSION_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..ACTIVITY_CONVERSION_tmp as select * from &dbschema..ACTIVITY_CONVERSION where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : ACTIVITY_CONVERSION_tmp , ACTIVITY_CONVERSION , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.ACTIVITY_CONVERSION_tmp  base=&tmplib..ACTIVITY_CONVERSION_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : ACTIVITY_CONVERSION_tmp , ACTIVITY_CONVERSION );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..ACTIVITY_CONVERSION AS b USING &tmpdbschema..ACTIVITY_CONVERSION_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            load_dttm = d.load_dttm, 
            activity_conversion_dttm_tz = d.activity_conversion_dttm_tz, activity_conversion_dttm = d.activity_conversion_dttm, 
            abtest_path_id = d.abtest_path_id, activity_id = d.activity_id, 
            activity_node_id = d.activity_node_id, channel_nm = d.channel_nm, 
            session_id_hex = d.session_id_hex, parent_event_designed_id = d.parent_event_designed_id, 
            identity_id = d.identity_id, goal_id = d.goal_id, 
            event_nm = d.event_nm, event_designed_id = d.event_designed_id, 
            detail_id_hex = d.detail_id_hex, context_val = d.context_val, 
            context_type_nm = d.context_type_nm, channel_user_id = d.channel_user_id
         WHEN NOT MATCHED THEN INSERT (
            load_dttm, activity_conversion_dttm_tz, activity_conversion_dttm, 
            abtest_path_id, activity_id, activity_node_id, channel_nm, 
            session_id_hex, parent_event_designed_id, identity_id, goal_id, 
            event_nm, event_id, event_designed_id, detail_id_hex, 
            context_val, context_type_nm, channel_user_id
         ) VALUES (
            d.load_dttm, d.activity_conversion_dttm_tz, d.activity_conversion_dttm, 
            d.abtest_path_id, d.activity_id, d.activity_node_id, d.channel_nm, 
            d.session_id_hex, d.parent_event_designed_id, d.identity_id, d.goal_id, 
            d.event_nm, d.event_id, d.event_designed_id, d.detail_id_hex, 
            d.context_val, d.context_type_nm, d.channel_user_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : ACTIVITY_CONVERSION_tmp , ACTIVITY_CONVERSION , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: ACTIVITY_FLOW_IN has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..ACTIVITY_FLOW_IN;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..ACTIVITY_FLOW_IN_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table ACTIVITY_FLOW_IN_tmp , ACTIVITY_FLOW_IN_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=ACTIVITY_FLOW_IN , table_keys=%str(EVENT_ID), out_table=work.ACTIVITY_FLOW_IN );
      DATA work.ACTIVITY_FLOW_IN_tmp ;
         SET work.ACTIVITY_FLOW_IN ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : ACTIVITY_FLOW_IN_tmp , ACTIVITY_FLOW_IN_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..ACTIVITY_FLOW_IN_tmp as select * from &dbschema..ACTIVITY_FLOW_IN where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : ACTIVITY_FLOW_IN_tmp , ACTIVITY_FLOW_IN , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.ACTIVITY_FLOW_IN_tmp  base=&tmplib..ACTIVITY_FLOW_IN_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : ACTIVITY_FLOW_IN_tmp , ACTIVITY_FLOW_IN );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..ACTIVITY_FLOW_IN AS b USING &tmpdbschema..ACTIVITY_FLOW_IN_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            load_dttm = d.load_dttm, 
            activity_flow_in_dttm = d.activity_flow_in_dttm, activity_flow_in_dttm_tz = d.activity_flow_in_dttm_tz, 
            task_id = d.task_id, identity_id = d.identity_id, 
            context_val = d.context_val, context_type_nm = d.context_type_nm, 
            event_nm = d.event_nm, channel_user_id = d.channel_user_id, 
            activity_node_id = d.activity_node_id, activity_id = d.activity_id, 
            abtest_path_id = d.abtest_path_id, channel_nm = d.channel_nm, 
            event_designed_id = d.event_designed_id
         WHEN NOT MATCHED THEN INSERT (
            load_dttm, activity_flow_in_dttm, activity_flow_in_dttm_tz, 
            task_id, identity_id, context_val, context_type_nm, 
            event_id, event_nm, channel_user_id, activity_node_id, 
            activity_id, abtest_path_id, channel_nm, event_designed_id
         ) VALUES (
            d.load_dttm, d.activity_flow_in_dttm, d.activity_flow_in_dttm_tz, 
            d.task_id, d.identity_id, d.context_val, d.context_type_nm, 
            d.event_id, d.event_nm, d.channel_user_id, d.activity_node_id, 
            d.activity_id, d.abtest_path_id, d.channel_nm, d.event_designed_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : ACTIVITY_FLOW_IN_tmp , ACTIVITY_FLOW_IN , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: ACTIVITY_START has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..ACTIVITY_START;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..ACTIVITY_START_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table ACTIVITY_START_tmp , ACTIVITY_START_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=ACTIVITY_START , table_keys=%str(EVENT_ID), out_table=work.ACTIVITY_START );
      DATA work.ACTIVITY_START_tmp ;
         SET work.ACTIVITY_START ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : ACTIVITY_START_tmp , ACTIVITY_START_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..ACTIVITY_START_tmp as select * from &dbschema..ACTIVITY_START where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : ACTIVITY_START_tmp , ACTIVITY_START , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.ACTIVITY_START_tmp  base=&tmplib..ACTIVITY_START_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : ACTIVITY_START_tmp , ACTIVITY_START );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..ACTIVITY_START AS b USING &tmpdbschema..ACTIVITY_START_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            activity_start_dttm = d.activity_start_dttm, 
            load_dttm = d.load_dttm, activity_start_dttm_tz = d.activity_start_dttm_tz, 
            channel_user_id = d.channel_user_id, channel_nm = d.channel_nm, 
            identity_id = d.identity_id, event_nm = d.event_nm, 
            activity_id = d.activity_id, event_designed_id = d.event_designed_id, 
            context_val = d.context_val, context_type_nm = d.context_type_nm
         WHEN NOT MATCHED THEN INSERT (
            activity_start_dttm, load_dttm, activity_start_dttm_tz, 
            channel_user_id, channel_nm, identity_id, event_nm, 
            event_id, activity_id, event_designed_id, context_val, 
            context_type_nm
         ) VALUES (
            d.activity_start_dttm, d.load_dttm, d.activity_start_dttm_tz, 
            d.channel_user_id, d.channel_nm, d.identity_id, d.event_nm, 
            d.event_id, d.activity_id, d.event_designed_id, d.context_val, 
            d.context_type_nm  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : ACTIVITY_START_tmp , ACTIVITY_START , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: ADVERTISING_CONTACT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..ADVERTISING_CONTACT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..ADVERTISING_CONTACT_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table ADVERTISING_CONTACT_tmp , ADVERTISING_CONTACT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=ADVERTISING_CONTACT , table_keys=%str(EVENT_ID), out_table=work.ADVERTISING_CONTACT );
      DATA work.ADVERTISING_CONTACT_tmp ;
         SET work.ADVERTISING_CONTACT ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : ADVERTISING_CONTACT_tmp , ADVERTISING_CONTACT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..ADVERTISING_CONTACT_tmp as select * from &dbschema..ADVERTISING_CONTACT where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : ADVERTISING_CONTACT_tmp , ADVERTISING_CONTACT , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.ADVERTISING_CONTACT_tmp  base=&tmplib..ADVERTISING_CONTACT_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : ADVERTISING_CONTACT_tmp , ADVERTISING_CONTACT );
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
            task_version_id = d.task_version_id, task_action_nm = d.task_action_nm, 
            segment_id = d.segment_id, occurrence_id = d.occurrence_id, 
            journey_occurrence_id = d.journey_occurrence_id, identity_id = d.identity_id, 
            event_nm = d.event_nm, event_designed_id = d.event_designed_id, 
            journey_id = d.journey_id, context_val = d.context_val, 
            response_tracking_cd = d.response_tracking_cd, context_type_nm = d.context_type_nm, 
            segment_version_id = d.segment_version_id, channel_nm = d.channel_nm, 
            task_id = d.task_id, audience_id = d.audience_id, 
            aud_occurrence_id = d.aud_occurrence_id, advertising_platform_nm = d.advertising_platform_nm
         WHEN NOT MATCHED THEN INSERT (
            load_dttm, advertising_contact_dttm_tz, advertising_contact_dttm, 
            task_version_id, task_action_nm, segment_id, occurrence_id, 
            journey_occurrence_id, identity_id, event_nm, event_id, 
            event_designed_id, journey_id, context_val, response_tracking_cd, 
            context_type_nm, segment_version_id, channel_nm, task_id, 
            audience_id, aud_occurrence_id, advertising_platform_nm
         ) VALUES (
            d.load_dttm, d.advertising_contact_dttm_tz, d.advertising_contact_dttm, 
            d.task_version_id, d.task_action_nm, d.segment_id, d.occurrence_id, 
            d.journey_occurrence_id, d.identity_id, d.event_nm, d.event_id, 
            d.event_designed_id, d.journey_id, d.context_val, d.response_tracking_cd, 
            d.context_type_nm, d.segment_version_id, d.channel_nm, d.task_id, 
            d.audience_id, d.aud_occurrence_id, d.advertising_platform_nm  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : ADVERTISING_CONTACT_tmp , ADVERTISING_CONTACT , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: ASSET_DETAILS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..ASSET_DETAILS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..ASSET_DETAILS) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate ASSET_DETAILS , ASSET_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: ASSET_DETAILS_CUSTOM_PROP has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..ASSET_DETAILS_CUSTOM_PROP;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..ASSET_DETAILS_CUSTOM_PROP) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate ASSET_DETAILS_CUSTOM_PROP , ASSET_DETAILS_CUSTOM_PROP );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: ASSET_FOLDER_DETAILS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..ASSET_FOLDER_DETAILS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..ASSET_FOLDER_DETAILS) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate ASSET_FOLDER_DETAILS , ASSET_FOLDER_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: ASSET_RENDITION_DETAILS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..ASSET_RENDITION_DETAILS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..ASSET_RENDITION_DETAILS) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate ASSET_RENDITION_DETAILS , ASSET_RENDITION_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: ASSET_REVISION has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..ASSET_REVISION;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..ASSET_REVISION) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate ASSET_REVISION , ASSET_REVISION );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: AUDIENCE_MEMBERSHIP_CHANGE has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..AUDIENCE_MEMBERSHIP_CHANGE;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..AUDIENCE_MEMBERSHIP_CHANGE_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table AUDIENCE_MEMBERSHIP_CHANGE_tmp , AUDIENCE_MEMBERSHIP_CHANGE_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=AUDIENCE_MEMBERSHIP_CHANGE , table_keys=%str(EVENT_ID), out_table=work.AUDIENCE_MEMBERSHIP_CHANGE );
      DATA work.AUDIENCE_MEMBERSHIP_CHANGE_tmp ;
         SET work.AUDIENCE_MEMBERSHIP_CHANGE ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : AUDIENCE_MEMBERSHIP_CHANGE_tmp , AUDIENCE_MEMBERSHIP_CHANGE_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..AUDIENCE_MEMBERSHIP_CHANGE_tmp as select * from &dbschema..AUDIENCE_MEMBERSHIP_CHANGE where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : AUDIENCE_MEMBERSHIP_CHANGE_tmp , AUDIENCE_MEMBERSHIP_CHANGE , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.AUDIENCE_MEMBERSHIP_CHANGE_tmp  base=&tmplib..AUDIENCE_MEMBERSHIP_CHANGE_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : AUDIENCE_MEMBERSHIP_CHANGE_tmp , AUDIENCE_MEMBERSHIP_CHANGE );
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
         WHEN NOT MATCHED THEN INSERT (
            audience_change_dttm, load_dttm, audience_change_dttm_tz, 
            identity_id, event_id, aud_occurrence_id, audience_id, 
            event_nm
         ) VALUES (
            d.audience_change_dttm, d.load_dttm, d.audience_change_dttm_tz, 
            d.identity_id, d.event_id, d.aud_occurrence_id, d.audience_id, 
            d.event_nm  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : AUDIENCE_MEMBERSHIP_CHANGE_tmp , AUDIENCE_MEMBERSHIP_CHANGE , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: BUSINESS_PROCESS_DETAILS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..BUSINESS_PROCESS_DETAILS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..BUSINESS_PROCESS_DETAILS_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table BUSINESS_PROCESS_DETAILS_tmp , BUSINESS_PROCESS_DETAILS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=BUSINESS_PROCESS_DETAILS , table_keys=%str(EVENT_ID), out_table=work.BUSINESS_PROCESS_DETAILS );
      DATA work.BUSINESS_PROCESS_DETAILS_tmp ;
         SET work.BUSINESS_PROCESS_DETAILS ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : BUSINESS_PROCESS_DETAILS_tmp , BUSINESS_PROCESS_DETAILS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..BUSINESS_PROCESS_DETAILS_tmp as select * from &dbschema..BUSINESS_PROCESS_DETAILS where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : BUSINESS_PROCESS_DETAILS_tmp , BUSINESS_PROCESS_DETAILS , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.BUSINESS_PROCESS_DETAILS_tmp  base=&tmplib..BUSINESS_PROCESS_DETAILS_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : BUSINESS_PROCESS_DETAILS_tmp , BUSINESS_PROCESS_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..BUSINESS_PROCESS_DETAILS AS b USING &tmpdbschema..BUSINESS_PROCESS_DETAILS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            is_start_flg = d.is_start_flg, 
            is_completion_flg = d.is_completion_flg, process_instance_no = d.process_instance_no, 
            step_order_no = d.step_order_no, process_attempt_cnt = d.process_attempt_cnt, 
            process_exception_dttm_tz = d.process_exception_dttm_tz, process_dttm = d.process_dttm, 
            load_dttm = d.load_dttm, process_exception_dttm = d.process_exception_dttm, 
            process_dttm_tz = d.process_dttm_tz, visit_id = d.visit_id, 
            session_id_hex = d.session_id_hex, process_step_nm = d.process_step_nm, 
            process_details_sk = d.process_details_sk, event_nm = d.event_nm, 
            detail_id = d.detail_id, attribute1_txt = d.attribute1_txt, 
            detail_id_hex = d.detail_id_hex, event_designed_id = d.event_designed_id, 
            identity_id = d.identity_id, next_detail_id = d.next_detail_id, 
            visit_id_hex = d.visit_id_hex, attribute2_txt = d.attribute2_txt, 
            event_source_cd = d.event_source_cd, process_exception_txt = d.process_exception_txt, 
            process_nm = d.process_nm, session_id = d.session_id
         WHEN NOT MATCHED THEN INSERT (
            is_start_flg, is_completion_flg, process_instance_no, 
            step_order_no, process_attempt_cnt, process_exception_dttm_tz, process_dttm, 
            load_dttm, process_exception_dttm, process_dttm_tz, visit_id, 
            session_id_hex, process_step_nm, process_details_sk, event_nm, 
            detail_id, attribute1_txt, detail_id_hex, event_designed_id, 
            identity_id, next_detail_id, visit_id_hex, attribute2_txt, 
            event_id, event_source_cd, process_exception_txt, process_nm, 
            session_id
         ) VALUES (
            d.is_start_flg, d.is_completion_flg, d.process_instance_no, 
            d.step_order_no, d.process_attempt_cnt, d.process_exception_dttm_tz, d.process_dttm, 
            d.load_dttm, d.process_exception_dttm, d.process_dttm_tz, d.visit_id, 
            d.session_id_hex, d.process_step_nm, d.process_details_sk, d.event_nm, 
            d.detail_id, d.attribute1_txt, d.detail_id_hex, d.event_designed_id, 
            d.identity_id, d.next_detail_id, d.visit_id_hex, d.attribute2_txt, 
            d.event_id, d.event_source_cd, d.process_exception_txt, d.process_nm, 
            d.session_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : BUSINESS_PROCESS_DETAILS_tmp , BUSINESS_PROCESS_DETAILS , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: CART_ACTIVITY_DETAILS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CART_ACTIVITY_DETAILS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..CART_ACTIVITY_DETAILS_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table CART_ACTIVITY_DETAILS_tmp , CART_ACTIVITY_DETAILS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=CART_ACTIVITY_DETAILS , table_keys=%str(EVENT_ID), out_table=work.CART_ACTIVITY_DETAILS );
      DATA work.CART_ACTIVITY_DETAILS_tmp ;
         SET work.CART_ACTIVITY_DETAILS ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : CART_ACTIVITY_DETAILS_tmp , CART_ACTIVITY_DETAILS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..CART_ACTIVITY_DETAILS_tmp as select * from &dbschema..CART_ACTIVITY_DETAILS where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : CART_ACTIVITY_DETAILS_tmp , CART_ACTIVITY_DETAILS , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.CART_ACTIVITY_DETAILS_tmp  base=&tmplib..CART_ACTIVITY_DETAILS_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : CART_ACTIVITY_DETAILS_tmp , CART_ACTIVITY_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..CART_ACTIVITY_DETAILS AS b USING &tmpdbschema..CART_ACTIVITY_DETAILS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            displayed_cart_amt = d.displayed_cart_amt, 
            unit_price_amt = d.unit_price_amt, displayed_cart_items_no = d.displayed_cart_items_no, 
            quantity_val = d.quantity_val, properties_map_doc = d.properties_map_doc, 
            activity_dttm_tz = d.activity_dttm_tz, activity_dttm = d.activity_dttm, 
            load_dttm = d.load_dttm, cart_activity_sk = d.cart_activity_sk, 
            availability_message_txt = d.availability_message_txt, activity_cd = d.activity_cd, 
            cart_id = d.cart_id, visit_id_hex = d.visit_id_hex, 
            visit_id = d.visit_id, shipping_message_txt = d.shipping_message_txt, 
            session_id_hex = d.session_id_hex, session_id = d.session_id, 
            saving_message_txt = d.saving_message_txt, product_sku = d.product_sku, 
            product_nm = d.product_nm, product_id = d.product_id, 
            product_group_nm = d.product_group_nm, mobile_app_id = d.mobile_app_id, 
            identity_id = d.identity_id, event_source_cd = d.event_source_cd, 
            event_nm = d.event_nm, event_key_cd = d.event_key_cd, 
            event_designed_id = d.event_designed_id, detail_id_hex = d.detail_id_hex, 
            detail_id = d.detail_id, currency_cd = d.currency_cd, 
            channel_nm = d.channel_nm, cart_nm = d.cart_nm
         WHEN NOT MATCHED THEN INSERT (
            displayed_cart_amt, unit_price_amt, displayed_cart_items_no, 
            quantity_val, properties_map_doc, activity_dttm_tz, activity_dttm, 
            load_dttm, cart_activity_sk, availability_message_txt, activity_cd, 
            cart_id, visit_id_hex, visit_id, shipping_message_txt, 
            session_id_hex, session_id, saving_message_txt, product_sku, 
            product_nm, product_id, product_group_nm, mobile_app_id, 
            identity_id, event_source_cd, event_nm, event_key_cd, 
            event_id, event_designed_id, detail_id_hex, detail_id, 
            currency_cd, channel_nm, cart_nm
         ) VALUES (
            d.displayed_cart_amt, d.unit_price_amt, d.displayed_cart_items_no, 
            d.quantity_val, d.properties_map_doc, d.activity_dttm_tz, d.activity_dttm, 
            d.load_dttm, d.cart_activity_sk, d.availability_message_txt, d.activity_cd, 
            d.cart_id, d.visit_id_hex, d.visit_id, d.shipping_message_txt, 
            d.session_id_hex, d.session_id, d.saving_message_txt, d.product_sku, 
            d.product_nm, d.product_id, d.product_group_nm, d.mobile_app_id, 
            d.identity_id, d.event_source_cd, d.event_nm, d.event_key_cd, 
            d.event_id, d.event_designed_id, d.detail_id_hex, d.detail_id, 
            d.currency_cd, d.channel_nm, d.cart_nm  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : CART_ACTIVITY_DETAILS_tmp , CART_ACTIVITY_DETAILS , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: CC_BUDGET_BREAKUP has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CC_BUDGET_BREAKUP;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..CC_BUDGET_BREAKUP) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate CC_BUDGET_BREAKUP , CC_BUDGET_BREAKUP );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: CC_BUDGET_BREAKUP_CCBDGT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CC_BUDGET_BREAKUP_CCBDGT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..CC_BUDGET_BREAKUP_CCBDGT) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate CC_BUDGET_BREAKUP_CCBDGT , CC_BUDGET_BREAKUP_CCBDGT );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
%if %sysfunc(exist(&udmmart..COMMITMENT_DETAILS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid=%sysfunc(open(&udmmart..COMMITMENT_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if &nrows = 0 %then %do;
      %put NOTE: COMMITMENT_DETAILS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..COMMITMENT_DETAILS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..COMMITMENT_DETAILS) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate COMMITMENT_DETAILS , COMMITMENT_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: COMMITMENT_LINE_ITEMS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..COMMITMENT_LINE_ITEMS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..COMMITMENT_LINE_ITEMS) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate COMMITMENT_LINE_ITEMS , COMMITMENT_LINE_ITEMS );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: COMMITMENT_LINE_ITEMS_CCBDGT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..COMMITMENT_LINE_ITEMS_CCBDGT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..COMMITMENT_LINE_ITEMS_CCBDGT) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate COMMITMENT_LINE_ITEMS_CCBDGT , COMMITMENT_LINE_ITEMS_CCBDGT );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: CONTACT_HISTORY has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CONTACT_HISTORY;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..CONTACT_HISTORY_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table CONTACT_HISTORY_tmp , CONTACT_HISTORY_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=CONTACT_HISTORY , table_keys=%str(CONTACT_ID), out_table=work.CONTACT_HISTORY );
      DATA work.CONTACT_HISTORY_tmp ;
         SET work.CONTACT_HISTORY ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND CONTACT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : CONTACT_HISTORY_tmp , CONTACT_HISTORY_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..CONTACT_HISTORY_tmp as select * from &dbschema..CONTACT_HISTORY where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : CONTACT_HISTORY_tmp , CONTACT_HISTORY , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.CONTACT_HISTORY_tmp  base=&tmplib..CONTACT_HISTORY_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : CONTACT_HISTORY_tmp , CONTACT_HISTORY );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..CONTACT_HISTORY AS b USING &tmpdbschema..CONTACT_HISTORY_tmp AS d ON (
            b.contact_id = d.contact_id )
         WHEN MATCHED THEN
         UPDATE SET
            control_group_flg = d.control_group_flg, 
            properties_map_doc = d.properties_map_doc, contact_dttm = d.contact_dttm, 
            contact_dttm_tz = d.contact_dttm_tz, load_dttm = d.load_dttm, 
            task_version_id = d.task_version_id, session_id_hex = d.session_id_hex, 
            occurrence_id = d.occurrence_id, journey_id = d.journey_id, 
            creative_id = d.creative_id, contact_nm = d.contact_nm, 
            contact_channel_nm = d.contact_channel_nm, aud_occurrence_id = d.aud_occurrence_id, 
            context_val = d.context_val, event_designed_id = d.event_designed_id, 
            journey_occurrence_id = d.journey_occurrence_id, response_tracking_cd = d.response_tracking_cd, 
            visit_id_hex = d.visit_id_hex, audience_id = d.audience_id, 
            context_type_nm = d.context_type_nm, detail_id_hex = d.detail_id_hex, 
            identity_id = d.identity_id, message_id = d.message_id, 
            parent_event_designed_id = d.parent_event_designed_id, task_id = d.task_id
         WHEN NOT MATCHED THEN INSERT (
            control_group_flg, properties_map_doc, contact_dttm, 
            contact_dttm_tz, load_dttm, task_version_id, session_id_hex, 
            occurrence_id, journey_id, creative_id, contact_nm, 
            contact_channel_nm, aud_occurrence_id, context_val, event_designed_id, 
            journey_occurrence_id, response_tracking_cd, visit_id_hex, audience_id, 
            contact_id, context_type_nm, detail_id_hex, identity_id, 
            message_id, parent_event_designed_id, task_id
         ) VALUES (
            d.control_group_flg, d.properties_map_doc, d.contact_dttm, 
            d.contact_dttm_tz, d.load_dttm, d.task_version_id, d.session_id_hex, 
            d.occurrence_id, d.journey_id, d.creative_id, d.contact_nm, 
            d.contact_channel_nm, d.aud_occurrence_id, d.context_val, d.event_designed_id, 
            d.journey_occurrence_id, d.response_tracking_cd, d.visit_id_hex, d.audience_id, 
            d.contact_id, d.context_type_nm, d.detail_id_hex, d.identity_id, 
            d.message_id, d.parent_event_designed_id, d.task_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : CONTACT_HISTORY_tmp , CONTACT_HISTORY , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: CONVERSION_MILESTONE has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CONVERSION_MILESTONE;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..CONVERSION_MILESTONE_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table CONVERSION_MILESTONE_tmp , CONVERSION_MILESTONE_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=CONVERSION_MILESTONE , table_keys=%str(EVENT_ID), out_table=work.CONVERSION_MILESTONE );
      DATA work.CONVERSION_MILESTONE_tmp ;
         SET work.CONVERSION_MILESTONE ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : CONVERSION_MILESTONE_tmp , CONVERSION_MILESTONE_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..CONVERSION_MILESTONE_tmp as select * from &dbschema..CONVERSION_MILESTONE where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : CONVERSION_MILESTONE_tmp , CONVERSION_MILESTONE , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.CONVERSION_MILESTONE_tmp  base=&tmplib..CONVERSION_MILESTONE_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : CONVERSION_MILESTONE_tmp , CONVERSION_MILESTONE );
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
            properties_map_doc = d.properties_map_doc, conversion_milestone_dttm_tz = d.conversion_milestone_dttm_tz, 
            conversion_milestone_dttm = d.conversion_milestone_dttm, load_dttm = d.load_dttm, 
            event_designed_id = d.event_designed_id, creative_version_id = d.creative_version_id, 
            context_type_nm = d.context_type_nm, channel_nm = d.channel_nm, 
            aud_occurrence_id = d.aud_occurrence_id, analysis_group_id = d.analysis_group_id, 
            activity_id = d.activity_id, audience_id = d.audience_id, 
            channel_user_id = d.channel_user_id, context_val = d.context_val, 
            creative_id = d.creative_id, detail_id_hex = d.detail_id_hex, 
            visit_id_hex = d.visit_id_hex, task_version_id = d.task_version_id, 
            task_id = d.task_id, subject_line_txt = d.subject_line_txt, 
            spot_id = d.spot_id, session_id_hex = d.session_id_hex, 
            segment_version_id = d.segment_version_id, segment_id = d.segment_id, 
            response_tracking_cd = d.response_tracking_cd, reserved_2_txt = d.reserved_2_txt, 
            reserved_1_txt = d.reserved_1_txt, rec_group_id = d.rec_group_id, 
            parent_event_designed_id = d.parent_event_designed_id, occurrence_id = d.occurrence_id, 
            mobile_app_id = d.mobile_app_id, message_version_id = d.message_version_id, 
            message_id = d.message_id, journey_occurrence_id = d.journey_occurrence_id, 
            journey_id = d.journey_id, identity_id = d.identity_id, 
            goal_id = d.goal_id, event_nm = d.event_nm
         WHEN NOT MATCHED THEN INSERT (
            test_flg, control_group_flg, total_cost_amt, 
            properties_map_doc, conversion_milestone_dttm_tz, conversion_milestone_dttm, load_dttm, 
            event_designed_id, creative_version_id, context_type_nm, channel_nm, 
            aud_occurrence_id, analysis_group_id, activity_id, audience_id, 
            channel_user_id, context_val, creative_id, detail_id_hex, 
            event_id, visit_id_hex, task_version_id, task_id, 
            subject_line_txt, spot_id, session_id_hex, segment_version_id, 
            segment_id, response_tracking_cd, reserved_2_txt, reserved_1_txt, 
            rec_group_id, parent_event_designed_id, occurrence_id, mobile_app_id, 
            message_version_id, message_id, journey_occurrence_id, journey_id, 
            identity_id, goal_id, event_nm
         ) VALUES (
            d.test_flg, d.control_group_flg, d.total_cost_amt, 
            d.properties_map_doc, d.conversion_milestone_dttm_tz, d.conversion_milestone_dttm, d.load_dttm, 
            d.event_designed_id, d.creative_version_id, d.context_type_nm, d.channel_nm, 
            d.aud_occurrence_id, d.analysis_group_id, d.activity_id, d.audience_id, 
            d.channel_user_id, d.context_val, d.creative_id, d.detail_id_hex, 
            d.event_id, d.visit_id_hex, d.task_version_id, d.task_id, 
            d.subject_line_txt, d.spot_id, d.session_id_hex, d.segment_version_id, 
            d.segment_id, d.response_tracking_cd, d.reserved_2_txt, d.reserved_1_txt, 
            d.rec_group_id, d.parent_event_designed_id, d.occurrence_id, d.mobile_app_id, 
            d.message_version_id, d.message_id, d.journey_occurrence_id, d.journey_id, 
            d.identity_id, d.goal_id, d.event_nm  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : CONVERSION_MILESTONE_tmp , CONVERSION_MILESTONE , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: CUSTOM_EVENTS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CUSTOM_EVENTS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..CUSTOM_EVENTS_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table CUSTOM_EVENTS_tmp , CUSTOM_EVENTS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=CUSTOM_EVENTS , table_keys=%str(EVENT_ID), out_table=work.CUSTOM_EVENTS );
      DATA work.CUSTOM_EVENTS_tmp ;
         SET work.CUSTOM_EVENTS ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : CUSTOM_EVENTS_tmp , CUSTOM_EVENTS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..CUSTOM_EVENTS_tmp as select * from &dbschema..CUSTOM_EVENTS where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : CUSTOM_EVENTS_tmp , CUSTOM_EVENTS , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.CUSTOM_EVENTS_tmp  base=&tmplib..CUSTOM_EVENTS_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : CUSTOM_EVENTS_tmp , CUSTOM_EVENTS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..CUSTOM_EVENTS AS b USING &tmpdbschema..CUSTOM_EVENTS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            custom_revenue_amt = d.custom_revenue_amt, 
            properties_map_doc = d.properties_map_doc, load_dttm = d.load_dttm, 
            custom_event_dttm = d.custom_event_dttm, custom_event_dttm_tz = d.custom_event_dttm_tz, 
            visit_id_hex = d.visit_id_hex, visit_id = d.visit_id, 
            session_id_hex = d.session_id_hex, session_id = d.session_id, 
            reserved_2_txt = d.reserved_2_txt, reserved_1_txt = d.reserved_1_txt, 
            page_id = d.page_id, mobile_app_id = d.mobile_app_id, 
            identity_id = d.identity_id, event_type_nm = d.event_type_nm, 
            event_source_cd = d.event_source_cd, event_nm = d.event_nm, 
            event_key_cd = d.event_key_cd, event_designed_id = d.event_designed_id, 
            detail_id_hex = d.detail_id_hex, detail_id = d.detail_id, 
            custom_events_sk = d.custom_events_sk, custom_event_nm = d.custom_event_nm, 
            custom_event_group_nm = d.custom_event_group_nm, channel_user_id = d.channel_user_id, 
            channel_nm = d.channel_nm
         WHEN NOT MATCHED THEN INSERT (
            custom_revenue_amt, properties_map_doc, load_dttm, 
            custom_event_dttm, custom_event_dttm_tz, visit_id_hex, visit_id, 
            session_id_hex, session_id, reserved_2_txt, reserved_1_txt, 
            page_id, mobile_app_id, identity_id, event_type_nm, 
            event_source_cd, event_nm, event_key_cd, event_id, 
            event_designed_id, detail_id_hex, detail_id, custom_events_sk, 
            custom_event_nm, custom_event_group_nm, channel_user_id, channel_nm
         ) VALUES (
            d.custom_revenue_amt, d.properties_map_doc, d.load_dttm, 
            d.custom_event_dttm, d.custom_event_dttm_tz, d.visit_id_hex, d.visit_id, 
            d.session_id_hex, d.session_id, d.reserved_2_txt, d.reserved_1_txt, 
            d.page_id, d.mobile_app_id, d.identity_id, d.event_type_nm, 
            d.event_source_cd, d.event_nm, d.event_key_cd, d.event_id, 
            d.event_designed_id, d.detail_id_hex, d.detail_id, d.custom_events_sk, 
            d.custom_event_nm, d.custom_event_group_nm, d.channel_user_id, d.channel_nm  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : CUSTOM_EVENTS_tmp , CUSTOM_EVENTS , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: CUSTOM_EVENTS_EXT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..CUSTOM_EVENTS_EXT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..CUSTOM_EVENTS_EXT_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table CUSTOM_EVENTS_EXT_tmp , CUSTOM_EVENTS_EXT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=CUSTOM_EVENTS_EXT , table_keys=%str(CUSTOM_EVENTS_SK), out_table=work.CUSTOM_EVENTS_EXT );
      DATA work.CUSTOM_EVENTS_EXT_tmp ;
         SET work.CUSTOM_EVENTS_EXT ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND CUSTOM_EVENTS_SK IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : CUSTOM_EVENTS_EXT_tmp , CUSTOM_EVENTS_EXT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..CUSTOM_EVENTS_EXT_tmp as select * from &dbschema..CUSTOM_EVENTS_EXT where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : CUSTOM_EVENTS_EXT_tmp , CUSTOM_EVENTS_EXT , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.CUSTOM_EVENTS_EXT_tmp  base=&tmplib..CUSTOM_EVENTS_EXT_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : CUSTOM_EVENTS_EXT_tmp , CUSTOM_EVENTS_EXT );
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
         WHEN NOT MATCHED THEN INSERT (
            custom_revenue_amt, load_dttm, event_designed_id, 
            custom_events_sk
         ) VALUES (
            d.custom_revenue_amt, d.load_dttm, d.event_designed_id, 
            d.custom_events_sk  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : CUSTOM_EVENTS_EXT_tmp , CUSTOM_EVENTS_EXT , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: DAILY_USAGE has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DAILY_USAGE;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DAILY_USAGE_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table DAILY_USAGE_tmp , DAILY_USAGE_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=DAILY_USAGE , table_keys=%str(EVENT_DAY), out_table=work.DAILY_USAGE );
      DATA work.DAILY_USAGE_tmp ;
         SET work.DAILY_USAGE ;
         WHERE 1=1 AND EVENT_DAY IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : DAILY_USAGE_tmp , DAILY_USAGE_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..DAILY_USAGE_tmp as select * from &dbschema..DAILY_USAGE where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : DAILY_USAGE_tmp , DAILY_USAGE , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.DAILY_USAGE_tmp  base=&tmplib..DAILY_USAGE_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : DAILY_USAGE_tmp , DAILY_USAGE );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..DAILY_USAGE AS b USING &tmpdbschema..DAILY_USAGE_tmp AS d ON (
            b.event_day = d.event_day )
         WHEN MATCHED THEN
         UPDATE SET
            customer_profiles_processed_str = d.customer_profiles_processed_str, 
            bc_subjcnt_str = d.bc_subjcnt_str, advertising_members_cnt_str = d.advertising_members_cnt_str, 
            api_usage_str = d.api_usage_str, web_impr_cnt = d.web_impr_cnt, 
            facebook_ads_cnt = d.facebook_ads_cnt, sms_frag_cnt = d.sms_frag_cnt, 
            mobile_in_app_msg_cnt = d.mobile_in_app_msg_cnt, mobile_push_cnt = d.mobile_push_cnt, 
            mob_sesn_cnt = d.mob_sesn_cnt, mob_impr_cnt = d.mob_impr_cnt, 
            linkedin_ads_cnt = d.linkedin_ads_cnt, email_send_cnt = d.email_send_cnt, 
            audience_usage_cnt = d.audience_usage_cnt, email_preview_cnt = d.email_preview_cnt, 
            dm_destinations_total_id_cnt = d.dm_destinations_total_id_cnt, mai_scored_project_cnt = d.mai_scored_project_cnt, 
            google_ads_cnt = d.google_ads_cnt, outbound_api_cnt = d.outbound_api_cnt, 
            sms_output_result_cnt = d.sms_output_result_cnt, web_sesn_cnt = d.web_sesn_cnt, 
            plan_users_cnt = d.plan_users_cnt, sms_send_cnt = d.sms_send_cnt, 
            dm_destinations_total_row_cnt = d.dm_destinations_total_row_cnt, db_size = d.db_size, 
            asset_size = d.asset_size, admin_user_cnt = d.admin_user_cnt, 
            sms_output_result_str = d.sms_output_result_str, sms_send_str = d.sms_send_str, 
            sms_frag_str = d.sms_frag_str
         WHEN NOT MATCHED THEN INSERT (
            customer_profiles_processed_str, bc_subjcnt_str, advertising_members_cnt_str, 
            api_usage_str, web_impr_cnt, facebook_ads_cnt, sms_frag_cnt, 
            mobile_in_app_msg_cnt, mobile_push_cnt, mob_sesn_cnt, mob_impr_cnt, 
            linkedin_ads_cnt, email_send_cnt, audience_usage_cnt, email_preview_cnt, 
            dm_destinations_total_id_cnt, mai_scored_project_cnt, google_ads_cnt, outbound_api_cnt, 
            sms_output_result_cnt, web_sesn_cnt, plan_users_cnt, sms_send_cnt, 
            dm_destinations_total_row_cnt, db_size, asset_size, admin_user_cnt, 
            sms_output_result_str, sms_send_str, sms_frag_str, event_day
         ) VALUES (
            d.customer_profiles_processed_str, d.bc_subjcnt_str, d.advertising_members_cnt_str, 
            d.api_usage_str, d.web_impr_cnt, d.facebook_ads_cnt, d.sms_frag_cnt, 
            d.mobile_in_app_msg_cnt, d.mobile_push_cnt, d.mob_sesn_cnt, d.mob_impr_cnt, 
            d.linkedin_ads_cnt, d.email_send_cnt, d.audience_usage_cnt, d.email_preview_cnt, 
            d.dm_destinations_total_id_cnt, d.mai_scored_project_cnt, d.google_ads_cnt, d.outbound_api_cnt, 
            d.sms_output_result_cnt, d.web_sesn_cnt, d.plan_users_cnt, d.sms_send_cnt, 
            d.dm_destinations_total_row_cnt, d.db_size, d.asset_size, d.admin_user_cnt, 
            d.sms_output_result_str, d.sms_send_str, d.sms_frag_str, d.event_day  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : DAILY_USAGE_tmp , DAILY_USAGE , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: DATA_VIEW_DETAILS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DATA_VIEW_DETAILS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DATA_VIEW_DETAILS_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table DATA_VIEW_DETAILS_tmp , DATA_VIEW_DETAILS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=DATA_VIEW_DETAILS , table_keys=%str(EVENT_ID), out_table=work.DATA_VIEW_DETAILS );
      DATA work.DATA_VIEW_DETAILS_tmp ;
         SET work.DATA_VIEW_DETAILS ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : DATA_VIEW_DETAILS_tmp , DATA_VIEW_DETAILS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..DATA_VIEW_DETAILS_tmp as select * from &dbschema..DATA_VIEW_DETAILS where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : DATA_VIEW_DETAILS_tmp , DATA_VIEW_DETAILS , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.DATA_VIEW_DETAILS_tmp  base=&tmplib..DATA_VIEW_DETAILS_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : DATA_VIEW_DETAILS_tmp , DATA_VIEW_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..DATA_VIEW_DETAILS AS b USING &tmpdbschema..DATA_VIEW_DETAILS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            total_cost_amt = d.total_cost_amt, 
            properties_map_doc = d.properties_map_doc, load_dttm = d.load_dttm, 
            data_view_dttm_tz = d.data_view_dttm_tz, data_view_dttm = d.data_view_dttm, 
            visit_id_hex = d.visit_id_hex, visit_id = d.visit_id, 
            session_id_hex = d.session_id_hex, session_id = d.session_id, 
            reserved_2_txt = d.reserved_2_txt, reserved_1_txt = d.reserved_1_txt, 
            parent_event_designed_id = d.parent_event_designed_id, identity_id = d.identity_id, 
            event_nm = d.event_nm, event_designed_id = d.event_designed_id, 
            detail_id_hex = d.detail_id_hex, detail_id = d.detail_id, 
            channel_user_id = d.channel_user_id
         WHEN NOT MATCHED THEN INSERT (
            total_cost_amt, properties_map_doc, load_dttm, 
            data_view_dttm_tz, data_view_dttm, visit_id_hex, visit_id, 
            session_id_hex, session_id, reserved_2_txt, reserved_1_txt, 
            parent_event_designed_id, identity_id, event_nm, event_id, 
            event_designed_id, detail_id_hex, detail_id, channel_user_id
         ) VALUES (
            d.total_cost_amt, d.properties_map_doc, d.load_dttm, 
            d.data_view_dttm_tz, d.data_view_dttm, d.visit_id_hex, d.visit_id, 
            d.session_id_hex, d.session_id, d.reserved_2_txt, d.reserved_1_txt, 
            d.parent_event_designed_id, d.identity_id, d.event_nm, d.event_id, 
            d.event_designed_id, d.detail_id_hex, d.detail_id, d.channel_user_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : DATA_VIEW_DETAILS_tmp , DATA_VIEW_DETAILS , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: DBT_ADV_CAMPAIGN_VISITORS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DBT_ADV_CAMPAIGN_VISITORS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_ADV_CAMPAIGN_VISITORS_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table DBT_ADV_CAMPAIGN_VISITORS_tmp , DBT_ADV_CAMPAIGN_VISITORS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=DBT_ADV_CAMPAIGN_VISITORS , table_keys=%str(SESSION_ID,VISIT_ID), out_table=work.DBT_ADV_CAMPAIGN_VISITORS );
      DATA work.DBT_ADV_CAMPAIGN_VISITORS_tmp ;
         SET work.DBT_ADV_CAMPAIGN_VISITORS ;
         WHERE 1=1 AND SESSION_ID IS NOT NULL AND VISIT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : DBT_ADV_CAMPAIGN_VISITORS_tmp , DBT_ADV_CAMPAIGN_VISITORS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..DBT_ADV_CAMPAIGN_VISITORS_tmp as select * from &dbschema..DBT_ADV_CAMPAIGN_VISITORS where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : DBT_ADV_CAMPAIGN_VISITORS_tmp , DBT_ADV_CAMPAIGN_VISITORS , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.DBT_ADV_CAMPAIGN_VISITORS_tmp  base=&tmplib..DBT_ADV_CAMPAIGN_VISITORS_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : DBT_ADV_CAMPAIGN_VISITORS_tmp , DBT_ADV_CAMPAIGN_VISITORS );
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
            rv_revenue = d.rv_revenue, ge_latitude = d.ge_latitude, 
            co_conversions = d.co_conversions, page_views = d.page_views, 
            average_visit_duration = d.average_visit_duration, visits = d.visits, 
            new_visitors = d.new_visitors, return_visitors = d.return_visitors, 
            bouncers = d.bouncers, session_start_dttm = d.session_start_dttm, 
            session_start_dttm_tz = d.session_start_dttm_tz, visit_dttm_tz = d.visit_dttm_tz, 
            session_complete_load_dttm = d.session_complete_load_dttm, visit_dttm = d.visit_dttm, 
            visitor_type = d.visitor_type, visitor_id = d.visitor_id, 
            visit_origination_tracking_code = d.visit_origination_tracking_code, visit_origination_name = d.visit_origination_name, 
            visit_origination_creative = d.visit_origination_creative, se_external_search_engine_domain = d.se_external_search_engine_domain, 
            se_external_search_engine = d.se_external_search_engine, se_external_search_engine_phrase = d.se_external_search_engine_phrase, 
            pl_device_operating_system = d.pl_device_operating_system, visit_origination_placement = d.visit_origination_placement, 
            landing_page_url_domain = d.landing_page_url_domain, landing_page_url = d.landing_page_url, 
            visit_origination_type = d.visit_origination_type, landing_page = d.landing_page, 
            ge_state_region = d.ge_state_region, ge_country = d.ge_country, 
            ge_city = d.ge_city, device_type = d.device_type, 
            device_name = d.device_name, cu_customer_id = d.cu_customer_id, 
            br_browser_version = d.br_browser_version, br_browser_name = d.br_browser_name, 
            bouncer = d.bouncer
         WHEN NOT MATCHED THEN INSERT (
            ge_longitude, rv_revenue, ge_latitude, 
            co_conversions, page_views, average_visit_duration, visits, 
            new_visitors, return_visitors, bouncers, session_start_dttm, 
            session_start_dttm_tz, visit_dttm_tz, session_complete_load_dttm, visit_dttm, 
            visitor_type, visitor_id, visit_origination_tracking_code, visit_origination_name, 
            visit_origination_creative, visit_id, se_external_search_engine_domain, se_external_search_engine, 
            se_external_search_engine_phrase, session_id, pl_device_operating_system, visit_origination_placement, 
            landing_page_url_domain, landing_page_url, visit_origination_type, landing_page, 
            ge_state_region, ge_country, ge_city, device_type, 
            device_name, cu_customer_id, br_browser_version, br_browser_name, 
            bouncer
         ) VALUES (
            d.ge_longitude, d.rv_revenue, d.ge_latitude, 
            d.co_conversions, d.page_views, d.average_visit_duration, d.visits, 
            d.new_visitors, d.return_visitors, d.bouncers, d.session_start_dttm, 
            d.session_start_dttm_tz, d.visit_dttm_tz, d.session_complete_load_dttm, d.visit_dttm, 
            d.visitor_type, d.visitor_id, d.visit_origination_tracking_code, d.visit_origination_name, 
            d.visit_origination_creative, d.visit_id, d.se_external_search_engine_domain, d.se_external_search_engine, 
            d.se_external_search_engine_phrase, d.session_id, d.pl_device_operating_system, d.visit_origination_placement, 
            d.landing_page_url_domain, d.landing_page_url, d.visit_origination_type, d.landing_page, 
            d.ge_state_region, d.ge_country, d.ge_city, d.device_type, 
            d.device_name, d.cu_customer_id, d.br_browser_version, d.br_browser_name, 
            d.bouncer  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : DBT_ADV_CAMPAIGN_VISITORS_tmp , DBT_ADV_CAMPAIGN_VISITORS , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: DBT_BUSINESS_PROCESS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DBT_BUSINESS_PROCESS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_BUSINESS_PROCESS_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table DBT_BUSINESS_PROCESS_tmp , DBT_BUSINESS_PROCESS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=DBT_BUSINESS_PROCESS , table_keys=%str(BUSINESS_PROCESS_NAME,BUSINESS_PROCESS_STEP_NAME,BUS_PROCESS_STARTED_DTTM,SESSION_ID), out_table=work.DBT_BUSINESS_PROCESS );
      DATA work.DBT_BUSINESS_PROCESS_tmp ;
         SET work.DBT_BUSINESS_PROCESS ;
         WHERE 1=1 AND BUSINESS_PROCESS_NAME IS NOT NULL AND BUSINESS_PROCESS_STEP_NAME IS NOT NULL AND BUS_PROCESS_STARTED_DTTM IS NOT NULL AND SESSION_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : DBT_BUSINESS_PROCESS_tmp , DBT_BUSINESS_PROCESS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..DBT_BUSINESS_PROCESS_tmp as select * from &dbschema..DBT_BUSINESS_PROCESS where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : DBT_BUSINESS_PROCESS_tmp , DBT_BUSINESS_PROCESS , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.DBT_BUSINESS_PROCESS_tmp  base=&tmplib..DBT_BUSINESS_PROCESS_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : DBT_BUSINESS_PROCESS_tmp , DBT_BUSINESS_PROCESS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..DBT_BUSINESS_PROCESS AS b USING &tmpdbschema..DBT_BUSINESS_PROCESS_tmp AS d ON (
            b.bus_process_started_dttm = d.bus_process_started_dttm AND 
            b.session_id = d.session_id AND b.business_process_name = d.business_process_name AND 
            b.business_process_step_name = d.business_process_step_name )
         WHEN MATCHED THEN
         UPDATE SET
            processes_abandoned = d.processes_abandoned, 
            processes_completed = d.processes_completed, steps = d.steps, 
            steps_completed = d.steps_completed, steps_abandoned = d.steps_abandoned, 
            processes = d.processes, last_step = d.last_step, 
            step_count = d.step_count, bus_process_started_dttm_tz = d.bus_process_started_dttm_tz, 
            session_start_dttm = d.session_start_dttm, session_complete_load_dttm = d.session_complete_load_dttm, 
            session_start_dttm_tz = d.session_start_dttm_tz, visitor_type = d.visitor_type, 
            visit_origination_type = d.visit_origination_type, visit_origination_placement = d.visit_origination_placement, 
            visit_origination_creative = d.visit_origination_creative, device_type = d.device_type, 
            cu_customer_id = d.cu_customer_id, business_process_attribute_2 = d.business_process_attribute_2, 
            business_process_attribute_1 = d.business_process_attribute_1, bouncer = d.bouncer, 
            device_name = d.device_name, visit_id = d.visit_id, 
            visit_origination_name = d.visit_origination_name, visit_origination_tracking_code = d.visit_origination_tracking_code, 
            visitor_id = d.visitor_id
         WHEN NOT MATCHED THEN INSERT (
            processes_abandoned, processes_completed, steps, 
            steps_completed, steps_abandoned, processes, last_step, 
            step_count, bus_process_started_dttm_tz, session_start_dttm, bus_process_started_dttm, 
            session_complete_load_dttm, session_start_dttm_tz, visitor_type, visit_origination_type, 
            visit_origination_placement, visit_origination_creative, session_id, device_type, 
            cu_customer_id, business_process_name, business_process_attribute_2, business_process_attribute_1, 
            bouncer, business_process_step_name, device_name, visit_id, 
            visit_origination_name, visit_origination_tracking_code, visitor_id
         ) VALUES (
            d.processes_abandoned, d.processes_completed, d.steps, 
            d.steps_completed, d.steps_abandoned, d.processes, d.last_step, 
            d.step_count, d.bus_process_started_dttm_tz, d.session_start_dttm, d.bus_process_started_dttm, 
            d.session_complete_load_dttm, d.session_start_dttm_tz, d.visitor_type, d.visit_origination_type, 
            d.visit_origination_placement, d.visit_origination_creative, d.session_id, d.device_type, 
            d.cu_customer_id, d.business_process_name, d.business_process_attribute_2, d.business_process_attribute_1, 
            d.bouncer, d.business_process_step_name, d.device_name, d.visit_id, 
            d.visit_origination_name, d.visit_origination_tracking_code, d.visitor_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : DBT_BUSINESS_PROCESS_tmp , DBT_BUSINESS_PROCESS , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: DBT_CONTENT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DBT_CONTENT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_CONTENT_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table DBT_CONTENT_tmp , DBT_CONTENT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=DBT_CONTENT , table_keys=%str(DETAIL_ID), out_table=work.DBT_CONTENT );
      DATA work.DBT_CONTENT_tmp ;
         SET work.DBT_CONTENT ;
         WHERE 1=1 AND DETAIL_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : DBT_CONTENT_tmp , DBT_CONTENT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..DBT_CONTENT_tmp as select * from &dbschema..DBT_CONTENT where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : DBT_CONTENT_tmp , DBT_CONTENT , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.DBT_CONTENT_tmp  base=&tmplib..DBT_CONTENT_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : DBT_CONTENT_tmp , DBT_CONTENT );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..DBT_CONTENT AS b USING &tmpdbschema..DBT_CONTENT_tmp AS d ON (
            b.detail_id = d.detail_id )
         WHEN MATCHED THEN
         UPDATE SET
            total_page_view_time = d.total_page_view_time, 
            entry_pages = d.entry_pages, visits = d.visits, 
            views = d.views, active_page_view_time = d.active_page_view_time, 
            bouncers = d.bouncers, exit_pages = d.exit_pages, 
            session_start_dttm = d.session_start_dttm, detail_dttm = d.detail_dttm, 
            detail_dttm_tz = d.detail_dttm_tz, session_start_dttm_tz = d.session_start_dttm_tz, 
            session_complete_load_dttm = d.session_complete_load_dttm, visitor_type = d.visitor_type, 
            visitor_id = d.visitor_id, visit_origination_type = d.visit_origination_type, 
            visit_origination_tracking_code = d.visit_origination_tracking_code, visit_origination_placement = d.visit_origination_placement, 
            visit_origination_creative = d.visit_origination_creative, session_id = d.session_id, 
            pg_page_url = d.pg_page_url, pg_page = d.pg_page, 
            pg_domain_name = d.pg_domain_name, device_name = d.device_name, 
            cu_customer_id = d.cu_customer_id, class1_id = d.class1_id, 
            bouncer = d.bouncer, class2_id = d.class2_id, 
            device_type = d.device_type, visit_id = d.visit_id, 
            visit_origination_name = d.visit_origination_name
         WHEN NOT MATCHED THEN INSERT (
            total_page_view_time, entry_pages, visits, 
            views, active_page_view_time, bouncers, exit_pages, 
            session_start_dttm, detail_dttm, detail_dttm_tz, session_start_dttm_tz, 
            session_complete_load_dttm, visitor_type, visitor_id, visit_origination_type, 
            visit_origination_tracking_code, visit_origination_placement, visit_origination_creative, session_id, 
            pg_page_url, pg_page, pg_domain_name, device_name, 
            cu_customer_id, class1_id, bouncer, class2_id, 
            detail_id, device_type, visit_id, visit_origination_name
         ) VALUES (
            d.total_page_view_time, d.entry_pages, d.visits, 
            d.views, d.active_page_view_time, d.bouncers, d.exit_pages, 
            d.session_start_dttm, d.detail_dttm, d.detail_dttm_tz, d.session_start_dttm_tz, 
            d.session_complete_load_dttm, d.visitor_type, d.visitor_id, d.visit_origination_type, 
            d.visit_origination_tracking_code, d.visit_origination_placement, d.visit_origination_creative, d.session_id, 
            d.pg_page_url, d.pg_page, d.pg_domain_name, d.device_name, 
            d.cu_customer_id, d.class1_id, d.bouncer, d.class2_id, 
            d.detail_id, d.device_type, d.visit_id, d.visit_origination_name  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : DBT_CONTENT_tmp , DBT_CONTENT , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: DBT_DOCUMENTS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DBT_DOCUMENTS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_DOCUMENTS_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table DBT_DOCUMENTS_tmp , DBT_DOCUMENTS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=DBT_DOCUMENTS , table_keys=%str(DETAIL_ID), out_table=work.DBT_DOCUMENTS );
      DATA work.DBT_DOCUMENTS_tmp ;
         SET work.DBT_DOCUMENTS ;
         WHERE 1=1 AND DETAIL_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : DBT_DOCUMENTS_tmp , DBT_DOCUMENTS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..DBT_DOCUMENTS_tmp as select * from &dbschema..DBT_DOCUMENTS where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : DBT_DOCUMENTS_tmp , DBT_DOCUMENTS , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.DBT_DOCUMENTS_tmp  base=&tmplib..DBT_DOCUMENTS_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : DBT_DOCUMENTS_tmp , DBT_DOCUMENTS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..DBT_DOCUMENTS AS b USING &tmpdbschema..DBT_DOCUMENTS_tmp AS d ON (
            b.detail_id = d.detail_id )
         WHEN MATCHED THEN
         UPDATE SET
            document_downloads = d.document_downloads, 
            document_download_dttm = d.document_download_dttm, session_complete_load_dttm = d.session_complete_load_dttm, 
            document_download_dttm_tz = d.document_download_dttm_tz, session_start_dttm = d.session_start_dttm, 
            session_start_dttm_tz = d.session_start_dttm_tz, visitor_type = d.visitor_type, 
            visitor_id = d.visitor_id, visit_origination_type = d.visit_origination_type, 
            visit_origination_tracking_code = d.visit_origination_tracking_code, visit_origination_name = d.visit_origination_name, 
            visit_id = d.visit_id, do_page_url = d.do_page_url, 
            device_type = d.device_type, class2_id = d.class2_id, 
            bouncer = d.bouncer, class1_id = d.class1_id, 
            cu_customer_id = d.cu_customer_id, device_name = d.device_name, 
            do_page_description = d.do_page_description, session_id = d.session_id, 
            visit_origination_creative = d.visit_origination_creative, visit_origination_placement = d.visit_origination_placement
         WHEN NOT MATCHED THEN INSERT (
            document_downloads, document_download_dttm, session_complete_load_dttm, 
            document_download_dttm_tz, session_start_dttm, session_start_dttm_tz, visitor_type, 
            visitor_id, visit_origination_type, visit_origination_tracking_code, visit_origination_name, 
            visit_id, do_page_url, device_type, detail_id, 
            class2_id, bouncer, class1_id, cu_customer_id, 
            device_name, do_page_description, session_id, visit_origination_creative, 
            visit_origination_placement
         ) VALUES (
            d.document_downloads, d.document_download_dttm, d.session_complete_load_dttm, 
            d.document_download_dttm_tz, d.session_start_dttm, d.session_start_dttm_tz, d.visitor_type, 
            d.visitor_id, d.visit_origination_type, d.visit_origination_tracking_code, d.visit_origination_name, 
            d.visit_id, d.do_page_url, d.device_type, d.detail_id, 
            d.class2_id, d.bouncer, d.class1_id, d.cu_customer_id, 
            d.device_name, d.do_page_description, d.session_id, d.visit_origination_creative, 
            d.visit_origination_placement  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : DBT_DOCUMENTS_tmp , DBT_DOCUMENTS , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: DBT_ECOMMERCE has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DBT_ECOMMERCE;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_ECOMMERCE_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table DBT_ECOMMERCE_tmp , DBT_ECOMMERCE_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=DBT_ECOMMERCE , table_keys=%str(BASKET_ID,PRODUCT_ACTIVITY_DTTM,PRODUCT_ID,PRODUCT_NAME,PRODUCT_SKU,VISIT_ID), out_table=work.DBT_ECOMMERCE );
      DATA work.DBT_ECOMMERCE_tmp ;
         SET work.DBT_ECOMMERCE ;
         WHERE 1=1 AND BASKET_ID IS NOT NULL AND PRODUCT_ACTIVITY_DTTM IS NOT NULL AND PRODUCT_ID IS NOT NULL AND PRODUCT_NAME IS NOT NULL AND PRODUCT_SKU IS NOT NULL AND VISIT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : DBT_ECOMMERCE_tmp , DBT_ECOMMERCE_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..DBT_ECOMMERCE_tmp as select * from &dbschema..DBT_ECOMMERCE where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : DBT_ECOMMERCE_tmp , DBT_ECOMMERCE , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.DBT_ECOMMERCE_tmp  base=&tmplib..DBT_ECOMMERCE_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : DBT_ECOMMERCE_tmp , DBT_ECOMMERCE );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..DBT_ECOMMERCE AS b USING &tmpdbschema..DBT_ECOMMERCE_tmp AS d ON (
            b.product_activity_dttm = d.product_activity_dttm AND 
            b.visit_id = d.visit_id AND b.product_sku = d.product_sku AND 
            b.product_name = d.product_name AND b.basket_id = d.basket_id AND 
            b.product_id = d.product_id )
         WHEN MATCHED THEN
         UPDATE SET
            basket_removes_revenue = d.basket_removes_revenue, 
            basket_adds_revenue = d.basket_adds_revenue, product_purchase_revenues = d.product_purchase_revenues, 
            basket_adds_units = d.basket_adds_units, basket_adds = d.basket_adds, 
            product_views = d.product_views, product_purchase_units = d.product_purchase_units, 
            basket_removes_units = d.basket_removes_units, product_purchases = d.product_purchases, 
            basket_removes = d.basket_removes, baskets_completed = d.baskets_completed, 
            baskets_abandoned = d.baskets_abandoned, baskets_started = d.baskets_started, 
            product_activity_dttm_tz = d.product_activity_dttm_tz, session_start_dttm = d.session_start_dttm, 
            session_complete_load_dttm = d.session_complete_load_dttm, session_start_dttm_tz = d.session_start_dttm_tz, 
            visitor_id = d.visitor_id, visit_origination_tracking_code = d.visit_origination_tracking_code, 
            visit_origination_name = d.visit_origination_name, product_group_name = d.product_group_name, 
            device_name = d.device_name, bouncer = d.bouncer, 
            cu_customer_id = d.cu_customer_id, device_type = d.device_type, 
            session_id = d.session_id, visit_origination_creative = d.visit_origination_creative, 
            visit_origination_placement = d.visit_origination_placement, visit_origination_type = d.visit_origination_type, 
            visitor_type = d.visitor_type
         WHEN NOT MATCHED THEN INSERT (
            basket_removes_revenue, basket_adds_revenue, product_purchase_revenues, 
            basket_adds_units, basket_adds, product_views, product_purchase_units, 
            basket_removes_units, product_purchases, basket_removes, baskets_completed, 
            baskets_abandoned, baskets_started, product_activity_dttm, product_activity_dttm_tz, 
            session_start_dttm, session_complete_load_dttm, session_start_dttm_tz, visitor_id, 
            visit_origination_tracking_code, visit_origination_name, visit_id, product_sku, 
            product_name, product_group_name, device_name, bouncer, 
            basket_id, cu_customer_id, device_type, product_id, 
            session_id, visit_origination_creative, visit_origination_placement, visit_origination_type, 
            visitor_type
         ) VALUES (
            d.basket_removes_revenue, d.basket_adds_revenue, d.product_purchase_revenues, 
            d.basket_adds_units, d.basket_adds, d.product_views, d.product_purchase_units, 
            d.basket_removes_units, d.product_purchases, d.basket_removes, d.baskets_completed, 
            d.baskets_abandoned, d.baskets_started, d.product_activity_dttm, d.product_activity_dttm_tz, 
            d.session_start_dttm, d.session_complete_load_dttm, d.session_start_dttm_tz, d.visitor_id, 
            d.visit_origination_tracking_code, d.visit_origination_name, d.visit_id, d.product_sku, 
            d.product_name, d.product_group_name, d.device_name, d.bouncer, 
            d.basket_id, d.cu_customer_id, d.device_type, d.product_id, 
            d.session_id, d.visit_origination_creative, d.visit_origination_placement, d.visit_origination_type, 
            d.visitor_type  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : DBT_ECOMMERCE_tmp , DBT_ECOMMERCE , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: DBT_FORMS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DBT_FORMS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_FORMS_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table DBT_FORMS_tmp , DBT_FORMS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=DBT_FORMS , table_keys=%str(DETAIL_ID), out_table=work.DBT_FORMS );
      DATA work.DBT_FORMS_tmp ;
         SET work.DBT_FORMS ;
         WHERE 1=1 AND DETAIL_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : DBT_FORMS_tmp , DBT_FORMS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..DBT_FORMS_tmp as select * from &dbschema..DBT_FORMS where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : DBT_FORMS_tmp , DBT_FORMS , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.DBT_FORMS_tmp  base=&tmplib..DBT_FORMS_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : DBT_FORMS_tmp , DBT_FORMS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..DBT_FORMS AS b USING &tmpdbschema..DBT_FORMS_tmp AS d ON (
            b.detail_id = d.detail_id )
         WHEN MATCHED THEN
         UPDATE SET
            attempts = d.attempts, 
            forms_started = d.forms_started, forms_completed = d.forms_completed, 
            forms_not_submitted = d.forms_not_submitted, form_attempt_dttm = d.form_attempt_dttm, 
            session_complete_load_dttm = d.session_complete_load_dttm, form_attempt_dttm_tz = d.form_attempt_dttm_tz, 
            session_start_dttm_tz = d.session_start_dttm_tz, session_start_dttm = d.session_start_dttm, 
            visitor_type = d.visitor_type, visit_origination_type = d.visit_origination_type, 
            visit_origination_placement = d.visit_origination_placement, visit_origination_creative = d.visit_origination_creative, 
            visit_id = d.visit_id, session_id = d.session_id, 
            last_field = d.last_field, form_nm = d.form_nm, 
            device_name = d.device_name, cu_customer_id = d.cu_customer_id, 
            bouncer = d.bouncer, device_type = d.device_type, 
            visit_origination_name = d.visit_origination_name, visit_origination_tracking_code = d.visit_origination_tracking_code, 
            visitor_id = d.visitor_id
         WHEN NOT MATCHED THEN INSERT (
            attempts, forms_started, forms_completed, 
            forms_not_submitted, form_attempt_dttm, session_complete_load_dttm, form_attempt_dttm_tz, 
            session_start_dttm_tz, session_start_dttm, visitor_type, visit_origination_type, 
            visit_origination_placement, visit_origination_creative, visit_id, session_id, 
            last_field, form_nm, device_name, detail_id, 
            cu_customer_id, bouncer, device_type, visit_origination_name, 
            visit_origination_tracking_code, visitor_id
         ) VALUES (
            d.attempts, d.forms_started, d.forms_completed, 
            d.forms_not_submitted, d.form_attempt_dttm, d.session_complete_load_dttm, d.form_attempt_dttm_tz, 
            d.session_start_dttm_tz, d.session_start_dttm, d.visitor_type, d.visit_origination_type, 
            d.visit_origination_placement, d.visit_origination_creative, d.visit_id, d.session_id, 
            d.last_field, d.form_nm, d.device_name, d.detail_id, 
            d.cu_customer_id, d.bouncer, d.device_type, d.visit_origination_name, 
            d.visit_origination_tracking_code, d.visitor_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : DBT_FORMS_tmp , DBT_FORMS , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: DBT_GOALS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DBT_GOALS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_GOALS_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table DBT_GOALS_tmp , DBT_GOALS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=DBT_GOALS , table_keys=%str(DETAIL_ID), out_table=work.DBT_GOALS );
      DATA work.DBT_GOALS_tmp ;
         SET work.DBT_GOALS ;
         WHERE 1=1 AND DETAIL_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : DBT_GOALS_tmp , DBT_GOALS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..DBT_GOALS_tmp as select * from &dbschema..DBT_GOALS where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : DBT_GOALS_tmp , DBT_GOALS , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.DBT_GOALS_tmp  base=&tmplib..DBT_GOALS_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : DBT_GOALS_tmp , DBT_GOALS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..DBT_GOALS AS b USING &tmpdbschema..DBT_GOALS_tmp AS d ON (
            b.detail_id = d.detail_id )
         WHEN MATCHED THEN
         UPDATE SET
            goal_revenue = d.goal_revenue, 
            visits = d.visits, session_complete_load_dttm = d.session_complete_load_dttm, 
            session_start_dttm = d.session_start_dttm, goal_reached_dttm_tz = d.goal_reached_dttm_tz, 
            session_start_dttm_tz = d.session_start_dttm_tz, goal_reached_dttm = d.goal_reached_dttm, 
            goals = d.goals, visit_origination_tracking_code = d.visit_origination_tracking_code, 
            visit_id = d.visit_id, device_name = d.device_name, 
            cu_customer_id = d.cu_customer_id, goal_group_name = d.goal_group_name, 
            bouncer = d.bouncer, session_id = d.session_id, 
            visit_origination_name = d.visit_origination_name, visitor_id = d.visitor_id, 
            device_type = d.device_type, goal_name = d.goal_name, 
            visit_origination_creative = d.visit_origination_creative, visit_origination_placement = d.visit_origination_placement, 
            visit_origination_type = d.visit_origination_type, visitor_type = d.visitor_type
         WHEN NOT MATCHED THEN INSERT (
            goal_revenue, visits, session_complete_load_dttm, 
            session_start_dttm, goal_reached_dttm_tz, session_start_dttm_tz, goal_reached_dttm, 
            goals, visit_origination_tracking_code, visit_id, device_name, 
            cu_customer_id, goal_group_name, bouncer, session_id, 
            visit_origination_name, visitor_id, detail_id, device_type, 
            goal_name, visit_origination_creative, visit_origination_placement, visit_origination_type, 
            visitor_type
         ) VALUES (
            d.goal_revenue, d.visits, d.session_complete_load_dttm, 
            d.session_start_dttm, d.goal_reached_dttm_tz, d.session_start_dttm_tz, d.goal_reached_dttm, 
            d.goals, d.visit_origination_tracking_code, d.visit_id, d.device_name, 
            d.cu_customer_id, d.goal_group_name, d.bouncer, d.session_id, 
            d.visit_origination_name, d.visitor_id, d.detail_id, d.device_type, 
            d.goal_name, d.visit_origination_creative, d.visit_origination_placement, d.visit_origination_type, 
            d.visitor_type  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : DBT_GOALS_tmp , DBT_GOALS , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: DBT_MEDIA_CONSUMPTION has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DBT_MEDIA_CONSUMPTION;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_MEDIA_CONSUMPTION_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table DBT_MEDIA_CONSUMPTION_tmp , DBT_MEDIA_CONSUMPTION_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=DBT_MEDIA_CONSUMPTION , table_keys=%str(DETAIL_ID,INTERACTIONS_COUNT,MAXIMUM_PROGRESS,MEDIA_COMPLETION_RATE,MEDIA_SECTION,VISIT_ID), out_table=work.DBT_MEDIA_CONSUMPTION );
      DATA work.DBT_MEDIA_CONSUMPTION_tmp ;
         SET work.DBT_MEDIA_CONSUMPTION ;
         WHERE 1=1 AND DETAIL_ID IS NOT NULL AND INTERACTIONS_COUNT IS NOT NULL AND MAXIMUM_PROGRESS IS NOT NULL AND MEDIA_COMPLETION_RATE IS NOT NULL AND MEDIA_SECTION IS NOT NULL AND VISIT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : DBT_MEDIA_CONSUMPTION_tmp , DBT_MEDIA_CONSUMPTION_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..DBT_MEDIA_CONSUMPTION_tmp as select * from &dbschema..DBT_MEDIA_CONSUMPTION where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : DBT_MEDIA_CONSUMPTION_tmp , DBT_MEDIA_CONSUMPTION , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.DBT_MEDIA_CONSUMPTION_tmp  base=&tmplib..DBT_MEDIA_CONSUMPTION_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : DBT_MEDIA_CONSUMPTION_tmp , DBT_MEDIA_CONSUMPTION );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..DBT_MEDIA_CONSUMPTION AS b USING &tmpdbschema..DBT_MEDIA_CONSUMPTION_tmp AS d ON (
            b.maximum_progress = d.maximum_progress AND 
            b.interactions_count = d.interactions_count AND b.visit_id = d.visit_id AND 
            b.detail_id = d.detail_id AND b.media_completion_rate = d.media_completion_rate AND 
            b.media_section = d.media_section )
         WHEN MATCHED THEN
         UPDATE SET
            time_viewing = d.time_viewing, 
            duration = d.duration, content_viewed = d.content_viewed, 
            counter = d.counter, session_complete_load_dttm = d.session_complete_load_dttm, 
            media_start_dttm = d.media_start_dttm, media_start_dttm_tz = d.media_start_dttm_tz, 
            session_start_dttm_tz = d.session_start_dttm_tz, session_start_dttm = d.session_start_dttm, 
            media_section_view = d.media_section_view, views_completed = d.views_completed, 
            views = d.views, views_started = d.views_started, 
            visitor_id = d.visitor_id, visit_origination_name = d.visit_origination_name, 
            media_name = d.media_name, device_name = d.device_name, 
            cu_customer_id = d.cu_customer_id, visit_origination_placement = d.visit_origination_placement, 
            visit_origination_tracking_code = d.visit_origination_tracking_code, bouncer = d.bouncer, 
            device_type = d.device_type, media_uri_txt = d.media_uri_txt, 
            session_id = d.session_id, visit_origination_creative = d.visit_origination_creative, 
            visit_origination_type = d.visit_origination_type, visitor_type = d.visitor_type
         WHEN NOT MATCHED THEN INSERT (
            maximum_progress, time_viewing, duration, 
            content_viewed, counter, interactions_count, session_complete_load_dttm, 
            media_start_dttm, media_start_dttm_tz, session_start_dttm_tz, session_start_dttm, 
            media_section_view, views_completed, views, views_started, 
            visitor_id, visit_origination_name, media_name, device_name, 
            cu_customer_id, visit_id, visit_origination_placement, visit_origination_tracking_code, 
            bouncer, detail_id, device_type, media_completion_rate, 
            media_section, media_uri_txt, session_id, visit_origination_creative, 
            visit_origination_type, visitor_type
         ) VALUES (
            d.maximum_progress, d.time_viewing, d.duration, 
            d.content_viewed, d.counter, d.interactions_count, d.session_complete_load_dttm, 
            d.media_start_dttm, d.media_start_dttm_tz, d.session_start_dttm_tz, d.session_start_dttm, 
            d.media_section_view, d.views_completed, d.views, d.views_started, 
            d.visitor_id, d.visit_origination_name, d.media_name, d.device_name, 
            d.cu_customer_id, d.visit_id, d.visit_origination_placement, d.visit_origination_tracking_code, 
            d.bouncer, d.detail_id, d.device_type, d.media_completion_rate, 
            d.media_section, d.media_uri_txt, d.session_id, d.visit_origination_creative, 
            d.visit_origination_type, d.visitor_type  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : DBT_MEDIA_CONSUMPTION_tmp , DBT_MEDIA_CONSUMPTION , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: DBT_PROMOTIONS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DBT_PROMOTIONS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_PROMOTIONS_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table DBT_PROMOTIONS_tmp , DBT_PROMOTIONS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=DBT_PROMOTIONS , table_keys=%str(DETAIL_ID), out_table=work.DBT_PROMOTIONS );
      DATA work.DBT_PROMOTIONS_tmp ;
         SET work.DBT_PROMOTIONS ;
         WHERE 1=1 AND DETAIL_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : DBT_PROMOTIONS_tmp , DBT_PROMOTIONS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..DBT_PROMOTIONS_tmp as select * from &dbschema..DBT_PROMOTIONS where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : DBT_PROMOTIONS_tmp , DBT_PROMOTIONS , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.DBT_PROMOTIONS_tmp  base=&tmplib..DBT_PROMOTIONS_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : DBT_PROMOTIONS_tmp , DBT_PROMOTIONS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..DBT_PROMOTIONS AS b USING &tmpdbschema..DBT_PROMOTIONS_tmp AS d ON (
            b.detail_id = d.detail_id )
         WHEN MATCHED THEN
         UPDATE SET
            displays = d.displays, 
            click_throughs = d.click_throughs, promotion_shown_dttm = d.promotion_shown_dttm, 
            promotion_shown_dttm_tz = d.promotion_shown_dttm_tz, session_complete_load_dttm = d.session_complete_load_dttm, 
            session_start_dttm_tz = d.session_start_dttm_tz, session_start_dttm = d.session_start_dttm, 
            visit_origination_tracking_code = d.visit_origination_tracking_code, visit_id = d.visit_id, 
            cu_customer_id = d.cu_customer_id, bouncer = d.bouncer, 
            device_name = d.device_name, promotion_creative = d.promotion_creative, 
            promotion_name = d.promotion_name, promotion_tracking_code = d.promotion_tracking_code, 
            visit_origination_name = d.visit_origination_name, visitor_id = d.visitor_id, 
            device_type = d.device_type, promotion_placement = d.promotion_placement, 
            promotion_type = d.promotion_type, session_id = d.session_id, 
            visit_origination_creative = d.visit_origination_creative, visit_origination_placement = d.visit_origination_placement, 
            visit_origination_type = d.visit_origination_type, visitor_type = d.visitor_type
         WHEN NOT MATCHED THEN INSERT (
            displays, click_throughs, promotion_shown_dttm, 
            promotion_shown_dttm_tz, session_complete_load_dttm, session_start_dttm_tz, session_start_dttm, 
            visit_origination_tracking_code, visit_id, cu_customer_id, bouncer, 
            device_name, promotion_creative, promotion_name, promotion_tracking_code, 
            visit_origination_name, visitor_id, detail_id, device_type, 
            promotion_placement, promotion_type, session_id, visit_origination_creative, 
            visit_origination_placement, visit_origination_type, visitor_type
         ) VALUES (
            d.displays, d.click_throughs, d.promotion_shown_dttm, 
            d.promotion_shown_dttm_tz, d.session_complete_load_dttm, d.session_start_dttm_tz, d.session_start_dttm, 
            d.visit_origination_tracking_code, d.visit_id, d.cu_customer_id, d.bouncer, 
            d.device_name, d.promotion_creative, d.promotion_name, d.promotion_tracking_code, 
            d.visit_origination_name, d.visitor_id, d.detail_id, d.device_type, 
            d.promotion_placement, d.promotion_type, d.session_id, d.visit_origination_creative, 
            d.visit_origination_placement, d.visit_origination_type, d.visitor_type  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : DBT_PROMOTIONS_tmp , DBT_PROMOTIONS , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: DBT_SEARCH has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DBT_SEARCH;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DBT_SEARCH_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table DBT_SEARCH_tmp , DBT_SEARCH_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=DBT_SEARCH , table_keys=%str(DETAIL_ID), out_table=work.DBT_SEARCH );
      DATA work.DBT_SEARCH_tmp ;
         SET work.DBT_SEARCH ;
         WHERE 1=1 AND DETAIL_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : DBT_SEARCH_tmp , DBT_SEARCH_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..DBT_SEARCH_tmp as select * from &dbschema..DBT_SEARCH where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : DBT_SEARCH_tmp , DBT_SEARCH , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.DBT_SEARCH_tmp  base=&tmplib..DBT_SEARCH_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : DBT_SEARCH_tmp , DBT_SEARCH );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..DBT_SEARCH AS b USING &tmpdbschema..DBT_SEARCH_tmp AS d ON (
            b.detail_id = d.detail_id )
         WHEN MATCHED THEN
         UPDATE SET
            searches = d.searches, 
            visits = d.visits, search_unknown_results = d.search_unknown_results, 
            search_returned_results = d.search_returned_results, search_no_results_returned = d.search_no_results_returned, 
            num_pages_viewed_afterwards = d.num_pages_viewed_afterwards, num_additional_searches = d.num_additional_searches, 
            exit_pages = d.exit_pages, session_start_dttm_tz = d.session_start_dttm_tz, 
            search_results_dttm = d.search_results_dttm, session_complete_load_dttm = d.session_complete_load_dttm, 
            search_results_dttm_tz = d.search_results_dttm_tz, session_start_dttm = d.session_start_dttm, 
            visit_origination_tracking_code = d.visit_origination_tracking_code, visit_id = d.visit_id, 
            device_type = d.device_type, bouncer = d.bouncer, 
            cu_customer_id = d.cu_customer_id, internal_search_term = d.internal_search_term, 
            visit_origination_name = d.visit_origination_name, visit_origination_placement = d.visit_origination_placement, 
            visitor_id = d.visitor_id, device_name = d.device_name, 
            search_name = d.search_name, session_id = d.session_id, 
            visit_origination_creative = d.visit_origination_creative, visit_origination_type = d.visit_origination_type, 
            visitor_type = d.visitor_type
         WHEN NOT MATCHED THEN INSERT (
            searches, visits, search_unknown_results, 
            search_returned_results, search_no_results_returned, num_pages_viewed_afterwards, num_additional_searches, 
            exit_pages, session_start_dttm_tz, search_results_dttm, session_complete_load_dttm, 
            search_results_dttm_tz, session_start_dttm, visit_origination_tracking_code, visit_id, 
            device_type, detail_id, bouncer, cu_customer_id, 
            internal_search_term, visit_origination_name, visit_origination_placement, visitor_id, 
            device_name, search_name, session_id, visit_origination_creative, 
            visit_origination_type, visitor_type
         ) VALUES (
            d.searches, d.visits, d.search_unknown_results, 
            d.search_returned_results, d.search_no_results_returned, d.num_pages_viewed_afterwards, d.num_additional_searches, 
            d.exit_pages, d.session_start_dttm_tz, d.search_results_dttm, d.session_complete_load_dttm, 
            d.search_results_dttm_tz, d.session_start_dttm, d.visit_origination_tracking_code, d.visit_id, 
            d.device_type, d.detail_id, d.bouncer, d.cu_customer_id, 
            d.internal_search_term, d.visit_origination_name, d.visit_origination_placement, d.visitor_id, 
            d.device_name, d.search_name, d.session_id, d.visit_origination_creative, 
            d.visit_origination_type, d.visitor_type  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : DBT_SEARCH_tmp , DBT_SEARCH , err_macro=SYSDBRC);
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
%if %sysfunc(exist(&udmmart..DECISION_EXECUTION)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid=%sysfunc(open(&udmmart..DECISION_EXECUTION));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if &nrows = 0 %then %do;
      %put NOTE: DECISION_EXECUTION has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DECISION_EXECUTION;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DECISION_EXECUTION_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table DECISION_EXECUTION_tmp , DECISION_EXECUTION_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=DECISION_EXECUTION , table_keys=%str(EXECUTION_ID), out_table=work.DECISION_EXECUTION );
      DATA work.DECISION_EXECUTION_tmp ;
         SET work.DECISION_EXECUTION ;
         WHERE 1=1 AND EXECUTION_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : DECISION_EXECUTION_tmp , DECISION_EXECUTION_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..DECISION_EXECUTION_tmp as select * from &dbschema..DECISION_EXECUTION where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : DECISION_EXECUTION_tmp , DECISION_EXECUTION , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.DECISION_EXECUTION_tmp  base=&tmplib..DECISION_EXECUTION_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : DECISION_EXECUTION_tmp , DECISION_EXECUTION );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..DECISION_EXECUTION AS b USING &tmpdbschema..DECISION_EXECUTION_tmp AS d ON (
            b.execution_id = d.execution_id )
         WHEN MATCHED THEN
         UPDATE SET
            input_json = d.input_json, 
            output_json = d.output_json, execution_dttm = d.execution_dttm, 
            environment = d.environment, hostname = d.hostname, 
            decision_id = d.decision_id, event_nm = d.event_nm, 
            image_url = d.image_url
         WHEN NOT MATCHED THEN INSERT (
            input_json, output_json, execution_dttm, 
            environment, hostname, decision_id, event_nm, 
            execution_id, image_url
         ) VALUES (
            d.input_json, d.output_json, d.execution_dttm, 
            d.environment, d.hostname, d.decision_id, d.event_nm, 
            d.execution_id, d.image_url  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : DECISION_EXECUTION_tmp , DECISION_EXECUTION , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..DECISION_EXECUTION_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DECISION_EXECUTION_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DECISION_EXECUTION;
         DROP TABLE work.DECISION_EXECUTION;
      QUIT;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table DECISION_EXECUTION;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..DIRECT_CONTACT)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid=%sysfunc(open(&udmmart..DIRECT_CONTACT));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if &nrows = 0 %then %do;
      %put NOTE: DIRECT_CONTACT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DIRECT_CONTACT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DIRECT_CONTACT_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table DIRECT_CONTACT_tmp , DIRECT_CONTACT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=DIRECT_CONTACT , table_keys=%str(EVENT_ID), out_table=work.DIRECT_CONTACT );
      DATA work.DIRECT_CONTACT_tmp ;
         SET work.DIRECT_CONTACT ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : DIRECT_CONTACT_tmp , DIRECT_CONTACT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..DIRECT_CONTACT_tmp as select * from &dbschema..DIRECT_CONTACT where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : DIRECT_CONTACT_tmp , DIRECT_CONTACT , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.DIRECT_CONTACT_tmp  base=&tmplib..DIRECT_CONTACT_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : DIRECT_CONTACT_tmp , DIRECT_CONTACT );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..DIRECT_CONTACT AS b USING &tmpdbschema..DIRECT_CONTACT_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            control_group_flg = d.control_group_flg, 
            control_active_flg = d.control_active_flg, properties_map_doc = d.properties_map_doc, 
            direct_contact_dttm = d.direct_contact_dttm, direct_contact_dttm_tz = d.direct_contact_dttm_tz, 
            load_dttm = d.load_dttm, segment_id = d.segment_id, 
            message_id = d.message_id, event_nm = d.event_nm, 
            context_type_nm = d.context_type_nm, channel_nm = d.channel_nm, 
            event_designed_id = d.event_designed_id, identity_type_nm = d.identity_type_nm, 
            task_version_id = d.task_version_id, channel_user_id = d.channel_user_id, 
            context_val = d.context_val, identity_id = d.identity_id, 
            occurrence_id = d.occurrence_id, response_tracking_cd = d.response_tracking_cd, 
            task_id = d.task_id
         WHEN NOT MATCHED THEN INSERT (
            control_group_flg, control_active_flg, properties_map_doc, 
            direct_contact_dttm, direct_contact_dttm_tz, load_dttm, segment_id, 
            message_id, event_nm, context_type_nm, channel_nm, 
            event_designed_id, identity_type_nm, task_version_id, channel_user_id, 
            context_val, event_id, identity_id, occurrence_id, 
            response_tracking_cd, task_id
         ) VALUES (
            d.control_group_flg, d.control_active_flg, d.properties_map_doc, 
            d.direct_contact_dttm, d.direct_contact_dttm_tz, d.load_dttm, d.segment_id, 
            d.message_id, d.event_nm, d.context_type_nm, d.channel_nm, 
            d.event_designed_id, d.identity_type_nm, d.task_version_id, d.channel_user_id, 
            d.context_val, d.event_id, d.identity_id, d.occurrence_id, 
            d.response_tracking_cd, d.task_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : DIRECT_CONTACT_tmp , DIRECT_CONTACT , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: DOCUMENT_DETAILS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..DOCUMENT_DETAILS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..DOCUMENT_DETAILS_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table DOCUMENT_DETAILS_tmp , DOCUMENT_DETAILS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=DOCUMENT_DETAILS , table_keys=%str(EVENT_ID), out_table=work.DOCUMENT_DETAILS );
      DATA work.DOCUMENT_DETAILS_tmp ;
         SET work.DOCUMENT_DETAILS ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : DOCUMENT_DETAILS_tmp , DOCUMENT_DETAILS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..DOCUMENT_DETAILS_tmp as select * from &dbschema..DOCUMENT_DETAILS where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : DOCUMENT_DETAILS_tmp , DOCUMENT_DETAILS , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.DOCUMENT_DETAILS_tmp  base=&tmplib..DOCUMENT_DETAILS_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : DOCUMENT_DETAILS_tmp , DOCUMENT_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..DOCUMENT_DETAILS AS b USING &tmpdbschema..DOCUMENT_DETAILS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            link_event_dttm = d.link_event_dttm, 
            link_event_dttm_tz = d.link_event_dttm_tz, load_dttm = d.load_dttm, 
            visit_id_hex = d.visit_id_hex, session_id = d.session_id, 
            link_id = d.link_id, event_source_cd = d.event_source_cd, 
            detail_id = d.detail_id, identity_id = d.identity_id, 
            link_name = d.link_name, link_selector_path = d.link_selector_path, 
            uri_txt = d.uri_txt, alt_txt = d.alt_txt, 
            detail_id_hex = d.detail_id_hex, event_key_cd = d.event_key_cd, 
            session_id_hex = d.session_id_hex, visit_id = d.visit_id
         WHEN NOT MATCHED THEN INSERT (
            link_event_dttm, link_event_dttm_tz, load_dttm, 
            visit_id_hex, session_id, link_id, event_source_cd, 
            detail_id, event_id, identity_id, link_name, 
            link_selector_path, uri_txt, alt_txt, detail_id_hex, 
            event_key_cd, session_id_hex, visit_id
         ) VALUES (
            d.link_event_dttm, d.link_event_dttm_tz, d.load_dttm, 
            d.visit_id_hex, d.session_id, d.link_id, d.event_source_cd, 
            d.detail_id, d.event_id, d.identity_id, d.link_name, 
            d.link_selector_path, d.uri_txt, d.alt_txt, d.detail_id_hex, 
            d.event_key_cd, d.session_id_hex, d.visit_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : DOCUMENT_DETAILS_tmp , DOCUMENT_DETAILS , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: EMAIL_BOUNCE has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..EMAIL_BOUNCE;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EMAIL_BOUNCE_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table EMAIL_BOUNCE_tmp , EMAIL_BOUNCE_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=EMAIL_BOUNCE , table_keys=%str(EVENT_ID), out_table=work.EMAIL_BOUNCE );
      DATA work.EMAIL_BOUNCE_tmp ;
         SET work.EMAIL_BOUNCE ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : EMAIL_BOUNCE_tmp , EMAIL_BOUNCE_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..EMAIL_BOUNCE_tmp as select * from &dbschema..EMAIL_BOUNCE where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : EMAIL_BOUNCE_tmp , EMAIL_BOUNCE , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.EMAIL_BOUNCE_tmp  base=&tmplib..EMAIL_BOUNCE_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : EMAIL_BOUNCE_tmp , EMAIL_BOUNCE );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..EMAIL_BOUNCE AS b USING &tmpdbschema..EMAIL_BOUNCE_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            test_flg = d.test_flg, 
            properties_map_doc = d.properties_map_doc, email_bounce_dttm = d.email_bounce_dttm, 
            email_bounce_dttm_tz = d.email_bounce_dttm_tz, load_dttm = d.load_dttm, 
            task_id = d.task_id, segment_version_id = d.segment_version_id, 
            response_tracking_cd = d.response_tracking_cd, occurrence_id = d.occurrence_id, 
            journey_occurrence_id = d.journey_occurrence_id, event_nm = d.event_nm, 
            context_type_nm = d.context_type_nm, bounce_class_cd = d.bounce_class_cd, 
            aud_occurrence_id = d.aud_occurrence_id, audience_id = d.audience_id, 
            context_val = d.context_val, event_designed_id = d.event_designed_id, 
            imprint_id = d.imprint_id, reason_txt = d.reason_txt, 
            recipient_domain_nm = d.recipient_domain_nm, segment_id = d.segment_id, 
            subject_line_txt = d.subject_line_txt, task_version_id = d.task_version_id, 
            analysis_group_id = d.analysis_group_id, channel_user_id = d.channel_user_id, 
            identity_id = d.identity_id, journey_id = d.journey_id, 
            program_id = d.program_id, raw_reason_txt = d.raw_reason_txt
         WHEN NOT MATCHED THEN INSERT (
            test_flg, properties_map_doc, email_bounce_dttm, 
            email_bounce_dttm_tz, load_dttm, task_id, segment_version_id, 
            response_tracking_cd, occurrence_id, journey_occurrence_id, event_nm, 
            context_type_nm, bounce_class_cd, aud_occurrence_id, audience_id, 
            context_val, event_designed_id, imprint_id, reason_txt, 
            recipient_domain_nm, segment_id, subject_line_txt, task_version_id, 
            analysis_group_id, channel_user_id, event_id, identity_id, 
            journey_id, program_id, raw_reason_txt
         ) VALUES (
            d.test_flg, d.properties_map_doc, d.email_bounce_dttm, 
            d.email_bounce_dttm_tz, d.load_dttm, d.task_id, d.segment_version_id, 
            d.response_tracking_cd, d.occurrence_id, d.journey_occurrence_id, d.event_nm, 
            d.context_type_nm, d.bounce_class_cd, d.aud_occurrence_id, d.audience_id, 
            d.context_val, d.event_designed_id, d.imprint_id, d.reason_txt, 
            d.recipient_domain_nm, d.segment_id, d.subject_line_txt, d.task_version_id, 
            d.analysis_group_id, d.channel_user_id, d.event_id, d.identity_id, 
            d.journey_id, d.program_id, d.raw_reason_txt  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : EMAIL_BOUNCE_tmp , EMAIL_BOUNCE , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: EMAIL_CLICK has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..EMAIL_CLICK;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EMAIL_CLICK_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table EMAIL_CLICK_tmp , EMAIL_CLICK_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=EMAIL_CLICK , table_keys=%str(EVENT_ID), out_table=work.EMAIL_CLICK );
      DATA work.EMAIL_CLICK_tmp ;
         SET work.EMAIL_CLICK ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : EMAIL_CLICK_tmp , EMAIL_CLICK_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..EMAIL_CLICK_tmp as select * from &dbschema..EMAIL_CLICK where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : EMAIL_CLICK_tmp , EMAIL_CLICK , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.EMAIL_CLICK_tmp  base=&tmplib..EMAIL_CLICK_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : EMAIL_CLICK_tmp , EMAIL_CLICK );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..EMAIL_CLICK AS b USING &tmpdbschema..EMAIL_CLICK_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            click_tracking_flg = d.click_tracking_flg, 
            is_mobile_flg = d.is_mobile_flg, open_tracking_flg = d.open_tracking_flg, 
            test_flg = d.test_flg, properties_map_doc = d.properties_map_doc, 
            email_click_dttm = d.email_click_dttm, email_click_dttm_tz = d.email_click_dttm_tz, 
            load_dttm = d.load_dttm, platform_desc = d.platform_desc, 
            occurrence_id = d.occurrence_id, mailbox_provider_nm = d.mailbox_provider_nm, 
            link_tracking_label_txt = d.link_tracking_label_txt, link_tracking_group_txt = d.link_tracking_group_txt, 
            journey_id = d.journey_id, imprint_id = d.imprint_id, 
            event_nm = d.event_nm, event_designed_id = d.event_designed_id, 
            context_val = d.context_val, audience_id = d.audience_id, 
            analysis_group_id = d.analysis_group_id, agent_family_nm = d.agent_family_nm, 
            aud_occurrence_id = d.aud_occurrence_id, channel_user_id = d.channel_user_id, 
            context_type_nm = d.context_type_nm, device_nm = d.device_nm, 
            identity_id = d.identity_id, journey_occurrence_id = d.journey_occurrence_id, 
            link_tracking_id = d.link_tracking_id, manufacturer_nm = d.manufacturer_nm, 
            platform_version = d.platform_version, user_agent_nm = d.user_agent_nm, 
            uri_txt = d.uri_txt, task_version_id = d.task_version_id, 
            task_id = d.task_id, subject_line_txt = d.subject_line_txt, 
            segment_version_id = d.segment_version_id, segment_id = d.segment_id, 
            response_tracking_cd = d.response_tracking_cd, recipient_domain_nm = d.recipient_domain_nm, 
            program_id = d.program_id
         WHEN NOT MATCHED THEN INSERT (
            click_tracking_flg, is_mobile_flg, open_tracking_flg, 
            test_flg, properties_map_doc, email_click_dttm, email_click_dttm_tz, 
            load_dttm, platform_desc, occurrence_id, mailbox_provider_nm, 
            link_tracking_label_txt, link_tracking_group_txt, journey_id, imprint_id, 
            event_nm, event_designed_id, context_val, audience_id, 
            analysis_group_id, agent_family_nm, aud_occurrence_id, channel_user_id, 
            context_type_nm, device_nm, event_id, identity_id, 
            journey_occurrence_id, link_tracking_id, manufacturer_nm, platform_version, 
            user_agent_nm, uri_txt, task_version_id, task_id, 
            subject_line_txt, segment_version_id, segment_id, response_tracking_cd, 
            recipient_domain_nm, program_id
         ) VALUES (
            d.click_tracking_flg, d.is_mobile_flg, d.open_tracking_flg, 
            d.test_flg, d.properties_map_doc, d.email_click_dttm, d.email_click_dttm_tz, 
            d.load_dttm, d.platform_desc, d.occurrence_id, d.mailbox_provider_nm, 
            d.link_tracking_label_txt, d.link_tracking_group_txt, d.journey_id, d.imprint_id, 
            d.event_nm, d.event_designed_id, d.context_val, d.audience_id, 
            d.analysis_group_id, d.agent_family_nm, d.aud_occurrence_id, d.channel_user_id, 
            d.context_type_nm, d.device_nm, d.event_id, d.identity_id, 
            d.journey_occurrence_id, d.link_tracking_id, d.manufacturer_nm, d.platform_version, 
            d.user_agent_nm, d.uri_txt, d.task_version_id, d.task_id, 
            d.subject_line_txt, d.segment_version_id, d.segment_id, d.response_tracking_cd, 
            d.recipient_domain_nm, d.program_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : EMAIL_CLICK_tmp , EMAIL_CLICK , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: EMAIL_COMPLAINT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..EMAIL_COMPLAINT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EMAIL_COMPLAINT_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table EMAIL_COMPLAINT_tmp , EMAIL_COMPLAINT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=EMAIL_COMPLAINT , table_keys=%str(EVENT_ID), out_table=work.EMAIL_COMPLAINT );
      DATA work.EMAIL_COMPLAINT_tmp ;
         SET work.EMAIL_COMPLAINT ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : EMAIL_COMPLAINT_tmp , EMAIL_COMPLAINT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..EMAIL_COMPLAINT_tmp as select * from &dbschema..EMAIL_COMPLAINT where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : EMAIL_COMPLAINT_tmp , EMAIL_COMPLAINT , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.EMAIL_COMPLAINT_tmp  base=&tmplib..EMAIL_COMPLAINT_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : EMAIL_COMPLAINT_tmp , EMAIL_COMPLAINT );
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
            email_complaint_dttm_tz = d.email_complaint_dttm_tz, email_complaint_dttm = d.email_complaint_dttm, 
            task_version_id = d.task_version_id, task_id = d.task_id, 
            subject_line_txt = d.subject_line_txt, segment_version_id = d.segment_version_id, 
            segment_id = d.segment_id, response_tracking_cd = d.response_tracking_cd, 
            recipient_domain_nm = d.recipient_domain_nm, program_id = d.program_id, 
            occurrence_id = d.occurrence_id, journey_occurrence_id = d.journey_occurrence_id, 
            journey_id = d.journey_id, imprint_id = d.imprint_id, 
            identity_id = d.identity_id, event_nm = d.event_nm, 
            event_designed_id = d.event_designed_id, context_val = d.context_val, 
            context_type_nm = d.context_type_nm, channel_user_id = d.channel_user_id, 
            audience_id = d.audience_id, aud_occurrence_id = d.aud_occurrence_id, 
            analysis_group_id = d.analysis_group_id
         WHEN NOT MATCHED THEN INSERT (
            test_flg, properties_map_doc, load_dttm, 
            email_complaint_dttm_tz, email_complaint_dttm, task_version_id, task_id, 
            subject_line_txt, segment_version_id, segment_id, response_tracking_cd, 
            recipient_domain_nm, program_id, occurrence_id, journey_occurrence_id, 
            journey_id, imprint_id, identity_id, event_nm, 
            event_id, event_designed_id, context_val, context_type_nm, 
            channel_user_id, audience_id, aud_occurrence_id, analysis_group_id
         ) VALUES (
            d.test_flg, d.properties_map_doc, d.load_dttm, 
            d.email_complaint_dttm_tz, d.email_complaint_dttm, d.task_version_id, d.task_id, 
            d.subject_line_txt, d.segment_version_id, d.segment_id, d.response_tracking_cd, 
            d.recipient_domain_nm, d.program_id, d.occurrence_id, d.journey_occurrence_id, 
            d.journey_id, d.imprint_id, d.identity_id, d.event_nm, 
            d.event_id, d.event_designed_id, d.context_val, d.context_type_nm, 
            d.channel_user_id, d.audience_id, d.aud_occurrence_id, d.analysis_group_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : EMAIL_COMPLAINT_tmp , EMAIL_COMPLAINT , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: EMAIL_OPEN has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..EMAIL_OPEN;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EMAIL_OPEN_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table EMAIL_OPEN_tmp , EMAIL_OPEN_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=EMAIL_OPEN , table_keys=%str(EVENT_ID), out_table=work.EMAIL_OPEN );
      DATA work.EMAIL_OPEN_tmp ;
         SET work.EMAIL_OPEN ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : EMAIL_OPEN_tmp , EMAIL_OPEN_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..EMAIL_OPEN_tmp as select * from &dbschema..EMAIL_OPEN where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : EMAIL_OPEN_tmp , EMAIL_OPEN , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.EMAIL_OPEN_tmp  base=&tmplib..EMAIL_OPEN_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : EMAIL_OPEN_tmp , EMAIL_OPEN );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..EMAIL_OPEN AS b USING &tmpdbschema..EMAIL_OPEN_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            test_flg = d.test_flg, 
            prefetched_flg = d.prefetched_flg, is_mobile_flg = d.is_mobile_flg, 
            click_tracking_flg = d.click_tracking_flg, open_tracking_flg = d.open_tracking_flg, 
            properties_map_doc = d.properties_map_doc, email_open_dttm_tz = d.email_open_dttm_tz, 
            load_dttm = d.load_dttm, email_open_dttm = d.email_open_dttm, 
            user_agent_nm = d.user_agent_nm, task_version_id = d.task_version_id, 
            task_id = d.task_id, subject_line_txt = d.subject_line_txt, 
            segment_version_id = d.segment_version_id, segment_id = d.segment_id, 
            response_tracking_cd = d.response_tracking_cd, recipient_domain_nm = d.recipient_domain_nm, 
            program_id = d.program_id, platform_version = d.platform_version, 
            platform_desc = d.platform_desc, occurrence_id = d.occurrence_id, 
            manufacturer_nm = d.manufacturer_nm, mailbox_provider_nm = d.mailbox_provider_nm, 
            journey_occurrence_id = d.journey_occurrence_id, journey_id = d.journey_id, 
            imprint_id = d.imprint_id, identity_id = d.identity_id, 
            event_nm = d.event_nm, event_designed_id = d.event_designed_id, 
            device_nm = d.device_nm, context_val = d.context_val, 
            context_type_nm = d.context_type_nm, channel_user_id = d.channel_user_id, 
            audience_id = d.audience_id, aud_occurrence_id = d.aud_occurrence_id, 
            analysis_group_id = d.analysis_group_id, agent_family_nm = d.agent_family_nm
         WHEN NOT MATCHED THEN INSERT (
            test_flg, prefetched_flg, is_mobile_flg, 
            click_tracking_flg, open_tracking_flg, properties_map_doc, email_open_dttm_tz, 
            load_dttm, email_open_dttm, user_agent_nm, task_version_id, 
            task_id, subject_line_txt, segment_version_id, segment_id, 
            response_tracking_cd, recipient_domain_nm, program_id, platform_version, 
            platform_desc, occurrence_id, manufacturer_nm, mailbox_provider_nm, 
            journey_occurrence_id, journey_id, imprint_id, identity_id, 
            event_nm, event_id, event_designed_id, device_nm, 
            context_val, context_type_nm, channel_user_id, audience_id, 
            aud_occurrence_id, analysis_group_id, agent_family_nm
         ) VALUES (
            d.test_flg, d.prefetched_flg, d.is_mobile_flg, 
            d.click_tracking_flg, d.open_tracking_flg, d.properties_map_doc, d.email_open_dttm_tz, 
            d.load_dttm, d.email_open_dttm, d.user_agent_nm, d.task_version_id, 
            d.task_id, d.subject_line_txt, d.segment_version_id, d.segment_id, 
            d.response_tracking_cd, d.recipient_domain_nm, d.program_id, d.platform_version, 
            d.platform_desc, d.occurrence_id, d.manufacturer_nm, d.mailbox_provider_nm, 
            d.journey_occurrence_id, d.journey_id, d.imprint_id, d.identity_id, 
            d.event_nm, d.event_id, d.event_designed_id, d.device_nm, 
            d.context_val, d.context_type_nm, d.channel_user_id, d.audience_id, 
            d.aud_occurrence_id, d.analysis_group_id, d.agent_family_nm  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : EMAIL_OPEN_tmp , EMAIL_OPEN , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: EMAIL_OPTOUT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..EMAIL_OPTOUT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EMAIL_OPTOUT_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table EMAIL_OPTOUT_tmp , EMAIL_OPTOUT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=EMAIL_OPTOUT , table_keys=%str(EVENT_ID), out_table=work.EMAIL_OPTOUT );
      DATA work.EMAIL_OPTOUT_tmp ;
         SET work.EMAIL_OPTOUT ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : EMAIL_OPTOUT_tmp , EMAIL_OPTOUT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..EMAIL_OPTOUT_tmp as select * from &dbschema..EMAIL_OPTOUT where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : EMAIL_OPTOUT_tmp , EMAIL_OPTOUT , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.EMAIL_OPTOUT_tmp  base=&tmplib..EMAIL_OPTOUT_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : EMAIL_OPTOUT_tmp , EMAIL_OPTOUT );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..EMAIL_OPTOUT AS b USING &tmpdbschema..EMAIL_OPTOUT_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            test_flg = d.test_flg, 
            properties_map_doc = d.properties_map_doc, load_dttm = d.load_dttm, 
            email_optout_dttm_tz = d.email_optout_dttm_tz, email_optout_dttm = d.email_optout_dttm, 
            task_version_id = d.task_version_id, task_id = d.task_id, 
            subject_line_txt = d.subject_line_txt, segment_version_id = d.segment_version_id, 
            segment_id = d.segment_id, response_tracking_cd = d.response_tracking_cd, 
            recipient_domain_nm = d.recipient_domain_nm, program_id = d.program_id, 
            optout_type_nm = d.optout_type_nm, occurrence_id = d.occurrence_id, 
            link_tracking_label_txt = d.link_tracking_label_txt, link_tracking_id = d.link_tracking_id, 
            link_tracking_group_txt = d.link_tracking_group_txt, journey_occurrence_id = d.journey_occurrence_id, 
            journey_id = d.journey_id, imprint_id = d.imprint_id, 
            identity_id = d.identity_id, event_nm = d.event_nm, 
            event_designed_id = d.event_designed_id, context_val = d.context_val, 
            context_type_nm = d.context_type_nm, channel_user_id = d.channel_user_id, 
            audience_id = d.audience_id, aud_occurrence_id = d.aud_occurrence_id, 
            analysis_group_id = d.analysis_group_id
         WHEN NOT MATCHED THEN INSERT (
            test_flg, properties_map_doc, load_dttm, 
            email_optout_dttm_tz, email_optout_dttm, task_version_id, task_id, 
            subject_line_txt, segment_version_id, segment_id, response_tracking_cd, 
            recipient_domain_nm, program_id, optout_type_nm, occurrence_id, 
            link_tracking_label_txt, link_tracking_id, link_tracking_group_txt, journey_occurrence_id, 
            journey_id, imprint_id, identity_id, event_nm, 
            event_id, event_designed_id, context_val, context_type_nm, 
            channel_user_id, audience_id, aud_occurrence_id, analysis_group_id
         ) VALUES (
            d.test_flg, d.properties_map_doc, d.load_dttm, 
            d.email_optout_dttm_tz, d.email_optout_dttm, d.task_version_id, d.task_id, 
            d.subject_line_txt, d.segment_version_id, d.segment_id, d.response_tracking_cd, 
            d.recipient_domain_nm, d.program_id, d.optout_type_nm, d.occurrence_id, 
            d.link_tracking_label_txt, d.link_tracking_id, d.link_tracking_group_txt, d.journey_occurrence_id, 
            d.journey_id, d.imprint_id, d.identity_id, d.event_nm, 
            d.event_id, d.event_designed_id, d.context_val, d.context_type_nm, 
            d.channel_user_id, d.audience_id, d.aud_occurrence_id, d.analysis_group_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : EMAIL_OPTOUT_tmp , EMAIL_OPTOUT , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: EMAIL_OPTOUT_DETAILS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..EMAIL_OPTOUT_DETAILS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EMAIL_OPTOUT_DETAILS_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table EMAIL_OPTOUT_DETAILS_tmp , EMAIL_OPTOUT_DETAILS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=EMAIL_OPTOUT_DETAILS , table_keys=%str(EVENT_ID), out_table=work.EMAIL_OPTOUT_DETAILS );
      DATA work.EMAIL_OPTOUT_DETAILS_tmp ;
         SET work.EMAIL_OPTOUT_DETAILS ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : EMAIL_OPTOUT_DETAILS_tmp , EMAIL_OPTOUT_DETAILS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..EMAIL_OPTOUT_DETAILS_tmp as select * from &dbschema..EMAIL_OPTOUT_DETAILS where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : EMAIL_OPTOUT_DETAILS_tmp , EMAIL_OPTOUT_DETAILS , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.EMAIL_OPTOUT_DETAILS_tmp  base=&tmplib..EMAIL_OPTOUT_DETAILS_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : EMAIL_OPTOUT_DETAILS_tmp , EMAIL_OPTOUT_DETAILS );
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
            task_version_id = d.task_version_id, task_id = d.task_id, 
            subject_line_txt = d.subject_line_txt, segment_version_id = d.segment_version_id, 
            segment_id = d.segment_id, response_tracking_cd = d.response_tracking_cd, 
            recipient_domain_nm = d.recipient_domain_nm, program_id = d.program_id, 
            optout_type_nm = d.optout_type_nm, occurrence_id = d.occurrence_id, 
            journey_occurrence_id = d.journey_occurrence_id, journey_id = d.journey_id, 
            imprint_id = d.imprint_id, identity_id = d.identity_id, 
            event_nm = d.event_nm, event_designed_id = d.event_designed_id, 
            email_address = d.email_address, context_val = d.context_val, 
            context_type_nm = d.context_type_nm, audience_id = d.audience_id, 
            aud_occurrence_id = d.aud_occurrence_id, analysis_group_id = d.analysis_group_id
         WHEN NOT MATCHED THEN INSERT (
            test_flg, properties_map_doc, email_action_dttm_tz, 
            email_action_dttm, load_dttm, task_version_id, task_id, 
            subject_line_txt, segment_version_id, segment_id, response_tracking_cd, 
            recipient_domain_nm, program_id, optout_type_nm, occurrence_id, 
            journey_occurrence_id, journey_id, imprint_id, identity_id, 
            event_nm, event_id, event_designed_id, email_address, 
            context_val, context_type_nm, audience_id, aud_occurrence_id, 
            analysis_group_id
         ) VALUES (
            d.test_flg, d.properties_map_doc, d.email_action_dttm_tz, 
            d.email_action_dttm, d.load_dttm, d.task_version_id, d.task_id, 
            d.subject_line_txt, d.segment_version_id, d.segment_id, d.response_tracking_cd, 
            d.recipient_domain_nm, d.program_id, d.optout_type_nm, d.occurrence_id, 
            d.journey_occurrence_id, d.journey_id, d.imprint_id, d.identity_id, 
            d.event_nm, d.event_id, d.event_designed_id, d.email_address, 
            d.context_val, d.context_type_nm, d.audience_id, d.aud_occurrence_id, 
            d.analysis_group_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : EMAIL_OPTOUT_DETAILS_tmp , EMAIL_OPTOUT_DETAILS , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: EMAIL_REPLY has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..EMAIL_REPLY;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EMAIL_REPLY_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table EMAIL_REPLY_tmp , EMAIL_REPLY_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=EMAIL_REPLY , table_keys=%str(EVENT_ID), out_table=work.EMAIL_REPLY );
      DATA work.EMAIL_REPLY_tmp ;
         SET work.EMAIL_REPLY ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : EMAIL_REPLY_tmp , EMAIL_REPLY_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..EMAIL_REPLY_tmp as select * from &dbschema..EMAIL_REPLY where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : EMAIL_REPLY_tmp , EMAIL_REPLY , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.EMAIL_REPLY_tmp  base=&tmplib..EMAIL_REPLY_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : EMAIL_REPLY_tmp , EMAIL_REPLY );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..EMAIL_REPLY AS b USING &tmpdbschema..EMAIL_REPLY_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            test_flg = d.test_flg, 
            properties_map_doc = d.properties_map_doc, load_dttm = d.load_dttm, 
            email_reply_dttm = d.email_reply_dttm, email_reply_dttm_tz = d.email_reply_dttm_tz, 
            uri_txt = d.uri_txt, task_version_id = d.task_version_id, 
            task_id = d.task_id, subject_line_txt = d.subject_line_txt, 
            segment_version_id = d.segment_version_id, segment_id = d.segment_id, 
            response_tracking_cd = d.response_tracking_cd, recipient_domain_nm = d.recipient_domain_nm, 
            program_id = d.program_id, occurrence_id = d.occurrence_id, 
            journey_occurrence_id = d.journey_occurrence_id, journey_id = d.journey_id, 
            imprint_id = d.imprint_id, identity_id = d.identity_id, 
            event_nm = d.event_nm, event_designed_id = d.event_designed_id, 
            context_val = d.context_val, context_type_nm = d.context_type_nm, 
            channel_user_id = d.channel_user_id, audience_id = d.audience_id, 
            aud_occurrence_id = d.aud_occurrence_id, analysis_group_id = d.analysis_group_id
         WHEN NOT MATCHED THEN INSERT (
            test_flg, properties_map_doc, load_dttm, 
            email_reply_dttm, email_reply_dttm_tz, uri_txt, task_version_id, 
            task_id, subject_line_txt, segment_version_id, segment_id, 
            response_tracking_cd, recipient_domain_nm, program_id, occurrence_id, 
            journey_occurrence_id, journey_id, imprint_id, identity_id, 
            event_nm, event_id, event_designed_id, context_val, 
            context_type_nm, channel_user_id, audience_id, aud_occurrence_id, 
            analysis_group_id
         ) VALUES (
            d.test_flg, d.properties_map_doc, d.load_dttm, 
            d.email_reply_dttm, d.email_reply_dttm_tz, d.uri_txt, d.task_version_id, 
            d.task_id, d.subject_line_txt, d.segment_version_id, d.segment_id, 
            d.response_tracking_cd, d.recipient_domain_nm, d.program_id, d.occurrence_id, 
            d.journey_occurrence_id, d.journey_id, d.imprint_id, d.identity_id, 
            d.event_nm, d.event_id, d.event_designed_id, d.context_val, 
            d.context_type_nm, d.channel_user_id, d.audience_id, d.aud_occurrence_id, 
            d.analysis_group_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : EMAIL_REPLY_tmp , EMAIL_REPLY , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: EMAIL_SEND has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..EMAIL_SEND;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EMAIL_SEND_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table EMAIL_SEND_tmp , EMAIL_SEND_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=EMAIL_SEND , table_keys=%str(EVENT_ID), out_table=work.EMAIL_SEND );
      DATA work.EMAIL_SEND_tmp ;
         SET work.EMAIL_SEND ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : EMAIL_SEND_tmp , EMAIL_SEND_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..EMAIL_SEND_tmp as select * from &dbschema..EMAIL_SEND where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : EMAIL_SEND_tmp , EMAIL_SEND , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.EMAIL_SEND_tmp  base=&tmplib..EMAIL_SEND_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : EMAIL_SEND_tmp , EMAIL_SEND );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..EMAIL_SEND AS b USING &tmpdbschema..EMAIL_SEND_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            test_flg = d.test_flg, 
            properties_map_doc = d.properties_map_doc, email_send_dttm = d.email_send_dttm, 
            load_dttm = d.load_dttm, email_send_dttm_tz = d.email_send_dttm_tz, 
            task_version_id = d.task_version_id, task_id = d.task_id, 
            subject_line_txt = d.subject_line_txt, segment_version_id = d.segment_version_id, 
            segment_id = d.segment_id, response_tracking_cd = d.response_tracking_cd, 
            recipient_domain_nm = d.recipient_domain_nm, program_id = d.program_id, 
            occurrence_id = d.occurrence_id, journey_occurrence_id = d.journey_occurrence_id, 
            journey_id = d.journey_id, imprint_url_txt = d.imprint_url_txt, 
            imprint_id = d.imprint_id, identity_id = d.identity_id, 
            event_nm = d.event_nm, event_designed_id = d.event_designed_id, 
            email_send_agent_name = d.email_send_agent_name, context_val = d.context_val, 
            context_type_nm = d.context_type_nm, channel_user_id = d.channel_user_id, 
            audience_id = d.audience_id, aud_occurrence_id = d.aud_occurrence_id, 
            analysis_group_id = d.analysis_group_id
         WHEN NOT MATCHED THEN INSERT (
            test_flg, properties_map_doc, email_send_dttm, 
            load_dttm, email_send_dttm_tz, task_version_id, task_id, 
            subject_line_txt, segment_version_id, segment_id, response_tracking_cd, 
            recipient_domain_nm, program_id, occurrence_id, journey_occurrence_id, 
            journey_id, imprint_url_txt, imprint_id, identity_id, 
            event_nm, event_id, event_designed_id, email_send_agent_name, 
            context_val, context_type_nm, channel_user_id, audience_id, 
            aud_occurrence_id, analysis_group_id
         ) VALUES (
            d.test_flg, d.properties_map_doc, d.email_send_dttm, 
            d.load_dttm, d.email_send_dttm_tz, d.task_version_id, d.task_id, 
            d.subject_line_txt, d.segment_version_id, d.segment_id, d.response_tracking_cd, 
            d.recipient_domain_nm, d.program_id, d.occurrence_id, d.journey_occurrence_id, 
            d.journey_id, d.imprint_url_txt, d.imprint_id, d.identity_id, 
            d.event_nm, d.event_id, d.event_designed_id, d.email_send_agent_name, 
            d.context_val, d.context_type_nm, d.channel_user_id, d.audience_id, 
            d.aud_occurrence_id, d.analysis_group_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : EMAIL_SEND_tmp , EMAIL_SEND , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: EMAIL_VIEW has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..EMAIL_VIEW;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EMAIL_VIEW_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table EMAIL_VIEW_tmp , EMAIL_VIEW_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=EMAIL_VIEW , table_keys=%str(EVENT_ID), out_table=work.EMAIL_VIEW );
      DATA work.EMAIL_VIEW_tmp ;
         SET work.EMAIL_VIEW ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : EMAIL_VIEW_tmp , EMAIL_VIEW_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..EMAIL_VIEW_tmp as select * from &dbschema..EMAIL_VIEW where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : EMAIL_VIEW_tmp , EMAIL_VIEW , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.EMAIL_VIEW_tmp  base=&tmplib..EMAIL_VIEW_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : EMAIL_VIEW_tmp , EMAIL_VIEW );
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
            email_view_dttm_tz = d.email_view_dttm_tz, email_view_dttm = d.email_view_dttm, 
            task_version_id = d.task_version_id, task_id = d.task_id, 
            subject_line_txt = d.subject_line_txt, segment_version_id = d.segment_version_id, 
            segment_id = d.segment_id, response_tracking_cd = d.response_tracking_cd, 
            recipient_domain_nm = d.recipient_domain_nm, program_id = d.program_id, 
            occurrence_id = d.occurrence_id, link_tracking_label_txt = d.link_tracking_label_txt, 
            link_tracking_id = d.link_tracking_id, link_tracking_group_txt = d.link_tracking_group_txt, 
            journey_occurrence_id = d.journey_occurrence_id, journey_id = d.journey_id, 
            imprint_id = d.imprint_id, identity_id = d.identity_id, 
            event_nm = d.event_nm, event_designed_id = d.event_designed_id, 
            context_val = d.context_val, context_type_nm = d.context_type_nm, 
            channel_user_id = d.channel_user_id, audience_id = d.audience_id, 
            aud_occurrence_id = d.aud_occurrence_id, analysis_group_id = d.analysis_group_id
         WHEN NOT MATCHED THEN INSERT (
            test_flg, properties_map_doc, load_dttm, 
            email_view_dttm_tz, email_view_dttm, task_version_id, task_id, 
            subject_line_txt, segment_version_id, segment_id, response_tracking_cd, 
            recipient_domain_nm, program_id, occurrence_id, link_tracking_label_txt, 
            link_tracking_id, link_tracking_group_txt, journey_occurrence_id, journey_id, 
            imprint_id, identity_id, event_nm, event_id, 
            event_designed_id, context_val, context_type_nm, channel_user_id, 
            audience_id, aud_occurrence_id, analysis_group_id
         ) VALUES (
            d.test_flg, d.properties_map_doc, d.load_dttm, 
            d.email_view_dttm_tz, d.email_view_dttm, d.task_version_id, d.task_id, 
            d.subject_line_txt, d.segment_version_id, d.segment_id, d.response_tracking_cd, 
            d.recipient_domain_nm, d.program_id, d.occurrence_id, d.link_tracking_label_txt, 
            d.link_tracking_id, d.link_tracking_group_txt, d.journey_occurrence_id, d.journey_id, 
            d.imprint_id, d.identity_id, d.event_nm, d.event_id, 
            d.event_designed_id, d.context_val, d.context_type_nm, d.channel_user_id, 
            d.audience_id, d.aud_occurrence_id, d.analysis_group_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : EMAIL_VIEW_tmp , EMAIL_VIEW , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: EVENT_ERRORS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..EVENT_ERRORS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EVENT_ERRORS_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table EVENT_ERRORS_tmp , EVENT_ERRORS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=EVENT_ERRORS , table_keys=%str(EVENT_ID), out_table=work.EVENT_ERRORS );
      DATA work.EVENT_ERRORS_tmp ;
         SET work.EVENT_ERRORS ;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : EVENT_ERRORS_tmp , EVENT_ERRORS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..EVENT_ERRORS_tmp as select * from &dbschema..EVENT_ERRORS where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : EVENT_ERRORS_tmp , EVENT_ERRORS , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.EVENT_ERRORS_tmp  base=&tmplib..EVENT_ERRORS_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : EVENT_ERRORS_tmp , EVENT_ERRORS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..EVENT_ERRORS AS b USING &tmpdbschema..EVENT_ERRORS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            error_dttm_tz = d.error_dttm_tz, 
            error_dttm = d.error_dttm, payload_txt = d.payload_txt, 
            ip_address = d.ip_address, event_source_cd = d.event_source_cd, 
            error_txt = d.error_txt, error_cd = d.error_cd
         WHEN NOT MATCHED THEN INSERT (
            error_dttm_tz, error_dttm, payload_txt, 
            ip_address, event_source_cd, event_id, error_txt, 
            error_cd
         ) VALUES (
            d.error_dttm_tz, d.error_dttm, d.payload_txt, 
            d.ip_address, d.event_source_cd, d.event_id, d.error_txt, 
            d.error_cd  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : EVENT_ERRORS_tmp , EVENT_ERRORS , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..EVENT_ERRORS_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EVENT_ERRORS_tmp ;
      QUIT;
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: EXTERNAL_EVENT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..EXTERNAL_EVENT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..EXTERNAL_EVENT_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table EXTERNAL_EVENT_tmp , EXTERNAL_EVENT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=EXTERNAL_EVENT , table_keys=%str(EVENT_ID), out_table=work.EXTERNAL_EVENT );
      DATA work.EXTERNAL_EVENT_tmp ;
         SET work.EXTERNAL_EVENT ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : EXTERNAL_EVENT_tmp , EXTERNAL_EVENT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..EXTERNAL_EVENT_tmp as select * from &dbschema..EXTERNAL_EVENT where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : EXTERNAL_EVENT_tmp , EXTERNAL_EVENT , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.EXTERNAL_EVENT_tmp  base=&tmplib..EXTERNAL_EVENT_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : EXTERNAL_EVENT_tmp , EXTERNAL_EVENT );
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
            event_designed_id = d.event_designed_id, context_val = d.context_val, 
            context_type_nm = d.context_type_nm, channel_user_id = d.channel_user_id, 
            channel_nm = d.channel_nm
         WHEN NOT MATCHED THEN INSERT (
            properties_map_doc, external_event_dttm_tz, load_dttm, 
            external_event_dttm, response_tracking_cd, identity_id, event_nm, 
            event_id, event_designed_id, context_val, context_type_nm, 
            channel_user_id, channel_nm
         ) VALUES (
            d.properties_map_doc, d.external_event_dttm_tz, d.load_dttm, 
            d.external_event_dttm, d.response_tracking_cd, d.identity_id, d.event_nm, 
            d.event_id, d.event_designed_id, d.context_val, d.context_type_nm, 
            d.channel_user_id, d.channel_nm  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : EXTERNAL_EVENT_tmp , EXTERNAL_EVENT , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: FISCAL_CC_BUDGET has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..FISCAL_CC_BUDGET;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..FISCAL_CC_BUDGET) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate FISCAL_CC_BUDGET , FISCAL_CC_BUDGET );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: FORM_DETAILS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..FORM_DETAILS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..FORM_DETAILS_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table FORM_DETAILS_tmp , FORM_DETAILS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=FORM_DETAILS , table_keys=%str(EVENT_ID), out_table=work.FORM_DETAILS );
      DATA work.FORM_DETAILS_tmp ;
         SET work.FORM_DETAILS ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : FORM_DETAILS_tmp , FORM_DETAILS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..FORM_DETAILS_tmp as select * from &dbschema..FORM_DETAILS where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : FORM_DETAILS_tmp , FORM_DETAILS , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.FORM_DETAILS_tmp  base=&tmplib..FORM_DETAILS_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : FORM_DETAILS_tmp , FORM_DETAILS );
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
            form_field_detail_dttm = d.form_field_detail_dttm, form_field_detail_dttm_tz = d.form_field_detail_dttm_tz, 
            load_dttm = d.load_dttm, visit_id_hex = d.visit_id_hex, 
            session_id = d.session_id, identity_id = d.identity_id, 
            form_nm = d.form_nm, form_field_value = d.form_field_value, 
            form_field_id = d.form_field_id, event_key_cd = d.event_key_cd, 
            detail_id_hex = d.detail_id_hex, detail_id = d.detail_id, 
            attempt_status_cd = d.attempt_status_cd, event_source_cd = d.event_source_cd, 
            form_field_nm = d.form_field_nm, session_id_hex = d.session_id_hex, 
            visit_id = d.visit_id
         WHEN NOT MATCHED THEN INSERT (
            submit_flg, change_index_no, attempt_index_cnt, 
            form_field_detail_dttm, form_field_detail_dttm_tz, load_dttm, visit_id_hex, 
            session_id, identity_id, form_nm, form_field_value, 
            form_field_id, event_key_cd, event_id, detail_id_hex, 
            detail_id, attempt_status_cd, event_source_cd, form_field_nm, 
            session_id_hex, visit_id
         ) VALUES (
            d.submit_flg, d.change_index_no, d.attempt_index_cnt, 
            d.form_field_detail_dttm, d.form_field_detail_dttm_tz, d.load_dttm, d.visit_id_hex, 
            d.session_id, d.identity_id, d.form_nm, d.form_field_value, 
            d.form_field_id, d.event_key_cd, d.event_id, d.detail_id_hex, 
            d.detail_id, d.attempt_status_cd, d.event_source_cd, d.form_field_nm, 
            d.session_id_hex, d.visit_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : FORM_DETAILS_tmp , FORM_DETAILS , err_macro=SYSDBRC);
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
%if %sysfunc(exist(&udmmart..IDENTITY_ADDRESSABLE_DEVICES)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid=%sysfunc(open(&udmmart..IDENTITY_ADDRESSABLE_DEVICES));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if &nrows = 0 %then %do;
      %put NOTE: IDENTITY_ADDRESSABLE_DEVICES has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..IDENTITY_ADDRESSABLE_DEVICES;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..IDENTITY_ADDRESSABLE_DEVICES_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table IDENTITY_ADDRESSABLE_DEVICES_tmp , IDENTITY_ADDRESSABLE_DEVICES_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=IDENTITY_ADDRESSABLE_DEVICES , table_keys=%str(DEVICE_ID,ENTRYTIME,IDENTITY_ID), out_table=work.IDENTITY_ADDRESSABLE_DEVICES );
      DATA work.IDENTITY_ADDRESSABLE_DEVICES_tmp ;
         SET work.IDENTITY_ADDRESSABLE_DEVICES ;
         WHERE 1=1 AND DEVICE_ID IS NOT NULL AND ENTRYTIME IS NOT NULL AND IDENTITY_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : IDENTITY_ADDRESSABLE_DEVICES_tmp , IDENTITY_ADDRESSABLE_DEVICES_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..IDENTITY_ADDRESSABLE_DEVICES_tmp as select * from &dbschema..IDENTITY_ADDRESSABLE_DEVICES where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : IDENTITY_ADDRESSABLE_DEVICES_tmp , IDENTITY_ADDRESSABLE_DEVICES , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.IDENTITY_ADDRESSABLE_DEVICES_tmp  base=&tmplib..IDENTITY_ADDRESSABLE_DEVICES_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : IDENTITY_ADDRESSABLE_DEVICES_tmp , IDENTITY_ADDRESSABLE_DEVICES );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..IDENTITY_ADDRESSABLE_DEVICES AS b USING &tmpdbschema..IDENTITY_ADDRESSABLE_DEVICES_tmp AS d ON (
            b.entrytime = d.entrytime AND 
            b.device_id = d.device_id AND b.identity_id = d.identity_id )
         WHEN MATCHED THEN
         UPDATE SET
            reachable_flg = d.reachable_flg, 
            mobile_app_id = d.mobile_app_id
         WHEN NOT MATCHED THEN INSERT (
            reachable_flg, entrytime, mobile_app_id, 
            device_id, identity_id
         ) VALUES (
            d.reachable_flg, d.entrytime, d.mobile_app_id, 
            d.device_id, d.identity_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : IDENTITY_ADDRESSABLE_DEVICES_tmp , IDENTITY_ADDRESSABLE_DEVICES , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..IDENTITY_ADDRESSABLE_DEVICES_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..IDENTITY_ADDRESSABLE_DEVICES_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..IDENTITY_ADDRESSABLE_DEVICES;
         DROP TABLE work.IDENTITY_ADDRESSABLE_DEVICES;
      QUIT;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table IDENTITY_ADDRESSABLE_DEVICES;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..IDENTITY_ATTRIBUTES)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid=%sysfunc(open(&udmmart..IDENTITY_ATTRIBUTES));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if &nrows = 0 %then %do;
      %put NOTE: IDENTITY_ATTRIBUTES has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..IDENTITY_ATTRIBUTES;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..IDENTITY_ATTRIBUTES_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table IDENTITY_ATTRIBUTES_tmp , IDENTITY_ATTRIBUTES_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=IDENTITY_ATTRIBUTES , table_keys=%str(ENTRYTIME,IDENTIFIER_TYPE_ID,USER_IDENTIFIER_VAL), out_table=work.IDENTITY_ATTRIBUTES );
      DATA work.IDENTITY_ATTRIBUTES_tmp ;
         SET work.IDENTITY_ATTRIBUTES ;
         WHERE 1=1 AND ENTRYTIME IS NOT NULL AND IDENTIFIER_TYPE_ID IS NOT NULL AND USER_IDENTIFIER_VAL IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : IDENTITY_ATTRIBUTES_tmp , IDENTITY_ATTRIBUTES_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..IDENTITY_ATTRIBUTES_tmp as select * from &dbschema..IDENTITY_ATTRIBUTES where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : IDENTITY_ATTRIBUTES_tmp , IDENTITY_ATTRIBUTES , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.IDENTITY_ATTRIBUTES_tmp  base=&tmplib..IDENTITY_ATTRIBUTES_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : IDENTITY_ATTRIBUTES_tmp , IDENTITY_ATTRIBUTES );
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
         WHEN NOT MATCHED THEN INSERT (
            entrytime, processed_dttm, user_identifier_val, 
            identity_id, identifier_type_id
         ) VALUES (
            d.entrytime, d.processed_dttm, d.user_identifier_val, 
            d.identity_id, d.identifier_type_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : IDENTITY_ATTRIBUTES_tmp , IDENTITY_ATTRIBUTES , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: IDENTITY_MAP has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..IDENTITY_MAP;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..IDENTITY_MAP_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table IDENTITY_MAP_tmp , IDENTITY_MAP_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=IDENTITY_MAP , table_keys=%str(SOURCE_IDENTITY_ID), out_table=work.IDENTITY_MAP );
      DATA work.IDENTITY_MAP_tmp ;
         SET work.IDENTITY_MAP ;
         WHERE 1=1 AND SOURCE_IDENTITY_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : IDENTITY_MAP_tmp , IDENTITY_MAP_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..IDENTITY_MAP_tmp as select * from &dbschema..IDENTITY_MAP where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : IDENTITY_MAP_tmp , IDENTITY_MAP , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.IDENTITY_MAP_tmp  base=&tmplib..IDENTITY_MAP_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : IDENTITY_MAP_tmp , IDENTITY_MAP );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..IDENTITY_MAP AS b USING &tmpdbschema..IDENTITY_MAP_tmp AS d ON (
            b.source_identity_id = d.source_identity_id )
         WHEN MATCHED THEN
         UPDATE SET
            entrytime = d.entrytime, 
            processed_dttm = d.processed_dttm, target_identity_id = d.target_identity_id
         WHEN NOT MATCHED THEN INSERT (
            entrytime, processed_dttm, target_identity_id, 
            source_identity_id
         ) VALUES (
            d.entrytime, d.processed_dttm, d.target_identity_id, 
            d.source_identity_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : IDENTITY_MAP_tmp , IDENTITY_MAP , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: IMPRESSION_DELIVERED has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..IMPRESSION_DELIVERED;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..IMPRESSION_DELIVERED_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table IMPRESSION_DELIVERED_tmp , IMPRESSION_DELIVERED_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=IMPRESSION_DELIVERED , table_keys=%str(EVENT_ID), out_table=work.IMPRESSION_DELIVERED );
      DATA work.IMPRESSION_DELIVERED_tmp ;
         SET work.IMPRESSION_DELIVERED ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : IMPRESSION_DELIVERED_tmp , IMPRESSION_DELIVERED_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..IMPRESSION_DELIVERED_tmp as select * from &dbschema..IMPRESSION_DELIVERED where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : IMPRESSION_DELIVERED_tmp , IMPRESSION_DELIVERED , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.IMPRESSION_DELIVERED_tmp  base=&tmplib..IMPRESSION_DELIVERED_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : IMPRESSION_DELIVERED_tmp , IMPRESSION_DELIVERED );
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
            load_dttm = d.load_dttm, task_version_id = d.task_version_id, 
            task_id = d.task_id, spot_id = d.spot_id, 
            segment_version_id = d.segment_version_id, response_tracking_cd = d.response_tracking_cd, 
            reserved_1_txt = d.reserved_1_txt, rec_group_id = d.rec_group_id, 
            product_sku_no = d.product_sku_no, product_nm = d.product_nm, 
            mobile_app_id = d.mobile_app_id, message_version_id = d.message_version_id, 
            message_id = d.message_id, journey_occurrence_id = d.journey_occurrence_id, 
            journey_id = d.journey_id, identity_id = d.identity_id, 
            event_nm = d.event_nm, event_designed_id = d.event_designed_id, 
            detail_id_hex = d.detail_id_hex, creative_id = d.creative_id, 
            context_val = d.context_val, channel_user_id = d.channel_user_id, 
            audience_id = d.audience_id, aud_occurrence_id = d.aud_occurrence_id, 
            channel_nm = d.channel_nm, context_type_nm = d.context_type_nm, 
            creative_version_id = d.creative_version_id, event_key_cd = d.event_key_cd, 
            event_source_cd = d.event_source_cd, product_id = d.product_id, 
            request_id = d.request_id, reserved_2_txt = d.reserved_2_txt, 
            segment_id = d.segment_id, session_id_hex = d.session_id_hex, 
            visit_id_hex = d.visit_id_hex
         WHEN NOT MATCHED THEN INSERT (
            control_group_flg, product_qty_no, properties_map_doc, 
            impression_delivered_dttm_tz, impression_delivered_dttm, load_dttm, task_version_id, 
            task_id, spot_id, segment_version_id, response_tracking_cd, 
            reserved_1_txt, rec_group_id, product_sku_no, product_nm, 
            mobile_app_id, message_version_id, message_id, journey_occurrence_id, 
            journey_id, identity_id, event_nm, event_id, 
            event_designed_id, detail_id_hex, creative_id, context_val, 
            channel_user_id, audience_id, aud_occurrence_id, channel_nm, 
            context_type_nm, creative_version_id, event_key_cd, event_source_cd, 
            product_id, request_id, reserved_2_txt, segment_id, 
            session_id_hex, visit_id_hex
         ) VALUES (
            d.control_group_flg, d.product_qty_no, d.properties_map_doc, 
            d.impression_delivered_dttm_tz, d.impression_delivered_dttm, d.load_dttm, d.task_version_id, 
            d.task_id, d.spot_id, d.segment_version_id, d.response_tracking_cd, 
            d.reserved_1_txt, d.rec_group_id, d.product_sku_no, d.product_nm, 
            d.mobile_app_id, d.message_version_id, d.message_id, d.journey_occurrence_id, 
            d.journey_id, d.identity_id, d.event_nm, d.event_id, 
            d.event_designed_id, d.detail_id_hex, d.creative_id, d.context_val, 
            d.channel_user_id, d.audience_id, d.aud_occurrence_id, d.channel_nm, 
            d.context_type_nm, d.creative_version_id, d.event_key_cd, d.event_source_cd, 
            d.product_id, d.request_id, d.reserved_2_txt, d.segment_id, 
            d.session_id_hex, d.visit_id_hex  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : IMPRESSION_DELIVERED_tmp , IMPRESSION_DELIVERED , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: IMPRESSION_SPOT_VIEWABLE has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..IMPRESSION_SPOT_VIEWABLE;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..IMPRESSION_SPOT_VIEWABLE_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table IMPRESSION_SPOT_VIEWABLE_tmp , IMPRESSION_SPOT_VIEWABLE_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=IMPRESSION_SPOT_VIEWABLE , table_keys=%str(EVENT_ID), out_table=work.IMPRESSION_SPOT_VIEWABLE );
      DATA work.IMPRESSION_SPOT_VIEWABLE_tmp ;
         SET work.IMPRESSION_SPOT_VIEWABLE ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : IMPRESSION_SPOT_VIEWABLE_tmp , IMPRESSION_SPOT_VIEWABLE_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..IMPRESSION_SPOT_VIEWABLE_tmp as select * from &dbschema..IMPRESSION_SPOT_VIEWABLE where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : IMPRESSION_SPOT_VIEWABLE_tmp , IMPRESSION_SPOT_VIEWABLE , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.IMPRESSION_SPOT_VIEWABLE_tmp  base=&tmplib..IMPRESSION_SPOT_VIEWABLE_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : IMPRESSION_SPOT_VIEWABLE_tmp , IMPRESSION_SPOT_VIEWABLE );
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
            impression_viewable_dttm_tz = d.impression_viewable_dttm_tz, impression_viewable_dttm = d.impression_viewable_dttm, 
            load_dttm = d.load_dttm, visit_id_hex = d.visit_id_hex, 
            task_version_id = d.task_version_id, task_id = d.task_id, 
            session_id_hex = d.session_id_hex, segment_id = d.segment_id, 
            reserved_2_txt = d.reserved_2_txt, request_id = d.request_id, 
            rec_group_id = d.rec_group_id, product_id = d.product_id, 
            mobile_app_id = d.mobile_app_id, message_id = d.message_id, 
            journey_occurrence_id = d.journey_occurrence_id, identity_id = d.identity_id, 
            event_nm = d.event_nm, detail_id_hex = d.detail_id_hex, 
            creative_id = d.creative_id, context_val = d.context_val, 
            channel_user_id = d.channel_user_id, audience_id = d.audience_id, 
            analysis_group_id = d.analysis_group_id, aud_occurrence_id = d.aud_occurrence_id, 
            channel_nm = d.channel_nm, context_type_nm = d.context_type_nm, 
            creative_version_id = d.creative_version_id, event_designed_id = d.event_designed_id, 
            event_key_cd = d.event_key_cd, event_source_cd = d.event_source_cd, 
            journey_id = d.journey_id, message_version_id = d.message_version_id, 
            occurrence_id = d.occurrence_id, product_nm = d.product_nm, 
            product_sku_no = d.product_sku_no, reserved_1_txt = d.reserved_1_txt, 
            response_tracking_cd = d.response_tracking_cd, segment_version_id = d.segment_version_id, 
            spot_id = d.spot_id
         WHEN NOT MATCHED THEN INSERT (
            control_group_flg, product_qty_no, properties_map_doc, 
            impression_viewable_dttm_tz, impression_viewable_dttm, load_dttm, visit_id_hex, 
            task_version_id, task_id, session_id_hex, segment_id, 
            reserved_2_txt, request_id, rec_group_id, product_id, 
            mobile_app_id, message_id, journey_occurrence_id, identity_id, 
            event_nm, event_id, detail_id_hex, creative_id, 
            context_val, channel_user_id, audience_id, analysis_group_id, 
            aud_occurrence_id, channel_nm, context_type_nm, creative_version_id, 
            event_designed_id, event_key_cd, event_source_cd, journey_id, 
            message_version_id, occurrence_id, product_nm, product_sku_no, 
            reserved_1_txt, response_tracking_cd, segment_version_id, spot_id
         ) VALUES (
            d.control_group_flg, d.product_qty_no, d.properties_map_doc, 
            d.impression_viewable_dttm_tz, d.impression_viewable_dttm, d.load_dttm, d.visit_id_hex, 
            d.task_version_id, d.task_id, d.session_id_hex, d.segment_id, 
            d.reserved_2_txt, d.request_id, d.rec_group_id, d.product_id, 
            d.mobile_app_id, d.message_id, d.journey_occurrence_id, d.identity_id, 
            d.event_nm, d.event_id, d.detail_id_hex, d.creative_id, 
            d.context_val, d.channel_user_id, d.audience_id, d.analysis_group_id, 
            d.aud_occurrence_id, d.channel_nm, d.context_type_nm, d.creative_version_id, 
            d.event_designed_id, d.event_key_cd, d.event_source_cd, d.journey_id, 
            d.message_version_id, d.occurrence_id, d.product_nm, d.product_sku_no, 
            d.reserved_1_txt, d.response_tracking_cd, d.segment_version_id, d.spot_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : IMPRESSION_SPOT_VIEWABLE_tmp , IMPRESSION_SPOT_VIEWABLE , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: INVOICE_DETAILS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..INVOICE_DETAILS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..INVOICE_DETAILS) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate INVOICE_DETAILS , INVOICE_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: INVOICE_LINE_ITEMS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..INVOICE_LINE_ITEMS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..INVOICE_LINE_ITEMS) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate INVOICE_LINE_ITEMS , INVOICE_LINE_ITEMS );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: INVOICE_LINE_ITEMS_CCBDGT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..INVOICE_LINE_ITEMS_CCBDGT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..INVOICE_LINE_ITEMS_CCBDGT) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate INVOICE_LINE_ITEMS_CCBDGT , INVOICE_LINE_ITEMS_CCBDGT );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: IN_APP_FAILED has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..IN_APP_FAILED;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..IN_APP_FAILED_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table IN_APP_FAILED_tmp , IN_APP_FAILED_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=IN_APP_FAILED , table_keys=%str(EVENT_ID), out_table=work.IN_APP_FAILED );
      DATA work.IN_APP_FAILED_tmp ;
         SET work.IN_APP_FAILED ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : IN_APP_FAILED_tmp , IN_APP_FAILED_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..IN_APP_FAILED_tmp as select * from &dbschema..IN_APP_FAILED where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : IN_APP_FAILED_tmp , IN_APP_FAILED , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.IN_APP_FAILED_tmp  base=&tmplib..IN_APP_FAILED_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : IN_APP_FAILED_tmp , IN_APP_FAILED );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..IN_APP_FAILED AS b USING &tmpdbschema..IN_APP_FAILED_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            properties_map_doc = d.properties_map_doc, 
            load_dttm = d.load_dttm, in_app_failed_dttm_tz = d.in_app_failed_dttm_tz, 
            in_app_failed_dttm = d.in_app_failed_dttm, task_version_id = d.task_version_id, 
            spot_id = d.spot_id, segment_version_id = d.segment_version_id, 
            segment_id = d.segment_id, reserved_2_txt = d.reserved_2_txt, 
            occurrence_id = d.occurrence_id, mobile_app_id = d.mobile_app_id, 
            message_id = d.message_id, identity_id = d.identity_id, 
            error_message_txt = d.error_message_txt, error_cd = d.error_cd, 
            creative_version_id = d.creative_version_id, context_val = d.context_val, 
            channel_user_id = d.channel_user_id, channel_nm = d.channel_nm, 
            context_type_nm = d.context_type_nm, creative_id = d.creative_id, 
            event_designed_id = d.event_designed_id, event_nm = d.event_nm, 
            message_version_id = d.message_version_id, reserved_1_txt = d.reserved_1_txt, 
            response_tracking_cd = d.response_tracking_cd, task_id = d.task_id
         WHEN NOT MATCHED THEN INSERT (
            properties_map_doc, load_dttm, in_app_failed_dttm_tz, 
            in_app_failed_dttm, task_version_id, spot_id, segment_version_id, 
            segment_id, reserved_2_txt, occurrence_id, mobile_app_id, 
            message_id, identity_id, event_id, error_message_txt, 
            error_cd, creative_version_id, context_val, channel_user_id, 
            channel_nm, context_type_nm, creative_id, event_designed_id, 
            event_nm, message_version_id, reserved_1_txt, response_tracking_cd, 
            task_id
         ) VALUES (
            d.properties_map_doc, d.load_dttm, d.in_app_failed_dttm_tz, 
            d.in_app_failed_dttm, d.task_version_id, d.spot_id, d.segment_version_id, 
            d.segment_id, d.reserved_2_txt, d.occurrence_id, d.mobile_app_id, 
            d.message_id, d.identity_id, d.event_id, d.error_message_txt, 
            d.error_cd, d.creative_version_id, d.context_val, d.channel_user_id, 
            d.channel_nm, d.context_type_nm, d.creative_id, d.event_designed_id, 
            d.event_nm, d.message_version_id, d.reserved_1_txt, d.response_tracking_cd, 
            d.task_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : IN_APP_FAILED_tmp , IN_APP_FAILED , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: IN_APP_MESSAGE has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..IN_APP_MESSAGE;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..IN_APP_MESSAGE_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table IN_APP_MESSAGE_tmp , IN_APP_MESSAGE_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=IN_APP_MESSAGE , table_keys=%str(EVENT_ID), out_table=work.IN_APP_MESSAGE );
      DATA work.IN_APP_MESSAGE_tmp ;
         SET work.IN_APP_MESSAGE ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : IN_APP_MESSAGE_tmp , IN_APP_MESSAGE_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..IN_APP_MESSAGE_tmp as select * from &dbschema..IN_APP_MESSAGE where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : IN_APP_MESSAGE_tmp , IN_APP_MESSAGE , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.IN_APP_MESSAGE_tmp  base=&tmplib..IN_APP_MESSAGE_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : IN_APP_MESSAGE_tmp , IN_APP_MESSAGE );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..IN_APP_MESSAGE AS b USING &tmpdbschema..IN_APP_MESSAGE_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            properties_map_doc = d.properties_map_doc, 
            in_app_action_dttm = d.in_app_action_dttm, in_app_action_dttm_tz = d.in_app_action_dttm_tz, 
            load_dttm = d.load_dttm, task_id = d.task_id, 
            segment_version_id = d.segment_version_id, response_tracking_cd = d.response_tracking_cd, 
            reserved_2_txt = d.reserved_2_txt, reserved_1_txt = d.reserved_1_txt, 
            occurrence_id = d.occurrence_id, mobile_app_id = d.mobile_app_id, 
            message_version_id = d.message_version_id, message_id = d.message_id, 
            identity_id = d.identity_id, creative_version_id = d.creative_version_id, 
            context_val = d.context_val, channel_user_id = d.channel_user_id, 
            channel_nm = d.channel_nm, context_type_nm = d.context_type_nm, 
            creative_id = d.creative_id, event_designed_id = d.event_designed_id, 
            event_nm = d.event_nm, reserved_3_txt = d.reserved_3_txt, 
            segment_id = d.segment_id, spot_id = d.spot_id, 
            task_version_id = d.task_version_id
         WHEN NOT MATCHED THEN INSERT (
            properties_map_doc, in_app_action_dttm, in_app_action_dttm_tz, 
            load_dttm, task_id, segment_version_id, response_tracking_cd, 
            reserved_2_txt, reserved_1_txt, occurrence_id, mobile_app_id, 
            message_version_id, message_id, identity_id, event_id, 
            creative_version_id, context_val, channel_user_id, channel_nm, 
            context_type_nm, creative_id, event_designed_id, event_nm, 
            reserved_3_txt, segment_id, spot_id, task_version_id
         ) VALUES (
            d.properties_map_doc, d.in_app_action_dttm, d.in_app_action_dttm_tz, 
            d.load_dttm, d.task_id, d.segment_version_id, d.response_tracking_cd, 
            d.reserved_2_txt, d.reserved_1_txt, d.occurrence_id, d.mobile_app_id, 
            d.message_version_id, d.message_id, d.identity_id, d.event_id, 
            d.creative_version_id, d.context_val, d.channel_user_id, d.channel_nm, 
            d.context_type_nm, d.creative_id, d.event_designed_id, d.event_nm, 
            d.reserved_3_txt, d.segment_id, d.spot_id, d.task_version_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : IN_APP_MESSAGE_tmp , IN_APP_MESSAGE , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: IN_APP_SEND has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..IN_APP_SEND;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..IN_APP_SEND_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table IN_APP_SEND_tmp , IN_APP_SEND_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=IN_APP_SEND , table_keys=%str(EVENT_ID), out_table=work.IN_APP_SEND );
      DATA work.IN_APP_SEND_tmp ;
         SET work.IN_APP_SEND ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : IN_APP_SEND_tmp , IN_APP_SEND_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..IN_APP_SEND_tmp as select * from &dbschema..IN_APP_SEND where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : IN_APP_SEND_tmp , IN_APP_SEND , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.IN_APP_SEND_tmp  base=&tmplib..IN_APP_SEND_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : IN_APP_SEND_tmp , IN_APP_SEND );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..IN_APP_SEND AS b USING &tmpdbschema..IN_APP_SEND_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            properties_map_doc = d.properties_map_doc, 
            in_app_send_dttm_tz = d.in_app_send_dttm_tz, load_dttm = d.load_dttm, 
            in_app_send_dttm = d.in_app_send_dttm, task_id = d.task_id, 
            segment_version_id = d.segment_version_id, response_tracking_cd = d.response_tracking_cd, 
            reserved_2_txt = d.reserved_2_txt, reserved_1_txt = d.reserved_1_txt, 
            occurrence_id = d.occurrence_id, mobile_app_id = d.mobile_app_id, 
            message_version_id = d.message_version_id, message_id = d.message_id, 
            identity_id = d.identity_id, event_nm = d.event_nm, 
            event_designed_id = d.event_designed_id, creative_version_id = d.creative_version_id, 
            creative_id = d.creative_id, context_type_nm = d.context_type_nm, 
            channel_nm = d.channel_nm, channel_user_id = d.channel_user_id, 
            context_val = d.context_val, segment_id = d.segment_id, 
            spot_id = d.spot_id, task_version_id = d.task_version_id
         WHEN NOT MATCHED THEN INSERT (
            properties_map_doc, in_app_send_dttm_tz, load_dttm, 
            in_app_send_dttm, task_id, segment_version_id, response_tracking_cd, 
            reserved_2_txt, reserved_1_txt, occurrence_id, mobile_app_id, 
            message_version_id, message_id, identity_id, event_nm, 
            event_id, event_designed_id, creative_version_id, creative_id, 
            context_type_nm, channel_nm, channel_user_id, context_val, 
            segment_id, spot_id, task_version_id
         ) VALUES (
            d.properties_map_doc, d.in_app_send_dttm_tz, d.load_dttm, 
            d.in_app_send_dttm, d.task_id, d.segment_version_id, d.response_tracking_cd, 
            d.reserved_2_txt, d.reserved_1_txt, d.occurrence_id, d.mobile_app_id, 
            d.message_version_id, d.message_id, d.identity_id, d.event_nm, 
            d.event_id, d.event_designed_id, d.creative_version_id, d.creative_id, 
            d.context_type_nm, d.channel_nm, d.channel_user_id, d.context_val, 
            d.segment_id, d.spot_id, d.task_version_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : IN_APP_SEND_tmp , IN_APP_SEND , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: IN_APP_TARGETING_REQUEST has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..IN_APP_TARGETING_REQUEST;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..IN_APP_TARGETING_REQUEST_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table IN_APP_TARGETING_REQUEST_tmp , IN_APP_TARGETING_REQUEST_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=IN_APP_TARGETING_REQUEST , table_keys=%str(EVENT_ID), out_table=work.IN_APP_TARGETING_REQUEST );
      DATA work.IN_APP_TARGETING_REQUEST_tmp ;
         SET work.IN_APP_TARGETING_REQUEST ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : IN_APP_TARGETING_REQUEST_tmp , IN_APP_TARGETING_REQUEST_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..IN_APP_TARGETING_REQUEST_tmp as select * from &dbschema..IN_APP_TARGETING_REQUEST where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : IN_APP_TARGETING_REQUEST_tmp , IN_APP_TARGETING_REQUEST , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.IN_APP_TARGETING_REQUEST_tmp  base=&tmplib..IN_APP_TARGETING_REQUEST_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : IN_APP_TARGETING_REQUEST_tmp , IN_APP_TARGETING_REQUEST );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..IN_APP_TARGETING_REQUEST AS b USING &tmpdbschema..IN_APP_TARGETING_REQUEST_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            eligibility_flg = d.eligibility_flg, 
            in_app_tgt_request_dttm_tz = d.in_app_tgt_request_dttm_tz, in_app_tgt_request_dttm = d.in_app_tgt_request_dttm, 
            load_dttm = d.load_dttm, mobile_app_id = d.mobile_app_id, 
            identity_id = d.identity_id, event_designed_id = d.event_designed_id, 
            context_type_nm = d.context_type_nm, channel_nm = d.channel_nm, 
            channel_user_id = d.channel_user_id, context_val = d.context_val, 
            event_nm = d.event_nm
         WHEN NOT MATCHED THEN INSERT (
            eligibility_flg, in_app_tgt_request_dttm_tz, in_app_tgt_request_dttm, 
            load_dttm, mobile_app_id, identity_id, event_id, 
            event_designed_id, context_type_nm, channel_nm, channel_user_id, 
            context_val, event_nm
         ) VALUES (
            d.eligibility_flg, d.in_app_tgt_request_dttm_tz, d.in_app_tgt_request_dttm, 
            d.load_dttm, d.mobile_app_id, d.identity_id, d.event_id, 
            d.event_designed_id, d.context_type_nm, d.channel_nm, d.channel_user_id, 
            d.context_val, d.event_nm  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : IN_APP_TARGETING_REQUEST_tmp , IN_APP_TARGETING_REQUEST , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: JOURNEY_ENTRY has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..JOURNEY_ENTRY;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..JOURNEY_ENTRY_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table JOURNEY_ENTRY_tmp , JOURNEY_ENTRY_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=JOURNEY_ENTRY , table_keys=%str(EVENT_ID), out_table=work.JOURNEY_ENTRY );
      DATA work.JOURNEY_ENTRY_tmp ;
         SET work.JOURNEY_ENTRY ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : JOURNEY_ENTRY_tmp , JOURNEY_ENTRY_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..JOURNEY_ENTRY_tmp as select * from &dbschema..JOURNEY_ENTRY where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : JOURNEY_ENTRY_tmp , JOURNEY_ENTRY , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.JOURNEY_ENTRY_tmp  base=&tmplib..JOURNEY_ENTRY_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : JOURNEY_ENTRY_tmp , JOURNEY_ENTRY );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..JOURNEY_ENTRY AS b USING &tmpdbschema..JOURNEY_ENTRY_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            load_dttm = d.load_dttm, 
            entry_dttm = d.entry_dttm, entry_dttm_tz = d.entry_dttm_tz, 
            identity_type_val = d.identity_type_val, context_type_nm = d.context_type_nm, 
            aud_occurrence_id = d.aud_occurrence_id, context_val = d.context_val, 
            identity_id = d.identity_id, identity_type_nm = d.identity_type_nm, 
            journey_occurrence_id = d.journey_occurrence_id, audience_id = d.audience_id, 
            event_nm = d.event_nm, journey_id = d.journey_id
         WHEN NOT MATCHED THEN INSERT (
            load_dttm, entry_dttm, entry_dttm_tz, 
            identity_type_val, event_id, context_type_nm, aud_occurrence_id, 
            context_val, identity_id, identity_type_nm, journey_occurrence_id, 
            audience_id, event_nm, journey_id
         ) VALUES (
            d.load_dttm, d.entry_dttm, d.entry_dttm_tz, 
            d.identity_type_val, d.event_id, d.context_type_nm, d.aud_occurrence_id, 
            d.context_val, d.identity_id, d.identity_type_nm, d.journey_occurrence_id, 
            d.audience_id, d.event_nm, d.journey_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : JOURNEY_ENTRY_tmp , JOURNEY_ENTRY , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: JOURNEY_EXIT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..JOURNEY_EXIT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..JOURNEY_EXIT_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table JOURNEY_EXIT_tmp , JOURNEY_EXIT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=JOURNEY_EXIT , table_keys=%str(EVENT_ID), out_table=work.JOURNEY_EXIT );
      DATA work.JOURNEY_EXIT_tmp ;
         SET work.JOURNEY_EXIT ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : JOURNEY_EXIT_tmp , JOURNEY_EXIT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..JOURNEY_EXIT_tmp as select * from &dbschema..JOURNEY_EXIT where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : JOURNEY_EXIT_tmp , JOURNEY_EXIT , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.JOURNEY_EXIT_tmp  base=&tmplib..JOURNEY_EXIT_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : JOURNEY_EXIT_tmp , JOURNEY_EXIT );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..JOURNEY_EXIT AS b USING &tmpdbschema..JOURNEY_EXIT_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            exit_dttm_tz = d.exit_dttm_tz, 
            load_dttm = d.load_dttm, exit_dttm = d.exit_dttm, 
            last_node_id = d.last_node_id, identity_type_nm = d.identity_type_nm, 
            drop_aud_occurrence_id = d.drop_aud_occurrence_id, aud_occurrence_id = d.aud_occurrence_id, 
            context_type_nm = d.context_type_nm, drop_audience_id = d.drop_audience_id, 
            event_nm = d.event_nm, group_id = d.group_id, 
            journey_id = d.journey_id, reason_cd = d.reason_cd, 
            audience_id = d.audience_id, context_val = d.context_val, 
            identity_id = d.identity_id, identity_type_val = d.identity_type_val, 
            journey_occurrence_id = d.journey_occurrence_id, reason_txt = d.reason_txt
         WHEN NOT MATCHED THEN INSERT (
            exit_dttm_tz, load_dttm, exit_dttm, 
            last_node_id, identity_type_nm, drop_aud_occurrence_id, aud_occurrence_id, 
            context_type_nm, drop_audience_id, event_id, event_nm, 
            group_id, journey_id, reason_cd, audience_id, 
            context_val, identity_id, identity_type_val, journey_occurrence_id, 
            reason_txt
         ) VALUES (
            d.exit_dttm_tz, d.load_dttm, d.exit_dttm, 
            d.last_node_id, d.identity_type_nm, d.drop_aud_occurrence_id, d.aud_occurrence_id, 
            d.context_type_nm, d.drop_audience_id, d.event_id, d.event_nm, 
            d.group_id, d.journey_id, d.reason_cd, d.audience_id, 
            d.context_val, d.identity_id, d.identity_type_val, d.journey_occurrence_id, 
            d.reason_txt  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : JOURNEY_EXIT_tmp , JOURNEY_EXIT , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: JOURNEY_HOLDOUT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..JOURNEY_HOLDOUT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..JOURNEY_HOLDOUT_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table JOURNEY_HOLDOUT_tmp , JOURNEY_HOLDOUT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=JOURNEY_HOLDOUT , table_keys=%str(EVENT_ID), out_table=work.JOURNEY_HOLDOUT );
      DATA work.JOURNEY_HOLDOUT_tmp ;
         SET work.JOURNEY_HOLDOUT ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : JOURNEY_HOLDOUT_tmp , JOURNEY_HOLDOUT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..JOURNEY_HOLDOUT_tmp as select * from &dbschema..JOURNEY_HOLDOUT where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : JOURNEY_HOLDOUT_tmp , JOURNEY_HOLDOUT , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.JOURNEY_HOLDOUT_tmp  base=&tmplib..JOURNEY_HOLDOUT_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : JOURNEY_HOLDOUT_tmp , JOURNEY_HOLDOUT );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..JOURNEY_HOLDOUT AS b USING &tmpdbschema..JOURNEY_HOLDOUT_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            holdout_dttm_tz = d.holdout_dttm_tz, 
            holdout_dttm = d.holdout_dttm, load_dttm = d.load_dttm, 
            journey_occurrence_id = d.journey_occurrence_id, identity_id = d.identity_id, 
            aud_occurrence_id = d.aud_occurrence_id, context_type_nm = d.context_type_nm, 
            identity_type_val = d.identity_type_val, audience_id = d.audience_id, 
            context_val = d.context_val, event_nm = d.event_nm, 
            identity_type_nm = d.identity_type_nm, journey_id = d.journey_id
         WHEN NOT MATCHED THEN INSERT (
            holdout_dttm_tz, holdout_dttm, load_dttm, 
            journey_occurrence_id, identity_id, event_id, aud_occurrence_id, 
            context_type_nm, identity_type_val, audience_id, context_val, 
            event_nm, identity_type_nm, journey_id
         ) VALUES (
            d.holdout_dttm_tz, d.holdout_dttm, d.load_dttm, 
            d.journey_occurrence_id, d.identity_id, d.event_id, d.aud_occurrence_id, 
            d.context_type_nm, d.identity_type_val, d.audience_id, d.context_val, 
            d.event_nm, d.identity_type_nm, d.journey_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : JOURNEY_HOLDOUT_tmp , JOURNEY_HOLDOUT , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: JOURNEY_NODE_ENTRY has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..JOURNEY_NODE_ENTRY;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..JOURNEY_NODE_ENTRY_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table JOURNEY_NODE_ENTRY_tmp , JOURNEY_NODE_ENTRY_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=JOURNEY_NODE_ENTRY , table_keys=%str(EVENT_ID), out_table=work.JOURNEY_NODE_ENTRY );
      DATA work.JOURNEY_NODE_ENTRY_tmp ;
         SET work.JOURNEY_NODE_ENTRY ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : JOURNEY_NODE_ENTRY_tmp , JOURNEY_NODE_ENTRY_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..JOURNEY_NODE_ENTRY_tmp as select * from &dbschema..JOURNEY_NODE_ENTRY where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : JOURNEY_NODE_ENTRY_tmp , JOURNEY_NODE_ENTRY , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.JOURNEY_NODE_ENTRY_tmp  base=&tmplib..JOURNEY_NODE_ENTRY_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : JOURNEY_NODE_ENTRY_tmp , JOURNEY_NODE_ENTRY );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..JOURNEY_NODE_ENTRY AS b USING &tmpdbschema..JOURNEY_NODE_ENTRY_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            node_entry_dttm = d.node_entry_dttm, 
            node_entry_dttm_tz = d.node_entry_dttm_tz, load_dttm = d.load_dttm, 
            journey_id = d.journey_id, group_id = d.group_id, 
            context_type_nm = d.context_type_nm, aud_occurrence_id = d.aud_occurrence_id, 
            context_val = d.context_val, identity_type_nm = d.identity_type_nm, 
            node_type_nm = d.node_type_nm, audience_id = d.audience_id, 
            event_nm = d.event_nm, identity_id = d.identity_id, 
            identity_type_val = d.identity_type_val, journey_occurrence_id = d.journey_occurrence_id, 
            node_id = d.node_id, previous_node_id = d.previous_node_id
         WHEN NOT MATCHED THEN INSERT (
            node_entry_dttm, node_entry_dttm_tz, load_dttm, 
            journey_id, group_id, context_type_nm, aud_occurrence_id, 
            context_val, event_id, identity_type_nm, node_type_nm, 
            audience_id, event_nm, identity_id, identity_type_val, 
            journey_occurrence_id, node_id, previous_node_id
         ) VALUES (
            d.node_entry_dttm, d.node_entry_dttm_tz, d.load_dttm, 
            d.journey_id, d.group_id, d.context_type_nm, d.aud_occurrence_id, 
            d.context_val, d.event_id, d.identity_type_nm, d.node_type_nm, 
            d.audience_id, d.event_nm, d.identity_id, d.identity_type_val, 
            d.journey_occurrence_id, d.node_id, d.previous_node_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : JOURNEY_NODE_ENTRY_tmp , JOURNEY_NODE_ENTRY , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: JOURNEY_SUCCESS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..JOURNEY_SUCCESS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..JOURNEY_SUCCESS_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table JOURNEY_SUCCESS_tmp , JOURNEY_SUCCESS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=JOURNEY_SUCCESS , table_keys=%str(EVENT_ID), out_table=work.JOURNEY_SUCCESS );
      DATA work.JOURNEY_SUCCESS_tmp ;
         SET work.JOURNEY_SUCCESS ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : JOURNEY_SUCCESS_tmp , JOURNEY_SUCCESS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..JOURNEY_SUCCESS_tmp as select * from &dbschema..JOURNEY_SUCCESS where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : JOURNEY_SUCCESS_tmp , JOURNEY_SUCCESS , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.JOURNEY_SUCCESS_tmp  base=&tmplib..JOURNEY_SUCCESS_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : JOURNEY_SUCCESS_tmp , JOURNEY_SUCCESS );
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
            identity_type_nm = d.identity_type_nm, aud_occurrence_id = d.aud_occurrence_id, 
            context_type_nm = d.context_type_nm, group_id = d.group_id, 
            journey_id = d.journey_id, success_aud_occurrence_id = d.success_aud_occurrence_id, 
            audience_id = d.audience_id, context_val = d.context_val, 
            event_nm = d.event_nm, identity_id = d.identity_id, 
            identity_type_val = d.identity_type_val, journey_occurrence_id = d.journey_occurrence_id, 
            parent_event_designed_id = d.parent_event_designed_id, success_audience_id = d.success_audience_id
         WHEN NOT MATCHED THEN INSERT (
            unit_qty, success_val, success_dttm, 
            load_dttm, success_dttm_tz, identity_type_nm, event_id, 
            aud_occurrence_id, context_type_nm, group_id, journey_id, 
            success_aud_occurrence_id, audience_id, context_val, event_nm, 
            identity_id, identity_type_val, journey_occurrence_id, parent_event_designed_id, 
            success_audience_id
         ) VALUES (
            d.unit_qty, d.success_val, d.success_dttm, 
            d.load_dttm, d.success_dttm_tz, d.identity_type_nm, d.event_id, 
            d.aud_occurrence_id, d.context_type_nm, d.group_id, d.journey_id, 
            d.success_aud_occurrence_id, d.audience_id, d.context_val, d.event_nm, 
            d.identity_id, d.identity_type_val, d.journey_occurrence_id, d.parent_event_designed_id, 
            d.success_audience_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : JOURNEY_SUCCESS_tmp , JOURNEY_SUCCESS , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: JOURNEY_SUPPRESSION has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..JOURNEY_SUPPRESSION;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..JOURNEY_SUPPRESSION_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table JOURNEY_SUPPRESSION_tmp , JOURNEY_SUPPRESSION_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=JOURNEY_SUPPRESSION , table_keys=%str(EVENT_ID), out_table=work.JOURNEY_SUPPRESSION );
      DATA work.JOURNEY_SUPPRESSION_tmp ;
         SET work.JOURNEY_SUPPRESSION ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : JOURNEY_SUPPRESSION_tmp , JOURNEY_SUPPRESSION_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..JOURNEY_SUPPRESSION_tmp as select * from &dbschema..JOURNEY_SUPPRESSION where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : JOURNEY_SUPPRESSION_tmp , JOURNEY_SUPPRESSION , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.JOURNEY_SUPPRESSION_tmp  base=&tmplib..JOURNEY_SUPPRESSION_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : JOURNEY_SUPPRESSION_tmp , JOURNEY_SUPPRESSION );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..JOURNEY_SUPPRESSION AS b USING &tmpdbschema..JOURNEY_SUPPRESSION_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            suppression_dttm_tz = d.suppression_dttm_tz, 
            load_dttm = d.load_dttm, suppression_dttm = d.suppression_dttm, 
            reason_txt = d.reason_txt, reason_cd = d.reason_cd, 
            identity_type_val = d.identity_type_val, aud_occurrence_id = d.aud_occurrence_id, 
            context_type_nm = d.context_type_nm, identity_id = d.identity_id, 
            journey_occurrence_id = d.journey_occurrence_id, audience_id = d.audience_id, 
            context_val = d.context_val, event_nm = d.event_nm, 
            identity_type_nm = d.identity_type_nm, journey_id = d.journey_id
         WHEN NOT MATCHED THEN INSERT (
            suppression_dttm_tz, load_dttm, suppression_dttm, 
            reason_txt, reason_cd, identity_type_val, event_id, 
            aud_occurrence_id, context_type_nm, identity_id, journey_occurrence_id, 
            audience_id, context_val, event_nm, identity_type_nm, 
            journey_id
         ) VALUES (
            d.suppression_dttm_tz, d.load_dttm, d.suppression_dttm, 
            d.reason_txt, d.reason_cd, d.identity_type_val, d.event_id, 
            d.aud_occurrence_id, d.context_type_nm, d.identity_id, d.journey_occurrence_id, 
            d.audience_id, d.context_val, d.event_nm, d.identity_type_nm, 
            d.journey_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : JOURNEY_SUPPRESSION_tmp , JOURNEY_SUPPRESSION , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: JOURNEY_TEST_SUCCESS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..JOURNEY_TEST_SUCCESS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..JOURNEY_TEST_SUCCESS_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table JOURNEY_TEST_SUCCESS_tmp , JOURNEY_TEST_SUCCESS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=JOURNEY_TEST_SUCCESS , table_keys=%str(EVENT_ID), out_table=work.JOURNEY_TEST_SUCCESS );
      DATA work.JOURNEY_TEST_SUCCESS_tmp ;
         SET work.JOURNEY_TEST_SUCCESS ;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : JOURNEY_TEST_SUCCESS_tmp , JOURNEY_TEST_SUCCESS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..JOURNEY_TEST_SUCCESS_tmp as select * from &dbschema..JOURNEY_TEST_SUCCESS where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : JOURNEY_TEST_SUCCESS_tmp , JOURNEY_TEST_SUCCESS , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.JOURNEY_TEST_SUCCESS_tmp  base=&tmplib..JOURNEY_TEST_SUCCESS_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : JOURNEY_TEST_SUCCESS_tmp , JOURNEY_TEST_SUCCESS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..JOURNEY_TEST_SUCCESS AS b USING &tmpdbschema..JOURNEY_TEST_SUCCESS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            success_dttm = d.success_dttm, 
            success_dttm_tz = d.success_dttm_tz, parent_event_designed_id = d.parent_event_designed_id, 
            identity_id = d.identity_id, group_id = d.group_id, 
            context_type_nm = d.context_type_nm, event_nm = d.event_nm, 
            journey_id = d.journey_id, success_audience_id = d.success_audience_id, 
            context_val = d.context_val, journey_occurrence_id = d.journey_occurrence_id, 
            success_aud_occurrence_id = d.success_aud_occurrence_id
         WHEN NOT MATCHED THEN INSERT (
            success_dttm, success_dttm_tz, parent_event_designed_id, 
            identity_id, group_id, context_type_nm, event_id, 
            event_nm, journey_id, success_audience_id, context_val, 
            journey_occurrence_id, success_aud_occurrence_id
         ) VALUES (
            d.success_dttm, d.success_dttm_tz, d.parent_event_designed_id, 
            d.identity_id, d.group_id, d.context_type_nm, d.event_id, 
            d.event_nm, d.journey_id, d.success_audience_id, d.context_val, 
            d.journey_occurrence_id, d.success_aud_occurrence_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : JOURNEY_TEST_SUCCESS_tmp , JOURNEY_TEST_SUCCESS , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_ACTIVITY has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_ACTIVITY;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_ACTIVITY) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_ACTIVITY , MD_ACTIVITY );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_ACTIVITY_ABTESTPATH has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_ACTIVITY_ABTESTPATH;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_ACTIVITY_ABTESTPATH) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_ACTIVITY_ABTESTPATH , MD_ACTIVITY_ABTESTPATH );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_ACTIVITY_ABTESTPATH_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_ACTIVITY_ABTESTPATH_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_ACTIVITY_ABTESTPATH_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_ACTIVITY_ABTESTPATH_ALL , MD_ACTIVITY_ABTESTPATH_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_ACTIVITY_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_ACTIVITY_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_ACTIVITY_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_ACTIVITY_ALL , MD_ACTIVITY_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_ACTIVITY_CUSTOM_PROP has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_ACTIVITY_CUSTOM_PROP;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_ACTIVITY_CUSTOM_PROP) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_ACTIVITY_CUSTOM_PROP , MD_ACTIVITY_CUSTOM_PROP );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_ACTIVITY_CUSTOM_PROP_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_ACTIVITY_CUSTOM_PROP_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_ACTIVITY_CUSTOM_PROP_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_ACTIVITY_CUSTOM_PROP_ALL , MD_ACTIVITY_CUSTOM_PROP_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_ACTIVITY_NODE has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_ACTIVITY_NODE;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_ACTIVITY_NODE) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_ACTIVITY_NODE , MD_ACTIVITY_NODE );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_ACTIVITY_NODE_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_ACTIVITY_NODE_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_ACTIVITY_NODE_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_ACTIVITY_NODE_ALL , MD_ACTIVITY_NODE_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_ACTIVITY_X_ACTIVITY_NODE has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_ACTIVITY_X_ACTIVITY_NODE;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_ACTIVITY_X_ACTIVITY_NODE) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_ACTIVITY_X_ACTIVITY_NODE , MD_ACTIVITY_X_ACTIVITY_NODE );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_ACTIVITY_X_ACTIVITY_NODE_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_ACTIVITY_X_ACTIVITY_NODE_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_ACTIVITY_X_ACTIVITY_NODE_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_ACTIVITY_X_ACTIVITY_NODE_ALL , MD_ACTIVITY_X_ACTIVITY_NODE_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_ACTIVITY_X_TASK has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_ACTIVITY_X_TASK;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_ACTIVITY_X_TASK) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_ACTIVITY_X_TASK , MD_ACTIVITY_X_TASK );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_ACTIVITY_X_TASK_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_ACTIVITY_X_TASK_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_ACTIVITY_X_TASK_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_ACTIVITY_X_TASK_ALL , MD_ACTIVITY_X_TASK_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_ASSET has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_ASSET;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_ASSET) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_ASSET , MD_ASSET );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_ASSET_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_ASSET_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_ASSET_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_ASSET_ALL , MD_ASSET_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_AUDIENCE has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_AUDIENCE;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_AUDIENCE) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_AUDIENCE , MD_AUDIENCE );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_AUDIENCE_OCCURRENCE has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_AUDIENCE_OCCURRENCE;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_AUDIENCE_OCCURRENCE) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_AUDIENCE_OCCURRENCE , MD_AUDIENCE_OCCURRENCE );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_AUDIENCE_X_SEGMENT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_AUDIENCE_X_SEGMENT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_AUDIENCE_X_SEGMENT) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_AUDIENCE_X_SEGMENT , MD_AUDIENCE_X_SEGMENT );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_BU has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_BU;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_BU) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_BU , MD_BU );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_BUSINESS_CONTEXT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_BUSINESS_CONTEXT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_BUSINESS_CONTEXT) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_BUSINESS_CONTEXT , MD_BUSINESS_CONTEXT );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_BUSINESS_CONTEXT_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_BUSINESS_CONTEXT_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_BUSINESS_CONTEXT_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_BUSINESS_CONTEXT_ALL , MD_BUSINESS_CONTEXT_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_COSTCENTER has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_COSTCENTER;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_COSTCENTER) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_COSTCENTER , MD_COSTCENTER );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_COST_CATEGORY has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_COST_CATEGORY;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_COST_CATEGORY) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_COST_CATEGORY , MD_COST_CATEGORY );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_CREATIVE has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_CREATIVE;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_CREATIVE) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_CREATIVE , MD_CREATIVE );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_CREATIVE_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_CREATIVE_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_CREATIVE_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_CREATIVE_ALL , MD_CREATIVE_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_CREATIVE_CUSTOM_PROP has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_CREATIVE_CUSTOM_PROP;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_CREATIVE_CUSTOM_PROP) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_CREATIVE_CUSTOM_PROP , MD_CREATIVE_CUSTOM_PROP );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_CREATIVE_CUSTOM_PROP_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_CREATIVE_CUSTOM_PROP_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_CREATIVE_CUSTOM_PROP_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_CREATIVE_CUSTOM_PROP_ALL , MD_CREATIVE_CUSTOM_PROP_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_CREATIVE_X_ASSET has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_CREATIVE_X_ASSET;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_CREATIVE_X_ASSET) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_CREATIVE_X_ASSET , MD_CREATIVE_X_ASSET );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_CREATIVE_X_ASSET_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_CREATIVE_X_ASSET_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_CREATIVE_X_ASSET_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_CREATIVE_X_ASSET_ALL , MD_CREATIVE_X_ASSET_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_CUSTATTRIB_TABLE_VALUES has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_CUSTATTRIB_TABLE_VALUES;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_CUSTATTRIB_TABLE_VALUES) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_CUSTATTRIB_TABLE_VALUES , MD_CUSTATTRIB_TABLE_VALUES );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_CUST_ATTRIB has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_CUST_ATTRIB;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_CUST_ATTRIB) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_CUST_ATTRIB , MD_CUST_ATTRIB );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_DATAVIEW has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_DATAVIEW;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_DATAVIEW) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_DATAVIEW , MD_DATAVIEW );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_DATAVIEW_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_DATAVIEW_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_DATAVIEW_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_DATAVIEW_ALL , MD_DATAVIEW_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_DATAVIEW_X_EVENT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_DATAVIEW_X_EVENT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_DATAVIEW_X_EVENT) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_DATAVIEW_X_EVENT , MD_DATAVIEW_X_EVENT );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_DATAVIEW_X_EVENT_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_DATAVIEW_X_EVENT_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_DATAVIEW_X_EVENT_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_DATAVIEW_X_EVENT_ALL , MD_DATAVIEW_X_EVENT_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_EVENT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_EVENT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_EVENT) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_EVENT , MD_EVENT );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_EVENT_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_EVENT_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_EVENT_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_EVENT_ALL , MD_EVENT_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_FISCAL_PERIOD has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_FISCAL_PERIOD;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_FISCAL_PERIOD) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_FISCAL_PERIOD , MD_FISCAL_PERIOD );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_GRID_ATTR_DEFN has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_GRID_ATTR_DEFN;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_GRID_ATTR_DEFN) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_GRID_ATTR_DEFN , MD_GRID_ATTR_DEFN );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_JOURNEY has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_JOURNEY;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_JOURNEY) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_JOURNEY , MD_JOURNEY );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_JOURNEY_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_JOURNEY_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_JOURNEY_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_JOURNEY_ALL , MD_JOURNEY_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_JOURNEY_NODE has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_JOURNEY_NODE;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_JOURNEY_NODE) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_JOURNEY_NODE , MD_JOURNEY_NODE );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_JOURNEY_NODE_OCCURRENCE has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_JOURNEY_NODE_OCCURRENCE;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_JOURNEY_NODE_OCCURRENCE) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_JOURNEY_NODE_OCCURRENCE , MD_JOURNEY_NODE_OCCURRENCE );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_JOURNEY_NODE_X_NEXT_NODE has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_JOURNEY_NODE_X_NEXT_NODE;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_JOURNEY_NODE_X_NEXT_NODE) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_JOURNEY_NODE_X_NEXT_NODE , MD_JOURNEY_NODE_X_NEXT_NODE );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_JOURNEY_NODE_X_PREVIOUS_NODE has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_JOURNEY_NODE_X_PREVIOUS_NODE;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_JOURNEY_NODE_X_PREVIOUS_NODE) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_JOURNEY_NODE_X_PREVIOUS_NODE , MD_JOURNEY_NODE_X_PREVIOUS_NODE );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_JOURNEY_NODE_X_VARIANT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_JOURNEY_NODE_X_VARIANT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_JOURNEY_NODE_X_VARIANT) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_JOURNEY_NODE_X_VARIANT , MD_JOURNEY_NODE_X_VARIANT );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_JOURNEY_OCCURRENCE has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_JOURNEY_OCCURRENCE;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_JOURNEY_OCCURRENCE) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_JOURNEY_OCCURRENCE , MD_JOURNEY_OCCURRENCE );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_JOURNEY_X_AUDIENCE has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_JOURNEY_X_AUDIENCE;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_JOURNEY_X_AUDIENCE) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_JOURNEY_X_AUDIENCE , MD_JOURNEY_X_AUDIENCE );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_JOURNEY_X_EVENT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_JOURNEY_X_EVENT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_JOURNEY_X_EVENT) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_JOURNEY_X_EVENT , MD_JOURNEY_X_EVENT );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_JOURNEY_X_TASK has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_JOURNEY_X_TASK;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_JOURNEY_X_TASK) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_JOURNEY_X_TASK , MD_JOURNEY_X_TASK );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_MESSAGE has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_MESSAGE;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_MESSAGE) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_MESSAGE , MD_MESSAGE );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_MESSAGE_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_MESSAGE_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_MESSAGE_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_MESSAGE_ALL , MD_MESSAGE_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_MESSAGE_CUSTOM_PROP has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_MESSAGE_CUSTOM_PROP;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_MESSAGE_CUSTOM_PROP) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_MESSAGE_CUSTOM_PROP , MD_MESSAGE_CUSTOM_PROP );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_MESSAGE_CUSTOM_PROP_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_MESSAGE_CUSTOM_PROP_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_MESSAGE_CUSTOM_PROP_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_MESSAGE_CUSTOM_PROP_ALL , MD_MESSAGE_CUSTOM_PROP_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_MESSAGE_X_CREATIVE has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_MESSAGE_X_CREATIVE;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_MESSAGE_X_CREATIVE) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_MESSAGE_X_CREATIVE , MD_MESSAGE_X_CREATIVE );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_MESSAGE_X_CREATIVE_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_MESSAGE_X_CREATIVE_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_MESSAGE_X_CREATIVE_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_MESSAGE_X_CREATIVE_ALL , MD_MESSAGE_X_CREATIVE_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_OBJECT_TYPE has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_OBJECT_TYPE;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_OBJECT_TYPE) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_OBJECT_TYPE , MD_OBJECT_TYPE );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_OCCURRENCE has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_OCCURRENCE;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_OCCURRENCE) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_OCCURRENCE , MD_OCCURRENCE );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_PICKLIST has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_PICKLIST;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_PICKLIST) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_PICKLIST , MD_PICKLIST );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_PURPOSE has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_PURPOSE;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_PURPOSE) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_PURPOSE , MD_PURPOSE );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_RTC has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_RTC;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_RTC) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_RTC , MD_RTC );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_SEGMENT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_SEGMENT , MD_SEGMENT );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_SEGMENT_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_SEGMENT_ALL , MD_SEGMENT_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_SEGMENT_CUSTOM_PROP has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_CUSTOM_PROP;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_CUSTOM_PROP) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_SEGMENT_CUSTOM_PROP , MD_SEGMENT_CUSTOM_PROP );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_SEGMENT_CUSTOM_PROP_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_CUSTOM_PROP_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_CUSTOM_PROP_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_SEGMENT_CUSTOM_PROP_ALL , MD_SEGMENT_CUSTOM_PROP_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_SEGMENT_MAP has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_MAP;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_MAP) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_SEGMENT_MAP , MD_SEGMENT_MAP );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_SEGMENT_MAP_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_MAP_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_MAP_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_SEGMENT_MAP_ALL , MD_SEGMENT_MAP_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_SEGMENT_MAP_CUSTOM_PROP has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_MAP_CUSTOM_PROP;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_MAP_CUSTOM_PROP) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_SEGMENT_MAP_CUSTOM_PROP , MD_SEGMENT_MAP_CUSTOM_PROP );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_SEGMENT_MAP_CUSTOM_PROP_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_MAP_CUSTOM_PROP_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_MAP_CUSTOM_PROP_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_SEGMENT_MAP_CUSTOM_PROP_ALL , MD_SEGMENT_MAP_CUSTOM_PROP_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_SEGMENT_MAP_X_SEGMENT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_MAP_X_SEGMENT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_MAP_X_SEGMENT) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_SEGMENT_MAP_X_SEGMENT , MD_SEGMENT_MAP_X_SEGMENT );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_SEGMENT_MAP_X_SEGMENT_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_MAP_X_SEGMENT_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_MAP_X_SEGMENT_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_SEGMENT_MAP_X_SEGMENT_ALL , MD_SEGMENT_MAP_X_SEGMENT_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_SEGMENT_TEST has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_TEST;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_TEST) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_SEGMENT_TEST , MD_SEGMENT_TEST );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_SEGMENT_TEST_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_TEST_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_TEST_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_SEGMENT_TEST_ALL , MD_SEGMENT_TEST_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_SEGMENT_TEST_X_SEGMENT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_TEST_X_SEGMENT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_TEST_X_SEGMENT) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_SEGMENT_TEST_X_SEGMENT , MD_SEGMENT_TEST_X_SEGMENT );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_SEGMENT_TEST_X_SEGMENT_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_TEST_X_SEGMENT_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_TEST_X_SEGMENT_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_SEGMENT_TEST_X_SEGMENT_ALL , MD_SEGMENT_TEST_X_SEGMENT_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_SEGMENT_X_EVENT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_X_EVENT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_X_EVENT) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_SEGMENT_X_EVENT , MD_SEGMENT_X_EVENT );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_SEGMENT_X_EVENT_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SEGMENT_X_EVENT_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_SEGMENT_X_EVENT_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_SEGMENT_X_EVENT_ALL , MD_SEGMENT_X_EVENT_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_SPOT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SPOT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_SPOT) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_SPOT , MD_SPOT );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_SPOT_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_SPOT_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_SPOT_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_SPOT_ALL , MD_SPOT_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_TARGET_ASSIST has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TARGET_ASSIST;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_TARGET_ASSIST) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_TARGET_ASSIST , MD_TARGET_ASSIST );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_TASK has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_TASK , MD_TASK );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_TASK_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_TASK_ALL , MD_TASK_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_TASK_CUSTOM_PROP has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_CUSTOM_PROP;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_CUSTOM_PROP) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_TASK_CUSTOM_PROP , MD_TASK_CUSTOM_PROP );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_TASK_CUSTOM_PROP_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_CUSTOM_PROP_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_CUSTOM_PROP_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_TASK_CUSTOM_PROP_ALL , MD_TASK_CUSTOM_PROP_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_TASK_X_AUDIENCE has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_AUDIENCE;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_AUDIENCE) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_TASK_X_AUDIENCE , MD_TASK_X_AUDIENCE );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_TASK_X_CREATIVE has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_CREATIVE;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_CREATIVE) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_TASK_X_CREATIVE , MD_TASK_X_CREATIVE );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_TASK_X_CREATIVE_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_CREATIVE_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_CREATIVE_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_TASK_X_CREATIVE_ALL , MD_TASK_X_CREATIVE_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_TASK_X_DATAVIEW has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_DATAVIEW;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_DATAVIEW) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_TASK_X_DATAVIEW , MD_TASK_X_DATAVIEW );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_TASK_X_DATAVIEW_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_DATAVIEW_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_DATAVIEW_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_TASK_X_DATAVIEW_ALL , MD_TASK_X_DATAVIEW_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_TASK_X_EVENT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_EVENT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_EVENT) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_TASK_X_EVENT , MD_TASK_X_EVENT );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_TASK_X_EVENT_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_EVENT_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_EVENT_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_TASK_X_EVENT_ALL , MD_TASK_X_EVENT_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_TASK_X_MESSAGE has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_MESSAGE;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_MESSAGE) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_TASK_X_MESSAGE , MD_TASK_X_MESSAGE );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_TASK_X_MESSAGE_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_MESSAGE_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_MESSAGE_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_TASK_X_MESSAGE_ALL , MD_TASK_X_MESSAGE_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_TASK_X_SEGMENT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_SEGMENT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_SEGMENT) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_TASK_X_SEGMENT , MD_TASK_X_SEGMENT );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_TASK_X_SEGMENT_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_SEGMENT_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_SEGMENT_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_TASK_X_SEGMENT_ALL , MD_TASK_X_SEGMENT_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_TASK_X_SPOT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_SPOT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_SPOT) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_TASK_X_SPOT , MD_TASK_X_SPOT );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_TASK_X_SPOT_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_SPOT_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_SPOT_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_TASK_X_SPOT_ALL , MD_TASK_X_SPOT_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_TASK_X_VARIANT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_VARIANT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_VARIANT) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_TASK_X_VARIANT , MD_TASK_X_VARIANT );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_TASK_X_VARIANT_ALL has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_TASK_X_VARIANT_ALL;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_TASK_X_VARIANT_ALL) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_TASK_X_VARIANT_ALL , MD_TASK_X_VARIANT_ALL );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_VENDOR has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_VENDOR;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_VENDOR) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_VENDOR , MD_VENDOR );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_WF_PROCESS_DEF has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_WF_PROCESS_DEF;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_WF_PROCESS_DEF) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_WF_PROCESS_DEF , MD_WF_PROCESS_DEF );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_WF_PROCESS_DEF_ATTR_GRP has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_WF_PROCESS_DEF_ATTR_GRP;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_WF_PROCESS_DEF_ATTR_GRP) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_WF_PROCESS_DEF_ATTR_GRP , MD_WF_PROCESS_DEF_ATTR_GRP );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_WF_PROCESS_DEF_CATEGORIES has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_WF_PROCESS_DEF_CATEGORIES;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_WF_PROCESS_DEF_CATEGORIES) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_WF_PROCESS_DEF_CATEGORIES , MD_WF_PROCESS_DEF_CATEGORIES );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_WF_PROCESS_DEF_TASKS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_WF_PROCESS_DEF_TASKS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_WF_PROCESS_DEF_TASKS) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_WF_PROCESS_DEF_TASKS , MD_WF_PROCESS_DEF_TASKS );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MD_WF_PROCESS_DEF_TASK_ASSG has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MD_WF_PROCESS_DEF_TASK_ASSG;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..MD_WF_PROCESS_DEF_TASK_ASSG) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate MD_WF_PROCESS_DEF_TASK_ASSG , MD_WF_PROCESS_DEF_TASK_ASSG );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: MEDIA_ACTIVITY_DETAILS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MEDIA_ACTIVITY_DETAILS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..MEDIA_ACTIVITY_DETAILS_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table MEDIA_ACTIVITY_DETAILS_tmp , MEDIA_ACTIVITY_DETAILS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=MEDIA_ACTIVITY_DETAILS , table_keys=%str(EVENT_ID), out_table=work.MEDIA_ACTIVITY_DETAILS );
      DATA work.MEDIA_ACTIVITY_DETAILS_tmp ;
         SET work.MEDIA_ACTIVITY_DETAILS ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : MEDIA_ACTIVITY_DETAILS_tmp , MEDIA_ACTIVITY_DETAILS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..MEDIA_ACTIVITY_DETAILS_tmp as select * from &dbschema..MEDIA_ACTIVITY_DETAILS where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : MEDIA_ACTIVITY_DETAILS_tmp , MEDIA_ACTIVITY_DETAILS , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.MEDIA_ACTIVITY_DETAILS_tmp  base=&tmplib..MEDIA_ACTIVITY_DETAILS_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : MEDIA_ACTIVITY_DETAILS_tmp , MEDIA_ACTIVITY_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..MEDIA_ACTIVITY_DETAILS AS b USING &tmpdbschema..MEDIA_ACTIVITY_DETAILS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            load_dttm = d.load_dttm, 
            action_dttm_tz = d.action_dttm_tz, action_dttm = d.action_dttm, 
            playhead_position = d.playhead_position, media_nm = d.media_nm, 
            detail_id = d.detail_id, action = d.action, 
            detail_id_hex = d.detail_id_hex, media_uri_txt = d.media_uri_txt
         WHEN NOT MATCHED THEN INSERT (
            load_dttm, action_dttm_tz, action_dttm, 
            playhead_position, media_nm, event_id, detail_id, 
            action, detail_id_hex, media_uri_txt
         ) VALUES (
            d.load_dttm, d.action_dttm_tz, d.action_dttm, 
            d.playhead_position, d.media_nm, d.event_id, d.detail_id, 
            d.action, d.detail_id_hex, d.media_uri_txt  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : MEDIA_ACTIVITY_DETAILS_tmp , MEDIA_ACTIVITY_DETAILS , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: MEDIA_DETAILS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MEDIA_DETAILS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..MEDIA_DETAILS_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table MEDIA_DETAILS_tmp , MEDIA_DETAILS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=MEDIA_DETAILS , table_keys=%str(EVENT_ID), out_table=work.MEDIA_DETAILS );
      DATA work.MEDIA_DETAILS_tmp ;
         SET work.MEDIA_DETAILS ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : MEDIA_DETAILS_tmp , MEDIA_DETAILS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..MEDIA_DETAILS_tmp as select * from &dbschema..MEDIA_DETAILS where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : MEDIA_DETAILS_tmp , MEDIA_DETAILS , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.MEDIA_DETAILS_tmp  base=&tmplib..MEDIA_DETAILS_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : MEDIA_DETAILS_tmp , MEDIA_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..MEDIA_DETAILS AS b USING &tmpdbschema..MEDIA_DETAILS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            media_duration_secs = d.media_duration_secs, 
            play_start_dttm_tz = d.play_start_dttm_tz, play_start_dttm = d.play_start_dttm, 
            load_dttm = d.load_dttm, visit_id_hex = d.visit_id_hex, 
            session_id_hex = d.session_id_hex, session_id = d.session_id, 
            media_uri_txt = d.media_uri_txt, media_player_nm = d.media_player_nm, 
            media_nm = d.media_nm, identity_id = d.identity_id, 
            event_key_cd = d.event_key_cd, detail_id_hex = d.detail_id_hex, 
            detail_id = d.detail_id, event_source_cd = d.event_source_cd, 
            media_player_version_txt = d.media_player_version_txt, visit_id = d.visit_id
         WHEN NOT MATCHED THEN INSERT (
            media_duration_secs, play_start_dttm_tz, play_start_dttm, 
            load_dttm, visit_id_hex, session_id_hex, session_id, 
            media_uri_txt, media_player_nm, media_nm, identity_id, 
            event_key_cd, detail_id_hex, detail_id, event_id, 
            event_source_cd, media_player_version_txt, visit_id
         ) VALUES (
            d.media_duration_secs, d.play_start_dttm_tz, d.play_start_dttm, 
            d.load_dttm, d.visit_id_hex, d.session_id_hex, d.session_id, 
            d.media_uri_txt, d.media_player_nm, d.media_nm, d.identity_id, 
            d.event_key_cd, d.detail_id_hex, d.detail_id, d.event_id, 
            d.event_source_cd, d.media_player_version_txt, d.visit_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : MEDIA_DETAILS_tmp , MEDIA_DETAILS , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: MEDIA_DETAILS_EXT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MEDIA_DETAILS_EXT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..MEDIA_DETAILS_EXT_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table MEDIA_DETAILS_EXT_tmp , MEDIA_DETAILS_EXT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=MEDIA_DETAILS_EXT , table_keys=%str(EVENT_ID), out_table=work.MEDIA_DETAILS_EXT );
      DATA work.MEDIA_DETAILS_EXT_tmp ;
         SET work.MEDIA_DETAILS_EXT ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : MEDIA_DETAILS_EXT_tmp , MEDIA_DETAILS_EXT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..MEDIA_DETAILS_EXT_tmp as select * from &dbschema..MEDIA_DETAILS_EXT where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : MEDIA_DETAILS_EXT_tmp , MEDIA_DETAILS_EXT , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.MEDIA_DETAILS_EXT_tmp  base=&tmplib..MEDIA_DETAILS_EXT_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : MEDIA_DETAILS_EXT_tmp , MEDIA_DETAILS_EXT );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..MEDIA_DETAILS_EXT AS b USING &tmpdbschema..MEDIA_DETAILS_EXT_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            exit_point_secs = d.exit_point_secs, 
            max_play_secs = d.max_play_secs, view_duration_secs = d.view_duration_secs, 
            end_tm = d.end_tm, media_display_duration_secs = d.media_display_duration_secs, 
            start_tm = d.start_tm, interaction_cnt = d.interaction_cnt, 
            play_end_dttm_tz = d.play_end_dttm_tz, play_end_dttm = d.play_end_dttm, 
            load_dttm = d.load_dttm, media_nm = d.media_nm, 
            detail_id_hex = d.detail_id_hex, detail_id = d.detail_id, 
            media_uri_txt = d.media_uri_txt
         WHEN NOT MATCHED THEN INSERT (
            exit_point_secs, max_play_secs, view_duration_secs, 
            end_tm, media_display_duration_secs, start_tm, interaction_cnt, 
            play_end_dttm_tz, play_end_dttm, load_dttm, media_nm, 
            event_id, detail_id_hex, detail_id, media_uri_txt
         ) VALUES (
            d.exit_point_secs, d.max_play_secs, d.view_duration_secs, 
            d.end_tm, d.media_display_duration_secs, d.start_tm, d.interaction_cnt, 
            d.play_end_dttm_tz, d.play_end_dttm, d.load_dttm, d.media_nm, 
            d.event_id, d.detail_id_hex, d.detail_id, d.media_uri_txt  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : MEDIA_DETAILS_EXT_tmp , MEDIA_DETAILS_EXT , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: MOBILE_FOCUS_DEFOCUS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MOBILE_FOCUS_DEFOCUS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..MOBILE_FOCUS_DEFOCUS_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table MOBILE_FOCUS_DEFOCUS_tmp , MOBILE_FOCUS_DEFOCUS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=MOBILE_FOCUS_DEFOCUS , table_keys=%str(EVENT_ID), out_table=work.MOBILE_FOCUS_DEFOCUS );
      DATA work.MOBILE_FOCUS_DEFOCUS_tmp ;
         SET work.MOBILE_FOCUS_DEFOCUS ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : MOBILE_FOCUS_DEFOCUS_tmp , MOBILE_FOCUS_DEFOCUS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..MOBILE_FOCUS_DEFOCUS_tmp as select * from &dbschema..MOBILE_FOCUS_DEFOCUS where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : MOBILE_FOCUS_DEFOCUS_tmp , MOBILE_FOCUS_DEFOCUS , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.MOBILE_FOCUS_DEFOCUS_tmp  base=&tmplib..MOBILE_FOCUS_DEFOCUS_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : MOBILE_FOCUS_DEFOCUS_tmp , MOBILE_FOCUS_DEFOCUS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..MOBILE_FOCUS_DEFOCUS AS b USING &tmpdbschema..MOBILE_FOCUS_DEFOCUS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            action_dttm = d.action_dttm, 
            load_dttm = d.load_dttm, action_dttm_tz = d.action_dttm_tz, 
            visit_id_hex = d.visit_id_hex, reserved_1_txt = d.reserved_1_txt, 
            mobile_app_id = d.mobile_app_id, event_nm = d.event_nm, 
            event_designed_id = d.event_designed_id, detail_id_hex = d.detail_id_hex, 
            channel_user_id = d.channel_user_id, identity_id = d.identity_id, 
            session_id_hex = d.session_id_hex
         WHEN NOT MATCHED THEN INSERT (
            action_dttm, load_dttm, action_dttm_tz, 
            visit_id_hex, reserved_1_txt, mobile_app_id, event_nm, 
            event_designed_id, detail_id_hex, channel_user_id, event_id, 
            identity_id, session_id_hex
         ) VALUES (
            d.action_dttm, d.load_dttm, d.action_dttm_tz, 
            d.visit_id_hex, d.reserved_1_txt, d.mobile_app_id, d.event_nm, 
            d.event_designed_id, d.detail_id_hex, d.channel_user_id, d.event_id, 
            d.identity_id, d.session_id_hex  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : MOBILE_FOCUS_DEFOCUS_tmp , MOBILE_FOCUS_DEFOCUS , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: MOBILE_SPOTS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MOBILE_SPOTS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..MOBILE_SPOTS_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table MOBILE_SPOTS_tmp , MOBILE_SPOTS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=MOBILE_SPOTS , table_keys=%str(EVENT_ID), out_table=work.MOBILE_SPOTS );
      DATA work.MOBILE_SPOTS_tmp ;
         SET work.MOBILE_SPOTS ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : MOBILE_SPOTS_tmp , MOBILE_SPOTS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..MOBILE_SPOTS_tmp as select * from &dbschema..MOBILE_SPOTS where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : MOBILE_SPOTS_tmp , MOBILE_SPOTS , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.MOBILE_SPOTS_tmp  base=&tmplib..MOBILE_SPOTS_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : MOBILE_SPOTS_tmp , MOBILE_SPOTS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..MOBILE_SPOTS AS b USING &tmpdbschema..MOBILE_SPOTS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            load_dttm = d.load_dttm, 
            action_dttm_tz = d.action_dttm_tz, action_dttm = d.action_dttm, 
            visit_id_hex = d.visit_id_hex, session_id_hex = d.session_id_hex, 
            mobile_app_id = d.mobile_app_id, event_nm = d.event_nm, 
            event_designed_id = d.event_designed_id, creative_id = d.creative_id, 
            context_val = d.context_val, context_type_nm = d.context_type_nm, 
            channel_user_id = d.channel_user_id, detail_id_hex = d.detail_id_hex, 
            identity_id = d.identity_id, spot_id = d.spot_id
         WHEN NOT MATCHED THEN INSERT (
            load_dttm, action_dttm_tz, action_dttm, 
            visit_id_hex, session_id_hex, mobile_app_id, event_nm, 
            event_designed_id, creative_id, context_val, context_type_nm, 
            channel_user_id, detail_id_hex, event_id, identity_id, 
            spot_id
         ) VALUES (
            d.load_dttm, d.action_dttm_tz, d.action_dttm, 
            d.visit_id_hex, d.session_id_hex, d.mobile_app_id, d.event_nm, 
            d.event_designed_id, d.creative_id, d.context_val, d.context_type_nm, 
            d.channel_user_id, d.detail_id_hex, d.event_id, d.identity_id, 
            d.spot_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : MOBILE_SPOTS_tmp , MOBILE_SPOTS , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: MONTHLY_USAGE has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..MONTHLY_USAGE;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..MONTHLY_USAGE_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table MONTHLY_USAGE_tmp , MONTHLY_USAGE_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=MONTHLY_USAGE , table_keys=%str(EVENT_MONTH), out_table=work.MONTHLY_USAGE );
      DATA work.MONTHLY_USAGE_tmp ;
         SET work.MONTHLY_USAGE ;
         WHERE 1=1 AND EVENT_MONTH IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : MONTHLY_USAGE_tmp , MONTHLY_USAGE_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..MONTHLY_USAGE_tmp as select * from &dbschema..MONTHLY_USAGE where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : MONTHLY_USAGE_tmp , MONTHLY_USAGE , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.MONTHLY_USAGE_tmp  base=&tmplib..MONTHLY_USAGE_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : MONTHLY_USAGE_tmp , MONTHLY_USAGE );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..MONTHLY_USAGE AS b USING &tmpdbschema..MONTHLY_USAGE_tmp AS d ON (
            b.event_month = d.event_month )
         WHEN MATCHED THEN
         UPDATE SET
            advertising_members_cnt_str = d.advertising_members_cnt_str, 
            api_usage_str = d.api_usage_str, customer_profiles_processed_str = d.customer_profiles_processed_str, 
            bc_subjcnt_str = d.bc_subjcnt_str, mobile_push_cnt = d.mobile_push_cnt, 
            sms_frag_cnt = d.sms_frag_cnt, google_ads_cnt = d.google_ads_cnt, 
            dm_destinations_total_id_cnt = d.dm_destinations_total_id_cnt, mai_scored_project_cnt = d.mai_scored_project_cnt, 
            linkedin_ads_cnt = d.linkedin_ads_cnt, mobile_in_app_msg_cnt = d.mobile_in_app_msg_cnt, 
            plan_users_cnt = d.plan_users_cnt, sms_output_result_cnt = d.sms_output_result_cnt, 
            web_impr_cnt = d.web_impr_cnt, mob_impr_cnt = d.mob_impr_cnt, 
            mob_sesn_cnt = d.mob_sesn_cnt, email_send_cnt = d.email_send_cnt, 
            facebook_ads_cnt = d.facebook_ads_cnt, outbound_api_cnt = d.outbound_api_cnt, 
            web_sesn_cnt = d.web_sesn_cnt, sms_send_cnt = d.sms_send_cnt, 
            email_preview_cnt = d.email_preview_cnt, dm_destinations_total_row_cnt = d.dm_destinations_total_row_cnt, 
            audience_usage_cnt = d.audience_usage_cnt, asset_size = d.asset_size, 
            db_size = d.db_size, admin_user_cnt = d.admin_user_cnt, 
            sms_send_str = d.sms_send_str, sms_frag_str = d.sms_frag_str, 
            sms_output_result_str = d.sms_output_result_str
         WHEN NOT MATCHED THEN INSERT (
            advertising_members_cnt_str, api_usage_str, customer_profiles_processed_str, 
            bc_subjcnt_str, mobile_push_cnt, sms_frag_cnt, google_ads_cnt, 
            dm_destinations_total_id_cnt, mai_scored_project_cnt, linkedin_ads_cnt, mobile_in_app_msg_cnt, 
            plan_users_cnt, sms_output_result_cnt, web_impr_cnt, mob_impr_cnt, 
            mob_sesn_cnt, email_send_cnt, facebook_ads_cnt, outbound_api_cnt, 
            web_sesn_cnt, sms_send_cnt, email_preview_cnt, dm_destinations_total_row_cnt, 
            audience_usage_cnt, asset_size, db_size, admin_user_cnt, 
            sms_send_str, sms_frag_str, sms_output_result_str, event_month
         ) VALUES (
            d.advertising_members_cnt_str, d.api_usage_str, d.customer_profiles_processed_str, 
            d.bc_subjcnt_str, d.mobile_push_cnt, d.sms_frag_cnt, d.google_ads_cnt, 
            d.dm_destinations_total_id_cnt, d.mai_scored_project_cnt, d.linkedin_ads_cnt, d.mobile_in_app_msg_cnt, 
            d.plan_users_cnt, d.sms_output_result_cnt, d.web_impr_cnt, d.mob_impr_cnt, 
            d.mob_sesn_cnt, d.email_send_cnt, d.facebook_ads_cnt, d.outbound_api_cnt, 
            d.web_sesn_cnt, d.sms_send_cnt, d.email_preview_cnt, d.dm_destinations_total_row_cnt, 
            d.audience_usage_cnt, d.asset_size, d.db_size, d.admin_user_cnt, 
            d.sms_send_str, d.sms_frag_str, d.sms_output_result_str, d.event_month  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : MONTHLY_USAGE_tmp , MONTHLY_USAGE , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: NOTIFICATION_FAILED has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..NOTIFICATION_FAILED;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..NOTIFICATION_FAILED_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table NOTIFICATION_FAILED_tmp , NOTIFICATION_FAILED_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=NOTIFICATION_FAILED , table_keys=%str(EVENT_ID), out_table=work.NOTIFICATION_FAILED );
      DATA work.NOTIFICATION_FAILED_tmp ;
         SET work.NOTIFICATION_FAILED ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : NOTIFICATION_FAILED_tmp , NOTIFICATION_FAILED_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..NOTIFICATION_FAILED_tmp as select * from &dbschema..NOTIFICATION_FAILED where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : NOTIFICATION_FAILED_tmp , NOTIFICATION_FAILED , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.NOTIFICATION_FAILED_tmp  base=&tmplib..NOTIFICATION_FAILED_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : NOTIFICATION_FAILED_tmp , NOTIFICATION_FAILED );
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
            segment_version_id = d.segment_version_id, response_tracking_cd = d.response_tracking_cd, 
            reserved_1_txt = d.reserved_1_txt, occurrence_id = d.occurrence_id, 
            message_version_id = d.message_version_id, message_id = d.message_id, 
            journey_id = d.journey_id, identity_id = d.identity_id, 
            event_nm = d.event_nm, event_designed_id = d.event_designed_id, 
            error_cd = d.error_cd, creative_id = d.creative_id, 
            context_val = d.context_val, context_type_nm = d.context_type_nm, 
            channel_nm = d.channel_nm, aud_occurrence_id = d.aud_occurrence_id, 
            audience_id = d.audience_id, channel_user_id = d.channel_user_id, 
            creative_version_id = d.creative_version_id, error_message_txt = d.error_message_txt, 
            journey_occurrence_id = d.journey_occurrence_id, mobile_app_id = d.mobile_app_id, 
            reserved_2_txt = d.reserved_2_txt, segment_id = d.segment_id, 
            spot_id = d.spot_id, task_version_id = d.task_version_id
         WHEN NOT MATCHED THEN INSERT (
            properties_map_doc, notification_failed_dttm, notification_failed_dttm_tz, 
            load_dttm, task_id, segment_version_id, response_tracking_cd, 
            reserved_1_txt, occurrence_id, message_version_id, message_id, 
            journey_id, identity_id, event_nm, event_designed_id, 
            error_cd, creative_id, context_val, context_type_nm, 
            channel_nm, aud_occurrence_id, audience_id, channel_user_id, 
            creative_version_id, error_message_txt, event_id, journey_occurrence_id, 
            mobile_app_id, reserved_2_txt, segment_id, spot_id, 
            task_version_id
         ) VALUES (
            d.properties_map_doc, d.notification_failed_dttm, d.notification_failed_dttm_tz, 
            d.load_dttm, d.task_id, d.segment_version_id, d.response_tracking_cd, 
            d.reserved_1_txt, d.occurrence_id, d.message_version_id, d.message_id, 
            d.journey_id, d.identity_id, d.event_nm, d.event_designed_id, 
            d.error_cd, d.creative_id, d.context_val, d.context_type_nm, 
            d.channel_nm, d.aud_occurrence_id, d.audience_id, d.channel_user_id, 
            d.creative_version_id, d.error_message_txt, d.event_id, d.journey_occurrence_id, 
            d.mobile_app_id, d.reserved_2_txt, d.segment_id, d.spot_id, 
            d.task_version_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : NOTIFICATION_FAILED_tmp , NOTIFICATION_FAILED , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: NOTIFICATION_OPENED has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..NOTIFICATION_OPENED;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..NOTIFICATION_OPENED_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table NOTIFICATION_OPENED_tmp , NOTIFICATION_OPENED_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=NOTIFICATION_OPENED , table_keys=%str(EVENT_ID), out_table=work.NOTIFICATION_OPENED );
      DATA work.NOTIFICATION_OPENED_tmp ;
         SET work.NOTIFICATION_OPENED ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : NOTIFICATION_OPENED_tmp , NOTIFICATION_OPENED_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..NOTIFICATION_OPENED_tmp as select * from &dbschema..NOTIFICATION_OPENED where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : NOTIFICATION_OPENED_tmp , NOTIFICATION_OPENED , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.NOTIFICATION_OPENED_tmp  base=&tmplib..NOTIFICATION_OPENED_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : NOTIFICATION_OPENED_tmp , NOTIFICATION_OPENED );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..NOTIFICATION_OPENED AS b USING &tmpdbschema..NOTIFICATION_OPENED_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            properties_map_doc = d.properties_map_doc, 
            notification_opened_dttm = d.notification_opened_dttm, load_dttm = d.load_dttm, 
            notification_opened_dttm_tz = d.notification_opened_dttm_tz, task_version_id = d.task_version_id, 
            spot_id = d.spot_id, segment_id = d.segment_id, 
            reserved_3_txt = d.reserved_3_txt, reserved_1_txt = d.reserved_1_txt, 
            occurrence_id = d.occurrence_id, mobile_app_id = d.mobile_app_id, 
            message_version_id = d.message_version_id, message_id = d.message_id, 
            journey_occurrence_id = d.journey_occurrence_id, journey_id = d.journey_id, 
            event_nm = d.event_nm, event_designed_id = d.event_designed_id, 
            creative_id = d.creative_id, context_type_nm = d.context_type_nm, 
            channel_nm = d.channel_nm, audience_id = d.audience_id, 
            aud_occurrence_id = d.aud_occurrence_id, channel_user_id = d.channel_user_id, 
            context_val = d.context_val, creative_version_id = d.creative_version_id, 
            identity_id = d.identity_id, reserved_2_txt = d.reserved_2_txt, 
            response_tracking_cd = d.response_tracking_cd, segment_version_id = d.segment_version_id, 
            task_id = d.task_id
         WHEN NOT MATCHED THEN INSERT (
            properties_map_doc, notification_opened_dttm, load_dttm, 
            notification_opened_dttm_tz, task_version_id, spot_id, segment_id, 
            reserved_3_txt, reserved_1_txt, occurrence_id, mobile_app_id, 
            message_version_id, message_id, journey_occurrence_id, journey_id, 
            event_nm, event_designed_id, creative_id, context_type_nm, 
            channel_nm, audience_id, aud_occurrence_id, channel_user_id, 
            context_val, creative_version_id, event_id, identity_id, 
            reserved_2_txt, response_tracking_cd, segment_version_id, task_id
         ) VALUES (
            d.properties_map_doc, d.notification_opened_dttm, d.load_dttm, 
            d.notification_opened_dttm_tz, d.task_version_id, d.spot_id, d.segment_id, 
            d.reserved_3_txt, d.reserved_1_txt, d.occurrence_id, d.mobile_app_id, 
            d.message_version_id, d.message_id, d.journey_occurrence_id, d.journey_id, 
            d.event_nm, d.event_designed_id, d.creative_id, d.context_type_nm, 
            d.channel_nm, d.audience_id, d.aud_occurrence_id, d.channel_user_id, 
            d.context_val, d.creative_version_id, d.event_id, d.identity_id, 
            d.reserved_2_txt, d.response_tracking_cd, d.segment_version_id, d.task_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : NOTIFICATION_OPENED_tmp , NOTIFICATION_OPENED , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: NOTIFICATION_SEND has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..NOTIFICATION_SEND;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..NOTIFICATION_SEND_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table NOTIFICATION_SEND_tmp , NOTIFICATION_SEND_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=NOTIFICATION_SEND , table_keys=%str(EVENT_ID), out_table=work.NOTIFICATION_SEND );
      DATA work.NOTIFICATION_SEND_tmp ;
         SET work.NOTIFICATION_SEND ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : NOTIFICATION_SEND_tmp , NOTIFICATION_SEND_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..NOTIFICATION_SEND_tmp as select * from &dbschema..NOTIFICATION_SEND where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : NOTIFICATION_SEND_tmp , NOTIFICATION_SEND , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.NOTIFICATION_SEND_tmp  base=&tmplib..NOTIFICATION_SEND_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : NOTIFICATION_SEND_tmp , NOTIFICATION_SEND );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..NOTIFICATION_SEND AS b USING &tmpdbschema..NOTIFICATION_SEND_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            properties_map_doc = d.properties_map_doc, 
            notification_send_dttm = d.notification_send_dttm, load_dttm = d.load_dttm, 
            notification_send_dttm_tz = d.notification_send_dttm_tz, task_version_id = d.task_version_id, 
            spot_id = d.spot_id, segment_id = d.segment_id, 
            reserved_2_txt = d.reserved_2_txt, reserved_1_txt = d.reserved_1_txt, 
            occurrence_id = d.occurrence_id, mobile_app_id = d.mobile_app_id, 
            message_id = d.message_id, journey_occurrence_id = d.journey_occurrence_id, 
            identity_id = d.identity_id, creative_version_id = d.creative_version_id, 
            creative_id = d.creative_id, context_val = d.context_val, 
            channel_user_id = d.channel_user_id, audience_id = d.audience_id, 
            aud_occurrence_id = d.aud_occurrence_id, channel_nm = d.channel_nm, 
            context_type_nm = d.context_type_nm, event_designed_id = d.event_designed_id, 
            event_nm = d.event_nm, journey_id = d.journey_id, 
            message_version_id = d.message_version_id, response_tracking_cd = d.response_tracking_cd, 
            segment_version_id = d.segment_version_id, task_id = d.task_id
         WHEN NOT MATCHED THEN INSERT (
            properties_map_doc, notification_send_dttm, load_dttm, 
            notification_send_dttm_tz, task_version_id, spot_id, segment_id, 
            reserved_2_txt, reserved_1_txt, occurrence_id, mobile_app_id, 
            message_id, journey_occurrence_id, identity_id, event_id, 
            creative_version_id, creative_id, context_val, channel_user_id, 
            audience_id, aud_occurrence_id, channel_nm, context_type_nm, 
            event_designed_id, event_nm, journey_id, message_version_id, 
            response_tracking_cd, segment_version_id, task_id
         ) VALUES (
            d.properties_map_doc, d.notification_send_dttm, d.load_dttm, 
            d.notification_send_dttm_tz, d.task_version_id, d.spot_id, d.segment_id, 
            d.reserved_2_txt, d.reserved_1_txt, d.occurrence_id, d.mobile_app_id, 
            d.message_id, d.journey_occurrence_id, d.identity_id, d.event_id, 
            d.creative_version_id, d.creative_id, d.context_val, d.channel_user_id, 
            d.audience_id, d.aud_occurrence_id, d.channel_nm, d.context_type_nm, 
            d.event_designed_id, d.event_nm, d.journey_id, d.message_version_id, 
            d.response_tracking_cd, d.segment_version_id, d.task_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : NOTIFICATION_SEND_tmp , NOTIFICATION_SEND , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: NOTIFICATION_TARGETING_REQUEST has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..NOTIFICATION_TARGETING_REQUEST;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..NOTIFICATION_TARGETING_REQUE_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table NOTIFICATION_TARGETING_REQUE_tmp , NOTIFICATION_TARGETING_REQUE_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=NOTIFICATION_TARGETING_REQUEST , table_keys=%str(EVENT_ID), out_table=work.NOTIFICATION_TARGETING_REQUEST );
      DATA work.NOTIFICATION_TARGETING_REQUE_tmp ;
         SET work.NOTIFICATION_TARGETING_REQUEST ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : NOTIFICATION_TARGETING_REQUE_tmp , NOTIFICATION_TARGETING_REQUE_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..NOTIFICATION_TARGETING_REQUE_tmp as select * from &dbschema..NOTIFICATION_TARGETING_REQUEST where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : NOTIFICATION_TARGETING_REQUE_tmp , NOTIFICATION_TARGETING_REQUEST , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.NOTIFICATION_TARGETING_REQUE_tmp  base=&tmplib..NOTIFICATION_TARGETING_REQUE_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : NOTIFICATION_TARGETING_REQUE_tmp , NOTIFICATION_TARGETING_REQUEST );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..NOTIFICATION_TARGETING_REQUEST AS b USING &tmpdbschema..NOTIFICATION_TARGETING_REQUE_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            eligibility_flg = d.eligibility_flg, 
            notification_tgt_req_dttm = d.notification_tgt_req_dttm, notification_tgt_req_dttm_tz = d.notification_tgt_req_dttm_tz, 
            load_dttm = d.load_dttm, task_id = d.task_id, 
            mobile_app_id = d.mobile_app_id, journey_occurrence_id = d.journey_occurrence_id, 
            journey_id = d.journey_id, event_nm = d.event_nm, 
            event_designed_id = d.event_designed_id, context_val = d.context_val, 
            context_type_nm = d.context_type_nm, channel_user_id = d.channel_user_id, 
            audience_id = d.audience_id, aud_occurrence_id = d.aud_occurrence_id, 
            channel_nm = d.channel_nm, identity_id = d.identity_id
         WHEN NOT MATCHED THEN INSERT (
            eligibility_flg, notification_tgt_req_dttm, notification_tgt_req_dttm_tz, 
            load_dttm, task_id, mobile_app_id, journey_occurrence_id, 
            journey_id, event_nm, event_designed_id, context_val, 
            context_type_nm, channel_user_id, audience_id, aud_occurrence_id, 
            channel_nm, event_id, identity_id
         ) VALUES (
            d.eligibility_flg, d.notification_tgt_req_dttm, d.notification_tgt_req_dttm_tz, 
            d.load_dttm, d.task_id, d.mobile_app_id, d.journey_occurrence_id, 
            d.journey_id, d.event_nm, d.event_designed_id, d.context_val, 
            d.context_type_nm, d.channel_user_id, d.audience_id, d.aud_occurrence_id, 
            d.channel_nm, d.event_id, d.identity_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : NOTIFICATION_TARGETING_REQUE_tmp , NOTIFICATION_TARGETING_REQUEST , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: ORDER_DETAILS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..ORDER_DETAILS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..ORDER_DETAILS_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table ORDER_DETAILS_tmp , ORDER_DETAILS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=ORDER_DETAILS , table_keys=%str(EVENT_ID), out_table=work.ORDER_DETAILS );
      DATA work.ORDER_DETAILS_tmp ;
         SET work.ORDER_DETAILS ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : ORDER_DETAILS_tmp , ORDER_DETAILS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..ORDER_DETAILS_tmp as select * from &dbschema..ORDER_DETAILS where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : ORDER_DETAILS_tmp , ORDER_DETAILS , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.ORDER_DETAILS_tmp  base=&tmplib..ORDER_DETAILS_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : ORDER_DETAILS_tmp , ORDER_DETAILS );
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
            activity_dttm = d.activity_dttm, load_dttm = d.load_dttm, 
            activity_dttm_tz = d.activity_dttm_tz, visit_id = d.visit_id, 
            shipping_message_txt = d.shipping_message_txt, session_id = d.session_id, 
            saving_message_txt = d.saving_message_txt, reserved_1_txt = d.reserved_1_txt, 
            record_type = d.record_type, product_sku = d.product_sku, 
            product_id = d.product_id, order_id = d.order_id, 
            mobile_app_id = d.mobile_app_id, identity_id = d.identity_id, 
            event_source_cd = d.event_source_cd, event_key_cd = d.event_key_cd, 
            event_designed_id = d.event_designed_id, detail_id = d.detail_id, 
            currency_cd = d.currency_cd, channel_nm = d.channel_nm, 
            cart_id = d.cart_id, availability_message_txt = d.availability_message_txt, 
            cart_nm = d.cart_nm, detail_id_hex = d.detail_id_hex, 
            event_nm = d.event_nm, product_group_nm = d.product_group_nm, 
            product_nm = d.product_nm, session_id_hex = d.session_id_hex, 
            visit_id_hex = d.visit_id_hex
         WHEN NOT MATCHED THEN INSERT (
            unit_price_amt, quantity_amt, properties_map_doc, 
            activity_dttm, load_dttm, activity_dttm_tz, visit_id, 
            shipping_message_txt, session_id, saving_message_txt, reserved_1_txt, 
            record_type, product_sku, product_id, order_id, 
            mobile_app_id, identity_id, event_source_cd, event_key_cd, 
            event_designed_id, detail_id, currency_cd, channel_nm, 
            cart_id, availability_message_txt, cart_nm, detail_id_hex, 
            event_id, event_nm, product_group_nm, product_nm, 
            session_id_hex, visit_id_hex
         ) VALUES (
            d.unit_price_amt, d.quantity_amt, d.properties_map_doc, 
            d.activity_dttm, d.load_dttm, d.activity_dttm_tz, d.visit_id, 
            d.shipping_message_txt, d.session_id, d.saving_message_txt, d.reserved_1_txt, 
            d.record_type, d.product_sku, d.product_id, d.order_id, 
            d.mobile_app_id, d.identity_id, d.event_source_cd, d.event_key_cd, 
            d.event_designed_id, d.detail_id, d.currency_cd, d.channel_nm, 
            d.cart_id, d.availability_message_txt, d.cart_nm, d.detail_id_hex, 
            d.event_id, d.event_nm, d.product_group_nm, d.product_nm, 
            d.session_id_hex, d.visit_id_hex  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : ORDER_DETAILS_tmp , ORDER_DETAILS , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: ORDER_SUMMARY has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..ORDER_SUMMARY;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..ORDER_SUMMARY_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table ORDER_SUMMARY_tmp , ORDER_SUMMARY_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=ORDER_SUMMARY , table_keys=%str(EVENT_ID), out_table=work.ORDER_SUMMARY );
      DATA work.ORDER_SUMMARY_tmp ;
         SET work.ORDER_SUMMARY ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : ORDER_SUMMARY_tmp , ORDER_SUMMARY_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..ORDER_SUMMARY_tmp as select * from &dbschema..ORDER_SUMMARY where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : ORDER_SUMMARY_tmp , ORDER_SUMMARY , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.ORDER_SUMMARY_tmp  base=&tmplib..ORDER_SUMMARY_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : ORDER_SUMMARY_tmp , ORDER_SUMMARY );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..ORDER_SUMMARY AS b USING &tmpdbschema..ORDER_SUMMARY_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            shipping_amt = d.shipping_amt, 
            total_tax_amt = d.total_tax_amt, total_price_amt = d.total_price_amt, 
            total_unit_qty = d.total_unit_qty, properties_map_doc = d.properties_map_doc, 
            activity_dttm = d.activity_dttm, activity_dttm_tz = d.activity_dttm_tz, 
            load_dttm = d.load_dttm, visit_id_hex = d.visit_id_hex, 
            visit_id = d.visit_id, shipping_postal_cd = d.shipping_postal_cd, 
            shipping_country_nm = d.shipping_country_nm, shipping_city_nm = d.shipping_city_nm, 
            session_id_hex = d.session_id_hex, record_type = d.record_type, 
            payment_type_desc = d.payment_type_desc, order_id = d.order_id, 
            mobile_app_id = d.mobile_app_id, identity_id = d.identity_id, 
            event_source_cd = d.event_source_cd, event_nm = d.event_nm, 
            event_designed_id = d.event_designed_id, detail_id_hex = d.detail_id_hex, 
            delivery_type_desc = d.delivery_type_desc, currency_cd = d.currency_cd, 
            channel_nm = d.channel_nm, cart_nm = d.cart_nm, 
            cart_id = d.cart_id, billing_state_region_cd = d.billing_state_region_cd, 
            billing_postal_cd = d.billing_postal_cd, billing_city_nm = d.billing_city_nm, 
            billing_country_nm = d.billing_country_nm, detail_id = d.detail_id, 
            event_key_cd = d.event_key_cd, session_id = d.session_id, 
            shipping_state_region_cd = d.shipping_state_region_cd
         WHEN NOT MATCHED THEN INSERT (
            shipping_amt, total_tax_amt, total_price_amt, 
            total_unit_qty, properties_map_doc, activity_dttm, activity_dttm_tz, 
            load_dttm, visit_id_hex, visit_id, shipping_postal_cd, 
            shipping_country_nm, shipping_city_nm, session_id_hex, record_type, 
            payment_type_desc, order_id, mobile_app_id, identity_id, 
            event_source_cd, event_nm, event_id, event_designed_id, 
            detail_id_hex, delivery_type_desc, currency_cd, channel_nm, 
            cart_nm, cart_id, billing_state_region_cd, billing_postal_cd, 
            billing_city_nm, billing_country_nm, detail_id, event_key_cd, 
            session_id, shipping_state_region_cd
         ) VALUES (
            d.shipping_amt, d.total_tax_amt, d.total_price_amt, 
            d.total_unit_qty, d.properties_map_doc, d.activity_dttm, d.activity_dttm_tz, 
            d.load_dttm, d.visit_id_hex, d.visit_id, d.shipping_postal_cd, 
            d.shipping_country_nm, d.shipping_city_nm, d.session_id_hex, d.record_type, 
            d.payment_type_desc, d.order_id, d.mobile_app_id, d.identity_id, 
            d.event_source_cd, d.event_nm, d.event_id, d.event_designed_id, 
            d.detail_id_hex, d.delivery_type_desc, d.currency_cd, d.channel_nm, 
            d.cart_nm, d.cart_id, d.billing_state_region_cd, d.billing_postal_cd, 
            d.billing_city_nm, d.billing_country_nm, d.detail_id, d.event_key_cd, 
            d.session_id, d.shipping_state_region_cd  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : ORDER_SUMMARY_tmp , ORDER_SUMMARY , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: OUTBOUND_SYSTEM has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..OUTBOUND_SYSTEM;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..OUTBOUND_SYSTEM_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table OUTBOUND_SYSTEM_tmp , OUTBOUND_SYSTEM_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=OUTBOUND_SYSTEM , table_keys=%str(EVENT_ID), out_table=work.OUTBOUND_SYSTEM );
      DATA work.OUTBOUND_SYSTEM_tmp ;
         SET work.OUTBOUND_SYSTEM ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : OUTBOUND_SYSTEM_tmp , OUTBOUND_SYSTEM_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..OUTBOUND_SYSTEM_tmp as select * from &dbschema..OUTBOUND_SYSTEM where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : OUTBOUND_SYSTEM_tmp , OUTBOUND_SYSTEM , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.OUTBOUND_SYSTEM_tmp  base=&tmplib..OUTBOUND_SYSTEM_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : OUTBOUND_SYSTEM_tmp , OUTBOUND_SYSTEM );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..OUTBOUND_SYSTEM AS b USING &tmpdbschema..OUTBOUND_SYSTEM_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            properties_map_doc = d.properties_map_doc, 
            load_dttm = d.load_dttm, outbound_system_dttm_tz = d.outbound_system_dttm_tz, 
            outbound_system_dttm = d.outbound_system_dttm, visit_id_hex = d.visit_id_hex, 
            task_id = d.task_id, session_id_hex = d.session_id_hex, 
            segment_version_id = d.segment_version_id, segment_id = d.segment_id, 
            reserved_2_txt = d.reserved_2_txt, reserved_1_txt = d.reserved_1_txt, 
            parent_event_id = d.parent_event_id, occurrence_id = d.occurrence_id, 
            mobile_app_id = d.mobile_app_id, message_version_id = d.message_version_id, 
            message_id = d.message_id, journey_id = d.journey_id, 
            identity_id = d.identity_id, event_nm = d.event_nm, 
            event_designed_id = d.event_designed_id, detail_id_hex = d.detail_id_hex, 
            creative_version_id = d.creative_version_id, context_val = d.context_val, 
            channel_user_id = d.channel_user_id, audience_id = d.audience_id, 
            aud_occurrence_id = d.aud_occurrence_id, channel_nm = d.channel_nm, 
            context_type_nm = d.context_type_nm, creative_id = d.creative_id, 
            journey_occurrence_id = d.journey_occurrence_id, response_tracking_cd = d.response_tracking_cd, 
            spot_id = d.spot_id, task_version_id = d.task_version_id
         WHEN NOT MATCHED THEN INSERT (
            properties_map_doc, load_dttm, outbound_system_dttm_tz, 
            outbound_system_dttm, visit_id_hex, task_id, session_id_hex, 
            segment_version_id, segment_id, reserved_2_txt, reserved_1_txt, 
            parent_event_id, occurrence_id, mobile_app_id, message_version_id, 
            message_id, journey_id, identity_id, event_nm, 
            event_designed_id, detail_id_hex, creative_version_id, context_val, 
            channel_user_id, audience_id, aud_occurrence_id, channel_nm, 
            context_type_nm, creative_id, event_id, journey_occurrence_id, 
            response_tracking_cd, spot_id, task_version_id
         ) VALUES (
            d.properties_map_doc, d.load_dttm, d.outbound_system_dttm_tz, 
            d.outbound_system_dttm, d.visit_id_hex, d.task_id, d.session_id_hex, 
            d.segment_version_id, d.segment_id, d.reserved_2_txt, d.reserved_1_txt, 
            d.parent_event_id, d.occurrence_id, d.mobile_app_id, d.message_version_id, 
            d.message_id, d.journey_id, d.identity_id, d.event_nm, 
            d.event_designed_id, d.detail_id_hex, d.creative_version_id, d.context_val, 
            d.channel_user_id, d.audience_id, d.aud_occurrence_id, d.channel_nm, 
            d.context_type_nm, d.creative_id, d.event_id, d.journey_occurrence_id, 
            d.response_tracking_cd, d.spot_id, d.task_version_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : OUTBOUND_SYSTEM_tmp , OUTBOUND_SYSTEM , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: PAGE_DETAILS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..PAGE_DETAILS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..PAGE_DETAILS_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table PAGE_DETAILS_tmp , PAGE_DETAILS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=PAGE_DETAILS , table_keys=%str(EVENT_ID), out_table=work.PAGE_DETAILS );
      DATA work.PAGE_DETAILS_tmp ;
         SET work.PAGE_DETAILS ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : PAGE_DETAILS_tmp , PAGE_DETAILS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..PAGE_DETAILS_tmp as select * from &dbschema..PAGE_DETAILS where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : PAGE_DETAILS_tmp , PAGE_DETAILS , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.PAGE_DETAILS_tmp  base=&tmplib..PAGE_DETAILS_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : PAGE_DETAILS_tmp , PAGE_DETAILS );
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
            detail_dttm = d.detail_dttm, visit_id_hex = d.visit_id_hex, 
            session_id_hex = d.session_id_hex, session_id = d.session_id, 
            protocol_nm = d.protocol_nm, page_desc = d.page_desc, 
            event_source_cd = d.event_source_cd, domain_nm = d.domain_nm, 
            event_key_cd = d.event_key_cd, mobile_app_id = d.mobile_app_id, 
            page_url_txt = d.page_url_txt, url_domain = d.url_domain, 
            window_size_txt = d.window_size_txt, detail_id = d.detail_id, 
            class8_id = d.class8_id, class7_id = d.class7_id, 
            class6_id = d.class6_id, class4_id = d.class4_id, 
            class2_id = d.class2_id, class15_id = d.class15_id, 
            class13_id = d.class13_id, class11_id = d.class11_id, 
            channel_nm = d.channel_nm, class10_id = d.class10_id, 
            class12_id = d.class12_id, class14_id = d.class14_id, 
            class1_id = d.class1_id, class3_id = d.class3_id, 
            class5_id = d.class5_id, class9_id = d.class9_id, 
            detail_id_hex = d.detail_id_hex, event_nm = d.event_nm, 
            identity_id = d.identity_id, referrer_url_txt = d.referrer_url_txt, 
            visit_id = d.visit_id
         WHEN NOT MATCHED THEN INSERT (
            session_dt_tz, session_dt, page_load_sec_cnt, 
            page_complete_sec_cnt, bytes_sent_cnt, detail_dttm_tz, load_dttm, 
            detail_dttm, visit_id_hex, session_id_hex, session_id, 
            protocol_nm, page_desc, event_source_cd, domain_nm, 
            event_key_cd, mobile_app_id, page_url_txt, url_domain, 
            window_size_txt, detail_id, class8_id, class7_id, 
            class6_id, class4_id, class2_id, class15_id, 
            class13_id, class11_id, channel_nm, class10_id, 
            class12_id, class14_id, class1_id, class3_id, 
            class5_id, class9_id, detail_id_hex, event_id, 
            event_nm, identity_id, referrer_url_txt, visit_id
         ) VALUES (
            d.session_dt_tz, d.session_dt, d.page_load_sec_cnt, 
            d.page_complete_sec_cnt, d.bytes_sent_cnt, d.detail_dttm_tz, d.load_dttm, 
            d.detail_dttm, d.visit_id_hex, d.session_id_hex, d.session_id, 
            d.protocol_nm, d.page_desc, d.event_source_cd, d.domain_nm, 
            d.event_key_cd, d.mobile_app_id, d.page_url_txt, d.url_domain, 
            d.window_size_txt, d.detail_id, d.class8_id, d.class7_id, 
            d.class6_id, d.class4_id, d.class2_id, d.class15_id, 
            d.class13_id, d.class11_id, d.channel_nm, d.class10_id, 
            d.class12_id, d.class14_id, d.class1_id, d.class3_id, 
            d.class5_id, d.class9_id, d.detail_id_hex, d.event_id, 
            d.event_nm, d.identity_id, d.referrer_url_txt, d.visit_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : PAGE_DETAILS_tmp , PAGE_DETAILS , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: PAGE_DETAILS_EXT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..PAGE_DETAILS_EXT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..PAGE_DETAILS_EXT_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table PAGE_DETAILS_EXT_tmp , PAGE_DETAILS_EXT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=PAGE_DETAILS_EXT , table_keys=%str(DETAIL_ID,LOAD_DTTM,SESSION_ID), out_table=work.PAGE_DETAILS_EXT );
      DATA work.PAGE_DETAILS_EXT_tmp ;
         SET work.PAGE_DETAILS_EXT ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND DETAIL_ID IS NOT NULL AND LOAD_DTTM IS NOT NULL AND SESSION_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : PAGE_DETAILS_EXT_tmp , PAGE_DETAILS_EXT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..PAGE_DETAILS_EXT_tmp as select * from &dbschema..PAGE_DETAILS_EXT where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : PAGE_DETAILS_EXT_tmp , PAGE_DETAILS_EXT , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.PAGE_DETAILS_EXT_tmp  base=&tmplib..PAGE_DETAILS_EXT_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : PAGE_DETAILS_EXT_tmp , PAGE_DETAILS_EXT );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..PAGE_DETAILS_EXT AS b USING &tmpdbschema..PAGE_DETAILS_EXT_tmp AS d ON (
            b.load_dttm = d.load_dttm AND 
            b.detail_id = d.detail_id AND b.session_id = d.session_id )
         WHEN MATCHED THEN
         UPDATE SET
            active_sec_spent_on_page_cnt = d.active_sec_spent_on_page_cnt, 
            seconds_spent_on_page_cnt = d.seconds_spent_on_page_cnt, session_id_hex = d.session_id_hex, 
            detail_id_hex = d.detail_id_hex
         WHEN NOT MATCHED THEN INSERT (
            active_sec_spent_on_page_cnt, seconds_spent_on_page_cnt, load_dttm, 
            session_id_hex, detail_id_hex, detail_id, session_id
         ) VALUES (
            d.active_sec_spent_on_page_cnt, d.seconds_spent_on_page_cnt, d.load_dttm, 
            d.session_id_hex, d.detail_id_hex, d.detail_id, d.session_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : PAGE_DETAILS_EXT_tmp , PAGE_DETAILS_EXT , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: PAGE_ERRORS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..PAGE_ERRORS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..PAGE_ERRORS_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table PAGE_ERRORS_tmp , PAGE_ERRORS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=PAGE_ERRORS , table_keys=%str(EVENT_ID), out_table=work.PAGE_ERRORS );
      DATA work.PAGE_ERRORS_tmp ;
         SET work.PAGE_ERRORS ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : PAGE_ERRORS_tmp , PAGE_ERRORS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..PAGE_ERRORS_tmp as select * from &dbschema..PAGE_ERRORS where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : PAGE_ERRORS_tmp , PAGE_ERRORS , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.PAGE_ERRORS_tmp  base=&tmplib..PAGE_ERRORS_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : PAGE_ERRORS_tmp , PAGE_ERRORS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..PAGE_ERRORS AS b USING &tmpdbschema..PAGE_ERRORS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            in_page_error_dttm_tz = d.in_page_error_dttm_tz, 
            load_dttm = d.load_dttm, in_page_error_dttm = d.in_page_error_dttm, 
            visit_id_hex = d.visit_id_hex, visit_id = d.visit_id, 
            session_id_hex = d.session_id_hex, in_page_error_txt = d.in_page_error_txt, 
            event_source_cd = d.event_source_cd, detail_id_hex = d.detail_id_hex, 
            identity_id = d.identity_id, session_id = d.session_id, 
            detail_id = d.detail_id, error_location_txt = d.error_location_txt
         WHEN NOT MATCHED THEN INSERT (
            in_page_error_dttm_tz, load_dttm, in_page_error_dttm, 
            visit_id_hex, visit_id, session_id_hex, in_page_error_txt, 
            event_source_cd, event_id, detail_id_hex, identity_id, 
            session_id, detail_id, error_location_txt
         ) VALUES (
            d.in_page_error_dttm_tz, d.load_dttm, d.in_page_error_dttm, 
            d.visit_id_hex, d.visit_id, d.session_id_hex, d.in_page_error_txt, 
            d.event_source_cd, d.event_id, d.detail_id_hex, d.identity_id, 
            d.session_id, d.detail_id, d.error_location_txt  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : PAGE_ERRORS_tmp , PAGE_ERRORS , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: PLANNING_HIERARCHY_DEFN has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..PLANNING_HIERARCHY_DEFN;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..PLANNING_HIERARCHY_DEFN) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate PLANNING_HIERARCHY_DEFN , PLANNING_HIERARCHY_DEFN );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: PLANNING_INFO has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..PLANNING_INFO;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..PLANNING_INFO) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate PLANNING_INFO , PLANNING_INFO );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: PLANNING_INFO_CUSTOM_PROP has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..PLANNING_INFO_CUSTOM_PROP;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..PLANNING_INFO_CUSTOM_PROP) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate PLANNING_INFO_CUSTOM_PROP , PLANNING_INFO_CUSTOM_PROP );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: PRODUCT_VIEWS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..PRODUCT_VIEWS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..PRODUCT_VIEWS_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table PRODUCT_VIEWS_tmp , PRODUCT_VIEWS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=PRODUCT_VIEWS , table_keys=%str(EVENT_ID), out_table=work.PRODUCT_VIEWS );
      DATA work.PRODUCT_VIEWS_tmp ;
         SET work.PRODUCT_VIEWS ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : PRODUCT_VIEWS_tmp , PRODUCT_VIEWS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..PRODUCT_VIEWS_tmp as select * from &dbschema..PRODUCT_VIEWS where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : PRODUCT_VIEWS_tmp , PRODUCT_VIEWS , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.PRODUCT_VIEWS_tmp  base=&tmplib..PRODUCT_VIEWS_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : PRODUCT_VIEWS_tmp , PRODUCT_VIEWS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..PRODUCT_VIEWS AS b USING &tmpdbschema..PRODUCT_VIEWS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            price_val = d.price_val, 
            properties_map_doc = d.properties_map_doc, action_dttm = d.action_dttm, 
            load_dttm = d.load_dttm, action_dttm_tz = d.action_dttm_tz, 
            session_id_hex = d.session_id_hex, product_sku = d.product_sku, 
            product_group_nm = d.product_group_nm, event_source_cd = d.event_source_cd, 
            event_designed_id = d.event_designed_id, channel_nm = d.channel_nm, 
            availability_message_txt = d.availability_message_txt, currency_cd = d.currency_cd, 
            detail_id = d.detail_id, event_key_cd = d.event_key_cd, 
            identity_id = d.identity_id, mobile_app_id = d.mobile_app_id, 
            product_id = d.product_id, saving_message_txt = d.saving_message_txt, 
            visit_id = d.visit_id, detail_id_hex = d.detail_id_hex, 
            event_nm = d.event_nm, product_nm = d.product_nm, 
            session_id = d.session_id, shipping_message_txt = d.shipping_message_txt, 
            visit_id_hex = d.visit_id_hex
         WHEN NOT MATCHED THEN INSERT (
            price_val, properties_map_doc, action_dttm, 
            load_dttm, action_dttm_tz, session_id_hex, product_sku, 
            product_group_nm, event_source_cd, event_designed_id, channel_nm, 
            availability_message_txt, currency_cd, detail_id, event_key_cd, 
            identity_id, mobile_app_id, product_id, saving_message_txt, 
            visit_id, detail_id_hex, event_id, event_nm, 
            product_nm, session_id, shipping_message_txt, visit_id_hex
         ) VALUES (
            d.price_val, d.properties_map_doc, d.action_dttm, 
            d.load_dttm, d.action_dttm_tz, d.session_id_hex, d.product_sku, 
            d.product_group_nm, d.event_source_cd, d.event_designed_id, d.channel_nm, 
            d.availability_message_txt, d.currency_cd, d.detail_id, d.event_key_cd, 
            d.identity_id, d.mobile_app_id, d.product_id, d.saving_message_txt, 
            d.visit_id, d.detail_id_hex, d.event_id, d.event_nm, 
            d.product_nm, d.session_id, d.shipping_message_txt, d.visit_id_hex  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : PRODUCT_VIEWS_tmp , PRODUCT_VIEWS , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: PROMOTION_DISPLAYED has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..PROMOTION_DISPLAYED;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..PROMOTION_DISPLAYED_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table PROMOTION_DISPLAYED_tmp , PROMOTION_DISPLAYED_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=PROMOTION_DISPLAYED , table_keys=%str(EVENT_ID), out_table=work.PROMOTION_DISPLAYED );
      DATA work.PROMOTION_DISPLAYED_tmp ;
         SET work.PROMOTION_DISPLAYED ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : PROMOTION_DISPLAYED_tmp , PROMOTION_DISPLAYED_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..PROMOTION_DISPLAYED_tmp as select * from &dbschema..PROMOTION_DISPLAYED where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : PROMOTION_DISPLAYED_tmp , PROMOTION_DISPLAYED , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.PROMOTION_DISPLAYED_tmp  base=&tmplib..PROMOTION_DISPLAYED_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : PROMOTION_DISPLAYED_tmp , PROMOTION_DISPLAYED );
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
            display_dttm = d.display_dttm, load_dttm = d.load_dttm, 
            display_dttm_tz = d.display_dttm_tz, visit_id_hex = d.visit_id_hex, 
            session_id = d.session_id, promotion_placement_nm = d.promotion_placement_nm, 
            mobile_app_id = d.mobile_app_id, event_nm = d.event_nm, 
            event_key_cd = d.event_key_cd, event_designed_id = d.event_designed_id, 
            channel_nm = d.channel_nm, detail_id = d.detail_id, 
            event_source_cd = d.event_source_cd, promotion_creative_nm = d.promotion_creative_nm, 
            promotion_tracking_cd = d.promotion_tracking_cd, session_id_hex = d.session_id_hex, 
            detail_id_hex = d.detail_id_hex, identity_id = d.identity_id, 
            promotion_nm = d.promotion_nm, promotion_type_nm = d.promotion_type_nm, 
            visit_id = d.visit_id
         WHEN NOT MATCHED THEN INSERT (
            derived_display_flg, promotion_number, properties_map_doc, 
            display_dttm, load_dttm, display_dttm_tz, visit_id_hex, 
            session_id, promotion_placement_nm, mobile_app_id, event_nm, 
            event_key_cd, event_designed_id, channel_nm, detail_id, 
            event_id, event_source_cd, promotion_creative_nm, promotion_tracking_cd, 
            session_id_hex, detail_id_hex, identity_id, promotion_nm, 
            promotion_type_nm, visit_id
         ) VALUES (
            d.derived_display_flg, d.promotion_number, d.properties_map_doc, 
            d.display_dttm, d.load_dttm, d.display_dttm_tz, d.visit_id_hex, 
            d.session_id, d.promotion_placement_nm, d.mobile_app_id, d.event_nm, 
            d.event_key_cd, d.event_designed_id, d.channel_nm, d.detail_id, 
            d.event_id, d.event_source_cd, d.promotion_creative_nm, d.promotion_tracking_cd, 
            d.session_id_hex, d.detail_id_hex, d.identity_id, d.promotion_nm, 
            d.promotion_type_nm, d.visit_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : PROMOTION_DISPLAYED_tmp , PROMOTION_DISPLAYED , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: PROMOTION_USED has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..PROMOTION_USED;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..PROMOTION_USED_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table PROMOTION_USED_tmp , PROMOTION_USED_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=PROMOTION_USED , table_keys=%str(EVENT_ID), out_table=work.PROMOTION_USED );
      DATA work.PROMOTION_USED_tmp ;
         SET work.PROMOTION_USED ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : PROMOTION_USED_tmp , PROMOTION_USED_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..PROMOTION_USED_tmp as select * from &dbschema..PROMOTION_USED where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : PROMOTION_USED_tmp , PROMOTION_USED , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.PROMOTION_USED_tmp  base=&tmplib..PROMOTION_USED_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : PROMOTION_USED_tmp , PROMOTION_USED );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..PROMOTION_USED AS b USING &tmpdbschema..PROMOTION_USED_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            promotion_number = d.promotion_number, 
            properties_map_doc = d.properties_map_doc, load_dttm = d.load_dttm, 
            click_dttm_tz = d.click_dttm_tz, click_dttm = d.click_dttm, 
            visit_id_hex = d.visit_id_hex, session_id = d.session_id, 
            promotion_placement_nm = d.promotion_placement_nm, mobile_app_id = d.mobile_app_id, 
            event_key_cd = d.event_key_cd, detail_id = d.detail_id, 
            channel_nm = d.channel_nm, event_designed_id = d.event_designed_id, 
            event_source_cd = d.event_source_cd, promotion_creative_nm = d.promotion_creative_nm, 
            promotion_tracking_cd = d.promotion_tracking_cd, session_id_hex = d.session_id_hex, 
            detail_id_hex = d.detail_id_hex, event_nm = d.event_nm, 
            identity_id = d.identity_id, promotion_nm = d.promotion_nm, 
            promotion_type_nm = d.promotion_type_nm, visit_id = d.visit_id
         WHEN NOT MATCHED THEN INSERT (
            promotion_number, properties_map_doc, load_dttm, 
            click_dttm_tz, click_dttm, visit_id_hex, session_id, 
            promotion_placement_nm, mobile_app_id, event_key_cd, detail_id, 
            channel_nm, event_designed_id, event_source_cd, promotion_creative_nm, 
            promotion_tracking_cd, session_id_hex, detail_id_hex, event_id, 
            event_nm, identity_id, promotion_nm, promotion_type_nm, 
            visit_id
         ) VALUES (
            d.promotion_number, d.properties_map_doc, d.load_dttm, 
            d.click_dttm_tz, d.click_dttm, d.visit_id_hex, d.session_id, 
            d.promotion_placement_nm, d.mobile_app_id, d.event_key_cd, d.detail_id, 
            d.channel_nm, d.event_designed_id, d.event_source_cd, d.promotion_creative_nm, 
            d.promotion_tracking_cd, d.session_id_hex, d.detail_id_hex, d.event_id, 
            d.event_nm, d.identity_id, d.promotion_nm, d.promotion_type_nm, 
            d.visit_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : PROMOTION_USED_tmp , PROMOTION_USED , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: RESPONSE_HISTORY has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..RESPONSE_HISTORY;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..RESPONSE_HISTORY_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table RESPONSE_HISTORY_tmp , RESPONSE_HISTORY_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=RESPONSE_HISTORY , table_keys=%str(RESPONSE_ID), out_table=work.RESPONSE_HISTORY );
      DATA work.RESPONSE_HISTORY_tmp ;
         SET work.RESPONSE_HISTORY ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND RESPONSE_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : RESPONSE_HISTORY_tmp , RESPONSE_HISTORY_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..RESPONSE_HISTORY_tmp as select * from &dbschema..RESPONSE_HISTORY where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : RESPONSE_HISTORY_tmp , RESPONSE_HISTORY , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.RESPONSE_HISTORY_tmp  base=&tmplib..RESPONSE_HISTORY_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : RESPONSE_HISTORY_tmp , RESPONSE_HISTORY );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..RESPONSE_HISTORY AS b USING &tmpdbschema..RESPONSE_HISTORY_tmp AS d ON (
            b.response_id = d.response_id )
         WHEN MATCHED THEN
         UPDATE SET
            properties_map_doc = d.properties_map_doc, 
            response_dttm = d.response_dttm, response_dttm_tz = d.response_dttm_tz, 
            load_dttm = d.load_dttm, visit_id_hex = d.visit_id_hex, 
            task_version_id = d.task_version_id, response_nm = d.response_nm, 
            response_channel_nm = d.response_channel_nm, message_id = d.message_id, 
            identity_id = d.identity_id, context_val = d.context_val, 
            aud_occurrence_id = d.aud_occurrence_id, audience_id = d.audience_id, 
            detail_id_hex = d.detail_id_hex, journey_occurrence_id = d.journey_occurrence_id, 
            parent_event_designed_id = d.parent_event_designed_id, session_id_hex = d.session_id_hex, 
            context_type_nm = d.context_type_nm, creative_id = d.creative_id, 
            event_designed_id = d.event_designed_id, journey_id = d.journey_id, 
            occurrence_id = d.occurrence_id, response_tracking_cd = d.response_tracking_cd, 
            task_id = d.task_id
         WHEN NOT MATCHED THEN INSERT (
            properties_map_doc, response_dttm, response_dttm_tz, 
            load_dttm, visit_id_hex, task_version_id, response_nm, 
            response_channel_nm, message_id, identity_id, context_val, 
            aud_occurrence_id, audience_id, detail_id_hex, journey_occurrence_id, 
            parent_event_designed_id, response_id, session_id_hex, context_type_nm, 
            creative_id, event_designed_id, journey_id, occurrence_id, 
            response_tracking_cd, task_id
         ) VALUES (
            d.properties_map_doc, d.response_dttm, d.response_dttm_tz, 
            d.load_dttm, d.visit_id_hex, d.task_version_id, d.response_nm, 
            d.response_channel_nm, d.message_id, d.identity_id, d.context_val, 
            d.aud_occurrence_id, d.audience_id, d.detail_id_hex, d.journey_occurrence_id, 
            d.parent_event_designed_id, d.response_id, d.session_id_hex, d.context_type_nm, 
            d.creative_id, d.event_designed_id, d.journey_id, d.occurrence_id, 
            d.response_tracking_cd, d.task_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : RESPONSE_HISTORY_tmp , RESPONSE_HISTORY , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: SEARCH_RESULTS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..SEARCH_RESULTS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SEARCH_RESULTS_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table SEARCH_RESULTS_tmp , SEARCH_RESULTS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=SEARCH_RESULTS , table_keys=%str(EVENT_ID), out_table=work.SEARCH_RESULTS );
      DATA work.SEARCH_RESULTS_tmp ;
         SET work.SEARCH_RESULTS ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : SEARCH_RESULTS_tmp , SEARCH_RESULTS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..SEARCH_RESULTS_tmp as select * from &dbschema..SEARCH_RESULTS where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : SEARCH_RESULTS_tmp , SEARCH_RESULTS , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.SEARCH_RESULTS_tmp  base=&tmplib..SEARCH_RESULTS_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : SEARCH_RESULTS_tmp , SEARCH_RESULTS );
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
            search_results_dttm = d.search_results_dttm, search_results_dttm_tz = d.search_results_dttm_tz, 
            load_dttm = d.load_dttm, srch_phrase = d.srch_phrase, 
            session_id_hex = d.session_id_hex, session_id = d.session_id, 
            search_results_sk = d.search_results_sk, mobile_app_id = d.mobile_app_id, 
            event_source_cd = d.event_source_cd, event_nm = d.event_nm, 
            detail_id_hex = d.detail_id_hex, channel_nm = d.channel_nm, 
            identity_id = d.identity_id, search_nm = d.search_nm, 
            srch_field_id = d.srch_field_id, visit_id_hex = d.visit_id_hex, 
            detail_id = d.detail_id, event_designed_id = d.event_designed_id, 
            event_key_cd = d.event_key_cd, srch_field_name = d.srch_field_name, 
            visit_id = d.visit_id
         WHEN NOT MATCHED THEN INSERT (
            results_displayed_flg, search_results_displayed, properties_map_doc, 
            search_results_dttm, search_results_dttm_tz, load_dttm, srch_phrase, 
            session_id_hex, session_id, search_results_sk, mobile_app_id, 
            event_source_cd, event_nm, detail_id_hex, channel_nm, 
            event_id, identity_id, search_nm, srch_field_id, 
            visit_id_hex, detail_id, event_designed_id, event_key_cd, 
            srch_field_name, visit_id
         ) VALUES (
            d.results_displayed_flg, d.search_results_displayed, d.properties_map_doc, 
            d.search_results_dttm, d.search_results_dttm_tz, d.load_dttm, d.srch_phrase, 
            d.session_id_hex, d.session_id, d.search_results_sk, d.mobile_app_id, 
            d.event_source_cd, d.event_nm, d.detail_id_hex, d.channel_nm, 
            d.event_id, d.identity_id, d.search_nm, d.srch_field_id, 
            d.visit_id_hex, d.detail_id, d.event_designed_id, d.event_key_cd, 
            d.srch_field_name, d.visit_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : SEARCH_RESULTS_tmp , SEARCH_RESULTS , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: SEARCH_RESULTS_EXT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..SEARCH_RESULTS_EXT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SEARCH_RESULTS_EXT_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table SEARCH_RESULTS_EXT_tmp , SEARCH_RESULTS_EXT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=SEARCH_RESULTS_EXT , table_keys=%str(EVENT_ID), out_table=work.SEARCH_RESULTS_EXT );
      DATA work.SEARCH_RESULTS_EXT_tmp ;
         SET work.SEARCH_RESULTS_EXT ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : SEARCH_RESULTS_EXT_tmp , SEARCH_RESULTS_EXT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..SEARCH_RESULTS_EXT_tmp as select * from &dbschema..SEARCH_RESULTS_EXT where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : SEARCH_RESULTS_EXT_tmp , SEARCH_RESULTS_EXT , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.SEARCH_RESULTS_EXT_tmp  base=&tmplib..SEARCH_RESULTS_EXT_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : SEARCH_RESULTS_EXT_tmp , SEARCH_RESULTS_EXT );
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
         WHEN NOT MATCHED THEN INSERT (
            search_results_displayed, load_dttm, event_id, 
            search_results_sk, event_designed_id
         ) VALUES (
            d.search_results_displayed, d.load_dttm, d.event_id, 
            d.search_results_sk, d.event_designed_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : SEARCH_RESULTS_EXT_tmp , SEARCH_RESULTS_EXT , err_macro=SYSDBRC);
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
%if %sysfunc(exist(&udmmart..SEGMENT_MEMBERSHIP)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid=%sysfunc(open(&udmmart..SEGMENT_MEMBERSHIP));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if &nrows = 0 %then %do;
      %put NOTE: SEGMENT_MEMBERSHIP has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..SEGMENT_MEMBERSHIP;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SEGMENT_MEMBERSHIP_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table SEGMENT_MEMBERSHIP_tmp , SEGMENT_MEMBERSHIP_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=SEGMENT_MEMBERSHIP , table_keys=%str(OCCURRENCE_ID,SEGMENT_VERSION_ID,USER_IDENTIFIER_VAL), out_table=work.SEGMENT_MEMBERSHIP );
      DATA work.SEGMENT_MEMBERSHIP_tmp ;
         SET work.SEGMENT_MEMBERSHIP ;
         WHERE 1=1 AND OCCURRENCE_ID IS NOT NULL AND SEGMENT_VERSION_ID IS NOT NULL AND USER_IDENTIFIER_VAL IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : SEGMENT_MEMBERSHIP_tmp , SEGMENT_MEMBERSHIP_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..SEGMENT_MEMBERSHIP_tmp as select * from &dbschema..SEGMENT_MEMBERSHIP where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : SEGMENT_MEMBERSHIP_tmp , SEGMENT_MEMBERSHIP , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.SEGMENT_MEMBERSHIP_tmp  base=&tmplib..SEGMENT_MEMBERSHIP_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : SEGMENT_MEMBERSHIP_tmp , SEGMENT_MEMBERSHIP );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..SEGMENT_MEMBERSHIP AS b USING &tmpdbschema..SEGMENT_MEMBERSHIP_tmp AS d ON (
            b.occurrence_id = d.occurrence_id AND 
            b.segment_version_id = d.segment_version_id AND b.user_identifier_val = d.user_identifier_val )
         WHEN MATCHED THEN
         UPDATE SET
            processed_dttm = d.processed_dttm, 
            processed_dttm_tz = d.processed_dttm_tz, segment_id = d.segment_id, 
            context_val = d.context_val, context_type_nm = d.context_type_nm
         WHEN NOT MATCHED THEN INSERT (
            processed_dttm, processed_dttm_tz, segment_id, 
            context_val, context_type_nm, occurrence_id, segment_version_id, 
            user_identifier_val
         ) VALUES (
            d.processed_dttm, d.processed_dttm_tz, d.segment_id, 
            d.context_val, d.context_type_nm, d.occurrence_id, d.segment_version_id, 
            d.user_identifier_val  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : SEGMENT_MEMBERSHIP_tmp , SEGMENT_MEMBERSHIP , err_macro=SYSDBRC);
   %end;
   %if %sysfunc(exist(&tmplib..SEGMENT_MEMBERSHIP_tmp )) %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SEGMENT_MEMBERSHIP_tmp ;
      QUIT;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..SEGMENT_MEMBERSHIP;
         DROP TABLE work.SEGMENT_MEMBERSHIP;
      QUIT;
   %end;
   %else %do;
      %put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;
   %end;
   %put %sysfunc(datetime(),E8601DT25.) --- Processing table SEGMENT_MEMBERSHIP;
   %put------------------------------------------------------------------;
%end;
%if %sysfunc(exist(&udmmart..SESSION_DETAILS)) %then %do;
   %let errFlag=0;
   %let nrows=0;
   %let dsid=%sysfunc(open(&udmmart..SESSION_DETAILS));
   %let nrows=%sysfunc(attrn(&dsid,nlobs));
   %let dsid=%sysfunc(close(&dsid));
   %if &nrows = 0 %then %do;
      %put NOTE: SESSION_DETAILS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..SESSION_DETAILS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SESSION_DETAILS_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table SESSION_DETAILS_tmp , SESSION_DETAILS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=SESSION_DETAILS , table_keys=%str(EVENT_ID), out_table=work.SESSION_DETAILS );
      DATA work.SESSION_DETAILS_tmp ;
         SET work.SESSION_DETAILS ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : SESSION_DETAILS_tmp , SESSION_DETAILS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..SESSION_DETAILS_tmp as select * from &dbschema..SESSION_DETAILS where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : SESSION_DETAILS_tmp , SESSION_DETAILS , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.SESSION_DETAILS_tmp  base=&tmplib..SESSION_DETAILS_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : SESSION_DETAILS_tmp , SESSION_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..SESSION_DETAILS AS b USING &tmpdbschema..SESSION_DETAILS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            java_script_enabled_flg = d.java_script_enabled_flg, 
            flash_enabled_flg = d.flash_enabled_flg, java_enabled_flg = d.java_enabled_flg, 
            is_portable_flag = d.is_portable_flag, cookies_enabled_flg = d.cookies_enabled_flg, 
            session_dt = d.session_dt, session_dt_tz = d.session_dt_tz, 
            longitude = d.longitude, latitude = d.latitude, 
            metro_cd = d.metro_cd, session_timeout = d.session_timeout, 
            screen_color_depth_no = d.screen_color_depth_no, client_session_start_dttm_tz = d.client_session_start_dttm_tz, 
            client_session_start_dttm = d.client_session_start_dttm, load_dttm = d.load_dttm, 
            session_start_dttm = d.session_start_dttm, session_start_dttm_tz = d.session_start_dttm_tz, 
            visitor_id = d.visitor_id, state_region_cd = d.state_region_cd, 
            session_id = d.session_id, screen_size_txt = d.screen_size_txt, 
            profile_nm4 = d.profile_nm4, profile_nm1 = d.profile_nm1, 
            previous_session_id_hex = d.previous_session_id_hex, platform_type_nm = d.platform_type_nm, 
            new_visitor_flg = d.new_visitor_flg, manufacturer = d.manufacturer, 
            java_version_no = d.java_version_no, ip_address = d.ip_address, 
            eventsource_cd = d.eventsource_cd, device_language = d.device_language, 
            country_cd = d.country_cd, carrier_name = d.carrier_name, 
            app_id = d.app_id, browser_nm = d.browser_nm, 
            city_nm = d.city_nm, device_nm = d.device_nm, 
            device_type_nm = d.device_type_nm, flash_version_no = d.flash_version_no, 
            mobile_country_code = d.mobile_country_code, parent_event_id = d.parent_event_id, 
            postal_cd = d.postal_cd, profile_nm2 = d.profile_nm2, 
            region_nm = d.region_nm, user_agent_nm = d.user_agent_nm, 
            app_version = d.app_version, browser_version_no = d.browser_version_no, 
            channel_nm = d.channel_nm, country_nm = d.country_nm, 
            identity_id = d.identity_id, network_code = d.network_code, 
            organization_nm = d.organization_nm, platform_desc = d.platform_desc, 
            platform_version = d.platform_version, previous_session_id = d.previous_session_id, 
            profile_nm3 = d.profile_nm3, profile_nm5 = d.profile_nm5, 
            sdk_version = d.sdk_version, session_id_hex = d.session_id_hex, 
            user_language_cd = d.user_language_cd
         WHEN NOT MATCHED THEN INSERT (
            java_script_enabled_flg, flash_enabled_flg, java_enabled_flg, 
            is_portable_flag, cookies_enabled_flg, session_dt, session_dt_tz, 
            longitude, latitude, metro_cd, session_timeout, 
            screen_color_depth_no, client_session_start_dttm_tz, client_session_start_dttm, load_dttm, 
            session_start_dttm, session_start_dttm_tz, visitor_id, state_region_cd, 
            session_id, screen_size_txt, profile_nm4, profile_nm1, 
            previous_session_id_hex, platform_type_nm, new_visitor_flg, manufacturer, 
            java_version_no, ip_address, eventsource_cd, device_language, 
            country_cd, carrier_name, app_id, browser_nm, 
            city_nm, device_nm, device_type_nm, flash_version_no, 
            mobile_country_code, parent_event_id, postal_cd, profile_nm2, 
            region_nm, user_agent_nm, app_version, browser_version_no, 
            channel_nm, country_nm, event_id, identity_id, 
            network_code, organization_nm, platform_desc, platform_version, 
            previous_session_id, profile_nm3, profile_nm5, sdk_version, 
            session_id_hex, user_language_cd
         ) VALUES (
            d.java_script_enabled_flg, d.flash_enabled_flg, d.java_enabled_flg, 
            d.is_portable_flag, d.cookies_enabled_flg, d.session_dt, d.session_dt_tz, 
            d.longitude, d.latitude, d.metro_cd, d.session_timeout, 
            d.screen_color_depth_no, d.client_session_start_dttm_tz, d.client_session_start_dttm, d.load_dttm, 
            d.session_start_dttm, d.session_start_dttm_tz, d.visitor_id, d.state_region_cd, 
            d.session_id, d.screen_size_txt, d.profile_nm4, d.profile_nm1, 
            d.previous_session_id_hex, d.platform_type_nm, d.new_visitor_flg, d.manufacturer, 
            d.java_version_no, d.ip_address, d.eventsource_cd, d.device_language, 
            d.country_cd, d.carrier_name, d.app_id, d.browser_nm, 
            d.city_nm, d.device_nm, d.device_type_nm, d.flash_version_no, 
            d.mobile_country_code, d.parent_event_id, d.postal_cd, d.profile_nm2, 
            d.region_nm, d.user_agent_nm, d.app_version, d.browser_version_no, 
            d.channel_nm, d.country_nm, d.event_id, d.identity_id, 
            d.network_code, d.organization_nm, d.platform_desc, d.platform_version, 
            d.previous_session_id, d.profile_nm3, d.profile_nm5, d.sdk_version, 
            d.session_id_hex, d.user_language_cd  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : SESSION_DETAILS_tmp , SESSION_DETAILS , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: SESSION_DETAILS_EXT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..SESSION_DETAILS_EXT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SESSION_DETAILS_EXT_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table SESSION_DETAILS_EXT_tmp , SESSION_DETAILS_EXT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=SESSION_DETAILS_EXT , table_keys=%str(LOAD_DTTM,SESSION_ID), out_table=work.SESSION_DETAILS_EXT );
      DATA work.SESSION_DETAILS_EXT_tmp ;
         SET work.SESSION_DETAILS_EXT ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND LOAD_DTTM IS NOT NULL AND SESSION_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : SESSION_DETAILS_EXT_tmp , SESSION_DETAILS_EXT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..SESSION_DETAILS_EXT_tmp as select * from &dbschema..SESSION_DETAILS_EXT where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : SESSION_DETAILS_EXT_tmp , SESSION_DETAILS_EXT , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.SESSION_DETAILS_EXT_tmp  base=&tmplib..SESSION_DETAILS_EXT_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : SESSION_DETAILS_EXT_tmp , SESSION_DETAILS_EXT );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..SESSION_DETAILS_EXT AS b USING &tmpdbschema..SESSION_DETAILS_EXT_tmp AS d ON (
            b.load_dttm = d.load_dttm AND 
            b.session_id = d.session_id )
         WHEN MATCHED THEN
         UPDATE SET
            active_sec_spent_in_sessn_cnt = d.active_sec_spent_in_sessn_cnt, 
            seconds_spent_in_session_cnt = d.seconds_spent_in_session_cnt, last_session_activity_dttm = d.last_session_activity_dttm, 
            session_expiration_dttm = d.session_expiration_dttm, session_expiration_dttm_tz = d.session_expiration_dttm_tz, 
            last_session_activity_dttm_tz = d.last_session_activity_dttm_tz, session_id_hex = d.session_id_hex
         WHEN NOT MATCHED THEN INSERT (
            active_sec_spent_in_sessn_cnt, seconds_spent_in_session_cnt, last_session_activity_dttm, 
            session_expiration_dttm, load_dttm, session_expiration_dttm_tz, last_session_activity_dttm_tz, 
            session_id, session_id_hex
         ) VALUES (
            d.active_sec_spent_in_sessn_cnt, d.seconds_spent_in_session_cnt, d.last_session_activity_dttm, 
            d.session_expiration_dttm, d.load_dttm, d.session_expiration_dttm_tz, d.last_session_activity_dttm_tz, 
            d.session_id, d.session_id_hex  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : SESSION_DETAILS_EXT_tmp , SESSION_DETAILS_EXT , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: SMS_MESSAGE_CLICKED has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..SMS_MESSAGE_CLICKED;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SMS_MESSAGE_CLICKED_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table SMS_MESSAGE_CLICKED_tmp , SMS_MESSAGE_CLICKED_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=SMS_MESSAGE_CLICKED , table_keys=%str(EVENT_ID), out_table=work.SMS_MESSAGE_CLICKED );
      DATA work.SMS_MESSAGE_CLICKED_tmp ;
         SET work.SMS_MESSAGE_CLICKED ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : SMS_MESSAGE_CLICKED_tmp , SMS_MESSAGE_CLICKED_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..SMS_MESSAGE_CLICKED_tmp as select * from &dbschema..SMS_MESSAGE_CLICKED where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : SMS_MESSAGE_CLICKED_tmp , SMS_MESSAGE_CLICKED , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.SMS_MESSAGE_CLICKED_tmp  base=&tmplib..SMS_MESSAGE_CLICKED_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : SMS_MESSAGE_CLICKED_tmp , SMS_MESSAGE_CLICKED );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..SMS_MESSAGE_CLICKED AS b USING &tmpdbschema..SMS_MESSAGE_CLICKED_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            sms_click_dttm_tz = d.sms_click_dttm_tz, 
            load_dttm = d.load_dttm, sms_click_dttm = d.sms_click_dttm, 
            task_version_id = d.task_version_id, task_id = d.task_id, 
            sms_message_id = d.sms_message_id, occurrence_id = d.occurrence_id, 
            journey_id = d.journey_id, identity_id = d.identity_id, 
            creative_version_id = d.creative_version_id, context_type_nm = d.context_type_nm, 
            aud_occurrence_id = d.aud_occurrence_id, country_cd = d.country_cd, 
            journey_occurrence_id = d.journey_occurrence_id, response_tracking_cd = d.response_tracking_cd, 
            sender_id = d.sender_id, audience_id = d.audience_id, 
            context_val = d.context_val, creative_id = d.creative_id, 
            event_designed_id = d.event_designed_id, event_nm = d.event_nm
         WHEN NOT MATCHED THEN INSERT (
            sms_click_dttm_tz, load_dttm, sms_click_dttm, 
            task_version_id, task_id, sms_message_id, occurrence_id, 
            journey_id, identity_id, creative_version_id, context_type_nm, 
            aud_occurrence_id, country_cd, event_id, journey_occurrence_id, 
            response_tracking_cd, sender_id, audience_id, context_val, 
            creative_id, event_designed_id, event_nm
         ) VALUES (
            d.sms_click_dttm_tz, d.load_dttm, d.sms_click_dttm, 
            d.task_version_id, d.task_id, d.sms_message_id, d.occurrence_id, 
            d.journey_id, d.identity_id, d.creative_version_id, d.context_type_nm, 
            d.aud_occurrence_id, d.country_cd, d.event_id, d.journey_occurrence_id, 
            d.response_tracking_cd, d.sender_id, d.audience_id, d.context_val, 
            d.creative_id, d.event_designed_id, d.event_nm  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : SMS_MESSAGE_CLICKED_tmp , SMS_MESSAGE_CLICKED , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: SMS_MESSAGE_DELIVERED has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..SMS_MESSAGE_DELIVERED;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SMS_MESSAGE_DELIVERED_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table SMS_MESSAGE_DELIVERED_tmp , SMS_MESSAGE_DELIVERED_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=SMS_MESSAGE_DELIVERED , table_keys=%str(EVENT_ID), out_table=work.SMS_MESSAGE_DELIVERED );
      DATA work.SMS_MESSAGE_DELIVERED_tmp ;
         SET work.SMS_MESSAGE_DELIVERED ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : SMS_MESSAGE_DELIVERED_tmp , SMS_MESSAGE_DELIVERED_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..SMS_MESSAGE_DELIVERED_tmp as select * from &dbschema..SMS_MESSAGE_DELIVERED where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : SMS_MESSAGE_DELIVERED_tmp , SMS_MESSAGE_DELIVERED , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.SMS_MESSAGE_DELIVERED_tmp  base=&tmplib..SMS_MESSAGE_DELIVERED_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : SMS_MESSAGE_DELIVERED_tmp , SMS_MESSAGE_DELIVERED );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..SMS_MESSAGE_DELIVERED AS b USING &tmpdbschema..SMS_MESSAGE_DELIVERED_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            sms_delivered_dttm = d.sms_delivered_dttm, 
            load_dttm = d.load_dttm, sms_delivered_dttm_tz = d.sms_delivered_dttm_tz, 
            task_id = d.task_id, sender_id = d.sender_id, 
            response_tracking_cd = d.response_tracking_cd, occurrence_id = d.occurrence_id, 
            journey_occurrence_id = d.journey_occurrence_id, country_cd = d.country_cd, 
            aud_occurrence_id = d.aud_occurrence_id, context_type_nm = d.context_type_nm, 
            creative_id = d.creative_id, creative_version_id = d.creative_version_id, 
            identity_id = d.identity_id, sms_message_id = d.sms_message_id, 
            task_version_id = d.task_version_id, audience_id = d.audience_id, 
            context_val = d.context_val, event_designed_id = d.event_designed_id, 
            event_nm = d.event_nm, journey_id = d.journey_id
         WHEN NOT MATCHED THEN INSERT (
            sms_delivered_dttm, load_dttm, sms_delivered_dttm_tz, 
            task_id, sender_id, response_tracking_cd, occurrence_id, 
            journey_occurrence_id, event_id, country_cd, aud_occurrence_id, 
            context_type_nm, creative_id, creative_version_id, identity_id, 
            sms_message_id, task_version_id, audience_id, context_val, 
            event_designed_id, event_nm, journey_id
         ) VALUES (
            d.sms_delivered_dttm, d.load_dttm, d.sms_delivered_dttm_tz, 
            d.task_id, d.sender_id, d.response_tracking_cd, d.occurrence_id, 
            d.journey_occurrence_id, d.event_id, d.country_cd, d.aud_occurrence_id, 
            d.context_type_nm, d.creative_id, d.creative_version_id, d.identity_id, 
            d.sms_message_id, d.task_version_id, d.audience_id, d.context_val, 
            d.event_designed_id, d.event_nm, d.journey_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : SMS_MESSAGE_DELIVERED_tmp , SMS_MESSAGE_DELIVERED , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: SMS_MESSAGE_FAILED has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..SMS_MESSAGE_FAILED;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SMS_MESSAGE_FAILED_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table SMS_MESSAGE_FAILED_tmp , SMS_MESSAGE_FAILED_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=SMS_MESSAGE_FAILED , table_keys=%str(EVENT_ID), out_table=work.SMS_MESSAGE_FAILED );
      DATA work.SMS_MESSAGE_FAILED_tmp ;
         SET work.SMS_MESSAGE_FAILED ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : SMS_MESSAGE_FAILED_tmp , SMS_MESSAGE_FAILED_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..SMS_MESSAGE_FAILED_tmp as select * from &dbschema..SMS_MESSAGE_FAILED where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : SMS_MESSAGE_FAILED_tmp , SMS_MESSAGE_FAILED , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.SMS_MESSAGE_FAILED_tmp  base=&tmplib..SMS_MESSAGE_FAILED_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : SMS_MESSAGE_FAILED_tmp , SMS_MESSAGE_FAILED );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..SMS_MESSAGE_FAILED AS b USING &tmpdbschema..SMS_MESSAGE_FAILED_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            sms_failed_dttm = d.sms_failed_dttm, 
            sms_failed_dttm_tz = d.sms_failed_dttm_tz, load_dttm = d.load_dttm, 
            task_id = d.task_id, sender_id = d.sender_id, 
            reason_cd = d.reason_cd, occurrence_id = d.occurrence_id, 
            identity_id = d.identity_id, event_designed_id = d.event_designed_id, 
            creative_version_id = d.creative_version_id, context_type_nm = d.context_type_nm, 
            aud_occurrence_id = d.aud_occurrence_id, country_cd = d.country_cd, 
            journey_occurrence_id = d.journey_occurrence_id, reason_description_txt = d.reason_description_txt, 
            sms_message_id = d.sms_message_id, task_version_id = d.task_version_id, 
            audience_id = d.audience_id, context_val = d.context_val, 
            creative_id = d.creative_id, event_nm = d.event_nm, 
            journey_id = d.journey_id, response_tracking_cd = d.response_tracking_cd
         WHEN NOT MATCHED THEN INSERT (
            sms_failed_dttm, sms_failed_dttm_tz, load_dttm, 
            task_id, sender_id, reason_cd, occurrence_id, 
            identity_id, event_designed_id, creative_version_id, context_type_nm, 
            aud_occurrence_id, country_cd, event_id, journey_occurrence_id, 
            reason_description_txt, sms_message_id, task_version_id, audience_id, 
            context_val, creative_id, event_nm, journey_id, 
            response_tracking_cd
         ) VALUES (
            d.sms_failed_dttm, d.sms_failed_dttm_tz, d.load_dttm, 
            d.task_id, d.sender_id, d.reason_cd, d.occurrence_id, 
            d.identity_id, d.event_designed_id, d.creative_version_id, d.context_type_nm, 
            d.aud_occurrence_id, d.country_cd, d.event_id, d.journey_occurrence_id, 
            d.reason_description_txt, d.sms_message_id, d.task_version_id, d.audience_id, 
            d.context_val, d.creative_id, d.event_nm, d.journey_id, 
            d.response_tracking_cd  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : SMS_MESSAGE_FAILED_tmp , SMS_MESSAGE_FAILED , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: SMS_MESSAGE_REPLY has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..SMS_MESSAGE_REPLY;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SMS_MESSAGE_REPLY_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table SMS_MESSAGE_REPLY_tmp , SMS_MESSAGE_REPLY_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=SMS_MESSAGE_REPLY , table_keys=%str(EVENT_ID), out_table=work.SMS_MESSAGE_REPLY );
      DATA work.SMS_MESSAGE_REPLY_tmp ;
         SET work.SMS_MESSAGE_REPLY ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : SMS_MESSAGE_REPLY_tmp , SMS_MESSAGE_REPLY_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..SMS_MESSAGE_REPLY_tmp as select * from &dbschema..SMS_MESSAGE_REPLY where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : SMS_MESSAGE_REPLY_tmp , SMS_MESSAGE_REPLY , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.SMS_MESSAGE_REPLY_tmp  base=&tmplib..SMS_MESSAGE_REPLY_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : SMS_MESSAGE_REPLY_tmp , SMS_MESSAGE_REPLY );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..SMS_MESSAGE_REPLY AS b USING &tmpdbschema..SMS_MESSAGE_REPLY_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            sms_reply_dttm = d.sms_reply_dttm, 
            load_dttm = d.load_dttm, sms_reply_dttm_tz = d.sms_reply_dttm_tz, 
            task_id = d.task_id, sms_content = d.sms_content, 
            sender_id = d.sender_id, journey_occurrence_id = d.journey_occurrence_id, 
            context_type_nm = d.context_type_nm, aud_occurrence_id = d.aud_occurrence_id, 
            country_cd = d.country_cd, identity_id = d.identity_id, 
            occurrence_id = d.occurrence_id, sms_message_id = d.sms_message_id, 
            task_version_id = d.task_version_id, audience_id = d.audience_id, 
            context_val = d.context_val, event_designed_id = d.event_designed_id, 
            event_nm = d.event_nm, journey_id = d.journey_id, 
            response_tracking_cd = d.response_tracking_cd
         WHEN NOT MATCHED THEN INSERT (
            sms_reply_dttm, load_dttm, sms_reply_dttm_tz, 
            task_id, sms_content, sender_id, journey_occurrence_id, 
            event_id, context_type_nm, aud_occurrence_id, country_cd, 
            identity_id, occurrence_id, sms_message_id, task_version_id, 
            audience_id, context_val, event_designed_id, event_nm, 
            journey_id, response_tracking_cd
         ) VALUES (
            d.sms_reply_dttm, d.load_dttm, d.sms_reply_dttm_tz, 
            d.task_id, d.sms_content, d.sender_id, d.journey_occurrence_id, 
            d.event_id, d.context_type_nm, d.aud_occurrence_id, d.country_cd, 
            d.identity_id, d.occurrence_id, d.sms_message_id, d.task_version_id, 
            d.audience_id, d.context_val, d.event_designed_id, d.event_nm, 
            d.journey_id, d.response_tracking_cd  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : SMS_MESSAGE_REPLY_tmp , SMS_MESSAGE_REPLY , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: SMS_MESSAGE_SEND has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..SMS_MESSAGE_SEND;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SMS_MESSAGE_SEND_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table SMS_MESSAGE_SEND_tmp , SMS_MESSAGE_SEND_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=SMS_MESSAGE_SEND , table_keys=%str(EVENT_ID), out_table=work.SMS_MESSAGE_SEND );
      DATA work.SMS_MESSAGE_SEND_tmp ;
         SET work.SMS_MESSAGE_SEND ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : SMS_MESSAGE_SEND_tmp , SMS_MESSAGE_SEND_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..SMS_MESSAGE_SEND_tmp as select * from &dbschema..SMS_MESSAGE_SEND where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : SMS_MESSAGE_SEND_tmp , SMS_MESSAGE_SEND , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.SMS_MESSAGE_SEND_tmp  base=&tmplib..SMS_MESSAGE_SEND_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : SMS_MESSAGE_SEND_tmp , SMS_MESSAGE_SEND );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..SMS_MESSAGE_SEND AS b USING &tmpdbschema..SMS_MESSAGE_SEND_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            fragment_cnt = d.fragment_cnt, 
            sms_send_dttm = d.sms_send_dttm, load_dttm = d.load_dttm, 
            sms_send_dttm_tz = d.sms_send_dttm_tz, task_id = d.task_id, 
            sender_id = d.sender_id, journey_occurrence_id = d.journey_occurrence_id, 
            identity_id = d.identity_id, event_nm = d.event_nm, 
            creative_id = d.creative_id, audience_id = d.audience_id, 
            context_val = d.context_val, creative_version_id = d.creative_version_id, 
            event_designed_id = d.event_designed_id, occurrence_id = d.occurrence_id, 
            task_version_id = d.task_version_id, aud_occurrence_id = d.aud_occurrence_id, 
            context_type_nm = d.context_type_nm, country_cd = d.country_cd, 
            journey_id = d.journey_id, response_tracking_cd = d.response_tracking_cd, 
            sms_message_id = d.sms_message_id
         WHEN NOT MATCHED THEN INSERT (
            fragment_cnt, sms_send_dttm, load_dttm, 
            sms_send_dttm_tz, task_id, sender_id, journey_occurrence_id, 
            identity_id, event_nm, creative_id, audience_id, 
            context_val, creative_version_id, event_designed_id, occurrence_id, 
            task_version_id, aud_occurrence_id, context_type_nm, country_cd, 
            event_id, journey_id, response_tracking_cd, sms_message_id
         ) VALUES (
            d.fragment_cnt, d.sms_send_dttm, d.load_dttm, 
            d.sms_send_dttm_tz, d.task_id, d.sender_id, d.journey_occurrence_id, 
            d.identity_id, d.event_nm, d.creative_id, d.audience_id, 
            d.context_val, d.creative_version_id, d.event_designed_id, d.occurrence_id, 
            d.task_version_id, d.aud_occurrence_id, d.context_type_nm, d.country_cd, 
            d.event_id, d.journey_id, d.response_tracking_cd, d.sms_message_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : SMS_MESSAGE_SEND_tmp , SMS_MESSAGE_SEND , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: SMS_OPTOUT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..SMS_OPTOUT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SMS_OPTOUT_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table SMS_OPTOUT_tmp , SMS_OPTOUT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=SMS_OPTOUT , table_keys=%str(EVENT_ID), out_table=work.SMS_OPTOUT );
      DATA work.SMS_OPTOUT_tmp ;
         SET work.SMS_OPTOUT ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : SMS_OPTOUT_tmp , SMS_OPTOUT_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..SMS_OPTOUT_tmp as select * from &dbschema..SMS_OPTOUT where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : SMS_OPTOUT_tmp , SMS_OPTOUT , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.SMS_OPTOUT_tmp  base=&tmplib..SMS_OPTOUT_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : SMS_OPTOUT_tmp , SMS_OPTOUT );
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
            task_version_id = d.task_version_id, sms_message_id = d.sms_message_id, 
            occurrence_id = d.occurrence_id, journey_id = d.journey_id, 
            identity_id = d.identity_id, creative_version_id = d.creative_version_id, 
            creative_id = d.creative_id, country_cd = d.country_cd, 
            context_val = d.context_val, context_type_nm = d.context_type_nm, 
            aud_occurrence_id = d.aud_occurrence_id, event_designed_id = d.event_designed_id, 
            journey_occurrence_id = d.journey_occurrence_id, optout_type_nm = d.optout_type_nm, 
            response_tracking_cd = d.response_tracking_cd, task_id = d.task_id, 
            audience_id = d.audience_id, event_nm = d.event_nm, 
            sender_id = d.sender_id
         WHEN NOT MATCHED THEN INSERT (
            load_dttm, sms_optout_dttm, sms_optout_dttm_tz, 
            task_version_id, sms_message_id, occurrence_id, journey_id, 
            identity_id, creative_version_id, creative_id, country_cd, 
            context_val, context_type_nm, aud_occurrence_id, event_designed_id, 
            event_id, journey_occurrence_id, optout_type_nm, response_tracking_cd, 
            task_id, audience_id, event_nm, sender_id
         ) VALUES (
            d.load_dttm, d.sms_optout_dttm, d.sms_optout_dttm_tz, 
            d.task_version_id, d.sms_message_id, d.occurrence_id, d.journey_id, 
            d.identity_id, d.creative_version_id, d.creative_id, d.country_cd, 
            d.context_val, d.context_type_nm, d.aud_occurrence_id, d.event_designed_id, 
            d.event_id, d.journey_occurrence_id, d.optout_type_nm, d.response_tracking_cd, 
            d.task_id, d.audience_id, d.event_nm, d.sender_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : SMS_OPTOUT_tmp , SMS_OPTOUT , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: SMS_OPTOUT_DETAILS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..SMS_OPTOUT_DETAILS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SMS_OPTOUT_DETAILS_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table SMS_OPTOUT_DETAILS_tmp , SMS_OPTOUT_DETAILS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=SMS_OPTOUT_DETAILS , table_keys=%str(EVENT_ID), out_table=work.SMS_OPTOUT_DETAILS );
      DATA work.SMS_OPTOUT_DETAILS_tmp ;
         SET work.SMS_OPTOUT_DETAILS ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : SMS_OPTOUT_DETAILS_tmp , SMS_OPTOUT_DETAILS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..SMS_OPTOUT_DETAILS_tmp as select * from &dbschema..SMS_OPTOUT_DETAILS where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : SMS_OPTOUT_DETAILS_tmp , SMS_OPTOUT_DETAILS , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.SMS_OPTOUT_DETAILS_tmp  base=&tmplib..SMS_OPTOUT_DETAILS_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : SMS_OPTOUT_DETAILS_tmp , SMS_OPTOUT_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..SMS_OPTOUT_DETAILS AS b USING &tmpdbschema..SMS_OPTOUT_DETAILS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            sms_optout_dttm = d.sms_optout_dttm, 
            load_dttm = d.load_dttm, sms_optout_dttm_tz = d.sms_optout_dttm_tz, 
            task_version_id = d.task_version_id, task_id = d.task_id, 
            sms_message_id = d.sms_message_id, occurrence_id = d.occurrence_id, 
            journey_occurrence_id = d.journey_occurrence_id, journey_id = d.journey_id, 
            identity_id = d.identity_id, creative_version_id = d.creative_version_id, 
            context_type_nm = d.context_type_nm, aud_occurrence_id = d.aud_occurrence_id, 
            country_cd = d.country_cd, optout_type_nm = d.optout_type_nm, 
            response_tracking_cd = d.response_tracking_cd, address_val = d.address_val, 
            audience_id = d.audience_id, context_val = d.context_val, 
            creative_id = d.creative_id, event_designed_id = d.event_designed_id, 
            event_nm = d.event_nm, sender_id = d.sender_id
         WHEN NOT MATCHED THEN INSERT (
            sms_optout_dttm, load_dttm, sms_optout_dttm_tz, 
            task_version_id, task_id, sms_message_id, occurrence_id, 
            journey_occurrence_id, journey_id, identity_id, creative_version_id, 
            context_type_nm, aud_occurrence_id, country_cd, event_id, 
            optout_type_nm, response_tracking_cd, address_val, audience_id, 
            context_val, creative_id, event_designed_id, event_nm, 
            sender_id
         ) VALUES (
            d.sms_optout_dttm, d.load_dttm, d.sms_optout_dttm_tz, 
            d.task_version_id, d.task_id, d.sms_message_id, d.occurrence_id, 
            d.journey_occurrence_id, d.journey_id, d.identity_id, d.creative_version_id, 
            d.context_type_nm, d.aud_occurrence_id, d.country_cd, d.event_id, 
            d.optout_type_nm, d.response_tracking_cd, d.address_val, d.audience_id, 
            d.context_val, d.creative_id, d.event_designed_id, d.event_nm, 
            d.sender_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : SMS_OPTOUT_DETAILS_tmp , SMS_OPTOUT_DETAILS , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: SPOT_CLICKED has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..SPOT_CLICKED;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SPOT_CLICKED_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table SPOT_CLICKED_tmp , SPOT_CLICKED_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=SPOT_CLICKED , table_keys=%str(EVENT_ID), out_table=work.SPOT_CLICKED );
      DATA work.SPOT_CLICKED_tmp ;
         SET work.SPOT_CLICKED ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : SPOT_CLICKED_tmp , SPOT_CLICKED_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..SPOT_CLICKED_tmp as select * from &dbschema..SPOT_CLICKED where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : SPOT_CLICKED_tmp , SPOT_CLICKED , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.SPOT_CLICKED_tmp  base=&tmplib..SPOT_CLICKED_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : SPOT_CLICKED_tmp , SPOT_CLICKED );
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
            spot_clicked_dttm_tz = d.spot_clicked_dttm_tz, task_id = d.task_id, 
            session_id_hex = d.session_id_hex, reserved_2_txt = d.reserved_2_txt, 
            rec_group_id = d.rec_group_id, product_id = d.product_id, 
            message_id = d.message_id, event_nm = d.event_nm, 
            detail_id_hex = d.detail_id_hex, creative_id = d.creative_id, 
            context_val = d.context_val, channel_nm = d.channel_nm, 
            channel_user_id = d.channel_user_id, identity_id = d.identity_id, 
            mobile_app_id = d.mobile_app_id, product_sku_no = d.product_sku_no, 
            request_id = d.request_id, segment_id = d.segment_id, 
            spot_id = d.spot_id, url_txt = d.url_txt, 
            context_type_nm = d.context_type_nm, creative_version_id = d.creative_version_id, 
            event_designed_id = d.event_designed_id, event_key_cd = d.event_key_cd, 
            event_source_cd = d.event_source_cd, message_version_id = d.message_version_id, 
            occurrence_id = d.occurrence_id, product_nm = d.product_nm, 
            reserved_1_txt = d.reserved_1_txt, response_tracking_cd = d.response_tracking_cd, 
            segment_version_id = d.segment_version_id, task_version_id = d.task_version_id, 
            visit_id_hex = d.visit_id_hex
         WHEN NOT MATCHED THEN INSERT (
            control_group_flg, product_qty_no, properties_map_doc, 
            spot_clicked_dttm, load_dttm, spot_clicked_dttm_tz, task_id, 
            session_id_hex, reserved_2_txt, rec_group_id, product_id, 
            message_id, event_nm, detail_id_hex, creative_id, 
            context_val, channel_nm, channel_user_id, event_id, 
            identity_id, mobile_app_id, product_sku_no, request_id, 
            segment_id, spot_id, url_txt, context_type_nm, 
            creative_version_id, event_designed_id, event_key_cd, event_source_cd, 
            message_version_id, occurrence_id, product_nm, reserved_1_txt, 
            response_tracking_cd, segment_version_id, task_version_id, visit_id_hex
         ) VALUES (
            d.control_group_flg, d.product_qty_no, d.properties_map_doc, 
            d.spot_clicked_dttm, d.load_dttm, d.spot_clicked_dttm_tz, d.task_id, 
            d.session_id_hex, d.reserved_2_txt, d.rec_group_id, d.product_id, 
            d.message_id, d.event_nm, d.detail_id_hex, d.creative_id, 
            d.context_val, d.channel_nm, d.channel_user_id, d.event_id, 
            d.identity_id, d.mobile_app_id, d.product_sku_no, d.request_id, 
            d.segment_id, d.spot_id, d.url_txt, d.context_type_nm, 
            d.creative_version_id, d.event_designed_id, d.event_key_cd, d.event_source_cd, 
            d.message_version_id, d.occurrence_id, d.product_nm, d.reserved_1_txt, 
            d.response_tracking_cd, d.segment_version_id, d.task_version_id, d.visit_id_hex  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : SPOT_CLICKED_tmp , SPOT_CLICKED , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: SPOT_REQUESTED has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..SPOT_REQUESTED;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..SPOT_REQUESTED_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table SPOT_REQUESTED_tmp , SPOT_REQUESTED_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=SPOT_REQUESTED , table_keys=%str(EVENT_ID), out_table=work.SPOT_REQUESTED );
      DATA work.SPOT_REQUESTED_tmp ;
         SET work.SPOT_REQUESTED ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : SPOT_REQUESTED_tmp , SPOT_REQUESTED_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..SPOT_REQUESTED_tmp as select * from &dbschema..SPOT_REQUESTED where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : SPOT_REQUESTED_tmp , SPOT_REQUESTED , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.SPOT_REQUESTED_tmp  base=&tmplib..SPOT_REQUESTED_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : SPOT_REQUESTED_tmp , SPOT_REQUESTED );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..SPOT_REQUESTED AS b USING &tmpdbschema..SPOT_REQUESTED_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            properties_map_doc = d.properties_map_doc, 
            spot_requested_dttm = d.spot_requested_dttm, spot_requested_dttm_tz = d.spot_requested_dttm_tz, 
            load_dttm = d.load_dttm, visit_id_hex = d.visit_id_hex, 
            request_id = d.request_id, event_source_cd = d.event_source_cd, 
            detail_id_hex = d.detail_id_hex, channel_nm = d.channel_nm, 
            context_type_nm = d.context_type_nm, event_designed_id = d.event_designed_id, 
            mobile_app_id = d.mobile_app_id, session_id_hex = d.session_id_hex, 
            channel_user_id = d.channel_user_id, context_val = d.context_val, 
            event_nm = d.event_nm, identity_id = d.identity_id, 
            spot_id = d.spot_id
         WHEN NOT MATCHED THEN INSERT (
            properties_map_doc, spot_requested_dttm, spot_requested_dttm_tz, 
            load_dttm, visit_id_hex, request_id, event_source_cd, 
            detail_id_hex, channel_nm, context_type_nm, event_designed_id, 
            event_id, mobile_app_id, session_id_hex, channel_user_id, 
            context_val, event_nm, identity_id, spot_id
         ) VALUES (
            d.properties_map_doc, d.spot_requested_dttm, d.spot_requested_dttm_tz, 
            d.load_dttm, d.visit_id_hex, d.request_id, d.event_source_cd, 
            d.detail_id_hex, d.channel_nm, d.context_type_nm, d.event_designed_id, 
            d.event_id, d.mobile_app_id, d.session_id_hex, d.channel_user_id, 
            d.context_val, d.event_nm, d.identity_id, d.spot_id  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : SPOT_REQUESTED_tmp , SPOT_REQUESTED , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: TAG_DETAILS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..TAG_DETAILS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..TAG_DETAILS) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate TAG_DETAILS , TAG_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: VISIT_DETAILS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..VISIT_DETAILS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         DROP TABLE &tmplib..VISIT_DETAILS_tmp ;
      QUIT;
      %err_check (Failed to drop temporary DB table VISIT_DETAILS_tmp , VISIT_DETAILS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      %check_duplicate_from_source(table_nm=VISIT_DETAILS , table_keys=%str(EVENT_ID), out_table=work.VISIT_DETAILS );
      DATA work.VISIT_DETAILS_tmp ;
         SET work.VISIT_DETAILS ;
         IF load_dttm = . THEN DO; 
            IF datekey < 30001231 THEN datekey=datekey*10;
            load_dttm=dhms(mdy(mod(int(datekey/10000),100),mod(int(datekey/100),100),int(datekey/1000000)),mod(datekey,100),0,0);
         END;
         WHERE 1=1 AND EVENT_ID IS NOT NULL;
      RUN;
      %err_check (Failed to prepare staging table : VISIT_DETAILS_tmp , VISIT_DETAILS_tmp );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL noerrorstop;
         connect to &database. (&sql_passthru_connection.);
         execute( create table &tmpdbschema..VISIT_DETAILS_tmp as select * from &dbschema..VISIT_DETAILS where 1=0;) by &database.;
         disconnect from &database.;
      QUIT;
      %err_check (Failed to create table : VISIT_DETAILS_tmp , VISIT_DETAILS , err_macro=SYSDBRC);
   %end;
   %if &errFlag = 0 %then %do;
      PROC APPEND data=work.VISIT_DETAILS_tmp  base=&tmplib..VISIT_DETAILS_tmp
         %if &nrows ge &DB_BL_THRESHOLD. and &DB_BL_THRESHOLD. gt 0 %then %do;
            (&DB_BL_OPTS)
         %end;
         force;
      RUN;
      %err_check (Failed to upload to temp location in DB : VISIT_DETAILS_tmp , VISIT_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (MERGE INTO &dbschema..VISIT_DETAILS AS b USING &tmpdbschema..VISIT_DETAILS_tmp AS d ON (
            b.event_id = d.event_id )
         WHEN MATCHED THEN
         UPDATE SET
            sequence_no = d.sequence_no, 
            load_dttm = d.load_dttm, visit_dttm_tz = d.visit_dttm_tz, 
            visit_dttm = d.visit_dttm, session_id = d.session_id, 
            search_term_txt = d.search_term_txt, referrer_query_string_txt = d.referrer_query_string_txt, 
            origination_placement_nm = d.origination_placement_nm, identity_id = d.identity_id, 
            origination_creative_nm = d.origination_creative_nm, origination_type_nm = d.origination_type_nm, 
            search_engine_desc = d.search_engine_desc, visit_id = d.visit_id, 
            origination_nm = d.origination_nm, origination_tracking_cd = d.origination_tracking_cd, 
            referrer_domain_nm = d.referrer_domain_nm, referrer_txt = d.referrer_txt, 
            search_engine_domain_txt = d.search_engine_domain_txt, session_id_hex = d.session_id_hex, 
            visit_id_hex = d.visit_id_hex
         WHEN NOT MATCHED THEN INSERT (
            sequence_no, load_dttm, visit_dttm_tz, 
            visit_dttm, session_id, search_term_txt, referrer_query_string_txt, 
            origination_placement_nm, identity_id, origination_creative_nm, origination_type_nm, 
            search_engine_desc, visit_id, event_id, origination_nm, 
            origination_tracking_cd, referrer_domain_nm, referrer_txt, search_engine_domain_txt, 
            session_id_hex, visit_id_hex
         ) VALUES (
            d.sequence_no, d.load_dttm, d.visit_dttm_tz, 
            d.visit_dttm, d.session_id, d.search_term_txt, d.referrer_query_string_txt, 
            d.origination_placement_nm, d.identity_id, d.origination_creative_nm, d.origination_type_nm, 
            d.search_engine_desc, d.visit_id, d.event_id, d.origination_nm, 
            d.origination_tracking_cd, d.referrer_domain_nm, d.referrer_txt, d.search_engine_domain_txt, 
            d.session_id_hex, d.visit_id_hex  );) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to Update/Insert into : VISIT_DETAILS_tmp , VISIT_DETAILS , err_macro=SYSDBRC);
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
   %if &nrows = 0 %then %do;
      %put NOTE: WF_PROCESS_DETAILS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..WF_PROCESS_DETAILS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..WF_PROCESS_DETAILS) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate WF_PROCESS_DETAILS , WF_PROCESS_DETAILS );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: WF_PROCESS_DETAILS_CUSTOM_PROP has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..WF_PROCESS_DETAILS_CUSTOM_PROP;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..WF_PROCESS_DETAILS_CUSTOM_PROP) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate WF_PROCESS_DETAILS_CUSTOM_PROP , WF_PROCESS_DETAILS_CUSTOM_PROP );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: WF_PROCESS_TASKS has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..WF_PROCESS_TASKS;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..WF_PROCESS_TASKS) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate WF_PROCESS_TASKS , WF_PROCESS_TASKS );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
   %if &nrows = 0 %then %do;
      %put NOTE: WF_TASKS_USER_ASSIGNMENT has 0 rows. Dropping and skipping load.;
      PROC SQL NOERRORSTOP;
         DROP TABLE &udmmart..WF_TASKS_USER_ASSIGNMENT;
      QUIT;
      %let errFlag=1;
   %end;
   %if &errFlag = 0 %then %do;
      PROC SQL NOERRORSTOP;
         CONNECT TO &database. (&sql_passthru_connection.);
         EXECUTE (TRUNCATE TABLE &dbschema..WF_TASKS_USER_ASSIGNMENT) BY &database.;
         DISCONNECT FROM &database.;
      QUIT;
      %err_check (Failed to truncate WF_TASKS_USER_ASSIGNMENT , WF_TASKS_USER_ASSIGNMENT );
   %end;
   %if &errFlag = 0 %then %do;
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
   %end;
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
