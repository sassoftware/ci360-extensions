/*******************************************************************************/
/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                         */
/* *****************************************************************************/
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..ABT_ATTRIBUTION (
      conversion_value int NULL, interaction_cost int NULL, interaction_dttm datetime2 NOT NULL, task_id varchar(36) NULL, 
      load_id varchar(36) NULL, interaction_type varchar(15) NULL, interaction_subtype varchar(100) NULL, interaction_id varchar(36) NOT NULL, 
      interaction varchar(260) NOT NULL, identity_id varchar(36) NULL, creative_id varchar(36) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_ABT_ATTRIBUTION_interaction_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_ABT_ATTRIBUTION_interaction_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_ABT_ATTRIBUTION_interaction_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_ABT_ATTRIBUTION_interaction_dttm
          AS PARTITION PF_ABT_ATTRIBUTION_interaction_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..ABT_ATTRIBUTION
      ADD CONSTRAINT ABT_ATTRIBUTION_pk
       PRIMARY KEY CLUSTERED (INTERACTION_DTTM,INTERACTION_ID)
        ON PS_ABT_ATTRIBUTION_interaction_dttm(interaction_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ABT_ATTRIBUTION, ABT_ATTRIBUTION);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..AB_TEST_PATH_ASSIGNMENT (
      load_dttm datetime2 NOT NULL, abtestpath_assignment_dttm_tz datetime2 NULL, abtestpath_assignment_dttm datetime2 NULL, session_id_hex varchar(29) NULL, 
      context_type_nm varchar(256) NULL, channel_user_id varchar(300) NULL, identity_id varchar(36) NULL, event_nm varchar(256) NULL, 
      channel_nm varchar(40) NULL, event_id varchar(36) NOT NULL, event_designed_id varchar(36) NULL, abtest_path_id varchar(36) NULL, 
      activity_id varchar(36) NULL, context_val varchar(256) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_AB_TEST_PATH_ASSIGNMENT_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_AB_TEST_PATH_ASSIGNMENT_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_AB_TEST_PATH_ASSIGNMENT_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_AB_TEST_PATH_ASSIGNMENT_load_dttm
          AS PARTITION PF_AB_TEST_PATH_ASSIGNMENT_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..AB_TEST_PATH_ASSIGNMENT
      ADD CONSTRAINT AB_TEST_PATH_ASSIGNMENT_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_AB_TEST_PATH_ASSIGNMENT_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: AB_TEST_PATH_ASSIGNMENT, AB_TEST_PATH_ASSIGNMENT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..ACTIVITY_CONVERSION (
      activity_conversion_dttm_tz datetime2 NULL, load_dttm datetime2 NOT NULL, activity_conversion_dttm datetime2 NULL, abtest_path_id varchar(36) NULL, 
      activity_id varchar(36) NULL, activity_node_id varchar(36) NULL, session_id_hex varchar(29) NULL, parent_event_designed_id varchar(36) NULL, 
      identity_id varchar(36) NULL, goal_id varchar(36) NULL, event_nm varchar(256) NULL, event_id varchar(36) NOT NULL, 
      event_designed_id varchar(36) NULL, detail_id_hex varchar(32) NULL, context_val varchar(256) NULL, channel_nm varchar(40) NULL, 
      context_type_nm varchar(256) NULL, channel_user_id varchar(300) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_ACTIVITY_CONVERSION_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_ACTIVITY_CONVERSION_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_ACTIVITY_CONVERSION_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_ACTIVITY_CONVERSION_load_dttm
          AS PARTITION PF_ACTIVITY_CONVERSION_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..ACTIVITY_CONVERSION
      ADD CONSTRAINT ACTIVITY_CONVERSION_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_ACTIVITY_CONVERSION_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ACTIVITY_CONVERSION, ACTIVITY_CONVERSION);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..ACTIVITY_FLOW_IN (
      activity_flow_in_dttm datetime2 NULL, activity_flow_in_dttm_tz datetime2 NULL, load_dttm datetime2 NOT NULL, task_id varchar(36) NULL, 
      identity_id varchar(36) NULL, context_val varchar(256) NULL, event_designed_id varchar(36) NULL, event_id varchar(36) NOT NULL, 
      channel_user_id varchar(300) NULL, activity_node_id varchar(36) NULL, activity_id varchar(36) NULL, abtest_path_id varchar(36) NULL, 
      channel_nm varchar(40) NULL, context_type_nm varchar(256) NULL, event_nm varchar(256) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_ACTIVITY_FLOW_IN_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_ACTIVITY_FLOW_IN_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_ACTIVITY_FLOW_IN_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_ACTIVITY_FLOW_IN_load_dttm
          AS PARTITION PF_ACTIVITY_FLOW_IN_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..ACTIVITY_FLOW_IN
      ADD CONSTRAINT ACTIVITY_FLOW_IN_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_ACTIVITY_FLOW_IN_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ACTIVITY_FLOW_IN, ACTIVITY_FLOW_IN);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..ACTIVITY_START (
      activity_start_dttm_tz datetime2 NULL, load_dttm datetime2 NOT NULL, activity_start_dttm datetime2 NULL, channel_nm varchar(40) NULL, 
      activity_id varchar(36) NULL, identity_id varchar(36) NULL, event_nm varchar(256) NULL, event_id varchar(36) NOT NULL, 
      channel_user_id varchar(300) NULL, event_designed_id varchar(36) NULL, context_val varchar(256) NULL, context_type_nm varchar(256) NULL
         )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_ACTIVITY_START_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_ACTIVITY_START_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_ACTIVITY_START_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_ACTIVITY_START_load_dttm
          AS PARTITION PF_ACTIVITY_START_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..ACTIVITY_START
      ADD CONSTRAINT ACTIVITY_START_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_ACTIVITY_START_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ACTIVITY_START, ACTIVITY_START);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..ADVERTISING_CONTACT (
      load_dttm datetime2 NOT NULL, advertising_contact_dttm_tz datetime2 NULL, advertising_contact_dttm datetime2 NULL, task_version_id varchar(36) NULL, 
      task_id varchar(36) NULL, task_action_nm varchar(40) NULL, segment_version_id varchar(36) NULL, segment_id varchar(36) NULL, 
      response_tracking_cd varchar(36) NULL, occurrence_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL, journey_id varchar(36) NULL, 
      identity_id varchar(36) NULL, event_nm varchar(256) NULL, event_id varchar(36) NOT NULL, event_designed_id varchar(36) NULL, 
      context_val varchar(256) NULL, context_type_nm varchar(256) NULL, channel_nm varchar(40) NULL, audience_id varchar(36) NULL, 
      aud_occurrence_id varchar(36) NULL, advertising_platform_nm varchar(100) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_ADVERTISING_CONTACT_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_ADVERTISING_CONTACT_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_ADVERTISING_CONTACT_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_ADVERTISING_CONTACT_load_dttm
          AS PARTITION PF_ADVERTISING_CONTACT_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..ADVERTISING_CONTACT
      ADD CONSTRAINT ADVERTISING_CONTACT_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_ADVERTISING_CONTACT_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ADVERTISING_CONTACT, ADVERTISING_CONTACT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..ASSET_DETAILS (
      asset_sk bigint NULL, user_rating_cnt bigint NULL, total_user_rating_val bigint NULL, public_media_id bigint NULL, 
      folder_sk bigint NULL, entity_revision_enabled_flg char(1) NULL, download_disabled_flg char(1) NULL, expired_flg char(1) NULL, 
      asset_locked_flg char(1) NULL, entity_attribute_enabled_flg char(1) NULL, folder_deleted_flg char(1) NULL, asset_deleted_flg char(1) NULL, 
      entity_subtype_enabled_flg char(1) NULL, external_sharing_error_dt date NULL, average_user_rating_val decimal(4,2) NULL, folder_level int NULL, 
      last_modified_dttm datetime2 NULL, created_dttm datetime2 NULL, download_disabled_dttm datetime2 NULL, recycled_dttm datetime2 NULL, 
      expired_dttm datetime2 NULL, load_dttm datetime2 NULL, asset_locked_dttm datetime2 NULL, folder_desc varchar(1332) NULL, 
      external_sharing_error_msg varchar(1024) NULL, entity_table_nm varchar(128) NULL, download_disabled_by_usernm varchar(128) NULL, created_by_usernm varchar(128) NULL, 
      asset_source_type varchar(128) NULL, entity_subtype_nm varchar(128) NULL, entity_type_usage_cd varchar(3) NULL, folder_entity_status_cd varchar(3) NULL, 
      folder_id varchar(128) NULL, asset_owner_usernm varchar(128) NULL, asset_nm varchar(128) NULL, asset_locked_by_usernm varchar(128) NULL, 
      asset_id varchar(128) NULL, asset_desc varchar(1332) NULL, asset_process_status varchar(36) NULL, asset_source_nm varchar(128) NULL, 
      entity_status_cd varchar(3) NULL, recycled_by_usernm varchar(128) NULL, entity_type_nm varchar(128) NULL, public_url varchar(1024) NULL, 
      public_link varchar(1) NULL, process_task_id varchar(128) NULL, process_id varchar(128) NULL, last_modified_by_usernm varchar(128) NULL, 
      folder_path varchar(1024) NULL, folder_owner_usernm varchar(128) NULL, folder_nm varchar(128) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ASSET_DETAILS, ASSET_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..ASSET_DETAILS_CUSTOM_PROP (
      attr_val varchar(4000) NULL, is_obsolete_flg char(1) NULL, is_grid_flg char(1) NULL, load_dttm datetime2 NULL, 
      last_modified_dttm datetime2 NULL, created_dttm datetime2 NULL, remote_pklist_tab_col varchar(128) NULL, last_modified_usernm varchar(128) NULL, 
      data_type varchar(32) NULL, data_formatter varchar(64) NULL, created_by_usernm varchar(128) NULL, attr_nm varchar(128) NULL, 
      attr_id varchar(128) NULL, attr_group_nm varchar(128) NULL, attr_group_id varchar(128) NULL, attr_group_cd varchar(128) NULL, 
      attr_cd varchar(128) NULL, asset_id varchar(128) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ASSET_DETAILS_CUSTOM_PROP, ASSET_DETAILS_CUSTOM_PROP);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..ASSET_FOLDER_DETAILS (
      deleted_flg char(1) NULL, folder_level int NULL, load_dttm datetime2 NULL, created_dttm datetime2 NULL, 
      last_modified_dttm datetime2 NULL, last_modified_by_usernm varchar(128) NULL, folder_owner_usernm varchar(128) NULL, folder_desc varchar(1332) NULL, 
      folder_id varchar(128) NULL, entity_status_cd varchar(3) NULL, folder_nm varchar(128) NULL, folder_path varchar(1024) NULL, 
      created_by_usernm varchar(128) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ASSET_FOLDER_DETAILS, ASSET_FOLDER_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..ASSET_RENDITION_DETAILS (
      download_cnt bigint NULL, revision_no bigint NULL, rend_deleted_flg char(1) NULL, current_revision_flg char(1) NULL, 
      rev_deleted_flg char(1) NULL, media_dpi decimal(10,2) NULL, file_size int NULL, media_height int NULL, 
      rend_duration int NULL, media_width int NULL, media_depth int NULL, created_dttm datetime2 NULL, 
      last_modified_dttm datetime2 NULL, load_dttm datetime2 NULL, revision_id varchar(128) NULL, revision_comment_txt varchar(512) NULL, 
      rendition_nm varchar(128) NULL, rendition_generated_type_cd varchar(3) NULL, last_modified_status_cd varchar(3) NULL, last_modified_by_usernm varchar(128) NULL, 
      file_nm varchar(128) NULL, file_format varchar(128) NULL, entity_status_cd varchar(3) NULL, created_by_usernm varchar(128) NULL, 
      asset_id varchar(128) NULL, rendition_id varchar(128) NULL, rendition_type_cd varchar(3) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ASSET_RENDITION_DETAILS, ASSET_RENDITION_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..ASSET_REVISION (
      revision_no bigint NULL, current_revision_flg char(1) NULL, deleted_flg char(1) NULL, load_dttm datetime2 NULL, 
      created_dttm datetime2 NULL, last_modified_dttm datetime2 NULL, revision_id varchar(128) NULL, last_modified_by_usernm varchar(128) NULL, 
      revision_comment_txt varchar(512) NULL, entity_status_cd varchar(3) NULL, created_by_usernm varchar(128) NULL, asset_id varchar(128) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ASSET_REVISION, ASSET_REVISION);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..AUDIENCE_MEMBERSHIP_CHANGE (
      audience_change_dttm datetime2 NULL, load_dttm datetime2 NOT NULL, audience_change_dttm_tz datetime2 NULL, identity_id varchar(36) NULL, 
      aud_occurrence_id varchar(36) NULL, event_id varchar(36) NOT NULL, audience_id varchar(36) NULL, event_nm varchar(256) NULL
         )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_AUDIENCE_MEMBERSHIP_CHANGE_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_AUDIENCE_MEMBERSHIP_CHANGE_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_AUDIENCE_MEMBERSHIP_CHANGE_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_AUDIENCE_MEMBERSHIP_CHANGE_load_dttm
          AS PARTITION PF_AUDIENCE_MEMBERSHIP_CHANGE_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..AUDIENCE_MEMBERSHIP_CHANGE
      ADD CONSTRAINT AUDIENCE_MEMBERSHIP_CHANGE_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_AUDIENCE_MEMBERSHIP_CHANGE_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: AUDIENCE_MEMBERSHIP_CHANGE, AUDIENCE_MEMBERSHIP_CHANGE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..BUSINESS_PROCESS_DETAILS (
      is_start_flg char(1) NULL, is_completion_flg char(1) NULL, process_attempt_cnt int NULL, step_order_no int NULL, 
      process_instance_no int NULL, process_dttm_tz datetime2 NULL, process_exception_dttm_tz datetime2 NULL, load_dttm datetime2 NOT NULL, 
      process_dttm datetime2 NULL, process_exception_dttm datetime2 NULL, visit_id varchar(32) NULL, process_step_nm varchar(130) NULL, 
      process_details_sk varchar(32) NULL, identity_id varchar(36) NULL, event_nm varchar(256) NULL, detail_id varchar(32) NULL, 
      attribute1_txt varchar(130) NULL, detail_id_hex varchar(32) NULL, event_designed_id varchar(36) NULL, event_id varchar(36) NOT NULL, 
      next_detail_id varchar(32) NULL, process_exception_txt varchar(1300) NULL, session_id varchar(29) NULL, session_id_hex varchar(29) NULL, 
      visit_id_hex varchar(32) NULL, attribute2_txt varchar(130) NULL, event_source_cd varchar(100) NULL, process_nm varchar(130) NULL
         )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_BUSINESS_PROCESS_DETAILS_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_BUSINESS_PROCESS_DETAILS_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_BUSINESS_PROCESS_DETAILS_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_BUSINESS_PROCESS_DETAILS_load_dttm
          AS PARTITION PF_BUSINESS_PROCESS_DETAILS_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..BUSINESS_PROCESS_DETAILS
      ADD CONSTRAINT BUSINESS_PROCESS_DETAILS_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_BUSINESS_PROCESS_DETAILS_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: BUSINESS_PROCESS_DETAILS, BUSINESS_PROCESS_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CART_ACTIVITY_DETAILS (
      unit_price_amt decimal(17,2) NULL, displayed_cart_amt decimal(17,2) NULL, quantity_val int NULL, displayed_cart_items_no int NULL, 
      properties_map_doc varchar(4000) NULL, activity_dttm datetime2 NULL, load_dttm datetime2 NOT NULL, activity_dttm_tz datetime2 NULL, 
      cart_activity_sk varchar(32) NULL, activity_cd varchar(20) NULL, visit_id_hex varchar(32) NULL, visit_id varchar(32) NULL, 
      shipping_message_txt varchar(650) NULL, session_id_hex varchar(29) NULL, session_id varchar(29) NULL, saving_message_txt varchar(650) NULL, 
      product_sku varchar(100) NULL, product_nm varchar(130) NULL, product_id varchar(130) NULL, product_group_nm varchar(130) NULL, 
      mobile_app_id varchar(40) NULL, identity_id varchar(36) NULL, event_source_cd varchar(100) NULL, event_nm varchar(256) NULL, 
      availability_message_txt varchar(650) NULL, cart_id varchar(42) NULL, event_id varchar(36) NOT NULL, event_designed_id varchar(36) NULL, 
      detail_id_hex varchar(32) NULL, detail_id varchar(32) NULL, currency_cd varchar(6) NULL, event_key_cd varchar(100) NULL, 
      channel_nm varchar(40) NULL, cart_nm varchar(100) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_CART_ACTIVITY_DETAILS_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_CART_ACTIVITY_DETAILS_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_CART_ACTIVITY_DETAILS_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_CART_ACTIVITY_DETAILS_load_dttm
          AS PARTITION PF_CART_ACTIVITY_DETAILS_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..CART_ACTIVITY_DETAILS
      ADD CONSTRAINT CART_ACTIVITY_DETAILS_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_CART_ACTIVITY_DETAILS_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CART_ACTIVITY_DETAILS, CART_ACTIVITY_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CC_BUDGET_BREAKUP (
      cc_obsolete_flg char(1) NULL, fin_accnt_obsolete_flg char(1) NULL, cc_budget_distribution decimal(17,2) NULL, load_dttm datetime2 NULL, 
      last_modified_dttm datetime2 NULL, created_dttm datetime2 NULL, planning_nm varchar(128) NULL, planning_id varchar(128) NULL, 
      last_modified_usernm varchar(128) NULL, gen_ledger_cd varchar(128) NULL, fin_accnt_nm varchar(128) NULL, fin_accnt_desc varchar(1332) NULL, 
      created_by_usernm varchar(128) NULL, cost_center_id varchar(128) NULL, cc_owner_usernm varchar(128) NULL, cc_nm varchar(128) NULL, 
      cc_desc varchar(1332) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CC_BUDGET_BREAKUP, CC_BUDGET_BREAKUP);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CC_BUDGET_BREAKUP_CCBDGT (
      cc_obsolete_flg char(1) NULL, fp_obsolete_flg char(1) NULL, fin_accnt_obsolete_flg char(1) NULL, fp_end_dt date NULL, 
      fp_start_dt date NULL, cc_bdgt_invoiced_amt decimal(17,2) NULL, cc_lvl_distribution decimal(17,2) NULL, cc_rldup_child_bdgt decimal(17,2) NULL, 
      cc_level_expense decimal(17,2) NULL, cc_rldup_total_expense decimal(17,2) NULL, cc_bdgt_cmtmnt_overspent_amt decimal(17,2) NULL, cc_bdgt_cmtmnt_invoice_amt decimal(17,2) NULL, 
      cc_bdgt_cmtmnt_outstanding_amt decimal(17,2) NULL, cc_bdgt_amt decimal(17,2) NULL, cc_bdgt_budget_amt decimal(17,2) NULL, cc_budget_distribution decimal(17,2) NULL, 
      cc_bdgt_committed_amt decimal(17,2) NULL, cc_bdgt_direct_invoice_amt decimal(17,2) NULL, cc_bdgt_cmtmnt_invoice_cnt int NULL, last_modified_dttm datetime2 NULL, 
      load_dttm datetime2 NULL, created_dttm datetime2 NULL, planning_id varchar(128) NULL, last_modified_usernm varchar(128) NULL, 
      gen_ledger_cd varchar(128) NULL, fp_nm varchar(128) NULL, fp_id varchar(128) NULL, planning_nm varchar(128) NULL, 
      fp_desc varchar(1332) NULL, fp_cls_ver varchar(128) NULL, fin_accnt_nm varchar(128) NULL, fin_accnt_desc varchar(1332) NULL, 
      created_by_usernm varchar(128) NULL, cost_center_id varchar(128) NULL, cc_owner_usernm varchar(128) NULL, cc_number varchar(128) NULL, 
      cc_nm varchar(128) NULL, cc_desc varchar(1332) NULL, cc_bdgt_budget_desc varchar(1332) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CC_BUDGET_BREAKUP_CCBDGT, CC_BUDGET_BREAKUP_CCBDGT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_ACTIVITY_CUSTOM_ATTR (
      attribute_numeric_val decimal(17,2) NULL, updated_dttm datetime2 NULL, attribute_dttm_val datetime2 NULL, updated_by_nm varchar(60) NULL, 
      attribute_character_val varchar(1500) NULL, activity_version_id varchar(36) NULL, activity_id varchar(36) NULL, attribute_data_type_cd varchar(30) NULL, 
      attribute_nm varchar(256) NULL, attribute_val varchar(1500) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_ACTIVITY_CUSTOM_ATTR, CDM_ACTIVITY_CUSTOM_ATTR);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_ACTIVITY_DETAIL (
      valid_from_dttm datetime2 NULL, updated_dttm datetime2 NULL, last_published_dttm datetime2 NULL, valid_to_dttm datetime2 NULL, 
      status_cd varchar(20) NULL, source_system_cd varchar(10) NULL, activity_nm varchar(256) NULL, activity_id varchar(36) NULL, 
      activity_desc varchar(1500) NULL, activity_category_nm varchar(100) NULL, activity_cd varchar(60) NULL, activity_version_id varchar(36) NULL, 
      updated_by_nm varchar(60) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_ACTIVITY_DETAIL, CDM_ACTIVITY_DETAIL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_ACTIVITY_X_TASK (
      updated_dttm datetime2 NULL, task_version_id varchar(36) NULL, activity_version_id varchar(36) NULL, activity_id varchar(36) NULL, 
      task_id varchar(36) NULL, updated_by_nm varchar(60) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_ACTIVITY_X_TASK, CDM_ACTIVITY_X_TASK);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_AUDIENCE_DETAIL (
      audience_schedule_flg char(1) NULL, create_dttm datetime2 NULL, delete_dttm datetime2 NULL, updated_dttm datetime2 NULL, 
      created_user_nm varchar(256) NULL, audience_source_nm varchar(100) NULL, audience_nm varchar(128) NULL, audience_id varchar(36) NULL, 
      audience_desc varchar(1332) NULL, audience_data_source_nm varchar(100) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_AUDIENCE_DETAIL, CDM_AUDIENCE_DETAIL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_AUDIENCE_OCCUR_DETAIL (
      audience_size_cnt int NULL, end_dttm datetime2 NULL, updated_dttm datetime2 NULL, start_dttm datetime2 NULL, 
      started_by_nm varchar(256) NULL, occurrence_type_nm varchar(100) NULL, audience_occur_id varchar(36) NULL, audience_id varchar(36) NULL, 
      execution_status_cd varchar(100) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_AUDIENCE_OCCUR_DETAIL, CDM_AUDIENCE_OCCUR_DETAIL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_AUDIENCE_X_SEGMENT (
      segment_id varchar(36) NULL, audience_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_AUDIENCE_X_SEGMENT, CDM_AUDIENCE_X_SEGMENT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_BUSINESS_CONTEXT (
      updated_dttm datetime2 NULL, updated_by_nm varchar(60) NULL, business_context_type_cd varchar(40) NULL, business_context_nm varchar(256) NULL, 
      business_context_id varchar(36) NULL, source_system_cd varchar(10) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_BUSINESS_CONTEXT, CDM_BUSINESS_CONTEXT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_CAMPAIGN_CUSTOM_ATTR (
      attribute_numeric_val decimal(17,2) NULL, updated_dttm datetime2 NULL, attribute_dttm_val datetime2 NULL, page_nm varchar(60) NULL, 
      campaign_id varchar(36) NULL, attribute_character_val varchar(1500) NULL, attribute_data_type_cd varchar(30) NULL, attribute_nm varchar(256) NULL, 
      attribute_val varchar(1500) NULL, extension_attribute_nm varchar(256) NULL, updated_by_nm varchar(60) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_CAMPAIGN_CUSTOM_ATTR, CDM_CAMPAIGN_CUSTOM_ATTR);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_CAMPAIGN_DETAIL (
      deleted_flg char(1) NULL, current_version_flg char(1) NULL, min_budget_offer_amt decimal(17,2) NULL, max_budget_amt decimal(17,2) NULL, 
      min_budget_amt decimal(17,2) NULL, max_budget_offer_amt decimal(17,2) NULL, campaign_version_no int NULL, deployment_version_no int NULL, 
      campaign_group_sk int NULL, approval_dttm datetime2 NULL, valid_from_dttm datetime2 NULL, run_dttm datetime2 NULL, 
      updated_dttm datetime2 NULL, valid_to_dttm datetime2 NULL, start_dttm datetime2 NULL, last_modified_dttm datetime2 NULL, 
      end_dttm datetime2 NULL, updated_by_nm varchar(60) NULL, source_system_cd varchar(10) NULL, campaign_status_cd varchar(3) NULL, 
      campaign_type_cd varchar(3) NULL, last_modified_by_user_nm varchar(60) NULL, campaign_nm varchar(60) NULL, campaign_folder_txt varchar(1024) NULL, 
      campaign_desc varchar(1500) NULL, campaign_cd varchar(60) NULL, approval_given_by_nm varchar(60) NULL, campaign_id varchar(36) NULL, 
      campaign_owner_nm varchar(60) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_CAMPAIGN_DETAIL, CDM_CAMPAIGN_DETAIL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_CONTACT_CHANNEL (
      updated_dttm datetime2 NULL, contact_channel_cd varchar(60) NULL, updated_by_nm varchar(60) NULL, contact_channel_nm varchar(40) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_CONTACT_CHANNEL, CDM_CONTACT_CHANNEL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_CONTACT_HISTORY (
      control_group_flg char(1) NULL, optimization_backfill_flg char(1) NULL, contact_dt date NOT NULL, updated_dttm datetime2 NULL, 
      contact_dttm datetime2 NULL, contact_dttm_tz datetime2 NULL, source_system_cd varchar(10) NULL, external_contact_info_2_id varchar(32) NULL, 
      context_type_nm varchar(256) NULL, audience_id varchar(36) NULL, contact_nm varchar(256) NULL, identity_id varchar(36) NULL, 
      audience_occur_id varchar(36) NULL, contact_id varchar(36) NOT NULL, contact_status_cd varchar(3) NULL, context_val varchar(256) NULL, 
      external_contact_info_1_id varchar(32) NULL, rtc_id varchar(36) NULL, updated_by_nm varchar(60) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_CDM_CONTACT_HISTORY_contact_dt') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_CDM_CONTACT_HISTORY_contact_dt (date) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_CDM_CONTACT_HISTORY_contact_dt'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_CDM_CONTACT_HISTORY_contact_dt
          AS PARTITION PF_CDM_CONTACT_HISTORY_contact_dt
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..CDM_CONTACT_HISTORY
      ADD CONSTRAINT CDM_CONTACT_HISTORY_pk
       PRIMARY KEY CLUSTERED (contact_dt ,CONTACT_ID)
        ON PS_CDM_CONTACT_HISTORY_contact_dt(contact_dt);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_CONTACT_HISTORY, CDM_CONTACT_HISTORY);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_CONTACT_STATUS (
      updated_dttm datetime2 NULL, contact_status_desc varchar(256) NULL, contact_status_cd varchar(3) NULL, updated_by_nm varchar(60) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_CONTACT_STATUS, CDM_CONTACT_STATUS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_CONTENT_CUSTOM_ATTR (
      attribute_numeric_val decimal(17,2) NULL, updated_dttm datetime2 NULL, attribute_dttm_val datetime2 NULL, updated_by_nm varchar(60) NULL, 
      attribute_val varchar(1500) NULL, attribute_data_type_cd varchar(30) NULL, attribute_nm varchar(256) NULL, content_version_id varchar(40) NULL, 
      attribute_character_val varchar(1500) NULL, content_id varchar(40) NULL, extension_attribute_nm varchar(256) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_CONTENT_CUSTOM_ATTR, CDM_CONTENT_CUSTOM_ATTR);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_CONTENT_DETAIL (
      active_flg char(1) NULL, created_dt date NULL, updated_dttm datetime2 NULL, valid_from_dttm datetime2 NULL, 
      valid_to_dttm datetime2 NULL, updated_by_nm varchar(60) NULL, owner_nm varchar(256) NULL, external_reference_url_txt varchar(1024) NULL, 
      content_id varchar(40) NULL, contact_content_status_cd varchar(60) NULL, contact_content_cd varchar(60) NULL, contact_content_class_nm varchar(100) NULL, 
      contact_content_desc varchar(1500) NULL, contact_content_nm varchar(256) NULL, contact_content_type_nm varchar(50) NULL, content_version_id varchar(40) NULL, 
      created_user_nm varchar(256) NULL, external_reference_txt varchar(1024) NULL, source_system_cd varchar(10) NULL, contact_content_category_nm varchar(256) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_CONTENT_DETAIL, CDM_CONTENT_DETAIL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_DYN_CONTENT_CUSTOM_ATTR (
      attribute_numeric_val decimal(17,2) NULL, attribute_dttm_val datetime2 NULL, updated_dttm datetime2 NULL, updated_by_nm varchar(60) NULL, 
      content_hash_val varchar(32) NULL, attribute_character_val varchar(1500) NULL, attribute_data_type_cd varchar(30) NULL, attribute_val varchar(1500) NULL, 
      content_version_id varchar(40) NULL, attribute_nm varchar(256) NULL, content_id varchar(40) NULL, extension_attribute_nm varchar(256) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_DYN_CONTENT_CUSTOM_ATTR, CDM_DYN_CONTENT_CUSTOM_ATTR);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_IDENTIFIER_TYPE (
      updated_dttm datetime2 NULL, updated_by_nm varchar(60) NULL, identifier_type_desc varchar(100) NULL, identifier_type_id varchar(36) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_IDENTIFIER_TYPE, CDM_IDENTIFIER_TYPE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_IDENTITY_ATTR (
      entry_dttm datetime2 NULL, valid_to_dttm datetime2 NULL, valid_from_dttm datetime2 NULL, updated_dttm datetime2 NULL, 
      identifier_type_id varchar(36) NULL, user_identifier_val varchar(5000) NULL, updated_by_nm varchar(60) NULL, source_system_cd varchar(10) NULL, 
      identity_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_IDENTITY_ATTR, CDM_IDENTITY_ATTR);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_IDENTITY_MAP (
      updated_dttm datetime2 NULL, identity_type_cd varchar(40) NULL, identity_id varchar(36) NULL, updated_by_nm varchar(60) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_IDENTITY_MAP, CDM_IDENTITY_MAP);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_IDENTITY_TYPE (
      updated_dttm datetime2 NULL, updated_by_nm varchar(60) NULL, identity_type_desc varchar(100) NULL, identity_type_cd varchar(40) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_IDENTITY_TYPE, CDM_IDENTITY_TYPE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_OCCURRENCE_DETAIL (
      occurrence_no int NULL, end_dttm datetime2 NULL, updated_dttm datetime2 NULL, start_dttm datetime2 NULL, 
      updated_by_nm varchar(60) NULL, source_system_cd varchar(10) NULL, occurrence_object_type_cd varchar(60) NULL, occurrence_object_id varchar(36) NULL, 
      occurrence_id varchar(36) NULL, execution_status_cd varchar(30) NULL, occurrence_type_cd varchar(100) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_OCCURRENCE_DETAIL, CDM_OCCURRENCE_DETAIL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_RESPONSE_CHANNEL (
      updated_dttm datetime2 NULL, updated_by_nm varchar(60) NULL, response_channel_nm varchar(60) NULL, response_channel_cd varchar(40) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_RESPONSE_CHANNEL, CDM_RESPONSE_CHANNEL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_RESPONSE_EXTENDED_ATTR (
      updated_dttm datetime2 NOT NULL, updated_by_nm varchar(60) NULL, response_id varchar(36) NOT NULL, response_attribute_type_cd varchar(10) NOT NULL, 
      attribute_val varchar(1500) NULL, attribute_nm varchar(256) NOT NULL, attribute_data_type_cd varchar(30) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_CDM_RESPONSE_EXTENDED_ATTR_updated_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_CDM_RESPONSE_EXTENDED_ATTR_updated_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_CDM_RESPONSE_EXTENDED_ATTR_updated_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_CDM_RESPONSE_EXTENDED_ATTR_updated_dttm
          AS PARTITION PF_CDM_RESPONSE_EXTENDED_ATTR_updated_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..CDM_RESPONSE_EXTENDED_ATTR
      ADD CONSTRAINT CDM_RESPONSE_EXTENDED_ATTR_pk
       PRIMARY KEY CLUSTERED (updated_dttm ,ATTRIBUTE_NM,RESPONSE_ATTRIBUTE_TYPE_CD,RESPONSE_ID)
        ON PS_CDM_RESPONSE_EXTENDED_ATTR_updated_dttm(updated_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_RESPONSE_EXTENDED_ATTR, CDM_RESPONSE_EXTENDED_ATTR);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_RESPONSE_HISTORY (
      inferred_response_flg char(1) NULL, conversion_flg char(1) NULL, response_dt date NOT NULL, response_val_amt decimal(17,2) NULL, 
      properties_map_doc varchar(4000) NULL, updated_dttm datetime2 NULL, response_dttm datetime2 NULL, response_dttm_tz datetime2 NULL, 
      updated_by_nm varchar(60) NULL, source_system_cd varchar(10) NULL, rtc_id varchar(36) NULL, response_type_cd varchar(60) NULL, 
      response_id varchar(36) NOT NULL, response_channel_cd varchar(40) NULL, response_cd varchar(256) NULL, identity_id varchar(36) NULL, 
      external_contact_info_2_id varchar(32) NULL, external_contact_info_1_id varchar(32) NULL, context_val varchar(256) NULL, context_type_nm varchar(256) NULL, 
      content_version_id varchar(40) NULL, content_id varchar(40) NULL, content_hash_val varchar(32) NULL, contact_id varchar(36) NULL, 
      audience_occur_id varchar(36) NULL, audience_id varchar(36) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_CDM_RESPONSE_HISTORY_response_dt') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_CDM_RESPONSE_HISTORY_response_dt (date) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_CDM_RESPONSE_HISTORY_response_dt'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_CDM_RESPONSE_HISTORY_response_dt
          AS PARTITION PF_CDM_RESPONSE_HISTORY_response_dt
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..CDM_RESPONSE_HISTORY
      ADD CONSTRAINT CDM_RESPONSE_HISTORY_pk
       PRIMARY KEY CLUSTERED (response_dt ,RESPONSE_ID)
        ON PS_CDM_RESPONSE_HISTORY_response_dt(response_dt);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_RESPONSE_HISTORY, CDM_RESPONSE_HISTORY);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_RESPONSE_LOOKUP (
      updated_dttm datetime2 NULL, updated_by_nm varchar(60) NULL, response_nm varchar(256) NULL, response_cd varchar(256) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_RESPONSE_LOOKUP, CDM_RESPONSE_LOOKUP);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_RESPONSE_TYPE (
      updated_dttm datetime2 NULL, updated_by_nm varchar(60) NULL, response_type_desc varchar(256) NULL, response_type_cd varchar(60) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_RESPONSE_TYPE, CDM_RESPONSE_TYPE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_RTC_DETAIL (
      deleted_flg char(1) NULL, response_tracking_flg char(1) NULL, task_occurrence_no int NULL, processed_dttm datetime2 NULL, 
      updated_dttm datetime2 NULL, updated_by_nm varchar(60) NULL, task_version_id varchar(36) NULL, task_id varchar(36) NULL, 
      source_system_cd varchar(10) NULL, segment_version_id varchar(36) NULL, segment_id varchar(36) NULL, rtc_id varchar(36) NULL, 
      occurrence_id varchar(36) NULL, execution_status_cd varchar(30) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_RTC_DETAIL, CDM_RTC_DETAIL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_RTC_X_CONTENT (
      sequence_no int NULL, updated_dttm datetime2 NULL, updated_by_nm varchar(60) NULL, rtc_x_content_sk varchar(36) NULL, 
      rtc_id varchar(36) NULL, content_version_id varchar(40) NULL, content_id varchar(40) NULL, content_hash_val varchar(32) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_RTC_X_CONTENT, CDM_RTC_X_CONTENT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_SEGMENT_CUSTOM_ATTR (
      attribute_numeric_val decimal(17,2) NULL, updated_dttm datetime2 NULL, attribute_dttm_val datetime2 NULL, updated_by_nm varchar(60) NULL, 
      segment_version_id varchar(36) NULL, segment_id varchar(36) NULL, attribute_val varchar(1500) NULL, attribute_nm varchar(256) NULL, 
      attribute_data_type_cd varchar(30) NULL, attribute_character_val varchar(1500) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_SEGMENT_CUSTOM_ATTR, CDM_SEGMENT_CUSTOM_ATTR);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_SEGMENT_DETAIL (
      valid_from_dttm datetime2 NULL, updated_dttm datetime2 NULL, valid_to_dttm datetime2 NULL, updated_by_nm varchar(60) NULL, 
      source_system_cd varchar(10) NULL, segment_version_id varchar(36) NULL, segment_status_cd varchar(20) NULL, segment_src_nm varchar(40) NULL, 
      segment_nm varchar(256) NULL, segment_map_version_id varchar(36) NULL, segment_map_id varchar(36) NULL, segment_id varchar(36) NULL, 
      segment_desc varchar(1500) NULL, segment_cd varchar(60) NULL, segment_category_nm varchar(100) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_SEGMENT_DETAIL, CDM_SEGMENT_DETAIL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_SEGMENT_MAP (
      valid_to_dttm datetime2 NULL, valid_from_dttm datetime2 NULL, updated_dttm datetime2 NULL, updated_by_nm varchar(60) NULL, 
      source_system_cd varchar(10) NULL, segment_map_version_id varchar(36) NULL, segment_map_status_cd varchar(20) NULL, segment_map_src_nm varchar(40) NULL, 
      segment_map_nm varchar(256) NULL, segment_map_id varchar(36) NULL, segment_map_desc varchar(1500) NULL, segment_map_cd varchar(60) NULL, 
      segment_map_category_nm varchar(100) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_SEGMENT_MAP, CDM_SEGMENT_MAP);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_SEGMENT_MAP_CUSTOM_ATTR (
      attribute_numeric_val decimal(17,2) NULL, updated_dttm datetime2 NULL, attribute_dttm_val datetime2 NULL, updated_by_nm varchar(60) NULL, 
      segment_map_version_id varchar(36) NULL, segment_map_id varchar(36) NULL, attribute_val varchar(1500) NULL, attribute_nm varchar(256) NULL, 
      attribute_data_type_cd varchar(30) NULL, attribute_character_val varchar(1500) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_SEGMENT_MAP_CUSTOM_ATTR, CDM_SEGMENT_MAP_CUSTOM_ATTR);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_SEGMENT_TEST (
      stratified_sampling_flg char(1) NULL, test_enabled_flg char(1) NULL, test_pct decimal(5,2) NULL, test_cnt int NULL, 
      updated_dttm datetime2 NULL, test_sizing_type_nm varchar(65) NULL, test_type_nm varchar(10) NULL, test_nm varchar(65) NULL, 
      test_cd varchar(60) NULL, task_version_id varchar(36) NULL, task_id varchar(36) NULL, stratified_samp_criteria_txt varchar(1024) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_SEGMENT_TEST, CDM_SEGMENT_TEST);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_SEGMENT_TEST_X_SEGMENT (
      updated_dttm datetime2 NULL, test_cd varchar(60) NULL, task_id varchar(36) NULL, segment_id varchar(36) NULL, 
      task_version_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_SEGMENT_TEST_X_SEGMENT, CDM_SEGMENT_TEST_X_SEGMENT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_TASK_CUSTOM_ATTR (
      attribute_numeric_val decimal(17,2) NULL, attribute_dttm_val datetime2 NULL, updated_dttm datetime2 NULL, task_version_id varchar(36) NULL, 
      task_id varchar(36) NULL, extension_attribute_nm varchar(256) NULL, attribute_character_val varchar(1500) NULL, attribute_data_type_cd varchar(30) NULL, 
      attribute_nm varchar(256) NULL, attribute_val varchar(1500) NULL, updated_by_nm varchar(60) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_TASK_CUSTOM_ATTR, CDM_TASK_CUSTOM_ATTR);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CDM_TASK_DETAIL (
      saved_flg char(1) NULL, active_flg char(1) NULL, published_flg char(1) NULL, update_contact_history_flg char(1) NULL, 
      limit_by_total_impression_flg char(1) NULL, staged_flg char(1) NULL, recurring_schedule_flg char(1) NULL, scheduled_flg char(1) NULL, 
      segment_tests_flg char(1) NULL, standard_reply_flg char(1) NULL, created_dt date NULL, budget_unit_usage_amt decimal(17,2) NULL, 
      min_budget_amt decimal(17,2) NULL, budget_unit_cost_amt decimal(17,2) NULL, max_budget_amt decimal(17,2) NULL, min_budget_offer_amt decimal(17,2) NULL, 
      max_budget_offer_amt decimal(17,2) NULL, maximum_period_expression_cnt int NULL, limit_period_unit_cnt int NULL, scheduled_end_dttm datetime2 NULL, 
      valid_from_dttm datetime2 NULL, export_dttm datetime2 NULL, valid_to_dttm datetime2 NULL, scheduled_start_dttm datetime2 NULL, 
      updated_dttm datetime2 NULL, task_version_id varchar(36) NULL, task_type_nm varchar(40) NULL, task_subtype_nm varchar(100) NULL, 
      task_status_cd varchar(20) NULL, task_id varchar(36) NULL, task_delivery_type_nm varchar(60) NULL, subject_type_nm varchar(60) NULL, 
      source_system_cd varchar(10) NULL, modified_status_cd varchar(20) NULL, contact_channel_cd varchar(60) NULL, business_context_id varchar(36) NULL, 
      campaign_id varchar(36) NULL, control_group_action_nm varchar(65) NULL, created_user_nm varchar(256) NULL, owner_nm varchar(256) NULL, 
      recurr_type_cd varchar(3) NULL, stratified_sampling_action_nm varchar(65) NULL, task_cd varchar(60) NULL, task_desc varchar(1500) NULL, 
      task_nm varchar(256) NULL, updated_by_nm varchar(60) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CDM_TASK_DETAIL, CDM_TASK_DETAIL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..COMMITMENT_DETAILS (
      vendor_obsolete_flg char(1) NULL, cmtmnt_overspent_amt decimal(17,2) NULL, vendor_amt decimal(17,2) NULL, cmtmnt_outstanding_amt decimal(17,2) NULL, 
      cmtmnt_amt decimal(17,2) NULL, last_modified_dttm datetime2 NULL, cmtmnt_payment_dttm datetime2 NULL, cmtmnt_created_dttm datetime2 NULL, 
      load_dttm datetime2 NULL, created_dttm datetime2 NULL, vendor_number varchar(128) NULL, vendor_id varchar(128) NULL, 
      planning_nm varchar(128) NULL, planning_id varchar(128) NULL, last_modified_usernm varchar(128) NULL, created_by_usernm varchar(128) NULL, 
      cmtmnt_status varchar(128) NULL, cmtmnt_nm varchar(128) NULL, cmtmnt_desc varchar(1332) NULL, cmtmnt_closure_note varchar(1332) NULL, 
      cmtmnt_id varchar(128) NULL, cmtmnt_no varchar(128) NULL, planning_currency_cd varchar(10) NULL, vendor_currency_cd varchar(10) NULL, 
      vendor_nm varchar(128) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: COMMITMENT_DETAILS, COMMITMENT_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..COMMITMENT_LINE_ITEMS (
      item_vend_alloc_unit bigint NULL, item_qty bigint NULL, item_alloc_unit bigint NULL, vendor_obsolete_flg char(1) NULL, 
      cc_recon_alloc_amt decimal(17,2) NULL, cmtmnt_overspent_amt decimal(17,2) NULL, cmtmnt_amt decimal(17,2) NULL, cmtmnt_outstanding_amt decimal(17,2) NULL, 
      item_rate decimal(17,2) NULL, item_alloc_amt decimal(17,2) NULL, vendor_amt decimal(17,2) NULL, cc_available_amt decimal(17,2) NULL, 
      item_vend_alloc_amt decimal(17,2) NULL, cc_allocated_amt decimal(17,2) NULL, item_number int NULL, created_dttm datetime2 NULL, 
      last_modified_dttm datetime2 NULL, load_dttm datetime2 NULL, cmtmnt_payment_dttm datetime2 NULL, cmtmnt_created_dttm datetime2 NULL, 
      vendor_nm varchar(128) NULL, vendor_currency_cd varchar(10) NULL, planning_nm varchar(128) NULL, planning_currency_cd varchar(10) NULL, 
      last_modified_usernm varchar(128) NULL, gen_ledger_cd varchar(128) NULL, cost_center_id varchar(128) NULL, cmtmnt_status varchar(128) NULL, 
      cmtmnt_no varchar(128) NULL, cmtmnt_nm varchar(128) NULL, cmtmnt_desc varchar(1332) NULL, cmtmnt_closure_note varchar(1332) NULL, 
      ccat_nm varchar(128) NULL, cc_owner_usernm varchar(128) NULL, cc_nm varchar(128) NULL, cc_desc varchar(1332) NULL, 
      cmtmnt_id varchar(128) NULL, created_by_usernm varchar(128) NULL, fin_acc_nm varchar(128) NULL, item_nm varchar(128) NULL, 
      planning_id varchar(128) NULL, vendor_id varchar(128) NULL, vendor_number varchar(128) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: COMMITMENT_LINE_ITEMS, COMMITMENT_LINE_ITEMS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..COMMITMENT_LINE_ITEMS_CCBDGT (
      fp_obsolete_flg char(1) NULL, vendor_obsolete_flg char(1) NULL, cc_obsolete_flg char(1) NULL, fp_end_dt date NULL, 
      fp_start_dt date NULL, item_alloc_amt decimal(17,2) NULL, cmtmnt_overspent_amt decimal(17,2) NULL, item_vend_alloc_amt decimal(17,2) NULL, 
      cc_allocated_amt decimal(17,2) NULL, vendor_amt decimal(17,2) NULL, cmtmnt_outstanding_amt decimal(17,2) NULL, cmtmnt_amt decimal(17,2) NULL, 
      cc_recon_alloc_amt decimal(17,2) NULL, cc_bdgt_cmtmnt_invoice_amt decimal(17,2) NULL, cc_bdgt_amt decimal(17,2) NULL, cc_available_amt decimal(17,2) NULL, 
      cc_bdgt_committed_amt decimal(17,2) NULL, item_rate decimal(17,2) NULL, cc_bdgt_cmtmnt_overspent_amt decimal(17,2) NULL, cc_bdgt_invoiced_amt decimal(17,2) NULL, 
      cc_bdgt_direct_invoice_amt decimal(17,2) NULL, cc_bdgt_cmtmnt_outstanding_amt decimal(17,2) NULL, cc_bdgt_budget_amt decimal(17,2) NULL, item_number int NULL, 
      item_alloc_unit int NULL, item_qty int NULL, cc_bdgt_cmtmnt_invoice_cnt int NULL, item_vend_alloc_unit int NULL, 
      created_dttm datetime2 NULL, cmtmnt_payment_dttm datetime2 NULL, last_modified_dttm datetime2 NULL, load_dttm datetime2 NULL, 
      cmtmnt_created_dttm datetime2 NULL, vendor_currency_cd varchar(10) NULL, planning_currency_cd varchar(10) NULL, last_modified_usernm varchar(128) NULL, 
      gen_ledger_cd varchar(128) NULL, fp_id varchar(128) NULL, item_nm varchar(128) NULL, planning_nm varchar(128) NULL, 
      vendor_nm varchar(128) NULL, fp_desc varchar(1332) NULL, fp_cls_ver varchar(128) NULL, fin_acc_nm varchar(128) NULL, 
      created_by_usernm varchar(128) NULL, cmtmnt_status varchar(128) NULL, cmtmnt_no varchar(128) NULL, cmtmnt_id varchar(128) NULL, 
      cmtmnt_desc varchar(1332) NULL, cc_nm varchar(128) NULL, cc_desc varchar(1332) NULL, cc_bdgt_budget_desc varchar(1332) NULL, 
      cc_number varchar(128) NULL, cc_owner_usernm varchar(128) NULL, ccat_nm varchar(128) NULL, cmtmnt_closure_note varchar(1332) NULL, 
      cmtmnt_nm varchar(128) NULL, cost_center_id varchar(128) NULL, fp_nm varchar(128) NULL, planning_id varchar(128) NULL, 
      vendor_id varchar(128) NULL, vendor_number varchar(128) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: COMMITMENT_LINE_ITEMS_CCBDGT, COMMITMENT_LINE_ITEMS_CCBDGT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CONTACT_HISTORY (
      control_group_flg char(1) NULL, properties_map_doc varchar(4000) NULL, contact_dttm_tz datetime2 NULL, load_dttm datetime2 NOT NULL, 
      contact_dttm datetime2 NULL, task_id varchar(36) NULL, parent_event_designed_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL, 
      detail_id_hex varchar(32) NULL, context_type_nm varchar(256) NULL, audience_id varchar(36) NULL, contact_id varchar(36) NOT NULL, 
      identity_id varchar(36) NULL, message_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, visit_id_hex varchar(32) NULL, 
      aud_occurrence_id varchar(36) NULL, contact_channel_nm varchar(19) NULL, contact_nm varchar(256) NULL, context_val varchar(256) NULL, 
      creative_id varchar(36) NULL, event_designed_id varchar(36) NULL, journey_id varchar(36) NULL, occurrence_id varchar(36) NULL, 
      session_id_hex varchar(29) NULL, task_version_id varchar(36) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_CONTACT_HISTORY_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_CONTACT_HISTORY_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_CONTACT_HISTORY_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_CONTACT_HISTORY_load_dttm
          AS PARTITION PF_CONTACT_HISTORY_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..CONTACT_HISTORY
      ADD CONSTRAINT CONTACT_HISTORY_pk
       PRIMARY KEY CLUSTERED (load_dttm ,CONTACT_ID)
        ON PS_CONTACT_HISTORY_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CONTACT_HISTORY, CONTACT_HISTORY);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CONVERSION_MILESTONE (
      control_group_flg char(1) NULL, test_flg char(1) NULL, total_cost_amt decimal(17,2) NULL, properties_map_doc varchar(4000) NULL, 
      load_dttm datetime2 NOT NULL, conversion_milestone_dttm datetime2 NULL, conversion_milestone_dttm_tz datetime2 NULL, visit_id_hex varchar(32) NULL, 
      task_id varchar(36) NULL, spot_id varchar(36) NULL, segment_version_id varchar(36) NULL, reserved_1_txt varchar(100) NULL, 
      occurrence_id varchar(36) NULL, message_version_id varchar(36) NULL, goal_id varchar(36) NULL, detail_id_hex varchar(32) NULL, 
      channel_user_id varchar(300) NULL, analysis_group_id varchar(36) NULL, audience_id varchar(36) NULL, context_val varchar(256) NULL, 
      creative_id varchar(36) NULL, event_id varchar(36) NOT NULL, journey_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, 
      activity_id varchar(36) NULL, aud_occurrence_id varchar(36) NULL, channel_nm varchar(36) NULL, context_type_nm varchar(256) NULL, 
      creative_version_id varchar(36) NULL, event_designed_id varchar(36) NULL, event_nm varchar(256) NULL, identity_id varchar(36) NULL, 
      journey_occurrence_id varchar(36) NULL, message_id varchar(36) NULL, mobile_app_id varchar(40) NULL, parent_event_designed_id varchar(36) NULL, 
      rec_group_id varchar(3) NULL, reserved_2_txt varchar(100) NULL, segment_id varchar(36) NULL, session_id_hex varchar(29) NULL, 
      subject_line_txt varchar(256) NULL, task_version_id varchar(36) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_CONVERSION_MILESTONE_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_CONVERSION_MILESTONE_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_CONVERSION_MILESTONE_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_CONVERSION_MILESTONE_load_dttm
          AS PARTITION PF_CONVERSION_MILESTONE_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..CONVERSION_MILESTONE
      ADD CONSTRAINT CONVERSION_MILESTONE_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_CONVERSION_MILESTONE_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CONVERSION_MILESTONE, CONVERSION_MILESTONE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CUSTOM_EVENTS (
      custom_revenue_amt decimal(17,2) NULL, properties_map_doc varchar(4000) NULL, custom_event_dttm datetime2 NULL, custom_event_dttm_tz datetime2 NULL, 
      load_dttm datetime2 NOT NULL, session_id varchar(29) NULL, page_id varchar(256) NULL, event_type_nm varchar(20) NULL, 
      event_id varchar(36) NOT NULL, channel_user_id varchar(300) NULL, custom_event_nm varchar(256) NULL, detail_id_hex varchar(32) NULL, 
      event_nm varchar(256) NULL, reserved_1_txt varchar(100) NULL, reserved_2_txt varchar(100) NULL, visit_id varchar(32) NULL, 
      channel_nm varchar(40) NULL, custom_event_group_nm varchar(256) NULL, custom_events_sk varchar(32) NULL, detail_id varchar(32) NULL, 
      event_designed_id varchar(36) NULL, event_key_cd varchar(100) NULL, event_source_cd varchar(100) NULL, identity_id varchar(36) NULL, 
      mobile_app_id varchar(64) NULL, session_id_hex varchar(29) NULL, visit_id_hex varchar(32) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_CUSTOM_EVENTS_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_CUSTOM_EVENTS_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_CUSTOM_EVENTS_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_CUSTOM_EVENTS_load_dttm
          AS PARTITION PF_CUSTOM_EVENTS_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..CUSTOM_EVENTS
      ADD CONSTRAINT CUSTOM_EVENTS_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_CUSTOM_EVENTS_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CUSTOM_EVENTS, CUSTOM_EVENTS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..CUSTOM_EVENTS_EXT (
      custom_revenue_amt decimal(17,2) NULL, load_dttm datetime2 NOT NULL, event_designed_id varchar(36) NULL, custom_events_sk varchar(32) NOT NULL
         )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_CUSTOM_EVENTS_EXT_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_CUSTOM_EVENTS_EXT_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_CUSTOM_EVENTS_EXT_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_CUSTOM_EVENTS_EXT_load_dttm
          AS PARTITION PF_CUSTOM_EVENTS_EXT_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..CUSTOM_EVENTS_EXT
      ADD CONSTRAINT CUSTOM_EVENTS_EXT_pk
       PRIMARY KEY CLUSTERED (load_dttm ,CUSTOM_EVENTS_SK)
        ON PS_CUSTOM_EVENTS_EXT_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: CUSTOM_EVENTS_EXT, CUSTOM_EVENTS_EXT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DAILY_USAGE (
      customer_profiles_processed_str varchar(4000) NULL, bc_subjcnt_str varchar(4000) NULL, api_usage_str varchar(4000) NULL, web_impr_cnt bigint NULL, 
      facebook_ads_cnt bigint NULL, mob_impr_cnt bigint NULL, plan_users_cnt bigint NULL, email_preview_cnt bigint NULL, 
      outbound_api_cnt bigint NULL, dm_destinations_total_row_cnt bigint NULL, web_sesn_cnt bigint NULL, linkedin_ads_cnt bigint NULL, 
      mobile_in_app_msg_cnt bigint NULL, mobile_push_cnt bigint NULL, audience_usage_cnt bigint NULL, google_ads_cnt bigint NULL, 
      dm_destinations_total_id_cnt bigint NULL, email_send_cnt bigint NULL, mob_sesn_cnt bigint NULL, db_size decimal(17,2) NULL, 
      asset_size decimal(17,2) NULL, admin_user_cnt int NULL, event_day varchar(36) NOT NULL    )) by &database.;
   execute (alter table &dbschema..DAILY_USAGE
      add constraint DAILY_USAGE_pk  primary key (EVENT_DAY)) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DAILY_USAGE, DAILY_USAGE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DATA_VIEW_DETAILS (
      total_cost_amt decimal(17,2) NULL, properties_map_doc varchar(4000) NULL, data_view_dttm datetime2 NULL, data_view_dttm_tz datetime2 NULL, 
      load_dttm datetime2 NOT NULL, visit_id varchar(32) NULL, reserved_2_txt varchar(100) NULL, event_designed_id varchar(36) NULL, 
      channel_user_id varchar(300) NULL, detail_id varchar(32) NULL, event_nm varchar(256) NULL, session_id_hex varchar(29) NULL, 
      detail_id_hex varchar(32) NULL, event_id varchar(36) NOT NULL, identity_id varchar(36) NULL, parent_event_designed_id varchar(36) NULL, 
      reserved_1_txt varchar(100) NULL, session_id varchar(29) NULL, visit_id_hex varchar(32) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_DATA_VIEW_DETAILS_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_DATA_VIEW_DETAILS_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_DATA_VIEW_DETAILS_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_DATA_VIEW_DETAILS_load_dttm
          AS PARTITION PF_DATA_VIEW_DETAILS_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..DATA_VIEW_DETAILS
      ADD CONSTRAINT DATA_VIEW_DETAILS_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_DATA_VIEW_DETAILS_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DATA_VIEW_DETAILS, DATA_VIEW_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DBT_ADV_CAMPAIGN_VISITORS (
      ge_longitude decimal(13,6) NULL, rv_revenue decimal(17,2) NULL, ge_latitude decimal(13,6) NULL, co_conversions int NULL, 
      new_visitors int NULL, return_visitors int NULL, bouncers int NULL, visits int NULL, 
      page_views int NULL, average_visit_duration int NULL, session_complete_load_dttm datetime2 NOT NULL, visit_dttm datetime2 NULL, 
      visit_dttm_tz datetime2 NULL, session_start_dttm_tz datetime2 NULL, session_start_dttm datetime2 NULL, se_external_search_engine varchar(130) NULL, 
      landing_page varchar(1332) NULL, ge_country varchar(85) NULL, cu_customer_id varchar(36) NULL, br_browser_version varchar(16) NULL, 
      device_type varchar(52) NULL, landing_page_url_domain varchar(215) NULL, se_external_search_engine_phrase varchar(1332) NULL, bouncer varchar(12) NULL, 
      br_browser_name varchar(52) NULL, device_name varchar(85) NULL, ge_city varchar(390) NULL, ge_state_region varchar(2) NULL, 
      landing_page_url varchar(1332) NULL, pl_device_operating_system varchar(78) NULL, se_external_search_engine_domain varchar(215) NULL, visitor_type varchar(10) NULL, 
      visitor_id varchar(32) NULL, visit_origination_type varchar(65) NULL, visit_origination_tracking_code varchar(65) NULL, visit_origination_placement varchar(390) NULL, 
      visit_origination_name varchar(260) NULL, visit_origination_creative varchar(260) NULL, visit_id varchar(32) NOT NULL, session_id varchar(29) NOT NULL
         )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_DBT_ADV_CAMPAIGN_VISITORS_session_complete_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_DBT_ADV_CAMPAIGN_VISITORS_session_complete_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_DBT_ADV_CAMPAIGN_VISITORS_session_complete_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_DBT_ADV_CAMPAIGN_VISITORS_session_complete_load_dttm
          AS PARTITION PF_DBT_ADV_CAMPAIGN_VISITORS_session_complete_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..DBT_ADV_CAMPAIGN_VISITORS
      ADD CONSTRAINT DBT_ADV_CAMPAIGN_VISITORS_pk
       PRIMARY KEY CLUSTERED (session_complete_load_dttm ,SESSION_ID,VISIT_ID)
        ON PS_DBT_ADV_CAMPAIGN_VISITORS_session_complete_load_dttm(session_complete_load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_ADV_CAMPAIGN_VISITORS, DBT_ADV_CAMPAIGN_VISITORS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DBT_BUSINESS_PROCESS (
      processes smallint NULL, steps_completed smallint NULL, step_count smallint NULL, processes_completed smallint NULL, 
      steps_abandoned smallint NULL, last_step smallint NULL, processes_abandoned smallint NULL, steps smallint NULL, 
      bus_process_started_dttm_tz datetime2 NULL, session_start_dttm_tz datetime2 NULL, session_start_dttm datetime2 NULL, bus_process_started_dttm datetime2 NOT NULL, 
      session_complete_load_dttm datetime2 NOT NULL, visitor_id varchar(32) NULL, visit_origination_tracking_code varchar(65) NULL, visit_origination_name varchar(260) NULL, 
      visit_id varchar(32) NULL, session_id varchar(29) NOT NULL, device_name varchar(85) NULL, cu_customer_id varchar(36) NULL, 
      business_process_step_name varchar(130) NOT NULL, business_process_attribute_2 varchar(130) NULL, bouncer varchar(12) NULL, business_process_attribute_1 varchar(130) NULL, 
      business_process_name varchar(130) NOT NULL, device_type varchar(52) NULL, visit_origination_creative varchar(260) NULL, visit_origination_placement varchar(390) NULL, 
      visit_origination_type varchar(65) NULL, visitor_type varchar(10) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_DBT_BUSINESS_PROCESS_session_complete_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_DBT_BUSINESS_PROCESS_session_complete_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_DBT_BUSINESS_PROCESS_session_complete_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_DBT_BUSINESS_PROCESS_session_complete_load_dttm
          AS PARTITION PF_DBT_BUSINESS_PROCESS_session_complete_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..DBT_BUSINESS_PROCESS
      ADD CONSTRAINT DBT_BUSINESS_PROCESS_pk
       PRIMARY KEY CLUSTERED (session_complete_load_dttm ,BUSINESS_PROCESS_NAME,BUSINESS_PROCESS_STEP_NAME,BUS_PROCESS_STARTED_DTTM,SESSION_ID)
        ON PS_DBT_BUSINESS_PROCESS_session_complete_load_dttm(session_complete_load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_BUSINESS_PROCESS, DBT_BUSINESS_PROCESS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DBT_CONTENT (
      total_page_view_time bigint NULL, entry_pages int NULL, active_page_view_time int NULL, views int NULL, 
      exit_pages int NULL, visits int NULL, bouncers int NULL, session_start_dttm datetime2 NULL, 
      session_start_dttm_tz datetime2 NULL, session_complete_load_dttm datetime2 NOT NULL, detail_dttm_tz datetime2 NULL, detail_dttm datetime2 NULL, 
      visitor_type varchar(10) NULL, visitor_id varchar(32) NULL, visit_origination_type varchar(65) NULL, visit_origination_tracking_code varchar(65) NULL, 
      visit_origination_placement varchar(390) NULL, visit_origination_name varchar(260) NULL, visit_origination_creative varchar(260) NULL, visit_id varchar(32) NULL, 
      session_id varchar(29) NULL, pg_page_url varchar(1332) NULL, pg_page varchar(1332) NULL, pg_domain_name varchar(215) NULL, 
      device_type varchar(52) NULL, device_name varchar(85) NULL, detail_id varchar(32) NOT NULL, cu_customer_id varchar(36) NULL, 
      class2_id varchar(650) NULL, bouncer varchar(12) NULL, class1_id varchar(650) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_DBT_CONTENT_session_complete_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_DBT_CONTENT_session_complete_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_DBT_CONTENT_session_complete_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_DBT_CONTENT_session_complete_load_dttm
          AS PARTITION PF_DBT_CONTENT_session_complete_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..DBT_CONTENT
      ADD CONSTRAINT DBT_CONTENT_pk
       PRIMARY KEY CLUSTERED (session_complete_load_dttm ,DETAIL_ID)
        ON PS_DBT_CONTENT_session_complete_load_dttm(session_complete_load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_CONTENT, DBT_CONTENT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DBT_DOCUMENTS (
      document_downloads int NULL, document_download_dttm_tz datetime2 NULL, session_start_dttm_tz datetime2 NULL, session_start_dttm datetime2 NULL, 
      session_complete_load_dttm datetime2 NOT NULL, document_download_dttm datetime2 NULL, visitor_type varchar(10) NULL, visitor_id varchar(32) NULL, 
      visit_origination_type varchar(65) NULL, visit_origination_tracking_code varchar(65) NULL, visit_origination_placement varchar(390) NULL, visit_origination_name varchar(260) NULL, 
      visit_origination_creative varchar(260) NULL, visit_id varchar(32) NULL, session_id varchar(29) NULL, do_page_url varchar(1332) NULL, 
      do_page_description varchar(1332) NULL, device_type varchar(52) NULL, device_name varchar(85) NULL, detail_id varchar(32) NOT NULL, 
      cu_customer_id varchar(36) NULL, class2_id varchar(650) NULL, class1_id varchar(650) NULL, bouncer varchar(12) NULL
         )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_DBT_DOCUMENTS_session_complete_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_DBT_DOCUMENTS_session_complete_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_DBT_DOCUMENTS_session_complete_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_DBT_DOCUMENTS_session_complete_load_dttm
          AS PARTITION PF_DBT_DOCUMENTS_session_complete_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..DBT_DOCUMENTS
      ADD CONSTRAINT DBT_DOCUMENTS_pk
       PRIMARY KEY CLUSTERED (session_complete_load_dttm ,DETAIL_ID)
        ON PS_DBT_DOCUMENTS_session_complete_load_dttm(session_complete_load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_DOCUMENTS, DBT_DOCUMENTS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DBT_ECOMMERCE (
      product_purchase_revenues decimal(17,2) NULL, basket_adds_revenue decimal(17,2) NULL, basket_removes_revenue decimal(17,2) NULL, product_views int NULL, 
      basket_adds int NULL, basket_adds_units int NULL, product_purchases int NULL, product_purchase_units int NULL, 
      basket_removes_units int NULL, basket_removes int NULL, baskets_abandoned smallint NULL, baskets_completed smallint NULL, 
      baskets_started smallint NULL, session_complete_load_dttm datetime2 NOT NULL, session_start_dttm_tz datetime2 NULL, product_activity_dttm_tz datetime2 NULL, 
      product_activity_dttm datetime2 NOT NULL, session_start_dttm datetime2 NULL, visitor_type varchar(10) NULL, visitor_id varchar(32) NULL, 
      visit_origination_type varchar(65) NULL, visit_origination_tracking_code varchar(65) NULL, visit_origination_placement varchar(390) NULL, visit_origination_name varchar(260) NULL, 
      visit_origination_creative varchar(260) NULL, visit_id varchar(32) NOT NULL, session_id varchar(29) NULL, product_sku varchar(100) NOT NULL, 
      product_name varchar(130) NOT NULL, product_id varchar(130) NOT NULL, product_group_name varchar(130) NULL, device_type varchar(52) NULL, 
      device_name varchar(85) NULL, cu_customer_id varchar(36) NULL, bouncer varchar(12) NULL, basket_id varchar(42) NOT NULL
         )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_DBT_ECOMMERCE_session_complete_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_DBT_ECOMMERCE_session_complete_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_DBT_ECOMMERCE_session_complete_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_DBT_ECOMMERCE_session_complete_load_dttm
          AS PARTITION PF_DBT_ECOMMERCE_session_complete_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..DBT_ECOMMERCE
      ADD CONSTRAINT DBT_ECOMMERCE_pk
       PRIMARY KEY CLUSTERED (session_complete_load_dttm ,BASKET_ID,PRODUCT_ACTIVITY_DTTM,PRODUCT_ID,PRODUCT_NAME,PRODUCT_SKU,VISIT_ID)
        ON PS_DBT_ECOMMERCE_session_complete_load_dttm(session_complete_load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_ECOMMERCE, DBT_ECOMMERCE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DBT_FORMS (
      attempts int NULL, forms_completed smallint NULL, forms_not_submitted smallint NULL, forms_started smallint NULL, 
      form_attempt_dttm datetime2 NULL, session_start_dttm datetime2 NULL, form_attempt_dttm_tz datetime2 NULL, session_complete_load_dttm datetime2 NOT NULL, 
      session_start_dttm_tz datetime2 NULL, visitor_type varchar(10) NULL, visitor_id varchar(32) NULL, visit_origination_type varchar(65) NULL, 
      visit_origination_tracking_code varchar(65) NULL, visit_origination_placement varchar(390) NULL, visit_origination_name varchar(260) NULL, visit_origination_creative varchar(260) NULL, 
      visit_id varchar(32) NULL, session_id varchar(29) NULL, last_field varchar(325) NULL, form_nm varchar(65) NULL, 
      device_type varchar(52) NULL, device_name varchar(85) NULL, detail_id varchar(32) NOT NULL, cu_customer_id varchar(36) NULL, 
      bouncer varchar(12) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_DBT_FORMS_session_complete_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_DBT_FORMS_session_complete_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_DBT_FORMS_session_complete_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_DBT_FORMS_session_complete_load_dttm
          AS PARTITION PF_DBT_FORMS_session_complete_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..DBT_FORMS
      ADD CONSTRAINT DBT_FORMS_pk
       PRIMARY KEY CLUSTERED (session_complete_load_dttm ,DETAIL_ID)
        ON PS_DBT_FORMS_session_complete_load_dttm(session_complete_load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_FORMS, DBT_FORMS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DBT_GOALS (
      goal_revenue decimal(17,2) NULL, visits int NULL, session_start_dttm datetime2 NULL, goal_reached_dttm_tz datetime2 NULL, 
      goal_reached_dttm datetime2 NULL, session_complete_load_dttm datetime2 NOT NULL, session_start_dttm_tz datetime2 NULL, goals tinyint NULL, 
      visitor_type varchar(10) NULL, visitor_id varchar(32) NULL, visit_origination_type varchar(65) NULL, visit_origination_tracking_code varchar(65) NULL, 
      visit_origination_placement varchar(390) NULL, visit_origination_name varchar(260) NULL, visit_origination_creative varchar(260) NULL, visit_id varchar(32) NULL, 
      session_id varchar(29) NULL, goal_name varchar(260) NULL, goal_group_name varchar(130) NULL, device_type varchar(52) NULL, 
      device_name varchar(85) NULL, detail_id varchar(32) NOT NULL, cu_customer_id varchar(36) NULL, bouncer varchar(12) NULL
         )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_DBT_GOALS_session_complete_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_DBT_GOALS_session_complete_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_DBT_GOALS_session_complete_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_DBT_GOALS_session_complete_load_dttm
          AS PARTITION PF_DBT_GOALS_session_complete_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..DBT_GOALS
      ADD CONSTRAINT DBT_GOALS_pk
       PRIMARY KEY CLUSTERED (session_complete_load_dttm ,DETAIL_ID)
        ON PS_DBT_GOALS_session_complete_load_dttm(session_complete_load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_GOALS, DBT_GOALS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DBT_MEDIA_CONSUMPTION (
      views varchar NULL, views_started varchar NULL, views_completed varchar NULL, media_section_view varchar NULL, 
      time_viewing decimal(11,3) NULL, duration decimal(11,3) NULL, content_viewed decimal(11,3) NULL, maximum_progress decimal(11,3) NOT NULL, 
      counter int NULL, interactions_count int NOT NULL, session_start_dttm_tz datetime2 NULL, session_start_dttm datetime2 NULL, 
      session_complete_load_dttm datetime2 NOT NULL, media_start_dttm datetime2 NULL, media_start_dttm_tz datetime2 NULL, visitor_type varchar(10) NULL, 
      visitor_id varchar(32) NULL, visit_origination_type varchar(65) NULL, visit_origination_tracking_code varchar(65) NULL, visit_origination_placement varchar(390) NULL, 
      visit_origination_name varchar(260) NULL, visit_origination_creative varchar(260) NULL, visit_id varchar(32) NOT NULL, session_id varchar(29) NULL, 
      media_uri_txt varchar(2024) NULL, media_section varchar(35) NOT NULL, media_name varchar(260) NULL, media_completion_rate varchar(35) NOT NULL, 
      device_type varchar(52) NULL, device_name varchar(85) NULL, detail_id varchar(32) NOT NULL, cu_customer_id varchar(36) NULL, 
      bouncer varchar(12) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_DBT_MEDIA_CONSUMPTION_session_complete_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_DBT_MEDIA_CONSUMPTION_session_complete_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_DBT_MEDIA_CONSUMPTION_session_complete_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_DBT_MEDIA_CONSUMPTION_session_complete_load_dttm
          AS PARTITION PF_DBT_MEDIA_CONSUMPTION_session_complete_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..DBT_MEDIA_CONSUMPTION
      ADD CONSTRAINT DBT_MEDIA_CONSUMPTION_pk
       PRIMARY KEY CLUSTERED (session_complete_load_dttm ,DETAIL_ID,INTERACTIONS_COUNT,MAXIMUM_PROGRESS,MEDIA_COMPLETION_RATE,MEDIA_SECTION,VISIT_ID)
        ON PS_DBT_MEDIA_CONSUMPTION_session_complete_load_dttm(session_complete_load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_MEDIA_CONSUMPTION, DBT_MEDIA_CONSUMPTION);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DBT_PROMOTIONS (
      click_throughs int NULL, displays int NULL, session_start_dttm_tz datetime2 NULL, promotion_shown_dttm_tz datetime2 NULL, 
      promotion_shown_dttm datetime2 NULL, session_complete_load_dttm datetime2 NOT NULL, session_start_dttm datetime2 NULL, visitor_type varchar(10) NULL, 
      visitor_id varchar(32) NULL, visit_origination_type varchar(65) NULL, visit_origination_tracking_code varchar(65) NULL, visit_origination_placement varchar(390) NULL, 
      visit_origination_name varchar(260) NULL, visit_origination_creative varchar(260) NULL, visit_id varchar(32) NULL, session_id varchar(29) NULL, 
      promotion_type varchar(65) NULL, promotion_tracking_code varchar(65) NULL, promotion_placement varchar(260) NULL, promotion_name varchar(260) NULL, 
      promotion_creative varchar(260) NULL, device_type varchar(52) NULL, device_name varchar(85) NULL, detail_id varchar(32) NOT NULL, 
      cu_customer_id varchar(36) NULL, bouncer varchar(12) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_DBT_PROMOTIONS_session_complete_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_DBT_PROMOTIONS_session_complete_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_DBT_PROMOTIONS_session_complete_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_DBT_PROMOTIONS_session_complete_load_dttm
          AS PARTITION PF_DBT_PROMOTIONS_session_complete_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..DBT_PROMOTIONS
      ADD CONSTRAINT DBT_PROMOTIONS_pk
       PRIMARY KEY CLUSTERED (session_complete_load_dttm ,DETAIL_ID)
        ON PS_DBT_PROMOTIONS_session_complete_load_dttm(session_complete_load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_PROMOTIONS, DBT_PROMOTIONS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DBT_SEARCH (
      num_additional_searches int NULL, num_pages_viewed_afterwards int NULL, searches int NULL, visits int NULL, 
      search_unknown_results int NULL, exit_pages int NULL, search_no_results_returned int NULL, search_returned_results int NULL, 
      search_results_dttm_tz datetime2 NULL, session_start_dttm datetime2 NULL, session_start_dttm_tz datetime2 NULL, session_complete_load_dttm datetime2 NOT NULL, 
      search_results_dttm datetime2 NULL, visitor_type varchar(10) NULL, visitor_id varchar(32) NULL, visit_origination_type varchar(65) NULL, 
      visit_origination_tracking_code varchar(65) NULL, visit_origination_placement varchar(390) NULL, visit_origination_name varchar(260) NULL, visit_origination_creative varchar(260) NULL, 
      visit_id varchar(32) NULL, session_id varchar(29) NULL, search_name varchar(42) NULL, internal_search_term varchar(128) NULL, 
      device_type varchar(52) NULL, device_name varchar(85) NULL, detail_id varchar(32) NOT NULL, cu_customer_id varchar(36) NULL, 
      bouncer varchar(12) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_DBT_SEARCH_session_complete_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_DBT_SEARCH_session_complete_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_DBT_SEARCH_session_complete_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_DBT_SEARCH_session_complete_load_dttm
          AS PARTITION PF_DBT_SEARCH_session_complete_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..DBT_SEARCH
      ADD CONSTRAINT DBT_SEARCH_pk
       PRIMARY KEY CLUSTERED (session_complete_load_dttm ,DETAIL_ID)
        ON PS_DBT_SEARCH_session_complete_load_dttm(session_complete_load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DBT_SEARCH, DBT_SEARCH);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DIRECT_CONTACT (
      control_active_flg char(1) NULL, control_group_flg char(1) NULL, properties_map_doc varchar(4000) NULL, load_dttm datetime2 NOT NULL, 
      direct_contact_dttm datetime2 NULL, direct_contact_dttm_tz datetime2 NULL, task_version_id varchar(36) NULL, task_id varchar(36) NULL, 
      segment_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, occurrence_id varchar(36) NULL, message_id varchar(36) NULL, 
      identity_type_nm varchar(36) NULL, identity_id varchar(36) NULL, event_nm varchar(256) NULL, event_id varchar(36) NOT NULL, 
      event_designed_id varchar(36) NULL, context_val varchar(256) NULL, context_type_nm varchar(256) NULL, channel_user_id varchar(300) NULL, 
      channel_nm varchar(40) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_DIRECT_CONTACT_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_DIRECT_CONTACT_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_DIRECT_CONTACT_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_DIRECT_CONTACT_load_dttm
          AS PARTITION PF_DIRECT_CONTACT_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..DIRECT_CONTACT
      ADD CONSTRAINT DIRECT_CONTACT_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_DIRECT_CONTACT_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DIRECT_CONTACT, DIRECT_CONTACT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..DOCUMENT_DETAILS (
      load_dttm datetime2 NOT NULL, link_event_dttm datetime2 NULL, link_event_dttm_tz datetime2 NULL, visit_id_hex varchar(32) NULL, 
      uri_txt varchar(1332) NULL, session_id varchar(29) NULL, link_selector_path varchar(1332) NULL, link_id varchar(1332) NULL, 
      link_name varchar(1332) NULL, identity_id varchar(36) NULL, event_source_cd varchar(100) NULL, session_id_hex varchar(29) NULL, 
      event_key_cd varchar(100) NULL, visit_id varchar(32) NULL, event_id varchar(36) NOT NULL, detail_id_hex varchar(32) NULL, 
      detail_id varchar(32) NULL, alt_txt varchar(1332) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_DOCUMENT_DETAILS_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_DOCUMENT_DETAILS_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_DOCUMENT_DETAILS_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_DOCUMENT_DETAILS_load_dttm
          AS PARTITION PF_DOCUMENT_DETAILS_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..DOCUMENT_DETAILS
      ADD CONSTRAINT DOCUMENT_DETAILS_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_DOCUMENT_DETAILS_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: DOCUMENT_DETAILS, DOCUMENT_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..EMAIL_BOUNCE (
      test_flg char(1) NULL, properties_map_doc varchar(4000) NULL, load_dttm datetime2 NOT NULL, email_bounce_dttm_tz datetime2 NULL, 
      email_bounce_dttm datetime2 NULL, task_id varchar(36) NULL, subject_line_txt varchar(256) NULL, segment_version_id varchar(36) NULL, 
      response_tracking_cd varchar(36) NULL, reason_txt varchar(1000) NULL, raw_reason_txt varchar(1000) NULL, occurrence_id varchar(36) NULL, 
      journey_occurrence_id varchar(36) NULL, imprint_id varchar(36) NULL, event_nm varchar(256) NULL, event_designed_id varchar(36) NULL, 
      context_type_nm varchar(256) NULL, bounce_class_cd varchar(5) NULL, aud_occurrence_id varchar(36) NULL, analysis_group_id varchar(36) NULL, 
      audience_id varchar(36) NULL, channel_user_id varchar(300) NULL, context_val varchar(256) NULL, event_id varchar(36) NOT NULL, 
      identity_id varchar(36) NULL, journey_id varchar(36) NULL, program_id varchar(50) NULL, recipient_domain_nm varchar(100) NULL, 
      segment_id varchar(36) NULL, task_version_id varchar(36) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_EMAIL_BOUNCE_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_EMAIL_BOUNCE_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_EMAIL_BOUNCE_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_EMAIL_BOUNCE_load_dttm
          AS PARTITION PF_EMAIL_BOUNCE_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..EMAIL_BOUNCE
      ADD CONSTRAINT EMAIL_BOUNCE_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_EMAIL_BOUNCE_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EMAIL_BOUNCE, EMAIL_BOUNCE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..EMAIL_CLICK (
      open_tracking_flg char(1) NULL, click_tracking_flg char(1) NULL, is_mobile_flg char(1) NULL, test_flg char(1) NULL, 
      properties_map_doc varchar(4000) NULL, email_click_dttm datetime2 NULL, email_click_dttm_tz datetime2 NULL, load_dttm datetime2 NOT NULL, 
      uri_txt varchar(1332) NULL, task_version_id varchar(36) NULL, task_id varchar(36) NULL, subject_line_txt varchar(256) NULL, 
      segment_id varchar(36) NULL, recipient_domain_nm varchar(100) NULL, program_id varchar(50) NULL, platform_version varchar(25) NULL, 
      platform_desc varchar(78) NULL, occurrence_id varchar(36) NULL, manufacturer_nm varchar(75) NULL, mailbox_provider_nm varchar(100) NULL, 
      link_tracking_label_txt varchar(256) NULL, link_tracking_id varchar(4) NULL, link_tracking_group_txt varchar(256) NULL, journey_id varchar(36) NULL, 
      imprint_id varchar(36) NULL, event_nm varchar(256) NULL, event_id varchar(36) NOT NULL, event_designed_id varchar(36) NULL, 
      context_val varchar(256) NULL, audience_id varchar(36) NULL, analysis_group_id varchar(36) NULL, agent_family_nm varchar(100) NULL, 
      aud_occurrence_id varchar(36) NULL, channel_user_id varchar(300) NULL, context_type_nm varchar(256) NULL, device_nm varchar(85) NULL, 
      identity_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, segment_version_id varchar(36) NULL, 
      user_agent_nm varchar(512) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_EMAIL_CLICK_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_EMAIL_CLICK_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_EMAIL_CLICK_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_EMAIL_CLICK_load_dttm
          AS PARTITION PF_EMAIL_CLICK_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..EMAIL_CLICK
      ADD CONSTRAINT EMAIL_CLICK_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_EMAIL_CLICK_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EMAIL_CLICK, EMAIL_CLICK);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..EMAIL_COMPLAINT (
      test_flg char(1) NULL, properties_map_doc varchar(4000) NULL, load_dttm datetime2 NOT NULL, email_complaint_dttm datetime2 NULL, 
      email_complaint_dttm_tz datetime2 NULL, task_id varchar(36) NULL, segment_version_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, 
      recipient_domain_nm varchar(100) NULL, occurrence_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL, imprint_id varchar(36) NULL, 
      event_nm varchar(256) NULL, event_id varchar(36) NOT NULL, event_designed_id varchar(36) NULL, context_type_nm varchar(256) NULL, 
      audience_id varchar(36) NULL, analysis_group_id varchar(36) NULL, aud_occurrence_id varchar(36) NULL, channel_user_id varchar(300) NULL, 
      context_val varchar(256) NULL, identity_id varchar(36) NULL, journey_id varchar(36) NULL, program_id varchar(50) NULL, 
      segment_id varchar(36) NULL, subject_line_txt varchar(256) NULL, task_version_id varchar(36) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_EMAIL_COMPLAINT_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_EMAIL_COMPLAINT_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_EMAIL_COMPLAINT_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_EMAIL_COMPLAINT_load_dttm
          AS PARTITION PF_EMAIL_COMPLAINT_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..EMAIL_COMPLAINT
      ADD CONSTRAINT EMAIL_COMPLAINT_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_EMAIL_COMPLAINT_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EMAIL_COMPLAINT, EMAIL_COMPLAINT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..EMAIL_OPEN (
      click_tracking_flg char(1) NULL, is_mobile_flg char(1) NULL, open_tracking_flg char(1) NULL, prefetched_flg char(1) NULL, 
      test_flg char(1) NULL, properties_map_doc varchar(4000) NULL, email_open_dttm datetime2 NULL, email_open_dttm_tz datetime2 NULL, 
      load_dttm datetime2 NOT NULL, user_agent_nm varchar(512) NULL, task_version_id varchar(36) NULL, subject_line_txt varchar(256) NULL, 
      segment_version_id varchar(36) NULL, segment_id varchar(36) NULL, recipient_domain_nm varchar(100) NULL, program_id varchar(50) NULL, 
      platform_version varchar(25) NULL, occurrence_id varchar(36) NULL, manufacturer_nm varchar(75) NULL, journey_id varchar(36) NULL, 
      imprint_id varchar(36) NULL, event_nm varchar(256) NULL, event_designed_id varchar(36) NULL, context_val varchar(256) NULL, 
      audience_id varchar(36) NULL, analysis_group_id varchar(36) NULL, agent_family_nm varchar(100) NULL, aud_occurrence_id varchar(36) NULL, 
      channel_user_id varchar(300) NULL, context_type_nm varchar(256) NULL, device_nm varchar(85) NULL, event_id varchar(36) NOT NULL, 
      identity_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL, mailbox_provider_nm varchar(100) NULL, platform_desc varchar(78) NULL, 
      response_tracking_cd varchar(36) NULL, task_id varchar(36) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_EMAIL_OPEN_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_EMAIL_OPEN_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_EMAIL_OPEN_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_EMAIL_OPEN_load_dttm
          AS PARTITION PF_EMAIL_OPEN_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..EMAIL_OPEN
      ADD CONSTRAINT EMAIL_OPEN_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_EMAIL_OPEN_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EMAIL_OPEN, EMAIL_OPEN);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..EMAIL_OPTOUT (
      test_flg char(1) NULL, properties_map_doc varchar(4000) NULL, email_optout_dttm_tz datetime2 NULL, email_optout_dttm datetime2 NULL, 
      load_dttm datetime2 NOT NULL, task_version_id varchar(36) NULL, subject_line_txt varchar(256) NULL, segment_id varchar(36) NULL, 
      recipient_domain_nm varchar(100) NULL, program_id varchar(50) NULL, optout_type_nm varchar(50) NULL, occurrence_id varchar(36) NULL, 
      link_tracking_label_txt varchar(256) NULL, link_tracking_group_txt varchar(256) NULL, journey_id varchar(36) NULL, identity_id varchar(36) NULL, 
      event_nm varchar(256) NULL, event_id varchar(36) NOT NULL, context_val varchar(256) NULL, channel_user_id varchar(300) NULL, 
      audience_id varchar(36) NULL, aud_occurrence_id varchar(36) NULL, analysis_group_id varchar(36) NULL, context_type_nm varchar(256) NULL, 
      event_designed_id varchar(36) NULL, imprint_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL, link_tracking_id varchar(4) NULL, 
      response_tracking_cd varchar(36) NULL, segment_version_id varchar(36) NULL, task_id varchar(36) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_EMAIL_OPTOUT_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_EMAIL_OPTOUT_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_EMAIL_OPTOUT_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_EMAIL_OPTOUT_load_dttm
          AS PARTITION PF_EMAIL_OPTOUT_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..EMAIL_OPTOUT
      ADD CONSTRAINT EMAIL_OPTOUT_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_EMAIL_OPTOUT_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EMAIL_OPTOUT, EMAIL_OPTOUT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..EMAIL_OPTOUT_DETAILS (
      test_flg char(1) NULL, properties_map_doc varchar(4000) NULL, email_action_dttm_tz datetime2 NULL, email_action_dttm datetime2 NULL, 
      load_dttm datetime2 NOT NULL, task_version_id varchar(36) NULL, subject_line_txt varchar(256) NULL, segment_id varchar(36) NULL, 
      recipient_domain_nm varchar(100) NULL, program_id varchar(50) NULL, optout_type_nm varchar(50) NULL, occurrence_id varchar(36) NULL, 
      journey_occurrence_id varchar(36) NULL, imprint_id varchar(36) NULL, event_nm varchar(256) NULL, event_designed_id varchar(36) NULL, 
      email_address varchar(300) NULL, context_val varchar(256) NULL, audience_id varchar(36) NULL, analysis_group_id varchar(36) NULL, 
      aud_occurrence_id varchar(36) NULL, context_type_nm varchar(256) NULL, event_id varchar(36) NOT NULL, identity_id varchar(36) NULL, 
      journey_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, segment_version_id varchar(36) NULL, task_id varchar(36) NULL
         )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_EMAIL_OPTOUT_DETAILS_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_EMAIL_OPTOUT_DETAILS_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_EMAIL_OPTOUT_DETAILS_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_EMAIL_OPTOUT_DETAILS_load_dttm
          AS PARTITION PF_EMAIL_OPTOUT_DETAILS_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..EMAIL_OPTOUT_DETAILS
      ADD CONSTRAINT EMAIL_OPTOUT_DETAILS_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_EMAIL_OPTOUT_DETAILS_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EMAIL_OPTOUT_DETAILS, EMAIL_OPTOUT_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..EMAIL_REPLY (
      test_flg char(1) NULL, properties_map_doc varchar(4000) NULL, email_reply_dttm datetime2 NULL, email_reply_dttm_tz datetime2 NULL, 
      load_dttm datetime2 NOT NULL, uri_txt varchar(1332) NULL, task_id varchar(36) NULL, subject_line_txt varchar(256) NULL, 
      segment_version_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, occurrence_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL, 
      imprint_id varchar(36) NULL, event_nm varchar(256) NULL, event_designed_id varchar(36) NULL, context_type_nm varchar(256) NULL, 
      audience_id varchar(36) NULL, analysis_group_id varchar(36) NULL, aud_occurrence_id varchar(36) NULL, channel_user_id varchar(300) NULL, 
      context_val varchar(256) NULL, event_id varchar(36) NOT NULL, identity_id varchar(36) NULL, journey_id varchar(36) NULL, 
      program_id varchar(50) NULL, recipient_domain_nm varchar(100) NULL, segment_id varchar(36) NULL, task_version_id varchar(36) NULL
         )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_EMAIL_REPLY_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_EMAIL_REPLY_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_EMAIL_REPLY_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_EMAIL_REPLY_load_dttm
          AS PARTITION PF_EMAIL_REPLY_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..EMAIL_REPLY
      ADD CONSTRAINT EMAIL_REPLY_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_EMAIL_REPLY_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EMAIL_REPLY, EMAIL_REPLY);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..EMAIL_SEND (
      test_flg char(1) NULL, properties_map_doc varchar(4000) NULL, email_send_dttm datetime2 NULL, email_send_dttm_tz datetime2 NULL, 
      load_dttm datetime2 NOT NULL, task_version_id varchar(36) NULL, subject_line_txt varchar(256) NULL, segment_id varchar(36) NULL, 
      recipient_domain_nm varchar(100) NULL, program_id varchar(50) NULL, journey_id varchar(36) NULL, imprint_id varchar(36) NULL, 
      event_nm varchar(256) NULL, event_designed_id varchar(36) NULL, context_type_nm varchar(256) NULL, channel_user_id varchar(300) NULL, 
      audience_id varchar(36) NULL, analysis_group_id varchar(36) NULL, aud_occurrence_id varchar(36) NULL, context_val varchar(256) NULL, 
      event_id varchar(36) NOT NULL, identity_id varchar(36) NULL, imprint_url_txt varchar(1332) NULL, journey_occurrence_id varchar(36) NULL, 
      occurrence_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, segment_version_id varchar(36) NULL, task_id varchar(36) NULL
         )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_EMAIL_SEND_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_EMAIL_SEND_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_EMAIL_SEND_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_EMAIL_SEND_load_dttm
          AS PARTITION PF_EMAIL_SEND_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..EMAIL_SEND
      ADD CONSTRAINT EMAIL_SEND_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_EMAIL_SEND_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EMAIL_SEND, EMAIL_SEND);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..EMAIL_VIEW (
      test_flg char(1) NULL, properties_map_doc varchar(4000) NULL, load_dttm datetime2 NOT NULL, email_view_dttm datetime2 NULL, 
      email_view_dttm_tz datetime2 NULL, task_version_id varchar(36) NULL, task_id varchar(36) NULL, subject_line_txt varchar(256) NULL, 
      segment_version_id varchar(36) NULL, segment_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, recipient_domain_nm varchar(100) NULL, 
      program_id varchar(50) NULL, occurrence_id varchar(36) NULL, link_tracking_id varchar(4) NULL, link_tracking_group_txt varchar(256) NULL, 
      journey_occurrence_id varchar(36) NULL, imprint_id varchar(36) NULL, event_nm varchar(256) NULL, event_designed_id varchar(36) NULL, 
      context_type_nm varchar(256) NULL, audience_id varchar(36) NULL, analysis_group_id varchar(36) NULL, aud_occurrence_id varchar(36) NULL, 
      channel_user_id varchar(300) NULL, context_val varchar(256) NULL, event_id varchar(36) NOT NULL, identity_id varchar(36) NULL, 
      journey_id varchar(36) NULL, link_tracking_label_txt varchar(256) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_EMAIL_VIEW_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_EMAIL_VIEW_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_EMAIL_VIEW_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_EMAIL_VIEW_load_dttm
          AS PARTITION PF_EMAIL_VIEW_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..EMAIL_VIEW
      ADD CONSTRAINT EMAIL_VIEW_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_EMAIL_VIEW_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EMAIL_VIEW, EMAIL_VIEW);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..EVENT_ERRORS (
      error_dttm_tz datetime2 NULL, error_dttm datetime2 NULL, ip_address varchar(64) NULL, event_source_cd varchar(100) NULL, 
      event_id varchar(36) NULL, error_cd varchar(65) NULL, error_txt varchar(4000) NULL, payload_txt varchar(4000) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EVENT_ERRORS, EVENT_ERRORS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..EXTERNAL_EVENT (
      properties_map_doc varchar(4000) NULL, external_event_dttm_tz datetime2 NULL, load_dttm datetime2 NOT NULL, external_event_dttm datetime2 NULL, 
      response_tracking_cd varchar(36) NULL, identity_id varchar(36) NULL, event_nm varchar(256) NULL, event_designed_id varchar(36) NULL, 
      context_type_nm varchar(256) NULL, channel_nm varchar(40) NULL, channel_user_id varchar(300) NULL, context_val varchar(256) NULL, 
      event_id varchar(36) NOT NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_EXTERNAL_EVENT_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_EXTERNAL_EVENT_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_EXTERNAL_EVENT_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_EXTERNAL_EVENT_load_dttm
          AS PARTITION PF_EXTERNAL_EVENT_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..EXTERNAL_EVENT
      ADD CONSTRAINT EXTERNAL_EVENT_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_EXTERNAL_EVENT_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: EXTERNAL_EVENT, EXTERNAL_EVENT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..FISCAL_CC_BUDGET (
      fin_accnt_obsolete_flg char(1) NULL, fp_obsolete_flg char(1) NULL, cc_obsolete_flg char(1) NULL, fp_start_dt date NULL, 
      fp_end_dt date NULL, cc_bdgt_cmtmnt_outstanding_amt decimal(17,2) NULL, cc_bdgt_invoiced_amt decimal(17,2) NULL, cc_bdgt_amt decimal(17,2) NULL, 
      cc_bdgt_cmtmnt_invoice_amt decimal(17,2) NULL, cc_bdgt_budget_amt decimal(17,2) NULL, cc_bdgt_committed_amt decimal(17,2) NULL, cc_bdgt_direct_invoice_amt decimal(17,2) NULL, 
      cc_bdgt_cmtmnt_overspent_amt decimal(17,2) NULL, cc_bdgt_cmtmnt_invoice_cnt int NULL, last_modified_dttm datetime2 NULL, created_dttm datetime2 NULL, 
      load_dttm datetime2 NULL, gen_ledger_cd varchar(128) NULL, last_modified_usernm varchar(128) NULL, fp_nm varchar(128) NULL, 
      fp_id varchar(128) NULL, fp_desc varchar(1332) NULL, fp_cls_ver varchar(128) NULL, fin_accnt_nm varchar(128) NULL, 
      cost_center_id varchar(128) NULL, cc_number varchar(128) NULL, cc_nm varchar(128) NULL, cc_bdgt_budget_desc varchar(1332) NULL, 
      cc_desc varchar(1332) NULL, cc_owner_usernm varchar(128) NULL, created_by_usernm varchar(128) NULL, fin_accnt_desc varchar(1332) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: FISCAL_CC_BUDGET, FISCAL_CC_BUDGET);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..FORM_DETAILS (
      submit_flg char(1) NULL, change_index_no int NULL, attempt_index_cnt int NULL, load_dttm datetime2 NOT NULL, 
      form_field_detail_dttm_tz datetime2 NULL, form_field_detail_dttm datetime2 NULL, visit_id varchar(32) NULL, form_field_nm varchar(325) NULL, 
      event_source_cd varchar(100) NULL, detail_id varchar(32) NULL, attempt_status_cd varchar(42) NULL, event_id varchar(36) NOT NULL, 
      form_field_value varchar(2600) NULL, form_nm varchar(65) NULL, session_id_hex varchar(29) NULL, detail_id_hex varchar(32) NULL, 
      event_key_cd varchar(100) NULL, form_field_id varchar(325) NULL, identity_id varchar(36) NULL, session_id varchar(29) NULL, 
      visit_id_hex varchar(32) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_FORM_DETAILS_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_FORM_DETAILS_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_FORM_DETAILS_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_FORM_DETAILS_load_dttm
          AS PARTITION PF_FORM_DETAILS_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..FORM_DETAILS
      ADD CONSTRAINT FORM_DETAILS_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_FORM_DETAILS_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: FORM_DETAILS, FORM_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..IDENTITY_ATTRIBUTES (
      processed_dttm datetime2 NULL, entrytime datetime2 NOT NULL, identity_id varchar(36) NULL, user_identifier_val varchar(5000) NOT NULL, 
      identifier_type_id varchar(36) NOT NULL    )) by &database.;
   execute (alter table &dbschema..IDENTITY_ATTRIBUTES
      add constraint IDENTITY_ATTRIBUTES_pk  primary key (ENTRYTIME,IDENTIFIER_TYPE_ID,USER_IDENTIFIER_VAL)) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: IDENTITY_ATTRIBUTES, IDENTITY_ATTRIBUTES);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..IDENTITY_MAP (
      processed_dttm datetime2 NULL, entrytime datetime2 NULL, target_identity_id varchar(36) NULL, source_identity_id varchar(36) NOT NULL
         )) by &database.;
   execute (alter table &dbschema..IDENTITY_MAP
      add constraint IDENTITY_MAP_pk  primary key (SOURCE_IDENTITY_ID)) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: IDENTITY_MAP, IDENTITY_MAP);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..IMPRESSION_DELIVERED (
      control_group_flg char(1) NULL, product_qty_no int NULL, properties_map_doc varchar(4000) NULL, impression_delivered_dttm_tz datetime2 NULL, 
      impression_delivered_dttm datetime2 NULL, load_dttm datetime2 NOT NULL, spot_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, 
      rec_group_id varchar(3) NULL, product_nm varchar(128) NULL, message_id varchar(36) NULL, event_nm varchar(256) NULL, 
      detail_id_hex varchar(32) NULL, context_val varchar(256) NULL, audience_id varchar(36) NULL, channel_user_id varchar(300) NULL, 
      creative_id varchar(36) NULL, event_id varchar(36) NOT NULL, identity_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL, 
      message_version_id varchar(36) NULL, mobile_app_id varchar(40) NULL, product_sku_no varchar(100) NULL, reserved_1_txt varchar(100) NULL, 
      segment_version_id varchar(36) NULL, task_version_id varchar(36) NULL, visit_id_hex varchar(32) NULL, aud_occurrence_id varchar(36) NULL, 
      channel_nm varchar(40) NULL, context_type_nm varchar(256) NULL, creative_version_id varchar(36) NULL, event_designed_id varchar(36) NULL, 
      event_key_cd varchar(100) NULL, event_source_cd varchar(100) NULL, journey_id varchar(36) NULL, product_id varchar(130) NULL, 
      request_id varchar(36) NULL, reserved_2_txt varchar(100) NULL, segment_id varchar(36) NULL, session_id_hex varchar(29) NULL, 
      task_id varchar(36) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_IMPRESSION_DELIVERED_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_IMPRESSION_DELIVERED_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_IMPRESSION_DELIVERED_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_IMPRESSION_DELIVERED_load_dttm
          AS PARTITION PF_IMPRESSION_DELIVERED_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..IMPRESSION_DELIVERED
      ADD CONSTRAINT IMPRESSION_DELIVERED_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_IMPRESSION_DELIVERED_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: IMPRESSION_DELIVERED, IMPRESSION_DELIVERED);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..IMPRESSION_SPOT_VIEWABLE (
      control_group_flg char(1) NULL, product_qty_no int NULL, properties_map_doc varchar(4000) NULL, impression_viewable_dttm_tz datetime2 NULL, 
      load_dttm datetime2 NOT NULL, impression_viewable_dttm datetime2 NULL, visit_id_hex varchar(32) NULL, session_id_hex varchar(29) NULL, 
      reserved_2_txt varchar(100) NULL, product_id varchar(128) NULL, message_id varchar(36) NULL, identity_id varchar(36) NULL, 
      event_id varchar(36) NOT NULL, creative_id varchar(36) NULL, channel_user_id varchar(300) NULL, analysis_group_id varchar(36) NULL, 
      audience_id varchar(36) NULL, context_val varchar(256) NULL, detail_id_hex varchar(32) NULL, event_nm varchar(256) NULL, 
      event_source_cd varchar(100) NULL, mobile_app_id varchar(40) NULL, rec_group_id varchar(3) NULL, request_id varchar(36) NULL, 
      segment_id varchar(36) NULL, task_id varchar(36) NULL, aud_occurrence_id varchar(36) NULL, channel_nm varchar(40) NULL, 
      context_type_nm varchar(256) NULL, creative_version_id varchar(36) NULL, event_designed_id varchar(36) NULL, event_key_cd varchar(100) NULL, 
      message_version_id varchar(36) NULL, occurrence_id varchar(36) NULL, product_nm varchar(128) NULL, product_sku_no varchar(100) NULL, 
      reserved_1_txt varchar(100) NULL, response_tracking_cd varchar(36) NULL, segment_version_id varchar(36) NULL, spot_id varchar(36) NULL, 
      task_version_id varchar(36) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_IMPRESSION_SPOT_VIEWABLE_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_IMPRESSION_SPOT_VIEWABLE_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_IMPRESSION_SPOT_VIEWABLE_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_IMPRESSION_SPOT_VIEWABLE_load_dttm
          AS PARTITION PF_IMPRESSION_SPOT_VIEWABLE_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..IMPRESSION_SPOT_VIEWABLE
      ADD CONSTRAINT IMPRESSION_SPOT_VIEWABLE_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_IMPRESSION_SPOT_VIEWABLE_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: IMPRESSION_SPOT_VIEWABLE, IMPRESSION_SPOT_VIEWABLE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..INVOICE_DETAILS (
      vendor_obsolete_flg char(1) NULL, invoice_amt decimal(17,2) NULL, vendor_amt decimal(17,2) NULL, reconcile_amt decimal(17,2) NULL, 
      last_modified_dttm datetime2 NULL, invoice_reconciled_dttm datetime2 NULL, payment_dttm datetime2 NULL, load_dttm datetime2 NULL, 
      created_dttm datetime2 NULL, invoice_created_dttm datetime2 NULL, vendor_nm varchar(128) NULL, planning_id varchar(128) NULL, 
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
   EXECUTE (CREATE TABLE &dbschema..INVOICE_LINE_ITEMS (
      item_vend_alloc_unit bigint NULL, item_qty bigint NULL, item_alloc_unit bigint NULL, vendor_obsolete_flg char(1) NULL, 
      cc_available_amt decimal(17,2) NULL, vendor_amt decimal(17,2) NULL, item_alloc_amt decimal(17,2) NULL, item_rate decimal(17,2) NULL, 
      cc_allocated_amt decimal(17,2) NULL, item_vend_alloc_amt decimal(17,2) NULL, cc_recon_alloc_amt decimal(17,2) NULL, reconcile_amt decimal(17,2) NULL, 
      invoice_amt decimal(17,2) NULL, item_number int NULL, payment_dttm datetime2 NULL, load_dttm datetime2 NULL, 
      invoice_created_dttm datetime2 NULL, invoice_reconciled_dttm datetime2 NULL, last_modified_dttm datetime2 NULL, created_dttm datetime2 NULL, 
      vendor_number varchar(128) NULL, vendor_desc varchar(1332) NULL, vendor_currency_cd varchar(10) NULL, reconcile_note varchar(1332) NULL, 
      planning_nm varchar(128) NULL, item_nm varchar(128) NULL, invoice_nm varchar(128) NULL, invoice_id varchar(128) NULL, 
      invoice_desc varchar(1332) NULL, fin_acc_nm varchar(128) NULL, cost_center_id varchar(128) NULL, cc_nm varchar(128) NULL, 
      ccat_nm varchar(128) NULL, cmtmnt_id varchar(128) NULL, plan_currency_cd varchar(10) NULL, vendor_id varchar(128) NULL, 
      cc_desc varchar(1332) NULL, cc_owner_usernm varchar(128) NULL, cmtmnt_nm varchar(128) NULL, created_by_usernm varchar(128) NULL, 
      fin_acc_ccat_nm varchar(128) NULL, gen_ledger_cd varchar(128) NULL, invoice_number varchar(128) NULL, invoice_status varchar(64) NULL, 
      last_modified_usernm varchar(128) NULL, planning_id varchar(128) NULL, vendor_nm varchar(128) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: INVOICE_LINE_ITEMS, INVOICE_LINE_ITEMS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..INVOICE_LINE_ITEMS_CCBDGT (
      cc_obsolete_flg char(1) NULL, vendor_obsolete_flg char(1) NULL, fp_obsolete_flg char(1) NULL, fp_start_dt date NULL, 
      fp_end_dt date NULL, cc_bdgt_cmtmnt_overspent_amt decimal(17,2) NULL, reconcile_amt decimal(17,2) NULL, vendor_amt decimal(17,2) NULL, 
      item_alloc_amt decimal(17,2) NULL, cc_bdgt_committed_amt decimal(17,2) NULL, cc_bdgt_invoiced_amt decimal(17,2) NULL, invoice_amt decimal(17,2) NULL, 
      cc_recon_alloc_amt decimal(17,2) NULL, cc_bdgt_direct_invoice_amt decimal(17,2) NULL, cc_available_amt decimal(17,2) NULL, cc_bdgt_cmtmnt_outstanding_amt decimal(17,2) NULL, 
      item_vend_alloc_amt decimal(17,2) NULL, item_rate decimal(17,2) NULL, cc_bdgt_amt decimal(17,2) NULL, cc_allocated_amt decimal(17,2) NULL, 
      cc_bdgt_budget_amt decimal(17,2) NULL, cc_bdgt_cmtmnt_invoice_amt decimal(17,2) NULL, item_qty int NULL, item_number int NULL, 
      item_vend_alloc_unit int NULL, cc_bdgt_cmtmnt_invoice_cnt int NULL, item_alloc_unit int NULL, created_dttm datetime2 NULL, 
      invoice_created_dttm datetime2 NULL, invoice_reconciled_dttm datetime2 NULL, last_modified_dttm datetime2 NULL, payment_dttm datetime2 NULL, 
      load_dttm datetime2 NULL, vendor_id varchar(128) NULL, reconcile_note varchar(1332) NULL, planning_nm varchar(128) NULL, 
      plan_currency_cd varchar(10) NULL, invoice_nm varchar(128) NULL, fp_nm varchar(128) NULL, fp_cls_ver varchar(128) NULL, 
      created_by_usernm varchar(128) NULL, ccat_nm varchar(128) NULL, cc_number varchar(128) NULL, cc_bdgt_budget_desc varchar(1332) NULL, 
      cc_desc varchar(1332) NULL, cc_owner_usernm varchar(128) NULL, cmtmnt_nm varchar(128) NULL, fin_acc_ccat_nm varchar(128) NULL, 
      invoice_desc varchar(1332) NULL, item_nm varchar(128) NULL, vendor_currency_cd varchar(10) NULL, vendor_number varchar(128) NULL, 
      cc_nm varchar(128) NULL, cmtmnt_id varchar(128) NULL, cost_center_id varchar(128) NULL, fin_acc_nm varchar(128) NULL, 
      fp_desc varchar(1332) NULL, fp_id varchar(128) NULL, gen_ledger_cd varchar(128) NULL, invoice_id varchar(128) NULL, 
      invoice_number varchar(128) NULL, invoice_status varchar(64) NULL, last_modified_usernm varchar(128) NULL, planning_id varchar(128) NULL, 
      vendor_desc varchar(1332) NULL, vendor_nm varchar(128) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: INVOICE_LINE_ITEMS_CCBDGT, INVOICE_LINE_ITEMS_CCBDGT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..IN_APP_FAILED (
      properties_map_doc varchar(4000) NULL, in_app_failed_dttm datetime2 NULL, in_app_failed_dttm_tz datetime2 NULL, load_dttm datetime2 NOT NULL, 
      task_version_id varchar(36) NULL, segment_id varchar(36) NULL, message_id varchar(36) NULL, identity_id varchar(36) NULL, 
      error_message_txt varchar(1332) NULL, context_val varchar(256) NULL, channel_user_id varchar(300) NULL, context_type_nm varchar(256) NULL, 
      creative_version_id varchar(36) NULL, event_id varchar(36) NOT NULL, mobile_app_id varchar(40) NULL, reserved_2_txt varchar(100) NULL, 
      spot_id varchar(36) NULL, channel_nm varchar(40) NULL, creative_id varchar(36) NULL, error_cd varchar(256) NULL, 
      event_designed_id varchar(36) NULL, event_nm varchar(256) NULL, message_version_id varchar(36) NULL, occurrence_id varchar(36) NULL, 
      reserved_1_txt varchar(100) NULL, response_tracking_cd varchar(36) NULL, segment_version_id varchar(36) NULL, task_id varchar(36) NULL
         )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_IN_APP_FAILED_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_IN_APP_FAILED_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_IN_APP_FAILED_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_IN_APP_FAILED_load_dttm
          AS PARTITION PF_IN_APP_FAILED_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..IN_APP_FAILED
      ADD CONSTRAINT IN_APP_FAILED_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_IN_APP_FAILED_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: IN_APP_FAILED, IN_APP_FAILED);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..IN_APP_MESSAGE (
      properties_map_doc varchar(4000) NULL, in_app_action_dttm_tz datetime2 NULL, load_dttm datetime2 NOT NULL, in_app_action_dttm datetime2 NULL, 
      segment_version_id varchar(36) NULL, reserved_2_txt varchar(100) NULL, mobile_app_id varchar(40) NULL, event_id varchar(36) NOT NULL, 
      context_val varchar(256) NULL, channel_user_id varchar(300) NULL, creative_version_id varchar(36) NULL, identity_id varchar(36) NULL, 
      message_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, task_id varchar(36) NULL, channel_nm varchar(40) NULL, 
      context_type_nm varchar(256) NULL, creative_id varchar(36) NULL, event_designed_id varchar(36) NULL, event_nm varchar(256) NULL, 
      message_version_id varchar(36) NULL, occurrence_id varchar(36) NULL, reserved_1_txt varchar(100) NULL, reserved_3_txt varchar(100) NULL, 
      segment_id varchar(36) NULL, spot_id varchar(36) NULL, task_version_id varchar(36) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_IN_APP_MESSAGE_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_IN_APP_MESSAGE_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_IN_APP_MESSAGE_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_IN_APP_MESSAGE_load_dttm
          AS PARTITION PF_IN_APP_MESSAGE_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..IN_APP_MESSAGE
      ADD CONSTRAINT IN_APP_MESSAGE_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_IN_APP_MESSAGE_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: IN_APP_MESSAGE, IN_APP_MESSAGE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..IN_APP_SEND (
      properties_map_doc varchar(4000) NULL, load_dttm datetime2 NOT NULL, in_app_send_dttm_tz datetime2 NULL, in_app_send_dttm datetime2 NULL, 
      task_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, occurrence_id varchar(36) NULL, message_id varchar(36) NULL, 
      event_nm varchar(256) NULL, creative_id varchar(36) NULL, channel_nm varchar(40) NULL, context_type_nm varchar(256) NULL, 
      creative_version_id varchar(36) NULL, event_designed_id varchar(36) NULL, message_version_id varchar(36) NULL, reserved_1_txt varchar(100) NULL, 
      segment_version_id varchar(36) NULL, channel_user_id varchar(300) NULL, context_val varchar(256) NULL, event_id varchar(36) NOT NULL, 
      identity_id varchar(36) NULL, mobile_app_id varchar(40) NULL, reserved_2_txt varchar(100) NULL, segment_id varchar(36) NULL, 
      spot_id varchar(36) NULL, task_version_id varchar(36) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_IN_APP_SEND_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_IN_APP_SEND_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_IN_APP_SEND_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_IN_APP_SEND_load_dttm
          AS PARTITION PF_IN_APP_SEND_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..IN_APP_SEND
      ADD CONSTRAINT IN_APP_SEND_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_IN_APP_SEND_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: IN_APP_SEND, IN_APP_SEND);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..IN_APP_TARGETING_REQUEST (
      eligibility_flg char(1) NULL, in_app_tgt_request_dttm datetime2 NULL, load_dttm datetime2 NOT NULL, in_app_tgt_request_dttm_tz datetime2 NULL, 
      event_id varchar(36) NOT NULL, context_type_nm varchar(256) NULL, channel_nm varchar(40) NULL, event_designed_id varchar(36) NULL, 
      identity_id varchar(36) NULL, mobile_app_id varchar(40) NULL, channel_user_id varchar(300) NULL, context_val varchar(256) NULL, 
      event_nm varchar(256) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_IN_APP_TARGETING_REQUEST_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_IN_APP_TARGETING_REQUEST_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_IN_APP_TARGETING_REQUEST_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_IN_APP_TARGETING_REQUEST_load_dttm
          AS PARTITION PF_IN_APP_TARGETING_REQUEST_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..IN_APP_TARGETING_REQUEST
      ADD CONSTRAINT IN_APP_TARGETING_REQUEST_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_IN_APP_TARGETING_REQUEST_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: IN_APP_TARGETING_REQUEST, IN_APP_TARGETING_REQUEST);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..JOURNEY_ENTRY (
      entry_dttm datetime2 NULL, entry_dttm_tz datetime2 NULL, load_dttm datetime2 NOT NULL, journey_occurrence_id varchar(36) NULL, 
      identity_id varchar(36) NULL, aud_occurrence_id varchar(36) NULL, audience_id varchar(36) NULL, context_type_nm varchar(256) NULL, 
      event_id varchar(36) NOT NULL, identity_type_val varchar(300) NULL, context_val varchar(256) NULL, event_nm varchar(256) NULL, 
      identity_type_nm varchar(100) NULL, journey_id varchar(36) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_JOURNEY_ENTRY_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_JOURNEY_ENTRY_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_JOURNEY_ENTRY_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_JOURNEY_ENTRY_load_dttm
          AS PARTITION PF_JOURNEY_ENTRY_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..JOURNEY_ENTRY
      ADD CONSTRAINT JOURNEY_ENTRY_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_JOURNEY_ENTRY_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: JOURNEY_ENTRY, JOURNEY_ENTRY);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..JOURNEY_EXIT (
      exit_dttm datetime2 NULL, exit_dttm_tz datetime2 NULL, load_dttm datetime2 NOT NULL, last_node_id varchar(36) NULL, 
      identity_type_nm varchar(100) NULL, context_type_nm varchar(256) NULL, aud_occurrence_id varchar(36) NULL, event_id varchar(36) NOT NULL, 
      group_id varchar(36) NULL, journey_id varchar(36) NULL, reason_cd varchar(100) NULL, audience_id varchar(36) NULL, 
      context_val varchar(256) NULL, event_nm varchar(256) NULL, identity_id varchar(36) NULL, identity_type_val varchar(300) NULL, 
      journey_occurrence_id varchar(36) NULL, reason_txt varchar(1000) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_JOURNEY_EXIT_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_JOURNEY_EXIT_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_JOURNEY_EXIT_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_JOURNEY_EXIT_load_dttm
          AS PARTITION PF_JOURNEY_EXIT_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..JOURNEY_EXIT
      ADD CONSTRAINT JOURNEY_EXIT_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_JOURNEY_EXIT_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: JOURNEY_EXIT, JOURNEY_EXIT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..JOURNEY_HOLDOUT (
      holdout_dttm_tz datetime2 NULL, load_dttm datetime2 NOT NULL, holdout_dttm datetime2 NULL, journey_occurrence_id varchar(36) NULL, 
      journey_id varchar(36) NULL, identity_type_val varchar(300) NULL, identity_type_nm varchar(100) NULL, identity_id varchar(36) NULL, 
      event_nm varchar(256) NULL, event_id varchar(36) NOT NULL, context_val varchar(256) NULL, context_type_nm varchar(256) NULL, 
      audience_id varchar(36) NULL, aud_occurrence_id varchar(36) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_JOURNEY_HOLDOUT_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_JOURNEY_HOLDOUT_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_JOURNEY_HOLDOUT_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_JOURNEY_HOLDOUT_load_dttm
          AS PARTITION PF_JOURNEY_HOLDOUT_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..JOURNEY_HOLDOUT
      ADD CONSTRAINT JOURNEY_HOLDOUT_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_JOURNEY_HOLDOUT_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: JOURNEY_HOLDOUT, JOURNEY_HOLDOUT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..JOURNEY_NODE_ENTRY (
      node_entry_dttm datetime2 NULL, load_dttm datetime2 NOT NULL, node_entry_dttm_tz datetime2 NULL, node_type_nm varchar(256) NULL, 
      node_id varchar(36) NULL, previous_node_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL, journey_id varchar(36) NULL, 
      identity_type_val varchar(300) NULL, identity_type_nm varchar(100) NULL, identity_id varchar(36) NULL, group_id varchar(36) NULL, 
      event_nm varchar(256) NULL, event_id varchar(36) NOT NULL, context_val varchar(256) NULL, context_type_nm varchar(256) NULL, 
      audience_id varchar(36) NULL, aud_occurrence_id varchar(36) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_JOURNEY_NODE_ENTRY_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_JOURNEY_NODE_ENTRY_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_JOURNEY_NODE_ENTRY_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_JOURNEY_NODE_ENTRY_load_dttm
          AS PARTITION PF_JOURNEY_NODE_ENTRY_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..JOURNEY_NODE_ENTRY
      ADD CONSTRAINT JOURNEY_NODE_ENTRY_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_JOURNEY_NODE_ENTRY_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: JOURNEY_NODE_ENTRY, JOURNEY_NODE_ENTRY);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..JOURNEY_SUCCESS (
      unit_qty int NULL, success_val int NULL, success_dttm datetime2 NULL, load_dttm datetime2 NOT NULL, 
      success_dttm_tz datetime2 NULL, parent_event_designed_id varchar(36) NULL, journey_id varchar(36) NULL, identity_type_nm varchar(100) NULL, 
      group_id varchar(36) NULL, event_id varchar(36) NOT NULL, context_type_nm varchar(256) NULL, audience_id varchar(36) NULL, 
      aud_occurrence_id varchar(36) NULL, context_val varchar(256) NULL, event_nm varchar(256) NULL, identity_id varchar(36) NULL, 
      identity_type_val varchar(300) NULL, journey_occurrence_id varchar(36) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_JOURNEY_SUCCESS_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_JOURNEY_SUCCESS_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_JOURNEY_SUCCESS_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_JOURNEY_SUCCESS_load_dttm
          AS PARTITION PF_JOURNEY_SUCCESS_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..JOURNEY_SUCCESS
      ADD CONSTRAINT JOURNEY_SUCCESS_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_JOURNEY_SUCCESS_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: JOURNEY_SUCCESS, JOURNEY_SUCCESS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..JOURNEY_SUPPRESSION (
      load_dttm datetime2 NOT NULL, suppression_dttm datetime2 NULL, suppression_dttm_tz datetime2 NULL, reason_txt varchar(1000) NULL, 
      reason_cd varchar(100) NULL, journey_occurrence_id varchar(36) NULL, identity_type_val varchar(300) NULL, identity_type_nm varchar(100) NULL, 
      identity_id varchar(36) NULL, event_id varchar(36) NOT NULL, context_type_nm varchar(256) NULL, audience_id varchar(36) NULL, 
      aud_occurrence_id varchar(36) NULL, context_val varchar(256) NULL, event_nm varchar(256) NULL, journey_id varchar(36) NULL
         )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_JOURNEY_SUPPRESSION_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_JOURNEY_SUPPRESSION_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_JOURNEY_SUPPRESSION_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_JOURNEY_SUPPRESSION_load_dttm
          AS PARTITION PF_JOURNEY_SUPPRESSION_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..JOURNEY_SUPPRESSION
      ADD CONSTRAINT JOURNEY_SUPPRESSION_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_JOURNEY_SUPPRESSION_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: JOURNEY_SUPPRESSION, JOURNEY_SUPPRESSION);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..JOURNEY_TEST_SUCCESS (
      success_dttm_tz datetime2 NULL, success_dttm datetime2 NULL, parent_event_designed_id varchar(36) NULL, journey_id varchar(36) NULL, 
      group_id varchar(36) NULL, event_nm varchar(256) NULL, event_id varchar(36) NOT NULL, context_type_nm varchar(256) NULL, 
      context_val varchar(256) NULL, identity_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL    )) by &database.;
   execute (alter table &dbschema..JOURNEY_TEST_SUCCESS
      add constraint JOURNEY_TEST_SUCCESS_pk  primary key (EVENT_ID)) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: JOURNEY_TEST_SUCCESS, JOURNEY_TEST_SUCCESS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_ACTIVITY (
      valid_to_dttm datetime2 NULL, last_published_dttm datetime2 NULL, valid_from_dttm datetime2 NULL, business_context_id varchar(36) NULL, 
      activity_version_id varchar(36) NULL, activity_status_cd varchar(20) NULL, activity_id varchar(36) NULL, activity_desc varchar(1332) NULL, 
      activity_cd varchar(60) NULL, activity_category_nm varchar(100) NULL, activity_nm varchar(60) NULL, folder_path_nm varchar(256) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY, MD_ACTIVITY);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_ACTIVITY_ABTESTPATH (
      next_node_val varchar(4000) NULL, abtest_dist_pct char(3) NULL, control_flg char(1) NULL, valid_to_dttm datetime2 NULL, 
      valid_from_dttm datetime2 NULL, activity_version_id varchar(36) NULL, activity_status_cd varchar(20) NULL, activity_id varchar(36) NULL, 
      abtest_path_id varchar(36) NULL, abtest_path_nm varchar(50) NULL, activity_node_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_ABTESTPATH, MD_ACTIVITY_ABTESTPATH);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_ACTIVITY_ABTESTPATH_ALL (
      next_node_val varchar(4000) NULL, abtest_dist_pct char(3) NULL, control_flg char(1) NULL, valid_to_dttm datetime2 NULL, 
      valid_from_dttm datetime2 NULL, activity_version_id varchar(36) NULL, activity_status_cd varchar(20) NULL, activity_node_id varchar(36) NULL, 
      activity_id varchar(36) NULL, abtest_path_nm varchar(50) NULL, abtest_path_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_ABTESTPATH_ALL, MD_ACTIVITY_ABTESTPATH_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_ACTIVITY_ALL (
      valid_to_dttm datetime2 NULL, valid_from_dttm datetime2 NULL, last_published_dttm datetime2 NULL, folder_path_nm varchar(256) NULL, 
      business_context_id varchar(36) NULL, activity_version_id varchar(36) NULL, activity_status_cd varchar(20) NULL, activity_nm varchar(60) NULL, 
      activity_id varchar(36) NULL, activity_desc varchar(1332) NULL, activity_cd varchar(60) NULL, activity_category_nm varchar(100) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_ALL, MD_ACTIVITY_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_ACTIVITY_CUSTOM_PROP (
      valid_to_dttm datetime2 NULL, valid_from_dttm datetime2 NULL, property_val varchar(1332) NULL, property_nm varchar(256) NULL, 
      property_datatype_cd varchar(256) NULL, activity_version_id varchar(36) NULL, activity_status_cd varchar(36) NULL, activity_id varchar(36) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_CUSTOM_PROP, MD_ACTIVITY_CUSTOM_PROP);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_ACTIVITY_CUSTOM_PROP_ALL (
      valid_to_dttm datetime2 NULL, valid_from_dttm datetime2 NULL, property_val varchar(1332) NULL, property_nm varchar(256) NULL, 
      property_datatype_cd varchar(256) NULL, activity_version_id varchar(36) NULL, activity_status_cd varchar(36) NULL, activity_id varchar(36) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_CUSTOM_PROP_ALL, MD_ACTIVITY_CUSTOM_PROP_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_ACTIVITY_NODE (
      next_node_val varchar(4000) NULL, previous_node_val varchar(4000) NULL, wait_tm bigint NULL, end_node_flg char(1) NULL, 
      time_boxed_flg char(1) NULL, specific_wait_flg char(1) NULL, start_node_flg char(1) NULL, node_sequence_no int NULL, 
      valid_from_dttm datetime2 NULL, valid_to_dttm datetime2 NULL, activity_version_id varchar(36) NULL, activity_status_cd varchar(20) NULL, 
      activity_node_type_nm varchar(100) NULL, activity_node_nm varchar(256) NULL, activity_node_id varchar(36) NULL, activity_id varchar(36) NULL, 
      abtest_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_NODE, MD_ACTIVITY_NODE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_ACTIVITY_NODE_ALL (
      next_node_val varchar(4000) NULL, previous_node_val varchar(4000) NULL, wait_tm bigint NULL, time_boxed_flg char(1) NULL, 
      end_node_flg char(1) NULL, specific_wait_flg char(1) NULL, start_node_flg char(1) NULL, node_sequence_no int NULL, 
      valid_to_dttm datetime2 NULL, valid_from_dttm datetime2 NULL, activity_version_id varchar(36) NULL, activity_status_cd varchar(20) NULL, 
      activity_node_type_nm varchar(100) NULL, activity_node_nm varchar(256) NULL, activity_node_id varchar(36) NULL, activity_id varchar(36) NULL, 
      abtest_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_NODE_ALL, MD_ACTIVITY_NODE_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_ACTIVITY_X_ACTIVITY_NODE (
      activity_version_id varchar(36) NULL, activity_status_cd varchar(20) NULL, activity_node_id varchar(36) NULL, activity_id varchar(36) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_X_ACTIVITY_NODE, MD_ACTIVITY_X_ACTIVITY_NODE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_ACTIVITY_X_ACTIVITY_NODE_ALL (
      activity_version_id varchar(36) NULL, activity_status_cd varchar(20) NULL, activity_node_id varchar(36) NULL, activity_id varchar(36) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_X_ACTIVITY_NODE_ALL, MD_ACTIVITY_X_ACTIVITY_NODE_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_ACTIVITY_X_TASK (
      task_version_id varchar(36) NULL, task_id varchar(36) NULL, activity_version_id varchar(36) NULL, activity_status_cd varchar(20) NULL, 
      activity_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_X_TASK, MD_ACTIVITY_X_TASK);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_ACTIVITY_X_TASK_ALL (
      task_version_id varchar(36) NULL, task_id varchar(36) NULL, activity_version_id varchar(36) NULL, activity_status_cd varchar(20) NULL, 
      activity_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ACTIVITY_X_TASK_ALL, MD_ACTIVITY_X_TASK_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_ASSET (
      valid_to_dttm datetime2 NULL, valid_from_dttm datetime2 NULL, last_published_dttm datetime2 NULL, owner_nm varchar(256) NULL, 
      created_user_nm varchar(256) NULL, asset_version_id varchar(36) NULL, asset_type_nm varchar(40) NULL, asset_status_cd varchar(20) NULL, 
      asset_nm varchar(256) NULL, asset_id varchar(36) NULL, asset_desc varchar(1332) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ASSET, MD_ASSET);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_ASSET_ALL (
      valid_to_dttm datetime2 NULL, valid_from_dttm datetime2 NULL, last_published_dttm datetime2 NULL, owner_nm varchar(256) NULL, 
      created_user_nm varchar(256) NULL, asset_version_id varchar(36) NULL, asset_type_nm varchar(40) NULL, asset_status_cd varchar(20) NULL, 
      asset_nm varchar(256) NULL, asset_id varchar(36) NULL, asset_desc varchar(1332) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_ASSET_ALL, MD_ASSET_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_AUDIENCE (
      audience_schedule_flg char(1) NULL, audience_expiration_val int NULL, update_dttm datetime2 NULL, create_dttm datetime2 NULL, 
      delete_dttm datetime2 NULL, created_user_nm varchar(256) NULL, audience_source_nm varchar(100) NULL, audience_nm varchar(128) NULL, 
      audience_id varchar(36) NULL, audience_desc varchar(1332) NULL, audience_data_source_nm varchar(100) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_AUDIENCE, MD_AUDIENCE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_AUDIENCE_OCCURRENCE (
      audience_size_val int NULL, update_dttm datetime2 NULL, end_tm datetime2 NULL, start_tm datetime2 NULL, 
      started_by_nm varchar(256) NULL, occurrence_type_nm varchar(100) NULL, execution_status_cd varchar(100) NULL, audience_id varchar(36) NULL, 
      aud_occurrence_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_AUDIENCE_OCCURRENCE, MD_AUDIENCE_OCCURRENCE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_AUDIENCE_X_SEGMENT (
      segment_id varchar(36) NULL, audience_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_AUDIENCE_X_SEGMENT, MD_AUDIENCE_X_SEGMENT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_BU (
      bu_obsolete_flg char(1) NULL, last_modified_dttm datetime2 NULL, created_dttm datetime2 NULL, load_dttm datetime2 NULL, 
      last_modified_usernm varchar(128) NULL, created_by_usernm varchar(128) NULL, bu_parentid varchar(128) NULL, bu_owner_usernm varchar(128) NULL, 
      bu_nm varchar(128) NULL, bu_id varchar(128) NULL, bu_desc varchar(1332) NULL, bu_currency_cd varchar(10) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_BU, MD_BU);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_BUSINESS_CONTEXT (
      valid_to_dttm datetime2 NULL, valid_from_dttm datetime2 NULL, last_published_dttm datetime2 NULL, owner_nm varchar(256) NULL, 
      locked_information_map_nm varchar(40) NULL, information_map_nm varchar(40) NULL, created_user_nm varchar(256) NULL, business_context_version_id varchar(36) NULL, 
      business_context_status_cd varchar(20) NULL, business_context_src_cd varchar(40) NULL, business_context_nm varchar(256) NULL, business_context_id varchar(36) NULL, 
      business_context_desc varchar(1332) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_BUSINESS_CONTEXT, MD_BUSINESS_CONTEXT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_BUSINESS_CONTEXT_ALL (
      valid_to_dttm datetime2 NULL, last_published_dttm datetime2 NULL, valid_from_dttm datetime2 NULL, owner_nm varchar(256) NULL, 
      locked_information_map_nm varchar(40) NULL, information_map_nm varchar(40) NULL, created_user_nm varchar(256) NULL, business_context_version_id varchar(36) NULL, 
      business_context_status_cd varchar(20) NULL, business_context_src_cd varchar(40) NULL, business_context_nm varchar(256) NULL, business_context_id varchar(36) NULL, 
      business_context_desc varchar(1332) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_BUSINESS_CONTEXT_ALL, MD_BUSINESS_CONTEXT_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_COSTCENTER (
      cc_obsolete_flg char(1) NULL, load_dttm datetime2 NULL, created_dttm datetime2 NULL, last_modified_dttm datetime2 NULL, 
      last_modified_usernm varchar(128) NULL, gen_ledger_cd varchar(128) NULL, fin_accnt_nm varchar(128) NULL, created_by_usernm varchar(128) NULL, 
      cost_center_id varchar(128) NULL, cc_owner_usernm varchar(128) NULL, cc_nm varchar(128) NULL, cc_desc varchar(1332) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_COSTCENTER, MD_COSTCENTER);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_COST_CATEGORY (
      ccat_obsolete_flg char(1) NULL, load_dttm datetime2 NULL, created_dttm datetime2 NULL, last_modified_dttm datetime2 NULL, 
      last_modified_usernm varchar(128) NULL, gen_ledger_cd varchar(128) NULL, fin_accnt_nm varchar(128) NULL, created_by_usernm varchar(128) NULL, 
      ccat_owner_usernm varchar(128) NULL, ccat_nm varchar(128) NULL, ccat_id varchar(128) NULL, ccat_desc varchar(1332) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_COST_CATEGORY, MD_COST_CATEGORY);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_CREATIVE (
      last_published_dttm datetime2 NULL, valid_to_dttm datetime2 NULL, valid_from_dttm datetime2 NULL, recommender_template_nm varchar(60) NULL, 
      recommender_template_id varchar(36) NULL, owner_nm varchar(256) NULL, folder_path_nm varchar(256) NULL, creative_version_id varchar(36) NULL, 
      creative_type_nm varchar(40) NULL, creative_txt varchar(1500) NULL, creative_status_cd varchar(20) NULL, creative_nm varchar(60) NULL, 
      creative_id varchar(36) NULL, creative_desc varchar(256) NULL, creative_cd varchar(60) NULL, creative_category_nm varchar(100) NULL, 
      created_user_nm varchar(256) NULL, business_context_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_CREATIVE, MD_CREATIVE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_CREATIVE_ALL (
      valid_to_dttm datetime2 NULL, valid_from_dttm datetime2 NULL, last_published_dttm datetime2 NULL, recommender_template_nm varchar(60) NULL, 
      recommender_template_id varchar(36) NULL, owner_nm varchar(256) NULL, folder_path_nm varchar(256) NULL, creative_version_id varchar(36) NULL, 
      creative_type_nm varchar(40) NULL, creative_txt varchar(1500) NULL, creative_status_cd varchar(20) NULL, creative_nm varchar(60) NULL, 
      creative_id varchar(36) NULL, creative_desc varchar(256) NULL, creative_cd varchar(60) NULL, creative_category_nm varchar(100) NULL, 
      created_user_nm varchar(256) NULL, business_context_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_CREATIVE_ALL, MD_CREATIVE_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_CREATIVE_CUSTOM_PROP (
      valid_to_dttm datetime2 NULL, valid_from_dttm datetime2 NULL, property_val varchar(1332) NULL, property_nm varchar(256) NULL, 
      property_datatype_cd varchar(256) NULL, creative_version_id varchar(36) NULL, creative_status_cd varchar(36) NULL, creative_id varchar(36) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_CREATIVE_CUSTOM_PROP, MD_CREATIVE_CUSTOM_PROP);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_CREATIVE_CUSTOM_PROP_ALL (
      valid_from_dttm datetime2 NULL, valid_to_dttm datetime2 NULL, property_val varchar(1332) NULL, property_nm varchar(256) NULL, 
      property_datatype_cd varchar(256) NULL, creative_version_id varchar(36) NULL, creative_status_cd varchar(36) NULL, creative_id varchar(36) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_CREATIVE_CUSTOM_PROP_ALL, MD_CREATIVE_CUSTOM_PROP_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_CREATIVE_X_ASSET (
      creative_version_id varchar(36) NULL, creative_status_cd varchar(20) NULL, creative_id varchar(36) NULL, asset_id varchar(36) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_CREATIVE_X_ASSET, MD_CREATIVE_X_ASSET);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_CREATIVE_X_ASSET_ALL (
      creative_version_id varchar(36) NULL, creative_status_cd varchar(20) NULL, creative_id varchar(36) NULL, asset_id varchar(36) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_CREATIVE_X_ASSET_ALL, MD_CREATIVE_X_ASSET_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_CUSTATTRIB_TABLE_VALUES (
      is_obsolete_flg char(1) NULL, load_dttm datetime2 NULL, last_modified_dttm datetime2 NULL, created_dttm datetime2 NULL, 
      table_val varchar(256) NULL, last_modified_usernm varchar(128) NULL, data_type varchar(32) NULL, data_formatter varchar(64) NULL, 
      created_by_usernm varchar(128) NULL, attr_nm varchar(128) NULL, attr_id varchar(128) NULL, attr_group_nm varchar(128) NULL, 
      attr_group_id varchar(128) NULL, attr_group_cd varchar(128) NULL, attr_cd varchar(128) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_CUSTATTRIB_TABLE_VALUES, MD_CUSTATTRIB_TABLE_VALUES);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_CUST_ATTRIB (
      is_grid_flg char(1) NULL, is_obsolete_flg char(1) NULL, load_dttm datetime2 NULL, last_modified_dttm datetime2 NULL, 
      created_dttm datetime2 NULL, remote_pklist_tab_col varchar(128) NULL, last_modified_usernm varchar(128) NULL, data_type varchar(32) NULL, 
      data_formatter varchar(64) NULL, created_by_usernm varchar(128) NULL, attr_nm varchar(128) NULL, attr_id varchar(128) NULL, 
      attr_group_nm varchar(128) NULL, attr_group_id varchar(128) NULL, attr_group_cd varchar(128) NULL, attr_cd varchar(128) NULL, 
      associated_grid varchar(128) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_CUST_ATTRIB, MD_CUST_ATTRIB);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_DATAVIEW (
      include_external_flg char(1) NULL, analytic_active_flg char(1) NULL, include_internal_flg char(1) NULL, max_path_time_val int NULL, 
      half_life_time_val int NULL, max_path_length_val int NULL, analytics_period_val int NULL, last_published_dttm datetime2 NULL, 
      valid_from_dttm datetime2 NULL, valid_to_dttm datetime2 NULL, selected_task_list varchar(1000) NULL, owner_nm varchar(256) NULL, 
      max_path_time_type_nm varchar(10) NULL, dataview_version_id varchar(36) NULL, dataview_status_cd varchar(20) NULL, dataview_nm varchar(60) NULL, 
      dataview_id varchar(36) NULL, dataview_desc varchar(1332) NULL, custom_recent_exclude_cd varchar(36) NULL, custom_recent_cd varchar(36) NULL, 
      created_user_nm varchar(256) NULL, analytics_period_type_nm varchar(10) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_DATAVIEW, MD_DATAVIEW);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_DATAVIEW_ALL (
      include_external_flg char(1) NULL, include_internal_flg char(1) NULL, analytic_active_flg char(1) NULL, max_path_length_val int NULL, 
      half_life_time_val int NULL, analytics_period_val int NULL, max_path_time_val int NULL, valid_to_dttm datetime2 NULL, 
      valid_from_dttm datetime2 NULL, last_published_dttm datetime2 NULL, selected_task_list varchar(1000) NULL, owner_nm varchar(256) NULL, 
      max_path_time_type_nm varchar(10) NULL, dataview_version_id varchar(36) NULL, dataview_status_cd varchar(20) NULL, dataview_nm varchar(60) NULL, 
      dataview_id varchar(36) NULL, dataview_desc varchar(1332) NULL, custom_recent_exclude_cd varchar(36) NULL, custom_recent_cd varchar(36) NULL, 
      created_user_nm varchar(256) NULL, analytics_period_type_nm varchar(10) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_DATAVIEW_ALL, MD_DATAVIEW_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_DATAVIEW_X_EVENT (
      event_id varchar(36) NULL, dataview_version_id varchar(36) NULL, dataview_status_cd varchar(20) NULL, dataview_id varchar(36) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_DATAVIEW_X_EVENT, MD_DATAVIEW_X_EVENT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_DATAVIEW_X_EVENT_ALL (
      event_id varchar(36) NULL, dataview_version_id varchar(36) NULL, dataview_status_cd varchar(20) NULL, dataview_id varchar(36) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_DATAVIEW_X_EVENT_ALL, MD_DATAVIEW_X_EVENT_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_EVENT (
      valid_from_dttm datetime2 NULL, last_published_dttm datetime2 NULL, valid_to_dttm datetime2 NULL, owner_nm varchar(256) NULL, 
      event_version_id varchar(36) NULL, event_type_nm varchar(40) NULL, event_subtype_nm varchar(100) NULL, event_status_cd varchar(20) NULL, 
      event_nm varchar(60) NULL, event_id varchar(36) NULL, event_desc varchar(1332) NULL, created_user_nm varchar(256) NULL, 
      channel_nm varchar(40) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_EVENT, MD_EVENT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_EVENT_ALL (
      last_published_dttm datetime2 NULL, valid_to_dttm datetime2 NULL, valid_from_dttm datetime2 NULL, owner_nm varchar(256) NULL, 
      event_version_id varchar(36) NULL, event_type_nm varchar(40) NULL, event_subtype_nm varchar(100) NULL, event_status_cd varchar(20) NULL, 
      event_nm varchar(60) NULL, event_id varchar(36) NULL, event_desc varchar(1332) NULL, created_user_nm varchar(256) NULL, 
      channel_nm varchar(40) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_EVENT_ALL, MD_EVENT_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_FISCAL_PERIOD (
      fp_obsolete_flg char(1) NULL, fp_end_dt date NULL, fp_start_dt date NULL, last_modified_dttm datetime2 NULL, 
      created_dttm datetime2 NULL, load_dttm datetime2 NULL, last_modified_usernm varchar(128) NULL, fp_nm varchar(128) NULL, 
      fp_id varchar(128) NULL, fp_desc varchar(1332) NULL, fp_cls_ver varchar(128) NULL, created_by_usernm varchar(128) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_FISCAL_PERIOD, MD_FISCAL_PERIOD);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_GRID_ATTR_DEFN (
      grid_mandatory_flg char(1) NULL, attr_obsolete_flg char(1) NULL, grid_obsolete_flg char(1) NULL, attr_order_no int NULL, 
      load_dttm datetime2 NULL, last_modified_dttm datetime2 NULL, created_dttm datetime2 NULL, remote_pklist_tab_col varchar(128) NULL, 
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
   EXECUTE (CREATE TABLE &dbschema..MD_JOURNEY (
      control_group_flg char(1) NULL, target_goal_qty int NULL, last_activated_dttm datetime2 NULL, test_type_nm varchar(40) NULL, 
      target_goal_type_nm varchar(20) NULL, purpose_id varchar(36) NULL, journey_version_id varchar(36) NULL, journey_status_cd varchar(20) NULL, 
      journey_nm varchar(256) NULL, journey_id varchar(36) NULL, created_user_nm varchar(256) NULL, activated_user_nm varchar(256) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY, MD_JOURNEY);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_JOURNEY_ALL (
      control_group_flg char(1) NULL, target_goal_qty int NULL, last_activated_dttm datetime2 NULL, test_type_nm varchar(40) NULL, 
      target_goal_type_nm varchar(20) NULL, purpose_id varchar(36) NULL, journey_version_id varchar(36) NULL, journey_status_cd varchar(20) NULL, 
      journey_nm varchar(256) NULL, journey_id varchar(36) NULL, created_user_nm varchar(256) NULL, activated_user_nm varchar(256) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_ALL, MD_JOURNEY_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_JOURNEY_NODE (
      previous_node_id varchar(36) NULL, node_type varchar(36) NULL, node_nm varchar(100) NULL, next_node_id varchar(36) NULL, 
      journey_version_id varchar(36) NULL, journey_node_id varchar(36) NULL, journey_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_NODE, MD_JOURNEY_NODE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_JOURNEY_NODE_OCCURRENCE (
      num_of_contacts_entered int NULL, end_dttm datetime2 NULL, start_dttm datetime2 NULL, journey_version_id varchar(36) NULL, 
      journey_occurrence_id varchar(36) NULL, journey_node_occurrence_id varchar(36) NULL, journey_node_id varchar(36) NULL, journey_id varchar(36) NULL, 
      group_id varchar(36) NULL, execution_status varchar(36) NULL, error_messages varchar(256) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_NODE_OCCURRENCE, MD_JOURNEY_NODE_OCCURRENCE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_JOURNEY_NODE_X_NEXT_NODE (
      next_node_id varchar(36) NULL, journey_node_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_NODE_X_NEXT_NODE, MD_JOURNEY_NODE_X_NEXT_NODE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_JOURNEY_NODE_X_PREVIOUS_NODE (
      previous_node_id varchar(36) NULL, journey_node_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_NODE_X_PREVIOUS_NODE, MD_JOURNEY_NODE_X_PREVIOUS_NODE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_JOURNEY_NODE_X_VARIANT (
      control_flg char(1) NULL, analysis_period_duration decimal(4,2) NULL, variant_dist_pct decimal(3,2) NULL, variant_nm varchar(256) NULL, 
      journey_node_id varchar(36) NULL, analysis_group_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_NODE_X_VARIANT, MD_JOURNEY_NODE_X_VARIANT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_JOURNEY_OCCURRENCE (
      num_of_contacts_entered int NULL, journey_occurrence_num int NULL, num_of_contacts_suppressed int NULL, start_dttm datetime2 NULL, 
      end_dttm datetime2 NULL, started_by_nm varchar(128) NULL, occurrence_type_nm varchar(36) NULL, journey_version_id varchar(36) NULL, 
      journey_occurrence_id varchar(36) NULL, journey_id varchar(36) NULL, execution_status varchar(36) NULL, error_messages varchar(256) NULL, 
      aud_occurrence_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_OCCURRENCE, MD_JOURNEY_OCCURRENCE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_JOURNEY_X_AUDIENCE (
      journey_version_id varchar(36) NULL, journey_node_id varchar(36) NULL, journey_id varchar(36) NULL, audience_id varchar(36) NULL, 
      aud_relationship_nm varchar(100) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_X_AUDIENCE, MD_JOURNEY_X_AUDIENCE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_JOURNEY_X_EVENT (
      journey_version_id varchar(36) NULL, journey_node_id varchar(36) NULL, journey_id varchar(36) NULL, event_relationship_nm varchar(100) NULL, 
      event_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_X_EVENT, MD_JOURNEY_X_EVENT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_JOURNEY_X_TASK (
      task_version_id varchar(36) NULL, task_id varchar(36) NULL, journey_version_id varchar(36) NULL, journey_node_id varchar(36) NULL, 
      journey_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_JOURNEY_X_TASK, MD_JOURNEY_X_TASK);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_MESSAGE (
      valid_to_dttm datetime2 NULL, valid_from_dttm datetime2 NULL, last_published_dttm datetime2 NULL, owner_nm varchar(256) NULL, 
      message_version_id varchar(36) NULL, message_type_nm varchar(40) NULL, message_status_cd varchar(20) NULL, message_nm varchar(60) NULL, 
      message_id varchar(36) NULL, message_desc varchar(1332) NULL, message_cd varchar(60) NULL, message_category_nm varchar(100) NULL, 
      folder_path_nm varchar(256) NULL, created_user_nm varchar(256) NULL, business_context_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_MESSAGE, MD_MESSAGE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_MESSAGE_ALL (
      valid_from_dttm datetime2 NULL, last_published_dttm datetime2 NULL, valid_to_dttm datetime2 NULL, owner_nm varchar(256) NULL, 
      message_version_id varchar(36) NULL, message_type_nm varchar(40) NULL, message_nm varchar(60) NULL, message_desc varchar(1332) NULL, 
      message_category_nm varchar(100) NULL, folder_path_nm varchar(256) NULL, message_cd varchar(60) NULL, message_id varchar(36) NULL, 
      message_status_cd varchar(20) NULL, created_user_nm varchar(256) NULL, business_context_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_MESSAGE_ALL, MD_MESSAGE_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_MESSAGE_CUSTOM_PROP (
      valid_from_dttm datetime2 NULL, valid_to_dttm datetime2 NULL, property_val varchar(1332) NULL, property_datatype_cd varchar(256) NULL, 
      message_status_cd varchar(36) NULL, message_id varchar(36) NULL, message_version_id varchar(36) NULL, property_nm varchar(256) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_MESSAGE_CUSTOM_PROP, MD_MESSAGE_CUSTOM_PROP);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_MESSAGE_CUSTOM_PROP_ALL (
      valid_to_dttm datetime2 NULL, valid_from_dttm datetime2 NULL, property_val varchar(1332) NULL, property_datatype_cd varchar(256) NULL, 
      message_status_cd varchar(36) NULL, message_id varchar(36) NULL, message_version_id varchar(36) NULL, property_nm varchar(256) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_MESSAGE_CUSTOM_PROP_ALL, MD_MESSAGE_CUSTOM_PROP_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_MESSAGE_X_CREATIVE (
      message_version_id varchar(36) NULL, message_id varchar(36) NULL, creative_id varchar(36) NULL, message_status_cd varchar(20) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_MESSAGE_X_CREATIVE, MD_MESSAGE_X_CREATIVE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_MESSAGE_X_CREATIVE_ALL (
      message_version_id varchar(36) NULL, message_id varchar(36) NULL, creative_id varchar(36) NULL, message_status_cd varchar(20) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_MESSAGE_X_CREATIVE_ALL, MD_MESSAGE_X_CREATIVE_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_OBJECT_TYPE (
      is_obsolete_flg char(1) NULL, created_dttm datetime2 NULL, last_modified_dttm datetime2 NULL, load_dttm datetime2 NULL, 
      object_type varchar(64) NULL, object_category varchar(64) NULL, last_modified_usernm varchar(128) NULL, data_type varchar(32) NULL, 
      data_formatter varchar(64) NULL, attr_nm varchar(128) NULL, attr_id varchar(128) NULL, attr_group_nm varchar(128) NULL, 
      attr_group_cd varchar(128) NULL, attr_cd varchar(128) NULL, attr_group_id varchar(128) NULL, created_by_usernm varchar(128) NULL, 
      remote_pklist_tab_col varchar(128) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_OBJECT_TYPE, MD_OBJECT_TYPE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_OCCURRENCE (
      occurrence_no int NULL, properties_map_doc varchar(4000) NULL, start_tm datetime2 NULL, end_tm datetime2 NULL, 
      started_by_nm varchar(100) NULL, occurrence_type_nm varchar(100) NULL, object_version_id varchar(36) NULL, object_id varchar(36) NULL, 
      execution_status_cd varchar(50) NULL, object_type_nm varchar(100) NULL, occurrence_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_OCCURRENCE, MD_OCCURRENCE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_PICKLIST (
      is_obsolete_flg char(1) NULL, last_modified_dttm datetime2 NULL, created_dttm datetime2 NULL, load_dttm datetime2 NULL, 
      plist_val varchar(256) NULL, plist_id varchar(128) NULL, plist_cd varchar(256) NULL, last_modified_usernm varchar(128) NULL, 
      created_by_usernm varchar(128) NULL, attr_id varchar(128) NULL, attr_group_id varchar(128) NULL, attr_cd varchar(128) NULL, 
      attr_group_nm varchar(128) NULL, attr_nm varchar(128) NULL, plist_desc varchar(1332) NULL, plist_nm varchar(128) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_PICKLIST, MD_PICKLIST);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_PURPOSE (
      purpose_nm varchar(256) NULL, purpose_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_PURPOSE, MD_PURPOSE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_RTC (
      occurrence_no int NULL, content_map_doc varchar(4000) NULL, rtc_dttm datetime2 NULL, task_id varchar(36) NULL, 
      segment_id varchar(36) NULL, rtc_id varchar(36) NULL, occurrence_id varchar(36) NULL, segment_version_id varchar(36) NULL, 
      task_version_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_RTC, MD_RTC);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT (
      last_published_dttm datetime2 NULL, valid_from_dttm datetime2 NULL, valid_to_dttm datetime2 NULL, segment_version_id varchar(36) NULL, 
      segment_status_cd varchar(20) NULL, segment_nm varchar(60) NULL, segment_map_id varchar(36) NULL, segment_id varchar(36) NULL, 
      segment_cd varchar(60) NULL, owner_nm varchar(256) NULL, folder_path_nm varchar(256) NULL, created_user_nm varchar(256) NULL, 
      business_context_id varchar(36) NULL, segment_category_nm varchar(100) NULL, segment_desc varchar(1332) NULL, segment_src_cd varchar(40) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT, MD_SEGMENT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_ALL (
      last_published_dttm datetime2 NULL, valid_from_dttm datetime2 NULL, valid_to_dttm datetime2 NULL, segment_version_id varchar(36) NULL, 
      segment_status_cd varchar(20) NULL, segment_nm varchar(60) NULL, segment_id varchar(36) NULL, segment_cd varchar(60) NULL, 
      segment_category_nm varchar(100) NULL, owner_nm varchar(256) NULL, folder_path_nm varchar(256) NULL, business_context_id varchar(36) NULL, 
      created_user_nm varchar(256) NULL, segment_desc varchar(1332) NULL, segment_map_id varchar(36) NULL, segment_src_cd varchar(40) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_ALL, MD_SEGMENT_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_CUSTOM_PROP (
      valid_to_dttm datetime2 NULL, valid_from_dttm datetime2 NULL, segment_version_id varchar(36) NULL, segment_status_cd varchar(36) NULL, 
      property_val varchar(1332) NULL, property_datatype_cd varchar(256) NULL, property_nm varchar(256) NULL, segment_id varchar(36) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_CUSTOM_PROP, MD_SEGMENT_CUSTOM_PROP);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_CUSTOM_PROP_ALL (
      valid_from_dttm datetime2 NULL, valid_to_dttm datetime2 NULL, segment_version_id varchar(36) NULL, segment_status_cd varchar(36) NULL, 
      property_val varchar(1332) NULL, property_datatype_cd varchar(256) NULL, property_nm varchar(256) NULL, segment_id varchar(36) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_CUSTOM_PROP_ALL, MD_SEGMENT_CUSTOM_PROP_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_MAP (
      scheduled_flg char(1) NULL, recurrence_day_of_month_no int NULL, valid_to_dttm datetime2 NULL, last_published_dttm datetime2 NULL, 
      valid_from_dttm datetime2 NULL, rec_scheduled_end_dttm datetime2 NULL, rec_scheduled_start_dttm datetime2 NULL, scheduled_end_dttm datetime2 NULL, 
      scheduled_start_dttm datetime2 NULL, segment_map_version_id varchar(36) NULL, segment_map_src_cd varchar(10) NULL, segment_map_nm varchar(60) NULL, 
      segment_map_id varchar(36) NULL, segment_map_cd varchar(60) NULL, segment_map_category_nm varchar(100) NULL, recurrence_monthly_type_nm varchar(36) NULL, 
      recurrence_frequency_cd varchar(36) NULL, recurrence_day_of_wk_ordinal_no varchar(36) NULL, recurrence_day_of_week_txt varchar(100) NULL, rec_scheduled_start_tm varchar(20) NULL, 
      owner_nm varchar(256) NULL, folder_path_nm varchar(256) NULL, business_context_id varchar(36) NULL, created_user_nm varchar(256) NULL, 
      recurrence_days_of_week_txt varchar(100) NULL, segment_map_desc varchar(1332) NULL, segment_map_status_cd varchar(20) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_MAP, MD_SEGMENT_MAP);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_MAP_ALL (
      scheduled_flg char(1) NULL, recurrence_day_of_month_no int NULL, rec_scheduled_start_dttm datetime2 NULL, valid_from_dttm datetime2 NULL, 
      rec_scheduled_end_dttm datetime2 NULL, scheduled_start_dttm datetime2 NULL, last_published_dttm datetime2 NULL, valid_to_dttm datetime2 NULL, 
      scheduled_end_dttm datetime2 NULL, segment_map_status_cd varchar(20) NULL, segment_map_nm varchar(60) NULL, segment_map_desc varchar(1332) NULL, 
      segment_map_category_nm varchar(100) NULL, recurrence_monthly_type_nm varchar(36) NULL, recurrence_days_of_week_txt varchar(100) NULL, recurrence_day_of_wk_ordinal_no varchar(36) NULL, 
      recurrence_day_of_week_txt varchar(100) NULL, rec_scheduled_start_tm varchar(20) NULL, owner_nm varchar(256) NULL, folder_path_nm varchar(256) NULL, 
      created_user_nm varchar(256) NULL, business_context_id varchar(36) NULL, recurrence_frequency_cd varchar(36) NULL, segment_map_cd varchar(60) NULL, 
      segment_map_id varchar(36) NULL, segment_map_src_cd varchar(10) NULL, segment_map_version_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_MAP_ALL, MD_SEGMENT_MAP_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_MAP_CUSTOM_PROP (
      valid_from_dttm datetime2 NULL, valid_to_dttm datetime2 NULL, segment_map_status_cd varchar(36) NULL, property_val varchar(1332) NULL, 
      property_nm varchar(256) NULL, property_datatype_cd varchar(256) NULL, segment_map_id varchar(36) NULL, segment_map_version_id varchar(36) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_MAP_CUSTOM_PROP, MD_SEGMENT_MAP_CUSTOM_PROP);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_MAP_CUSTOM_PROP_ALL (
      valid_from_dttm datetime2 NULL, valid_to_dttm datetime2 NULL, segment_map_status_cd varchar(36) NULL, property_val varchar(1332) NULL, 
      property_nm varchar(256) NULL, property_datatype_cd varchar(256) NULL, segment_map_id varchar(36) NULL, segment_map_version_id varchar(36) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_MAP_CUSTOM_PROP_ALL, MD_SEGMENT_MAP_CUSTOM_PROP_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_MAP_X_SEGMENT (
      segment_version_id varchar(36) NULL, segment_map_status_cd varchar(20) NULL, segment_id varchar(36) NULL, segment_map_id varchar(36) NULL, 
      segment_map_version_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_MAP_X_SEGMENT, MD_SEGMENT_MAP_X_SEGMENT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_MAP_X_SEGMENT_ALL (
      segment_map_version_id varchar(36) NULL, segment_map_status_cd varchar(20) NULL, segment_map_id varchar(36) NULL, segment_id varchar(36) NULL, 
      segment_version_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_MAP_X_SEGMENT_ALL, MD_SEGMENT_MAP_X_SEGMENT_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_TEST (
      test_enabled_flg char(1) NULL, stratified_sampling_flg char(1) NULL, test_pct decimal(5,2) NULL, test_cnt int NULL, 
      test_type_nm varchar(10) NULL, test_sizing_type_nm varchar(65) NULL, test_nm varchar(65) NULL, test_cd varchar(60) NULL, 
      task_id varchar(36) NULL, stratified_samp_criteria_txt varchar(1024) NULL, task_version_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_TEST, MD_SEGMENT_TEST);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_TEST_ALL (
      test_enabled_flg char(1) NULL, stratified_sampling_flg char(1) NULL, test_pct decimal(5,2) NULL, test_cnt int NULL, 
      test_type_nm varchar(10) NULL, test_sizing_type_nm varchar(65) NULL, test_nm varchar(65) NULL, task_version_id varchar(36) NULL, 
      task_id varchar(36) NULL, stratified_samp_criteria_txt varchar(1024) NULL, test_cd varchar(60) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_TEST_ALL, MD_SEGMENT_TEST_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_TEST_X_SEGMENT (
      task_version_id varchar(36) NULL, segment_id varchar(36) NULL, task_id varchar(36) NULL, test_cd varchar(60) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_TEST_X_SEGMENT, MD_SEGMENT_TEST_X_SEGMENT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_TEST_X_SEGMENT_ALL (
      test_cd varchar(60) NULL, task_version_id varchar(36) NULL, segment_id varchar(36) NULL, task_id varchar(36) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_TEST_X_SEGMENT_ALL, MD_SEGMENT_TEST_X_SEGMENT_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_X_EVENT (
      segment_status_cd varchar(20) NULL, event_id varchar(36) NULL, segment_id varchar(36) NULL, segment_version_id varchar(36) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_X_EVENT, MD_SEGMENT_X_EVENT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_SEGMENT_X_EVENT_ALL (
      segment_version_id varchar(36) NULL, segment_status_cd varchar(20) NULL, event_id varchar(36) NULL, segment_id varchar(36) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SEGMENT_X_EVENT_ALL, MD_SEGMENT_X_EVENT_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_SPOT (
      multi_page_flg char(1) NULL, location_selector_flg char(1) NULL, last_published_dttm datetime2 NULL, valid_from_dttm datetime2 NULL, 
      valid_to_dttm datetime2 NULL, spot_width_val_no varchar(10) NULL, spot_type_nm varchar(40) NULL, spot_nm varchar(60) NULL, 
      spot_id varchar(36) NULL, spot_desc varchar(1332) NULL, owner_nm varchar(256) NULL, dimension_label_txt varchar(156) NULL, 
      channel_nm varchar(40) NULL, created_user_nm varchar(256) NULL, height_width_ratio_val_txt varchar(25) NULL, spot_height_val_no varchar(10) NULL, 
      spot_key varchar(40) NULL, spot_status_cd varchar(20) NULL, spot_version_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SPOT, MD_SPOT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_SPOT_ALL (
      multi_page_flg char(1) NULL, location_selector_flg char(1) NULL, valid_from_dttm datetime2 NULL, valid_to_dttm datetime2 NULL, 
      last_published_dttm datetime2 NULL, spot_version_id varchar(36) NULL, spot_status_cd varchar(20) NULL, spot_nm varchar(60) NULL, 
      spot_key varchar(40) NULL, spot_height_val_no varchar(10) NULL, owner_nm varchar(256) NULL, height_width_ratio_val_txt varchar(25) NULL, 
      created_user_nm varchar(256) NULL, channel_nm varchar(40) NULL, dimension_label_txt varchar(156) NULL, spot_desc varchar(1332) NULL, 
      spot_id varchar(36) NULL, spot_type_nm varchar(40) NULL, spot_width_val_no varchar(10) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_SPOT_ALL, MD_SPOT_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TARGET_ASSIST (
      use_targeting_flg char(1) NULL, threshold_type_nm char(30) NULL, percent_target_population_size int NULL, last_modified_dttm datetime2 NULL, 
      model_available_dttm datetime2 NULL, task_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TARGET_ASSIST, MD_TARGET_ASSIST);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK (
      activity_flg char(1) NULL, export_template_flg char(1) NULL, scheduled_flg char(1) NULL, rtdm_flg char(1) NULL, 
      use_modeling_flg char(1) NULL, recurring_schedule_flg char(1) NULL, segment_tests_flg char(1) NULL, impressions_life_time_cnt int NULL, 
      test_duration int NULL, limit_period_unit_cnt int NULL, impressions_per_session_cnt int NULL, display_priority_no int NULL, 
      recurrence_day_of_month_no int NULL, maximum_period_expression_cnt int NULL, impressions_qty_period_cnt int NULL, valid_from_dttm datetime2 NULL, 
      scheduled_start_dttm datetime2 NULL, last_published_dttm datetime2 NULL, valid_to_dttm datetime2 NULL, rec_scheduled_start_dttm datetime2 NULL, 
      model_start_dttm datetime2 NULL, scheduled_end_dttm datetime2 NULL, rec_scheduled_end_dttm datetime2 NULL, task_version_id varchar(36) NULL, 
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
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_ALL (
      use_modeling_flg char(1) NULL, activity_flg char(1) NULL, segment_tests_flg char(1) NULL, export_template_flg char(1) NULL, 
      recurring_schedule_flg char(1) NULL, rtdm_flg char(1) NULL, scheduled_flg char(1) NULL, recurrence_day_of_month_no int NULL, 
      impressions_per_session_cnt int NULL, test_duration int NULL, display_priority_no int NULL, limit_period_unit_cnt int NULL, 
      impressions_qty_period_cnt int NULL, maximum_period_expression_cnt int NULL, impressions_life_time_cnt int NULL, last_published_dttm datetime2 NULL, 
      model_start_dttm datetime2 NULL, rec_scheduled_end_dttm datetime2 NULL, rec_scheduled_start_dttm datetime2 NULL, scheduled_end_dttm datetime2 NULL, 
      valid_from_dttm datetime2 NULL, scheduled_start_dttm datetime2 NULL, valid_to_dttm datetime2 NULL, template_id varchar(36) NULL, 
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
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_CUSTOM_PROP (
      valid_from_dttm datetime2 NULL, valid_to_dttm datetime2 NULL, task_status_cd varchar(36) NULL, task_id varchar(36) NULL, 
      property_val varchar(1332) NULL, property_datatype_nm varchar(256) NULL, property_nm varchar(256) NULL, task_version_id varchar(36) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_CUSTOM_PROP, MD_TASK_CUSTOM_PROP);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_CUSTOM_PROP_ALL (
      valid_from_dttm datetime2 NULL, valid_to_dttm datetime2 NULL, task_version_id varchar(36) NULL, task_status_cd varchar(36) NULL, 
      task_id varchar(36) NULL, property_val varchar(1332) NULL, property_datatype_nm varchar(256) NULL, property_nm varchar(256) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_CUSTOM_PROP_ALL, MD_TASK_CUSTOM_PROP_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_AUDIENCE (
      audience_id varchar(36) NULL, task_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_AUDIENCE, MD_TASK_X_AUDIENCE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_CREATIVE (
      variant_nm varchar(256) NULL, task_version_id varchar(36) NULL, task_id varchar(36) NULL, creative_id varchar(36) NULL, 
      arbitration_method_cd varchar(36) NULL, arbitration_method_val varchar(3) NULL, spot_id varchar(36) NULL, task_status_cd varchar(20) NULL, 
      variant_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_CREATIVE, MD_TASK_X_CREATIVE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_CREATIVE_ALL (
      variant_nm varchar(256) NULL, variant_id varchar(36) NULL, task_status_cd varchar(20) NULL, spot_id varchar(36) NULL, 
      arbitration_method_val varchar(3) NULL, arbitration_method_cd varchar(36) NULL, creative_id varchar(36) NULL, task_id varchar(36) NULL, 
      task_version_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_CREATIVE_ALL, MD_TASK_X_CREATIVE_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_DATAVIEW (
      secondary_metric_flg char(1) NULL, primary_metric_flg char(1) NULL, targeting_flg char(1) NULL, task_version_id varchar(36) NULL, 
      task_id varchar(36) NULL, dataview_id varchar(36) NULL, task_status_cd varchar(20) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_DATAVIEW, MD_TASK_X_DATAVIEW);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_DATAVIEW_ALL (
      secondary_metric_flg char(1) NULL, targeting_flg char(1) NULL, primary_metric_flg char(1) NULL, task_status_cd varchar(20) NULL, 
      task_id varchar(36) NULL, dataview_id varchar(36) NULL, task_version_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_DATAVIEW_ALL, MD_TASK_X_DATAVIEW_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_EVENT (
      secondary_metric_flg char(1) NULL, targeting_flg char(1) NULL, primary_metric_flg char(1) NULL, task_version_id varchar(36) NULL, 
      task_id varchar(36) NULL, event_id varchar(36) NULL, task_status_cd varchar(20) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_EVENT, MD_TASK_X_EVENT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_EVENT_ALL (
      secondary_metric_flg char(1) NULL, targeting_flg char(1) NULL, primary_metric_flg char(1) NULL, task_status_cd varchar(20) NULL, 
      task_id varchar(36) NULL, event_id varchar(36) NULL, task_version_id varchar(36) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_EVENT_ALL, MD_TASK_X_EVENT_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_MESSAGE (
      task_version_id varchar(36) NULL, task_status_cd varchar(20) NULL, message_id varchar(36) NULL, task_id varchar(36) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_MESSAGE, MD_TASK_X_MESSAGE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_MESSAGE_ALL (
      task_status_cd varchar(20) NULL, message_id varchar(36) NULL, task_id varchar(36) NULL, task_version_id varchar(36) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_MESSAGE_ALL, MD_TASK_X_MESSAGE_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_SEGMENT (
      task_version_id varchar(36) NULL, task_status_cd varchar(20) NULL, segment_id varchar(36) NULL, task_id varchar(36) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_SEGMENT, MD_TASK_X_SEGMENT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_SEGMENT_ALL (
      task_status_cd varchar(20) NULL, segment_id varchar(36) NULL, task_id varchar(36) NULL, task_version_id varchar(36) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_SEGMENT_ALL, MD_TASK_X_SEGMENT_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_SPOT (
      task_status_cd varchar(20) NULL, spot_id varchar(36) NULL, task_id varchar(36) NULL, task_version_id varchar(36) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_SPOT, MD_TASK_X_SPOT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_SPOT_ALL (
      task_version_id varchar(36) NULL, task_status_cd varchar(20) NULL, spot_id varchar(36) NULL, task_id varchar(36) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_SPOT_ALL, MD_TASK_X_SPOT_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_VARIANT (
      variant_type_nm varchar(100) NULL, variant_nm varchar(256) NULL, task_status_cd varchar(20) NULL, analysis_group_id varchar(36) NULL, 
      task_id varchar(36) NULL, task_version_id varchar(36) NULL, variant_source_nm varchar(100) NULL, variant_val varchar(1332) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_VARIANT, MD_TASK_X_VARIANT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_TASK_X_VARIANT_ALL (
      variant_type_nm varchar(100) NULL, variant_nm varchar(256) NULL, task_status_cd varchar(20) NULL, analysis_group_id varchar(36) NULL, 
      task_id varchar(36) NULL, task_version_id varchar(36) NULL, variant_source_nm varchar(100) NULL, variant_val varchar(1332) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_TASK_X_VARIANT_ALL, MD_TASK_X_VARIANT_ALL);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_VENDOR (
      is_obsolete_flg char(1) NULL, last_modified_dttm datetime2 NULL, created_dttm datetime2 NULL, load_dttm datetime2 NULL, 
      vendor_number varchar(128) NULL, vendor_nm varchar(128) NULL, vendor_desc varchar(1332) NULL, owner_usernm varchar(128) NULL, 
      last_modified_usernm varchar(128) NULL, created_by_usernm varchar(128) NULL, vendor_currency_cd varchar(10) NULL, vendor_id varchar(128) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_VENDOR, MD_VENDOR);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_WF_PROCESS_DEF (
      version_num bigint NULL, file_tobecatlgd_flg char(1) NULL, default_approval_flg char(1) NULL, buildin_template_flg char(1) NULL, 
      latest_version_flg char(1) NULL, last_modified_dttm datetime2 NULL, load_dttm datetime2 NULL, created_dttm datetime2 NULL, 
      pdef_state varchar(128) NULL, pdef_nm varchar(128) NULL, pdef_id varchar(128) NULL, owner_usernm varchar(128) NULL, 
      last_modified_usernm varchar(128) NULL, engine_pdef_key varchar(128) NULL, engine_pdef_id varchar(128) NULL, created_by_usernm varchar(128) NULL, 
      associated_object_type varchar(128) NULL, pdef_desc varchar(1332) NULL, pdef_type varchar(128) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_WF_PROCESS_DEF, MD_WF_PROCESS_DEF);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_WF_PROCESS_DEF_ATTR_GRP (
      load_dttm datetime2 NULL, pdef_id varchar(128) NULL, attr_group_id varchar(128) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_WF_PROCESS_DEF_ATTR_GRP, MD_WF_PROCESS_DEF_ATTR_GRP);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_WF_PROCESS_DEF_CATEGORIES (
      default_category_flg char(1) NULL, load_dttm datetime2 NULL, pdef_id varchar(128) NULL, category_type varchar(128) NULL, 
      category_id varchar(128) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_WF_PROCESS_DEF_CATEGORIES, MD_WF_PROCESS_DEF_CATEGORIES);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_WF_PROCESS_DEF_TASKS (
      default_duration_perassignee bigint NULL, show_sourceitemlink_flg char(1) NULL, resp_file_enabled_flg char(1) NULL, show_workflowlink_flg char(1) NULL, 
      resp_enabled_flg char(1) NULL, url_enabled_flg char(1) NULL, is_sequential_flg char(1) NULL, file_mandatory_flg char(1) NULL, 
      ciobject_enabled_flg char(1) NULL, multiple_asgnsuprt_flg char(1) NULL, comment_mandatory_flg char(1) NULL, comment_enabled_flg char(1) NULL, 
      file_enabled_flg char(1) NULL, outgoing_flow_flg char(1) NULL, res_mandatory_flg char(1) NULL, load_dttm datetime2 NULL, 
      task_type varchar(128) NULL, task_subtype varchar(128) NULL, task_instruction varchar(128) NULL, task_desc varchar(1332) NULL, 
      source_item_field varchar(128) NULL, predecessor_task_id varchar(128) NULL, pdef_id varchar(128) NULL, item_approval_state varchar(128) NULL, 
      assignee_type varchar(128) NULL, task_id varchar(128) NULL, task_nm varchar(128) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_WF_PROCESS_DEF_TASKS, MD_WF_PROCESS_DEF_TASKS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MD_WF_PROCESS_DEF_TASK_ASSG (
      load_dttm datetime2 NULL, pdef_id varchar(128) NULL, assignee_type varchar(128) NULL, assignee_id varchar(128) NULL, 
      assignee_duration varchar(128) NULL, assignee_instruction varchar(128) NULL, task_id varchar(128) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MD_WF_PROCESS_DEF_TASK_ASSG, MD_WF_PROCESS_DEF_TASK_ASSG);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MEDIA_ACTIVITY_DETAILS (
      action_dttm datetime2 NULL, action_dttm_tz datetime2 NULL, load_dttm datetime2 NOT NULL, playhead_position varchar(50) NULL, 
      media_nm varchar(260) NULL, event_id varchar(36) NOT NULL, detail_id varchar(32) NULL, action varchar(50) NULL, 
      detail_id_hex varchar(32) NULL, media_uri_txt varchar(2024) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_MEDIA_ACTIVITY_DETAILS_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_MEDIA_ACTIVITY_DETAILS_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_MEDIA_ACTIVITY_DETAILS_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_MEDIA_ACTIVITY_DETAILS_load_dttm
          AS PARTITION PF_MEDIA_ACTIVITY_DETAILS_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..MEDIA_ACTIVITY_DETAILS
      ADD CONSTRAINT MEDIA_ACTIVITY_DETAILS_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_MEDIA_ACTIVITY_DETAILS_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MEDIA_ACTIVITY_DETAILS, MEDIA_ACTIVITY_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MEDIA_DETAILS (
      media_duration_secs decimal(11,3) NULL, load_dttm datetime2 NOT NULL, play_start_dttm_tz datetime2 NULL, play_start_dttm datetime2 NULL, 
      visit_id_hex varchar(32) NULL, visit_id varchar(32) NULL, session_id_hex varchar(29) NULL, session_id varchar(29) NULL, 
      media_uri_txt varchar(2024) NULL, media_player_nm varchar(30) NULL, media_nm varchar(260) NULL, identity_id varchar(36) NULL, 
      event_key_cd varchar(100) NULL, detail_id_hex varchar(32) NULL, detail_id varchar(32) NULL, event_id varchar(36) NOT NULL, 
      event_source_cd varchar(100) NULL, media_player_version_txt varchar(20) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_MEDIA_DETAILS_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_MEDIA_DETAILS_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_MEDIA_DETAILS_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_MEDIA_DETAILS_load_dttm
          AS PARTITION PF_MEDIA_DETAILS_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..MEDIA_DETAILS
      ADD CONSTRAINT MEDIA_DETAILS_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_MEDIA_DETAILS_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MEDIA_DETAILS, MEDIA_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MEDIA_DETAILS_EXT (
      media_display_duration_secs decimal(11,3) NULL, view_duration_secs decimal(11,3) NULL, end_tm decimal(11,3) NULL, start_tm decimal(11,3) NULL, 
      exit_point_secs decimal(11,3) NULL, max_play_secs decimal(11,3) NULL, interaction_cnt int NULL, play_end_dttm datetime2 NULL, 
      play_end_dttm_tz datetime2 NULL, load_dttm datetime2 NOT NULL, media_uri_txt varchar(2024) NULL, media_nm varchar(260) NULL, 
      event_id varchar(36) NOT NULL, detail_id_hex varchar(32) NULL, detail_id varchar(32) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_MEDIA_DETAILS_EXT_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_MEDIA_DETAILS_EXT_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_MEDIA_DETAILS_EXT_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_MEDIA_DETAILS_EXT_load_dttm
          AS PARTITION PF_MEDIA_DETAILS_EXT_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..MEDIA_DETAILS_EXT
      ADD CONSTRAINT MEDIA_DETAILS_EXT_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_MEDIA_DETAILS_EXT_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MEDIA_DETAILS_EXT, MEDIA_DETAILS_EXT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MOBILE_FOCUS_DEFOCUS (
      action_dttm_tz datetime2 NULL, action_dttm datetime2 NULL, load_dttm datetime2 NOT NULL, visit_id_hex varchar(32) NULL, 
      session_id_hex varchar(29) NULL, reserved_1_txt varchar(100) NULL, mobile_app_id varchar(40) NULL, identity_id varchar(36) NULL, 
      event_nm varchar(256) NULL, event_designed_id varchar(36) NULL, detail_id_hex varchar(32) NULL, channel_user_id varchar(300) NULL, 
      event_id varchar(36) NOT NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_MOBILE_FOCUS_DEFOCUS_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_MOBILE_FOCUS_DEFOCUS_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_MOBILE_FOCUS_DEFOCUS_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_MOBILE_FOCUS_DEFOCUS_load_dttm
          AS PARTITION PF_MOBILE_FOCUS_DEFOCUS_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..MOBILE_FOCUS_DEFOCUS
      ADD CONSTRAINT MOBILE_FOCUS_DEFOCUS_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_MOBILE_FOCUS_DEFOCUS_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MOBILE_FOCUS_DEFOCUS, MOBILE_FOCUS_DEFOCUS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MOBILE_SPOTS (
      action_dttm_tz datetime2 NULL, action_dttm datetime2 NULL, load_dttm datetime2 NOT NULL, visit_id_hex varchar(32) NULL, 
      spot_id varchar(36) NULL, session_id_hex varchar(29) NULL, mobile_app_id varchar(40) NULL, event_nm varchar(256) NULL, 
      event_designed_id varchar(36) NULL, detail_id_hex varchar(32) NULL, creative_id varchar(36) NULL, context_type_nm varchar(256) NULL, 
      channel_user_id varchar(300) NULL, context_val varchar(256) NULL, event_id varchar(36) NOT NULL, identity_id varchar(36) NULL
         )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_MOBILE_SPOTS_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_MOBILE_SPOTS_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_MOBILE_SPOTS_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_MOBILE_SPOTS_load_dttm
          AS PARTITION PF_MOBILE_SPOTS_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..MOBILE_SPOTS
      ADD CONSTRAINT MOBILE_SPOTS_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_MOBILE_SPOTS_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MOBILE_SPOTS, MOBILE_SPOTS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..MONTHLY_USAGE (
      api_usage_str varchar(4000) NULL, customer_profiles_processed_str varchar(4000) NULL, bc_subjcnt_str varchar(4000) NULL, web_sesn_cnt bigint NULL, 
      google_ads_cnt bigint NULL, dm_destinations_total_id_cnt bigint NULL, web_impr_cnt bigint NULL, linkedin_ads_cnt bigint NULL, 
      mob_sesn_cnt bigint NULL, plan_users_cnt bigint NULL, mob_impr_cnt bigint NULL, mobile_push_cnt bigint NULL, 
      dm_destinations_total_row_cnt bigint NULL, audience_usage_cnt bigint NULL, mobile_in_app_msg_cnt bigint NULL, email_preview_cnt bigint NULL, 
      outbound_api_cnt bigint NULL, facebook_ads_cnt bigint NULL, email_send_cnt bigint NULL, asset_size decimal(17,2) NULL, 
      db_size decimal(17,2) NULL, admin_user_cnt int NULL, event_month varchar(36) NOT NULL    )) by &database.;
   execute (alter table &dbschema..MONTHLY_USAGE
      add constraint MONTHLY_USAGE_pk  primary key (EVENT_MONTH)) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: MONTHLY_USAGE, MONTHLY_USAGE);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..NOTIFICATION_FAILED (
      properties_map_doc varchar(4000) NULL, notification_failed_dttm datetime2 NULL, notification_failed_dttm_tz datetime2 NULL, load_dttm datetime2 NOT NULL, 
      task_id varchar(36) NULL, segment_version_id varchar(36) NULL, segment_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, 
      occurrence_id varchar(36) NULL, message_version_id varchar(36) NULL, journey_id varchar(36) NULL, event_designed_id varchar(36) NULL, 
      creative_id varchar(36) NULL, channel_user_id varchar(300) NULL, channel_nm varchar(40) NULL, aud_occurrence_id varchar(36) NULL, 
      context_type_nm varchar(256) NULL, error_cd varchar(256) NULL, event_id varchar(36) NOT NULL, event_nm varchar(256) NULL, 
      message_id varchar(36) NULL, mobile_app_id varchar(40) NULL, reserved_1_txt varchar(100) NULL, audience_id varchar(36) NULL, 
      context_val varchar(256) NULL, creative_version_id varchar(36) NULL, error_message_txt varchar(1332) NULL, identity_id varchar(36) NULL, 
      journey_occurrence_id varchar(36) NULL, reserved_2_txt varchar(100) NULL, spot_id varchar(36) NULL, task_version_id varchar(36) NULL
         )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_NOTIFICATION_FAILED_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_NOTIFICATION_FAILED_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_NOTIFICATION_FAILED_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_NOTIFICATION_FAILED_load_dttm
          AS PARTITION PF_NOTIFICATION_FAILED_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..NOTIFICATION_FAILED
      ADD CONSTRAINT NOTIFICATION_FAILED_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_NOTIFICATION_FAILED_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: NOTIFICATION_FAILED, NOTIFICATION_FAILED);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..NOTIFICATION_OPENED (
      properties_map_doc varchar(4000) NULL, load_dttm datetime2 NOT NULL, notification_opened_dttm_tz datetime2 NULL, notification_opened_dttm datetime2 NULL, 
      task_version_id varchar(36) NULL, segment_version_id varchar(36) NULL, segment_id varchar(36) NULL, reserved_1_txt varchar(100) NULL, 
      message_id varchar(36) NULL, identity_id varchar(36) NULL, event_nm varchar(256) NULL, creative_id varchar(36) NULL, 
      channel_user_id varchar(300) NULL, channel_nm varchar(40) NULL, aud_occurrence_id varchar(36) NULL, context_type_nm varchar(256) NULL, 
      event_designed_id varchar(36) NULL, journey_id varchar(36) NULL, message_version_id varchar(36) NULL, occurrence_id varchar(36) NULL, 
      reserved_3_txt varchar(100) NULL, spot_id varchar(36) NULL, audience_id varchar(36) NULL, context_val varchar(256) NULL, 
      creative_version_id varchar(36) NULL, event_id varchar(36) NOT NULL, journey_occurrence_id varchar(36) NULL, mobile_app_id varchar(40) NULL, 
      reserved_2_txt varchar(100) NULL, response_tracking_cd varchar(36) NULL, task_id varchar(36) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_NOTIFICATION_OPENED_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_NOTIFICATION_OPENED_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_NOTIFICATION_OPENED_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_NOTIFICATION_OPENED_load_dttm
          AS PARTITION PF_NOTIFICATION_OPENED_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..NOTIFICATION_OPENED
      ADD CONSTRAINT NOTIFICATION_OPENED_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_NOTIFICATION_OPENED_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: NOTIFICATION_OPENED, NOTIFICATION_OPENED);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..NOTIFICATION_SEND (
      properties_map_doc varchar(4000) NULL, load_dttm datetime2 NOT NULL, notification_send_dttm_tz datetime2 NULL, notification_send_dttm datetime2 NULL, 
      task_id varchar(36) NULL, spot_id varchar(36) NULL, reserved_2_txt varchar(100) NULL, occurrence_id varchar(36) NULL, 
      message_id varchar(36) NULL, identity_id varchar(36) NULL, creative_version_id varchar(36) NULL, channel_user_id varchar(300) NULL, 
      audience_id varchar(36) NULL, context_val varchar(256) NULL, event_id varchar(36) NOT NULL, journey_id varchar(36) NULL, 
      journey_occurrence_id varchar(36) NULL, mobile_app_id varchar(40) NULL, reserved_1_txt varchar(100) NULL, segment_id varchar(36) NULL, 
      task_version_id varchar(36) NULL, aud_occurrence_id varchar(36) NULL, channel_nm varchar(40) NULL, context_type_nm varchar(256) NULL, 
      creative_id varchar(36) NULL, event_designed_id varchar(36) NULL, event_nm varchar(256) NULL, message_version_id varchar(36) NULL, 
      response_tracking_cd varchar(36) NULL, segment_version_id varchar(36) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_NOTIFICATION_SEND_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_NOTIFICATION_SEND_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_NOTIFICATION_SEND_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_NOTIFICATION_SEND_load_dttm
          AS PARTITION PF_NOTIFICATION_SEND_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..NOTIFICATION_SEND
      ADD CONSTRAINT NOTIFICATION_SEND_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_NOTIFICATION_SEND_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: NOTIFICATION_SEND, NOTIFICATION_SEND);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..NOTIFICATION_TARGETING_REQUEST (
      eligibility_flg char(1) NULL, notification_tgt_req_dttm datetime2 NULL, load_dttm datetime2 NOT NULL, notification_tgt_req_dttm_tz datetime2 NULL, 
      task_id varchar(36) NULL, mobile_app_id varchar(40) NULL, event_nm varchar(256) NULL, context_val varchar(256) NULL, 
      audience_id varchar(36) NULL, channel_user_id varchar(300) NULL, event_designed_id varchar(36) NULL, journey_id varchar(36) NULL, 
      aud_occurrence_id varchar(36) NULL, channel_nm varchar(40) NULL, context_type_nm varchar(256) NULL, event_id varchar(36) NOT NULL, 
      identity_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_NOTIFICATION_TARGETING_REQUEST_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_NOTIFICATION_TARGETING_REQUEST_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_NOTIFICATION_TARGETING_REQUEST_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_NOTIFICATION_TARGETING_REQUEST_load_dttm
          AS PARTITION PF_NOTIFICATION_TARGETING_REQUEST_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..NOTIFICATION_TARGETING_REQUEST
      ADD CONSTRAINT NOTIFICATION_TARGETING_REQUEST_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_NOTIFICATION_TARGETING_REQUEST_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: NOTIFICATION_TARGETING_REQUEST, NOTIFICATION_TARGETING_REQUEST);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..ORDER_DETAILS (
      unit_price_amt decimal(17,2) NULL, quantity_amt int NULL, properties_map_doc varchar(4000) NULL, load_dttm datetime2 NOT NULL, 
      activity_dttm datetime2 NULL, activity_dttm_tz datetime2 NULL, visit_id varchar(32) NULL, session_id varchar(29) NULL, 
      record_type varchar(15) NULL, product_id varchar(130) NULL, mobile_app_id varchar(40) NULL, event_nm varchar(256) NULL, 
      event_key_cd varchar(100) NULL, detail_id varchar(32) NULL, cart_id varchar(42) NULL, availability_message_txt varchar(650) NULL, 
      channel_nm varchar(40) NULL, event_designed_id varchar(36) NULL, event_source_cd varchar(100) NULL, order_id varchar(42) NULL, 
      product_nm varchar(130) NULL, product_sku varchar(100) NULL, reserved_1_txt varchar(100) NULL, session_id_hex varchar(29) NULL, 
      shipping_message_txt varchar(650) NULL, cart_nm varchar(100) NULL, currency_cd varchar(6) NULL, detail_id_hex varchar(32) NULL, 
      event_id varchar(36) NOT NULL, identity_id varchar(36) NULL, product_group_nm varchar(130) NULL, saving_message_txt varchar(650) NULL, 
      visit_id_hex varchar(32) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_ORDER_DETAILS_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_ORDER_DETAILS_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_ORDER_DETAILS_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_ORDER_DETAILS_load_dttm
          AS PARTITION PF_ORDER_DETAILS_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..ORDER_DETAILS
      ADD CONSTRAINT ORDER_DETAILS_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_ORDER_DETAILS_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ORDER_DETAILS, ORDER_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..ORDER_SUMMARY (
      total_price_amt decimal(17,2) NULL, shipping_amt decimal(17,2) NULL, total_tax_amt decimal(17,2) NULL, total_unit_qty int NULL, 
      properties_map_doc varchar(4000) NULL, load_dttm datetime2 NOT NULL, activity_dttm_tz datetime2 NULL, activity_dttm datetime2 NULL, 
      visit_id varchar(32) NULL, shipping_postal_cd varchar(10) NULL, session_id_hex varchar(29) NULL, payment_type_desc varchar(42) NULL, 
      identity_id varchar(36) NULL, event_id varchar(36) NOT NULL, delivery_type_desc varchar(42) NULL, cart_id varchar(42) NULL, 
      billing_city_nm varchar(390) NULL, billing_postal_cd varchar(10) NULL, channel_nm varchar(40) NULL, detail_id_hex varchar(32) NULL, 
      event_nm varchar(256) NULL, mobile_app_id varchar(40) NULL, record_type varchar(15) NULL, shipping_city_nm varchar(390) NULL, 
      visit_id_hex varchar(32) NULL, billing_country_nm varchar(85) NULL, billing_state_region_cd varchar(256) NULL, cart_nm varchar(100) NULL, 
      currency_cd varchar(6) NULL, detail_id varchar(32) NULL, event_designed_id varchar(36) NULL, event_key_cd varchar(100) NULL, 
      event_source_cd varchar(100) NULL, order_id varchar(42) NULL, session_id varchar(29) NULL, shipping_country_nm varchar(85) NULL, 
      shipping_state_region_cd varchar(256) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_ORDER_SUMMARY_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_ORDER_SUMMARY_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_ORDER_SUMMARY_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_ORDER_SUMMARY_load_dttm
          AS PARTITION PF_ORDER_SUMMARY_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..ORDER_SUMMARY
      ADD CONSTRAINT ORDER_SUMMARY_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_ORDER_SUMMARY_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: ORDER_SUMMARY, ORDER_SUMMARY);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..OUTBOUND_SYSTEM (
      properties_map_doc varchar(4000) NULL, outbound_system_dttm_tz datetime2 NULL, outbound_system_dttm datetime2 NULL, load_dttm datetime2 NOT NULL, 
      visit_id_hex varchar(32) NULL, session_id_hex varchar(29) NULL, reserved_2_txt varchar(100) NULL, reserved_1_txt varchar(100) NULL, 
      parent_event_id varchar(36) NULL, message_version_id varchar(36) NULL, journey_id varchar(36) NULL, event_designed_id varchar(36) NULL, 
      context_val varchar(256) NULL, audience_id varchar(36) NULL, channel_nm varchar(40) NULL, channel_user_id varchar(300) NULL, 
      creative_id varchar(36) NULL, creative_version_id varchar(36) NULL, event_nm varchar(256) NULL, message_id varchar(36) NULL, 
      mobile_app_id varchar(40) NULL, occurrence_id varchar(36) NULL, segment_id varchar(36) NULL, task_id varchar(36) NULL, 
      aud_occurrence_id varchar(36) NULL, context_type_nm varchar(256) NULL, detail_id_hex varchar(32) NULL, event_id varchar(36) NOT NULL, 
      identity_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, segment_version_id varchar(36) NULL, 
      spot_id varchar(36) NULL, task_version_id varchar(36) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_OUTBOUND_SYSTEM_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_OUTBOUND_SYSTEM_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_OUTBOUND_SYSTEM_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_OUTBOUND_SYSTEM_load_dttm
          AS PARTITION PF_OUTBOUND_SYSTEM_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..OUTBOUND_SYSTEM
      ADD CONSTRAINT OUTBOUND_SYSTEM_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_OUTBOUND_SYSTEM_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: OUTBOUND_SYSTEM, OUTBOUND_SYSTEM);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..PAGE_DETAILS (
      session_dt_tz date NULL, session_dt date NULL, page_load_sec_cnt int NULL, page_complete_sec_cnt int NULL, 
      bytes_sent_cnt int NULL, detail_dttm_tz datetime2 NULL, load_dttm datetime2 NOT NULL, detail_dttm datetime2 NULL, 
      url_domain varchar(215) NULL, session_id_hex varchar(29) NULL, session_id varchar(29) NULL, page_url_txt varchar(1332) NULL, 
      mobile_app_id varchar(40) NULL, event_key_cd varchar(100) NULL, detail_id_hex varchar(32) NULL, detail_id varchar(32) NULL, 
      class8_id varchar(650) NULL, class4_id varchar(650) NULL, class15_id varchar(650) NULL, class12_id varchar(650) NULL, 
      class11_id varchar(650) NULL, channel_nm varchar(40) NULL, class13_id varchar(650) NULL, class2_id varchar(650) NULL, 
      class6_id varchar(650) NULL, domain_nm varchar(165) NULL, event_source_cd varchar(100) NULL, page_desc varchar(1332) NULL, 
      protocol_nm varchar(8) NULL, visit_id varchar(32) NULL, visit_id_hex varchar(32) NULL, class10_id varchar(650) NULL, 
      class14_id varchar(650) NULL, class1_id varchar(650) NULL, class3_id varchar(650) NULL, class5_id varchar(650) NULL, 
      class7_id varchar(650) NULL, class9_id varchar(650) NULL, event_id varchar(36) NOT NULL, event_nm varchar(256) NULL, 
      identity_id varchar(36) NULL, referrer_url_txt varchar(1332) NULL, window_size_txt varchar(20) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_PAGE_DETAILS_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_PAGE_DETAILS_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_PAGE_DETAILS_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_PAGE_DETAILS_load_dttm
          AS PARTITION PF_PAGE_DETAILS_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..PAGE_DETAILS
      ADD CONSTRAINT PAGE_DETAILS_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_PAGE_DETAILS_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: PAGE_DETAILS, PAGE_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..PAGE_DETAILS_EXT (
      active_sec_spent_on_page_cnt int NULL, seconds_spent_on_page_cnt int NULL, load_dttm datetime2 NOT NULL, session_id varchar(29) NOT NULL, 
      detail_id varchar(32) NOT NULL, detail_id_hex varchar(32) NULL, session_id_hex varchar(29) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_PAGE_DETAILS_EXT_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_PAGE_DETAILS_EXT_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_PAGE_DETAILS_EXT_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_PAGE_DETAILS_EXT_load_dttm
          AS PARTITION PF_PAGE_DETAILS_EXT_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..PAGE_DETAILS_EXT
      ADD CONSTRAINT PAGE_DETAILS_EXT_pk
       PRIMARY KEY CLUSTERED (DETAIL_ID,LOAD_DTTM,SESSION_ID)
        ON PS_PAGE_DETAILS_EXT_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: PAGE_DETAILS_EXT, PAGE_DETAILS_EXT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..PAGE_ERRORS (
      in_page_error_dttm datetime2 NULL, in_page_error_dttm_tz datetime2 NULL, load_dttm datetime2 NOT NULL, visit_id_hex varchar(32) NULL, 
      session_id varchar(29) NULL, identity_id varchar(36) NULL, error_location_txt varchar(41) NULL, detail_id_hex varchar(32) NULL, 
      event_id varchar(36) NOT NULL, in_page_error_txt varchar(260) NULL, session_id_hex varchar(29) NULL, detail_id varchar(32) NULL, 
      event_source_cd varchar(100) NULL, visit_id varchar(32) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_PAGE_ERRORS_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_PAGE_ERRORS_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_PAGE_ERRORS_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_PAGE_ERRORS_load_dttm
          AS PARTITION PF_PAGE_ERRORS_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..PAGE_ERRORS
      ADD CONSTRAINT PAGE_ERRORS_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_PAGE_ERRORS_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: PAGE_ERRORS, PAGE_ERRORS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..PLANNING_HIERARCHY_DEFN (
      level_no int NULL, load_dttm datetime2 NULL, created_dttm datetime2 NULL, last_modified_dttm datetime2 NULL, 
      last_modified_usernm varchar(128) NULL, hier_defn_nm varchar(128) NULL, hier_defn_id varchar(128) NULL, hier_defn_desc varchar(1332) NULL, 
      hier_defn_subtype varchar(128) NULL, level_desc varchar(1332) NULL, created_by_usernm varchar(128) NULL, hier_defn_type varchar(128) NULL, 
      level_nm varchar(128) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: PLANNING_HIERARCHY_DEFN, PLANNING_HIERARCHY_DEFN);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..PLANNING_INFO (
      reserved_budget_same_flg char(1) NULL, bu_obsolete_flg char(1) NULL, alloc_budget decimal(17,2) NULL, total_budget decimal(17,2) NULL, 
      reserved_budget decimal(17,2) NULL, rolledup_budget decimal(17,2) NULL, tot_invoiced decimal(17,2) NULL, tot_expenses decimal(17,2) NULL, 
      tot_cmtmnt_outstanding decimal(17,2) NULL, tot_committed decimal(17,2) NULL, available_budget decimal(17,2) NULL, tot_cmtmnt_overspent decimal(17,2) NULL, 
      created_dttm datetime2 NULL, last_modified_dttm datetime2 NULL, planned_start_dttm datetime2 NULL, load_dttm datetime2 NULL, 
      planned_end_dttm datetime2 NULL, task_id varchar(128) NULL, planning_owner_usernm varchar(128) NULL, planning_level_no varchar(10) NULL, 
      planning_desc varchar(1332) NULL, parent_id varchar(128) NULL, lev6_nm varchar(128) NULL, lev2_nm varchar(128) NULL, 
      last_modified_usernm varchar(128) NULL, hier_defn_id varchar(128) NULL, currency_cd varchar(10) NULL, bu_nm varchar(128) NULL, 
      bu_currency_cd varchar(10) NULL, activity_nm varchar(128) NULL, activity_desc varchar(1332) NULL, all_msgs varchar(4000) NULL, 
      bu_desc varchar(1332) NULL, category_nm varchar(128) NULL, hier_defn_nodeid varchar(128) NULL, lev10_nm varchar(128) NULL, 
      lev3_nm varchar(128) NULL, lev4_nm varchar(128) NULL, lev7_nm varchar(128) NULL, lev8_nm varchar(128) NULL, 
      parent_nm varchar(128) NULL, planning_id varchar(128) NULL, planning_level_type varchar(32) NULL, planning_nm varchar(128) NULL, 
      planning_type varchar(32) NULL, task_channel varchar(64) NULL, task_status varchar(64) NULL, activity_id varchar(128) NULL, 
      activity_status varchar(128) NULL, bu_id varchar(128) NULL, created_by_usernm varchar(128) NULL, lev1_nm varchar(128) NULL, 
      lev5_nm varchar(128) NULL, lev9_nm varchar(128) NULL, planning_item_path varchar(4000) NULL, planning_number varchar(128) NULL, 
      planning_status varchar(32) NULL, task_desc varchar(1332) NULL, task_nm varchar(128) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: PLANNING_INFO, PLANNING_INFO);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..PLANNING_INFO_CUSTOM_PROP (
      attr_val varchar(4000) NULL, is_obsolete_flg char(1) NULL, is_grid_flg char(1) NULL, last_modified_dttm datetime2 NULL, 
      created_dttm datetime2 NULL, load_dttm datetime2 NULL, planning_id varchar(128) NULL, last_modified_usernm varchar(128) NULL, 
      created_by_usernm varchar(128) NULL, attr_group_nm varchar(128) NULL, attr_cd varchar(128) NULL, attr_group_cd varchar(128) NULL, 
      attr_id varchar(128) NULL, attr_nm varchar(128) NULL, data_formatter varchar(64) NULL, remote_pklist_tab_col varchar(128) NULL, 
      attr_group_id varchar(128) NULL, data_type varchar(32) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: PLANNING_INFO_CUSTOM_PROP, PLANNING_INFO_CUSTOM_PROP);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..PRODUCT_VIEWS (
      price_val decimal(17,2) NULL, properties_map_doc varchar(4000) NULL, action_dttm_tz datetime2 NULL, load_dttm datetime2 NOT NULL, 
      action_dttm datetime2 NULL, visit_id_hex varchar(32) NULL, visit_id varchar(32) NULL, saving_message_txt varchar(650) NULL, 
      product_id varchar(130) NULL, mobile_app_id varchar(40) NULL, event_nm varchar(256) NULL, event_key_cd varchar(100) NULL, 
      detail_id varchar(32) NULL, availability_message_txt varchar(650) NULL, channel_nm varchar(40) NULL, event_designed_id varchar(36) NULL, 
      event_source_cd varchar(100) NULL, product_group_nm varchar(130) NULL, product_sku varchar(100) NULL, session_id_hex varchar(29) NULL, 
      currency_cd varchar(6) NULL, detail_id_hex varchar(32) NULL, event_id varchar(36) NOT NULL, identity_id varchar(36) NULL, 
      product_nm varchar(130) NULL, session_id varchar(29) NULL, shipping_message_txt varchar(650) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_PRODUCT_VIEWS_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_PRODUCT_VIEWS_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_PRODUCT_VIEWS_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_PRODUCT_VIEWS_load_dttm
          AS PARTITION PF_PRODUCT_VIEWS_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..PRODUCT_VIEWS
      ADD CONSTRAINT PRODUCT_VIEWS_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_PRODUCT_VIEWS_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: PRODUCT_VIEWS, PRODUCT_VIEWS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..PROMOTION_DISPLAYED (
      derived_display_flg char(1) NULL, promotion_number int NULL, properties_map_doc varchar(4000) NULL, display_dttm_tz datetime2 NULL, 
      load_dttm datetime2 NOT NULL, display_dttm datetime2 NULL, session_id_hex varchar(29) NULL, promotion_tracking_cd varchar(65) NULL, 
      promotion_nm varchar(260) NULL, promotion_creative_nm varchar(260) NULL, event_source_cd varchar(100) NULL, event_designed_id varchar(36) NULL, 
      detail_id varchar(32) NULL, channel_nm varchar(40) NULL, detail_id_hex varchar(32) NULL, event_key_cd varchar(100) NULL, 
      mobile_app_id varchar(40) NULL, promotion_placement_nm varchar(260) NULL, session_id varchar(29) NULL, visit_id_hex varchar(32) NULL, 
      event_id varchar(36) NOT NULL, event_nm varchar(256) NULL, identity_id varchar(36) NULL, promotion_type_nm varchar(65) NULL, 
      visit_id varchar(32) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_PROMOTION_DISPLAYED_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_PROMOTION_DISPLAYED_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_PROMOTION_DISPLAYED_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_PROMOTION_DISPLAYED_load_dttm
          AS PARTITION PF_PROMOTION_DISPLAYED_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..PROMOTION_DISPLAYED
      ADD CONSTRAINT PROMOTION_DISPLAYED_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_PROMOTION_DISPLAYED_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: PROMOTION_DISPLAYED, PROMOTION_DISPLAYED);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..PROMOTION_USED (
      promotion_number int NULL, properties_map_doc varchar(4000) NULL, click_dttm_tz datetime2 NULL, click_dttm datetime2 NULL, 
      load_dttm datetime2 NOT NULL, session_id_hex varchar(29) NULL, promotion_tracking_cd varchar(65) NULL, promotion_creative_nm varchar(260) NULL, 
      event_source_cd varchar(100) NULL, event_id varchar(36) NOT NULL, event_designed_id varchar(36) NULL, detail_id varchar(32) NULL, 
      detail_id_hex varchar(32) NULL, event_key_cd varchar(100) NULL, mobile_app_id varchar(40) NULL, promotion_nm varchar(260) NULL, 
      promotion_placement_nm varchar(260) NULL, session_id varchar(29) NULL, visit_id_hex varchar(32) NULL, channel_nm varchar(40) NULL, 
      event_nm varchar(256) NULL, identity_id varchar(36) NULL, promotion_type_nm varchar(65) NULL, visit_id varchar(32) NULL
         )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_PROMOTION_USED_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_PROMOTION_USED_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_PROMOTION_USED_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_PROMOTION_USED_load_dttm
          AS PARTITION PF_PROMOTION_USED_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..PROMOTION_USED
      ADD CONSTRAINT PROMOTION_USED_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_PROMOTION_USED_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: PROMOTION_USED, PROMOTION_USED);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..RESPONSE_HISTORY (
      properties_map_doc varchar(4000) NULL, load_dttm datetime2 NOT NULL, response_dttm datetime2 NULL, response_dttm_tz datetime2 NULL, 
      session_id_hex varchar(29) NULL, response_id varchar(36) NOT NULL, response_channel_nm varchar(40) NULL, parent_event_designed_id varchar(36) NULL, 
      journey_occurrence_id varchar(36) NULL, detail_id_hex varchar(32) NULL, audience_id varchar(36) NULL, context_type_nm varchar(256) NULL, 
      context_val varchar(256) NULL, identity_id varchar(36) NULL, message_id varchar(36) NULL, response_nm varchar(256) NULL, 
      task_version_id varchar(36) NULL, aud_occurrence_id varchar(36) NULL, creative_id varchar(36) NULL, event_designed_id varchar(36) NULL, 
      journey_id varchar(36) NULL, occurrence_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, task_id varchar(36) NULL, 
      visit_id_hex varchar(32) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_RESPONSE_HISTORY_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_RESPONSE_HISTORY_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_RESPONSE_HISTORY_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_RESPONSE_HISTORY_load_dttm
          AS PARTITION PF_RESPONSE_HISTORY_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..RESPONSE_HISTORY
      ADD CONSTRAINT RESPONSE_HISTORY_pk
       PRIMARY KEY CLUSTERED (load_dttm ,RESPONSE_ID)
        ON PS_RESPONSE_HISTORY_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: RESPONSE_HISTORY, RESPONSE_HISTORY);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..SEARCH_RESULTS (
      results_displayed_flg char(1) NULL, search_results_displayed int NULL, properties_map_doc varchar(4000) NULL, load_dttm datetime2 NOT NULL, 
      search_results_dttm datetime2 NULL, search_results_dttm_tz datetime2 NULL, visit_id_hex varchar(32) NULL, srch_field_name varchar(325) NULL, 
      srch_field_id varchar(325) NULL, search_results_sk varchar(100) NULL, search_nm varchar(42) NULL, identity_id varchar(36) NULL, 
      event_key_cd varchar(100) NULL, event_id varchar(36) NOT NULL, channel_nm varchar(40) NULL, detail_id varchar(32) NULL, 
      detail_id_hex varchar(32) NULL, event_nm varchar(256) NULL, mobile_app_id varchar(40) NULL, session_id varchar(29) NULL, 
      srch_phrase varchar(2600) NULL, event_designed_id varchar(36) NULL, event_source_cd varchar(100) NULL, session_id_hex varchar(29) NULL, 
      visit_id varchar(32) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_SEARCH_RESULTS_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_SEARCH_RESULTS_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_SEARCH_RESULTS_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_SEARCH_RESULTS_load_dttm
          AS PARTITION PF_SEARCH_RESULTS_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..SEARCH_RESULTS
      ADD CONSTRAINT SEARCH_RESULTS_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_SEARCH_RESULTS_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SEARCH_RESULTS, SEARCH_RESULTS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..SEARCH_RESULTS_EXT (
      search_results_displayed int NULL, load_dttm datetime2 NOT NULL, search_results_sk varchar(100) NULL, event_designed_id varchar(36) NULL, 
      event_id varchar(36) NOT NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_SEARCH_RESULTS_EXT_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_SEARCH_RESULTS_EXT_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_SEARCH_RESULTS_EXT_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_SEARCH_RESULTS_EXT_load_dttm
          AS PARTITION PF_SEARCH_RESULTS_EXT_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..SEARCH_RESULTS_EXT
      ADD CONSTRAINT SEARCH_RESULTS_EXT_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_SEARCH_RESULTS_EXT_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SEARCH_RESULTS_EXT, SEARCH_RESULTS_EXT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..SESSION_DETAILS (
      java_enabled_flg char(1) NULL, cookies_enabled_flg char(1) NULL, java_script_enabled_flg char(1) NULL, is_portable_flag char(1) NULL, 
      flash_enabled_flg char(1) NULL, session_dt_tz date NULL, session_dt date NULL, longitude decimal(13,6) NULL, 
      latitude decimal(13,6) NULL, session_timeout int NULL, metro_cd int NULL, screen_color_depth_no int NULL, 
      client_session_start_dttm datetime2 NULL, load_dttm datetime2 NOT NULL, session_start_dttm_tz datetime2 NULL, session_start_dttm datetime2 NULL, 
      client_session_start_dttm_tz datetime2 NULL, user_agent_nm varchar(512) NULL, state_region_cd varchar(2) NULL, region_nm varchar(256) NULL, 
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
      user_language_cd varchar(12) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_SESSION_DETAILS_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_SESSION_DETAILS_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_SESSION_DETAILS_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_SESSION_DETAILS_load_dttm
          AS PARTITION PF_SESSION_DETAILS_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..SESSION_DETAILS
      ADD CONSTRAINT SESSION_DETAILS_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_SESSION_DETAILS_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SESSION_DETAILS, SESSION_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..SESSION_DETAILS_EXT (
      active_sec_spent_in_sessn_cnt int NULL, seconds_spent_in_session_cnt int NULL, load_dttm datetime2 NOT NULL, last_session_activity_dttm datetime2 NOT NULL, 
      session_expiration_dttm datetime2 NULL, last_session_activity_dttm_tz datetime2 NULL, session_expiration_dttm_tz datetime2 NULL, session_id varchar(29) NOT NULL, 
      session_id_hex varchar(29) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_SESSION_DETAILS_EXT_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_SESSION_DETAILS_EXT_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_SESSION_DETAILS_EXT_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_SESSION_DETAILS_EXT_load_dttm
          AS PARTITION PF_SESSION_DETAILS_EXT_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..SESSION_DETAILS_EXT
      ADD CONSTRAINT SESSION_DETAILS_EXT_pk
       PRIMARY KEY CLUSTERED (load_dttm ,LAST_SESSION_ACTIVITY_DTTM,SESSION_ID)
        ON PS_SESSION_DETAILS_EXT_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SESSION_DETAILS_EXT, SESSION_DETAILS_EXT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..SMS_MESSAGE_CLICKED (
      sms_click_dttm_tz datetime2 NULL, sms_click_dttm datetime2 NULL, load_dttm datetime2 NOT NULL, task_id varchar(36) NULL, 
      sms_message_id varchar(40) NULL, sender_id varchar(40) NULL, journey_occurrence_id varchar(36) NULL, event_nm varchar(256) NULL, 
      event_id varchar(36) NOT NULL, country_cd varchar(3) NULL, audience_id varchar(36) NULL, aud_occurrence_id varchar(36) NULL, 
      context_type_nm varchar(256) NULL, creative_version_id varchar(36) NULL, identity_id varchar(36) NULL, occurrence_id varchar(36) NULL, 
      context_val varchar(256) NULL, creative_id varchar(36) NULL, event_designed_id varchar(36) NULL, journey_id varchar(36) NULL, 
      response_tracking_cd varchar(36) NULL, task_version_id varchar(36) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_SMS_MESSAGE_CLICKED_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_SMS_MESSAGE_CLICKED_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_SMS_MESSAGE_CLICKED_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_SMS_MESSAGE_CLICKED_load_dttm
          AS PARTITION PF_SMS_MESSAGE_CLICKED_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..SMS_MESSAGE_CLICKED
      ADD CONSTRAINT SMS_MESSAGE_CLICKED_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_SMS_MESSAGE_CLICKED_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SMS_MESSAGE_CLICKED, SMS_MESSAGE_CLICKED);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..SMS_MESSAGE_DELIVERED (
      sms_delivered_dttm_tz datetime2 NULL, sms_delivered_dttm datetime2 NULL, load_dttm datetime2 NOT NULL, sms_message_id varchar(40) NULL, 
      occurrence_id varchar(36) NULL, journey_id varchar(36) NULL, identity_id varchar(36) NULL, creative_version_id varchar(36) NULL, 
      context_type_nm varchar(256) NULL, aud_occurrence_id varchar(36) NULL, country_cd varchar(3) NULL, event_id varchar(36) NOT NULL, 
      journey_occurrence_id varchar(36) NULL, sender_id varchar(40) NULL, task_id varchar(36) NULL, audience_id varchar(36) NULL, 
      context_val varchar(256) NULL, creative_id varchar(36) NULL, event_designed_id varchar(36) NULL, event_nm varchar(256) NULL, 
      response_tracking_cd varchar(36) NULL, task_version_id varchar(36) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_SMS_MESSAGE_DELIVERED_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_SMS_MESSAGE_DELIVERED_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_SMS_MESSAGE_DELIVERED_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_SMS_MESSAGE_DELIVERED_load_dttm
          AS PARTITION PF_SMS_MESSAGE_DELIVERED_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..SMS_MESSAGE_DELIVERED
      ADD CONSTRAINT SMS_MESSAGE_DELIVERED_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_SMS_MESSAGE_DELIVERED_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SMS_MESSAGE_DELIVERED, SMS_MESSAGE_DELIVERED);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..SMS_MESSAGE_FAILED (
      sms_failed_dttm_tz datetime2 NULL, load_dttm datetime2 NOT NULL, sms_failed_dttm datetime2 NULL, task_version_id varchar(36) NULL, 
      task_id varchar(36) NULL, sms_message_id varchar(40) NULL, reason_description_txt varchar(1500) NULL, journey_occurrence_id varchar(36) NULL, 
      event_id varchar(36) NOT NULL, creative_id varchar(36) NULL, country_cd varchar(3) NULL, aud_occurrence_id varchar(36) NULL, 
      context_type_nm varchar(256) NULL, creative_version_id varchar(36) NULL, event_nm varchar(256) NULL, identity_id varchar(36) NULL, 
      occurrence_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, sender_id varchar(40) NULL, audience_id varchar(36) NULL, 
      context_val varchar(256) NULL, event_designed_id varchar(36) NULL, journey_id varchar(36) NULL, reason_cd varchar(5) NULL
         )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_SMS_MESSAGE_FAILED_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_SMS_MESSAGE_FAILED_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_SMS_MESSAGE_FAILED_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_SMS_MESSAGE_FAILED_load_dttm
          AS PARTITION PF_SMS_MESSAGE_FAILED_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..SMS_MESSAGE_FAILED
      ADD CONSTRAINT SMS_MESSAGE_FAILED_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_SMS_MESSAGE_FAILED_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SMS_MESSAGE_FAILED, SMS_MESSAGE_FAILED);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..SMS_MESSAGE_REPLY (
      load_dttm datetime2 NOT NULL, sms_reply_dttm_tz datetime2 NULL, sms_reply_dttm datetime2 NULL, task_version_id varchar(36) NULL, 
      sms_message_id varchar(40) NULL, response_tracking_cd varchar(36) NULL, occurrence_id varchar(36) NULL, identity_id varchar(36) NULL, 
      country_cd varchar(3) NULL, aud_occurrence_id varchar(36) NULL, context_type_nm varchar(256) NULL, event_id varchar(36) NOT NULL, 
      journey_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL, sender_id varchar(40) NULL, task_id varchar(36) NULL, 
      audience_id varchar(36) NULL, context_val varchar(256) NULL, event_designed_id varchar(36) NULL, event_nm varchar(256) NULL, 
      sms_content varchar(40) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_SMS_MESSAGE_REPLY_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_SMS_MESSAGE_REPLY_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_SMS_MESSAGE_REPLY_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_SMS_MESSAGE_REPLY_load_dttm
          AS PARTITION PF_SMS_MESSAGE_REPLY_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..SMS_MESSAGE_REPLY
      ADD CONSTRAINT SMS_MESSAGE_REPLY_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_SMS_MESSAGE_REPLY_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SMS_MESSAGE_REPLY, SMS_MESSAGE_REPLY);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..SMS_MESSAGE_SEND (
      fragment_cnt int NULL, sms_send_dttm datetime2 NULL, sms_send_dttm_tz datetime2 NULL, load_dttm datetime2 NOT NULL, 
      occurrence_id varchar(36) NULL, identity_id varchar(36) NULL, event_id varchar(36) NOT NULL, event_designed_id varchar(36) NULL, 
      context_val varchar(256) NULL, aud_occurrence_id varchar(36) NULL, audience_id varchar(36) NULL, country_cd varchar(3) NULL, 
      creative_id varchar(36) NULL, event_nm varchar(256) NULL, journey_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL, 
      sender_id varchar(40) NULL, task_id varchar(36) NULL, context_type_nm varchar(256) NULL, creative_version_id varchar(36) NULL, 
      response_tracking_cd varchar(36) NULL, sms_message_id varchar(40) NULL, task_version_id varchar(36) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_SMS_MESSAGE_SEND_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_SMS_MESSAGE_SEND_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_SMS_MESSAGE_SEND_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_SMS_MESSAGE_SEND_load_dttm
          AS PARTITION PF_SMS_MESSAGE_SEND_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..SMS_MESSAGE_SEND
      ADD CONSTRAINT SMS_MESSAGE_SEND_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_SMS_MESSAGE_SEND_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SMS_MESSAGE_SEND, SMS_MESSAGE_SEND);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..SMS_OPTOUT (
      load_dttm datetime2 NOT NULL, sms_optout_dttm datetime2 NULL, sms_optout_dttm_tz datetime2 NULL, task_id varchar(36) NULL, 
      sms_message_id varchar(40) NULL, sender_id varchar(40) NULL, journey_occurrence_id varchar(36) NULL, event_id varchar(36) NOT NULL, 
      country_cd varchar(3) NULL, aud_occurrence_id varchar(36) NULL, context_type_nm varchar(256) NULL, creative_version_id varchar(36) NULL, 
      identity_id varchar(36) NULL, occurrence_id varchar(36) NULL, audience_id varchar(36) NULL, context_val varchar(256) NULL, 
      creative_id varchar(36) NULL, event_designed_id varchar(36) NULL, event_nm varchar(256) NULL, journey_id varchar(36) NULL, 
      response_tracking_cd varchar(36) NULL, task_version_id varchar(36) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_SMS_OPTOUT_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_SMS_OPTOUT_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_SMS_OPTOUT_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_SMS_OPTOUT_load_dttm
          AS PARTITION PF_SMS_OPTOUT_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..SMS_OPTOUT
      ADD CONSTRAINT SMS_OPTOUT_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_SMS_OPTOUT_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SMS_OPTOUT, SMS_OPTOUT);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..SMS_OPTOUT_DETAILS (
      load_dttm datetime2 NOT NULL, sms_optout_dttm datetime2 NULL, sms_optout_dttm_tz datetime2 NULL, task_version_id varchar(36) NULL, 
      sms_message_id varchar(40) NULL, occurrence_id varchar(36) NULL, event_nm varchar(256) NULL, creative_id varchar(36) NULL, 
      context_type_nm varchar(256) NULL, audience_id varchar(36) NULL, address_val varchar(20) NULL, context_val varchar(256) NULL, 
      event_designed_id varchar(36) NULL, journey_id varchar(36) NULL, response_tracking_cd varchar(36) NULL, task_id varchar(36) NULL, 
      aud_occurrence_id varchar(36) NULL, country_cd varchar(3) NULL, creative_version_id varchar(36) NULL, event_id varchar(36) NOT NULL, 
      identity_id varchar(36) NULL, journey_occurrence_id varchar(36) NULL, sender_id varchar(40) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_SMS_OPTOUT_DETAILS_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_SMS_OPTOUT_DETAILS_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_SMS_OPTOUT_DETAILS_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_SMS_OPTOUT_DETAILS_load_dttm
          AS PARTITION PF_SMS_OPTOUT_DETAILS_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..SMS_OPTOUT_DETAILS
      ADD CONSTRAINT SMS_OPTOUT_DETAILS_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_SMS_OPTOUT_DETAILS_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SMS_OPTOUT_DETAILS, SMS_OPTOUT_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..SPOT_CLICKED (
      control_group_flg char(1) NULL, product_qty_no int NULL, properties_map_doc varchar(4000) NULL, spot_clicked_dttm datetime2 NULL, 
      load_dttm datetime2 NOT NULL, spot_clicked_dttm_tz datetime2 NULL, session_id_hex varchar(29) NULL, reserved_2_txt varchar(100) NULL, 
      rec_group_id varchar(3) NULL, product_id varchar(128) NULL, message_id varchar(36) NULL, event_source_cd varchar(100) NULL, 
      event_nm varchar(256) NULL, detail_id_hex varchar(32) NULL, context_val varchar(256) NULL, channel_user_id varchar(300) NULL, 
      creative_id varchar(36) NULL, event_id varchar(36) NOT NULL, identity_id varchar(36) NULL, mobile_app_id varchar(40) NULL, 
      product_nm varchar(128) NULL, product_sku_no varchar(100) NULL, request_id varchar(36) NULL, segment_id varchar(36) NULL, 
      spot_id varchar(36) NULL, channel_nm varchar(40) NULL, context_type_nm varchar(256) NULL, creative_version_id varchar(36) NULL, 
      event_designed_id varchar(36) NULL, event_key_cd varchar(100) NULL, message_version_id varchar(36) NULL, occurrence_id varchar(36) NULL, 
      reserved_1_txt varchar(100) NULL, response_tracking_cd varchar(36) NULL, segment_version_id varchar(36) NULL, visit_id_hex varchar(32) NULL, 
      url_txt varchar(1332) NULL, task_version_id varchar(36) NULL, task_id varchar(36) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_SPOT_CLICKED_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_SPOT_CLICKED_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_SPOT_CLICKED_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_SPOT_CLICKED_load_dttm
          AS PARTITION PF_SPOT_CLICKED_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..SPOT_CLICKED
      ADD CONSTRAINT SPOT_CLICKED_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_SPOT_CLICKED_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SPOT_CLICKED, SPOT_CLICKED);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..SPOT_REQUESTED (
      properties_map_doc varchar(4000) NULL, load_dttm datetime2 NOT NULL, spot_requested_dttm_tz datetime2 NULL, spot_requested_dttm datetime2 NULL, 
      visit_id_hex varchar(32) NULL, spot_id varchar(36) NULL, session_id_hex varchar(29) NULL, request_id varchar(36) NULL, 
      mobile_app_id varchar(40) NULL, identity_id varchar(36) NULL, event_source_cd varchar(100) NULL, event_nm varchar(256) NULL, 
      event_id varchar(36) NOT NULL, event_designed_id varchar(36) NULL, detail_id_hex varchar(32) NULL, context_val varchar(256) NULL, 
      context_type_nm varchar(256) NULL, channel_user_id varchar(300) NULL, channel_nm varchar(40) NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_SPOT_REQUESTED_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_SPOT_REQUESTED_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_SPOT_REQUESTED_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_SPOT_REQUESTED_load_dttm
          AS PARTITION PF_SPOT_REQUESTED_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..SPOT_REQUESTED
      ADD CONSTRAINT SPOT_REQUESTED_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_SPOT_REQUESTED_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: SPOT_REQUESTED, SPOT_REQUESTED);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..TAG_DETAILS (
      created_dttm datetime2 NULL, load_dttm datetime2 NULL, last_modified_dttm datetime2 NULL, tag_owner_usernm varchar(128) NULL, 
      tag_nm varchar(128) NULL, tag_id varchar(128) NULL, tag_desc varchar(1332) NULL, last_modified_usernm varchar(128) NULL, 
      identity_cd varchar(128) NULL, created_by_usernm varchar(128) NULL, component_type varchar(32) NULL, component_id varchar(128) NULL
         )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: TAG_DETAILS, TAG_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..VISIT_DETAILS (
      sequence_no int NULL, visit_dttm_tz datetime2 NULL, load_dttm datetime2 NOT NULL, visit_dttm datetime2 NULL, 
      visit_id_hex varchar(32) NULL, visit_id varchar(32) NULL, session_id_hex varchar(29) NULL, session_id varchar(29) NULL, 
      search_term_txt varchar(1332) NULL, search_engine_domain_txt varchar(215) NULL, search_engine_desc varchar(130) NULL, referrer_txt varchar(1332) NULL, 
      referrer_query_string_txt varchar(1332) NULL, referrer_domain_nm varchar(215) NULL, origination_type_nm varchar(65) NULL, origination_tracking_cd varchar(65) NULL, 
      origination_placement_nm varchar(390) NULL, origination_nm varchar(260) NULL, origination_creative_nm varchar(260) NULL, identity_id varchar(36) NULL, 
      event_id varchar(36) NOT NULL    )) by &database.;
	execute (
		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_VISIT_DETAILS_load_dttm') BEGIN
			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));
			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));
			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION PF_VISIT_DETAILS_load_dttm (datetime2) AS RANGE RIGHT FOR VALUES ';
	;WITH d AS (
	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d
                UNION ALL
                SELECT DATEADD(MONTH, 1, d) FROM d
                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)
			)
	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +
		N'''' + CONVERT(varchar(10), d, 120) + N''''
		FROM d OPTION (MAXRECURSION 0);
			SET @sql = @sql + N');';
			EXEC sys.sp_executesql @sql;
		END
	) by &database.;
  execute (
    IF NOT EXISTS (
        SELECT 1
        FROM sys.partition_schemes
        WHERE name = 'PS_VISIT_DETAILS_load_dttm'
    )
    BEGIN
        CREATE PARTITION SCHEME PS_VISIT_DETAILS_load_dttm
          AS PARTITION PF_VISIT_DETAILS_load_dttm
          ALL TO ([PRIMARY]);
    END
  ) by &database.;
  execute (
    ALTER TABLE &dbschema..VISIT_DETAILS
      ADD CONSTRAINT VISIT_DETAILS_pk
       PRIMARY KEY CLUSTERED (load_dttm ,EVENT_ID)
        ON PS_VISIT_DETAILS_load_dttm(load_dttm);
  ) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: VISIT_DETAILS, VISIT_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..WF_PROCESS_DETAILS (
      percent_complete bigint NULL, user_tasks_cnt bigint NULL, delayed_by_day bigint NULL, completed_dttm datetime2 NULL, 
      indexed_dttm datetime2 NULL, planned_end_dttm datetime2 NULL, load_dttm datetime2 NULL, timeline_calculated_dttm datetime2 NULL, 
      start_dttm datetime2 NULL, published_dttm datetime2 NULL, submitted_dttm datetime2 NULL, deleted_dttm datetime2 NULL, 
      created_dttm datetime2 NULL, last_modified_dttm datetime2 NULL, projected_end_dttm datetime2 NULL, submitted_by_usernm varchar(128) NULL, 
      published_by_usernm varchar(128) NULL, process_type varchar(128) NULL, process_status varchar(128) NULL, process_nm varchar(128) NULL, 
      process_id varchar(128) NULL, process_comment varchar(128) NULL, process_category varchar(128) NULL, pdef_id varchar(128) NULL, 
      modified_status_cd varchar(128) NULL, last_modified_usernm varchar(128) NULL, deleted_by_usernm varchar(128) NULL, created_by_usernm varchar(128) NULL, 
      business_info_type varchar(128) NULL, business_info_nm varchar(128) NULL, business_info_id varchar(128) NULL, process_desc varchar(1332) NULL, 
      process_instance_version varchar(128) NULL, process_owner_usernm varchar(128) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: WF_PROCESS_DETAILS, WF_PROCESS_DETAILS);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..WF_PROCESS_DETAILS_CUSTOM_PROP (
      attr_val varchar(4000) NULL, is_grid_flg char(1) NULL, is_obsolete_flg char(1) NULL, load_dttm datetime2 NULL, 
      created_dttm datetime2 NULL, last_modified_dttm datetime2 NULL, process_id varchar(128) NULL, last_modified_usernm varchar(128) NULL, 
      data_type varchar(32) NULL, data_formatter varchar(64) NULL, created_by_usernm varchar(128) NULL, attr_id varchar(128) NULL, 
      attr_group_nm varchar(128) NULL, attr_group_id varchar(128) NULL, attr_cd varchar(128) NULL, attr_group_cd varchar(128) NULL, 
      attr_nm varchar(128) NULL, remote_pklist_tab_col varchar(128) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: WF_PROCESS_DETAILS_CUSTOM_PROP, WF_PROCESS_DETAILS_CUSTOM_PROP);
PROC SQL ;
   CONNECT to &database. (&sql_passthru_connection.);
   EXECUTE (CREATE TABLE &dbschema..WF_PROCESS_TASKS (
      duration_per_assignee bigint NULL, delayed_by_day bigint NULL, percent_complete bigint NULL, version_num bigint NULL, 
      existobj_update_flg char(1) NULL, locally_updated_flg char(1) NULL, multi_assig_suprt_flg char(1) NULL, skip_update_scanning_flg char(1) NULL, 
      skip_peerupdate_scanning_flg char(1) NULL, latest_flg char(1) NULL, cancelled_task_flg char(1) NULL, approval_task_flg char(1) NULL, 
      is_sequential_flg char(1) NULL, first_usertask_flg char(1) NULL, projected_start_dttm datetime2 NULL, published_dttm datetime2 NULL, 
      load_dttm datetime2 NULL, started_dttm datetime2 NULL, indexed_dttm datetime2 NULL, created_dttm datetime2 NULL, 
      modified_dttm datetime2 NULL, projected_end_dttm datetime2 NULL, completed_dttm datetime2 NULL, deleted_dttm datetime2 NULL, 
      due_dttm datetime2 NULL, engine_task_cancelled_dttm datetime2 NULL, task_type varchar(128) NULL, task_status varchar(128) NULL, 
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
   EXECUTE (CREATE TABLE &dbschema..WF_TASKS_USER_ASSIGNMENT (
      delayed_by_day bigint NULL, usan_duration_day bigint NULL, is_latest_flg char(1) NULL, is_assigned_flg char(1) NULL, 
      is_replaced_flg char(1) NULL, activation_completed_flg char(1) NULL, created_dttm datetime2 NULL, modified_dttm datetime2 NULL, 
      projected_end_dttm datetime2 NULL, deleted_dttm datetime2 NULL, completed_dttm datetime2 NULL, due_dttm datetime2 NULL, 
      load_dttm datetime2 NULL, projected_start_dttm datetime2 NULL, start_dttm datetime2 NULL, user_nm varchar(128) NULL, 
      user_assignment_id varchar(128) NULL, usan_status varchar(128) NULL, usan_instruction varchar(128) NULL, usan_desc varchar(1332) NULL, 
      task_id varchar(128) NULL, replacement_userid varchar(128) NULL, replacement_reason varchar(128) NULL, replacement_assignee_id varchar(128) NULL, 
      process_id varchar(128) NULL, owner_usernm varchar(128) NULL, modified_status_cd varchar(128) NULL, modified_by_usernm varchar(128) NULL, 
      instance_version varchar(128) NULL, initiator_comment varchar(128) NULL, deleted_by_usernm varchar(128) NULL, created_by_usernm varchar(128) NULL, 
      assignee_id varchar(128) NULL, approval_status varchar(128) NULL, assignee_type varchar(128) NULL, usan_comment varchar(128) NULL, 
      user_id varchar(128) NULL    )) by &database.;
   DISCONNECT FROM &database.;
QUIT;
%err_check (Failed to create Table: WF_TASKS_USER_ASSIGNMENT, WF_TASKS_USER_ASSIGNMENT);
