/********************************************************************************/
/* Copyright (c) 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                          */
/********************************************************************************/
/* CDM as SQL Views                                                             */
/********************************************************************************/

%let database=POSTGRES;
%let dbserver=xxxxxxxxxxxxxxxxx;  
%let dbport=5432;      * needed for postgres e.g. 5432;
%let dbname=xxxxxxxxxxxxxxx;  * Database instance - case sensitive;  
%let dbschema=xxxxxxxxxxxxx;* Schema is case sensitive;
%let dbuser=xxxxxxxxxxxxx;   * user; 
%let dbpass=xxxxxxxxxxxxxxxxxxx; * lets you connect to database with a user ID; 
%let sql_passthru_connection =%str(SERVER=&dbserver. PORT=&dbport. USER=&dbuser. PASSWORD=&dbpass. DATABASE=&dbname.); * connection string for POSTGRES;

/* taget DB libname*/
libname target &database. &sql_passthru_connection. schema=&dbschema.; 

/* debug options */
/*options  msglevel=i sastrace=',,,d' sastraceloc=saslog nostsuffix;*/
/*options sql_ip_trace=(note,source);*/

PROC SQL ;
	CONNECT TO &database. (&sql_passthru_connection.);
	*EXECUTE (DROP VIEW &dbschema..cdm_activity_custom_attr
	) BY &database.;
   EXECUTE (
		CREATE OR REPLACE VIEW &dbschema..cdm_activity_custom_attr AS
		SELECT
		    activity_id,
		    activity_version_id,
		    property_nm AS attribute_nm,
		    property_datatype_cd AS attribute_data_type_cd,
		    property_val AS attribute_val,
			CASE 
				WHEN property_datatype_cd in ('BOOLEAN', 'TEXT') THEN property_val 
				ELSE CAST(NULL AS VARCHAR(1024)) END as attribute_character_val,
			CASE 
				WHEN property_datatype_cd = 'DATEONLY' THEN CAST(property_val AS DATE)
				WHEN property_datatype_cd = 'DATETIME' THEN CAST(property_val AS TIMESTAMP)
				ELSE CAST(NULL AS TIMESTAMP) END as attribute_dttm_val,
			CASE
				WHEN property_datatype_cd = 'DOUBLE' THEN CAST(property_val AS DECIMAL(17,2))
				WHEN property_datatype_cd = 'INTEGER' THEN CAST(property_val AS INTEGER)
				ELSE CAST(NULL AS DECIMAL(17,2)) END as attribute_numeric_val,
/*			property_val AS attribute_character_val,*/
/*		    CAST(NULL AS DECIMAL(17,2)) AS attribute_numeric_val,*/
/*		    CAST(NULL AS TIMESTAMP) AS attribute_dttm_val,*/
		    CAST(NULL AS VARCHAR(60)) AS updated_by_nm,
		    CAST(NULL AS TIMESTAMP) AS updated_dttm
		FROM &dbschema..md_activity_custom_prop_all
	) BY &database.;
   DISCONNECT FROM &database.;
QUIT;

PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (
		CREATE OR REPLACE VIEW &dbschema..cdm_activity_detail AS
		SELECT
		    activity_id,                        
		    activity_version_id,                
		    valid_from_dttm,                    
		    valid_to_dttm,                      
		    activity_status_cd      AS status_cd,
		    activity_nm,
		    activity_desc,
		    activity_cd,
		    activity_category_nm,
		    last_published_dttm,    
		    CAST('360' AS VARCHAR(10)) AS source_system_cd,
		    CAST('CDM2.0' AS VARCHAR(60)) AS updated_by_nm,
		    CAST(NULL AS TIMESTAMP) AS updated_dttm
		FROM &dbschema..md_activity_all
	) BY &database.;
   DISCONNECT FROM &database.;
QUIT;

PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   *EXECUTE (DROP VIEW &dbschema..cdm_activity_x_task 
	) BY &database.;
   EXECUTE (
		CREATE OR REPLACE VIEW &dbschema..cdm_activity_x_task AS
		SELECT
		    x.activity_id,                              
		    x.activity_version_id,                      
		    x.task_id,
		    t.task_version_id,                
		    CAST(NULL AS VARCHAR(60)) AS updated_by_nm,
		    CAST(NULL AS TIMESTAMP) AS updated_dttm
		FROM &dbschema..md_activity_x_task_all x
		left join &dbschema..md_task t on t.task_id=x.task_id
	) BY &database.;
   DISCONNECT FROM &database.;
QUIT;

PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (
		CREATE OR REPLACE VIEW &dbschema..cdm_audience_detail AS
		SELECT
		    audience_id,                  
		    audience_nm,                  
		    audience_desc,                
		    created_user_nm,              
		    audience_schedule_flg,        
		    audience_source_nm,           
		    audience_data_source_nm,      
		    create_dttm,                  
		    delete_dttm,                  
		    update_dttm AS updated_dttm   
		FROM &dbschema..md_audience
	) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (
		CREATE OR REPLACE VIEW &dbschema..cdm_audience_occur_detail AS
		SELECT
		    audience_id,                                  
		    aud_occurrence_id AS audience_occur_id,       
		    execution_status_cd,                          
		    audience_size_val AS audience_size_cnt,      
		    started_by_nm,                               
		    occurrence_type_nm,                          
		    start_tm AS start_dttm,                      
		    end_tm AS end_dttm,                          
		    update_dttm AS updated_dttm                  
		FROM &dbschema..md_audience_occurrence
	) BY &database.;
   DISCONNECT FROM &database.;
QUIT;

PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (
		CREATE OR REPLACE VIEW &dbschema..cdm_audience_x_segment AS
		SELECT
		    audience_id,    
		    segment_id      
		FROM &dbschema..md_audience_x_segment
	) BY &database.;
   DISCONNECT FROM &database.;
QUIT;

PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (
		CREATE OR REPLACE VIEW &dbschema..cdm_business_context AS
		SELECT
		    business_context_id,                        
		    business_context_nm,                        
		    business_context_src_cd
		        AS business_context_type_cd,            
		    /* Not available in source */
		    CAST(NULL AS VARCHAR(10)) AS source_system_cd,
		    CAST(NULL AS VARCHAR(60)) AS updated_by_nm,
		    CAST(NULL AS TIMESTAMP) AS updated_dttm
		FROM &dbschema..md_business_context
	) BY &database.;
   DISCONNECT FROM &database.;
QUIT;

PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   *EXECUTE (
		drop VIEW &dbschema..cdm_contact_history
	) BY &database.;
   EXECUTE (
		CREATE OR REPLACE VIEW &dbschema..cdm_contact_history AS
		SELECT
		    contact_id,                                 
		    identity_id,                                
		    contact_nm,                                 
		    CAST(contact_dttm AS DATE) AS contact_dt,          
		    contact_dttm,                                      
		    CAST(NULL AS VARCHAR(3)) AS contact_status_cd,
		    CAST(NULL AS CHAR(1)) AS optimization_backfill_flg,
		    CAST(NULL AS VARCHAR(32)) AS external_contact_info_1_id,       
		    CAST(NULL AS VARCHAR(32)) AS external_contact_info_2_id,      
		    response_tracking_cd AS rtc_id,                    
		    CAST(NULL AS VARCHAR(10)) AS source_system_cd,
		    CAST(NULL AS VARCHAR(60)) AS updated_by_nm,
		    load_dttm AS updated_dttm,                        
		    control_group_flg,                                
		    contact_dttm_tz,                                  
		    audience_id,                                      
		    aud_occurrence_id AS audience_occur_id,           
		    context_type_nm,                                  
		    context_val                                       
		FROM &dbschema..contact_history
	) BY &database.;
   DISCONNECT FROM &database.;
QUIT;

PROC SQL;
   CONNECT TO &database. (&sql_passthru_connection.);
   *EXECUTE (
		DROP VIEW &dbschema..cdm_identity_attr
	) BY &database.;
   EXECUTE (
		CREATE OR REPLACE VIEW &dbschema..cdm_identity_attr AS
		SELECT
		    identity_id,                               
		    identifier_type_id,                        
		    user_identifier_val,                       
		    entrytime AS entry_dttm,                   
		    CAST(NULL AS TIMESTAMP) AS valid_from_dttm,
		    CAST(NULL AS TIMESTAMP) AS valid_to_dttm,
		    CAST(NULL AS VARCHAR(10)) AS source_system_cd,
		    CAST(NULL AS VARCHAR(60)) AS updated_by_nm,
		    processed_dttm AS updated_dttm             
		FROM &dbschema..identity_attributes
		WHERE identifier_type_id='subject_id'
	) BY &database.;
   DISCONNECT FROM &database.;
QUIT;

PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (
		CREATE OR REPLACE VIEW &dbschema..cdm_identity_map AS
		SELECT
		    target_identity_id AS identity_id,   
		    /* Not available in source */
		    CAST(NULL AS VARCHAR(40)) AS identity_type_cd,
		    CAST(NULL AS VARCHAR(60)) AS updated_by_nm,
		    processed_dttm AS updated_dttm       
		FROM &dbschema..identity_map
	) BY &database.;
   DISCONNECT FROM &database.;
QUIT;

PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (
		CREATE OR REPLACE VIEW &dbschema..cdm_occurrence_detail AS
		SELECT
		    occurrence_id,                       
		    start_tm AS start_dttm,              
		    end_tm AS end_dttm,                  
		    occurrence_no,                       
		    occurrence_type_nm AS occurrence_type_cd,     
		    object_id AS occurrence_object_id,            
		    object_type_nm AS occurrence_object_type_cd,  
		    execution_status_cd,                          
		    CAST('360' AS VARCHAR(10)) AS source_system_cd,
      		CAST('CDM2.0' AS VARCHAR(60)) AS updated_by_nm,
			CAST(NULL AS TIMESTAMP) AS updated_dttm
		FROM &dbschema..md_occurrence
	) BY &database.;
   DISCONNECT FROM &database.;
QUIT;

PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (
		CREATE OR REPLACE VIEW &dbschema..cdm_response_history AS
		SELECT
		    response_id,                                  
		    response_nm AS response_cd,                   
		    CAST(response_dttm AS DATE) AS response_dt,   
		    response_dttm,                                      
		    detail_id_hex AS external_contact_info_1_id,        
		    session_id_hex AS external_contact_info_2_id,       
		    response_channel_nm AS response_channel_cd,         
		    identity_id,                                        
		    response_tracking_cd AS rtc_id,                     
		    CAST(NULL AS VARCHAR(60)) AS response_type_cd,
		    CAST(NULL AS CHAR(1)) AS conversion_flg,
		    CAST(NULL AS CHAR(1)) AS inferred_response_flg,
		    CAST(NULL AS VARCHAR(40)) AS content_id,
		    CAST(NULL AS VARCHAR(40)) AS content_version_id,
		    CAST(NULL AS DECIMAL(17,2)) AS response_val_amt,
		    CAST(NULL AS VARCHAR(10)) AS source_system_cd,
		    CAST(NULL AS VARCHAR(36)) AS contact_id,            
		    CAST(NULL AS VARCHAR(32)) AS content_hash_val,
		    CAST(NULL AS VARCHAR(60)) AS updated_by_nm,
		    load_dttm AS updated_dttm,                         
		    properties_map_doc,                           
		    response_dttm_tz,                             
		    audience_id,                                  
		    aud_occurrence_id AS audience_occur_id,       
		    context_type_nm,                              
		    context_val                                   
		FROM &dbschema..response_history
	) BY &database.;
   DISCONNECT FROM &database.;
QUIT;

PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (
		CREATE OR REPLACE VIEW &dbschema..cdm_rtc_detail AS
		SELECT
		    rtc_id,
		    occurrence_no                         AS task_occurrence_no,
		    rtc_dttm                              AS processed_dttm,
		    segment_version_id,
		    task_version_id,
		    occurrence_id,
		    segment_id,
		    task_id,
		    CAST(NULL AS CHAR(1))                 AS response_tracking_flg,
		    CAST(NULL AS VARCHAR(30))             AS execution_status_cd,
		    CAST(NULL AS CHAR(1))                 AS deleted_flg,
		    CAST(NULL AS VARCHAR(10))             AS source_system_cd,
		    CAST(NULL AS VARCHAR(60))             AS updated_by_nm,
		    CAST(NULL AS TIMESTAMP)               AS updated_dttm
		FROM &dbschema..md_rtc
	) BY &database.;
   DISCONNECT FROM &database.;
QUIT;

PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   *EXECUTE (
		DROP VIEW &dbschema..cdm_segment_detail
	) BY &database.;
   EXECUTE (
		CREATE OR REPLACE VIEW &dbschema..cdm_segment_detail AS
		SELECT
		    s.segment_version_id,
		    s.segment_id,
		    s.valid_from_dttm,
		    s.valid_to_dttm,
		    s.segment_nm,
		    s.segment_desc,
		    s.segment_category_nm,
		    s.segment_cd,
		    s.segment_status_cd,
		    s.segment_map_id,
			s.segment_src_cd                        AS segment_src_nm,
		    CAST(NULL AS VARCHAR(10))             AS source_system_cd,
		    CAST(NULL AS VARCHAR(60))             AS updated_by_nm,
		    CAST(NULL AS TIMESTAMP)               AS updated_dttm,
		    m.segment_map_version_id
		FROM &dbschema..md_segment_all s
			left join &dbschema..md_segment_map as m on s.segment_map_id=m.segment_map_id
	) BY &database.;
   DISCONNECT FROM &database.;
QUIT;

PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
	*EXECUTE (
		DROP VIEW &dbschema..cdm_segment_custom_attr
	) BY &database.;
	EXECUTE (
		CREATE OR REPLACE VIEW &dbschema..cdm_segment_custom_attr AS
		SELECT		    
		    segment_version_id,
		    segment_id,
		    property_nm                    AS attribute_nm,
		    property_datatype_cd           AS attribute_data_type_cd,
		    property_val                   AS attribute_val,
			CASE 
				WHEN property_datatype_cd in ('BOOLEAN', 'TEXT') THEN property_val 
				ELSE CAST(NULL AS VARCHAR(1024)) END as attribute_character_val,
			CASE 
				WHEN property_datatype_cd = 'DATEONLY' THEN CAST(property_val AS DATE)
				WHEN property_datatype_cd = 'DATETIME' THEN CAST(property_val AS TIMESTAMP)
				ELSE CAST(NULL AS TIMESTAMP) END as attribute_dttm_val,
			CASE
				WHEN property_datatype_cd = 'DOUBLE' THEN CAST(property_val AS DECIMAL(17,2))
				WHEN property_datatype_cd = 'INTEGER' THEN CAST(property_val AS INTEGER)
				ELSE CAST(NULL AS DECIMAL(17,2)) END as attribute_numeric_val,
/*			property_val                   AS attribute_character_val,*/
/*		    CAST(NULL AS DECIMAL(17,2))    AS attribute_numeric_val,*/
/*		    CAST(NULL AS TIMESTAMP)        AS attribute_dttm_val,*/
		    CAST(NULL AS VARCHAR(60))      AS updated_by_nm,
		    CAST(NULL AS TIMESTAMP)        AS updated_dttm
		FROM &dbschema..md_segment_custom_prop_all
	) BY &database.;
   DISCONNECT FROM &database.;
QUIT;

PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   *EXECUTE (
		DROP VIEW &dbschema..cdm_segment_map
	) BY &database.;
   EXECUTE (
		CREATE OR REPLACE VIEW &dbschema..cdm_segment_map AS
		SELECT
		    segment_map_version_id,
		    segment_map_id,
		    valid_from_dttm,
		    valid_to_dttm,
		    segment_map_nm,
		    segment_map_desc,
		    segment_map_src_cd                  AS segment_map_category_nm,
		    segment_map_cd,
		    segment_map_status_cd,
		    segment_map_category_nm 			AS segment_map_src_nm,
		    CAST('360' AS VARCHAR(10))          AS source_system_cd,
		    CAST('CDM2.0' AS VARCHAR(60))       AS updated_by_nm,
		    CAST(NULL AS TIMESTAMP)             AS updated_dttm
		FROM &dbschema..md_segment_map_all
	) BY &database.;
   DISCONNECT FROM &database.;
QUIT;

PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
  * EXECUTE (
		DROP VIEW &dbschema..cdm_segment_map_custom_attr
	) BY &database.;
   EXECUTE (
		CREATE OR REPLACE VIEW &dbschema..cdm_segment_map_custom_attr AS
		SELECT
		    segment_map_version_id,
		    segment_map_id,
		    property_nm                  AS attribute_nm,
		    property_datatype_cd         AS attribute_data_type_cd,
		    property_val                 AS attribute_val,

			CASE 
				WHEN property_datatype_cd in ('BOOLEAN', 'TEXT') THEN property_val 
				ELSE CAST(NULL AS VARCHAR(1024)) END as attribute_character_val,
			CASE 
				WHEN property_datatype_cd = 'DATEONLY' THEN CAST(property_val AS DATE)
				WHEN property_datatype_cd = 'DATETIME' THEN CAST(property_val AS TIMESTAMP)
				ELSE CAST(NULL AS TIMESTAMP) END as attribute_dttm_val,
			CASE
				WHEN property_datatype_cd = 'DOUBLE' THEN CAST(property_val AS DECIMAL(17,2))
				WHEN property_datatype_cd = 'INTEGER' THEN CAST(property_val AS INTEGER)
				ELSE CAST(NULL AS DECIMAL(17,2)) END as attribute_numeric_val,

/*			property_val                 AS attribute_character_val,*/
/*		    CAST(NULL AS DECIMAL(17,2))  AS attribute_numeric_val,*/
/*		    CAST(NULL AS TIMESTAMP)      AS attribute_dttm_val,*/
		    CAST(NULL AS VARCHAR(60))  AS updated_by_nm,
		    CAST(NULL AS TIMESTAMP)    AS updated_dttm
		FROM &dbschema..md_segment_map_custom_prop_all
	) BY &database.;
   DISCONNECT FROM &database.;
QUIT;

PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (
		CREATE OR REPLACE VIEW &dbschema..cdm_segment_test AS
		SELECT
		    test_cd,
		    task_version_id,
		    task_id,
		    test_nm,
		    test_type_nm,
		    test_enabled_flg,
		    test_sizing_type_nm,
		    test_cnt,
		    test_pct,
		    stratified_sampling_flg,
		    stratified_samp_criteria_txt,
		    CAST(NULL AS TIMESTAMP) AS updated_dttm
		FROM &dbschema..md_segment_test_all
	) BY &database.;
   DISCONNECT FROM &database.;
QUIT;

PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   EXECUTE (
		CREATE OR REPLACE VIEW &dbschema..cdm_segment_test_x_segment AS
		SELECT
		    test_cd,
		    task_version_id,
		    task_id,
		    segment_id,
		    CAST(NULL AS TIMESTAMP) AS updated_dttm
		FROM &dbschema..md_segment_test_x_segment_all
	) BY &database.;
   DISCONNECT FROM &database.;
QUIT;

PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
  *EXECUTE (
		DROP VIEW &dbschema..cdm_task_custom_attr
	) BY &database.;
	EXECUTE (
		CREATE OR REPLACE VIEW &dbschema..cdm_task_custom_attr AS
		SELECT
		    task_version_id,
		    task_id,
		    property_nm                  AS attribute_nm,
		    property_datatype_nm         AS attribute_data_type_cd,
		    property_val                 AS attribute_val,
			CASE 
				WHEN property_datatype_nm in ('BOOLEAN', 'TEXT') THEN property_val 
				ELSE CAST(NULL AS VARCHAR(1024)) END as attribute_character_val,
			CASE 
				WHEN property_datatype_nm = 'DATEONLY' THEN CAST(property_val AS DATE)
				WHEN property_datatype_nm = 'DATETIME' THEN CAST(property_val AS TIMESTAMP)
				ELSE CAST(NULL AS TIMESTAMP) END as attribute_dttm_val,
			CASE
				WHEN property_datatype_nm = 'DOUBLE' THEN CAST(property_val AS DECIMAL(17,2))
				WHEN property_datatype_nm = 'INTEGER' THEN CAST(property_val AS INTEGER)
				ELSE CAST(NULL AS DECIMAL(17,2)) END as attribute_numeric_val,
/*		    property_val                 AS attribute_character_val,*/
/*		    CAST(NULL AS DECIMAL(17,2))  AS attribute_numeric_val,*/
/*		    CAST(NULL AS TIMESTAMP)      AS attribute_dttm_val,*/
		    CAST(NULL AS VARCHAR(256))   AS extension_attribute_nm,
		    CAST(NULL AS VARCHAR(60))    AS updated_by_nm,
		    CAST(NULL AS TIMESTAMP)      AS updated_dttm
		FROM &dbschema..md_task_custom_prop_all
	) BY &database.;
   DISCONNECT FROM &database.;
QUIT;

PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
*   EXECUTE (
		DROP VIEW &dbschema..cdm_task_detail
	) BY &database.;
   EXECUTE (
		CREATE OR REPLACE VIEW &dbschema..cdm_task_detail AS
		SELECT
		    task_version_id,
		    task_id,
		    valid_from_dttm,
		    valid_to_dttm,
		    task_nm,
		    task_desc,
		    task_type_nm,
		    task_status_cd,
		    task_subtype_nm AS task_subtype_nm,
		    task_cd,
		    task_delivery_type_nm,
		    CASE WHEN task_status_cd='active' THEN '1'
				ELSE '0' END AS active_flg,
		    CAST(NULL AS CHAR(1)) AS saved_flg,
		    '1' AS published_flg,
		    owner_nm,
		    secondary_status AS modified_status_cd,
		    created_user_nm,
		    CAST(valid_from_dttm AS DATE) AS created_dt,
		    scheduled_start_dttm,
		    scheduled_end_dttm,
		    scheduled_flg,
		    maximum_period_expression_cnt,
		    limit_period_unit_cnt,
		    CAST(impressions_life_time_cnt AS CHAR(1))as limit_by_total_impression_flg,
		    CAST(NULL AS TIMESTAMP) AS export_dttm,
		    CAST(NULL AS CHAR(1)) AS update_contact_history_flg,
		    CAST(NULL AS VARCHAR(60)) AS subject_type_nm,
		    CAST(NULL AS DECIMAL(17,2)) AS min_budget_offer_amt,
		    CAST(NULL AS DECIMAL(17,2)) AS max_budget_offer_amt,
		    CAST(NULL AS DECIMAL(17,2)) AS min_budget_amt,
		    CAST(NULL AS DECIMAL(17,2)) AS max_budget_amt,
		    CAST(NULL AS DECIMAL(17,2)) AS budget_unit_cost_amt,
		    CAST(NULL AS VARCHAR(3)) AS recurr_type_cd,
		    CAST(NULL AS DECIMAL(17,2)) AS budget_unit_usage_amt,
		    CAST(NULL AS CHAR(1)) AS standard_reply_flg,
		    CAST(NULL AS CHAR(1)) AS staged_flg,
		    channel_nm AS contact_channel_cd,
		    CAST(NULL AS VARCHAR(36)) AS campaign_id,
		    business_context_id,
		    CAST('360' AS VARCHAR(10)) AS source_system_cd,
      		CAST('CDM2.0' AS VARCHAR(60)) AS updated_by_nm,
		    last_published_dttm AS updated_dttm,
		    recurring_schedule_flg,
		    control_group_action_nm,
		    stratified_sampling_action_nm,
		    segment_tests_flg
		FROM &dbschema..md_task_all
	) BY &database.;
   DISCONNECT FROM &database.;
QUIT;

PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
*   EXECUTE (
		DROP VIEW &dbschema..cdm_content_detail
	) BY &database.;
   EXECUTE (
		CREATE OR REPLACE VIEW &dbschema..cdm_content_detail AS
		SELECT
		    creative_version_id AS content_version_id,
		    creative_id AS content_id,
		    valid_from_dttm,
		    valid_to_dttm,
		    creative_nm AS contact_content_nm,
		    creative_desc AS contact_content_desc,
		    creative_type_nm AS contact_content_type_nm,
		    creative_status_cd AS contact_content_status_cd,
		    creative_category_nm AS contact_content_category_nm,
		    CAST(NULL AS VARCHAR(100)) AS contact_content_class_nm,
		    creative_cd AS contact_content_cd,
		    CASE WHEN creative_status_cd='active' THEN '1' ELSE '0' END AS active_flg,
		    owner_nm,
		    created_user_nm,
		    CAST(valid_from_dttm AS DATE) AS created_dt,
		    creative_txt AS external_reference_txt,
		    CAST(NULL AS VARCHAR(1024)) AS external_reference_url_txt,
		    CAST(NULL AS VARCHAR(10)) AS source_system_cd,
			CAST(NULL AS VARCHAR(60)) AS updated_by_nm,
		    last_published_dttm AS updated_dttm
		FROM &dbschema..md_creative_all
		UNION ALL
		SELECT
		    message_version_id AS content_version_id,
		    message_id AS content_id,
		    valid_from_dttm,
		    valid_to_dttm,
		    message_nm AS contact_content_nm,
		    message_desc AS contact_content_desc,
		    message_type_nm AS contact_content_type_nm,
		    message_status_cd AS contact_content_status_cd,
		    message_category_nm AS contact_content_category_nm,
		    CAST(NULL AS VARCHAR(100)) AS contact_content_class_nm,
		    message_cd AS contact_content_cd,
		    CASE WHEN message_status_cd='active' THEN '1' ELSE '0' END AS active_flg,
		    owner_nm,
		    created_user_nm,
		    CAST(valid_from_dttm AS DATE) AS created_dt,
		    CAST(NULL AS VARCHAR(1024)) AS external_reference_txt,
		    CAST(NULL AS VARCHAR(1024)) AS external_reference_url_txt,
		    CAST(NULL AS VARCHAR(10)) AS source_system_cd,
			CAST(NULL AS VARCHAR(60)) AS updated_by_nm,
		    last_published_dttm AS updated_dttm
		FROM &dbschema..md_message_all
	) BY &database.;
   DISCONNECT FROM &database.;
QUIT;

PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
  * EXECUTE (
		DROP VIEW &dbschema..cdm_content_custom_attr 
	) BY &database.;
	EXECUTE (
		CREATE OR REPLACE VIEW &dbschema..cdm_content_custom_attr AS
		SELECT
		    creative_id AS content_id,
		    creative_version_id AS content_version_id,
		    property_nm AS attribute_nm,
		    property_datatype_cd AS attribute_data_type_cd,
		    property_val AS attribute_val,
			CASE 
				WHEN property_datatype_cd in ('BOOLEAN', 'TEXT') THEN property_val 
				ELSE CAST(NULL AS VARCHAR(1024)) END as attribute_character_val,
			CASE 
				WHEN property_datatype_cd = 'DATEONLY' THEN CAST(property_val AS DATE)
				WHEN property_datatype_cd = 'DATETIME' THEN CAST(property_val AS TIMESTAMP)
				ELSE CAST(NULL AS TIMESTAMP) END as attribute_dttm_val,
			CASE
				WHEN property_datatype_cd = 'DOUBLE' THEN CAST(property_val AS DECIMAL(17,2))
				WHEN property_datatype_cd = 'INTEGER' THEN CAST(property_val AS INTEGER)
				ELSE CAST(NULL AS DECIMAL(17,2)) END as attribute_numeric_val,
/*		    property_val AS attribute_character_val,*/
/*		    CAST(NULL AS DECIMAL(17,2)) AS attribute_numeric_val,*/
/*		    CAST(NULL AS TIMESTAMP) AS attribute_dttm_val,*/
		    CAST(NULL AS VARCHAR(256)) AS extension_attribute_nm,
		    CAST(NULL AS VARCHAR(60)) AS updated_by_nm,
		    CAST(NULL AS TIMESTAMP) AS updated_dttm
		FROM &dbschema..md_creative_custom_prop_all
		UNION ALL
		SELECT
		    message_id AS content_id,
		    message_version_id AS content_version_id,
		    property_nm AS attribute_nm,
		    property_datatype_cd AS attribute_data_type_cd,
		    property_val AS attribute_val,
			CASE 
				WHEN property_datatype_cd in ('BOOLEAN', 'TEXT') THEN property_val 
				ELSE CAST(NULL AS VARCHAR(1024)) END as attribute_character_val,
			CASE 
				WHEN property_datatype_cd = 'DATEONLY' THEN CAST(property_val AS DATE)
				WHEN property_datatype_cd = 'DATETIME' THEN CAST(property_val AS TIMESTAMP)
				ELSE CAST(NULL AS TIMESTAMP) END as attribute_dttm_val,
			CASE
				WHEN property_datatype_cd = 'DOUBLE' THEN CAST(property_val AS DECIMAL(17,2))
				WHEN property_datatype_cd = 'INTEGER' THEN CAST(property_val AS INTEGER)
				ELSE CAST(NULL AS DECIMAL(17,2)) END as attribute_numeric_val,
/*		    property_val AS attribute_character_val,*/
/*		    CAST(NULL AS DECIMAL(17,2)) AS attribute_numeric_val,*/
/*		    CAST(NULL AS TIMESTAMP) AS attribute_dttm_val,*/
		    CAST(NULL AS VARCHAR(256)) AS extension_attribute_nm,
		    CAST(NULL AS VARCHAR(60)) AS updated_by_nm,
		    CAST(NULL AS TIMESTAMP) AS updated_dttm
		FROM &dbschema..md_message_custom_prop_all
	) BY &database.;
   DISCONNECT FROM &database.;
QUIT;

PROC SQL ;
   CONNECT TO &database. (&sql_passthru_connection.);
   *EXECUTE (
		DROP VIEW &dbschema..cdm_rtc_x_content 
	) BY &database.;
   EXECUTE (
		CREATE OR REPLACE VIEW &dbschema..cdm_rtc_x_content AS
		SELECT 
		    CAST(NULL AS VARCHAR(36)) AS rtc_x_content_sk,
			rtc_id, 
			CASE WHEN POSITION('|' in content_map_doc) > 10 THEN CAST(substr(content_map_doc,POSITION('|' in content_map_doc)+1,32) AS VARCHAR(40)) 
				ELSE CAST(NULL AS VARCHAR(40)) 
				END AS content_version_id, 
		    CAST(NULL AS VARCHAR(32)) AS content_hash_val,
		    CAST(NULL AS INTEGER) AS sequence_no,
		    CAST(NULL AS VARCHAR(60)) AS updated_by_nm,
		    CAST(NULL AS TIMESTAMP) AS updated_dttm,
		    CAST(NULL AS VARCHAR(36)) AS content_id
		FROM &dbschema..MD_RTC t1
		WHERE content_map_doc IS NOT NULL
	) BY &database.;
   DISCONNECT FROM &database.;
QUIT;
