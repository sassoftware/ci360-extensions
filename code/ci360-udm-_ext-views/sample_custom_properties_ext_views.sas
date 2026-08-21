
/********************************************************************************/
/* Copyright (c) 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                          */
/********************************************************************************/
/* Sample code to generate MD_*_CUSTOM_PROP_EXT views                           */
/* Adapt table names, property names and data type casting to your needs.       */
/* Get property names from MD_*_CUSTOM_PROP tables or CI 360 Custom Properties. */
/********************************************************************************/

/* Assign the UDM Target library - ADAPT TO YOUR DATABASE -----*/
%let sql_passthru_connection = SERVER=xxxxxxxxx PORT=5432 USER=xxxxx PASSWORD=xxxxxxxxxxx DATABASE=postgres;
%let dbschema = udm;
%let database = Postgres;
libname TARGET &database. &sql_passthru_connection. schema=&dbschema.; * target DB location;

/*  */
PROC SQL;
	CONNECT to Postgres (&sql_passthru_connection.);

	%if %sysfunc(exist(TARGET.MD_TASK_CUSTOM_PROP_EXT )) %then %do;
	   EXECUTE (drop VIEW &dbschema..MD_TASK_CUSTOM_PROP_EXT ) by &database.;
	%end;
	EXECUTE (
	   create VIEW &dbschema..MD_TASK_CUSTOM_PROP_EXT as
	   select TASK_id
	      , MAX(TASK_status_cd) AS TASK_status_cd
	      , MAX(CASE WHEN property_nm = 'Campaign_End_Date' THEN CAST(property_val AS date) END) AS CAMPAIGN_END_DATE
	      , MAX(CASE WHEN property_nm = 'Campaign_Group' THEN property_val END) AS CAMPAIGN_GROUP
	      , MAX(CASE WHEN property_nm = 'Cost' THEN CAST(property_val AS decimal) END) AS COST
	      , MAX(CASE WHEN property_nm = 'Reminder_Duration' THEN CAST(property_val AS int) END) AS REMINDER_DURATION
	      , MAX(CASE WHEN property_nm = 'Sender_ID (SMS)' THEN property_val END) AS SENDER_ID_SMS
	      , MAX(CASE WHEN property_nm = 'SMS_Opt-in' THEN property_val END) AS SMS_OPT_IN
	   from &dbschema..MD_TASK_CUSTOM_PROP
	   group by TASK_id
	) by &database.;


	%if %sysfunc(exist(TARGET.MD_TASK_CUSTOM_PROP_ALL_EXT )) %then %do;
	   EXECUTE (drop VIEW &dbschema..MD_TASK_CUSTOM_PROP_ALL_EXT ) by &database.;
	%end;
	EXECUTE (
	   create VIEW &dbschema..MD_TASK_CUSTOM_PROP_ALL_EXT as
	   select TASK_version_id
	      , MAX(TASK_status_cd) AS TASK_status_cd
	      , MAX(CASE WHEN property_nm = 'Campaign_End_Date' THEN CAST(property_val AS date) END) AS CAMPAIGN_END_DATE
	      , MAX(CASE WHEN property_nm = 'Campaign_Group' THEN property_val END) AS CAMPAIGN_GROUP
	      , MAX(CASE WHEN property_nm = 'Cost' THEN CAST(property_val AS decimal) END) AS COST
	      , MAX(CASE WHEN property_nm = 'Reminder_Duration' THEN CAST(property_val AS int) END) AS REMINDER_DURATION
	      , MAX(CASE WHEN property_nm = 'Sender_ID (SMS)' THEN property_val END) AS SENDER_ID_SMS
	      , MAX(CASE WHEN property_nm = 'SMS_Opt-in' THEN property_val END) AS SMS_OPT_IN
	   from &dbschema..MD_TASK_CUSTOM_PROP_ALL
	   group by TASK_version_id
	) by &database.;

	%if %sysfunc(exist(TARGET.MD_CREATIVE_CUSTOM_PROP_EXT )) %then %do;
	   EXECUTE (drop VIEW &dbschema..MD_CREATIVE_CUSTOM_PROP_EXT ) by &database.;
	%end;
	EXECUTE (
	   create VIEW &dbschema..MD_CREATIVE_CUSTOM_PROP_EXT as
	   select CREATIVE_id
	      , MAX(CREATIVE_status_cd) AS CREATIVE_status_cd
	      , MAX(CASE WHEN property_nm = 'Asset_usage' THEN property_val END) AS ASSET_USAGE
	      , MAX(CASE WHEN property_nm = 'Prod_Category' THEN property_val END) AS PROD_CATEGORY
	   from &dbschema..MD_CREATIVE_CUSTOM_PROP
	   group by CREATIVE_id
	) by &database.;


	%if %sysfunc(exist(TARGET.MD_MESSAGE_CUSTOM_PROP_EXT )) %then %do;
	   EXECUTE (drop VIEW &dbschema..MD_MESSAGE_CUSTOM_PROP_EXT ) by &database.;
	%end;
	EXECUTE (
	   create VIEW &dbschema..MD_MESSAGE_CUSTOM_PROP_EXT as
	   select MESSAGE_id
	      , MAX(MESSAGE_status_cd) AS MESSAGE_status_cd
	      , MAX(CASE WHEN property_nm = 'Asset_usage' THEN property_val END) AS ASSET_USAGE
	      , MAX(CASE WHEN property_nm = 'GIValidityEnd' THEN CAST(property_val AS date) END) AS GIVALIDITYEND
	      , MAX(CASE WHEN property_nm = 'GIValidityStart' THEN CAST(property_val AS date) END) AS GIVALIDITYSTART
	   from &dbschema..MD_MESSAGE_CUSTOM_PROP
	   group by MESSAGE_id
	) by &database.;

	DISCONNECT FROM &database.;
QUIT;

