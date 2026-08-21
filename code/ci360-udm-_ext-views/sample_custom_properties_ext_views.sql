/********************************************************************************/
/* Copyright (c) 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                          */
/* ******************************************************************************/
/* Sample code to generate MD_*_CUSTOM_PROP_EXT views                           */
/* Adapt table names, property names and data type casting to your needs.       */
/* Get property names from MD_*_CUSTOM_PROP tables or CI 360 Custom Properties. */
/* ******************************************************************************/

   create VIEW udm.MD_TASK_CUSTOM_PROP_EXT as
   select TASK_id
      , MAX(TASK_status_cd) AS TASK_status_cd
      , MAX(CASE WHEN property_nm = 'Campaign_End_Date' THEN CAST(property_val AS date) END) AS CAMPAIGN_END_DATE
      , MAX(CASE WHEN property_nm = 'Campaign_Group' THEN property_val END) AS CAMPAIGN_GROUP
      , MAX(CASE WHEN property_nm = 'Cost' THEN CAST(property_val AS decimal) END) AS COST
      , MAX(CASE WHEN property_nm = 'Reminder_Duration' THEN CAST(property_val AS int) END) AS REMINDER_DURATION
      , MAX(CASE WHEN property_nm = 'Sender_ID (SMS)' THEN property_val END) AS SENDER_ID_SMS
      , MAX(CASE WHEN property_nm = 'SMS_Opt-in' THEN property_val END) AS SMS_OPT_IN
   from udm.MD_TASK_CUSTOM_PROP
   group by TASK_id
;

   create VIEW udm.MD_TASK_CUSTOM_PROP_ALL_EXT as
   select TASK_version_id
      , MAX(TASK_status_cd) AS TASK_status_cd
      , MAX(CASE WHEN property_nm = 'Campaign_End_Date' THEN CAST(property_val AS date) END) AS CAMPAIGN_END_DATE
      , MAX(CASE WHEN property_nm = 'Campaign_Group' THEN property_val END) AS CAMPAIGN_GROUP
      , MAX(CASE WHEN property_nm = 'Cost' THEN CAST(property_val AS decimal) END) AS COST
      , MAX(CASE WHEN property_nm = 'Reminder_Duration' THEN CAST(property_val AS int) END) AS REMINDER_DURATION
      , MAX(CASE WHEN property_nm = 'Sender_ID (SMS)' THEN property_val END) AS SENDER_ID_SMS
      , MAX(CASE WHEN property_nm = 'SMS_Opt-in' THEN property_val END) AS SMS_OPT_IN
   from udm.MD_TASK_CUSTOM_PROP_ALL
   group by TASK_version_id
;

   create VIEW udm.MD_CREATIVE_CUSTOM_PROP_EXT as
   select CREATIVE_id
      , MAX(CREATIVE_status_cd) AS CREATIVE_status_cd
      , MAX(CASE WHEN property_nm = 'Asset_usage' THEN property_val END) AS ASSET_USAGE
      , MAX(CASE WHEN property_nm = 'Prod_Category' THEN property_val END) AS PROD_CATEGORY
   from udm.MD_CREATIVE_CUSTOM_PROP
   group by CREATIVE_id
;

   create VIEW udm.MD_MESSAGE_CUSTOM_PROP_EXT as
   select MESSAGE_id
      , MAX(MESSAGE_status_cd) AS MESSAGE_status_cd
      , MAX(CASE WHEN property_nm = 'Asset_usage' THEN property_val END) AS ASSET_USAGE
      , MAX(CASE WHEN property_nm = 'GIValidityEnd' THEN CAST(property_val AS date) END) AS GIVALIDITYEND
      , MAX(CASE WHEN property_nm = 'GIValidityStart' THEN CAST(property_val AS date) END) AS GIVALIDITYSTART
   from udm.MD_MESSAGE_CUSTOM_PROP
   group by MESSAGE_id
;

