# CI 360 CDM as Views

CI_360_CDM_Views.sas contains the definitions of 24 views that map data from the CI 360 Unified Data Model (UDM) to the CI 360 Common Data Model (CDM) format. These views provide a compatibility layer for existing data flows that rely on CDM tables by exposing the underlying UDM data through CDM-based views. 

New deployments should bypass these views and use the underlying UDM tables directly. 

## Requirements 
- Load these UDM tables into the relational database that hosts the CDM tables. 
    - Snapshot metadata tables
        - md_activity_all
        - md_activity_custom_prop_all
        - md_activity_x_task_all
        - md_audience
        - md_audience_occurrence
        - md_audience_x_segment
        - md_business_context
        - md_creative_all
        - md_creative_custom_prop_all
        - md_message_all
        - md_message_custom_prop_all
        - md_occurrence
        - md_rtc
        - md_segment_all
        - md_segment_custom_prop_all
        - md_segment_map
        - md_segment_map_all
        - md_segment_map_custom_prop_all
        - md_segment_test_all
        - md_segment_test_x_segment_all
        - md_task
        - md_task_all
        - md_task_custom_prop_all

    - Snapshot identity tables
        - identity_attributes
        - identity_map

    - Detail tables
        - contact_history
        - response_history

- The UDM tables need to be Schema version 20 or above.


## Running CI_360_CDM_Views.sas
CI_360_CDM_Views.sas is written to run from SAS Studio or SAS Entreprise Guide.
- Provide the database connection details.
- The views have the same name as the CDM tables as they are intended to replace the CDM tables. Rename, move or drop the existing CDM tables first. 

Alternatively, to run this script in directly in your data client
- remove all syntax except the CREATE VIEW blocks 
- and replace &dbschema. with your target schema.

## Supported databases
The views definition have been created and tested using a Postgres database. The SQL is database agnostic except for the **POSITION** function used twice in CDM_RTC_X_CONTENT to return the location of a substring, '|' in this case, from the content_map_doc column. Change the POSITION function to your database equivalent as needed. A few examples:
- POSSTR('|', content_map_doc)  /* DB2 */
- CHARINDEX( '|', content_map_doc) /* Microsoft SQL Server */
- INSTR (content_map_doc, '|') /* Oracle */
- POSITION('|' IN content_map_doc) /* MySQL supports the same as PostgreSQL */

## Supported CDM tables
Of the 35 tables defined by the CDM, 24 are provided as views. The other 11 tables are outside the scope of CI 360 data, as they are either empty or designed to hold CI 6.x data. Only CI 360 data is available through the CDM views.

- CDM tables available as views
    - CDM_ACTIVITY_CUSTOM_ATTR 
    - CDM_ACTIVITY_DETAIL
    - CDM_ACTIVITY_X_TASK
    - CDM_AUDIENCE_DETAIL
    - CDM_AUDIENCE_OCCUR_DETAIL
    - CDM_AUDIENCE_X_SEGMENT 
    - CDM_BUSINESS_CONTEXT
    - CDM_CONTACT_HISTORY
    - CDM_CONTENT_CUSTOM_ATTR
    - CDM_CONTENT_DETAIL 
    - CDM_IDENTITY_ATTR
    - CDM_IDENTITY_MAP
    - CDM_OCCURRENCE_DETAIL
    - CDM_REPONSE_HISTORY
    - CDM_RTC_DETAIL
    - CDM_RTC_X_CONTENT
    - CDM_SEGMENT_CUSTOM_ATTR
    - CDM_SEGMENT_DETAIL
    - CDM_SEGMENT_MAP
    - CDM_SEGMENT_MAP_CUSTOM_ATTR 
    - CDM_SEGMENT_TEST
    - CDM_SEGMENT_TEST_X_SEGMENT
    - CDM_TASK_DETAIL
    - CDM_TASK_CUSTOM_ATTR

- CDM tables without CI 360 data that are not represented as views
    - CDM_CAMPAIGN_CUSTOM_ATTR
    - CDM_CAMPAIGN_DETAIL
    - CDM_CONTACT_CHANNEL
    - CDM_CONTACT_STATUS
    - CDM_DYN_CONTENT_CUSTOM_ATTR
    - CDM_IDENTIFIER_TYPE
    - CDM_IDENTITY_TYPE
    - CDM_RESPONSE_CHANNEL 
    - CDM_RESPONSE_EXTENDED_ATTR
    - CDM_RESPONSE_LOOKUP 
    - CDM_RESPONSE_TYPE

## Decomission CDM tables
Remove the CDM table from your CI 360 download and database load processes.