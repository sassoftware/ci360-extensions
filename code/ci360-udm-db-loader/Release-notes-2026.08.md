# UDM DB Loader Release Notes

## Version 2026.08

### Usage
- This release is tested with UDM schema V22. 
- New flags include or exclude the CDM tables and the dbtReport table from the DDL and load process.
- In February 2026, with schema version 20, a new detail_id column was added to some of the dbtReport tables as primary key. Rows from before February 2026 don't have data for that column. As data with blank values in primary key columns cannot be uploaded to the database, those rows are discarded by this utility. To analyse historical data from the dbtReport tables use the Discover detailed tables instead.

## Implementation
- Previous versions of this tool adapted the timezone of the \*_dttm_tz  columns from UTC to the local timezone. The tenant configuration should now be used to do that. Take this into account during upgrades.
- New SEGMENT_MEMBERSHIP.user_identifier_val column has a length of 5000. This will be adapted to varchar(256). IDENTITY_ATTRIBUTES.user_identifier_val is similar. Expected values for both columns are up to 170 characters long, so varchar(256) is sufficient.


## Development
- The five create\_\<database\>\_etl.sas macros have been removed. The ETL logic has been unified and split up in create_etl.sas, create_etl_full.sas and create_etl_incremental.sas.
- The METADATA_TABLE.csv has been replaced by MISSING_PRIMARY_KEYS.csv.
- The MISSING_PRIMARY_KEYS.csv can be used to flag additional columns as primary keys in the unlikely event that the schema JSON primary key flagging is incomplete. Flagging for schema V22 has been done.
