# Engage Direct Information Map Spreadsheet

This spreadsheet is intended to support the automated creation of information maps for use with ci360 and is based on the Marketing Automation version of this spreadsheet. By providing the information map details, a list of tables, folders, and data items an information map creation script will be generated complete with the specified custom attributes set for all data items. The script can be executed via SAS Studio or SAS Enterprise Guide. Note, that you can create a new information map script or you can generate "update script" to an existing information map.

Open the spreadsheet with Excel and customize for the specific tenant/business context. You will need to supply the table names, their relationships and the data item characteristics similar to how you define it in Information Map Studio.  The "data items" tab contains a "create information map" button that allows you to generate a creation script after you enter all the required information.

The spreadsheet contains additional comments to help with the usage.

**Caution**:  If you have an existing information map that you created using the SAS Information map studio and the 360 Direct application is in active/production use, do not use this spreadsheet to make updates to pre-existing data items etc. as this may result in new IDs being generated and potential issues with existing segment map and task objects.  You may however use this spreadsheet to add a new table and/or new data items that did not previously exist.  This does not apply if you always use this spreadsheet to make information map updates.
