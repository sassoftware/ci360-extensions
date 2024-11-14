# CI360 Engage Direct Log Data Item Extraction

## Overview

This utility program parses the onprem logs, and for every executed Direct Marketing task and segment map extracts all the data items used in those tasks/segment maps.  Executing this program on a nightly/weekly basis allows you to build a cross-reference of every task/segment-map executed in CI360 and the data items used in those tasks/segment maps.

## Assumptions

1.	The onprem logs are named onprem_direct.log.yyyy-mm-dd.
2.	All the onprem logs to be read are from the same directory/folder

## Configuration

Refer to utl_onpremdiext.sas and update the following variables.

**c_lastUpdateFile:**
- This file stores the last "date" processed
- It should contain 1 record with date formatted as yyyy-mm-dd
- If the file does not exist - one will be created for you. In this case,(only) yesterdays' log file will be processed

**c_outputDir:**	
- Specify the location to generate output SAS datasets.

**c_infile_dir:**

- This is the directory containing the onprem_direct.log files
- It is assumed that the files are suffixed by a date: .yyyy-mm-dd

**c_extract_dataitem_values:**

- Controls whether to retrieve data items values from criteria nodes
- If you are only interested in data item names rather than data item values, you may choose to set c_extract_dataitem_values=N

## Running

Run the utl_onpremdiext.sas program and output files will be generated in the SAS datasets format.For detailed program outputs, please refer the PDF document.

## Upgrade Considerations

The table structure may change with each new version of this utility.  One of the following approaches is recommended:
-	Rename the current SAS datasets with the _old suffix, and let the first execution of this process recreate the tables.  Afterwards, copy the data from _old tables to the new tables
-	Review the new table structure from the above document, and update your old SAS datasets to add the new columns to match the new structure.  

