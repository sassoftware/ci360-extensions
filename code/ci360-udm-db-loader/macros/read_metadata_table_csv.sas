/********************************************************************************/
/* Copyright (c) 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                          */
/* ******************************************************************************/
%Macro read_metadata_table_csv(IMPORTED_METADATA_TABLE=IMPORTED_METADATA_TABLE);
%local IMPORTED_METADATA_TABLE;

	data &IMPORTED_METADATA_TABLE.;
		infile "&UtilityLocation.&slash.config/MISSING_PRIMARY_KEYS.csv"
			delimiter=',' missover firstobs=1 DSD lrecl = 32767;
		format Table_Name Column_Name $100. ;
		input 
			Table_Name $ 
			Column_Name $ 
			;
	run;

	%err_check(Unable to add data to metadata, &SYSMACRONAME.);
	%put %sysfunc(datetime(),E8601DT25.) --- &UDM_ErrMsg;

%mend read_metadata_table_csv;
