/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
/* ******************************************************************/
/* Description: Manages the process of keeping SUBJECT_IDs synched 	*/
/*				with SAS CI 360 by looking in the EMPLOYEES table 	*/
/*				for any IDs not already in "pending" or "processed" */
/*				tables, then creating a file, uploading it to S3, 	*/
/*				and then asking 360 to import it.					*/
/*																	*/
/*				Also should handle any failure files in case IDs	*/
/*				are not processed.									*/
/*																	*/
/*				NOTE: Update SQL statement in "generate" macro		*/
/*					  to accomodate your specific column(s) for		*/
/*					  all relevant CI 360 identity types.			*/
/*																	*/
/* Required Job Properties											*/
/* =======================											*/
/*  Variable			Purpose										*/
/* ------------------   ------------------------------------------- */
/* sas_include_path	  	Full path to the location of macro files	*/
/* sas_parameters_file 	Sets the environment variables and libname 	*/
/*																	*/
/* Version History													*/
/* ===============													*/
/* Date		Author			Description								*/
/* --------	---------------	--------------------------------------- */
/* 23Oct19	Steve Hill		Initial Release							*/
/* 15Nov20	Steve Hill		Incorporated CI 360 Utilities v4		*/
/* 01Jan21	Mark Nelson		Added LoadIdentities.sas and SQL Server	*/
/* ******************************************************************/

%macro IDMappingProcess();
	options nosource nonotes linesize=MAX;

/* useful for troubleshooting - they control which macros are called and how they behave */
	%let sas_generate_idmap = 1;										/* indicates whether to do this part or not: 0 = false, 1 = true */
	%let sas_send_idmap = 1;											/* indicates whether to do this part or not: 0 = false, 1 = true */
	%let sas_create_file = 1;											/* indicates whether to create a new file or use an existing one: 0 = false, 1 = true */

/* if we want trace, turn up the noise */
	%if %eval(&sas_log_level > 2) %then %do;
		options source notes;
	%end;

/* INTERNAL VARIABLES */
	%global rc;

	libname &sas_utility_library. BASE "&sas_utility_path";

/* Create the package if needed - this process should not require editing */
	%include "&sas_utility_path/CI360Utilities.sas";
	%CreatePackage();
	%if (&rc = 1) %then %do;

		/* check version */
		PROC DS2 NOLIBS CONN="((DRIVER=BASE;CATALOG=&sas_utility_library;SCHEMA=(NAME=&sas_utility_library;PRIMARYPATH={&sas_utility_path/data}));(DRIVER=BASE;CATALOG=WORK;SCHEMA=(NAME=WORK;PRIMARYPATH={%sysfunc(pathname(work))}));)";
			data work.VersionCheck (OVERWRITE=YES);
				declare package &sas_utility_library..CI360Utilities ci360(%tslit(&ci360_env), &api_tenant, &api_agent, &api_secret, &sas_log_level);
				declare int version;

				method run();
					version = ci360.MajorVersion();
				end;
			enddata;
			run;
		QUIT;

		proc sql noprint;
			SELECT 	version
			INTO 	:version separated by ''
			FROM	work.VersionCheck;
		quit;
		%let rc = %eval((&SQLOBS > 0) and (&version >= &sas_utility_version));
		%Log(3, %nrstr(%IDMappingProcess), CI360Utilities Major Version = &version, 0);

	%end;
	%if &rc = 0 %then %do;
		%Log(0, %nrstr(%IDMappingProcess), Requires CI360Utilities version &sas_utility_version but found &version, 0);
	%end;

	/* Request the export & download any updated identity files - this process should not require editing  */
	%if (&rc = 1 and &sas_generate_idmap) %then %do;
		/* %include "&sas_include_path/&sas_generate_idmap_code";*/
		%PUT NOTE: GENERATE!;
		%GenerateIDMap; 
	%end;

	/* Request the export & download the all files from the last time to the latest by hour - this process should not require editing  */
	%if (&rc = 1 and &sas_send_idmap) %then %do;
		/* %include "&sas_include_path/&sas_send_idmap_code";*/
		%PUT NOTE: SEND!;
		%SendIDMap;
	%end;

	%if (&rc = 1) %then %do;
		%Log(1, IDMappingProcess.sas, **********************************************, 0);
		%Log(1, IDMappingProcess.sas, Process completed successfully., 0);
		%Log(1, IDMappingProcess.sas, **********************************************, 0);
	%end;
	%else %do;
		%Log(1, IDMappingProcess.sas, **********************************************, 0);
		%Log(1, IDMappingProcess.sas, Process failed. See the log for details., 0);
		%Log(1, IDMappingProcess.sas, **********************************************, 0);

		ABORT;
	%end;

	/* put the "normal" settings back */
	options source notes linesize=78;
%mend IDMappingProcess;
