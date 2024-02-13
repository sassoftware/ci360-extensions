/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/

/* ***************************************/
/*    ALWAYS CUSTOMIZE THESE VARIABLES   */
/* ***************************************/

/* CI360-specific variables */
%let api_agent =  '';		/*'ID Mapping Client';*/								/* Name of Access Point from 360 UI */
%let api_tenant = '';		/* Tenant ID from 360 UI*/
%let api_secret = '';		/* Tenant secret from 360 UI */

%let ci360_env = /*extapigwservice-*/training.ci360.sas.com;		/* SAS AWS hostname suffix */
%let descriptor_name = IdentityMap;									/* the name of the descriptor used for upload ID mapping data */
%let import_wait_mins = 120;										/* How long to wait for the import to be ready in minutes */
%let db_upload_list = subject_id,customer_id;	

/* ***************************************/
/* CONSTANTS - SHOULD NOT NEED TO CHANGE */
/* ***************************************/
%let sas_include_path = &IB_idmapping_path.;
%let sas_utility_library = CI360UTL;								/* the location of the CI360Utilities DS2 package or where it will be created */
%let sas_utility_path = &sas_include_path/util;						/* where to create the CI360Utilities package - normally same as include path */
%let sas_utility_version = 4;										/* The minimum version of the CI360Utilities DS2 package required */

%let sas_process_idmap_code = idmappingprocess.sas;					/* Main orchestration process file */
%let sas_generate_idmap_code = generateidmap.sas;					/* needs to contain the GenerateIDMap() macro and set rc (1 = success) */
%let sas_send_idmap_code = sendidmap.sas;							/* needs to contain the SendIDMap() macro and set rc (1 = success) */

%let sas_upload_file = &sas_include_path/output_file/idmap_upload.csv; 			/* full path to the file to use for uploads */
%let sas_upload_code = &sas_include_path/upload_idmap.sas;			/* full path of the dynamic code file to write to execute the upload */

%let sas_db_libref_cust = CMDM /*idmapcst*/;									/* libref to use throughout when referencing ID Map source */
%let sas_db_libref_idmap = CMDM /*idmapdb*/;									/* libref to use throughout when referencing ID Map tables */

%let sas_log_level = 4;												/* Possible log levels:
																	    0 = Errors Only
																		1 = Info
																	    2 = Debug 
																	    3 = Trace (sensitive info obfuscated)
																	    4 = Trace (sensitive info plain text) */


OPTIONS DBIDIRECTEXEC SASTRACE=',,,d' SASTRACELOC=saslog NOSYMBOLGEN NOMLOGIC SPOOL MPRINT;
options 
    append      =  (sasautos="&sas_include_path./macro")
    append      =  (sasautos="&sas_include_path./util")
;
options mprint mlogic symbolgen;

%check_tables_ddl;


