/****************************************************************************************************************
PROGRAM:        SAS Customer Intelligence 360 Direct Agent log's SQL(s) extraction utility
DESCRIPTION:    This utility is intended to extract sql statements from the
                onprem_direct.log.<date>. Extracts SQL for a single task or segment-map for a given time range
                If multiple executions are found in the time range, then all the SQLs for those executions are
                retrieved.
VERSION: 		0.0
DATE MODIFIED:  03-SEPTEMBER-2024
AUTHOR:         GLOBAL CUSTOMER INTELLIGENCE ENABLEMENT TEAM

#Copyright  2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0
******************************************************************************************************************/

/* Global Variables */
%global _CLIENTAPP;
%global returnKode;
%global p_infileDir p_objName p_timestamp_min p_timestamp_max;
%global g_logfilename g_max_ts g_min_ts;
options errabend fullstimer;
options nosource nosource2;
options nomlogic nomprint nosymbolgen;
options noxwait;

/* Custom Initialization */
%let returnKode=0;
/*******Customize -- begin***********************/
%let p_infileDir=D:\install\CI360Direct\logs;
/*******Customize -- end*************************/
%let g_logfilename=%str(onprem_direct.log);
%let MAXSQLRECLEN=32767;
%let MAXRECLEN=500;
%let MAXSQLLINES=1500;
%let debug=N;
%let syscc=0;

/*
Function Name:	init
Description:	Initialize variables used further in program. 
Parameters:		None
*/
%macro init;

%if %index(%bquote(&_CLIENTAPP), %str(Enterprise Guide))=0 and %index(%bquote(&_CLIENTAPP), %str(Studio))=0 %then %do;
	%stpbegin;
%end;
%else %do;
	%let p_objname=Test Munvo Segment;
	%let p_objname=Optimize Segments New;
	%let p_timestamp_min=23Oct2024 00:00:00;
	%let p_timestamp_max=23Oct2024 9:00:00;
%end;
%let datetimenow=%sysfunc(datetime(), 15.);
%let datenow=%sysfunc(datepart(&datetimenow.));

%let report_date=%sysfunc(putn(&datenow.,weekdate17.)) %sysfunc(time(),time8.) %sysfunc(tzonename());
%let currdate = %sysfunc(putn(&datenow., yymmddn8.),8.);
%let currtime = %sysfunc(compress(%sysfunc(putn(&datetimenow.,tod8.)),":"));
%let currdttm = &currdate.&currtime.;


%mend init;

/*
Function Name:	validate_input
Description:	Verify the input parameters:
				-	Only one log file can be processed, and since each log file contains data for a particular
				     day, both the min/max timestamps must be for the same day
				-	The log file named is derived based on the time range to be searched
				-	If the log file is not found the process terminates with error: 8
Parameters:		None
*/
%macro validate_input;


%let l_min_date=%substr(&p_timestamp_min, 1, 9);
%let l_max_date=%substr(&p_timestamp_max, 1, 9);
%if "&l_min_date." ne "&l_max_date." %then %do;
	%put &=l_min_date. &=l_max_date.;
	%put %str(Both first and last timestamps must be on the same day. Processing aborted);
	%let returnKode=16;
	%goto exit_validate_input;
%end;

%let l_date = %sysfunc(putn("&l_min_date."d, yymmdd10.));
%let l_time = %substr(&p_timestamp_min, 11);
%if %length(&l_time) < 8 %then %let l_time=0&l_time.;
%let g_min_ts = &l_date. &l_time.;
%let l_date = %sysfunc(putn("&l_max_date."d, yymmdd10.));
%let l_time = %substr(&p_timestamp_max, 11);
%if %length(&l_time) < 8 %then %let l_time=0&l_time.;
%let g_max_ts = &l_date. &l_time.;
%let l_date_today = %sysfunc(putn(%sysfunc(today(), 15.), yymmdd10.));
 
%if "&l_date." eq "&l_date_today." %then %do;
	%let g_logfilename=&g_logfilename.;
%end;
%else %do;
	%let g_logfilename=&g_logfilename..&l_date.;
%end;
 
%put NOTE: Input log file name is &p_infileDir.\&g_logfilename.;
%if %sysfunc(fileexist(&p_infileDir.\&g_logfilename.)) ne 1 %then %do;
	%put Input file does not exist. Processing aborted.;
	%let returnKode=8;
	%goto exit_validate_input;
%end;

%exit_validate_input:
%mend validate_input;


/*
Function Name:	extract_sql
Description: 	We process the onprem log file here, after excluding skipping any log records prior to the
				lower bound or after the higher bound on timestamp

				1. Log an error and ignore record if the input record length exceeds MAXSQLRECLEN
				2. If a timestamp is found at the beginning of the record, ensure that the timestamp is within range
				3. Ignore one line SQLs that are often internal to DM Agent function rather than user queries. Note,
				    that one line SQLs still have at least 3 lines.  First record, SQL, Terminator record
				4. Find the end of SQL text is sometimes tricky, as the line with hyphens is missing. So we are
				    checking for either "quit;" or hyphen line for termination

				In order to improve performance in reading the large log file, we avoid reading all the
				 record in one go. Multiple input statements are used to read the full record only when needed.
Parameters:		None
*/
%macro extract_sql;

	filename inlog "&p_infileDir.\&g_logfilename";
	data sqlext(keep=logTimestamp logRecordnumber sqlText nodeType nodeName sessionId threadId execId nodeId objName);

		attrib logTimestamp length=8 format=datetime21.;
		attrib sqlText length=$32767;
		attrib logRecordnumber length=8;
		attrib nodeName length=$50;
		attrib nodeType length=$20;
		attrib objName length=$100;
		attrib threadId length=$25;
		attrib sessionId length=$50;
		attrib execId length=$50;
		attrib nodeId length=$50;

		retain logTimestamp sqlText nodeType nodeName sessionId threadId execId nodeId objName;
		retain capture_sql_flag error_count lineStart_expr 
				logRecordnumber skip_min_ts_flag skip_max_ts_flag 
				sqlRecordNumber sqlStart_expr ;

		infile inlog truncover obs=max end=eof length=input_line_length;
		input @1 logts $25. @;
		logRecordnumber = _N_;

		if _N_=1 then do;
			sqlStart_pat="/^([\d\-: ]+),[\d]+ INFO  \[([\S]+)\] SID\[([\S]+)\] .* CC\[([^:]+):([\S)]+)\] .{1,50} com.sas.analytics.crm.custdata.sql[\s]+- TID\[([^\/]+)\/([^\/]+)\/([^\]]+)\]/";
			sqlStart_expr=prxparse(sqlStart_pat);
			lineStart_pat="/^%substr(%superq(g_max_ts),1,10) [\d\:]{8},[\d]{3} /";
			lineStart_expr=prxparse(lineStart_pat);
			skip_min_ts_flag = 'Y';
			skip_max_ts_flag = 'N';
		end;

		if input_line_length gt &MAXSQLRECLEN. then do;
			putlog 'ERROR: Maximum line length exceeded.  Record skipped. ' _N_= input_line_length=;
			error_count+1;
			delete;
		end;

		if capture_sql_flag='Y' then do;
			input @1 logtxt $varying&MAXSQLRECLEN.. input_line_length;
			sqlRecordNumber + 1;
			if sqlRecordNumber eq 1 then do;
				sqlText='';
				delete;
			end;
			if substr(logtxt, 1, 20) eq repeat('-', 19) then do;
				capture_sql_flag='N';
			end;
			if substr(logtxt,1,5) eq 'quit;' then do;
				sqlText = catx('^n', sqlText, logtxt);
				capture_sql_flag='N';
			end;
			if sqlRecordNumber gt &MAXSQLLINES. and capture_sql_flag = 'Y' then do;
				putlog "WARNING: SQL spans too many log record.  Captured &MAXSQLLINES. records. Rest of sql is skipped";
				capture_sql_flag='N';	
			end;
			if capture_sql_flag='N' then do;
				if sqlRecordNumber gt 3 then
					output;
			end;
			else do;
				sqlText = catx('^n', sqlText, logtxt);
			end;
			delete;
		end;

		if input_line_length lt 25 then delete;

		if substr(logts, 1, 3) ne "202" then delete;
  
		if prxmatch(lineStart_expr, logts) then do;
			if skip_min_ts_flag = 'Y' then do;
				if "&g_min_ts." le substr(logts, 1, 19) then do;
					skip_min_ts_flag = 'N';
					putlog "NOTE: Processing started at log record number: " _N_ " since timestamp range begins at &g_min_ts.";
				end;
			end;
			if skip_max_ts_flag = 'N' then do;
				if "&g_max_ts." lt substr(logts, 1, 19) then do;
					skip_max_ts_flag = 'Y';
					putlog "NOTE: Processing stopped at log record number: " _N_ " since timestamp range ends at &g_max_ts." ;
					stop;
				end;
			end;
		end;
  
		if skip_min_ts_flag = 'Y' then delete;
		input_line_length = &MAXRECLEN.;
		input @1 logtxt $varying&MAXSQLRECLEN.. input_line_length;
		if prxmatch(sqlStart_expr, trim(logtxt)) then do;
			sqlRecordNumber = 0;
			do i=1 to prxparen(sqlStart_expr);
				call prxposn(sqlStart_expr, i, start, length);
				if start ne 0 then do;
					select (i);
						when (1) logTimestamp = input(substr(logtxt, start, length), ymddttm19.);
						when (2) threadId = substr(logtxt, start, length);
						when (3) sessionId = substr(logtxt, start, length);
						when (4) objName = substr(logtxt, start, length);
						when (5) execId = substr(logtxt, start, length);
						when (6) nodeType = substr(logtxt, start, length);
						when (7) nodeName = substr(logtxt, start, length);
						when (8) nodeId = substr(logtxt, start, length);
						otherwise do;
							putlog "WARNING: Additional unhandled match in the regex pattern ignored";
							error_count+1;
							if error_count le 25 then
								putlog 'NOTE: ' logtxt=;
						end;        /* end of select-otherwise */ 
					end;            /* end of select statement */
				end;                /* end of matches found    */
			end;                    /* do loop for each match  */
			if upcase(objName) eq "%upcase(&p_objname.)" then
				capture_sql_flag = 'Y';
			else capture_sql_flag = 'N';
		end;                        /* sql line found in log   */
	run;

	filename inlog clear;
%mend extract_sql;

/*
Function Name:	display_report
Description: 	In order to ensure that the resultant report properly renders the SQL, the ods option
				is required. Otherwise, all the SQL will be shown on one line.
				Further, "node name" is set to the object name for some nodes.
Parameters:		None
*/
%macro display_report;

ods escapechar='^';
Title1 "SQL Extracted between &g_min_ts. and &g_max_ts.";
Title2 "Object name is &p_objname.";
Title3 "Input file is &p_infileDir.\&g_logfilename on &systcpiphostname.";

%let nobs=0;
data _null_;
	set sqlext nobs=nobs;
	if _N_ eq 1 then do;
		call symputx('nobs', nobs);
		stop;
	end;
run;
%if &nobs. eq 0 %then %do;
	data no_results;
	  msg = 'There was no SQL extracted with this name and timestamp range';
	run;
	proc report data=no_results;
	run;
%end;
%else %do; 
	proc report data=sqlext(keep=logTimestamp nodeName nodeType sqlText) nowindows;
		columns logTimestamp nodeName nodeType sqlText;
		define logTimestamp / display 'Log timestamp' width=20;
		define sqlText / display 'SQL text' width=80;
		define nodeName / display 'Node name' width=25;
		define nodeType / display 'Node type' width=12; 
	run;
%end;
 
%mend display_report;

/*
Function Name:	finish
Description: 	Processing completed with a return code.
Parameters:		None
*/
%macro finish;

%put NOTE: Processing completed with return code &returnKode.;
%let syscc=&returnKode.;
%if %index(%bquote(&_CLIENTAPP), %str(Enterprise Guide))=0 and %index(%bquote(&_CLIENTAPP), %str(Studio))=0 %then %do;
	%stpend;
%end;


%mend finish;

/*
Function Name:	copycurrentlog
Description: 	This macro creates a copy of same day log file and if the copied file already exists then it delete and create
				a new copy of same day log file.
Parameters:		None
*/
%macro copycurrentlog;

	%let l_min_date=%substr(&p_timestamp_min, 1, 9);
	%let l_date = %sysfunc(putn("&l_min_date."d, yymmdd10.));
	%let l_date_today = %sysfunc(putn(%sysfunc(today(), 15.), yymmdd10.));
	 
	%if "&l_date." eq "&l_date_today." %then %do;
		%let currentlog = Current_log.log;
		%let oldname = &p_infileDir.\&g_logfilename;
		%let newname = &p_infileDir.\&currentlog;
		%if %sysfunc(fileexist(&newname)) eq 1 %then %do;
				%let rc=%sysfunc(filename(temp,&newname));
				%let rc=%sysfunc(fdelete(&temp));
				%put NOTE: Existing file &currentlog is deleted;
		%end;
		
		data _null_;	
			rc= system("copy &oldname &newname");
		  /*rc= system("copy D:\install\CI360Direct\logs\onprem_direct.log D:\install\CI360Direct\logs\Current_log.log");*/
			put rc=;
		run;
		%let g_logfilename=&currentlog;
	%end;
	
%mend copycurrentlog;

options nomprint nomlogic nosymbolgen;
%init;
%validate_input;
%copycurrentlog;
%extract_sql;
%display_report;
%finish;

