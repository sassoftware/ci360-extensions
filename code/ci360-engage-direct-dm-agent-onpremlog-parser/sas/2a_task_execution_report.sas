/*
Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0

Assign a libname and use, or specify a permanent libname that is 
 pre-assigned in SMC
*/

%let workdir=/sas/ci360jobs/Jobs/dirlogparse/;
libname worklib "&workdir." compress=yes;

Title Task Execution report;
proc sort data=&worklib..tasks out=tasks_list;
	by start_time;
run;
proc report data=tasks_list ;
	columns rpt_line_number task_thread task_name external_code started_by start_time task_duration end_time error_text;
	define rpt_line_number / computed '#';
	define task_thread / display 'Task thread';
	define task_name / display 'Task name';
	define external_code / display 'Task/Code';
	define started_by / display 'User';
	define task_duration / display 'Task/Duration/(seconds)';
	define start_time / display 'Start time (UTC)';
	define end_time / display 'End time (UTC)';
	define error_text / display 'Error text';
	compute rpt_line_number;
		line_number+1;
		rpt_line_number = line_number;
	endcomp;
run;
Title;
libname worklib clear;