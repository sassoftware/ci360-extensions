/*
Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0

Assign a libname and use, or specify a permanent libname that is 
 pre-assigned in SMC

02/04/2022
	Changed the tasks/subtasks join to conform to latest update in Engage:Direct

*/

%let workdir=/sas/ci360jobs/Jobs/dirlogparse/;
libname worklib "&workdir." compress=yes;
%let worklib=worklib;
/*  
proc sql number;
	select *
      from &worklib..subtasks
;
quit;
*/
/*
A report on STPs that were run and their durations
*/

proc sql noerrorstop;

create view sub_tasks_list as
	select s.sub_task_stp_name
		,t.task_name
		,s.sub_task_name
	    ,s.sub_task_start_time
		,s.sub_task_stp_duration
		,s.errorText
	  from &worklib..tasks t
	  inner join &worklib..subtasks s
	  	on t.task_seq = s.task_seq
	  and s.sub_task_start_time between t.start_time and t.end_time
   where s.sub_task_stp_name not like 'OnPrem%'
	and s.sub_task_stp_name not like ''
	order by s.sub_task_start_time;
quit;
Title Stored Process Execution report;
proc report data=sub_tasks_list nowd split='|';
	columns rpt_line_number sub_task_stp_name task_name sub_task_name sub_task_start_time sub_task_stp_duration errorText;
	define rpt_line_number / computed '#';
	define task_name / display 'Task name';
	define sub_task_stp_name / display 'STP name';
	define sub_task_name / display 'Node/sub-task';
	define sub_task_start_time / display 'Start time';
	define sub_task_stp_duration / display 'Duration|(seconds)';
	define errorText / display 'Error message';
	compute rpt_line_number;
		line_number+1;
		rpt_line_number = line_number;
	endcomp;
run;
Title;
libname worklib clear;
