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
This report does not need any customization unless you want to improve 
 the report
*/
proc sql noprint;
 
	create view v_detail_rpt as
		select t.task_thread
			,t.start_time
			,t.end_time
		    ,t.started_by
			,t.external_code
			,t.task_name
			,t.task_type
			,coalesce(t.task_duration, -1) as task_duration
			,s.sub_task_name
			,s.sub_task_duration
			,s.sub_task_stp_name
			,s.sub_task_id
			,s.sub_task_start_time
			,s.sub_task_end_time
            ,s.errorText
            ,s.sub_task_id
            ,s.log_record_number
            ,t.task_seq	
		  from &worklib..tasks t
		  left outer join &worklib..subtasks s
		  on t.task_seq = s.task_seq
		  and s.sub_task_start_time between t.start_time and coalesce(t.end_time, datetime())
	where (1=1)
    order by t.start_time, s.sub_task_start_time
		  ;
quit;

proc format;
  value exectime
     0-high = [comma6.0]
	 other = 'Incomplete';
run;

Title Task detail report;
options missing=" ";
proc report data=v_detail_rpt  nowd split='~';
	columns rpt_line_number start_time started_by external_code task_name task_duration
		sub_task_start_time sub_task_name  sub_task_duration errorText sub_task_id;
	define rpt_line_number /  computed '#';
	define task_name / order 'Task name';
	define external_code / order 'Task code';
	define start_time / order 'Start time';
	define started_by / order 'Task owner';
	define task_duration / order 'Execution~time~(seconds)' format=exectime.;
	define sub_task_name / display 'Sub task name';
	define sub_task_start_time / display 'Sub task~start time';
	define sub_task_duration / display 'Sub~task~duration~(seconds)' format=exectime.;
	define errorText / display 'Error text' flow width=50;
	define sub_task_id / display 'Sub~task~id' width=15;
	compute before task_name;
		line_number+1;
		line_number_txt=put(line_number,z5.);
	endcomp;
	compute rpt_line_number;
		rpt_line_number = line_number_txt;
		line_number_txt = " ";
	endcomp;

run;
Title ;
libname worklib clear;

