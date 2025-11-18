/*
Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0

Assign a libname and use, or specify a permanent libname that is 
 pre-assigned in SMC
*/

%let workdir=/sas/ci360jobs/Jobs/dirlogparse/;
libname worklib "&workdir." compress=yes;
%let worklib=worklib;
/*
If you wish to zoom in, on a particular time period, uncomment the lines
 in sgplot and specify the appropriate time period of interest
*/
data threads (keep=sub_task_end_time sub_task_thread_cnt sub_task_thread_max
			sys_thread_cnt sys_thread_max);
 set &worklib..subtasks;
 /*
 where sub_task_end_time gt '10Feb22:00:30:00.000'dt
   and sub_task_end_time lt '10Feb22:04:00:00.000'dt;
 */
run; 
proc sgplot data=threads; 
   series x=sub_task_end_time y=sub_task_thread_cnt;
   series x=sub_task_end_time y=sys_thread_cnt;
   xaxis interval=hour
/*		values=('10Feb2022:00:15:00'dt to '10Feb2022:00:17:00'dt by 1)   */
	;

run;
libname worklib clear;