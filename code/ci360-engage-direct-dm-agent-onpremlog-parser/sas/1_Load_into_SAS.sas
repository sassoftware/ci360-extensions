/**********************************************************************
Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0

This script loads the tasks.csv and subtasks.csv data to SAS datasets
 for reporting purposes
***********************************************************************/
/*
Update the following line to tell the program the location
 of tasks.csv and subtasks.csv are located. Please include
 a trailing path separator
Assign a libname and use, or specify a permanent libname that is 
 pre-assigned in SMC
03/02/2022:  Due to log file updates in the most recent maintenance,
	the TASK_SEQ variable is now 100 characters wide.
*/
%let workdir=/sas/ci360jobs/Jobs/dirlogparse/;
libname worklib "&workdir." compress=yes;
%let worklib=worklib;

data &worklib..tasks(drop=start_time_char end_time_char);
	attrib task_name length=$50;
	attrib task_type length=$5;
	attrib started_by length=$50;
	attrib task_id	length=$50;
	attrib segment_map_id length=$50;
	attrib Occurrence_uid	length=$50;
	attrib external_code length=$15;
	attrib start_time length=8 format=datetime23.3;
	attrib task_thread length=$20;
	attrib log_record_count length=4;
	attrib end_time length=8 format=datetime23.3;
	attrib task_duration length=4; 
	attrib task_seq length=$100;
	attrib stp_log length=$200;
	attrib error_text length=$300;
	attrib start_time_char length=$23;
	attrib end_time_char length=$23;
	filename tsk "&workdir.tasks.csv";
	infile tsk dsd truncover delimiter=',' firstobs=2;
	input task_name $ task_type $
		started_by $ task_id $ segment_map_id $ occurrence_uid $ external_code $
		start_time_char $ task_thread $ log_record_count end_time_char $ task_duration 
	    task_seq $ stp_log $ error_text $;

	start_time = input(tranwrd(start_time_char, ',', '.'), ?ymddttm23.3);
	end_time = input(tranwrd(end_time_char, ',', '.'), ?ymddttm23.3);
run;

data &worklib..subtasks(drop=start_time_char end_time_char);
	attrib sub_task_id length=$20;
	attrib task_seq length=$100;
	attrib sub_task_start_time length=8 format=datetime23.3;
	attrib sub_task_end_time length=8 format=datetime23.3;
	attrib start_time_char length=$23;
	attrib end_time_char length=$23;
	attrib sub_task_name length=$100;
	attrib sub_task_stp_name length=$50;
	attrib sub_task_duration length=4;
	attrib sub_task_stp_duration length=4;
	attrib sub_task_qry_duration length=4;
	attrib sub_task_thread_cnt length=4;
	attrib sub_task_thread_max length=4;
	attrib sys_thread_cnt length=4;
	attrib sys_thread_max length=4;
	attrib log_record_number length=4;
	attrib errorText length=$200;

	filename stsk "&workdir.subtasks.csv";
	infile stsk dsd truncover delimiter=',' firstobs=2;
	input sub_task_id $ task_seq $ start_time_char $
		sub_task_name $ sub_task_stp_name $ sub_task_duration
		sub_task_stp_duration sub_task_qry_duration
		sub_task_thread_cnt sub_task_thread_max
		sys_thread_cnt sys_thread_max end_time_char $
		log_record_number errorText $;
	sub_task_start_time = input(tranwrd(start_time_char, ',', '.'), ?ymddttm23.3);
	sub_task_end_time = input(tranwrd(end_time_char, ',', '.'), ?ymddttm23.3);
run;
libname worklib clear;