/*
Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0

The use of this script is optional, and is primarily meant for use in 
 SAS EG or SAS-Studio.

logfl = Full path name of the onprem_direct.log to process
workdir = Location where you copied the dirlogparse executable.  CSV files
             are generated here. Please end the location with a path separator 
			(forward slash or back slash as needed)
*/

%if &sysscp. eq %str(WIN) %then %do;
	%let logfl=C:\SAS\Software\DirectAgent\logs\onprem_direct_thread_test.log;
	%let workdir=%str(C:\workspace\360\engage_direct_dm_agent_onpremlog_parser\bin\windows\);
	%let separator=%str(\);
%end;
%else %do;
	%let logfl=/sas/ci360direct/logs/onprem_direct.log;
	%let workdir=/sas/ci360jobs/Jobs/dirlogparse/;
	%let separator=%str(/);
%end;


%sysexec(&workdir.dirlogparse -f &logfl. -o &workdir. > &workdir.&separator.dirlogparse.log);
filename in "&workdir.&separator.dirlogparse.log";
data _null_;

infile in;
input;

put _infile_;

run;