/**********************************************************************************************************************
PROGRAM: SAS CUSTOMER INTELLIGENCE 360 Custom On-Prem Scheduler for Direct Marketing Tasks, Bulk Email Tasks and Segment Maps
DESCRIPTION: This application helps the CI 360 User to create deployed jobs from the CI 360 tenant 
             for Direct Marketing Tasks, Bulk Email Tasks and Direct Segment Maps for a SAS Administrator 
             to schedule on-prem in the SAS LSF scheduler.
FILE NAME: parseExecutionStatus.sas
DESCRIPTION: Parses CI 360 execution status. 
VERSION: 2.0
DATE MODIFIED: 25-APRIL-2024
AUTHOR: GLOBAL CUSTOMER INTELLIGENCE ENABLEMENT TEAM

#Copyright � 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0
**********************************************************************************************************************/

%macro parseExecutionStatus(JSONFREF, DATASET=_null_);
  %global OBJECT_ID EXECUTION_STATE COMPLETION_DTTM ERROR_CODE ERROR_MESSAGE;
  %let OBJECT_ID=;
  %let EXECUTION_STATE=;
  %let COMPLETION_DTTM=;
  %let ERROR_CODE=;
  %let ERROR_MESSAGE=;

  data _null_;
    putlog ;
    putlog "NOTE: --- %sysfunc(timestamp()) ---";
    putlog ;
  run;
  data _null_;
    infile %superq(JSONFREF);
    input;
    putlog "json:" _infile_;
  run;

  libname status json fileref=%superq(JSONFREF);

  data %superq(DATASET);
    keep ObjectJobId executionState errorCode;
    format ObjectJobId $100. errorMessage $256. errorCode 8. p2 $200.;
    length completionDttm $26;
    call missing(p2);
    set status.alldata end=done;
    retain taskJobId ObjectJobId executionState errorMessage errorCode completionDttm;
    if(p=1 and p1="taskJobId") then ObjectJobId = value;
    if(p=1 and p1="segmentMapJobId") then ObjectJobId = value;
    call symputx("OBJECTJOBID",ObjectJobId);
    taskJobId = ObjectJobId;
    if(p=1 and p1="executionState") then executionState = value;
    if(p=1 and p1="errorCode") then errorCode = input(value, 8.);
    if(p=1 and p1="message") then errorMessage = value;
    if(p=1 and p1="endTimeStamp") then do;
      completionDttm = "";
      if(value ne "" and value ne "null") then localDttm = tzoneu2s(input(scan(value,1,'+'), anydtdtm26.));
      if(localDttm) then completionDttm = strip(translate(put(localDttm, e8601dt23.3), " ", "T"));
    end;
    if done then do;
      putlog ;
      putlog "NOTE: --- %sysfunc(timestamp()) ---";
      putlog "  " executionState=;
      putlog "  " completionDttm=;
      putlog "  " errorCode=;
      putlog "  " errorMessage=;
      putlog ;
      call symputx("OBJECT_ID",       taskJobId);
      call symputx("OBJECTJOBID",	  ObjectJobId);
      call symputx("EXECUTION_STATE", executionState);
      call symputx("COMPLETION_DTTM", completionDttm);
      call symputx("ERROR_CODE",      errorCode);
      call symputx("ERROR_MESSAGE",   errorMessage);
      output;
    end;
  run;

  libname status clear;
%mend parseExecutionStatus;
