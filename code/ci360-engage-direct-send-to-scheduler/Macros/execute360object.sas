/**********************************************************************************************************************
PROGRAM: SAS CUSTOMER INTELLIGENCE 360 Custom On-Prem Scheduler for Direct Marketing Tasks, Bulk Email Tasks and Segment Maps
DESCRIPTION: This application helps the CI 360 User to create deployed jobs from the CI 360 tenant 
             for Direct Marketing Tasks, Bulk Email Tasks and Direct Segment Maps for a SAS Administrator 
             to schedule on-prem in the SAS LSF scheduler.
FILE NAME: execute360object.sas
DESCRIPTION: Calls CI 360 external API gateway to excute Segment Map or Task. 
VERSION: 2.0
DATE MODIFIED: 25-APRIL-2024
AUTHOR: GLOBAL CUSTOMER INTELLIGENCE ENABLEMENT TEAM

#Copyright � 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0
**********************************************************************************************************************/

%macro execute360object(OBJECT_TYPE, OBJECT_NAME);
  %if %superq(OBJECT_TYPE) ne SegmentMap and %superq(OBJECT_TYPE) ne DMTask and %superq(OBJECT_TYPE) ne EmailTask %then %do;
    %let ERROR = OBJECT_TYPE(%superq(OBJECT_TYPE)) must be either SegmentMap or DMTask or EmailTask;
    %put ERROR: %superq(ERROR);
    %abort abend 2;
  %end;

  %put NOTE: --- %sysfunc(timestamp()) ---;
  %put Executes %superq(OBJECT_TYPE) "%superq(OBJECT_NAME)";
  
  %global EXECUTION_STATE COMPLETION_DTTM ERROR_CODE ERROR_MESSAGE OBJECTJOBID;
  %let EXECUTE_URL=;
  %let STATUS_URL=;

  /* Generate urls */
  data _null_;
    length statusUrl executeUrl objectName $1024;
    objectType = strip(symget("OBJECT_TYPE"));
    objectName = strip(symget("OBJECT_NAME"));
    endpoint   = strip(symget("ENDPOINT"));
    if objectType="SegmentMap" then do;
     executeUrl = cats(endpoint, '/marketingExecution/segmentMapJobs');  
     statusUrl  = cats(endpoint, '/marketingExecution/segmentMapJobs/'); 
    end;
    if objectType="DMTask" then do;
      executeUrl = cats(endpoint, '/marketingExecution/taskJobs'); 
      statusUrl  = cats(endpoint, '/marketingExecution/taskJobs/');
    end;
    if objectType="EmailTask" then do;
      executeUrl = cats(endpoint, '/marketingExecution/taskJobs'); 
      statusUrl  = cats(endpoint, '/marketingExecution/taskJobs/');
    end;
    call symputx("EXECUTE_URL", executeUrl);
    call symputx("STATUS_URL", statusUrl);
  run;

  /* call API to execute 360 object */
  %put NOTE: --- %sysfunc(timestamp()) ---;
  %put POST %superq(EXECUTE_URL);
  filename out temp;

  %if "%superq(OBJECT_TYPE)" = "SegmentMap" %then %do;
    %let strBody = %str({"segmentMapName":"&OBJECT_NAME.", "overrideSchedule":true}); /*RM Feb 2022*/
    %put &strBody.;
  %end;
  %if ("%superq(OBJECT_TYPE)" = "DMTask" or "%superq(OBJECT_TYPE)" = "EmailTask" ) %then %do;
    %let strBody = %str({"taskName":"&OBJECT_NAME.", "overrideSchedule":true}); /*RM Feb 2022*/
    %put &strBody.;
  %end;
  
  proc http clear_cache 
    url="%superq(EXECUTE_URL)" 
    method="POST" 
    OAUTH_BEARER="%superq(TOKEN)" 
    in="%superq(strBody)" 
    out=out;
    headers "Content-Type"="application/json"; 
  run;

  %prochttp_check_return(201);
  %parseExecutionStatus(out);
  filename out clear;
  
  /* todo: manage http errors gracefully */ 
  %let STATUS_URL = &STATUS_URL.&OBJECTJOBID.; 
  %put &STATUS_URL.;

  %let EXECUTION_STATE=%str(In progress);
  %let failed=0;
  
  /* Wait for completion */
  %do %while(%superq(EXECUTION_STATE)=%str(In progress));
    data _null_;
      rc = sleep(10,1);
    run;

    %put NOTE: --- %sysfunc(timestamp()) ---;
    %put GET %superq(STATUS_URL);

    filename out temp;
    proc http clear_cache url="%superq(STATUS_URL)" method="GET" OAUTH_BEARER="%superq(TOKEN)" out=out;
    run;
    /*prochttp_check_return(200);*/
  %if %superq(SYS_PROCHTTP_STATUS_CODE) eq 200 %then %do;
    %let failed=0;
    %parseExecutionStatus(out);
  %end;
  %else %do;
    %let failed=%eval(&failed.+1);
    %put Warning: HTTP %superq(SYS_PROCHTTP_STATUS_CODE) Received (&failed.);
    %if &failed gt 10 %then %do;
      %put Error: &failed. Consecutive HTTP failures - aborts;
      %abort abend 2;
    %end;
  %end;
    filename out clear;
  %end;
  
  %put NOTE: --- %sysfunc(timestamp()) ---;
  %put %superq(OBJECT_TYPE) ID: %superq(OBJECT_ID);

  %if ("%superq(EXECUTION_STATE)"="Success" and "%superq(ERROR_MESSAGE)"="" and "%superq(COMPLETION_DTTM)" ne "") %then %do;
    %put %superq(OBJECT_TYPE) "%superq(OBJECT_NAME)"finished successfully;
    %put NOTE: Execution completed %superq(COMPLETION_DTTM);
    %put NOTE: Execution state: %superq(EXECUTION_STATE);
  %end;
  %else %do;
    %put ERROR: %superq(OBJECT_TYPE) "%superq(OBJECT_NAME)" failed;
    %put ERROR: Execution state: %superq(EXECUTION_STATE);
    %put ERROR: Execution error code: %superq(ERROR_CODE);
    %put ERROR: Execution error message: %superq(ERROR_MESSAGE);
    %put ERROR: Execution completed: %superq(COMPLETION_DTTM);
    %abort abend 2;
  %end;
%mend execute360object;
