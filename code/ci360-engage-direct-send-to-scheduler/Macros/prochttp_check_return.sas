/**********************************************************************************************************************
PROGRAM: SAS CUSTOMER INTELLIGENCE 360 Custom On-Prem Scheduler for Direct Marketing Tasks, Bulk Email Tasks and Segment Maps
DESCRIPTION: This application helps the CI 360 User to create deployed jobs from the CI 360 tenant 
             for Direct Marketing Tasks, Bulk Email Tasks and Direct Segment Maps for a SAS Administrator 
             to schedule on-prem in the SAS LSF scheduler.
FILE NAME: prochttp_check_return.sas
DESCRIPTION: This macro helps check status codes on the http requests
VERSION: 2.0
DATE MODIFIED: 25-APRIL-2024
AUTHOR: GLOBAL CUSTOMER INTELLIGENCE ENABLEMENT TEAM

#Copyright � 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0
**********************************************************************************************************************/


%macro prochttp_check_return(code);
  %if %symexist(SYS_PROCHTTP_STATUS_CODE) ne 1 %then %do;
    %put ERROR: Expected &code., but a response was not received from the HTTP Procedure;
    %abort;
  %end;
  %else %do;
    %if &SYS_PROCHTTP_STATUS_CODE. ne &code. %then %do;
      %put ERROR: Expected &code., but received &SYS_PROCHTTP_STATUS_CODE. &SYS_PROCHTTP_STATUS_PHRASE.;
      %abort abend 2;
    %end;
  %end;
%mend;
