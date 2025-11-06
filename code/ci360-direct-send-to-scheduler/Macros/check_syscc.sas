/**********************************************************************************************************************
PROGRAM: SAS CUSTOMER INTELLIGENCE 360 Custom On-Prem Scheduler for Direct Marketing Tasks, Bulk Email Tasks and Segment Maps
DESCRIPTION: This application helps the CI 360 User to create deployed jobs from the CI 360 tenant 
             for Direct Marketing Tasks, Bulk Email Tasks and Direct Segment Maps for a SAS Administrator 
             to schedule on-prem in the SAS LSF scheduler.
FILE NAME: check_syscc.sas
DESCRIPTION: This macro helps check system errors and warnings in code. 
VERSION: 2.0
DATE MODIFIED: 25-APRIL-2024
AUTHOR: GLOBAL CUSTOMER INTELLIGENCE ENABLEMENT TEAM

#Copyright � 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0
**********************************************************************************************************************/

%macro check_syscc;
  %global STATUS ERROR;
  %if %superq(SYSCC) = 4 %then %do;
    %let ERROR = WARNING in SAS Code: %superq(SYSWARNINGTEXT);
    %let STATUS = error;
    %return;
  %end;
  %if %superq(SYSCC) ne 0 %then %do;
    %let ERROR = ERROR in SAS Code: %superq(SYSERRORTEXT);
    %let STATUS = error;
  %end;
%mend;
