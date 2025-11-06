/**********************************************************************************************************************
PROGRAM: SAS CUSTOMER INTELLIGENCE 360 Custom On-Prem Scheduler for Direct Marketing Tasks, Bulk Email Tasks and Segment Maps
DESCRIPTION: This application helps the CI 360 User to create deployed jobs from the CI 360 tenant 
             for Direct Marketing Tasks, Bulk Email Tasks and Direct Segment Maps for a SAS Administrator 
             to schedule on-prem in the SAS LSF scheduler.
FILE NAME: check_value.sas
DESCRIPTION: This macro helps check if a macro variable exists and is not empty. 
VERSION: 2.0
DATE MODIFIED: 25-APRIL-2024
AUTHOR: GLOBAL CUSTOMER INTELLIGENCE ENABLEMENT TEAM

#Copyright � 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0
**********************************************************************************************************************/

%macro check_value(MACROVAR, ALLOW_BLANK=FALSE);
  %global STATUS ERROR;
  %if %symexist(%superq(MACROVAR)) ne 1 %then %do;
    %let ERROR = Macro variable %superq(MACROVAR) does not exist;
    %put ERROR: %superq(ERROR);
    %let STATUS = error;
    %return;
  %end;
  %put %superq(MACROVAR) = %superq(&MACROVAR.);
  %if "%superq(&MACROVAR.)" eq "" and %upcase(%superq(ALLOW_BLANK)) ne TRUE %then %do;
    %let ERROR =Macro variable %superq(MACROVAR) cannot be blank;
    %put ERROR: %superq(ERROR);
    %let STATUS = error;
  %end;  
%mend;
