/************************************************************************************************
| File Name: config.sas   
| Program Description: This file stores librefs, filerefs, variables, macros and pre-assigned 
|					   values required for the execution of the Stored Process.   
|
|Copyright  2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
|SPDX-License-Identifier: Apache-2.0	
|***********************************************************************************************/
   
/* Global Variables */
%global DTTM;
%global liblocation;
%global datafile;
%global metadatafile;
%global columnnames;
%global saslogdirect;
%global debug;

/* Variable Assignments */
%let saslogredirect = true; /* true - redirect STP log to a file */
%let debug = true; /* true - save all files created in program to log folder */
%let liblocation = %str(c:\STP);

/* Format to define datetimestamp in yymmddHHMMSS format */
data _null_;
  DTTM=strip(compress(tranwrd(put(datetime(), B8601DT.),"T","")));
  call symput("DTTM",DTTM);
run;

/*Check for Errors*/
%macro check_for_errors;
   %if &syserr > 4 %then %do;
      endsas;
   %end;
%mend check_for_errors;

/*Check return codes from Proc HTTP*/
%macro prochttp_check_return(code);
%if %symexist(SYS_PROCHTTP_STATUS_CODE) ne 1 %then %do;
  %put ERROR: Expected &code., but a response was not received from the HTTP Procedure;
  %abort;%end;
%else %do;
  %if &SYS_PROCHTTP_STATUS_CODE. ne &code. %then %do;
   %put ERROR: Expected &code., but received &SYS_PROCHTTP_STATUS_CODE. &SYS_PROCHTTP_STATUS_PHRASE.;
   %abort;%end;
%end;
%mend;
