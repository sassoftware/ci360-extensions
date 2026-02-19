/*******************************************************************************/
/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                         */
/* *****************************************************************************/

%macro err_check(err_msg,table,err_macro=SYSERR);
	%global UDM_ErrMsg;
	%if (&syserr. > 4) %then %do;
		%let errFlag = 1;
		%let UDM_ErrMsg = 'Unable to load  ' &table. and process aborted at :&err_msg. ;
		%put  %sysfunc(datetime(),E8601DT25.) --- &syserr.;
		%put  %sysfunc(datetime(),E8601DT25.) --- ERR%str()OR: %superq(syserrortext);
	%end;
	%else %do;
	%let UDM_ErrMsg = ;
	%end;

	%if &err_macro=SYSDBRC %then %do;
		%put &err_macro. : &&&err_macro. ;
		%if ("&database."="MSSQL" and &&&err_macro. > 01000) OR
			("&database."="POSTGRES" and &&&err_macro. > 0) 
			%then %do; 
			%let errFlag = 1;
			%let UDM_ErrMsg = 'Unable to load UDM  ' &table. and process aborted at :&err_msg. ;
			%put &=SYSDBMSG;
			%put %sysfunc(datetime(),E8601DT25.) --- &syserr.;
			%put %sysfunc(datetime(),E8601DT25.) --- ERR%str()OR: %superq(syserrortext);
		%end;
	%end;
%mend;
/*%err_check(test,table_name,err_macro=SYSDBRC);*/
