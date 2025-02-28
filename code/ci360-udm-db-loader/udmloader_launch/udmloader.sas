
/******************************************************************************/
/* Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.*/
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/

%macro ErrCheck(errormsg,table,err_macro=SYSERR);
%global CDM_ErrMsg;
	%if (&syserr. > 4) %then %do;
		%let errFlag = 1;
		%let CDM_ErrMsg = 'Unable to load UDM/CDM  ' &table. and process aborted at :&errormsg. ;
		%put  %sysfunc(datetime(),E8601DT25.) --- &syserr.;
		%put  %sysfunc(datetime(),E8601DT25.) --- ERROR: &syserrortext;
	%end;
	%else %do; 
	%let CDM_ErrMsg = ;
	%end;

	%if &err_macro=SYSDBRC %then %do;
		%put &err_macro. : &&&err_macro. ;
		%if ("&database."="MSSQL" and &&&err_macro. > 01000) %then %do; 
			%let errFlag = 1;
			%let CDM_ErrMsg = 'Unable to load UDM  ' &table. and process aborted at :&errormsg. ;
			%put  %sysfunc(datetime(),E8601DT25.) --- &syserr.;
			%put  %sysfunc(datetime(),E8601DT25.) --- ERROR: %superq(&syserrortext);
		%end;
	%end;
%mend;
/*%ErrCheck(test,table_name,err_macro=SYSDBRC);*/

%macro udmloader;
%let errFlag=0;
options mprint SPOOL ;
%include "/userdata/udmloader/config/config.sas"; 

/*%let sysparameter=&sysparm;*/
%Let sysparameter=CREATEDDL;
%put sysparm=%str(%upcase(&sysparameter)) ;
%if  "&sysparameter." eq "GENERATEMETADATA" %then %generate_base_metadata_tbl;
/*%else %if  "&sysparameter." = "UPDATEMETADATA" %then %upsert_metadata;*/
%else %if  "&sysparameter." = "CREATEDDL" OR "&sysparameter." = "CREATEETLCODE" %then %generate_code;
%else %if  "&sysparameter." = "EXECUTEDDL" %then %execute_ddl;
%else %if  "&sysparameter." = "LOADDATA" %then %load_data;
%else %if  "&sysparameter." = "CREATEUPGRADEDDL" %then %create_upgrade_ddl;
%else %if  "&sysparameter." = "EXECUTEUPGRADEDDL" %then %do;
	%include "&UtilityLocation.&slash.code&slash.mig_&database._udm_V&previous_schema_version._to_V&schema_version..sas";
%end;
%else %do;
%put Inappropriate parameter; 
%end;

%put ******** CDM-UDM Utility END HERE *********;

/*%ERREXIT:*/
%put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;

/*****************************************************************************
	STOP PRINTING LOG
******************************************************************************/
proc printto; run;
%mend udmloader;
%udmloader;
