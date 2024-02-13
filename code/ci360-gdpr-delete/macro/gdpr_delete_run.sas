/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%macro gdpr_delete_run(
	mpGdprDeleteTable = &gdprDeleteTable.
	, mpEventLogTable = &eventLogTable.);
	
    /*print to log - start*/
	%let __suffix=%sysfunc(compress(%sysfunc(translate(%sysfunc(date(),yymmdd10.)_%sysfunc(time(),tod.),-,:)))); 
	proc printto log="&ci360_log_path/gdpr_delete_&__suffix..log";
	run;

	/*check if source table is available. Filters may be adjusted as needed*/
	proc sql noprint;
		select count(*) into :_cnt_gdpr trimmed
		from &IB_METADATALIB..&diBatchControlIn.
		where Table_Name="&mpGdprDeleteTable."
			/* and mdy(input(substr(Effective_Datetime,5,2),best12.),input(substr(Effective_Datetime,7,2),best12.),input(substr(Effective_Datetime,1,4),best12.)) = today() - 5 */
			and datepart(input(substr(Effective_Datetime, 1, 19), anydtdtm29.)) = today() - 5
		;
	quit;

	/*check Event_log table if GDPR delete process was already run for the day*/
	proc sql noprint;
		select count(*) into :_cnt_upl trimmed
		from &IB_DBOLIB..&mpEventLogTable.
		where upcase(event_domain)='GDPR_DELETE'
			and datepart(event_start)=today()  
	;
	quit;

	%put NOTE: &_cnt_gdpr &_cnt_upl;

	%if &_cnt_gdpr ne 0 and &_cnt_upl = 0 %then %do;

		%put NOTE: Input tables &mpGdprDeleteTable. is ready to upload.;	

		%let time_from=%sysfunc(datetime());

		%gdpr_delete(
			mpGdprDeleteTable = &mpGdprDeleteTable.
		);

		%let time_to=%sysfunc(datetime());
		%let dur=%eval(%sysfunc(round(%sysfunc(inputn(&time_to,32.0))))-%sysfunc(round(%sysfunc(inputn(&time_from,32.0)))));

		%if not %is_blank(_cntreq) %then %do;
			PROC SQL NOPRINT;
				INSERT INTO &IB_DBOLIB..&mpEventLogTable.
				(event_domain, event_desc, event_user, event_hostname, event_start, event_end, event_duration, event_row_cnt, event_rc)
				VALUES ('GDPR_DELETE', 'GDPR delete to Cloud', "&SYSUSERID", "&syshostname", &time_from, &time_to, &dur, &_cntreq, &syscc)
			;
			QUIT;
		%end;
		%else %do;
			%put WARNING: Table &mpGdprDeleteTable. has no subjects to gdpr delete.;
		%end;

	%end;
	%else %do;
		%put NOTE: Input tables &mpGdprDeleteTable. is not ready to upload.;
	%end;

	proc printto log=log;
	run;
 
%mend;