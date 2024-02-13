/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%macro upload_customer_run();

	/*print to log - start*/
	%let __suffix=%sysfunc(compress(%sysfunc(translate(%sysfunc(date(),yymmdd10.)_%sysfunc(time(),tod.),-,:)))); 
	proc printto log="&ci360_log_path/upload_customer_run_&__suffix..log";
	run;
	
    /* Check if source tables are available for upload */
	/* Script may be changed depending on source tables and required logic for checking of table availability */
	proc sql noprint;
		/*check for customer table source */
		select count(*) into :_cnt_cust trimmed
		from &IB_DBOLIB..&semaphoresTable.
		where upcase(table_name)='CUSTOMER'
		and datepart(effective_datetime)=today()  
		and active_flg='1';
		
		/*check logs if upload customer & identity scripts are still running or if scripts have been executed for the day. Ensure there are no pending identities for upload to avoid mismatchs.  NOTE: Logic may vary depending on project requirements and table administration. */
		select count(*) into :_cnt_upl trimmed
		from &IB_DBOLIB..&eventLogTable.
		where upcase(event_domain)='UPLOAD_CUSTOMER'
		and datepart(event_start)=today();

		select count(*) into :_cnt_sub trimmed
		from &IB_DBOLIB..&eventLogTable.
		where upcase(event_domain)='UPLOAD_SUBJECTS'
		and datepart(event_start)=today();

		select count(1) into :_cnt_pending trimmed
		from &IB_DBOLIB..&idmapPendingTable.
		where STATUS = 'PENDING';
	quit;

	%PUT NOTE: &_cnt_cust  &_cnt_upl &_cnt_sub  &_cnt_pending;

	%if &_cnt_cust. ne 0 and &_cnt_upl. eq 0  and &_cnt_pending. eq 0 %then %do;

		%put NOTE: Input tables are ready for upload.;

		%let time_from=%sysfunc(datetime());

		%upload_customer;

		%let time_to=%sysfunc(datetime());
		%let dur=%eval(%sysfunc(round(%sysfunc(inputn(&time_to,32.0))))-%sysfunc(round(%sysfunc(inputn(&time_from,32.0)))));
		
		%if &__cnt. gt 0 %then %do;
			PROC SQL NOPRINT;
				INSERT INTO &IB_DBOLIB..&eventLogTable. 
				(event_domain, event_desc, event_user, event_hostname, event_start, event_end, event_duration, event_row_cnt, event_rc)
				VALUES ('UPLOAD_CUSTOMER', 'Upload customer to Cloud', "&SYSUSERID", "&syshostname", &time_from, &time_to, &dur, &__cnt, &syscc)
			;
			QUIT;
		%end;
		%else %do;
			%put WARNING:  No customers to upload.;
		%end;
	%end;
	%else %do;
		%put NOTE: Input tables are not ready for upload.;
	%end;

%mend upload_customer_run;

