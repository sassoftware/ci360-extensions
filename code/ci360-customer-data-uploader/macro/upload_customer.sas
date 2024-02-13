/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/

%macro upload_customer();
	/*print to log - start*/
	%local loc_rc;
	%let loc_rc = 0;
	%let __suffix=%sysfunc(compress(%sysfunc(translate(%sysfunc(date(),yymmdd10.)_%sysfunc(time(),tod.),-,:)))); 
	proc printto log="&ci360_log_path/upload_customer_&__suffix..log";
	run;

	%let token2=%str(&token.);

	/* %get_version(&IB_UPLOAD_CUST_PATH./version) */
	%global __cnt;
	%let _msg=;
	%let __cnt=0;

	%let _maxpokus=120; /*timeout v min pro web service*/
	options mprint;

	/* 1. Prepare customer data for upload */
	/* Note: update logic accordingly based on the source for customer data upload. Logic may 	include table joins, transformations and additional attributes to identify data for upload to CI360. Code may be defined to run and scheduled separately. */
	%if not %member_exists(&IB_DBOLIB..&dataHubTable.)
   		%then %do;
		%PUT WARNING: NOT Exist some of the table &IB_DBOLIB..&dataHubTable. &IB_BATCHLIB..&custTable. ;
		%return;
	%end;
	
    /* Create an auxiliary table to identify records that need to be added or updated to CI360. Define the DateEffectiveChange (last_updated_ts) column to help identify new data points compared to those already uploaded in CI360. */
	PROC SQL NOPRINT;
		create table merged_data as
		select
			s.subject_id,
			coalescec(s.customer_id, t.customer_id) as customer_id,
      		coalesce(s.individual_key, t.individual_key) as individual_key,
			coalescec(s.email, t.email) as email,
			coalesce(s.phonenumber, t.phonenumber) as phonenumber,
      		case when (datepart(s.last_updated_ts)=datepart(t.last_updated_ts)) then t.last_updated_ts else datetime()end as last_updated_ts
			/* add more fields as needed */
		from &IB_DBOLIB..&dataHubTable. t
			right join (
				select 
					 individual_key,
           last_updated_ts,
           subject_id,
           customer_id,
           email,
           phonenumber
           /* add more fields as needed */
				from &IB_BATCHLIB..&custTable. 
			) s
			on t.subject_id = s.subject_id;	  
	QUIT;		
	
	PROC SQL NOPRINT;
		delete * from &IB_DBOLIB..&dataHubTable.
		where subject_id in (select subject_id from WORK.merged_data);
	QUIT;

	PROC APPEND BASE=&IB_DBOLIB..&dataHubTable. DATA=merged_data FORCE;
	RUN;

	proc sql;
		create table customer_upl as
		select 
      a.subject_id,
			a.customer_id,
			a.individual_key,
			a.email,
			a.phonenumber
      /* add more fields as needed */
		from &IB_DBOLIB..&dataHubTable. a
		WHERE datepart(a.last_updated_ts)=today()
		;
	quit;


	/* 2. Prepare web identity data for upload */
	/* Note: Update logic accordingly based on the source and definition of identities. Logic may include joins and transformations between data sources. Code may be defined to run and scheduled separately. Initial identity upload may be done through the seperate identity mapping upload with supplemental web identity upload with customer data */

	proc sql;
		create table webidentity as
			select 
				a.subject_id as subject_id,
				a.subject_id as login_id
			from &IB_DBOLIB..&dataHubTable. a
			where datepart(a.last_updated_ts)=today()
		;
	quit;

	proc sql noprint;
		select count(*)
			into : __cnt trimmed
		from customer_upl
		;
	quit;
	%put NOTE: Count of uploaded customers: &__cnt;

	/*3. Create export csv*/
	/* Note: update logic accordingly based on final list of customer attributes. List must be aligned to the defined Data Descriptor in the CI 360 UI */	

	%macro upl_cust(indata=,descriptor=,idx=);

		%let DBcsv=&IB_UPLOAD_CUST_PATH./req/customer_upl&idx..csv;
		filename reqBody "&IB_UPLOAD_CUST_PATH./req/reqBody&idx..txt";
		filename outBody1 "&IB_UPLOAD_CUST_PATH./req/outBody1&idx..txt";
		filename outHdr1 "&IB_UPLOAD_CUST_PATH./req/outHdr1&idx..txt";
		filename outBody2 "&IB_UPLOAD_CUST_PATH./req/outBody2&idx..txt";
		filename outHdr2 "&IB_UPLOAD_CUST_PATH./req/outHdr2&idx..txt";
		filename outBody3 "&IB_UPLOAD_CUST_PATH./req/outBody3&idx..txt";
		filename outHdr3 "&IB_UPLOAD_CUST_PATH./req/outHdr3&idx..txt";
		filename outBody4 "&IB_UPLOAD_CUST_PATH./req/outBody4&idx..txt";
		filename outHdr4 "&IB_UPLOAD_CUST_PATH./req/outHdr4&idx..txt";
		filename outBody5 "&IB_UPLOAD_CUST_PATH./req/outBody5&idx..txt";
		filename outHdr5 "&IB_UPLOAD_CUST_PATH./req/outHdr5&idx..txt";

		%if &__cnt ne 0 %then %do;
			options NOBOMFILE;
			filename output "&DBcsv" encoding="utf-8";

			proc export data=&indata /*(obs=1)*/ outfile=output dbms=csv replace;
			run;

			/*4. get id for Data Descriptor*/
			proc http
				url="https://&CI360_server./marketingData/tables?limit=99999"
				method="get"
				out=outBody1
				headerout=outHdr1;
					headers
					"Content-Type"="application/json"
					"Authorization"="Bearer &token2.";
			run;

			libname tableIDs JSON fileref=outBody1;
			proc sql noprint;
				select id into: table_id TRIMMED from tableIDs.ITEMS where name="&descriptor";
			quit;
			libname tableIDs clear;

			%if %is_blank(table_id) %then %do;
				%put WARNING: No table_id for &=descriptor;
				%let loc_rc = %sysfunc(max(&loc_rc., 888));
				%goto exit_upl_cust;
			%end;

			%put NOTE: &descriptor ID: &table_id;

			/* 5. post for temp url*/
			proc http
				url="https://&CI360_server./marketingData/fileTransferLocation"
				method="post"
				out=outBody2
				headerout=outHdr2;
					headers
					"Content-Type"="application/json"
					"Authorization"="Bearer &token2.";
			run;

			libname urlresp JSON fileref=outBody2;
			proc sql noprint;
				select Value into: url_tmp TRIMMED from urlresp.alldata where P1="signedURL";
			quit;
			libname urlresp clear;
			%put NOTE: Temporary url: &url_tmp;


			/*6. upload csv to temporary url*/
			%put NOTE: Upload csv: &DBcsv;
			filename reqCSV "&DBcsv";

			proc http 
				url="&url_tmp"
				method="put"
				in=reqCSV
				out=outBody3
				headerout=outHdr3;
				;
			run;


			/*7. create final json*/
			data _null_;
				file reqBody;
				put "{
					""contentName"":""upload_customer&idx."",
					""dataDescriptorId"":""&table_id"",
					""fieldDelimiter"":"","",
					""fileLocation"":""&url_tmp"",
					""fileType"":""CSV"",
					""headerRowIncluded"": true,
					""recordLimit"":0,
					""updateMode"":""upsert""
				}"
				;
			run;

			/*8. post final json to cloud*/
			proc http
				url="https://&CI360_server./marketingData/importRequestJobs"
				method="post"
				in=reqBody
				out=outBody4
				headerout=outHdr4;
					headers
					"Content-Type"="application/json"
					"Authorization"="Bearer &token2.";
			run;

			/*9. Check job result*/
			libname result JSON fileref=outBody4;
			proc sql noprint;
				select id into: _id TRIMMED from result.root where name="upload_customer&idx.";
				select status into: _status TRIMMED from result.root where name="upload_customer&idx.";
			quit;
			libname result clear;
			%put NOTE: STATUS = &_status;
			%put NOTE: ID = &_id;

			%macro check_job;

				%let _pokus=0;

				%do %while (&_status ne Imported and &_status ne %str(Failed Validation) and %eval(&_pokus<&_maxpokus));
					proc http 
						url="https://&CI360_server./marketingData/importRequestJobs/&_id"
						method="get"
						out=outBody5
						headerout=outHdr5;
							headers 
							"Content-Type"="application/json" 
							"Authorization"="Bearer &token2.";
					run;

					libname result JSON fileref=outBody5;
					proc sql noprint;
						select status into: _status TRIMMED from result.root where name="upload_customer&idx.";
					quit;
					libname result clear;
					%let _pokus=%eval(&_pokus+1);
					%put NOTE: STATUS = &_status;
					%put NOTE: POKUS = &_pokus;
					%let _slp=%sysfunc(sleep(60,1));
				%end;

				%if &_status=Imported %then %do;
					%let _msg=NOTE: &__cnt. records. successfully IMPORTED.;
					%put &_msg.;
				%end;
				%else %do;
					%let _msg=ERROR: Data were not imported correctly: &_status;
					%put &_msg.;
					%let loc_rc = %sysfunc(max(&loc_rc., 999));
				%end;

			%mend;

			%check_job;

			%exit_upl_cust:
		%end;
		%else %do;
			%let _msg=WARNING: No customers to upload.;
			%put &_msg.;
		%end;

	%mend;

	/* Upload Customer Data */
	/* Note: Specify final customer table for upload under the indata parameter */
	%upl_cust(indata=customer_upl,descriptor=&descriptor_cust.,idx=_1);
	
	/* Upload Web Identity Data */
	/* Note: (Optional) Specify final web identity table for upload under the indata parameter */
  	%upl_cust(indata=webidentity,descriptor=&descriptor_identity.,idx=_2);
		
    %let syscc=&loc_rc.;
	/*print to log - end*/
	proc printto;
	run;

%mend upload_customer;
