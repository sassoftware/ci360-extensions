/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%macro SendIDMap();
	%local msg recs upload_url import_rc file_count;
	%let rc = 0;

	%macro CHECK_SQL_ERROR(SQLRC=,MSG=,MSG_FAIL=);
		%if (%eval(&SQLRC <= 4)) %then %do;
			%Log(2, %nrstr(%SendIDMap), &MSG, 0);
		%end;
		%else %do;
			%let msg = &MSG_FAIL &SQLRC;
			%Log(0, %nrstr(%SendIDMap), **********************************************, 0);
			%log(0, %nrstr(%SendIDMap), Processing failed: &msg.;, 0);
			%Log(0, %nrstr(%SendIDMap), **********************************************, 0);
			%let rc = 0;
		%end;
	%mend;

	%Log(1, %nrstr(%SendIDMap), **********************************************, 0);
	%Log(1, %nrstr(%SendIDMap), Preparing to send IDs to 360 ..., 0);
	%Log(1, %nrstr(%SendIDMap), **********************************************, 0);

	/* ************************************************************** */
	/* CREATE THE UPLOAD FILE
	/* ************************************************************** */

	/* delete output data sets if they already exist */
	PROC DATASETS LIBRARY=WORK NOPRINT NOWARN;
	  	DELETE idmap_upload existing_file FailedFiles idmap_failures;
	QUIT;


	%if &sas_create_file %then %do;

		PROC SQL NOPRINT;
			/* start by marking all existing pending records as WIP so we know which ones we are working with in case more get added */
			UPDATE &sas_db_libref_idmap..CI360_IDMAP_PENDING SET status='WIP' WHERE status='PENDING';												  
		QUIT;

		%if (%eval(&SQLRC <= 4)) %then %do;
			%Log(2, %nrstr(%SendIDMap), Changed PENDING to WIP., 0);
		%end;
		%else %do;
			%let msg = Failed to change the status from PENDING to WIP: &SQLRC;
			%goto exit_end;
		%end;

		PROC SQL NOPRINT;
			/* build the list of IDs to upload based on the WIP records */
			CREATE TABLE work.idmap_upload AS
				SELECT &db_upload_list 
				FROM &sas_db_libref_idmap..CI360_IDMAP_PENDING 
				WHERE status='WIP'
			;
		QUIT;

		data _NULL_;
			if 0 then set WORK.idmap_upload nobs=n;
			call symputx('recs',n);
			stop;
		run;

		%if (%eval(&SQLRC <= 4)) %then %do;
			%Log(2, %nrstr(%SendIDMap), Succesfully downloaded a list of &recs records to be uploaded., 0);
		%end;
		%else %do;
			%let msg = Failed to download the list of new IDs to be processed: &SQLRC;
			%goto exit_end;
		%end;

		/* only continue if there were any records this time */
		%if (&recs > 0) %then %do;

			/* This approach doesn't add the record delimiter (CRLF) to the final line */
			data _null_;
				file "&sas_upload_file" recfm=n;
				length columns $ 2048;
				set work.idmap_upload;
				if _n_ > 1 then put "0A"x;
				columns = compress(customer_id || "," || subject_id);
				put columns;
			run;

			%if (%eval((&SYSERR > 0) and not(&SYSERR = 4))) %then %do;
				%let msg = Failed to export the file to be uploaded: &SYSERR;
				%goto exit_fail;
			%end;
			%else %do;
				%Log(2, %nrstr(%SendIDMap), Succesfully created a list of &recs records to be uploaded., 0);
			%end;

		%end;

	%end;
	%else %do;

		DATA work.existing_file;
			INFILE "&sas_upload_file";
			INPUT;
		RUN;

		%let recs = &SYSNOBS;

		%if (%eval((&SYSERR > 0) and not(&SYSERR = 4))) %then %do;
			%let msg = Failed to read the file to be uploaded: &SYSERR;
			%goto exit_fail;
		%end;
		%else %do;
			%Log(2, %nrstr(%SendIDMap), Using existing file with &recs records to be uploaded., 0);
		%end;

	%end;

	/* regardless of how we got here, still need to at least have some rows to work with */
	%if &recs > 0 %then %do;

		/* ************************************************************** */
		/* UPLOAD THE FILE
		/* ************************************************************** */

		/* get the upload URL */
		PROC DS2 NOLIBS CONN="((DRIVER=BASE;CATALOG=&sas_utility_library;SCHEMA=(NAME=&sas_utility_library;PRIMARYPATH={&sas_utility_path/data}));(DRIVER=BASE;CATALOG=WORK;SCHEMA=(NAME=WORK;PRIMARYPATH={%sysfunc(pathname(work))}));)";
			data WORK.UploadPathResult (OVERWRITE=YES);
				declare package &sas_utility_library..CI360Utilities ci360(%tslit(&ci360_env), &api_tenant, &api_agent, &api_secret, &sas_log_level);
				declare varchar(2048) url;

				method run();
					url = ci360.RequestUploadPath('fileTransferLocation');
				end;
			enddata;
			run;
		QUIT;

		/* see if that worked */
		PROC SQL NOPRINT;
			SELECT 	url
			INTO	:upload_url
			FROM	work.UploadPathResult
			WHERE	url IS NOT NULL;
		QUIT;

		%let rc = %eval((&SQLOBS = 1) and ((&SQLRC = 0) or (&SQLRC = 4)));
		%if (&rc = 0) %then %do;
			%let msg = Failed to get the upload URL..;
			%goto exit_fail;
		%end;
		%else %do;
			%let pos = %eval(%index(%superq(upload_url), ?) - 1);
			%Let pattern = %SysFunc(PRXPARSE(s/(.{&pos}).*/$1/));
			%let upload_url = %sysfunc(PRXCHANGE(&pattern, -1, %superq(upload_url)));
			%Log(3, %nrstr(%SendIDMap), Uploaded URL: %superq(upload_url), 0);
		%end;

		/* upload the file to the URL provided */
		/* (Dynamic SAS code in order to deal with ampersands in the URL) */
		filename ulcode "&sas_upload_code";
		data _null_;
			set work.UploadPathResult;
			file ulcode;
			put 'filename upload "&sas_upload_file";';
			put '%let url=%nrstr(' url +(-1) ');';
			put 'proc http';
			put '	url="&url"';
			put '	method="PUT"';
			put '	in=upload;';
			put 'quit;';
		run;
		%include ulcode;

		/* see if that worked */
		%let rc = %eval(((&SYSERR = 0) or (&SYSERR = 4)) and (&SYS_PROCHTTP_STATUS_CODE = 200));
		%if (&rc = 0) %then %do;
			%let msg = Failed to upload URL the file to S3: &SYS_PROCHTTP_STATUS_CODE;
			%goto exit_fail;
		%end;

		/* complete the import process */
		%Log(2, %nrstr(%SendIDMap), Requesting import of uploaded file., 0);
		PROC DS2 NOLIBS CONN="((DRIVER=BASE;CATALOG=&sas_utility_library;SCHEMA=(NAME=&sas_utility_library;PRIMARYPATH={&sas_utility_path/data}));(DRIVER=BASE;CATALOG=WORK;SCHEMA=(NAME=WORK;PRIMARYPATH={%sysfunc(pathname(work))}));)";
		    data WORK.ImportFileResult (OVERWRITE=YES);
		        declare package &sas_utility_library..CI360Utilities ci360(%tslit(&ci360_env), &api_tenant, &api_agent, &api_secret, &sas_log_level);
				declare int rc intImportDataStatus;
				dcl varchar(2048) strFailedURL strRejectedURL;

		        method run();

					/* get the files - depending on which variables are enabled */
		            rc = ci360.ImportData(%tslit(&descriptor_name), %tslit(&descriptor_name), %tslit(&upload_url), 'upsert', 0, 
											&import_wait_mins, intImportDataStatus, strFailedURL, strRejectedURL);
					put 'intImportDataStatus ' intImportDataStatus;
		        end;

		    enddata;
		    run;
		QUIT;

		/* ************************************************************** */
		/* HANDLE THE RESULTS
		/* ************************************************************** */

		/* see if that worked */
		proc sql noprint;
			SELECT 	rc, intImportDataStatus, strip(strRejectedURL), strip(strFailedURL) 
			INTO 	:import_rc, :importDataStatus, :strRejectedURL, :strFailedURL
			FROM	work.ImportFileResult;
		quit;

		%CHECK_SQL_ERROR(SQLRC=&SQLRC.,MSG='Macro variables successfully created.',MSG_FAIL='Macro variables failed created:');
		
		%put &=strFailedURL;
		%put &=strRejectedURL;

		proc sql noprint;
			create table distinct_identities as
				SELECT distinct subject_id
				FROM &sas_db_libref_idmap..&MTABLE;
		quit;


		%CHECK_SQL_ERROR(SQLRC=&SQLRC.,MSG='Succesfully created table.',MSG_FAIL='Failed created table:');

		%let importDataStatusDesc=;

		/* create a table to house failures */
		DATA work.idmap_failures;
			LENGTH subject_id $ 7;
			stop;
		RUN;
		
		/* create a table to house failed */
		DATA work.idmap_failed;
			LENGTH subject_id $ 50;
			LENGTH customer_id $ 50;
			LENGTH url_error $ 2048;
			stop;
		RUN;

		/* create a table to house rejected */
		DATA work.idmap_rejected;
			LENGTH subject_id $ 50;
			LENGTH customer_id $ 50;
			LENGTH url_error $ 2048;
			stop;
		RUN;
		
		%if (&import_rc = 1) %then %do;
			/* populate the table of failures */
			proc append base=work.idmap_failures data=distinct_identities;
			run;

			%if &importDataStatus = 4 %then %let importDataStatusDesc = FAILED_VALIDATION; 
			%if &importDataStatus = 5 %then %let importDataStatusDesc = FAILED_PROCESSING; 
			%if &importDataStatus = 6 %then %let importDataStatusDesc = FAILED_IDENTITIES; 
			%if &importDataStatus = 7 %then %let importDataStatusDesc = FAILED_TARGETING_PROCESSED;
			

			/* anyone in the list of failures needs to change status */
			PROC SQL NOPRINT;	
				UPDATE &sas_db_libref_idmap..CI360_IDMAP_PENDING
				SET status = "&importDataStatusDesc."
				WHERE status='WIP' AND
					subject_id IN (SELECT subject_id FROM work.idmap_failures);															  
			QUIT;
				

			%CHECK_SQL_ERROR(SQLRC=&SQLRC.,MSG='Succesfully updated status WIP records before download.',MSG_FAIL='Failed to update status WIP records before download:');

		%end;
		%else %if (&import_rc = 0) %then %do;
			%if &importDataStatus = 1 %then %let importDataStatusDesc = DOWNLOAD_FAILED; 
			%if &importDataStatus = 2 %then %let importDataStatusDesc = DOWNLOAD_REJECTED;
			%if &importDataStatus = 3 %then %let importDataStatusDesc = DOWNLOAD_FAILED_AND_REJECTED;
			%if &importDataStatus = 0 %then %let importDataStatusDesc = IMPORTED;
			

			%if &importDataStatus = 1 or &importDataStatus = 3 %then %do; /*IF DOWNLOAD FAILED OR REJECTED + FAILED*/

				/*download failed + populate idmap_failed*/
				%let data = work.idmap_failed;
				filename resp "<root path>\idmapping-master\output_file\failed.csv";
				%include "&sas_include_path/macro/download_rejected_failed.sas";
				%download_url(url=&strFailedURL,data=&data);
			
				PROC SQL NOPRINT;	
					UPDATE work.IDMAP_FAILED 
						SET url_error="&strFailedURL.";
				QUIT;

				PROC SQL NOPRINT;
					UPDATE &sas_db_libref_idmap..CI360_IDMAP_PENDING
					SET status = "&importDataStatusDesc."
					WHERE status='WIP' AND
						subject_id IN (SELECT subject_id FROM work.idmap_failed);																  
				QUIT;
		

			%CHECK_SQL_ERROR(SQLRC=&SQLRC.,MSG='Succesfully updated status WIP records after download.',MSG_FAIL='Failed to update WIP records after download:');
			
		
			%end;
			%else %if &importDataStatus = 2 or &importDataStatus = 3 %then %do;/*IF DOWNLOAD REJECTED OR REJECTED + FAILED*/

			/*download rejected + populate idmap_rejected*/

			   	%let data = work.idmap_rejected;
				filename resp "<root path>\idmapping-master\output_file\rejected.csv";
				%include "&sas_include_path/macro/download_rejected_failed.sas";
				%download_url(url=&strRejectedURL,data=&data);

				PROC SQL NOPRINT;	
					UPDATE work.IDMAP_REJECTED 
						SET url_error="&strRejectedURL.";
				QUIT;


				PROC SQL NOPRINT;
					UPDATE &sas_db_libref_idmap..CI360_IDMAP_PENDING 
						SET status = "&importDataStatusDesc."
						WHERE status='WIP' AND
							subject_id IN (SELECT subject_id FROM work.IDMAP_REJECTED);																	  
				QUIT;

			%CHECK_SQL_ERROR(SQLRC=&SQLRC.,MSG='Succesfully updated status WIP records after download.',MSG_FAIL='Failed to update WIP records after download:');


			%end;
			%let importDataStatusDesc = IMPORTED;
		%end;
		%else %do;
			%let importDataStatusDesc = FAILED;
		%end;
		
			
			PROC SQL NOPRINT;
				UPDATE &sas_db_libref_idmap..CI360_IDMAP_PENDING 
					SET status = "&importDataStatusDesc."
					WHERE status='WIP' AND
						subject_id IN (SELECT subject_id FROM work.distinct_identities) and
						subject_id not IN (SELECT subject_id FROM work.idmap_rejected) and
						subject_id not IN (SELECT subject_id FROM work.idmap_failed)
						;																  
			QUIT;



			%CHECK_SQL_ERROR(SQLRC=&SQLRC.,MSG='Succesfully updated status WIP records.',MSG_FAIL='Failed to update WIP records:');


			PROC SQL NOPRINT;
					INSERT INTO &sas_db_libref_idmap..CI360_IDMAP_PROCESSED	(subject_id, customer_id,processed_dttm,status)
						SELECT	subject_id,
								customer_id,
								datetime(),
								status
						FROM	&sas_db_libref_idmap..CI360_IDMAP_PENDING
						WHERE status <> 'WIP';
				;																	  
			QUIT;

	/*	%IF findw(importDataStatusDesc,'FAILED') OR findw(importDataStatusDesc,'REJECTED') %THEN %DO;
			%send_email(FLAG=RED,MSG=RECORD IDENTITY FAILED OR REJECTED);
		%END;
		%ELSE %DO;
			%send_email(FLAG=GREEN,MSG=RECORD IDENTITY UPLOADED WITH SUCCESS);
		%END;
*/
		%CHECK_SQL_ERROR(SQLRC=&SQLRC.,MSG='Succesfully inserted rows not WIP.',MSG_FAIL='Failed to insert rows not WIP:');

		PROC SQL NOPRINT;
				DELETE FROM &sas_db_libref_idmap..CI360_IDMAP_PENDING
					WHERE status <> 'WIP';
            ;  
		QUIT;

		%CHECK_SQL_ERROR(SQLRC=&SQLRC.,MSG='Succesfully removed rows not WIP.',MSG_FAIL='Failed to remove rows not WIP:');
		
	%end;
	%else %do;
		%put No records to process.;
	%end;

	%Log(1, %nrstr(%SendIDMap), **********************************************, 0);
	%Log(1, %nrstr(%SendIDMap), Completed successfully, 0);
	%Log(1, %nrstr(%SendIDMap), **********************************************, 0);

	%let rc = 1;

	%goto exit_end;

%exit_fail:

	%Log(0, %nrstr(%SendIDMap), **********************************************, 0);
	%log(0, %nrstr(%SendIDMap), Processing failed: &msg.;, 0);
	%Log(0, %nrstr(%SendIDMap), **********************************************, 0);

	%let rc = 0;

%exit_end:

%mend;


