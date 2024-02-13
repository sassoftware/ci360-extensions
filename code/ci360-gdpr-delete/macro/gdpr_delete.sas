/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%macro gdpr_delete(
	mpGdprDeleteTable = &gdprDeleteTable.
);
	%global _cntreq;
	%let GDPR_token=%str(&token.);
	%let _maxpokus=120; /*timeout (min) web service*/

	filename reqBody "&IB_GDPR_DELETE_PATH./req/reqBody.txt";
	filename outBody "&IB_GDPR_DELETE_PATH./req/outBody.txt";
	filename outHdr "&IB_GDPR_DELETE_PATH./req/outHdr.txt";
	filename outBody2 "&IB_GDPR_DELETE_PATH./req/outBody2.txt";
	filename outHdr2 "&IB_GDPR_DELETE_PATH./req/outHdr2.txt";

	/*Adjust filters as needed to identify records to be deleted*/
	proc sql noprint;
		select count(*) into :_cntreq trimmed
		from &IB_STAGELIB..&mpGdprDeleteTable.
		WHERE datepart(TransferEffectiveDtime) = today() - 5
		;
	quit;

	%macro gdpr;
		%if &_cntreq ne 0 %then %do;
		
			%put NOTE: Table &mpGdprDeleteTable. is ready with &_cntreq subjects to gdpr delete.;

			/*Adjust filters as needed to identify records to be deleted*/
			proc sql;
				create table &mpGdprDeleteTable. as
					select SubjectId
					from &IB_STAGELIB..&mpGdprDeleteTable.
					WHERE datepart(TransferEffectiveDtime) = today() - 5
				;
			quit;

			data _null_;
				file reqBody;

				if _n_=1 then put "{ ""jobType"": ""GDPR_DELETE"", ""identityType"": ""subject_id"", ""identityList"": [";

				set &mpGdprDeleteTable. end=_konec;
				if not _konec then put '"' SubjectId +(-1) '",';

				if _konec then put '"' SubjectId +(-1) '" ] }';
			run;

			proc http
				url="https://&CI360_server./marketingData/customerJobs"
				method="post"
				in=reqBody
				out=outBody
				headerout=outHdr;
				headers
				"Content-Type"="application/json"
				"Authorization"="Bearer &GDPR_token.";
			run;

			/*check job*/
			libname result JSON fileref=outBody;
			proc sql noprint;
				select id into: _id TRIMMED from result.root where jobType='GDPR_DELETE';
				select status into: _status TRIMMED from result.root where jobType='GDPR_DELETE';
			quit;
			libname result clear;
			%put NOTE: STATUS = &_status;
			%put NOTE: ID = &_id;

			%macro check_job;

				%let _pokus=0;

				%do %while (&_status ne DELETED_IDENTITIES and %eval(&_pokus<&_maxpokus));

					proc http 
						url="https://&CI360_server./marketingData/customerJobs/&_id"
						method="get"
						out=outBody2
						headerout=outHdr2;
						headers 
						"Content-Type"="application/json" 
						"Authorization"="Bearer &GDPR_token.";
					run;

					libname result JSON fileref=outBody2;
					proc sql noprint;
						select status into: _status TRIMMED from result.root where jobType='GDPR_DELETE';
					quit;
					libname result clear;
					%let _pokus=%eval(&_pokus+1);
					%put NOTE: STATUS = &_status;
					%put NOTE: POKUS = &_pokus;
					%let _slp=%sysfunc(sleep(60,1));
				%end;

				%if &_status=DELETED_IDENTITIES %then %do;
					%put NOTE: Data successfully DELETED.;
				%end;
				%else %do;
					%put ERROR: Data were not deleted correctly: &_status;
				%end;

			%mend check_job;

			%check_job;

		%end;

		%else %do;
			%put WARNING: Table &mpGdprDeleteTable. has no subjects to gdpr delete.;
		%end;

	%mend gdpr;

	%gdpr;

%mend gdpr_delete;