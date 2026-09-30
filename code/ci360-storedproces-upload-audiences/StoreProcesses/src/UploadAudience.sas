/*-----------------------------------------------------------------------------
Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
-----------------------------------------------------------------------------*/

/*
COPY TO AUTOEXEC /app/sas/config/Lev1/SASApp/StoredProcessServer/autoexec_usermods.sas or eg. C:\SAS\Config\Lev1\SASApp\StoredProcessServer
%let STP_AUD_LOG_DIR          = C:\SAS\Contexts\Banking\Exports\ or /logs/sas/ci360/UploadAudience/;
%let STP_AUD_EMAIL_FROM       = AudienceProcess@yourcompany.com;
%let STP_AUD_GATEWAY          = https://extapigwservice-eu-prod.ci360.sas.com;
%let STP_AUD_TENANT_ID        = tenant_uid;
%let STP_AUD_CLIENT_SECRET    = access_point_secret_key;
%let STP_AUD_API_USER         = api_user;
%let STP_AUD_API_PW           = api_password;
%let STP_AUD_VAL_MINUTES      = 30; --how long script check the audience upload status
%let STP_AUD_EMAIL_LIST       = "admin@yourcompany.com" "marketing@yourcompany.com";
%let STP_proxyhost            = proxy.yoursite.com;   --do not create in autoexec if not used
%let STP_proxypw              = proxy_password;       --do not create in autoexec if not used 
%let STP_proxyuser            = proxy_username;       --do not create in autoexec if not used
%let STP_proxyport            = 1234;                 --do not create in autoexec if not used      
*/
options nosource nosource2;
options nomprint nomlogic nosymbolgen;
options msglevel=i fullstimer;

/*************************************/
/* GLOBAL MACRO VARIABLES            */
/*************************************/
%global
	audience_id
	temp_token
	audience_name
	uploadmissval
	STOP_AUDIENCE_PROCESS
	STP_AUD_LOG_DIR
;
%global abort_process;
%global _CLIENTAPP projectpath;

%macro log_error_vars;
	%put NOTE: &=syserr. &=syscc. &=syserrortext. &=syswarningtext.;
%mend;

%macro setLogDefault;
	%if %bquote(&projectpath.) eq %then %do;
		%let projectpath=%str(C:\workspace\360\ci360-storedproces-upload-audiences-main\StoreProcesses\);
	%end;

	%if %bquote(&STP_AUD_LOG_DIR.) eq  %then %do;
		%let STP_AUD_LOG_DIR=%bquote(&projectpath.logs\);
	%end;

	%put &=STP_AUD_LOG_DIR.;
	%log_error_vars;
%mend setLogDefault;

%setLogDefault;

%if %index(%bquote(&_CLIENTAPP), %str(Enterprise Guide))=0 and %index(%bquote(&_CLIENTAPP), %str(Studio))=0 %then %do;
	%stpbegin;
	filename outdata temp lrecl=32767;

	%binary_file_copy_cust( infile=livedata, outfile=outdata );
	%include outdata;
	filename outdata clear;

	%maspinit(xmlstream=macroVar neighbor);
	options dlcreatedir;
	libname mydir "&STP_AUD_LOG_DIR.";
	libname mydir clear;
	options nodlcreatedir;
	%let _stpUniqueId = %sysfunc(substr(%sysfunc(uuidgen(1, 0)), 1, 8));
	options nosource nosource2;
	options nomprint nomlogic nosymbolgen;

	proc printto log="&STP_AUD_LOG_DIR.stpAudienceUpload_%left(%sysfunc(datetime(),B8601DT15.))_&_stpUniqueId..log";
	run;

	&matables.;

	data matables.&taskcd._macrovar;
		set macrovar;
	run;

%end;
%else %do;
	/* options mprint mlogic symbolgen; */
	libname matables 'C:\SAS\Contexts\Banking\MATables';
	%let audience_id=%str(84368e42-3132-4eda-834b-355126e19f1d);
	%let uploadmissval=N;
	%let abort_process=0;
	%let syscc=0;

	%symdel STP_AUD_GATEWAY /nowarn;
	%global taskcd;
	%let taskcd=TSK_126;

	data macrovar;
		set matables.&taskcd._macrovar;
	run;

%end;
%log_error_vars;

%let STOP_AUDIENCE_PROCESS=0;
%let MSGERROR=;

data _null_;
	put "PRocess Started 1";
run;

%macro RetrieveConfigParameters(config_path=config.dat);
	%if %symexist(STP_AUD_GATEWAY) %then %do;
		%put &=STP_AUD_GATEWAY;
		%put NOTE: Macro variables are defined.  Configuration file processing skipped;
		%put _user_;
		%goto exit_RetrieveConfigParameters;
	%end;

	/* Assign a fileref to the configuration file */
	filename cfgfile &config_path.;
	%local rc fid;
	%let rc = %sysfunc(fexist(cfgfile));

	%if &rc = 0 %then %do;
		%put ERROR: Configuration file &config_path. was not found or is not accessible.;

		%return;
	%end;

	data _null_;
		length record $1000 key $200 value $800;
		infile cfgfile lrecl=1000 pad truncover;
		input record $char1000.;

		/* Trim trailing blanks (pad-related) but keep everything up to real content */
		record = trimn(record);

		/* Skip blank lines */
		if record = '' then
			return;

		/* Skip comment lines - those beginning with '#' */
		if substr(record,1,1) = '#' then
			return;

		/* Locate the FIRST '=' - value itself may contain '=' characters */
		eq_pos = index(record,'=');

		/* Skip malformed lines with no '=' at all */
		if eq_pos = 0 then do;
			put "WARNING: Skipping malformed config record (no '=' found): " record;
			return;
		end;

		key   = strip(substr(record,1,eq_pos-1));
		value = strip(substr(record,eq_pos+1));

		/* Skip records with an empty key */
		if key = '' then do;
			put "WARNING: Skipping config record with empty key: " record;
			return;
		end;

		/* Create the global macro variable */
		if value ne '' then do;
			call symputx(key, value, 'G');
			if prxmatch('/pw|secret/i', key) eq 0 then do;
				put "NOTE: Set global macro variable " key "= " value;
			end;
		end;
	run;

	filename cfgfile clear;

%exit_RetrieveConfigParameters:
%mend RetrieveConfigParameters;

%put &=projectpath;

%RetrieveConfigParameters(config_path="&projectpath.config.dat");
%log_error_vars;
data _null_;
	put "Process started 2";
run;

/*************************************/
/* MACROS DEFINED                          */
/*************************************/
/*Forcing an error to have Failed status in DM task. In case of errors in the validation process, we force an error*/
%macro forcingerror();
	%if &abort_process. = 1 %then %do;

		data msgerror;
			set ForcingError
				msg="Process aborted";
			put msg;
		run;

	%end;
%mend;

/*MACRO TO ABORT THE PROCESS IN CASE OF ERRROS*/
%macro check_status_process(stop=,msg=);

	data _null_;
		if &stop. eq 1 then do;
			put "ERROR: &msg";
		end;
	run;

	%if &stop. eq 1 %then
		%let abort_process=1;
%mend;

%macro get_authentication_token(GT_TENANT_ID=,GT_SECRET_KEY=);

	data _null_;
		length encHeader encPayload $2000;
		header='{"alg":"HS256","typ":"JWT"}';
		payload='{"clientID":"' || strip(symget("GT_TENANT_ID")) || '"}';
		encHeader  = compress(translate(put(strip(header),$base64x64.), '-_', '+/'), '=');
		encPayload = compress(translate(put(strip(payload),$base64x64.), '-_', '+/'), '=');
		key=put(strip(symget("GT_SECRET_KEY")),$base64x100.);
		digest=sha256hmachex(strip(key),catx(".",encHeader,encPayload), 0);
		encDigest=translate(put(input(digest,$hex64.),$base64x100.), "-_ ", "+/=");
		token=catx(".", encHeader,encPayload,encDigest);
		call symputx("AUTH_TOKEN",token,'G');
	run;

%mend get_authentication_token;

%macro get_temporary_token(APIUSR,APIUPW);
	filename outfile temp;
	filename outhd temp;

	proc http
		method="GET"  TIMEOUT=20 
		url="&STP_AUD_GATEWAY./token"
		ct='application/x-www-form-urlencoded'
		QUERY= ("username"="&APIUSR." "password"="%superq(APIUPW)" "grant_type"="password")
		headerout=outhd
		%if %symexist(STP_proxyhost) %then %do;
			proxyhost="&STP_proxyhost."
			proxyport=&STP_proxyport.
			proxyusername="&STP_proxyuser."
			proxypassword="&STP_proxypw."
		%end;

	out=outfile;
	headers "Authorization" = "Bearer %superq(AUTH_TOKEN)"
		"Content-Type"="application/x-www-form-urlencoded";
	/*	debug level =  2;  */
	run;

	%if &SYS_PROCHTTP_STATUS_CODE. ne 200 %then %do;
		%let STOP_AUDIENCE_PROCESS=1;
		data _null_;
			infile outfile;
			input;
			put _infile_;
		run;
	%end;

	libname outfile JSON;

	data _null_;
		set outfile.root;
		call symputx("TEMP_TOKEN", access_token,'G');
	run;

	libname outfile;


%mend;

/*CHECKING STATUS OF UPLOADING AUDIENCE*/
%macro check_upload();
	%let retryWaitSec=60;

	/*Configuring parameters for attemps*/
	data _null_;
		maxRetryAttempts=(&STP_AUD_VAL_MINUTES.*60)/&retryWaitSec.;
		call symput('maxRetryAttempts',maxRetryAttempts);
	run;

	%let retryAttemptNo=0;
	%let runningstatus= QUERY_SUBMITTED FILE_SUBMITTED QUERY_STARTING SEGMENT_COMPLETE QUERY_RUNNING SEGMENT_RUNNING FILE_PROCESSING UPLOADING_QUERY_RESULTS QUERY_COMPLETE FILE_PROCESSING_COMPLETE IDENTITY_RESOLUTION_SUBMITTED IDENTITY_RESOLUTION_STARTED IDENTITY_RESOLUTION_COMPLETE SEED_COMPLETE;

%UPLOADTRYAGAIN:

	/*Getting status details*/
	filename resp temp;

	proc http
		method="GET" ct="application/json" timeout=10
		url="&STP_AUD_GATEWAY./marketingAudience/audiences/&audience_id./history/&historyId."
		%if %symexist(STP_proxyhost) %then %do;
			proxyhost="&STP_proxyhost."
			proxyport=&STP_proxyport.
			proxyusername="&STP_proxyuser."
			proxypassword="&STP_proxypw."
		%end;

	out=resp;
	headers "Authorization" = "Bearer %superq(TEMP_TOKEN)";

	/*debug level=3;*/
	run;

	libname resp JSON;

	data _null_;
		set resp.root;
		call symputx("status_upload", strip(status));
	run;

	%put &=status_upload;

	/*Validating status and calling again for status info if needed*/
	%if (&status_upload. eq COMPLETE ) %then %do;
		%put INFO: Audience uploaded;
	%end;
	%else %do;
		%if %sysfunc(FINDW(%upcase(%superq(runningstatus)), %upcase(&status_upload))) %then %do;
			%if &retryAttemptNo. < &maxRetryAttempts. %then %do;
				%let RetryAttemptNo=%sysevalf(&RetryAttemptNo + 1);

				data _null_;
					call sleep(&retryWaitSec.,1);
				run;

				%put NOTE: Uploading Audiences > Trying again. Attemp: &retryAttemptNo. for status: &status_upload;
				%goto UPLOADTRYAGAIN;
			%end;
			%else %do;
				%put NOTE: Uploading Audiences > sending Email, Audience process is running;
				filename outbox email &STP_AUD_EMAIL_FROM.;

				data _null_;
					file outbox
						from=(&STP_AUD_EMAIL_FROM.)
						to=(&STP_AUD_EMAIL_LIST.)

						subject="[SAS CI 360] - Audience Process is running for audience &audience_name.";
					put '!em_importance! high';
					put "Uploading audience is still running for audience &audience_name.. Please validate status in CI 360 > Targeting > Audiences";
					put " ";
				run;

			%end;
		%end;
		%else %do;
			%put ERROR: Uploading Audiences > Process failed uploading audience with status &status_upload.;
			%let abort_process=1;
		%end;
	%end;
%mend;

/*VALIDATING AUDIENCE ATTRIBUTES AND MISSGIN VALUES*/
%macro ValidationFields();
	%get_authentication_token(GT_TENANT_ID=&STP_AUD_TENANT_ID., GT_SECRET_KEY=&STP_AUD_CLIENT_SECRET.);
	%get_temporary_token(APIUSR=&STP_AUD_API_USER., APIUPW=&STP_AUD_API_PW.);
	%check_status_process(stop=&STOP_AUDIENCE_PROCESS.,msg=Process failed retrieving access token);

	%if &abort_process. =1 %then
		%goto exit_validation;

	/*Getting columns defined in audience*/
	filename addd temp;
	filename head temp;

	proc http
		method="GET"
		url="&STP_AUD_GATEWAY./marketingAudience/audiences/&audience_id."
		out=addd
		%if %symexist(STP_proxyhost) %then %do;
			proxyhost="&STP_proxyhost."
			proxyport=&STP_proxyport.
			proxyusername="&STP_proxyuser."
			proxypassword="&STP_proxypw."
		%end;

		headerout=head;
		headers "Authorization" = "Bearer %superq(TEMP_TOKEN)";

	/*debug level=3;*/
	run;

	%if &SYS_PROCHTTP_STATUS_CODE. ne 200 %then
		%let STOP_AUDIENCE_PROCESS=1;

	%check_status_process(stop=&STOP_AUDIENCE_PROCESS.,msg=%str(Process aborted. Error retrieving audience attributes));

	%if &abort_process. =1 %then
		%goto exit_validation;

	libname addd json;

	/*retrieving identity column*/
	data _null_;
		set addd.alldata;

		if upcase(p1) eq "IDENTITYCOLUMNNAME" then
			call symput('identColumn',strip(value));
	run;

	data attributes(keep=name datatype columnnumber);/*table contains name and datatype of each attribute*/
		retain columnnumber 0;
		set addd.dataitems(rename=(name=name1));

		if upcase(strip(name1)) eq upcase("CONTEXT_TYPE") then delete; /*support of context type */
			columnnumber+1;
			name=upcase(strip(label));

			if upcase(name1) eq upcase("&identColumn.") then
				call symput('identName',strip(label));
	run;

	data _null_;
		set addd.root;
		call symput('audience_name',strip(name));
	run;

	/*Getting exported data set */
	data _null_;
		set macrovar;

		if category eq 'EXPORTINFO' then do;
			if name eq 'EXPORTOUTPUTNAME' then
				call symput('table',strip(value));

			if name eq 'EXPORTOUTPUTPATH' then
				call symput('lib',strip(value));
		end;
	run;

	proc contents data=&lib..&table. out=_outc noprint;
	run;

	data _outc;
		set _outc(rename=(name=name1));
		name=upcase(strip(name1));
	run;

	/*Validating columns from audience vs columns from DM Task output*/
	proc sql noprint;
		create table NotInAud as select name from attributes where name not in (select distinct name from _outc);
	quit;

	proc sql noprint;
		create table NotInDM as select name from _outc where name not in (select distinct name from attributes);
	quit;

	proc sql noprint;
		create table okAudDM as select name, type from _outc where name  in (select distinct name from attributes);
	quit;

	/*Forcing error due to missing audience attributes*/
	data _null_;
		set NotInAud end=eof;
		format variables $1000.;
		retain variables;
		variables=strip(name)||' '||strip(variables);

		if eof then do;
			call symput('msgerror', "There are missing audience attributes in DM Export: " ||variables);
			call symput('STOP_AUDIENCE_PROCESS', 1);
		end;
	run;

	%check_status_process(stop=&STOP_AUDIENCE_PROCESS.,msg=&msgerror.);

	%if &abort_process. =1 %then
		%goto exit_validation;

	data _null_;
		set NotInDM end=eof;
		format variables $1000.;
		retain variables;
		variables=strip(name)||' '||strip(variables);

		if eof then do;
			put "NOTE: Uploading Audiences > list of variables not included in the audience: " variables;
		end;
	run;

	/*Validation of missing values */
	%if "&uploadmissval." eq "N" %then %do;
		%let namesStr=;
		%let namesnum=;

		data _null_;
			set okAudDM;

			if type eq 2 then
				call symput('namesStr',strip(symget('namesStr')||" "||strip(name)));

			if type eq 1 then
				call symput('namesNum',strip(symget('namesNum')||" "||strip(name)));
		run;

		%let _namesStr=%sysfunc(coalescec(%superq(namesStr),));
		%let _namesNum=%sysfunc(coalescec(%superq(namesNum),));

		data missingvalues;
			set &lib..&table. end=eof;
			length missvars $ 1055;

			%if %length(&_namesStr) %then %do;
				array varc (*) &_namesStr.;

				do i=1 to dim(varc);
					if missing(varc[i]) then
						missvars=catx(',',missvars,vname(varc[i]));
				end;
			%end;

			%if %length(&_namesNum) %then %do;
				array varn (*) &_namesNum.;

				do i=1 to dim(varn);
					if missing(varn[i]) then
						missvars=catx(',',missvars,vname(varn[i]));
				end;
			%end;

			drop i;

			if not missing(missvars) then do;
				stopProcess+1;
			end;

			if eof then do;
				if stopProcess gt 0 then do;
					call symput('abortPr','Y');
				end;
				else do;
					call symput('abortPr','N');
				end;
			end;
		run;

		%if "&abortPr" eq "Y" %then %do;

			proc freq data= missingvalues noprint;
				tables missvars/ out=t1(where=(missvars <> ''));
			run;

			data _null_;
				set t1 end=eof;
				format listMissVars $1000.;
				retain listMissVars;
				listMissVars=catx(',',listMissVars,missvars);

				if eof then do;
					call symput('msgerror', "Process aborted. Missing values for variables: " ||listMissVars);
					call symput('STOP_AUDIENCE_PROCESS', 1);
				end;
			run;

			%check_status_process(stop=&STOP_AUDIENCE_PROCESS.,msg=%str(&msgerror.));

			%if &abort_process. =1 %then
				%goto exit_validation;
		%end;
	%end;
	%else %do;
		%put NOTE: Uploading Audiences > Missing values allowed;
	%end;

	/*Creating csv file if validations are ok*/
	%let namesAud=;

	proc sort data=attributes;
		by columnnumber;
	run;

	data _null_;
		set attributes;
		call symput('namesAud',strip(symget('namesAud')||" "||strip(name)));
	run;

	proc sql noprint;
		create table checkingduplicates as select distinct &identName., count(*) as nrows
			from &lib..&table.
				group by 1
					order by 2 desc;
	quit;

	data checkingduplicates1;
		set checkingduplicates;

		if _N_ eq 1 then do;
			if nrows gt 1 then do;
				put "NOTE: Uploading Audiences > There are duplicate rows for &identName.. Only a row will be upload per &identName..";
			end;
		end;
	run;

	data CreateCsv;
		set &lib..&table.(keep=&namesAud.);
		by &identName.;

		if first.&identName.;
	run;

	/*Validation of erros when creating the csv file*/
	%if &syserr. gt 0 %then
		%let STOP_AUDIENCE_PROCESS=1;

	%check_status_process(stop=&STOP_AUDIENCE_PROCESS.,msg=%str(Process aborted. &SYSERRORTEXT.));

	%if &abort_process. =1 %then
		%goto exit_validation;

	filename aud_CSV temp;

	data _null_;
		set CreateCsv;
		file aud_CSV dsd lrecl=32767;
		put &namesAud.;
	run;

%exit_validation:
%mend;

/* Executing ipload process */
%macro UploadAud();
	/* Executing macro to validate data */
	%ValidationFields;
	%log_error_vars;
	%if &abort_process. =1 %then
		%goto exit_upload;

	/* Call to obtain signed url*/
	filename resp temp;
	filename head temp;

	proc http
		method="POST" ct="application/json" timeout=10
		url="&STP_AUD_GATEWAY./marketingAudience/audiences/fileTransferLocation"
		out=resp
		%if %symexist(STP_proxyhost) %then %do;
		proxyhost="&STP_proxyhost."
			proxyport=&STP_proxyport.
			proxyusername="&STP_proxyuser."
			proxypassword="&STP_proxypw."
	%end;

	headerout=head;
	headers "Authorization" = "Bearer %superq(TEMP_TOKEN)";
	run;
	%log_error_vars;
	libname resp JSON;

	data _null_;
		set resp.root;
		call symputx("Quoted_SignedURL", "'"||signedURL||"'");
	run;

	libname resp;

	%if &SYS_PROCHTTP_STATUS_CODE. ne 200 %then
		%let STOP_AUDIENCE_PROCESS=1;

	%check_status_process(stop=&STOP_AUDIENCE_PROCESS.,msg=%str(Process aborted. Error creating signed URL));
	%log_error_vars;
	%if &abort_process. =1 %then
		%goto exit_upload;

	/* Upload CSV file  */
	filename response TEMP;
	filename headout TEMP;

	proc http
		method="PUT" timeout=900
		url=&Quoted_SignedURL.
		in=aud_csv
		out=response
		%if %symexist(STP_proxyhost) %then %do;
		proxyhost="&STP_proxyhost."
			proxyport=&STP_proxyport.
			proxyusername="&STP_proxyuser."
			proxypassword="&STP_proxypw."
	%end;

	headerout=headout;
	;
	run;

	%if &SYS_PROCHTTP_STATUS_CODE. ne 200 %then
		%let STOP_AUDIENCE_PROCESS=1;

	%check_status_process(stop=&STOP_AUDIENCE_PROCESS.,msg=%str(Process aborted. Error uploading csv files));
	%log_error_vars;
	%if &abort_process. =1 %then
		%goto exit_upload;

	/* Run the audience */
	filename requ temp;

	data _null_;
		file requ;
		SignedURL=&Quoted_SignedURL;
		put "{""name"": ""&AUDIENCE_NAME.""," @;
		put " ""fileLocation"": """ SignedURL +(-1) """," @;
		put " ""headerRowIncluded"":false," @;
		put " ""audienceId"":""&audience_id.""}";
	run;
	%log_error_vars;
	filename resp temp;

	proc http
		method="PUT" ct="application/json" timeout=180
		url="&STP_AUD_GATEWAY./marketingAudience/audiences/&audience_id./data"
		in=requ
		out=resp
		%if %symexist(STP_proxyhost) %then %do;
			proxyhost="&STP_proxyhost."
			proxyport=&STP_proxyport.
			proxyusername="&STP_proxyuser."
			proxypassword="&STP_proxypw."
		%end;

		headerout=head;
		headers "Authorization" = "Bearer %superq(TEMP_TOKEN)";
	run;
	%log_error_vars;
	%if %sysfunc(FINDW((200 202), &SYS_PROCHTTP_STATUS_CODE.)) =0 %then
		%let STOP_AUDIENCE_PROCESS=1;

	%check_status_process(stop=&STOP_AUDIENCE_PROCESS.,msg=%str(Process aborted. Error running audience));
	%log_error_vars;
	%if &abort_process. =1 %then
		%goto exit_upload;

	libname resp JSON;

	data _null_;
		set resp.root;
		call symputx("historyId", historyId);
	run;

	%check_upload();
%exit_upload:
%mend;

%UploadAud();
%log_error_vars;
%forcingerror();
%log_error_vars;

%if %index(%bquote(&_CLIENTAPP), %str(Enterprise Guide))=0 and %index(%bquote(&_CLIENTAPP), %str(Studio))=0 %then %do;
	%MACount(&inTable.);
	%MAStatus(&_stpwork.status.txt);

	proc printto;
	run;

	%stpend;
%end;