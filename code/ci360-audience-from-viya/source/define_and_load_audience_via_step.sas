/*-----------------------------------------------------------------------------
 Copyright © 2024, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 SPDX-License-Identifier: Apache-2.0
-----------------------------------------------------------------------------*/
/*************************************/
/* Step 1: GLOBAL MACRO VARIABLES    */
/*************************************/
%let External_gateway=https://extapigwservice-training.ci360.sas.com;
%let proxy_host=; 
%let proxy_port=;
%let proxy_auth=;
%let TENANT_ID=;
%let CLIENT_SECRET=%nrstr(xxxxxxxxxxxx);
%let API_USER=;
%let API_PASSWORD=;
/*%let AUDIENCE_NAME = UNIQUE NAME;*/
%let AUDIENCE_DEBUG=0; /* 0 or 1 where 1 increases the log level */
%let STOP_PROCESS=0; /* Resets error flag for reruns */

/* Get tenant connection details */
%if %sysfunc(exist(WORK.USER_PROFILES)) %then %do;
	PROC SQL noprint;
		SELECT External_gateway, TENANT_ID, CLIENT_SECRET, API_USER, API_PASSWORD
		INTO :External_gateway trimmed, :TENANT_ID trimmed, :CLIENT_SECRET trimmed, :API_USER trimmed, :API_PASSWORD trimmed
		FROM work.user_profiles
	    WHERE upcase(profile)=upcase("&profile");
	quit;
%end;
%if &AUDIENCE_DEBUG %then %do;
	%put &=TENANT_ID;
%end;
/*options spool;*/


/*************************************/
/* Step 2: REUSED MACROS             */
/*************************************/
options nomprint nomlogic nosymbolgen; /* Operational log level */
%if &AUDIENCE_DEBUG %then %do;
    options mprint mlogic symbolgen; /* Debug log level */
	%put _user_;
%end;

%macro dsc_httprequest(	infile=, outfile=,
						headerin=, headerout=,
						requestCT=, requestURL=, requestMethod=,
						requestTIMEOUT=, requestQUERY=,
						service_auth=,
						proxy_host=, proxy_port=, proxy_auth=
						);
	/* disable quote warnings */
	options NOQUOTELENMAX;
    proc http 
		method="&requestMethod" 
		%if "%substr(%str(&requestURL),1,1)"="'" 
			%then %do; url=&requestURL %end;
			%else %do; url="%superq(requestURL)" %end;
		%if &requestQUERY ne %then %do; QUERY=&requestQUERY %end;
		%if &requestCT ne %then %do; ct="&requestCT ; charset=utf-8" %end;
		%if &requestTIMEOUT ne %then %do; TIMEOUT=&requestTIMEOUT %end;
		%if &infile    ne %then %do; in=&infile  %end;
		%if &outfile   ne %then %do; out=&outfile %end;
		%if &headerin  ne %then %do; headerin=&headerin %end;
		%if &headerout ne %then %do; headerout=&headerout HEADEROUT_OVERWRITE %end;
		/* need proxy ? */
		%if (%length(&proxy_host) > 0 and %length(&proxy_port) > 0) %then
		%do;
			proxyhost="&proxy_host"
			proxyport=&proxy_port
			%if &proxy_auth ne %then %do; &proxy_auth %end;
		%end;
		;
		%if &service_auth ne %then %do; &service_auth ; %end;
/* 			debug level=3; */
    run;

    %* Check proc http execution status;
    %if &SYSERR. > 4 %then 
    %do;
       	%put &SYSERRORTEXT. ;
		%put ERR%str()OR: Err%str()or in executing proc http call;		
		%let  STOP_PROCESS=1;
		%let fileRefs=&headerin &headerout &outfile;
		%* Echo the file contents to log if file exists;
		%if %length(&fileRefs) > 0 %then
		%do;
			%let i=1;
			%do %while (%scan(&fileRefs,&i,' ') ne );
				%let fileRef=%scan(&fileRefs,&i);
				/* if the fileref & its associated file exists? */
				%if %sysfunc(fileref(&fileRef)) = 0 %then
				%do;				
					data _null_;			
						length linetxt $32767; 
						if _n_ = 1 then 
						do;
	/*						fileHeader=sasmsg("&msg_dset","_cxa_norm_19_note","noquote","&fileRef");*/
	/*						put fileHeader;*/
						end;
						infile &fileRef. length=reclen ; 
						input linetxt $varying32767. reclen ;
						put linetxt;
					run;
				%end;
				%let i=%eval(&i+1);
			%end;/* %do %while */
		%end;/*%if %length(&fileRefs)*/		
   	%end;
%mend dsc_httprequest;

%macro get_authentication_token(TENANT_ID,SECRET_KEY);
     data _null_;
          header='{"alg":"HS256","typ":"JWT"}';
          payload='{"clientID":"' || strip(symget("TENANT_ID")) || '"}';
          encHeader =translate(put(strip(header ),$base64x64.), "-_ ", "+/=");
          encPayload=translate(put(strip(payload),$base64x64.), "-_ ", "+/=");
          key=put(strip(symget("SECRET_KEY")),$base64x100.);
          digest=sha256hmachex(strip(key),catx(".",encHeader,encPayload), 0);
          encDigest=translate(put(input(digest,$hex64.),$base64x100.), "-_ ", "+/=");
          token=catx(".", encHeader,encPayload,encDigest);
          call symputx("AUTH_TOKEN",token,'G');
     run;
%mend get_authentication_token;

%get_authentication_token(TENANT_ID=&TENANT_ID, SECRET_KEY=&CLIENT_SECRET);
filename tokenout temp;
%dsc_httprequest(	requestCT=%str(application/x-www-form-urlencoded), 
					requestURL=&External_gateway./token, 
					requestMethod=GET,
					requestTIMEOUT=20,
					requestQUERY=%str(("username"="&API_USER." "password"="&API_PASSWORD." "grant_type"="password")),
					outfile=tokenout,
					service_auth= %str(headers "Authorization" = "Bearer &AUTH_TOKEN."),
					proxy_host=&proxy_host, proxy_port=&proxy_port, proxy_auth=&proxy_auth
					);	 	
libname tokenout JSON;
%global length_temp_token; %let length_temp_token=0;
data _null_;
	set tokenout.root;	
	call symputx("TEMP_TOKEN", access_token,'G'); 
	call symputx("length_temp_token", length(access_token),'G'); 
run;
libname tokenout;

/* Get token and temporary token for tenant */
%if &AUDIENCE_DEBUG %then %do;
	%put &=TEMP_TOKEN;
%end;
%if &length_temp_token.=0 %then %do;
	%put ERR%str()OR: Temporary access token is not generated. Check URL for &External_gateway. or internet access.;
	%let STOP_PROCESS=1;
%end;

/* clear tenant connection details */
%let TENANT_ID=;
%let CLIENT_SECRET=;
%let AUTH_TOKEN=;
%let API_USER=;
%let API_PASSWORD=;

/* Check if audience already exists */
%if &STOP_PROCESS=0 %then %do;
	filename aud_out temp;
	%dsc_httprequest(requestCT=%str(application/json), 
					requestURL=&External_gateway./marketingAudience/audiences?name=&Audience_name, 
					requestMethod=GET,
					requestTIMEOUT=20,
					outfile=aud_out,
					service_auth= %str(headers "Authorization" = "Bearer &TEMP_TOKEN."),
					proxy_host=&proxy_host, proxy_port=&proxy_port, proxy_auth=&proxy_auth
					);	 
	/* Response in case of a new audience: {"name":"items","start":0,"count":0,"items":[],"limit":10,"version":2}	*/
	/* Response when audience API is not available: {"timestamp":1710342618576,"status":404,"error":"Not Found","path":"/marketingAudience/audiences"}*/
	libname aud_out JSON;
	data _null_;
		set aud_out.root;
		call symputx('audience_exists',count);
		call symputx('api_response_status',status);
	run;
	%put &=audience_exists;
%end;
%if &AUDIENCE_DEBUG %then %do;
	data _null_; infile aud_out; input; put _infile_; run; 
%end;

%if &audience_exists = . and &api_response_status ne . and &STOP_PROCESS=0 %then %do;
	%put ERR%str()OR: Audience API response code is &api_response_status.;
	data _null_; infile aud_out; input; put _infile_; run; 
	%let audience_exists=0;
	%let STOP_PROCESS=1;
%end;

/* Get the audience ID if it exists */
%if &audience_exists and &STOP_PROCESS=0 %then %do;
	data _null_;
		set aud_out.items;
		call symputx('audience_id',audienceId);
	run;
	%put &=audience_id;
%end;
libname aud_out;


/* if Audience exists, the columns so the CSV follows in the same order */
%if &audience_exists and &STOP_PROCESS=0 %then %do;
	filename aud_out temp;
	%dsc_httprequest(requestCT=%str(application/json), 
					requestURL=&External_gateway./marketingAudience/audiences/&audience_id, 
					requestMethod=GET,
					requestTIMEOUT=20,
					outfile=aud_out,
					service_auth= %str(headers "Authorization" = "Bearer &TEMP_TOKEN."),
					proxy_host=&proxy_host, proxy_port=&proxy_port, proxy_auth=&proxy_auth
					);	 
	libname aud_out JSON;
	data _null_;
	 	set aud_out.root;
        if audienceType ne "upload"
		then do;
			put "ERR%str()OR: The audience " name "cannot be loaded as it is connected to a " source "cloud database.";
			call symputx('STOP_PROCESS','1','G');
		end;
	run;
%end;
%if &audience_exists and &AUDIENCE_DEBUG %then %do;
 	data _null_; infile aud_out; input; put _infile_; run;  
%end;

%let JSON_OBS=0;
%let MATCHED_OBS=X;
%if &audience_exists and &STOP_PROCESS=0 %then %do;
	%let same_csv_col_order=;
	proc contents data=&inputtable1 out=content_inputtable1(keep=name label type varnum) noprint; 
	run;
	data dataitems;
	 	set aud_out.dataitems;
		json_order=_n_;
	run;
	%let JSON_OBS=&SYSNOBS;

	libname aud_out;
	proc sql;
		create table Columnmatcher as 
		select json.json_order
			, json.name as json_name
			, json.label as audience_label
			, json.dataType as dataType
			, cont.label as input_label
			, cont.name as input_name
		from dataitems json
		inner join content_inputtable1 cont
		on cont.name=json.label or (cont.name ne json.label and cont.label=json.label)
		order by json_order
		;
	quit;
	%let MATCHED_OBS=&SYSNOBS;
%end;

%if &audience_exists and &STOP_PROCESS=0 and &JSON_OBS NE &MATCHED_OBS %THEN %DO;
	data _null_;
		put "ERR%str()OR: The input table needs to have a column with matching name or label for each attrbute in the audience. Extra input columns will be ignored.";
		call symputx('STOP_PROCESS','1','G');
	run;
	title "Input table columns";
	PROC PRINT DATA=content_inputtable1; var name label; run;
	title "Audience attributes";
	footnote "The input table needs to have a column with matching name or label for each attrbute in the audience. Extra input columns will be ignored";
	PROC PRINT DATA=dataitems; var label; run;
%end;

	
%if &audience_exists and &STOP_PROCESS=0  %THEN %DO;
	data _null_;
		length same_csv_col_order $360;
	    retain same_csv_col_order;
	 	set Columnmatcher;
	    same_csv_col_order=catx(' ',strip(same_csv_col_order),input_name);
		call symputx('same_csv_col_order',same_csv_col_order);
	run;
	%put &=same_csv_col_order;
%end;

/* For New audiences, create the audience based on the parameter values */
%macro audience_dataitems;
	%do i=1 %to &columnSelector1_count;
		if colNum > 0 then put ',' @;
	    colNum+1;
 	    put '{"columnNumber":' colNum +(-1) ',' @;
	    put '"label":"' @;
		%if "&&columnSelector1_&i._label"="" %then %do;
			put "&&columnSelector1_&i._name" '",' @;
		%end;
		%else %do;
			put "&&columnSelector1_&i._label" '",' @;
		%end;
		%if %index(&&columnSelector1_&i._format,E8601DA) %then %do;
			put '"dataType":"date",' @;
		%end;
		%else %if %index(&&columnSelector1_&i._format,E8601DT) %then %do;
			put '"dataType":"datetime",' @;
		%end;
		%else %if "&&columnSelector1_&i._type"="Numeric" %then %do;
			put '"dataType":"numerical",' @;
		%end;
		%else %do;
			put '"dataType":"' "&&columnSelector1_&i._type" '",' @;
		%end;
	    put '"usedForTracking":false}';
		same_csv_col_order=catx(' ',strip(same_csv_col_order),"&&&columnSelector1_&i._name_base");
		putlog same_csv_col_order=;
	%end;
%mend;

%macro create_audience_definition;
	filename aud_json temp;
	data _null_;
	    length same_csv_col_order $360;
		file aud_json;
		put '{"name": "' "&Audience_name"  '",' @;
	    put '"description": "An Audience created in SAS Studio Viya",';
	    put '"source": "API Audience",' @;
	    put '"iconName": "SasViya",' @;
	    put '"expiration": ' "&Expiration" ',';
	    put '"identityType": "' "&Identity_type" '",' @;
	    put '"identityColumnName": "' "&Identity_column" '",' @;
		%if "&email_column" ne "" %then %do;
	    	put '"emailColumnName": "' "&email_column" '",';
		%end;
	    put '"dataItems": [';
		/* Add Identity_column if not selected as an attributed */
		colNum=0;
		if findw("&columnSelector1","&Identity_column", ' ', 'i')=0 then do;
			if colNum > 0 then put ',' @;
		    colNum+1;
		    put '{"columnNumber":' colNum +(-1) ',' @;
		    put '"label":"' "&Identity_column" '",' @;
		    put '"dataType":"character",' @;
		    put '"usedForTracking":false}';
			same_csv_col_order=catx(' ',strip(same_csv_col_order),"&Identity_column");
		end;
		/* Add email_column if specified and not selected as an attributed */
		if "&email_column" ne "" and findw("&columnSelector1","&email_column", ' ', 'i')=0 then do;
		if colNum>0 then put ',' @;
		    colNum+1;
		    put '{"columnNumber":' colNum +(-1) ',' @;
		    put '"label":"' "&email_column" '",' @;
		    put '"dataType":"character",' @;
		    put '"usedForTracking":false}';
			same_csv_col_order=catx(' ',strip(same_csv_col_order),"&email_column");
		end;
		%audience_dataitems;
		put']}'; 
		call symputx('same_csv_col_order',same_csv_col_order);
	run;

	filename aud_out temp;
	%dsc_httprequest(requestCT=%str(application/json), 
					requestURL=&External_gateway./marketingAudience/audiences, 
					requestMethod=POST,
					requestTIMEOUT=10,
					infile=aud_json,
					outfile=aud_out,
					service_auth= %str(headers "Authorization" = "Bearer &TEMP_TOKEN."),
					proxy_host=&proxy_host, proxy_port=&proxy_port, proxy_auth=&proxy_auth
					);	 
	%if &AUDIENCE_DEBUG %then %do;
		 data _null_; infile aud_json; input; put _infile_; run; 
		 data _null_; infile aud_out; input; put _infile_; run; 
	%end;

	/* get the audience ID back */
	libname aud_out JSON;
	data _null_;
		set aud_out.root;	
		call symputx('audience_id',audienceId);
	run;
	%put &=audience_id;
	libname aud_out;
%mend create_audience_definition;


%if &audience_exists=0 and &STOP_PROCESS=0 %then %do;	
	%let same_csv_col_order=;
	%create_audience_definition;
%end;

/* create CSV without header row and with quoted values */
%if &STOP_PROCESS=0 %then %do;	
	filename aud_csv temp;
	Data _null_;   
	   file aud_csv dsd dlm=',';
	   set &inputtable1;
	   put (&same_csv_col_order) (~);
	run;
%end;
%if &STOP_PROCESS=0 and &AUDIENCE_DEBUG %then %do;	
	data _null_; infile aud_csv; input; put _infile_; run;	
%end;

	
/* Generate the signed URL and upload > POST WITH NO BODY */
%if &STOP_PROCESS=0 %then %do;	
	filename resp temp;
	filename head temp;
	%dsc_httprequest(requestCT=%str(application/json), 
					requestURL=&External_gateway./marketingAudience/audiences/fileTransferLocation, 
					requestMethod=POST,
					requestTIMEOUT=10,
					outfile=resp,
					headerout=head,
					service_auth= %str(headers "Authorization" = "Bearer &TEMP_TOKEN."),
					proxy_host=&proxy_host, proxy_port=&proxy_port, proxy_auth=&proxy_auth
					);	 
	libname resp JSON;
	data _null_;
		set resp.root;	
	   call symputx("Quoted_SignedURL", "'"||signedURL||"'") ; 
	run;
	libname resp;
%end;
%if &STOP_PROCESS=0 and &AUDIENCE_DEBUG %then %do;	
	data _null_; infile head; input; put _infile_; run;
	data _null_; infile resp; input; put _infile_; run;
	%put &=Quoted_SignedURL; 
%end;


/* upload CSV file */
%if &STOP_PROCESS=0 %then %do;	
	filename response TEMP;
	filename headout TEMP;
	%dsc_httprequest(requestURL=&Quoted_SignedURL, 
					requestMethod=PUT,
					requestTIMEOUT=900,
					infile=aud_csv,
					outfile=response,
					headerout=headout,
					proxy_host=&proxy_host, proxy_port=&proxy_port, proxy_auth=&proxy_auth
					);	 
%end;
%if &STOP_PROCESS=0 and &AUDIENCE_DEBUG %then %do;	
	data _null_; infile response; input; put _infile_; run;
	data _null_; infile headout; input; put _infile_; run;
%end;


/* Process the uploaded file = Run the audience occurrence */
%if &STOP_PROCESS=0 %then %do;	
	filename requ temp;
	data _null_;
	file requ;
		SignedURL=&Quoted_SignedURL;
		put "{""name"": ""&audience_name.""," @;
		put " ""fileLocation"": """ SignedURL +(-1) """," @;
		put " ""headerRowIncluded"":false," @;
		put " ""audienceId"":""&audience_id.""}";
	run;
	
	filename resp temp;
	%dsc_httprequest(requestCT=%str(application/json), 
					requestURL=&External_gateway./marketingAudience/audiences/&audience_id./data, 
					requestMethod=PUT,
					requestTIMEOUT=60,
					infile=requ,
					outfile=resp,
					headerout=head,
					service_auth= %str(headers "Authorization" = "Bearer &TEMP_TOKEN."),
					proxy_host=&proxy_host, proxy_port=&proxy_port, proxy_auth=&proxy_auth
					);	 
	
	libname resp JSON;
	proc sql;
	 select p1 as Name, Value from resp.alldata; 
	quit;
	data &outputtable1;
		set resp.root;
	run;
%end;
%if &STOP_PROCESS=0 and &AUDIENCE_DEBUG %then %do;	
	data _null_; infile requ; input; put _infile_; run;
	data _null_; infile head; input; put _infile_; run;
	data _null_; infile resp; input; put _infile_; run;
%end;
