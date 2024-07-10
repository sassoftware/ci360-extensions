/*-----------------------------------------------------------------------------
 Copyright © 2024, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 SPDX-License-Identifier: Apache-2.0
-----------------------------------------------------------------------------*/

/*************************************/
/* Step 1: GLOBAL MACRO VARIABLES    */
/*************************************/
%let External_gateway=https://extapigwservice-training.ci360.sas.com;
%let TENANT_ID=;
%let CLIENT_SECRET=%nrstr(xxxxxxxxxxxx);
%let API_USER=;
%let API_PASSWORD=;
%let AUDIENCE_NAME = UNIQUE NAME;
%let AUDIENCE_DEBUG=0; /* 0 or 1 where 1 increases the log level */
%let STOP_AUDIENCE_PROCESS=0; /* Reset error flag for reruns */



/*************************************/
/* Step 2: Generate temporary token  */
/*************************************/
options nomprint nomlogic nosymbolgen; /* Operational log level */
%if &AUDIENCE_DEBUG %then %do;
    options mprint mlogic symbolgen; /* Debug log level */
	%put _user_;
%end;

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
%macro get_temporary_token(APIUSR,APIUPW);
     filename outfile temp;
     proc http 
          method="GET" ct="application/x-www-form-urlencoded" TIMEOUT=20
          url="&External_gateway./token"
          QUERY=("username"="&APIUSR." "password"="&APIUPW." "grant_type"="password")
          out=outfile; 
          headers "Authorization" = "Bearer &AUTH_TOKEN.";
     run;
	libname outfile JSON;
	data _null_;
		set outfile.root;	
	   call symputx("TEMP_TOKEN", access_token,'G'); 
	run;
	libname outfile;
%mend;

/* Get token and temporary token for tenant */
%get_authentication_token(TENANT_ID=&TENANT_ID, SECRET_KEY=&CLIENT_SECRET);
%get_temporary_token(APIUSR=&API_USER, APIUPW=&API_PASSWORD);
%if %sysfunc(fexist(outfile)) %then %do;              
	title  'Step 2: Generate temporary token';
	data _null_; infile outfile; input; temp_token_response=_infile_;file print ods; put _ods_; run;
%end;

/* clear tenant connection details */
%let TENANT_ID=;
%let CLIENT_SECRET=;
%let AUTH_TOKEN=;
%let API_USER=;
%let API_PASSWORD=;



/*************************************/
/* Step 3: Create CSV                */
/*************************************/
filename aud_CSV temp;
data _null_;
	file aud_CSV;
	do i=1 to 20;
		age=20+mod(i,80);
		put '"id' i +(-1) '","Name' i +(-1) '","' age +(-1) '"';
	end;
run;
%if &AUDIENCE_DEBUG %then %do;
	data _null_; infile aud_CSV; input; put _infile_; run; 
%end;
%if %sysfunc(fexist(aud_CSV)) %then %do;
	title  'Step 3: Create CSV';
	data _null_; infile aud_CSV; input; csv=_infile_;file print ods; put _ods_; run;
%end;



/*******************************************/
/* Step 4: Create Audience Definition JSON */
/*******************************************/
filename aud_JSON temp;
data _null_;
	file aud_JSON;
	put '{"name": "' "&AUDIENCE_NAME"  '",'; 
	put '"description":  "Audience created in SAS code",';
	put '"source":       "API Audience",';
	put '"iconName":     "SasViya",';
	put '"expiration":   14,';
	put '"identityType": "customer_id",';
	put '"identityColumnName": "id",';
	put '"dataItems": ';
	put '[{"columnNumber":1,"label":"id",  "dataType":"character","usedForTracking":false}';
	put ',{"columnNumber":2,"label":"name","dataType":"character","usedForTracking":false}';
	put ',{"columnNumber":3,"label":"age", "dataType":"numerical","usedForTracking":false}';
	put ']}';
run;
%if &AUDIENCE_DEBUG %then %do;
	data _null_; infile aud_JSON; input; put _infile_; run; 
%end;
%if %sysfunc(fexist(aud_JSON)) %then %do;        
 	title  'Step 4: Create Audience Definition JSON';
	data _null_; infile aud_JSON; input; aud_JSON=_infile_;file print ods; put _ods_; run;
%end;



/*******************************************/
/* Step 5: Create the Audience Definition  */
/*******************************************/
filename aud_crea temp;
proc http
	method="POST" ct="application/json" TIMEOUT=10
	url="&External_gateway./marketingAudience/audiences"
	in=aud_JSON
	out=aud_crea;
	headers "Authorization" = "Bearer &TEMP_TOKEN.";
run;
%if &AUDIENCE_DEBUG %then %do;
	 data _null_; infile aud_JSON; input; put _infile_; run; 
	 data _null_; infile aud_crea;  input; put _infile_; run; 
%end;
title  'Step 5: Create the Audience Definition object';
%if %sysfunc(fexist(aud_crea)) %then %do;                                                                                              
	data _null_; infile aud_crea; input; audience_create_response=_infile_;file print ods; put _ods_; run;
	/* get the audience ID */
	libname aud_crea JSON;
	data _null_;
		set aud_crea.root;	
		call symputx('audience_id',audienceId);
	run;
	%put &=audience_id;
	libname aud_crea;
	title;
%end;
data _null_; audienceId="&audience_id";file print ods; put _ods_; run;



/***********************************/
/* Step 6: Generate the signed URL */
/***********************************/
/* POST call witout JSON body */
filename resp temp;
filename head temp;
proc http 
	method="POST" ct="application/json" timeout=10
	url="&External_gateway./marketingAudience/audiences/fileTransferLocation" 
	out=resp 
	headerout=head;
	headers "Authorization" = "Bearer &TEMP_TOKEN.";
run;
libname resp JSON;
data _null_;
	set resp.root;	
   call symputx("Quoted_SignedURL", "'"||signedURL||"'") ; 
run;
libname resp;
%if &AUDIENCE_DEBUG %then %do;	
	data _null_; infile head; input; put _infile_; run;
	data _null_; infile resp; input; put _infile_; run;
	%put &=Quoted_SignedURL; 
%end;
title 'Step 6: Generate the signed URL';
%if %sysfunc(fexist(head)) %then %do;                                                                                              
	data _null_; infile head; input; Signed_URL_head=_infile_;file print ods; put _ods_; run;
	title;
%end;
%if %sysfunc(fexist(resp)) %then %do;                                                                                              
	data _null_; infile resp; input; Signed_URL_resp=_infile_;file print ods; put _ods_; run;
	title;
%end;
data _null_; Signed_URL="&Quoted_SignedURL";file print ods; put _ods_; run;



/****************************/
/* Step 7: Upload CSV file  */
/****************************/
filename response TEMP;
filename headout TEMP;
proc http 
	method="PUT" timeout=900
	url=&Quoted_SignedURL. 
	in=aud_csv 
	out=response 
	headerout=headout;
run;
%if &AUDIENCE_DEBUG %then %do;	
	data _null_; infile headout; input; put _infile_; run;
	data _null_; infile response; input; put _infile_; run;
%end;
title 'Step 7: Upload CSV file response';
%if %sysfunc(fexist(headout)) %then %do;                                                                                              
	data _null_; infile headout; input; upload_csv_resp_head=_infile_;file print ods; put _ods_; run;
	title;
%end;
%if %sysfunc(fexist(response)) %then %do;                                                                                              
	data _null_; infile response; input; upload_csv_resp=_infile_;file print ods; put _ods_; run;
%end;



/****************************/
/* Step 8: Run the audience */
/****************************/
filename requ temp;
data _null_;
file requ;
	SignedURL=&Quoted_SignedURL;
	put "{""name"": ""&AUDIENCE_NAME.""," @;
	put " ""fileLocation"": """ SignedURL +(-1) """," @;
	put " ""headerRowIncluded"":false," @;
	put " ""audienceId"":""&audience_id.""}";
run;
	
filename resp temp;
proc http
	method="PUT" ct="application/json" timeout=10
	url="&External_gateway./marketingAudience/audiences/&audience_id./data" 
	in=requ
	out=resp 
	headerout=head;
	headers "Authorization" = "Bearer &TEMP_TOKEN.";
run;
	
title 'Step 8: Audience Run response';
libname resp JSON;
proc sql;
 select p1 as Name, Value from resp.alldata; 
quit;
data _null_;
	set resp.root;	
   call symputx("occurrence_Id", occurrenceId) ; 
run;
libname resp;
%if &AUDIENCE_DEBUG %then %do;	
	data _null_; infile requ; input; put _infile_; run;
	data _null_; infile head; input; put _infile_; run;
	data _null_; infile resp; input; put _infile_; run;
	%put &=occurrence_Id;
%end;



/****************************/
/* Step 9: Check the status */
/****************************/
filename resp temp;
proc http
	method="GET" ct="application/json" timeout=10
	url="&External_gateway./marketingAudience/audiences/&audience_id./history/&occurrence_Id." 
	out=resp;
	headers "Authorization" = "Bearer &TEMP_TOKEN.";
run;
%if &AUDIENCE_DEBUG %then %do;	
	data _null_; infile resp; input; put _infile_; run;
%end;
libname resp JSON;
title "Step 9: Status check" ;
proc sql;
 select p1 as Name, Value from resp.alldata; 
quit;



/**********************************************/
/* Step 10: Get the audience ID from the name. */
/*         This is FYI. You already have it.  */
/**********************************************/
filename resp temp;
proc http
	method="GET" ct="application/json" timeout=10
	url="&External_gateway./marketingAudience/audiences?name=&audience_name." 
	out=resp;
	headers "Authorization" = "Bearer &TEMP_TOKEN.";
run;
libname resp JSON;
data _null_;
	set resp.items;	
   call symputx("audience_id", audienceid) ; 
run;
libname resp;
%if &AUDIENCE_DEBUG %then %do;	
	data _null_; infile resp; input; put _infile_; run;
	%put &=audience_id;
%end;
title "Step 10: Verify audience &audience_name." ;
%if %sysfunc(fexist(resp)) %then %do;                                                                                             	
	data _null_; infile resp; input; audience_details=_infile_;file print ods; put _ods_; run;
%end;



/********************************/
/* Step 11: Delete the audience */
/********************************/
filename head temp; 
filename resp temp; 
proc http
	method="DELETE" ct="application/json" timeout=10
	url="&External_gateway./marketingAudience/audiences/&audience_id." 
	out=resp 
	headerout=head;
	headers "Authorization" = "Bearer &TEMP_TOKEN.";
	debug level=3;
run;
title 'Step 11: Delete the audience';
%if %sysfunc(fexist(head)) %then %do;                                                                                             	
	data _null_; infile head; input; delete_header=_infile_;file print ods; put _ods_; run;
	title;
%end;
%if %sysfunc(fexist(resp)) %then %do;                                                                                              
	data _null_; infile resp; input; delete_response=_infile_;file print ods; put _ods_; run;
%end;
