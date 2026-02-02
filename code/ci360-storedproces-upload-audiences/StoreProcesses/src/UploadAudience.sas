
/*-----------------------------------------------------------------------------
Copyright © 2024, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
-----------------------------------------------------------------------------*/
/*
COPY TO AUTOEXEC /app/sas/config/Lev1/SASApp/StoredProcessServer/autoexec_usermods.sas or eg. C:\SAS\Config\Lev1\SASApp\StoredProcessServer
%let STP_AUD_LOG_DIR          = C:\SAS\Contexts\Banking\Exports\ or /logs/sas/ci360/UploadAudience/;
%let STP_AUD_EMAIL_FROM       = AudienceProcess@sas.com;
%let STP_AUD_GATEWAY          = https://extapigwservice-eu-prod.ci360.sas.com;
%let STP_AUD_TENANT_ID        = tenant_uid;
%let STP_AUD_CLIENT_SECRET    = access_point_secret_key;
%let STP_AUD_API_USER         = api_user;
%let STP_AUD_API_PW           = api_password;
%let STP_AUD_VAL_MINUTES      = 10; --how long script check the audience upload status
%let STP_AUD_EMAIL_LIST       = "admin@yourcompany.com" "marketing@yourcompany.com";
%let STP_proxyhost            = proxy.yoursite.com;   --do not create in autoexec if not used
%let STP_proxypw              = proxy_password;       --do not create in autoexec if not used 
%let STP_proxyuser            = proxy_username;       --do not create in autoexec if not used
%let STP_proxyport            = 1234;                 --do not create in autoexec if not used      
*/
%stpbegin;


filename outdata temp lrecl=32767;
%binary_file_copy_cust( infile=livedata, outfile=outdata );
%include outdata;
filename outdata clear;
%maspinit(xmlstream=macroVar neighbor);

/*************************************/
/* GLOBAL MACRO VARIABLES            */
/*************************************/
%global
audience_id
temp_token
audience_name
uploadmissval
STOP_AUDIENCE_PROCESS
;

%let STOP_AUDIENCE_PROCESS=0;
%let MSGERROR=;


options mprint mlogic ;
/*%put &=projectpath;*/

%macro RetrieveConfigParameters();

%if %symexist(STP_AUD_GATEWAY) %then %do;

%put &=STP_AUD_GATEWAY;

    %put Macro variables are defined;
	%put _user_;


%end;
%else %do;
    %put Macro variables are not defined. We retrieve the information from config file;

	%global STP_AUD_LOG_DIR
	STP_AUD_TENANT_ID
	STP_AUD_GATEWAY
	STP_AUD_CLIENT_SECRET
	STP_AUD_API_USER
	STP_AUD_API_PW
	STP_AUD_VAL_MINUTES
	STP_AUD_EMAIL_LIST
	STP_AUD_EMAIL_FROM;


	/*%let projectpath=C:\SAS\Contexts\Banking\;*/
	%let STP_AUD_LOG_DIR=&projectpath.\Exports\;


	filename tt "&projectpath.config.dat";
	data _null;
	      format inforet $1000.;
	      infile tt length=reclen dsd;
	      input inforet;
	      If _N_ eq 1 then call symput('STP_AUD_GATEWAY',strip(scan(inforet,2,'=')));
	      else If _N_ eq 2 then call symput('STP_AUD_TENANT_ID',strip(scan(inforet,2,'=')));
	      else If _N_ eq 3 then call symput('STP_AUD_CLIENT_SECRET',%nrstr(strip(scan(inforet,2,'='))));
	      else If _N_ eq 4 then call symput('STP_AUD_API_USER',strip(scan(inforet,2,'=')));
	      else If _N_ eq 5 then call symput('STP_AUD_API_PW',strip(scan(inforet,2,'=')));
	      else If _N_ eq 6 then call symput('STP_AUD_VAL_MINUTES',strip(scan(inforet,2,'=')));
	      else If _N_ eq 7 then call symput('STP_AUD_EMAIL_LIST',strip(scan(inforet,2,'=')));
		  else If _N_ eq 8 then call symput('STP_AUD_EMAIL_FROM',strip(scan(inforet,2,'=')));
		  
	      else If _N_ eq 9 then call symput('STP_proxyhost',strip(scan(inforet,2,'=')));
	      else If _N_ eq 10 then call symput('STP_proxypw',strip(scan(inforet,2,'=')));
	      else If _N_ eq 11 then call symput('STP_proxyuser',strip(scan(inforet,2,'=')));
	      else If _N_ eq 12 then call symput('STP_proxyport',strip(scan(inforet,2,'=')));
	run;
	
%put &STP_AUD_LOG_DIR
&STP_AUD_TENANT_ID
&STP_AUD_GATEWAY
&STP_AUD_CLIENT_SECRET
&STP_AUD_API_USER
&STP_AUD_API_PW
&STP_AUD_VAL_MINUTES
&STP_AUD_EMAIL_LIST
&STP_AUD_EMAIL_FROM;
%end;


%mend;
%RetrieveConfigparameters();

/*Redirect log*/
proc printto log="&STP_AUD_LOG_DIR.stpAudienceUpload.log";
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
%global abort_process;
%if &stop. eq 1 %then %let abort_process=1;
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
          method="GET" ct="application/x-www-form-urlencoded" TIMEOUT=20
          url="&STP_AUD_GATEWAY./token"
          QUERY=("username"="&APIUSR." "password"="%superq(APIUPW)" "grant_type"="password")
          headerout=outhd
                %if %symexist(STP_proxyhost) %then %do;
                    proxyhost="&STP_proxyhost."
                    proxyport=&STP_proxyport.
                    proxyusername="&STP_proxyuser."
                    proxypassword="&STP_proxypw."
              %end;                                      
          out=outfile;
          headers "Authorization" = "Bearer %superq(AUTH_TOKEN)";
          
     run;
      libname outfile JSON;
      data _null_;
            set outfile.root;
         call symputx("TEMP_TOKEN", access_token,'G');
      run;
      libname outfile;
      
      
      %if &SYS_PROCHTTP_STATUS_CODE. ne 200 %then %let STOP_AUDIENCE_PROCESS=1;

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
         call symputx("status_upload", strip(status)) ;
      run;
      %put &=status_upload;
      
      /*Validating status and calling again for status info if needed*/
      %if (&status_upload. eq COMPLETE ) %then
      %do;
            %put INFO: Audience uploaded;
      %end;
      %else
      %do;
      
            %if %sysfunc(FINDW(%upcase(%superq(runningstatus)), %upcase(&status_upload))) %then %do;
 
 
                  %if &retryAttemptNo. < &maxRetryAttempts. %then
                  %do;
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
      %if &abort_process. =1 %then %goto exit_validation;
      
      
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
      
      %if &SYS_PROCHTTP_STATUS_CODE. ne 200 %then %let STOP_AUDIENCE_PROCESS=1;
      %check_status_process(stop=&STOP_AUDIENCE_PROCESS.,msg=%str(Process aborted. Error retrieving audience attributes));
      %if &abort_process. =1 %then %goto exit_validation;
      
      
      libname addd json;
      
      /*retrieving identity column*/
      data _null_;
      set addd.alldata;
      if upcase(p1) eq "IDENTITYCOLUMNNAME" then call symput('identColumn',strip(value));
      run;
      
      data attributes(keep=name datatype columnnumber);/*table contains name and datatype of each attribute*/
      set addd.dataitems(rename=(name=name1));
      name=upcase(strip(label));
      if upcase(name1) eq upcase("&identColumn.") then call symput('identName',strip(label));
      run;
      
      data _null_;
            set addd.root;
            call symput('audience_name',strip(name));
      run;
      
      /*Getting exported data set */
      data _null_;
      set macrovar;
      if category eq 'EXPORTINFO' then do;
            if name eq 'EXPORTOUTPUTNAME' then call symput('table',strip(value));
            if name eq 'EXPORTOUTPUTPATH' then call symput('lib',strip(value));
      end;
      run;
      
      
      proc contents data=&lib..&table. out=_outc noprint;run;
      data _outc;set _outc(rename=(name=name1));
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
      %if &abort_process. =1 %then %goto exit_validation;
      
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

                  if type eq 2 then call symput('namesStr',strip(symget('namesStr')||" "||strip(name)));
                  if type eq 1 then call symput('namesNum',strip(symget('namesNum')||" "||strip(name)));
            run;

           %let _namesStr=%sysfunc(coalescec(%superq(namesStr),));
           %let _namesNum=%sysfunc(coalescec(%superq(namesNum),));

            data missingvalues;
               set &lib..&table. end=eof;
               length missvars $ 1055;           
             %if %length(&_namesStr) %then %do;
                     array varc (*) &_namesStr.;
                     do i=1 to dim(varc);
                        if missing(varc[i]) then missvars=catx(',',missvars,vname(varc[i]));
                     end;
               %end;

               %if %length(&_namesNum) %then %do;
                     array varn (*) &_namesNum.;
                     do i=1 to dim(varn);
                        if missing(varn[i]) then missvars=catx(',',missvars,vname(varn[i]));
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
                  %if &abort_process. =1 %then %goto exit_validation;
                  
            %end;
      %end;
      %else %do;
            %put NOTE: Uploading Audiences > Missing values allowed;
      %end;
      
      /*Creating csv file if validations are ok*/
      %let namesAud=;
      proc sort data=attributes; by columnnumber;run;
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
      %if &syserr. gt 0 %then %let STOP_AUDIENCE_PROCESS=1;
      %check_status_process(stop=&STOP_AUDIENCE_PROCESS.,msg=%str(Process aborted. &SYSERRORTEXT.));
      %if &abort_process. =1 %then %goto exit_validation;

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

      %if &abort_process. =1 %then %goto exit_upload;

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
      libname resp JSON;
      data _null_;
            set resp.root;   
         call symputx("Quoted_SignedURL", "'"||signedURL||"'") ;
      run;
      libname resp;

      %if &SYS_PROCHTTP_STATUS_CODE. ne 200 %then %let STOP_AUDIENCE_PROCESS=1;
      %check_status_process(stop=&STOP_AUDIENCE_PROCESS.,msg=%str(Process aborted. Error creating signed URL));
      %if &abort_process. =1 %then %goto exit_upload;

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
 
      %if &SYS_PROCHTTP_STATUS_CODE. ne 200 %then %let STOP_AUDIENCE_PROCESS=1;
      %check_status_process(stop=&STOP_AUDIENCE_PROCESS.,msg=%str(Process aborted. Error uploading csv files));
      %if &abort_process. =1 %then %goto exit_upload;



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
      
      %if %sysfunc(FINDW((200 202), &SYS_PROCHTTP_STATUS_CODE.)) =0 %then %let STOP_AUDIENCE_PROCESS=1;
      %check_status_process(stop=&STOP_AUDIENCE_PROCESS.,msg=%str(Process aborted. Error running audience));
      %if &abort_process. =1 %then %goto exit_upload;

      libname resp JSON;

      data _null_;
            set resp.root;   
         call symputx("historyId", historyId) ;
      run;

      %check_upload();

%exit_upload:

%mend;


%UploadAud();
%forcingerror();

%MACount(&inTable.);
%MAStatus(&_stpwork.status.txt);
proc printto;
run;
%stpend;
quit;

