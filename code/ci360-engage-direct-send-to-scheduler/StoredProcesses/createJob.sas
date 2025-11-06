/**********************************************************************************************************************
PROGRAM: SAS CUSTOMER INTELLIGENCE 360 Custom On-Prem Scheduler for Direct Marketing Tasks, Bulk Email Tasks and Segment Maps
DESCRIPTION: This application helps the CI 360 User to create deployed jobs from the CI 360 tenant 
             for Direct Marketing Tasks, Bulk Email Tasks and Direct Segment Maps for a SAS Administrator 
             to schedule on-prem in the SAS LSF scheduler.
FILE NAME: createJob.sas
DESCRIPTION: This stored process code creates a SAS JOB for SegmentMap or DMTask or Bulk Email Task.
Parameters:                                                       		   
   OBJECT_TYPE: SegmentMap or DMTask or EmailTask                   
   OBJECT_NAME: Name of DM task or Segment map or Bulk Email Task   
   OBJECT_ID  : ID of DM task or Segment map or Bulk Email Task     
   DESCRIPTION: Description to be added to deployed Job in metadata 
VERSION: 2.0
DATE MODIFIED: 25-APRIL-2024
AUTHOR: GLOBAL CUSTOMER INTELLIGENCE ENABLEMENT TEAM

#Copyright � 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0
**********************************************************************************************************************/

*  Begin EG generated code (do not edit this line);
*
*  Stored process registered by
*  Enterprise Guide Stored Process Manager V7.1
*
*  ====================================================================
*  Stored process name: createJob
*
*  Description: Creates SAS JOB for SegmentMap or DMTask or Bulk Email Task
*  ====================================================================
*
*  Stored process prompt dictionary:
*  ____________________________________
*  DESCRIPTION
*       Type: Text
*      Label: Job description
*       Attr: Visible
*  ____________________________________
*  OBJECT_NAME
*       Type: Text
*      Label: 360 Object name
*       Attr: Visible, Required
*  ____________________________________
*  OBJECT_TYPE
*       Type: Text
*      Label: 360 Object type
*       Attr: Visible, Required
*    Default: DMTask
*  ____________________________________
*;


*ProcessBody;

%global DESCRIPTION
        OBJECT_NAME
        OBJECT_TYPE;
*  End EG generated code (do not edit this line);

%let CI360_FOLDER = %substr(&SYSINCLUDEFILEDIR.,1,(%sysfunc(findc(&SYSINCLUDEFILEDIR.,&SEPARATOR.,"b"))));
%include "&CI360_FOLDER.&SEPARATOR.Config&SEPARATOR.autoexec.sas";

/* Redirects log to designated location*/
%if &sas_log_redirect = Y %then %do;
   proc printto log="&CI360_FOLDER.&SEPARATOR.Logs&SEPARATOR.createJob_&sysdate._%sysfunc(compress(&systime.,:)).log";
   run;
%end;

/**************************************/
/* Part 1: Check input                */
/**************************************/
%let STATUS=continue;
%let ERROR=;

*ProcessBody;
%check_value(OBJECT_NAME);
%check_value(OBJECT_ID);
%check_value(OBJECT_TYPE);
%check_value(DESCRIPTION, ALLOW_BLANK=TRUE);
%if %superq(OBJECT_TYPE) ne SegmentMap and %superq(OBJECT_TYPE) ne DMTask and %superq(OBJECT_TYPE) ne EmailTask %then %do; 
%let ERROR = OBJECT_TYPE(%superq(OBJECT_TYPE)) must be either SegmentMap, DMTask or EmailTask;
  %put ERROR: %superq(ERROR);
  %let status=error;
%end;

/**************************************/
/* Part 2: Check if job exists        */
/**************************************/
%put NOTE: --- %sysfunc(timestamp()) STATUS=%superq(STATUS) ---;

/* Does deployed job exist ? */
%let METADATA_PATH = %sysfunc(deploypath(%superq(OBJECT_TYPE), %superq(OBJECT_NAME)));
%let REF_DEPLOYED_METADATA=;
data _null_;
  length type $32 deployedId $20;
  call missing(type, deployedId);
  rc = metadata_pathobj("", "%superq(METADATA_PATH)", "DeployedJob", type, deployedId);
  call symputx("REF_DEPLOYED_METADATA", deployedId);
run;
%put &=REF_DEPLOYED_METADATA;
%put &=METADATA_PATH;

%if %superq(STATUS)=continue and "%superq(REF_DEPLOYED_METADATA)" ne "" %then %do;
  /* Query metadata for job details */
  filename meta_in temp;
  filename meta_out temp;

  /* create xml query */
  data _null_;
    file meta_in encoding="utf-8";
    put '<?xml version="1.0" encoding="utf-8"?>';
    put "<GetMetadata>";
    put "<Metadata>";
    put "  <JFJob Id=""%superq(REF_DEPLOYED_METADATA)""/>";
    put "</Metadata>";
    put "<Objects/>";
    put "<Reposid>$METAREPOSITORY</Reposid>";
    put "<NS>SAS</NS>";


    put "<Flags>OMI_GET_METADATA+OMI_XMLSELECT+OMI_TEMPLATE</Flags>";
    put "<Options>";
    put "<Templates>";
    put "  <JFJob Name='' Desc='' MetadataCreated=''><AssociatedJob/><SourceCode/><Trees/></JFJob>";
    put "  <Job Name=''><Trees/><SourceCode/></Job>";
    put "  <Tree Name=''><ParentTree/></Tree>";
    put "  <File Name='' FileName=''><Directories/></File>";
    put "  <Directory Name='' DirectoryName=''/>";
    put "</Templates>";
    put "</Options>";
    put "</GetMetadata>";
  run;

  /* send xml to metadata server */
  proc metadata in=meta_in out=meta_out repository="Foundation" header=full;
  run;
  filename meta_in clear;

  %parseJobMetadata(TABLE=work.job, XML_FILEREF=meta_out, XML_PATH=/GetMetadata/Metadata);

  libname meta_out clear;
  filename meta_out clear;

  /* Does job exist ? */
  data _null_;
    set work.job;
    put _all_;
    if(jobCreated ne "") then call symputx("STATUS", "jobexists");
  run;

  %put NOTE: --- %sysfunc(timestamp()) STATUS=%superq(STATUS) ---;

  %check_syscc;
%end;

/***********************************************/
/* Part 3: Lookup required objects in metadata */
/***********************************************/
%put NOTE: --- %sysfunc(timestamp()) STATUS=%superq(STATUS) ---;

%if %superq(STATUS)=continue %then %do;
  %let JOB_CREATED=;
  %let DEPLOYED_METADATA=;
  %let DEPLOYED_CODE=;
  %let JOB_METADATA=;
  %let JOB_CODE=;
  %let JOB_NAME          = %sysfunc(jobName(%superq(OBJECT_TYPE), %superq(OBJECT_NAME)));
  %let JOB_METADATA      = %sysfunc(jobPath(%superq(OBJECT_TYPE), %superq(OBJECT_NAME)));
  %let DEPLOYED_METADATA = %sysfunc(deployPath(%superq(OBJECT_TYPE), %superq(OBJECT_NAME)));
  %let REF_JOB_FOLDER=;
  %let REF_DEPLOY_FOLDER=;
  %let REF_COMPUTE_LOC=;
  %let REF_BATCH_SERVER=;
  %let REF_JOB_DIR=;
  %let REF_DEPLOY_DIR=;
  %let REF_RESPONSIBLE=;
  %let JOB_DIR=;
  %let DEPLOY_DIR=;

  data _null_;
    length jobFolderId $20 deployFolderId $20 computeLocationId $20 batchServerId $20 jobDirectoryId $20 deployDirectoryId $20
            responsiblePartyId $20 jobDirectory $256 deployDirectory $256 type $32;
    call missing(type);

    rc = metadata_pathobj("", strip(symget("META_JOB_FOLDER")), "Folder", type, jobFolderId);
    rc = metadata_pathobj("", strip(symget("META_DEPLOY_FOLDER")), "Folder", type, deployFolderId);
    rc = metadata_resolve("omsobj:ServerContext?@Name='" || strip(symget("META_COMPUTE_LOC")) || "'", type, computeLocationId);
    rc = metadata_resolve("omsobj:ServerComponent?@Name='" || strip(symget("META_BATCH_SERVER")) || "'", type, batchServerId);
    rc = metadata_resolve("omsobj:Directory?@Name='" || strip(symget("META_JOB_DIR")) || "'", type, jobDirectoryId);
    rc = metadata_resolve("omsobj:Directory?@Name='" || strip(symget("META_DEPLOY_DIR")) || "'", type, deployDirectoryId);
    rc = metadata_resolve("omsobj:ResponsibleParty?@Name='" || strip(symget("META_RESPONSIBLE")) || "'", type, responsiblePartyId);
    rc = metadata_getattr("omsobj:Directory?@Name='" || strip(symget("META_JOB_DIR")) || "'", "DirectoryName", jobDirectory);
    rc = metadata_getattr("omsobj:Directory?@Name='" || strip(symget("META_DEPLOY_DIR")) || "'", "DirectoryName", deployDirectory);

    call symputx("REF_JOB_FOLDER",    jobFolderId);
    call symputx("REF_DEPLOY_FOLDER", deployFolderId);
    call symputx("REF_COMPUTE_LOC",   computeLocationId);
    call symputx("REF_BATCH_SERVER",  batchServerId);
    call symputx("REF_JOB_DIR",       jobDirectoryId);
    call symputx("REF_DEPLOY_DIR",    deployDirectoryId);
    call symputx("REF_RESPONSIBLE",   responsiblePartyId);
    call symputx("JOB_DIR",           jobDirectory);
    call symputx("DEPLOY_DIR",        deployDirectory);
  run;

  %check_value(REF_JOB_FOLDER);
  %check_value(REF_DEPLOY_FOLDER);
  %check_value(REF_COMPUTE_LOC);
  %check_value(REF_BATCH_SERVER);
  %check_value(REF_JOB_DIR);
  %check_value(REF_DEPLOY_DIR);
  %check_value(REF_RESPONSIBLE);
  %check_value(JOB_DIR);
  %check_value(DEPLOY_DIR);
%end;
%check_syscc;

/*******************************************/
/* Part 4: Create physical job code files  */
/*******************************************/
%put NOTE: --- %sysfunc(timestamp()) STATUS=%superq(STATUS) ---;

%if %superq(STATUS)=continue %then %do;
  %let SAS_FILE      = %superq(JOB_NAME).sas;
  %let JOB_CODE      = %superq(JOB_DIR)\%superq(SAS_FILE);
  %let DEPLOYED_CODE = %superq(DEPLOY_DIR)\%superq(SAS_FILE);
  %let OBJECT_DESC   = %superq(OBJECT_NAME)(%superq(OBJECT_ID));
  
  /* Prepare header for SAS code */
  data _null_;
    w1 = 16 + length(strip(symget("JOB_METADATA")));
    w2 = 16 + length(strip(symget("JOB_CODE")));
    w3 = 16 + length(strip(symget("DEPLOYED_METADATA")));
    w4 = 16 + length(strip(symget("DEPLOYED_CODE")));
    w5 = 16 + length(strip(symget("OBJECT_DESC")));
    width = max(78, w1, w2, w3, w4, w5);
    stars = cats("/", repeat("*", width+2), "/");
    call symputx("WIDTH", width);
    call symputx("STARS", stars);

  run;

  /* Job code */ 
  data _null_;
    file "%superq(JOB_CODE)";
    length s $&WIDTH.;
    zonedDttm = prxchange('s/(\d\d\d\d\-\d\d-\d\d)T(\d\d:\d\d:\d\d.\d\d\d)([+-]\d\d:\d\d)$/$1 $2 UTC$3/io', 1, put(datetime(), e8601lx29.3));
    put "%superq(STARS)";
    s = "Job name:    %superq(JOB_NAME)";       put "/* " s $&WIDTH.. "*/";
    s = "Job:         %superq(JOB_METADATA)";   put "/* " s $&WIDTH.. "*/";
    s = "Code:        %superq(JOB_CODE)";       put "/* " s $&WIDTH.. "*/";
    s = "Created:     " || zonedDttm;           put "/* " s $&WIDTH.. "*/";
    s = "User:        %superq(_METAUSER)";      put "/* " s $&WIDTH.. "*/";
    s = "Hostname:    %superq(SYSHOSTNAME)";    put "/* " s $&WIDTH.. "*/";
    s = "Tenant:      %superq(TENANT_NAME)";    put "/* " s $&WIDTH.. "*/";
    s = "Environment: %superq(ENVIRONMENT_NM)"; put "/* " s $&WIDTH.. "*/";
    s = "360 Type:    %superq(OBJECT_TYPE)";    put "/* " s $&WIDTH.. "*/";
    s = "360 Object:  %superq(OBJECT_DESC)";    put "/* " s $&WIDTH.. "*/";
    put "%superq(STARS)";
    put;
    put '%include ' """%superq(AUTOEXEC)"" /source2;";
    put;
    put '%execute360object(' "%superq(OBJECT_TYPE), %superq(OBJECT_NAME)" ');';
  run;

  /* Deployed code */ 
  data _null_;
    file "%superq(DEPLOYED_CODE)";
    length s $&WIDTH. desc $512 description $4096;
    description = strip(symget("DESCRIPTION")); 
    putlog description=;
    jobCreated = timestamp();
    call symputx("JOB_CREATED", jobCreated);
    ts = input(jobCreated, anydtdtm23.);
    zonedDttm = prxchange('s/(\d\d\d\d\-\d\d-\d\d)T(\d\d:\d\d:\d\d.\d\d\d)([+-]\d\d:\d\d)$/$1 $2 UTC$3/io', 1, put(ts, e8601lx29.3));
    put "%superq(STARS)";
    s = "Job name:      %superq(JOB_NAME)";          put "/* " s $&WIDTH.. "*/";
    count = 0;
    call missing(desc);
    do until(desc=' ');
      count = count + 1;
      desc = scan(description, count, "0a"x);
      if(desc ne ' ') then do;
         if(count=1) then s = "Description:   " || desc;
                     else s = "               " || desc;
         put "/* " s $&WIDTH.. "*/";
      end;

    end;
    s = "Deployed job:  %superq(DEPLOYED_METADATA)"; put "/* " s $&WIDTH.. "*/";
    s = "Deployed code: %superq(DEPLOYED_CODE)";     put "/* " s $&WIDTH.. "*/";
    s = "Created:       " || zonedDttm;              put "/* " s $&WIDTH.. "*/";
    s = "User:          %superq(_METAUSER)";         put "/* " s $&WIDTH.. "*/";
    s = "Hostname:      %superq(SYSHOSTNAME)";       put "/* " s $&WIDTH.. "*/";
    s = "Tenant:        %superq(TENANT_NAME)";       put "/* " s $&WIDTH.. "*/";
    s = "Environment:   %superq(ENVIRONMENT_NM)";    put "/* " s $&WIDTH.. "*/";
    s = "360 Type:      %superq(OBJECT_TYPE)";       put "/* " s $&WIDTH.. "*/";
    s = "360 Object:    %superq(OBJECT_DESC)";       put "/* " s $&WIDTH.. "*/";
    put "%superq(STARS)";
    put;
    put '%include ' """%superq(AUTOEXEC)"" /source2;";
    put;
    put '%execute360object(' "%superq(OBJECT_TYPE), %superq(OBJECT_NAME)" ');';

  run;
%end;
%check_syscc;

/**************************************/
/* Part 5: Create metadata JFJob      */
/**************************************/
%put NOTE: --- %sysfunc(timestamp()) STATUS=%superq(STATUS) ---;

%if %superq(STATUS) = continue %then %do;
  filename meta_in temp;
  filename meta_out temp;

  data _null_;
    length description description_encoded $4096;
    description = "%superq(OBJECT_TYPE): %superq(OBJECT_NAME)";
    if length("%superq(DESCRIPTION)") > 1 then do;
      description = strip(description) || "  " || "0a"x || strip(symget("DESCRIPTION"));
    end;
    description_encoded = tranwrd(htmlencode(strip(description), 'amp gt lt apos quot 7bit'), '0a'x, '&#x0a;');
    file meta_in encoding = utf8;
    put '<?xml version="1.0" encoding="utf-8"?>';
    put '<AddMetadata>';
    put '  <Reposid>$METAREPOSITORY</Reposid>';
    put '  <NS>SAS</NS>';
    put '  <Type>JFJob</Type>';
    put '  <Flags>268435456</Flags>';
    put '  <Metadata>';
    put "    <JFJob Name=""%superq(JOB_NAME)"" Desc=""" description_encoded +(-1) """ IsActive=""1"" IsHidden=""0"" IsUserDefined=""0"" PublicType=""DeployedJob"" TransformRole=""SCHEDULER_JOB"" UsageVersion=""2000000"">";
    put '      <AssociatedJob>';
    put "        <Job Name=""%superq(JOB_NAME)"" Desc=""%superq(OBJECT_TYPE): %superq(OBJECT_NAME)"" IsActive=""1"" IsHidden=""0"" IsUserDefined=""1"" PublicType=""Job"" TransformRole=""SCHEDULER_JOB"" UsageVersion=""1000000"">";
    put '          <ResponsibleParties>';
    put "            <ResponsibleParty ObjRef=""%superq(REF_RESPONSIBLE)""/>";
    put '          </ResponsibleParties>';
    put '          <SourceCode>';
    put "          <File FileExtension=""sas"" FileName=""%superq(JOB_NAME)"" IsARelativeName=""1"" IsHidden=""0"" Name=""%superq(SAS_FILE)"" TextRole=""SourceCode"" UsageVersion=""0"">";
    put '            <Directories>';
    put "              <Directory ObjRef=""%superq(REF_JOB_DIR)""/>";
    put '            </Directories>';
    put '          </File>';
    put '          </SourceCode>';
    put '          <Trees>';
    put "            <Tree ObjRef=""%superq(REF_JOB_FOLDER)""/>";
    put '          </Trees>';
    put '        </Job>';
    put '      </AssociatedJob>';
    put '      <ComputeLocations>';
    put "        <ServerContext ObjRef=""%superq(REF_COMPUTE_LOC)""/>";
    put '      </ComputeLocations>';
    put '      <Properties>';
    put '        <Property DefaultValue="true" IsExpert="0" IsLinked="0" IsRequired="0" IsUpdateable="0" IsVisible="0" Name="LogFlag" PropertyName="LogFlag" SQLType="12" UsageVersion="0" UseValueOnly="0"/>';
    put '      </Properties>';
    put '      <ResponsibleParties>';
    put "        <ResponsibleParty ObjRef=""%superq(REF_RESPONSIBLE)""/>";
    put '      </ResponsibleParties>';
    put '      <SourceCode>';
    put "        <File Name=""%superq(JOB_NAME)"" FileName=""%superq(SAS_FILE)"" IsARelativeName=""1"" IsHidden=""0"" TextRole=""SourceCode"" UsageVersion=""0"">";
    put '          <Directories>';
    put "            <Directory ObjRef=""%superq(REF_DEPLOY_DIR)""/>";

    put '          </Directories>';
    put '        </File>';
    put '      </SourceCode>';

    put '      <TargetSpecifications>';
    put "        <ServerComponent ObjRef=""%superq(REF_BATCH_SERVER)""/>";
    put '      </TargetSpecifications>';
    put '      <Trees>';
    put "        <Tree ObjRef=""%superq(REF_DEPLOY_FOLDER)""/>";
    put '      </Trees>';
    put '    </JFJob>';
    put '  </Metadata>';
    put '</AddMetadata>';
  run;

  /* send xml to metadata server */
  proc metadata in=meta_in out=meta_out repository="Foundation" header=full;
  run;
  filename meta_in clear;
%end;
%check_syscc;

%if %superq(STATUS) = continue and %sysfunc(fexist(meta_out)) %then %do;
  data _null_;
    infile meta_out;
    input;
    putlog _infile_;
  run;
  filename meta_out clear;
  %let STATUS=success;
%end;

/**************************************/
/* Part 6: Return results             */

/**************************************/
%put NOTE: --- %sysfunc(timestamp()) STATUS=%superq(STATUS) ---;

/* HTTP Output */
data _null_;
  rc = stpsrv_header("Content-type", "application/json;");
  rc = stpsrv_header("Pragma", "nocache");
  rc = stpsrv_header("Expires", "0");
  if(strip(symget("STATUS")) ne "success" and strip(symget("STATUS")) ne "jobexists") then do;
    rc = stpsrv_header("Status-Code", "400"); /* 400 = bad request */
  end;
run;

%if %superq(STATUS)=jobexists %then %do;
  data work.job;
    status = strip(symget("STATUS")); 
    set work.job;
  run;
%end;
%if %superq(STATUS)=success %then %do;
  data work.job;
    length status jobName description jobCreated deployedMetadata deployedCode jobMetadata jobCode $1024;
    status = strip(symget("STATUS")); 
    jobName          = strip(symget("JOB_NAME"));
    description      = strip(symget("DESCRIPTION"));
    jobCreated       = strip(symget("JOB_CREATED"));
    deployedMetadata = strip(symget("DEPLOYED_METADATA"));

    deployedCode     = strip(symget("DEPLOYED_CODE"));
    jobMetadata      = strip(symget("JOB_METADATA"));
    jobCode          = strip(symget("JOB_CODE"));
  run;
%end;
%if %superq(STATUS) ne success and %superq(STATUS) ne jobexists %then %do;
  data work.job;
    status = strip(symget("STATUS")); 
    error = strip(symget("ERROR"));
    call missing(jobName, jobCreated, deployedMetadata, deployedCode, jobMetadata, jobCode);
  run;
%end;

data _null_;
 set work.job;
 old = stpsrv_header("Content-type", "application/json; encoding=utf-8"); 
 file _webout;

 put "{" ;
 put '"status":"'status+(-1)'",'; 
 put '"jobName":"' jobName+(-1)'",';
 put '"jobCreated":"' jobCreated+(-1)'",';
 put '"deployedMetadata":"' deployedMetadata+(-1)'",';
 deployedCode=tranwrd(deployedCode,'\','\\');
 put '"deployedCode":"' deployedCode+(-1)'",';
 put '"jobMetadata":"' jobMetadata+(-1)'",';
 jobCode=tranwrd(jobCode,'\','\\');
 put '"jobCode":"' jobCode+(-1)'",';
 put '"error":"' error+(-1)'"';  
 put "}" ;

run;
/*
proc json out=_webout pretty ;	
  	write open object;

	write close;	
run;
*/
proc sql noprint;
  drop table work.job;
quit;

proc printto;
run;

*  Begin EG generated code (do not edit this line);
;*';*";*/;quit;

*  End EG generated code (do not edit this line);

