/************************************************************************************************
| File Name: stpDownloadFile.sas   
| Program Description: This SAS STP web service accespts XML payload through POST requests, 
|                      parses the XML, reads file URLs and downloads the datafiles  from AWS.  
|  					   The data from the AWS file is also saved in SAS datasets.
|
| Copyright  2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.                                   #
| SPDX-License-Identifier: Apache-2.0  	
|***********************************************************************************************/

/* Set Project Working Directory */
%let projdir = %sysfunc(dlgcdir('C:/STP')); 
/* Include Configuration Files */
%include "./conf/config.sas";
%include "./conf/credentials.sas";

/* Print STP log to external file */
%if &saslogredirect. = %str(true) %then %do;
	proc printto log="./logs/STPLog_&DTTM..txt";
	run;
%end;

/* Main Macro of this STP */
%macro Main();
libname instream xmlv2 xmlmap="./conf/inpxmlmap.map"; /* Libname to read incoming XML payload */
libname proglib "&liblocation."; /* Library definition for program */
libname outdata "./output"; /* STP output will be saved to this library */

/* Write files only if config parameter is set to true */
%if &debug. = %str(true) %then %do;
	filename xmlfile "./logs/InStream_&DTTM..xml";
%end;
%else %do;
	filename xmlfile TEMP;
%end;

/* Read incoming payload into a file */
data _null_;
   file xmlfile;
   infile instream LRECL=3000;
   INPUT;   
   PUT _INFILE_;
run;

/* Library definition based on payload XML file */
libname instrtmp xmlv2 xmlfileref=xmlfile xmlmap="./conf/procxmlmap.map" automap=replace;
proc copy in=instrtmp out=proglib memtype=data; /* Copy table data from payload to program library */
 select InData;
run;

/* Read datafile and metadatafile URLs from payload data */
data _null_;
	set proglib.indata;	
	call symput("datafile",datafile);
	call symput("metadatafile",metadatafile);
run;

/* Download data and metadata files from AWS */
%downloadawsfile(fileUrl=&metadatafile., fileType=metadatafile);
%downloadawsfile(fileUrl=&datafile., fileType=datafile);

%mend;

/* Macro to download files from AWS */
%macro downloadawsfile(fileUrl=, fileType=);
/* Write files only if config parameter is set to true */
%if &fileType. = %str(datafile) %then %do;
	%if &debug. = %str(true) %then %do;
		filename outfile "./output/datafile_&DTTM..csv";
		filename apiresp "./output/datafileApiResponse_&DTTM..txt";
	%end;
	%else %do;
		filename outfile TEMP;
		filename apiresp TEMP;
	%end;	
%end;
%else %if &fileType. = %str(metadatafile) %then %do;
	%if &debug. = %str(true) %then %do;
		filename outfile "./output/metadatafile_&DTTM..txt";
		filename apiresp "./output/metadatafileApiResponse_&DTTM..txt";
	%end;
	%else %do;
		filename outfile TEMP;
		filename apiresp TEMP;
	%end;
%end;

/* HTTP request to download file from AWS */
proc http 	   
	   out=outfile
	   headerout=apiresp
	   method="GET"
	   url="&fileUrl."
	   timeout=50;
run;
%prochttp_check_return(200); /* Check if http return code is 200 */
%check_for_errors; /* Check for any errors in output */

/* Read metadatafile for getting column names in a variable */
%if &fileType. = %str(metadatafile) %then %do;
	filename jsonmap "./conf/jsonmap.map";
	libname objjson JSON fileref=outfile map=jsonmap automap=create;

	data proglib.columnnames_&DTTM.;
		length tempstr colnamesstr $1000;	
		n=1;
		do until(eof);
			set objjson.root (keep=columnName) end=eof;					
			tempstr = strip("rename=Var")||strip(put(n,2.))||strip("=")||strip(columnName);
     		colnamesstr = catx(" ", colnamesstr, tempstr);	
			n = n + 1;
		end;
		call symputx("columnnames",colnamesstr);
	run;	
%end;
%else %if &fileType. = %str(datafile) %then %do;
/* Import data from datafile in a SAS dataset */	
	proc import 
		datafile=outfile 
		dbms=csv 
		out=outdata.datafile_&DTTM. (&columnnames.) replace;
		getnames=no;
	run;
%end;

%mend;
/* Call Main macro */
%Main();

%if &saslogredirect. = %str(true) %then %do;
	proc printto;
	run;
%end;