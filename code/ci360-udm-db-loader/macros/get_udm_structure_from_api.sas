/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%macro get_udm_structure_from_api(api_schema_version=&schema_version, schema_table=schema_details);

	%local api_schema_version schema_table;

	%let errFlag = 0;
	/******************************************************************************
		GENERATE AUTHENTICATION TOKEN 
	******************************************************************************/
	%get_authentication_token;
	%ErrCheck(Error occured while getting authentication token, get_udm_structure_from_api);
	%if &errFlag %then %do;
		%goto ERREXIT;
	%end;
	
	/******************************************************************************/
/*	    Download URLs */
	/******************************************************************************/
	Data Download_URLs;
		length DOWNLOAD_URL $1000 mart_type $30;

		/* metadata, Plan, Identity */
		DOWNLOAD_URL="&External_gateway.%nrstr(/discoverService/dataDownload/eventData/detail/nonPartitionedData?schemaVersion=)&api_schema_version.%nrstr(&category=)all";
		mart_type="detail";
		output;

	
		/* detail */
		DOWNLOAD_URL="&External_gateway.%nrstr(/discoverService/dataDownload/eventData/detail/partitionedData?schemaVersion=)&api_schema_version.%nrstr(&category=)all";
		mart_type="detail";
		output;
		
		/* CDM - No non-partitioned url needed.*/;
		DOWNLOAD_URL="&External_gateway.%nrstr(/discoverService/dataDownload/eventData/detail/partitionedData?schemaVersion=)&api_schema_version.%nrstr(&category=)cdm";
		mart_type="cdm";
		output;
		
		/* dbtReport */
		DOWNLOAD_URL="&External_gateway.%nrstr(/discoverService/dataDownload/eventData/dbtReport?schemaVersion=)&api_schema_version.%nrstr(&category=)";
		mart_type="dbtrpt";
		output;
	run;
	%ErrCheck(Unable to get data from Download URL, get_udm_structure_from_api);
	%if &errFlag %then %do;
		%goto ERREXIT;
	%end;
	/******************************************************************************/
	/* Generate Schema*/	
	/******************************************************************************/
	filename sascodes temp lrecl=2000;
	data _null_;
	 set Download_URLs; 
		file sascodes;
/*	    put 'call symputx("mart_type",'mart_type');';	*/
		put 'filename outfile temp;';
		put 'PROC HTTP method="GET" out=outfile';
		put +3 'ct="application/json"';
		put +3 'url="%nrstr(' DOWNLOAD_URL +(-1) ')";';
		put +3 'headers "Authorization" = "Bearer &DSC_AUTH_TOKEN.";';
		put 'run;';
		
		put 'libname jsondata json fileref=outfile;';
		put 'PROC DELETE data=WORK.ITEMS;run;';
		put 'proc copy in=jsondata out=work;';
		put 'run;';

 		put '%let nobs = 0;';
		put '%let SchemaExist = 0;';
 		put '%let dsid=%sysfunc(open(WORK.ITEMS));';
		put '%if &dsid %then %do;';
 		put +3 '%let nobs=%sysfunc(attrn(&dsid,nlobs));'; 
 		put +3 '%let SchemaExist=%sysfunc(varnum(&dsid,schemaUrl));';
 		put +3 '%let dsid=%sysfunc(close(&dsid)); ';
		put '%end;';
 		put '%if &nobs and &SchemaExist %then %do; ';

		/* get schema URL */
		put '%let schemaUrl=;';
		put 'data _null_;';
		put +3 'set items (keep=schemaUrl);';
		put +3 'if _n_=1 then call symputx("schemaUrl",schemaUrl);';
		put 'run;';
	
		put 'filename mdfile temp;';
		put 'PROC HTTP method="GET" out=mdfile';
		put +3 'url="&schemaUrl";';
		put 'run;';
		
		put 'libname mdjson json fileref=mdfile;';
		put 'proc copy in=mdjson out=work;';
		put 'run;';

		put 'data mart_schema;';
		put 'length mart_type $30 column_name $50;';
		put 'set root;';
		put 'mart_type= "' mart_type +(-1) '";';
		put 'run;';
		
		/* Rename the json data tables for later use ?*/
		if _n_=1 then do;
			put 'data  &schema_table.;';
			put +3 'set mart_schema;';
			put 'run;';
		end;
		else do;
			put 'proc append data=mart_schema base=&schema_table. force;';
			put 'run;';
		end;
		put 'PROC DELETE data=work.mart_schema;run;';
		put 'libname mdjson;';
		put 'filename mdfile;';
		put '%end;';
	run;
	
	%if "&Verbose" eq "1" %then %do;
		data _null_;
			infile sascodes;
			input;	
			put  _infile_;
		run;			
	%end;
	
	%include sascodes;	
	filename sascodes;
		
	proc sort data= &schema_table. nodup;
		by table_name column_name;
	run;

	proc sql;
	create table cdmcnfg.table_list as
	select distinct table_name, mart_type, 'Y' as execution_flag  from &schema_table.;
	run;

%ErrCheck(ERROR in getting UDM Structure from API,get_udm_structure_from_api);
	%if &errFlag %then %do;
		%goto ERREXIT;
	%end;


%ERREXIT: 
 %put ERROR in getting UDM Structure from API;
	
%mend;

/*%get_udm_structure_from_api;*/
