/********************************************************************************/
/* Copyright (c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                          */
/* ******************************************************************************/
%macro get_udm_mart(mart_type=, schema_table=, partitioning_table=, download_url=);
	%local mart_type schema_table partitioning_table download_url;

	filename outfile temp;
	PROC HTTP method="GET" out=outfile
		ct="application/json"
		url="%superq(download_url)";
		headers "Authorization" = "Bearer &DSC_AUTH_TOKEN.";
	run;
	%if "&Verbose" eq "1" %then %do;
		data _null_;
			infile outfile;
			input;	
			put _infile_;
		run;			
	%end;		

	libname jsondata json fileref=outfile;
	PROC DELETE data=WORK.ITEMS; run;
	proc copy in=jsondata out=work;
	run;
	libname jsondata;
	filename outfile;

 	%let nobs = 0;
	%let SchemaExist = 0;
 	%let dsid=%sysfunc(open(WORK.ITEMS));
	%if &dsid %then %do;
 		%let nobs=%sysfunc(attrn(&dsid,nlobs));
 		%let SchemaExist=%sysfunc(varnum(&dsid,schemaUrl));
 		%let dsid=%sysfunc(close(&dsid)); 
	%end;

 	%if &nobs and &SchemaExist %then %do; 
		%let schemaUrl=;
		data _null_;
			set items (keep=schemaUrl);
			if _n_=1 then call symputx("schemaUrl",schemaUrl);
		run;

		filename mdfile temp;
		PROC HTTP method="GET" out=mdfile
			url="%superq(schemaUrl)";
		run;
		
		libname mdjson json fileref=mdfile;
		PROC DELETE data=work.root work.alldata; run;
		proc copy in=mdjson out=work;
		run;
		libname mdjson;
		filename mdfile;

		data mart_schema;
			length mart_type $30 column_name $50;
			set root;
			mart_type= "&mart_type.";
		run;
		
		%if %sysfunc(exist(&schema_table.)) %then %do;
			proc append data=mart_schema base=&schema_table. force;
			run;
		%end;
		%else %do;
			data  &schema_table.;
				set mart_schema;
				length primary_key foreign_key 8;
			run;
		%end;
	
		PROC DELETE data=work.mart_schema; run;

		%set_partitioned_flag(json_all_data=work.alldata, partition_out=&partitioning_table);

	%end;

%mend get_udm_mart;
