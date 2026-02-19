/********************************************************************************/
/* Copyright (c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                          */
/* ******************************************************************************/
%macro get_udm_schema(api_schema_version=, schema_table=, partitioning_table=);
	%local api_schema_version schema_table partitioning_table;

	/* delete output tables if exist already */
	proc delete data=&schema_table &partitioning_table; 
	run;

	%get_authentication_token;

	%err_check(Error occured while getting authentication token, &SYSMACRONAME.);
	%if &errFlag %then %do;	%goto ERREXIT; %end;

	%let download_url=&External_gateway./discoverService/dataDownload/eventData/detail/partitionedData?schemaVersion=&api_schema_version.%nrstr(&category)=all;
	%get_udm_mart(mart_type=detail, schema_table=&schema_table., partitioning_table=&partitioning_table., download_url=&download_url.);

	%let download_url=&External_gateway./discoverService/dataDownload/eventData/detail/nonPartitionedData?schemaVersion=&api_schema_version.%nrstr(&category)=cdm;
	%get_udm_mart(mart_type=cdm, schema_table=&schema_table., partitioning_table=&partitioning_table., download_url=&download_url.);

	%let download_url=&External_gateway./discoverService/dataDownload/eventData/dbtReport?schemaVersion=&api_schema_version.;
	%get_udm_mart(mart_type=dbtrpt, schema_table=&schema_table., partitioning_table=&partitioning_table., download_url=&download_url.);

	%err_check(Unable to get data from Download URL, &SYSMACRONAME.);
	%if &errFlag %then %do;	%goto ERREXIT; %end;



	proc sort data= &schema_table. nodup;
		by table_name column_name;
	run;

	proc sql;
		create table cdmcnfg.table_list as
		select distinct table_name, mart_type, 'Y' as execution_flag  from &schema_table.;
	run;

	%err_check(ERROR in getting UDM Structure from API,&SYSMACRONAME.);

	%ERREXIT:
%mend get_udm_schema;
