/*
Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0

This process is intended to be used in a task to combine the task exported SAS dataset
 with an append dataset from another segment map or any other source.

The logs are written to <home>/stplogs/export_file. This directory is created
 and permission adjusted, if not already present.

The exported SDS are accessed via the Macrovar table

Modification History

05/10/2025: Raja M: Written 

*/
options  msglevel=i fullstimer nostsuffix sastrace=',,,d' sastraceloc=saslog;
options noerrabend ;

/*
%let logdir=%str(C:\SAS\Software\DirectAgent\stplogs); 
%let bkpdir=%str(c:\temp); 
*/ 
%let logdir=;   /* Optional...else logs go to default  */
%let bkpdir=;   /* When provided, the input export ds from ci360 is saved, before update */

%let g_drop_dn_columns=Y;
	
%global _CLIENTAPP returnKode;
%global g_input_ds_count g_col_name_list g_join_dsn_list;

%let filesep=%str(/);
%let returnKode=0;
%let MAMsg=;
%let syscc=0;
%let g_input_ds_count=0;

options nosource nosource2;

/*
This macro initializes the appropriate macro variables and (optionally) redirects the log
*/
%macro errorCheck(msg=);
%if not %symexist(sqlrc) %then %do;
	%let sqlrc=0;
%end;
%if not %symexist(sysdbrc) %then %do;
	%let sysdbrc=0;
%end;
%if &syscc. ne 0 or &syserr. eq 3 or &syserr. gt 4 or &sqlrc. gt 4 or (&sysdbrc. ne 0 and &sysdbrc. ne 1 and &sysdbrc. ne 100) %then %do;
	%let returnKode=16;
	%put &=syscc &=syserr;
	%put syswarningtext = %superq(syswarningtext);
	%put syserrortext = %superq(syserrortext);
	%if %symexist(sysdbmsg) %then %put sysdbmsg = %superq(sysdbmsg);;
	%if %symexist(MAMsg) %then %let MAMsg=%bquote(&msg.);
	%put ERROR: %bquote(&msg.);
%end;
%mend errorCheck;

/*
This macro purges any pre-existing work datasets for the subsequent iteration
*/
%macro purge_work(dsn=);

	%if %sysfunc(exist(&dsn.)) %then %do;
		proc delete library=work data=&dsn.;
		run;	
	%end;

%mend purge_work;
/*
Used to create the log directory if it does not already exist
*/
%macro create_dir(indir=);
	options dlcreatedir;
	libname logdir "&indir.";
	libname logdir clear;
	options nodlcreatedir;
%mend create_dir;
/*
Get table row count and store it in the quoted &var variable
*/
%macro getobs(inds=, var=);

data _null_;
	if 0 then
		set &inds. nobs=n;
	call symputx(&var., n);
	stop;
run;
%mend getobs;

/*
Verify that the appendDS exists.  If one does not exist, then this process quits processing.  There can be
 more than one datasets that provide this append information.
Each "append" SAS dataset must be assigned to SAS library that is either pre-assigned or
 assigned via mausrexp.sas.
*/
%macro md_setupEnv;

%local datenow datetimenow ;

%let datetimenow=%sysfunc(datetime(), 15.);
%let datenow=%sysfunc(datepart(&datetimenow.));
%let expdate = %sysfunc(putn(&datenow., yymmddn8.),8.);
%let exptime = %sysfunc(compress(%sysfunc(putn(&datetimenow.,tod8.)),":"));

%if "&p_join_type." = "" %then %do;
	%let p_join_type=%str(inner);
%end;

/**************************************************/
/* Generate random string to be used in filenames */
/**************************************************/
data _null_;
	%if %index(%bquote(&_CLIENTAPP), %str(Enterprise Guide))=0 and %index(%bquote(&_CLIENTAPP), %str(Studio))=0 %then %do;
		x=compress(uuidgen(),"-");
   %end;
   %else %do;
		x="_test_12345678901234567890";
   %end;
  x=substr(x, 2, 14);
  call symputx('g_suffix', x, 'G');
run;



%if %index(%bquote(&_CLIENTAPP), %str(Enterprise Guide))=0 and %index(%bquote(&_CLIENTAPP), %str(Studio))=0 %then %do;
	%if %symexist(segmapcd) %then %let l_fileid=&segmapcd.;
	%else %if %symexist(taskcd) %then %let l_fileid=&taskcd.;
	%else %let l_fileid=;
	%let currdttm = &expdate.&exptime.;
	%if &logdir. ne  %then %do;
		%create_dir(indir=&logdir.);
		%put NOTE: Log for export_file written to "&logdir.&filesep.mrgvars_&l_fileid._&currdttm..log";
		proc printto log="&logdir.&filesep.mrgvars_&l_fileid._&currdttm._&g_suffix..log" new;
		run;
	
		%if %sysfunc(exist(work.macrovar)) %then %do;
			data matables.mv_&l_fileid._&currdttm.;
				set macrovar;
			run;
		%end;
	%end;

%end;
%else %do;
 	
%end;

%errorcheck(msg=Failed at setupEnv prior to retrieving db table columns);
options dsnferr;

%if &p_append_dsn. eq %then %do;
	%put NOTE: The Merge/append variables process is skipped as no Append dataset is specified;
	%goto exit_setupEnv;
%end;


%errorcheck(msg=Failed while exiting setupEnv);

%exit_setupEnv:
%mend md_setupEnv;

/*
This process retrieves/validates the append datasets
*/
%macro md_process_append_ds;

%local i __md_custom_join_ind dsn;

%let g_join_dsn_list=;
%do i=1 %to %sysfunc(countw(&p_append_dsn., %str( )));
	%let __md_custom_join_ind=N;
	%let dsn=%scan(&p_append_dsn., &i., %str( ));
	%if %index(&dsn., %str(.)) eq 0 %then %do;
		%put ERROR: Two part libname not specified for element #&i. in &p_append_dsn.;
		%let returnKode=16;
	%end;

	%else %do;
		%let dsnlib=%scan(&dsn., 1, %str(.));
		%if "%sysfunc(libref(&dsnlib.))" ne "0" %then
			%mausrexp(&dsnlib., Execute);;
		%if %sysfunc(exist(&dsn.)) = 0 %then %do;
			%put ERROR: Append dataset specified at element #&i. in &p_append_dsn. does not exist;
			%let returnKode=16;		
		%end;
		%else %do;
		
			proc contents data=&dsn. nodetails;
				ods output variables=append_col_list;
			run;
			%let __md_custom_join_ind=;
			data append_col_list;
				attrib format length=$20;
				attrib label length=$50;			
				set append_col_list;
				if lowcase(variable) eq "__join_keys" then do;
					call symputx("__md_custom_join_ind", "Y");
				end;
			run;
			%if &__md_custom_join_ind. = Y %then %do;
				%let g_join_dsn_list = %str(&g_join_dsn_list. %(1=1%));
				%let __join_keys=;
				data _null_;
					set &dsn.(obs=1);
					call symputx('__join_keys', __join_keys);
					stop;
				run;

				%do j=1 %to %sysfunc(countw(&__join_keys., |));
					%let join_col = %scan(&__join_keys., &j., |);
					%let g_join_dsn_list = %str(&g_join_dsn_list. and a&i..&join_col. = ex.&join_col.);
				%end;
				%let g_join_dsn_list=%str(&g_join_dsn_list.|);
							
			%end;
			%else %do;
				%let g_join_dsn_list = %str(&g_join_dsn_list. %(%(&i.=&i.%) and a&i..&insubjectid. = ex.&insubjectid.%)|);
			%end;
			%put NOTE: &=g_join_dsn_list.;
			proc sql;
				describe table append_col_list;
			quit;
			proc print data=append_col_list;
			run;
			data v_append_col_list / view=v_append_col_list;
				attrib table_name length=$32;
				set append_col_list (keep=variable format label);
				attrib col_name_in_select length=$100;
				attrib table_alias_name length=$5;

				rename variable=col_name;
				if variable ne "&insubjectid.";
				table_alias_name = cats('a', "&i.");
				col_name_in_select = cats(table_alias_name, '.', variable);

				table_name = "&dsn.";
				
				if format ne "" then col_name_in_select = catt(col_name_in_select, " format=", format );
				if label ne "" then col_name_in_select = catt(col_name_in_select, " label=", quote(trim(label)) );				
			run;
			%if %sysfunc(exist(work.all_append_col_list)) %then %do;
				proc sql noprint;
					create table append_col_list_nodup as
						select vacl.*
						  from v_append_col_list vacl
						  left join all_append_col_list aacl
						  	on aacl.col_name = vacl.col_name
						  where aacl.col_name is null;
				quit;
				proc append base=all_append_col_list data=append_col_list_nodup;
				run;			
			%end;
			%else %do;
				proc append base=all_append_col_list data=v_append_col_list;
				run;			
			%end;

			proc sql noprint;
				drop table append_col_list;
				drop view v_append_col_list;
				%if %sysfunc(exist(work.append_col_list_nodup)) %then %do;
				drop table append_col_list_nodup;
				%end;
			quit;
		%end;
	%end;
%end;


%mend md_process_append_ds;

/*
Get table row count and store it in the quoted &var variable
*/
%macro getobs(inds=, var=);

data _null_;
	if 0 then
		set &inds. nobs=n;
	call symputx(&var., n);
	stop;
run;
%mend getobs;
/*
Read macrovar table and get the input datasets
*/
%macro md_get_exp_dsn;

%if %sysfunc(exist(macroVar)) = 0 %then %do;
	%let MAMsg=%str(The MacroVar table does not exist. Is the Export_file process connected to export node?);
	%let returnKode=16;
	%goto exit_md_get_exp_dsn;
%end;

proc sql noprint;
	create view v_macrovar as
		select *
		  from macrovar
		  where category='EXPORTINFO' 
		  and name like 'EXPORTOUTPUT%'
		  and parent ne ''
		  order by parent
	;
quit;

proc transpose data=v_macrovar out=work.expinfo(drop=_name_ _label_);
	by parent;
	id name;
	var value;
run;

data work.expinfo;
	set work.expinfo;
	if exportoutputtype = 'sas';
	if prxmatch("m/&p_export_file_pattern./i", exportoutputname) gt 0;
run;	

%getobs(inds=work.expinfo, var='g_input_ds_count');
%if &g_input_ds_count. eq 0 %then %do;
	%let MAMsg=%str(There are no export datasets to process based on pattern specified. Processing stopped);
	%let returnKode=16;
	%goto exit_md_get_exp_dsn;
%end;

proc sort data=work.expinfo;
	by exportoutputname;
run;

proc sql noprint;
	select exportoutputpath
		,exportoutputname
	  into
	  	:exportoutputpath_list separated by '|'
	   ,:exportoutputname_list separated by '|'
	  from work.expinfo
	  order by exportoutputname;
quit;

%put NOTE: &exportoutputpath_list.;
%put NOTE: &exportoutputname_list.;

%errorCheck(msg=Error detected while exiting md_get_exp_dsn macro process);
%if &returnKode. ne 0 %then %goto exit_md_get_exp_dsn;	

%exit_md_get_exp_dsn:
%mend md_get_exp_dsn;

/*
Combine the export columns and append columns into a single list
*/
%macro gen_select_col_list;

proc sql noprint;

	select coalesce(a.col_name_in_select, e.col_name_in_select)
	  into :select_clause_txt separated by ','
	from export_cols e
		left join all_append_col_list a
			on upcase(e.col_name) = upcase(a.col_name)
%if &g_drop_dn_columns. eq %str(Y) %then %do;
	where lowcase(e.col_name) not like '~_~_dn~_%' escape '~'
%end;	
	order by e.varnum
			;

quit;
%put &=select_clause_txt;

%mend gen_select_col_list;

/*
This process applies the column values from the append datasets to the export dataset
*/

%macro update_exp_dsn(ds=);

%local i dsname dsn;

%let dsname=%scan(&ds., 2, %str(.));
proc sql noprint feedback;
create table work.&dsname. as
	select &select_clause_txt.
		from &ds. ex
		%do i=1 %to %sysfunc(countw(&p_append_dsn., %str( )));
			%let dsn=%scan(&p_append_dsn., &i., %str( ));
			%let join_condition=%scan(%bquote(&g_join_dsn_list.), &i., %str(|));
			&p_join_type. join &dsn. a&i. on &join_condition.
		%end;;
quit;

%errorCheck(msg=Checking successful creation of updated export dataset);
%if &returnKode. ne 0 %then %goto exit_update_exp_dsn;

%if &p_sort_clause. ne %then %do;
	proc sort data=work.&dsname.;
		by &p_sort_clause.;
	run;
%end;		

%exit_update_exp_dsn:
%mend update_exp_dsn;

/*
When a backup directory is provided, the input export dataset is copied to the backup
The final/output export dataset replaces the original dm task export dataset
*/
%macro swap_exp_dsn(ds=);

	%local dslib dsname;

	%let dslib=%scan(&ds., 1, %str(.));
	%let dsname=%scan(&ds., 2, %str(.));
	
	%if &bkpdir. ne %then %do;	
		%if %sysfunc(exist(&bkpdir.)) eq 0 %then %do;
			%create_dir(indir=&logdir.);
		%end;
		libname bkplib base "&bkpdir." compress=yes;

		proc copy in=&dslib. out=bkplib memtype=(data);
			select &dsname.;
		run;
		quit;
		%errorCheck(msg=Checking completion of export dataset backup);
		%if &returnKode. ne 0 %then %goto exit_swap_exit_dsn;
	%end;
	
	proc delete library=&dslib. data=&dsname.;
	run;
	%errorCheck(msg=Checking deletion of export dataset);
	%if &returnKode. ne 0 %then %goto exit_swap_exit_dsn;		
	
	proc copy in=work out=&dslib. memtype=(data);
		select &dsname.;
	run;
	quit;
	%errorCheck(msg=Checking successful creation of appended export dataset);
	%if &returnKode. ne 0 %then %goto exit_swap_exit_dsn;	
	
%exit_swap_exit_dsn:
%mend swap_exp_dsn;

/*
Each export dataset is processed here one after another
*/
%macro md_process_exp_dsn;

%local i;
%let select_clause_txt=;

%do i=1 %to %sysfunc(countw(&exportoutputpath_list., %str(|)));
	%let dsnlib=%scan(&exportoutputpath_list., &i., %str(|));
	%let dsnds=%scan(&exportoutputname_list., &i., %str(|));
	%let dsn=&dsnlib..&dsnds.;
	%let select_clause_txt=;

	%if "%sysfunc(libref(&dsnlib.))" ne "0" %then
		%mausrexp(&dsnlib., Execute);;
	%if %sysfunc(exist(&dsn.)) = 0 %then %do;
		%put ERROR: Export dataset &dsn. does not exist;
		%let returnKode=16;		
	%end;
	%else %do;
		ods trace on;
		ods exclude sortedby (nowarn);
		proc contents data=&dsn. nodetails;
			ods output variables=export_col_list;
			ods output sortedby=export_sortedby;
		run;
		ods trace off;
		proc sort data=export_col_list;
			by num;
		run;
		%let sort_column_list=;
		%if %sysfunc(exist(export_sortedby)) %then %do;
			data _null_;
				set export_sortedby;
				where label1 = "Sortedby";
				call symputx('sort_column_list', cValue1);
			run;
		%end;
		data export_cols;
			set export_col_list (keep=variable num format label);
			attrib col_name_in_select length=$100;
			col_name_in_select = cats("ex.", variable);
			if format ne "" then col_name_in_select = catt(col_name_in_select, " format=", format );
			if label ne "" then col_name_in_select = catt(col_name_in_select, " label=", quote(trim(label)) );
			rename variable=col_name;
			rename num=varnum;
		run;
		%gen_select_col_list;
		%update_exp_dsn(ds=&dsn.);
		%errorCheck(msg=Checking successful update of export dataset);
		%if &returnKode. ne 0 %then %goto exit_md_process_exp_dsn;
		%if &sort_column_list. ne %then %do;
			proc sort data=work.&dsnds.;
				by &sort_column_list.;
			run;
		%end;

		%swap_exp_dsn(ds=&dsn.);

	%end;
%end;


%exit_md_process_exp_dsn:
%mend md_process_exp_dsn;

/*
Combine all datasets 
*/
%macro combine_all_ds;
	%if &g_input_ds_count. eq 1 %then %do;
		proc sql noprint;
			select name
				into :g_col_name_list separated by ","
				from work.sasxp_col_list
				order by name
				;
		quit;
		%goto exit_combine_all_ds;
	%end;

%exit_combine_all_ds:
%mend combine_all_ds;

/*
Check if there are any columns on the combined dataset that do not exist on the database table
*/
%macro check_exp_columns;

proc sql ;
create table missing_columns as
	select xp.name, xp.type
	  from work.sasxp_col_list xp
	  left join work.dbtab_col_list dbc
	  	on upper(xp.name) = upper(dbc.name)
	  	and xp.type = dbc.type
	  where dbc.name is missing
	  	;
quit;

data _null_;
	set missing_columns;
	putlog "ERROR: Column name: " name " Type: " type " is not matching with database table";
	if _N_ eq 1 then do;
		call symputx('MAMsg', "Database column name mismatch found");
		call symputx('returnKode', 16, 'g');
	end;
run;

%mend check_exp_columns;


%macro process_append_dsns;
	%local i;
	%local export_path exportname;
	
	%purge_work(dsn=work.sasxp_col_list);
	
	%do i=1 %to %sysfunc(countw(&exportoutputpath_list, %str(|)));
		%let exportpath=%scan(&exportoutputpath_list, &i., %str(|));
		%let exportname=%scan(&exportoutputname_list, &i., %str(|));
		
		%if "%sysfunc(libref(&exportpath.))" ne "0" %then
			%mausrexp(&exportpath., Execute);;
		
		%let ipds=&exportpath..&exportname.;

		%if "&countonly." ne "Y" %then %do;
			%if %sysfunc(exist(&ipds.)) ne 1 %then %do;
				%let MAMsg="The input export dataset (&ipds.) does not exist. Are all connected export nodes executed?";
				%put ERROR: &MAMsg.;
				%let returnKode=12;
				%goto exit_process_append_dsns;
			%end;
		%end;
		
		proc contents data=&ipds. out=xp_col_list nodetails noprint;
		run;
		
		proc append base=sasxp_col_list data=xp_col_list;
		run;
		
	%end;
	
	%errorCheck(msg=Failed after retreiving export dataset columns);

	%combine_all_ds;
	%errorCheck(msg=Failed after combining export datasets);
	%check_exp_columns;
	

%exit_process_append_dsns:
%mend process_append_dsns;

/*
Reset the log location to default, but only if we are logging to a different location
*/
%macro wrapUp;
	data &outtable.;
		set &intable.;
	run;
	%if %index(%bquote(&_CLIENTAPP), %str(Enterprise Guide))=0 and %index(%bquote(&_CLIENTAPP), %str(Studio))=0 %then %do;
		%if &returnKode. ne 0 %then %do;
			%put ERROR: &=MAMsg.;
		%end;
		%else %do;
			%put INFO: SASCI360_Direct_Merge_Variables process successfully completed.;
		%end;
 
 		%if &logdir. ne  %then %do;
			proc printto;
			run;
		%end;
 
	%end;

%mend wrapUp;

%macro mMerge_variables;

%let exportoutputpath_list=;
%let exportoutputname_list=;

%md_setupEnv;
%if &returnKode. ne 0 %then %goto exit_mMerge_variables;

%md_process_append_ds;
%if &returnKode. ne 0 %then %goto exit_mMerge_variables;

%md_get_exp_dsn;
%if &returnKode. ne 0 %then %goto exit_mMerge_variables;

%md_process_exp_dsn;
%if &returnKode. ne 0 %then %goto exit_mMerge_variables;
 
%wrapUp;

%exit_mMerge_variables:
%mend mMerge_variables;


%mMerge_variables;



%if &returnKode. ne 0 %then %do;
	%let syscc=&ReturnKode.;;
	%put ERROR: &=MAMsg.;
%end;

*  Begin EG generated code (do not edit this line);
;*';*";*/;quit;

*  End EG generated code (do not edit this line);
