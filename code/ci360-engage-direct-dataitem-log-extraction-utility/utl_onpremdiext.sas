/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/******************************************************************************

This utility is intended to extract data-items and/or sql statements from the
 onprem_direct.log.<date>.  Note, that the current day file is never (without
 the date suffix) is never accessed - to avoid processing an incomplete file.

MAXFILES: This number determines the number of files processed in a single
           execution of this process.

Modification History
04/22/2022	Force TASK dataset to have fixed set of columns. With each new release
             there may be new columns added and this can cause issues when combining
             current data to history.
			Updated default maxreclen to 18000 (previously, 12000) due to issue
             while processing sample log provided from Denmark team
            Avoid capturing same dataitem multiple times in a queryTask, as this
             could lead to overflow of dataitem_list and truncation of dataitem_name.
            Fix handling of CountOnly segment maps
01/15/2024	Expand data item width to accommodate larger names
	Provide option to extract data item values specified in filter predicates
	Capture calc data items
11/25/2025  Added sample reports
            Extract segment map names correctly
			Save the results from each execution to SAS datasets
******************************************************************************/
options errabend fullstimer;
options nosource nosource2;
options nomlogic nomprint nosymbolgen;

%global _CLIENTAPPABREV;

/*
The following variables will need to be customized
c_lastUpdateFile:	
	This file stores the last "date" processed
	It should contain 1 record with date formatted as yyyy-mm-dd
	If the file does not exist - one will be created for you. In this case,
		(only) yesterdays' log file will be processed
c_outputDir:	
	Specify the location to generate output SAS datasets.

c_infile_dir:
	This is the directory containing the onprem_direct.log files
	It is assumed that the files are suffixed by a date: .yyyy-mm-dd

c_debug:
	When set to Y any pre-existing c_lastUpdateFile is removed
*/
%let c_lastUpdateFile=C:\SAS\Software\DirectAgent\logs\utl_opremdiext.dat;
%let c_infile_dir=C:\SAS\Software\DirectAgent\logs\;
%let c_outputDir=C:\Temp\onpremdiext;
%let c_extract_dataitem_values=N;
%let c_debug=N;
libname outlib "&c_outputDir." compress=yes;
%let datetimenow=%sysfunc(datetime(), 15.);
* %let datetimenow=%sysfunc(putn('24Nov2025 20:45:00'dt, 15.));

/*
This macro deletes an external file.  The input file name should not be enclosed in quotes.
*/
%macro deleteFile(dsn=);
    data _null_;
        length msg $ 512;
        fname="tempfile";
        rc=filename(fname, "&dsn.");
        if rc=0 and fexist(fname) then do;
           rc=fdelete(fname);
        end;
        msg=sysmsg();
        call symputx('msg', msg);
        call symputx('rc', rc);
        rc=filename(fname);
    run;
 
   %let rc=&rc.;
   %if "&rc." ne "0" %then %do;
       %let returnkode=12;
       %let MAMsg="File &dsn. delete step failed. RC:&rc. Message: &msg.";
       %put ERROR: &=MAMsg.; 
   %end;
   %else
   	   %put NOTE: "File &dsn. successfully deleted. &msg.";;
%mend deleteFile;
/*
This macro removes all files from a directory
*/
%macro cleanDir(dir=, delete_dir=N);

	data _null_;
		dirname = "&dir.";
		dref = 'thisdir';
		filref = 'thisfile';
		length fname $200;
		rc = filename(dref,dirname);
		did = dopen(dref);

		do i = 1 to dnum(did);
			fname = dread(did,i);
			rc = filename(filref,catx('/',dirname,fname));
			rc = fdelete(filref);
			msg = catx(' ', 'NOTE: File', fname, 'deleted from work directory');
			putlog msg;
		end;

		rc = dclose(did);
		%if &delete_dir. eq Y %then %do;
			rc = fdelete(dref);
			msg = catx(' ', 'NOTE: Directory', dirname, 'deleted rc=', rc);
			putlog msg;
		%end;
	run;

%mend cleanDir;


%let MAXRECLEN=18000;
%let MAXSQLLEN=30000;
%let MAXSQLLINES=1500;
%let MAXFILES=31;


%symdel g_last_date_processed g_file_list workLocation /nowarn;
%global g_last_date_processed g_file_list g_counts_only;
%global workLocation;

%let syscc=0;


%let LOGFILENAME=onprem_direct.log;
/*
%let LOGFILENAME=onprem_direct_small.log;
%let datetimenow=%sysfunc(putn('28MAR2022 20:45:00'dt, 15.));
%cleanDir(dir=&c_outputDir);
*/
%let datenow=%sysfunc(datepart(&datetimenow.));

%let report_date=%sysfunc(putn(&datenow.,weekdate17.)) %sysfunc(time(),time8.) %sysfunc(tzonename());
%let currdate = %sysfunc(putn(&datenow., yymmddn8.),8.);
%let currtime = %sysfunc(compress(%sysfunc(putn(&datetimenow.,tod8.)),":"));
%let currdttm = &currdate.&currtime.;


/*
1. Read the configuration file and get the last date already processed
2. Get each date from the date in #1 to current date (maximum of 10) excluding today
3. Generate list of input file and assign filename.

If the configuration file does not exist, then one will be created and yesterdays' log
 file is processed
*/
%macro init;

options dlcreatedir;
%let workLocation=%sysfunc(getoption(work))/onpremext;

libname appdir "&workLocation.";
options nodlcreatedir;
options user=appdir;
%put &=workLocation.;
%cleanDir(dir=&workLocation.);

%if &c_debug eq %str(Y) %then %do;
	%deleteFile(dsn=&c_lastUpdateFile.);
	%cleanDir(dir=&c_outputDir.);
	options mlogic mprint symbolgen;
%end;

%if %sysfunc(fileexist(&c_lastUpdateFile.)) %then %do;

	data _null_;
		attrib file_list length=$8192.;
		attrib file_suffix length=$10;
		infile "&c_lastUpdateFile." dsd truncover;
		input last_date_processed yymmdd10.;
		today_date = datepart(&datetimenow.);

		if ((today_date - last_date_processed)) gt 100 or
			((today_date - last_date_processed) le 1) then do;
			put "Invalid configuration file: Today is: " today_date date9. " Last processed date: " last_date_processed date9.;
            put "NOTE: This program does not process todays' log file.";
			put "NOTE: If too much time has elapsed since last run, try updating/deleting &c_lastUpdateFile.";
			put "NOTE: There are too many or zero days between last_date_processed and yesterday";
			abort cancel;
		end;

		file_list='';

		do i=last_date_processed+1 to today_date-1 by 1;
			if file_count lt &MAXFILES. then do;
				file_suffix=put(i, yymmdd10.);
				this_filename=quote(cats("&c_infile_dir./&LOGFILENAME..", file_suffix));

				if fileexist(this_filename) then do;
					file_count+1;
					file_list=catx(' ', file_list, this_filename);
					call symputx('g_last_date_processed', file_suffix, 'g');
					call symputx('g_file_list', file_list, 'g');
				end;
			end;
		end;
	run;

%end;
%else %do;
	%let yesterday=%eval(%sysfunc(datepart(&datetimenow.)) - 1);
	%let g_last_date_processed=%sysfunc(putn(&yesterday., yymmdd10.));
	%let g_file_list="&c_infile_dir./&LOGFILENAME..&g_last_date_processed";

	data _null_;
		if fileexist(&g_file_list.) ne 1 then do;
			put "ERROR: The input file "  &g_file_list.  " does not exist.  Process terminated";
			abort cancel;
		end;
	run;

%end;
%put &=g_last_date_processed.;
%put &=g_file_list.;
 
%if &g_file_list. eq  %then %do;
     %put "There are no files to process.  Processing aborted";
     data _null_;
           abort cancel;
     run;
%end;
%else %do;
     filename inlog (&g_file_list.);
%end;
 
%mend init;
 
%macro update_cfg_file;

/* %let g_last_date_processed=%str(2022-04-05); */
data _null_;
     file "&c_lastUpdateFile." dsd;
     put "&g_last_date_processed.";
run;
 
%mend update_cfg_file;
 
/*
The "clientData" lines in runTasks JSON can be extremely long and are not of value
 for the current project.  So, we choose to drop that record.
For each "runTasks" log record, we capture the entire JSON and write it to a flat file
file_record_nbr:
	Since we are processing multiple input files in one go, we need this counter to count
     the number of records in the specific file we are processing.
*/
%macro extract_runtask_data;
data filesList(keep= task_filename task_runtime file_record_nbr);
;
	 attrib capture_taskinfo_flag length=$1;
	 attrib task_filename length=$512;
	 attrib fname length=$512;
	 attrib task_runtime length=$23;
	 attrib input_filename length=$512;
	 attrib prev_input_filename length=$512;

	 retain capture_taskinfo_flag file_record_nbr fname prev_input_filename 
		rownum taskStart_expr task_runtime;

     infile inlog lrecl=&MAXRECLEN. truncover obs=max end=eof filename=input_filename length=input_line_length;
     input @1 logtxt $&MAXRECLEN..;

	 if prev_input_filename ne input_filename then do;
	 	prev_input_filename = input_filename;
		file_record_nbr=0;
	 end;
     rownum+1;
	 file_record_nbr+1;

     if _N_=1 then do;
       taskStart_pat="/^.{23} INFO {2}\[([\S]+)].{10,75}?agent.service.DirectMarketingTaskService - runTasks /";
       taskStart_expr=prxparse(taskStart_pat);
	 end;

	if capture_taskinfo_flag='Y' then do;
		if input_line_length gt 20 then do;
			if index(substr(logtxt,1,20), """clientData"":") then
				delete;
		end;

		if input_line_length ge &MAXRECLEN. then do;
			putlog 'ERROR: Line length exceeded.  Record skipped. ' input_filename= file_record_nbr= input_line_length=;
			error_count+1;

			if error_count le 5 then
				putlog logtxt=;
			delete;
		end;

		file taskinfo filevar=fname;
		put logtxt;

		if  logtxt = '}' and length(_infile_) = 1 then do;
			capture_taskinfo_flag='N';
			task_filename = fname;
			output filesList;
		end;

		delete;
	end;

	 *if length(logtxt) lt 50 or length(logtxt) gt 200 then delete;
	 if prxmatch(taskStart_expr, trim(logtxt)) then do;
	 	runtask_cnt+1;
		capture_taskinfo_flag='Y';
		task_runtime=substr(logtxt,1,23);
		fname=cats("&workLocation.", '/', scan(input_filename,-1,'/\'), '_', file_record_nbr);
		putlog "Write to file: " fname=;
	 end;
run;

filename inlog clear;

%mend extract_runtask_data;


%macro printall(libname,worklib=work);
   %local num i;
   proc datasets library=&libname memtype=data nodetails;
      contents out=&worklib..temp1(keep=memname) data=_all_ noprint;
   run;
   data _null_;
      set &worklib..temp1 end=final;
      by memname notsorted;
      if last.memname;
      n+1;
      call symput('ds'||left(put(n,8.)),trim(memname));
      if final then call symput('num',put(n,8.));
   run;
   %do i=1 %to &num;
      proc print data=&libname..&&ds&i noobs;
         title "Data Set &libname..&&ds&i";
      run;
   %end;
   proc sql noprint;
   	drop table &worklib..temp1;
   quit;
%mend printall;
/*
The top level information about the runTask is captured here
The variables g_task_id, g_task_name are saved and written to other
 datasets to enable common-key join between the different output tables

*/
%macro create_runtask(inds=);

%local col_names_list;
proc sql noprint;
    select name into :col_names_list separated by ' '
        from dictionary.columns
        where libname = upcase(scan("&inds.", 1, '.'))
        and memname = upcase(scan("&inds.", 2, '.'))
        ;
quit;
title &inds.;
proc print data=&inds.;
run;
title;

data stg_runtask(keep=task_name runtask_id type externalCode modifiedByUserName  task_runtime businessContextUUID countsOnly log_filename file_record_nbr );
	attrib name length=$100;
	attrib type length=$15;
	attrib externalCode length=$15;
	attrib modifiedByUserName length=$50;
	attrib id length=$40;
	attrib log_filename length=$100;
	attrib task_runtime length=8 format=e8601dt.;
	set &inds.;
	log_filename = scan("&task_filename.", -1, "/\");
	task_runtime =  &task_runtime. ;
	file_record_nbr = &file_record_nbr.;

	rename name=task_name;
	rename id=runtask_id;
	call symputx('g_task_id', id, 'g');
	call symputx('g_task_name', name, 'g');
	call symputx('g_counts_only', countsOnly, 'g');
run;

%mend create_runtask;
/*
Some of the export header information is shown at the end of other export
 definition information, which is not convenient for the purpose of writing
 export data items one at a time.
*/
%macro setup_exportHeader_view;

%if %sysfunc(exist(injson.exportdatatasks_exportdefiniti)) and "&g_counts_only." ne "1" %then %do;
	proc sql noprint;
		create table vw_exp_header as
			select et.segmentNodeId, et.outputSubjectId, et.id length=50,
				ed.outputName, ed.outputPath, ed.outputType, ed.removeDups, 
				ed.quoteOption
			  from injson.exportdatatasks et
			  	inner join injson.exportdatatasks_exportdefiniti ed
					on et.ordinal_exportDataTasks = ed.ordinal_exportDataTasks
					;
	quit;	
%end;
%else %do;
	data vw_exp_header;
		attrib outputName length=$60;
		attrib outputSubjectId length=$50;
	run;
%end;

%mend setup_exportHeader_view;

%macro get_segmap_data(inds=);

filename segmapd temp;
data _null_;
	file segmapd;
	set &inds.;
	where p3="segmentMapData" ;
	put value;
run;
libname jsondata JSON fileref=segmapd;
data stg_segmap_data;
	attrib runtask_id length=$40;
	runtask_id = "&g_task_id.";
	set jsondata.root;
run;

%mend get_segmap_data;


/*
If dataitem contains a full-stop, then it is from the Information map.
It is debatable, whether auto dataitems, from Segment/task in CDM should
 be included?  Here we have chosen to include them.
The "AllData" dataset is dynamic and can contain more/less columns based on
 the query complexity. So, we first retrieve the list of columns and dynamically
 generate the code to loop through all the columns.
*/
%macro capture_nodeInfo(inds=);

%local col_names_list i p_col_count;
%let p_col_count=0;
proc sql noprint;
    select name into :col_names_list separated by ' '
        from dictionary.columns
        where libname = upcase(scan("&inds.", 1, '.'))
        and memname = upcase(scan("&inds.", 2, '.'))
        ;
quit;

%do i=1 %to %sysfunc(countw(&col_names_list.));
	%if %index(&col_names_list., %str( P&i. )) %then
		%let p_col_count=%eval(&p_col_count.+1);;
%end;

%let this_libname=%upcase(%scan(&inds., 1, %str(.)));
%put &=this_libname.;
%if &c_debug eq %str(Y) %then %do;
	data outlib.save_output;
		set &inds.;
	run;
%end;
%setup_exportHeader_view;
%get_segmap_data(inds=&inds.);

/*

%printall(&this_libname.);
* 
proc sql number;
	select *
	from &inds.
	;
quit;
*/

data stg_node (keep=externalCode runtask_id task_runtime node_type node_id node_name outputSubjectId dataitem_name 
	%if &c_extract_dataitem_values eq %str(Y) %then %do;
	dataitem_value
	operator_name
	%end;
	)
	stg_export (keep=externalCode runtask_id task_runtime outputName outputSubjectId dataitem_name)
	stg_calc_item (keep=ci_id ci_type ci_name ci_expression ci_related_items)
;

	attrib externalCode length=$15;
	attrib runtask_id length=$40;
	attrib dataitem_name length=$100;
	attrib outputName length=$60;
	attrib node_id length=$40;
	attrib node_name length=$60;
	attrib prev_dataitem_name length=$100;
	attrib dataitem_name_list length=$2048;
	attrib outputSubjectId length=$50;
	attrib task_runtime length=8 format=e8601dt.;
	attrib dataitem_value length=$200;
	attrib dataitem_value_list length=$32000;
	attrib operator_list length=$256;
	attrib operator_name length=$20;
	attrib dataitem_name_switch length=$1;
	attrib dateType	length=$100;
	attrib capture_calc_columns length=$1;
	attrib ci_id length=$36;
	attrib ci_name length=$100;
	attrib ci_type length=$20;
	attrib ci_expression length=$2048;
	attrib ci_related_items length=$100;
	retain capture_calc_columns;
	retain ci_id ci_type ci_name ci_expression ci_related_items;

	retain dataitem_name dataitem_name_list externalCode node_id node_name segmentMapName
		outputName outputSubjectId prev_dataitem_name query_id dataitem_value dataitem_value_list
		operator_list dateType dataitem_name_switch;
	if 0 then set &inds. vw_exp_header;

	set &inds.;
	runtask_id = "&g_task_id";
	task_runtime =  &task_runtime. ;
	rename p1=node_type;

	where p1 in ("exportDataTasks", "queryTasks", "splitTasks", "segmentsInfo" "calculatedDataItems") or (p=1);
	if _n_ = 1 then do;
		declare hash explookup (dataset: 'vw_exp_header');
		explookup.definekey('outputName');
		explookup.definedata('outputSubjectId');
		explookup.definedone();
	end;
	if p1 eq "externalCode" then do;
		externalCode = Value;
		if substr(externalCode, 1, 4) eq 'MAP_' then
			segmentMapName = "%superq(g_task_name)";
		delete;
	end;

	if p1 eq "exportDataTasks" and p3 eq "outputName" then do;
		outputName = Value;
		rc = explookup.find(key:outputName);
		if rc then call missing(outputSubjectId);
		delete;
	end;
	if p2 eq "id" then node_id = value;
	if p1 eq "exportDataTasks" and p4 eq "columnValue" and p5 eq "id" then do;
		dataitem_name = Value;
		if index(Value, '.') then
			output stg_export;
		delete;
	end;
	if p1 eq "splitTasks" and p2 eq "name" then do;
		node_name = Value;
		prev_dataitem_name='';
	end;
	if p1 eq "splitTasks" and p5 eq "varRefId" then do;
		dataitem_name = Value;
		if index(Value, '.') then do;
			if prev_dataitem_name ne dataitem_name then do;
				output stg_node;
				prev_dataitem_name = dataitem_name;
			end;
			delete;
		end;
	end;

	if p1 eq "calculatedDataItems" then do;
		capture_calc_columns='Y';
	end;
	if capture_calc_columns eq 'Y' then do;
		if p2 eq "type" then ci_type = Value;
		else if p2 eq "expression" then  ci_expression = Value;
		else if p2 eq "name" then ci_name = Value;
		else if p2 eq "relatedCalculatedItems" then ci_related_items = Value;
		else if p2 eq "id" then do;
			ci_id = Value;			
			capture_calc_columns='N';
			output stg_calc_item;
		end;
		delete;
	end;

	if p = 1 and p1 = 'queryTasks' then do;
		prev_dataitem_name = '';
		dateType = '';
		dataitem_name_switch = 'N';
	end;
	%if &p_col_count. >= 6 %then %do;
		if p >= 6 then do;
			select (p);
				%do i=6 %to &p_col_count;
				when (&i.) do;
					if p1 eq "queryTasks" and p&i. eq "varRefId" then do;
						if prev_dataitem_name ne Value or dataitem_name_switch = 'Y' then do;
							dataitem_name_list = catx(',', dataitem_name_list, Value);		                    
							if prev_dataitem_name ne '' or dataitem_name_switch = 'Y' then do;
								dataitem_value_list = cats(dataitem_value_list, '|');
								operator_list = cats(operator_list, ',');
							end;
							prev_dataitem_name=Value;
							dataitem_name_switch = 'N';
						end;
					end;
					if p1 eq "queryTasks" and substr(p&i.,1,6) eq "values" and length(p&i.) gt 6 then do;
						if index(Value, '%%') and index(Value, '.') then do;
							dataitem_name_list = catx(',', dataitem_name_list, compress(Value, '%'));
						end;
						else do;
							if Value = "" then do;
								dateType = "";
								Value = ".";
							end;
							if dataitem_value_list = '' then
								dataitem_value_list = cats(dateType,Value);
							else if substr(dataitem_value_list, length(dataitem_value_list)) eq '|' then
								dataitem_value_list = cats(dataitem_value_list, dateType, Value);
							else
								dataitem_value_list = catx(',', dataitem_value_list, cats(dateType,Value));
							dateType = '';
						end;
					end;
					if p1 eq "queryTasks" and p&i eq "operator" then do;
						if Value ne "equals" then do;
							dataitem_name_switch = 'Y';
						end;
						if operator_list = '' then
							operator_list = Value;
						else
							operator_list = cats(operator_list, Value);
					end;
					if p1 eq "queryTasks" and p&i eq "dateType" then do;
						dateType = Value;
					end;	
				end;
				%end;
				otherwise do;
				end;
			end;		
		end;
	%end;

	if p2 eq "outputSubjectId" then do;
		outputSubjectId = Value;
		delete;
	end;

/*
	Here we create one row per data item from the comma separated dataitem_name_list 
	However data_item_value_list can contain multiple values per data item separated
	 by pipe symbol.
	These multiple values are stored in dataitem_value separated by commas
*/
	if p1 eq "queryTasks" and p2 eq "name" then do;
		node_name = Value;
		prev_dataitem_name='';
		j=1;
		do i=1 to countw(dataitem_name_list, ',');
			dataitem_name = scan(dataitem_name_list, i, ',');
			k = index(dataitem_value_list, '|') - 1;
			if k > 0 then do;
				dataitem_value = substr(dataitem_value_list, j, k);
				dataitem_value_list = substr(dataitem_value_list, k+2);
			end;
			else do;
				dataitem_value = substr(dataitem_value_list, j);
				dataitem_value_list = '';
			end;
			operator_name = scan(operator_list, i, ',');
			if substr(operator_name,1,6) = "equals" then
				operator_name = "equals";
			output stg_node;
		end;
		dataitem_name_list = '';
		dataitem_value_list = '';
		operator_list = '';
        delete;	
	end;
run;


%mend capture_nodeInfo;

/*
The data for each runTask is saved to the combined (one for each execution) tables 
	for reference
*/
%macro save_runtask_data;

%if "&g_task_id." eq "" %then %goto exit_save_runtask_data;
proc append base=runtask data=stg_runtask;
run;
proc append base=export data=stg_export;
run;
proc append base=node data=stg_node;
run;
proc append base=calc_item data=stg_calc_item;
run;
proc append base=segmap_data data=stg_segmap_data;
run;

%exit_save_runtask_data:
%mend save_runtask_data;

/*
This process considers each file (corresponds to one runTask) in sequence
It creates three outputs:
	- Task/Map header
	- Node
	- Export
which are linked together by common keys
*/
%macro process_each_taskinfo(ids=fileslist);

%local i start_file_no;
data _null_;
	if 0 then set &ids. nobs=nobs;
	call symputx('filesCount', nobs);
	stop;
run;
%put &=filesCount.;
%let start_file_no=1;
  
  
%do i=&start_file_no. %to &filesCount.; 

	data _null_;
		obsNum=&i.;
		set &ids. point=obsNum;
		call symputx('task_filename', task_filename);
		call symputx('task_runtime', input(substr(task_runtime,1,19), ymddttm19.));
		call symputx('file_record_nbr', file_record_nbr);	
		stop;
	run;

	%if &c_debug eq Y %then %do;
		%put &=task_filename. &=task_runtime. &=i. &=file_record_nbr.;     	 
	%end;

	filename injson "&task_filename.";
	libname injson json;
	%create_runtask(inds=injson.root);
	%if "&g_task_id." ne "" %then %do;
		%capture_nodeInfo(inds=injson.alldata);
		%save_runtask_data;
	%end;
	libname injson clear;

%end;

%mend process_each_taskinfo;

/*
The data for each execution of this process is saved to a permanent data store
*/
%macro save_data;

%if %sysfunc(exist(runtask)) %then %do;
proc append base=outlib.runtask data=runtask force;
run;
%end;

%if %sysfunc(exist(export)) %then %do;
proc append base=outlib.export data=export force;
run;
%end;

%if %sysfunc(exist(node)) %then %do;
proc append base=outlib.node data=node force;
run;
%end;

%if %sysfunc(exist(segmap_data)) %then %do;
proc append base=outlib.segmap_data data=segmap_data force;
run;
%end;

%exit_save_data:
%mend save_data;

/*
1. The required data is extracted from the log files (multiple files in JSON format)
2. Each JSON file is processed and saved as a SAS dataset in work library
3. The SAS datasets from work library are copied to permanent library for future use
4. The "last date processed" is saved to an external file. This file will be read/updated
    on the next execution of the program to avoid re-processing the same log files.
*/
%init;
%extract_runtask_data;
%process_each_taskinfo;
%save_data;
%update_cfg_file;
libname outlib clear;

/*
proc contents data=runtask nodetails varnum;
run;

proc sql number;
describe table dictionary.columns;
select name, type, length, varnum from dictionary.columns
where libname = 'APPDIR' and upper(memname) = 'RUNTASK'
order by varnum
;
quit;
*/
 
