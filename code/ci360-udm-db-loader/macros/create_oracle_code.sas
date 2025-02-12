/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%macro create_oracle_code;
%let errFlag = 0;
%let code_file_path=&cdmcodes_path.&slash.orclcode.sas;
/*%if %sysfunc(fileexist("&code_file_path.")) ge 1 %then %do;*/
/*   %let rc=%sysfunc(filename(temp,"&code_file_path."));*/
/*   %let rc=%sysfunc(fdelete(&temp));*/
/*%put Old Code file deleted...;*/
/*%end; */
%symdel  default_col_tst timeZone pk_col;

data _null_;
    length pk_col $ 32767; /* Ensure the length is sufficient */
    retain pk_col ''; /* Retain ensures the value persists across iterations */
 
    set work.temp_metadata end=last;
 
    /* Check the condition and concatenate values */
    if ispk eq 'TS' then do;
        if pk_col = '' then 
            pk_col = column_name;
		
        else 
            pk_col = catx(',', pk_col, column_name);
		
    end;
 
    /* Store the concatenated string in a macro variable at the end of data step */
    if last then call symputx('pk_col', pk_col);
 
    /* If there are no rows, ensure default_col_tst is set to an empty string */
    if _n_ = 1 and _error_ then call symputx('pk_col', '');
run;
 
data _null_;
    length default_col_tst $ 32767; /* Ensure the length is sufficient */
    retain default_col_tst ''; /* Retain ensures the value persists across iterations */
 
    set work.temp_metadata end=last;
 
    /* Check the condition and concatenate values */
    if default_val ne '' then do;
        if default_col_tst = '' then
            default_col_tst = default_col;
        else
            default_col_tst = catx(',', default_col_tst, default_col);
    end;
 
    /* Store the concatenated string in a macro variable at the end of data step */
    if last then call symputx('default_col_tst', default_col_tst);
 
    /* If there are no rows, ensure default_col_tst is set to an empty string */
    if _n_ = 1 and _error_ then call symputx('default_col_tst', '');
run;
 
data _null_;
    length timeZone $ 32767; /* Ensure the length is sufficient */
    retain timeZone ''; /* Retain ensures the value persists across iterations */
 
    set work.temp_metadata end=last;
 
    /* Check the condition and concatenate values */
    if timeZone_flg eq 'Y' then do;
        if timeZone = '' then
            timeZone = timeZone_col;
        else
            timeZone = catx(',', timeZone, timeZone_col);
    end;
 
    /* Store the concatenated string in a macro variable at the end of data step */
    if last then call symputx('timeZone', timeZone);
 
    /* If there are no rows, ensure default_col_tst is set to an empty string */
    if _n_ = 1 and _error_ then call symputx('timeZone', '');
run;
 
 
filename orclcode DISK "&code_file_path.";

data _null_;
    file orclcode mod;
 
    /* Generate ORACLE code */
    put '%if %sysfunc(exist(&udmmart..'"&table_name"')) %then %do;';
	put +1 '%let errFlag=0;';
	put +1 '%let nrows=0;';
    put +1 '%get_Eventdttm('%trim("&mart_type")');';
	put+1 '%if %sysfunc(exist(&tmplib..'%trim("&staging_table")')) %then %do;';
	put +4 'proc sql noerrorstop;';
    put +6 'connect to oracle (user=&tmpdbuser password=&tmpdbpass path=&tmpdbpath);'; 
	put +6 'execute (drop table ' %trim("&staging_table") ';) by oracle;';
	put +4 'disconnect from oracle;';
    put +4 'quit;'; 
	put+1 '%end;';
	
    put +4 'proc sql noerrorstop;';
 
    put +4 'connect to oracle (user=&tmpdbuser password=&tmpdbpass path=&tmpdbpath);'; 
    put +6 'execute (create table ' %trim("&staging_table")  ;
	put +6 '(' ;
    if "&col_nm_key" ne '' then do;
	put+6 "&col_nm_key";
	put+6 ',';
	end;
	put +6 "&col_nm" ;
	put +6 ')) by oracle;';
	if "&pk_col" ne '' then do;
		put +6 'execute (alter table ' %trim("&staging_table") ' add constraint ' %trim("&tmp_pk_name") ' primary key (' %trim("&tmp_pk_col") ')) by oracle;';
	end;
	put +4 'disconnect from oracle;';
    put +4 'quit;';

	put +1 ' %check_duplicate_from_source('"&table_name"',%str('"&pk_col"'));';
	put +1 '%ErrorCheck(Failed to create : '%trim("&staging_table") ', '%trim("&table_name")');';
	put +1 '%if &errFlag = 0 %then %do;';
 
    put +2 'proc sql noerrorstop;';
 
    put +2 'create view work.' %trim("&staging_view") ' as';
    put +2 'select';
	put +6	 %trim("&col_nm1");
 
	if "&timeZone" ne '' then do;
		put +6   ','%trim("&timeZone");
	end;
	if "&default_col_tst" ne '' then do;
        put +6 ',' %trim("&default_col_tst");
    end;
    put +4 'from work.' %trim("&table_name");
 
    if %unquote(%nrbquote(%trim('&datetime_filter.'))) eq 'Y' then do;
         put +4 "&datetime_where_condtion"' input(strip("&CDM_UDMFirstEventDate_fmt"), &format.)';
    end;

 
    put +4 ';';
    put +1 'quit;';

	put +1 '%ErrorCheck(Failed to create : '%trim("&staging_view") ', '%trim("&table_name")');';
	put +2 '%if &errFlag = 0 %then %do;';
    put +2 'proc sql noerrorstop;';
    put +2 'select count(*) into: nrows';
    put +6 'from &udmmart..' %trim("&table_name") ';';
    put +1 'quit;';

    /* Conditional logic for number of rows */
    put '%if &nrows ge &DB_BL_THRESHOLD %then %do;';
    put +2 'proc append data=work.' %trim("&staging_view") ' base=&tmplib..' %trim("&staging_table") ' (' %trim("&DB_BL_OPTS") ') force;';
    put +2 'run;';
    put '%end;';
    put '%else %do;';
    put +2 'proc append data=work.' %trim("&staging_view") ' base=&tmplib..' %trim("&staging_table") ' (' %trim("&DB_LD_OPTS") ') force;';
    put +2 'run;';
    put '%end;';
 
	put +4 '%ErrorCheck(Failed to Append Data to :'%trim("&staging_table") ', '%trim("&table_name")');';
	put +4 '%if &errFlag = 0 %then %do;';
	if "&pk_col" ne '' then do;
	 	put+1 'proc sql noerrorstop;';
		put+1 'connect to oracle (user=&dbuser pass=&dbpass path=&dbpath);';
		put+1 'execute';
		put+8  '(merge into '%trim("&table_name")' using '%trim("&staging_table");
		put+8     'on ('"&merge_condition"')';
		put+8 'when matched then update';
		put+8 'set ' %trim("&merge_update");
		put+8 'when not matched then insert( ' %trim("&merge_insert");
		put+8')';
		put+8 'values( ' %trim("&merge_value");
		put+8 ') )by oracle;';
		put+1  'run;';
		put+1 'disconnect from oracle;';
		put+1 'quit;';
		put +4 '%ErrorCheck(Failed to Update/Insert into  :'%trim("&staging_table") ', '%trim("&table_name")');';
		put +6 '%if &errFlag = 0 %then %do;';

		put+1 'proc sql noerrorstop;';
		put+1 'connect to oracle (user=&tmpdbuser pass=&tmpdbpass path=&tmpdbpath);';
		put+1 'execute (drop table '%trim("&staging_table")') by oracle;';
		put+1 'disconnect from oracle;';
		put+1 'quit;';
		put+1 '%put ######## Staging table: '%trim("&staging_table") ' Deleted ############;';


	end;
	else do;
	  	put +2 'proc sql noerrorstop;';
	  	put +2 'connect to oracle (user=&dbuser password=&dbpass path=&dbpath);';   
	  	put +4 'execute ( Truncate table '%trim("&table_name")'; ) by oracle;';
	  	put +4 'execute ( Insert into '%trim("&table_name")' ('%trim("&merge_insert")') ';
	  	put +6 'select ' %trim("&merge_value") ' from '%trim("&staging_table") ';) by oracle; ';
    	put +4 'execute (drop table '%trim("&staging_table") ';) by oracle;';
    	put +2 'disconnect from oracle;';
    	put +2 'quit;';
		put +2 '%ErrorCheck(Failed to Update/Insert into :'%trim("&staging_table") ', '%trim("&table_name")');';
		put +2 '%put ######## Staging table: ' %trim("&staging_table") ' Deleted ############;';

	end;
	put+6 '%end;';
	put+4 '%end;';
	put+2 '%end;';
	put+1 '%end;';

	put +1 '%put %sysfunc(datetime(),E8601DT25.) --- Processing table ' %trim("&table_name") ';';
	put +1 '%if &errFlag = 0 %then %do;';
	put +2 'proc sql noerrorstop;';
    put +6 'drop table &udmmart..' %trim("&table_name") ';';
    put +6 'drop table work.' %trim("&table_name") ';';

    put +2 'quit;';
	put +1 '%end;';
	put +1 '%else %do;';
	put +4 '%put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;';
	put+1 '%end;';
	put+1 '%end;';


    put '%put------------------------------------------------------------------------------------------------------------;';
run;
/*%ErrorCheck(Unable to generate load code for Table Name:%trim("&table_name") );*/
/*%if &errFlag %then %do;*/
/*	%goto ERREXIT;*/
/*%end;*/
data _null_;
	infile orclcode;
	input;	
	put  _infile_;
run;

filename _all_ list;
filename orclcode;
filename _all_ list;


%mend create_oracle_code;
/*%create_oracle_code;*/