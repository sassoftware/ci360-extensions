/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/

/***********************************************************************************************************/
/*  */
/* If the download contains a DATEKEY column ADD it to the target */
/***********************************************************************************************************/



%macro get_udm_columns;*/
	%if not %sysfunc(exist(work.UDM_COLUMNS)) %then %do;

		PROC SQL;
			CREATE TABLE work.UDM_COLUMNS AS 
		 	SELECT
		 		upcase(libname) as libname,  upcase(memname) as table_nm, upcase(name) as column_nm, 
		 		upcase(type)    as datatype, upcase(format)  as format,   length
			FROM DICTIONARY.COLUMNS
			WHERE upcase(libname) in ('UDMMART', 'TARGET')
			;
		QUIT;
	%end;	
%mend;


%macro create_oracle_code;
	%let errFlag = 0;

	/*%let code_file_path=&cdmcodes_path.&slash.mscode.sas;*/
	%let code_file_path=&cdmcodes_path.&slash.&database..sas;
	%let default_col_tst=;
	%let timeZone=;
	%let pk_col=;
	data _null_;
		length pk_col $ 32767; /* Ensure the length is sufficient */
		retain pk_col ''; /* Retain ensures the value persists across iterations */
		set work.temp_metadata end=last;

		if ispk eq 'TS' then
			do;
				if pk_col = '' then
					pk_col = column_name;
				else pk_col = catx(',', pk_col, column_name);
			end;

		if last then
			call symputx('pk_col', pk_col);

		if _n_ = 1 and _error_ then
			call symputx('pk_col', '');
	run;

	data _null_;
		length default_col_tst $ 32767; /* Ensure the length is sufficient */
		retain default_col_tst ''; /* Retain ensures the value persists across iterations */
		set work.temp_metadata end=last;

		/* Check the condition and concatenate values */
		if default_val ne '' then
			do;
				if default_col_tst = '' then
					default_col_tst = default_col;
				else default_col_tst = catx(',', default_col_tst, default_col);
			end;

		/* Store the concatenated string in a macro variable at the end of data step */
		if last then
			call symputx('default_col_tst', default_col_tst);

		/* If there are no rows, ensure default_col_tst is set to an empty string */
		if _n_ = 1 and _error_ then
			call symputx('default_col_tst', '');
	run;

	data _null_;
		length timeZone $ 32767; /* Ensure the length is sufficient */
		retain timeZone ''; /* Retain ensures the value persists across iterations */
		set work.temp_metadata end=last;

		/* Check the condition and concatenate values */
		if timeZone_flg eq 'Y' then
			do;
				if timeZone = '' then
					timeZone = timeZone_col;
				else timeZone = catx(';', timeZone, timeZone_col);
			end;

		/* Store the concatenated string in a macro variable at the end of data step */
		if last then
			call symputx('timeZone', timeZone);

		/* If there are no rows, ensure default_col_tst is set to an empty string */
		if _n_ = 1 and _error_ then
			call symputx('timeZone', '');
	run;

	%put pk_col: &pk_col;
	%put Time_Zone: &timeZone;

	/*	%if &pk_col eq '' %then*/
	/*		%do;*/
	/*			%let errFlag = 1;*/
	/*			%put inside pkcol condtion;*/
	/*			%errcheck(Failed : No primary found for table, &table_name);*/
	/*		%end;*/
	/**/
	/*	%if errFlag eq 0 %then*/
	/*		%do;*/
/*	%get_udm_columns;*/
	%add_datekey;
	%put inside generate code  condtion;
	filename %upcase(&database.) DISK "&code_file_path.";

	data _null_;
		file %upcase(&database.) mod;  /* Output the code to the file 'mscode' */
		length staging_table $32 timeZone_value1 $4000;
 		timeZone_value1 = symget('timeZone'); 
		/* Generate oracle code */
		put '%if %sysfunc(exist(&udmmart..' %trim("&table_name") ') ) %then %do;';
		put +1 '%let errFlag=0;';
		put +1 '%let nrows=0;';
		put +1 '%if %sysfunc(exist(&tmplib..' %trim("&staging_table") ') ) %then %do;';
		put +6 'proc sql noerrorstop;';
		put +8 'drop table &tmplib..' %trim("&staging_table") ';';
		put +6 'quit;';
		put +1 '%end;';
		put +1 '%check_duplicate_from_source(table_nm='%trim("&table_name")', table_keys=%str('%trim("&tmp_pk_col")'), out_table=work.'%trim("&table_name")');';
		put +1 'data &tmplib..' %trim("&staging_table") ';';
		put +2 '   set work.' %trim("&table_name") ';';

		if "&timeZone" ne "" then
			do;
				put +2  %trim(%nrstr(timeZone_value1))';';
			end;

		if &col_hash_flag. eq 1 then
			do;
				put +2 "Hashed_pk_col = put(sha256(catx('|',&pk_col)), $hex64.);";
			end;

		if "&init_char_pk_col" ne "" then
			do;
				put +2 "&init_char_pk_col";
			end;

		put +1 'run;';
		put +1 '%ErrCheck (Failed to Append Data to :'%trim("&staging_table") ', '%trim("&table_name")');';
		put +1'%if &errFlag = 0 %then %do;';
		put +4 'proc sql noerrorstop;';
		put +4 'connect to ORACLE (&sql_passthru_connection.);';
		put +8 'execute (merge into &dbschema..' %trim("&table_name") ' using &tmpdbschema..' %trim("&staging_table");
		
		if &col_hash_flag. eq 1 then
			do;
			staging_table="&staging_table";
				put+8 ' on (' %trim("&table_name")'.Hashed_pk_col = ' staging_table +(-1) '.Hashed_pk_col)';
			end;
		else
			do;
				put +8 ' on (' %trim("&merge_condition") ')';
			end;

		if "&merge_update." eq "" then do;
				/**/
		end;
		else do;
			put +8 'when matched then  ';
			put +8 'update set ' %trim("&merge_update");
		end;
		put +8 'when not matched then insert ( ' ;

		if &col_hash_flag. eq 1 then
			do;
			put +8 %trim("&merge_insert");

			put +8 ',Hashed_pk_col ) values ( ' ;
				
			put +8 %trim("&merge_value") ',' staging_table +(-1) '.Hashed_pk_col' ;
		end;
		else do;

			put +8 %trim("&merge_insert");

			put +8 ' ) values ( ' ;
				
			put +8 %trim("&merge_value");
		end;


		put +4 ' )) by ORACLE;';
		put +4 'disconnect from ORACLE;';
		put +4 'quit;';
		put +1 '%ErrCheck (Failed to Update/Insert into  :'%trim("&staging_table") ', '%trim("&table_name")', err_macro=SYSDBRC);';
		put +1 '%if &errFlag = 0 %then %do;';
		put +4 'proc sql noerrorstop;';
		put +8 'drop table &tmplib..' %trim("&staging_table") ';';
		put +4 'quit;';
		put +4 '%put ######## Staging table: ' %trim("&staging_table") ' Deleted ############;';
		put +6 '%end;';
		put +4 '%end;';
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
		put +1 '%put %sysfunc(datetime(),E8601DT25.) --- Processing table ' %trim("&table_name") ';';
		put '%put------------------------------------------------------------------;';
	run;

	filename %upcase(&database.);

%mend create_oracle_code;

/*%create_oracle_code;*/
