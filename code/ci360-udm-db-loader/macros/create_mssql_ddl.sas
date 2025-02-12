/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%macro create_mssql_ddl;
%let errFlag = 0;

%put ******create_mssql_ddl****** &target_table_name;
%let pk_col=;
%let hash_column=Hashed_pk_col;

data _null_;
    length pk_col $32767; /* Ensure the length is sufficient */
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

%put pkcol:::: &pk_col;


filename MSSQLDDL DISK "&code_file_path.";


data _null_;
    file MSSQLDDL mod; 

    PUT +1 'PROC SQL ;';

    /* Connect to SQL Server */
    PUT+1 'connect to SQLSVR (DATASRC=&dbsrc user=&dbuser password=&dbpass);';       

    	
	PUT+4 'EXECUTE (CREATE TABLE &dbschema..'%trim("&target_table_name") '(';
	if "&col_nm_key" ne '' then do;
		put+8 "&col_nm_key";
		
	end;
	if "&col_nm_type" ne '' then do;
		put+8 ',';
		put +8 "&col_nm_type" ;
	end;
	if &col_hash_flag. eq 1 then do;
	put+8 ",&hash_column varchar(64) NOT NULL";
	end;
	put+8 ')) by SQLSVR;';

	if "&pk_col" ne '' then do;
		if &col_hash_flag. eq 1 then do;
			put +6 'execute (alter table &dbschema..' %trim("&target_table_name") ' add constraint ' %trim("&pk_name") ' primary key (' %trim("&hash_column") ')) by SQLSVR;';
		end;
		else do;
			put +6 'execute (alter table &dbschema..' %trim("&target_table_name") ' add constraint ' %trim("&pk_name") ' primary key (' %trim("&pk_col") ')) by SQLSVR;';
		end;
	end;
/*	PUT+6 'EXECUTE ( ALTER TABLE '"&target_table_name"' ADD CONSTRAINT  '"&pk_name"'  PRIMARY KEY ('%trim("&pk_col")')) BY SQLSVR;';*/

	   
    PUT+1 'DISCONNECT FROM SQLSVR;';
    PUT+1 'QUIT;';
	put +1 '%ErrCheck (Failed to create Table: '%trim("&table_name") ', '%trim("&table_name")');';


run;


 data _null_;
	infile MSSQLDDL;
	input;	
	put  _infile_;
run;

filename _all_ list;
filename MSSQLDDL;
filename _all_ list;
%ErrCheck (Unable to generate ddl code for MSSQL,create_mssql_ddl);

%mend create_mssql_ddl;
/*%create_mssql_ddl;*/
