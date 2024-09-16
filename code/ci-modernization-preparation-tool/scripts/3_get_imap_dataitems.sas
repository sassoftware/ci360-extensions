/*-----------------------------------------------------------------------------
 Copyright © 2024, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 SPDX-License-Identifier: Apache-2.0
-----------------------------------------------------------------------------*/

/*****************************/ 
/* Program: 3. IMAP_dataitem_extraction.sas
/* Input:
	-	macro variables of 0_environment_parameters.sas
/* Output: 
	-	madata.imap_dataitem_detail      
/*****************************/ 
/* Infomap data items are printed to the log.
	Capture the data items by redirecting the log temporarily and parseing it. */
/*****************************/ 

Filename imaplog temp;
%let revert_printto=&sysprinttolog;
proc printto log=imaplog; 
run;
PROC INFOMAPS 
	mappath="&mappath"
	metaserver="&metaserver."
	metaport=&metaport.
	user="&uid."
	password="&pass.";
	UPDATE INFOMAP "&mapname." mappath="&mappath" verify=yes;
	LIST DATAITEMS;
	CLOSE INFOMAP;
run;
/* Restore the previous log file locations. */
proc printto 
	%if %length(&revert_printto) %then %do; 
		log=&revert_printto
	%end;
	; 
run;

data madata.imap_dataitem_detail (drop=line);
	length line $2000;
	length imap_nm data_item_nm data_item_id $80
		data_item_folder data_item_description expression $256
		source_table source_column $60
		expression_type classification data_item_format $20;
	retain data_item_nm data_item_id data_item_folder data_item_description expression
		source_table source_column expression_type classification data_item_format ;

	infile imaplog;
	input;
	line=_infile_;

	imap_nm="&mapname.";
	if index(line,"Data item name:") then data_item_nm=scan(line,2,':');
	if index(line,"ID:") then data_item_id=strip(scan(line,2,':'));
	if index(line,"Folder:") then data_item_folder=scan(line,2,':');
	if index(line,"Description:") then data_item_description=scan(line,2,':');
	if index(line,"Expression:") then do;
		expression=scan(line,2,':');
		source_table=scan(scan(expression,1,'.'),-1);
		source_column=scan(scan(scan(expression,2,'.'),1),1,'>');
	end;
	if index(line,"Expression type:") then expression_type=scan(line,2,':');
	if index(line,"Classification:") then classification=scan(line,2,':');
	if index(line,"Format:") then do;
		data_item_format=scan(line,2,':');
		output;
		data_item_nm=""; data_item_ID=""; data_item_folder=""; data_item_description=""; expression="";
		source_table=""; source_column=""; expression_type=""; classification=""; data_item_format="";
	end;
run;
Filename imaplog;

%if &export_key_tables_to_csv. %then %do;
	%DS2CSV(colhead=Y,runmode=B,openmode=REPLACE
			,data=madata.imap_dataitem_detail 
			,csvfile=&dataFolder./imap_dataitem_detail.csv);
%end;