/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%Macro add_data_to_metadata;
data work.metadata_table;
	length Staging_table $32 default_val $32 isnull $5 ispk $5 isfk $5 fk_reference $100 timeZone_flg $32 timeZone_value $32 data_type $50;
	
	set work.schema_details (drop=ordinal_root Column_label column_sequence) ;
	Staging_table=cats(substr(table_name,1,28),"_tmp");
	fk_reference='';
	isnull='';
	ispk='';
	isfk='';
	timeZone_flg = '';
    timeZone_value = '';
	if data_type ='timestamp' then do;
	    timeZone_flg = 'Y';
	    timeZone_value ="&timeZone_Value" ;
	 
	end;
run;


data work.udm7_pk_null_fk;
infile "&UtilityLocation.&slash.config/METADATA_TABLE.csv"
                 delimiter='|'
                 missover
                 firstobs=2
                 DSD
                 lrecl = 32767;

format Table_Name $100. ; 
format Column_Name $100. ;
format isnull $5.;
format ispk $5.;
format isfk $5.;
format fk_reference $100.;


input 
	Table_Name $ 
	Column_Name $ 
	isnull $ 
	ispk $ 
	isfk $ 
	fk_reference $
;

run;

proc sql;

 create table cdmcnfg.metadata_table as 
	(select t1.default_val,t2.isnull,t2.ispk,t2.isfk,t2.fk_reference,
								t1.mart_type, t1.table_name, t1.column_name,t1.data_type,t1.data_length,
								t1.column_type,t1.Staging_table,t1.timeZone_flg  from work.metadata_table t1 left join work.udm7_pk_null_fk t2
    on t1.table_name =t2.Table_Name and t1.column_name =t2.Column_Name );

run;
%ErrCheck(Unable to add data to metadata,add_data_to_metadata);
%if &errFlag %then %do;
	%goto ERREXIT;
%end;


proc sql;
select count(*) from cdmcnfg.metadata_table;
run;

%ERREXIT: %put %sysfunc(datetime(),E8601DT25.) --- &CDM_ErrMsg;

%mend add_data_to_metadata;
/*%add_data_to_metadata;*/
