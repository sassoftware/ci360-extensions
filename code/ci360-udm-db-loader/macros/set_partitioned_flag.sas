/********************************************************************************/
/* Copyright (c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                          */
/* ******************************************************************************/
%macro set_partitioned_flag(json_all_data=, partition_out=);
	%local	json_all_data partition_out;

	data work.partitioning_temp (keep=current_table partitioned_flg rename=(current_table=table_name));
	 set &json_all_data. end=last;
		length current_table $32;
		retain current_table "";
		retain partitioned_flg 1;

		* write table when first row of next table is identified;
		if P1='table_name' and current_table ne value and _n_ ne 1 then do;
			output; 
			partitioned_flg=1;
			current_table="";
		end;

		if P1='table_name' then current_table=value;
		if substr(P2,1,10)='categories' and upcase(value) in ('ENGAGEMETADATA','PLAN') then partitioned_flg=0;
		if substr(current_table,1,4)='cdm_' and upcase(current_table) not in 
			('CDM_CONTACT_HISTORY','CDM_RESPONSE_HISTORY','CDM_RESPONSE_EXTENDED_ATTR') then partitioned_flg=0;
		if substr(current_table,1,3)='md_' then partitioned_flg=0;
		
		if last then output; * write last table;
	run;

	%if %sysfunc(exist(&partition_out.)) %then %do;
		proc append data=work.partitioning_temp base=&partition_out. force;
		run;
	%end;
	%else %do;
		data &partition_out.;
			set work.partitioning_temp;
		run;
	%end;
	PROC DELETE data=work.partitioning; run;
%mend;