/*-----------------------------------------------------------------------------
 Copyright © 2024, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 SPDX-License-Identifier: Apache-2.0
-----------------------------------------------------------------------------*/

/*****************************/ 
/* Program:5. Caclulated_Items_analysis.sas
/* Input:
	-	macro variables of 0_environment_parameters.sas
/* Output: 
	-	madata.calculated_items      
/*****************************/ 
/* To get the JSON files 
	-	Sign in to SASCIStudio 
	-	F12 to show the Browser Dev Tools 
	-	Navigate to the Network tab
	-	Refresh the list of Calculated Items
	-	Copy Response JSON
	-	Paste into the corresponding JSON file in the data folder
/*****************************/ 

/* If you have multiple each Business Contexts, you may need to run this multiple times */
libname calcitem json "&Calculated_Items_json.";
proc copy inlib=calcitem outlib=work;
   select volist;  
run;
data madata.calculated_items(keep=id name type expression);
 set work.volist;
run;



/********************/
/* Calculated Data items usage */
/********************/
proc SQL;
	create table madata.calculated_items_used_by_camp as
	select c._name as Campaign_Nm
		, c._code as Campaign_Cd
		, c.id as Campaign_id
		, c._parentFolder as Parent_Folder
		, n._varName as Data_item_Nm
		, ci.id as Data_item_Id
		, ci.name
		, ci.expression
		, count(n._varInfoId) as times_used
		, 1 as campaigns_using
	from madata.campaign c
	inner join madata.all_dataitems_in_nodes n on c.id=n.campaign_id
	inner join madata.calculated_items ci on ci.id=strip(substr(n._varInfoId,6))
	group by 1,2,3,4,5,6
	order by 4,1,5,6;
quit;
proc SQL;
	create table calculated_items_used as
	select  Data_item_Id
		, sum(times_used) as times_used
		, sum(campaigns_using) as campaigns_using
	from madata.calculated_items_used_by_camp
	group by Data_item_Id
	order by Data_item_Id desc;
quit;

proc sql;
create table madata.calculated_items_used as
	select c.*
		, u.times_used
		, u.campaigns_using
	from MADATA.calculated_items c
	inner join calculated_items_used u on c.id=u.data_item_id;
quit;
%let value_calculated_items_used=&SQLOBS;
data madata.campaign_summary;
 set madata.campaign_summary end=last;
	output;
	if last then do;
		label="Number of calculated data items used in campaigns (see table madata.calculated_items_used_by_camp)";
		value=&value_calculated_items_used;
		output;
	end;
run;

proc sql;
create table madata.calculated_items_not_used as
	select *
	from MADATA.calculated_items 
	where id not in (select distinct data_item_id from calculated_items_used)
;
quit;
%let value_calculated_items_not_used=&SQLOBS;
data madata.campaign_summary;
 set madata.campaign_summary end=last;
	output;
	if last then do;
		label="Number of calculated data items not used in campaigns (see table madata.calculated_items_not_used)";
		value=&value_calculated_items_not_used;
		output;
	end;
run;


%if &export_key_tables_to_csv. %then %do;
	%DS2CSV(colhead=Y,runmode=B,openmode=REPLACE
			,data=madata.calculated_items_used 
			,csvfile=&dataFolder./calculated_items_used.csv);
	%DS2CSV(colhead=Y,runmode=B,openmode=REPLACE
			,data=madata.calculated_items_not_used 
			,csvfile=&dataFolder./calculated_items_not_used.csv);
	%DS2CSV(colhead=Y,runmode=B,openmode=REPLACE
			,data=madata.campaign_summary 
			,csvfile=&dataFolder./campaign_summary.csv);
%end;

/* cleanup */
proc delete data=work.volist
	WORK.CALCULATED_ITEMS_USED;
quit;
/********************/
/* Summpary report */
/********************/
title 'Key metrics';
proc sql; select * from madata.campaign_summary; quit;
title;
