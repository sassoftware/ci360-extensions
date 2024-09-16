/*-----------------------------------------------------------------------------
 Copyright © 2024, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 SPDX-License-Identifier: Apache-2.0
-----------------------------------------------------------------------------*/

/* Campaign Analysis */
/* Total number of campaigns and campaigns per run or modified */
data madata.campaign_summary (keep=label value); 
	set madata.campaign end=last;
 	length label $100;
	retain value_bin_1 value_bin_2 value_bin_3 value_bin_4 value_bin_5 0;
	if      year(datepart(_lastRunDate))=year(today())   then value_bin_1+1;  
	else if year(datepart(_lastRunDate))=year(today())-1 then value_bin_2+1;  
	else if year(datepart(_lastModDate))=year(today())   then value_bin_3+1;  
	else if year(datepart(_lastModDate))=year(today())-1 then value_bin_4+1;  
	else value_bin_5+1;
	if last then do;
		label =  "Total number of campaigns";value=_n_;output;
		label =  "Campaign run this year";value=value_bin_1;output;
		label =  "Campaign run last year";value=value_bin_2;output;
		label =  "Other campaign modified this year, but not run this or last year";value=value_bin_3;output;
		label =  "Other campaign modified last year, but not run this or last year";value=value_bin_4;output;
		label =  "Remaining campaigns";value=value_bin_5;output;
	end;
run;

/* Nodes per campaign > campaign complexity */

PROC SQL;
	CREATE TABLE Nodes AS 
	SELECT t1.campaign_id, 
		(COUNT(DISTINCT(t1.node_id))) AS nodes_per_campaign
	FROM MADATA.NODE t1
	GROUP BY t1.campaign_id
	ORDER BY nodes_per_campaign DESC;
QUIT;

data campaign_summary(keep=label value) ids_oversize_campaigns(keep=campaign_id);
 set WORK.Nodes end=last;
 	length label $100;
	retain value_bin_1 value_bin_2 value_bin_3 value_bin_4 value_bin_5 0;
	if _n_=1 then do;
		label = "Number of nodes in largest campaign"; value=nodes_per_campaign; output campaign_summary;
	end;
	if      nodes_per_campaign > 800 then do; value_bin_1+1; output ids_oversize_campaigns; end;
	else if nodes_per_campaign > 200 then value_bin_2+1;
	else if nodes_per_campaign >  50 then value_bin_3+1;
	else if nodes_per_campaign >  10 then value_bin_4+1;
	else  value_bin_5+1;
	if last then do;
		label = "Campaigns having >800 nodes (see table madata.campaigns_having_over_800_nodes)";value=value_bin_1;output campaign_summary;
		label = 'Campaigns having 201 to 800 nodes';value=value_bin_2;output campaign_summary;
		label = 'Campaigns having 51 to 200 nodes';value=value_bin_3;output campaign_summary;
		label = 'Campaigns having 11 to 51 nodes';value=value_bin_4;output campaign_summary;
		label = 'Campaigns having 1 to 10 nodes';value=value_bin_5;output campaign_summary;
	end;
run;
proc append base=madata.campaign_summary data=campaign_summary;
run;
proc sql;
 create table madata.campaigns_having_over_800_nodes as
 select * from madata.campaign 
 where id in (select campaign_id from ids_oversize_campaigns);
quit;
proc delete data=Nodes ids_oversize_campaigns campaign_summary; run;

/********************/
/* Code usage */
/********************/
proc SQL;
	create table madata.stored_processes_used as
	select catx('\',n.StoredProcess_parent_folder,StoredProcess_folder) as StoredProcess_path
		, n.StoredProcess_name
		, n.isCustomNode as in_custom_node
		, count(distinct Campaign_id) as campaigns
		, count(*) as nodes
	from madata.codenode n
	where n.StoredProcess_folder ne ""
	group by 1, 2, 3
	order by 1, 2, 3;
quit;

proc SQL;
	create table madata.campaigns_with_stp_process_nodes as
	select c._name as Campaign_Nm
		, c._code as Campaign_Cd
		, c.id as Campaign_id
		, c.full_folder_path as Campaign_path
		, n._nodeName as Node_nm
		, catx('\',n.StoredProcess_parent_folder,StoredProcess_folder) as STP_path
		, n.StoredProcess_name as STP_name
	from madata.campaign c
	inner join madata.codenode n on c.id=n.campaign_id
	where isCustomNode="false" and ProcessNode_type = 'storedProc' 
	order by Campaign_path, Campaign_Nm;
quit;
proc SQL;
	create table campaigns_with_stp_procnodes_sum as
	select 'Campaigns with STP based Process Nodes (see table madata.campaigns_with_stp_process_nodes)' as label length=100
		, count(distinct Campaign_id) as value
	from madata.campaigns_with_stp_process_nodes;
quit;
proc append base= madata.campaign_summary data=campaigns_with_stp_procnodes_sum;
quit;
proc delete data=campaigns_with_stp_procnodes_sum; run;


proc SQL;
	create table madata.campaigns_with_custom_nodes as
	select c._name as Campaign_Nm
		, c._code as Campaign_Cd
		, c.id as Campaign_id
		, c.full_folder_path as Campaign_path
		, n._nodeName as Node_nm
		, catx('\',n.StoredProcess_parent_folder,StoredProcess_folder) as STP_path
		, n.StoredProcess_name as STP_name
	from madata.campaign c
	inner join madata.codenode n on c.id=n.campaign_id
	where isCustomNode="true"
	order by Campaign_path, Campaign_Nm;
quit;
proc SQL;
	create table campaigns_with_custom_nodes_sum as
	select 'Campaigns with Custom Nodes (see table madata.campaigns_with_custom_nodes)' as label length=100
		, count(distinct Campaign_id) as value
	from madata.campaigns_with_custom_nodes;
quit;
proc append base= madata.campaign_summary data=campaigns_with_custom_nodes_sum;
quit;
proc delete data=campaigns_with_custom_nodes_sum; run;

proc SQL;
	create table madata.campaigns_with_manual_code as
	select c._name as Campaign_Nm
		, c._code as Campaign_Cd
		, c.id as Campaign_id
		, c.full_folder_path as Campaign_path
		, n._nodeName as Node_nm
		, n._codeText as Code_extract
	from madata.campaign c
	inner join madata.codenode n on c.id=n.campaign_id
	where ProcessNode_type='manual' 
	order by Campaign_path, Campaign_Nm;
quit;
proc SQL;
	create table campaigns_with_manual_code_sum as
	select 'Campaigns with user written code (see table madata.campaigns_with_manual_code)' as label length=100
		, count(distinct Campaign_id) as value
	from madata.campaigns_with_manual_code;
quit;
proc append base= madata.campaign_summary data=campaigns_with_manual_code_sum;
quit;
proc delete data=campaigns_with_manual_code_sum; run;


/********************/
/* Data items usage */
/********************/
proc SQL;
	create table data_items_by_campaign as
	select c._name as Campaign_Nm
		, c._code as Campaign_Cd
		, c.id as Campaign_id
		, c.full_folder_path as Campaign_path
		, n._varName as Data_item_Nm
		, case when upcase(substr(n._varInfoId,1,5))='ROOT.' 
		then strip(substr(n._varInfoId,6))
		else strip(n._varInfoId) end as Data_item_Id
		, count(n._varInfoId) as times_used
		, 1 as campaigns_using
	from madata.campaign c
	inner join madata.all_dataitems_in_nodes n on c.id=n.campaign_id
	group by 1,2,3,4,5,6
	order by 4,1,5,6;
quit;
proc SQL;
	create table data_item_usage as
	select  Data_item_Id
		, sum(times_used) as times_used
		, sum(campaigns_using) as campaigns_using
	from data_items_by_campaign
	group by Data_item_Id
	order by Data_item_Id desc;
quit;

proc sql;
create table madata.imap_dataitems_used as
	select a.*
		, b.times_used
		, b.campaigns_using
	from MADATA.IMAP_DATAITEM_DETAIL a
	left join data_item_usage b on a.data_item_id=b.data_item_id
	where a.data_item_id in (select distinct data_item_id from data_items_by_campaign)
	order by a.data_item_id
;
quit;
%let value_imap_dataitems_used=&SQLOBS;
%put &=value_imap_dataitems_used;

data _NULL_;
	if 0 then set madata.IMAP_DATAITEM_DETAIL nobs=n;
	call symputx('value_total_imap_dataitems',n);
	stop;
run;

data madata.campaign_summary;
 set madata.campaign_summary end=last;
	output;
	if last then do;
		label="Total number of data items in IMAP - up to 2000 for 360Direct";
		value=&value_total_imap_dataitems;
		output;
		label="Number of IMAP data items used in campaigns (see table madata.imap_dataitems_used)";
		value=&value_imap_dataitems_used;
		output;
	end;
run;

proc sql;
create table madata.imap_dataitems_not_used as
	select *
	from MADATA.IMAP_DATAITEM_DETAIL 
	where data_item_id not in (select distinct data_item_id from data_items_by_campaign)
;
quit;
%let value_imap_dataitems_not_used=&SQLOBS;
%put &=value_imap_dataitems_not_used;
data madata.campaign_summary;
 set madata.campaign_summary end=last;
	output;
	if last then do;
		label="Number of IMAP data items not used in campaigns (see table madata.imap_dataitems_not_used)";
		value=&value_imap_dataitems_not_used;
		output;
	end;
run;
proc delete data=WORK.data_item_usage WORK.DATA_ITEMS_BY_CAMPAIGN; run;

%if &export_key_tables_to_csv. %then %do;
	%DS2CSV(colhead=Y,runmode=B,openmode=REPLACE
			,data=madata.stored_processes_used 
			,csvfile=&dataFolder./stored_processes_used.csv);
	%DS2CSV(colhead=Y,runmode=B,openmode=REPLACE
			,data=madata.imap_dataitems_used 
			,csvfile=&dataFolder./imap_dataitems_used.csv);
	%DS2CSV(colhead=Y,runmode=B,openmode=REPLACE
			,data=madata.imap_dataitems_not_used 
			,csvfile=&dataFolder./imap_dataitems_not_used.csv);
	%DS2CSV(colhead=Y,runmode=B,openmode=REPLACE
			,data=madata.campaign_summary 
			,csvfile=&dataFolder./campaign_summary.csv);
%end;
/********************/
/* Summpary report */
/********************/
title 'Key metrics';
proc sql; select * from madata.campaign_summary; quit;
title;
