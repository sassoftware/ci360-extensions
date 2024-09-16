/*-----------------------------------------------------------------------------
 Copyright © 2024, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 SPDX-License-Identifier: Apache-2.0
-----------------------------------------------------------------------------*/

/*****************************/ 
/* Program: 6_link_node_analysis.sas
/* Input:
	-	macro variables of 0_environment_parameters.sas
	-	madata.campaign             
	-	madata.linknode             
	-	madata.Multiselectvar       
	-	madata.selectnodevar        
	-	BC_ID from 4. Calculated_Items_And_Business_Contexts.sas
/* Output: 
	-	MADATA.CampaignImportOrder
	-	madata.CALCULATED_ITEMS_Used             
/*****************************/ 

PROC SQL;
   CREATE TABLE CAMPAIGNS_WITH_LINK_NODES AS 
   SELECT t1.full_folder_path	AS Link_Node_Parent_Folder 
         ,t1._folder  		 	AS Link_Node_Campaign_Folder
         ,t1._name 				AS Link_Node_Campaign_Name 
         ,t1.flow_id 			AS Link_Node_Flow_Id
         ,t2.campaign_id 		AS Link_Node_Campaign_Id
         ,t2._nodeName 			AS Link_Node_Name
         ,t2._nodeCode 			AS Link_Node_Code 
         ,t2._flowparentFolder 	AS Linkable_Cell_Parent_Folder
         ,t2._flowFolder 		AS Linkable_Cell_Campaign_Folder
         ,t2._flowName 			AS Linkable_Cell_Campaign_Name
         ,t2._cellName 			AS Linkable_Cell_Name
         ,t2._flowId 			AS Linkable_Cell_Flow_Id
         ,t3.Id 				AS Linkable_Cell_Campaign_Id
	FROM MADATA.CAMPAIGN t1
	INNER JOIN MADATA.LINKNODE t2 ON (t1.id = t2.campaign_id)
	LEFT JOIN MADATA.CAMPAIGN T3 ON(t2._flowID = t3.flow_id);
QUIT;


PROC SQL;
	CREATE TABLE MADATA.CAMP_0_UNLINKED AS 
	SELECT *
		, "Campaigns without Link Nodes or linked Cells (see table madata.CAMP_0_UNLINKED)" as label length=100
	FROM MADATA.CAMPAIGN t1
	Where t1.id not in (select Linkable_Cell_Campaign_Id from CAMPAIGNS_WITH_LINK_NODES )
	  and t1.id not in (select Link_Node_Campaign_Id from CAMPAIGNS_WITH_LINK_NODES );
QUIT;
data campaign_link_overview;
	set MADATA.CAMP_0_UNLINKED(keep=id label);
run;
%if &export_key_tables_to_csv. %then %do;
	%DS2CSV(colhead=Y,runmode=B,openmode=REPLACE
			,data=madata.CAMP_0_UNLINKED 
			,csvfile=&dataFolder./camp_0_unlinked.csv);
%end;

PROC SQL;
	CREATE TABLE CAMP_DELTA AS 
	SELECT *
	FROM MADATA.CAMPAIGN t1
	Where t1.id not in (select ID from MADATA.CAMP_0_UNLINKED );
QUIT;
PROC SQL;
	CREATE TABLE MADATA.CAMP_1_LINKED AS 
	SELECT *
		, "Campaigns with leaf linked cells (see table madata.CAMP_1_LINKED)" as label length=100
	FROM CAMP_DELTA t1
	Where t1.id in (select Linkable_Cell_Campaign_Id from CAMPAIGNS_WITH_LINK_NODES )
	  and t1.id not in (select Link_Node_Campaign_Id from CAMPAIGNS_WITH_LINK_NODES );
QUIT;

%macro recursion;
	%let remaining_campaigns=1;
	%let lev=1;
	/* initialize */
	%do %while (&remaining_campaigns.>0 and &lev.< 100); /* result not reached */

		%if &export_key_tables_to_csv. %then %do;
			%DS2CSV(colhead=Y,runmode=B,openmode=REPLACE
					,data=madata.CAMP_0_UNLINKED 
					,csvfile=&dataFolder./camp_&lev._linked.csv);
		%end;

		proc append base=campaign_link_overview data=MADATA.CAMP_&lev._LINKED(keep=id label);
		run;

		PROC SQL;
			DELETE FROM CAMP_DELTA 
			Where id in (select id from MADATA.CAMP_&lev._LINKED);
		QUIT;

		PROC SQL;
			CREATE TABLE MADATA.CAMP_%eval(&lev.+1)_LINKED AS 
			SELECT *
				, "Campaigns with level %eval(&lev.+1) linked cells (see table madata.CAMP_%eval(&lev.+1)_LINKED)" as label length=100
			FROM CAMP_DELTA t1
			Where t1.id in (select Link_Node_Campaign_Id 
			        from CAMPAIGNS_WITH_LINK_NODES
					where Linkable_Cell_Campaign_Id in (select id from MADATA.CAMP_&lev._LINKED)
					)
			;
		QUIT;
		%let remaining_campaigns=&sqlobs;
		%let lev=%eval(&lev+1);

		%put &=remaining_campaigns;
		%put &=lev;
	%end;
	proc delete data=MADATA.CAMP_&lev._LINKED; run; /* delete obsolete, empty level */
%mend;
%recursion

proc sql;
	create table link_summary as
	select label
		, count(id) as value
	from campaign_link_overview
	group by label;
run;
proc append base=madata.campaign_summary data=work.link_summary;
run;
proc delete data=
	CAMPAIGNS_WITH_LINK_NODES
	CAMP_DELTA
	campaign_link_overview
	link_summary
	;
run;


%if &export_key_tables_to_csv. %then %do;
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




