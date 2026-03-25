/*
Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/
/******************************************************************************

Waterfall report showing Initial and Match/Excluded counts produced.
This process sends an email with the report attached to the email address you define during execution.

******************************************************************************/


%stpbegin;
 filename outdata temp lrecl=32767;
 
%binary_file_copy_cust( infile=livedata, outfile=outdata );
 
%include outdata;
 
filename outdata clear;
%maspinit(xmlstream=macroVar neighbor);
 
options nomprint nomlogic;

/* 
This stored procedure saves accepts multiple input MAtables that are specified to represent:
1- Universe Audience from which other groups should be excluded. 
2-   Some number of additional input nodes each of which can represent SubjectIDs to Match or exclude from Universe Audience (input node1)
3- only 1 MA Outtable is created representig the final Audience (Based on all Matchs/exclusions).
A waterfall report is created showing drop counts due to each of the input nodes.

*/
proc sql noprint;
select trim(value) into :campcode from work.macrovar
where (Category='SEGMENTMAPINFO' and Name='SEGMENTMAPCODE')
or (Category='CAMPAIGNINFO' and Name='CAMP_CODE');
quit;

/* setup Savelib as work for debug*/
%let savelib = work;

%global uploadtablename TOTALAUDIENCE_NUM excl_num  MustExcl_cnt add_exl Inital_audience_Cnt;
%global    lib_name nodename  directory RPTNAME camp_name base_directory slash;
%let IdVars = &SubjectKeys.;
/* default values for base_directory and slash --- change as appropriate for environment */
%let base_directory = &ReportFolder.;
%let slash=/;

%put "TypeAnd=" &typeAnd. 'XYZ';


%macro And_Matables;

%do i9 = 1 %to &intable0; 
/* for 1st through last priority (number of MAinput tables), Upload Intable rows into sasdataset table 
and then exclude based on Subjects in lower priority nodes
put results into Outtable
*/


%if &i9. = 1 %then %do;
	proc sql UNDO_POLICY=NONE;
	select name into :nodename from work.inputnodes where tablename="&intable1.";
	  insert into  &savelib..MainTable 
	  select &i9. as node_order,  "&nodename." as Node, I.&IdVars.
	  from  &intable1 I;
	  Create table &savelib..nodeMatchs as 
	  	Select &i9. as node_order,  "&nodename." as Node length 32, I.&IdVars.
	  	from  &&intable&i9 I ;
	  Create table &savelib..waterfall_MatchInitialIDs as
	  	 select &i9. as node_order,  "&nodename." as Node length 32, m.&IdVars.
		from &savelib..maintable m;
	  select count(*) into :TotalAudience_num from &&intable&i9 I;
	quit;
      %end;
%else  %do;
	Proc sql  UNDO_POLICY=NONE;
	select name into :nodename from work.inputnodes where tablename="&&intable&i9.";
	 insert into &savelib..includes 
	  Select &i9. as node_order,  "&nodename." as Node, I.&IdVars.
	  from  &&intable&i9 I ;
	%if %trim("&TypeAnd.") = "Match" %then %do;
	 Insert into &savelib..nodeMatchs
	  Select  &i9. as node_order,  "&nodename." as Node, I.&IdVars.
	   from  &&intable&i9 I 
	   join &savelib..nodeMatchs m on I.&IdVars. = m.&IdVars. and m.node_order = &i9. -1;
	 insert into &savelib..waterfall_MatchInitialIDs
	  select &i9. as node_order,  "&nodename." as Node,  m.&IdVars.
	  	from &savelib..maintable m
		inner join &&intable&i9 d on m.&IdVars. = d.&IdVars.;
	  %end;
	  %else %do;  /* only insert rows from above nodeMatch that do not match InputN Matable */
	  Insert into &savelib..nodeMatchs
	     Select  &i9. as node_order,  "&nodename." as Node, M.&IdVars.
	  	  from  &&intable&i9 I 
	   	right join &savelib..nodeMatchs m on I.&IdVars. = m.&IdVars. and m.node_order = &i9. -1
	   	where i.&IdVars. is missing and m.node_order = &i9. -1;
	  insert into &savelib..waterfall_MatchInitialIDs
	     select &i9. as node_order,  "&nodename." as Node,  m.&IdVars.
	   	from &savelib..maintable m
	   	left join  &&intable&i9 d on m.&IdVars. = d.&IdVars.
		where d.&IdVars. is missing;
	   %end;
	quit;
	
      %end;


%end;  /* End of %do I9 loop */

%mend And_matables;



proc sql outobs=0;
/* create empty Work tables (outobs=0)
*/
  create table &savelib..MainTable as 
	  select 0 as node_order, '                                ' as node length 60,  i.&IdVars.
	  from &intable1 i ;
  create table &savelib..Includes as 
  	  select 0 as node_order, '                                ' as node length 60, I.&IdVars.
  	  from  &intable1 i ;
quit;

%And_matables;



/* Create outtable1 from final NodeMatch row  */
Proc sql UNDO_POLICY=NONE;

Create table &outtable1. as 
	Select distinct m.&IdVars. from &savelib..nodeMatchs M
	where node_order=&intable0. ;
Quit;	

/* Below is code for Report creation */


/* checkit macro checks if directory exists, if not create subdirectory  */

%MACRO CHECKIT;
%let filrf=MYDIR;
%let rc=%sysfunc(filename(filrf,&base_directory.&slash.&CAMPCODE.));
%put rc= &rc.;
%let did=%sysfunc(dopen(&filrf));
%put did= &DID.;
%if &did. = 1 %then %do;
	%let numopts=%sysfunc(doptnum(&did));
	%put numopts= &numopts.;
	%let foption=%sysfunc(doptname(&did,&numopts));
	%put foption= &foption.;
	%let charval=%sysfunc(dinfo(&did,&foption));
	%put charval= &charval.;
	%let rc=%sysfunc(dclose(&did));
	%END;
%ELSE %do;
  Data _null_;
	NewDirectory=dcreate("&CAMPCODE.","&base_directory.&slash.");
	call symput('NewDirectory',NewDirectory);
   run;
  %put NewDirectory=&NewDirectory.;
  %end;
%put rc= &rc.;

%MEND;

%CHECKIT;

%macro ODS_open;
	%let directory=&base_directory.&slash.&CAMPCODE.;
	%if %trim("&output_type") = "PDF" %then %do;
		ODS PDF file="&directory./&rptName._&sysdate..PDF";
		%let RPTNAME=&rptName._&sysdate..PDF;
		%end;
	%if %trim("&output_type") = "RTF" %then %do;
		ODS RTF file="&directory./&rptName._&sysdate..RTF";
		%let RPTNAME=&rptName._&sysdate..RTF;
		%end;
	
	%if %trim("&output_type") = "HTML" %then %do;
		%let html_body=&rptName._&sysdate..html;
		ods HTML path="&directory."
		body="&html_body";
		%let RPTNAME=&rptName._&sysdate..html;
		%end;
	%if %trim("&output_type") = "XLS" %then %do;
			ods excel file="&directory./&rptName._&sysdate..xlsx";
			%let RPTNAME=&rptName._&sysdate..xlsx;
	%end;
	
%mend ods_open;

%macro ODS_close;
	%if %trim("&output_type") = "PDF" %then 
		ODS PDF Close;
	%if %trim("&output_type") = "RTF" %then 
		ODS RTF Close;
	%if %trim("&output_type") = "HTML" %then 	
		ods HTML close;
		
	%if %trim("&output_type")= "XLS" %then %do;
		ods tagsets.excelxp close;
	    %end;
	
/* XLSX file does not seem to close until process ends so removing the option as output type parameter */
%mend ods_close;


/* Moved Email Address into STP Parm instead of retrieving from Campaign Custom Detail*/
proc sql noprint;
select trim(value) into :camp_name
from work.macrovar
where (Category='SEGMENTMAPINFO' and Name='SEGMENTMAPNAME')
or (Category='CAMPAIGNINFO' and Name='CAMP_NAME');
quit;


proc sql noprint UNDO_POLICY=NONE;
create table &savelib..waterfall_NodeCnt as
select d.node_order label 'Node Order' , d.node length 32 label 'Node', in.description label 'Description', count(*) as Initial_Row_Cnt  format comma9. label 'Count of Subjects for Input Node'
from &savelib..mainTable d /*left*/ join work.inputnodes in on d.node=in.name
where in.name not = 'abc'
group by 1,2,3;
Insert into &savelib..waterfall_NodeCnt 
	select d.node_order, d.node, in.description, count(*) as Initial_Row_Cnt  
	from &savelib..includes d /*left full*/  join work.inputnodes in on d.node=in.name
	where in.name not = 'abc'
group by 1,2,3;

select Initial_Row_Cnt format 9. into :Inital_audience_Cnt from &savelib..waterfall_NodeCnt where node_order=1;
quit;
%put Inital_audience_Cnt=&Inital_audience_Cnt. ;

proc sql noprint UNDO_POLICY=NONE;	
		
Create table &savelib..waterfall_MatchInitialCnt as
	select M.node_order, M.node, in.description, count(*) as MatchInitialAud_Row_Cnt  format comma9. label "Initial Node count after &typeAnd. of this Node"
		from &savelib..waterfall_MatchInitialIDs m
		/*left full*/  join work.inputnodes in on m.node=in.name
	where in.name not = 'abc'
	group by 1,2,3;
	
	
Create table &savelib..waterfall_MatchAboveNodeCnt as
	select M.node_order, M.node, in.description, count(*) as MatchsAbove_Row_Cnt  format comma9. label "Above Node count after &typeAnd. of this Node"
		from &savelib..nodeMatchs m
		/*left */  join work.inputnodes in on m.node=in.name
	where in.name not = 'abc'
	group by 1,2,3;
	
update &savelib..waterfall_MatchAboveNodeCnt
	set MatchsAbove_Row_Cnt = 0 
	where MatchsAbove_Row_Cnt is missing;

create table &savelib..waterfall as
select Initial.*, MatchInitial.MatchInitialAud_Row_Cnt,
	&Inital_audience_Cnt. - MatchInitial.MatchInitialAud_Row_Cnt as drop_FromInitial_Cnt  format comma9. label 'Drop Count from Inital Node'
	,MatchAbove.MatchsAbove_Row_Cnt 
	, MatchAbove2.MatchsAbove_Row_Cnt - MatchAbove.MatchsAbove_Row_Cnt as drop_fromAboveRow_Cnt  Format comma9. label 'Drop Count from above Node'
from &savelib..waterfall_NodeCnt Initial
left join &savelib..waterfall_MatchInitialCnt MatchInitial on MatchInitial.node_order =Initial.node_order
left join &savelib..waterfall_MatchAboveNodeCnt MatchAbove on MatchAbove.node_order = Initial.Node_Order
left join &savelib..waterfall_MatchAboveNodeCnt MatchAbove2 on MatchAbove.node_order -1 = MatchAbove2.node_order 
order by initial.node_order;
;
quit;

%ods_open;
ODS GRAPHICS ON;

/* */

TITLE;
TITLE1 "Waterfall Report for Segment Map - &camp_name";
TITLE3 "Results based on &typeAnd. ALL Node Order >=2 from Node Order 1";

FOOTNOTE;
FOOTNOTE1 "Generated by the SAS System  on %TRIM(%QSYSFUNC(DATE(), NLDATE20.)) at %TRIM(%SYSFUNC(TIME(), TIMEAMPM12.))";


  proc report data=&savelib..waterfall nowd headline headskip missing split='*'
 style(header)=[foreground=cxFFFFFF background=CX034f85
                /*font_face="verdana" font_size=2*/]

  style(column)=[foreground=black background=cxffffff
               /*font_face="verdana" font_size=2*/ ]
 
  style(summary)=[foreground=CX000000 background=cxF2F2F2
                /*font_face="verdana" font_size=2 */just=r];

column  node_order node description Initial_Row_Cnt MatchInitialAud_Row_Cnt drop_FromInitial_Cnt MatchsAbove_Row_Cnt drop_fromAboveRow_Cnt;

define node_order / center "Node*Order";
define node / left "Node";
define description / left "Description";
define Initial_Row_Cnt / right "Count of*Subjects*for Input*Node" format=comma12.;
define MatchInitialAud_Row_Cnt / right "Initial*Node*count after*&typeAnd. of*this Node" format=comma12.;
define drop_FromInitial_Cnt / right "Drop*Count*from Initial*Node" format=comma12.;
define MatchsAbove_Row_Cnt / right "Above*Node*count after*&typeAnd. of*this Node" format=comma12.;
define drop_fromAboveRow_Cnt / right "Drop*Count*from Above*Node" format=comma12.;
run;
 
/* -------------------------------------------------------------------
   End of task code.
   ------------------------------------------------------------------- */
RUN; QUIT;

TITLE; FOOTNOTE;
RUN; QUIT;


%ods_close;

/* Now put 0 rows into any  outtables other than 1 (only existis due to defect)*/
%macro RestOfMAtables;
%do i=2 %to &outtable0;
Proc sql outobs=0;
Create table &&outtable&i as 
(select &IdVars. from &intable1);
quit;

%end; /* end of %Do for outtable processing */
%mend RestOfMATables;

%RestOfMAtables;	


/* Send report in email to Campaign Audit user */
%include '/sas/ci/common/macros/sendmail.sas';

%macro build_Email;
Proc sql;
create table work.maillist (mailname char(40), mailto char(50), mailfrom char(60), subjecttext char(60), line1 Char(80), line2 char(80), line3 char(60), signoff char(40));
quit;
/*SM  Set to 1 until multiple parm values are supported */
 %let emailAddress_Count=1;
%if &emailAddress_Count=0 %then %return;
Proc sql;
insert into work.maillist values ("&emailAddress", "&emailAddress", "sasemail@cidemo.sas.com", "&rptName. Report for &campcode. ","This is a Sample Engage Direct email", "For SegmentMap: &camp_name.","Campaign Code: &Campcode.", "-CI Administrator");
quit;
%do i=2  %to &emailAddress_Count;
Proc sql;
insert into work.maillist values ("&&emailAddress&i.", "&&emailAddress&i", "sasemail@cidemo.sas.com", "&rptName. Report for &campcode. ","This is a Sample Engage Direct email", "For Campaign: &camp_name.","Campaign Code: &Campcode.", "-CI Administrator");
quit;
%end;
/* now send the email */
%if &emailAddress_Count>0 %then 

%sendmail(sendlist=work.maillist, folder=&directory., file=&rptname.);

%mend;

%build_Email;
	

%macount(&outTable);
%MAStatus(&_stpwork.status.txt);


%stpend;
