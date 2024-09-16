/*-----------------------------------------------------------------------------
 Copyright © 2024, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 SPDX-License-Identifier: Apache-2.0
-----------------------------------------------------------------------------*/

/*****************************/ 
/* Program: 2_get_campaigns_data.sas
/* Input:
	-	macro variables of 0_environment_parameters.sas
	-	mapo.map in the location specified by &xmlmap. 
	-	madata.meta_campaign table 
/* Output: 
	-	madata.campaign             
	-	madata.linknode             
	-	madata.Communicationlineitem
	-	madata.Multiselectvar       
	-	madata.node                 
	-	madata.noderelation         
	-	madata.codenode             
	-	madata.selectnodevar
	-	madata.ABTestNodeVars     
	-	madata.LimitNodeVars      
	-	madata.PrioritizeNodeVars 
	-	madata.MapNode         	 
	-	madata.ExportNodeVars     
	-	madata.SplitNode          
	-	madata.all_dataitems_in_nodes
/*****************************/ 
/* Filter to test, specify a campaign name, as this process takes about 5s to run */
%let campaignfilter=where index(fullpath,"&metadata_root_folder")=1; /* one BC */
%*let campaignfilter=where name = "SAS for Finance Deposit Acccount Cross Sell Campaign"; /* one campaign - for testing */

%macro mapo2xml(source, xml_file);
  &OS_specific_options.;
  /* If javecmd is quoted on windows you get "The system cannot find the path specified." */
  filename cmd pipe "&JAVACMD. -cp ""&classpath."" MAPO2Xml &uid. &pass. ""&source."" ""&xml_file.""";
  data _null_; 
    infile cmd truncover; 
    input; 
    put _infile_; 
  run;
  filename cmd clear;
%mend;

%macro xml2data(xmlFile);
	libname mapo xmlv2 "&xmlfile." XMLMAP="&xmlmap.";
	proc append base=madata.campaign              data=mapo.campaign;              run; 
	proc append base=madata.linknode              data=mapo.linknode;              run; 
	proc append base=madata.Communicationlineitem data=mapo.Communicationlineitem; run; 
	proc append base=madata.Multiselectvar        data=mapo.Multiselectvar;        run; 
	proc append base=madata.node                  data=mapo.node;                  run; 
	proc append base=madata.noderelation          data=mapo.noderelation;          run; 
	proc append base=madata.codenode              data=mapo.codenode;              run; 
	proc append base=madata.selectnodevar         data=mapo.selectnodevar;         run; 
	proc append base=madata.ABTestNodeVars        data=mapo.ABTestNodeVars;        run; 
	proc append base=madata.LimitNodeVars         data=mapo.LimitNodeVars;         run; 
	proc append base=madata.PrioritizeNodeVars    data=mapo.PrioritizeNodeVars;    run; 
	proc append base=madata.MapNode         	  data=mapo.MapNode;    		   run; 
	proc append base=madata.ExportNodeVars     	  data=mapo.ExportNodeVars;        run; 
	proc append base=madata.SplitNode         	  data=mapo.SplitNode;         	   run; 
	libname mapo clear;

	data all_dataitems_in_nodes(drop=line);
		length campaign_id _nodeId $36 _nodeName _varInfoId _varName $80 line $1000 ;
		retain campaign_id _nodeId _nodeName _varInfoId;
		infile "&xmlFile.";
		input;
		line=_infile_;
		if index (line,'<PersistenceDO ') then campaign_id=scan(scan("&xmlFile.",-1,'/'),1,'.');
		if index (line,'<_nodeId type="String">') then _nodeId=scan(scan(line,2,'>'),1,'<'); 
		if index (line,'<_nodeName type="String">') then _nodeName=scan(scan(line,2,'>'),1,'<'); 
		if index (line,'<_varInfoId type="String">') then _varInfoId=scan(scan(line,2,'>'),1,'<');
		if index (line,'<_varName type="String">') then do; 
			_varName=scan(scan(line,2,'>'),1,'<');
			output;
		end;
	run;
	proc append base=madata.all_dataitems_in_nodes data=all_dataitems_in_nodes; run;
%mend;

/* SET LOG LOCATION */
proc printto log="&utillityFolder./logs/2_get_campaign_data_%left(%sysfunc(datetime(),B8601DT15.)).log"; 
run;

/* loop campaigns and generate xml */
filename sascode temp;
data _null_;
  set madata.meta_campaign;
  	file sascode;
	source  = catx("/", "&davroot.", substr(mapo_id, 1, 2), substr(mapo_id, 3, 2), mapo_id);
	xmlfile = "&xmlFolder/" || strip(mapo_id) || ".xml";
	if not fileexist(xmlfile) or &xml_file_replace=1 then do; 
		put '%mapo2xml(' source ', ' xmlFile ');';
	end;
	&campaignfilter.; /* see top */
run;
/*data _null_; infile sascode; input; put _infile_; run;*/
%include sascode;
filename sascode; 

/* Delete data from a previous run */
proc delete data=
	madata.campaign             
	madata.linknode             
	madata.Communicationlineitem
	madata.Multiselectvar       
	madata.node                 
	madata.noderelation         
	madata.codenode             
	madata.selectnodevar        
	madata.ABTestNodeVars       
	madata.LimitNodeVars        
	madata.PrioritizeNodeVars   
	madata.MapNode         		
	madata.ExportNodeVars     	
	madata.SplitNode         
	madata.all_dataitems_in_nodes	
	;
quit;

filename sascode temp;
data _null_;
	set madata.meta_campaign;
  	file sascode;
	xmlfile = "&xmlFolder/" || strip(mapo_id) || ".xml";
	if fileexist(xmlfile) then do; 
		put '%xml2data(' xmlfile ');';  
	end;
	&campaignfilter.;
run;
/*data _null_; infile sascode; input; put _infile_; run;*/
%include sascode;
filename sascode; 
proc delete data=WORK.ALL_DATAITEMS_IN_NODES;
quit;
/*****************************************************************************
	RESET LOG LOCATION
******************************************************************************/
proc printto; run;

data madata.campaign;
 set madata.campaign;
	length full_folder_path $356;
   if _parentFolder ne '' 
	then full_folder_path=catx('\',_parentFolder,_folder);
	else full_folder_path=_folder;
run;




