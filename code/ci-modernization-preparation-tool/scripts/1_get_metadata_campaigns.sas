/*-----------------------------------------------------------------------------
 Copyright © 2024, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 SPDX-License-Identifier: Apache-2.0
-----------------------------------------------------------------------------*/

/*****************************/ 
/* Program: 1. get_metadata_campaigns.sas
/* Input: Requires macro variables of 0_environment_parameters.sas
/* Output: meta_campaign dataset
/*****************************/ 

filename meta_in temp;
filename meta_out temp;
/*filename meta_out "&dataFolder./out.xml";*/
filename map temp;

data _null_;
  file meta_in;
  put "<GetMetadataObjects>";
  put "<Type>TransformationActivity</Type>";
  put "<Objects/>";
  put "<Reposid>$METAREPOSITORY</Reposid>";
  put "<NS>SAS</NS>";
  put "<Flags>OMI_GET_METADATA+OMI_XMLSELECT+OMI_TEMPLATE</Flags>";
  put "<Options>";
  put "<Templates>";
  put "  <TransformationActivity Name='' isActive='' PublicType='' TransformRole='' MetadataCreated='' MetadataUpdated=''><Extensions/><Trees/></TransformationActivity>";
  put "  <Tree Name='' />";
  put "  <Extension Name='' DefaultValue=''/>";
  put "</Templates>";
  OMR_QUERY = """TransformationActivity[@TransformRole='CICampaign' OR @PublicType='CIDiagram']""";
  put "<XMLSelect search=" OMR_QUERY "/>";
  put "</Options>";
  put "</GetMetadataObjects>";
run;

/* send xml to metadata server */
options metaserver="&metaserver." metaport=&metaport. metauser="&uid." metapass="&pass."
        metaprotocol=bridge metarepository="Foundation";
proc metadata in=meta_in out=meta_out repository="Foundation" header=full;
run;

/* map */
data _null_;
  file map;
  put '<?xml version="1.0" ?>';
  put '<SXLEMAP name="SXLEMAP" version="2.1">';
  put '<NAMESPACES count="0"/>';
  put '<TABLE name="campaign">';
  put '  <TABLE-PATH syntax="XPath">/GetMetadataObjects/Objects/TransformationActivity</TABLE-PATH>';
  put '  <COLUMN name="mapo_id">';
  put '    <PATH syntax="XPath">/GetMetadataObjects/Objects/TransformationActivity/Extensions/Extension/@Name</PATH>';
  put '    <TYPE>character</TYPE><DATATYPE>string</DATATYPE><LENGTH>20</LENGTH>';
  put '  </COLUMN>';
  put '  <COLUMN name="metadata_id">';
  put '    <PATH syntax="XPath">/GetMetadataObjects/Objects/TransformationActivity/@Id</PATH>';
  put '    <TYPE>character</TYPE><DATATYPE>string</DATATYPE><LENGTH>20</LENGTH>';
  put '  </COLUMN>';
  put '  <COLUMN name="name">';
  put '    <PATH syntax="XPath">/GetMetadataObjects/Objects/TransformationActivity/@Name</PATH>';
  put '    <TYPE>character</TYPE><DATATYPE>string</DATATYPE><LENGTH>128</LENGTH>';
  put '  </COLUMN>';
  put '  <COLUMN name="isactive">';
  put '    <PATH syntax="XPath">/GetMetadataObjects/Objects/TransformationActivity/@IsActive</PATH>';
  put '    <TYPE>numeric</TYPE><DATATYPE>integer</DATATYPE>';
  put '  </COLUMN>';
  put '  <COLUMN name="PublicType">';
  put '    <PATH syntax="XPath">/GetMetadataObjects/Objects/TransformationActivity/@PublicType</PATH>';
  put '    <TYPE>character</TYPE><DATATYPE>string</DATATYPE><LENGTH>64</LENGTH>';
  put '  </COLUMN>';
  put '  <COLUMN name="TransformRole">';
  put '    <PATH syntax="XPath">/GetMetadataObjects/Objects/TransformationActivity/@TransformRole</PATH>';
  put '    <TYPE>character</TYPE><DATATYPE>string</DATATYPE><LENGTH>64</LENGTH>';
  put '  </COLUMN>';
  put '  <COLUMN name="created">';
  put '    <PATH syntax="XPath">/GetMetadataObjects/Objects/TransformationActivity/@MetadataCreated</PATH>';
  put '    <TYPE>numeric</TYPE><DATATYPE>datetime</DATATYPE><INFORMAT>datetime</INFORMAT><FORMAT>e8601dt</FORMAT>';
  put '  </COLUMN>';
  put '  <COLUMN name="updated">';
  put '    <PATH syntax="XPath">/GetMetadataObjects/Objects/TransformationActivity/@MetadataUpdated</PATH>';
  put '    <TYPE>numeric</TYPE><DATATYPE>datetime</DATATYPE><INFORMAT>datetime</INFORMAT><FORMAT>e8601dt</FORMAT>';
  put '  </COLUMN>';
  put '  <COLUMN name="folder">';
  put '    <PATH syntax="XPath">/GetMetadataObjects/Objects/TransformationActivity/Trees/Tree/@Name</PATH>';
  put '    <TYPE>character</TYPE><DATATYPE>string</DATATYPE><LENGTH>64</LENGTH>';
  put '  </COLUMN>';
  put '  <COLUMN name="folder_id">';
  put '    <PATH syntax="XPath">/GetMetadataObjects/Objects/TransformationActivity/Trees/Tree/@Id</PATH>';
  put '    <TYPE>character</TYPE><DATATYPE>string</DATATYPE><LENGTH>20</LENGTH>';
  put '  </COLUMN>';
  put '</TABLE>';
  put '</SXLEMAP>';
run;

libname maplib xmlv2 xmlfileref=meta_out xmlmap=map access=readonly;
data madata.meta_campaign;
  set maplib.campaign;
  if length(folder) > 1;
run;
libname maplib clear;




data madata.meta_campaign(drop=treeid uri parent_uri parent_nm folder_id);
 set madata.meta_campaign;
	length treeid uri parent_uri fullpath $500 parent_nm $200;
	treeid=folder_id;
	parent_nm = folder;
	nobj=metadata_getnobj("omsobj:tree?@id='"||treeid||"'",1,uri);
	rc=metadata_getattr(uri,"Name",fullpath);
	parent_rc=metadata_getnasn(uri,"parenttree",1,parent_uri);
	do while( parent_rc ge 1);
		rc=metadata_getattr(parent_uri,"Name",parent_nm);
		fullpath = catx("/",parent_nm,fullpath);
		uri=parent_uri;
		parent_rc=metadata_getnasn(uri,"parenttree",1,parent_uri);
	end;
	fullpath = "/" || fullpath;
run;
