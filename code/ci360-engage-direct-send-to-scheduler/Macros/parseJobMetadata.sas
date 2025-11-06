/**********************************************************************************************************************
PROGRAM: SAS CUSTOMER INTELLIGENCE 360 Custom On-Prem Scheduler for Direct Marketing Tasks, Bulk Email Tasks and Segment Maps
DESCRIPTION: This application helps the CI 360 User to create deployed jobs from the CI 360 tenant 
             for Direct Marketing Tasks, Bulk Email Tasks and Direct Segment Maps for a SAS Administrator 
             to schedule on-prem in the SAS LSF scheduler.
FILE NAME: parsejobMetadata.sas
DESCRIPTION: Parses output from job metadata query 
VERSION: 2.0
DATE MODIFIED: 25-APRIL-2024
AUTHOR: GLOBAL CUSTOMER INTELLIGENCE ENABLEMENT TEAM

#Copyright � 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0
**********************************************************************************************************************/

%macro parsejobMetadata(TABLE=, XML_FILEREF=, XML_PATH=, DEPTH=14);
  data _null_;
    call symputx("XML_PATH", symget("XML_PATH"));
  run;

  /* xml map */
  filename map temp;
  data _null_;
    length path $4096 pn $32;
    file map encoding="utf-8";
    put '<SXLEMAP version="2.1">';
    put '  <NAMESPACES count="0"/>';
    put '  <TABLE name="JFJob">';
    put "    <TABLE-PATH syntax=""XPath"">%superq(XML_PATH)/JFJob</TABLE-PATH>";
    put '    <COLUMN name="jobName">';
    put "      <PATH syntax=""XPath"">%superq(XML_PATH)/JFJob/@Name</PATH>";
    put '      <TYPE>character</TYPE>';
    put '      <DATATYPE>string</DATATYPE>';
    put '      <LENGTH>128</LENGTH>';
    put '    </COLUMN>';
    put '    <COLUMN name="createdDttm">';
    put "      <PATH syntax=""XPath"">%superq(XML_PATH)/JFJob/@MetadataCreated</PATH>";
    put '      <TYPE>numeric</TYPE>';
    put '      <DATATYPE>datetime</DATATYPE>';
    put '      <FORMAT width="19">e8601dt</FORMAT>';
    put '      <INFORMAT width="32">ANYDTDTM</INFORMAT>';
    put '    </COLUMN>';
    put '    <COLUMN name="deployDirectory">';
    put "      <PATH syntax=""XPath"">%superq(XML_PATH)/JFJob/SourceCode/File/Directories/Directory/@DirectoryName</PATH>";
    put '      <TYPE>character</TYPE>';
    put '      <DATATYPE>string</DATATYPE>';
    put '      <LENGTH>512</LENGTH>';
    put '    </COLUMN>';
    put '    <COLUMN name="deployFile">';
    put "      <PATH syntax=""XPath"">%superq(XML_PATH)/JFJob/SourceCode/File/@FileName</PATH>";
    put '      <TYPE>character</TYPE>';
    put '      <DATATYPE>string</DATATYPE>';
    put '      <LENGTH>128</LENGTH>';
    put '    </COLUMN>';
    path = "%superq(XML_PATH)/JFJob/Trees/Tree";
    do n = 0 to %superq(DEPTH);
      pn = cats('deployPath', n);
      put '    <COLUMN name="' pn +(-1) '">';
      put '      <PATH syntax="XPath">' path +(-1) '/@Name</PATH>';
      put '      <TYPE>character</TYPE>';
      put '      <DATATYPE>string</DATATYPE>';
      put '      <LENGTH>128</LENGTH>';
      put '    </COLUMN>';
      path = cats(path, '/ParentTree/Tree');
    end;
    put '    <COLUMN name="jobDirectory">';
    put "      <PATH syntax=""XPath"">%superq(XML_PATH)/JFJob/AssociatedJob/Job/SourceCode/File/Directories/Directory/@DirectoryName</PATH>";
    put '      <TYPE>character</TYPE>';
    put '      <DATATYPE>string</DATATYPE>';
    put '      <LENGTH>512</LENGTH>';
    put '    </COLUMN>';
    put '    <COLUMN name="jobFile">';
    put "      <PATH syntax=""XPath"">%superq(XML_PATH)/JFJob/AssociatedJob/Job/SourceCode/File/@Name</PATH>";
    put '      <TYPE>character</TYPE>';
    put '      <DATATYPE>string</DATATYPE>';
    put '      <LENGTH>128</LENGTH>';
    put '    </COLUMN>';
    put '    <COLUMN name="associatedJobName">';
    put "      <PATH syntax=""XPath"">%superq(XML_PATH)/JFJob/AssociatedJob/Job/@Name</PATH>";
    put '      <TYPE>character</TYPE>';
    put '      <DATATYPE>string</DATATYPE>';
    put '      <LENGTH>128</LENGTH>';
    put '    </COLUMN>';
    path = "%superq(XML_PATH)/JFJob/AssociatedJob/Job/Trees/Tree";
    do n = 0 to %superq(DEPTH);
      pn = cats('jobPath', n);
      put '    <COLUMN name="' pn +(-1) '">';
      put '      <PATH syntax="XPath">' path +(-1) '/@Name</PATH>';
      put '      <TYPE>character</TYPE>';
      put '      <DATATYPE>string</DATATYPE>';
      put '      <LENGTH>128</LENGTH>';
      put '    </COLUMN>';
      path = cats(path, '/ParentTree/Tree');
    end;
    put '  </TABLE>';
    put '</SXLEMAP>';
  run;

  /* parse result */
  libname meta_out xmlv2 xmlmap=map xmlfileref=%superq(XML_FILEREF) access=readonly;
  data %superq(TABLE);
    length jobCreated deployedMetadata deployedCode jobMetadata jobCode $512;
    keep jobName jobCreated deployedMetadata deployedCode jobMetadata jobCode;
    set meta_out.jfjob;
    jobCreated       = translate(put(tzoneu2s(createdDttm), E8601DT19.), " ", "T");
    deployedMetadata = '/' || catx('/', of deployPath&DEPTH.-deployPath0, jobName);
    deployedCode     = catx("\", deployDirectory, deployFile);
    jobMetadata      = '/' || catx('/', of jobPath&DEPTH.-jobPath0, associatedJobName);
    jobCode          = catx("\", jobDirectory, jobFile);
  run;

  filename map clear;
%mend parsejobMetadata;
