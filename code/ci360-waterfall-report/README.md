# SAS Customer Intelligence 360 Waterfall Report - Segment Map


## Overview

Waterfall report showing Initial and Match/Excluded counts produced between 2 or more segment nodes.
This report consolidates all analyzed nodes into a single view, showing client counts before and after the criterion applied at each node. This process sends an email with the report attached to the email address you define during execution.


## Prerequisites
Configure the SMTP server on the SAS compute server

## Configurations
Steps for enabling the STP report

1. Download the Waterfall.spk file and unzip in your computer
2. Open Management Console
3. Select a folder where you want install de stp and import the spk file
4. Create a path in your processing enviroment, something like /sasdata/ci/waterfall/
- Note: Make sure to set read and write permissions for proper execution
5. Copy the Custom+Waterfall.sas and sendmail.sas files to the path
6. Configure the stp imported mapping the Custom+Waterfall.sas file in the Execution tap
7. Edit the Custom+Waterfall.sas in the line 350 to change the path to include the sendmail.sas
8. Execute the sstp command in the Direct agent

## Important considerations of the STP report
The following macro vars come from STP Parameters, the values are defined in Process node into CI 360
- ***TypeAnd***: How the audience is filtered in the waterfall report, Match (to include profiles that meet the condition) and Exclude (to remove profiles that meet the condition)
- ***output_type***: Report format (PDF, RTF, HTML)
- ***RptName***: Report Name
- ***emailAddress***: Email address where the report will be sent
- ***ReportFolder***: Path from your on‑premise environment, where the report is generated

Especially for **ReportFolder**, you can define it directly into STP, 
1. go to properties from the STP
2. go to Parameters tab
3. double click on the ReportFolder parameter
4. go to Prompt Type and Values tab, and chage the path into Default Value section

Note: Only this value cannot be configured during the execution of the STP in CI 360


## Usage
Run the Waterfall Report in CI 360 creating your segment map, and your segments, to get the report follow this:

1. Use a process node and select the "CUSTOM AND with Waterfall report" stp
2. Connect the first criteria node you want to use as the starting point of the report
3. Connect the next node you want to compare in the report
4. Repeat step 3 for the number of nodes you want to compare in the report
5. Configure the parameters of the STP
- [ ] a) Match or Exclude
- [ ] b) Output type
- [ ] c) Report name
- [ ] d) Email address

6. Publish and execute the segment map or only execute de process node

View the usage.docx for visual instructions to use the Waterfall Report in CI 360

