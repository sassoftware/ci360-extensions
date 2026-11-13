# Sankey diagram of most-navigated paths

[← Back to all reporting templates](README.md)


Suneel Grover explains how Sankey Diagrams visualize customer journey data  in this blog post: https://blogs.sas.com/content/customeranalytics/2020/10/07/sas-customer-intelligence-360-visual-analytics-sankey-diagrams-and-customer-journeys/. 

He also shows how to navigate the data in SAS Viya Visual Analytics (VA). This reporting template demonstrates how to transform UDM BUSINESS_PROCESS_DETAILS data into a format suitable for visualization with the VA Path Analysis object.
    

<TABLE>
<TR>
<TH>process_nm</TH>
<TH>dedup_path_id</TH>
<TH>sequence_no</TH>
<TH>process_step_nm</TH>
<TH>no_of_sessions</TH>
</TR>
<TR>
<TD>Car Insurance Application Eligiblility</TD>
<TD>01</TD>
<TD ALIGN=RIGHT>0</TD>
<TD>Step 01: Start Car Loan Wizard</TD>
<TD ALIGN=RIGHT>10933</TD>
</TR>
<TR>
<TD>Car Insurance Application Eligiblility</TD>
<TD>0102</TD>
<TD ALIGN=RIGHT>0</TD>
<TD>Step 01: Start Car Loan Wizard</TD>
<TD ALIGN=RIGHT>1856</TD>
</TR>
<TR>
<TD>Car Insurance Application Eligiblility</TD>
<TD>0102</TD>
<TD ALIGN=RIGHT>1</TD>
<TD>Step 02: Enter Car Details</TD>
<TD ALIGN=RIGHT>1849</TD>
</TR>
<TR>
<TD>Car Insurance Application Eligiblility</TD>
<TD>010201</TD>
<TD ALIGN=RIGHT>0</TD>
<TD>Step 01: Start Car Loan Wizard</TD>
<TD ALIGN=RIGHT>33</TD>
</TR>
<TR>
<TD>Car Insurance Application Eligiblility</TD>
<TD>010201</TD>
<TD ALIGN=RIGHT>1</TD>
<TD>Step 02: Enter Car Details</TD>
<TD ALIGN=RIGHT>31</TD>
</TR>
<TR>
<TD>Car Insurance Application Eligiblility</TD>
<TD>010201</TD>
<TD ALIGN=RIGHT>2</TD>
<TD>Step 01: Start Car Loan Wizard</TD>
<TD ALIGN=RIGHT>30</TD>
</TR>
<TR>
<TD>Car Insurance Application Eligiblility</TD>
<TD>010203</TD>
<TD ALIGN=RIGHT>0</TD>
<TD>Step 01: Start Car Loan Wizard</TD>
<TD ALIGN=RIGHT>2587</TD>
</TR>
<TR>
<TD>Car Insurance Application Eligiblility</TD>
<TD>010203</TD>
<TD ALIGN=RIGHT>1</TD>
<TD>Step 02: Enter Car Details</TD>
<TD ALIGN=RIGHT>2542</TD>
</TR>
<TR>
<TD>Car Insurance Application Eligiblility</TD>
<TD>010203</TD>
<TD ALIGN=RIGHT>2</TD>
<TD>Step 03: Enter Financial Details</TD>
<TD ALIGN=RIGHT>2538</TD>
</TR>
</TABLE>

Output columns:
- Process_nm: name of the business process as defined in CI 360 Events
- dedup_path_id: unique id for each sequence of steps found in the data. Instead of 10933 data rows for visitors who abandoned after landing on step 1 of the car insurance flow, the above table contains only one row. This approach saves time an resources by aggregating millions of events to thousands of unique paths. 
- sequence_no: a sequence number to order the steps inside a path.
- process_step_nm: the step as defined in CI 360 Business Process Event.
- no_of_sessions: The number of session that followed this path. 

Note that dedup_path_id=0102 groups paths 0102, 010102,010202 and 01010202, where the same page is refeshed before progressing. This can results in a slight difference in the number of sessions at each step of the same dedup_path_id. 

Input table:
- BUSINESS_PROCESS_DETAILS

Parameters used:
- lookback: Only events after this timetamp are considered. 
- path_cut_off: e.g. 20. Navigation paths are stopped after  20 steps. For longer navigations only the first 20 steps are considered. This reduced the number of distinct paths which reduces the data volume and makes the Sankey diagram more readable.
