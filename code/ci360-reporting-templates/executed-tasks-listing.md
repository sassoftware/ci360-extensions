# Executed tasks listing

[← Back to all reporting templates](README.md)


List Direct Marketing Tasks with their segments showing the total, targeted and holdouts counts per task occurrence.

<TABLE>
<TR>
<TH>task_cd</TH>
<TH>task_nm</TH>
<TH>occurrence_no</TH>
<TH>direct_contact_dttm</TH>
<TH>segment_nm</TH>
<TH>total_count</TH>
<TH>targeted_count</TH>
<TH>holdout_count</TH>
</TR>
<TR>
<TD>TASK_123</TD>
<TD>Welcome Campaign</TD>
<TD ALIGN=RIGHT>1</TD>
<TD ALIGN=RIGHT>17OCT2026:14:00:25</TD>
<TD>New_Members</TD>
<TD ALIGN=RIGHT>1164</TD>
<TD ALIGN=RIGHT>1106</TD>
<TD ALIGN=RIGHT>58</TD>
</TR>
<TR>
<TD>TASK_124</TD>
<TD>Call Customers at Risk</TD>
<TD ALIGN=RIGHT>1</TD>
<TD ALIGN=RIGHT>17OCT2026:08:00:25</TD>
<TD>High Value at Risk</TD>
<TD ALIGN=RIGHT>186</TD>
<TD ALIGN=RIGHT>186</TD>
<TD ALIGN=RIGHT>0</TD>
</TR>
<TD>TASK_124</TD>
<TD>Call Customers at Risk</TD>
<TD ALIGN=RIGHT>2</TD>
<TD ALIGN=RIGHT>24OCT2026:08:00:34</TD>
<TD>High Value at Risk</TD>
<TD ALIGN=RIGHT>121</TD>
<TD ALIGN=RIGHT>121</TD>
<TD ALIGN=RIGHT>0</TD>
</TR>
</TABLE>

These UDM tables are used as input: 
- MD_TASK
- MD_OCCURRENCE
- DIRECT_CONTACT
- MD_TASK_X_SEGMENT
- MD_SEGMENT

Parameters used:
- p_begin_timestamp and p_end_timestamp: Only contact events between these timetamp are considered.