# Task performance

[← Back to all reporting templates](README.md)


The Task Performance template shows task performance based on its defined metrics, with the primary metric reported as conversions and secondary metrics as milestones.

<TABLE>
<TR>
<TH>task_name</TH>
<TH>task_cd</TH>
<TH>occurrence_no</TH>
<TH>contacts</TH>
<TH>unique_conversions</TH>
<TH>unique_milestones</TH>
<TH>conversions</TH>
<TH>milestones</TH>
<TH>occurrence_run_dttm</TH>
<TH>latest_contact_dttm</TH>
<TH>channel_nm</TH>
<TH>delivery_type</TH>
<TH>task_status_cd</TH>
<TH>owner_nm</TH>
</TR>
<TR>
<TD ALIGN=LEFT>Flash Sale</TD>
<TD ALIGN=LEFT>TSK_374</TD>
<TD ALIGN=RIGHT>1</TD>
<TD ALIGN=RIGHT>85242</TD>
<TD ALIGN=RIGHT>456</TD>
<TD ALIGN=RIGHT>987</TD>
<TD ALIGN=RIGHT>567</TD>
<TD ALIGN=RIGHT>1263</TD>
<TD ALIGN=RIGHT>20/10/2026 11:08:12</TD>
<TD ALIGN=RIGHT>20/10/2026 11:08:12</TD>
<TD ALIGN=LEFT>email</TD>
<TD ALIGN=LEFT>bulk</TD>
<TD ALIGN=LEFT>active</TD>
<TD ALIGN=LEFT>joe@sas.com</TD>
</TR>
<TR>
<TD ALIGN=LEFT>CC action</TD>
<TD ALIGN=LEFT>TSK_375</TD>
<TD ALIGN=RIGHT>2</TD>
<TD ALIGN=RIGHT>24200</TD>
<TD ALIGN=RIGHT>250</TD>
<TD ALIGN=RIGHT>360</TD>
<TD ALIGN=RIGHT>420</TD>
<TD ALIGN=RIGHT>501</TD>
<TD ALIGN=RIGHT>20/12/2026 11:08:12</TD>
<TD ALIGN=RIGHT>20/12/2026 11:08:12</TD>
<TD ALIGN=LEFT>direct</TD>
<TD ALIGN=LEFT>bulk</TD>
<TD ALIGN=LEFT>active</TD>
<TD ALIGN=LEFT>joe@sas.com</TD>
</TR>
</TABLE>

These UDM tables are used as input: 
- CONTACT_HISTORY
- CONVERSION_MILESTONE
- MD_TASK
- MD_OCCURRENCE


Parameters used:
- lookback: Only events after this timetamp are considered. 

The lookback parameter reduces the processing on the CONTACT_HISTORY and CONVERSION_MILESTONE detail tables as they are queried for the first time. Processing is further reduced by aggregating the resulting data at the task occurrence level.
