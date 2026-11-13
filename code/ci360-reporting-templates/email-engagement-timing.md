# Email engagement timing

[← Back to all reporting templates](README.md)


Shows when recipients typically open and click emails and the median time from send to first open and first click, by bulk email task and occurrence.

The query connects each email send with its first valid open and first click. It then calculates the hour of the interaction and the time between send and engagement. The results are summarized by email task execution to show the most frequent open and click hours, together with the median time to first engagement.

<TABLE>
<TR>
<TH>task_nm</TH>
<TH>task_cd</TH>
<TH>occurrence_no</TH>
<TH>email_opens</TH>
<TH>email_clicks</TH>
<TH>most_frequent_open_hour</TH>
<TH>median_hours_to_open</TH>
<TH>most_frequent_click_hour</TH>
<TH>median_hours_to_click</TH>
<TH>occurrence_run_dttm</TH>
</TR>
<TR>
<TD>Holidays upgrade plan </TD>
<TD>TSK_654</TD>
<TD ALIGN=RIGHT>2</TD>
<TD ALIGN=RIGHT>10302</TD>
<TD ALIGN=RIGHT>7501</TD>
<TD ALIGN=RIGHT>1.32</TD>
<TD ALIGN=RIGHT>12</TD>
<TD ALIGN=RIGHT>2.03</TD>
<TD ALIGN=RIGHT>19</TD>
<TD ALIGN=RIGHT>2026-06-20 09:05:18</TD>
</TR>
<TR>
<TD>Loyalty plan welcome</TD>
<TD>TSK_512</TD>
<TD ALIGN=RIGHT>8</TD>
<TD ALIGN=RIGHT>3302</TD>
<TD ALIGN=RIGHT>2501</TD>
<TD ALIGN=RIGHT>0.75</TD>
<TD ALIGN=RIGHT>12</TD>
<TD ALIGN=RIGHT>1.20</TD>
<TD ALIGN=RIGHT>20</TD>
<TD ALIGN=RIGHT>2026-06-21 08:03:34</TD>
</TR>
</TABLE>

Input tables:
- EMAIL_SEND
- EMAIL_OPEN
- EMAIL_CLICK
- MD_TASK
- MD_OCCURRENCE

Parameters used:
- lookback: Only events after this timetamp are considered.
