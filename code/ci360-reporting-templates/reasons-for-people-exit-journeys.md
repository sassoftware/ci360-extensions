# Reasons for people exit journeys

[← Back to all reporting templates](README.md)


This report shows the reasons for people exit journeys. The report distiguishes between drop events, GDPR related drops (suppressions), success events, holdout exits and reaching end nodes.

<TABLE>
<TR>
<TH>journey_nm</TH>
<TH>journey_occurrence_num</TH>
<TH>reason_cd</TH>
<TH>end_node_nm</TH>
<TH>reason_txt</TH>
<TH>member_nbr</TH>
<TH>exit_total</TH>
<TH>exit_pct</TH>
</TR>
<TR>
<TD ALIGN=LEFT>Onboarding</TD>
<TD ALIGN=RIGHT>1</TD>
<TD ALIGN=RIGHT>reached_end</TD>
<TD ALIGN=RIGHT>End: null</TD>
<TD ALIGN=RIGHT>Reached End Node 5286c3cc-9ef9</TD>
<TD ALIGN=RIGHT>23</TD>
<TD ALIGN=RIGHT>1509</TD>
<TD ALIGN=RIGHT>1.52</TD>
</TR>
<TR>
<TD ALIGN=LEFT>Onboarding</TD>
<TD ALIGN=RIGHT>1</TD>
<TD ALIGN=RIGHT>reached_end</TD>
<TD ALIGN=RIGHT>End: null</TD>
<TD ALIGN=RIGHT>Reached End Node 2581e9d7-30b2</TD>
<TD ALIGN=RIGHT>1411</TD>
<TD ALIGN=RIGHT>1509</TD>
<TD ALIGN=RIGHT>93.51</TD>
</TR>
<TR>
<TD ALIGN=LEFT>Onboarding</TD>
<TD ALIGN=RIGHT>1</TD>
<TD ALIGN=RIGHT>holdout_exit</TD>
<TD ALIGN=RIGHT></TD>
<TD ALIGN=RIGHT>Holdout contact exit</TD>
<TD ALIGN=RIGHT>75</TD>
<TD ALIGN=RIGHT>1509</TD>
<TD ALIGN=RIGHT>4.97</TD>
</TR>
<TR>
<TD ALIGN=LEFT>Onboarding</TD>
<TD ALIGN=RIGHT>2</TD>
<TD ALIGN=RIGHT>reached_end</TD>
<TD ALIGN=RIGHT>End: null</TD>
<TD ALIGN=RIGHT>Reached End Node 5286c3cc-9ef9</TD>
<TD ALIGN=RIGHT>23</TD>
<TD ALIGN=RIGHT>2092</TD>
<TD ALIGN=RIGHT>1.10</TD>
</TR>
</TABLE>

These UDM tables are used as input: 
- JOURNEY_EXIT
- MD_JOURNEY
- MD_JOURNEY_OCCURRENCE
- MD_JOURNEY_NODE

The JOURNEY_EXIT detail table is aggregated. Exit events are counted by type (reason) and journey execution (occurrence). To reduce the processing, a lookback parameter at the top queries data only up to a certain date.
