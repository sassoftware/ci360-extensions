# Journey suppressions

[← Back to all reporting templates](README.md)


This template shows the number of people suppressed from each journey occurrence, broken down by suppression reason.

<TABLE>
<TR>
<TH>journey_nm</TH>
<TH>journey_occurrence_num</TH>
<TH>audience_nm</TH>
<TH>audience_size_val</TH>
<TH>reason_cd</TH>
<TH>reason_txt</TH>
<TH>members_suppressed</TH>
</TR>
<TR>
<TD ALIGN=LEFT>CC action</TD>
<TD ALIGN=RIGHT>1</TD>
<TD ALIGN=RIGHT>Eligible Customers</TD>
<TD ALIGN=RIGHT>5623</TD>
<TD ALIGN=RIGHT>entry_limit</TD>
<TD ALIGN=RIGHT>Entry limit reached.</TD>
<TD ALIGN=RIGHT>56</TD>
</TR>
<TR>
<TD ALIGN=LEFT>CC action</TD>
<TD ALIGN=RIGHT>1</TD>
<TD ALIGN=RIGHT>Eligible Customers</TD>
<TD ALIGN=RIGHT>5623</TD>
<TD ALIGN=RIGHT>holdout_suppression</TD>
<TD ALIGN=RIGHT>Contact already in holdout test.</TD>
<TD ALIGN=RIGHT>123</TD>
</TR>
<TR>
<TD ALIGN=LEFT>CC action</TD>
<TD ALIGN=RIGHT>2</TD>
<TD ALIGN=RIGHT>Eligible Customers</TD>
<TD ALIGN=RIGHT>6104</TD>
<TD ALIGN=RIGHT>entry_limit</TD>
<TD ALIGN=RIGHT>Entry limit reached.</TD>
<TD ALIGN=RIGHT>41</TD>
</TR>
</TABLE>


These UDM tables are used as input: 
- JOURNEY_SUPPRESSION
- MD_JOURNEY
- MD_JOURNEY_OCCURRENCE
- MD_AUDIENCE_OCCURRENCE
- MD_AUDIENCE

The JOURNEY_SUPPRESSION detail table is aggregated. Suppression events are counted by type (reason) and journey execution (occurrence). To reduce the processing, a lookback parameter at the top queries data only up to a certain date.
