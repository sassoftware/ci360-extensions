# Journey performance

[← Back to all reporting templates](README.md)


Shows success outcomes for each journey occurrence, including the number of success events and the associated success value and quantity, alongside contacts entered and suppressed.

For each purpose, journey and journey occurrence, it reports:
- Contacts entered
- Contacts suppressed
- Number of success events (success_nbr)
- Total success value (success_val), when the target goal defines a conversion value

<TABLE>
<TR>
<TH>purpose_nm</TH>
<TH>journey_nm</TH>
<TH>journey_occurrence_num</TH>
<TH>num_of_contacts_entered</TH>
<TH>num_of_contacts_suppressed</TH>
<TH>success_val</TH>
<TH>success_nbr</TH>
</TR>
<TR>
<TD>Cross Sell, Up Sell</TD>
<TD>Easy Invest</TD>
<TD ALIGN=RIGHT>1</TD>
<TD ALIGN=RIGHT>10933</TD>
<TD ALIGN=RIGHT>0</TD>
<TD ALIGN=RIGHT>0</TD>
<TD ALIGN=RIGHT>976</TD>
</TR>
<TR>
<TD>Onboarding</TD>
<TD>New to Bank</TD>
<TD ALIGN=RIGHT>1</TD>
<TD ALIGN=RIGHT>1468</TD>
<TD ALIGN=RIGHT>0</TD>
<TD ALIGN=RIGHT>0</TD>
<TD ALIGN=RIGHT>13</TD>
</TR>
<TR>
<TD>Onboarding</TD>
<TD>New to Bank</TD>
<TD ALIGN=RIGHT>2</TD>
<TD ALIGN=RIGHT>1516</TD>
<TD ALIGN=RIGHT>35</TD>
<TD ALIGN=RIGHT>0</TD>
<TD ALIGN=RIGHT>16</TD>
</TR>
</TABLE>

Input tables:
- JOURNEY_SUCCESS
- MD_PURPOSE
- MD_JOURNEY
- MD_JOURNEY_OCCURRENCE

Parameters used:
- lookback: Only events after this timetamp are considered.
