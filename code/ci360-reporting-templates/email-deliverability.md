# Email deliverability

[← Back to all reporting templates](README.md)


**email_deliverability.sql** shows the most problematic  emails tasks. The report calculates opt-outs and spam complaints rate per email task execution.

The result shows the worst email execution first:
<table style="text-align:right;">
  <tr>
    <th style="text-align:left;">task_nm</th>
    <th>task_cd</th>
    <th>email_sends</th>
    <th>email_complaints</th>
    <th>complaint_rate</th>
    <th>email_optouts</th>
    <th>optout_rate</th>
    <th>task_delivery_type_nm</th>
    <th>occurrence_no</th>
    <th>occurrence_run_dttm</th>
  </tr>
  <tr>
    <td style="text-align:left;">Newsletter</td>
    <td>TSK_123</td>
    <td>65432</td>
    <td>1963</td>
    <td>0.03</td>
    <td>6543</td>
    <td>0.10</td>
    <td style="text-align:left;">bulk</td>
    <td>36</td>
    <td>23APR2028:15:00:14</td>
  </tr>
  <tr>
    <td style="text-align:left;">Flash Sale </td>
    <td>TSK_234</td>
    <td>35432</td>
    <td>354</td>
    <td>0.01</td>
    <td>353</td>
    <td>0.01</td>
    <td style="text-align:left;">bulk</td>
    <td>1</td>
    <td>25APR2024:09:00:00</td>
  </tr>
  <tr>
    <td style="text-align:left;">Confirmation</td>
    <td>TSK_345</td>
    <td>1543</td>
    <td>0</td>
    <td>0</td>
    <td>0</td>
    <td>0</td>
    <td style="text-align:left;">triggered</td>
    <td>.</td>
    <td>.</td>
  </tr>
</table>

These UDM tables are used as input: 
- EMAIL_SEND
- EMAIL_OPTOUT
- EMAIL_COMPLAINT
- MD_TASK
- MD_OCCURRENCE

The email detail tables are aggregated by task execution. To reduce the processing, a lookback parameter at the top of the query looks back only up to a certain date.
