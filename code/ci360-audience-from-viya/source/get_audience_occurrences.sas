/*-----------------------------------------------------------------------------
 Copyright © 2024, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 SPDX-License-Identifier: Apache-2.0
-----------------------------------------------------------------------------*/
/* Get status information of  audience occurrences */
/* Note: This code uses macros that are available after running the custom step - see readme.md */

filename resp temp;
filename head temp;
%dsc_httprequest(requestCT=%str(application/json), 
				requestURL=&External_gateway./marketingAudience/audiences/&audience_id./history, 
				requestMethod=GET,
				requestTIMEOUT=60,
				outfile=resp,
				headerout=head,
				service_auth= %str(headers "Authorization" = "Bearer &TEMP_TOKEN."),
				proxy_host=&proxy_host, proxy_port=&proxy_port, proxy_auth=&proxy_auth
				);	 

libname resp JSON;
proc sql;
 select * from resp.items;
quit;
%if &STOP_PROCESS=0 and &AUDIENCE_DEBUG %then %do;	
	data _null_; infile head; input; put _infile_; run;
	data _null_; infile resp; input; put _infile_; run;
%end;