/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%macro download_url(url=,data=);

	proc http 
	   method="GET" 
	   url="&url"
	   out=resp;
	run;

	data &data.;
		infile resp
		delimiter=','
		missover
		firstobs=2
		dsd
		lrecl=32767 mod;
		informat subject_id $50. customer_id $50. url_error $2048.;
		format subject_id $50. customer_id $50. url_error $2048.;
		input subject_id $ customer_id $ url_error $;
	run;

%mend;