/*
Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

%macro sendmail(sendlist=, folder=, file=);
	/* Setup email as output device */
	filename reports email "sasemail@cidemo.sas.com" lrecl=500;

	data _null_;
		/* Open email adress list */
		set &sendlist;
       
		/* Open output device */
		file reports;

		
		/* Add sender and receiver from sendlist table */
		put '!EM_TO!' mailto;
		put '!EM_FROM!' mailfrom;
		put '!EM_SENDER!' mailfrom;

		/* Add subject text from sendlist table*/
		put '!EM_SUBJECT!' subjecttext;

		/* Put email message lines */
		Put "Hi " mailname ',';
        Put " ";
        Put line1;
        Put " ";
        Put line2;
        Put line3;
        Put " ";
        Put signoff;
		attach1 = "'&folder./&file.'";			
		put '!EM_ATTACH!' "(" attach1 ")";
		put '!EM_SEND!';
		put '!EM_NEWMSG!';
		put '!EM_ABORT!';

		/* Wait 1 seconds between each email */
		call sleep(1);
	run;
%mend;
