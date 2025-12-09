/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
options emailsys=smtp emailhost="your.smtp.server.com";
data _null_;
	l_attach_file=scan("&sysparm.", 1, ',');
	call symputx('l_attach_file', l_attach_file);
run;

filename outbox email to="your.name@email.com"
	subject="Error Report from utl_onpremdiext"
	attach=("&l_attach_file.");
;
data _null_;
	file outbox;
	put "The error report is attached.";
run;