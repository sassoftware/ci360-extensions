/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/

%macro get_authentication_token;
	data _null_;
		header='{"alg":"HS256","typ":"JWT"}';
		payload='{"clientID":"' || strip(symget("DSC_TENANT_ID"))  || '"}';
		encHeader=translate(put(strip(header),$base64x64.), "-_ ", "+/=");
		encPayload=translate(put(strip(payload),$base64x64.), "-_ ", "+/=");
		key=put(strip(symget("DSC_SECRET_KEY")),$base64x100.);
		digest=sha256hmachex(strip(key),catx(".",encHeader,encPayload), 0);
		encDigest=translate(put(input(digest,$hex64.),$base64x100.), "-_ ", "+/=");
		token=catx(".", encHeader,encPayload,encDigest);
		call symputx("DSC_AUTH_TOKEN",token,'G');
	run;
%mend;

/*%get_authentication_token;*/