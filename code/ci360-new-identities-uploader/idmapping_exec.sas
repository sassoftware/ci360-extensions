/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/


%let sysin=%qsysfunc(getoption(sysin));
%let file=%qscan(&sysin,-1,/);
%let path=%qsubstr(&sysin,1,%length(&sysin)-%length(&file) - 1);
%put &=sysin;
%put &=file;
%put &=path;


%let IB_idmapping_path = &path.;

%include "&IB_idmapping_path./initialize_parameter.sas" /source2;

%LoadIdentities;