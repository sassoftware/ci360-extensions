/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%macro member_exists (
   mpTarget             ,
   mpMemberTypes        =  DATA VIEW
);
   %local lmvExists;
   %let lmvExists = 0;

   %local lmvI lmvType;
   %do lmvI = 1 %to 10;
      %let lmvType = %scan(&mpMemberTypes, &lmvI, %str( ) ) ;
      %if %is_blank(lmvType) %then %goto results;

      %if %sysfunc(exist(&mpTarget, &lmvType)) %then %do;
         %let lmvExists = 1;
         %goto results;
      %end;
   %end;

%results:
   %do;&lmvExists%end;
%mend member_exists;