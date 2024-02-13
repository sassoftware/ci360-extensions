/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
/*****************************************************************
*  FUNCTION:
*     Check if macro variable value is empty.
*
*  PARAMETERS:
*     mpValue                 +  macro variable name
*
******************************************************************
*  External macro used:
*     none
*
*  Sets macro variables:
*     none
*
******************************************************************
*  Example of use:
*     %let mvVar = ...;
*     %if %is_blank(mvVar) %then ...;
*
******************************************************************/

%macro is_blank (mpValue);
%sysevalf(%superq(&mpValue)=,boolean)
%mend is_blank;
