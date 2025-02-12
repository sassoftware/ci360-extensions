/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%macro generate_base_metadata_tbl;
%let errFlag = 0;

%get_udm_structure_from_api;
%ErrCheck(Unable to get schema data,generate_base_metadata_tbl);
%if &errFlag %then %do;
	%goto ERREXIT;
%end;

%add_data_to_metadata;
%ErrCheck(Unable to add metadata,generate_base_metadata_tbl);
%if &errFlag %then %do;
	%goto ERREXIT;
%end;

%ERREXIT: 


%mend generate_base_metadata_tbl;
/*%generate_metadata;*/
