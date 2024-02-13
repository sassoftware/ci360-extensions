/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/

%macro check_tables_ddl();

    /* Creating CI360_IDMAP_PENDING table with sample data */
    %if not %member_exists(&sas_db_libref_idmap..CI360_IDMAP_PENDING) %then %do;
        data &sas_db_libref_idmap..CI360_IDMAP_PENDING;
            length subject_id $80 customer_id $80 added_dttm 8 status $20;
            format added_dttm DATETIME20.;
            
            /* Sample data */
            /* infile "&sas_include_path./data/CI360_IDMAP_PENDING_EXAMPLE.txt" dlm = ' ' truncover;
			input subject_id $ customer_id $  added_dttm :datetime20. status $; */
    
            stop;
        run;

    %end; 

    /* Creating CI360_IDMAP_PROCESSED table with sample data */   
    %if not %member_exists(&sas_db_libref_idmap..CI360_IDMAP_PROCESSED) %then %do;
        data &sas_db_libref_idmap..CI360_IDMAP_PROCESSED;
            length subject_id $80 customer_id $80 processed_dttm 8 status $20;
            format processed_dttm DATETIME20.;
            
            /* Sample data */
            /* infile "&sas_include_path./data/CI360_IDMAP_PROCESSED_EXAMPLE.txt" dlm = ' ' truncover;
            input subject_id $ customer_id $ processed_dttm :datetime20. status $;*/
            
            stop;
        run;

    %end; 
    

    /* Creating V_DIM_INDIVIDUAL table with LAST_UPDATED_TS field */
    %if not %member_exists(&sas_db_libref_cust..V_DIM_INDIVIDUAL) %then %do;
        data &sas_db_libref_cust..V_DIM_INDIVIDUAL;
            length individual_key 8 last_updated_ts 8 subject_id $80 customer_id $80 SYSDATE 8;
            format last_updated_ts datetime20. SYSDATE datetime20.;
        
            /* Sample data from V_DIM_INDIVIDUAL table */
            /* infile "&sas_include_path./data/V_DIM_INDIVIDUAL_EXAMPLE.txt" dlm = ' ' truncover;
            input individual_key last_updated_ts :datetime20. subject_id $ customer_id $ sysdate :datetime20.; */

            stop;
        run;
    %end; 


%mend check_tables_ddl;