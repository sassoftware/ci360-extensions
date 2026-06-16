/********************************************************************************/
/* Copyright (c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                          */
/* ******************************************************************************/

%macro set_partitioned_flag(json_all_data=, partition_out=);
    %local json_all_data partition_out;

    DATA work.partitioning_temp
        (KEEP=current_table partitioned_flg RENAME=(current_table=table_name));
        SET &json_all_data. END=last;
        LENGTH current_table $32;
        RETAIN current_table "";
        RETAIN partitioned_flg 1;

        * Write current table record when the first row of the next table is seen;
        IF P1 = 'table_name' AND current_table NE value AND _n_ NE 1 THEN DO;
            OUTPUT;
            partitioned_flg = 1;
            current_table   = "";
        END;

        IF P1 = 'table_name' THEN current_table = value;

        IF substr(P2, 1, 10) = 'categories' AND upcase(value) IN ('ENGAGEMETADATA','PLAN')
            THEN partitioned_flg = 0;

        IF substr(current_table, 1, 4) = 'cdm_'
            AND upcase(current_table) NOT IN
                ('CDM_CONTACT_HISTORY','CDM_RESPONSE_HISTORY','CDM_RESPONSE_EXTENDED_ATTR')
            THEN partitioned_flg = 0;

        IF substr(current_table, 1, 3) = 'md_' THEN partitioned_flg = 0;

        IF last THEN OUTPUT;  * Write the final table record;
    RUN;

    %if %sysfunc(exist(&partition_out.)) %then %do;
        PROC APPEND DATA=work.partitioning_temp BASE=&partition_out. FORCE;
        RUN;
    %end;
    %else %do;
        DATA &partition_out.;
            SET work.partitioning_temp;
        RUN;
    %end;

    PROC DELETE DATA=work.partitioning_temp; RUN;

%mend set_partitioned_flag;
