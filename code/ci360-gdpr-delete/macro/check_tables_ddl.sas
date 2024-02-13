/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%macro check_tables_ddl();

    %if not %member_exists(&IB_METADATALIB..&diBatchControlIn.) %then %do;
    /* Create table METADATA.&diBatchControlIn. */
        data &IB_METADATALIB..&diBatchControlIn.;
            length Table_Name $32. Effective_Datetime $19.;
            /* Add other variables and attributes as needed */
            stop;
        run;
    %end;

    %if not %member_exists(&IB_DBOLIB..&EventLogTable.) %then %do;
        /* Create table DBO.&mpEventLogTable. */
        data &IB_DBOLIB..&EventLogTable.;
            length event_domain $32. event_desc $256. event_user $32. event_hostname $32.;
            length event_start event_end event_duration 8;
            length event_row_cnt event_rc 8;
            format event_start event_end datetime.;
            /* Add other variables and attributes as needed */
            stop;
        run;
    %end;

    %if not %member_exists(&IB_STAGELIB..&gdprDeleteTable.) %then %do;
        /* Create table METADATA.&diBatchControlIn. */
        data &IB_STAGELIB..&gdprDeleteTable.;
            length SubjectId $128. TransferEffectiveDtime 8;
            format SubjectId_pre best32.;
            format TransferEffectiveDtime datetime.;
            /* Add other variables and attributes as needed */
/* do SubjectId_pre = 1 to 5;
SubjectId = put(SubjectId_pre, best32.);
    TransferEffectiveDtime = intnx('dtday', datetime(), -5);
    output;
end; */
            drop SubjectId_pre;
            stop;
        run;
    %end;

    
%mend check_tables_ddl;
