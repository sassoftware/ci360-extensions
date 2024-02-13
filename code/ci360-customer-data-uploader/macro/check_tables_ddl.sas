/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%macro check_tables_ddl();

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

    %if not %member_exists(&IB_DBOLIB..&semaphoresTable.) %then %do;
        data &IB_DBOLIB..&semaphoresTable.;
            length table_name $32. effective_datetime 8 active_flg $1;
            format effective_datetime datetime.;
            /* Add other variables and attributes as needed */
            stop;
        run;
    %end;

    %if not %member_exists(&IB_DBOLIB..&idmapPendingTable.) %then %do;
        data &IB_DBOLIB..&idmapPendingTable.;
            length STATUS $32.; /* Assuming STATUS is a character variable */
            /* Add other variables and attributes as needed */
            stop;
        run;
    %end;  
    
    %if not %member_exists(&IB_DBOLIB..&dataHubTable.) %then %do;
        data &IB_DBOLIB..&dataHubTable..;
            length subject_id $80. customer_id $80. individual_key 8 email $30. phonenumber 8  last_updated_ts 8;
            format last_updated_ts datetime.;
            /* Add other variables and attributes as needed */
            stop;
        run;
    %end;  

/**TEST DATA */
/* 
    data &IB_DBOLIB..&dataHubTable..;	
    input  individual_key last_updated_ts :datetime20.  subject_id :$80. customer_id :$80. SYSDATE :datetime20. email :$30. phonenumber;
    format last_updated_ts datetime20. SYSDATE datetime20.;
    datalines;
    1011 01JAN2023:12:00:00 011 C011 01JAN2023:08:00:00 email1@sas.com 1923456701 
    1012 02JAN2023:14:30:00 012 C012 02JAN2023:09:30:00 email2@sas.com 1923456702 
    1013 03JAN2023:08:45:00 013 C013 03JAN2023:10:15:00 email3@sas.com 1923456703 
    1014 04JAN2023:09:15:00 014 C014 04JAN2023:11:45:00 email4@sas.com 1923456704 
    1015 05JAN2023:16:20:00 015 C015 05JAN2023:12:30:00 email5@sas.com 1923456705
    ;
    run;

 */


%mend check_tables_ddl;
