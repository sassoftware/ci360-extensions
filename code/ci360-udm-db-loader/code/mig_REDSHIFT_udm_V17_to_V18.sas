/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
 PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..impression_delivered  ADD request_id  varchar(36)  NULL) BY &DBNAME;
DISCONNECT FROM REDSHIFT;
QUIT;
%ErrCheck (Failed to alter table: impression_delivered , impression_delivered );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..impression_spot_viewable  ADD request_id  varchar(36)  NULL) BY &DBNAME;
DISCONNECT FROM REDSHIFT;
QUIT;
%ErrCheck (Failed to alter table: impression_spot_viewable , impression_spot_viewable );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..md_spot  ADD location_selector_flg  char(1)  NULL) BY &DBNAME;
   EXECUTE (ALTER TABLE &dbschema..md_spot  ADD spot_key  varchar(40)  NULL) BY &DBNAME;
DISCONNECT FROM REDSHIFT;
QUIT;
%ErrCheck (Failed to alter table: md_spot , md_spot );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..md_task  ADD secondary_status  varchar(40)  NULL) BY &DBNAME;
DISCONNECT FROM REDSHIFT;
QUIT;
%ErrCheck (Failed to alter table: md_task , md_task );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..sms_message_clicked  ADD context_type_nm  varchar(256)  NULL) BY &DBNAME;
   EXECUTE (ALTER TABLE &dbschema..sms_message_clicked  ADD context_val  varchar(256)  NULL) BY &DBNAME;
DISCONNECT FROM REDSHIFT;
QUIT;
%ErrCheck (Failed to alter table: sms_message_clicked , sms_message_clicked );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..sms_message_delivered  ADD context_type_nm  varchar(256)  NULL) BY &DBNAME;
   EXECUTE (ALTER TABLE &dbschema..sms_message_delivered  ADD context_val  varchar(256)  NULL) BY &DBNAME;
DISCONNECT FROM REDSHIFT;
QUIT;
%ErrCheck (Failed to alter table: sms_message_delivered , sms_message_delivered );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..sms_message_failed  ADD context_type_nm  varchar(256)  NULL) BY &DBNAME;
   EXECUTE (ALTER TABLE &dbschema..sms_message_failed  ADD context_val  varchar(256)  NULL) BY &DBNAME;
DISCONNECT FROM REDSHIFT;
QUIT;
%ErrCheck (Failed to alter table: sms_message_failed , sms_message_failed );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..sms_message_reply  ADD context_type_nm  varchar(256)  NULL) BY &DBNAME;
   EXECUTE (ALTER TABLE &dbschema..sms_message_reply  ADD context_val  varchar(256)  NULL) BY &DBNAME;
   EXECUTE (ALTER TABLE &dbschema..sms_message_reply  ADD sms_content  varchar(40)  NULL) BY &DBNAME;
DISCONNECT FROM REDSHIFT;
QUIT;
%ErrCheck (Failed to alter table: sms_message_reply , sms_message_reply );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..sms_message_send  ADD context_type_nm  varchar(256)  NULL) BY &DBNAME;
   EXECUTE (ALTER TABLE &dbschema..sms_message_send  ADD context_val  varchar(256)  NULL) BY &DBNAME;
DISCONNECT FROM REDSHIFT;
QUIT;
%ErrCheck (Failed to alter table: sms_message_send , sms_message_send );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..sms_optout  ADD context_type_nm  varchar(256)  NULL) BY &DBNAME;
   EXECUTE (ALTER TABLE &dbschema..sms_optout  ADD context_val  varchar(256)  NULL) BY &DBNAME;
DISCONNECT FROM REDSHIFT;
QUIT;
%ErrCheck (Failed to alter table: sms_optout , sms_optout );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..sms_optout_details  ADD context_type_nm  varchar(256)  NULL) BY &DBNAME;
   EXECUTE (ALTER TABLE &dbschema..sms_optout_details  ADD context_val  varchar(256)  NULL) BY &DBNAME;
DISCONNECT FROM REDSHIFT;
QUIT;
%ErrCheck (Failed to alter table: sms_optout_details , sms_optout_details );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..spot_clicked  ADD request_id  varchar(36)  NULL) BY &DBNAME;
DISCONNECT FROM REDSHIFT;
QUIT;
%ErrCheck (Failed to alter table: spot_clicked , spot_clicked );

PROC SQL;
CONNECT TO &DBNAME (&sql_passthru_connection);
   EXECUTE (ALTER TABLE &dbschema..spot_requested  ADD properties_map_doc  varchar(4000)  NULL) BY &DBNAME;
   EXECUTE (ALTER TABLE &dbschema..spot_requested  ADD request_id  varchar(36)  NULL) BY &DBNAME;
DISCONNECT FROM REDSHIFT;
QUIT;
%ErrCheck (Failed to alter table: spot_requested , spot_requested );
