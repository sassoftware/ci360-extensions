/******************************************************************************/
/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.*/
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
/**/	
%macro add_column_metadata(schema_table=, metadata_table=, database=);
    proc sql;
        create table work.metadata_table as 
        select 
            a.*,
            case
                when index(column_type, 'array')                                  then 'varchar(4000)'
                when data_length ne .                                             then cats(b.rdbms_datatype,'(',data_length,')')
                when data_length=. and substr(column_type, 1, 6)='string'         then 'varchar(4000)'
                when data_length=. and data_type = 'decimal'                      then column_type
                when data_length=. and data_type ne 'decimal'                     then b.rdbms_datatype
                else ''        
            end as rdbms_column_type,
            case 
                when prxmatch('/^identity_/i', table_name) then 0
                when upcase(table_name) = 'ABT_ATTRIBUTION' and upcase(column_name) = 'INTERACTION_DTTM'  then 1
                when upcase(table_name) = 'CDM_CONTACT_HISTORY' and upcase(column_name) = 'CONTACT_DT'  then 1
                when upcase(table_name) = 'CDM_RESPONSE_HISTORY' and upcase(column_name) = 'RESPONSE_DT' then 1
                when upcase(table_name) = 'CDM_RESPONSE_EXTENDED_ATTR' and upcase(column_name) = 'UPDATED_DTTM'  then 1
                when prxmatch('/^dbt_/i', table_name)  and upcase(column_name) = 'SESSION_COMPLETE_LOAD_DTTM' then 1
                when partitioned_flg = 1 and upcase(column_name) = 'LOAD_DTTM' then 1
				else 0
			
            end as partition_column        

        from &schema_table a
        left join cdmcnfg.datatypes b 
          on a.data_type = b.schema_datatype
        where upcase(b.rdbms) = upcase("&database");
    quit;

    data &metadata_table;
        set work.metadata_table;
    run;

    
    
%mend add_column_metadata;

