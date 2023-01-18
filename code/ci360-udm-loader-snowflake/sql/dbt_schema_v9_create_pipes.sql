/*
Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

create pipe <PREFIX>_abt_attribution AUTO_INGEST = FALSE AS COPY INTO <PREFIX>_abt_attribution FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_dbt_adv_campaign_visitors AUTO_INGEST = FALSE AS COPY INTO <PREFIX>_dbt_adv_campaign_visitors FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_dbt_business_process AUTO_INGEST = FALSE AS COPY INTO <PREFIX>_dbt_business_process FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_dbt_content AUTO_INGEST = FALSE AS COPY INTO <PREFIX>_dbt_content FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_dbt_documents AUTO_INGEST = FALSE AS COPY INTO <PREFIX>_dbt_documents FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_dbt_ecommerce AUTO_INGEST = FALSE AS COPY INTO <PREFIX>_dbt_ecommerce FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_dbt_forms AUTO_INGEST = FALSE AS COPY INTO <PREFIX>_dbt_forms FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_dbt_goals AUTO_INGEST = FALSE AS COPY INTO <PREFIX>_dbt_goals FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_dbt_media_consumption AUTO_INGEST = FALSE AS COPY INTO <PREFIX>_dbt_media_consumption FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_dbt_promotions AUTO_INGEST = FALSE AS COPY INTO <PREFIX>_dbt_promotions FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
create pipe <PREFIX>_dbt_search AUTO_INGEST = FALSE AS COPY INTO <PREFIX>_dbt_search FROM @<STAGE> FILE_FORMAT = ( FORMAT_NAME = <FILE FORMAT>);
