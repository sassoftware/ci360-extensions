/********************************************************************************/
/* Copyright (c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                          */
/* ******************************************************************************/

%macro get_udm_mart(mart_type=, schema_table=, partitioning_table=, download_url=);
    %local mart_type schema_table partitioning_table download_url;

    /* Fetch the schema index JSON from the CI360 API */
    FILENAME outfile TEMP;
    PROC HTTP
        METHOD = "GET"
        OUT    = outfile
        CT     = "application/json"
        URL    = "%superq(download_url)"
		TIMEOUT= 30;
        HEADERS "Authorization" = "Bearer &DSC_AUTH_TOKEN.";
    RUN;

    %if "&Verbose" = "1" %then %do;
        DATA _NULL_;
            INFILE outfile;
            INPUT;
            PUT _infile_;
        RUN;
    %end;

    /* Parse the JSON response */
    LIBNAME jsondata JSON FILEREF=outfile;
    PROC DELETE DATA=WORK.ITEMS; RUN;
    PROC COPY IN=jsondata OUT=work; RUN;
    LIBNAME jsondata;
    FILENAME outfile;

    /* Check that the response contains schema data */
    %let nobs        = 0;
    %let SchemaExist = 0;
    %let dsid = %sysfunc(open(WORK.ITEMS));
    %if &dsid %then %do;
        %let nobs        = %sysfunc(attrn(&dsid, nlobs));
        %let SchemaExist = %sysfunc(varnum(&dsid, schemaUrl));
        %let dsid        = %sysfunc(close(&dsid));
    %end;

    %if &nobs AND &SchemaExist %then %do;

        /* Extract the schema URL from the response */
        %let schemaUrl = ;
        DATA _NULL_;
            SET items (KEEP=schemaUrl);
            IF _n_ = 1 THEN CALL SYMPUTX("schemaUrl", schemaUrl);
        RUN;

        /* Fetch the schema detail JSON */
        FILENAME mdfile TEMP;
        PROC HTTP
            METHOD = "GET"
            OUT    = mdfile
            URL    = "%superq(schemaUrl)"
			TIMEOUT= 30;
        RUN;

        LIBNAME mdjson JSON FILEREF=mdfile;
        PROC DELETE DATA=work.root work.alldata; RUN;
        PROC COPY IN=mdjson OUT=work; RUN;
        LIBNAME mdjson;
        FILENAME mdfile;

        DATA mart_schema;
            LENGTH mart_type $30 column_name $50;
            SET root;
            mart_type = "&mart_type.";
        RUN;

        /* Append to (or create) the accumulated schema table */
        %if %sysfunc(exist(&schema_table.)) %then %do;
            PROC APPEND DATA=mart_schema BASE=&schema_table. FORCE;
            RUN;
        %end;
        %else %do;
            DATA &schema_table.;
                SET mart_schema;
                LENGTH primary_key foreign_key 8;
            RUN;
        %end;

        PROC DELETE DATA=work.mart_schema; RUN;

        %set_partitioned_flag(json_all_data=work.alldata, partition_out=&partitioning_table.);

    %end;

%mend get_udm_mart;
