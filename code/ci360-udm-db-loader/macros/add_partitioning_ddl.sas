/******************************************************************************/
/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.*/
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/

%macro add_partitioning_ddl(database=, table_name=, column_name=, key_list=, column_datatype=);
    %local database table_name column_name key_list column_datatype;

    /* ------------------------------------------------------------------ */
    /* Oracle: MODIFY ... PARTITION BY RANGE INTERVAL                      */
    /* ------------------------------------------------------------------ */
    %if &database. = ORACLE %then %do;
        DATA _NULL_;
            FILE ddlfile MOD;
            day_txt = PUT(today(), yymmdd10.);
            PUT +3 "EXECUTE (ALTER TABLE %nrstr(&dbschema)..&table_name MODIFY";
            PUT +6 "PARTITION BY RANGE( &column_name ) INTERVAL (NUMTODSINTERVAL(1, 'DAY')) (";
            PUT +9 "PARTITION p0 VALUES LESS THAN (TO_DATE('" day_txt +(-1) "','YYYY-MM-DD') )";
            PUT +9 "SEGMENT CREATION IMMEDIATE";
            PUT +6 ')) BY &database.;';
        RUN;
    %end;

    /* ------------------------------------------------------------------ */
    /* SQL Server: partition function, scheme, and clustered primary key   */
    /* ------------------------------------------------------------------ */
    %if &database. = SQLSVR %then %do;
        %let pf_name = PF_&table_name._&column_name.;
        %let ps_name = PS_&table_name._&column_name.;
        %let pk_name = &table_name._pk;

        DATA _NULL_;
            FILE ddlfile MOD;

            /* Create monthly partition function if it does not already exist */
            PUT "    EXECUTE (";
            PUT "        IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = '&pf_name.') BEGIN";
            PUT "            DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));";
            PUT "            DECLARE @endDate   date = DATEADD(MONTH,  1,  CONVERT(date, GETDATE()));";
            PUT "            DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION &pf_name. (&column_datatype.) AS RANGE RIGHT FOR VALUES ';";
            PUT "    ;WITH d AS (";
            PUT "    SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d";
            PUT "                UNION ALL";
            PUT "                SELECT DATEADD(MONTH, 1, d) FROM d";
            PUT "                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)";
            PUT "            )";
            PUT "    SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +";
            PUT "        N'''' + CONVERT(varchar(10), d, 120) + N''''";
            PUT "        FROM d OPTION (MAXRECURSION 0);";
            PUT "            SET @sql = @sql + N');';";
            PUT "            EXEC sys.sp_executesql @sql;";
            PUT "        END";
            PUT '    ) BY &database.;';

            /* Create partition scheme if it does not already exist */
            PUT '    EXECUTE (';
            PUT '        IF NOT EXISTS (';
            PUT '            SELECT 1';
            PUT '            FROM sys.partition_schemes';
            PUT "            WHERE name = '&ps_name.'";
            PUT '        )';
            PUT '        BEGIN';
            PUT "            CREATE PARTITION SCHEME &ps_name.";
            PUT "              AS PARTITION &pf_name.";
            PUT '              ALL TO ([PRIMARY]);';
            PUT '        END';
            PUT '    ) BY &database.;';

            /* Add clustered primary key on the partition scheme */
            PUT '    EXECUTE (';
            PUT '        ALTER TABLE &dbschema..' "&table_name.";
            PUT "          ADD CONSTRAINT &pk_name.";
            if index(upcase(cats(',',&key_list.,',')),upcase(cats(',',"&column_name.",','))) then do;
                PUT "           PRIMARY KEY CLUSTERED ( " &key_list. " )" ;
			end;
            else do;
                PUT "           PRIMARY KEY CLUSTERED (&column_name., " &key_list. " )" ;
            end;
			PUT "            ON &ps_name.(&column_name.);";
            PUT '    ) BY &database.;';
        RUN;
    %end;

%mend add_partitioning_ddl;
