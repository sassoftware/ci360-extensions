/******************************************************************************/
/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.*/
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/

%macro add_partitioning_ddl(database=, table_name=, column_name= ,key_list=, column_datatype=);
%local database table_name column_name key_list column_datatype;


%if &database. = ORACLE %then %do;

	data _null_;
	file ddlfile mod; 

	    PUT +3 "EXECUTE (ALTER TABLE %nrstr(&dbschema)..&table_name MODIFY";
		PUT +6 "PARTITION BY RANGE( &column_name ) INTERVAL (NUMTODSINTERVAL(1, 'DAY')) (";
		/* Oracle Enterprise Editions */			
		day_txt=PUT(today(),yymmdd10.);
		PUT  +9 "PARTITION p0 VALUES LESS THAN (TO_DATE('" day_txt +(-1) "','YYYY-MM-DD') )" ;
		PUT  +9 "SEGMENT CREATION IMMEDIATE" ;
		PUT  +6 ')) by &database.;';

	run;

%end;
	
%if &database. = SQLSVR %then %do;
	%let primary_key_defined=1;
	%let pf_name = PF_&table_name._&column_name;
	%let ps_name = PS_&table_name._&column_name;
	%let pk_name = &table_name._pk;
	%let _key_list = %sysfunc(tranwrd(%superq(key_list),%str(%"),%str()));
	%let keylist = ,%upcase(&_key_list),;
	%let pcol  = ,%upcase(%superq(column_name)),;

	data _null_;

	file ddlfile mod;

		put "	execute (";
		put "		IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = '&pf_name.') BEGIN";
		put "			DECLARE @startDate date = DATEADD(MONTH, -12, CONVERT(date, GETDATE()));";
	 	put "			DECLARE @endDate   date = DATEADD(MONTH,  1, CONVERT(date, GETDATE()));";
		put "			DECLARE @sql nvarchar(max) = N'CREATE PARTITION FUNCTION &pf_name. (&column_datatype.) AS RANGE RIGHT FOR VALUES ';";
		put "	;WITH d AS (";
		put "	SELECT DATEFROMPARTS(YEAR(@startDate), MONTH(@startDate), 1) AS d";
	    put "                UNION ALL";
	    put "                SELECT DATEADD(MONTH, 1, d) FROM d";
	    put "                WHERE d < DATEFROMPARTS(YEAR(@endDate), MONTH(@endDate), 1)";
	    put "			)";
		put "	SELECT @sql = @sql + CASE WHEN d > @startDate THEN N',' ELSE N'(' END +";
		put "		N'''' + CONVERT(varchar(10), d, 120) + N''''";
		put "		FROM d OPTION (MAXRECURSION 0);";
		put "			SET @sql = @sql + N');';";
		put "			EXEC sys.sp_executesql @sql;";
		put "		END";
		PUT'	) by &database.;';
		
		put '  execute (';
		put '    IF NOT EXISTS (';
		put '        SELECT 1';
		put '        FROM sys.partition_schemes';
		put "        WHERE name = '&ps_name.'";
		put '    )';
		put '    BEGIN';
		put "        CREATE PARTITION SCHEME &ps_name.";
		put "          AS PARTITION &pf_name.";
		put '          ALL TO ([PRIMARY]);';
		put '    END';
		put '  ) by &database.;';

		put '  execute (';
		put "    ALTER TABLE &dbschema..&table_name.";
		put "      ADD CONSTRAINT &pk_name.";
		%if %index(%superq(keylist), %superq(pcol)) %then %do;
		  PUT "       PRIMARY KEY CLUSTERED (" &key_list ")";
		%end;
		%else %do;
		  PUT "       PRIMARY KEY CLUSTERED (&column_name. ," &key_list ")";
		%end;
		put "        ON &ps_name.(&column_name.);";
		put '  ) by &database.;';

	run;
%end;


	
%mend add_partitioning_ddl;
/*%add_partitioning;*/
