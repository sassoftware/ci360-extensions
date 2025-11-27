/***********************************************************************************************************/
/*  */
/* If the download contains a DATEKEY column ADD it to the target */
/***********************************************************************************************************/

%macro add_datekey();
	%if not %sysfunc(exist(work.UDM_COLUMNS)) %then %do;

		PROC SQL;
			CREATE TABLE work.UDM_COLUMNS AS 
		 	SELECT
		 		upcase(libname) as libname,  upcase(memname) as table_nm, upcase(name) as column_nm, 
		 		upcase(type)    as datatype, upcase(format)  as format,   length
			FROM DICTIONARY.COLUMNS
			WHERE upcase(libname) in ('UDMMART', 'TARGET') 
			;

		QUIT;
	%end;	

	proc sql;
		CREATE TABLE work.DATEKEY_TO_ADD AS
		SELECT distinct table_nm
		FROM work.UDM_COLUMNS u 
	 	WHERE libname = 'TARGET' 
	 		AND table_nm not in (SELECT table_nm  FROM work.UDM_COLUMNS WHERE libname = 'TARGET' and column_nm="DATEKEY") 
	 		AND table_nm     in (SELECT table_nm  FROM work.UDM_COLUMNS WHERE libname = 'UDMMART' and column_nm="DATEKEY") 
		ORDER BY 1
		;
	quit;
	
	/* Generate DDL */
	filename sascd temp;
/*	filename sascd "/userdata/dev/common/projects/UDMLoader_Git/cdm-udmloader-sas/code/sascd.sas" mod;	*/

	data _null_;
	/*  set NEW_TABLES end=last; */
	 set work.DATEKEY_TO_ADD end=last;
	    file sascd temp;
/*		file sascd;*/
	  
	    if _n_=1 then do;
			put 'PROC SQL NOERRORSTOP;';
			put "CONNECT TO ORACLE (&sql_passthru_connection.);";
	    end;	 
	    
		put '%if %sysfunc(exist(Target.' table_nm ')) %then %do;';
		%if %upcase(ORACLE)=SASIOGBQ %then %do;
			put +3 "EXECUTE (ALTER TABLE &DBSCHEMA.." table_nm " ADD COLUMN DATEKEY INT64 NOT NULL) BY ORACLE;";
		%end;
		%if %upcase(ORACLE)=ORACLE %then %do;
			put +3 "EXECUTE (ALTER TABLE &DBSCHEMA.." table_nm " ADD DATEKEY DATE NOT NULL) BY ORACLE;";
			put +3 "EXECUTE (ALTER TABLE &DBSCHEMA.." table_nm " MODIFY";
			put +6 "PARTITION BY RANGE(DATEKEY) INTERVAL (NUMTODSINTERVAL(1, 'DAY')) (";
			/* Oracle Enterprise Editions */			
			day_txt=put(today(),yymmdd10.);
			put  +9 "PARTITION p0 VALUES LESS THAN (TO_DATE('" day_txt +(-1) "','YYYY-MM-DD') )" ;
/*			put  +9 "PARTITION p0 VALUES LESS THAN (TO_DATE('" day_txt "00:00','YYYY-MM-DD HH24:MI') )" ;*/
			put  +9 "SEGMENT CREATION IMMEDIATE" ;
			put  +6 ")) by ORACLE;";
/*			put  +6 ") ONLINE) by ORACLE";*/
			put;
		%end;
		put '%end;';
		put ;
	    
	    
	    if last then do;
			put 'DISCONNECT FROM ORACLE;';
			put 'QUIT;'; 
	    end; 
	run;
	
	/* Put generated code to log for increased visibility. */	
	data _null_;
		infile sascd;
		input;	
		put  _infile_;
	run;
		
		
	/* Execute DDL */	
	%include sascd;
	filename sascd;	
	
 
 /* Refresh UDM_COLUMNS if columns were added */
  %let nobs_DATEKEY_TO_ADD=0;
  %let dsid=%sysfunc(open(work.DATEKEY_TO_ADD));
  %let nobs_DATEKEY_TO_ADD=%sysfunc(attrn(&dsid,nlobs));
  %let dsid=%sysfunc(close(&dsid));
  %if &nobs_DATEKEY_TO_ADD %then %do;
  	PROC SQL;
  		CREATE TABLE UDM_COLUMNS AS 
  	 	SELECT
  	 		upcase(libname) as libname,  upcase(memname) as table_nm, upcase(name) as column_nm, 
  	 		upcase(type)    as datatype, upcase(format)  as format,   length
  		FROM DICTIONARY.COLUMNS
  		WHERE upcase(libname) in (&udmmart., &trglib.) 
  		;
  	QUIT;
  %end;
   
%mend add_datekey;	
