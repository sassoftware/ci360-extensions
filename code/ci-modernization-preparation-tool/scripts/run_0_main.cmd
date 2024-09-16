:: ---------------------------------------------------------------------------
:: Copyright © 2024, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
:: SPDX-License-Identifier: Apache-2.0
:: ---------------------------------------------------------------------------
@echo off
:: Tool location
set tool_path=C:\sas\ci-modernization-preparation-tool
:: SAS 
set sas="C:\Program Files\SASHome\SASFoundation\9.4\sas.exe"


:: Extract year, month, and day from the system's date
set YEAR=%DATE:~6,4%
set MONTH=%DATE:~3,2%
set DAY=%DATE:~0,2%

:: Extract hour, minute, and second from the system's time
set HOUR=%TIME:~0,2%
set MINUTE=%TIME:~3,2%
set SECOND=%TIME:~6,2%

:: Ensure hour, minute, and second are two digits (add leading zero if necessary)
if %HOUR% LSS 10 set HOUR=0%HOUR:~1,1%
if %MINUTE% LSS 10 set MINUTE=0%MINUTE:~1,1%
if %SECOND% LSS 10 set SECOND=0%SECOND:~1,1%

:: Create a log file with the current timestamp in its name
set file_name=main_%YEAR%-%MONTH%-%DAY%_%HOUR%-%MINUTE%-%SECOND%


:: Invocation
cd %tool_path%
%sas% -sysin .\scripts\0_main.sas -log .\logs\%file_name%.log -print .\data\%file_name%.out 
