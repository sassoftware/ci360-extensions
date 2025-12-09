::
:: Script adapted from : https://blogs.sas.com/content/sgf/2022/09/20/automating-sas-processes-using-windows-batch-files/
::
SetLocal EnableDelayedExpansion

: generate suffix dt=YYYYMMDD_HHMMSS, pad HH with leading 0
set z=!time: =0!
set dt=!date:~-4!!date:~4,2!!date:~7,2!_!z:~0,2!!z:~3,2!!z:~6,2!

: Update the following lines to apply to your environment
:  1. Use the sas variable to specify the location where you installed sas
:  2. Use the proj variable to specify the location where you copied this tool/code
:  3. Use the logdir variable to specify where you want the logs to be written
: NOTE: Do not use spaces in the directory names (except for the sas executable)
:
set sas="C:\Program Files\SASHome\SASFoundation\9.4\sas.exe"
set proj=C:\workspace\360\ci360-extensions-main\code\ci360-engage-direct-dataitem-log-extraction-utility\
set logdir=C:\SAS\Software\DirectAgent\logs
set name=utl_onpremdiext
set pgm="!proj!\!name!.sas"

set log="!logdir!\!name!_!dt!.log"

!sas! -sysin !pgm! -log !log! -nosplash -nologo -icon

: capture exit code from sas
set exitcode=!ERRORLEVEL!

: generate email if ERROR and/or WARNING
if not !exitcode! == 0 (
   set ename=email_error
   set epgm="!proj!\!ename!.sas"
   set elog="!logdir!\!ename!_!dt!.log"
   !sas! -sysin !epgm! -log !elog! -nosplash -nologo -icon -sysparm !log!
)
