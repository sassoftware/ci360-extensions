@echo off

REM Set default URL if no command-line argument is provided
if "%1"=="" (
  set URL=https://extapigwservice-prod.ci360.sas.com
) else (
  set URL=%1
)

echo %URL%
echo ------------------------------
curl.exe -o NUL -s -w "DNS Lookup Time   : %%{time_namelookup} s\nTCP Connect Time  : %%{time_connect} s\nSSL Handshake     : %%{time_appconnect} s\nTime To First Byte: %%{time_starttransfer} s\nTotal Time        : %%{time_total} s\n" %URL%
echo ------------------------------
