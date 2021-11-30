@echo off
set JAVA_HOME=C:\Program Files\Java\jdk1.8.0_191

curl -L https://extapigwservice-prod.ci360.sas.com/marketingGateway/agent --output mkt-agent-sdk.zip

"%JAVA_HOME%\bin\jar.exe" -tf mkt-agent-sdk.zip | cmd /q /v:on /c "set/p .=&echo(!.!" >> agent_folder.txt
for /f "delims=/" %%i in (agent_folder.txt) do set output=%%i
del agent_folder.txt

"%JAVA_HOME%\bin\jar.exe" -xvf mkt-agent-sdk.zip %output%/lib/

cd %output%
move lib ..
cd ..
rmdir %output%
del mkt-agent-sdk.zip
