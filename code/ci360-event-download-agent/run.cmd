

REM #######################################################
REM ### If running manually, export your JAVA_HOME path ###
REM ### For Linux use $var syntax...and "set" is not needed for environment variables ###
REM #######################################################
set JAVA_HOME=C:\Software\jdk-17.0.16_8
set PATH=%JAVA_HOME%\bin;%path%
set JAVACMD="%JAVA_HOME%\bin\java"

REM Use forward slashes in the directory path below
set APP_HOME=C:/SAS/Software/mkt-agent-sdk-3.2512.2512151029

%JAVACMD% -cp ".\EventStreamAgent-1.0-SNAPSHOT.jar;%APP_HOME%/lib/*;%APP_HOME%/libThirdParty/*" -Dlogback.configurationFile="%APP_HOME%/config/logback.xml" -DconfigFile="%APP_HOME%/config/agent-custom-configuration.properties" client.ci360.eventstreamagent.EventStreamAgent

