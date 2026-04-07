setlocal
@set JAVA_HOME=C:/SAS/Thirdparty/jdk-21.0.10+7
@set MVN_HOME=C:/SAS/Thirdparty/apache-maven-3.9.14
@set AGENT_HOME=C:/SAS/Software/mkt-agent-sdk-3.2603.2603231009
@set PATH=%JAVA_HOME/bin%;%MVN_HOME%/bin;%PATH%
 
call mvn install:install-file -Dfile=%AGENT_HOME%/sdk/mkt-agent-sdk-jar-3.2603.2603231009.jar -DpomFile=%AGENT_HOME%/sdk/pom.xml



 %MVN_HOME%\bin\mvn.cmd clean package -Dproject.build.sourceEncoding=UTF-8
 endlocal