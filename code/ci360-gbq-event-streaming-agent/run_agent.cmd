@echo off
java -Dlogback.configurationFile=logback.xml -DconfigFile=agent.config -jar target/tim-ci360-event-stream-agent-2023.02.jar 
