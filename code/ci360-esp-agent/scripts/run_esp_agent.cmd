@echo off
java -Dlogback.configurationFile=logback.xml -DconfigFile=agent.config -jar ${project.artifactId}-${project.version}.jar &
