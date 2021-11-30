@echo off
java -Dlogback.configurationFile=logback.xml -DconfigFile=agent.config -Xms16m -Xmx256m -jar ${project.artifactId}-${project.version}.jar 
