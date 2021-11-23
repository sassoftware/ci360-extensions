@echo off

set CONFIG=config.properties

echo Starting ESP Subcribe Client...

java -Dlogback.configurationFile=logback.xml -jar ${project.artifactId}-${project.version}.jar %CONFIG%

pause