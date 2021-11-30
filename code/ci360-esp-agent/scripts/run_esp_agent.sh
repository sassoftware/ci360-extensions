#!/bin/bash
echo
echo Starting CI360 ESP Agent
java -Dlogback.configurationFile=logback.xml -DconfigFile=agent.config -jar ${project.artifactId}-${project.version}.jar &
echo
