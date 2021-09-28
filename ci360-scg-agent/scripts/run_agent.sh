#!/bin/bash
echo
echo Starting CI360 Custom Agent
logfile=./logs/agent.log
export d=`date +%Y-%m-%d_%H%M%S`

if [ -f ${logfile} ]; then
    mv ${logfile} ./logs/${d}_agent.log
fi

java -Dlogback.configurationFile=logback.xml -DconfigFile=agent.config -Xms16m -Xmx256m -jar ${project.artifactId}-${project.version}.jar > ${logfile} 2>&1 &
echo
