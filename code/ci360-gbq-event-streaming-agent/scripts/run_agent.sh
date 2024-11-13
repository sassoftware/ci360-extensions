#!/bin/bash
echo
echo Starting CI360 Custom Agent
logfile=./logs/agent.log
export d=`date +%Y-%m-%d_%H%M%S`

if [ -f ${logfile} ]; then
    mv ${logfile} ./logs/${d}_agent.log
fi

java -Dlogback.configurationFile=logback.xml -DconfigFile=agent.config -jar target/tim-ci360-event-stream-agent-2023.02.jar > ${logfile} 2>&1 &
echo
