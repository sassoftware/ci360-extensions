. ./setvars.sh >/dev/null
echo 
echo Starting CI360 Agent
java -Dlogback.configurationFile=logback.xml -DconfigFile=agent.config -jar ${project.artifactId}-${project.version}.jar > /dev/null 2>&1 &
echo
