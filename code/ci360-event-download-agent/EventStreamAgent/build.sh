#!/usr/bin/env ksh

JAVA_HOME=/opt/SAS/Thirdparty/jdk-21.0.10+7
MVN_HOME=/opt/SAS/Thirdparty/apache-maven-3.9.14
AGENT_HOME=/opt/SAS/Software/mkt-agent-sdk-3.2603.2603231009
export PATH=${JAVA_HOME}/bin:${MVN_HOME}/bin:${PATH}

mvn install:install-file \
    -Dfile=${AGENT_HOME}/sdk/mkt-agent-sdk-jar-3.2603.2603231009.jar \
    -DpomFile=${AGENT_HOME}/sdk/pom.xml

${MVN_HOME}/bin/mvn clean package -Dproject.build.sourceEncoding=UTF-8