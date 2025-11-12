#FROM container-registry.oracle.com/java/jdk-no-fee-term:17
FROM eclipse-temurin:21

# download latest agent
RUN curl -L https://extapigwservice-prod.ci360.sas.com/marketingGateway/agent --output /tmp/agent.zip

# unzip
WORKDIR /opt/
RUN jar xf /tmp/agent.zip
RUN ln -s /opt/mkt-agent-sdk* /opt/agent
RUN chmod +x /opt/agent/bin/mkt-agent-sdk

# Config
RUN echo agent.shell.enable=false>/opt/agent/config/agent-shell.properties
RUN echo agent.runtime.local.config=true>>/opt/agent/config/agent-runtime.properties

# JSON Logging
RUN mv /opt/agent/config/logback-spring.xml /opt/agent/config/logback-spring.xml.orig
COPY logback-spring.xml /opt/agent/config/

# Startup script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# start
WORKDIR /opt/agent
ENTRYPOINT /entrypoint.sh

#ENTRYPOINT ["tail", "-f", "/dev/null"]
