FROM openjdk:8-jre-alpine
LABEL version="${project.version}"
LABEL author="Bojan Belovic"
LABEL company="SAS Institute"
# build arguments
ARG VERSION=${project.version}
# copy app into image
COPY . /opt/ci360-cas-agent/
# set working dir
WORKDIR /opt/ci360-cas-agent
# set HEALTHCHECK
HEALTHCHECK CMD ps -ef | grep java | grep ci360-cas-agent
# run application with this command line
ENTRYPOINT ["java", "-Dlogback.configurationFile=logback.xml", "-DconfigFile=agent.config", "-Djavax.net.ssl.trustStore=viya4_trustedcerts.jks", "-Xms32m", "-Xmx2048m", "-jar", "ci360-cas-agent-${project.version}.jar"]
