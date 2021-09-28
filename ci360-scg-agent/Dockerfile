FROM openjdk:8-jre-alpine
LABEL version="${project.version}"
LABEL author="Bojan Belovic"
LABEL company="SAS Institute"
# build arguments
ARG VERSION=${project.version}
# copy app into image
COPY . /opt/ci360-scg-agent/
# just dir output for debug
# RUN ls -la /opt/ci360-scg-agent/*
# set working dir
WORKDIR /opt/ci360-scg-agent
# Inform Docker that the container is listening on the specified port at runtime
EXPOSE 8080
# set HEALTHCHECK
HEALTHCHECK CMD ps -ef | grep java | grep ci360-scg-agent
# run application with this command line
ENTRYPOINT ["java", "-Dlogback.configurationFile=logback.xml", "-DconfigFile=agent.config", "-Xms16m", "-Xmx256m", "-jar", "ci360-scg-agent-${project.version}.jar"]
