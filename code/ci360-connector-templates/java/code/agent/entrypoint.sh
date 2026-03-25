#!/bin/bash
set -Eeuo pipefail

sed -i \
    -e "s/event.streaming.endPointNodeName=.*/event.streaming.endPointNodeName=${CI360_GATEWAY_HOST}/" \
    /opt/agent/config/agent-endpoints.properties

sed -i \
    -e "s/event.streaming.tenantID=.*/event.streaming.tenantID=${CI360_TENANT_ID}/" \
    /opt/agent/config/agent-runtime.properties

sed -i \
    -e "s/event.streaming.clientSecret=.*/event.streaming.clientSecret=${CI360_CLIENT_SECRET}/" \
    -e "s/event.streaming.jwt=/#event.streaming.jwt=/" \
    /opt/agent/config/event-streaming-configuration.properties

sed -i \
    -e "s/agent.shell.enable=.*/agent.shell.enable=false/" \
    /opt/agent/config/agent-shell.properties

/opt/agent/bin/mkt-agent-sdk
