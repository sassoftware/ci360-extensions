#!/bin/bash

set -xe

echo event.streaming.endPointNodeName=$CI360_GATEWAY_HOST > /opt/agent/config/agent-endpoints.properties
echo event.streaming.tenantID=$CI360_TENANT_ID > /opt/agent/config/event-streaming-configuration.properties
echo event.streaming.clientSecret=$CI360_CLIENT_SECRET >> /opt/agent/config/event-streaming-configuration.properties

/opt/agent/bin/mkt-agent-sdk
