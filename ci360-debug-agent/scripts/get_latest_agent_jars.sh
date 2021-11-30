#!/bin/bash
curl -L https://extapigwservice-prod.ci360.sas.com/marketingGateway/agent --output mkt-agent-sdk.zip

output=`jar -tf mkt-agent-sdk.zip | head -1 | cut -d "/" -f 1`

echo $output
jar -xvf mkt-agent-sdk.zip $output/lib/
mv $output/lib .
rm -R $output
rm mkt-agent-sdk.zip
