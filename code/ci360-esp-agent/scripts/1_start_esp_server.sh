#cd /opt/sas/viya/home/SASEventStreamProcessingEngine
. ./setvars.sh >/dev/null

echo
echo "start ESP XML server on port ${espHttpPort}"
echo
$DFESP_HOME/bin/dfesp_xml_server -pubsub ${espPubSubPort} -http ${espHttpPort} -plugindir $DFESP_HOME/lib/tk > ./logs/${espLogfile} 2>&1 &

