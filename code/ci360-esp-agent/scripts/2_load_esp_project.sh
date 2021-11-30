#cd /opt/sas/viya/home/SASEventStreamProcessingEngine
. ./setvars.sh >/dev/null
echo
echo "Load ESP Project: ${espProjectFile}"
echo
$DFESP_HOME/bin/dfesp_xml_client -url ${espProjectUrl} -put "file://${espProjectFile}"
