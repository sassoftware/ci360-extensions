export pwd=`pwd`
export d=`date +%Y-%m-%d_%H%M%S`
export folder=`pwd | awk '{gsub("/esp_sid_connector","",$0);print $0}' | awk -F"/" '{print $NF}'`

export agent_config=agent.config

# ESP Settings
export espHost=sasserver.demo.sas.com
export espPubSubPort=3390
export espHttpPort=3389
export espLogfile=espserver_${espHttpPort}.log
export espProjectFile=${pwd}/CI360_Event_Stream.xml
export espProject=CI360_Event_Stream
export espContQuery=eventstream
export espServerUrl=http://${espHost}:${espHttpPort}
export espProjectUrl=${espServerUrl}/SASESP/projects/${espProject}

# CAS Adapter Settings
export cas1_connect_to_espWindow=${espProject}/${espContQuery}/ci360_events
export cas1_table=ci360_events

# Viya Settings
export SSLCALISTLOC=/opt/sas/viya/config/etc/SASSecurityCertificateFramework/cacerts/trustedcerts.pem
export DFESP_JAVA_TRUSTSTORE=/opt/sas/viya/config/etc/SASSecurityCertificateFramework/cacerts/trustedcerts.jks
export DFESP_HOME=/opt/sas/viya/home/SASEventStreamProcessingEngine/current
export LD_LIBRARY_PATH=$DFESP_HOME/lib:/opt/sas/viya/home/SASFoundation/sasexe

