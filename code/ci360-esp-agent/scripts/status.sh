. ./setvars.sh >/dev/null

echo
echo '  ************************************************************'
echo '  * '
echo "  * Agent Configuration File: ${agent_config}"
echo '  * '
echo '  * Running ESP XML server: '
ps -ef | grep esp_xml | grep ${espHttpPort} | awk '{print $2, $8, $12}' | grep -v grep | awk '{print "  *   > PID:",$0}'
echo '  * '
echo '  * '
echo '  * Loaded ESP Projects: '
$DFESP_HOME/bin/dfesp_xml_client -url "http://${espHost}:${espHttpPort}/SASESP/projects" | grep "project name" | awk -F"\"" '{print "  *   >",$2}' 
echo '  * '
echo '  * '
echo '  * Get Event Counts: '
echo '  * '
$DFESP_HOME/bin/dfesp_xml_client -url "http://${espHost}:${espHttpPort}/SASESP/windows?count=true" | awk 'NR>1{print $2,$4,$5}' | head -n -1 | awk -F"\"" 'BEGIN { print "  *   Project                   Window                    Events\n  *   ------------------------- ------------------------- ----------"} {printf "  *   %-25s %-25s %s \n",$4,$2,$6}'
echo '  * '
echo '  * '
echo '  * Running CAS Adapters: '
ps -ef | grep dfesp_cas_adapter | grep ${espPubSubPort} | grep -v grep | awk '{print "  *   > PID:", $2,$13}'
echo '  * '
echo '  * '
echo '  * Running CI360 Agents: '
ps -ef | grep ${project.artifactId} | grep ${agent_config} | grep -v grep | awk '{print "  *   > PID:", $2," ",$11}'
echo '  * '
memory=`top -n 1 | awk 'NR==4 {print $7}'`
mem=${memory/k/}
#memkb=$((mem/1000))
m=`printf "%'d" $mem`
echo "  * Memory free: $m kb"
echo '  * '
echo '  ************************************************************'
echo
