echo
echo 'Running ESP XML server: '
echo
ps -ef | grep esp_xml | awk '{print $2, $8, $12}' | grep -v grep
echo
if [ $# -gt 0 ]
then
    port=$1
    echo "You are killing ESP server with port: $port"
    pid=`ps -ef | grep esp_xml | grep $port | grep -v grep | awk '{print $2}'`
    kill -9 $pid
fi

echo
