echo 
echo Starting ESP Subscriber
config=$1
esp_window=`cat ${config} | grep ^esp.Window | awk '{gsub("\r","",$3);print $3}'`
echo ...subscribing to ESP window: $esp_window

java -Dlogback.configurationFile=logback.xml -jar ${project.artifactId}-${project.version}.jar ${config}
echo
