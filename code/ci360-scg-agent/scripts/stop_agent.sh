#!/bin/bash
echo
echo Stopping CI360 Custom Agent

PID=`ps -eaf | grep ${project.artifactId} | grep java | awk '{print $2}'`
if [[ "" !=  "$PID" ]]; then
  echo "Stopping PID $PID"
  kill $PID
fi
