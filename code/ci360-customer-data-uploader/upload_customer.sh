#!/bin/sh
#. /etc/profile
#. /home/sas/.profile

DATE=`date +%Y-%m-%d-%H-%M-%S`
LOGFILE='upload_customer_sh_'
APPENDIX='.log'
LOG=$LOGFILE$DATE$APPENDIX
PIDFILE=$HOME/$LOGFILE.pid

echo $PIDFILE

if [ -f $PIDFILE ]
then
  PID=$(cat $PIDFILE)
  ps -p $PID > /dev/null 2>&1
  if [ $? -eq 0 ]
  then
    echo "Process already running"
    exit 1
  else
    ## Process not found assume not running
    echo $$ > $PIDFILE
    if [ $? -ne 0 ]
    then
      echo "Could not create PID file"
      exit 1
    fi
  fi
else
  echo $$ > $PIDFILE
  if [ $? -ne 0 ]
  then
    echo "Could not create PID file"
    exit 1
  fi
fi

/sas/sashome/SASFoundation/9.4/bin/sas_u8 -unbuflog -noterminal -autoexec /sas/sasconfig/Lev1/SASApp/WorkspaceServer/autoexec.sas -sysin /sas/software/ci360-customer-data-uploader/upload_customer_exec.sas  -log /sas/software/ci360-customer-data-uploader/logs/$LOG >/dev/null 2>&1 
rm $PIDFILE