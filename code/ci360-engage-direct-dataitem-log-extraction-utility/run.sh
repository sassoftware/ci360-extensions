#!/bin/ksh
#
# Make this file executable using the command: chmod +x run.sh
#
# Update mBaseDir to the location where you installed -sas executable
# Update mLogDir to the location where you would like to store the sas log
# Update mCodeDir to the location where you copied this tool/program(s)
#
# Run this shell script using the command:  ./run.sh utl_onpremdiext
# 

mBaseDir="/userdata/sas/sasconfig/Lev1/SASApp"
basefile=$(basename "$1" .sas)
mLogDir="/userdata/dev/common/projects/test/log"
mCodeDir="/userdata/dev/common/projects/test"

mLog=$mLogDir/${basefile}_$(date +"%Y%m%d%H%M%S").log
cd ${mCodeDir}

if [[ -n $1 ]];then
  echo "Running [$1]"
else
  echo "Usage: $0 sas-program.sas"
  exit 1
fi

echo Start `date`
cmd="$mBaseDir/sas.sh -sysin $mCodeDir/$1 -log $mLog -print $mLogDir/${basefile}_$(date +"%Y%m%d%H%M%S").lst"
echo $cmd
$cmd
rc=$?
echo End with rc=$rc at `date`
if [ $rc -ne 0 ]; then
        $mBaseDir/sas.sh -sysin $mCodeDir/email_error.sas -log $mLogDir/email_error_$(date +"%Y%m%d%H%M%S").log -print $mLogDir/email_error_$(date +"%Y%m%d%H%M%S").lst -sysparm $mLog
fi
exit $rc

