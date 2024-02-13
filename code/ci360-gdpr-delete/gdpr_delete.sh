#!/bin/sh
#. /etc/profile
#. /home/sas/.profile

DATE=`date +%Y-%m-%d-%H-%M-%S`
LOGFILE='gdpr_delete_sh_'
APPENDIX='.log'
LOG=$LOGFILE$DATE$APPENDIX

nohup /sas/sashome/SASFoundation/9.4/sas -unbuflog -noterminal -autoexec /sas/config/Lev1/SASApp/WorkspaceServer/autoexec.sas -sysin /sas/ci360/ci360-gdpr-delete/GDPR_delete_exec.sas  -log /sas/log/ci360/$LOG >/dev/null 2>&1 &
