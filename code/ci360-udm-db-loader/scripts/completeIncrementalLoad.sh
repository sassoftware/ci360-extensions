cd /opt/sas/scripts/CI360/ci360-download-client-sas
/opt/sas/SASHome/SASFoundation/9.4/sas -nodms -log logs macros/dsc_download_snapshot.sas
cd /opt/sas/scripts/CI360/ci360-udm-db-loader
/opt/sas/SASHome/SASFoundation/9.4/sas -sysin udmloader_launcher/UDMLoader.sas -sysparm LOADDATA -log logs
cd /opt/sas/scripts/CI360/ci360-download-client-sas
/opt/sas/SASHome/SASFoundation/9.4/sas -nodms -log logs macros/dsc_download_detail.sas
cd /opt/sas/scripts/CI360/ci360-udm-db-loader
/opt/sas/SASHome/SASFoundation/9.4/sas -sysin udmloader_launcher/UDMLoader.sas -sysparm LOADDATA -log logs
cd /opt/sas/scripts/CI360/ci360-download-client-sas
/opt/sas/SASHome/SASFoundation/9.4/sas -nodms -log logs .macros/dsc_download_dbtreport.sas
cd /opt/sas/scripts/CI360/ci360-udm-db-loader
/opt/sas/SASHome/SASFoundation/9.4/sas -sysin udmloader_launcher/UDMLoader.sas -sysparm LOADDATA -log logs
