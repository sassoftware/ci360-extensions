cd /opt/sas/scripts/CI360/ci360-download-client-sas
/opt/sas/SASHome/SASFoundation/9.4/sas -nodms -log logs macros/dsc_download_snapshot.sas
/opt/sas/SASHome/SASFoundation/9.4/sas -nodms -log logs macros/dsc_download_detail.sas
/opt/sas/SASHome/SASFoundation/9.4/sas -nodms -log logs .macros/dsc_download_dbtreport.sas
