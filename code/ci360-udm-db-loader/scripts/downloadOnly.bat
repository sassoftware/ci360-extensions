set sas="C:\Program Files\SASHome\SASFoundation\9.4\sas.exe"
set proj=C:\sas\ci360-download-client-sas-T36
%sas% -sysin  %proj%\macros\dsc_download_training36_snapshot.sas -log %proj%\logs -nosplash -nologo -icon
%sas% -sysin  %proj%\macros\dsc_download_training36_detail.sas -log %proj%\logs -nosplash -nologo -icon
%sas% -sysin  %proj%\macros\dsc_download_training36_dbtreport.sas -log %proj%\logs -nosplash -nologo -icon
