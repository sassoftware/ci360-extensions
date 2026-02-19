set sas="C:\Program Files\SASHome\SASFoundation\9.4\sas.exe"
set dw=C:\sas\ci360-download-client-sas
set db=C:\sas\ci360-udm-db-loader
%sas% -sysin %dw%\macros\dsc_download_snapshot.sas -log %dw%\logs -nosplash -nologo -icon
%sas% -sysin %db%\udmloader_launcher\UDMLoader.sas -sysparm LOADDATA -log %db%\logs -nosplash -nologo -icon
%sas% -sysin %dw%\macros\dsc_download_detail.sas -log %dw%\logs -nosplash -nologo -icon
%sas% -sysin %db%\udmloader_launcher\UDMLoader.sas -sysparm LOADDATA -log %db%\logs -nosplash -nologo -icon
%sas% -sysin %dw%\macros\dsc_download_dbtReport.sas -log %dw%\logs -nosplash -nologo -icon
%sas% -sysin %db%\udmloader_launcher\UDMLoader.sas -sysparm LOADDATA -log %db%\logs -nosplash -nologo -icon

