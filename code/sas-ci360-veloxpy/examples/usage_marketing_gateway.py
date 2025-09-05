# ---------------------------------------------------------------
# Example usage for Marketing Gateway API client
#
# Required parameters:
#   - schema_version: (int) The schema version to use for report downloads.
#   - mart_type: (str) The mart type for reprocessed data (e.g., "detail").
#   - data_url_file_path: (str) Folder path to save output text files.
#
# Methods demonstrated:
#   - download_Discover_Base_Tables_and_Analytical_Base_Tables(schema_version, data_url_file_path)
#       Downloads DBT and ABT reports and writes results to a text file.
#   - download_Detail_tables(schema_version, data_url_file_path)
#       Downloads detail reports and writes results to a text file.
#   - download_identity_metadata_plan_tables(schema_version, data_url_file_path)
#       Downloads identity metadata plan tables and writes results to a text file.
#   - download_reprocessed_data(schema_version, mart_type, data_url_file_path)
#       Downloads reprocessed data for the given mart type and writes results to a text file.
# Event methods:
#   - send_external_events(event_data)
#       Sends a single external event to CI360.
#   - upload_file_for_external_events(file_path)
#       Uploads a bulk CSV file of external events to CI360.
# Output:
#   Each method writes its results to a text file in the specified data_url_file_path folder.
#   Event methods send data to CI360 and return API responses.
# ---------------------------------------------------------------

from sas_ci360_veloxpy import initApp, APIClient, GLOBAL_CONFIG

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("Logs_Client_API.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("GatewayRootLoggerfromClient")
#Make this method async if want to get token 
def main():
    initApp("<<full_path>>\\sasci360veloxpy.ini")    
    logger.info("App Initiated")

    #BUlk Events upload using csv
    # upload_file_for_external_eventsFilePath = "<FullPathToFile>\\BulkUploadEvents.csv"
    # res_data = res.upload_file_for_external_events(upload_file_for_external_eventsFilePath)
    
    # logger.info("res send_external_events %s", res_data)

    #Send Single Event
    # sendSingleEvent()

    apiclient = APIClient()

    schema_version = 17
    # Download reports
    # Mart Types : Enum: "detail" "dbt-report"
    # Example: martType=detail
    # Name of the set of data tables.
    mart_type = "detail"
    data_url_file_path = "<<full_path>>\\examples\\Resources"
    res_data = apiclient.download_Discover_Base_Tables_and_Analytical_Base_Tables(schema_version, data_url_file_path)
    res_data = apiclient.download_Detail_tables(schema_version, data_url_file_path)
    res_data = apiclient.download_identity_metadata_plan_tables(schema_version, data_url_file_path)
    res_data = apiclient.download_reprocessed_data(schema_version, mart_type, data_url_file_path)
    print("res Gateway ", res_data)
if __name__ == "__main__":
    main()