from sas_ci360_veloxpy import   initApp, APIClient, GLOBAL_CONFIG

import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("Logs_Client_API.log"),
        logging.StreamHandler()
    ]
)


logger = logging.getLogger("ClientRootLogger")

def main():
    print("Initializing application...")
    initApp("<<path>>\\sasci360veloxpy.ini")
    print("Application initialized.")

    print("Global Config:", GLOBAL_CONFIG)

    # from sas_ci360_veloxpy import APIClient

    
    apiClient = APIClient()
    print("---apiClient---", apiClient)
    # # Create and Run a Customer Data Export Job
    # createJob = apiClient.marketing_data.create_and_run_customer_job("GDPR_EXPORT","subject_id",['10001', '10002'],["login_id","subject_id","visitor_id"])
    # print("---res create job---", createJob)
    
    # res = apiClient.marketing_data.get_customer_job_details_by_ID(createJob["id"])

    # id = "e-0c29665e-49a6-448a-b2ba-49617b7a73bb"
    # res = apiClient.marketing_data.get_customer_job_details_by_ID(id)
    # print("---res---", res)


    # # Get an Identity Record for a Customer    
    # moreInfo = apiClient.marketing_data.get_identity_record_by_ID_filter("subjectId","10001")
    # print("---moreInfo---", moreInfo)

    # # Get more info about this  user
    # identityRecord = apiClient.marketing_data.get_identity_record(moreInfo['id'])
    # print("---identityRecord---", identityRecord)

    # # Update Customer Data with an Import Request Job
    # custDataImportJob = apiClient.create_and_run_import_request()
    # print("---custDataImportJob---", custDataImportJob)
    # # Get a Summary of Import Request Jobs
    # summaryJobs = apiClient.marketing_data.get_summary_of_import_requests()
    # print("---summaryJobs---", summaryJobs)
    # recordToFetchInfo = summaryJobs["items"][0]
    # print("---recordToFetchInfo---", recordToFetchInfo)
    # # Get an Import Request Job by ID
    # importJob = apiClient.marketing_data.get_details_of_import_request_by_job_ID(recordToFetchInfo["id"])
    # print("---importJob---", importJob)


    #Customer tables
    
  #   name= "sample_descriptor_Test"
  #   description = "customer tables_test"
  #   type = "customer",
  #   makeAvailableForTargeting = False
    
  #   dataItems = [
  #   {
  #     "name": "customer_id_test",
  #     "label": "Customer ID ",
  #     "description": "Customer ID ",
  #     "type": "STRING",
  #     "tags": [
  #       "DEMOGRAPHICS"
  #     ],
  #     "identity": True,
  #     "key": True,
  #     "identityType": "customer_id",
  #     "excludeFromAnalytics": True
  #   },
  #   {
  #     "name": "email_test",
  #     "label": "email",
  #     "description": "email address",
  #     "type": "STRING",
  #     "tags": [
  #       "DEMOGRAPHICS",
  #       "EMAIL_CONTACT"
  #     ],
  #     "excludeFromAnalytics": True,
  #     "identity": False,
  #     "segmentation": True
  #   },
  #   {
  #     "name": "gender_test",
  #     "label": "Gender",
  #     "description": "Gender",
  #     "type": "STRING",
  #     "tags": [
  #       "DEMOGRAPHICS"
  #     ],
  #     "excludeFromAnalytics": True,
  #     "identity": False,
  #     "predefinedValues": [
  #       "M",
  #       "F"
  #     ],
  #     "segmentProfilingField": True,
  #     "segmentation": True,
  #     "uniqueValuesAvailable": True
  #   },
  #   {
  #     "name": "age_test",
  #     "label": "Age",
  #     "description": "Age",
  #     "type": "INT",
  #     "tags": [
  #       "DEMOGRAPHICS"
  #     ],
  #     "excludeFromAnalytics": True,
  #     "identity": False,
  #     "segmentation": True
  #   },
  #   {
  #     "name": "purchaseDate_test",
  #     "label": "Recent Purchase Date",
  #     "description": "Recent Purchase Date",
  #     "type": "TIMESTAMP",
  #     "tags": [
  #       "DEMOGRAPHICS"
  #     ],
  #     "identity": False,
  #     "excludeFromAnalytics": True,
  #     "segmentation": True
  #   },
  #   {
  #     "name": "purchaseValue_test",
  #     "label": "Recent Purchase Value",
  #     "description": "Recent Purchase Value",
  #     "type": "DOUBLE",
  #     "tags": [
  #       "DEMOGRAPHICS"
  #     ],
  #     "identity": False,
  #     "excludeFromAnalytics": True,
  #     "segmentation": True
  #   },
  #   {
  #     "name": "emailOk_test",
  #     "label": "Email is approved",
  #     "description": "customer can receive emails",
  #     "type": "STRING",
  #     "tags": [
  #       "DEMOGRAPHICS"
  #     ],
  #     "excludeFromAnalytics": True,
  #     "identity": False,
  #     "key": False,
  #     "segmentProfilingField": False,
  #     "uniqueValuesAvailable": False,
  #     "segmentation": False,
  #     "channelContactInformation": True,
  #     "identityAttribute": False
  #   }
  # ]

  #   createTable = apiClient.marketing_data.create_customer_table(name=name, description=description, type=type, makeAvailableForTargeting=makeAvailableForTargeting, dataItems=dataItems)
  #   print("---createTable---", createTable)
#     createTable = {
#     "version": 1,
#     "id": "334a5ec7-aaf5-47af-8f39-dbc05c0347d4",
#     "name": "sample_descriptor",
#     "description": "customer tables using API",
#     "type": "customer",
#     "makeAvailableForTargeting": False,
#     "dataItems": [
#         {
#             "name": "customer_id",
#             "label": "Customer ID ",
#             "description": "Customer ID ",
#             "type": "STRING",
#             "tags": [
#                 "DEMOGRAPHICS"
#             ],
#             "excludeFromAnalytics": True,
#             "identityType": "customer_id",
#             "uniqueValuesAvailable": False,
#             "segmentation": False,
#             "channelContactInformation": False,
#             "identityAttribute": False,
#             "availableForTargeting": False,
#             "segmentProfilingField": False,
#             "identity": True,
#             "key": True
#         },
#         {
#             "name": "email",
#             "label": "email",
#             "description": "email address",
#             "type": "STRING",
#             "tags": [
#                 "DEMOGRAPHICS",
#                 "EMAIL_CONTACT"
#             ],
#             "excludeFromAnalytics": True,
#             "uniqueValuesAvailable": False,
#             "segmentation": True,
#             "channelContactInformation": False,
#             "identityAttribute": False,
#             "availableForTargeting": False,
#             "segmentProfilingField": False,
#             "identity": False,
#             "key": False
#         },
#         {
#             "name": "gender",
#             "label": "Gender",
#             "description": "Gender",
#             "type": "STRING",
#             "tags": [
#                 "DEMOGRAPHICS"
#             ],
#             "excludeFromAnalytics": True,
#             "predefinedValues": [
#                 "M",
#                 "F"
#             ],
#             "uniqueValuesAvailable": True,
#             "segmentation": True,
#             "channelContactInformation": False,
#             "identityAttribute": False,
#             "availableForTargeting": False,
#             "segmentProfilingField": True,
#             "identity": False,
#             "key": False
#         },
#         {
#             "name": "age",
#             "label": "Age",
#             "description": "Age",
#             "type": "INT",
#             "tags": [
#                 "DEMOGRAPHICS"
#             ],
#             "excludeFromAnalytics": True,
#             "uniqueValuesAvailable": False,
#             "segmentation": True,
#             "channelContactInformation": False,
#             "identityAttribute": False,
#             "availableForTargeting": False,
#             "segmentProfilingField": False,
#             "identity": False,
#             "key": False
#         },
#         {
#             "name": "purchaseDate",
#             "label": "Recent Purchase Date",
#             "description": "Recent Purchase Date",
#             "type": "TIMESTAMP",
#             "tags": [
#                 "DEMOGRAPHICS"
#             ],
#             "excludeFromAnalytics": True,
#             "uniqueValuesAvailable": False,
#             "segmentation": True,
#             "channelContactInformation": False,
#             "identityAttribute": False,
#             "availableForTargeting": False,
#             "segmentProfilingField": False,
#             "identity": False,
#             "key": False
#         },
#         {
#             "name": "purchaseValue",
#             "label": "Recent Purchase Value",
#             "description": "Recent Purchase Value",
#             "type": "DOUBLE",
#             "tags": [
#                 "DEMOGRAPHICS"
#             ],
#             "excludeFromAnalytics": True,
#             "uniqueValuesAvailable": False,
#             "segmentation": True,
#             "channelContactInformation": False,
#             "identityAttribute": False,
#             "availableForTargeting": False,
#             "segmentProfilingField": False,
#             "identity": False,
#             "key": False
#         },
#         {
#             "name": "emailOk",
#             "label": "Email is approved",
#             "description": "customer can receive emails",
#             "type": "STRING",
#             "tags": [
#                 "DEMOGRAPHICS"
#             ],
#             "excludeFromAnalytics": True,
#             "uniqueValuesAvailable": False,
#             "segmentation": False,
#             "channelContactInformation": True,
#             "identityAttribute": False,
#             "availableForTargeting": False,
#             "segmentProfilingField": False,
#             "identity": False,
#             "key": False
#         }
#     ],
#     "customProperties": {},
#     "createdTimeStamp": "2025-08-25T13:31:55.141Z",
#     "createdBy": "user",
#     "modifiedTimeStamp": "2025-08-25T13:31:55.141Z",
#     "modifiedBy": "user",
#     "links": [
#         {
#             "method": "GET",
#             "rel": "self",
#             "href": "https://extapigwservice-training.ci360.sas.com/marketingData/tables/334a5ec7-aaf5-47af-8f39-dbc05c0347d4",
#             "uri": "/tables/334a5ec7-aaf5-47af-8f39-dbc05c0347d4",
#             "type": "application/vnd.sas.marketing.table"
#         },
#         {
#             "method": "DELETE",
#             "rel": "delete",
#             "href": "https://extapigwservice-training.ci360.sas.com/marketingData/tables/334a5ec7-aaf5-47af-8f39-dbc05c0347d4",
#             "uri": "/tables/334a5ec7-aaf5-47af-8f39-dbc05c0347d4",
#             "type": "application/vnd.sas.marketing.table"
#         }
#     ]
# }
    # # Return a summary of all tables
    allTable = apiClient.get_summary_of_all_tables()
    print("---allTable---", allTable)

    # #Return a table object by ID
    # tableObjById = apiClient.marketing_data.get_table_object_by_ID(createTable["id"])
    # print("---tableObjById---", tableObjById)

    # # Update a table by ID
    # updateTable = apiClient.marketing_data.update_table_by_ID_POST(createTable["id"], description="updated description_test_new_update", makeAvailableForTargeting=True,name="",customProperties="",dataItems="",tableId="",type="")
    # print("---updateTable---", updateTable)

    # Delete a table by ID
    # deleteTable = apiClient.marketing_data.delete_table_by_ID(createTable["id"])
    # print("---deleteTable---", deleteTable)

    #Create a table job
    # tableJob = apiClient.marketing_data.create_table_job(tableId=createTable["id"], jobType="IMPORT", dataDescriptorId="local_file", fileLocation="C:\\<<path>>\\examples\\customer_data_test.csv", headerRowIncluded=True, includeSourceAndTimestamp=True)
    # print("---tableJob---", tableJob)

    #Return a specific table job by ID
    # tableJobRunById = apiClient.marketing_data.get_specific_table_job_details_by_ID(tableJob["id"])
    # print("---tableJobRunById---", tableJobRunById)

    # getSignedUrl = apiClient.create_signed_URL_to_upload_files_for_customer_table()
    # logger.info("Signed URL to upload files: %s", getSignedUrl["signedURL"])
    # location = getSignedUrl["signedURL"]
    # print("---location---", location)

    # Simulate file upload using requests library like curl command
    # import requests

    # file_path = r"<<path>>\\examples\\Resources\\TestDataCustomerData.csv"
    # url = location

    # with open(file_path, 'rb') as f:
    #   response = requests.put(url, data=f)
    #   print("Upload Response:", response.json())

    # time.sleep(60)
    
    # createTableJob = apiClient.create_and_run_import_request(name="sample_descriptor",dataDescriptorId="334a5ec7-aaf5-47af-8f39-dbc05c0347d4", fileLocation=location, fileType="CSV", fieldDelimiter=",", headerRowIncluded=False, recordLimit=1000)
    # print("---createTableJob---", createTableJob)


if __name__ == "__main__":
    main()