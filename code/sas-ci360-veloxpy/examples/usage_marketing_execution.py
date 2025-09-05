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
    apiClient = APIClient()
    print("---apiClient---", apiClient)

    #Bulk Task Jobs
    taskId = "86e5d6c6-6d50-4cfb-b1e3-7408389cef67"

    res=apiClient.create_job_to_execute_bulk_task(taskId)
    logger.info("Response from create_job_to_execute_bulk_task: %s", res)


    resGetDetails = apiClient.get_a_task_job(res["taskJobId"])
    logger.info("Response from get_a_task_job: %s", resGetDetails)

    #Segment Task Jobs
    # segMapTask = "86e5d6c6-6d50-4cfb-b1e3-7408389cef67"

    # res=apiClient.create_job_to_execute_segment_map(segMapTask)
    # logger.info("Response from create_job_to_execute_segment_map: %s", res)


    # resGetDetails = apiClient.get_a_segment_map_job(res["segmentMapJobId"])
    # logger.info("Response from get_a_segment_map_job: %s", resGetDetails)

    #Response Tracking Codes
    #from=2025-01-01T00:00%2B00:00&to=2025-08-25T00:00%2B00:00&limit=2
    # from_date="2025-01-01T00:00%2B00:00"
    # to_date="2025-08-25T00:00%2B00:00"
    # limit=2
    
    # resRTC = apiClient.retrieve_response_tracking_codes(task_id=taskId , from_time=from_date, to_time=to_date, limit=limit)
    # logger.info("Response from retrieve_response_tracking_codes: %s", resRTC)

    # resGetDetailsRTC = apiClient.retrieve_response_tracking_code_by_id(resRTC["responseTrackingCodes"]["rtcId"])
    # logger.info("Response from get_a_segment_map_job: %s", resGetDetailsRTC)


    #Execution occurances
    #from=2025-01-01T00:00%2B00:00&to=2025-08-25T00:00%2B00:00&limit=2
    # from_date="2025-01-01T00:00%2B00:00"
    # to_date="2025-08-25T00:00%2B00:00"
    # limit=2
    
    # resExecOccurances = apiClient.retrieve_execution_occurrences(task_id=taskId , from_time=from_date, to_time=to_date, limit=limit)
    # logger.info("Response from retrieve_response_tracking_codes: %s", resExecOccurances)

    # resGetDetailsExecOccurances = apiClient.retrieve_execution_occurrences_by_id(resExecOccurances["occurrences"]["occurrenceId"])
    # logger.info("Response from get_a_segment_map_job: %s", resGetDetailsExecOccurances)
    
if __name__ == "__main__":
    main()