# Extend BaseAPI, add endpoint-specific methods.
from ..base.base_service import SASCI360VeloxPyBaseService
import requests
import json
from aiohttp import FormData
from ...config.config_loader import get_config, deep_clone, replace_path_params
from ...io.log import get_logger

logger = get_logger("MarketingExecutionApi")
# MarketingGatewayApi class to handle marketing gateway specific API calls  

class MarketingExecutionApi(SASCI360VeloxPyBaseService):
    def __init__(self, config={}):
        """
        Initialize the MarketingExecutionApi with optional configuration.
        :param config: Dictionary containing configuration parameters.
        """
        super().__init__()
        self.config = config
            

    def create_job_to_execute_bulk_task(self, task_id=""):
        """
        Initiates a job to execute a bulk task.

        Parameters
        ----------
        task_id : str
            The unique identifier for the task for which the bulk execution is initiated.

        Request Body Schema
        -------------------
        taskId : str, optional
            The unique identifier for the task. Required if `taskName` is not provided.
        taskName : str, optional
            The name of the task. Required if `taskId` is not provided.
        folderPath : str, optional
            The path of the folder where the task exists. Used with `taskId` and/or `taskName`.
        version : int, optional
            The version number of the task job's representation. Default is 1.
        overrideSchedule : bool, optional
            Set to True to override the task's existing schedule and execute immediately.
            The bulk task must be in an Active or Scheduled state.

        Notes
        -----
        - If `taskName` is not unique and exists in multiple folders, an error is returned.
        - Either `taskId` or `taskName` must be provided.
        """
        logger.info("create_job_to_execute_bulk_task: %s", task_id)

        marketing_execution = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_execution")
        marketing_executionObj = deep_clone(marketing_execution)
        # Always create deep copy to avoid updation
        argsDetails = marketing_executionObj['create_job_to_execute_bulk_task']
        # argsDetails["data"]=json.dumps(event_data, ensure_ascii=False)
        print(argsDetails)

        #convert argsDetails to dict if it's a string

        if "data" not in argsDetails or not isinstance(argsDetails["data"], dict):
            argsDetails["data"] = {}

        argsDetails["data"]["taskId"] = task_id
        argsDetails["data"]=json.dumps(argsDetails['data'], ensure_ascii=False)
        logger.debug("Arguments details for sending external single event: %s", argsDetails)
        # Make the API request
        logger.debug("Making API request to send external single event")
        return self.syncRequest('POST', args=argsDetails) 

    
    def get_a_task_job(self, task_job_id):
        """
        Retrieves details for a specific task job.

        Parameters
        ----------
        task_job_id : str
            The unique identifier for the task job.

        Path Parameters
        ---------------
        taskJobId : str
            The unique identifier for the task job.
            Example: c5ff01fb-0222-4ef7-8c4e-466fd0d416b2
        """
        logger.info("Getting a task job details", task_job_id)
        marketing_execution = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_execution")
        marketing_executionObj = deep_clone(marketing_execution)
        argsDetails = marketing_executionObj['get_a_task_job']
        # if argsDetails["path"] includes {param_name} then replace param_name with variable
        # for e.g. if  "path": "/marketingExecution/taskJobs/{taskJobId}", it has {taskJobId} update url  "/marketingExecution/taskJobs/task_job_id"

        argsDetails["path"] = replace_path_params(argsDetails["path"], {"taskJobId": task_job_id})

        logger.debug("Arguments details for getting a task job: %s", argsDetails)
        return self.syncRequest('GET', args=argsDetails)

    def create_job_to_execute_segment_map(self, segment_map_id=None, segment_map_name=None, folder_path=None, version=1, override_schedule=False):
        """
        Initiates a job to execute a segment map.

        Parameters
        ----------
        segment_map_id : str, optional
            The unique identifier for the segment map. Required if `segment_map_name` is not provided.
        segment_map_name : str, optional
            The name of the segment map. Required if `segment_map_id` is not provided.
        folder_path : str, optional
            The path of the folder where the segment map exists.
        version : int, optional
            The version number of the segment map job's representation. Default is 1.
        override_schedule : bool, optional
            Set to True to override the segment map's existing schedule and execute immediately.

        Request Body Schema
        -------------------
        segmentMapId : str, optional
        segmentMapName : str, optional
        folderPath : str, optional
        version : int, optional
        overrideSchedule : bool, optional

        Notes
        -----
        - If `segment_map_name` is not unique and exists in multiple folders, an error is returned.
        - Either `segment_map_id` or `segment_map_name` must be provided.
        - The segment map must be in an Active or Scheduled state to execute.
        """
        logger.info("Create job to execute segment map", segment_map_id or segment_map_name)
        marketing_execution = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_execution")
        marketing_executionObj = deep_clone(marketing_execution)
        # ...existing code...
        argsDetails = marketing_executionObj['create_job_to_execute_segment_map']

        if "data" not in argsDetails or not isinstance(argsDetails["data"], dict):
            argsDetails["data"] = {}
            
        if isinstance(argsDetails["data"], dict):
            argsDetails["data"]["segmentMapId"] = segment_map_id
            argsDetails["data"]["segmentMapName"] = segment_map_name
            argsDetails["data"]["folderPath"] = folder_path
            argsDetails["data"]["version"] = version
            argsDetails["data"]["overrideSchedule"] = override_schedule
        else:
            logger.error("Expected 'data' to be a dict, got %s", type(argsDetails["data"]))
            raise TypeError("Expected 'data' to be a dict")
        # ...existing code...

        logger.debug("Arguments details for creating a job to execute segment map: %s", argsDetails)
        return self.syncRequest('POST', args=argsDetails)

    def get_a_segment_map_job(self, segment_map_job_id):
        """
        Retrieves details for a specific segment map job.

        Parameters
        ----------
        segment_map_job_id : str
            The unique identifier for the segment map job.

        Path Parameters
        ---------------
        segmentMapJobId : str
            The unique identifier for the segment map job.
            Example: c5ff01fb-0222-4ef7-8c4e-466fd0d416b6
        """
        logger.info("Get a segment map job", segment_map_job_id)
        marketing_execution = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_execution")
        marketing_executionObj = deep_clone(marketing_execution)
        # ...existing code...
        argsDetails = marketing_executionObj['get_a_segment_map_job']
        argsDetails["path"] = replace_path_params(argsDetails["path"], {"segmentMapJobId": segment_map_job_id})

        logger.debug("Arguments details to Get a segment map job: %s", argsDetails)
        return self.syncRequest('GET', args=argsDetails)

    def retrieve_response_tracking_codes(self, task_id=None, occurrence_id=None, task_version_id=None, from_time=None, to_time=None, limit=None, start=None, to_file=False, delimiter_param="comma", include_header_row_param=False):
        """
        Retrieves response tracking codes associated with specified criteria.

        Parameters
        ----------
        task_id : str, optional
            Query for response tracking codes associated with the specified task.
        occurrence_id : str, optional
            Query for response tracking codes associated with the specified occurrence.
        task_version_id : str, optional
            Query for response tracking codes associated with the specified task version.
        from_time : str, optional
            Query for occurrences after the specified time (ISO 8601 format).
        to_time : str, optional
            Query for occurrences before the specified time (ISO 8601 format).
        limit : int, optional
            The maximum number of occurrences returned by the query (maximum 500).
        start : str, optional
            The first item to return in the query.
        to_file : bool, optional
            Flag to indicate whether results should be put in a file.
        delimiter_param : str, optional
            If results are put in a file, this specifies the delimiter ("ctrlA" or "comma").
        include_header_row_param : bool, optional
            If results are put in a file, flag to indicate whether results should include a header row.

        Query Parameters
        ----------------
        See above.

        Notes
        -----
        - Date-time fields must use the format {year}-{month}-{day}T{hour (24)}-{minute}{time zone}.
        - Use %2B instead of + in the time zone field.
        """
        logger.info("Retrieving response tracking codes")
        marketing_execution = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_execution")
        marketing_executionObj = deep_clone(marketing_execution)
        # ...existing code...
        argsDetails = marketing_executionObj['retrieve_response_tracking_codes']

        if "params" not in argsDetails or not isinstance(argsDetails["params"], dict):
            argsDetails["params"] = {}

        if isinstance(argsDetails["params"], dict):

            if task_id is not None:
                argsDetails["params"]["taskId"] = task_id
            if occurrence_id is not None:
                argsDetails["params"]["occurrenceId"] = occurrence_id
            if task_version_id is not None:
                argsDetails["params"]["taskVersionId"] = task_version_id
            if from_time is not None:
                argsDetails["params"]["from"] = from_time
            if to_time is not None:
                argsDetails["params"]["to"] = to_time
            if limit is not None:
                argsDetails["params"]["limit"] = limit
            if start is not None:
                argsDetails["params"]["start"] = start
            if to_file is not None:
                argsDetails["params"]["toFile"] = str(to_file).lower()
            if delimiter_param is not None:
                argsDetails["params"]["delimiter"] = delimiter_param
            if include_header_row_param is not None:
                argsDetails["params"]["includeHeaderRow"] = str(include_header_row_param).lower()
        else:
            logger.error("Expected 'query' to be a dict, got %s", type(argsDetails["params"]))
            raise TypeError("Expected 'query' to be a dict")
        
        return self.syncRequest('GET', args=argsDetails)

    def retrieve_response_tracking_code_by_id(self, response_tracking_code_id):
        """
        Retrieves a response tracking code by its ID.

        Parameters
        ----------
        response_tracking_code_id : str
            The unique identifier for the response tracking code.

        Path Parameters
        ---------------
        responseTrackingCodeId : str
            ID of the response tracking code.
        """
        logger.info("Retrieve response tracking code by ID", response_tracking_code_id)
        marketing_execution = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_execution")
        marketing_executionObj = deep_clone(marketing_execution)
        # ...existing code...
        argsDetails = marketing_executionObj['retrieve_response_tracking_code_by_id']
        
        argsDetails["path"] = replace_path_params(argsDetails["path"], {"responseTrackingCodeId": response_tracking_code_id})
        # ...existing code...
        logger.debug("Arguments details for retrieving response tracking code by ID: %s", argsDetails)
        return self.syncRequest('GET', args=argsDetails)

    def retrieve_execution_occurrences(self, task_id=None, segment_map_id=None, type=None, status=None, from_time=None, to_time=None, start_time_from=None, start_time_to=None, limit=None, start=None, to_file=False, delimiter_param="comma", include_header_row_param=False):
        """
        Retrieves execution occurrences based on specified criteria.

        Parameters
        ----------
        task_id : str, optional
            Query for occurrences associated with the specified task.
        segment_map_id : str, optional
            Query for occurrences associated with the specified segment map.
        type : str, optional
            Query for occurrences associated with the specified item type ("task" or "segment").
        status : str, optional
            Query for occurrences with the specified execution status ("Success", "Failure", "In progress").
        from_time : str, optional
            Query for occurrences that ended after the specified time (ISO 8601 format).
        to_time : str, optional
            Query for occurrences that ended before the specified time (ISO 8601 format).
        start_time_from : str, optional
            Query for occurrences that started after the specified time (ISO 8601 format).
        start_time_to : str, optional
            Query for occurrences that started before the specified time (ISO 8601 format).
        limit : int, optional
            The maximum number of occurrences returned by the query (maximum 500).
        start : str, optional
            The first item to return in the query.
        to_file : bool, optional
            Flag to indicate whether results should be put in a file.
        delimiter_param : str, optional
            If results are put in a file, this specifies the delimiter ("ctrlA" or "comma").
        include_header_row_param : bool, optional
            If results are put in a file, flag to indicate whether results should include a header row.

        Query Parameters
        ----------------
        See above.

        Notes
        -----
        - Date-time fields must use the format {year}-{month}-{day}T{hour (24)}-{minute}{time zone}.
        - Use %2B instead of + in the time zone field.
        """
        logger.info("Retrieve execution occurrences")
        marketing_execution = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_execution")
        marketing_executionObj = deep_clone(marketing_execution)
        # ...existing code...
        argsDetails = marketing_executionObj['retrieve_execution_occurrences']
        if isinstance(argsDetails["params"], dict):
            if task_id is not None:
                argsDetails["params"]["taskId"] = task_id
            if segment_map_id is not None:
                argsDetails["params"]["segmentMapId"] = segment_map_id
            if type is not None:
                argsDetails["params"]["type"] = type
            if status is not None:
                argsDetails["params"]["status"] = status
            if from_time is not None:
                argsDetails["params"]["from"] = from_time
            if to_time is not None:
                argsDetails["params"]["to"] = to_time
            if start_time_from is not None:
                argsDetails["params"]["startTimeFrom"] = start_time_from
            if start_time_to is not None:
                argsDetails["params"]["startTimeTo"] = start_time_to
            if limit is not None:
                argsDetails["params"]["limit"] = limit
            if start is not None:
                argsDetails["params"]["start"] = start
            if to_file is not None:
                argsDetails["params"]["toFile"] =str(to_file).lower()
            if delimiter_param is not None:
                argsDetails["params"]["delimiter"] = delimiter_param
            if include_header_row_param is not None:
                argsDetails["params"]["includeHeaderRow"] = str(include_header_row_param).lower()
        else:
            logger.error("Expected 'query' to be a dict, got %s", type(argsDetails["params"]))
            raise TypeError("Expected 'query' to be a dict")

        logger.debug("Arguments details for retrieving execution occurrences: %s", argsDetails)
        return self.syncRequest('GET', args=argsDetails)

    def retrieve_execution_occurrences_by_id(self, occurrence_id):
        """
        Retrieves execution occurrence details by occurrence ID.

        Parameters
        ----------
        occurrence_id : str
            The unique identifier for the occurrence.

        Path Parameters
        ---------------
        occurrenceId : str
            ID of occurrence.
        """
        logger.info("Retrieve execution occurrences", occurrence_id)

        marketing_execution = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_execution")
        marketing_executionObj = deep_clone(marketing_execution)
        # ...existing code...
        argsDetails = marketing_executionObj['retrieve_execution_occurrences_by_id']
        argsDetails["path"] = replace_path_params(argsDetails["path"], {"occurrenceId": occurrence_id})

        logger.debug("Arguments details for retrieving execution occurrences by ID: %s", argsDetails)
        return self.syncRequest('GET', args=argsDetails)