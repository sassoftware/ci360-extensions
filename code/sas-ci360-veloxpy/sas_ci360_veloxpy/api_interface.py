
from .apis.marketing_audience.audiences import marketing_audiences
from .apis.marketing_gateway.marketing_gateway import MarketingGatewayApi
from .apis.marketing_execution.marketing_execution import MarketingExecutionApi
from .apis.marketing_data.marketing_data import MarketingDataApi

from .io.log import get_logger

logger = get_logger("APIClientInterface")

class APIClient:
    def __init__(self):
        self.marketing_audience = marketing_audiences()
        self.marketing_gateway = MarketingGatewayApi()
        self.marketing_execution = MarketingExecutionApi()
        self.marketing_data = MarketingDataApi()
        self.scim = None

    # Optional: wrapper methods
    # Marketing Audience API's
        
    def get_audiences(self):  
        """
        Return a list of all audiences, including the audiences created in the application and the audiences created using the API.

        query Parameters
        -----------------

        sortBy : string
        
            Default: "modifiedTimestamp:descending"
            Enum: "modifiedTimestamp:ascending" "modifiedTimestamp:descending"
            Specify the sort field and order for audience result collection.

        start : integer <int32>

            Default: 0
            The index of the first audience to return. The default value is 0.

        limit : integer <int32>
        
            Default: 10
            The maximum number of audiences to return. The default value is 10.

        status : string
        
            Default: "all"
            Enum: "all" "inactive" "canceled_by_user" "paused_by_system" "failed"
            The current status of the audience. The default is all.

        source : string
        
            Default: "all"
            Enum: "all" "snowflake" "google_bigquery" "customer_defined"
            The specified audience source. Default is all.

        audienceType : string
        
            Default: "all"
            Enum: "all" "upload" "clouddb"
            The specified audience type. Default is all.

        name :string

            The audience name.
        """
        return self.marketing_audience.sync_get_audiences()
    

    def async_get_audiences(self):
        return self.marketing_audience.async_get_audiences(),
            
    def get_audience_by_id(self, audience_id):
        return self.marketing_audience.get_audience_by_id(audience_id)
    
    def delete_audience_by_id(self, audience_id):
        return self.marketing_audience.delete_audience_by_id(audience_id)

    def patch_audience(self, audience_id):
        return self.marketing_audience.patch_audience(audience_id)  
    
    def update_audience_by_id(self, file_path, audienceId):
        return self.marketing_audience.update_audience_by_id(file_path, audienceId)

    def upload_file_for_external_events(self,file_path):
        """
        Submits a request to upload a file with external events. When you send this request with the appropriate JSON body, the response includes a signed, temporary URL. Use this URL to upload a bulk events file.

        You can use a PUT request to upload the file through the REST API.

        Parameters
        ----------
        file_path : str
            Provides file path to upload the csv file

        Request Body Schema
        -------------------
        The JSON body that triggers the system to generate a URL.

        applicationId: string, required
            Default: "eventGenerator"
            The application ID for the external event. To upload batch events, the value should be set to eventGenerator.

        version	: integer <int32>
            Default: 1
            The version of the application.
        """
        return self.marketing_gateway.upload_file_for_external_events(file_path)
    
    def create_audience_definition(self, file_path):
        """
        Creates an audience definition and an audience. This API supports uploading data using a CSV file. You can also create the audience from the Audience Definitions page in the application and upload the data using the API. You can see the list of audience definitions created by navigating to General Settings > Audience Definitions. You can see the audience by navigating to Audiences.

        Request Body schema:
        --------------------------

        version	: integer
            
            Contains the schema version number for the media type. This representation is version 1.

        audienceId: string
            
            The audience identifier

        name : string
            
            Contains the name of the object.

        description	: string
            
            Contains the audience description

        source : string
            
            Value: "customer_defined"   
            The source of the audience  

        iconName : string

            The icon to use for the audience

        expiration : number
            The number of days before the audience expires

        identityType : string
            
            The identity type to use for the audience

        identityColumnName : string
            
            The column name to use for the identity information

        emailColumnName	: string
            
            The column name to use for the email identity information

        dataItems : array	          
            Array of objects (AudienceColumnBody)
            A list of columns to be used in the audience
        
        """
        return self.marketing_audience.create_audience_definition(file_path)
    
    def get_signed_url(self):
        return self.marketing_audience.get_signed_url()
    
    def upload_audience(self, file_path):

        return self.marketing_audience.upload_audiences(file_path)

    def get_audience_upload_history(self, audienceId):
        """
        Returns the upload history for a specific audience.

        Path Parameters
        ----------------
        audienceId: audienceId

        """
        return self.marketing_audience.get_audience_upload_history(audienceId)

    def get_file_history_by_upload_id(self, audienceId, historyId):
        """
        Returns the file history for a specific audience and history ID.

        Path Parameters
        ----------------
        audienceId: audienceId
        historyId: historyId

        """
        return self.marketing_audience.get_file_history_by_upload_id(audienceId, historyId)

    # Marketing Execution API's
    def create_job_to_execute_bulk_task(self, task_id):
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
        logger.info("create_job_to_execute_bulk_task", task_id)
        return self.marketing_execution.create_job_to_execute_bulk_task(task_id)

    
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
        return self.marketing_execution.get_a_task_job(task_job_id)

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
        return self.marketing_execution.create_job_to_execute_segment_map(segment_map_id,segment_map_name,folder_path, version,override_schedule)

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
        return self.marketing_execution.get_a_segment_map_job(segment_map_job_id)

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
        return self.marketing_execution.retrieve_response_tracking_codes(task_id, occurrence_id, task_version_id,from_time,to_time,limit,start,to_file,delimiter_param, include_header_row_param)

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
        return self.marketing_execution.retrieve_response_tracking_code_by_id(response_tracking_code_id)

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
        return self.marketing_execution.retrieve_execution_occurrences(task_id,segment_map_id,type,status,from_time,to_time,start_time_from,start_time_to,limit,start,to_file,delimiter_param,include_header_row_param)

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
        return self.marketing_execution.retrieve_execution_occurrences_by_id(occurrence_id)
    

    # Marketing Data API's
    def access_analytic_services(self):
        """
        Get the links to analytic services or items.


        """
        logger.info("access analytic services")
        return self.marketing_data.access_analytic_services()
    
    def get_collection_of_transfer_items(self):
        """
        
        """
        logger.info("Returns a collection of transfer items.")
        return self.marketing_data.get_collection_of_transfer_items()

    def create_transfer_location_to_upload_analytic_data(self, columns, listType):
        """
        Creates a new transfer location that you can use to upload a file. The uploaded file must match the data descriptor in the request body. You can use this resource to define either which products are approved for use in recommendation tasks or which products are excluded from use in recommendation tasks.

        Request Body schema:
        ----------
        columns : list
            Array of analyticColumnInfo objects.
        listType : str
            Enum: "allowlist", "denylist". Type of list being uploaded.
            When the value is allowlist, the data should contain a list of products that can be recommended by a recommendations task. 
            When the value is denylist, the data should contain a list of products that are excluded from recommendations.

        """
        logger.info("Generate transfer location to upload analytic data")
        return self.marketing_data.create_transfer_location_to_upload_analytic_data(columns,listType)

    def get_transfer_result_by_ID(self, transferId):
        """
        Returns a transfer result based on the ID that is specified in the path.

        path Parameters
        -----------------

        transferId: required, string
            
            Contains the transfer result ID.
        """
        logger.info("Returns a transfer result based on the ID that is specified in the path.")
        return self.marketing_data.get_transfer_result_by_ID(transferId=transferId)

    def create_and_run_customer_job(self, jobType, identityType, identityList):
        """
        Create a customer job that targets a specific customer or targets the data associated with the customer.
        
        header Parameters
        -----------------

        Accept: : string
            
            Indicates that the request was accepted.

        Request Body schema: 
        ---------------------

        jobType : required, string
        
            Enum: "GDPR_DELETE" "GDPR_EXPORT"
            Indicates the job type.

        identityType : required, string
        
            Enum: "login_id" "subject_id" "customer_id" "visitor_id" "device_id" "email_id" "identity_id"
            Describes the type of identity that is used in the identityList property. The identities must all be of the same type such as email ID or login ID. The value is used to determine how the associated customer data is accessed and processed.

            Note: Some identity types cannot be used with specific job types. For example, you cannot use visitor_id with a GDPR export.

        identityList : Array of strings
            
            Contains a set of identities for the customer job to process. For example, the property could contain email addresses that uniquely identify customers.
        """
        logger.info("Create and run a customer job")
        return self.marketing_data.create_and_run_customer_job(jobType=jobType, identityType=identityType, identityList=identityList,outputIdentityTypes=None)

    def get_customer_job_details_by_ID(self,customerJobId):
        """
        Returns a customer job based on the ID that is specified.


        path Parameters
        -----------------

        customerJobId : required, string
            
            Contains the unique ID associated with this object.
        """
        logger.info("Returns a customer job based on the ID that is specified.")
        return self.marketing_data.get_customer_job_details_by_ID(customerJobId=customerJobId)
    
    def create_signed_URL_to_upload_files(self):
        """
        Creates and return an object containing a signed URL, to be used for secure file upload to a temporary location.

        
        """
        logger.info("Create a signed URL to upload files")
        return self.marketing_data.create_signed_URL_to_upload_files()
    
    def create_signed_URL_to_upload_files_for_customer_table(self):
        """
        Creates and return an object containing a signed URL, to be used for secure file upload to a temporary location.

        
        """
        logger.info("Create a signed URL to upload files")
        return self.marketing_data.create_signed_URL_to_upload_files_for_customer_table()
    
    
    def get_identity_record_by_ID_filter(self, filterType, value):
        """
        Returns an identity record by the identity's type and the identity's value.

        If you need identity values to filter on, you can download the identity tables from the UDM. For more information, see Download Identity Tables, Metadata Tables, and SAS 360 Plan Tables in the Administration Guide.

        query Parameters
        ---------------------

        identity filter type: required, string
            An identity type that you are filtering on and the corresponding value.

            The parameter name must be one of these values:
                emailId
                customerId
                deviceId
                loginId
                identityId
                subjectId
                visitorId

                For example, this is a partial GET path with a query parameter:

                /marketingData/identityRecords?customerId=larry1234

        """
        logger.info("Return an identity record by ID filter")
        return self.marketing_data.get_identity_record_by_ID_filter(filterType, value)
    
    def get_identity_record(self,identityRecord):
         """
         Returns an identity record based on the specified ID. The response contains metadata that describes an existing identity in SAS Customer Intelligence 360.

        path Parameters
        ---------------------
        identityRecordId : required, string
            
            Contains the unique ID that is associated with this object.
         """
         logger.info("Return an identity record")
         return self.marketing_data.get_identity_record(identityRecord)
    
    def get_summary_of_import_requests(self, dataDescriptorId=None, start=0, limit=10):
        """
        Returns a summary of the import requests. Each returned summary object contains metadata that describes an import request.

        query Parameters
        ----------------------
        dataDescriptorId : string
            
            Contains the ID of the data descriptor (customer table) that is associated with the import requests.

        start : integer <int32>

            Contains the first item to return.

        limit : integer <int32>
        
            Specifies the maximum number of items to return.
        """
        logger.info("Returns a summary of import requests")
        return self.marketing_data.get_summary_of_import_requests(dataDescriptorId=dataDescriptorId, start=start, limit=limit)

    def create_and_run_import_request(self,name="", dataDescriptorId="", fieldDelimiter=",", fileLocation="", fileType="CSV", headerRowIncluded=False, recordLimit=1000, updateMode="upsert"):
        """
        Creates a job that processes uploaded data for a customer table. The import request processes an import file that you uploaded to a temporary URL (with the /marketingData/fileTransferLocation endpoint).

        Request Body schema: 
        -----------------------

        name : string

            The name of the import.

        dataDescriptorId : required, string
            
            The ID of the table that corresponds to the data file that you are uploading.

        fieldDelimiter	: string
            
            The delimiter that separates columns in the data file. Only commas (,) are currently supported.

        fileLocation : required, string
            The signed URL that you generated from the /fileTransferLocation endpoint.

        fileType : required, string

            The file type of the data. Set the value to CSV.

        headerRowIncluded : required, boolean
        
            Specifies whether the file contains a header row. Set to true to skip processing on the first row in the data file.

        recordLimit	: integer

            Specifies the number of records to import. If the value is empty or set to 0, all of the records are imported.

        updateMode : required, string

            Enum: "replace" "upsert"
            The mode in which the imported data is processed. Typically, you should use upsert mode. For more information, see Import Data through the REST API.
        """
        logger.info("Create and run an import request")
        return self.marketing_data.create_and_run_import_request(name=name, dataDescriptorId=dataDescriptorId, fieldDelimiter=fieldDelimiter, fileLocation=fileLocation, fileType=fileType, headerRowIncluded=headerRowIncluded, recordLimit=recordLimit, updateMode=updateMode)

    def get_details_of_import_request_by_job_ID(self, importRequestJobId):
        """
        Returns a specific import request based on the ID of the request job. The response contains a JSON representation of import request that describes an attempt to import table data from a temporary location.

        path Parameters
        ----------------

        importRequestJobId : required, string
        
            Contains the unique ID associated with the import request.
        """
        logger.info("Create and run an import request")
        return self.marketing_data.get_details_of_import_request_by_job_ID(importRequestJobId=importRequestJobId)

    def get_summary_of_all_tables(self, start=0, limit=10, name=None, type=None):
        """
        Returns a summary of tables in the system. Each entry that is returned describes an existing table with a JSON representation.

        query Parameters
        ------------------
        start : integer
            Contains the first item to return.

        limit : integer
            
            Specifies the maximum number of items to return.

        name	: string
        
            Specifies the name of the table to return. The parameter's value must match a table's name exactly.

        type:string
        
            Enum: "customer" "deleteList" "identity" "importedList" "transient" "transientDeviceIdList"
            Specifies the type of tables to return.
            Note: You can also use dataType as an alias for this parameter.
        """
        logger.info("Get summary of all tables")
        return self.marketing_data.get_summary_of_all_tables(start=start, limit=limit, name=name, type=type)

    def create_customer_table(self, name, description, makeAvailableForTargeting, dataItems, customProperties=None):
        """
        Creates a customer table based on the body of the JSON request. The JSON body defines properties of the table and the columns that it contains. For more information about how to define tables in JSON, see Manually Create and Import the JSON for a Customer Table in the Administration Guide.

        Tip: You can also use the user interface to create a table. For more information, see Create a Customer Table in the Administration Guide.

        Request Body schema: 
        ---------------------

        name: required, string

            Specifies the name of this table.

        description: required, string
            
            Describes the purpose of this specific table.

        type: required, string

            Enum: "customer" "identity (deprecated)" "deleteList" "importedList" "transientList" "transientDeviceIdList"
            Describes the type of data that is stored in the table.

                customer is used for uploading customer data. The imported data is processed for identities (regardless of the presence of identity data items).

                Note: Customer tables must include a column with the key property set to true.

                deleteList used for removing customer identities from the data collection. A deleteList descriptor is similar to the customer type of descriptor, except the identities you upload are removed from your data (if they exist) instead of added.

                importedList is used for imports of predefined segments such as a list of subject IDs.

                transient is used for temporary storage of personal identifiable information (PII) while a task uses the segment data.

                transientDeviceIdList is used for device IDs (which are usually mobile devices). This descriptor type is used to target a list of devices based on their device ID. You do not need existing customer data that is related to these devices.

                For more information, see Properties for a Customer Table’s Descriptor in the Administration Guide.

        makeAvailableForTargeting: required, boolean

            Specifies if this table information should provided to real-time targeting after the data is imported.

        dataItems : required, Array of objects (DataItemRequest)

            Contains a list of data items that describe the columns in the table. For example, data items could define columns for the customer_id, a customer's age, and so on.

        customProperties : Array of strings
        
            Contains an object with optional kay/value pairs that contain additional metadata for the table
        """
        logger.info("create customer table")
        return self.marketing_data.create_customer_table(name=name, description=description, type=type, makeAvailableForTargeting=makeAvailableForTargeting, dataItems=dataItems, customProperties=customProperties)

    def get_table_object_by_ID(self, tableId):
        """
        Returns an object that contains metadata that describes an existing table.

        path Parameters
        ------------------

        tableId : required, string
            
            Contains the unique ID that is associated with the table object.        
        """
        logger.info("Return a table object by ID")
        return self.marketing_data.get_table_object_by_ID(tableId=tableId)

    def update_table_by_ID_PATCH(self, tableId, name, description, makeAvailableForTargeting, dataItems, customProperties=None):
        """
        Updates a table from SAS Customer Intelligence 360 based on the ID that is specified.

        path Parameters
        -------------------
        tableId: required, string

            Example: 56aca176-2e73-4ced-adcd-a87817f1ff63
            Contains the unique ID that is associated with this item.

        Request Body schema: 
        ------------------------

        name: required, string

            Specifies the name of this table.

        description: required, string
            
            Describes the purpose of this specific table.

        type: required, string
            
            Enum: "customer" "identity (deprecated)" "deleteList" "importedList" "transientList" "transientDeviceIdList"
            Describes the type of data that is stored in the table.

                customer is used for uploading customer data. The imported data is processed for identities (regardless of the presence of identity data items).
                Note: Customer tables must include a column with the key property set to true.

                deleteList used for removing customer identities from the data collection. A deleteList descriptor is similar to the customer type of descriptor, except the identities you upload are removed from your data (if they exist) instead of added.

                importedList is used for imports of predefined segments such as a list of subject IDs.

                transient is used for temporary storage of personal identifiable information (PII) while a task uses the segment data.

                transientDeviceIdList is used for device IDs (which are usually mobile devices). This descriptor type is used to target a list of devices based on their device ID. You do not need existing customer data that is related to these devices.

            For more information, see Properties for a Customer Table’s Descriptor in the Administration Guide.

        makeAvailableForTargeting : required, string
            
            Specifies if this table information should provided to real-time targeting after the data is imported.

        dataItems : required,  Array of objects (DataItemRequest)
            
            Contains a list of data items that describe the columns in the table. For example, data items could define columns for the customer_id, a customer's age, and so on.

        customProperties : Array of strings

            Contains an object with optional kay/value pairs that contain additional metadata for the table
        """

        logger.info("Update a table by ID by Patch")
        return self.marketing_data.update_table_by_ID_PATCH(tableId=tableId, name=name, description=description,type="", makeAvailableForTargeting=makeAvailableForTargeting, dataItems=dataItems, customProperties=customProperties)

    def update_table_by_ID_POST(self, tableId, name, description, makeAvailableForTargeting, dataItems, customProperties=None):
        """
        Updates a table from SAS Customer Intelligence 360 based on the ID specified.
        This is a POST call that sets the update method to act as a PATCH operation.

        path Parameters
        -------------------
        tableId: required, string

            Example: 56aca176-2e73-4ced-adcd-a87817f1ff63
            Contains the unique ID that is associated with this item.

        query Parameters
        -------------------
        _method : required, string

            Value: "PATCH"
            Specifies the value for the HttpMethod type.
            Note: The _method query parameter is required and should match the listed option exactly.
        
        """
        logger.info("Update a table by ID by POST ")
        return self.marketing_data.update_table_by_ID_POST(tableId=tableId, name=name, description=description,type="", makeAvailableForTargeting=makeAvailableForTargeting, dataItems=dataItems, customProperties=customProperties)

    def delete_table_by_ID(self, tableId):
        """
        Removes a table from SAS Customer Intelligence 360 based on the ID that is specified.

        path Parameters
        ---------------------
        tableId: required, string

            Example: 56aca176-2e73-4ced-adcd-a87817f1ff63
            Contains the unique ID that is associated with this object.

        
        """
        logger.info("Delete a table by ID")
        return self.marketing_data.delete_table_by_ID(tableId=tableId)

    def create_table_job(self, tableId, jobType, dataDescriptorId, fileLocation=None, headerRowIncluded=None, includeSourceAndTimestamp=None):
        """
        Creates and runs a table job.

        Request Body schema: 
        -----------------------

        Contains the object with the information that is necessary to generate and run a job for a customer table.

        jobType : required, string

            Enum: "CONTACT_PREFERENCE_EXPORT" "CONTACT_PREFERENCE_IMPORT" "TABLE_DOWNLOAD"
            The type of table job that you are submitting. When the value is an export, the response includes URLs to download the table's data.

        dataDescriptorId : required, string
                
            The ID of the table that corresponds to the job request.
            Note: When jobType is set to "CONTACT_PREFERENCE_EXPORT", the dataDescriptorId property is not required.

        fileLocation : string

            When jobType is an import (for example "CONTACT_PREFERENCE_IMPORT"), this property is the temporary URL that you used to upload the table's data. Before you submit the table job, use the marketingData/fileTransferLocation endpoint to generate the URL, then upload the import file.

        headerRowIncluded : boolean
            When jobType is an import (for example "CONTACT_PREFERENCE_IMPORT"), this property specifies whether the first row of the input file is a header (and should not be processed).

        includeSourceAndTimestamp : boolean

            For jobs that export contact preferences, setting this attribute to "true" exports these columns:

                preference_valuesource_txt: 
                    the source of the opt-out value. This value can be the ID of an import job or the event that updated the contact preference.
                processed_dttm: 
                    the most recent date that the record was processed.
        """
        logger.info("Create a table job")
        return self.marketing_data.create_table_job(tableId=tableId, jobType=jobType, dataDescriptorId=dataDescriptorId, fileLocation=fileLocation, headerRowIncluded=headerRowIncluded, includeSourceAndTimestamp=includeSourceAndTimestamp)

    def get_specific_table_job_details_by_ID(self, tableJobId):
        """
        Returns an existing table job based on the ID in the path.

        path Parameters
        -----------------

        tableJobId: required, string

            Contains the unique ID that is associated with the table job.
        """
        logger.info("Return a specific table job by ID")
        return self.marketing_data.get_specific_table_job_details_by_ID(tableJobId=tableJobId)
    
    # Marketing Gateway API's
    def download_Discover_Base_Tables_and_Analytical_Base_Tables(self, schema_version, data_url_file_path):
        """
        Downloads the Discover Base Tables and Analytical Base Tables reports.

        """
        logger.info("Download Discover Base Tables and Analytical Base Tables reports")
        return self.marketing_gateway.download_Discover_Base_Tables_and_Analytical_Base_Tables(schema_version=schema_version, data_url_file_path=data_url_file_path)
    def download_Detail_tables(self, schema_version, data_url_file_path):
        """
        Downloads the Detail Tables reports.

        """
        logger.info("Download Detail Tables reports")
        return self.marketing_gateway.download_Detail_tables(schema_version=schema_version, data_url_file_path=data_url_file_path)

    def download_identity_metadata_plan_tables(self, schema_version, data_url_file_path):

        """
        Downloads the Identity, Metadata and Plan Tables reports.

        """
        logger.info("Download Identity, Metadata and Plan Tables reports")
        return self.marketing_gateway.download_identity_metadata_plan_tables(schema_version=schema_version, data_url_file_path=data_url_file_path)

    def download_reprocessed_data(self, schema_version, mart_type, data_url_file_path):

        """
        Downloads the Reprocessed Data reports.

        """
        logger.info("Download Reprocessed Data reports")
        return self.marketing_gateway.download_reprocessed_data(schema_version=schema_version, mart_type=mart_type, data_url_file_path=data_url_file_path)
    
    def send_external_events(self,event_data):     
        """
        Inject an external event into SAS Customer Intelligence 360. Define the external event in the JSON body of the request.

        Request Body schema
        -----------------------
        A string that contains an external event as a JSON object.

        applicationId: string

            This property is reserved for future use.
            Any value that is provided is available only in the streamed event but is not persisted in the database. If you do not provide a value it is set to the name of the access point that is associated with the request.

        eventName: required, string
            
            The name of the event, which must already be defined in SAS Customer Intelligence 360. If the event does not exist in SAS Customer Intelligence 360, the injected event is rejected.

        ID type: required, string

            Enum: "customer_id" "datahub_id" "login_id" "subject_id"
            The identity type that is associated with the event. Set the ID type as the property name and the value to the actual ID.
            For example, this is an example of the property using a subject ID: "subject_id": "123345"
            Note: If you send an ID value that is not in the system (other than a datahub_id value), that ID type and value is created and associated with a new datahub ID. However, if you send a datahub_id value in the payload, this value can be used only for record lookups and does not create a new identity record.

        custom_property: string

            Additional name/value properties that you want to submit with the event.
            The value of the custom property can be any date type, but the property's name must be a string. For example: "Price": 100

        """
        return self.marketing_gateway.send_external_events(event_data)
   
