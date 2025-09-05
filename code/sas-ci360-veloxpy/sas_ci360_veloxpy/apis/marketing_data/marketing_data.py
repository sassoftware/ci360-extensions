# Extend BaseAPI, add endpoint-specific methods.
import json
from ..base.base_service import SASCI360VeloxPyBaseService
from ...config.config_loader import get_config, deep_clone, replace_path_params
from ...io.log import get_logger

logger = get_logger("MarketingDataApi")
# MarketingGatewayApi class to handle marketing gateway specific API calls  

class MarketingDataApi(SASCI360VeloxPyBaseService):
    def __init__(self, config={}):
        """
        Initialize the MarketingExecutionApi with optional configuration.
        :param config: Dictionary containing configuration parameters.
        """
        super().__init__()
        self.config = config
            
    def access_analytic_services(self):
        """
        Get the links to analytic services or items.


        """
        logger.info("access analytic services") 
        marketing_data = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_data")
        marketing_dataObj = deep_clone(marketing_data)
        argsDetails = marketing_dataObj['access_analytic_services']

        logger.debug("Arguments details for accessing analytic services: %s", argsDetails)
        return self.syncRequest('GET', args=argsDetails)

    def get_collection_of_transfer_items(self, sortBy="creationTimeStamp:descending", start=0, limit=10):
        """
        Returns a collection of transfer items.

        query Parameters
        --------------------

        sortBy:  string        
            Default: "creationTimeStamp:descending"
            Enum: "creationTimeStamp:ascending" "creationTimeStamp:descending" "expiryTimeStamp:ascending" "expiryTimeStamp:descending" "transferType:ascending" "transferType:descending"
            Specify the sort field and order for transfer result collection.

        start : integer <int32>        
            Default: 0
            The index of the first transfer item to return. The default value is 0.

        limit : integer <int32>        
            Default: 10
            The maximum number of transfer items to return. The default value is 10.
        """
        logger.info("Returns a collection of transfer items.")
        marketing_data = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_data")
        marketing_dataObj = deep_clone(marketing_data)
        argsDetails = marketing_dataObj['get_collection_of_transfer_items']
        argsDetails["query"]["sortBy"] = sortBy
        argsDetails["query"]["start"] = start
        argsDetails["query"]["limit"] = limit

        logger.debug("Arguments details for getting a collection of transfer items: %s", argsDetails)
        return self.syncRequest('GET', args=argsDetails)


    def create_transfer_location_to_upload_analytic_data(self, columns, listType):
        """
        Creates a new transfer location that you can use to upload a file. The uploaded file must match the data descriptor in the request body. You can use this resource to define either which products are approved for use in recommendation tasks or which products are excluded from use in recommendation tasks.

        Request Body schema: 
        ---------------------
        columns	: Array of objects (analyticColumnInfo)
            Array 
                length	: integer <int32> (Length)
                Specifies the length of the column.
            name :  string (Name)
                Specifies the name of the column.
            type : string (Type)
                Specifies the type of column.

        listType : string
            Enum: "allowlist" "denylist"
            Contains the type of list that is being uploaded.
            When the value is allowlist, the data should contain a list of products that can be recommended by a recommendations task. When the value is denylist, the data should contain a list of products that are excluded from recommendations.
        """
        logger.info("Generate transfer location to upload analytic data")
        marketing_data = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_data")
        marketing_dataObj = deep_clone(marketing_data)
        argsDetails = marketing_dataObj['create_transfer_location_to_upload_analytic_data']
        argsDetails["body"]["columns"] = columns
        argsDetails["body"]["listType"] = listType

        logger.debug("Arguments details for creating transfer location to upload analytic data: %s", argsDetails)
        return self.syncRequest('POST', args=argsDetails)

    def get_transfer_result_by_ID(self, transferId):
        """
        Returns a transfer result based on the ID that is specified in the path.

        path Parameters
        -----------------
        transferId: required, string
            Contains the transfer result ID.
        """
        logger.info("Returns a transfer result based on the ID that is specified in the path.")

        marketing_data = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_data")
        marketing_dataObj = deep_clone(marketing_data)
        argsDetails = marketing_dataObj['get_transfer_result_by_ID']
        argsDetails["path"] = replace_path_params(argsDetails["path"], {"transferId": transferId})  


        logger.debug("Arguments details for getting transfer result by ID: %s", argsDetails)
        return self.syncRequest('GET', args=argsDetails)

    def create_and_run_customer_job(self, jobType, identityType, identityList, outputIdentityTypes):
        """
        Create a customer job that targets a specific customer or targets the data associated with the customer.
        
        header Parameters
        -----------------
        Accept: : string
            Indicates that the request was accepted.

        Request Body schema: 
        ---------------------
        **jobType** : required, string
            Enum: 
            ```
            "GDPR_DELETE" 
            "GDPR_EXPORT"
            ```
            Indicates the job type.
        **identityType** : required, string
            Enum: 
            ```
            "login_id" 
            "subject_id" 
            "customer_id" 
            "visitor_id" 
            "device_id" 
            "email_id" 
            "identity_id"
            ```
            Describes the type of identity that is used in the identityList property. The identities must all be of the same type such as email ID or login ID. The value is used to determine how the associated customer data is accessed and processed.
            **Note**: Some identity types cannot be used with specific job types. For example, you cannot use visitor_id with a GDPR export.
        **identityList** : Array of strings
            Contains a set of identities for the customer job to process. 
            For example, the property could contain email addresses that uniquely identify customers.
        **outputIdentityTypes** : Array of strings
            Contains a set of identity types that are expected in the output for the customer job.
        """
        logger.info("Create and run a customer job")
        
        if not isinstance(identityList, list):
            raise ValueError("identityList must be a list of strings.")

        for identity in identityList:
            if not isinstance(identity, str):
                raise ValueError("All elements in identityList must be strings.")
        
        if jobType not in ["GDPR_DELETE", "GDPR_EXPORT"]:
            raise ValueError("Invalid jobType. Must be one of: ['GDPR_DELETE', 'GDPR_EXPORT']")

        if identityType not in ["login_id", "subject_id", "customer_id", "visitor_id", "device_id", "email_id", "identity_id"]:
            raise ValueError("Invalid identityType. Must be one of: ['login_id', 'subject_id', 'customer_id', 'visitor_id', 'device_id', 'email_id', 'identity_id']")

        if jobType is None:
            raise ValueError("jobType is required.")
        if identityType is None:
            raise ValueError("identityType is required.")
        
        marketing_data = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_data")
        marketing_dataObj = deep_clone(marketing_data)
        argsDetails = marketing_dataObj['create_and_run_customer_job']
                
        if "data" not in argsDetails or not isinstance(argsDetails["data"], dict):
            argsDetails["data"] = {}

        # argsDetails["data"]["jobType"] = jobType
        # argsDetails["data"]["identityType"] = identityType
        # argsDetails["data"]["identityList"] = identityList
        # argsDetails["data"]["outputIdentityTypes"] = outputIdentityTypes
        argsDetails["data"] ={
            "jobType": jobType,
            "identityType": identityType,
            "identityList": identityList,
            "outputIdentityTypes": outputIdentityTypes
        }
        #Required to convert to json as per requiremnt 
        argsDetails["data"]=json.dumps(argsDetails['data'], ensure_ascii=False)
        
        logger.debug("Arguments details for creating and running a customer job: %s", argsDetails)
        return self.syncRequest('POST', args=argsDetails)

    def get_customer_job_details_by_ID(self, customerJobId):
        """
        Returns a customer job based on the ID that is specified.

        path Parameters
        -----------------
        customerJobId : required, string
            Contains the unique ID associated with this object.
        """
        logger.info("Returns a customer job based on the ID that is specified.")
        marketing_data = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_data")
        marketing_dataObj = deep_clone(marketing_data)
        argsDetails = marketing_dataObj['get_customer_job_details_by_ID']
        argsDetails["path"] = replace_path_params(argsDetails["path"], {"customerJobId": customerJobId})

        logger.debug("Arguments details for getting customer job by ID: %s", argsDetails)
        return self.syncRequest('GET', args=argsDetails)

    def create_signed_URL_to_upload_files_for_customer_table(self):
        """
        Creates and return an object containing a signed URL, to be used for secure file upload to a temporary location.

        """
        logger.info("Create a signed URL to upload files")
        marketing_data = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_data")
        marketing_dataObj = deep_clone(marketing_data)
        argsDetails = marketing_dataObj['create_signed_URL_to_upload_files_for_customer_table']

        logger.debug("Arguments details for creating signed URL to upload files: %s", argsDetails)
        return self.syncRequest('POST', args=argsDetails)
    
    def create_signed_URL_to_upload_files(self):
        """
        Creates and return an object containing a signed URL, to be used for secure file upload to a temporary location.

        """
        logger.info("Create a signed URL to upload files")
        marketing_data = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_data")
        marketing_dataObj = deep_clone(marketing_data)
        argsDetails = marketing_dataObj['create_signed_URL_to_upload_files']

        logger.debug("Arguments details for creating signed URL to upload files: %s", argsDetails)
        return self.syncRequest('POST', args=argsDetails)

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
        marketing_data = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_data")
        marketing_dataObj = deep_clone(marketing_data)
        argsDetails = marketing_dataObj['get_identity_record_by_ID_filter']

        if "params" not in argsDetails or not isinstance(argsDetails["params"], dict):
            argsDetails["params"] = {}

        argsDetails["params"][filterType] = value
        # argsDetails["params"]["value"] = value
        
        #TODO: Check this if any encryption required as its identity records
        logger.debug("Arguments details for getting identity record by ID filter: %s", argsDetails)
        return self.syncRequest('GET', args=argsDetails)

    def get_identity_record(self, identityRecordId):
        """
        Returns an identity record based on the specified ID. The response contains metadata that describes an existing identity in SAS Customer Intelligence 360.

        path Parameters
        ---------------------
        identityRecordId : required, string
            Contains the unique ID that is associated with this object.
        """
        logger.info("Return an identity record")
        marketing_data = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_data")
        marketing_dataObj = deep_clone(marketing_data)
        argsDetails = marketing_dataObj['get_identity_record']
        argsDetails["path"] = replace_path_params(argsDetails["path"], {"identityRecordId": identityRecordId})

        logger.debug("Arguments details for getting identity record: %s", argsDetails)
        return self.syncRequest('GET', args=argsDetails)

    def get_summary_of_import_requests(self,  dataDescriptorId=None, start=0, limit=10):
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
        marketing_data = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_data")
        marketing_dataObj = deep_clone(marketing_data)
        argsDetails = marketing_dataObj['get_summary_of_import_requests']

        if "data" not in argsDetails or not isinstance(argsDetails["data"], dict):
            argsDetails["data"] = {}

        argsDetails["data"]["dataDescriptorId"] = dataDescriptorId
        argsDetails["data"]["start"] = start
        argsDetails["data"]["limit"] = limit

        logger.debug("Arguments details for getting summary of import requests: %s", argsDetails)
        return self.syncRequest('GET', args=argsDetails)

    def create_and_run_import_request(self,  name, dataDescriptorId, fieldDelimiter, fileLocation, fileType, headerRowIncluded, recordLimit, updateMode):
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
        # getSignedUrl = self.create_signed_URL_to_upload_files_for_customer_table()
        # logger.info("Signed URL to upload files: %s", getSignedUrl["signedURL"])
        marketing_data = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_data")
        marketing_dataObj = deep_clone(marketing_data)
        argsDetails = marketing_dataObj['create_and_run_import_request']
        if "data" not in argsDetails or not isinstance(argsDetails["data"], dict):
            argsDetails["data"] = {}

        argsDetails["data"]["name"] = name
        argsDetails["data"]["dataDescriptorId"] = dataDescriptorId
        argsDetails["data"]["fieldDelimiter"] = fieldDelimiter
        argsDetails["data"]["fileLocation"] = fileLocation
        argsDetails["data"]["fileType"] = fileType
        argsDetails["data"]["headerRowIncluded"] = headerRowIncluded
        argsDetails["data"]["recordLimit"] = recordLimit
        argsDetails["data"]["updateMode"] = updateMode

        argsDetails["data"]=json.dumps(argsDetails['data'], ensure_ascii=False)

        logger.debug("Arguments details for creating and running import request: %s", argsDetails)
        return self.syncRequest('POST', args=argsDetails)

    def get_details_of_import_request_by_job_ID(self, importRequestJobId):
        """
        Returns a specific import request based on the ID of the request job. The response contains a JSON representation of import request that describes an attempt to import table data from a temporary location.

        path Parameters
        ----------------
        importRequestJobId : required, string
            Contains the unique ID associated with the import request.
        """
        logger.info("Get details of import request by job ID")
        marketing_data = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_data")
        marketing_dataObj = deep_clone(marketing_data)
        argsDetails = marketing_dataObj['get_details_of_import_request_by_job_ID']
        argsDetails["path"] = replace_path_params(argsDetails["path"], {"importRequestJobId": importRequestJobId})  

        logger.debug("Arguments details for getting details of import request by job ID: %s", argsDetails)
        return self.syncRequest('GET', args=argsDetails)

    def get_summary_of_all_tables(self, start=0, limit=10, name=None, type=None):
        """
        Returns a summary of tables in the system. Each entry that is returned describes an existing table with a JSON representation.

        query Parameters
        ------------------
        start : integer
            Contains the first item to return.
        limit : integer
            Specifies the maximum number of items to return.
        name : string
            Specifies the name of the table to return. The parameter's value must match a table's name exactly.
        type : string        
            Enum: "customer" "deleteList" "identity" "importedList" "transient" "transientDeviceIdList"
            Specifies the type of tables to return.
            Note: You can also use dataType as an alias for this parameter.
        """
        logger.info("Get summary of all tables")
        marketing_data = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_data")
        marketing_dataObj = deep_clone(marketing_data)
        argsDetails = marketing_dataObj['get_summary_of_all_tables']
        if "params" not in argsDetails or not isinstance(argsDetails["params"], dict):
            argsDetails["params"] = {}

        argsDetails["params"]["start"] = start
        argsDetails["params"]["limit"] = limit
        if name:
            argsDetails["params"]["name"] = name
        if type:
            argsDetails["params"]["type"] = type

        logger.debug("Arguments details for getting summary of all tables: %s", argsDetails)
        return self.syncRequest('GET', args=argsDetails)

    def create_customer_table(self,  name, description, type, makeAvailableForTargeting, dataItems, customProperties=None):
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
        marketing_data = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_data")
        marketing_dataObj = deep_clone(marketing_data)
        argsDetails = marketing_dataObj['create_customer_table']

        if "data" not in argsDetails or not isinstance(argsDetails["data"], dict):
            argsDetails["data"] = {}

        argsDetails["data"]["name"] = name
        argsDetails["data"]["description"] = description
        argsDetails["data"]["type"] = type
        argsDetails["data"]["makeAvailableForTargeting"] = makeAvailableForTargeting
        argsDetails["data"]["dataItems"] = dataItems
        argsDetails["data"]["customProperties"] = customProperties

        argsDetails["data"]=json.dumps(argsDetails['data'], ensure_ascii=False)


        logger.debug("Arguments details for creating customer table: %s", argsDetails)
        return self.syncRequest('POST', args=argsDetails)

    def get_table_object_by_ID(self, tableId):
        """
        Returns an object that contains metadata that describes an existing table.

        path Parameters
        ------------------
        tableId : required, string
            Contains the unique ID that is associated with the table object.        
        """
        logger.info("Return a table object by ID")
        marketing_data = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_data")
        marketing_dataObj = deep_clone(marketing_data)
        argsDetails = marketing_dataObj['get_table_object_by_ID']
        argsDetails["path"]= replace_path_params(argsDetails["path"], {"tableId": tableId})

        logger.debug("Arguments details for getting table object by ID: %s", argsDetails)
        return self.syncRequest('GET', args=argsDetails)

    def update_table_by_ID_PATCH(self, tableId="", name="", description="", type="", makeAvailableForTargeting=False, dataItems=None, customProperties=None):
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
        marketing_data = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_data")
        marketing_dataObj = deep_clone(marketing_data)
        argsDetails = marketing_dataObj['update_table_by_ID_PATCH']
        argsDetails["path"] = replace_path_params(argsDetails["path"], {"tableId": tableId})

        if "data" not in argsDetails or not isinstance(argsDetails["data"], dict):
            argsDetails["data"] = {}

        argsDetails["data"]["name"] = name
        argsDetails["data"]["description"] = description
        argsDetails["data"]["type"] = type
        argsDetails["data"]["makeAvailableForTargeting"] = makeAvailableForTargeting
        argsDetails["data"]["dataItems"] = dataItems
        argsDetails["data"]["customProperties"] = customProperties

        logger.debug("Arguments details for updating table by ID: %s", argsDetails)
        return self.syncRequest('PATCH', args=argsDetails)

    def update_table_by_ID_POST(self, tableId="", name="", description="", type="", makeAvailableForTargeting="", dataItems=[], customProperties=[],_method="POST"):
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
        logger.info("Update a table by ID by POST ")
        marketing_data = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_data")
        marketing_dataObj = deep_clone(marketing_data)
        argsDetails = marketing_dataObj['update_table_by_ID_POST']
        argsDetails["path"] = replace_path_params(argsDetails["path"], {"tableId": tableId})

        if "params" not in argsDetails or not isinstance(argsDetails["params"], dict):
            argsDetails["params"] = {}

        argsDetails["params"]["_method"] = _method

        if "data" not in argsDetails or not isinstance(argsDetails["data"], dict):
            argsDetails["data"] = {}

        argsDetails["data"]["name"] = name
        argsDetails["data"]["description"] = description
        argsDetails["data"]["type"] = type
        argsDetails["data"]["makeAvailableForTargeting"] = makeAvailableForTargeting
        argsDetails["data"]["dataItems"] = dataItems
        argsDetails["data"]["customProperties"] = customProperties

        logger.debug("Arguments details for updating table by ID: %s", argsDetails)
        return self.syncRequest('POST', args=argsDetails)

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
        marketing_data = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_data")
        argsDetails = marketing_data['delete_table_by_ID']
        argsDetails["path"] = replace_path_params(argsDetails["path"], {"tableId": tableId})
        
        logger.debug("Arguments details for deleting table by ID: %s", argsDetails)
        return self.syncRequest('DELETE', args=argsDetails)

    def create_table_job(self,tableId, jobType, dataDescriptorId=None, fileLocation=None, headerRowIncluded=None, includeSourceAndTimestamp=None):
        """
        Creates and runs a table job.

        Request Body schema: 
        -----------------------
        Contains the object with the information that is necessary to generate and run a job for a customer table.
        jobType : required, string
            Enum: "CONTACT_PREFERENCE_EXPORT" "CONTACT_PREFERENCE_IMPORT" "TABLE_DOWNLOAD"
            The type of table job that you are submitting. When the value is an export, the response includes URLs to download the table's data.
        dataDescriptorId : required, string
            The ID of the table that corresponds to the job request. For example, if you are submitting a contact preference export job, this property is the ID of the contact preference table.            The ID of the table that corresponds to the job request.
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
        marketing_data = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_data")
        argsDetails = marketing_data['create_table_job']
        argsDetails["data"] = {
            "jobType": jobType,
            "dataDescriptorId": dataDescriptorId,
            "fileLocation": fileLocation,
            "headerRowIncluded": headerRowIncluded,
            "includeSourceAndTimestamp": includeSourceAndTimestamp
        }
        argsDetails["data"] = json.dumps(argsDetails['data'], ensure_ascii=False)
        logger.debug("Arguments details for creating a table job: %s", argsDetails)
        return self.syncRequest('POST', args=argsDetails)

    def get_specific_table_job_details_by_ID(self,  tableJobId):
        """
        Returns an existing table job based on the ID in the path.

        path Parameters
        -----------------
        tableJobId: required, string
            Contains the unique ID that is associated with the table job.
        """
        logger.info("Return a specific table job by ID", tableJobId)
        marketing_data = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_data")
        argsDetails = marketing_data['get_specific_table_job_details_by_ID']
        argsDetails["path"] = replace_path_params(argsDetails["path"], {"tableJobId": tableJobId})

        logger.debug("Arguments details for getting a specific table job by ID: %s", argsDetails)
        return self.syncRequest('GET', args=argsDetails)
