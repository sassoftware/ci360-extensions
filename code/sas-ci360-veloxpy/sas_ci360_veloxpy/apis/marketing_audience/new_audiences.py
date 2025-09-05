# Extend BaseAPI, add endpoint-specific methods.
from ..base.base_service import SASCI360VeloxPyBaseService
from ...config.config_loader import get_config, deep_clone, replace_path_params
from ...io.log import get_logger

logger = get_logger("MarketingAudiencesApi")
# MarketingGatewayApi class to handle marketing gateway specific API calls  

class MarketingAudiencesApi(SASCI360VeloxPyBaseService):
    def __init__(self, config={}):
        """
        Initialize the MarketingExecutionApi with optional configuration.
        :param config: Dictionary containing configuration parameters.
        """
        super().__init__()
        self.config = config

    def get_list_of_all_audiences(self, sortBy, start, limit, status, source, audienceType, name):
        """
        Return a list of all audiences, including the audiences created in the application and the audiences created using the API.

        query Parameters
        ------------------
        sortBy : string, 

            Default: "modifiedTimestamp:descending"
            Enum: "modifiedTimestamp:ascending" "modifiedTimestamp:descending"
            Specify the sort field and order for audience result collection.

        start :  integer <int32>
        
            Default: 0
            The index of the first audience to return. The default value is 0.

        limit :  integer <int32>
        
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

        name : string
            
            The audience name.

        """
        logger.info("Get list of all audiences") 

        logger.info("Getting all the available audiences in CI360")
        aud = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_audiences")
        args=deep_clone(aud) #Always create deep copy to avoid updation
        argsDetails = args['get_list_of_all_audiences']

        argsDetails["query"]["sortBy"] = sortBy
        argsDetails["query"]["start"] = start
        argsDetails["query"]["limit"] = limit
        argsDetails["query"]["status"] = status
        argsDetails["query"]["source"] = source
        argsDetails["query"]["audienceType"] = audienceType
        argsDetails["query"]["name"] = name

        logger.debug("Calling get audiences with arguments details", args)
        return self.syncRequest('GET', args=argsDetails)

    def create_audience(self, version, audienceId, name, description, source, iconName, expiration, identityType, identityColumnName, emailColumnName, dataItems):
        """
        Creates an audience definition and an audience. 
        This API supports uploading data using a CSV file. 
        You can also create the audience from the Audience Definitions page in the application and upload the data using the API. 
        You can see the list of audience definitions created by navigating to General Settings > Audience Definitions. You can see the audience by navigating to Audiences.

        Request Body schema: 
        ----------------------
        version	: integer
        
            Contains the schema version number for the media type. This representation is version 1.

        audienceId: string
            
            The audience identifier

        name : string
            
            Contains the name of the object.

        description	: string

            Contains the audience description

        source :  string
        
            Value: "customer_defined"
            The source of the audience

        iconName: string
            
            The icon to use for the audience

        expiration: number

            The number of days before the audience expires

        identityType : string

            The identity type to use for the audience

        identityColumnName : string
            
            The column name to use for the identity information

        emailColumnName	: string

            The column name to use for the email identity information

        dataItems : Array of objects (AudienceColumnBody)
            
            A list of columns to be used in the audience


        """

        logger.debug("Creating audience with details", {
            "version": version,
            "audienceId": audienceId,
            "name": name,
            "description": description,
            "source": source,
            "iconName": iconName,
            "expiration": expiration,
            "identityType": identityType,
            "identityColumnName": identityColumnName,
            "emailColumnName": emailColumnName,
            "dataItems": dataItems
        })

        aud = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_audiences")
        args=deep_clone(aud) #Always create deep copy to avoid updation
        argsDetails = args['create_audience']

        argsDetails["body"]["version"] = version
        argsDetails["body"]["audienceId"] = audienceId
        argsDetails["body"]["name"] = name
        argsDetails["body"]["description"] = description
        argsDetails["body"]["source"] = source
        argsDetails["body"]["iconName"] = iconName
        argsDetails["body"]["expiration"] = expiration
        argsDetails["body"]["identityType"] = identityType
        argsDetails["body"]["identityColumnName"] = identityColumnName
        argsDetails["body"]["emailColumnName"] = emailColumnName
        argsDetails["body"]["dataItems"] = dataItems


        logger.debug("Calling create audience with arguments details", argsDetails)
        return self.syncRequest('POST', args=argsDetails)

    def get_audience_by_id(self, audienceId):
        """
        Returns an audience based on the ID that is specified in the path.

        Path Parameters
        ----------------
        audienceId: audienceId

        """
        logger.debug("Getting audience by ID", {"audienceId": audienceId})
        aud = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_audiences")
        args = deep_clone(aud)  # Always create deep copy to avoid updation
        argsDetails = args['get_audience_by_id']
        argsDetails["path"]["audienceId"] = audienceId
        logger.debug("Calling get audience by ID with arguments details", argsDetails)
        return self.syncRequest('GET', args=argsDetails)

    def delete_audience_by_id(self, audienceId):
        """
        Deletes an audience based on the ID that is specified in the path.

        Path Parameters
        ----------------
        audienceId: audienceId

        """
        logger.debug("Deleting audience by ID", {"audienceId": audienceId})
        aud = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_audiences")
        args = deep_clone(aud)
        argsDetails = args['delete_audience_by_id']
        argsDetails["path"]["audienceId"] = audienceId
        logger.debug("Calling delete audience by ID with arguments details", argsDetails)
        return self.syncRequest('DELETE', args=argsDetails)
    
    def patch_audience(self, audienceId):
        """
        Patch an audience based on the ID that is specified in the path.

        Path Parameters
        ----------------
        audienceId: audienceId
        
        """
        logger.debug("Patching audience by ID", {"audienceId": audienceId})
        aud = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_audiences")
        args = deep_clone(aud)
        argsDetails = args['patch_audience']
        argsDetails["path"]["audienceId"] = audienceId
        logger.debug("Calling patch audience by ID with arguments details", argsDetails)
        return self.syncRequest('PATCH', args=argsDetails)

    def update_audience_by_id(self):
        """
        Patch an audience based on the ID that is specified in the path.

        Request Body schema
        ----------------
        version	: integer

            Contains the schema version number for the media type. This representation is version 1.

        audienceId	: string

            The audience identifier

        name :  string

            Contains the name of the object.

        description	: string

            Contains the audience description

        source	: string

            Value: "customer_defined"
            The source of the audience

        iconName : string

            The icon to use for the audience

        expiration	: number
            
            The number of days before the audience expires

        identityType: string
            
            The identity type to use for the audience

        identityColumnName: string

            The column name to use for the identity information

        emailColumnName : string

            The column name to use for the email identity information

        dataItems :  Array of objects (AudienceColumnBody)
            
            A list of columns to be used in the audience
        
        """
        logger.debug("Updating audience by ID", {"audienceId": audienceId})
        aud = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_audiences")
        args = deep_clone(aud)
        argsDetails = args['update_audience_by_id']
        argsDetails["path"] = replace_path_params(argsDetails["path"], {"audienceId": audienceId})
        logger.debug("Calling update audience by ID with arguments details", argsDetails)
        return self.syncRequest('PUT', args=argsDetails)

    def upload_audience_data_file(self):
        """
        Upload a new file that is used to replace the audience data. Replaces audience data based on the audience ID that is specified in the path.

        Request Body schema:
        -----------------------
        name	: string
            
            The name of the import.

        audienceId : required, string
            
            The ID of the audience that corresponds to the data file that you are uploading.

        fileLocation : required, string
            
            The signed URL that you generated from the /fileTransferLocation endpoint.

        headerRowIncluded: required, boolean
            
            Specifies whether the file contains a header row. Set to true to skip processing on the first row in the data file.

        ignoreDotConversion	: boolean
            
            Specifies whether the conversion of SAS missing (dot) in the data should be ignored.

        """
        logger.debug("Uploading audience data file", {"audienceId": audienceId})
        aud = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_audiences")
        args = deep_clone(aud)
        argsDetails = args['upload_audience_data_file']
        argsDetails["path"]= replace_path_params(argsDetails["path"], {"audienceId": audienceId})

        logger.debug("Calling upload audience data file with arguments details", argsDetails)
        return self.syncRequest('POST', args=argsDetails)

    def get_audience_upload_history(self, audienceId):
        """
        Return the history of uploaded data files for an audience. Returns history based on the audience ID that is specified in the path.

        Path Parameters
        ----------------
        audienceId: audienceId

        """
        logger.debug("Getting audience upload history", {"audienceId": audienceId})
        aud = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_audiences")
        args = deep_clone(aud)
        argsDetails = args['get_audience_upload_history']
        argsDetails["path"] = replace_path_params(argsDetails["path"], {"audienceId": audienceId})

        logger.debug("Calling get audience upload history with arguments details", argsDetails)
        return self.syncRequest('GET', args=argsDetails)

    def get_file_history_by_upload_id(self, audienceId, uploadId):
        """
        Retrieves the upload history of a data file based on the ID of the data file and the ID of the audience.

        Path Parameters
        ----------------
        audienceId: audienceId
        uploadId: uploadId

        """
        logger.debug("Getting file history by upload ID", {"audienceId": audienceId, "uploadId": uploadId})
        aud = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_audiences")
        args = deep_clone(aud)
        argsDetails = args['get_file_history_by_upload_id']
        argsDetails["path"] = replace_path_params(argsDetails["path"], {"audienceId": audienceId, "uploadId": uploadId})

        logger.debug("Calling get file history by upload ID with arguments details", argsDetails)
        return self.syncRequest('GET', args=argsDetails)

    def create_signed_url_to_upload_files(self):
        """
        Creates and return an object containing a signed URL, to be used for secure file upload to a temporary location.

        
        """
        logger.debug("Creating signed URL to upload files")
        aud = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_audiences")
        args = deep_clone(aud)
        argsDetails = args['create_signed_url_to_upload_files']

        logger.debug("Calling create signed URL to upload files with arguments details", argsDetails)
        return self.syncRequest('POST', args=argsDetails)
