# Extend BaseAPI, add endpoint-specific methods.
from ..base.base_service import SASCI360VeloxPyBaseService
import os
import json
import configparser
import time
import requests
from ...config.config_loader import get_config, deep_clone, replace_path_params
from ...io.log import get_logger

logger = get_logger("marketing_audience_api")

class marketing_audiences(SASCI360VeloxPyBaseService):
    def __init__(self, config={}):
        super().__init__()
        self.config = config
    #TODO: Change names for sync and async and handle it on runtime
    async def async_get_audiences(self):
        logger.info("Calling get audiences")
        args ={
            "client_id" : get_config("GLOBAL_CONFIG", "ini_file_path", "tenant.client_id", ""),
            "extapigateway_url" : get_config("GLOBAL_CONFIG", "ini_file_path", "tenant.extapigateway_url", ""),
            "client_secret" : get_config("GLOBAL_CONFIG", "ini_file_path", "tenant.client_secret", "")
        }
        logger.debug("Calling get audiences with arguments deatils",args)
        return await self.asyncRequest('GET', **args)
    
    #TODO: Change names for sync and async and handle it on runtime
    def sync_get_audiences(self):
        logger.info("Getting all the available audiences in CI360")
        aud = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.aud")
        args=deep_clone(aud) #Always create deep copy to avoid updation
        argsDetails = args['get_all_audiences']
        logger.debug("Calling get audiences with arguments deatils",args)
        return self.syncRequest('GET', args=argsDetails)
    
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
        if "path" in argsDetails and isinstance(argsDetails["path"], str):
            argsDetails["path"] = replace_path_params(argsDetails["path"], {"audienceId": audienceId})
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
        if "path" in argsDetails and isinstance(argsDetails["path"], str):
            argsDetails["path"] = replace_path_params(argsDetails["path"], {"audienceId": audienceId})
        logger.info("Calling delete audience by ID with arguments details", argsDetails)
        logger.debug("Calling delete audience by ID with arguments details", argsDetails)
        return self.syncRequest('DELETE', args=argsDetails)

    def patch_audience(self, audienceId):

        # Inprogress : Check PATCH method usage
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
        if "path" in argsDetails and isinstance(argsDetails["path"], str):
            argsDetails["path"] = replace_path_params(argsDetails["path"], {"audienceId": audienceId})
        logger.debug("Calling patch audience by ID with arguments details", argsDetails)
        return self.syncRequest('PATCH', args=argsDetails)

    def create_audience_definition(self,file_path):
        logger.info("Creating an audience definition in CI360")
          # START: Read JSON data from file and convert into string.
        try:
            json_path = file_path
            with open(json_path, 'r', encoding='utf-8') as file:
                json_data = json.load(file)
                logger.info("JSON data successfully read from file.")
        except FileNotFoundError as e:
            logger.error(f"JSON file not found: {e}")
            raise FileNotFoundError(f"JSON file not found: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON file: {e}")
            raise json.JSONDecodeError(f"Error decoding JSON file: {e}")

        # Convert JSON object to string without special characters.
        json_string = json.dumps(json_data, ensure_ascii=False)
        json_data = json.loads(json_string)
        logger.info("Obtained Audience defination JSON data.")
        logger.debug("Audience defination JSON string:", json_string)
        aud = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.aud")
        args=deep_clone(aud) #Always create deep copy to avoid updation
        argsDetails = args['create_audience']
        
        argsDetails["data"]=json_string
        logger.info("Calling create audience definition with arguments details", argsDetails)
        logger.debug("Calling create audience definition with arguments details", argsDetails)  
        audience_def=self.syncRequest('POST', args=argsDetails)
        print("Audience definition response:", audience_def)
        audience_id = audience_def.get('audienceId', None)
        logger.info("Audience definition created with ID: %s", audience_id)
        return audience_id 
      
    def get_signed_url(self):
        """Obtain a signed URL for file upload from CI360."""
        logger.info("obtaining signed URL for file upload...")

        aud = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.aud")
        args=deep_clone(aud) #Always create deep copy to avoid updation
        argsDetails = args['audience_signedUrl']
        payload = ""
        argsDetails["data"]=payload

        try:
            response = self.syncRequest('POST', args=argsDetails)
            logger.info("Signed URL obtained for file upload.")
            return response['signedURL']
        except requests.RequestException as e:
            logger.info(f"Error obtaining signed URL: {e}")
            raise Exception(f"Error obtaining signed URL: {e}")
        
    def upload_file_to_signed_location(self, signed_url, file_name):
        """Upload a CSV file to the signed location provided by CI360."""
        try:
            csv_path = os.path.join(os.path.dirname(__file__), file_name)
            with open(csv_path, 'rb') as file:
                argsDetails ={
                    "method":"PUT",
                    "url": signed_url,
                    "data": file,  # File data will be set later
                    "headers":{
                        "Content-Type" : "text/csv"
                    },
                }
                response = self.syncRequest('PUT', args=argsDetails)
                
            logger.info("File uploaded to signed location successfully.")
        except FileNotFoundError as e:
            print(f"File not found: {e}")
            raise FileNotFoundError(f"File not found: {e}")
        except requests.RequestException as e:
            print(f"Error uploading file: {e}")
            raise Exception(f"Error uploading file: {e}")
        
    def start_data_upload_job(self, signed_url, audience_id, audience_name):
        """Start a data upload job for the specified audience in CI360."""
        aud = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.aud")
        args=deep_clone(aud) #Always create deep copy to avoid updation
        argsDetails = args['upload_audience']
        payload = json.dumps({
            "name": audience_name,
            "audienceId": audience_id,
            "fileLocation": signed_url,
            "headerRowIncluded": False
        })
       
        argsDetails["path"] = argsDetails["path"].replace("{audience_id}", audience_id)
        argsDetails["data"]=payload
        
        response = self.syncRequest('PUT', args=argsDetails)
      
        logger.info("Data upload job started successfully.")
        return response
    
    def upload_audiences(self,file_path):
        """High-level utility to upload audience data using config file settings."""
        config = configparser.ConfigParser()
        config_path = file_path
        config.read(config_path)
        try:
            audience_file_name = config['Audience_Configuration']['audience_file_name']
            audience_name = config['Audience_Configuration']['audience_name']
            audience_id = config['Audience_Configuration']['audience_id']
            print('Successfully read Audience_Configuration section from config file.')
        except KeyError as e:
            print(f"Missing configuration key in Audience_Configuration section: {e}")
            raise KeyError(f"Missing configuration key: {e}")

        signed_url = self.get_signed_url()
        time.sleep(2)

        self.upload_file_to_signed_location(signed_url, audience_file_name)
        time.sleep(5)

        start_data_job_response = self.start_data_upload_job(signed_url, audience_id, audience_name)
        time.sleep(10)
        logger.info("Audience Data upload completed.")

    def update_audience_by_id(self,file_path,audienceId):
        # Inprogress : Check update audience JSON Format 
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
        
        try:
            json_path = file_path
            with open(json_path, 'r', encoding='utf-8') as file:
                json_data = json.load(file)
                logger.info("JSON data successfully read from file.")
        except FileNotFoundError as e:
            logger.error(f"JSON file not found: {e}")
            raise FileNotFoundError(f"JSON file not found: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON file: {e}")
            raise json.JSONDecodeError(f"Error decoding JSON file: {e}")
        
        logger.debug("Updating audience by ID", {"audienceId": audienceId})
        aud = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_audiences")
        args = deep_clone(aud)
        argsDetails = args['update_audience_by_id']
        json_string = json.dumps(json_data, ensure_ascii=False)
        json_data = json.loads(json_string)
        logger.info("Obtained Audience defination JSON data For Updatation.")
        logger.debug("Update Audience defination JSON string:", json_string)
        argsDetails = args['update_audience_by_id']

        argsDetails["data"]=json_string
        if "path" in argsDetails and isinstance(argsDetails["path"], str):
            argsDetails["path"] = replace_path_params(argsDetails["path"], {"audienceId": audienceId})
        logger.debug("Calling update audience by ID with arguments details", argsDetails)
        return self.syncRequest('PUT', args=argsDetails)

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
        if "path" in argsDetails and isinstance(argsDetails["path"], str):
            argsDetails["path"] = replace_path_params(argsDetails["path"], {"audienceId": audienceId})

        logger.debug("Calling get audience upload history with arguments details", argsDetails)
        audience_hist = self.syncRequest('GET', args=argsDetails)
        logger.info("Audience upload history retrieved.")
        logger.info("Audience upload history details: %s", audience_hist)
       
        audience_items = audience_hist.get('items', [])
        history_ids = [item.get('historyId') for item in audience_items]

        logger.info("Audience upload history retrieved with HistoryID: %s", history_ids)
        return history_ids

    def get_file_history_by_upload_id(self, audienceId, historyId):
        """
        Retrieves the upload history of a data file based on the ID of the data file and the ID of the audience.

        Path Parameters
        ----------------
        audienceId: audienceId
        historyId: historyId

        """
        logger.debug("Getting file history by upload ID", {"audienceId": audienceId, "uploadId": historyId})
        aud = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_audiences")
        args = deep_clone(aud)
        argsDetails = args['get_file_history_by_upload_id']
        if "path" in argsDetails and isinstance(argsDetails["path"], str):
            argsDetails["path"] = replace_path_params(argsDetails["path"], {"audienceId": audienceId, "historyId": historyId})
       
        logger.debug("Calling get file history by upload ID with arguments details", argsDetails)
        return self.syncRequest('GET', args=argsDetails)