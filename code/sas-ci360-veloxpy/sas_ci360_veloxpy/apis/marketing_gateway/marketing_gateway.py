# Extend BaseAPI, add endpoint-specific methods.
from itertools import count
from ..base.base_service import SASCI360VeloxPyBaseService
import requests
import json
from aiohttp import FormData
from ...config.config_loader import get_config, deep_clone, replace_path_params
from ...io.log import get_logger

logger = get_logger("MarketingGatewayApi")
# MarketingGatewayApi class to handle marketing gateway specific API calls  

class MarketingGatewayApi(SASCI360VeloxPyBaseService):
    def __init__(self, config={}):
        """
        Initialize the MarketingGatewayApi with optional configuration.
        :param config: Dictionary containing configuration parameters.
        """
        super().__init__()
        self.config = config
            
    def send_external_events(self, event_data):
        """
        Send a single external event.
        :param event_data: Dictionary containing the event data to be sent.
        :return: Response from the API call.
        """
        logger.info("Calling send external single event: %s", event_data)

        marketing_gateway = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_gateway")
        marketing_gatewayObj = deep_clone(marketing_gateway)
        # Always create deep copy to avoid updation
        argsDetails = marketing_gatewayObj['send_single_external_event']
        argsDetails["data"]=json.dumps(event_data, ensure_ascii=False)
        
        logger.debug("Arguments details for sending external single event: %s", argsDetails)
        # Make the API request
        logger.debug("Making API request to send external single event")
        return self.syncRequest('POST', args=argsDetails) 

    def get_signed_url_for_batch_upload_external_event(self):
        """
        Get the S3 URL for batch upload of external events.
        :return: Response containing the S3 URL for uploading the file.
        """
        logger.info("Calling batch upload external event")
        
        marketing_gateway = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_gateway")
        marketing_gatewayObj = deep_clone(marketing_gateway)    
        # Always create deep copy to avoid updation
        argsDetailsObj = marketing_gatewayObj['get_signed_url_for_batch_upload_external_event']
        logger.debug("Arguments details for batch upload external event: %s", argsDetailsObj)
        # Prepare the arguments for the API request
        # The argsDetails should contain the necessary parameters for the API request
        # Ensure the data is in JSON format
        logger.debug("Converting data to JSON format for batch upload external event")
        argsDetailsObj["data"]=json.dumps(argsDetailsObj['data'], ensure_ascii=False)

        # Make the API request
        logger.debug("Making API request to get S3 URL for batch upload external event",argsDetailsObj)
        return self.syncRequest('POST', args=argsDetailsObj)
             
    def upload_file_for_external_events(self,file_path):
        """
        Upload a bulk file to the S3 bucket for external events.    
        :param file_path: Path to the file to be uploaded.
        :return: Response from the API call.
        """
        logger.info("Calling upload bulk file: %s", file_path)
        if not file_path:
            raise ValueError("File path is required for bulk upload")
        if not isinstance(file_path, str):
            raise TypeError("File path must be a string")
        if not file_path.endswith('.csv'):
            raise ValueError("File must be a CSV file")
        logger.debug("File path for bulk upload: %s", file_path)
        # Get the S3 URL for batch upload
        logger.debug("Getting S3 URL for batch upload external event")
        # Use the get_signed_url_for_batch_upload_external_event method to get the S3 URL
        # This method will return a dictionary containing the S3 URL and other details
        # Ensure the file is opened in binary mode for upload
        # Use aiohttp's FormData to handle the file upload
        logger.debug("Calling get_signed_url_for_batch_upload_external_event to get S3 URL")
        
        s3UrlData = self.get_signed_url_for_batch_upload_external_event()
        logger.debug("Received S3 URL data: %s", s3UrlData)
        if not s3UrlData or 'links' not in s3UrlData or not s3UrlData['links']:
            logger.error("No S3 URL found in the response")
            raise ValueError("No S3 URL found in the response")
        # Extract the S3 URL from the response
        logger.debug("Extracting S3 URL from the response")
        # Assuming the S3 URL is in the 'links' key of the response
        if not isinstance(s3UrlData['links'], list) or not s3UrlData['links']:
            logger.error("Invalid S3 URL format in the response")
            raise ValueError("Invalid S3 URL format in the response")
        
        # Extract the first link from the list
        logger.debug("Extracting first link from the S3 URL data")
        # Assuming the first link is the one we need for uploading the file
        # This may vary based on the actual structure of the response
        logger.debug("S3 URL data links: %s", s3UrlData['links'])
        if not isinstance(s3UrlData['links'][0], dict) or 'href' not in s3UrlData['links'][0]:
            logger.error("Invalid S3 URL format in the response links")
            raise ValueError("Invalid S3 URL format in the response links")
        logger.debug("S3 URL data href: %s", s3UrlData['links'][0]['href'])
        # Extract the href from the first link
        # This is the URL where we will upload the file
        # The href should be a valid URL for uploading the file
        logger.debug("Extracting href from the S3 URL data")
        # Assuming the href is the URL where we will upload the file
        # Prepare the arguments for the PUT request
        logger.debug("Preparing arguments for the PUT request to upload the file")
        
        hrefData=s3UrlData['links'][0]
        argsDetails ={
            "method":"PUT",
            "url": hrefData['href'],
            "headers":{
                "Content-Type" : "application/octet-stream"
            },
        }
        logger.debug("Arguments details for uploading the file: %s", argsDetails)
        # Add the file to the arguments for the PUT request
        # Use aiohttp's FormData to handle the file upload
        logger.debug("Adding file to the arguments for the PUT request")
        # The file will be added as a field in the FormData object  
        # The file will be uploaded as a binary stream
        # The file will be uploaded as a CSV file
        # The file will be uploaded to the S3 URL obtained from the get_signed_url_for_batch_upload_external_event method
        logger.debug("File path for uploading: %s", file_path)
        # Open the file in binary mode for upload
        # Use the FormData object to handle the file upload

        logger.debug("Opening file in binary mode for upload: %s", file_path)
        # Use the FormData object to handle the file upload
        # The FormData object will handle the file upload as a binary stream
        logger.debug("Creating FormData object for file upload")
        # Create a FormData object to handle the file upload
        argsDetails = deep_clone(argsDetails)
        # Always create deep copy to avoid updation
        # argsDetails['headers']['Content-Type'] = 'text/csv'  # Set the content type to CSV
        logger.debug("Setting Content-Type to text/csv in the headers")
        # Create a FormData object to handle the file upload
        # The FormData object will handle the file upload as a binary stream
        logger.debug("Creating FormData object for file upload")
        # The FormData object will handle the file upload as a binary stream
        logger.debug("FormData object created for file upload")
        # Add the file to the FormData object
        logger.debug("Adding file to the FormData object")
        # The file will be added as a field in the FormData object  

        # The file will be uploaded as a binary stream
        # The file will be uploaded as a CSV file   
        logger.debug("File path for uploading: %s", file_path)
        # The file will be uploaded to the S3 URL obtained from the get_signed_url_for_batch_upload_external_event
        
        try: 
            fileOpened = open(file_path, "rb")  # keep it open
            form = FormData()
            form.add_field(
                name='file',
                value=fileOpened,
                filename=file_path,
                content_type='text/csv'
            )
            logger.debug("File added to FormData object for upload")
            # Add the FormData object to the arguments for the PUT request
            argsDetails['data'] = form
            logger.debug("FormData object added to the arguments for the PUT request")
            # Make the API request to upload the file
            logger.debug("Making API request to upload the file to S3 URL")
            # Use the syncRequest method to make the API request
            # The syncRequest method will handle the PUT request to the S3 URL
            logger.debug("Calling syncRequest method to upload the file")
            # The syncRequest method will handle the PUT request to the S3 URL
            # The syncRequest method will return the response from the API call

            try:
                return self.syncRequest('PUT', args=argsDetails)
            except requests.exceptions.RequestException as e:
                logger.error("Error uploading file to S3 URL: %s", e)
                raise Exception(f"Error uploading file to S3 URL: {e}")
            
            finally:
                fileOpened.close()  # manually close after request is done
        except FileNotFoundError as e:
            logger.error("File not found: %s", e)
            raise FileNotFoundError(f"File not found: {e}")
        except IOError as e:
            logger.error("Error reading file: %s", e)
            raise IOError(f"Error reading file: {e}")
        
        except requests.exceptions.ConnectionError as e:
            logger.error("Connection error while uploading the file: %s", e)
            raise ConnectionError(f"Connection error while uploading the file: {e}")
        except requests.exceptions.Timeout as e:
            logger.error("Timeout error while uploading the file: %s", e)
            raise TimeoutError(f"Timeout error while uploading the file: {e}")
        except requests.exceptions.HTTPError as e:
            logger.error("HTTP error while uploading the file: %s", e)
            raise HTTPError(f"HTTP error while uploading the file: {e}")
        except Exception as e:
            logger.error("An unexpected error occurred while uploading the file: %s", e)
            raise Exception(f"An unexpected error occurred while uploading the file: {e}")

    def download_Discover_Base_Tables_and_Analytical_Base_Tables(self, schema_version, data_url_file_path):

        logger.info("Calling download dbt and abt reports")
        # Implement the logic to download the reports
        marketing_gateway = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_gateway")
        marketing_gatewayObj = deep_clone(marketing_gateway)
        # Always create deep copy to avoid updation
        argsDetails = marketing_gatewayObj['download_Discover_Base_Tables_and_Analytical_Base_Tables']
        if "params" not in argsDetails or not isinstance(argsDetails["params"], dict):
            argsDetails["params"] = {}
            argsDetails["params"]["schemaVersion"] = schema_version
        logger.debug("Arguments details for downloading dbt and abt reports: %s", argsDetails)
        print("argsDetails ", argsDetails)
        dbtreport_data=self.syncRequest('GET', args=argsDetails)
        logger.info("Downloaded dbt and abt reports data:")
        
        output_lines = []
        if dbtreport_data.get('count', 0) == 0:
            logger.warning("No data found for dbt and abt reports")
            return "No data found for dbt and abt reports"
        else:
            logger.info("Data found for dbt and abt reports")
            items = dbtreport_data.get('items', [])
            for item in items:
                start_time_stamp = item.get('dataRangeStartTimeStamp')
                end_time_stamp = item.get('dataRangeEndTimeStamp')
                schema_url = item.get('schemaUrl')
                output_lines.append(f"startTimeStamp: {start_time_stamp}")
                output_lines.append(f"endTimeStamp: {end_time_stamp}")
                output_lines.append(f"schemaURL: {schema_url}")
                entities = item.get('entities', [])
                for entity in entities:
                    entity_name = entity.get('entityName')
                    data_url_details = entity.get('dataUrlDetails', [])
                    for url_detail in data_url_details:
                        entity_url = url_detail.get('url')
                        output_lines.append(f"entityName: {entity_name}, url: {entity_url}")
            # Write output to text file in Resources folder
            output_file_path = f"{data_url_file_path}\\dbt_abt_report_output.txt"
            with open(output_file_path, "w", encoding="utf-8") as f:
                f.write("\n".join(output_lines))
            return f"Output written to {output_file_path}"

    def download_Detail_tables(self, schema_version, data_url_file_path):

        logger.info("Calling download detail reports")
        # Implement the logic to download the reports
        marketing_gateway = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_gateway")
        marketing_gatewayObj = deep_clone(marketing_gateway)
        # Always create deep copy to avoid updation
        argsDetails = marketing_gatewayObj['download_Detail_tables']
        if "params" not in argsDetails or not isinstance(argsDetails["params"], dict):
            argsDetails["params"] = {}
            argsDetails["params"]["schemaVersion"] = schema_version
        logger.debug("Arguments details for downloading detail reports: %s", argsDetails)

        detail_data=self.syncRequest('GET', args=argsDetails)
        logger.info("Downloaded detail reports data:")
        output_lines = []
        if detail_data.get('count', 0) == 0:
            logger.warning("No data found for detail  reports")
            return "No data found for detail reports"
        else:
            items = detail_data.get('items', [])

            for item in items:
                start_time_stamp = item.get('dataRangeStartTimeStamp')
                end_time_stamp = item.get('dataRangeEndTimeStamp')
                schema_url = item.get('schemaUrl')
                output_lines.append(f"startTimeStamp: {start_time_stamp}")
                output_lines.append(f"endTimeStamp: {end_time_stamp}")
                output_lines.append(f"schemaURL: {schema_url}")
                entities = item.get('entities', [])
                for entity in entities:
                    entity_name = entity.get('entityName')
                    data_url_details = entity.get('dataUrlDetails', [])
                    for url_detail in data_url_details:
                        entity_url = url_detail.get('url')
                        output_lines.append(f"entityName: {entity_name}, url: {entity_url}")

            # Write output to text file in Resources folder
            output_file_path = f"{data_url_file_path}\\detail_report.txt"
            with open(output_file_path, "w", encoding="utf-8") as f:
                f.write("\n".join(output_lines))

            return f"Output written to {output_file_path}"

    def download_identity_metadata_plan_tables(self, schema_version, data_url_file_path):

        logger.info("Calling download identity metadata plan tables reports")
        # Implement the logic to download the reports
        marketing_gateway = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_gateway")
        marketing_gatewayObj = deep_clone(marketing_gateway)
        # Always create deep copy to avoid updation
        argsDetails = marketing_gatewayObj['download_identity_metadata_plan_tables']
        if "params" not in argsDetails or not isinstance(argsDetails["params"], dict):
            argsDetails["params"] = {}
            argsDetails["params"]["schemaVersion"] = schema_version
        logger.debug("Arguments details for downloading identity metadata plan tables reports: %s", argsDetails)

        identity_data=self.syncRequest('GET', args=argsDetails)
        logger.info("Downloaded identity metadata plan tables reports data:")
        print('Identity Data: ', identity_data)
        if identity_data.get('count', 0) == 0:
            logger.warning("No data found for identity metadata plan tables reports")
            return "No data found for identity metadata plan tables reports"
        
        else:
            output_lines = []
            items = identity_data.get('items', [])
            for item in items:
            
                schema_url = item.get('schemaUrl')
                output_lines.append(f"schemaURL: {schema_url}")
                entities = item.get('entities', [])
                for entity in entities:
                    entity_name = entity.get('entityName')
                    data_url_details = entity.get('dataUrlDetails', [])
                    for url_detail in data_url_details:
                        entity_url = url_detail.get('url')
                        output_lines.append(f"entityName: {entity_name}, url: {entity_url}")

            # Write output to text file in Resources folder
            output_file_path = f"{data_url_file_path}\\identity_metadata_plan_report.txt"
            with open(output_file_path, "w", encoding="utf-8") as f:
                f.write("\n".join(output_lines))

            return f"Output written to {output_file_path}"

    def download_reprocessed_data(self, schema_version, mart_type, data_url_file_path):
        logger.info("Calling download reprocessed data reports")
        # Implement the logic to download the reports
        marketing_gateway = get_config("GLOBAL_API_CONFIG", "json_file_path", "APIS.marketing_gateway")
        marketing_gatewayObj = deep_clone(marketing_gateway)
        # Always create deep copy to avoid updation
        argsDetails = marketing_gatewayObj['download_reprocessed_data']
        if "params" not in argsDetails or not isinstance(argsDetails["params"], dict):
            argsDetails["params"] = {}
            argsDetails["params"]["martType"] = mart_type
            argsDetails["params"]["schemaVersion"] = schema_version

        logger.debug("Arguments details for downloading  reprocessed data reports: %s", argsDetails)
        print("argsDetails ", argsDetails)
        reprocess_data=self.syncRequest('GET', args=argsDetails)
        logger.info("Downloaded reprocessed data reports data:")
        print('Reprocess Data: ', reprocess_data)
        if reprocess_data.get('count', 0) == 0:
            logger.warning("No data found for reprocessed data reports")
            return "No data found for reprocessed data reports"
        else:
            output_lines = []
            items = reprocess_data.get('items', [])
            for item in items:
                start_time_stamp = item.get('dataRangeStartTimeStamp')
                end_time_stamp = item.get('dataRangeEndTimeStamp')
                schema_url = item.get('schemaUrl')
                output_lines.append(f"startTimeStamp: {start_time_stamp}")
                output_lines.append(f"endTimeStamp: {end_time_stamp}")
                output_lines.append(f"schemaURL: {schema_url}")
                entities = item.get('entities', [])
                for entity in entities:
                    entity_name = entity.get('entityName')
                    data_url_details = entity.get('dataUrlDetails', [])
                    for url_detail in data_url_details:
                        entity_url = url_detail.get('url')
                        output_lines.append(f"entityName: {entity_name}, url: {entity_url}")

            # Write output to text file in Resources folder
            output_file_path = f"{data_url_file_path}\\reprocessed_data_report.txt"
            with open(output_file_path, "w", encoding="utf-8") as f:
                f.write("\n".join(output_lines))

            return f"Output written to {output_file_path}"

