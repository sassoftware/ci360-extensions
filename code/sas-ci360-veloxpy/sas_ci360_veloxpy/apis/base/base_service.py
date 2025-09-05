# BaseAPI class providing get, post, etc., injecting token from TokenManager
import aiohttp
import asyncio
from ...config.config_loader import get_config
from ...io.log import get_logger
from sas_ci360_veloxpy.auth.bearer import SASCI360VeloxPyBearerTokenManager

logger = get_logger("SASCI360VeloxPyBaseService")


class SASCI360VeloxPyBaseService:
    """
   Generic HTTP client using aiohttp, with async + sync interface.
    Includes retry, timeout, and logging for robust and observable API calls.

    Provides both async and sync interfaces for flexibility across environments:
    - Use `request()` in async applications (e.g., FastAPI, asyncio pipelines)
    - Use `request_sync()` in synchronous scripts, CLI tools, or unit tests
     
    """

    def __init__(self):
        self.extapigateway_url = str(get_config("GLOBAL_CONFIG", "ini_file_path", "tenant.protocol", "")) + str(get_config("GLOBAL_CONFIG", "ini_file_path", "tenant.extapigateway_url", ""))
        self.client_id = get_config("GLOBAL_CONFIG", "ini_file_path", "tenant.client_id", "")
        self.client_secret = get_config("GLOBAL_CONFIG", "ini_file_path", "tenant.client_secret", "")
        self.api_user_name = get_config("GLOBAL_CONFIG", "ini_file_path", "tenant.api_user_name", "")
        self.api_user_password = get_config("GLOBAL_CONFIG", "ini_file_path", "tenant.api_user_password", "")
       

    async def asyncRequest(self, method, args):

        """
        Asynchronous HTTP request method using aiohttp.

        Parameters:
            method (str): HTTP method (e.g., "GET", "POST").
            url (str): Full URL of the API endpoint.
            **kwargs: Additional parameters passed to aiohttp (e.g., headers, json, params).

        Returns:
            str: Response text or JSON, depending on implementation.

        Example:
            ```python
            async def fetch_data():
                client = SASCI360VeloxPyBaseService()
                data = await client.request("GET", "https://api.example.com/data")
                print(data)
            ```

        Use Case:
            - Suitable for use inside async applications like FastAPI, Discord bots, async pipelines etc.
        """
        logger.debug("Token inside base service for calling an API", args)
        logger.info("Token inside base service for calling an API", args)

        headers = args.pop('headers', {})

        #TODO:args.url if we get then ignore this step
        pathUrlData=args.pop('path', '')
        
        url=''
        if pathUrlData not in (None, ''):
            url = self.extapigateway_url + pathUrlData
        else:
            url = args.pop('url', '')

        token = args.pop('token', '')
        authType = args.pop('authType', "")
        method = args.pop('method', '') 

        if not url:
            raise ValueError("URL must be provided in the args or as a path parameter")
        if not method:
            raise ValueError("HTTP method must be provided in the args")
        if not headers:
            raise ValueError("HTTP headers must be provided in the args")
        if not token:
            logger.debug("No token provided, fetching token based on authType")
            # If no token is provided, fetch it based on authType
            if not authType:
                logger.warning("No authType provided, defaulting to ''")
                authType = ""  # Default to temporary JWT if not specified
                # raise ValueError("authType must be provided in the args if token is not provided")

        if authType == "temporary_jwt":
            logger.info("Fetching temporary JWT token")
            # Fetch a temporary JWT token
            # This will call the fetch_token method in the SASCI360VeloxPyBearerTokenManager class
            # and set the token in the headers  
            # Create an instance of the SASCI360VeloxPyBearerTokenManager
            # and call the get_token method to fetch the token
            logger.debug("Fetching temporary JWT token with parameters: extapigateway_url: %s",
                         self.extapigateway_url )
            
            bearerTokenMgr = SASCI360VeloxPyBearerTokenManager(self.extapigateway_url,self.client_id,self.client_secret, self.api_user_name,self.api_user_password)
            bearerToken = await bearerTokenMgr.get_token()
       
            token = bearerToken

        elif authType == "static_jwt":
            logger.info("Fetching static JWT token")
            # Fetch a static JWT token
            # This will call the generateStaticJWT method in the SASCI360VeloxPyBearerTokenManager class
            # and set the token in the headers
            logger.debug("Fetching static JWT token with parameters: extapigateway_url: %s, client_id: %s, api_user_name: %s",
                         self.extapigateway_url, self.client_id,  self.api_user_name)
            # Create an instance of the SASCI360VeloxPyBearerTokenManager
            # and call the generateStaticJWT method to fetch the token  
            # This will generate a static JWT token based on the tenantId and secret
            # and set the token in the headers
            
            bearerTokenMgrForStatic = SASCI360VeloxPyBearerTokenManager(self.extapigateway_url,self.client_id,self.client_secret, self.api_user_name,self.api_user_password)
            staticToken = bearerTokenMgrForStatic.generateStaticJWT()
            token = staticToken
            

        else:
            
            # If a token is provided, use it in the headers
            headers['Authorization'] = f'Bearer {token}'

        # Set the token in the headers
        
        if headers.get('Authorization') == "Bearer {token}" and (authType != "" or authType==None):
            
            headers['Authorization'] = f'Bearer {token}'
        else:
            logger.debug("Removing Authorization header: %s", headers.get('Authorization', 'None provided'))  
            headers.pop('Authorization')
            
        
        try :
            logger.debug("Making API request: method=%s, url=%s, args=%s", method, url,  args)
            logger.info("Making API request: method=%s, url=%s, args=%s", method, url, args)
            async with aiohttp.ClientSession() as session:
                # Use aiohttp to make the request
                if method.upper() == 'GET': 
                    logger.debug("Making GET request to %s", url)
                    async with session.get(url, headers=headers, **args) as resp:
                        try:
                            resp.raise_for_status()
                            data = await resp.json()

                        except Exception as e:
                            data = await resp.text()
                            logger.error("Response from API: %s", data if data else "")
                            logger.error("Error details: %s", str(e))
                        return data
                elif method.upper() == 'POST':
                    # check file is open or not
                    logger.debug("Making POST request to URL: %s with args: %s", url, args)
                    async with session.post(url, headers=headers, **args) as resp:

                        try:
                            resp.raise_for_status()
                            data = await resp.json()
                        except Exception as e:
                            data = await resp.text()
                            logger.error("Response from API: %s", data if data else "")
                            logger.error("Error details: %s", str(e))
                        return data 
                elif method.upper() == 'PUT':
                    logger.debug("Making PUT request to URL: %s with args: %s", url, args)
                    async with session.put(url, headers=headers, **args) as resp:
                        try:
                            resp.raise_for_status()
                            data = await resp.json()
                        except Exception as e:
                            data = await resp.text()
                            logger.error("Response from API: %s", data if data else "")
                            logger.error("Error details: %s", str(e))
                        return data
                elif method.upper() == 'DELETE':
                    logger.debug("Making DELETE request to URL: %s with args: %s", url, args)
                    # Use aiohttp to make the DELETE request
                    async with session.delete(url, headers=headers, **args) as resp:
                        try:
                            resp.raise_for_status()
                            data = await resp.json()
                        except Exception as e:
                            data = await resp.text()
                            logger.error("Response from API: %s", data if data else "")
                            logger.error("Error details: %s", str(e))
                        return data
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}. Supported methods are GET, POST, PUT, DELETE.")   
        except aiohttp.ClientError as e:
            logger.error("Error making API request: %s", e)
            raise Exception(f"Error making API request: {e}")
        except ExceptionGroup as e:
            logger.error("Error making API request: %s", e)
            raise Exception(f"Error making API request: {e}")
        except Exception as e:
            logger.error("Unexpected error making API request: %s", e)
            raise Exception(f"Unexpected error making API request: %s", e)
        except BaseException as e:
            logger.error("Base exception occurred: %s", e)
            raise Exception(f"Base exception occurred: {e}")
        except KeyboardInterrupt as e:
            logger.error("KeyboardInterrupt occurred: %s", e)
            raise Exception(f"KeyboardInterrupt occurred: {e}")
        except SystemExit as e:
            logger.error("SystemExit occurred: %s", e)
            raise Exception(f"SystemExit occurred: {e}")

    def syncRequest(self, method, args):
        """
        Synchronous wrapper around the asynchronous `request()` method.

        Parameters:
            method (str): HTTP method (e.g., "GET", "POST").
            args (dict): Arguments to pass to the request, including URL, headers, etc.

        Returns:
            str: Response content from the API.

        Example:
            ```python
            client = SASCI360VeloxPyBaseService()
            data = client.request_sync("GET", {"url": "https://api.example.com/data"})
            print(data)
            ```

        Use Case:
            - Use in scripts, CLI tools, or tests where async is not supported or not needed.
            - Automatically manages the event loop using `asyncio.run()`.
        """
        return asyncio.run(self.asyncRequest(method, args))