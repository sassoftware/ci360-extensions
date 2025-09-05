import requests
import base64, jwt
from .base import SASCI360VeloxPyBaseAuthManager
import sas_ci360_veloxpy
from datetime import datetime, timedelta, timezone
from ..config.config_loader import get_config
from ..io.log import get_logger

logger = get_logger("SASCI360VeloxPyBearerTokenManager")

class SASCI360VeloxPyBearerTokenManager(SASCI360VeloxPyBaseAuthManager):
    def __init__(self, extapigateway_url, client_id, client_secret,api_user_name, api_user_password):
        super().__init__()
               
        self.token_url = "https://"+str(get_config("GLOBAL_CONFIG", "ini_file_path", "tenant.extapigateway_url", "") or extapigateway_url )+"/token"
        self.api_user_name = api_user_name
        self.api_user_password = api_user_password
        self.client_id = client_id
        self.client_secret = client_secret
      

    async def fetch_token(self):
        token = self.generateStaticJWT()
        token_b = "Bearer " + token
      
        headers = {
            "Authorization": token_b ,
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive"
        }
        
        body = {
            "grant_type": "password",
            "username": self.api_user_name,
            "password": self.api_user_password
        }
        
        try:
            logger.debug("Generating an API token : paramateres passed : body: %s,  token url: %s",  body, self.token_url)
            response = requests.post(self.token_url, data=body, headers=headers)
            response.raise_for_status()
            response_data = response.json()
            expires_in = response_data["expires_in"]
            
            self.expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in - 30)  # 30s buffer
            self._expiry_time = datetime.now(timezone.utc) + timedelta(seconds=expires_in - 30)  # 30s buffer
            
            self._token = response_data['access_token']

            sas_ci360_veloxpy.tokenData = {
                "access_token" : response_data['access_token'],
                "expiry_time" : datetime.now(timezone.utc) + timedelta(seconds=expires_in - 30)
            }
            
            return response_data['access_token']
        except requests.RequestException as e:
            logger.error(f"Error obtaining temporary access token: {e}")
            raise Exception(f"Error obtaining temporary access token: {e}")

    # PyJWT returns str type for jwt.encode() function: https://pyjwt.readthedocs.io/en/latest/changelog.html#improve-typings
    # For backwards compatibility for older PyJwt releases, decode or return token value based on type.
    def decodeToken(self,token):
        if (type(token)) == bytes:
            return bytes.decode(token)
        else:
            return token
        
    def generateStaticJWT(self):
        tenantId = self.client_id
        secret = self.client_secret

        tenantId = str(tenantId.strip())
        secret = str(secret.strip())

        encodedSecret = base64.b64encode(bytes(secret, 'utf-8'))
        token = jwt.encode({'clientID': tenantId}, encodedSecret, algorithm='HS256')
        token = self.decodeToken(token)
        self._token = token
        
        logger.debug("Generated static JWT token")
        return token
    