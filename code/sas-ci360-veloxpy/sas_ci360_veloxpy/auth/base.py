from abc import ABC, abstractmethod
from datetime import datetime,timezone
import asyncio, sas_ci360_veloxpy
from ..io.log import get_logger

logger = get_logger("SASCI360VeloxPyBaseAuthManager")

class SASCI360VeloxPyBaseAuthManager(ABC):
    def __init__(self):
        self._token = None
        self._expiry_time = None
        self._lock = asyncio.Lock()

    @abstractmethod
    async def fetch_token(self):
        """Fetch a new token from the auth server."""
        pass
    
    @property
    def token(self):
        """Return the current token."""
        if not self._token:
            raise ValueError("Token has not been fetched yet.")
        if self.is_token_expired():
            raise ValueError("Token is expired. Please refresh the token.")
        return self._token or sas_ci360_veloxpy.tokenData.get("access_token", None)
    
    @property
    def expiry_time(self):
        """Return the expiry time of the current token."""
        if not self._expiry_time:
            raise ValueError("Expiry time has not been set.")
        return self._expiry_time or sas_ci360_veloxpy.tokenData.get("expiry_time", None)    
    
    async def get_token(self):
        """Get the current token, refreshing it if necessary."""
        async with self._lock:
            if not self._token or self.is_token_expired():
                logger.info("Token is expired or not set, fetching a new one")
                await self.fetch_token()
            else:
                logger.info("Token is valid and not expired")
        # Return the current token
        if not self._token:
            if not sas_ci360_veloxpy.tokenData or "access_token" not in sas_ci360_veloxpy.tokenData:
                raise ValueError("No valid token available.")
            self._token = sas_ci360_veloxpy.tokenData["access_token"]
        logger.debug("Returning token: %s", self._token or sas_ci360_veloxpy.tokenData.get("access_token", None))
        logger.info("Returning token: %s", self._token or sas_ci360_veloxpy.tokenData.get("access_token", None))
        
        return self._token or sas_ci360_veloxpy.tokenData["access_token"]

    def is_token_expired(self):
        """Check if the token is expired."""
        
        if not sas_ci360_veloxpy.tokenData or "expiry_time" not in sas_ci360_veloxpy.tokenData:
            return True
        tknData = sas_ci360_veloxpy.tokenData
        if tknData == None or tknData == {}:
            exp_time = datetime.now(timezone.utc)
            token = str("")
        else:
            exp_time = tknData['expiry_time']
            token = tknData['access_token']

        if tknData == None or tknData == {}:
            logger.warning("Token data is empty or None, returning True for token expiration check")
            # If tokenData is empty, we assume the token is expired
            return True 
        if not (self._token or token) or not (self._expiry_time or exp_time):
            logger.warning("Token or expiry time is not set, returning True for token expiration check")
            # If token or expiry time is not set, we assume the token is expired
            return True
       
        if not (self._expiry_time or exp_time):
            exp_time = datetime.now(timezone.utc)
            token = str("")
        if not (self._token or token):
            self._token = str("")
            exp_time = datetime.now(timezone.utc)
                 
        logger.debug("Checking token expiration: current time: %s, expiry time: %s", datetime.now(timezone.utc), (self._expiry_time or exp_time))

        if not (self._token or token) or not (self._expiry_time or exp_time):
            logger.warning("Token or expiry time is not set, returning True for token expiration check")
            # If token or expiry time is not set, we assume the token is expired
            return True
        logger.debug("Token is not expired, returning False")
        return datetime.now(timezone.utc) >= (self._expiry_time or exp_time)

    async def refresh_token(self):
        """Refresh the token by fetching a new one."""
        logger.info("Refreshing token")
        async with self._lock:
            await self.fetch_token()
            logger.info("Token refreshed successfully")
        # After refreshing, we can return the new token
        logger.debug("Returning refreshed token: %s", self._token or sas_ci360_veloxpy.tokenData.get("access_token", None))
        logger.info("Returning refreshed token: %s", self._token or sas_ci360_veloxpy.tokenData.get("access_token", None))
        return self._token or sas_ci360_veloxpy.tokenData.get("access_token", None)
        
