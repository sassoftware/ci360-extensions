from sas_ci360_veloxpy import initApp
import asyncio
from sas_ci360_veloxpy.utils.logging import SASCI360VeloxPyLogging
import time
from sas_ci360_veloxpy.io.loader import getConfigDetails
from sas_ci360_veloxpy.auth.bearer import SASCI360VeloxPyBearerTokenManager


async def getToken():
        configData=getConfigDetails()
        # print("Config details", configData) 

        bearerTokenMgr = SASCI360VeloxPyBearerTokenManager(configData['extapigateway_url'],configData['client_id'],configData['client_secret'], configData['api_user_name'],configData['api_user_password'])
        bearerToken = await bearerTokenMgr.get_token()
        token = bearerToken
        return token

#Make this method async if want to get token 
async def main():
    initApp("<FullPathToFile>\\sasci360veloxpy.ini")
    print("App Initiated")
    
    while True:
        start = time.time()       
        try:
            tokenValue = await getToken()
            print("tokenValue",tokenValue)
        except Exception as exc:
          print("Unexpected error:", exc)

        elapsed = time.time() - start
        sleep_duration = max(60 - elapsed, 0)  # Test code : Token refreshes after interval of 60
        time.sleep(sleep_duration)  # Sleep for remaining time

if __name__ == "__main__":
    asyncio.run(main()) 

