import time
from sas_ci360_veloxpy import initApp, tokenData, APIClient
# from sas_ci360_veloxpy.apis.marketing_audience.audiences import marketing_audiences
from sas_ci360_veloxpy.io.loader import getConfigDetails
from sas_ci360_veloxpy.auth.bearer import SASCI360VeloxPyBearerTokenManager

async def getToken():
    configData=getConfigDetails()
    print("Config details", configData) 

    bearerTokenMgr = SASCI360VeloxPyBearerTokenManager(configData['extapigateway_url'],configData['client_id'],configData['client_secret'], configData['api_user_name'],configData['api_user_password'])
    bearerToken = await bearerTokenMgr.get_token()
    token = bearerToken
    return token

#Make this method async if want to get token 
def main():
    initApp("<FullPathToFile>\\sasci360veloxpy.ini")
    print("App Initiated")
    
    res = APIClient()
    
    while True:
        start = time.time()       
        try:
          # print("dfghj")
          res_data = res.get_audiences()

          print("res Audience ", res_data, tokenData)
            # load_config("")
        except Exception as exc:
          print("Unexpected error:", exc)

        elapsed = time.time() - start
        sleep_duration = max(60 - elapsed, 0)  # Test code : Token refreshes after interval
        time.sleep(sleep_duration)  # Sleep for remaining time
    
    
    
if __name__ == "__main__":
    main()

