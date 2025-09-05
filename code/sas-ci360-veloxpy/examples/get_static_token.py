from sas_ci360_veloxpy import initApp
import base64
import jwt, asyncio
from sas_ci360_veloxpy.io.loader import getConfigDetails
from sas_ci360_veloxpy.auth.bearer import SASCI360VeloxPyBearerTokenManager


#Make this method async if want to get token 
def main():

    initApp("<FullPathToFile>\\sasci360veloxpy.ini")
    configData=getConfigDetails()

    tenantId = str(configData.get("client_id"))
    secret = str(configData.get("client_secret"))
    
    #trim tenantid and secret
    tenantId = tenantId.strip()
    secret = secret.strip()

    print(":::tenantId,secret-----",tenantId,secret)

    encodedSecret = base64.b64encode(bytes(secret, 'utf-8'))
    token = jwt.encode({'clientID': tenantId}, encodedSecret, algorithm='HS256')
    print('\nJWT token: ', token)

    staticToken = getToken()
    # print("StaticToken",staticToken)

    print("----Are both token same----", token == staticToken)

def getToken():
    configData=getConfigDetails()

    bearerTokenMgr = SASCI360VeloxPyBearerTokenManager(configData['extapigateway_url'],configData['client_id'],configData['client_secret'], configData['api_user_name'],configData['api_user_password'])
    bearerToken = bearerTokenMgr.generateStaticJWT()
    token = bearerToken
    return token    
    
if __name__ == "__main__":
    main()

