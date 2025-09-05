from sas_ci360_veloxpy import initApp, APIClient, GLOBAL_CONFIG


import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("Logs_Client_API.log"),
        # logging.StreamHandler()
    ]
)


logger = logging.getLogger("AudienceRootLoggerfromClient")

#Make this method async if want to get token 
def main():
    initApp("<<full path>>\\sasci360veloxpy.ini")
    logger.info("App Initiated")
   
    # get_audiences = res.get_audiences()
    apiclient = APIClient()
    uploadAudience = "<<full path>>\\examples\\Resources\\audience_config.ini"
    createAudience = "<<full path>>\\examples\\Resources\\audience_definition.json"
    # res = apiclient.create_audience_definition(createAudience)
    # res = apiclient.get_audience_by_id("8d1fd19b-058b-4b99-89d3-b3c014d5e83f")
    # res = apiclient.delete_audience_by_id("8d1fd19b-058b-4b99-89d3-b3c014d5e83f")  
    # res = apiclient.patch_audience("5102b447-2846-432c-8380-146fd79ee80c")
    # res = apiclient.get_audience_by_id("a860c989-83e5-4849-87a2-e3ba80d55b2d")
    # res = apiclient.update_audience_by_id("<<full path>>\\examples\\Resources\\audience_definition.json", "a860c989-83e5-4849-87a2-e3ba80d55b2d")
    res = apiclient.get_file_history_by_upload_id("f2490bac-9b4a-4620-a47e-312ebf772e07", "ab95f180-5456-4c15-beda-6b54606a5bb7")
    print("res Audience ", res)
   
    
    
if __name__ == "__main__":
    main()
    # asyncio.run(main()) # test code for async def

