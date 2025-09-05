from sas_ci360_veloxpy import   initApp, APIClient, GLOBAL_CONFIG


def main():
    
    print("Initializing application...")
    initApp("<<full path>>\\sasci360veloxpy.ini")
    print("Application initialized.")

    print("Global Config:", GLOBAL_CONFIG)
    apiClient = APIClient()
    print("---apiClient---", apiClient.send_external_events)
    
    eventData= {
    "eventName": "External_Event_Example",
        "subject_id": "267756",
        "testattr1": "test",
        "testattr2": "test@sas.com"
    }

    res = apiClient.send_external_events(event_data=eventData)
    print("---rse---",res)

if __name__ == "__main__":
    main()