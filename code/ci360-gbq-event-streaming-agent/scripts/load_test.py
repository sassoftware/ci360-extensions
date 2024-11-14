import requests
import json
import time
import random
from datetime import datetime
from multiprocessing import Process



def external_event():
    
    url = "https://extapigwservice-training.ci360.sas.com/marketingGateway/events"
    payload = json.dumps({
    "eventName": "CounterPurchaseEventLoadTest",
    "login_id": "Nikola",
    "applicationId": "Sales-Counter-App",
    "attributes": {
        "category": "Electronics",
        "contact_email_address": "exaample@example.com",
        "amount": 123
    }
    })
    headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJjbGllbnRJRCI6ImRhMWExMDVmNTMwMDAxM2I0ZGRlMWIxOCJ9.0VVNUIhekH8o5geI3i-lBn-fH9DXRUiDPC00Tkz68QM'
    }
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    print(dt_string+"")
    response = requests.request("POST", url, headers=headers, data=payload)
    print("api response: "+response.text)
    


# Define a dummy function to be called during the simulation
def dummy_function():
    x = random.randint(1, 3)    # Pick a random number between 1 and 100.
    for j in range (x):
        # external_event(counter)
        p = Process(target=external_event)
        # you have to set daemon true to not have to wait for the process to join
        p.daemon = True
        p.start()
    # datetime object containing current date and time
    

# Calculate the time interval between each function call
interval = 8 * 60 * 60 / 250  # 8 hours in seconds divided by 25,000 calls

# Perform the simulation
if __name__ == '__main__':
    for i in range(25000):
        dummy_function()
        # Wait for a random amount of time between 0 and the interval
        wait_time= random.uniform(0, interval)
        print("wait time: ",wait_time)
        time.sleep(wait_time)
    



