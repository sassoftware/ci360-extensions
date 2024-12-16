# EventHub to CI360 Connector  

## Overview  
The **EventHub to CI360 Connector** enables seamless integration between **Azure Event Hub** and **SAS CI360**. This connector processes events from an Event Hub and sends them to CI360 as external events, enabling real-time personalization, customer journey orchestration, and analytics.

---

## Features  
- **Real-Time Event Ingestion**: Receives and processes events from Azure Event Hub.  
- **Configurable**: Easily set up with environment variables for CI360 and Event Hub configurations.  
- **Scalable**: Handles high volumes of events reliably.  
- **Event Mapping**: Maps Event Hub data to CI360 external event schemas.

---

## Prerequisites  
- **Azure Event Hub**:  
  - Configured Event Hub namespace and Event Hub.  
  - Consumer group for event processing.  
- **SAS CI360**:  
  - Access to CI360 with external event ingestion API enabled.  
- **Environment Variables**:  
  - Required variables (`ci360_url` and `token`) must be configured.

---

## Environment Variables  

| Variable Name | Description                                     |
|---------------|-------------------------------------------------|
| `ci360_url`   | The URL of the CI360 external event ingestion API. |
| `token`       | The authentication token for accessing CI360.   |

### Setting Environment Variables in Azure  

1. **Navigate to the Azure Function App**:  
   - Open the [Azure Portal](https://portal.azure.com).  
   - Go to your Function App.  

2. **Access Configuration**:  
   - In the left-hand menu, click **Configuration** under the **Settings** section.  

3. **Add New Application Settings**:  
   - Click **+ New application setting** and add the following:  
     - **Name**: `ci360_url`  
       **Value**: Your CI360 API endpoint.  
     - **Name**: `token`  
       **Value**: Your CI360 authentication token.  

# Deployment Instructions  

## Configure Event Hub Trigger  
1. **Select Your Event Hub**:  
   - During configuration, link the Azure Function to your specific **Event Hub** by providing:  
     - **Event Hub Name**: Name of your Event Hub (e.g., `ci360_events`).  
     - **Connection String**: Use the **RootManageSharedAccessKey** from your Event Hub namespace for secure access.  

2. **Verify Connection**:  
   - Ensure the connection string is correct and the Event Hub is reachable from the Azure Function.  


## Deploy the Azure Function  
- Use the **Azure CLI**, **Visual Studio Code**, or the **Azure Portal** to deploy the function app.  
- Ensure the `requirements.txt` file includes all necessary dependencies like:  
  - `azure-functions`  
  - `requests`  
  - `logging`  


## Verify Environment Variables  
- After deployment, confirm that the environment variables are correctly set by checking the **Application Settings** in the **Azure Portal**.  

## Test the Function  
1. **Send a Test Event**:  
   - Publish a sample event to your Event Hub.  

2. **Monitor Logs**:  
   - Verify that the Azure Function processes the event and sends it to CI360 by reviewing the logs.  

---

# Logging and Troubleshooting  

## Logs  
- Use the **Log Stream** feature in the **Azure Portal** to monitor real-time logs from the Azure Function.  

## Diagnostics  
- Enable **Application Insights** to gain access to advanced diagnostics and telemetry data.  

## Common Errors  
| **Issue**                         | **Resolution**                                                                 |
|------------------------------------|-------------------------------------------------------------------------------|
| Missing environment variables      | Check and update the **Application Settings** in the Azure Portal.            |
| API authentication failure         | Verify that the `token` value is valid and has the required permissions.       |
| Event Hub connection failure       | Ensure the Event Hub namespace and consumer group are correctly configured.    |

---

4. **Save Changes**:  
   - Click **Save** to apply changes. Restart the Function App if prompted.

---

