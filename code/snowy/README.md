# Snowy: Monitor SAS CI 360 Network Traffic

Snowy is a browser extension developed to help you troubleshoot SAS CI360 Events. You can use Snowy to inspect the data captured by CI360 Tag from your website on your browser in real time. 

Snowy can also act as an alternative to General Agent or a Diagnostic Agent, so that you can stream Events from the tenant to your browser for troubleshooting purposes. 

If you want to capture the JSON payload sent by CI360 to your Connector Endpoint, you can assign a General Agent to your Connector, under CI360 Tenant > General Setting > Connector, and activate that Access Point details in Snowy.

# Features
1. Monitor CI360 Event information captured by CI360 Tag.
2. Act as a General Agent or Diagnostic Agent to capture real time events from any channels, including Mobile SDK.
3. Inspect the payload sent to Connector Endpoint, while using a General Agent as Proxy.
 
# Installation
## Chrome
1. Download the content of the `Chrome` folder to your machine and extract it to a folder.
2. Open Chrome 
3. Navigate to `Settings` > `More Tools` > `Extension`
4. Enable `Developer Mode` (usually on the upper right-hand side)
5. Click `Load Unpacked` 
6. Select the folder where you extracted the content
7. The extension is now added to Chrome
8. Click on the `Details` button
9. Ensure the extension is `On`
### Optional steps
1. Under the `Details` of the extension
2. Set the `Site access` to your choice. The preferred option is `On Click`
3. If you usually browse the sites in Incognito mode, then enable `Allow in incognito`
## Microsoft Edge
1. Download the content of the `Chrome` folder to your machine and extract it to a folder.
2. Open Microsoft Edge
3. Navigate to `Settings` > `Extensions`
4. Enable `Developer Mode` (usually on the lower left-hand side)
5. Click `Load Unpacked` 
6. Select the folder where you extracted the content
7. The extension is now added to Edge
8. Click on the `Details` link
9. Ensure the extension is `On`
### Optional steps
1. Under the `Details` of the extension
2. Set the `Site access` to your choice. The preferred option is `On Click`
3. If you usually browse the sites in InPrivate mode, then enable `Allow in InPrivate`

# Update to a newer version
Since the extension is not distributed via the public domains (like Chrome Web Store), you will have to follow manual steps to install the updates.
## Chrome and Microsoft Edge
1. Download the content of the `Chrome` folder to your machine and extract it to the same folder where you extracted it during the installation.
2. Open the `Extensions` section under your `Settings`
3. Click the `Reload` icon or `Reload` button


# Usage
## Monitoring Network Traffic from CI 360 Tag
1. Open your browser
2. Navigate to the website you want to inspect.
3. Based on the configuration you selected under the `Optional Steps` above, you might have to click on the icon of the extension to start it.
4. Open the `Developer Toolbar` (`F12` or `Ctrl+Shift+I`)
5. Inside the Developer Toolbar, go to the tab `CI 360`
6. Browse around your site as usual and you will see all the 360 network traffic getting added to the list.
7. The list will be reset when you Close the developer toolbar
## Streaming Events into Snowy
`Warning!`
When you start streaming events to Snowy by activating the Access Point (aka Agent), ensure you do not have another instance of the Agent running elsewhere.
1. Open your browser
2. Navigate to the website you want to inspect.
3. Based on the configuration you selected under the `Optional Steps` above, you might have to click on the icon of the extension to start it.
4. Open the `Developer Toolbar` (`F12` or `Ctrl+Shift+I`)
5. Inside the Developer Toolbar, go to the tab `CI 360`
6. Go to the Setting Tab with in Snowy (the little `Purple` gear icon on the top right hand side)
7. Enter the Tenant Id, Client secret and other details as needed
8. Save and Activate the Agent.
9. If you have a details saved already, you can select that from the dropdown and activate it.
10. Go to the `360 Event Stream` Tab with in Snowy.
11. Click on the Start button.
12. If the Access Point you configured is a Diagnostic Access Point, then you will be prompted to select a filter based on Identity. Provide necessary details. 

# Note
1. If you are looking to demo the data captured by CI 360 to a prospect then please use CI 360 Event Inspector by the Sales Team
2. Snowy captures only the POST requests in its raw format, any processing done by CI 360 in the cloud will not reflect here
3. When you activate Snowy's event stream capabilities, (aka. Agent), it will temporarily break your integration, if you have done any, using that Access Point. 