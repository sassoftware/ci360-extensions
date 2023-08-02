# Snowy: Monitor SAS CI 360 Network Traffic
An easier way to monitor the network traffic (POST) to SAS CI 360, with the ability to search the form data. For example, you can use this extension to filter and find out the identity event or search for any other kind of event type or input field.
# Features
1. Tracks all POST requests initiated by SAS CI 360 on any website
2. Tracks all Form data from your request. All Form data will be displayed in a nice format.
3. Search and filter request based on Form data or Event (Similar to [Tintin](https://gitlab.sas.com/psd-ci-enablement/tintin))
4. Option to sort on any column
4. Option to enable and disable per website (provided by your browser)
 
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
1. Open your browser
2. Navigate to the website you want to inspect.
3. Based on the configuration you selected under the `Optional Steps` above, you might have to click on the icon of the extension to start it.
4. Open the `Developer Toolbar` (`F12` or `Ctrl+Shift+I`)
5. Inside the Developer Toolbar, go to the tab `CI 360`
6. Browser around your site as usual and you will see all the 360 network traffic getting added to the list.
7. The list will be reset when you Close the developer toolbar

# Note
1. If you are looking to demo the data captured by CI 360 to a prospect then please use CI 360 Event Inspector by the Sales Team
2. Snowy captures only the POST requests in its raw format, any processing done by CI 360 in the cloud will not reflect here