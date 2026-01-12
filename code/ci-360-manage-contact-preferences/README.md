#  SAS Customer Intelligence 360 Contact Preference Download Package: Python

## Overview

This Python script enables you to download/upload/validate the CI 360 contact preference data for both Email and SMS into csv using a command-line interface

This script can perform the following tasks currently:
   * Download different contact preferences: Email or SMS
   * Specify file name with datetime
   * Specify if additional columns should be added to the output file


## Prerequisites
1. Install Python (version 3 or later) from https://www.python.org/.

   **Tip:** Select the option to add Python to your PATH variable. If you choose the advanced installation option, make sure to install the pip utility.

2. Clone this repo to your local machine

3. Make sure the required modules are installed. 
You can install the required packages with the following command:

 ```pip install -r requirements.txt```

4. Create an access point in SAS Customer Intelligence 360.
   1. From the user interface, navigate to **General Settings** > **External** > **Access**.
   2. Create a new general access point if one does not exist.
   3. Get the following information from the access point:  
      - **External gateway address**: *e.g. https://extapigwservice-prod.ci360.sas.com*
      - **Tenant ID**: *abc123-ci360-tenant-id-xyz*
      - **Client secret**: *ABC123ci360clientSecretXYZ*

5. In the root directory find the **sample.config.ini** file set the values and *rename* to **config.ini**
   ```
   [tenant_information]
   ci360_url=https://extapigwservice-prod.ci360.sas.com
   tenantID=abc123-ci360-tenant-id-xyz 
   client_secret=ABC123ci360clientSecretXYZ
   ```
6. Verify the installation by running the following command from command prompt:  
```py contact_preference.py –h```
7. See below for usage

## Options

### Example usage:
```console
 py contact_preference.py upload email -k
 py contact_preference.py download sms -H
```
### Default Options
```
usage: contact_preference.py [-h] [-c CONFIG] [-v] {upload,download} ...

    Export or Upload contact preference data for Email and SMS from CI360.
    Requires a configuration ini file with tenant information.
    Default config file path is 'config.ini' in the current directory.


positional arguments:
  {upload,download}    Sub-command help
    upload             Upload a contact preference file.
    download           Download contact preferences data

options:
  -h, --help           show this help message and exit
  -c, --config CONFIG  Path to config ini file. default: config.ini
  -v, --verbose        Increase log verbosity (e.g., show INFO messages)
```
### Uploading Options
```
usage: contact_preference.py upload [-h] [-H] [-k] [-d KEEP_DESTINATION] {email,sms} filename

    Upload contact preference data for Email and SMS to CI360.

    Example:
        py contact_preference.py upload email path/to/contact_preference_file.csv -H
        py contact_preference.py upload sms path/to/contact_preference_file.csv -k -d uploaded_files/


positional arguments:
  {email,sms}           Type of contact preference to upload. Options are "email" or "sms".
  filename              Path to the contact preference file to upload

options:
  -h, --help            show this help message and exit
  -H, --headers         Headers included in the contact preference file.
  -k, --keep-file-uploaded
                        Copy the file uploaded in CI360 to Folder. By default, file is not copied. Files are stored with the (date)_(Export_Table_Job_ID).csv
  -d, --keep-destination KEEP_DESTINATION
                        Destination folder to keep uploaded file. default: uploaded_files/ in the base directory
```
### Downloading Contact Preference Options
```
usage: contact_preference.py download [-h] [-O DOWNLOAD_DIR] [-f FILE_NAME] [-e FILE_EXTENSION] [-nt] [-s] [-H] [-w WAIT_TIME] [-m MAX_TRIES] {email,sms}

    Download contact preference data for Email and SMS from CI360.

    Example:
        py contact_preference.py download email -O downloads/
        py contact_preference.py download sms -O downloads/ -f sms_preferences -e .txt -nt -s -H -w 15 -m 20


positional arguments:
  {email,sms}           Type of contact preference to download. Options are "email" or "sms".

options:
  -h, --help            show this help message and exit
  -O, --download-dir DOWNLOAD_DIR
                        Output download directory. default: downloads/
  -f, --file-name FILE_NAME
                        Base output file name (without extension). default: contact_preference_data
  -e, --file-extension FILE_EXTENSION
                        File extension for the output file, default: .csv
  -nt, --no-file-name-timestamp
                        Do not append timestamp to file name. By default, timestamp is appended.
  -s, --include-source-and-timestamp
                        Include source and timestamp in export file. Source and timestamp fields are NOT added by default.
  -H, --include-export-headers
                        Include headers in export file. Headers are NOT included by default.
  -w, --wait-time WAIT_TIME
                        Seconds to wait between status checks. default: 10
  -m, --max-tries MAX_TRIES
                        Maximum number of status check retries. default: 10
```

## Contributing

We welcome your contributions! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to submit contributions to this project.


<a id="license"> </a>

## License

This project is licensed under the [Apache 2.0 License](LICENSE).


<a id="resources"> </a>

## Additional Resources
For more information, see [Downloading Contact Preference Data from SAS Customer Intelligence 360](https://go.documentation.sas.com/?cdcId=cintcdc&cdcVersion=production.a&docsetId=cintag&docsetTarget=dat-contactpref-export.htm&locale=enc).
