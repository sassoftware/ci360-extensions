# Connector Endpoint


## Usage
The below contains the URL needed for the endpoint for Adobe
```
https://<Regional_Server_Host_Name>.demdex.net/event?c_caller=ci360sts&c_ci360event={{$event}}&c_ci360eventcategory={{$eventcategory}}&mid={{$s_ecid}}
```

## Required Values:
* **<Regional_Server_Host_Name>** : To be replaced with using Adobe's documentation [here](https://docs.adobe.com/content/help/en/audience-manager/user-guide/api-and-sdk-code/dcs/dcs-api-reference/dcs-regions.html)
* **mid={{$s_ecid}}** : Required for sending information to AAM, this identifies the user and attaches the information in the variables below to a user

## Optional/Custom Values:

Note that anything prefixed with c_ is a (Adobe) customer defined value and can be modified to whatever you want as long is doesn't conflict with already in use variables. The currently settings are an attempt to standardize this but can be changed to conform to customer standards or usage. Keep this all the way it is unless you are aware of the changes you must make
* **c_caller=ci360sts** : This is set to a default value to define where the data is coming from
* **c_ci360event={{$event}}** : We Use this to define the name of the event, in our use cases we are differentiating most data using the triggering event name
* **c_ci360eventcategory={{$eventcategory}}** : This is the event category used similarly to the c_ci360event name value pair except using the event category


## Notes

The values in the endpoint of the format {{$value}} are values replaced in the CI360 interface using information from JSON payload. An example of that payload can be found [here](Example JSON Payload.json). JSON selector testing can be done [here](https://www.jsonpath.com)
