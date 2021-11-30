# Connector Endpoint


## Usage
The below contains the URL needed for the endpoint for Google Measurement Protcol
There is no need to use any mediation code (lambda or azure functions for example) since this directly connects to Google Analytics's API
```
https://www.google-analytics.com/collect?v=1&tid=UA-8886598-2&t=event&cid={{$clientid}}&uid={{$userid}}&cd5={{$event}}&el=ci360_api&ea=sas_custom_event&ec=general


``` 

## Required Values:
* **cid={{$clientid}}** : Required for sending information to GA, this identifies the user and attaches the information in the variables below to a user
* **uid={{$userid}}** : Required for sending information to GA, this used for map the known user
* **cd5={{$event}}** : Required for sending segment value to GA and under the custom dimension index 5 column.  Please note: the index number is different depended on the custom dimension table order under the GA account.

## Support Values:

Note that for best pratices to store data in GA, it should always contains Event Label, Event Category, and Event Action in the measurement protcol.   For SAS connector, we prefiexed these value as below, it desinged for easier filtering in the GA report:
* **el=ci360_api** : This is Event Label by default named as ci360_api for seperate the event from pixel and api (connector)
* **ea=sas_custom_event** : We Use this to define the name of the event, in our use cases we are differentiating most data using the triggering event name
* **ec=general** : This is the event category by default it is "general"


## Notes

The values in the endpoint of the format {{$value}} are values replaced in the CI360 interface using information from JSON payload. An example of that payload can be found [here](Example JSON Payload.json). JSON selector testing can be done [here](https://www.jsonpath.com)