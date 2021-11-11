# Connector Endpoint


## Usage
The below contains the URL needed for the endpoint for Facebook Pixel Event via Graph API
There is no need to use any mediation code (lambda or azure functions for example) since this requires you know the user's email and directly connects to Facebook's Graph API
```
https://graph.facebook.com/v8.0/<pixel_id>/events?data=[{event_name:"{{$eventname}}",event_time:{{$timestamp}},user_data:{fbq: "{{$fbq}},em:{{$fbem}}"},custom_data:{{{$eventcategory}}:"{{$event}}"}}]&test_event_code=TEST50468
```

## Required Values:
* **{{$eventname}}** : Required for sending information to FB Pixel, this event name to store against with Facebook.  
* **{{$timestamp}}** : Required for sending information to FB Pixel, this event timestamp in 10 digital format.
* **{{$fbq}}** : Required to identify the user by fb cookie.  It will be use when other identifier is not available in the code.
* **{{$fbem}}** : Required to identify the user by fb email.

## Support Values:

Note that for best pratices to store data in FB, it should always contains Event Label, Event Category, and Event Action in the measurement protcol.   For SAS connector, we prefixed these value as below, it designed for easier filtering in the FB report:
* **{{$eventcategory}}** : This is Event Category to group all events come from SAS to FB
* **{{$event}}}** : The event value for FB retargeting purpose


## Notes

The values in the endpoint of the format {{$value}} are values replaced in the CI360 interface using information from JSON payload. An example of that payload can be found [here](Example JSON Payload.json). JSON selector testing can be done [here](https://www.jsonpath.com)
