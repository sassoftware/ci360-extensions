# No-code CI360 integration with Twilio

## Overview

A connector can be configured to send outwards to Twilio directly in some cases without the need for an intermediate connector function. It is still strictly a one-way connector, so no information will come back to CI360 without additional setup. This includes:

- Replies, including STOP requests
- Send verification


## Setting up the connector and endpoint

For the actual process of adding a connector and endpoint you can refer to the CI360 documentation:

>**Add and Register a Connector**  Please refer to  [`Add and Register a Connector`](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/ext-connectors-add.htm)  in SAS Customer Intelligence 360 admin guide.
>
>**Add an Endpoint**  Please refer to  [`Add an Endpoint`](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/ext-connectors-add-endpoint.htm)  in SAS Customer Intelligence 360 admin guide.

The connector should be created using the standard items, describing the name, description, contact, etc. Then you can create an endpoint for Twilio's SMS api. The endpoint is provided by Twilio for their API specific to your account. It will look similar to the following:

```
https://api.twilio.com/2010-04-01/Accounts/ACabcdeff85123456a291e1da42/Messages.json
```

The above endpoint expects to receive payload data in **application/x-www-form-urlencoded** format. Which is similar to how form data on websites is submitted.  

Although CI360 does not explicitly support this, we can emulate this format using a POST request as part of the endpoint and use custom payload format 
```
variable1_key=variable1_value&variable1_key=variable1_value&variable1_key=variable1_value
```

> The format follows
> - To: Phone Number with Country Code included
> - From: This information would be the number you're using in Twilio, whether it be a short code or another number
> - Body: The actual message to send to the number you want
```
To=+11231234567&From=+1800123123123&Body=Testing
```

or in the case of our connector we can replace these values with variables from our connector payload and assign them using JSON selector notation.

![form-urlencoded post payload](images/custompayload.png?raw=true)

```
To={{$toPhoneNumber}}&From={{$fromPhoneNumber}}&Body={{$messageBody}}
```
> Note: Although CI360 mentions that it should be in JSON format for this section, **DO NOT** add brackets to the beginning and end as the **application/x-www-form-urlencoded** format Twilio expects does not expect that

Then using these variables in the connector, you can assign them based on the body of the connector payload to build your payload. 

![JSON Variable Example](images/json-variable-example.png?raw=true)

In the above screenshot, you can see that the **From Phone Number** is defined, in most cases you'll have a specific number you can hardcode instead so that each endpoint sends from a specific phone number. When multiple sender phone numbers are in use, these can be configured as dropdown list attributes in Custom Task type definition.

After setup we can use variables from within the task to setup SMS, with custom tasks or similar for use with Twilio. Depending on your use-case you could pull variable information from the event or the task itself to send data.