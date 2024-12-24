# JavaScript Event API Support Document

`getSpotDefinitions` and `getCreative` JavaScript Event API Support Document

This Code snippet is helpful for SPAs (single page applications), whenever targeted spot becomes available too late with SPA (single page application), application looks for a spot/creative to deliver using SAS Customer Intelligence 360. This is similar to the old days when some content would be loaded onto a website at a very late stage.

For such scenarios we have JavaScript Event API `getSpotDefinitions`/`getCreatives`. 
This Code Snippet shows a custom script to verify the targeted spot location, if it didn't exist, this script would continue to search for it and stop only under certain conditions. 

## Description

This document will help to get real world implementation for below events in JavaScript Events API V2.

getSpotDefinitions :- https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/p154ypyeyrtp6in1udhhlazu6oex.htm

getCreatives :- https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/ing-eventsapiv2-spotcreat.htm

## Assumption

1.  SAS tag is Included already using `ot2.min.js` or `ot-api.min.js` as per tag instructions.
2.  Already know how events behave, and how to send an event.
3.  Already know building blocks of CI360 (like creatives, spots etc).
4.  Have basic knowledge of javaScript and its functionalities.

## Usage

This snippet will be used in SPA, whenever any change in dependency and it runs a lifecycle of component after everything is rendered run this snippet to get the updated content for `spots` and `creatives`.

In case of React (SPA) please add this code in any `useEffect` snippet and on change of any `dependency` will run this code snippet. 

Refer this code snippet that help to get real world implementation for events like `getSpotDefinitions` and `getCreative` in JavaScript Events API V2.

`getSpotDefinitions` This method find out spots on a page and to ask the system what content should be injected.

- Use this information to retrieve the relevant task, spot, creative, and variant IDs and pass this information to events (such as impression events or impression viewable events)

  ```
  ci360("getSpotDefinitions", {
   domain: "site domain",
   path: "site path",
   params: {
       param1_name: "URL param1 value",
       param2_name: "URL param2 value"
   },
   names:["<spot name>"]
  }, <callback>);

  ```

`getCreatives` getCreatives call or the getPersonalizedCreative call will retrieve a creative from SAS Customer Intelligence 360 and populate a spot with that content.

- Note: The getCreatives() function is asynchronous, so you must use a callback function to process the result.

```
ci360("getCreatives", {
    domain: "site domain",
    path: "site path",
    params: {
        param1_name: "URL param1 value",
        param2_name: "URL param2 value"
    },
    attributes:{
        "spot_name": {spot_attribute_name: "spot attribute value"}
    },
    names:["spot name"]
}, <callback>);

```

## Overview of code

This Code snippet has

- Standard way to define events
- Variables and dynamic parameters declaration
- ci360("getSpotDefinitions", {}) - Call a function
  - Iterate over each spot
  - Get all info about spots with specific details, call ci360("getCreatives", {})
    - Iterate over creatives
    - Get all info about content, selector and spot name
    - If selector is available means its web spot
      - If content does not contain `/FCID=-4` its `WebSpot`
      - Send `impression` and `impressionViewable` events for specific spot
    - If Selector not present its a JavaScript Spot
      - If we detect '<' symbol at the start of content means its a creative, Send `impression` and `impressionViewable` events for specific spot
