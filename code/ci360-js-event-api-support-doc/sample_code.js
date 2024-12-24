/*
Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

/**
 * This document will help to get real world implementation for below events.
 *
 * getSpotDefinitions   :- https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/p154ypyeyrtp6in1udhhlazu6oex.htm
 * getCreatives         :- https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/ing-eventsapiv2-spotcreat.htm
 */

//Standard way to define events
ci360("send", {
  eventName: "load",
  uri: location.href, //Dynamic Param
  pageTitle: document.title, //Dynamic Param
  referrer: document.referrer, //Dynamic Param
});

//Variable declaration
var timeout = 60000; // Retry timing
var params = {};
location.search
  .replace(/^.*\?/, "")
  .replace(/([^\=]+)\=([^\&]*)\&?/g, function (n, o, t) {
    return (params[o] = decodeURIComponent(t)), "";
  });

//Dynamic Param Configuration
let siteTarget = {
  domain: document.location.hostname,
  path: document.location.pathname,
  params: params,
}; // Dynamic Params

/**
 *  This method find out spots on a page and to ask the system what content should be injected.
 *  Use this information to retrieve the relevant task, spot, creative, and variant IDs and pass this information
 *  to events (such as impression events or impression viewable events).
 *
 */

/**
 * Assuming JS API 2 i already   included in webpage and ci360 is available
 * param 1: getSpotDefinitions  - Event name
 * param 2: siteTarget          - Configured above with dynamic site related information
 * param 3: callback function   - A callback function where we can do post processing with all spots info, once this method is called
 */

ci360("getSpotDefinitions", siteTarget, function (err, spots) {
  console.log(spots); //Logging all spots info to console for debug purpose

  spotsInfo = [];

  siteTarget.attributes = {};
  siteTarget.names = [];
  //Iterating over each spot
  spots.forEach((value, key) => {
    spotsInfo.push([key, value]);

    let attributes = {};
    value.attributes.forEach((attr) => {
      attributes[attr.name] = attr.value;
    });
    siteTarget.attributes[key] = attributes;
    siteTarget.names.push(key);
  });
  console.log(siteTarget); // Logging site related info for debug purpose

  /**
   * getCreatives call or the getPersonalizedCreative call will retrieve a creative
   * from SAS Customer Intelligence 360 and populate a spot with that content.
   *
   * Note: The getCreatives() function is asynchronous, so you must use a callback function to process the result.
   */

  /**
   * Assuming JS API 2 is already included in webpage and ci360 is available
   * param 1: getCreatives        - Event name
   * param 2: siteTarget          - Configured above with dynamic site related information
   * param 3: callback function   - A callback function where we can do post processing with all creatives info, once this method is called
   */
  ci360("getCreatives", siteTarget, function (err, creatives) {
    console.log(creatives); //Logging all creatives info to console for debug purpose

    var arr = 0;
    //Iterating over creatives
    creatives.forEach((value, key) => {
      spotsInfo[arr][0] == key && spotsInfo[arr].push(value);

      //Data structure for rendering the contents

      var content = spotsInfo[arr][2].content,
        selector = spotsInfo[arr][1].selector,
        spotId = spotsInfo[arr][1].id,
        spotName = spotsInfo[arr][0];

      //If selector is available means its web spot
      if (selector) {
        console.log("Web Spot");
        //If content does not include /FCID=-4 then its WebSpot
        if (!content.includes("/FCID=-4")) {
          console.log("Web Spot: Content Dectected.");
          //If spot detected and creative is available, fire events for tracking
          if (document.querySelector(selector)) {
            console.log("Web Spot: " + spotName + ": Spot Found.");
            //If spot is found send impression (spot change) events track when a creative is delivered by SAS Customer Intelligence 360
            ci360("send", {
              eventName: "impression",
              spotName: spotName,
            });
            console.log("Web Spot: " + spotName + ": Impression Fired.");
            document.querySelector(selector).parentNode.innerHTML = content;
            console.log("Web Spot: " + spotName + ": Content Loaded.");

            //Send Impression-viewable (spot viewable) events track when a creative is displayed in your content.
            ci360("send", {
              eventName: "impressionViewable",
              spotName: spotName,
            });
            console.log(
              "Web Spot: " + spotName + ": ImpressionViewable Fired."
            );
          } else {
            //If spot not detected add an observer to track if anything found.
            //If we find spot later but before timeout (configured above) deliver creative content

            console.log(
              "Web Spot: " + spotName + ": Spot Not Found and Searching..."
            );
            //No Spot found, still seraching add a mutation observer

            let checkURL = location.href;
            const spot_mobs = new MutationObserver((mutations, obs) => {
              const currentURL = location.href;
              //On url change stop the observer
              if (currentURL !== checkURL) {
                spot_mobs.disconnect();
                console.log("Web Spot: " + spotName + ": Spot Stop Searching.");
              } else {
                //If Spot is found deliver the  content
                var s = document.querySelector(selector);
                if (s) {
                  console.log("Web Spot: " + spotName + ": Spot Found.");
                  document.querySelector(selector).parentNode.innerHTML =
                    content;
                  return;
                }
              }
            });
            //Add an observer
            spot_mobs.observe(document, {
              childList: !0,
              subtree: !0,
            });
            //After timeout stop looking for spots
            setTimeout(function () {
              spot_mobs.disconnect();
              console.log(
                "Web Spot: " +
                  spotName +
                  ": Spot Stop Searching after " +
                  timeout +
                  "ms."
              );
            }, timeout);
          }
        } else {
          //No Web spot detected, no content to deliver
          console.log("Web Spot: No Content Detected.");
          var a = document.createElement("div");
          a.style.display = "none";
          (a.innerHTML = content), document.body.appendChild(a);
        }
      } else {
        //Its a JavaScript Spot
        console.log("JavaScript Spot");

        if ("<" != content.charAt(0)) {
          //If we detected < symbol at the start of content means its an creative so we send impression event
          console.log("JavaScript Spot: Content Detected.");
          ci360("send", {
            eventName: "impression",
            spotName: k,
          });
          try {
            (fx = new Function("return(function(){" + content + "})")()), fx();
            //Once we render content to spot fire and impressionViewable event
            ci360("send", {
              eventName: "impressionViewable",
              spotName: spotName,
            });
          } catch (e) {
            //In case of errors show specific errors or log the details
            console.log("JavaScript Spot: Content Error.");
          }
        } else {
          //If no content available, log details accordingly
          console.log("JavaScript Spot: No Content Detected.");
        }
      }
      arr++;
    });
  });
});
