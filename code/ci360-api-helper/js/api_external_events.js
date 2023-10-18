function saveExternalEvent(element) {
  let endpoint = "ci360";
  let form_attributes = getAttributes(endpoint);
  console.log("collected attributes: ", form_attributes);
}

function sendExternalEvent(element) {
  var endpoint = "ci360";
  console.log("send event to: ", endpoint);
  $("#" + endpoint + "ResponseDiv").hide();
  showLoaderIconAfterButtonClick(element);
  //$('#' + endpoint + 'SendEventBtn').hide();
  //$('#' + endpoint + 'LoaderImage').show();
  var url =
    "https://" + $("#ci360restUrlDropDown").val() + "/marketingGateway/events";
  console.log(endpoint + " url: " + url);

  var input = {};
  input.token = $("#token").html();

  var mergedInputs = {};
  input = mergeObjects(input, getAttributes(endpoint));

  console.log("json object for external event: ", input);

  $("#" + endpoint + "response").val("");
  callApi(url, input)
    .error(function (response, error) {
      console.log("error response: ", response);
      $("#" + endpoint + "Response").val(
        JSON.stringify(response.responseJSON, null, 4)
      );
      if (error == "error") {
        $("#" + endpoint + "response").val(
          "Error occured - maybe connection refused - Hit F12 to see developers console"
        );
      } else {
        $("#" + endpoint + "response").val(response.responseJSON.message);
        console.log(response.responseJSON.message);
      }
      if (response.statusText == "error") {
        $("#" + endpoint + "Response").val(response.responseText);
        /*$('#'+endpoint+'Response').val("error - please open the javascript console to see details"
                +"\nStart Google Chrome in unsecure mode to avoid CORS (Access-Control-Allow-Origin) issues"
                +"\nUse following command to start Chrome:"
                +"\n   chrome.exe https://www.cidemo.sas.com/apihelper --disable-web-security --user-data-dir"
            );*/
      }
      $("#" + endpoint + "ResponseDiv").show();

      hideLoaderIconAndShowButton(element);
      //$('#' + endpoint + 'LoaderImage').hide();
      //$('#' + endpoint + 'SendEventBtn').show();
    })
    .success(function (response) {
      console.log("success response: ", response);
      $("#" + endpoint + "Response").val(JSON.stringify(response, null, 4));
      console.log(response);
      $("#" + endpoint + "ResponseDiv").show();

      hideLoaderIconAndShowButton(element);
      //$('#' + endpoint + 'LoaderImage').hide();
      //$('#' + endpoint + 'SendEventBtn').show();
    });
}

function addCi360Attribute(dataItem) {
  if (dataItem != undefined) {
    dataItem.endpoint = "ci360";
    $("#ci360EventAttributes").append(htmlTemplates.templateAttrItem(dataItem));
  } else {
    var data = {};
    data.endpoint = "ci360";
    data.eventName = "";
    data.eventValue = "";
    $("#ci360EventAttributes").append(htmlTemplates.templateAttrItem(data));
  }
}

function renderEvent(index) {
  let event_list = JSON.parse(atob(localStorage.getItem("currentEvents")));
  console.log("event list: ", event_list);
  let render_event = event_list[index];
  $("#ci360EventAttributes").html("");
  let data = {};
  data.endpoint = "ci360";
  for (const [key, value] of Object.entries(render_event)) {
    if (key === "buttonName") {
    } else if (key === "eventName") {
      $("#ci360EventName").val(value);
    } else if (key === "datahub_id") {
      console.log("datahub!");
      $("#ci360IDType option[value='subject_id']").attr("selected", false);
      $("#ci360IDType option[value='customer_id']").attr("selected", false);
      $("#ci360IDType option[value='login_id']").attr("selected", false);
      $("#ci360IDType option[value='datahub_id']").attr("selected", true);
      $("#ci360IDvalue").val(value);
    } else if (key === "subject_id") {
      console.log("subject!");
      $("#ci360IDType option[value='subject_id']").attr("selected", true);
      $("#ci360IDType option[value='customer_id']").attr("selected", false);
      $("#ci360IDType option[value='login_id']").attr("selected", false);
      $("#ci360IDType option[value='datahub_id']").attr("selected", false);
      $("#ci360IDvalue").val(value);
    } else if (key === "customer_id") {
      console.log("customer!");
      $("#ci360IDType option[value='subject_id']").attr("selected", false);
      $("#ci360IDType option[value='customer_id']").attr("selected", true);
      $("#ci360IDType option[value='login_id']").attr("selected", false);
      $("#ci360IDType option[value='datahub_id']").attr("selected", false);
      $("#ci360IDvalue").val(value);
    } else if (key === "login_id") {
      console.log("login!");
      $("#ci360IDType option[value='subject_id']").attr("selected", false);
      $("#ci360IDType option[value='customer_id']").attr("selected", false);
      $("#ci360IDType option[value='login_id']").attr("selected", true);
      $("#ci360IDType option[value='datahub_id']").attr("selected", false);
      $("#ci360IDvalue").val(value);
    } else {
      data.eventName = key;
      data.eventValue = value;
      addCi360Attribute(data);
    }
  }
}

function addContactEvent() {
  $("#ci360EventAttributes").html("");
  var data = {};
  data.endpoint = "ci360";
  data.eventName = "ContactChannel";
  data.eventValue = "Web";
  addCi360Attribute(data);
  data.eventName = "ContactType";
  data.eventValue = "retargeting";
  addCi360Attribute(data);
  data.eventName = "OfferCode";
  data.eventValue = "oc05";
  addCi360Attribute(data);
  data.eventName = "OfferName";
  data.eventValue = "golf-products";
  addCi360Attribute(data);
  data.eventName = "OfferImage";
  data.eventValue = "https://c.neh.tw/thumb/f/720/0daa94dcf2494fc4a5e6.jpg";
  addCi360Attribute(data);
  data.eventName = "OfferDesc";
  data.eventValue = "Shop now with 10% discount";
  addCi360Attribute(data);
  $("#ci360EventName").val("Contact Event");
}

function addSendEmailEvent() {
  $("#ci360EventAttributes").html("");
  var data = {};
  data.endpoint = "ci360";
  data.eventName = "email_recipient";
  data.eventValue = "mathias.bouten@sas.com";
  addCi360Attribute(data);
  data.eventName = "email_subject";
  data.eventValue = "360 has an offer for you!";
  addCi360Attribute(data);
  data.eventName = "email_content";
  data.eventValue = "<!DOCTYPE html> <html></html>";
  addCi360Attribute(data);
  $("#ci360EventName").val("Trigger_Send_Email");
}

function removeAttr(elem) {
  console.log(elem.parentElement);
  elem.parentElement.parentElement.remove();
}

function getAttributes(endpoint) {
  var attrNames = $("." + endpoint + "attrName");
  var attrValues = $("." + endpoint + "attrValue");
  var attributes = {};
  for (i = 0; i < attrNames.length; i++) {
    attributes[attrNames[i].value] = attrValues[i].value;
  }
  return attributes;
}

function mergeObjects(obj1, obj2) {
  var result = {};
  for (var key in obj1) result[key] = obj1[key];
  for (var key in obj2) result[key] = obj2[key];
  return result;
}

function btnGetExternalEvents() {
  $("#imgLoad_getexternalevents").show();
  $("#btn_GetExternalEvents").hide();
  var api_user = $("#api_user").val();
  var api_secret = $("#api_user_secret").val();
  var designUrl =
    "https://" +
    $("#ci360DesignCenterUrl").val() +
    "/SASWebMarketingMid/rest/events";
  console.log("design url: ", designUrl);

  var settings = {
    url: designUrl,
    method: "GET",
    headers: {
      contentType: "application/json",
      authorization: "Basic " + btoa(api_user + ":" + api_secret),
    },
  };

  callProxyAPI(settings, "designServerCall").done(function (response) {
    var option = "<option value='' > --- Select Event --- </option> ";
    var i = 0;
    response.json.items.sort(function (a, b) {
      if (a.name.toLowerCase() < b.name.toLowerCase()) return -1;
      if (a.name.toLowerCase() > b.name.toLowerCase()) return 1;
      return 0;
    });

    response.json.items.forEach(function (element) {
      i++;
      if (element.type == "external") {
        option +=
          '<option value="' + element.id + '" >' + element.name + "</option>";
      }
    });
    $("#ci360ExternalEvents").html(option);
    $("#ci360ExternalEventsDropDown").show();
    $("#imgLoad_getexternalevents").hide();
    $("#btn_GetExternalEvents").show();
  });
}

function getExternalEventAttributes(element) {
  var eventid = $("#ci360ExternalEvents").val();
  var eventname = $("#ci360ExternalEvents option:selected").text();
  var api_user = $("#api_user").val();
  var api_secret = $("#api_user_secret").val();
  var designUrl =
    "https://" +
    $("#ci360DesignCenterUrl").val() +
    "/SASWebMarketingMid/rest/events/" +
    eventid;
  console.log("design url: ", designUrl);

  var settings = {
    url: designUrl,
    method: "GET",
    headers: {
      contentType: "application/json",
      authorization: "Basic " + btoa(api_user + ":" + api_secret),
    },
  };
  //callProxyAPI(settings, "designServerCall").done(function (response) {
  //console.log("getExternalEventAttributes response: ",response);
  $("#ci360EventAttributes").html("");
  //addCi360Attribute({"eventName":"eventName", "eventValue":eventname});
  $("#ci360EventName").val(eventname);
  //addCi360Attribute({"eventName":"subject_id", "eventValue":"372"});
  //});
}
