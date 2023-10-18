var lastItemId = 0;
var lastJson = {};
var version_text = "";
var tables = {};
var config = loadSettingsFromBrowserStorage();

let eventsConfigExample = [
  {
    buttonName: "OpenAIEvent",
    datahub_id: "",
    eventName: "external trigger event OpenAI",
    IMAGE_PROMPT: "",
    TEXT_PROMPT: "",
  },
  {
    buttonName: "Contact Event",
    datahub_id: "",
    eventName: "Contact Event",
    OfferDesc: "...",
    OfferName: "...",
  },
  {
    buttonName: "Send Email Event",
    subject_id: 372,
    email_recipient: "mathias.bouten@sas.com",
    email_subject: "360 has an offer for you!",
    email_content: "<!DOCTYPE html> <html></html>",
    eventName: "Trigger_Send_Email",
  },
];

var configExample = [
  {
    tenantName: "Demo Tenant 1",
    tenantApiGateway: "extapigwservice-demo.cidemo.sas.com",
    tenantId: "4069255a2b00055ad8c55d5a",
    tenantSecret: "MTAyNzEybWk5YWw4ZGdkMWFtNTBtbW45Nm5oxxxxxxxx",
    tenantCi360Url: "design-prod.cidemo.sas.com",
    tenantApiUser: "API-prdxxxx-mb",
    tenantApiSecret: "C3CJH4LTYBQFFIHxxxxxx",
    events: eventsConfigExample,
  },
  {
    tenantName: "Demo Tenant 2",
    tenantApiGateway: "extapigwservice-demo.cidemo.sas.com",
    tenantId: "4069255a2b00055ad8c55d5a",
    tenantSecret: "MTAyNzEybWk5YWw4ZGdkMWFtNTBtbW45Nm5oxxxxxxxx",
    tenantCi360Url: "design-prod.cidemo.sas.com",
    tenantApiUser: "API-prdxxxx-mb",
    tenantApiSecret: "C3CJH4LTYBQFFIHxxxxxx",
    events: eventsConfigExample,
  },
];

var style = loadStyleFromBrowserStorage();
var styleConfigExamle = {
  "tab-home": "Login",
  "tab-ext-api-ci360": "Events",
  "tab-datahub-api": "Tables",
  "tab-gdpr-api": "Profile",
  "tab-jobs-api": "Jobs",
  title: "SAS® CI360 API Helper",
  bgcolor: "#012036",
  btncolor: "#012036",
};

function customizeStyle() {
  for (var tab in style) {
    $("#" + tab).html(style[tab]);
  }

  if (style["title"]) $(".title").html(style["title"]);

  if (style.bgcolor != undefined) {
    console.log("applying styles...");

    /*$('.bg').css("background-color", style.bgcolor);
        $('.btn-primary').css("background-color", style.btncolor+" !important;");
        $('.btn-primary').css("border-color", style.btncolor+" !important;");
        $('.alert-primary').css("background-color", style.btncolor+" !important;");
        $('.alert-primary').css("border-color", style.btncolor+" !important;");
        $('a.nav-link').css("color", style.btncolor+" !important;");*/

    styleString =
      ".bg {background: " +
      style.bgcolor +
      " !important; }" +
      ".btn-primary { background-color: " +
      style.btncolor +
      " !important;" +
      "    border-color: " +
      style.btncolor +
      " !important; }" +
      ".alert-primary { background-color: " +
      style.btncolor +
      " !important;" +
      "    border-color: " +
      style.btncolor +
      " !important; }" +
      "a.nav-link { color: " +
      style.btncolor +
      "  }";

    var node = document.createElement("style");
    node.innerHTML = styleString;
    document.body.appendChild(node);
  }
}

/***  initializing the app
 ****/
function initializeApp(version) {
  version_text = version;
  $.ajaxSetup({ cache: false });
  // $.fn.selectpicker.Constructor.BootstrapVersion = '4';

  console.log("initApp");

  hideTabs();
  customizeStyle();

  initTabHome();
  initTabEventAPI();
  initTabDatahubAPI();

  $("#body").show();
}

function showTabs() {
  $("#tab-datahub-api").show();
  $("#tab-ext-api-ci360").show();
  $("#tab-gdpr-api").show();
  $("#tab-jobs-api").show();
}

function hideTabs() {
  $("#tab-ext-api-ci360").hide();
  $("#tab-datahub-api").hide();
  $("#tab-gdpr-api").hide();
  $("#tab-jobs-api").hide();
}

function initTabHome() {
  $("#ci360ApiHelperConfigDropDown").html("");
  if (jQuery.isEmptyObject(config) != true) {
    var option = "<option value='' > -- select your configuration -- </option>";
    config.forEach(function (element) {
      option +=
        '<option value="' +
        element.tenantId +
        '" >' +
        element.tenantName +
        "</option>";
    });
    $("#ci360ApiHelperConfigDropDown").html(option);
  } else {
    var option =
      "<option value='' > click on the tool icon on the right to add configs  >>>> </option>";
    $("#ci360ApiHelperConfigDropDown").html(option);
  }

  $("#password").keypress(function (e) {
    var keycode = e.keyCode ? e.keyCode : e.which;
    if (keycode == "13") {
      btnCreateToken(this);
      //alert('You pressed enter! - keypress');
    }
  });
  $("#ci360restUrl").val($("#ci360restUrlDropDown").val());
}

function initTabEventAPI() {}

function initTabDatahubAPI() {
  $(".selectpicker").select2({
    tags: true,
    width: "100%",
  });
}

function btnCreateToken() {
  $(".tenantDetails").hide();
  $(".loginwrong").hide();
  $("#btn_verifyLogin").hide();
  $("#imgLoad_verifyLogin").show();
  var token = createToken($("#password").val(), $("#username").val());
  if (token.includes("ERROR")) {
    $(".loginwrong").show();
    $("#imgLoad_verifyLogin").hide();
    $("#btn_verifyLogin").show();
  } else {
    $("#token").html(token);
    $(".tenantDetails").show();
    $("#btn_verifyLogin").show();
    $("#imgLoad_verifyLogin").hide();
    showTabs();
  }
  // SET EVENTS
}

function toggle(elementId) {
  var x = document.getElementById(elementId);
  if (x.style.display === "none") {
    x.style.display = "block";
  } else {
    x.style.display = "none";
  }
}

function createToken(secretText, tenantText) {
  var token = " ";
  if (secretText.length < 44) {
    token = "ERROR - Secret value is too short";
    console.log(token);
  } else if (tenantText.length < 24) {
    token = "ERROR - TenantID is not valid";
    console.log(token);
  } else {
    var header = { alg: "HS256", typ: "JWT" };
    var payload = { clientID: tenantText };
    token = KJUR.jws.JWS.sign(
      "HS256",
      JSON.stringify(header),
      JSON.stringify(payload),
      btoa(secretText)
    );
  }

  return token;
}

function setModal(title, body, footer, jsonObject) {
  $("#jsonOutput").hide();

  $("#modal_title").html(title);
  $("#modal_body").html(body);
  if (jsonObject != undefined) {
    $("#jsonOutput").val(JSON.stringify(jsonObject, null, 2));
    $("#jsonOutput").show();
  }

  $("#modal_footer").html(footer);
  $("#myModal").modal("show");
}

function updateModal(jsonObject) {
  console.log("updateModal: ", jsonObject);
  if (jsonObject != undefined) {
    $("#jsonOutput").val(JSON.stringify(jsonObject, null, 2));
    $("#jsonOutput").show();
  }
  $("#imgLoad_modal").hide();
}

function removeSpaces(element) {
  var val = $(element).val();
  var newval = val.replace(/\s/g, "");
  $(element).val(newval);
}

/** API functions **/

function callApi(url, parameters) {
  var headers = {
    Accept: "application/json",
    "X-Requested-With": "XMLHttpRequest",
  };
  if (parameters.token) {
    headers.Authorization = "Bearer " + parameters.token;
    delete parameters.token;
  }
  //console.log("headers: ", headers);
  return $.ajax(url, {
    type: "POST",
    contentType: "application/json",
    headers: headers,
    data: JSON.stringify(parameters),
  });
}

function callProxyAPI(settings, action) {
  settings.action = action;
  settings.email = $("#username").val();
  var settingsForProxy = {
    url: "./api/",
    method: "POST",
    headers: { "content-type": "application/json" },
    data: JSON.stringify(settings),
  };

  return $.ajax(settingsForProxy);
}

function btnManageConfigs() {
  if (config[0] == undefined) {
    config = [];
  }
  setModal(
    "Your configurations: ",
    '<div class="row"><div class="col-lg-6">' +
      '<textarea rows="20" id="jsonConfigString" class="form-control" value="">' +
      JSON.stringify(config, null, 4) +
      "</textarea>" +
      "</div>" +
      '<div class="col-lg-6"> Here is an example (<a href="javascript:copyConfigExample()">copy to left side</a>)<br>' +
      "<pre><code> " +
      JSON.stringify(configExample, null, 4) +
      "</code></pre></div>" +
      "</div>",
    '<button id="btn_updateConfig" type="button" class="btn btn-default btn-sm mybtn" onclick="updateConfig();" data-dismiss="modal">' +
      '  <span class="oi oi-circle-check"></span> &nbsp;Update Config</button>' +
      '<button type="button" class="btn btn-secondary btn-sm" data-dismiss="modal">Close</button>'
  );
}

function copyConfigExample() {
  $("#jsonConfigString").val(JSON.stringify(configExample, null, 4));
}

function updateConfig() {
  var jsonConfigString = $("#jsonConfigString").val();
  config = JSON.parse(jsonConfigString);
  localStorage.setItem("CI360ApiHelper", btoa(jsonConfigString));
  console.log(config);
  //** update dropdown list */
  $("#ci360ApiHelperConfigDropDown").html("");
  var option = "<option value='' > -- select your configuration -- </option>";
  config.forEach(function (element) {
    option +=
      '<option value="' +
      element.tenantId +
      '" >' +
      element.tenantName +
      "</option>";
  });
  $("#ci360ApiHelperConfigDropDown").html(option);
}

function updateStyleConfig() {
  var jsonConfigString = $("#jsonStyleString").val();
  if (jsonConfigString == "") jsonConfigString = "{}";
  style = JSON.parse(jsonConfigString);
  localStorage.setItem("CI360ApiHelperStyle", jsonConfigString);
  location.reload();
}

function updateEventsConfig() {
  let jsonConfigString = $("#jsonEventsString").val();
  if (jsonConfigString == "") jsonConfigString = "{}";
  events = JSON.parse(jsonConfigString);
  console.log("saved new events:", events);
  localStorage.setItem("CI360ApiHelperEvents", jsonConfigString);
  location.reload();
}

function addEvents(tenantID) {
  console.log("CALLED ADD EVENTS");
  let configuration = JSON.parse(atob(localStorage.getItem("CI360ApiHelper")));
  let events_list = [];
  configuration.forEach((item) => {
    if (item.tenantId == tenantID) events_list = item.events;
  });
  console.log("Events LIST: ");
  console.log(events_list);
  localStorage.setItem("currentEvents", btoa(JSON.stringify(events_list)));
  $("#eventsContainer").empty();
  if (events_list.length != 0)
    events_list.forEach((event_item, index) => addEvent(event_item, index));
}

function addEvent(event_item, index) {
  $("#eventsContainer").append(
    `<dd class="col-lg-2 col-md-4 col-sm-4 col-xs-12">` +
      ` <button type="button" class="btn btn-default btn-sm eventBtn" style="width: 100%;" onclick="renderEvent(` +
      index +
      `);">
            <span class="oi oi-cog"></span> &nbsp;` +
      event_item.buttonName +
      `
        </button>
    ` +
      `</dd>`
  );
}

function openAbout() {
  console.log("style: ", style);
  if (jQuery.isEmptyObject(style)) style = styleConfigExamle;
  // if (jQuery.isEmptyObject(events)) events = eventsConfigExample;

  setModal(
    '<img src="images/logo.png">',
    "This application has been developed by the GPCI <br><br>" +
      "If you have questions or comments please " +
      '<b><a href="mailto:rob.sneath@sas.com;mathias.bouten@sas.com?Subject=CI360APIHelper%20Question" target="_top">contact us via email</a></b>!' +
      "<br><br>If you want to change the look and feel click <b>" +
      '  <a href="#" onclick="$(\'.styleConfig\').show();">here</a></b> ' +
      '<br><div class="styleConfig" style="display:none">' +
      '  <textarea rows="10" id="jsonStyleString" class="form-control" value="">' +
      JSON.stringify(style, null, 4) +
      "</textarea>" +
      '  <br><button type="button" class="btn btn-default btn-sm" onclick="updateStyleConfig(this);">' +
      "    Save and Reload</button>" +
      '<span class="mr-auto">' +
      version_text +
      "</span>" +
      '<button type="button" class="btn btn-sm btn-secondary" data-dismiss="modal">Close</button>'
  );
}

/*
        "</div>" +
      "<br>If you want to add saved external events click <b>" +
      '  <a href="#" onclick="$(\'.eventConfig\').show();">here</a></b> ' +
      '<br><div class="eventConfig" style="display:none">' +
      '  <textarea rows="10" id="jsonEventsString" class="form-control" value="">' +
      JSON.stringify(events, null, 4) +
      "</textarea>" +
      '  <br><button type="button" class="btn btn-default btn-sm" onclick="updateEventsConfig(this);">' +
      "    Save and Reload</button>" +
      "</div>",
*/
/*** some helper functions */

function insertAfter(newNode, referenceNode) {
  referenceNode.parentNode.insertBefore(newNode, referenceNode.nextSibling);
}

function showLoaderIconAfterButtonClick(buttonElement) {
  $(buttonElement).hide();
  var loaderImageElement = document.createElement("img");
  loaderImageElement.src = "./images/ajax_loader_green_32.gif";

  // insert the loader image after the button, that is now hidden
  buttonElement.parentNode.insertBefore(
    loaderImageElement,
    buttonElement.nextSibling
  );
}

function hideLoaderIconAndShowButton(buttonElement) {
  loaderImageElement = buttonElement.nextSibling;
  loaderImageElement.remove();
  $(buttonElement).show();
}

function dropTableIfExist(tableid) {
  //console.log("drop table: " + tableid);
  if (tables[tableid]) {
    console.log("destroy: " + tableid);
    tables[tableid].destroy();
    $("#" + tableid).empty();
  }
  return tables[tableid];
}

/*
showLoaderIconAfterButtonClick (element);
hideLoaderIconAndShowButton (element);
*/

/**** browser storage functions */

function loadSettingsFromBrowserStorage() {
  if (
    localStorage.getItem("CI360ApiHelper") &&
    localStorage.getItem("CI360ApiHelper").includes("tenantName")
  ) {
    localStorage.setItem(
      "CI360ApiHelper",
      btoa(localStorage.getItem("CI360ApiHelper"))
    );
  }
  return JSON.parse(
    localStorage.getItem("CI360ApiHelper")
      ? atob(localStorage.getItem("CI360ApiHelper"))
      : "{}"
  );
}

function loadStyleFromBrowserStorage() {
  return JSON.parse(
    localStorage.getItem("CI360ApiHelperStyle")
      ? localStorage.getItem("CI360ApiHelperStyle")
      : "{}"
  );
}

// function loadEventsFromBrowserStorage() {
//   let evnts = localStorage.getItem("CI360ApiHelperEvents")
//     ? localStorage.getItem("CI360ApiHelperEvents")
//     : "[]";
//   console.log(evnts);
//   if (evnts == null) evnts = "[]";
//   console.log(evnts);
//   return JSON.parse(evnts);
// }

function onChangeCi360ApiHelperConfig(element) {
  var tenantId = $("#ci360ApiHelperConfigDropDown").val();
  config.forEach(function (element) {
    if (element.tenantId == tenantId) {
      $(".tenantApiGateway").val(element.tenantApiGateway);
      $(".tenantId").val(element.tenantId);
      $(".tenantSecret").val(element.tenantSecret);
      $(".tenantCi360Url").val(element.tenantCi360Url);
      $(".tenantApiUser").val(element.tenantApiUser);
      $(".tenantApiSecret").val(element.tenantApiSecret);
      addEvents(element.tenantId);
    }
  });
  hideTabs();
  $(".tenantDetails").hide();
  $(".loginwrong").hide();
  $("#token").html("");
}
