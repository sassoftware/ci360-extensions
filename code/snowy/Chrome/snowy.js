/**
    Developed by Sudheesh Warrier
    Contact for any issues: Sudheesh.Warrier+Snowy@sas.com
 */
var IsExtension = false;

var DIAG_VERSION = "diag10(2304)";
var SDK_VERSION = "sdk11(2304)";

var LookupSDK_version_from_server = true;
var token = null;

const NO_TAG_FOUND_MSG =
  "Yet to detect a 360 tag on this page, please wait a few seconds.<br/>If the message persists, please check the page and ensure the tag is configured correctly and that the domain is approved in CI 360.";
const NOT_DIAG_AP_MSG =
  "This is not a Diagnostics Access Point. Do you want to activate?";
const NOT_DIAG_AP_SAVE_MSG =
  "This is not a Diagnostics Access Point. Do you want to save and activate?";
const UNABLE_TO_AP_CONFIG_MSG =
  "Unable to get CI 360 Access Point configurations. Please check the External gateway address, Tenant Id and Client Secret.";
const ERROR_CONNECTING_AP_MSG =
  "Error connecting to CI 360 Access Point. Please check the External gateway address, Tenant Id and Client Secret.";
const NO_ACTIVE_AP_MSG =
  "You have not activated any Access Point yet. Please configure and activate an Access Point.";
const UNDOCKED_DEVTOOL_MSG =
  "<li>If you un-docked the dev tools to a separate window and/or using Firefox, Script information will not be available. You need to check the console log to see the same.</li>";
const IDENTITY_INFO_NOT_VALID_MSG =
  "Please check the identity type and identity values and try again.";
const NO_TAG_LOADED_MSG =
  "<li>No CI 360 related scripts are loaded yet. Please ensure that the site is tagged.This message is refreshed only when the page is first loaded.</li>";
const DOMAIN_NOT_APPROVED_MSG =
  "<li class='alert alert-warning'>The domain is either not approved or not activated in CI 360.</li>";
const COLS_WITH_META = ["task_id"];
var META_LIST = {};

$(document).ready(function () {
  if (chrome && chrome.storage && chrome.storage.sync) IsExtension = true;
  log("Snowy is waking up.");

  //keep the network tab active
  $(".settings-main").hide();
  $(".network-stream-main").show();
  $(".event-stream-main").hide();
  $(".debugging-main").hide();

  checkLatestVersion();

  initSettingsPage();

  initDebuggingPage();
  addFunctionsforDataTables();

  initEventStreamTable();

  initNetWorkTable();
});

/** common functions */
var console_log_enabled = true;
var ci360SavedTenantList = null;
var current_tenant_gateway = null;
var popup_context = null;
var auto_add_360_tag = false;
var current_360_tag = null;
var ci360SavedTagList = null;
var IsTagAdded = false;
//var display_metadata_from_360 = false;

function checkLatestVersion() {
  var settings = {
    url:
      "https://www.cidemo.sas.com/tools/snowy/version-remote.json?dt=" +
      Date.now(),
    method: "GET",
    timeout: 0,
    headers: {
      "Access-Control-Allow-Origin": "www.cidemo.sas.com",
    },
  };

  $.ajax(settings)
    .done(function (response) {
      log(response);
      if (
        response &&
        response.latest_version &&
        response.message &&
        chrome &&
        chrome.runtime &&
        chrome.runtime.getManifest() &&
        chrome.runtime.getManifest().version
      ) {
        var version_local = parseFloat(chrome.runtime.getManifest().version);
        var latest_version = parseFloat(response.latest_version);
        if (latest_version > version_local) {
          $(".status-version").html(response.message);
          $("#status_version").html(
            "<p style='padding:.5em'>" + response.message + "</p>"
          );
          $(".status-version").addClass("status-version-active");
          setTimeout(function () {
            $(".status-version").removeClass("status-version-active");
          }, 15000);
        }

        if (LookupSDK_version_from_server === true) {
          DIAG_VERSION = response.DIAG_VERSION;
          SDK_VERSION = response.SDK_VERSION;
        }
      }
    })
    .fail(function (error) {
      log("Unable to connect to server to check for latest version of Snowy.");
    });
}

function addEventsToControls() {
  //add events to buttons in settings page. For event streams.
  $("#settings_activate_selected_tenant").on(
    "click",
    settings_activate_selected_tenant
  );
  $("#settings_forget_selected_tenant").on(
    "click",
    settings_forget_selected_tenant
  );
  $("#settings_save_activate").on("click", settings_save_activate);
  $("#settings_without_save_activate").on(
    "click",
    settings_without_save_activate
  );
  $("#settings_clear").on("click", settings_clear);

  //add events to buttons in settings page. For tags.
  $("#settings_tag_activate_selected_tenant").on(
    "click",
    settings_tag_activate_selected_tenant
  );
  $("#settings_tag_forget_selected_tenant").on(
    "click",
    settings_tag_forget_selected_tenant
  );
  $("#settings_tag_save_activate").on("click", settings_tag_save_activate);
  $("#settings_tag_without_save_activate").on(
    "click",
    settings_tag_without_save_activate
  );
  $("#settings_tag_clear").on("click", settings_tag_clear);

  //add events to the menu links
  $("#network_stream").click(function () {
    setActivePage(
      ".network-stream-main",
      "#network_stream",
      "Network",
      "w3-text-white"
    );
  });
  $("#event_stream").click(function () {
    setActivePage(
      ".event-stream-main",
      "#event_stream",
      "Event Stream",
      "w3-text-white"
    );
    var table = $("#eventStreamTable").DataTable();
    table.columns.adjust().draw();
  });
  $("#settings").click(function () {
    setActivePage(".settings-main", "#settings", "Settings", "");
  });
  $("#debugging").click(function () {
    setActivePage(".debugging-main", "#debugging", "Debugging", "");
  });

  $("input").on("input", function () {
    $(this).removeClass("border_red");
  });
  $("select").on("change", function () {
    $(this).removeClass("border_red");
  });

  //add event to display metadata check box

  $("#display_metadata_from_360").on("change", function () {
    if (this.checked) {
      log(
        "For future rows added to the table, will attempt to fetch metadata from 360."
      );
      if (IsExtension === true)
        chrome.storage.sync.set(
          { display_metadata_from_360_prop: true },
          function () {}
        );
      display_metadata_from_360 = true;
    } else {
      log(
        "For future rows added to the table, will not attempt to fetch metadata from 360."
      );
      if (IsExtension === true)
        chrome.storage.sync.set(
          { display_metadata_from_360_prop: false },
          function () {}
        );
      display_metadata_from_360 = false;
    }
  });

  //add event to display console check box

  $("#display_console").on("change", function () {
    if (this.checked) {
      log("Displaying Console.");
      $("#eventStreamConsole").show();
      if (IsExtension === true)
        chrome.storage.sync.set({ display_console_prop: true }, function () {});
    } else {
      log("Hiding console.");
      $("#eventStreamConsole").hide();
      if (IsExtension === true)
        chrome.storage.sync.set(
          { display_console_prop: false },
          function () {}
        );
    }
  });

  //add event to display gateway info check box

  $("#display_gateway_eventstream").on("change", function () {
    if (this.checked) {
      if (current_tenant_gateway != null) {
        log("Displaying gateway information on event stream page.");
        $("#gateway_info").show();
      }

      if (IsExtension === true)
        chrome.storage.sync.set(
          { display_gateway_eventstream_prop: true },
          function () {}
        );
    } else {
      log("Hiding gateway information on event stream page.");
      $("#gateway_info").hide();
      if (IsExtension === true)
        chrome.storage.sync.set(
          { display_gateway_eventstream_prop: false },
          function () {}
        );
    }
  });
}

function initPopup() {
  //set up modal popup
  // Get the modal
  var modal = document.getElementById("id01");

  // When the user clicks anywhere outside of the modal, close it
  window.onclick = function (event) {
    if (event.target == modal) {
      modal.style.display = "none";
      log("Close the popup.");
    }
  };
  $("#close_modal").on("click", function () {
    document.getElementById("id01").style.display = "none";
    log("Close the popup");
  });

  $("#popup_btn_1").on("click", function () {
    log("Clicked first button in the popup.");
    document.getElementById("id01").style.display = "none";
    log("Close the popup");
    popup_btn_1_click();
  });

  $("#popup_btn_2").on("click", function () {
    log("Clicked second button in the popup.");
    document.getElementById("id01").style.display = "none";
    log("Close the popup");
    popup_btn_2_click();
  });

  //popup 2 - For Filter for Diag agent.

  $("#close_modal02").on("click", function () {
    document.getElementById("id02").style.display = "none";
    log("Close the popup");
  });

  $("#popup_btn_102").on("click", function () {
    log("Clicked first button in the popup.");
    document.getElementById("id01").style.display = "none";
    log("Close the popup");
    popup_filters_yes_click();
  });

  $("#popup_btn_202").on("click", function () {
    log("Clicked second button in the popup.");
    document.getElementById("id02").style.display = "none";
    log("Close the popup");
    popup_filters_no_click();
  });
}
function loadPreference() {
  //Load the preference for metadata visibility.
  if (IsExtension === true)
    chrome.storage.sync.get("display_metadata_from_360_prop", function (data) {
      if (data.display_metadata_from_360_prop === true) {
        log(
          "For future rows added to the table, will attempt to fetch metadata from 360."
        );
        $("#display_metadata_from_360").prop("checked", true);
        display_metadata_from_360 = true;
      } else {
        log("Hiding console.");
        $("#display_metadata_from_360").prop("checked", false);
        display_metadata_from_360 = false;
      }
    });

  //Load the preference for console visibility.
  if (IsExtension === true)
    chrome.storage.sync.get("display_console_prop", function (data) {
      if (data.display_console_prop === true) {
        log("Displaying Console.");
        $("#display_console").prop("checked", true);
        $("#eventStreamConsole").show();
      } else {
        log("Hiding console.");
        $("#display_console").prop("checked", false);
        $("#eventStreamConsole").hide();
      }
    });

  //Load the preference for gateway information visibility on event stream page.
  if (IsExtension === true)
    chrome.storage.sync.get(
      "display_gateway_eventstream_prop",
      function (data) {
        if (data.display_gateway_eventstream_prop === true) {
          log("Displaying gateway information on event stream page.");
          $("#display_gateway_eventstream").prop("checked", true);
          if (current_tenant_gateway != null) $("#gateway_info").show();
        } else {
          log("Hiding gateway information on event stream page.");
          $("#display_gateway_eventstream").prop("checked", false);
          $("#gateway_info").hide();
        }
      }
    );
  //check if 360 tag add option is enabled or not.
  if (IsExtension === true)
    chrome.storage.sync.get("auto_add_360_tag_prop", function (data) {
      if (data.auto_add_360_tag_prop === true) {
        auto_add_360_tag = true;
      } else {
        auto_add_360_tag = false;
      }
    });
}

function setActivePage(
  active_page_class,
  selected_menu_id,
  selected_page_header,
  selected_page_class
) {
  $(".settings-main").hide();
  $(".network-stream-main").hide();
  $(".event-stream-main").hide();
  $(".debugging-main").hide();

  $("#network_stream").removeClass("header-menu-item-selected");
  $("#event_stream").removeClass("header-menu-item-selected");
  $("#settings").removeClass("header-menu-item-selected");
  $("#debugging").removeClass("header-menu-item-selected");
  $(selected_menu_id).addClass("header-menu-item-selected");
  $("#selected_page_header").removeClass();
  $("#selected_page_header").addClass(selected_page_class);
  $("#selected_page_header").addClass("w3-large");
  $("#selected_page_header").html(selected_page_header);

  $(active_page_class).show();
  log("Changing the active page.");
}
function log(msg) {
  if (console_log_enabled) {
    console.log(msg);
    if (typeof msg === "object") msg = JSON.stringify(msg);
    $("#eventStreamConsole").prepend(
      "<span style='color:silver'> >> </span>" + msg + "<br />"
    );
  }
}
function uuidv4() {
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, function (c) {
    var r = (Math.random() * 16) | 0,
      v = c == "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}
function makeToken(tenantId, clientSecret) {
  var header = { alg: "HS256", typ: "JWT" };
  var payload = { clientID: tenantId };
  token = KJUR.jws.JWS.sign(
    "HS256",
    JSON.stringify(header),
    JSON.stringify(payload),
    btoa(clientSecret)
  );
  return token;
}

function show_msg(msg, show_popup_btn) {
  if (msg && msg.data && msg.data.param) {
    $("#popup_footer_spacer").show();
    $("#popup_btn_section").hide();
    $("#modal_msg").html(
      "<p class='body'>" + format({ event_json: msg.data.param }, true) + "</p>"
    );
  } else {
    if (msg && msg.data && msg.data.error) msg = msg.data.error;
    msg = "<p>" + msg + "</p>";
    $("#modal_msg").html(msg);
    if (show_popup_btn === true) {
      $("#popup_btn_section").show();
      $("#popup_footer_spacer").hide();
    } else {
      $("#popup_footer_spacer").show();
      $("#popup_btn_section").hide();
    }
  }
  document.getElementById("id01").style.display = "block";
  log("Displaying the popup.");
}

function format(d, IsMeta) {
  // `d` is the original data object for the row
  if (d && IsMeta) {
    d = JSON.stringify(d);
    d = JSON.parse(d);
  }

  var data = d.event_json;

  if (d.attributes) data = d.attributes;
  var raw = "";
  if (d.event_json && IsMeta) {
    raw = JSON.stringify(d.event_json);
  }
  if (IsMeta && data.customProperties) {
    data.customProperties = "Check the JSON data.";
  }

  table = syntaxHighlight(data, IsMeta ? raw : d.event_json, IsMeta);
  table = table.replaceAll("</td>,", "</td>");
  if (IsMeta)
    return (
      '<table cellpadding="0" cellspacing="0" border="0" style="width: 100%">' +
      "<tr>" +
      '<td><div class="body" style="overflow:auto">' +
      table +
      "</div></td>" +
      "</tr>" +
      "</table>"
    ); //syntaxHighlight(data,d.event_json)
  return (
    '<table cellpadding="0" cellspacing="0" border="0" style="width: 100%">' +
    "<tr>" +
    '<td><div style="overflow:auto">' +
    table +
    "</div></td>" +
    "</tr>" +
    "</table>"
  ); //syntaxHighlight(data,d.event_json)
}
function syntaxHighlight(json, raw, IsMeta) {
  if (typeof json != "string") {
    json = JSON.stringify(json, undefined, 2);
  }
  if (typeof raw != "string") {
    raw = JSON.stringify(raw, undefined, 2);
  }
  var counter = 0;
  //json = json.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  //raw = raw.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

  var next_col_need_meta = "";

  var str = json.replace(
    /("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g,
    function (match) {
      var cls = "number";
      if (/^"/.test(match)) {
        if (/:$/.test(match)) {
          cls = "key";
        } else {
          cls = "string";
        }
      } else if (/true|false/.test(match)) {
        cls = "boolean";
      } else if (/null/.test(match)) {
        cls = "null";
      }

      if (cls == "key") match = match.substring(1, match.length - 2);
      else if (cls != "null") match = match.substring(1, match.length - 1);

      if (cls == "key") {
        if (counter % 4 === 0)
          val = '<tr> <td class="' + cls + '">' + match + "</td>";
        else val = '<td class="' + cls + '">' + match + "</td>";
      } else {
        temp_val = htmlCodes(match, false);
        len = temp_val.length;
        if (len > 100)
          temp_val = "<textarea readonly>" + temp_val + "</textarea>";

        if (IsValuePresent(next_col_need_meta) && IsMeta != true) {
          if (
            current_tenant_gateway != undefined &&
            IsValuePresent(current_tenant_gateway.token) &&
            ((current_tenant_gateway.display_metadata_from_360 === true &&
              current_tenant_gateway.display_metadata_from_360 != undefined) ||
              current_tenant_gateway.display_metadata_from_360 === undefined)
          ) {
            var temp_id = uuidv4();
            temp_val =
              temp_val +
              "<span id='" +
              temp_id +
              "' class='meta-link-txt w3-text-amber' data-meta-type='" +
              next_col_need_meta +
              "' data-meta-id='" +
              match +
              "'>(Please wait...)</span>";
            getMeta(next_col_need_meta, match, temp_id);
          }
        }

        if (counter % 4 === 0) {
          val = '<td class="' + cls + '">' + temp_val + "</td></tr>";
        } else val = '<td class="' + cls + '">' + temp_val + "</td>";
      }

      counter = counter + 1;
      if (cls == "key" && COLS_WITH_META.includes(match))
        next_col_need_meta = match;
      else next_col_need_meta = "";
      return val;
    }
  );
  str = str.substring(1, str.length - 1);

  str =
    str +
    "<tr><td colspan=4 class='w3-text-khaki w3-tiny'><div style='width:100%; '>" +
    htmlCodes(raw, false) +
    "</div></td></tr>";

  return (
    "<table border=0 style='width:100%' cellpadding='0' cellspacing='1' >" +
    str +
    "</table>"
  );
}
function getMeta(meta_type, meta_id, html_id) {
  if (
    meta_id in META_LIST &&
    META_LIST[meta_id] &&
    META_LIST[meta_id] != undefined &&
    META_LIST[meta_id].length > 0
  ) {
    setTimeout(function () {
      handle_meta_response(
        JSON.parse(META_LIST[meta_id]),
        meta_type,
        meta_id,
        html_id
      );
    }, 1000);

    return;
  }

  //TODO: when APIs for Spot and Creatives are available, use that and comment below two lines...
  if (meta_type !== "task_id") return;

  var url = "https://" + current_tenant_gateway.tenantUrl + "/marketingDesign";

  if (meta_type === "task_id") url = url + "/tasks";
  url = url + "/" + meta_id;
  var settings = {
    url: url,
    method: "GET",
    timeout: 0,
    headers: {
      Authorization: "Bearer " + current_tenant_gateway.token,
    },
  };

  $.ajax(settings)
    .done(function (response) {
      handle_meta_response(response, meta_type, meta_id, html_id);
      META_LIST[meta_id] = JSON.stringify(response);
    })
    .fail(function (error) {
      log(
        "Error connecting to CI 360 Access Point. Please check the External gateway address, Tenant Id and Client Secret."
      );
      log(error);
      var tmp_msg = "";
      if (
        error != undefined &&
        error.responseJSON != undefined &&
        error.responseJSON.message != undefined
      )
        tmp_msg = error.responseJSON.message;
      log(tmp_msg);
      $("#" + html_id).html("Unable to get metadata.");
      $("#" + html_id).bind(
        "click",
        {
          error: error
            ? JSON.stringify(error)
            : "Unable to get metadata from tenant.",
        },
        show_msg
      );
    });
}
function handle_meta_response(response, meta_type, meta_id, html_id) {
  var display_text = "";
  if (meta_type === "task_id") {
    display_text = "(Task Name: " + response.name;
  }
  display_text = display_text + ". Click here for more details.)";
  $("#" + html_id).html(display_text);
  $("#" + html_id).bind("click", { param: response }, show_msg);
}
/** end of common functions */

/** Settings page related JS functions */

function initSettingsPage() {
  var acc = document.getElementsByClassName("accordion");
  var i;

  for (i = 0; i < acc.length; i++) {
    acc[i].addEventListener("click", function () {
      this.classList.toggle("active");
      var panel = this.nextElementSibling;
      if (panel.classList.contains("panel-active")) {
        //panel.style.display = "none";
        panel.classList.remove("panel-active");
      } else {
        //panel.style.display = "block";
        panel.classList.add("panel-active");
      }
    });
  }

  loadCi360SavedTagList();

  addEventsToControls();

  initPopup();

  loadPreference();

  loadCi360SavedTenantList();
}

function popup_btn_1_click() {
  if (null != popup_context && undefined != popup_context) {
    saveAndActivate(
      popup_context.response,
      popup_context.tenant_gateway,
      popup_context.save
    );
    popup_context = null;
  }
}
function popup_btn_2_click() {
  if (null != popup_context && undefined != popup_context) {
    popup_context = null;
  }
}
function activate_gateway(gateway) {
  log("Setting this tenant as the active tenant.");
  current_tenant_gateway = gateway;
  update_pull_down();
  show_msg(
    "New gateway configuration is activated. You might want to stop and start the data collection under Event Stream tab."
  );
  $(".status-feedback").removeClass("status-feedback-active");
}
function update_pull_down() {
  if (current_tenant_gateway != null) {
    log("Updating the Gateway information in the pull-down menu.");
    $("#lblActiveTenantName").html(current_tenant_gateway.tenantName);
    $("#lblActiveUrl").html(current_tenant_gateway.tenantUrl);
    $("#lblActiveId").html(current_tenant_gateway.tenantId);
    if (IsExtension === true)
      chrome.storage.sync.get(
        "display_gateway_eventstream_prop",
        function (data) {
          if (data.display_gateway_eventstream_prop === true) {
            log("Displaying gateway information on event stream page.");
            $("#gateway_info").show();
          } else {
            log("Hiding gateway information on event stream page.");
            $("#gateway_info").hide();
          }
        }
      );
  }
}
function settings_activate_selected_tenant() {
  var temp_tenantId = $("#ci360SavedTenantList :selected").val();
  gateway =
    ci360SavedTenantList.tenants[
      ci360SavedTenantList.tenants.findIndex((a) => a.id === temp_tenantId)
    ];
  if (gateway.type.toLowerCase() == "diag") {
    //it is safe to connect to this agent
    log("Diagnostics Access Point detected.");
    activate_gateway(gateway);
  } else {
    //we need to first warn the user that this is not a diagnostics agent and ask for confirmation.
    popup_context = { response: null, tenant_gateway: gateway, save: false };
    msg = NOT_DIAG_AP_MSG;
    show_msg(msg, true);
    return;
  }
}
function settings_forget_selected_tenant() {
  var temp_tenantId = $("#ci360SavedTenantList :selected").val();
  ci360SavedTenantList.tenants.splice(
    ci360SavedTenantList.tenants.findIndex((a) => a.id === temp_tenantId),
    1
  );
  if (IsExtension === true)
    chrome.storage.sync.set(
      { ci360SavedTenantList_prop: ci360SavedTenantList },
      function () {
        log("Removed the gateway configuration.");
        log("Total number of saved tenants:");
        log(ci360SavedTenantList.tenants.length);
      }
    );

  loadCi360SavedTenantList();
}
function settings_save_activate() {
  validate_and_Activate(true);
}
function validate_and_Activate(save) {
  var valid = true;
  $(".event-stream-settings input").removeClass("border_red");
  var tenantUrl = $("#ci360restUrlDropDown :selected").val();
  var tenantId = $("#ci360TenantIdTxt").val().trim();
  var tenantSecret = $("#ci360TenantSecretTxt").val().trim();
  var tenantName = $("#ci360TenantNameTxt").val().trim();
  var display_metadata_from_360 = $("#display_metadata_from_360").is(
    ":checked"
  );

  if ($("#ci360restUrlTxt").val().trim().length > 0) {
    tenantUrl = $("#ci360restUrlTxt").val().trim();
    if (tenantUrl.includes(":") || tenantUrl.includes(" ")) {
      $("#ci360restUrlTxt").addClass("border_red");
      tenantUrl = "";
      valid = false;
    } else {
      log("Tenant url: " + tenantUrl);
    }
  } else {
    log("Tenant url: " + tenantUrl);
  }

  if (tenantId.length > 0) {
    log("Tenant Id: " + tenantId);
  } else {
    $("#ci360TenantIdTxt").addClass("border_red");
    valid = false;
    tenantId = "";
  }

  if (tenantSecret.length > 0) {
    log("Tenant Secret is set.");
  } else {
    $("#ci360TenantSecretTxt").addClass("border_red");
    valid = false;
    tenantSecret = "";
  }

  if (tenantName.length > 0) {
    log("Friendly Name: " + tenantName);
  } else {
    $("#ci360TenantNameTxt").addClass("border_red");
    valid = false;
    tenantName = "";
  }

  if (valid) {
    var uuid = uuidv4();
    if (ci360SavedTenantList === null || ci360SavedTenantList === undefined) {
      ci360SavedTenantList = { tenants: [] };
    }

    tenant_gateway = {
      id: uuid,
      tenantUrl: tenantUrl,
      tenantId: tenantId,
      tenantSecret: tenantSecret,
      tenantName: tenantName,
      display_metadata_from_360: display_metadata_from_360,
    };
    log("Received basic gateway details.");
    validateGatewayDetailsAndActivate(tenant_gateway, save);
  } else {
    $(".status-feedback").addClass("status-feedback-active");
    log("Configuration is not valid. Please review the details provided.");
  }
}
function settings_without_save_activate() {
  validate_and_Activate(false);
}
function settings_clear() {
  log("Clearing all values from the form.");
  $("#ci360restUrlDropDown")[0].selectedIndex = 0;
  $("#ci360restUrlTxt").val("");
  $("#ci360TenantIdTxt").val("");
  $("#ci360TenantSecretTxt").val("");
  $("#ci360TenantNameTxt").val("");
}

function loadCi360SavedTenantList() {
  $("#settings_activate_selected_tenant").prop("disabled", true);
  $("#settings_forget_selected_tenant").prop("disabled", true);
  if (IsExtension === true)
    chrome.storage.sync.get("ci360SavedTenantList_prop", function (data) {
      $("#ci360SavedTenantList").html("");
      ci360SavedTenantList = data.ci360SavedTenantList_prop;
      if (
        ci360SavedTenantList === undefined ||
        ci360SavedTenantList.tenants.length < 1
      ) {
        log("There are no saved tenants.");
        return;
      }
      log(
        "There are " +
          ci360SavedTenantList.tenants.length +
          " saved gateway configurations."
      );

      for (const tenant of ci360SavedTenantList.tenants) {
        log("Tenant Id: " + tenant.id);
        log("Tenant Name: " + tenant.tenantName);
        log("Tenant Url: " + tenant.tenantUrl);
        $("#ci360SavedTenantList").append(
          $("<option>", {
            value: tenant.id,
            text: tenant.tenantName,
          })
        );
      }
      $("#settings_activate_selected_tenant").prop("disabled", false);
      $("#settings_forget_selected_tenant").prop("disabled", false);
    });
}

function validateGatewayDetailsAndActivate(tenant_gateway, save) {
  token = makeToken(tenant_gateway.tenantId, tenant_gateway.tenantSecret);
  log("Token generated.");

  var settings = {
    url:
      "https://" + tenant_gateway.tenantUrl + "/marketingGateway/configuration",
    method: "GET",
    timeout: 0,
    headers: {
      Authorization: "Bearer " + token,
    },
  };

  $.ajax(settings)
    .done(function (response) {
      if (response && response.agentName && response.type) {
        log("Access Point Name: " + response.agentName);
        log("Access Point Type: " + response.type);
        tenant_gateway.type = response.type.toLowerCase();
        tenant_gateway.token = token;
        if (
          response.agentName.toLowerCase().trim() !=
          tenant_gateway.tenantName.toLowerCase().trim()
        )
          tenant_gateway.tenantName =
            tenant_gateway.tenantName + " (" + response.agentName + ")";

        if (response.type.toLowerCase() == "diag") {
          //it is safe to connect to this agent
          log("Diagnostics Access Point detected.");
        } else {
          //we need to first warn the user that this is not a diagnostics agent and ask for confirmation.
          popup_context = {
            response: response,
            tenant_gateway: tenant_gateway,
            save: save,
          };
          msg = NOT_DIAG_AP_MSG;
          if (save === true) {
            msg = NOT_DIAG_AP_SAVE_MSG;
          }
          show_msg(msg, true);
          return;
        }
        saveAndActivate(response, tenant_gateway, save);
      } else {
        log(
          "Unable to get CI 360 Access Point configurations. Please check the External gateway address, Tenant Id and Client Secret."
        );
        show_msg(UNABLE_TO_AP_CONFIG_MSG, false);
      }
    })
    .fail(function (error) {
      log(
        "Error connecting to CI 360 Access Point. Please check the External gateway address, Tenant Id and Client Secret."
      );
      log(error);
      var tmp_msg = "";
      if (
        error != undefined &&
        error.responseJSON != undefined &&
        error.responseJSON.message != undefined
      )
        tmp_msg = error.responseJSON.message;
      show_msg(ERROR_CONNECTING_AP_MSG + "<p>" + tmp_msg + "</p>", false);
    });
}
function saveAndActivate(response, tenant_gateway, save) {
  if (save) {
    log("Saving the gateway configuration.");
    tenant_gateway.type = response.type.toLowerCase();
    ci360SavedTenantList.tenants.push(tenant_gateway);
    log("Number of saved tenants: " + ci360SavedTenantList.tenants.length);
    if (IsExtension === true)
      chrome.storage.sync.set(
        { ci360SavedTenantList_prop: ci360SavedTenantList },
        function () {
          log("Tenant information is saved.");
          loadCi360SavedTenantList();
          settings_clear();
        }
      );
  }
  activate_gateway(tenant_gateway);
}

/** Start Tag Settings related functions. */
function loadCi360SavedTagList() {
  $("#settings_tag_activate_selected_tenant").prop("disabled", true);
  $("#settings_tag_forget_selected_tenant").prop("disabled", true);
  if (IsExtension === true)
    chrome.storage.sync.get("ci360SavedTagList_prop", function (data) {
      $("#ci360SavedTagList").html("");
      ci360SavedTagList = data.ci360SavedTagList_prop;
      if (
        ci360SavedTagList === undefined ||
        ci360SavedTagList.tenants.length < 1
      ) {
        log("There are no saved Tags.");
        current_360_tag = null;
        return;
      }
      log("There are " + ci360SavedTagList.tenants.length + " saved tags.");

      for (const tenant of ci360SavedTagList.tenants) {
        log("Tenant Id: " + tenant.id);
        log("Tenant Name: " + tenant.tenantName);
        log("Tenant Url: " + tenant.tenantUrl);
        $("#ci360SavedTagList").append(
          $("<option>", {
            value: tenant.id,
            text: tenant.tenantName,
          })
        );
      }
      $("#settings_tag_activate_selected_tenant").prop("disabled", false);
      $("#settings_tag_forget_selected_tenant").prop("disabled", false);
    });
}
function settings_tag_clear() {
  log("Clearing all values from the tag form.");
  $("#ci360tagUrlDropDown")[0].selectedIndex = 0;
  $("#ci360tagUrlTxt").val("");
  $("#ci360TenantId_TagTxt").val("");
  $("#ci360TenantNameTagTxt").val("");
}
function settings_tag_without_save_activate() {
  validate_and_Activate_Tag(false);
}
function settings_tag_save_activate() {
  validate_and_Activate_Tag(true);
}
function settings_tag_activate_selected_tenant() {
  var temp_tenantId = $("#ci360SavedTagList :selected").val();
  tag =
    ci360SavedTagList.tenants[
      ci360SavedTagList.tenants.findIndex((a) => a.id === temp_tenantId)
    ];
  activate_tag(tag);
}
function settings_tag_forget_selected_tenant() {
  var temp_tenantId = $("#ci360SavedTagList :selected").val();
  ci360SavedTagList.tenants.splice(
    ci360SavedTagList.tenants.findIndex((a) => a.id === temp_tenantId),
    1
  );
  if (IsExtension === true)
    chrome.storage.sync.set(
      { ci360SavedTagList_prop: ci360SavedTagList },
      function () {
        log("Removed the Tag configuration.");
        log("Total number of saved tags:");
        log(ci360SavedTagList.tenants.length);
      }
    );
  loadCi360SavedTagList();
}
function validate_and_Activate_Tag(save) {
  var valid = true;
  $(".tag-settings input").removeClass("border_red");
  var tenantUrl = $("#ci360tagUrlDropDown :selected").val();
  var tenantId = $("#ci360TenantId_TagTxt").val().trim();
  var tenantName = $("#ci360TenantNameTagTxt").val().trim();

  if ($("#ci360tagUrlTxt").val().trim().length > 0) {
    tenantUrl = $("#ci360tagUrlTxt").val().trim();
    if (tenantUrl.includes(":") || tenantUrl.includes(" ")) {
      $("#ci360tagUrlTxt").addClass("border_red");
      tenantUrl = "";
      valid = false;
    } else {
      log("Tenant url: " + tenantUrl);
    }
  } else {
    log("Tenant url: " + tenantUrl);
  }

  if (tenantId.length > 0) {
    log("Tenant Id: " + tenantId);
  } else {
    $("#ci360TenantId_TagTxt").addClass("border_red");
    valid = false;
    tenantId = "";
  }

  if (tenantName.length > 0) {
    log("Friendly Name: " + tenantName);
  } else {
    $("#ci360TenantNameTagTxt").addClass("border_red");
    valid = false;
    tenantName = "";
  }

  if (valid) {
    var uuid = uuidv4();
    if (ci360SavedTagList === null || ci360SavedTagList === undefined) {
      ci360SavedTagList = { tenants: [] };
    }

    tenant_tag = {
      id: uuid,
      tenantUrl: tenantUrl,
      tenantId: tenantId,
      tenantName: tenantName,
    };
    log("Recieved basic tag details.");
    if (save === true) {
      log("attempting to save the tag configuration.");
      ci360SavedTagList.tenants.push(tenant_tag);
      if (IsExtension === true)
        chrome.storage.sync.set(
          { ci360SavedTagList_prop: ci360SavedTagList },
          function () {
            log("Tag information is saved.");
            loadCi360SavedTagList();
            settings_tag_clear();
          }
        );
    }
    activate_tag(tenant_tag);
  } else {
    log("Tag configuration is not valid. Please review the details provided.");
  }
}
function activate_tag(tag) {
  log("Setting this Tag as the active tag. Name: " + tag.tenantName);
  current_360_tag = tag;
  add_360_tag();
}
/** End Tag Settings related functions. */
/** End of Settings page related JS functions */

/** Event stream page related JS functions */

firstMessage = true;
filter_condition = { identity_type: "", value: "" };
last_profile = null;
ws = null;
var event_stream_ON = false;
function initEventStreamTable() {
  var table = $("#eventStreamTable").DataTable({
    pageLength: 50,
    deferRender: true,
    order: [[5, "desc"]],
    language: {
      search: "_INPUT_",
      searchPlaceholder: "Search...",
    },
    dom: '<"toolbar">frtip',
    buttons: ["copyHtml5", "excelHtml5", "csvHtml5"],
    columns: [
      {
        className: "details-control",
        orderable: false,
        data: null,
        width: "10px",
        defaultContent:
          '<a href="#" style="text-decoration: none;" tabindex="0">&nbsp;</a>',
      },
      { data: "eventName" },
      { data: "event" },
      { data: "datahub_id" },
      { data: "event_json" },
      { data: "timestamp" },
      { data: "sessionId" },
      { data: "vid" },
    ],
    columnDefs: [
      {
        targets: [4],
        visible: false,
      },

      {
        targets: 5,
        render: $.fn.dataTable.render.moment("X", "DD MMM YYYY : hh:mm:ss.SSS"),
      },
    ],
  });

  // Add event listener for opening and closing details
  $("#eventStreamTable tbody").on("click", "td.details-control", function () {
    var tr = $(this).closest("tr");
    var row = table.row(tr);

    if (row.child.isShown()) {
      // This row is already open - close it
      row.child.hide();
      tr.removeClass("shown");
    } else {
      // Open this row
      row.child(format(row.data())).show();
      tr.addClass("shown");
    }
  });

  // Setup - add a text input to each footer cell
  $("#eventStreamTable tfoot th").each(function () {
    var title = $(this).text();
    $(this).html('<input type="text" placeholder="Search ' + title + '" />');
  });

  // DataTable
  var table = $("#eventStreamTable").DataTable();

  // Apply the search
  table.columns().every(function () {
    var that = this;

    $("input", this.footer()).on("keyup change clear", function () {
      if (that.search() !== this.value) {
        that.search(this.value).draw();
      }
    });
  });

  $("div.toolbar").html(
    '<button class="w3-btn  w3-deep-orange round left-margin" id=\'event_stream_clear\' title="Clear">X</button><button class="w3-btn w3-teal round "  title="Start" id=\'event_stream_start_stop\'>></button><span class="filter-details  w3-small w3-text-amber" id="filter_details"></span>'
  );

  $("#event_stream_clear").on("click", function () {
    var table = $("#eventStreamTable").DataTable();
    table.clear().draw();
    log("Clearing the event stream table.");
  });
  $("#event_stream_start_stop").on("click", event_stream_start_stop_click);
}
// Add event listener for opening and closing details
$("#eventStreamTable tbody").on("click", "td.details-control", function () {
  var tr = $(this).closest("tr");
  var row = table.row(tr);

  if (row.child.isShown()) {
    // This row is already open - close it
    row.child.hide();
    tr.removeClass("shown");
  } else {
    // Open this row
    row.child(format(row.data())).show();
    tr.addClass("shown");
  }
});
function event_stream_start_stop_click() {
  if (current_tenant_gateway != null) {
    if (event_stream_ON === false) {
      log("Attempting to turn on Event Stream.");
      $("#filter_details").html("");
      connect360();
      event_stream_ON = true;
      firstMessage = true;
      $("#event_stream_start_stop").html("||");
      $("#event_stream_start_stop").attr("title", "Stop");
      $("#event_stream_start_stop").removeClass("w3-teal");
      $("#event_stream_start_stop").addClass("w3-orange");
    } else {
      log("Attempting to turn off Event Stream.");

      close_event_stream_connection();
    }
  } else {
    log(NO_ACTIVE_AP_MSG);
    show_msg(NO_ACTIVE_AP_MSG);
  }
}

function popup_filters_yes_click() {
  update_diagnostics_filter(true);
}

function popup_filters_no_click() {
  close_event_stream_connection();
}

function close_event_stream_connection() {
  if (ws && ws !== null) {
    ws.send('DIAGNOSTIC_AGENT: {"operation":"clear"}');
    ws.close();
    ws = null;
  }

  event_stream_ON = false;
  $("#event_stream_start_stop").html(">");
  $("#event_stream_start_stop").attr("title", "Start");
  $("#event_stream_start_stop").removeClass("3-orange");
  $("#event_stream_start_stop").addClass("w3-teal");
}

function connect360() {
  //"wss://" + document.getElementById("tenantUrl").value + "/marketingGateway/agent/stream";
  ws = new WebSocket(
    "wss://" +
      current_tenant_gateway.tenantUrl +
      "/marketingGateway/agent/stream"
  );
  ws.onerror = function (event) {
    log("Error: " + event.data);
  };
  ws.onopen = function (event) {
    log("Opening Connection to SAS CI 360 Event Stream.");
    if (current_tenant_gateway.type === "diag") {
      log("DIAG_VERSION: " + DIAG_VERSION);
      ws.send(current_tenant_gateway.token + "\n" + DIAG_VERSION);
    } else {
      log("SDK_VERSION: " + SDK_VERSION);
      ws.send(current_tenant_gateway.token + "\n" + SDK_VERSION);
    }
  };

  ws.onmessage = function (event) {
    if (firstMessage) {
      firstMessage = false;
      log("Ready to receive events.");

      if (current_tenant_gateway.type === "diag") {
        $("#filter_details").html("Filters");
        log("Diag agent needs filters.");
        show_diagnostics_filters();
      }
    } else {
      if (event.data && event.data.includes("ping")) log("Message: ping.");
    }

    if (event.data && event.data.includes("rowKey"))
      addToGrid_eventStream(JSON.parse(event.data), event.data);
    send("ack");
  };
  ws.onclose = function (event) {
    close_event_stream_connection();
    log("Connection closed.");
    log(event.reason);
  };
}
function show_diagnostics_filters() {
  if (last_profile === null) {
    //This means there has been no network traffic caputred. So we can't auto detect any IDs.
    //Diable those options in the popup.
    $('input:radio[name="diagnostics_filter"]')
      .filter('[value="custom"]')
      .attr("checked", true);
    $('input[name="diagnostics_filter"]').attr("disabled", true);
  } else {
    $('input:radio[name="diagnostics_filter"]')
      .filter('[value="datahub_id"]')
      .attr("checked", true);
  }
  document.getElementById("id02").style.display = "block";
  log("Displaying the popup for filters.");
}
function addToGrid_eventStream(event_json, raw_json) {
  if (!event_json || !event_json.attributes) {
    return;
  }

  if (
    !event_json.attributes.eventName ||
    event_json.attributes.eventName === undefined ||
    event_json.attributes.eventName === null
  )
    event_json.attributes.eventName = "";
  if (
    !event_json.attributes.internalTenantId ||
    event_json.attributes.internalTenantId === undefined ||
    event_json.attributes.internalTenantId === null
  )
    event_json.attributes.internalTenantId = "";

  if (
    !event_json.attributes.event ||
    event_json.attributes.event === undefined ||
    event_json.attributes.event === null
  )
    event_json.attributes.event = "";
  if (
    !event_json.attributes.datahub_id ||
    event_json.attributes.datahub_id === undefined ||
    event_json.attributes.datahub_id === null
  )
    event_json.attributes.datahub_id = "";
  if (
    !event_json.attributes.sessionId ||
    event_json.attributes.sessionId === undefined ||
    event_json.attributes.sessionId === null
  )
    event_json.attributes.sessionId = "";
  if (
    !event_json.rowKey ||
    event_json.rowKey === undefined ||
    event_json.rowKey === null
  )
    event_json.rowKey = "";
  if (
    !event_json.attributes.timestamp ||
    event_json.attributes.timestamp === undefined ||
    event_json.attributes.timestamp === null
  )
    event_json.attributes.timestamp = "";
  if (
    !event_json.attributes.channel_user_id ||
    event_json.attributes.channel_user_id === undefined ||
    event_json.attributes.channel_user_id === null
  )
    event_json.attributes.channel_user_id = "";
  let row = {
    eventName: event_json.attributes.eventName,
    internalTenantId: event_json.attributes.internalTenantId,
    //"event": event_json.attributes.event,
    event: getSuitableCol(JSON.stringify(event_json.attributes)),
    datahub_id: event_json.attributes.datahub_id,
    attributes: JSON.stringify(event_json.attributes, undefined, 2),
    event_json: JSON.stringify(event_json, undefined, 2),
    sessionId: event_json.attributes.sessionId,
    rowKey: event_json.rowKey,
    timestamp: event_json.attributes.timestamp,
    vid: event_json.attributes.channel_user_id,
  };
  var t = $("#eventStreamTable").DataTable();
  t.row.add(row).draw(false);
  log("Event recieved: " + event_json.attributes.eventName);
}
function send(message, callback) {
  waitForConnection(function () {
    ws.send(message);
    if (typeof callback !== "undefined") {
      callback();
    }
  }, 1000);
}
function waitForConnection(callback, interval) {
  if (ws && ws.readyState === 1) {
    callback();
  } else {
    var that = this;
    // optional: implement backoff for interval here
    setTimeout(function () {
      that.waitForConnection(callback, interval);
    }, interval);
  }
}
function addFunctionsforDataTables() {
  (function (factory) {
    "use strict";

    if (typeof define === "function" && define.amd) {
      // AMD
      define(["jquery"], function ($) {
        return factory($, window, document);
      });
    } else if (typeof exports === "object") {
      // CommonJS
      module.exports = function (root, $) {
        if (!root) {
          root = window;
        }

        if (!$) {
          $ =
            typeof window !== "undefined"
              ? require("jquery")
              : require("jquery")(root);
        }

        return factory($, root, root.document);
      };
    } else {
      // Browser
      factory(jQuery, window, document);
    }
  })(function ($, window, document) {
    $.fn.dataTable.render.moment = function (from, to, locale) {
      // Argument shifting
      if (arguments.length === 1) {
        locale = "en";
        to = from;
        from = "YYYY-MM-DD";
      } else if (arguments.length === 2) {
        locale = "en";
      }

      return function (d, type, row) {
        var m = window.moment(d.slice(0, -3), from, locale, true);
        // Order and type get a number value from Moment, everything else
        // sees the rendered value
        return m.format(type === "sort" || type === "type" ? "x" : to);
      };
    };
  });
}
/** End of Event stream page related JS functions */

/** Start of Debugging page related JS functions */
var id_get360TagScripts = [];
var id_get360TagScripts_console = [];
function initDebuggingPage() {
  id_get360TagScripts.push(setInterval(get360TagScripts, 2000));
  id_get360TagScripts_console.push(setInterval(get360TagScripts_console, 2000));
}
function getTitle() {
  return document.title;
}
function onExecuted(result) {
  console.log(`We made it green`);
}

function onError(error) {
  console.log(`Error: ${error}`);
}
function get360TagScripts_console() {
  var ev2 =
    'document.querySelectorAll("script[id^=ob-]").forEach((s) => {console.log("From Snowy, 360 Tag: "+s.outerHTML);}); ';
  if (IsExtension)
    chrome.devtools.inspectedWindow.eval(ev2, function (result, isException) {
      if (isException) {
        log("Unable to get the 360 script tag from page. Will try again.");
      } else {
        log("Got Script from page. Check the Console log of dev tools please.");
        if (
          null != id_get360TagScripts_console &&
          id_get360TagScripts_console.length > 0
        ) {
          id_get360TagScripts_console.forEach((intv) => {
            clearInterval(intv);
          });
          id_get360TagScripts_console = [];
        }
      }
    });
}
function get360TagScripts() {
  $("#script_tag").html(UNDOCKED_DEVTOOL_MSG);
  if (
    IsExtension &&
    chrome.tabs !== undefined &&
    chrome.tabs.executeScript !== undefined
  ) {
    chrome.tabs.executeScript(
      null,
      {
        code: 'var result=[]; document.querySelectorAll("script[id^=ob-]").forEach((s) => {result.push(s.outerHTML);}); result',
      },
      function (results) {
        if (
          results &&
          results != undefined &&
          results != null &&
          results.length > 0
        ) {
          log("Got the script tags from page.");
          if (null != id_get360TagScripts && id_get360TagScripts.length > 0) {
            id_get360TagScripts.forEach((intv) => {
              clearInterval(intv);
            });
            id_get360TagScripts = [];
          }
          updateScriptsTagDetails(results[0]);
        }
      }
    );
  }
}
function updateScriptsTagDetails(scripts) {
  if (scripts && scripts !== undefined && scripts.length > 0) {
    $("#script_tag").html("");
    scripts.forEach((script, index) => {
      script = decodeURIComponent(script);
      script = script.replace(/</g, "&lt;").replace(/>/g, "&gt;");
      log(script);
      $("#script_tag").append("<li>" + script + "</li>");
    });
  }
}
/** End of Debugging page related JS functions */

/** Start of Network stream page related JS functions */
var last_profile = {
  timestamp: "",
  session: "",
  visitor: "",
  datahub_id: "",
  pii: "",
  event: "",
  row_num: "",
  touched: false,
};
var row_num = 1;
var network_stream_ON = true;
function initNetWorkTable() {
  var table = $("#trafficTable").DataTable({
    deferRender: true,
    pageLength: 50,
    order: [[4, "desc"]],
    language: {
      search: "_INPUT_",
      searchPlaceholder: "Search...",
    },
    dom: '<"toolbar2">frtip',
    columns: [
      {
        className: "details-control",
        orderable: false,
        data: null,
        defaultContent:
          '<a href="#" style="text-decoration: none;" tabindex="0">&nbsp;</a>',
      },
      { data: "event" },
      { data: "event_json" },
      { data: "eventname" },
      { data: "timestamp" },
      { data: "status" },
      { data: "datahub_id" },
      { data: "row_num" },
    ],
    columnDefs: [
      {
        targets: [2],
        visible: false,
      },

      {
        targets: [0],
        width: "10px",
      },
      {
        targets: [5, 7],
        width: "10px",
      },
    ],
  });

  // Add event listener for opening and closing details
  $("#trafficTable tbody").on("click", "td.details-control", function () {
    var tr = $(this).closest("tr");
    var row = table.row(tr);

    if (row.child.isShown()) {
      // This row is already open - close it
      row.child.hide();
      tr.removeClass("shown");
    } else {
      // Open this row
      row.child(format(row.data())).show();
      tr.addClass("shown");
    }
  });

  $("#trafficTable").on("mouseenter", "tbody tr", function () {
    var rowData = table.row(this).data();
    if (rowData && rowData.event_json !== undefined) {
      //var target = JSON.parse(rowData.event_json);
      var target = JSON.parse(
        rowData.event_json
          .replace(/\n/g, "\\n")
          .replace(/\r/g, "\\r")
          .replace(/\t/g, "\\t")
          .replace("\b", "")
      );
      var t = getSelector(target);
      if (t && t != undefined && t.length > 0) {
        t = t.trim();
        t = t.replaceAll(". ", "");
        //works var ev2 = "$('" + t + "').css({border: '2px solid red', transform: 'scale(1.5)'})";
        log("Selector: " + t);
        var ev2 =
          "document.querySelectorAll('" +
          t +
          "').forEach ((i) => {i.style.border_tmp = i.style.border;i.style.border='2px solid red'; i.style.transform='scale(1.5)';})";
        if (
          IsValuePresent(target["event"]) &&
          (target["event"] === "spot_change" ||
            target["event"] === "spot_viewable")
        )
          ev2 =
            "document.querySelectorAll('" +
            t +
            "').forEach ((i) => {i.parentElement.style.border_tmp = i.parentElement.style.border;i.parentElement.style.border='2px solid red'; i.parentElement.style.transform='scale(1.5)';})";
        chrome.devtools.inspectedWindow.eval(ev2);
      }
    }
  });
  $("#trafficTable").on("mouseleave", "tbody tr", function () {
    var rowData = table.row(this).data();
    if (rowData && rowData.event_json !== undefined) {
      //var target = JSON.parse(rowData.event_json);
      var target = JSON.parse(
        rowData.event_json
          .replace(/\n/g, "\\n")
          .replace(/\r/g, "\\r")
          .replace(/\t/g, "\\t")
          .replace("\b", "")
      );
      //var t = target["targetSelectorPath"];
      var t = getSelector(target);
      if (t && t != undefined && t.length > 0) {
        t = t.trim();
        t = t.replaceAll(". ", "");
        //works var ev = "$('" + t + "').css({border: '0px solid red', transform: 'scale(1)'})";
        var ev =
          "document.querySelectorAll('" +
          t +
          "').forEach ((i) => {i.style.border=i.style.border_tmp;i.style.transform='scale(1)';})";
        if (
          IsValuePresent(target["event"]) &&
          (target["event"] === "spot_change" ||
            target["event"] === "spot_viewable")
        )
          ev =
            "document.querySelectorAll('" +
            t +
            "').forEach ((i) => {i.parentElement.style.border=i.parentElement.style.border_tmp;i.parentElement.style.transform='scale(1)';})";
        chrome.devtools.inspectedWindow.eval(ev);
      }
    }
  });

  // Setup - add a text input to each footer cell
  $("#trafficTable tfoot th").each(function () {
    var title = $(this).text();
    $(this).html('<input type="text" placeholder="Search ' + title + '" />');
  });

  // DataTable
  var table = $("#trafficTable").DataTable();

  // Apply the search
  table.columns().every(function () {
    var that = this;

    $("input", this.footer()).on("keyup change clear", function () {
      if (that.search() !== this.value) {
        that.search(this.value).draw();
      }
    });
  });

  $("div.toolbar2").html(
    '<button class="w3-btn  w3-deep-orange round left-margin" id=\'network_stream_clear\' title="Clear">X</button><button class="w3-btn w3-orange round "  title="Stop" id=\'network_stream_start_stop\'>||</button><span id="Msg360Tag" class="filter-details-network w3-text-red w3-small">' +
      NO_TAG_FOUND_MSG +
      "</span>"
  );

  var hightCfg = {
    showInfo: true,
    showStyles: true,
    contentColor: { r: 155, g: 11, b: 239, a: 0.7 },
  };

  $("#network_stream_clear").on("click", function () {
    var table = $("#trafficTable").DataTable();
    table.clear().draw();
    log("Clearing the network stream table.");
    $("#profileTable").find("tr:not(:last-child)").remove();
    last_profile = {
      timestamp: "",
      session: "",
      visitor: "",
      datahub_id: "",
      pii: "",
      event: "",
      row_num: "",
      touched: false,
    };
    $("#lblDatahubId").html("");
    $("#lblIPii").html("");
    add_360_tag();
  });
  $("#network_stream_start_stop").on("click", network_stream_start_stop_click);

  $("#attach_identity").on("click", function () {
    log("Trying to fire attach identity event.");
    var valid = true;
    var identity_type = $("#identity_type :selected").val();
    var identity_value = $("#txtIdentity_value").val().trim();
    var identity_obscure = $("#identity_obscure :selected").val();
    $("#txtIdentity_value").removeClass("border_red");
    $("#identity_type").removeClass("border_red");
    $("#identity_obscure").removeClass("border_red");

    if (
      !identity_type ||
      identity_type === undefined ||
      identity_type === "select"
    ) {
      $("#identity_type").addClass("border_red");
      valid = false;
    }

    if (
      !identity_obscure ||
      identity_obscure === undefined ||
      identity_obscure === "select"
    ) {
      $("#identity_obscure").addClass("border_red");
      valid = false;
    }

    if (
      !identity_value ||
      identity_value === undefined ||
      identity_value.length < 1
    ) {
      $("#txtIdentity_value").addClass("border_red");
      valid = false;
    }
    if (valid) {
      log("attempting to fire JS APIs");
      var js_api =
        "ci360('attachIdentity', { 'loginId': '" +
        identity_value +
        "', 'loginEventType': '" +
        identity_type +
        "' });";
      if (identity_obscure === "yes")
        js_api =
          "ci360('attachIdentity', { 'loginId': '" +
          identity_value +
          "', 'loginEventType': '" +
          identity_type +
          "', 'obfuscateFields': '[\"loginId\"]' });";

      if (IsExtension) {
        chrome.devtools.inspectedWindow.eval(js_api);
        log(
          "Fired the event. Type: " +
            identity_type +
            ", Value: " +
            identity_value
        );
        log(js_api);
      }
    } else {
      log(IDENTITY_INFO_NOT_VALID_MSG);
      show_msg(IDENTITY_INFO_NOT_VALID_MSG);
    }
  });

  $("#detach_identity").on("click", function () {
    log("Trying to fire detach identity event.");
    log("attempting to fire JS APIs");
    var js_api = "ci360('detachIdentity');";
    if (IsExtension) {
      chrome.devtools.inspectedWindow.eval(js_api);
      log("Fired the event.");
    }
  });
}
function getSelector(target) {
  if (IsValuePresent(target["_tsp1"]) === true) return target["_tsp1"];
  if (IsValuePresent(target["targetSelectorPath"]) === true) {
    var sel = target["targetSelectorPath"];
    if (
      (sel.trim().endsWith(">a") || sel.trim().endsWith("> a")) &&
      IsValuePresent(target["anchor_href"])
    ) {
      //a[href*="about"]
      sel = sel + '[href*="' + target["anchor_href"] + '"]';
      return sel;
    }
    return target["targetSelectorPath"];
  }
  if (IsValuePresent(target["targetselector"]) === true)
    return target["targetselector"];
  if (IsValuePresent(target["targetid"]) === true)
    return "#" + target["targetid"];
  if (IsValuePresent(target["creative_id"]) === true)
    return 'data[data-creativeid="' + target["creative_id"] + '"]';

  for (key in target) {
    var v = getMatchingKeyValues(key, target);
    if (IsValuePresent(v)) return v;
  }
  return "";
}
function getMatchingKeyValues(key, target) {
  if (key && key != undefined && key.length > 0) {
    if (
      key.startsWith("form.f.") &&
      key.endsWith("._h.id") &&
      IsValuePresent(target[key])
    ) {
      return "#" + target[key];
    }
  }
  return "";
}
function IsValuePresent(val) {
  if (val && val != null && val != undefined && val.trim().length > 0)
    return true;
  return false;
}
function add_360_tag() {
  if (
    IsTagAdded === false &&
    current_360_tag &&
    current_360_tag.tenantUrl &&
    current_360_tag.tenantUrl.length > 0 &&
    current_360_tag.tenantId &&
    current_360_tag.tenantId.length > 0
  ) {
    log("Attempting to add the 360 tag to current page.");
    log("Tenant name: " + current_360_tag.tenantName);
    if (IsExtension)
      chrome.devtools.inspectedWindow.eval(
        "var tag = document.createElement('script'); tag.src = 'https://" +
          current_360_tag.tenantUrl +
          "/js/ot-all.min.js'; tag.setAttribute('id','ob-script-async'); tag.setAttribute('data-efname','ci360'); tag.setAttribute('data-a','" +
          current_360_tag.tenantId +
          "'); var head = document.getElementsByTagName('head')[0]; head.appendChild(tag); console.log('CI 360 Tag added to this site by Snowy.');"
      );
    IsTagAdded = true;
  }
}
function copyProfile(current_profile) {
  last_profile.session = current_profile.session;
  last_profile.visitor = current_profile.visitor;
  last_profile.datahub_id = current_profile.datahub_id;
  last_profile.event = current_profile.event;
  last_profile.row_num = current_profile.row_num;
  last_profile.pii = current_profile.pii;
  last_profile.timestamp = current_profile.timestamp;
  update_diagnostics_filter(false);
}
function update_diagnostics_filter(manual_update) {
  var valid = true;
  var selectedOption = $("input:radio[name=diagnostics_filter]:checked").val();
  if (selectedOption == "session") {
    filter_condition.identity_type = "session_id";
    filter_condition.value = last_profile.session;
  } else if (selectedOption == "datahub_id") {
    filter_condition.identity_type = "datahub_id";
    filter_condition.value = last_profile.datahub_id;
  } else if (selectedOption == "channel_id") {
    filter_condition.identity_type = "channel_id";
    filter_condition.value = last_profile.visitor;
  } else if (selectedOption == "custom" && manual_update == true) {
    filter_condition.identity_type = $(
      "#diagnostics_filter_custom_attr :selected"
    ).val();
    filter_condition.value = $("#diagnostics_filter_custom_fixed").val().trim();
    if (filter_condition.value.length === 0) {
      $("#diagnostics_filter_custom_fixed").addClass("border_red");
      filter_condition.value = "";
      valid = false;
    }
  }
  if (valid) {
    if (IsValuePresent(filter_condition.identity_type))
      $("#filter_details").html(
        "Filter: " +
          filter_condition.identity_type +
          " = <span>" +
          filter_condition.value +
          "</span>"
      );
    document.getElementById("id02").style.display = "none";
    log("Closing the popup for filters.");
    if (ws) {
      message =
        'DIAGNOSTIC_AGENT: {"operation":"filter","attribute":"' +
        filter_condition.identity_type +
        '", "value":"' +
        filter_condition.value +
        '"}';
      log("Sending filter condition: " + message);
      send(message);
      send('DIAGNOSTIC_AGENT: {"operation":"show"}');
    }
  }
}
function addProfile() {
  var table = document.getElementById("profileTable");
  //var row = table.insertRow(table.rows.length);
  var row = table.insertRow(1);
  var cell0 = row.insertCell(0);
  var cell1 = row.insertCell(1);
  var cell2 = row.insertCell(2);
  var cell3 = row.insertCell(3);
  var cell4 = row.insertCell(4);
  var cell5 = row.insertCell(5);
  var cell6 = row.insertCell(6);

  var dId = cell3.innerHTML;
  cell0.innerHTML = last_profile.timestamp;
  cell1.innerHTML = last_profile.session;
  cell2.innerHTML = last_profile.visitor;
  cell3.innerHTML = last_profile.datahub_id;
  cell4.innerHTML = last_profile.pii;
  cell5.innerHTML = last_profile.event;
  cell6.innerHTML = last_profile.row_num;
  $("#lblDatahubId").html("Datahub Id: " + last_profile.datahub_id);
  if (last_profile.pii.length > 0)
    $("#lblIPii").html(", Identity: " + last_profile.pii);
  checkAndUpdateIdentityInfo();
}
function checkAndUpdateIdentityInfo() {
  $("#no_identity_info").show();
  if (
    current_tenant_gateway != undefined &&
    IsValuePresent(current_tenant_gateway.token) &&
    ((current_tenant_gateway.display_metadata_from_360 === true &&
      current_tenant_gateway.display_metadata_from_360 != undefined) ||
      current_tenant_gateway.display_metadata_from_360 === undefined)
  ) {
    log(
      "Attempting to get the identity information. Datahub Id: " +
        last_profile.datahub_id
    );
    $("#no_identity_info").hide();
    $("#no_identity_info").show();
    $("#identityTable").find("tr:not(:last-child)").remove();
    var url =
      "https://" +
      current_tenant_gateway.tenantUrl +
      "/marketingData/identityRecords/" +
      last_profile.datahub_id;

    var settings = {
      url: url,
      method: "GET",
      timeout: 0,
      headers: {
        Authorization: "Bearer " + current_tenant_gateway.token,
      },
    };

    $.ajax(settings)
      .done(function (response) {
        log(response);
        if (response && response.identities && response.identities.length > 0) {
          var table = document.getElementById("identityTable");
          //var row = table.insertRow(table.rows.length);
          for (var i = 0; i < response.identities.length; i++) {
            var row = table.insertRow(1);
            var cell0 = row.insertCell(0);
            var cell1 = row.insertCell(1);
            var cell2 = row.insertCell(2);
            var cell3 = row.insertCell(3);
            var cell4 = row.insertCell(4);
            cell0.innerHTML = response.identities[i].id;
            cell1.innerHTML = response.identities[i].type;
            if (response.identities[i].source)
              cell2.innerHTML = response.identities[i].source;
            if (response.identities[i].createTime)
              cell3.innerHTML = response.identities[i].createTime;
            if (response.identities[i].attributes) {
              for (var key in response.identities[i].attributes) {
                cell4.innerHTML=cell4.innerHTML+"<div style='border-bottom:1px sold #1b6194'>"+key+": "+response.identities[i].attributes[key]+"</div>";
              }
            }
          }
        }
      })
      .fail(function (error) {
        log(
          "Error connecting to CI 360 Access Point. Please check the External gateway address, Tenant Id and Client Secret."
        );
        log(error);
      });
  }
}
function checkAndUpdateProfile(event_json) {
  var current_profile = {
    timestamp: "",
    session: "",
    visitor: "",
    datahub_id: "",
    pii: "",
    event: "",
    row_num: "",
  };
  current_profile.timestamp = event_json.timestamp;
  current_profile.datahub_id = event_json.datahub_id;
  current_profile.session = event_json.session;
  current_profile.visitor = event_json.visitor;
  current_profile.event = event_json.event + " / " + event_json.eventname;
  current_profile.row_num = event_json.row_num;
  current_profile.pii = "";

  if (
    event_json.event &&
    (event_json.event.toLowerCase() == "identityevent" ||
      event_json.event.includes("Inferred Event: attachIdentity") ||
      event_json.event.includes("Inferred Event: detachIdentity"))
  ) {
    if (
      (event_json.login_event || event_json.User_ID_Attribute) &&
      event_json.login_event_type
    )
      current_profile.pii =
        decodeURIComponent(event_json.login_event_type) +
        ": " +
        decodeURIComponent(
          event_json.login_event
            ? event_json.login_event
            : event_json.User_ID_Attribute
        );
    else current_profile.pii = " ";
  }

  if (!last_profile.touched) {
    //first request and hence current profile is the last profile. copy them.
    last_profile.touched = true;
    copyProfile(current_profile);
    addProfile();
  } else {
    if (
      (current_profile.session &&
        current_profile.session != last_profile.session) ||
      (current_profile.visitor &&
        current_profile.visitor != last_profile.visitor) ||
      (current_profile.datahub_id &&
        current_profile.datahub_id != last_profile.datahub_id) ||
      (current_profile.pii &&
        current_profile.pii != "" &&
        current_profile.pii != last_profile.pii)
    ) {
      copyProfile(current_profile);
      addProfile();
    }
  }
}
//const inspectButton = document.querySelector("#inspect");
//const inspectString = "inspect(document.querySelector('h1'))";
const evalString = "$('h1').css({backgroundColor: 'red'})";

function handleError(error) {
  if (error.isError) {
    console.log(`Devtools error: ${error.code}`);
  } else {
    console.log(`JavaScript error: ${error.value}`);
  }
}

function handleResult(result) {
  if (result[1]) {
    handleError(result[1]);
  } else {
    chrome.devtools.inspectedWindow.eval(evalString);
  }
}

function network_stream_start_stop_click() {
  if (network_stream_ON === false) {
    log("Attempting to turn on Network Stream.");
    network_stream_ON = true;
    $("#network_stream_start_stop").html("||");
    $("#network_stream_start_stop").attr("title", "Stop");
    $("#network_stream_start_stop").removeClass("w3-teal");
    $("#network_stream_start_stop").addClass("w3-orange");
  } else {
    log("Attempting to turn off Network Stream.");
    network_stream_ON = false;
    $("#network_stream_start_stop").html(">");
    $("#network_stream_start_stop").attr("title", "Start");
    $("#network_stream_start_stop").addClass("w3-teal");
    $("#network_stream_start_stop").removeClass("w3-orange");
  }
}
function addToNetworkGrid(event_json, raw_json) {
  if (event_json.eventname === undefined || event_json.eventname === null)
    event_json.eventname = "";
  if (
    event_json.eventDesignedName === undefined ||
    event_json.eventDesignedName === null
  )
    event_json.eventDesignedName = "";
  if (event_json.event === undefined || event_json.event === null) {
    if (event_json.login_event_type && event_json.login_event) {
      event_json.event = "*Inferred Event: attachIdentity";
    } else event_json.event = "unidentified event";
  }
  if (!event_json.datahub_id) {
    event_json.datahub_id = "";
  }

  if (
    !event_json.status ||
    event_json.status === undefined ||
    event_json.status === null
  )
    event_json.status = "";
  if (
    !event_json.timestamp ||
    event_json.timestamp === undefined ||
    event_json.timestamp === null
  )
    event_json.timestamp = "";
  if (
    !event_json.datahub_id ||
    event_json.datahub_id === undefined ||
    event_json.datahub_id === null
  )
    event_json.datahub_id = "";
  if (
    !event_json.row_num ||
    event_json.row_num === undefined ||
    event_json.row_num === null
  )
    event_json.row_num = "";

  let row = {
    event: event_json.event,
    event_json: decodeURIComponent(raw_json),
    status: event_json.status,
    //"eventname": decodeURIComponent(event_json.eventname) + (event_json.eventDesignedName != "" ? " / " + event_json.eventDesignedName : ""),
    eventname: getSuitableCol(raw_json),
    timestamp: event_json.timestamp,
    datahub_id: event_json.datahub_id,
    row_num: event_json.row_num,
  };
  var t = $("#trafficTable").DataTable();
  t.row.add(row).draw(false);

  checkAndUpdateProfile(event_json);
}
function getSuitableCol(json) {
  json = JSON.parse(
    json
      .replace(/\n/g, "\\n")
      .replace(/\r/g, "\\r")
      .replace(/\t/g, "\\t")
      .replace("\b", "")
  );
  if (
    IsValuePresent(json.eventname) &&
    IsValuePresent(json.eventDesignedName) &&
    json.eventDesignedName != json.eventname
  )
    return (
      "Event Name: " +
      json.eventname +
      " / Design Name: " +
      json.eventDesignedName
    );
  if (IsValuePresent(json.eventname) && json.eventname != json.event)
    return "Event Name: " + json.eventname;
  if (
    IsValuePresent(json.eventDesignedName) &&
    json.eventDesignedName != json.event
  )
    return "Design Name: " + json.eventDesignedName;
  if (IsValuePresent(json.login_event_type) && IsValuePresent(json.login_event))
    return json.login_event_type + ": " + json.login_event;

  var v = null;

  if (IsValuePresent(json.elementTagName)) v = json.elementTagName;
  if (IsValuePresent(json.anchor_href))
    v = v + " (href: " + json.anchor_href + ")";
  if (IsValuePresent(json.anchor_id)) v = v + " (id: " + json.anchor_id + ")";
  if (IsValuePresent(json.targetInnerText))
    v = v + " (text: " + decodeURIComponent(json.targetInnerText) + ")";
  if (v != null) return v;

  if (IsValuePresent(json.searchTerm))
    return "term: " + decodeURIComponent(json.searchTerm);

  if (IsValuePresent(json.apiEventKey) && json.apiEventKey != json.event)
    return "apiEventKey: " + decodeURIComponent(json.apiEventKey);

  v = null;
  if (IsValuePresent(json.initiator))
    v = "initiator: " + decodeURIComponent(json.initiator);

  if (IsValuePresent(json.page_title))
    return (
      "Page Title: " +
      decodeURIComponent(json.page_title) +
      (v === null ? "" : " (" + v + ")")
    );
  if (IsValuePresent(json.page_path))
    return "Page path: " + decodeURIComponent(json.page_path);

  return "";
}
function htmlCodes(str, replace) {
  if (replace === true) {
    str = str.replaceAll('="', "$SW1$sw1$");
    str = str.replaceAll('"', "$SW2$sw2$");
    str = str.replaceAll("'", "$SW3$sw3$");
    str = str.replaceAll("=", "$SW4$sw4$");
    str = str.replaceAll("%3D%22", "$SW5$sw5$");
    str = str.replaceAll("%22", "$SW6$sw6$");
  } else {
    str = str.replaceAll("$SW1$sw1$", '="');
    str = str.replaceAll("$SW2$sw2$", '"');
    str = str.replaceAll("$SW3$sw3$", "'");
    str = str.replaceAll("$SW4$sw4$", "=");
    str = str.replaceAll("$SW5$sw5$", '="');
    str = str.replaceAll("$SW6$sw6$", '"');
    str = str
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }

  return str;
}
function url_domain(data) {
  var a = document.createElement("a");
  a.href = data;
  return a.hostname;
}
var clear_entries = true;
var tag_add_id = [];
if (chrome && chrome.devtools)
  chrome.devtools.network.onNavigated.addListener(function (url) {
    $("#Msg360Tag").show();
    if (tag_add_id !== null && tag_add_id.length > 0) {
      tag_add_id.forEach((intv) => {
        clearInterval(intv);
      });
      tag_add_id = [];
    }
    clear_entries = true;
    IsTagAdded = false;
    log("Navigated to a new page.");
    tag_add_id.push(
      setInterval(function () {
        add_360_tag();
      }, 2000)
    );
    id_get360TagScripts.push(setInterval(get360TagScripts, 2000));
    id_get360TagScripts_console.push(
      setInterval(get360TagScripts_console, 2000)
    );
    $("#current_domain").html(url_domain(url));
    $("#scripts_loaded").html(NO_TAG_LOADED_MSG);
    $("#generic_collection_rules").html("");
    $("#form_collection_rules").html("");
    $("#script_tag").html("");
  });

if (chrome && chrome.devtools)
  chrome.devtools.network.onRequestFinished.addListener(function (request) {
    if (
      request._initiator !== undefined &&
      request._initiator.stack &&
      request._initiator.stack.parent &&
      (request._initiator.stack.parent.callFrames[0].url.includes("/t/e/") ||
        request._initiator.stack.parent.callFrames[0].url.includes("/t/s/") ||
        request._initiator.stack.parent.callFrames[0].url.includes("ot-min") ||
        request._initiator.stack.parent.callFrames[0].url.includes("ot-api") ||
        request._initiator.stack.parent.callFrames[0].url.includes("ot-all"))
    ) {
      IsTagAdded = true;
    }
    if (request.request && request.request.method == "GET") {
      if (
        request.request.url.includes("ot-min") ||
        request.request.url.includes("ot-api") ||
        request.request.url.includes("ot-all")
      ) {
        if (clear_entries) {
          clear_entries = false;
          $("#scripts_loaded").html("");
        }
        $("#scripts_loaded").append("<li>" + request.request.url + "</li>");
        $("#Msg360Tag").hide();
      } else if (request.request.url.includes("/t/s/c/")) {
        $("#Msg360Tag").hide();
        //loading configurations
        $("#tenant_id").html(request.request.url.split("/")[6].split("?")[0]);
        if (!$("#tenantId").val() || $("#tenantId").val().trim().length < 1)
          $("#tenantId").val(request.request.url.split("/")[6].split("?")[0]);

        request.getContent(function (content, encoding) {
          if (content && content.length > 0) {
            if (content.includes("collect['clicks']"))
              $("#generic_collection_rules").append(
                "<li>" +
                  content
                    .match("'clicks'(.*);")[0]
                    .replaceAll("'", "")
                    .replaceAll("]", "") +
                  "</li>"
              );
            if (content.includes("collect['contentevents']"))
              $("#generic_collection_rules").append(
                "<li>" +
                  content
                    .match("'contentevents'(.*);")[0]
                    .replaceAll("'", "")
                    .replaceAll("]", "") +
                  "</li>"
              );
            if (content.includes("collect['fieldinteractions']"))
              $("#generic_collection_rules").append(
                "<li>" +
                  content
                    .match("'fieldinteractions'(.*);")[0]
                    .replaceAll("'", "")
                    .replaceAll("]", "") +
                  "</li>"
              );
            if (content.includes("collect['formsubmits']"))
              $("#generic_collection_rules").append(
                "<li>" +
                  content
                    .match("'formsubmits'(.*);")[0]
                    .replaceAll("'", "")
                    .replaceAll("]", "") +
                  "</li>"
              );
            if (content.includes("collect['jsvars']"))
              $("#generic_collection_rules").append(
                "<li>" +
                  content
                    .match("'jsvars'(.*);")[0]
                    .replaceAll("'", "")
                    .replaceAll("]", "") +
                  "</li>"
              );
            if (content.includes("collect['media']"))
              $("#generic_collection_rules").append(
                "<li>" +
                  content
                    .match("'media'(.*);")[0]
                    .replaceAll("'", "")
                    .replaceAll("]", "") +
                  "</li>"
              );
            if (content.includes("collect['mouseovers']"))
              $("#generic_collection_rules").append(
                "<li>" +
                  content
                    .match("'mouseovers'(.*);")[0]
                    .replaceAll("'", "")
                    .replaceAll("]", "") +
                  "</li>"
              );
            if (content.includes("collect['pageloads']"))
              $("#generic_collection_rules").append(
                "<li>" +
                  content
                    .match("'pageloads'(.*);")[0]
                    .replaceAll("'", "")
                    .replaceAll("]", "") +
                  "</li>"
              );

            if (
              content.includes(
                "com_sas_ci_acs._ob_configure.prototype.getFormConfig"
              )
            ) {
              var tempstr = content
                .substring(
                  content.indexOf("var ff = {}") + 12,
                  content.indexOf("return ff")
                )
                .replaceAll("\n", "<br/>");
              $("#form_collection_rules").append(tempstr);
            }
          } else {
            $("#generic_collection_rules").html(DOMAIN_NOT_APPROVED_MSG);
          }
        });
      }
    }

    if (!network_stream_ON) return;
    if (
      request.request.method == "POST" &&
      ((request.request &&
        (request.request.url.includes("/t/e/") ||
          request.request.url.includes("/t/s/"))) ||
        (request._initiator.stack &&
          request._initiator.stack.parent &&
          (request._initiator.stack.parent.callFrames[0].url.includes("/t/e") ||
            request._initiator.stack.parent.callFrames[0].url.includes(
              "ot-min"
            ) ||
            request._initiator.stack.parent.callFrames[0].url.includes(
              "ot-api"
            ) ||
            request._initiator.stack.parent.callFrames[0].url.includes(
              "ot-all"
            ))))
    ) {
      IsTagAdded = true;
      $("#Msg360Tag").hide();
      var req = JSON.stringify(request, null, 4);
      if (request.request.postData && request.request.postData.params) {
        var params = "{";
        var param_raw = "{";
        var first = 0;
        params =
          params +
          '"status": "' +
          (request.response.status == "0"
            ? "0 (cancelled)"
            : request.response.status) +
          '"';
        params = params + ',"row_num": "' + row_num + '"';
        row_num = row_num + 1;
        for (var i = 0; i < request.request.postData.params.length; i++) {
          if (first == 0) {
            first = 1;
            param_raw =
              param_raw +
              '"' +
              request.request.postData.params[i].name +
              '": "' +
              htmlCodes(request.request.postData.params[i].value, true) +
              '"';
          } else {
            param_raw =
              param_raw +
              ',"' +
              request.request.postData.params[i].name +
              '": "' +
              htmlCodes(request.request.postData.params[i].value, true) +
              '"';
          }

          params =
            params +
            ',"' +
            request.request.postData.params[i].name +
            '": "' +
            htmlCodes(request.request.postData.params[i].value, true) +
            '"';
        }
        params = params + "}";
        param_raw = param_raw + "}";

        var obj = JSON.parse(params);
        //obj.timestamp = moment(request.startedDateTime).format('x');
        obj.timestamp = request.startedDateTime;
        addToNetworkGrid(obj, param_raw);
      }
    } else if (
      (request.request.method == "OPTIONS" ||
        request.request.method == "DELETE") &&
      ((request.request &&
        (request.request.url.includes("/t/e/") ||
          request.request.url.includes("/t/s/"))) ||
        (request._initiator.stack &&
          request._initiator.stack.parent &&
          (request._initiator.stack.parent.callFrames[0].url.includes("/t/e") ||
            request._initiator.stack.parent.callFrames[0].url.includes(
              "ot-min"
            ) ||
            request._initiator.stack.parent.callFrames[0].url.includes(
              "ot-api"
            ) ||
            request._initiator.stack.parent.callFrames[0].url.includes(
              "ot-all"
            ))))
    ) {
      $("#Msg360Tag").hide();
      IsTagAdded = true;
      var params = "{";
      var param_raw = "{";
      var first = 0;
      params =
        params +
        '"status": "' +
        (request.response.status == "0"
          ? "0 (cancelled)"
          : request.response.status) +
        '"';
      params =
        params +
        ',"event": "' +
        "*Inferred Event: detachIdentity (method: " +
        request.request.method +
        ")" +
        '"';
      params = params + ',"row_num": "' + row_num + '"';
      row_num = row_num + 1;

      params = params + "}";
      param_raw = param_raw + "}";

      var obj = JSON.parse(params);
      //obj.timestamp = moment(request.startedDateTime).format('x');
      obj.timestamp = request.startedDateTime;
      addToNetworkGrid(obj, param_raw);
    }
  });

/** End of Network stream page related JS functions */
