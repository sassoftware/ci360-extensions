var row_num = 1;
var last_profile = { timestamp: "", session: "", visitor: "", datahub_id: "", pii: "", event: "", row_num: "", touched: false };
var EnableNetworkCapture = true;
var StartEventStream = false;

var tenantUrl = "";
var tenantId = "";
var clientSecret = "";
var token = "";
var ws;
var firstMessage = true;
var diagAgent = false;

$(document).ready(function() {


    chrome.storage.sync.get('tenantUrl', function(data) {
        $("#tenantUrl").val(data.tenantUrl);
    });
    chrome.storage.sync.get('tenantId', function(data) {
        $("#tenantId").val(data.tenantId);
    });
    chrome.storage.sync.get('clientSecret', function(data) {
        $("#clientSecret").val(data.clientSecret);
    });


    var networkTab = document.getElementById("networkTab");
    networkTab.addEventListener("click", function() {
        openPage('netwrok', this, 'white');
    });
    var eventStreamTab = document.getElementById("eventStreamTab");
    eventStreamTab.addEventListener("click", function() {
        openPage('event_stream', this, 'white');
    });
    var config360Tab = document.getElementById("config360Tab");
    config360Tab.addEventListener("click", function() {
        openPage('config_360', this, 'white');
    });

    $("#btnStartStop").click(function() {
        $("#btnStartStop").toggleClass("btnStart");
        EnableNetworkCapture = !EnableNetworkCapture;
        if (EnableNetworkCapture)
            $("#btnStartStop_text").text("Stop");
        else
            $("#btnStartStop_text").text("Start");
    });

    $("#btnClear").click(function() {
        //clear profile table
        $("#profileTable").find("tr:gt(0)").remove();
        var table = $('#trafficTable').DataTable();
        table.clear().draw();
        last_profile = { timestamp: "", session: "", visitor: "", datahub_id: "", pii: "", event: "", row_num: "", touched: false };
    });
    $("#btnClearEventStream").click(function() {
        var table = $('#eventStreamTable').DataTable();
        table.clear().draw();
    });

    $("#btnStartStopEventStream").click(function() {

        toggleEventStreamBtn();
        if (StartEventStream) {
            makeToken();
            checkAccessPointConfig();
        }
    });

    $("#btnConfirm").click(function() {
        $("#divConfirmationBox").hide();
        connect360();
    });
    $("#btnDonotConnect").click(function() {
        $("#divConfirmationBox").hide();
        if (StartEventStream)
            toggleEventStreamBtn();
    });
    $("#btnApplyDiagnosticsFilter").click(function() {

        if (!ws || ws.readyState != 1) {
            connect360();
        } else
            update_diagnostics_filter();
    });


    addFunctionsforDataTables();

    initNetWorkTable();

    initEventStreamTable();

    //var tempstr = parstFormRules("");
    //var tempstr = content.substring(content.indexOf("var ff = {}") + 12, content.indexOf("return ff")).replaceAll("\n", "<br/>")

});

function toggleEventStreamBtn() {
    StartEventStream = !StartEventStream;
    $("#btnStartStopEventStream").toggleClass("btnStop_bg");
    if (StartEventStream)
        $("#btnStartStopEventStream").text("Stop");
    else {
        $("#btnStartStopEventStream").text("Start");
        $("#divAgentFilterBox").hide();
    }
    $("#divConfirmationBox").hide();
    if (ws && ws.readyState === 1) {
        log("Disconnecting.");
        ws.send("DIAGNOSTIC_AGENT: {\"operation\":\"clear\"}");
        ws.close();
        ws = null;
        $("#divAgentFilterBox").hide();
    }
}

function checkAccessPointConfig() {
    chrome.storage.sync.set({ tenantUrl: document.getElementById("tenantUrl").value }, function() {});
    chrome.storage.sync.set({ tenantId: document.getElementById("tenantId").value }, function() {});
    chrome.storage.sync.set({ clientSecret: document.getElementById("clientSecret").value }, function() {});
    var settings = {
        "url": "https://" + document.getElementById("tenantUrl").value + "/marketingGateway/configuration",
        "method": "GET",
        "timeout": 0,
        "headers": {
            "Authorization": "Bearer " + token
        },
    };

    $.ajax(settings).done(function(response) {
        if (response && response.agentName && response.type) {
            log("Access Point Name: " + response.agentName);
            log("Access Point Type: " + response.type);
            //response.type = "diag";
            if (response.type.toLowerCase() == "diag") {
                //it is safe to connect to this agent
                //ask for filter condition
                diagAgent = true;
                $("#divAgentFilterBox").show();

            } else {
                //we need to first warn the user that this is not a diagnostics agent and ask for confirmation.
                diagAgent = false;
                $("#divAgentFilterBox").hide();
                $("#divConfirmationBox").show();
            }

        } else {
            log("Unable to get CI 360 Access Point configurations. Please check the External gateway address, Tenant Id and Client Secret.")
            toggleEventStreamBtn();
        }

    }).fail(function(error) {
        log("Error connecting to CI 360 Access Point. Please check the External gateway address, Tenant Id and Client Secret.")
        log(JSON.stringify(error));
        toggleEventStreamBtn();
    });
}

function initNetWorkTable() {
    var table = $('#trafficTable').DataTable({
        deferRender: true,
        "pageLength": 50,
        order: [
            [4, "desc"]
        ],
        language: {
            search: "_INPUT_",
            searchPlaceholder: "Search..."
        },
        dom: 'Bfrtip',
        columns: [{
                "className": 'details-control',
                "orderable": false,
                "data": null,
                "defaultContent": ''
            },
            { data: 'event' },
            { data: 'event_json' },
            { data: 'eventname' },
            { data: 'timestamp' },
            { data: 'status' },
            { data: 'datahub_id' },
            { data: 'row_num' }


        ],
        "columnDefs": [{
                "targets": [2],
                "visible": false
            },

            {
                targets: [0],
                "width": "10px"
            },
            {
                targets: [5, 7],
                "width": "10px"
            }
        ]
    });

    // Add event listener for opening and closing details
    $('#trafficTable tbody').on('click', 'td.details-control', function() {
        var tr = $(this).closest('tr');
        var row = table.row(tr);

        if (row.child.isShown()) {
            // This row is already open - close it
            row.child.hide();
            tr.removeClass('shown');
        } else {
            // Open this row
            row.child(format(row.data())).show();
            tr.addClass('shown');
        }
    });

    // Setup - add a text input to each footer cell
    $('#trafficTable tfoot th').each(function() {
        var title = $(this).text();
        $(this).html('<input type="text" placeholder="Search ' + title + '" />');
    });

    // DataTable
    var table = $('#trafficTable').DataTable();

    // Apply the search
    table.columns().every(function() {
        var that = this;

        $('input', this.footer()).on('keyup change clear', function() {
            if (that.search() !== this.value) {
                that
                    .search(this.value)
                    .draw();
            }
        });
    });
}

function initEventStreamTable() {
    var table = $('#eventStreamTable').DataTable({
        deferRender: true,
        order: [
            [5, "desc"]
        ],
        language: {
            search: "_INPUT_",
            searchPlaceholder: "Search..."
        },
        dom: 'Bfrtip',
        buttons: [
            'copyHtml5',
            'excelHtml5',
            'csvHtml5'

        ],
        columns: [{
                "className": 'details-control',
                "orderable": false,
                "data": null,
                "defaultContent": ''
            },
            { data: 'eventName' },
            { data: 'event' },
            { data: 'datahub_id' },
            { data: 'event_json' },
            { data: 'timestamp' },
            { data: "sessionId" },
            { data: "vid" }

        ],
        "columnDefs": [{
                "targets": [4],
                "visible": false
            },
            {
                "targets": [0],
                "width": "10px"
            },
            {
                targets: 5,
                render: $.fn.dataTable.render.moment('X', 'DD MMM YYYY : hh:mm:ss.SSS')
            }
        ]
    });

    // Add event listener for opening and closing details
    $('#eventStreamTable tbody').on('click', 'td.details-control', function() {
        var tr = $(this).closest('tr');
        var row = table.row(tr);

        if (row.child.isShown()) {
            // This row is already open - close it
            row.child.hide();
            tr.removeClass('shown');
        } else {
            // Open this row
            row.child(format(row.data())).show();
            tr.addClass('shown');
        }
    });

    // Setup - add a text input to each footer cell
    $('#eventStreamTable tfoot th').each(function() {
        var title = $(this).text();
        $(this).html('<input type="text" placeholder="Search ' + title + '" />');
    });

    // DataTable
    var table = $('#eventStreamTable').DataTable();

    // Apply the search
    table.columns().every(function() {
        var that = this;

        $('input', this.footer()).on('keyup change clear', function() {
            if (that.search() !== this.value) {
                that
                    .search(this.value)
                    .draw();
            }
        });
    });
}

function addFunctionsforDataTables() {
    (function(factory) {
            "use strict";

            if (typeof define === 'function' && define.amd) {
                // AMD
                define(['jquery'], function($) {
                    return factory($, window, document);
                });
            } else if (typeof exports === 'object') {
                // CommonJS
                module.exports = function(root, $) {
                    if (!root) {
                        root = window;
                    }

                    if (!$) {
                        $ = typeof window !== 'undefined' ?
                            require('jquery') :
                            require('jquery')(root);
                    }

                    return factory($, root, root.document);
                };
            } else {
                // Browser
                factory(jQuery, window, document);
            }
        }
        (function($, window, document) {
            $.fn.dataTable.render.moment = function(from, to, locale) {
                // Argument shifting
                if (arguments.length === 1) {
                    locale = 'en';
                    to = from;
                    from = 'YYYY-MM-DD';
                } else if (arguments.length === 2) {
                    locale = 'en';
                }

                return function(d, type, row) {
                    var m = window.moment(d.slice(0, -3), from, locale, true);
                    // Order and type get a number value from Moment, everything else
                    // sees the rendered value
                    return m.format(type === 'sort' || type === 'type' ? 'x' : to);
                };
            };


        }));

}

function addToGrid(event_json, raw_json) {
    if (event_json.eventname === undefined || event_json.eventname === null)
        event_json.eventname = "";
    if (event_json.eventDesignedName === undefined || event_json.eventDesignedName === null)
        event_json.eventDesignedName = "";
    if (event_json.event === undefined || event_json.event === null) {
        if (event_json.login_event_type && event_json.login_event) {
            event_json.event = "*Inferred Event: attachIdentity";
        } else
            event_json.event = "unidentified event";
    }
    if (!event_json.datahub_id) {
        event_json.datahub_id = "";
    }


    if (!event_json.status || event_json.status === undefined || event_json.status === null)
        event_json.status = "";
    if (!event_json.timestamp || event_json.timestamp === undefined || event_json.timestamp === null)
        event_json.timestamp = "";
    if (!event_json.datahub_id || event_json.datahub_id === undefined || event_json.datahub_id === null)
        event_json.datahub_id = "";
    if (!event_json.row_num || event_json.row_num === undefined || event_json.row_num === null)
        event_json.row_num = "";


    let row = {
        "event": event_json.event,
        "event_json": decodeURIComponent(raw_json),
        "status": event_json.status,
        "eventname": decodeURIComponent(event_json.eventname) + (event_json.eventDesignedName != "" ? " / " + event_json.eventDesignedName : ""),
        "timestamp": event_json.timestamp,
        "datahub_id": event_json.datahub_id,
        "row_num": event_json.row_num
    }
    var t = $('#trafficTable').DataTable();
    t.row.add(row).draw(false);


    checkAndUpdateProfile(event_json);
}

function checkAndUpdateProfile(event_json) {
    //event_json.eventname.toLowerCase() == "identityevent"
    var current_profile = { timestamp: "", session: "", visitor: "", datahub_id: "", pii: "", event: "", row_num: "" };



    current_profile.timestamp = event_json.timestamp;
    current_profile.datahub_id = event_json.datahub_id;
    current_profile.session = event_json.session;
    current_profile.visitor = event_json.visitor;
    current_profile.event = event_json.event + " / " + event_json.eventname;
    current_profile.row_num = event_json.row_num;
    current_profile.pii = "";

    if (event_json.event && (event_json.event.toLowerCase() == "identityevent" || event_json.event.includes("Inferred Event: attachIdentity") || event_json.event.includes("Inferred Event: detachIdentity"))) {
        if ((event_json.login_event || event_json.User_ID_Attribute) && event_json.login_event_type)
            current_profile.pii = decodeURIComponent(event_json.login_event_type) + ": " + decodeURIComponent(event_json.login_event ? event_json.login_event : event_json.User_ID_Attribute);
        else current_profile.pii = " ";
    }


    if (!last_profile.touched) {
        //first request and hence current profile is the last profile. copy them.
        last_profile.touched = true;
        copyProfile(current_profile);
        addProfile();
    } else {

        if ((current_profile.session && current_profile.session != last_profile.session) ||
            (current_profile.visitor && current_profile.visitor != last_profile.visitor) ||
            (current_profile.datahub_id && current_profile.datahub_id != last_profile.datahub_id) ||
            (current_profile.pii && current_profile.pii != "" && current_profile.pii != last_profile.pii)
        ) {
            copyProfile(current_profile);
            addProfile();
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

    cell0.innerHTML = moment(last_profile.timestamp, 'x').format('DD MMM YYYY : hh:mm:ss.SSS');
    cell1.innerHTML = last_profile.session;
    cell2.innerHTML = last_profile.visitor;
    cell3.innerHTML = last_profile.datahub_id;
    cell4.innerHTML = last_profile.pii;
    cell5.innerHTML = last_profile.event;
    cell6.innerHTML = last_profile.row_num;
}

function copyProfile(current_profile) {
    last_profile.session = current_profile.session;
    last_profile.visitor = current_profile.visitor;
    last_profile.datahub_id = current_profile.datahub_id;
    last_profile.event = current_profile.event;
    last_profile.row_num = current_profile.row_num;
    last_profile.pii = current_profile.pii;
    last_profile.timestamp = current_profile.timestamp;
    update_diagnostics_filter();
}

function update_diagnostics_filter() {

    var attribute = "channel_id";
    var value = last_profile.visitor;
    var selectedOption = $("input:radio[name=diagnostics_filter]:checked").val()
    if (selectedOption == "session") {
        attribute = "session_id";
        value = last_profile.session;
    } else if (selectedOption == "datahub_id") {
        attribute = "datahub_id";
        value = last_profile.datahub_id;
    } else if (selectedOption == "custom") {
        attribute = $("#diagnostics_filter_custom_attr :selected").val();;
        value = document.getElementById("diagnostics_filter_custom_fixed").value;
    }
    $("#divfiltervalues").html("Current " + attribute + " value is: " + value);
    if (ws) {
        message = "DIAGNOSTIC_AGENT: {\"operation\":\"filter\",\"attribute\":\"" + attribute + "\", \"value\":\"" + value + "\"}";
        log("Sending filter condition: " + message)
        send(message);
        send("DIAGNOSTIC_AGENT: {\"operation\":\"show\"}");
    }
}

function format(d) {
    // `d` is the original data object for the row
    return '<table cellpadding="5" cellspacing="0" border="0" style="padding-left:50px;background-color: rgb(12, 63, 97)">' +
        '<tr>' +

        '<td>' + syntaxHighlight(d.event_json) + '</td>' +
        '</tr>' +
        '</table>';
}

function syntaxHighlight(json) {
    if (typeof json != 'string') {
        json = JSON.stringify(json, undefined, 2);
    }
    json = json.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    var str = json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, function(match) {
        var cls = 'number';
        if (/^"/.test(match)) {
            if (/:$/.test(match)) {
                cls = 'key';
            } else {
                cls = 'string';
            }
        } else if (/true|false/.test(match)) {
            cls = 'boolean';
        } else if (/null/.test(match)) {
            cls = 'null';
        }
        if (cls == 'key') {
            return '<br/><span class="' + cls + '">' + match + '</span>';
        } else
            return '<span class="' + cls + '">' + match + '</span>';

    });

    return str;
}

var coll = document.getElementsByClassName("collapsible");
var i;

for (i = 0; i < coll.length; i++) {
    coll[i].addEventListener("click", function() {
        this.classList.toggle("active");
        var content = this.nextElementSibling;
        debugger;
        var open_icon = this.firstElementChild.firstElementChild.firstElementChild.firstElementChild.nextElementSibling; //document.getElementById("open_icon");
        var close_icon = this.firstElementChild.firstElementChild.firstElementChild.firstElementChild; //document.getElementById("close_icon");
        if (content.style.display === "block") {
            content.style.display = "none";
            close_icon.style.display = "none";
            open_icon.style.display = "block";
        } else {
            content.style.display = "block";
            close_icon.style.display = "block";
            open_icon.style.display = "none";
        }
    });
}

function openPage(pageName, elmnt, color) {
    var i, tabcontent, tablinks;
    tabcontent = document.getElementsByClassName("tabcontent");
    for (i = 0; i < tabcontent.length; i++) {
        tabcontent[i].style.display = "none";
    }
    tablinks = document.getElementsByClassName("tablink");
    for (i = 0; i < tablinks.length; i++) {
        tablinks[i].style.backgroundColor = "";
        tablinks[i].classList.remove("tabActive");
    }
    document.getElementById(pageName).style.display = "block";
    elmnt.classList.add("tabActive");
}

function url_domain(data) {
    var a = document.createElement('a');
    a.href = data;
    return a.hostname;
}
var clear_entries = true;
chrome.devtools.network.onNavigated.addListener(
    function(url) {
        clear_entries = true;
        $("#current_domain").html(url_domain(url));
        $("#scripts_loaded").html("<li>No CI 360 related scripts are loaded yet. Please ensure that the site is tagged.</li>");
        $("#generic_collection_rules").html("");
        $("#form_collection_rules").html("");
    }
);
chrome.devtools.network.onRequestFinished.addListener(
    function(request) {

        if (request.request && request.request.method == "GET") {
            if (request.request.url.includes("ot-min") || request.request.url.includes("ot-api") || request.request.url.includes("ot-all")) {
                if (clear_entries) {
                    clear_entries = false;
                    $("#scripts_loaded").html("");
                }
                $("#scripts_loaded").append("<li>" + request.request.url + "</li>");
            } else if (request.request.url.includes("/t/s/c/")) {
                //loading configurations
                $("#tenant_id").html(request.request.url.split("/")[6].split("?")[0]);
                if (!$("#tenantId").val() || $("#tenantId").val().trim().length < 1)
                    $("#tenantId").val(request.request.url.split("/")[6].split("?")[0]);

                request.getContent(function(content, encoding) {

                    if (content && content.length > 0) {


                        if (content.includes("collect['clicks']"))
                            $("#generic_collection_rules").append("<li>" + content.match("'clicks'(.*);")[0].replaceAll("'", "").replaceAll("]", "") + "</li>");
                        if (content.includes("collect['contentevents']"))
                            $("#generic_collection_rules").append("<li>" + content.match("'contentevents'(.*);")[0].replaceAll("'", "").replaceAll("]", "") + "</li>");
                        if (content.includes("collect['fieldinteractions']"))
                            $("#generic_collection_rules").append("<li>" + content.match("'fieldinteractions'(.*);")[0].replaceAll("'", "").replaceAll("]", "") + "</li>");
                        if (content.includes("collect['formsubmits']"))
                            $("#generic_collection_rules").append("<li>" + content.match("'formsubmits'(.*);")[0].replaceAll("'", "").replaceAll("]", "") + "</li>");
                        if (content.includes("collect['jsvars']"))
                            $("#generic_collection_rules").append("<li>" + content.match("'jsvars'(.*);")[0].replaceAll("'", "").replaceAll("]", "") + "</li>");
                        if (content.includes("collect['media']"))
                            $("#generic_collection_rules").append("<li>" + content.match("'media'(.*);")[0].replaceAll("'", "").replaceAll("]", "") + "</li>");
                        if (content.includes("collect['mouseovers']"))
                            $("#generic_collection_rules").append("<li>" + content.match("'mouseovers'(.*);")[0].replaceAll("'", "").replaceAll("]", "") + "</li>");
                        if (content.includes("collect['pageloads']"))
                            $("#generic_collection_rules").append("<li>" + content.match("'pageloads'(.*);")[0].replaceAll("'", "").replaceAll("]", "") + "</li>");

                        if (content.includes("com_sas_ci_acs._ob_configure.prototype.getFormConfig")) {

                            var tempstr = content.substring(content.indexOf("var ff = {}") + 12, content.indexOf("return ff")).replaceAll("\n", "<br/>")
                            $("#form_collection_rules").append(tempstr);
                        }

                    } else {
                        $("#generic_collection_rules").html("<li class='alert alert-warning'>The domain is either not approved or not activated in CI 360.</li>");
                    }
                });

            }
        }
        if (!EnableNetworkCapture)
            return;
        if (request.request.method == "POST"

            &&
            (
                (
                    request.request &&
                    (request.request.url.includes("/t/e/") || request.request.url.includes("/t/s/"))
                ) ||
                (
                    request._initiator.stack &&
                    request._initiator.stack.parent &&
                    (
                        request._initiator.stack.parent.callFrames[0].url.includes("/t/e") ||
                        request._initiator.stack.parent.callFrames[0].url.includes("ot-min") ||
                        request._initiator.stack.parent.callFrames[0].url.includes("ot-api") ||
                        request._initiator.stack.parent.callFrames[0].url.includes("ot-all")
                    )
                )
            )
        ) {
            var req = JSON.stringify(request, null, 4);
            if (request.request.postData && request.request.postData.params) {
                var params = "{";
                var param_raw = "{";
                var first = 0;
                params = params + "\"status\": \"" + (request.response.status == "0" ? "0 (cancelled)" : request.response.status) + "\"";
                params = params + ",\"row_num\": \"" + (row_num) + "\"";
                row_num = row_num + 1;
                for (var i = 0; i < request.request.postData.params.length; i++) {
                    if (first == 0) {
                        param_raw = param_raw + "\"" + request.request.postData.params[i].name + "\": \"" + request.request.postData.params[i].value + "\"";
                    } else {
                        param_raw = param_raw + ",\"" + request.request.postData.params[i].name + "\": \"" + request.request.postData.params[i].value + "\"";
                    }

                    params = params + ",\"" + request.request.postData.params[i].name + "\": \"" + request.request.postData.params[i].value + "\"";
                }

                params = params + "}";
                param_raw = param_raw + "}";

                var obj = JSON.parse(params)
                    //obj.timestamp = moment(request.startedDateTime).format('x');
                obj.timestamp = request.startedDateTime;
                addToGrid(obj, param_raw);
            }
        } else if ((request.request.method == "OPTIONS" || request.request.method == "DELETE")

            &&
            (
                (
                    request.request &&
                    (request.request.url.includes("/t/e/") || request.request.url.includes("/t/s/"))
                ) ||
                (
                    request._initiator.stack &&
                    request._initiator.stack.parent &&
                    (
                        request._initiator.stack.parent.callFrames[0].url.includes("/t/e") ||
                        request._initiator.stack.parent.callFrames[0].url.includes("ot-min") ||
                        request._initiator.stack.parent.callFrames[0].url.includes("ot-api") ||
                        request._initiator.stack.parent.callFrames[0].url.includes("ot-all")
                    )
                )
            )
        ) {
            var params = "{";
            var param_raw = "{";
            var first = 0;
            params = params + "\"status\": \"" + (request.response.status == "0" ? "0 (cancelled)" : request.response.status) + "\"";
            params = params + ",\"event\": \"" + "*Inferred Event: detachIdentity (method: " + request.request.method + ")" + "\"";
            params = params + ",\"row_num\": \"" + (row_num) + "\"";
            row_num = row_num + 1;


            params = params + "}";
            param_raw = param_raw + "}";

            var obj = JSON.parse(params)
            obj.timestamp = moment(request.startedDateTime).format('x');
            addToGrid(obj, param_raw);
        }
    });

function parstFormRules(content) {
    $.get('/js.txt', // url
        function(data, textStatus, jqXHR) { // success callback
            content = data;
            content = content.substring(content.indexOf("var ff = {}") + 12, content.indexOf("return ff")).split("\n");

            var j = 0;
            for (var i = 0; i < content.length; i++) {
                for (; j < content.length; j++) {
                    var line = content[j];
                    line = line.trim();
                    if (line && line.trim().length > 0) {
                        if (line.startsWith("ff['ign'][" + i + "]"))
                            console.log(line);
                        else if (line == "ff['nature'] = 'inc';" || line == "ff['nature'] = 'inc';" || line == "ff['obs'] = [];" || line == "ff['obs'] = [];" || line == "ff['ign'] = [];" || line == "ff['inc'] = [];" || line == "ff['nature'] = 'ign';")
                            continue;
                        else {
                            console.log("next");
                            break;
                        }
                    }
                }
            }
            $("#form_collection_rules").append(content);

            return content;
        });

}

function makeToken() {
    tenantUrl = "wss://" + document.getElementById("tenantUrl").value + "/marketingGateway/agent/stream";
    tenantId = document.getElementById("tenantId").value;
    clientSecret = document.getElementById("clientSecret").value;
    var header = { alg: 'HS256', typ: 'JWT' };
    var payload = { clientID: tenantId };
    token = KJUR.jws.JWS.sign("HS256", JSON.stringify(header), JSON.stringify(payload), btoa(clientSecret));
}


function log(msg) {
    var logdiv = document.getElementById("eventStreamConsole");
    logdiv.innerHTML = msg + "<br/>" + logdiv.innerHTML;
}

function connect360() {
    ws = new WebSocket(tenantUrl);
    ws.onerror = function(event) {
        log("Error: " + event.data);
    }
    ws.onopen = function(event) {

        log("Opening Connection to SAS CI 360 Event Stream.");
        if (diagAgent) {
            ws.send(token + "\n" + "diag1(v2009)");


        } else {
            ws.send(token + "\n" + "sdk3(v2009)");
        }
    };

    ws.onmessage = function(event) {
        if (firstMessage) {
            firstMessage = false;
            log("Ready to receive events.");

            if (diagAgent) {
                update_diagnostics_filter();
            }
        } else {
            if (event.data && event.data.includes("ping"))
                log("Message: ping.");
        }

        if (event.data && event.data.includes("rowKey"))
            addToGrid_eventStream(JSON.parse(event.data), event.data);
        send("ack");
    }
    ws.onclose = function(event) {
        log("Connection closed.");
        if (StartEventStream)
            toggleEventStreamBtn();
        $("#divAgentFilterBox").hide();
        $("#divConfirmationBox").hide();

    };
}

function addToGrid_eventStream(event_json, raw_json) {
    if (!event_json || !event_json.attributes) {
        return;
    }

    if (!event_json.attributes.eventName || event_json.attributes.eventName === undefined || event_json.attributes.eventName === null)
        event_json.attributes.eventName = "";
    if (!event_json.attributes.internalTenantId || event_json.attributes.internalTenantId === undefined || event_json.attributes.internalTenantId === null)
        event_json.attributes.internalTenantId = "";

    if (!event_json.attributes.event || event_json.attributes.event === undefined || event_json.attributes.event === null)
        event_json.attributes.event = "";
    if (!event_json.attributes.datahub_id || event_json.attributes.datahub_id === undefined || event_json.attributes.datahub_id === null)
        event_json.attributes.datahub_id = "";
    if (!event_json.attributes.sessionId || event_json.attributes.sessionId === undefined || event_json.attributes.sessionId === null)
        event_json.attributes.sessionId = "";
    if (!event_json.rowKey || event_json.rowKey === undefined || event_json.rowKey === null)
        event_json.rowKey = "";
    if (!event_json.attributes.timestamp || event_json.attributes.timestamp === undefined || event_json.attributes.timestamp === null)
        event_json.attributes.timestamp = "";
    if (!event_json.attributes.channel_user_id || event_json.attributes.channel_user_id === undefined || event_json.attributes.channel_user_id === null)
        event_json.attributes.channel_user_id = "";
    let row = {
        "eventName": event_json.attributes.eventName,
        "internalTenantId": event_json.attributes.internalTenantId,
        "event": event_json.attributes.event,
        "datahub_id": event_json.attributes.datahub_id,
        "attributes": JSON.stringify(event_json.attributes, undefined, 2),
        "event_json": JSON.stringify(event_json, undefined, 2),
        "sessionId": event_json.attributes.sessionId,
        "rowKey": event_json.rowKey,
        "timestamp": event_json.attributes.timestamp,
        "vid": event_json.attributes.channel_user_id

    }
    var t = $('#eventStreamTable').DataTable();
    t.row.add(row).draw(false);
    log("Event recieved: " + event_json.attributes.eventName);
}

function send(message, callback) {
    waitForConnection(function() {
        ws.send(message);
        if (typeof callback !== 'undefined') {
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
        setTimeout(function() {
            that.waitForConnection(callback, interval);
        }, interval);
    }
}