var lastItemId=0;
var lastJson={};
var version_text = "Version 1.03 - last updated 2020-11-24";
var config = loadSettingsFromBrowserStorage();

/***  recent change: clean up api helper
****/

function initializeApp() {
	$.ajaxSetup({ cache: false });
	// $.fn.selectpicker.Constructor.BootstrapVersion = '4';

    console.log("initApp");
     
    $('#tab-ext-api-ci360').hide();
    $('#tab-datahub-api').hide();
    $('#tab-gdpr-api').hide();
    
    initTabHome();
    initTabEventAPI();
    initTabDatahubAPI();
    
}

function initTabHome() {
    $('#ci360ApiHelperConfigDropDown').html("");
    if (config[0] != undefined) {
        $('#ci360ApiHelperConfig').show();
        var option = "<option value='' > -- select one of your configs -- </option>";
        config.forEach(function(element) {
            //console.log("name: " + element.name + "   -type: " + element.type);
            option += '<option value="' + element.tenantId + '" >' + element.tenantName + '</option>';
        });
        $('#ci360ApiHelperConfigDropDown').html(option);
    }

	$('#password').keypress(function(e) {
        var keycode = (e.keyCode ? e.keyCode : e.which);
        if (keycode == '13') {
            btnCreateToken(this);
            //alert('You pressed enter! - keypress');
        }
    });
	$('#ci360restUrl').val($('#ci360restUrlDropDown').val());
}

function initTabEventAPI() {
	//var data = {};
    //data.eventName = "eventName";data.eventValue="Contact Event";
    //addCi360Attribute(data);
    //data = {};data.eventName = "subject_id";data.eventValue="372";
    //addCi360Attribute(data);
    //data = {};data.eventName = "contactType";data.eventValue="mypush";
    //addCi360Attribute(data);
}

function initTabDatahubAPI() {
	$('.selectpicker').select2({
        tags: true,
        width: '100%'
      });
}

function btnCreateToken() {
    $('.tenantDetails').hide();
    $('.loginwrong').hide();
    $('#btn_verifyLogin').hide();
    $('#imgLoad_verifyLogin').show();
    var token = createToken($('#password').val(),$('#username').val())
    if (token.includes("ERROR")) {
        $('.loginwrong').show();
        $('#imgLoad_verifyLogin').hide();
        $('#btn_verifyLogin').show();
    } else {
        $('#token').html(token);
        $('.tenantDetails').show();
        $('#btn_verifyLogin').show();
        $('#imgLoad_verifyLogin').hide();
        $('#tab-datahub-api').show();
        $('#tab-ext-api-ci360').show();
        $('#tab-gdpr-api').show();
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
        var header = {alg: 'HS256', typ: 'JWT'};
        var payload = { clientID: tenantText };
        token = KJUR.jws.JWS.sign("HS256", JSON.stringify(header), JSON.stringify(payload), btoa(secretText));
    }
    
    return token;
}

function sendExternalEvent() {
    var endpoint = "ci360";
    console.log("send event to: ", endpoint);
    $('#'+endpoint+'ResponseDiv').hide();
    $('#'+endpoint+'SendEventBtn').hide();    
    $('#'+endpoint+'LoaderImage').show();
    var url = "https://" + $('#ci360restUrl').val() + "/marketingGateway/events";
    console.log(endpoint+" url: " + url);
    
    var input = {};
    input.token = $('#token').html();

    var mergedInputs = {};
    input = mergeObjects(input, getAttributes(endpoint));

    console.log("json object for external event: ", input);

    $('#'+endpoint+'response').val("");
    callApi(url,input).error(function (response, error) {
        console.log("error response: ", response);
        $('#'+endpoint+'Response').val(JSON.stringify(response.responseJSON,null,4));
        if(error == 'error') {
            $('#'+endpoint+'response').val('Error occured - maybe connection refused - Hit F12 to see developers console');
        } else {
            $('#'+endpoint+'response').val(response.responseJSON.message);
            console.log(response.responseJSON.message);
        }
        if (response.statusText == "error") {
            $('#'+endpoint+'Response').val(response.responseText);
            /*$('#'+endpoint+'Response').val("error - please open the javascript console to see details"
                +"\nStart Google Chrome in unsecure mode to avoid CORS (Access-Control-Allow-Origin) issues"
                +"\nUse following command to start Chrome:"
                +"\n   chrome.exe https://www.cidemo.sas.com/apihelper --disable-web-security --user-data-dir"
            );*/
        }
        $('#'+endpoint+'ResponseDiv').show();
        $('#'+endpoint+'LoaderImage').hide();
        $('#'+endpoint+'SendEventBtn').show();
    }).success(function (response) {
        console.log("success response: ", response);
        $('#'+endpoint+'Response').val(JSON.stringify(response,null,4));
        console.log(response);
        $('#'+endpoint+'ResponseDiv').show();
        $('#'+endpoint+'LoaderImage').hide();
        $('#'+endpoint+'SendEventBtn').show();
    });

}

function addCi360Attribute(dataItem) {
    if (dataItem != undefined) {
        dataItem.endpoint = 'ci360';
        $("#ci360EventAttributes").append(htmlTemplates.templateAttrItem(dataItem));
    } else {
        var data = {};
        data.endpoint = 'ci360';
        data.eventName = "";
        data.eventValue = "";
        $("#ci360EventAttributes").append(htmlTemplates.templateAttrItem(data));
    }
}

function setModal(title, body, footer, jsonObject) {
    $('#jsonOutput').hide();

    $('#modal_title').html(title);
    $('#modal_body').html(body);
    if (jsonObject != undefined) {
        $('#jsonOutput').val(JSON.stringify(jsonObject,null,2));
        $('#jsonOutput').show();
    }
    
    $('#modal_footer').html(footer);
    $('#myModal').modal('show');
}

function updateModal(jsonObject) {
	console.log('updateModal: ', jsonObject);
    if (jsonObject != undefined) {
        $('#jsonOutput').val(JSON.stringify(jsonObject,null,2));
        $('#jsonOutput').show();
    }
    $('#imgLoad_modal').hide();
}

function removeSpaces(element) {
    var val = $(element).val();
    var newval = val.replace(/\s/g, '');
    $(element).val(newval);
}

function removeAttr(elem) {
    console.log(elem.parentElement);
    elem.parentElement.parentElement.remove();
}

function getAttributes(endpoint) {
    var attrNames = $('.'+endpoint+'attrName');
    var attrValues = $('.'+endpoint+'attrValue');
    var attributes = {};
    for (i=0; i < attrNames.length;i++) {
        attributes[attrNames[i].value] = attrValues[i].value;
    }
    return attributes;
}

function mergeObjects(obj1, obj2) {
    var result = {};
    for(var key in obj1) result[key] = obj1[key];
    for(var key in obj2) result[key] = obj2[key];
    return result;
}

function callApi(url, parameters) {
    var headers = {"Accept": "application/json", 'X-Requested-With': 'XMLHttpRequest'};
    if (parameters.token) {
        headers.Authorization = "Bearer " + parameters.token;
    }
    //console.log("headers: ", headers);
	return $.ajax(url, {
		type: 'POST',
        contentType: "application/json",
        headers: headers,
		data: JSON.stringify(parameters)
	} );
}

function callProxyAPI(settings, action) {
    settings.action = action;
    settings.email = $('#username').val();
    var settingsForProxy = {
        "url": "./api/",
        "method": "POST",
        "headers": {"content-type": "application/json"},
        "data": JSON.stringify(settings)
    };

    return $.ajax(settingsForProxy);
}

function btnGetExternalEvents() {
    $('#imgLoad_getexternalevents').show();
    $('#btn_GetExternalEvents').hide();
    var api_user = $('#api_user').val();
    var api_secret = $('#api_user_secret').val();
    var designUrl = "https://" + $('#ci360DesignCenterUrl').val() + "/SASWebMarketingMid/rest/events";
    console.log("design url: ", designUrl);

    var settings = {
        "url": designUrl,
        "method": "GET",
        "headers": {
            "contentType": "application/json",
            "authorization": "Basic " + btoa(api_user + ":" + api_secret)
        }
      };
  
    callProxyAPI(settings, "designServerCall").done(function (response) {
        console.log("designServerCall response: ",response.json.items);
        var option = "<option value='' > --- Select Event --- </option> ";
        var i=0;
        response.json.items.sort(function(a, b) { 
            if ( a.name.toLowerCase() < b.name.toLowerCase() ) return -1;
            if ( a.name.toLowerCase() > b.name.toLowerCase() ) return 1;
            return 0;
        });

        response.json.items.forEach(function(element) {
            i++;
            if (element.type == "external") {
                option += '<option value="' + element.id + '" >' + element.name + '</option>';
            }
        });
        $('#ci360ExternalEvents').html(option);
        $('#ci360ExternalEventsDropDown').show();
        $('#imgLoad_getexternalevents').hide();
        $('#btn_GetExternalEvents').show();
    });
}

function getExternalEventAttributes(element) {
    var eventid = $('#ci360ExternalEvents').val();
    var eventname = $('#ci360ExternalEvents option:selected').text();
    var api_user = $('#api_user').val();
    var api_secret = $('#api_user_secret').val();
    var designUrl = "https://" + $('#ci360DesignCenterUrl').val() + "/SASWebMarketingMid/rest/events/"+eventid;
    console.log("design url: ", designUrl);

    var settings = {
        "url": designUrl,
        "method": "GET",
        "headers": {
            "contentType": "application/json",
            "authorization": "Basic " + btoa(api_user + ":" + api_secret)
        }
      };
    //callProxyAPI(settings, "designServerCall").done(function (response) {
        //console.log("getExternalEventAttributes response: ",response);
        $('#ci360EventAttributes').html('');
        //addCi360Attribute({"eventName":"eventName", "eventValue":eventname});
        $('#ci360EventName').val(eventname);
        //addCi360Attribute({"eventName":"subject_id", "eventValue":"372"});
    //});
}


function openAbout() {
    setModal('<img src="images/logo.png">'
            ,'This application has been developed by the GPCI <br><br>'
            +'If you have questions or comments please '
            +'<b><a href="mailto:rob.sneath@sas.com;mathias.bouten@sas.com?Subject=CI360APIHelper%20Question" target="_top">contact us via email</a><b>!'
            ,'<span class="mr-auto">'+ version_text + '</span>'
            +'<button type="button" class="btn btn-secondary" data-dismiss="modal">Close</button>'
        );
}

/**** browser storage functions */

function loadSettingsFromBrowserStorage() {
    return JSON.parse(localStorage.getItem('CI360ApiHelper') ? localStorage.getItem('CI360ApiHelper') : "{}");
}

function onChangeCi360ApiHelperConfig(element) {
    var tenantId = $('#ci360ApiHelperConfigDropDown').val();
    config.forEach(function(element) {
        if (element.tenantId == tenantId) {
            $('.tenantApiGateway').val(element.tenantApiGateway);
            $('.tenantId').val(element.tenantId);
            $('.tenantSecret').val(element.tenantSecret);
            $('.tenantCi360Url').val(element.tenantCi360Url);
            $('.tenantApiUser').val(element.tenantApiUser);
            $('.tenantApiSecret').val(element.tenantApiSecret);
        } 
    });
    $('#tab-ext-api-ci360').hide();
    $('#tab-datahub-api').hide();
    $('#tab-gdpr-api').hide();
    $('.tenantDetails').hide();
    $('.loginwrong').hide();
    $('#token').html("");
}