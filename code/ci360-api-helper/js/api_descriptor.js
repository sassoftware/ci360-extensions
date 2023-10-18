var fileContent = "";

function getDescriptors(element) {
    $('#accordion').html("");
    $('#tableHeader').html("");
    showLoaderIconAfterButtonClick (element);
    //$('#btn_getDescriptors').hide();
    //$('#imgLoad_getDescriptors').show();
    $('.descriptor_buttons').hide();
    $('.descriptor_details').hide();

    var settings = {
        "async": true,
        "crossDomain": true,
        "url": "https://" + $('#ci360restUrlDropDown').val() + "/marketingData/tables/?limit=1000",
        "method": "GET",
        "headers": {
            "authorization": "Bearer " + $('#token').html(),
            "cache-control": "no-cache"
        }
    }

    $('#descriptors').html("");

    /*$.ajax(settings)*/

    callProxyAPI(settings, "getDescriptors").done(function (proxyResponse) {
        var response = proxyResponse.json;

        if (response != null && response.hasOwnProperty('items')) {
            //Sort descriptors by name  
            response.items.sort(function (a, b) {
                return a.name.localeCompare(b.name);
            });

            var count = 0;
            // loop through retrieved descriptor names
            for (var i = 0; i < response.items.length; i++) {

                var name = response.items[i].name;
                var id = response.items[i].id;
                var optionHtml = "";
                // check filter: if descriptor names contains brackets then it is an importedList and don't show it
                if ($('#radioAll').prop('checked')) {
                    var optionHtml = "<option id='desc_" + id + "' value='" + id + "' name='" + name + "'>" + name + "</option>";
                    count = count + 1;
                } else if ($('#radioOnlyImportedList').prop('checked') && name.includes('(')) {
                    var optionHtml = "<option id='desc_" + id + "' value='" + id + "' name='" + name + "'>" + name + "</option>";
                    count = count + 1;
                } else if ($('#radioNoImportedList').prop('checked') && name.includes('(') == false) {
                    var optionHtml = "<option id='desc_" + id + "' value='" + id + "' name='" + name + "'>" + name + "</option>";
                    count = count + 1;
                }
                $('#descriptors').append(optionHtml);
                $('.descriptor_buttons').show();
                $('.descriptor_details').show();
                //$('.separator-line').show();
                $('.selectpicker').select2({ tags: true, width: '100%' });
            }


            $('#numberDescriptors').html("Count: " + count);
            getDescriptorDetails();
        } else {
            setModal('Error'
                , 'Something went wrong, see response below:<br><br>'
                + JSON.stringify(proxyResponse)
                , '<button type="button" class="btn btn-secondary btn-sm" data-dismiss="modal">Close</button>'
            );
            $('#myModal').modal('show');
        }
        //$('#btn_getDescriptors').show();
        //$('#imgLoad_getDescriptors').hide();
        hideLoaderIconAndShowButton (element);
    });
}


function getDescriptorDetails() {
    $('.descriptor_buttons').hide();
    $('.descriptor_details').hide();
    //$('#btn_getDescriptors').hide();
    $('#btn_getDescriptors').attr("disabled",true);
    //$('#imgLoad_getDescriptors').show();

    $('#accordion').html("");
    var data = {};
    $("#tableHeader").html(htmlTemplates.templateTableHeader(data));
    var descriptorid = $('#descriptors').val();
    console.log('selected id: ' + descriptorid);
    var settings = {
        "async": true,
        "crossDomain": true,
        "url": "https://" + $('#ci360restUrlDropDown').val() + "/marketingData/tables/" + descriptorid,
        "method": "GET",
        "headers": {
            "authorization": "Bearer " + $('#token').html(),
            "cache-control": "no-cache"
        }
    }

    //$.ajax(settings).
    callProxyAPI(settings, "getDescriptorDetails").done(function (response) {
        response = response.json;
        $('#jsonOutput').val(JSON.stringify(response, null, 2));
        lastJson = response;
        loadDescriptorDetailsIntoGui(response);
        $('.descriptor_buttons').show();
        $('.descriptor_details').show();
        $('#btn_getDescriptors').attr("disabled",false);
        //$('#btn_getDescriptors').show();
        //$('#imgLoad_getDescriptors').hide();
    });
}

function showRetrievedDescriptorJSON() {
    setModal('JSON Response'
        , 'Following JSON object was retrieved by CI360:<br>'
        , '<button type="button" class="btn btn-secondary btn-sm" data-dismiss="modal">Close</button>'
        , lastJson
    );
}

function loadDescriptorDetailsIntoGui(response) {
    lastItemId = 0;
    $('#accordion').html("");
    $('#tableHeader').html("");
    newTable(response);
    $('.existingDescriptor').show();
    //console.log("load descriptor details into GUI:", response);
}

function newTable(table) {
    $("#accordion").html("");
    lastItemId = 0;
    if (table != undefined) {
        $('.descriptor_buttons').show();
        $('.descriptor_details').show();
        var createdTS = table.createdTimeStamp.replace('Z', '').replace('T', ' ');
        var modifiedTS = table.modifiedTimeStamp.replace('Z', '').replace('T', ' ');
        table.createdTimeStamp = createdTS;
        table.modifiedTimeStamp = modifiedTS;
        $("#tableHeader").html(htmlTemplates.templateTableHeader(table));
        $("#type").val(String(table['type']));
        $("#makeAvailableForTargeting").val(String(table['makeAvailableForTargeting']));

        for (i = 0; i < table.dataItems.length; i++) {
            addDataItem(table.dataItems[i]);
        }
        $('.table-buttons').show();
    } else {
        //$('.descriptor_buttons').hide();
        $('.descriptor_details').show();
        $('#descriptors').text('');
        var data = {};
        data.name = "";
        data.description = "";
        data.type = "transient";
        data.makeAvailableForTargeting = false;
        $('#descriptors').val('');
        $("#tableHeader").html(htmlTemplates.templateTableHeader(data));
        $("#type").val(String(data['type']));
        $("#makeAvailableForTargeting").val(String(data['makeAvailableForTargeting']));
        $('.table-buttons').show();
    }
    //$('.separator-line').show();
    $('.existingDescriptor').hide();
}

function addDataItem(dataItem) {
    lastItemId = lastItemId + 1;
    //console.log("[addDataItem] new item id: " + lastItemId);

    if (dataItem != undefined) {
        var data1 = {}; data1.itemid = lastItemId;
        dataItem.itemid = lastItemId;
        $("#accordion").append(htmlTemplates.templateDataItem(data1));
        for (prop in dataItem) {
            $("[name='" + prop + "']").last().val(String(dataItem[prop]));
        }
    } else {
        var data = {};
        data.itemid = lastItemId;
        $("#accordion").append(htmlTemplates.templateDataItem(data));
        data.name = "";
        data.label = data.name;
        data.type = "STRING";
        data.tags = "DEMOGRAPHICS";
        data.key = false;
        data.identity = "false";
        data.identityType = "";
        data.identityAttribute = "false";
        data.uniqueValuesAvailable = false;
        data.excludeFromAnalytics = "false";
        data.segmentProfilingField = "true";
        data.channelContactInformation = "true";
        data.segmentation = "true";
        data.availableForTargeting = "false";

        for (prop in data) {
            $("[name='" + prop + "']").last().val(String(data[prop]));
        }
    }
}

function createTable() {
    var jsonData = $('#jsonOutput').val();
    var settings = {
        "async": true,
        "crossDomain": true,
        "url": "https://" + $('#ci360restUrlDropDown').val() + "/marketingData/tables/",
        "method": "POST",
        "headers": {
            "content-type": "application/json",
            "authorization": "Bearer " + $('#token').html(),
            "cache-control": "no-cache"
        },
        "processData": false,
        "data": jsonData
    }


    /*$.ajax(settings)*/
    setModal('Create Table'
        , 'Response from Datahub API <br><br>'
        //+ JSON.stringify(response,null,2)
        + '<img src="./images/ajax_loader_green_32.gif" id="imgLoad_modal">'
        , '<button type="button" class="btn btn-secondary btn-sm" data-dismiss="modal">Close</button>'
    );


    callProxyAPI(settings, "createDescriptor").done(function (response) {
        console.log('response from ci360: ', response);
        var responseString = JSON.stringify(response.resp, null, 2);
        if (response.json != null) {
            responseString = JSON.stringify(response.json, null, 2);;
        }

        console.log(JSON.stringify(response, null, 2));
        /*setModal('Create Table'
            ,'Response from Datahub API <br><br>'
            //+ JSON.stringify(response,null,2)
            + responseString
            ,'<button type="button" class="btn btn-secondary" data-dismiss="modal">Close</button>'
        );*/
        updateModal(response.json);
    });
}

function updateTable() {
    console.log('[updateDescriptor] trying to update descriptor');
    var descriptor = $('#descriptors').val();
    var descriptorName = $('#desc_' + descriptor).attr('name');
    var basic_auth = "Basic " + btoa($('#username').val() + ":" + $('#password').val());
    // old endpoint for descriptors
    //var patch_url = "https://" + $('#ci360restUrlDropDown').val() + "/SASWebMarketingMid/rest/descriptors/" + descriptor + "?_method=PATCH";
    
    // new endpoint
    var patch_url = "https://" + $('#ci360restUrlDropDown').val() + "/marketingData/tables/" + descriptor;
    
    var jsonData = $('#jsonOutput').val();
    var settings = {
        "async": true,
        "crossDomain": true,
        "url": patch_url,
        "method": "PATCH",
        "headers": {
            "content-type": "application/json",
            "authorization": "Bearer " + $('#token').html(),
            "cache-control": "no-cache"
        },
        "processData": false,
        "data": jsonData
    }
    console.log('[updateDescriptor] settings object: ');
    console.log(settings);

    /*$.ajax(settings)*/
    setModal('Update Descriptor'
        , 'Response from Datahub API <br><br>'
        //+ JSON.stringify(response,null,2)
        + '<img src="./images/ajax_loader_green_32.gif" id="imgLoad_modal">'
        , '<button type="button" class="btn btn-secondary btn-sm" data-dismiss="modal">Close</button>'
    );


    /*updateDescriptor(patch_url, basic_auth, jsonData).done(function (response) {
        console.log('[updateDescriptor] direct call - response from ci360: ');
        console.log(response);
        updateModal(response);
    });*/

    callProxyAPI(settings, "updateDescriptor").done(function (response) {
        console.log('[updateDescriptor] response from ci360: ', response);
        var responseString = JSON.stringify(response.resp, null, 2);
        if (response.json != null) {
            responseString = JSON.stringify(response.json, null, 2);;
        }

        console.log(JSON.stringify(response, null, 2));
        updateModal(response.json);
    });
}

function updateDescriptor(url, basic_auth, jsonString) {
    console.log('[updateDescriptor] direct call from browser');
    var settings = {
        "url": url,
        "method": "POST",
        "timeout": 0,
        "headers": {
            "Authorization": basic_auth,
            "Content-Type": "application/json"
        },
        "data": jsonString
    };
    console.log(settings);

    return $.ajax(settings);
}

function generateJSON(element) {
    var action = $(element).attr('name');
    var title = "";
    var btncolor = "btn-primary";
    if (action == "createTable") {
        title = "Create new Descriptor";
    } else if (action == "updateTable") {
        title = "Update existing Descriptor";
        btncolor = "btn-danger";
    }

    console.log("name = " + action);
    var payload = {};
    payload.name = $('#name').val();
    payload.description = $('#description').val();
    payload.type = $('#type').val();
    payload.makeAvailableForTargeting = ($('#makeAvailableForTargeting').val() == 'true');
    payload.dataItems = [];

    for (i = 0; i < $('.card').length; i++) {
        //item is the whole attribute
        var item = $('.card')[i];
        var prop = $(item).find(".item");
        var obj = {};
        for (j = 0; j < prop.length; j++) {
            //prop is one property of a table attribute
            var propName = $(prop)[j]['name'];
            var propValue = $(prop)[j]['value'];
            var propType = $(prop)[j]['type'];
            if (propName == "tags") {
                //remove spaces, then split by comma
                obj[propName] = propValue.replace(/\s/g, '').split(",");
            } else if (propType == "number") {
                obj[propName] = parseInt(propValue);
            }
            else {
                if (propValue == 'true') {
                    obj[propName] = true;
                } else if (propValue == 'false') {
                    obj[propName] = false;
                } else if (propValue == '') {
                    // do nothing
                } else {
                    obj[propName] = propValue;
                }
            }
        }
        payload.dataItems[i] = obj;

    }

    payload.customProperties = {};
    setModal('JSON Payload to ' + title
        , 'Following JSON object will be send to the CI360 descriptor API:<br>'
        //,'<button type="button" class="btn btn-primary" data-dismiss="modal" onclick="createTable();">Create Table</button>'
        , '<button type="button" class="btn ' + btncolor + '" data-dismiss="modal" onclick="' + action + '();">' + title + '</button>'
        , payload
    );

}

function deleteDescriptor() {
    var descriptor = $('#descriptors').val();
    var descriptorName = $('#desc_' + descriptor).attr('name');

    setModal('Delete Datahub Descriptor'
        , 'Do you really want to delete the following descriptor?'
        + '<br><br><b>ID:</b>   ' + descriptor
        + '<br><br><b>Name:</b> ' + descriptorName
        + '<br><br><b>Type in name again:</b>'
        + '<input class="form-control form-control-sm" id="delDescriptorName"></input> '
        , '<button type="button" class="btn btn-danger" '
        + 'onclick="executeDeleteDescriptor(\'' + descriptor + '\',\'' + descriptorName + '\')" '
        + 'data-dismiss="modal">Yes, delete it!</button>'
    );
}

function executeDeleteDescriptor(descriptor, descriptorName) {
    var settings = {
        "async": true,
        "crossDomain": true,
        "url": "https://" + $('#ci360restUrlDropDown').val() + "/marketingData/tables/"
            + descriptor,
        "method": "DELETE",
        "headers": {
            "authorization": "Bearer " + $('#token').html(),
            "content-type": "application/x-www-form-urlencoded",
            "cache-control": "no-cache"
        }
    }

    if (descriptorName == $('#delDescriptorName').val()) {
        console.log("[execute delete] sending delete call now");

        /*$.ajax(settings)*/
        setModal('Delete Descriptor'
            , 'Response from Datahub API <br><br>'
            //+ JSON.stringify(response,null,2)
            + '<img src="./images/ajax_loader_green_32.gif" id="imgLoad_modal">'
            , '<button type="button" class="btn btn-secondary btn-sm" data-dismiss="modal">Close</button>'
        );

        callProxyAPI(settings, "deleteDescriptor").done(function (response) {
            var responseString = JSON.stringify(response.resp, null, 2);
            console.log(responseString);
            if (response.json != null) {
                responseString = JSON.stringify(response.json, null, 2);;
            }
            console.log("[deleteDescriptor] ", JSON.stringify(response, null, 2));
            /*setModal('Delete Descriptor'
                ,'Response from Datahub API <br><br>'
                //+ JSON.stringify(response,null,2)
                + responseString
                ,'<button type="button" class="btn btn-secondary" data-dismiss="modal">Close</button>'
            );*/
            updateModal(response.resp);
        });

    } else {
        console.log("[execute delete] no delete call executed");
    }
    console.log("[deleteDescriptor] settings: ", settings);

}


function uploadDataModal() {
    var header_line = "";
    var default_record1 = "";
    var default_record2 = "";
    for (i = 0; i < $('.card').length; i++) {
        //item is the whole attribute
        var item = $('.card')[i];
        var prop = $(item).find(".item");
        for (j = 0; j < prop.length; j++) {
            //prop is one property of a table attribute
            var propName = $(prop)[j]['name'];
            var propValue = $(prop)[j]['value'];
            if (propName == "name") {
                header_line += propValue
                default_record1 += "record1-" + propValue;
                default_record2 += "record2-" + propValue;
            }
        }
        if (i < $('.card').length - 1) {
            header_line += ",";
            default_record1 += ",";
            default_record2 += ",";
        }
    }
    header_line += '\n';
    console.log("header_line:", header_line);
    //var example_upload_data = "record1-value1,record1-value2,record1-value3,...\r\nrecord2-value1,record2-value2,record2-value3,...";
    var example_upload_data = default_record1 + "\r\n" + default_record2;

    setModal('Upload Data to Descriptor'
        , 'Please enter comma separated values, one row per record <br><br>'
        + 'Header Line: <b><span id="csvHeader">' + header_line + '</span></b><br>'
        + '<textarea rows="13" id="csvDataToUpload" class="form-control" placeholder="' + example_upload_data + '"></textarea>'
        + '<br> <input type="file" id="fileToUpload" name="files[]" onchange="readFileForUpload(this)" />'
        , '<span id="importid"  ></span>'
        + '<button id="btn_upload" type="button" class="btn btn-default btn-sm mybtn" onclick="uploadDataToDescriptor(this);">'
        + '  <span class="oi oi-cloud-upload"></span> &nbsp;Upload Data</button>'
        + '<button id="btn_checkUpload" type="button" class="btn btn-default btn-sm mybtn" onclick="checkImportStatus(this);" style="display:none">'
        + '  <span class="oi oi-eye"></span> &nbsp;Check Import Status</button>'
        + '<img src="images/ajax_loader_green_32.gif" id="uploadLoaderImage" style="display:none">'
        + '<button type="button" class="btn btn-secondary btn-sm" data-dismiss="modal">Close</button>'
    );
}

function readFileForUpload(element) {
    var files = element.files;
    console.log(files);
    var reader = new FileReader();
    reader.onload = function(event) {
        fileContent = event.target.result;
        var first50characters = fileContent.substring(0, 1000);
        $('#csvDataToUpload').val(first50characters + "...");
    };
    reader.readAsText(files[0]);
}

function uploadDataToDescriptor(element) {
    const fileField = document.getElementById("fileToUpload");
    var csvdata = "";
    if (fileField.files[0]) {
        console.log("file: " + fileField.files[0].name + " will be uploaded !");
        csvdata = $('#csvHeader').html() + fileContent;   
    } else {
        console.log("NO FILENAME: data from textfield will be uploaded !");
        csvdata = $('#csvHeader').html() + $('#csvDataToUpload').val();       
    }
    uploadTextDataToDescriptor(csvdata);
}

function uploadTextDataToDescriptor(csvdata) {
    $('#uploadLoaderImage').show();
    $('#btn_upload').hide();
    var descriptorid = $('#descriptors').val();
    var getUploadUrl = "https://" + $('#ci360restUrlDropDown').val() + "/marketingData/fileTransferLocation";
    //var csvdata = $('#csvHeader').html() + $('#csvDataToUpload').val();
    var settings = {
        "url": getUploadUrl,
        "method": "POST",
        "headers": {
            "content-type": "application/json",
            "authorization": "Bearer " + $('#token').html(),
        }
    }

    /* STEP 1 - get upload URL */
    callProxyAPI(settings, "uploadDataToDescriptor").done(function (response) {
        console.log("response from getUploadUrl: ", response);
        var signedUrl = response.json.signedURL;
        //console.log(JSON.stringify(response,null,2));

        //$('#csvDataToUpload').html(response.json.signedURL + "\n" + descriptorid);

        /* STEP 2 - upload CSV */
        settings.url = signedUrl;
        settings.method = "PUT";
        settings.headers = { "content-type": "text/plain" };
        settings["data"] = csvdata;
        callProxyAPI(settings, "uploadDataToDescriptor").done(function (response) {
            console.log('response from uploadCSV: ', response);

            /* STEP 3 - start import process */
            settings.url = "https://" + $('#ci360restUrlDropDown').val() + "/marketingData/importRequestJobs";
            settings.method = "POST";
            settings.headers = { "content-type": "application/json", "authorization": "Bearer " + $('#token').html() };

            var import_config_json = {
                "contentName": "import to " + $('#name').val(),
                "dataDescriptorId": descriptorid,
                "fieldDelimiter": ",",
                "fileLocation": signedUrl,
                "fileType": "CSV",
                "headerRowIncluded": true,
                "recordLimit": 0,
                "updateMode": "upsert"
            }
            settings["data"] = JSON.stringify(import_config_json);
            //mathias.bouten@sas.com,mathias.bouten@sas.com,test,Mathias
            callProxyAPI(settings, "uploadDataToDescriptor").done(function (response) {
                console.log('response from import process: ', response);
                console.log("import id: ", response.json.id);
                $('#csvDataToUpload').val(JSON.stringify(response.json, null, 2));
                $('#importid').html(response.json.id);
                $('#btn_checkUpload').show();
                $('#uploadLoaderImage').hide();
            });
        });

    });
}


function checkImportStatus(element) {
    $('#csvDataToUpload').val("");
    $('#btn_checkUpload').hide();
    $('#uploadLoaderImage').show();
    var importid = $('#importid').html();
    var url = "https://" + $('#ci360restUrlDropDown').val() + "/marketingData/importRequestJobs/" + importid;
    var settings = {
        "url": url,
        "method": "GET",
        "headers": {
            "content-type": "application/json",
            "authorization": "Bearer " + $('#token').html(),
        }
    }

    callProxyAPI(settings, "uploadDataToDescriptor").done(function (response) {
        console.log("response from checkImportProcess: ", response);
        $('#csvDataToUpload').val(JSON.stringify(response.json, null, 2));
        $('#btn_checkUpload').show();
        $('#uploadLoaderImage').hide();
    });
}