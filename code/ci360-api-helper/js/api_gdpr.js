
var email_record = [];

function btn_dropIdentity() {
    var identityType = $('#identityType').val();
    setModal('GDPR API - Delete Identity'
        , '**Warning, deleting this will remove all data for this identity value of type <b>' + identityType + '</b>. '
        + ' This includes all events and data loaded to any of the descriptors in this tenant.**'
        + '<br><br><label>CI360 Response for Identity Deletion</label><br>'
        + '<textarea rows="13" id="gdprResponse" class="form-control" style="display:none"></textarea>'
        , '<button type="button" id="btn_reallyDropIdentity" class="btn btn-danger" onclick="btn_reallyDropIdentity(this);">Yes I want to delete it!</button>'
        + '<button type="button" class="btn btn-secondary" data-dismiss="modal">Close</button>'
    );
}

function btn_reallyDropIdentity(element) {
    showLoaderIconAfterButtonClick (element);
    var identityType = $('#identityType').val();
    var identityValue = $('#identityValue').val().replaceAll(" ", "");
    var identityValues = identityValue.split(",");
    var payload = {
        "jobType": "GDPR_DELETE",
        "identityType": identityType,
        "identityList": identityValues,
        "token": $('#token').html()
    }

    var settings = {
        "url": "https://" + $('#ci360restUrlDropDown').val() + "/marketingData/customerJobs",
        "method": "POST",
        "headers": {
            "authorization": "Bearer " + $('#token').html(),
            "Content-Type" : "application/json",
            "cache-control": "no-cache"
        },
        "data": JSON.stringify(payload)
    }
    
    callProxyAPI(settings, "getIdentity").done(function (response) {
        console.log("response: ", response);
        $('#gdprResponse').val(JSON.stringify(response.json, null, 4));
        hideLoaderIconAndShowButton (element);
        $('#gdprResponse').show();
        if (response.status) { 
            $('#btn_reallyDropIdentity').hide();
        }

    });
}

function btn_checkIdentity(element) {
    showLoaderIconAfterButtonClick (element);
    $('#btn_dropIdentity').hide();
    var identityType = $('#identityType').val();
    var identityValue = $('#identityValue').val().replaceAll(" ", "");
    var identityValues = identityValue.split(",");
    var identityAttribute="";

    if (identityType == "datahub_id") {
        getIdentityDetailsFor(identityValue, element);
    } else {
        if (identityType == "login_id") identityAttribute = "loginId";
        if (identityType == "customer_id") identityAttribute = "customerId";
        if (identityType == "subject_id") identityAttribute = "subjectId";
        
        var settings = {
            "url": "https://" + $('#ci360restUrlDropDown').val() + "/marketingData/identityRecords?" + identityAttribute + "=" + identityValue,
            "method": "GET",
            "headers": {
                "authorization": "Bearer " + $('#token').html(),
                "cache-control": "no-cache"
            }
        }

        /* 1. getDatahubID call */
        callProxyAPI(settings, "getIdentity").done(function (proxyResponse) {
            console.log("getDatahubID");
            var datahub_id = proxyResponse.json.id;
            /* 2. getIdentityDetails call */
            getIdentityDetailsFor(datahub_id, element);
        });  /* end of getDatahubID call */
    }
}

function getIdentityDetailsFor(datahub_id, element) {
    console.log("getIdentityDetailsFor " + datahub_id);
    var settings = {
        "url": "https://" + $('#ci360restUrlDropDown').val() + "/marketingData/identityRecords/" + datahub_id,
        "method": "GET",
        "headers": {
            "authorization": "Bearer " + $('#token').html(),
            "cache-control": "no-cache"
        }
    }
    callProxyAPI(settings, "getIdentity").done(function (proxyResponse) {
        var response = proxyResponse.json;
        var mainIdentityTable = [];
        var identitiesTable = [];
        var attributesTable = [];
		var preferencesTable = [];
        if (response != null && response.hasOwnProperty('identities')) {

            var recordDatahubId = ["datahub_id",datahub_id];
            mainIdentityTable.push(recordDatahubId);
            
            for (var i = 0; i < response.identities.length; i++) {
                var record = [];
                var identity = response.identities[i];
                
                var type = identity.type;
                var id = identity.id;
                record.push(type);
                record.push(id);
                if (type.includes("customer_id") || type.includes("login_id") ||
                    type.includes("email_id") || type.includes("subject_id") ) {
                        mainIdentityTable.push(record);
						if (type.includes("email_id") ) {
							email_record.push(record);
						}
                        for (var key in identity.attributes ) {
                            var attrRecord = [];
                            attrRecord.push(type);
                            attrRecord.push(key.replace("cci.",""));
                            //console.log("key: " +key+ "; value: "+identity.attributes[key]);
                            attrRecord.push(identity.attributes[key]);
                            attributesTable.push(attrRecord);
							if(key.includes("cpi")) { 
							  preferencesTable.push(attrRecord); 
							}
                        }
                        
                } else {
                    identitiesTable.push(record);
                }
            };

        }  

        dropTableIfExist("identitiesTable");
        tables["identitiesTable"] = $('#identitiesTable').DataTable( {
            //dom: 'Bfrtip',
            responsive: true,
	        data: identitiesTable,
            columns: [
                { title: "Identity Type" },
                { title: "Value" }
            ],
            "scrollX": true,
            "scrollY": "300px",
            "paging": false,
            //"order": [[ 0, 'asc' ]]
            "order": []
            //"buttons": ['copy', 'excel', 'pdf']
        } );

        dropTableIfExist("mainIdentityTable");
        tables["mainIdentityTable"] = $('#mainIdentityTable').DataTable( {
            responsive: true,
            data: mainIdentityTable,
            columns: [
                { title: "Identity Type" },
                { title: "Value" }
            ],
            "scrollX": true,
            "scrollY": "300px",
            "paging": false,
            "order": []
        } );

        dropTableIfExist("attributesTable");
        tables["attributesTable"] = $('#attributesTable').DataTable( {
            responsive: true,
            data: attributesTable,
            columns: [
                { title: "Identity Type" },
                { title: "Attribute Name" },
                { title: "Attribute Value" }
            ],
            "scrollX": true,
            "scrollY": "300px",
            "paging": false,
            "order": [[ 0, 'asc' ],[ 1, 'asc' ]]
        } );
		
		dropTableIfExist("preferencesTable");
        tables["preferencesTable"] = $('#preferencesTable').DataTable( {
            responsive: true,
            data: preferencesTable,
            columns: [
                { title: "Identity Type" },
                { title: "Attribute Name" },
                { title: "Attribute Value" }
            ],
            "scrollX": true,
            "scrollY": "120px",
            "paging": false,
            "order": [[ 0, 'asc' ],[ 1, 'asc' ]]
        } );
        
        initTabs();
        $('.identity_details').show();
        hideLoaderIconAndShowButton (element);
        //$('#gdprDropIdentityBtn').show();
        $('#btn_dropIdentity').show();
        $($.fn.dataTable.tables(true)).DataTable().columns.adjust();
      });  /* end of getIdentityDetails */
}

function initTabs() {
    var triggerTabList = [].slice.call(document.querySelectorAll('#gdprTabs a'));
    triggerTabList.forEach(function (triggerEl) {
        var tabTrigger = new bootstrap.Tab(triggerEl)
        triggerEl.addEventListener('click', function (event) {
            event.preventDefault();
            tabTrigger.show();            
        });
        
    });

    // readjust the column with of header and records
    $('a[data-bs-toggle="tab"]').on('shown.bs.tab', function (e) {
        $($.fn.dataTable.tables(true)).DataTable()
           .columns.adjust();
    });    
    
}


function btn_optin(element) {
	updatePreferenceTable(element, "false");
	setModal('Update Preference with Opt-In'
        , '<br> We are now updating the value, this can take a few seconds.... <br><br>'
		+ 'You can close this window and after 15 seconds click the button "Get Identity Details" again <br>'
        , '<button type="button" class="btn btn-secondary btn-sm" data-dismiss="modal">Close</button>'
    );
}

function btn_optout(element) {
	updatePreferenceTable(element, "true");
	setModal('Update Preference with Opt-Out'
        , '<br> We are now updating the value, this can take a few seconds.... <br><br>'
		+ 'You can close this window and after 15 seconds click the button "Get Identity Details" again <br>'
        , '<button type="button" class="btn btn-secondary btn-sm" data-dismiss="modal">Close</button>'
    );
}
	
function updatePreferenceTable(element, prefValue) {
	showLoaderIconAfterButtonClick (element);
    var csvdata = "identity_type_cd,identity_value,preference_type_cd,preference_value\n";
    var program = $('#programValue').val();
	var email = email_record[0][1];
	
	if (program != '') {
		csvdata = csvdata + "email_id,"+email+","+program+"@OPT-OUT,"+prefValue;  
	} else if (program == '') {
		csvdata = csvdata + "email_id,"+email+",OPT-OUT,"+prefValue; 
    }
	
		var descriptorid = '9aa72601-24ec-4c61-b435-c8127f640234';
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
		console.log("STEP 1 - get upload URL");
		callProxyAPI(settings, "uploadDataToDescriptor").done(function (response) {
			console.log("response from getUploadUrl: ", response);
			var signedUrl = response.json.signedURL;
			//console.log(JSON.stringify(response,null,2));

			//$('#csvDataToUpload').html(response.json.signedURL + "\n" + descriptorid);

			/* STEP 2 - upload CSV */
			console.log("STEP 2 - upload CSV");
			settings.url = signedUrl;
			settings.method = "PUT";
			settings.headers = { "content-type": "text/plain" };
			settings["data"] = csvdata;
			callProxyAPI(settings, "uploadDataToDescriptor").done(function (response) {
				console.log('response from uploadCSV: ', response);

				/* STEP 3 - start import process */
				console.log("STEP 3 - start import proces");
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
					console.log(JSON.stringify(response.json, null, 2));
					hideLoaderIconAndShowButton (element);					
				});
			});

		});
	/*} else { 
		alert("Program field is empty! Please provide a program. ");
		hideLoaderIconAndShowButton (element);
	}*/
}