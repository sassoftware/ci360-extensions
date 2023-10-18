
function btn_getJobs(element) {
    showLoaderIconAfterButtonClick (element);

    var url = "https://" + $('#ci360restUrlDropDown').val() + "/marketingData/importRequestJobs?limit=1000" ;
    var settings = {
        "url": url,
        "method": "GET",
        "headers": {
            "content-type": "application/json",
            "authorization": "Bearer " + $('#token').html(),
        }
    }

    callProxyAPI(settings, "getJobs").done(function (response) {
        //console.log("response from checkImportProcess: ", response);
        populateJobsTable(response.json.items);
        $('#jobsResponseJson').val(JSON.stringify(response.json.items, null, 2));        
        $('.job_details').show();

        // readjust column widths of table after table is visible in the DOM !!
        $($.fn.dataTable.tables(true)).DataTable().columns.adjust();
        
        hideLoaderIconAndShowButton(element);
    });
}

function populateJobsTable(jobItems) {
    
    //console.log(jobItems);
    var jobsData = [];
    for (item in jobItems) {
        var jobRecord = [];
        var statusButton = '<button class="btn btn-sm btn-primary" '
            + 'onclick="checkJobStatus(this, \''+ jobItems[item].id +'\')">Status</button>'
            + '&nbsp;&nbsp;<span id="status_'+ jobItems[item].id + '"></span>';
        jobRecord.push(statusButton);
        jobRecord.push(jobItems[item].name);
        jobRecord.push(jobItems[item].id);
        jobRecord.push(jobItems[item].dataDescriptorId);
        
        jobsData.push(jobRecord);
    }
    

    dropTableIfExist("jobtable");
    tables["jobtable"] = $('#jobTable').DataTable( {
            scrollX: true,
            scrollY: "400px",
            responsive: true,
            data: jobsData,
            columns: [
                { title: "Status" }, 
                { title: "Job Name" },
                { title: "Job ID" },
                { title: "Data DescriptorId" },
            ],
            "order": [],
            "paging": false
        } );

}


function checkJobStatus(element, id) {
    showLoaderIconAfterButtonClick (element);

    $('#jobsResponseJson').val("");
    var url = "https://" + $('#ci360restUrlDropDown').val() + "/marketingData/importRequestJobs/" + id;
    var settings = {
        "url": url,
        "method": "GET",
        "headers": {
            "content-type": "application/json",
            "authorization": "Bearer " + $('#token').html(),
        }
    }

    callProxyAPI(settings, "getJobs").done(function (response) {
        console.log("response from checkImportProcess: ", response);
        
        $('#jobsResponseJson').val(JSON.stringify(response.json, null, 2));
        $('#jobsResponseJson').show();

        hideLoaderIconAndShowButton (element);

        $('#status_'+id).html(response.json.status);
        $($.fn.dataTable.tables(true)).DataTable().columns.adjust();
    });
}
