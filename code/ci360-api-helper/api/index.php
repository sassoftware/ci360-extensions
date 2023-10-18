<?php
session_set_cookie_params(3600*24*30);
session_start();

@$enable_logging = true;

return processRequest();
exit;
/**********************************************/



function processRequest() {
	//this web service returns the content in JSON
	@header('Content-type: application/json');

	//get the request body as JSON
	$request_body = file_get_contents('php://input');
	$jsonRequest  = json_decode($request_body,true);

	//get a property from the JSON request body
	$action = $jsonRequest['action'];
	//$email = $jsonRequest['email'];


	if($action == 'getDescriptors' 
		|| $action == 'createDescriptor' 
		|| $action == 'deleteDescriptor'
		|| $action == 'getDescriptorDetails'
		|| $action == 'getTenantName'
		|| $action == 'updateDescriptor'		
		|| $action == 'uploadDataToDescriptor'
		|| $action == 'designServerCall'
		|| $action == 'getIdentity'
		|| $action == 'getJobs'
		) {
		//$settings = $jsonRequest['settings'];	
		echo json_encode(callCI360Gateway($jsonRequest));
	}
	else {
		// display how to use service.
		$endpointVariables = array("endpoint" => "url", "method" => "get,post,delete", "header" => "web service header");
		$serviceEndpoints = array( 	
			array("Name" => "Get Descriptors", "Description" => "....", "Endpoint" => "/api?action=getDescriptors", "Variables" => $endpointVariables),
			array("Name" => "Get Descriptor Details", "Description" => "....", "Endpoint" => "/api?action=getDescriptorDetails"),
			array("Name" => "Create Descriptor", "Description" => "....", "Endpoint" => "/api?action=createDescriptor"),
			array("Name" => "Delete Descriptor", "Description" => "....", "Endpoint" => "/api?action=deleteDescriptor"),
			array("Name" => "json Payload", "Description" => "....", "json_request" => $jsonRequest)
		);
		$serviceEndpointDesc = array("Service Endpoints" => $serviceEndpoints);

		echo json_encode($serviceEndpointDesc);	
	}

	return;
}


/**
*  FUNCTION: callCI360Gateway
*/
function callCI360Gateway($settings) {

	$message = 'API call: '.$settings['action'];
	$headers = $settings['headers'];
	$payload = array(); 

	//create string array header for CURL
	$curlHeader = array();
	$headerProp = array_keys($headers);
	$headerVal = array_values($headers);
	for ($i=0; $i<sizeof($headers); $i++) {
	   	$curlHeader[$i] = $headerProp[$i].":".$headerVal[$i];
	}

	$curl = curl_init();
	curl_setopt($curl, CURLOPT_URL, $settings['url']);
	curl_setopt($curl, CURLOPT_RETURNTRANSFER, true);
	curl_setopt($curl, CURLOPT_SSL_VERIFYPEER, false);
	curl_setopt($curl, CURLOPT_ENCODING, "");
	curl_setopt($curl, CURLOPT_MAXREDIRS, 10);
	curl_setopt($curl, CURLOPT_TIMEOUT, 400);
	curl_setopt($curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
	curl_setopt($curl, CURLOPT_CUSTOMREQUEST, $settings['method']);
	curl_setopt($curl, CURLOPT_HTTPHEADER, $curlHeader);

	//add json payload if exists
	if(isset($settings['data'])) {
		$payload = $settings['data'];
		curl_setopt($curl, CURLOPT_POSTFIELDS, $payload);
	} 

	//execute CURL
	$response = curl_exec($curl);
	$err = curl_error($curl);
	
	curl_close($curl);


	if ($err) {
		$resp = "cURL Error #:" . $err;
		return array('msg' => $message, 'settings' => $settings, 'payload' => $payload, 'resp' => $resp);
	} else {
	   	$resp = $response;
	   	return array('msg' => $message, 'settings' => $settings, 'payload' => $payload, 'json' => json_decode($resp), 'resp' => $resp);
	}
}

?>