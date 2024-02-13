/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%global ds2proxysupported;
%macro CheckDS2Proxy();
	/* get the system version and find the Maintenance level */
	%let mlevel = %SYSFUNC(SUBSTR(&sysvlong, %sysfunc(INDEXC(&sysvlong, M))+1, 1));

	/* if we have found the maintenance level check it and set the Proxy Supported flag */
	%if %symexist(mlevel) & ^ %length(&mlevel) = 0 %then
		%do;
			%if &mlevel LT 6 %then %do;
				%put **************************************************************************;
				%put SAS Version &sysvlong does not support Proxy methods on DS2 HTTP object.;
				%put Package will be compiled without proxy support.;
				%put **************************************************************************;
				%let ds2proxysupported=0;
			%end;
			%else %do;
				%let ds2proxysupported=1;
			%end;
		%end;
	%else
		%do;
			%put Error finding SAS Maintenance Level. Aborting.;
			%abort return;
		%end;
%mend;

%macro CreatePackage();

	%local force_create version;
	%let version = 4.2;
	%let force_create = 1;	/* SET THIS TO 1 TO FORCE THE PACKAGE TO BE RECREATED */

	%if (%sysfunc(exist(&sas_utility_library..CI360Utilities)) and &force_create = 0) %then %do;
		%put ************************************************;
		%put CI360Utilities package already exists, skipping.;
		%put ************************************************;
		
		%let rc = 1;
	%end;
	%else %do;
		%put ************************************************;
		%put Creating CI360Utilities package...;
		%put ************************************************;

		%let rc = 0;

		%CheckDS2Proxy();

		/* THIS PACKAGE CONTAINS ALL THE METHODS NEEDED TO MANAGE THE PROCESS OF GETTING A LIST OF FILES TO DOWNLOAD */
		/* HOWEVER, DS2 DOESN'T HAVE THE ABILITY TO WRITE TO A FILE AND THERE ARE LIIMTS TO THE SIZE OF STRING VARS */
		/* SO IT PROVIDES A DATASET LISTING ALL THE FILES THAT NEED TO BE RETRIEVED, BUT RETRIEVAL NEEDS PROC HTTP. */
		PROC DS2 NOLIBS CONN="((DRIVER=BASE;CATALOG=&sas_utility_library;SCHEMA=(NAME=&sas_utility_library;PRIMARYPATH={&sas_utility_path/data}));(DRIVER=BASE;CATALOG=WORK;SCHEMA=(NAME=WORK;PRIMARYPATH={%sysfunc(pathname(work))}));)";
		    package &sas_utility_library..CI360Utilities /overwrite=yes;

				declare double dblVersion;

				/* hash variables have to be global, and become the column names, so using basic conventions here */
				declare varchar(32) part;
				declare varchar(2048) file;
				declare varchar(32767) url;
				declare double last_modified;
				declare int status;
				declare varchar(3) type;
				declare package hash m_hashFiles();

		        declare varchar(32) m_strEnvironment;
		        declare varchar(32) m_strEGWEnvironment;
				declare varchar(256) m_strJWT;
				declare varchar(128) m_strAgent;
				declare int m_intLogLevel;
				/* Possible log levels:
				    0 = Errors Only
					1 = Info
				    2 = Debug 
				    3 = Trace (sensitive info obfuscated)
				    4 = Trace (sensitive info plain text) */

				declare int m_intUseProxy;
				declare varchar(512) m_strProxyURL;
				declare varchar(128) m_strProxyUser;
				declare varchar(128) m_strProxyPassword;

				/* PUTS TO LOG AND ASSUMES ALLOWED */
				method Log(int intLevel, varchar(100) strMethod, varchar(32767) strMessage);
					Log(intLevel, strMethod, strMessage, 0);
				end;

				/* PUTS TO LOG IF ALLOWED */
				method Log(int intLevel, varchar(100) strMethod, varchar(32767) strMessage, int intObfuscate);

					declare varchar(32767) strLogLine;
					declare timestamp tsLog;

					if (m_intLogLevel >= intLevel) then do;
						tsLog = to_timestamp(datetime());

						/* if this is protected info and we're not using the "override" log level, don't show the message */
						if ((m_intLogLevel < 4) and (intObfuscate = 1)) then strMessage = '<protected>';
						strLogLine = tsLog || ', CI360Utilities.' || strMethod || ', ' || strMessage;
						put strLogLine;
					end;
				end;
				
				/* HANDLES FORMATTING TIMESTAMPS FOR REQUEST PARAMETERS */
				method TimestampString(timestamp tsValue) returns varchar(32);

					declare varchar(32) strResult;

					Log(3, 'TimestampString', '>>>> TimestampString(' || tsValue || ')');

					strResult = put(datepart(to_double(tsValue)), yymmdd10.) || 'T' || put(timepart(to_double(tsValue)), TOD12.3) || 'Z';

					Log(3, 'TimestampString', '<<<< TimestampString=' || strResult);

					return strResult;
				end;

				/* CONVERTS THE PATH PROVIDED BY THE API INTO A LOCAL FILE */
				method CreateFileName(varchar(2048) strPath, varchar(2048) strURL) returns varchar(2048);

					declare varchar(2048) strResult strID;
					
					Log(3, 'CreateFileName', '>>>> CreateFileName(' || strPath || ', ' || strURL || ')');

					strID = prxchange('s/.*?(.{36})/$1/', -1, strip(strURL));
					Log(3, 'CreateFileName', 'ID = ' || strID);

					strResult = substr(strPath, index(strPath, strID) + length(strID) + 1) || '.csv';

					Log(3, 'CreateFileName', '<<<< CreateFileName=' || strResult);

					return strResult;
				end;

				method Base64URLEncode(varchar(1024) strInput) returns varchar(1024);

					declare varchar(1024) strResult;

					Log(3, 'Base64URLEncode', '>>>> Base64URLEncode(' || strInput || ')', 1);

					strResult = transtrn(transtrn(transtrn(put(trim(strInput), $base64x64.), '=', ''), '/', '_'), '+', '-');

					Log(3, 'Base64URLEncode', '<<<< Base64URLEncode=' || strResult, 1);

					return trim(strResult);
				end;

				/* CREATES A JSON WEB TOKEN FOR API GATEWAY ACCESS USING A REGISTERED AGENT */
				method GenerateJWT(varchar(64) strTenantID, varchar(256) strSecret) returns varchar(1024);

					declare varchar(256) strKey strHeader strPayload strResult strSignature;

					Log(3, 'GenerateJWT', '>>>> GenerateJWT(' || strTenantID || ', ' || strSecret || ')', 1);

					/* the key needs to be base64 encoded */
					strKey = trim(put(strSecret, $base64x72.));

					strHeader = Base64URLEncode('{"typ":"JWT","alg":"HS256"}');
					strPayload = Base64URLEncode('{"clientID":"' || strTenantID || '"}');
					strResult = strHeader || '.' || strPayload;

					strSignature = Base64URLEncode(inputc(sha256hmachex(strKey, strResult, 0), '$hex64.'));								
					strResult = strResult || '.' || strSignature;

					Log(3, 'GenerateJWT', '<<<< GenerateJWT=' || strResult, 1);

					return strResult;
				end;

				/* HANDLES ALL HTTP GET COMMUNICATION */
				method DoHTTPGet(varchar(2048) strURL, in_out varchar strResponse) returns int;

					declare package http objHTTP();
					declare int intRC;

					Log(3, 'DoHTTPGet', '>>>> DoHTTPGet(' || strURL || ')');

					objHTTP.createGetMethod(strURL);
					objHTTP.addRequestHeader('Authorization', 'Bearer ' || m_strJWT);

					/* only use this proxy code if supported */
					%if (&ds2proxysupported=1) %then %do;
						if (m_intUseProxy > 0) then do;
							Log(2, 'DoHTTPGet', 'DoHTTPGet using Proxy:[' || m_strProxyURL || ',' || m_strProxyUser || ']');
							objHTTP.setProxyUrl(m_strProxyURL);
							objHTTP.setProxyUserName(m_strProxyUser);
							objHTTP.setProxyPassword(m_strProxyPassword);
						end;
					%end;

					objHTTP.executeMethod();

					intRC = objHTTP.getStatusCode();  
					Log(2, 'DoHTTPGet', 'Response Code=' || intRC);
		            if intRC = 200 then do;     /* 200 = OK */

		                /* retrieve the body from the response that came from the server */
						intRC = 0;
		                objHTTP.getResponseBodyAsString(strResponse, intRC);
		                if intRC <> 0 then
		                    Log(0, 'DoHTTPGet', 'Download of response body failed (' || intRC || ')');

					end;
					else
						Log(0, 'DoHTTPGet', 'HTTP GET Failed: ' || intRC);

					Log(3, 'DoHTTPGet', '<<<< DoHTTPGet = ' || intRC || ' (' || strResponse || ')');

					return intRC;

				end;
								
				/* HANDLES ALL HTTP POST COMMUNICATION */
				method DoHTTPPost(varchar(2048) strURL, varchar(32767) strData, in_out varchar strResponse, in_out varchar strHeader) returns int;

					declare package http objHTTP();
					declare int intRC;
					
					Log(3, 'DoHTTPPost', '>>>> DoHTTPPost(' || strURL || ', ' || strData || ')');

					objHTTP.createPostMethod(strURL);
					objHTTP.setRequestContentType('application/json; charset=utf-8');
					objHTTP.addRequestHeader('Authorization', 'Bearer ' || m_strJWT);				            
		            objHTTP.setRequestBodyAsString(strData);

					/* only use this proxy code if supported */
					%if (&ds2proxysupported=1) %then %do;
						if (m_intUseProxy > 0) then do;
							Log(2, 'DoHTTPPost', 'DoHTTPPost using Proxy:[' || m_strProxyURL || ',' || m_strProxyUser || ']');
							objHTTP.setProxyUrl(m_strProxyURL);
							objHTTP.setProxyUserName(m_strProxyUser);
							objHTTP.setProxyPassword(m_strProxyPassword);
						end;
					%end;

					objHTTP.executeMethod();

					intRC = objHTTP.getStatusCode();  
					Log(2, 'DoHTTPPost', 'Response Code=' || intRC);
		            if intRC = 200 or intRC = 201 then do;     /* 200 = OK, 201 = CREATED */

		                /* retrieve the body from the response that came from the server */
						intRC = 0;
		                objHTTP.getResponseHeadersAsString(strHeader, intRC);
		                if intRC <> 0 then
							Log(0, 'DoHTTPPost', 'Download of response headers failed (' || intRC || ').');
						else do;
			                objHTTP.getResponseBodyAsString(strResponse, intRC);
			                if intRC <> 0 then
			                    Log(0, 'DoHTTPPost', 'Download of response body failed (' || intRC || ').');
						end;
					end;
					else
						Log(0, 'DoHTTPPost', 'HTTP POST Failed: ' || intRC);

					Log(3, 'DoHTTPPost', '<<<< DoHTTPPost=' || intRC || ' (Header: ' || strHeader || ', Body: ' || strResponse || ')');

					return intRC;

				end;				
																
				/* PARSE A JSON STRING LOOKING FOR A SPECIFIC TOKEN */
				method GetJSONValue(in_out varchar strJSON, varchar(100) strName) returns varchar(2048);

					declare package json objJSON();
					declare int intTokenType intParseFlags intRC;
					declare bigint bigLineNum bigColNum;
					declare varchar(2048) strToken strResult;

					Log(3, 'GetJSONValue', '>>>> GetJSONValue(' || strJSON || ', ' || strName || ')');

					intRC = objJSON.createParser(strJSON);
					Log(3, 'GetJSONValue','Looking for token=' || strName || ', rc=' || intRC);
					do while (intRC = 0); 					
						
						/* search until we find the token */
						objJSON.getNextToken(intRC, strToken, intTokenType, intParseFlags, bigLineNum, bigColNum);
						Log(3, 'GetJSONValue','Token=' || strToken || ', rc=' || intRC);
						if (intRC = 0) then do;
							if (strToken eq strName) then do;

								/* then get the very next token, which will be the value */
								objJSON.getNextToken(intRC, strResult, intTokenType, intParseFlags, bigLineNum, bigColNum);
								if (intRC = 0) then do;
									Log(3, 'GetJSONValue','Found desired value=' || strResult || ', rc=' || intRC);

									/* this should really be a LEAVE statement, but that is broken in do loops in proc ds2 apparently */
									intRC = -1;
								end;

							end;
						end;
						else
							Log(0, 'GetJSONValue', 'Failed to parse JSON text: ' || strJSON);

					end;
					objJSON.destroyParser();

					if lengthn(strResult) = 0 then
						Log(0, 'GetJSONValue', 'Failed to find token ' || strName || ' in JSON text: ' || strJSON);

					Log(3, 'GetJSONValue', '<<<< GetJSONValue=' || strResult);

		            return strResult;

				end;
				
				/* KEEP CHECKING UNTIL IT'S READY AND RETURN RESPONSE */
				method IsReady(varchar(2048) strURL, varchar(100) strWaitFor, in_out varchar strResponse) returns tinyint;

					dcl package pyUtils pyUtils('pyUtils');
					declare varchar(32) strStatus;
		            declare int intRC;
					declare tinyint blnResult;

					Log(3, 'IsReady', '>>>> IsReady(' || strURL || ', ' || strWaitFor || ', <string>)');

					intRC = DoHTTPGet(strURL, strResponse);
		            if intRC = 0 then do;
						strStatus = pyUtils.getStatus(strResponse);
						blnResult = (strStatus eq strWaitFor);
					end;
					
					Log(3, 'IsReady', '<<<< strResponse=' || strResponse);
					Log(3, 'IsReady', '<<<< strStatus=' || strStatus);
					Log(3, 'IsReady', '<<<< IsReady=' || blnResult);

					return blnResult;
				end;


				/* TELL CI 360 TO PROCESS THE UPLOADED DATA */
				/* THE CLIENT MUST FIRST DO A HTTP PUT REQUEST OF THE PHYSICAL FILE TO THE AWS S3 LOCATION BEFORE CALLING THIS METHOD */
				method RequestImport(varchar(128) strUploadName, varchar(2048) strLocation, varchar(36) strDescriptorID, varchar(36) strUpdateMode, int blnHeaderRow) returns varchar(2048);

					declare package http objHTTP();
					declare varchar(32767) strBody strResponse strHeaders;
					declare varchar(5) strHeaderRow;
					declare varchar(2048) strURL strResult;
					declare int intRC;
					declare tinyint blnResult;

					Log(3, 'RequestImport', '>>>> RequestImport(' || strDescriptorID || ', ' || strLocation || ', ' || strUploadName || ')');

					/* does it need the header row flag */
					if blnHeaderRow then
						strHeaderRow = 'true';
					else
						strHeaderRow = 'false';

					/* Build the JSON for the body of the request */
					strBody = '{"dataDescriptorId": "' || strDescriptorID || '", "fileLocation": "' || strLocation || '", "fieldDelimiter": ",", "contentName": "' || strUploadName || '", "fileType" :"CSV", "recordLimit": 0, "updateMode": "' || strUpdateMode || '", "headerRowIncluded": ' || strHeaderRow || '}';
					strURL = 'https://extapigwservice-' || m_strEGWEnvironment || '/marketingData/importRequestJobs';

					intRC = DoHTTPPost(strURL, strBody, strResponse, strHeaders);
		            if intRC = 0 then strResult = GetJSONValue(strResponse, 'id');							
					
					if lengthn(strResult)>0 then do;
						strURL = strURL || '/' || strResult; 
					end;
					else do;
						strURL = '';
					end;
						
					Log(3, 'RequestImport', '<<<< RequestImportURL = ' || strURL);
					
					return strURL;
				end;
				
				/* KEEP CHECKING UNTIL IT'S READY AND RETURN COUNTS */
				method IsImportReady(varchar(2048) strURL, varchar(100) strWaitFor, in_out int intNotProcessed, in_out int intUpdated, 
															in_out int intCreated, in_out int intRejected, in_out int intProcessed) returns tinyint;

					declare package json objJSON();
					declare int intTokenType intParseFlags intRC;
					declare bigint bigLineNum bigColNum;
		 			declare varchar(2048) strToken; 

					declare varchar(32767) strResponse;
					declare tinyint blnResult;

					Log(3, 'IsImportReady', '>>>> IsImportReady(' || strURL || ', ' || strWaitFor || ', <int>, <int>, <int>, <int>)');

					/*get the real result */
					blnResult = IsReady(strURL, strWaitFor, strResponse);

					/* if we're ready now, take the result and update the counts */
					if blnResult then do;

						intRC = objJSON.createParser(strResponse);
						if (intRC eq 0) then do;

							/* search until we find the tokens */
							objJSON.getNextToken(intRC, strToken, intTokenType, intParseFlags, bigLineNum, bigColNum);
							do while (intRC eq 0);
								select (strToken);
									when ('Total Number of Records Not Processed')
										do; 
											objJSON.getNextToken(intRC, strToken, intTokenType, intParseFlags, bigLineNum, bigColNum);
											intNotProcessed = strToken;
										end;
									when ('Total Number of Identities Updated')
										do; 
											objJSON.getNextToken(intRC, strToken, intTokenType, intParseFlags, bigLineNum, bigColNum);
											intUpdated = strToken;
										end;
									when ('Total Number of Identities Created')
										do; 
											objJSON.getNextToken(intRC, strToken, intTokenType, intParseFlags, bigLineNum, bigColNum);
											intCreated = strToken;
										end;
									when ('Total Number of Identities Rejected')
										do; 
											objJSON.getNextToken(intRC, strToken, intTokenType, intParseFlags, bigLineNum, bigColNum);
											intRejected = strToken;
										end;
									when ('Total Number of Records Processed')
										do; 
											objJSON.getNextToken(intRC, strToken, intTokenType, intParseFlags, bigLineNum, bigColNum);
											intProcessed = strToken;
										end;
									otherwise;
										objJSON.getNextToken(intRC, strToken, intTokenType, intParseFlags, bigLineNum, bigColNum);
								end;
							end;
							if (intRC ne 101) then Log(0, 'IsImportReady', 'Failed to get JSON token (' || intRC || '): ' || strResponse);
							
						end;
						else
							Log(0, 'IsImportReady', 'Failed to parse JSON text (' || intRC || '): ' || strResponse);
	
						Log(2, 'IsImportReady', 'Import completed - Not Processed: ' || intNotProcessed || ' (Rejected: ' || intRejected || '), Processed: ' || intProcessed || ' (Created: ' || intCreated || ', Updated: ' || intUpdated || ')');
					end;

					Log(3, 'IsImportReady', '<<<< IsImportReady=' || blnResult);

					return blnResult;
				end;


				/* RETURN THE STATUS CODE */
				method getImportDataStatus(varchar(2048) strURL, varchar(100) strWaitFor, 
											in_out varchar strFailedURL, in_out varchar strRejectedURL,
											in_out int intIdentityRowsSuccessful, in_out int intIdentityRowsFailed,
											in_out int intIdentityRowsRejected, in_out int intIdentityRowsNotFound) returns int;

					dcl package pyUtils pyUtils('pyUtils');
					dcl int intImportDataStatus;
					dcl varchar(2048) strIdentityRowsFailedURL strIdentityRowsRejectedURL;
					dcl varchar(100) strStepStatus;
					dcl varchar(32767) strResponse;
					dcl tinyint blnIdentityRowsFailed blnIdentityRowsRejected
								blnIdentityRowsFailedURL blnIdentityRowsRejectedURL
								blnStepStatusValidation blnStepStatusDataProcessing
								blnStepStatusIdentityProcessing blnStepStatusTargetingProcessing
								blnReady blnIdentityRowsSuccess blnIdentityRowsNotFound
					; 

					/*Initialize vars */
					intImportDataStatus = -1;
					strFailedURL = 'URL_FAILED_NOT_FOUND';
					strRejectedURL = 'URL_REJECTED_NOT_FOUND';

					/* return 0 is imported 1 otherwise */
					/* get the real result */
					blnReady = IsReady(strURL, strWaitFor, strResponse);

					/* if imported */
					if blnReady then do;
						intImportDataStatus = 0;

						blnIdentityRowsFailedURL = pyUtils.getStepUrlFailed(strResponse, strIdentityRowsFailedURL);
						if blnIdentityRowsFailedURL then do;
							intImportDataStatus = 1;
							strFailedURL = strIdentityRowsFailedURL;
						end;

						blnIdentityRowsRejectedURL = pyUtils.getStepUrlRejected(strResponse, strIdentityRowsRejectedURL);
						if blnIdentityRowsRejectedURL then do;
							intImportDataStatus = 2;
							strRejectedURL = strIdentityRowsRejectedURL;
						end;

						if (blnIdentityRowsFailedURL and blnIdentityRowsRejectedURL) then do;
							intImportDataStatus = 3;
						end;

						blnIdentityRowsSuccess =  pyUtils.getIdentityRowsByStatus(strResponse, 'Successful', intIdentityRowsSuccessful);
						blnIdentityRowsFailed =  pyUtils.getIdentityRowsByStatus(strResponse, 'Failed', intIdentityRowsFailed);
						blnIdentityRowsRejected =  pyUtils.getIdentityRowsByStatus(strResponse, 'Rejected', intIdentityRowsRejected);
						blnIdentityRowsNotFound =  pyUtils.getIdentityRowsByStatus(strResponse, 'NotFound', intIdentityRowsNotFound);
					

						Log(3,'getImportDataStatus','>>>> blnIdentityRowsFailedURL = ' || blnIdentityRowsFailedURL || ' strIdentityRowsFailedURL = ' || strIdentityRowsFailedURL);
						Log(3,'getImportDataStatus','>>>> blnIdentityRowsRejectedURL = ' || blnIdentityRowsRejectedURL || ' strIdentityRowsRejectedURL = ' || strIdentityRowsRejectedURL);
						Log(3,'getImportDataStatus','>>>> blnIdentityRowsSuccess = ' || blnIdentityRowsSuccess || ' intIdentityRowsSuccessful = ' || intIdentityRowsSuccessful);
						Log(3,'getImportDataStatus','>>>> blnIdentityRowsFailed = ' || blnIdentityRowsFailed || ' intIdentityRowsFailed = ' || intIdentityRowsFailed);
						Log(3,'getImportDataStatus','>>>> blnIdentityRowsRejected = ' || blnIdentityRowsRejected || ' intIdentityRowsRejected = ' || intIdentityRowsRejected);
						Log(3,'getImportDataStatus','>>>> blnIdentityRowsNotFound = ' || blnIdentityRowsNotFound || ' intIdentityRowsNotFound = ' || intIdentityRowsNotFound);
					end;
					/* if not imported */
					else do;
						blnStepStatusValidation =  pyUtils.getStepStatus(strResponse, 'Import Validation', strStepStatus);
						Log(3, 'getImportDataStatus','>>>> blnStepStatusValidation = ' || blnStepStatusValidation || ' strStepStatus = ' || strStepStatus);
						if blnStepStatusValidation then do;
							intImportDataStatus = 4;
						end;

						blnStepStatusDataProcessing =  pyUtils.getStepStatus(strResponse, 'Data Processing', strStepStatus);
						Log(3, 'getImportDataStatus','>>>> blnStepStatusDataProcessing = ' || blnStepStatusDataProcessing || ' strStepStatus = ' || strStepStatus);
						if blnStepStatusDataProcessing then do;
							intImportDataStatus = 5;
						end;

						blnStepStatusIdentityProcessing =  pyUtils.getStepStatus(strResponse, 'Identity Processing', strStepStatus);
						Log(3, 'getImportDataStatus','>>>> blnStepStatusIdentityProcessing = ' || blnStepStatusIdentityProcessing || ' strStepStatus = ' || strStepStatus);
						if blnStepStatusIdentityProcessing then do;
							intImportDataStatus = 6;
						end;

						blnStepStatusTargetingProcessing =  pyUtils.getStepStatus(strResponse, 'Targeting Data Processing', strStepStatus);
						Log(3, 'getImportDataStatus','>>>> blnStepStatusTargetingProcessing = ' || blnStepStatusTargetingProcessing || ' strStepStatus = ' || strStepStatus);
						if blnStepStatusTargetingProcessing then do;
							intImportDataStatus = 7;
						end;

					end;
						
					%Log(3, 'getImportDataStatus','>>>> intImportDataStatus = ' || intImportDataStatus);

					return intImportDataStatus;
										
				end;

				/* GET A LIST OF ALL THE FILES FOR THIS EXPORT REQUEST */
				method CreateFileList(varchar(2048) strURL) returns int;

					declare package json objJSON();
					declare int intTokenType intParseFlags;
					declare bigint bigLineNum bigColNum;
		 			declare varchar(2048) strToken; 

					declare varchar(32767) strBody;
					declare varchar(2048) strPath strDownloadURL;
		            declare int intRC intCount;

					declare package pcrxfind objRegEx();
					declare varchar(4) strPart;

					Log(3, 'CreateFileList', '>>>> CreateFileList(' || strURL || ')');

					intRC = DoHTTPGet(strURL, strBody);
					if intRC = 0 then do;

						intCount = 0;
						intRC = objJSON.createParser(strBody);
						do while (intRC = 0); 
							Log(3, 'CreateFileList','Looking for Token=path, rc=' || intRC);

							/* search until we find the "path" tokens */
							objJSON.getNextToken(intRC, strToken, intTokenType, intParseFlags, bigLineNum, bigColNum);
							if (intRC = 0) then do;

								if (strToken eq 'path') then do;
									/* then get the very next token, which will be the value */
									objJSON.getNextToken(intRC, strPath, intTokenType, intParseFlags, bigLineNum, bigColNum);
									if (intRC = 0) then do;				

										/* now the next one up is the URL token */	
										objJSON.getNextToken(intRC, strToken, intTokenType, intParseFlags, bigLineNum, bigColNum);
										if (intRC = 0) then do;

											/* followed by the value */
											objJSON.getNextToken(intRC, strDownloadURL, intTokenType, intParseFlags, bigLineNum, bigColNum);
											if (intRC = 0) then do;
												Log(2, 'CreateFileList', strPath || '=' || strDownloadURL);
												intCount = intCount + 1;

												/* grab the part num from the path of non-header files */
												objRegEx.Parse('/(?<!header)_(\d{4})_part_\d{2}/');
							                    intRC = objRegEx.Match(strPath);
							                    if intRC > 0 then do;
							                        intRC = objRegEx.GetGroup(strPart, 1);

													part = strPart;
													file = CreateFileName(strPath, strURL);
													url = strDownloadURL;
													last_modified = .; /* we don't use this here */
													status = 0;
													m_hashFiles.add();
												end;
							                    else
							                        Log(0, 'CreateFileList', 'Parsing of path parts failed. Path: ' || strPath);

											end;
										end;
									end;
								end;

								/* reset for the next round */
								intRC = 0;

								part = '';
								file = '';
								url = '';
								status = .;

								strPart = '';
								strPath = '';
								strDownloadURL = '';
								strToken = '';

							end;
							else if (intCount = 0) then do;
								Log(0, 'CreateFileList', 'Failed to parse JSON text: ' || strBody);
								put strBody;
							end;
						end;

						objJSON.destroyParser();
					end;

					Log(3, 'CreateFileList', '<<<< CreateFileList=' || intCount);

					return intCount;
				end;

				/* PARSE THE JSON RESPONSE AND BUILD THE LIST OF FILES FOR DISCOVER PARTITIONED DETAIL DATA */
				method ParsePartitionedFileList(in_out varchar strData, varchar(3) strExportType, varchar(20) strLabel) returns int;

					declare package pcrxfind objRegEx('"entityName":\s?"(.*?)",\s*?"dataUrlDetails":.*?"url":\s?"(.*?)"');
					declare int intMatchStart intMatchEnd intRC intEntities intPos intCount;

					Log(3, 'ParsePartitionedFileList', '>>>> ParsePartitionedFileList(' || strExportType || ', ' || strLabel || ')');

					intPos = 0;
					intRC = 0;
					intEntities = 0;

					/* see how many entities were in the list so we can make sure we get them all */
					do until (intRC = 0);
						intRC = findw(strData, 'entityName', '"', intPos);
						if (intRC > 0) then do;
							intPos = intRC + 1;
							intEntities = intEntities + 1;
						end;
					end;

					/* this loop matches the data until there's no more data or matches available */
					do until ((intRC <= 0) or (lengthn(strData) = 0));
						intRC = objRegEx.Match(strData);
						if (intRC > 0) then do;
							intMatchStart = objRegEx.GetMatchStart();
							intMatchEnd = objRegEx.GetMatchEnd();

							type = strExportType;
							objRegEx.GetGroup(part, 1);
							file = type || '_' || part || '_' || strLabel || '.gz';
							objRegEx.GetGroup(url, 2);
							last_modified = .; /* we don't use this here */
							status = 0;

							m_hashFiles.add();

							/* reduce the input data by removing what we found so far before we match again */
							strData = substr(strData, intMatchEnd);

							/* reset for the next round */
							part = '';
							file = '';
							url = '';
							status = .;
						end;

					end;

					intCount = m_hashFiles.num_items;

					/* if the number we parsed doesn't equal the number we expected, return 0 so things stop */
					if (intEntities ^= intCount) then do;
						Log(0, 'ParsePartitionedFileList', 'Export included ' || intEntities || ' tables, but only ' || intCount || ' URLs were parsed from the list - stopping.');
						intCount = 0;
					end;

					Log(3, 'ParsePartitionedFileList', '<<<< ParsePartitionedFileList=' || intCount);

					return intCount;
				end;

				/* PARSE THE JSON RESPONSE AND BUILD THE LIST OF FILES FOR DISCOVER NON-PARTITIONED IDENTITY DATA */
				method CreateIdentityFileList(in_out varchar strData) returns int;

					declare package pcrxfind objRegEx('"entityName":\s*?"(.*?)",\s*?"dataUrlDetails":.*?"url":\s*?"(.*?)",\s*?"lastModifiedTimestamp":\s*?"(.*?)Z"');
					declare int intMatchStart intMatchEnd intRC intEntities intPos intCount;
					declare varchar(32) strModified;

					Log(3, 'CreateIdentityFileList', '>>>> CreateIdentityFileList()');

					intPos = 0;
					intRC = 0;
					intEntities = 0;

					/* see how many entities were in the list so we can make sure we get them all */
					do until (intRC = 0);
						intRC = findw(strData, 'entityName', '"', intPos);
						if (intRC > 0) then do;
							intPos = intRC + 1;
							intEntities = intEntities + 1;
						end;
					end;

					/* this loop matches the data until there's no more data or matches available */
					do until ((intRC <= 0) or (lengthn(strData) = 0));
						intRC = objRegEx.Match(strData);
						if (intRC > 0) then do;
							intMatchStart = objRegEx.GetMatchStart();
							intMatchEnd = objRegEx.GetMatchEnd();

							objRegEx.GetGroup(part, 1);

							if (part = 'IDENTITY') then type = 'IDS';
							if (part = 'IDENTITY_ATTRIBUTES') then type = 'IDA';
							if (part = 'IDENTITY_MAP') then type = 'IDM';

							objRegEx.GetGroup(url, 2);
							objRegEx.GetGroup(strModified, 3);
							last_modified = inputn(strModified, 'b8601dt.');
							file = type || '_' || part || '_' || put(last_modified, DTDATE.) || '_' || trim(put(last_modified, TOD2.)) || '.gz';
							status = 0;

							m_hashFiles.add();

							/* reduce the input data by removing what we found so far before we match again */
							strData = substr(strData, intMatchEnd);

							/* reset for the next round */
							part = '';
							file = '';
							url = '';
							type = '';
							last_modified = .;
							status = .;
						end;

					end;

					intCount = m_hashFiles.num_items;

					/* if the number we parsed doesn't equal the number we expected, return 0 so things stop */
					if (intEntities ^= intCount) then do;
						Log(0, 'CreateIdentityFileList', 'Export included ' || intEntities || ' tables, but only ' || intCount || ' URLs were parsed from the list - stopping.');
						intCount = 0;
					end;

					Log(3, 'CreateIdentityFileList', '<<<< CreateDiscoverFileList=' || intCount);

					return intCount;
				end;

				/* REQUEST EXPORT OF DISCOVER DBTs */
				method ExportPartitionedData(varchar(2048) strURL, varchar(256) strExportName, varchar(3) strExportType, timestamp tsFrom, timestamp tsTo) returns int;

					declare package tz objTZ();
					declare timestamp tsFromUTC tsToUTC;
					declare varchar(20) strLabel;

					declare varchar(10485760) strBody;
					declare int intRC intCount;

					Log(3, 'ExportPartitionedData', '>>>> ExportPartitionedData(' || strURL || ', ' || strExportName || ', ' || strExportType || ', ' || tsFrom || ', ' || tsTo || ')');

					if lengthn(m_strJWT) > 0 then do;

						/* convert the local times to UTC for the request */
						tsFromUTC = to_timestamp(objTZ.toUTCTime(tsFrom));
						tsToUTC = to_timestamp(objTZ.toUTCTime(tsTo));

						strURL = strURL || '&dataRangeStartTimeStamp=' || TimestampString(tsFromUTC) || '&dataRangeEndTimeStamp=' || TimestampString(tsToUTC) || '&limit=999';
						intRC = DoHTTPGet(strURL, strBody);
						if intRC = 0 then do;

							/* use from date as a timestamp - for labelling */
							strLabel =  put(tsFrom, DTDATE.) || '_' || trim(put(tsFrom, TOD2.)) ;

							/* parse the results */
							intCount = ParsePartitionedFileList(strBody, strExportType, strLabel);
						end;
					end;
					else
						Log(0, 'ExportPartitionedData', 'Cannot request a Discover export without a JSON Web Token (JWT).');

					Log(3, 'ExportPartitionedData', '<<<< ExportPartitionedData=' || intCount);

					return intCount;

				end;

				/* TRANSLATE THE SUPPLIED NAME INTO THE DESCRIPTOR ID (GUID) */
				method GetDescriptorID(varchar(2048) strDescriptorName) returns varchar(2048);

					declare varchar(32767) strBody;
					declare varchar(2048) strResult;
					declare int intRC;

					Log(3, 'GetDescriptorID', '>>>> GetDescriptorID(' || strDescriptorName || ')');

					intRC = DoHTTPGet('https://extapigwservice-' || m_strEGWEnvironment || '/marketingData/tables?name=' || strDescriptorName, strBody);
				    if intRC = 0 then strResult = GetJSONValue(strBody, 'id');

					Log(3, 'GetDescriptorID', '<<<< GetDescriptorID=' || strResult);

					return strResult;
				end;


				/*****************************************************************************************************************/
				/*                                        MAJOR EXTERNAL METHODS                                                 */
				/*****************************************************************************************************************/

				/* Create a Descriptor in 360 */
				/* This method uses a varchar variable for the Descriptor body */				
				method CreateDescriptor(varchar(32767) strDescriptorJSON) returns varchar(2048);

					declare varchar(32767) strResponse;
					declare varchar(32767) strHeader;					
					declare varchar(32767) strBody;
					declare varchar(2048) strResult;
					declare varchar(100) strDescriptorName;
		            declare int intRC;
					
					strDescriptorName = GetJSONValue(strDescriptorJSON, 'name');
					Log(3, 'CreateDescriptor', '>>>> CreateDescriptor(' || strDescriptorName || ')');

					if lengthn(strDescriptorName) <> 0 then do;
						strResult = GetDescriptorID(strDescriptorName);
					end;
					else
						Log(0, 'CreateDescriptor',  'Descriptor name not found in Input JSON!');

					if lengthn(strResult) = 0 then do;										
						intRC = DoHTTPPost('https://extapigwservice-' || m_strEGWEnvironment || '/marketingData/tables', strDescriptorJSON, strResponse, strHeader);
						if intRC = 0 then strResult = GetJSONValue(strResponse, 'id');
					end;
					else
						Log(0, 'CreateDescriptor',  'Descriptor ' || strDescriptorName || ' already exists on this Tenant.');

					Log(3, 'CreateDescriptor', '<<<< CreateDescriptor=' || strResult);

					return strResult;
				end;
				
				/* ASK CI 360 FOR A LOCATION TO UPLOAD THE SPECIFIED TYPE OF FILE */
				/* strMethod : bulkEventsFileLocation, fileTransferLocation, etc. */
				method RequestUploadPath(varchar(100) strMethod) returns varchar(2048);

					declare varchar(32767) strResponse;
					declare varchar(32767) strHeader;					
					declare varchar(32767) strBody;
					declare varchar(2048)  strResult;
		            declare int intRC;
				
					Log(3, 'RequestUploadPath', '>>>> RequestUploadPath(' || strMethod || ')');

					strResult = RequestUploadPath(strMethod, '{}');

					Log(3, 'RequestUploadPath', '<<<< RequestUploadPath=' || strResult);

					return strResult;
				end;

				/* strApplicationID is an External Application created in CI360 */
				method RequestUploadPath(varchar(100) strMethod, varchar(100) strApplicationID) returns varchar(2048);

					declare varchar(32767) strResponse;
					declare varchar(32767) strHeader;					
					declare varchar(32767) strBody;
					declare varchar(2048)  strResult;
		            declare int intRC;
				
					Log(3, 'RequestUploadPath', '>>>> RequestUploadPath(' || strMethod || ', ' || strApplicationID || ')');

					if lengthn(m_strJWT) > 0 then do;
						if lengthn(strApplicationID) > 0 then strBody = '{"version": "1", "applicationId": "' || strApplicationID || '"}';
						intRC = DoHTTPPost('https://extapigwservice-' || m_strEGWEnvironment || '/marketingData/' || strMethod, strBody, strResponse, strHeader);
						if intRC = 0 then strResult = GetJSONValue(strResponse, 'signedURL');									
					end;
					else
						Log(0, 'RequestUploadPath',  'Cannot request an upload URL without a valid JWT - make sure you are initializing this package using the correct constructor.');
					
					Log(3, 'RequestUploadPath', '<<<< RequestUploadPath=' || strResult);

					return strResult;
				end;


				/* PRIMARY METHOD TO COORDINATE COMPLETING A FILE IMPORT - 1 = SUCCESS WITH UPDATE MODE AS AN ARGUMENT*/
			   method ImportData(varchar(256) strUploadName, varchar(2048) strDescriptor, varchar(2048) strLocation, varchar(36) strUpdateMode, 
													int blnHeaderRow, int intWaitMins, in_out int intImportDataStatus, in_out varchar strFailedURL, 
													in_out varchar strRejectedURL) returns int;
				
					declare varchar(2048) strResult strImportURL;
					declare varchar(36) strDescriptorID;
					dcl int intIdentityRowsSuccessful intIdentityRowsFailed intIdentityRowsRejected intIdentityRowsNotFound;

					declare int intTries intCount intRC;


					Log(3, 'ImportData', '>>>> ImportData(' || strUploadName || ', ' || strDescriptor || ', <S3 URL>, ' || blnHeaderRow || ', ' || intWaitMins || ')');

					intRC = 0;
					intImportDataStatus = -1;
					intTries = 0;

					m_hashFiles.clear();
				
							/* figure out the descriptor ID */
							strDescriptorID = GetDescriptorID(strDescriptor);
							if lengthn(strDescriptorID) > 0 then do;

								/* then ask 360 to start the export */
								strImportURL = RequestImport(strUploadName, strLocation, strDescriptorID, strUpdateMode, blnHeaderRow);
								if lengthn(strImportURL) > 0 then do;

									/* Wait for it to be finished - up to <waitMins> times * 60 seconds each */
									do while ((intImportDataStatus = -1) and (intTries < intWaitMins));
										sleep(60, 1); /* wait 60 seconds */
										intImportDataStatus = getImportDataStatus(strImportURL, 'Imported', strFailedURL, strRejectedURL,
																					intIdentityRowsSuccessful, intIdentityRowsFailed,
																					intIdentityRowsRejected, intIdentityRowsNotFound);
										intTries = intTries + 1;
										Log(3, 'ImportData', 'Attempt ' || intTries || ': ' || intImportDataStatus);
									end;

									/* two reasons we could have fallen out of the loop above, so make sure we handle this correctly */
									if intImportDataStatus ne -1 and (intTries <= intWaitMins) then do; /*if I finished*/
									
										/*if 0,1,2,3 return rc=0 import completed*/
										if intImportDataStatus eq 0 or intImportDataStatus eq 1 or intImportDataStatus eq 2 or intImportDataStatus eq 3 then do;
											Log(0, 'ImportData',  'Imported completed');
										end;
										/*if 4,5,6,7 return rc=1 import error */
										else if intImportDataStatus eq 4 or intImportDataStatus eq 5 or intImportDataStatus eq 6 or intImportDataStatus eq 7 then do;
											Log(0, 'ImportData',  'Imported error');
											intRc = 1;
										end;
									end;
									else do;
										/*if 8,9,10 return rc=2 serious error*/
										intImportDataStatus = 8;
										Log(0, 'ImportData', 'Timed out waiting for files to be ready.');
										intRc = 2;
									end;
								end;
								else do;
									intImportDataStatus = 9;
									Log(0, 'ImportData', 'Failed to get the import job URL.');
									intRc = 2;
								end;
							end;
							else do;
								intImportDataStatus = 10;
								Log(0, 'ImportData',  'Could not get Descriptor ID for ' || strDescriptor || '.');
								intRc = 2;
							end;

					Log(3, 'ImportData', '>>>>> ImportData = ' || intRC);

					return intRC;

				end;

				/* REQUEST EXPORT OF DISCOVER IDENTITY DATA */
				method ExportIdentityData(varchar(256) strExportName) returns int;

					declare varchar(10485760) strBody;
					declare int intRC intCount;

					Log(3, 'ExportIdentityData', '>>>> ExportIdentityData(' || strExportName || ')');

					if lengthn(m_strJWT) > 0 then do;
						m_hashFiles.clear();

						intRC = DoHTTPGet('https://extapigwservice-' || m_strEGWEnvironment || '/marketingGateway/discoverService/dataDownload/eventData/detail/nonPartitionedData?agentName=' || m_strAgent, strBody);
			            if (intRC = 0) then do;
							/* parse the results */
							intCount = CreateIdentityFileList(strBody);

							/* output the table */
							if (intCount > 0) then
								m_hashFiles.output('work.' || strExportName || '_ids');
							else
								Log(0, 'ExportIdentityData', 'The file list is empty.');
			            end;

					end;
					else
						Log(0, 'ExportIdentityData', 'Cannot request a Discover export without a JSON Web Token (JWT).');

					Log(3, 'ExportIdentityData', '<<<< ExportIdentityData = ' || intCount);

					return intCount;

				end;				

				/* REQUEST EXPORT OF DISCOVER DETAIL MART DATA */
				/* external DBT-only method */
				method ExportDiscoverDetail(varchar(256) strExportName, timestamp tsFrom, timestamp tsTo) returns int;
					return ExportDiscoverDetail(strExportName, tsFrom, tsTo, 1);
				end;

				/* full method that knows whether this is being run independently or as part of a larger request */
				method ExportDiscoverDetail(varchar(256) strExportName, timestamp tsFrom, timestamp tsTo, int blnIndependent) returns int;

					declare int intCount;

					Log(3, 'ExportDiscoverDetail', '>>>> ExportDiscoverDetail(' || strExportName || ', ' || tsFrom || ', ' || tsTo || ', ' || blnIndependent || ')');

					/* make sure we have a clean hash for this run if this is being called independently */
					if (blnIndependent) then m_hashFiles.clear();

					intCount = ExportPartitionedData('https://extapigwservice-' || m_strEGWEnvironment || '/marketingGateway/discoverService/dataDownload/eventData/detail/partitionedData?agentName=' || m_strAgent, strExportName, 'DTL', tsFrom, tsTo);

					/* output the table if this is being called independently */
					if (blnIndependent) then do;
						if (intCount > 0) then
							m_hashFiles.output('work.' || strExportName || '_dtls');
						else
							Log(0, 'ExportDiscoverDetail', 'The file list is empty.');
					end;

					Log(3, 'ExportDiscoverDetail', '<<<< ExportDiscoverDetail=' || intCount);

					return intCount;

				end;

				/* REQUEST EXPORT OF DISCOVER DBTs */
				/* external DBT-only method */
				method ExportDiscoverDBT(varchar(256) strExportName, timestamp tsFrom, timestamp tsTo) returns int;
					return ExportDiscoverDBT(strExportName, tsFrom, tsTo, 1);
				end;

				/* full method that knows whether this is being run independently or as part of a larger request */
				method ExportDiscoverDBT(varchar(256) strExportName, timestamp tsFrom, timestamp tsTo, int blnIndependent) returns int;

					declare int intCount;

					Log(3, 'ExportDiscoverDBT', '>>>> ExportDiscoverDBT(' || strExportName || ', ' || tsFrom || ', ' || tsTo || ', ' || blnIndependent || ')');

					/* make sure we have a clean hash for this run if this is being called independently */
					if (blnIndependent) then m_hashFiles.clear();

					intCount = ExportPartitionedData('https://extapigwservice-' || m_strEGWEnvironment || '/marketingGateway/discoverService/dataDownload/eventData/dbtReport?agentName=' || m_strAgent, strExportName, 'DBT', tsFrom, tsTo);

					/* output the table if this is being called independently */
					if (blnIndependent) then do;
						if (intCount > 0) then
							m_hashFiles.output('work.' || strExportName || '_dbts');
						else
							Log(0, 'ExportDiscoverDBT', 'The file list is empty.');
					end;

					Log(3, 'ExportDiscoverDBT', '<<<< ExportDiscoverDBT=' || intCount);

					return intCount;

				end;

				/* REQUEST EXPORT OF DISCOVER DATA */
				method DiscoverExport(varchar(256) strExportName, int blnDetails, int blnDBTs, timestamp tsFrom, timestamp tsTo) returns int;

					declare int intCount;

					Log(3, 'DiscoverExport', '>>>> DiscoverExport(' || strExportName || ', ' || blnDetails || ', ' || blnDBTs || ', ' || tsFrom || ', ' || tsTo || ')');

					intCount = 0;

					/* make sure we have a clean hash for this run */
					m_hashFiles.clear();

					/* get the files - depending on which variables are enabled */
					if blnDetails then intCount = ExportDiscoverDetail(strExportName, tsFrom, tsTo, 0);
					if blnDBTs then intCount = intCount + ExportDiscoverDBT(strExportName, tsFrom, tsTo, 0);

					/* output the table */
					if (intCount > 0) then
						m_hashFiles.output('work.' || strExportName || '_discover');
					else
						Log(0, 'DiscoverExport', 'The file list is empty.');

					Log(3, 'DiscoverExport', '<<<< DiscoverExport=' || intCount);

					return intCount;

				end;


				/*****************************************************************************************************************/
				/*                                               PROPERTIES                                                      */
				/*****************************************************************************************************************/

				/* PROPERTY: Debug Mode */
				method LogLevel(int intLevel);
					m_intLogLevel = intLevel;
				end;

				method LogLevel() returns int;
					return m_intLogLevel;
				end;

				/* READ-ONLY PROPERTY: Version */
				method Version() returns varchar(10);
					return dblVersion;
				end;

				method MajorVersion() returns int;
					return dblVersion;
				end;

				/* READ-ONLY PROPERTY: JWT */
				method JWT() returns varchar(256);
					return m_strJWT;
				end;

		
				/*****************************************************************************************************************/
				/*                                              CONSTRUCTORS                                                     */
				/*****************************************************************************************************************/

				/* COMMON SETUP STUFF CALLED BY ALL CONSTRUCTORS */
				method Setup(varchar(40) strEnvironment, int intLogLevel);

					Log(3, 'Setup', '>>>> Setup(' || strEnvironment || ', ' || intLogLevel || ')');

					dblVersion = &version;

					Log(1, 'Setup', 'CI360Utilities Package Version ' || Version());

					m_strEnvironment = strEnvironment;

					/* Set up the External Gateway Values */
					if strEnvironment 	   = 'use.ci360.sas.com' 	  then m_strEGWEnvironment = 'prod.ci360.sas.com';
					else if strEnvironment = 'euw.ci360.sas.com' 	  then m_strEGWEnvironment = 'eu-prod.ci360.sas.com';
					else if strEnvironment = 'int.cidev.sas.com' 	  then m_strEGWEnvironment = 'tst.cidev.sas.us';
					else if strEnvironment = 'prod.cidemo.sas.com' 	  then m_strEGWEnvironment = 'demo.cidemo.sas.com';
					else if strEnvironment = 'training.ci360.sas.com' then m_strEGWEnvironment = 'training.ci360.sas.com';
					else if strEnvironment = 'eurc.ci3dev.sas.com' 	  then m_strEGWEnvironment = 'eurc.cidev.sas.us';
					else if strEnvironment = 'syd.ci360.sas.com'      then m_strEGWEnvironment = 'syd-prod.ci360.sas.com';
					else if strEnvironment = 'extapigwservice-eu-prod.ci360.sas.com' then m_strEGWEnvironment = 'eu-prod.ci360.sas.com';

					Log(2, 'Setup', 'CI360Utilities EGW Environment=' || m_strEGWEnvironment);


					/* Set up the hash */
					m_hashFiles.keys([part]);
					m_hashFiles.data([part file type url last_modified status]);
					m_hashFiles.ordered('yes');
					m_hashFiles.defineDone();

					Log(3, 'Setup', '<<<< Setup');

				end;

				/* Proxy method to be called if needed */
				method SetupProxy(varchar(512) strProxyURL, varchar(128) strProxyUser, varchar(128) strProxyPassword );
					
					if m_intLogLevel >= 4 then
						Log(4, 'SetupProxy', '>>>> SetupProxy(' || strProxyURL || ', ' || strProxyUser || ', ' || strProxyPassword || ')', 1);
					else
						Log(3, 'SetupProxy', '>>>> SetupProxy(' || strProxyURL || ', ' || strProxyUser || ', ' ||  ', ******************)');
	
					/* Set the flag to tell the DoHTTP methods to use the proxy */
					m_intUseProxy = 1;

					/* Set up the proxy server */
					m_strProxyURL = strProxyURL;
					m_strProxyUser = strProxyUser;
					m_strProxyPassword = strProxyPassword;
					
					Log(3, 'SetupProxy', '<<<< SetupProxy');
				
				end;
			
				/* CONSTRUCTOR FOR AGENT-BASED APIS: CI360Utilities(environment abbreviation, tenant, agent, secret, debug flag) */
		        method CI360Utilities(varchar(100) strEnvironment, varchar(64) strTenantID, varchar(128) strAgent, varchar(256) strSecret, int intLogLevel);
					m_intLogLevel = intLogLevel; /* have to do this first for Log to work! */

					if m_intLogLevel >= 4 then
						Log(4, 'CI360Utilities', '>>>> CI360Utilities(' || strEnvironment || ', ' || strTenantID || ', ' || strAgent ||  ', ' || strSecret || ', ' || intLogLevel || ')', 1);
					else
						Log(3, 'CI360Utilities', '>>>> CI360Utilities(' || strEnvironment || ', ' || strTenantID || ', ' || strAgent ||  ', ****************** , ' || intLogLevel || ')');

					Setup(strEnvironment, intLogLevel);

					m_strAgent = strAgent;

			        /* Get this once and then we can reuse it */
		            m_strJWT = GenerateJWT(strTenantID, strSecret);

					Log(4, 'CI360Utilities', '<<<< CI360Utilities');

		        end;

		    endpackage;
		    run;
		quit;

		/* see if it worked */
		%let rc = %eval(((&SYSERR = 0) or (&SYSERR = 4)));		

	%end;

%mend;

proc format;
   picture logdttm other='%Y-%0m-%0d %0H:%0M:%0s' (datatype=datetime);
run;
/*PYTHON PACKAGE CREATION AND INTERFACE WITH DS2*/
%macro createPyUtilsPackage(pythonFile=);

	data work.pyUtils_tmp;
		length line $32767.;
	   	infile "&pythonFile." lrecl=32767 end=eof;
	    input;
		line = CAT("'",_infile_,"'");

		call symputx(CATS('append_src_line', put(_N_, 8. -l)), line);

		if eof then do;
			call symputx('num_line', _N_);
		end;
	run;

	proc ds2 NOLIBS CONN="((DRIVER=BASE;CATALOG=&sas_utility_library;SCHEMA=(NAME=&sas_utility_library;PRIMARYPATH={&sas_utility_path/data}));(DRIVER=BASE;CATALOG=WORK;SCHEMA=(NAME=WORK;PRIMARYPATH={%sysfunc(pathname(work))}));)";
	    ds2_options sas;
	    package &sas_utility_library..pyUtils /overwrite=yes;
	        declare package pymas py();
			declare varchar(67108864) character set utf8 pycode;
			declare int rc revision;
			declare tinyint blnResult;
			
			/*PYTHON MODULE PUBLICATION*/
			method pyUtils( varchar(256) modulename);
				%do _i=1 %to &num_line. ;
					rc = py.appendSrcLine(&&append_src_line&_i);	
				%end ;
				if rc then %Log(0,'pyUtils','py.appendSrcLine() failed.');
				pycode = py.getSource();
		        revision = py.publish( pycode , modulename);
				/*if revision lt 1 then 
                	Log(0, 'pyUtils','py.publish() failed.');*/

	       	end ;

			 method getStatus(varchar(32000) response) returns varchar;
				dcl varchar(100) status;

				rc = py.useMethod( 'get_status' );
				if rc then 
                	%Log(0, 'getStatus','py.useMethod() failed.');
				rc = py.setString( 'response', response );
				rc = py.execute();
	            status = py.getString( 'status' );
				/*Log(3, 'getStatus','>>>> status=' || status);*/
				return status;
				
	        end;

				/*GET STATUS FROM RESPONSE*/

				method getStepStatus(varchar(32000) response, 
								 varchar(100) stepName, in_out varchar stepStatus ) returns tinyint;

				rc = py.useMethod( 'get_step_status' );
				if rc then 
                	%Log(0, 'getStepStatus','py.useMethod() failed.');
				rc = py.setString( 'response', response ); 
	            rc = py.setString( 'stepName', stepName );
				rc = py.execute();
	            stepStatus = py.getString( 'stepStatus' );
			/*	Log(3,'getStepStatus','>>>> stepStatus=' || stepStatus);*/
				if stepStatus in('FAILED','FAILED_VALIDATION','FAILED_IDENTITIES') then do;
					blnResult = 1;
				/*	Log(0, 'getStepStatus','There is a failed step status');*/
				end;
				else do;
					blnResult = 0;
					/*Log(3, 'getStepStatus','There is a no failed step status');*/
				end;
				return blnResult;
				
	        end;
			
			/*CHECK NUMBER OF IDENTITY BY STATUS*/
			method getIdentityRowsByStatus(varchar(32000) response, varchar(100) status, in_out int identity_rows ) returns tinyint;

				rc = py.useMethod( 'get_identity_rows_by_status' );
				if rc then 
                	%Log(0, 'getIdentityRowsByStatus','py.useMethod() failed.');
				rc = py.setString( 'response', response );  
				rc = py.setString( 'status', status );   
				rc = py.execute();             
	            identity_rows = py.getInt( 'identity_rows' );
				if identity_rows = -1 then do;
					blnResult = 1;
				/*	Log(0, 'getIdentityRowsByStatus','There is a no identity rows');*/
				end;
				else do;
					blnResult = 0;
				/*	Log(3, 'getIdentityRowsByStatus','There is a identity rows');*/
				end;
				return blnResult;
				
	        end;

			/*CHECK IF STEP IS FAILED*/
		    method getStepUrlFailed(varchar(32000) response,in_out varchar stepUrlFailed) returns tinyint;

				rc = py.useMethod('get_step_url_failed');
				if rc then 
                	%Log(0, 'getStepUrlFailed','py.useMethod() failed.');
				rc = py.setString('response', response);  
				rc = py.execute();             
	            stepUrlFailed = py.getString( 'stepUrlFailed' );
				/*%Log(3,'getStepUrlFailed','>>>> stepUrlFailed = ' || stepUrlFailed);*/

				if (stepUrlFailed eq 'RECORD_UNAVAILABLE') then do;
					blnResult = 0;
					/*Log(0, 'getStepUrlFailed','There is no url failed');*/
				end;
				else do;
					blnResult = 1;
				/*	Log(3, 'getStepUrlFailed','There is a failed url');*/
				end;
				return blnResult;
				
				
	        end;

			/*CHECK IF STEP IS REJECTED*/
		    method getStepUrlRejected(varchar(32000) response,in_out varchar stepUrlRejected) returns tinyint;

				rc = py.useMethod('get_step_url_rejected');
				if rc then 
                	%Log(0, 'getStepUrlRejected','py.useMethod() failed.');
				rc = py.setString('response', response);   
				rc = py.execute();             
	            stepUrlRejected = py.getString( 'stepUrlRejected' );
				/*%Log(3,'getStepUrlRejected','>>>> stepUrlRejected = ' || stepUrlRejected);*/
				if (stepUrlRejected eq 'RECORD_UNAVAILABLE') then do;
				/*	Log(0, 'getStepUrlRejected','There is no url rejected');*/
					blnResult = 0;
				end;
				else do;
				/*	Log(3, 'getStepUrlRejected','There is a url rejected');*/
					blnResult = 1;
				end;
				return blnResult;
				
	        end;

	       
	    endpackage;

	    run;
	quit;
%mend createPyUtilsPackage;
%macro Log(level, method, message, protected);
	%if %eval(&sas_log_level >= &level) %then %do;
		/* if this is protected info and we're not using the "override" log level, don't show the message */
		%if %eval((&sas_log_level < 4) and (&protected = 1)) %then %let message = <protected>;
		%put %sysfunc(datetime(), logdttm.6)000, &method, &message;
	%end;	
%mend;
%createPyUtilsPackage(pythonFile=%nrquote(&sas_utility_path./pyUtils.py));
%CreatePackage();