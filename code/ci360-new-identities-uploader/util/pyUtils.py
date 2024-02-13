# /******************************************************************************/
# /* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
# /* SPDX-License-Identifier: Apache-2.0                                        */
# /* ****************************************************************************/

import json

def get_status(response):
    "Output: status"
    
    status = json.loads(response).get("status")
    if not status: 
        status = "STATUS_NOT_AVAILABLE"
    return status

def get_step_status(response, stepName):
    "Output: stepStatus"
    stepStatus=""
    steps = json.loads(response).get("statusInfo")
    if steps: 
        for step in steps:
            if(step.get("step") == stepName):
                stepStatus = step.get("status")
    if not stepStatus:
        stepStatus="STATUS_NOT_AVAILABLE"
    return stepStatus
    
    
def get_version(response):
    "Output: version"
    
    version = json.loads(response).get("version")

    return version
    
def get_identity_rows_by_status(response, status):
    "Output: identity_rows"
    
    identity_rows = json.loads(response).get("identityRows"+status)
    if not identity_rows:
        identity_rows=-1  
    return identity_rows
    
def get_step_url_failed(response):
    "Output: stepUrlFailed"
    
    stepUrlFailed = "RECORD_UNAVAILABLE"
    substring = "failures"
    steps_failed = json.loads(response).get("identityRowsFailed")
    if steps_failed: 
        if steps_failed>=1:
            steps = json.loads(response).get("downloadItems")
            for step in steps:
                if substring in step.get("url"):
                    stepUrlFailed = step.get("url")
    return stepUrlFailed
    
def get_step_url_rejected(response):
    "Output: stepUrlRejected"
    
    stepUrlRejected = "RECORD_UNAVAILABLE"
    substring = "rejections"
    steps_rejected = json.loads(response).get("identityRowsRejected")
    if steps_rejected: 
        if steps_rejected>=1:
            steps = json.loads(response).get("downloadItems")
            for step in steps:
                if substring in step.get("url"):
                    stepUrlRejected = step.get("url")
    return stepUrlRejected
    