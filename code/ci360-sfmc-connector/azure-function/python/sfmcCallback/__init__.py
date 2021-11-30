"""
Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import logging
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request. Callback function not currently implemented.')

    body = req.get_body()
    logging.info("body: %s", body)

    return func.HttpResponse(
            "This HTTP triggered function executed successfully. Callback function is not currently implemented.",
            status_code=200
    )
