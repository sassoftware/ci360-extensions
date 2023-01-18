/*
Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

CREATE STAGE <PREFIX>_S3_DSCWH URL = 's3://<S3 BUCKET>/dscwh/' CREDENTIALS = (AWS_KEY_ID = '<KEY>' AWS_SECRET_KEY = '<SECRET>');