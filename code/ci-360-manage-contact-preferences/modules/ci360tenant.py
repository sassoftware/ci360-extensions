# Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import base64
import logging
import jwt
import sys

class Ci360Tenant:

    def __init__(self, tenant_id, client_secret, base_url):
        self.log = logging.getLogger(__name__)
        # Instance attributes, unique to each object
        self.tenant_id = tenant_id
        self.client_secret = client_secret
        self.base_url = base_url
        self.jwt_token = self.generate_jwt(tenant_id, client_secret)   

    # An instance method, operates on the object's data
    def generate_jwt(self, tenantID, client_secret):
        """Return JWT token string."""
        encodedSecret = base64.b64encode(bytes(client_secret, 'utf-8'))
        token = jwt.encode({'clientID': tenantID}, encodedSecret, algorithm='HS256')
        if not token:
            self.log.error("Error while generating jwt token.")
            sys.exit(1)
        jwt_token = token if isinstance(token, str) else token.decode()
        jwt_token = jwt_token.strip()
        self.log.debug("Generated jwt token.")
        return jwt_token

