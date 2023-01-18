"""
Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import datetime, jwt, json, os
from jwt.algorithms import RSAAlgorithm

with open("/app/config/.snowflake_rsa_private_key.pem") as f:
    private_key = f.read()

with open("/app/config/.passwd-snowflake") as f:
	keys = f.read().split(":")
	sf_username = keys[2]
	sf_public_key_fp = keys[0]+":"+keys[1]

_sf_account = os.getenv('SF_ACCOUNT').upper()

payload = {
  'iss': _sf_account+'.'+sf_username+'.'+sf_public_key_fp,
  'sub': _sf_account+'.'+sf_username,
  'iat': datetime.datetime.utcnow(),
  'exp': datetime.datetime.utcnow() + datetime.timedelta(seconds=360000)
}

def main():
    encoded_jwt_token = jwt.encode(payload,private_key,'RS256')
    return encoded_jwt_token

if __name__ == "__main__":
    x = main()
    print(x)
