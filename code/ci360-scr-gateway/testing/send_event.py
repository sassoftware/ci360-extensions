import json
import sys
import jwt
import requests
import shlex
from pathlib import Path
from base64 import b64encode

# Load configuration from file "360.env"
config_path = Path(sys.path[0]).parent / '360.env'
lexer = shlex.shlex(config_path.read_text(), posix=True)
lexer.whitespace_split = True
lexer.whitespace = '\n'
config = dict(list(map(str.strip, line.split('=', 1))) for line in lexer)

CI360_GATEWAY_HOST  = config['CI360_GATEWAY_HOST']
CI360_TENANT_ID     = config['CI360_TENANT_ID']
CI360_CLIENT_SECRET = config['CI360_CLIENT_SECRET']

# Create token
b64_secret = b64encode(bytes(CI360_CLIENT_SECRET, encoding='utf8'))
token = jwt.encode({'clientID': CI360_TENANT_ID}, b64_secret, algorithm='HS256')

# Setup requests session
session = requests.session()
session.headers.update({ 
  'Content-Type': 'application/json', 
  'Authorization': f'Bearer {token}'
})

# POST payload to /marketingGateway/events endpoint
endpoint = f'https://{CI360_GATEWAY_HOST}/marketingGateway/events'
payload = {
  'eventName': 'TheTrigger',
  'subject_id': 1669,
  'name': 'hello',
  'number': 121
}
response = session.request('POST', endpoint, data=json.dumps(payload))
print(response.text)
