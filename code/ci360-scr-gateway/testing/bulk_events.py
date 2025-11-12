import time
import sys
import jwt
import requests
import shlex
from pathlib import Path
from base64 import b64encode
from datetime import datetime, timedelta

def bulk_events(number_of_events: int):
  event_name = 'TheTrigger'

  # Create event data
  event_data = '\n'.join(f'{event_name},subject_id,1660,name,"Hello world!",number,{number}' for number in range(number_of_events))
  #print(event_data)

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

  # get AWS url
  endpoint = f'https://{CI360_GATEWAY_HOST}/marketingGateway/bulkEventsFileLocation'
  payload = { 'version': 1, 'applicationId': 'eventGenerator' }
  headers = { 
    'Content-Type': 'application/json', 
    'Authorization': f'Bearer {token}'
  }
  response = requests.post(endpoint, json=payload, headers=headers)
  print(response.status_code, response.text)
  response.raise_for_status()
  aws_url = next(link['href'] for link in response.json().get('links', []) if link['rel']=='self')

  # PUT event_data to AWS url
  headers = { 'Content-Type': 'application/octet-stream' } 
  response = requests.put(aws_url, data=event_data, headers=headers)
  print(response.status_code, response.text)
  response.raise_for_status()

bulk_events(150)
