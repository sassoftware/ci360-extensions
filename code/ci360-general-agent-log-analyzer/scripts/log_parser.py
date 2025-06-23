# Todo:
# Support for multiple agent logs

import gzip
import json
from datetime import UTC, datetime
from pathlib import Path
import re
from collections import defaultdict

# regex patterns
LINE_PATTERN = re.compile('^(.+) thread:"(.*)" level:"(.*)" logger:"(.*)" message:"(.*)" exception:"(.*)"$')
URL_PATTERN = re.compile('^rawUrl=.*, method=(.+), decodedUrlTxt=(.+)$')

# z85+gzip decoder
Z85_CHARS = ('0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#')
Z85_INDEX = {c: n for n, c in enumerate(Z85_CHARS)}

def gz85_decode(encoded):
  padding = (5 - len(encoded)) % 5
  z85_str = encoded + Z85_CHARS[-1]*padding
  data = bytearray()
  for i in range(0, len(z85_str), 5):
    chunk = z85_str[i:i+5]
    value = 0
    for c in chunk:
      value = value*85 + Z85_INDEX[c]
    data.extend([(value >> 24) & 0xFF, (value >> 16) & 0xFF, (value >> 8) & 0xFF, value & 0xFF])
  bs = bytes(data[:-padding]) if padding else bytes(data)
  return gzip.decompress(bs).decode('utf-8')

def parse_request(request_str: str, decode_event: bool):
  request_data = json.loads(request_str)
  if proxy_event_str := request_data.get('attributes', {}).get('connector-agent-proxy-event'):
    proxy_event = json.loads(proxy_event_str)
    request_id = proxy_event['requestId']
    if not decode_event:
      return request_id
    return request_id, gz85_decode(proxy_event['body'])

def request_after_prefix(message: str, prefix:str, decode_event=False):
  if message.startswith(prefix + '{'):
    return parse_request(message.removeprefix(prefix), decode_event)

def parse_line(line_no: int, line: str, thread_prefix:str):
  match = LINE_PATTERN.match(line)
  logline = {
    'line_no': line_no + 1,
    'timestamp': datetime.fromisoformat(match.group(1)).replace(tzinfo=None),
    'thread': f'{thread_prefix}-{match.group(2)}' if thread_prefix else match.group(2),
    'level': match.group(3),
    'logger': match.group(4),
    'message': match.group(5).replace('↵', '\n'),
    'stack_trace': match.group(6).replace('↵', '\n')
  }
  return logline

def parse_log(log_files):
  loglines_by_thread = defaultdict(list)
  record_list = list()
  request_dict = dict()

  def get_request(request_id):
    if request_id not in request_dict:
      request_dict[request_id] = {
        'request_id': request_id,
        'event_guid': None,
        'websocket_thread': None,
        'cms_thread': None,
        'connector_thread': None,
        'event_dttm': None,
        'request_start_dttm': None,
        'request_end_dttm': None,
        'cms_start_dttm': None,
        'cms_end_dttm': None,
        'connector_start_dttm':None,
        'connector_end_dttm': None,
        'endpoint_start_dttm': None,
        'endpoint_end_dttm': None,
        'http_method': None,
        'target_url': None,
        'http_status': None,
        'error_line_no': None,
        'stack_trace': None,
        'response_payload': None,
        'event_payload': None
      }
    return request_dict[request_id]

  # Loop log files, parse log lines and group by thread
  if isinstance(log_files, str) or isinstance(log_files, Path):
    log_files = [ log_files ]
  for agent_no, log_file in enumerate(log_files):
    thread_prefix = f'a{agent_no}' if len(log_files)>1 else None
    line_no = 0
    with open(log_file, 'r') as f:
      for line in f:
        line_no = line_no + 1
        logline = parse_line(line_no, line, thread_prefix)
        loglines_by_thread[logline['thread']].append(logline)

  # Process log lines by thread
  for thread, loglines in loglines_by_thread.items():
    request = None

    for logline in loglines:
      message = logline['message']
      line_no = logline['line_no']
      timestamp = logline['timestamp']

      match logline['logger']:
        case 'com.sas.mkt.apigw.sdk.streaming.agent.connection.EventStreamWebSocketHandler':
          # Request received by web socket 
          if result := request_after_prefix(message, prefix='EventStreamWebSocketHandler received message: ', decode_event=True):
            request_id, event_payload = result
            request = get_request(request_id)
            event = json.loads(event_payload)
            event_dttm = datetime.fromtimestamp(event['date']['generatedTimestamp']*0.001, UTC).replace(tzinfo=None)
            request['request_id'] = request_id
            request['event_guid'] = event['guid']
            request['event_dttm'] = event_dttm
            request['event_payload'] = event_payload
            request['websocket_thread'] = thread
            request['request_start_dttm'] = timestamp
            record_list.append({ 'timestamp': event_dttm, 'line_no': None,    'thread': None,   'event': 'request-generated', 'source':'360',    'request_id': request['request_id'] })
            record_list.append({ 'timestamp': timestamp,  'line_no': line_no, 'thread': thread, 'event': 'request-received',  'source':'stream', 'request_id': request['request_id'] })
            request = None

          # Processing complete
          if message.startswith('Sending message:') and request:
            request['request_end_dttm'] = timestamp
            request = None

        case 'com.sas.mkt.apigw.sdk.streaming.agent.plugin.listener.thirdparty.CMSProcessor':
          # CMS request processing starts
          if request_id := request_after_prefix(message, prefix='received:'):
            request = get_request(request_id)
            request['request_id'] = request_id
            request['cms_thread'] = thread
            request['cms_start_dttm'] = timestamp
            record_list.append({ 'timestamp': timestamp, 'line_no': line_no, 'thread': thread, 'event': 'thread-acquired', 'source':'cms', 'request_id': request['request_id'] })

          # CMS request processing ends
          if message.startswith('CMS Integration not enabled') and request:
            request['cms_end_dttm'] = timestamp
            record_list.append({ 'timestamp': timestamp, 'line_no': line_no, 'thread': thread, 'event': 'thread-released', 'source':'cms', 'request_id': request['request_id'] })
            request = None

        case 'com.sas.mkt.apigw.sdk.streaming.agent.plugin.listener.thirdparty.ConnectorsProcessor':
          # Connector processing starts 
          if request_id := request_after_prefix(message, prefix='received:'):
            request = get_request(request_id)
            request['request_id'] = request_id
            request['connector_thread'] = thread
            request['connector_start_dttm'] = timestamp
            record_list.append({ 'timestamp': timestamp, 'line_no': line_no, 'thread': thread, 'event': 'thread-acquired', 'source':'connector', 'request_id': request['request_id'] })

          # HTTP method and target url
          elif match := URL_PATTERN.match(message):
            if request:
              request['http_method'] = match.group(1)
              request['target_url'] = match.group(2)

          # Response body from target endpoint
          elif message.startswith('responseBody=') and request:
            request['response_payload'] = message.removeprefix('responseBody=')
            request['endpoint_end_dttm'] = timestamp

          # Connecotor processing ends
          elif message.startswith('sending response:CONNECTORS_AGENT_PROXY:') and request:
            request['connector_end_dttm'] = timestamp
            record_list.append({ 'timestamp': timestamp, 'line_no': line_no, 'thread': thread, 'event': 'thread-released', 'source':'connector', 'request_id': request['request_id'] })

          # Error
          elif logline['level']=='ERROR' and request:
            request['connector_end_dttm'] = timestamp
            request['request_end_dttm'] = timestamp
            request['error_line_no'] = line_no
            request['stack_trace'] = logline['stack_trace']
            record_list.append({ 'timestamp': timestamp, 'line_no': line_no, 'thread': thread, 'event': 'request-failed',  'source':'connector', 'request_id': request['request_id'] })
            record_list.append({ 'timestamp': timestamp, 'line_no': line_no, 'thread': thread, 'event': 'thread-released', 'source':'connector', 'request_id': request['request_id'] })
            if request['endpoint_start_dttm']:
              request['http_status'] = 'Failed'
              record_list.append({ 'timestamp': timestamp, 'line_no': line_no, 'thread': thread, 'event': 'endpoint-complete', 'source':'endpoint', 'request_id': request['request_id'] })
            request = None
    
        case 'org.springframework.web.client.RestTemplate':
          # Request sent to target endpoint
          if message.startswith('Writing ') and request:
            request['endpoint_start_dttm'] = timestamp
            record_list.append({ 'timestamp': timestamp, 'line_no': line_no, 'thread': thread, 'event': 'endpoint-request', 'source':'endpoint', 'request_id': request['request_id'] })
          
          # Http response from target endpoint
          elif message.startswith('Response ') and request:
            http_status = message.removeprefix('Response ')
            request['http_status'] = http_status
            request['endpoint_end_dttm'] = timestamp
            record_list.append({ 'timestamp': timestamp, 'line_no': line_no, 'thread': thread, 'event': 'endpoint-complete', 'source':'endpoint', 'request_id': request['request_id'] })

  # Sort log records
  record_list.sort(key=lambda record: (record['timestamp'], record['line_no'] or -1))

  # Add metrics to log records
  occupied_threads   = 0
  endpoint_waiting   = 0
  requests_generated = 0
  requests_received  = 0
  requests_completed = 0
  requests_failed    = 0
  requests_pending   = 0
  requests_queued    = 0
  for record in record_list:
    match record['event']:
      case 'request-generated':
        requests_generated = requests_generated + 1
      case 'thread-acquired':
        occupied_threads = occupied_threads + 1
      case 'thread-released':
        occupied_threads = occupied_threads - 1
        if record['source'] == 'connector': 
          requests_completed = requests_completed + 1
      case 'request-received':
        requests_received = requests_received + 1
      case 'request-failed':
        requests_failed = requests_failed + 1
      case 'endpoint-request':
        endpoint_waiting = endpoint_waiting + 1
      case 'endpoint-complete':
        endpoint_waiting = endpoint_waiting - 1
    requests_pending = requests_received - requests_completed
    requests_queued =  requests_generated - requests_received
    record['occupied_threads']   = occupied_threads
    record['endpoint_waiting']   = endpoint_waiting
    record['requests_generated'] = requests_generated
    record['requests_received']  = requests_received
    record['requests_completed'] = requests_completed
    record['requests_failed']    = requests_failed
    record['requests_pending']   = max(requests_pending, 0)
    record['requests_queued']    = max(requests_queued, 0)

  # request list 
  request_list = [request for request in request_dict.values() if request['event_dttm']]
  request_list.sort(key=lambda request: request['event_dttm'])

  return request_list, record_list

def dump(item):
  if isinstance(item, datetime):
    return item.isoformat(sep=' ', timespec='milliseconds')
  return str(item)

# Save as JSON lines
def save_as_jsonl(item_list: list, file):
  with open(file, 'w') as f:
    for item in item_list:
      line = json.dumps(item, ensure_ascii=False, separators=(',', ':'), default=dump)
      f.write(line + '\n')

def line_to_dict(line):
  item = json.loads(line)  
  for key, value in item.items():
    if key.endswith('_dttm') or key == 'timestamp':
      item[key] = datetime.fromisoformat(value)
  return item

# Load JSON lines
def load_jsonl(file) -> list:
  with open(file, 'r') as f:
    return [ line_to_dict(line) for line in f ]
