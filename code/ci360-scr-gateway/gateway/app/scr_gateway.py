import json
import logging
import httpx
from pathlib import Path
from typing import Iterator
from collections.abc import Mapping
from datetime import datetime
from models import CallRecord

logger = logging.getLogger('gateway')

class SCREndpoint:
  def __init__(self, json_path: Path):
    data = json.loads(json_path.read_text(encoding='utf-8'))
    self.id = data['id']
    self.url = data['endpoint']
    payload = data['payload']
    self.metadata = [ self.mapping(key, value) for key, value in payload.get('metadata', {}).items() ]
    self.inputs = [ self.mapping(key, value) for key, value in payload.get('inputs', {}).items() ]
    logger.debug(self)

  def __repr__(self):
    metadata_vars = [var['name'] for var in self.metadata]
    input_vars = [var['name'] for var in self.inputs]
    return f'SCREndpoint({self.id}: {self.url}, metadata:{metadata_vars}, inputs:{input_vars})'

  def mapping(self, key: str, value: str) -> dict:
    # raw event or record reference
    if isinstance(value, str) and value.startswith('${') and value.endswith('}'):
      path = value[2:-1].split('.')
      if len(path) > 1 and path[0] in { 'event', 'record' }:
        return { 'name': key, 'source': path[0], 'keys': path[1:] }
      
    # cast data as string
    if isinstance(value, str) and value.startswith('$str{') and value.endswith('}'):
      path = value[5:-1].split('.')
      if len(path) > 1 and path[0] in { 'event', 'record' }:
        return { 'name': key, 'source': path[0], 'keys': path[1:], 'cast': str }

    # cast data as iso timestamp
    if isinstance(value, str) and value.startswith('$timestamp{') and value.endswith('}'):
      path = value[11:-1].split('.')
      if len(path) > 1 and path[0] in { 'event', 'record' }:
        return { 'name': key, 'source': path[0], 'keys': path[1:], 'cast': self.iso_timestamp }

    # simple value
    return { 'name': key, 'source': 'literal', 'value': value }

  def dig(self, event: dict, json_keys: list):
    item = event
    for key in json_keys:
      if not isinstance(item, dict):
        return None
      item = item.get(key, None)
    if isinstance(item, (str, int, float)):
      return item
    return None
  
  def iso_timestamp(self, epoch_millis):
    epoch_seconds = float(epoch_millis)*0.001
    return datetime.fromtimestamp(epoch_seconds).isoformat(timespec='milliseconds')

  def cast(self, value, caster):
    if not caster:
      return value
    try:
      return caster(value)
    except:
      return None   

  def get_value(self, var: dict, event: dict, call_record: CallRecord):
    source = var['source']
    if source == 'literal':
      return var['value']

    caster = var.get('cast', None)
  
    if source == 'event':
      raw = self.dig(event, var['keys'])
      return self.cast(raw, caster)

    if source == 'record':
      raw = getattr(call_record, var['keys'][0], None)
      value = self.cast(raw, caster)
      if isinstance(value, (str, int, float)):
        return value

    return None

  async def call(self, client: httpx.AsyncClient, event: dict, call_record: CallRecord) -> dict:
    # create SCR payload
    scr_payload = {
      'data': { var['name']: self.get_value(var, event, call_record) for var in self.inputs }
    }
    if self.metadata:
      scr_payload['metadata'] = { var['name']: self.get_value(var, event, call_record) for var in self.metadata }

    call_record.scr_request_payload = scr_payload
    logger.debug(f'POST {scr_payload} to {self.url}')
    response = await client.post(self.url, json=scr_payload)
    logger.debug(f'HTTP {response.status_code}')
    call_record.scr_status_code = response.status_code
    call_record.scr_response_payload = response.text
    response.raise_for_status()
    scr_response = response.json()
    logger.debug(f'Response: {scr_response}')
    return scr_response

class SCRGateway(Mapping[str, SCREndpoint]):
  def __init__(self, dir_str: str):
    dir_path = Path(dir_str)
    self._endpoints = {
      ep.id: ep for ep in map(SCREndpoint, dir_path.glob('*.json'))
    }

  def __getitem__(self, id: str) -> SCREndpoint:
    return self._endpoints[id]

  def __iter__(self) -> Iterator[str]:
    return iter(self._endpoints)

  def __len__(self) -> int:
    return len(self._endpoints)
