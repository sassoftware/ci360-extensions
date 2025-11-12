import time
import httpx
import logging
from fastapi import APIRouter, Depends, Request, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from db import get_db
from models import CallRecord
from deps import get_http_client, get_scr_gateway
from utilities import parse_payload, request_complete
from scr_gateway import SCRGateway


logger = logging.getLogger('gateway')
router = APIRouter()

@router.get('/health')
def health_check():
  return { 'status': 'ok' }

@router.post('/scr')
async def scr(request: Request, client: httpx.AsyncClient = Depends(get_http_client), scr_gateway: SCRGateway = Depends(get_scr_gateway), db: AsyncSession = Depends(get_db)):
  start_time = time.perf_counter()
  call_record = CallRecord()
  request_headers = dict(request.headers)

  payload_raw = await request.body()
  payload_text, payload_json = parse_payload(payload_raw)

  logger.debug(f'Headers: {request_headers}')
  logger.debug(f'Payload: {payload_text}')

  call_record.connector_payload = payload_text
  call_record.connector_headers = request_headers

  if not payload_json:
    logger.warning('Expected JSON request')
    await request_complete(call_record, start_time, db)
    raise HTTPException(status_code=400, detail='Expected JSON request')
  call_record.guid = payload_json.get('guid')
  
  # Get SCR endpoint name
  try:
    endpoint_id = payload_json['outboundProperties']['properties']['scr_endpoint']
  except:
    call_record.duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
    logger.warning('Missing required field: outboundProperties.properties.scr_endpoint')
    await request_complete(call_record, start_time, db)
    raise HTTPException(status_code=400, detail='Missing required field: outboundProperties.properties.scr_endpoint')
  
  call_record.endpoint_id = endpoint_id
  logger.debug(f'outboundProperties.properties.scr_endpoint: {endpoint_id}')

  # Get SCR endpoint from gateway
  scr_endpoint = scr_gateway.get(endpoint_id)
  if not scr_endpoint:
    logger.warning(f'Unknown SCR endpoint: {endpoint_id}')
    await request_complete(call_record, start_time, db)
    raise HTTPException(status_code=400, detail=f'Unknown SCR endpoint: {endpoint_id}')

  call_record.endpoint_url = scr_endpoint.url
  # Call SCR endpoint
  try:
    return await scr_endpoint.call(client, payload_json, call_record)
  except Exception as e:
    logger.exception('SCR call failed')
    raise HTTPException(status_code=502, detail='SCR service error')
  finally:
    await request_complete(call_record, start_time, db)

@router.get('/endpoints')
async def scr_endpoints(scr_gateway: SCRGateway = Depends(get_scr_gateway)):
  return {
    'dataProvider': [ {'key': id, 'text': str(endpoint) } for id, endpoint in scr_gateway.items() ]
  }
