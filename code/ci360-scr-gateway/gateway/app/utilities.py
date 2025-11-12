import base64
from sqlalchemy.ext.asyncio import AsyncSession
import json
import time
from typing import Tuple
from models import CallRecord
from db import USE_DB


def parse_payload(payload_raw: bytes) -> Tuple[str, dict | None]:
  # Decode as UTF-8 string
  try:
    payload_text = payload_raw.decode('utf-8')
  except:
    b64 = base64.b64encode(payload_raw).decode('ascii')
    return f'__base64__:{b64}', None

  # Parse as json
  try:
    payload_dict = json.loads(payload_text)
  except:
    payload_dict = None
  
  return payload_text, payload_dict

async def request_complete(call_record: CallRecord, start_time: float, db: AsyncSession):
  call_record.duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
  if USE_DB:
    db.add(call_record)
    await db.commit()
