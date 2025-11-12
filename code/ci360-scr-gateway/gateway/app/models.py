from uuid import uuid4
from sqlalchemy import Column, Integer, String, Text, DateTime, JSON, Float
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime, timezone

Base = declarative_base()

class CallRecord(Base):
  __tablename__ = 'call_records'

  call_id = Column(String(36), primary_key=True)
  timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None))
  endpoint_id = Column(String, nullable=True)
  endpoint_url = Column(String, nullable=True)
  connector_payload = Column(Text, nullable=True)
  connector_headers = Column(JSON, nullable=True)
  guid = Column(String(36), nullable=True)
  scr_request_payload = Column(JSON, nullable=True)
  scr_response_payload = Column(Text, nullable=True)
  scr_status_code = Column(Integer, nullable=True)
  duration_ms = Column(Float, nullable=False)
  
  def __init__(self, **kwargs):
    super().__init__(**kwargs)
    self.call_id = self.call_id or str(uuid4())
