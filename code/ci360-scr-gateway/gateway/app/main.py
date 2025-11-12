from contextlib import asynccontextmanager
import logging
from sys import stdout
from datetime import datetime, timezone
import httpx
from fastapi import FastAPI
from scr_gateway import SCRGateway
from api import router
from db import engine, USE_DB
from models import Base
import os

#
# Lifespan
#
@asynccontextmanager
async def lifespan(app: FastAPI):
  logger.info(f'USE_DB={USE_DB}')

  # Setup
  VERIFY_SSL = os.getenv('VERIFY_SSL', 'true').strip().lower() != 'false'
  CA_CERT_PATH = os.getenv('CA_CERT_PATH')

  if CA_CERT_PATH and os.path.exists(CA_CERT_PATH):
    logger.info(f'🔒 Using custom CA cert: {CA_CERT_PATH}')

  elif CA_CERT_PATH and not os.path.exists(CA_CERT_PATH):
    logger.warning(f'⚠️ CA_CERT_PATH set but file not found: {CA_CERT_PATH}')
    CA_CERT_PATH = None

  elif not VERIFY_SSL:
    logger.warning('⚠️ SSL validation disabled (VERIFY_SSL=false)')

  else:
    logger.info('✅ Using default CA store for HTTPS verification')

  app.state.http_client = httpx.AsyncClient(verify=CA_CERT_PATH or VERIFY_SSL)
  app.state.scr_gateway = SCRGateway('/app/endpoints')

  if USE_DB:
    logger.info('🗄️  Initializing database (USE_DB=true)...')
    async with engine.begin() as conn:
      await conn.run_sync(Base.metadata.create_all)
  else:
    logger.info('🚫 Database disabled (USE_DB=false)')

  yield  # ← This yields control to run the app

  # Teardown (replaces shutdown_event)
  await app.state.http_client.aclose()


#
# Router
#
app = FastAPI(lifespan=lifespan)
app.include_router(router)

#
# Logging
#
class Formatter(logging.Formatter):
 def formatTime(self, record, datefmt=None):
   dt = datetime.fromtimestamp(record.created, tz=timezone.utc).replace(tzinfo=None)
   return dt.isoformat(timespec='milliseconds', sep=' ')
 
formatter = Formatter(fmt='%(asctime)s %(levelname)s [%(process)d]: %(filename)s %(funcName)s %(message)s')

logger = logging.getLogger('gateway')
logger.setLevel(logging.DEBUG)

if not logger.hasHandlers():
  handler = logging.StreamHandler(stdout)
  handler.setFormatter(formatter)
  logger.addHandler(handler)
