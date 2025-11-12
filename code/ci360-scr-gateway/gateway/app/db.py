# db.py
import os
from collections.abc import AsyncGenerator
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

USE_DB = os.getenv('USE_DB', 'false').strip().lower() == 'true'

# Require explicit DATABASE_URL if DB is enabled
if USE_DB:
  DATABASE_URL = os.getenv('DATABASE_URL')
  if not DATABASE_URL:
    raise RuntimeError('❌ DATABASE_URL environment variable is required when USE_DB=true')
  SQL_ECHO = os.getenv('SQL_ECHO', 'false').strip().lower() == 'true'
  
  engine = create_async_engine(DATABASE_URL, echo=SQL_ECHO)
  AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
  )
else:
  engine = None
  AsyncSessionLocal = None

async def get_db() -> AsyncGenerator[AsyncSession | None, None]:
  if not USE_DB:
    yield None
    return
  async with AsyncSessionLocal() as session:
    yield session
