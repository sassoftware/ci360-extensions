#Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from common.appConfig import ApplicationConfiguration

# # Initialize the application configuration
# ApplicationConfiguration.initializeConfiguration()
# from sqlalchemy.orm import scoped_session
# # Create a scoped session factory for database interactions
# SessionFactory = sessionmaker()
# SessionLocal = scoped_session(SessionFactory)
# from sqlalchemy import text
# # Create a database engine using SQLAlchemy with the configuration parameters
# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy.orm import declarative_base
# from common.appConfig import ApplicationConfiguration
# from sqlalchemy.orm import scoped_session


# Initialize the application configuration
# Define the database connection parameters using the application configuration
driver = ApplicationConfiguration.get('driver', 'postgresql+psycopg')
host = ApplicationConfiguration.get( 'host', 'localhost')    
port = ApplicationConfiguration.get( 'port', 5432)
user = ApplicationConfiguration.get('user', 'somedbuser')
password = ApplicationConfiguration.get( 'password', 'somedbpassword')
database = ApplicationConfiguration.get( 'database', 'postgres')
charset = ApplicationConfiguration.get( 'charset', 'utf8')
timeout = ApplicationConfiguration.get( 'timeout', 30)
sslmode = ApplicationConfiguration.get('sslmode', 'prefer')
pool_pre_ping = ApplicationConfiguration.get('pool_pre_ping', True)
isolation_level = ApplicationConfiguration.get('isolation_level', 'READ COMMITTED')
pool_reset_on_return = ApplicationConfiguration.get('pool_reset_on_return', 'rollback')
pool_size = ApplicationConfiguration.get('pool_size', 5)
max_overflow = ApplicationConfiguration.get('max_overflow', 10)
pool_recycle = ApplicationConfiguration.get('pool_recycle', 3600)
pool_timeout = ApplicationConfiguration.get('pool_timeout', 30)
pool_use_lifo = ApplicationConfiguration.get('pool_use_lifo', True)
echo = ApplicationConfiguration.get('echo', False)
echo_pool = ApplicationConfiguration.get('echo_pool', False)
application_name = ApplicationConfiguration.get('application_name', 'MyApp')
client_encoding = ApplicationConfiguration.get('client_encoding', 'utf8')
external_api_message_queue = ApplicationConfiguration.get('external_api_message_queue', False)
external_api_message_queue_db_url = ApplicationConfiguration.get('external_api_message_queue_db_url', "sqlite:///./dbstore/apimessages.db")


SQLALCHAMY_DATABASE_URL = f"{driver}://{user}:{password}@{host}:{port}/{database}?client_encoding={charset}&application_name={application_name}&sslmode={sslmode}"


engine = create_engine(SQLALCHAMY_DATABASE_URL, connect_args={"connect_timeout": timeout},
                       pool_pre_ping=pool_pre_ping,
                       isolation_level=isolation_level,
                       pool_reset_on_return=pool_reset_on_return, 
                       pool_size=pool_size,
                       max_overflow=max_overflow, 
                       pool_recycle=pool_recycle, 
                       pool_timeout=pool_timeout,
                       pool_use_lifo=pool_use_lifo, 
                       echo=echo, 
                       echo_pool=echo_pool
                       )

SessionLocal = sessionmaker(bind=engine, autoflush=False)

EventBase = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# DATABASE_URL = "postgresql+psycopg://username:password@localhost:5432/yourdb"

# engine = create_engine(DATABASE_URL, pool_pre_ping=True)
# SessionFactory = sessionmaker(bind=engine)
# SessionLocal = scoped_session(SessionFactory)
# Base = declarative_base()

SQLALCHAMY_MSGQUEUE_DATABASE_URL = f"{external_api_message_queue_db_url}"


queueDBEngine = create_engine(SQLALCHAMY_MSGQUEUE_DATABASE_URL, connect_args={"check_same_thread": False})

SessionLocalQueue = sessionmaker(bind=queueDBEngine, autocommit=False, autoflush=False)


MessageQueueBase = declarative_base()

def get_message_queue_db():
    queuedb = SessionLocalQueue()
    try:
        yield queuedb
    finally:
        queuedb.close()