#Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0

from fastapi import FastAPI
from routers.connector_routers import eventRouter, echoAPIRouter, echoAPIQueueRouter
from utils.logging import SASCI360VeloxPyLogging
from repository.connector_repository import ExternalAPI
from connector_models import MessageQueueBase
from database.database import queueDBEngine
from contextlib import asynccontextmanager
from common.appConfig import ApplicationConfiguration
from common.appCommon import AppCommon,ThreadedAppCommon
from common.appCommonDefaultLoadBalancer import AppCommonDefaultLoadBalancer #Just importing this class will register the default load balancer methods
import threading, time
appLogging = SASCI360VeloxPyLogging()
appLogging.loggerName = "FastAPIApp"
appLogging.logFileName = "Connector_{LOGGERNAME}_PID{PID}_TH{THREADID}_DTTM{TIMESTAMP}_RUN{RUNID}_web.log"
appLogging.logLevel = "TRACE"
appLogging.logFilePath = "C:\\temp"
appLogging.startLogger()
external_api_max_workers = ApplicationConfiguration.get('external_api_max_workers', 5)
external_api_workers_timeout = ApplicationConfiguration.get('external_api_workers_timeout', 30)
external_api_message_queue = ApplicationConfiguration.get('external_api_message_queue', False)
external_api_workers_max_load = ApplicationConfiguration.get('external_api_workers_max_load', 5)
external_api_workers_load_distribution_method = ApplicationConfiguration.get('external_api_workers_load_distribution_method', 'RR')
logger = SASCI360VeloxPyLogging.getDefaultLogger()
logger.info("FastAPI application is starting.")

# externalAPIMessageProcessingThread = threading.Thread(target=ExternalAPI.processExternalAPICallsFromQueue)
# externalAPIMessageProcessingThread.start()

@asynccontextmanager
async def lifespan(app: FastAPI):   
    # Startup
    logger.info("FastAPI application startup: Setting up external API message queue.")
    MessageQueueBase.metadata.create_all(bind=queueDBEngine)
    logger.info("FastAPI application startup: Initializing external API message queue processing threads.")
    for _i in range(external_api_max_workers):
        await ThreadedAppCommon.createAppThread(apiName="echo", max_work_load=external_api_workers_max_load, worker_load_distribution_method=external_api_workers_load_distribution_method, vAppName="EventQueueProcessor",vTarget=ExternalAPI.processExternalAPICallsFromQueue)
    yield
    # Shutdown
    logger.info("FastAPI application shutdown: Stopping external API message queue processing threads.")
    await ThreadedAppCommon.signalStopToAppThreads(vAppName="EventQueueProcessor")
    await ThreadedAppCommon.closeAllAppThreads(vAppName="EventQueueProcessor")


connectorApp = FastAPI(lifespan=lifespan)

logger.info("Loading External API configurations for 'echo' API.")
ExternalAPI.loadExternalAPIConfigurations("echo")
logger.info("External API configurations loaded successfully.")

logger.info("Adding REST end points into FastAPI application.")
# connectorApp.include_router(eventRouter)
connectorApp.include_router(echoAPIRouter)
connectorApp.include_router(echoAPIQueueRouter)
logger.info("REST end points added successfully.")

#ThreadedAppCommon.startAppThreads(vAppName="EventQueueProcessor")
# ExternalAPI.processExternalAPICallsFromQueue(apiName="echo")


# externalAPIMessageProcessingThread.join(timeout=1)