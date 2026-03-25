#Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0

from fastapi import APIRouter, status, Depends
from sqlalchemy.orm import Session
from connector_schemas import ShowEvent
from database.database import get_db, get_message_queue_db
from repository.connector_repository import create_event, queueExternalAPIRequestToDB, callExternalAPI
from utils.logging import SASCI360VeloxPyLogging
from fastapi import HTTPException
from utils.logging import SASCI360VeloxPyLogging
from repository.connector_repository import ExternalAPI

eventRouter = APIRouter(
    prefix="/event",
    tags=['Events']
)

echoAPIRouter = APIRouter(
    prefix="/EchoAPICall",
    tags=['ExternalAPICall']
)

echoAPIQueueRouter = APIRouter(
    prefix="/EchoAPIQueue",
    tags=['ExternalAPICall']
)

@eventRouter.get("/", status_code=status.HTTP_405_METHOD_NOT_ALLOWED)
async def get_event():
    raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED, detail="{Method Not Allowed}")

@eventRouter.post("/", status_code=status.HTTP_201_CREATED, response_model=ShowEvent)
async def create(request: ShowEvent, db: Session = Depends(get_db)):
    logger:SASCI360VeloxPyLogging=SASCI360VeloxPyLogging.getDefaultLogger()
    logger.info(f"Received event creation request: {request.json() if hasattr(request, 'json') else request}")
    try:
        await create_event(request, db)
        logger.info("Event created successfully.")
        # return JSONResponse(status_code=status.HTTP_201_CREATED, content={request.json() if hasattr(request, 'json') else request})
        return request
    except Exception as e:
        logger.error(f"Error creating event: {e}")
        #return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={"status": "error", "message": str(e)})
        return request

@echoAPIRouter.get("/", status_code=status.HTTP_405_METHOD_NOT_ALLOWED)
async def get_echo_api():
    raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED, detail="{Method Not Allowed}")

@echoAPIRouter.post("/", status_code=status.HTTP_201_CREATED)
async def call_echo_api(request: ShowEvent):
    logger:SASCI360VeloxPyLogging=SASCI360VeloxPyLogging.getDefaultLogger()
    logger.info(f"Received external API call request: {request.json() if hasattr(request, 'json') else request}")
    try:
        response = await callExternalAPI("echo",request, queueMessage=False)
        logger.info("Event sent successfully to External API.")
        return response.json()
    except Exception as e:
        logger.error(f"Error sending event: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@echoAPIQueueRouter.get("/", status_code=status.HTTP_405_METHOD_NOT_ALLOWED)
async def get_event():
    raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED, detail="{Method Not Allowed}")

@echoAPIQueueRouter.post("/", status_code=status.HTTP_201_CREATED)
async def queue_echo_api(request: ShowEvent, db: Session = Depends(get_message_queue_db)):
    logger:SASCI360VeloxPyLogging=SASCI360VeloxPyLogging.getDefaultLogger()
    logger.info(f"Received external API queue request: {request.json() if hasattr(request, 'json') else request}")
    try:
        # await ExternalAPI.queueExternalAPIRequest(ExternalAPI,name="echo", request=request)
        await queueExternalAPIRequestToDB(name="echo", event=request, db=db)
        logger.info("Event queued successfully for External API.")
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Error queuing event: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))   