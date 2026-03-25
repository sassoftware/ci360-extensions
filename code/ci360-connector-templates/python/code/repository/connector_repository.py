#Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0

from datetime import datetime
import random
from fastapi import HTTPException, logger, status, Depends
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError, DataError, ProgrammingError
from connector_models import ExternalEvents, ExternalEventsProperties, ExternalAPIMessageQueueEntry
from connector_schemas import ShowEvent, EventBase
from database.database import get_db, get_message_queue_db, external_api_message_queue, SessionLocalQueue, SessionLocal
from utils.logging import SASCI360VeloxPyLogging
import httpx
from common.appConfig import ApplicationConfiguration
from common.appCommon import ThreadedAppCommon
from typing import Dict
import json
import time
import asyncio


class ExternalAPIConfig:
    def __init__(self, name: str):
        self.external_api_base_url: str = ApplicationConfiguration.get(name+"_api_base_url","https://postman-echo.com")
        self.external_api_route: str = ApplicationConfiguration.get(name+"_api_route", "post")
        self.token: str = ApplicationConfiguration.get(name+"_bearer_token", "exampleToken")
        self.payload: str = ApplicationConfiguration.get(name+"_DefaultJSONPayload", {})
        self.send_interval = ApplicationConfiguration.get(name+"_throttling_interval", 900)
        self.timeout = ApplicationConfiguration.get(name+"_timeout", 10)
        self.retries = ApplicationConfiguration.get(name+"_retries", 3)
        self.headers: str = ApplicationConfiguration.get(name+"_headers", {"User-Agent": "ExampleClient/1.0"})

class ExternalAPI():
    externalAPILibrary: Dict[str, ExternalAPIConfig] = {} 
    logger: SASCI360VeloxPyLogging = SASCI360VeloxPyLogging.getDefaultLogger()
    @classmethod
    def loadExternalAPIConfigurations(cls, name: str):
        cls.externalAPILibrary[name] = ExternalAPIConfig(name)
        
    @ThreadedAppCommon.threadedWorker(threadedAppName="EventQueueProcessor",iterationWait=5, worker_load_distribution_method="RND")
    @classmethod
    async def processExternalAPICallsFromQueue(cls,*args,apiName: str, max_work_load: int = 5,thread_id:str, vAppName:str, **kwargs) -> None:
            logger:SASCI360VeloxPyLogging=SASCI360VeloxPyLogging.getDefaultLogger()
            logger.info("<<<<QUEUE>>>> Starting to process external API calls from queue.")
        # while True:
            dbsession:Session = SessionLocalQueue()
            if not dbsession:
                pass
            else:
                await ExternalAPI.processNextBatchOfExternalAPIQueueMessages(cls,vThreadId=thread_id, vAppName=vAppName,dbsession=dbsession, apiName=apiName, max_work_load=max_work_load)
                # # Process the API calls from the queue
                # queue = dbsession.query(ExternalAPIMessageQueueEntry).filter(ExternalAPIMessageQueueEntry.status != "processed", ExternalAPIMessageQueueEntry.api_name == apiName).all()
                # queueCount=len(queue)
                # # if we have more rows to process than max_work_load, limit to max_work_load
                # maxRowsToProcess = max_work_load if queueCount > max_work_load else queueCount
                # logger.info(f"<<<<QUEUE>>>> Found {queueCount} pending API calls in queue for API '{apiName}'. Processing up to {maxRowsToProcess} calls in this iteration.")
                # #limit the queue to maxRowsToProcess
                # limitedQueue = queue[:int(maxRowsToProcess)]
                # if maxRowsToProcess > 0:
                #     for api_call_queue_entry in limitedQueue:
                #         vApiName = api_call_queue_entry.api_name
                #         # vApiCall: ShowEvent =  dbsession.query(ShowEvent).filter(ShowEvent.id == api_call_queue_entry.payload.id).first()
                #         strPayload = api_call_queue_entry.payload.replace("'", '"')
                #         vApiCall: ShowEvent = ShowEvent.model_validate_json(strPayload)
                #         # try:
                #         response = await callExternalAPI(uniqueAPIName=vApiName, request=vApiCall, queueMessage=False)
                #         if response.status_code >= 400:
                #             logger.error(f"<<<<QUEUE>>>> Failed to process API call ID {api_call_queue_entry.id}: {response.status_code} - {response.content}")
                #             continue
                #         api_call_queue_entry.status = "processed"
                #         dbsession.add(api_call_queue_entry)
                #         dbsession.commit()
                #         logger.info(f"<<<<QUEUE>>>> Successfully processed API call with Queue ID {api_call_queue_entry.id}")
                #         # except Exception as e:
                #         #     logger.error(f"Error processing API call: {e}")
                #         # finally:
                #         #     continue
                #     logger.info("<<<<QUEUE>>>> All Pending API calls assigned to this worker in queue have been processed.")
                # else:
                #     logger.info("<<<<QUEUE>>>> All Pending API calls in queue have been processed.")
                # dbsession.close()


    @ThreadedAppCommon.BlockingOrAtomicOperation(retryCount=3,dbsession=None,apiName="", max_work_load=5)
    @classmethod
    async def processNextBatchOfExternalAPIQueueMessages(cls,*args,dbsession:Session, apiName:str, max_work_load:int=5,**kwargs) -> None:
        logger:SASCI360VeloxPyLogging=SASCI360VeloxPyLogging.getDefaultLogger()
        logger.info("<<<<QUEUE>>>> Starting to process next batch of external API calls from queue.")
        # Process the API calls from the queue
        queue = dbsession.query(ExternalAPIMessageQueueEntry).filter(ExternalAPIMessageQueueEntry.status != "processed", ExternalAPIMessageQueueEntry.api_name == apiName).all()
        queueCount=len(queue)
        # if we have more rows to process than max_work_load, limit to max_work_load
        maxRowsToProcess = max_work_load if queueCount > max_work_load else queueCount
        logger.info(f"Found {queueCount} pending API calls in queue for API '{apiName}'. Processing up to {maxRowsToProcess} calls in this iteration.")
        #limit the queue to maxRowsToProcess
        limitedQueue = queue[:int(maxRowsToProcess)]
        if maxRowsToProcess > 0:
            for api_call_queue_entry in limitedQueue:
                vApiName = api_call_queue_entry.api_name
                # vApiCall: ShowEvent =  dbsession.query(ShowEvent).filter(ShowEvent.id == api_call_queue_entry.payload.id).first()
                strPayload = api_call_queue_entry.payload.replace("'", '"')
                vApiCall: ShowEvent = ShowEvent.model_validate_json(strPayload)
                # try:
                response = await callExternalAPI(uniqueAPIName=vApiName, request=vApiCall, queueMessage=False)
                if response.status_code >= 400:
                    logger.error(f"Failed to process API call ID {api_call_queue_entry.id}: {response.status_code} - {response.content}")
                    continue
                api_call_queue_entry.status = "processed"
                dbsession.add(api_call_queue_entry)
                dbsession.commit()
                logger.info(f"Successfully processed API call with Queue ID {api_call_queue_entry.id}")
            logger.info("All Pending API calls assigned to this worker in queue have been processed.")
            dbsession.close()


async def callExternalAPI(uniqueAPIName:str, request: ShowEvent, queueMessage:bool=False) -> httpx.Response:
    logger:SASCI360VeloxPyLogging=SASCI360VeloxPyLogging.getDefaultLogger()
    logger.info(f"Sending POST request to external API")
    if external_api_message_queue and queueMessage:
        await queueExternalAPIRequestToDB(uniqueAPIName, request)
    else:
        # resp = await ExternalAPI.callExternalAPI(request)
        externalAPI: ExternalAPIConfig = ExternalAPI.externalAPILibrary.get(uniqueAPIName)
        try:
            externalAPI.payload = request.model_dump()
        except Exception as e:
            raise httpx.HTTPStatusError(
                f"ERROR: {e}",
                request=request
            )
        externalAPI.headers = {
            "Authorization": f"Bearer {externalAPI.token}",
            "Content-Type": "application/json"
        }
        async with httpx.AsyncClient(base_url=externalAPI.external_api_base_url, verify=False) as client:
            response = await client.post(url=f"{externalAPI.external_api_route}", json=externalAPI.payload, headers=externalAPI.headers)
            #response = await client.post(url=f"{externalAPI.external_api_route}", json={"somefield": "somevalue", "fewproperties": {"firstprop": "firstvalue"}}, headers=externalAPI.headers)
            if response.status_code >= 400:
                logger.error(f"Status Code: {response.status_code}")
                logger.error(f"Response JSON: {response.json()}")
                return httpx.Response(status_code=response.status_code, content=response.json())
                # raise HTTPException(
                #     status_code=response.status_code,
                #     detail=f"ERROR: {response.status_code} - {response.json()}",
                #     request=request
                # )
            else:
                logger.info(f"Status Code: {response.status_code}")
                logger.info(f"Response JSON: {response.json()}")
                return httpx.Response(status_code=response.status_code, content=json.dumps(response.json()))

async def queueExternalAPIRequestToDB(name: str, event: ShowEvent, db: Session = Depends(get_message_queue_db)):
    logger:SASCI360VeloxPyLogging=SASCI360VeloxPyLogging.getDefaultLogger()
    logger.info(f"Queuing event for external API: {event.guid}")
    try:
        externalAPI: ExternalAPIConfig = ExternalAPI.externalAPILibrary.get(name)
        if not externalAPI:
            raise ValueError(f"External API configuration for '{name}' not found.")
        # Here you would implement the logic to add the request to a queue
        # For example, using a background task or a message broker
        vExternalAPIMessageQueueEntry = ExternalAPIMessageQueueEntry(
            api_name=name,
            api_base_url=externalAPI.external_api_base_url,
            api_route=externalAPI.external_api_route,
            bearer_token=externalAPI.token,
            payload=str(event.model_dump()),
            send_interval=externalAPI.send_interval,
            timeout=externalAPI.timeout
        )
        db.add(vExternalAPIMessageQueueEntry)
        db.commit()
        return httpx.Response(status_code=status.HTTP_202_ACCEPTED, content=str(event.model_dump()))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


async def create_event(event: ShowEvent,  db: Session = Depends(get_db)):
    logger:SASCI360VeloxPyLogging=SASCI360VeloxPyLogging.getDefaultLogger()
    # db = get_db()
    try:
        logger.info(f"Received event: {event.guid}")
        logger.debug(f"Event payload: {str(event.model_dump())}")
        now= datetime.now()
        rnd=random.randint(10000, 99999)
        main_event = ExternalEvents(
            ci360guid=f"{event.guid}{now}{rnd}",
            eventDesignedId=event.eventDesignedId,
            eventDesignedName=event.eventDesignedName,
            eventName=event.eventName,
            eventType=event.eventType,
            sessionId=event.sessionId,
            channelId=event.channelId,
            channelType=event.channelType
        )
        logger.debug(f"Prepared ExternalEvents instance for guid {event.guid}")
        logger.debug(f"Adding main_event to session for guid {event.guid}")
        db.add(main_event)
        for vProp,vProp_value in event.properties.items():
           event_prop = ExternalEventsProperties(
            id=int(now.timestamp()+random.randint(10000, 99999)),  # Auto-incremented
            ci360guid=f"{event.guid}{now}{rnd}",
            eventProperty=vProp,
            eventPropertyValue=vProp_value
            )
           logger.debug(f"Adding property for guid {event.guid}: {vProp}={vProp_value} with ID {event_prop.id}")
           db.add(event_prop)
        # main_event.EventProperties.append(event_prop)
        logger.debug(f"Committing transaction for guid {event.guid}")
        db.commit()
        await callExternalAPI(uniqueAPIName="echo", request=event, queueMessage=external_api_message_queue)

        logger.info(f"Inserted event {event.guid} and its properties successfully.")
        return JSONResponse(status_code=status.HTTP_201_CREATED, content={"status": "success"})
    except IntegrityError as e:
        db.rollback()
        logger.error(f"Integrity error for event {event.guid}: {str(e)}")
        logger.debug(f"IntegrityError details: {e}")
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Integrity error: {str(e)}"
        )
    except OperationalError as e:
        db.rollback()
        logger.error(f"Operational error for event {event.guid}: {str(e)}")
        logger.debug(f"OperationalError details: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Operational error: {str(e)}"
        )
    except DataError as e:
        db.rollback()
        logger.error(f"Data error for event {event.guid}: {str(e)}")
        logger.debug(f"DataError details: {e}")
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Data error: {str(e)}"
        )
    except ProgrammingError as e:
        db.rollback()
        logger.error(f"Programming error for event {event.guid}: {str(e)}")
        logger.debug(f"ProgrammingError details: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Programming error: {str(e)}"
        )
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Database error for event {event.guid}: {str(e)}")
        logger.debug(f"SQLAlchemyError details: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}"
        )
    except ValidationError as e:
        logger.error(f"Validation error for event: {str(e)}")
        logger.debug(f"ValidationError details: {e}")
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Validation error: {str(e)}"
        )
    except Exception as e:
        db.rollback()
        logger.error(f"Unexpected error for event {getattr(event, 'guid', 'unknown')}: {str(e)}")
        logger.debug(f"Exception details: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unexpected error: {str(e)}"
        )
    finally:
        logger.debug(f"Closing DB session for guid {getattr(event, 'guid', 'unknown')}")
        db.close()