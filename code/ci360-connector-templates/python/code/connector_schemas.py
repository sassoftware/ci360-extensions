#Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0

import logging
from fastapi import FastAPI, APIRouter, HTTPException, status, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Dict, Tuple



class EventPropertiesBase(BaseModel):
    id: int
    ci360guid: str
    eventProperty : str
    eventPropertyValue : str

class EventBase(BaseModel):
    guid: str
    eventDesignedId: str
    eventDesignedName: str
    eventName: str
    eventType: str
    sessionId: str
    channelId: str
    channelType: str
    class Config:
        extra = "allow"

class ShowProperties(EventPropertiesBase):
    eventProperty: str
    eventPropertyValue: str
    class Config():
        orm_mode = True

class ShowEvent(EventBase):
    guid: str
    eventDesignedId: str
    eventDesignedName: str
    eventName: str  
    eventType: str
    sessionId: str
    channelId: str
    channelType: str
    properties: Dict[str, str] = {}
    class Config():
        orm_mode = True


