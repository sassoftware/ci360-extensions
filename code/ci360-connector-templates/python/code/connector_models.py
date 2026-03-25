#Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from sqlalchemy import Column, String, BigInteger, ForeignKey
from sqlalchemy.orm import relationship
from database.database import EventBase, MessageQueueBase


class ExternalEvents(EventBase):
    __tablename__ = "externalEvents"
    __table_args__ = {"schema": "devdb"}
    ci360guid = Column(String(50), primary_key=True, index=True)
    eventDesignedId = Column(String(50))
    eventDesignedName = Column(String(100))
    eventName = Column(String(100))
    eventType = Column(String(100))
    sessionId = Column(String(20))
    channelId = Column(String(20))
    channelType = Column(String(20))
    EventProperties = relationship("ExternalEventsProperties", back_populates="ParentEvent")

class ExternalEventsProperties(EventBase):
    __tablename__ = "externalEventsPoperties"
    __table_args__ = {"schema": "devdb"}
    id = Column(BigInteger, primary_key=True, autoincrement=True, index=True)
    ci360guid = Column(String(50), ForeignKey("devdb.externalEvents.ci360guid"))
    eventProperty = Column(String(100))
    eventPropertyValue = Column(String(100))
    ParentEvent = relationship("ExternalEvents", back_populates="EventProperties")


class ExternalAPIMessageQueueEntry(MessageQueueBase):
    """
    Message queue entry for sending data to an external API.
    Stores API configuration and payload for processing.
    """
    __tablename__ = "external_api_message_queue"
    id = Column(BigInteger, primary_key=True, autoincrement=True, index=True, default=lambda: int(datetime.utcnow().timestamp() * 1000))
    #id = Column(BigInteger, primary_key=True, autoincrement=True, index=True)
    api_name = Column(String(100), nullable=False)
    api_base_url = Column(String(255), nullable=False)
    api_route = Column(String(255), nullable=False)
    bearer_token = Column(String(255), nullable=False)
    payload = Column(String, nullable=False)
    send_interval = Column(BigInteger, nullable=True)
    timeout = Column(BigInteger, nullable=True)
    created_at = Column(BigInteger, default=lambda: int(datetime.utcnow().timestamp() * 1000), nullable=False)
    status = Column(String(50), nullable=False, default="pending")

    

        