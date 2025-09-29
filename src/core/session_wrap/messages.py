from enum import Enum
from typing import Any

from pydantic import BaseModel

from core.session_wrap.types import FlowType
from core.worker.types import ImageFormat


class ProcessorContentData(BaseModel):
    width: int
    height: int
    img_format: ImageFormat


class IncomingMessage(BaseModel):
    flow_type: FlowType
    context_data: ProcessorContentData


class OutcomingMessageType(Enum):
    message = 0
    exception = 1


class OutcomingMessage(BaseModel):
    type: OutcomingMessageType
    data: Any