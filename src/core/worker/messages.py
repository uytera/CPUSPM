import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from core.worker.types import CommandType


class MessageTypes(Enum):
    # from manager to worker
    data = 0
    clear = 1
    init_session = 2

    # from worker to manager
    ok = 3
    check_shared_memory = 4
    chunk_shared_memory = 5
    chunk_shared_memory_fin = 6


@dataclass
class WorkerMessage:
    type: MessageTypes
    # request answer check key
    # uses for validate that answer is correlated with request message
    rack: str = field(default_factory=lambda: str(uuid.uuid4()))
    command: Optional[CommandType] = None
    data: Optional[Any] = None
    session_id: Optional[str] = None
