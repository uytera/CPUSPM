from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from logging import Logger
from multiprocessing import Pipe
from multiprocessing.shared_memory import SharedMemory
from typing import Dict, Any, Optional

from core.messages import WorkerMessage
from core.types import CommandType


@dataclass(kw_only=True)
class ProcessorContext(ABC):
    command_type: CommandType
    creation_date: datetime = field(default_factory=datetime.now)


class Processor(ABC):
    def __init__(
            self,
            sessions: Dict[str, Any],
            outer_logger: Logger,
            manager_pipe: Pipe,
            manager_shared_memory: SharedMemory
    ):
        self._sessions = sessions
        self._logger = outer_logger
        self._manager_pipe = manager_pipe
        self._manager_shared_memory = manager_shared_memory

    def init_session(self, message: WorkerMessage):
        raise NotImplementedError()

    @abstractmethod
    def process_message(self, message: WorkerMessage):
        raise NotImplementedError()

    @abstractmethod
    def clear_session(self, session_id, rack: Optional[str] = None):
        raise NotImplementedError()
