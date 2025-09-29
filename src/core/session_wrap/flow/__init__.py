import logging
import uuid
from abc import ABC, abstractmethod

import settings
from core.session_wrap.messages import IncomingMessage
from core.session_wrap.transport import TwoWayTransportInterface
from core.worker.worker_manager import CPUCommands
from utils import get_console_logger


class TwoWayFlowInterface(ABC):
    # hexadecimal number 5 in 1 byte in big endian format #5.to_bytes(1, 'big')
    # binary data ended if 5 comes
    binary_stop_send_signal = b'\x05'
    flow_name = "Base"

    def __init__(self, tw_transport: TwoWayTransportInterface, cpu_commands: CPUCommands):
        self._tw_transport = tw_transport
        self._cpu_commands = cpu_commands
        self._session_id = str(uuid.uuid4())[:5]

        formatter = logging.Formatter(
            f'{settings.BASE_LOGGING_FORMAT}|{self._session_id}|TwoWayFlow|{self.flow_name}|%(message)s'
        )
        self.logger = get_console_logger(
            f'{__name__}-{self._session_id}',
            formatter
        )
        self._tw_transport.logger.info(f"Init {self.flow_name} session with id: {self._session_id}")

    @abstractmethod
    async def start(self, init_message: IncomingMessage):
        pass
