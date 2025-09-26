from abc import ABC, abstractmethod

from core.session_wrap.messages import IncomingMessage
from core.session_wrap.transport import TwoWayTransportInterface
from core.worker.worker_manager import CPUCommands


class TwoWayFlowInterface(ABC):
    # hexadecimal number 5 in 1 byte in big endian format #5.to_bytes(1, 'big')
    # binary data ended if 5 comes
    binary_stop_send_signal = b'\x05'

    def __init__(self, tw_transport: TwoWayTransportInterface, cpu_commands: CPUCommands):
        self._tw_transport = tw_transport
        self._cpu_commands = cpu_commands

    @abstractmethod
    async def start(self, init_message: IncomingMessage):
        pass
