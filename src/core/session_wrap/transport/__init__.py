import asyncio
import logging
import uuid
from abc import ABC, abstractmethod

import settings
from core.session_wrap.messages import IncomingMessage, OutcomingMessage, OutcomingMessageType
from utils import get_console_logger
from utils.exceptions import ClientMessageSendTimeout


class TwoWayTransportInterface(ABC):

    def __init__(self):
        self.transport_session_id = str(uuid.uuid4())[:4]
        formatter = logging.Formatter(
            f'{settings.BASE_LOGGING_FORMAT}|Transport|{self.transport_session_id}|%(message)s'
        )
        self.logger = get_console_logger(
            __name__,
            formatter
        )

    async def recv_message(self) -> IncomingMessage:
        try:
            message = await asyncio.wait_for(
                self._recv_text(),
                timeout=settings.MESSAGE_WAIT_TIMEOUT
            )
        except asyncio.TimeoutError:
            raise ClientMessageSendTimeout()

        self.logger.info(f"Receive message from client: {message}")

        return IncomingMessage.load_model(message)

    async def send_message(self, message: str):
        self.logger.info(f"Send test message to client: {message}")
        await self._send_text(
            OutcomingMessage(
                type=OutcomingMessageType.message,
                data=message
            ).model_dump_json()
        )

    async def send_exception(self, exception: Exception):
        exception_class = type(exception).__name__
        exception_message = str(exception)
        self.logger.warning(f"Send exception info to client: {exception_class}-{exception_message}")

        await self._send_text(
            OutcomingMessage(
                type=OutcomingMessageType.exception,
                data={"type": exception_class, "message": exception_message}
            ).model_dump_json()
        )

    async def send_outcoming_message(self, out_message: OutcomingMessage):
        self.logger.info(f"Send predefined message to client")
        await self._send_text(out_message.model_dump_json())

    @abstractmethod
    async def recv_bytes(self) -> bytes:
        pass

    @abstractmethod
    async def send_bytes(self, data: bytes):
        pass

    @abstractmethod
    async def _recv_text(self) -> str:
        pass

    @abstractmethod
    async def _send_text(self, data: str):
        pass
