import asyncio

from starlette.websockets import WebSocket

import settings
from core.session_wrap.transport import TwoWayTransportInterface
from utils.exceptions import ClientBinaryDataSendTimeout


class WebSocketTransport(TwoWayTransportInterface):
    def __init__(self, websocket_connection: WebSocket):
        super().__init__()
        self.websocket_connection = websocket_connection

    async def send_bytes(self, data: bytes):
        await self.websocket_connection.send_bytes(data)

    async def _recv_text(self) -> str:
        return await self.websocket_connection.receive_text()

    async def _send_text(self, data: str):
        await self.websocket_connection.send_text(data)

    async def recv_bytes(self) -> bytes:
        try:
            return await asyncio.wait_for(
                self.websocket_connection.receive_bytes(),
                timeout=settings.MESSAGE_WAIT_TIMEOUT
            )
        except asyncio.TimeoutError as ex:
            raise ClientBinaryDataSendTimeout() from ex
