import asyncio
from anyio import get_cancelled_exc_class
from starlette.websockets import WebSocketDisconnect
from websockets import ConnectionClosed


from core.session_wrap.flow.factory import TwoWayFlowFactory
from core.session_wrap.transport import TwoWayTransportInterface
from core.worker.worker_manager import CPUCommands


class MessageHandler:
    def __init__(self,
                 tw_transport: TwoWayTransportInterface,
                 cpu_commands: CPUCommands):
        self.tw_transport = tw_transport
        self.cpu_commands = cpu_commands
        self.logger = self.tw_transport.logger

    async def handle_cycle(self):
        while True:
            try:
                message = await self.tw_transport.recv_message()

                required_flow = TwoWayFlowFactory.get_flow(
                    message.flow_type,
                    self.tw_transport,
                    self.cpu_commands
                )

                await required_flow.start(message)

            except (TimeoutError,
                    asyncio.TimeoutError,
                    get_cancelled_exc_class(),
                    WebSocketDisconnect,
                    ConnectionClosed,
                    RuntimeError) as ex:
                self.logger.debug("Stop message handling cycle: %s-%s", type(ex).__name__, ex)
                # raise errors to fastapi router level and abort connection
                raise ex
            except Exception as ex:
                self.logger.error(
                    "Error occurred on handling client messages: %s-%s",
                    type(ex).__name__,
                    ex,
                    exc_info=True
                )

                # try to inform client about error without aborting connection
                await self.tw_transport.send_exception(ex)
                continue
