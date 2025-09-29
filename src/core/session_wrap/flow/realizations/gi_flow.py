from core.session_wrap.flow import TwoWayFlowInterface
from core.session_wrap.messages import IncomingMessage


class GIFlow(TwoWayFlowInterface):
    flow_name = "GI"

    async def start(self, init_message: IncomingMessage):
        await self._tw_transport.send_message("Transferring permitted")

        image = await self._tw_transport.recv_bytes()

        await self._tw_transport.send_bytes(
            await self._cpu_commands.image_to_grayscale(image)
        )

        await self._tw_transport.send_message("Flow ended")
