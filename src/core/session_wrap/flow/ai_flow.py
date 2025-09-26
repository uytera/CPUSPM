import uuid

from core.session_wrap.flow import TwoWayFlowInterface
from core.session_wrap.messages import IncomingMessage


class AIFlow(TwoWayFlowInterface):
    async def start(self, init_message: IncomingMessage):
        session_id = uuid.uuid4()
        self._tw_transport.logger.info(f"Init AI session with id: {session_id}")

        async with self._cpu_commands.average_image_accumulator(
                init_message.context_data.height,
                init_message.context_data.width,
                init_message.context_data.img_format,
                session_id
        ) as args_tuple:
            work_function, result_list = args_tuple

            await self._tw_transport.send_message("Transferring permitted")

            while True:
                image = self._tw_transport.recv_bytes()

                # hexadecimal number 5 in 1 byte in big endian format #5.to_bytes(1, 'big')
                # recording ended if 5 comes
                if image == self.binary_stop_send_signal:
                    break

                await work_function(image)

        await self._tw_transport.send_bytes(result_list[0])

        await self._tw_transport.send_message("Flow ended")
