from io import BytesIO
from typing import Optional

from PIL import Image

from core.messages import WorkerMessage, MessageTypes
from core.worker.processors.interface import Processor
from core.worker.transport_utils import send_to_shared_memory


#############################################################
# Example of in time processing without session #
#############################################################


class GrayscaleImageProcessor(Processor):
    processor_abbreviation = "GsI"

    def process_message(self, message: WorkerMessage):
        rack = message.rack
        data = message.data

        image_buffer = BytesIO()
        try:
            image_obj = Image.open(data).convert("L")
            image_obj.save(image_buffer)

            send_to_shared_memory(
                self._manager_pipe,
                self._manager_shared_memory,
                WorkerMessage(
                    type=MessageTypes.data,
                    data=image_buffer.getvalue(),
                    # rack and session_id must be equal to original message
                    rack=rack
                )
            )
        finally:
            image_buffer.close()

    def clear_session(self, session_id, rack: Optional[str] = None):
        # no session realized in this processor
        pass
