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

        self._logger.info(f"{self.processor_abbreviation}|Handle image request")

        image_buffer = BytesIO(data)
        result_image_buffer = BytesIO()
        try:
            image_obj = Image.open(image_buffer).convert("L")
            # TODO add params to worker message for configuring format and compression level
            image_obj.save(result_image_buffer, format="JPEG")

            send_to_shared_memory(
                self._manager_pipe,
                self._manager_shared_memory,
                WorkerMessage(
                    type=MessageTypes.data,
                    data=result_image_buffer.getvalue(),
                    # rack and session_id must be equal to original message
                    rack=rack
                )
            )
        finally:
            image_buffer.close()
            result_image_buffer.close()

    def clear_session(self, session_id, rack: Optional[str] = None):
        # no session realized in this processor
        pass
