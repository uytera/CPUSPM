import time
from dataclasses import dataclass
from io import BytesIO
from typing import Optional

import numpy as np
from PIL import Image

from core.messages import WorkerMessage, MessageTypes
from core.types import CommandType
from utils import ColorSpace, color_space_depth_map
from core.worker.processors.interface import ProcessorContext, Processor
from core.worker.transport_utils import blocking_retry_send, send_to_shared_memory


#############################################################
# Example of session data accumulate with one result at end #
#############################################################

@dataclass
class AISessionInitInfo:
    width: int
    height: int
    img_format: str
    img_colorspace: ColorSpace


@dataclass
class AIContext(ProcessorContext):
    width: int
    height: int
    img_format: str
    img_colorspace: ColorSpace
    result_image_buffer: np.ndarray
    img_compression_level = 90


class AverageImageProcessor(Processor):
    processor_abbreviation = "AI"

    def init_session(self, message: WorkerMessage):
        message_data: AISessionInitInfo = message.data

        session_context = self._sessions.get(message.session_id)
        if session_context is not None:
            raise Exception(f"Session with id: {message.session_id} already exists")

        result_image_buffer = np.zeros(
            (message_data.width, message_data.height, color_space_depth_map[message_data.img_colorspace]),
            np.float32
        )

        session_context = AIContext(
            command_type=CommandType.average_image,
            width=message_data.width,
            height=message_data.height,
            img_format=message_data.img_format,
            img_colorspace=message_data.img_colorspace,
            result_image_buffer=result_image_buffer
        )

        self._logger.info(f"{message.session_id}|{self.processor_abbreviation}|Init session")
        self._sessions[message.session_id] = session_context

        blocking_retry_send(
            self._manager_pipe,
            WorkerMessage(
                type=MessageTypes.ok,
                # rack and session_id must be equal to original message
                session_id=message.session_id,
                rack=message.rack
            )
        )

    def process_message(self, message: WorkerMessage):
        session_id = message.session_id
        data = message.data

        if session_id is None:
            raise Exception(f"Session must specified for {self.processor_abbreviation} command processing")

        session_context: AIContext = self._sessions.get(session_id)

        if session_context is None:
            raise Exception(f"Session with id: {session_id} not exists but processing requested")
        else:
            img_colorspace = session_context.img_colorspace
            result_image_buffer = session_context.result_image_buffer

        data_buffer = BytesIO(data)
        try:
            image_obj = Image.open(data_buffer).convert(img_colorspace.value)
        finally:
            data_buffer.close()

        image_array = np.asarray(image_obj, dtype=np.float32)

        rows = min(result_image_buffer.shape[0], image_array.shape[0])
        cols = min(result_image_buffer.shape[1], image_array.shape[1])
        result_image_buffer[:rows, :cols] += image_array[:rows, :cols]

        blocking_retry_send(
            self._manager_pipe,
            WorkerMessage(
                type=MessageTypes.ok,
                # rack and session_id must be equal to original message
                session_id=message.session_id,
                rack=message.rack
            )
        )

    def clear_session(self, session_id, rack: Optional[str] = None):
        image_buffer = BytesIO()
        try:
            self._logger.info(f"{session_id}|{self.processor_abbreviation}|Clear session")
            session_context: AIContext = self._sessions[session_id]

            result = Image.fromarray(np.uint8(session_context.result_image_buffer))
            result.save(
                image_buffer,
                format=session_context.img_format,
                compress_level=session_context.img_compression_level
            )

            del self._sessions[session_id]

            send_to_shared_memory(
                self._manager_pipe,
                self._manager_shared_memory,
                WorkerMessage(
                    type=MessageTypes.data,
                    data=image_buffer.getvalue(),
                    # rack and session_id must be equal to original message
                    session_id=session_id,
                    rack=rack
                )
            )
        finally:
            image_buffer.close()
