from dataclasses import dataclass
from io import BytesIO
from typing import Optional

import numpy as np
from PIL import Image

from core.messages import WorkerMessage, MessageTypes
from core.types import CommandType
from core.worker.processors.interface import ProcessorContext, Processor
from core.worker.transport_utils import blocking_retry_send, send_to_shared_memory


######################################################################
# Example of session data processing with result from each data pass #
######################################################################

@dataclass
class HISessionInitInfo:
    width: int
    height: int
    img_format: str


@dataclass
class HIContext(ProcessorContext):
    width: int
    height: int
    img_format: str
    heatmap_buffer: np.ndarray
    img_compression_level = 90


class HeatmapImageProcessor(Processor):
    processor_abbreviation = "HI"

    @staticmethod
    def _get_heatmap(heatmap_buffer: np.ndarray):
        max_value = heatmap_buffer.max()
        if max_value == 0:
            return heatmap_buffer

        return (heatmap_buffer / max_value) * 255

    def init_session(self, message: WorkerMessage):
        message_data: HISessionInitInfo = message.data

        session_context = self._sessions.get(message.session_id)
        if session_context is not None:
            raise Exception(f"Session with id: {message.session_id} already exists")

        heatmap_buffer = np.zeros(
            (message_data.width, message_data.height),
            np.float32
        )

        session_context = HIContext(
            command_type=CommandType.heatmap_image,
            width=message_data.width,
            height=message_data.height,
            img_format=message_data.img_format,
            heatmap_buffer=heatmap_buffer
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
        rack = message.rack
        data = message.data

        if session_id is None:
            raise Exception(f"Session must specified for {self.processor_abbreviation} command processing")

        session_context: HIContext = self._sessions.get(session_id)

        if session_context is None:
            raise Exception(f"Session with id: {session_id} not exists but processing requested")
        else:
            heatmap_buffer = session_context.heatmap_buffer

        image_buffer = BytesIO(data)
        try:
            image_obj = Image.open(image_buffer).convert("RGB")
            image_array = np.asarray(image_obj, dtype=np.float32) / 255.0
        finally:
            image_buffer.close()

        gray = 0.299 * image_array[:, :, 0] + 0.587 * image_array[:, :, 1] + 0.114 * image_array[:, :, 2]

        rows = min(heatmap_buffer.shape[0], gray.shape[0])
        cols = min(heatmap_buffer.shape[1], gray.shape[1])
        heatmap_buffer[:rows, :cols] += gray[:rows, :cols]

        image_buffer = BytesIO()
        try:
            result = Image.fromarray(
                self._get_heatmap(heatmap_buffer), mode="L"
            )
            result.save(
                image_buffer,
                format=session_context.img_format,
                compress_level=session_context.img_compression_level
            )

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

    def clear_session(self, session_id, rack: Optional[str] = None):
        self._logger.info(f"{session_id}|{self.processor_abbreviation}|Clear session")

        try:
            del self._sessions[session_id]
        except KeyError:
            # skip if session already cleared. For example process restarted.
            pass

        blocking_retry_send(
            self._manager_pipe,
            WorkerMessage(
                type=MessageTypes.ok,
                # rack and session_id must be equal to original message
                session_id=session_id,
                rack=rack
            )
        )
