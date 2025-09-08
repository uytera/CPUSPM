from logging import Logger
from multiprocessing import Pipe
from multiprocessing.shared_memory import SharedMemory
from typing import Dict, Any

from core.types import CommandType
from core.worker.processors.interface import Processor
from core.worker.processors.realizations.average_image_processor import AverageImageProcessor
from core.worker.processors.realizations.grayscale_image_processor import GrayscaleImageProcessor
from core.worker.processors.realizations.heatmap_image_processor import HeatmapImageProcessor


class ProcessorFactory:
    processor_cmd_type_mapping: Dict[CommandType, type[Processor]] = {
        CommandType.average_image: AverageImageProcessor,
        CommandType.heatmap_image: HeatmapImageProcessor,
        CommandType.grayscale_image: GrayscaleImageProcessor
    }

    def __init__(
            self,
            sessions: Dict[str, Any],
            outer_logger: Logger,
            manager_pipe: Pipe,
            manager_shared_memory: SharedMemory
    ):
        self._sessions = sessions
        self._outer_logger = outer_logger
        self._manager_pipe = manager_pipe
        self._manager_shared_memory = manager_shared_memory
        self._processors_storage = {}

    def return_processor_by_command(self, command_type: CommandType) -> Processor:
        if (processor := self._processors_storage.get(command_type)) is not None:
            pass
        else:
            try:
                processor = self.processor_cmd_type_mapping[command_type](
                    self._sessions,
                    self._outer_logger,
                    self._manager_pipe,
                    self._manager_shared_memory
                )
            except KeyError:
                raise KeyError(f"Unknown command type: {command_type}. Not found any suitable processor")
            self._processors_storage[command_type] = processor

        return processor
