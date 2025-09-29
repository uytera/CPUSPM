import logging
import multiprocessing
import os
import sys
from datetime import datetime
from multiprocessing import Process
from multiprocessing.shared_memory import SharedMemory
from typing import Dict

import settings
from core.worker.messages import WorkerMessage, MessageTypes
from utils import get_console_logger
from utils.exceptions import WorkerCriticalError
from core.worker.worker_process.processors.factory import ProcessorFactory
from core.worker.worker_process.processors.interface import ProcessorContext
from core.worker.worker_process.transport_utils import blocking_retry_recv, blocking_retry_send


class Worker(Process):

    def __init__(self,
                 pipe: multiprocessing.Pipe,
                 transfer_memory_name: str):
        super().__init__()

        self.pipe = pipe
        self.transfer_memory = SharedMemory(name=transfer_memory_name)
        self.periodic_functions = []
        self.sessions: Dict[str, ProcessorContext] = {}
        self.logger = None
        self.last_clear_time = datetime.now()

    def _init_logging(self):
        logging.basicConfig(format=settings.LOGGING_FORMAT, level=settings.LOGGING_LEVEL)

        formatter = logging.Formatter(
            f'{settings.BASE_LOGGING_FORMAT}|PID: {os.getpid()}|%(message)s'
        )

        self.logger = get_console_logger(
            __name__,
            formatter
        )

    def _init_command_processor(self):
        self.command_processor_factory = ProcessorFactory(
            self.sessions,
            self.logger,
            self.pipe,
            self.transfer_memory
        )

    def run(self):
        # move logger init on process level
        self._init_logging()
        self._init_command_processor()

        self.logger.info(
            f"Start process. "
            f"Pipe: {self.pipe.fileno()}. "
            f"Shared memory: {self.transfer_memory.name}. "
        )

        # flush process output
        sys.stdout.flush()

        try:
            break_cycle = False

            while True:
                try:
                    # TODO add mechanism to break out from message wait and go to execute some background functions
                    message: WorkerMessage = blocking_retry_recv(self.pipe)
                    message_type = message.type
                except KeyboardInterrupt:
                    # close process on keyboard interrupt
                    return
                except Exception as ex:
                    self.logger.error(f"Exception occurred while receiving message. {type(ex).__name__}-{ex}", exc_info=True)
                    # close process
                    return

                # Message must be received from process manager side otherwise process manager will hang up
                if break_cycle:
                    # send error to manager that signal process is unstable and must be restarted
                    blocking_retry_send(
                        self.pipe,
                        WorkerCriticalError("Fatal exception occurred in process handling cycle")
                    )
                    return

                try:
                    processor = self.command_processor_factory.return_processor_by_command(message.command)

                    if message_type == MessageTypes.clear:
                        processor.clear_session(message.session_id, message.rack)
                    elif message_type == MessageTypes.init_session:
                        processor.init_session(message)
                    elif message_type == MessageTypes.data:
                        processor.process_message(message)
                    else:
                        self.logger.error(f"Unknown message type")
                        blocking_retry_send(self.pipe, Exception("Unknown message type"))

                except Exception as ex:
                    self.logger.error(f"Exception occurred in process. {type(ex).__name__}-{ex}", exc_info=True)
                    blocking_retry_send(self.pipe, ex)
                finally:
                    # flush process output
                    sys.stdout.flush()
        finally:
            self.pipe.close()
