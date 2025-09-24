import asyncio
import logging
import multiprocessing
import os
import threading
import time
from io import BytesIO
from multiprocessing import Pipe
from multiprocessing.shared_memory import SharedMemory
from pickle import dumps, UnpicklingError
from pickle import loads
from threading import Thread
from typing import Any, Callable, Optional, Union

import settings
from core.messages import WorkerMessage, MessageTypes
from utils import ReadNoCopyIO, get_console_logger
from settings import PIPE_WAIT_TO_RETRY, PIPE_RETRY_COUNT, SH_MEM_RETRY_COUNT, SH_MEM_WAIT_TO_RETRY

logger = logging.getLogger(__name__)


class PipeWaitThread(Thread):
    def __init__(self, pipe_end: multiprocessing.Pipe, data_available_event: asyncio.Event):
        self.loop = asyncio.get_event_loop()
        self.pipe_end = pipe_end
        self.data_available_event = data_available_event
        super().__init__()

    def _init_logging(self):
        formatter = logging.Formatter(
            f'{settings.BASE_LOGGING_FORMAT}|Thread: {threading.get_ident()}|%(message)s'
        )

        self.logger = get_console_logger(
            __name__,
            formatter
        )

    def run(self):
        self._init_logging()
        self.logger.info(f"Start thread for pipe: {self.pipe_end.fileno()} polling")

        try:
            while True:
                if self.pipe_end.poll() and not self.data_available_event.is_set():
                    self.loop.call_soon_threadsafe(self.data_available_event.set)
                else:
                    time.sleep(0.01)
        except OSError:
            self.logger.warning(f"Thread closed because pipe is closed")


class SharedMemoryWrap:
    def __init__(self, sh_mem: SharedMemory):
        self._shared_memory = sh_mem

    @property
    def size(self):
        return self._shared_memory.size

    def clear_ready_to_write(self):
        self._shared_memory.buf[0] = 0

    def set_ready_to_write(self):
        self._shared_memory.buf[0] = 1

    def check_ready_to_write(self) -> bool:
        return self._shared_memory.buf[0] == 1

    def write_data(self, data: Union[memoryview | bytes]):

        if isinstance(data, memoryview):
            self._shared_memory.buf[1:len(data) + 1] = data[:]
        else:
            self._shared_memory.buf[1:len(data) + 1] = data

        self.clear_ready_to_write()

    def read_data(self, data_length: int) -> memoryview:
        return self._shared_memory.buf[1:data_length + 1]


def retry_send_on_ready_shared_memory(sh_mem_wrap: SharedMemoryWrap, data: memoryview) -> None:
    send_count = 0

    while True:
        if send_count >= SH_MEM_RETRY_COUNT:
            raise BlockingIOError(
                "Too much wait to send next chunk on memory view. Probably client dont read sent chunks"
            )

        send_count += 1
        # check that shared memory is empty
        if sh_mem_wrap.check_ready_to_write():
            sh_mem_wrap.write_data(data)
            return

        time.sleep(SH_MEM_WAIT_TO_RETRY)


def send_to_shared_memory(
        signal_pipe: Pipe,
        sh_mem: SharedMemory,
        message: WorkerMessage
):
    sh_mem_wrap = SharedMemoryWrap(sh_mem)
    message_data = dumps(message)

    if len(message_data) > sh_mem_wrap.size:
        # ensure that ready to write flag is set before start chunked session
        sh_mem_wrap.set_ready_to_write()

        message_buf = ReadNoCopyIO(message_data)
        try:
            while chunk := message_buf.read(sh_mem_wrap.size):
                retry_send_on_ready_shared_memory(sh_mem_wrap, chunk)

                blocking_retry_send(
                    signal_pipe,
                    WorkerMessage(
                        type=MessageTypes.chunk_shared_memory,
                        data=len(chunk),
                        # rack and session_id must be equal to original message
                        session_id=message.session_id,
                        rack=message.rack
                    )
                )

            blocking_retry_send(
                signal_pipe,
                WorkerMessage(
                    type=MessageTypes.chunk_shared_memory_fin,
                    session_id=message.session_id,
                    # rack must be equal to original message
                    rack=message.rack
                )
            )
        finally:
            message_buf.close()
    else:
        message_data_size = len(message_data)

        sh_mem_wrap.write_data(message_data)

        blocking_retry_send(
            signal_pipe,
            WorkerMessage(
                type=MessageTypes.check_shared_memory,
                data=message_data_size,
                session_id=message.session_id,
                # rack must be equal to original message
                rack=message.rack
            )
        )


async def async_get_from_shared_memory(
        signal_pipe: Pipe,
        sh_mem: SharedMemory,
        previous_get_message: WorkerMessage
) -> WorkerMessage:
    sh_mem_wrap = SharedMemoryWrap(sh_mem)

    if previous_get_message.type == MessageTypes.check_shared_memory:
        message = loads(sh_mem_wrap.read_data(previous_get_message.data))
    elif previous_get_message.type == MessageTypes.chunk_shared_memory:
        result_buffer = BytesIO()

        try:
            while True:
                result_buffer.write(sh_mem_wrap.read_data(previous_get_message.data))

                # signal that data is read on manager side
                sh_mem_wrap.set_ready_to_write()

                previous_get_message = await async_blocking_retry_recv(signal_pipe)

                # stop buffer filling if end chunk signal get
                if previous_get_message.type == MessageTypes.chunk_shared_memory_fin:
                    result_buffer.write(sh_mem_wrap.read_data(previous_get_message.data))
                    break

            message = loads(result_buffer.getvalue())
        finally:
            result_buffer.close()
    else:
        raise Exception(f"Expected shared memory operation got {previous_get_message.type.name} type message")

    return message


def _blocking_retry_body(func: Callable, method_name: str, function_data: Optional[Any] = None) -> Optional[Any]:
    send_count = 0
    while True:
        # try to recv chunk if BlockingIOError occurred
        if send_count >= PIPE_RETRY_COUNT:
            raise BlockingIOError(f"Too much attempts to retry sync {method_name} on blocked pipe")

        send_count += 1
        try:
            if function_data:
                return func(function_data)
            else:
                return func()
        except (BlockingIOError, EOFError, UnpicklingError) as ex:
            logger.warning(f"Error on pipe sync {method_name}: {type(ex).__name__}-{ex}")
            time.sleep(PIPE_WAIT_TO_RETRY)


# TODO think about more pythonc way to create async func versions
async def _async_blocking_retry_body(
        func: Callable,
        method_name: str,
        function_data: Optional[Any] = None
) -> Optional[Any]:
    send_count = 0
    while True:
        # try to recv chunk if BlockingIOError occurred
        if send_count >= PIPE_RETRY_COUNT:
            raise BlockingIOError(f"Too much attempts to retry {method_name} on blocked pipe")

        send_count += 1
        try:
            if function_data:
                return func(function_data)
            else:
                return func()
        except (BlockingIOError, EOFError, UnpicklingError) as ex:
            logger.warning(f"Error on pipe {method_name}: {type(ex).__name__}-{ex}")
            await asyncio.sleep(PIPE_WAIT_TO_RETRY)


def blocking_retry_recv(pipe: Pipe) -> Any:
    return _blocking_retry_body(pipe.recv, "recv", None)


def blocking_retry_send(pipe: Pipe, data: Any) -> None:
    return _blocking_retry_body(pipe.send, "send", data)


async def async_blocking_retry_recv(pipe: Pipe) -> Any:
    def inner_func():
        if pipe.poll():
            return pipe.recv()

    return await _async_blocking_retry_body(inner_func, "recv")


async def async_blocking_retry_send(pipe: Pipe, data: Any) -> None:
    await _async_blocking_retry_body(pipe.send, "recv", data)


def clear_pipe(pipe: multiprocessing.Pipe) -> int:
    remain_messages = 0
    while pipe.poll():
        try:
            pipe.recv()
            remain_messages += 1
        except (BlockingIOError, EOFError, UnpicklingError) as ex:
            logger.warning(f"Error on clean pipe recv: {type(ex).__name__}-{ex}")
            pass

    return remain_messages
