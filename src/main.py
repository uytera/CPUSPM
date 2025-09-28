import logging
import os
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI
from starlette.websockets import WebSocket, WebSocketDisconnect
from uvicorn_worker import UvicornWorker
from websockets import ConnectionClosedOK, ConnectionClosed

import settings
from core.session_wrap.message_handler import MessageHandler
from core.session_wrap.transport.realizations.websocket import WebSocketTransport
from core.worker.worker_manager import WorkerProcessManager, CPUCommands
from utils.exceptions import ClientTimeout
from utils.metrics import CpuSpmMetrics
from utils.metrics.manager import get_metrics_manager

ws_timeout = (int(os.environ['WEBSOCKET_TIMEOUT']) + 10) // 2


class CustomUvicornWorker(UvicornWorker):
    CONFIG_KWARGS = {
        "ws_ping_interval": ws_timeout,
        "ws_ping_timeout": ws_timeout,
        "http": "httptools",
        # no proper work without configuration param below
        "loop": "asyncio"
    }


logger = logging.getLogger(__name__)
logging.basicConfig(format=settings.LOGGING_FORMAT, level=settings.LOGGING_LEVEL)

# set formater to all loggers
for name in logging.root.manager.loggerDict:
    logger = logging.getLogger(name)
    for handler in logger.handlers:
        handler.setFormatter(logging.Formatter(settings.LOGGING_FORMAT))

uvicorn_logger = logging.getLogger("uvicorn.error")
uvicorn_logger.setLevel(settings.LOGGING_LEVEL)

gunicorn_logger = logging.getLogger("gunicorn.error")
gunicorn_logger.setLevel(settings.LOGGING_LEVEL)

sub_loggers = [logging.getLogger(logger_name) for logger_name in settings.SUB_LOGGERS]
for logger_ in sub_loggers:
    logger_.setLevel(settings.SUB_LOGGERS_LEVEL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Start {settings.WORKER_PROCESS_COUNT} decoding processes.")
    process_manager = WorkerProcessManager(settings.WORKER_PROCESS_COUNT)
    app.state.cpu_commands = CPUCommands(process_manager)

    yield

    # close process pool executor
    process_manager.stop_processes()

metrics_manager = get_metrics_manager()
app = FastAPI(
    title='video recoding service',
    version=settings.APP_VERSION,
    description='Save video and estimate liveness for it',
    root_path=settings.ROOT_PATH,
    lifespan=lifespan
)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    unique_session_id = str(uuid.uuid4())
    logger_prefix = f"{unique_session_id}|"

    connection_start_time = time.time()
    try:
        await websocket.accept()

        metrics_manager.update_gauge_metric(CpuSpmMetrics.WebsocketConnectionCount.value, 1)

        await MessageHandler(
            tw_transport=WebSocketTransport(websocket),
            cpu_commands=websocket.app.state.cpu_commands
        ).handle_cycle()
    except ClientTimeout as ex:
        logger.warning(f"{logger_prefix}Client not send any data. Exception: {type(ex).__name__}")
        await websocket.close(code=3008)
    except RuntimeError as ex:
        if isinstance(ex, RuntimeError) and str(ex).startswith("Unexpected ASGI message"):
            logger.warning(f"Unexpected socket state: {ex}")
        else:
            raise

    except (ConnectionClosed, WebSocketDisconnect, RuntimeError) as ex:
        # warning on unexpected connection close
        if not (
            (type(ex) is WebSocketDisconnect and (ex.code == 1000 or ex.code == 1001)) or
            type(ex) is ConnectionClosedOK
        ):
            logger.warning(f"Unexpected connection close: {ex}")
    except Exception:
        await websocket.close(code=1011)
        raise
    finally:
        metrics_manager.update_average_metric(
            CpuSpmMetrics.WebsocketAverageConnectionDuration.value,
            time.time() - connection_start_time
        )
        metrics_manager.update_gauge_metric(CpuSpmMetrics.WebsocketConnectionCount.value, -1)
