import json
import logging
import os

# logging settings

DEBUG = json.loads(os.environ.get('DEBUG', 'False').lower())
SUB_DEBUG = json.loads(os.environ.get('SUB_DEBUG', 'False').lower())
BASE_LOGGING_FORMAT = os.environ.get('BASE_LOGGING_FORMAT', '%(asctime)s|%(levelname)s|%(name)s')
LOGGING_FORMAT = f"{BASE_LOGGING_FORMAT}|%(message)s"
SUB_LOGGERS = ["aiortc", "aioice.ice", "urllib3.connectionpool", "multipart.multipart"]
LOGGING_LEVEL = logging.DEBUG if DEBUG else logging.INFO
SUB_LOGGERS_LEVEL = logging.DEBUG if SUB_DEBUG else logging.WARNING

# web server settings

ROOT_PATH = os.environ.get('ROOT_PATH', '/')
WEBSOCKET_TIMEOUT = int(os.environ.get('WEBSOCKET_TIMEOUT', 60))

# transport timeouts

MESSAGE_WAIT_TIMEOUT = int(os.environ.get('MESSAGE_WAIT_TIMEOUT', 60))

# worker process settings

WORKER_PROCESS_COUNT = int(os.environ.get('WORKER_PROCESS_COUNT', 5))

SHARED_MEMORY_SIZE_MB = int(os.environ.get('SHARED_MEMORY_SIZE_MB', 100)) * 1024 * 1024  # 100 MB

PROCESS_HUNG_TIMEOUT = float(os.environ.get('PIPE_WAIT_TO_RETRY', 10))

PIPE_WAIT_TO_RETRY = float(os.environ.get('PIPE_WAIT_TO_RETRY', 0.1))
PIPE_RETRY_COUNT = int(os.environ.get('PIPE_RETRY_COUNT', 10))

SH_MEM_WAIT_TO_RETRY = float(os.environ.get('SH_MEM_WAIT_TO_RETRY', 0.1))
SH_MEM_RETRY_COUNT = int(os.environ.get('SH_MEM_RETRY_COUNT', 10))

