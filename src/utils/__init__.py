import logging
from enum import Enum

from utils.io import ReadNoCopyIO


def get_console_logger(logger_name: str, logger_format: logging.Formatter) -> logging.Logger:
    logger = logging.getLogger(logger_name)
    logger.setLevel(level=logging.INFO)
    # clear previous handlers for exclude log duplication
    logger.handlers.clear()
    s_handler = logging.StreamHandler()
    s_handler.setFormatter(logger_format)
    logger.addHandler(s_handler)
    logger.propagate = False

    return logger


class ColorSpace(str, Enum):
    rgb = "RGB"


color_space_depth_map = {
    ColorSpace.rgb: 3
}


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
