import logging
from enum import Enum

from core.utils.io import ReadNoCopyIO


def get_console_logger(logger_name: str, logger_format: logging.Formatter) -> logging.Logger:
    logger = logging.getLogger(logger_name)
    # clear previous handlers for exclude log duplication
    logger.handlers.clear()
    s_handler = logging.StreamHandler()
    s_handler.setFormatter(logger_format)
    logger.addHandler(s_handler)
    logger.propagate = False

    return logger


logger = logging.getLogger(__name__)


class ColorSpace(str, Enum):
    rgb = "RGB"


color_space_depth_map = {
    ColorSpace.rgb: 3
}