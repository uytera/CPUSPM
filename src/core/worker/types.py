from enum import Enum


class CommandType(Enum):
    summary_image = 1
    heatmap_image = 2
    grayscale_image = 3


class ImageFormat(str, Enum):
    jpeg = "jpeg"
