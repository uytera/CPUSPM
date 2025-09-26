from enum import Enum


# duplicate of CommandType, but it can be extended if new flow not mapped in CommandType one to one
class FlowType(Enum):
    average_image = 1
    heatmap_image = 2
    grayscale_image = 3
