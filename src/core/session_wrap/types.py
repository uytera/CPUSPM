from enum import Enum


# duplicate of CommandType, but it can be extended if new flow not mapped in CommandType one to one
class FlowType(Enum):
    summary_image = 0
    heatmap_image = 1
    grayscale_image = 2
