# WS EXCEPTIONS

class ComponentTimeouts(Exception):
    pass


class ComponentMessageSendTimeout(ComponentTimeouts):
    pass


class ComponentBinaryDataSendTimeout(ComponentTimeouts):
    pass


class ComponentPacketDataSendTimeout(ComponentTimeouts):
    pass


class ComponentVideoChunkDataSendTimeout(ComponentTimeouts):
    pass


class NoVideoTransferred(Exception):
    pass


# DECODING MANAGER EXCEPTIONS

class FreeProcessObtainTimeout(Exception):
    pass


class PipeMessageReceiveTimeout(Exception):
    pass


# WORKER ERROR

class WorkerCriticalError(Exception):
    pass
