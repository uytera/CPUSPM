# WS EXCEPTIONS

class ClientTimeout(Exception):
    pass


class ClientMessageSendTimeout(ClientTimeout):
    pass


class ClientBinaryDataSendTimeout(ClientTimeout):
    pass


# WORKER MANAGER EXCEPTIONS

class FreeProcessObtainTimeout(Exception):
    pass


class PipeMessageReceiveTimeout(Exception):
    pass


# WORKER ERROR

class WorkerCriticalError(Exception):
    pass
