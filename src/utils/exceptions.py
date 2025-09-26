# WS EXCEPTIONS

class ClientSendDataTimeout(Exception):
    pass


# WORKER MANAGER EXCEPTIONS

class FreeProcessObtainTimeout(Exception):
    pass


class PipeMessageReceiveTimeout(Exception):
    pass


# WORKER ERROR

class WorkerCriticalError(Exception):
    pass
