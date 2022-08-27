class CuriumError(Exception):
    """ Base class for curium exceptions """


class CuriumConnectionError(CuriumError):
    """ Base class for connection related errors """


class ConnectionFailed(CuriumConnectionError):
    """ Failed while connecting to backend server """


class NotConnectedError(CuriumConnectionError):
    """ No connection when invoking an operation """


class ServerDisconnectedError(CuriumConnectionError):
    """ Server disconnected during operation """


class InvalidChannelError(CuriumError, ValueError):
    """ Channel name is not valid for the backend """
