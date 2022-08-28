class CuriumError(Exception):
    """ Base class for curium exceptions """


class CuriumConnectionError(CuriumError):
    """ Base class for connection related errors """


class ConnectionFailedError(CuriumConnectionError):
    """ Failed while connecting to backend server """


class NotConnectedError(CuriumConnectionError):
    """ No connection when invoking an operation """


class ServerDisconnectedError(CuriumConnectionError):
    """ Server disconnected during operation """


class InvalidChannelError(CuriumError, ValueError):
    """ Channel name is not valid for the backend """


class CuriumSerializationError(CuriumError):
    """ Base class for serialization errors """


class UnsupportedObjectError(CuriumSerializationError, TypeError):
    """ Found unsupported object in data to be serialize """


class InvalidFormatError(CuriumSerializationError, ValueError):
    """ Found unsupported data format while deserializing """


class CommandNotRegisteredError(CuriumSerializationError, KeyError):
    """ Command wasn't registered in the serializer """


class CommandHasRegisteredError(CuriumSerializationError, ValueError):
    """ Command already registered in the serializer"""
