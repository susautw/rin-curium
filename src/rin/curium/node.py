from itertools import count
from typing import List, Union, TypeVar, Optional, Type, Any, Dict

from . import ICommand, IConnection, ResponseHandlerBase, ISerializer

R = TypeVar("R")


class Node:
    _sent_cmd_response_handlers: Dict[str, ResponseHandlerBase]
    _nid: str
    _connection: IConnection
    _serializer: ISerializer
    _cmd_count: count

    def __init__(self, connection: IConnection = None, serializer: ISerializer = None): ...

    def connect(self) -> None:
        self._nid = self._connection.connect()

    def close(self) -> None:
        self._connection.close()

    def join(self, name: str) -> None:
        self._connection.join(name)

    def leave(self, name: str) -> None:
        self._connection.leave(name)

    def send(
            self,
            cmd: ICommand[R],
            destinations: List[str],
            response_handler: Union[None, str, ResponseHandlerBase[R]],
    ) -> ResponseHandlerBase[R]:
        cid = str(next(self._cmd_count))
        wrapped_cmd = NodeCommandWrapper(self._nid, cid, cmd)
        rh = self.get_response_handler(response_handler)
        self._sent_cmd_response_handlers[cid] = rh
        self._connection.send(self._serializer.serialize(wrapped_cmd), destinations)
        return rh

    def get_response_handler(
            self,
            response_handler: Union[None, str, ResponseHandlerBase[R]]
    ) -> ResponseHandlerBase[R]:
        ...

    def recv(self, block=True, timeout=None) -> Optional[ICommand]:
        """
        :param block: is blocking or not
        :param timeout: timeout of this operation
        :return: ICommand or None, None present no command received
        """
        data = self._connection.recv(block, timeout)
        if data is None:
            return None
        cmd = self._serializer.deserialize(data)
        # TODO No Response
        assert isinstance(cmd, NodeCommandWrapper)


    def register_cmd(self, cmd_typ: Type[ICommand], ctx: Any) -> None: ...


class NodeCommandWrapper(ICommand[R]):
    def __init__(self, nid: str, cid: str, cmd: ICommand[R]):
        self.nid = nid
        self.cid = cid
        self.cmd = cmd

    def execute(self, ctx) -> R:
        return self.cmd.execute(ctx)
