from functools import lru_cache
from itertools import count
from typing import List, Union, TypeVar, Optional, Type, Any, Dict, Callable

from fancy import config as cfg

from . import CommandBase, IConnection, ResponseHandlerBase, ISerializer

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
            cmd: CommandBase[R],
            destinations: List[str],
            response_handler: Union[None, str, ResponseHandlerBase[R]],
    ) -> ResponseHandlerBase[R]:
        cid = str(next(self._cmd_count))
        wrapped_cmd = CommandWrapper(self._nid, cid, cmd)
        rh = self.get_response_handler(response_handler)
        self._sent_cmd_response_handlers[cid] = rh
        self._connection.send(self._serializer.serialize(wrapped_cmd), destinations)
        return rh

    def get_response_handler(
            self,
            response_handler: Union[None, str, ResponseHandlerBase[R]]
    ) -> ResponseHandlerBase[R]:
        ...

    def recv(self, block=True, timeout=None) -> Optional["CommandWrapper"]:
        """
        :param block: is blocking or not
        :param timeout: timeout of this operation
        :return: ICommand or None, None present no command received
        """
        raw_data = self._connection.recv(block, timeout)
        if raw_data is None:
            return None
        cmd = self._serializer.deserialize(raw_data)
        if isinstance(cmd, CommandWrapper):
            return cmd
        else:
            raise RuntimeError(f"Received command ({cmd}) is not a CommandWrapper")

    def register_cmd(self, cmd_typ: Type[CommandBase], ctx: Any) -> None: ...

    def get_context(self, name: str) -> Any: ...

    @property
    def nid(self) -> str:
        return self._nid


def option_only_filter(p: cfg.PlaceHolder) -> bool:
    return isinstance(p, cfg.Option)


def to_cmd_dict(o) -> dict:
    if isinstance(o, CommandBase):
        return o.to_dict(prevent_circular=True, filter=option_only_filter)
    elif isinstance(o, dict):
        return o
    raise TypeError(f"Type of {o} neither CommandBase nor dict")


class CommandWrapper(CommandBase[R]):
    nid: str = cfg.Option(required=True, type=str)
    cid: str = cfg.Option(required=True, type=str)
    cmd: dict = cfg.Option(required=True, type=to_cmd_dict)

    __cmd_name__ = "__cmd_wrapper__"

    def execute(self, ctx: Node) -> R:
        # TODO handle response
        return self.get_cmd(ctx).execute(ctx)

    @lru_cache
    def get_cmd(self, node: Node) -> CommandBase:
        s = node.get_context(self.__cmd_name__)
        assert isinstance(s, ISerializer)
        return s.deserialize(self.cmd)

    def to_dict(
            self,
            recursive=True,
            prevent_circular=False, *,
            load_lazies=None,
            filter: Callable[[cfg.PlaceHolder], bool] = None
    ) -> dict:
        # cmd already convert to a dict.
        return super().to_dict(recursive=False, prevent_circular=True)
