from functools import lru_cache
from itertools import count
from typing import List, TypeVar, Optional, Type, Any, Dict, Callable, MutableMapping, Union
from weakref import WeakValueDictionary

from fancy import config as cfg
from redis import Redis

from . import CommandBase, IConnection, ResponseHandlerBase, ISerializer
from .connections import RedisConnection
from .response_handles import BlockUntilAllReceived
from .serializers import JSONSerializer
from .utils import option_only_filter

R = TypeVar("R")


class NoResponseType:
    pass


NoResponse = NoResponseType()

NoContextSpecified = object()


class Node:
    _sent_cmd_response_handlers: MutableMapping[str, ResponseHandlerBase]
    _nid: str
    _connection: IConnection
    _serializer: ISerializer
    _cmd_count: count

    _cmd_contexts: Dict[str, Any]

    def __init__(self, connection: Union[Redis, IConnection], serializer: ISerializer = None):
        if isinstance(connection, Redis):
            connection = RedisConnection(connection)
        self._connection = connection
        self._serializer = JSONSerializer() if serializer is None else serializer

        self._sent_cmd_response_handlers = WeakValueDictionary()
        self._cmd_contexts = {}

        self.register_cmd(CommandWrapper, self._serializer)
        self.register_cmd(SetResponse)

    def connect(self) -> None:
        self._nid = self._connection.connect()
        self.join(self._nid)
        self.join("all")

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
            response_handler: Optional[ResponseHandlerBase[R]],
            response_timeout: float = None
    ) -> ResponseHandlerBase[R]:
        cid = str(next(self._cmd_count))
        wrapped_cmd = CommandWrapper(nid=self._nid, cid=cid, cmd=cmd)
        rh = self._create_response_handler(response_handler, response_timeout)
        self._sent_cmd_response_handlers[cid] = rh
        num_receivers = self._connection.send(self._serializer.serialize(wrapped_cmd), destinations)
        rh.set_num_receivers(num_receivers)
        return rh

    def send_no_response(self, cmd: CommandBase, destinations: List[str]) -> None:
        self._connection.send(self._serializer.serialize(cmd), destinations)

    def _create_response_handler(
            self,
            response_handler: Optional[ResponseHandlerBase[R]],
            response_timeout: Optional[float]
    ) -> ResponseHandlerBase[R]:
        if response_handler is None:
            return BlockUntilAllReceived(timeout=response_timeout)
        elif response_timeout is not None:
            raise ValueError("response_timeout must be None when response_handler exists")
        return response_handler

    def recv(self, block=True, timeout=None) -> Optional[CommandBase]:
        """
        :param block: is blocking or not
        :param timeout: timeout of this operation
        :return: ICommand or None, None present no command received
        """
        raw_data = self._connection.recv(block, timeout)
        if raw_data is None:
            return None
        cmd = self._serializer.deserialize(raw_data)
        return cmd

    def register_cmd(self, cmd_typ: Type[CommandBase], ctx: Any = NoContextSpecified) -> None:
        self._serializer.register_cmd(cmd_typ)
        if ctx is not NoContextSpecified:
            self._cmd_contexts[cmd_typ.__cmd_name__] = ctx

    def get_cmd_context(self, name: str) -> Any:
        return self._cmd_contexts[name]

    def set_response(self, cid: str, response: Any) -> None:
        if cid in self._sent_cmd_response_handlers:
            self._sent_cmd_response_handlers[cid].set_response(response)
        else:
            pass  # TODO log warn here received response but command cid not found

    @property
    def nid(self) -> str:
        return self._nid

    def __del__(self) -> None:
        try:
            self._connection.close()
        except RuntimeError:
            pass


def to_cmd_dict(o) -> dict:
    if isinstance(o, CommandBase):
        return o.to_dict(prevent_circular=True, filter=option_only_filter)
    elif isinstance(o, dict):
        return o
    raise TypeError(f"Type of {o} neither CommandBase nor dict")


class CommandWrapper(CommandBase[NoResponseType]):
    nid: str = cfg.Option(required=True, type=str)
    cid: str = cfg.Option(required=True, type=str)
    cmd: dict = cfg.Option(required=True, type=to_cmd_dict)

    __cmd_name__ = "__cmd_wrapper__"

    def execute(self, ctx: Node) -> NoResponseType:
        response = self.get_cmd(ctx).execute(ctx)
        if not isinstance(response, NoResponseType):
            ctx.send_no_response(SetResponse(cid=self.cid, response=response), [self.nid])
        return NoResponse

    @lru_cache
    def get_cmd(self, node: Node) -> CommandBase:
        s = node.get_cmd_context(self.__cmd_name__)
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


class SetResponse(CommandBase[NoResponseType]):
    cid: str = cfg.Option(required=True, type=str)
    response: Any = cfg.Option(required=True)

    __cmd_name__ = '__cmd_set_response__'

    def execute(self, ctx: Node) -> NoResponseType:
        ctx.set_response(self.cid, self.response)
        return NoResponse
