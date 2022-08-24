import logging
import time
from functools import lru_cache
from itertools import count
from threading import Lock, Thread
from typing import List, TypeVar, Optional, Type, Any, Dict, Callable, Union
from weakref import WeakValueDictionary

from fancy import config as cfg
from redis import Redis

from . import CommandBase, IConnection, ResponseHandlerBase, ISerializer
from .connections import RedisConnection
from .response_handles import BlockUntilAllReceived
from .serializers import JSONSerializer
from .utils import cmd_to_dict_filter, atomicmethod

R = TypeVar("R")


class NoResponseType:
    pass


NoResponse = NoResponseType()

NoContextSpecified = object()


class Node:
    _sent_cmd_response_handlers: WeakValueDictionary  # weak Dict[str, ResponseHandlerBase]
    check_response_handlers_interval: float
    _rh_lock: Lock

    _nid: str = None
    _connection: IConnection

    _serializer: ISerializer
    _cmd_count: count

    _cmd_contexts: Dict[str, Any]
    _cmd_contexts_lock: Lock

    def __init__(
            self,
            connection: Union[Redis, IConnection] = None,
            serializer: ISerializer = None,
            check_response_handlers_interval: float = 0.01
    ):
        if isinstance(connection, Redis) or connection is None:
            connection = RedisConnection(connection)
        self._connection = connection
        self._connection_lock = Lock()
        self._serializer = JSONSerializer() if serializer is None else serializer
        self._sent_cmd_response_handlers = WeakValueDictionary()
        self._rh_lock = Lock()
        self.check_response_handlers_interval = check_response_handlers_interval

        self._cmd_contexts = {}
        self._cmd_contexts_lock = Lock()
        self._cmd_count = count()

        self.register_cmd(CommandWrapper, self._serializer)
        self.register_cmd(SetResponse)

    def connect(self) -> None:
        if self._nid is not None:
            # TODO warn about connection already connected or closed
            return
        self._nid = self._connection.connect()
        Thread(target=self._check_response_handlers, daemon=True).start()
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
            destinations: Union[str, List[str]],
            response_handler: Optional[ResponseHandlerBase[R]] = None,
            response_timeout: float = None
    ) -> ResponseHandlerBase[R]:
        if isinstance(destinations, str):
            destinations = [destinations]

        cid = self._generate_cid()
        wrapped_cmd = CommandWrapper(nid=self._nid, cid=cid, cmd=cmd)
        rh = self._create_response_handler(response_handler, response_timeout)
        num_receivers = self.send_no_response(wrapped_cmd, destinations)
        rh.set_num_receivers(num_receivers)
        self._add_response_handler(cid, rh)
        return rh

    def send_no_response(self, cmd: CommandBase, destinations: Union[str, List[str]]) -> Optional[int]:
        if isinstance(destinations, str):
            destinations = [destinations]
        return self._connection.send(self._serializer.serialize(cmd), destinations)

    @atomicmethod
    def _generate_cid(self) -> str:
        return str(next(self._cmd_count))

    def _get_response_handler_by_cid(self, cid: str, default=None) -> Optional[ResponseHandlerBase]:
        with self._rh_lock:
            return self._sent_cmd_response_handlers.get(cid, default)

    def _add_response_handler(self, cid: str, rh: ResponseHandlerBase) -> None:
        with self._rh_lock:
            self._sent_cmd_response_handlers[cid] = rh

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

    def run_forever(self, sleep=0.5, close_connection=True) -> None:
        try:
            while True:
                cmd = self.recv(block=True, timeout=sleep)
                if cmd is not None:
                    print(cmd)  # TODO use logging.info instead
                    cmd.execute(self)
        finally:
            if close_connection:
                self._connection.close()

    def register_cmd(self, cmd_typ: Type[CommandBase], ctx: Any = NoContextSpecified) -> None:
        with self._cmd_contexts_lock:
            self._serializer.register_cmd(cmd_typ)
            if ctx is not NoContextSpecified:
                self._cmd_contexts[cmd_typ.__cmd_name__] = ctx

    def get_cmd_context(self, name: str) -> Any:
        with self._cmd_contexts_lock:
            return self._cmd_contexts[name]

    def set_response(self, cid: str, response: Any) -> None:
        rh = self._get_response_handler_by_cid(cid)
        if rh is not None:
            rh.set_response(response)
        else:
            response_str = f'{response}'
            if len(response_str) > 50:
                response_str = response_str[:50] + '...'
            logging.warning(f"Received response {response_str}, but command {cid} not found")

    def _check_response_handlers(self):
        while True:
            with self._rh_lock:
                rhs = self._sent_cmd_response_handlers.copy()
            for rh in rhs.values():
                rh.finalize()
            time.sleep(self.check_response_handlers_interval)

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
        return o.to_dict(prevent_circular=True, filter=cmd_to_dict_filter)
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
            ctx.send_no_response(SetResponse(cid=self.cid, response=response), self.nid)
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
        return super().to_dict(recursive=False, prevent_circular=True, filter=filter)


class SetResponse(CommandBase[NoResponseType]):
    cid: str = cfg.Option(required=True, type=str)
    response: Any = cfg.Option(required=True)

    __cmd_name__ = '__cmd_set_response__'

    def execute(self, ctx: Node) -> NoResponseType:
        ctx.set_response(self.cid, self.response)
        return NoResponse
