import logging
import os
import time
import warnings
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from itertools import count
from threading import Lock, Thread, Event
from typing import List, TypeVar, Optional, Type, Any, Dict, Callable, Union
from weakref import WeakValueDictionary

from fancy import config as cfg
from redis import Redis

from . import CommandBase, IConnection, ResponseHandlerBase, ISerializer, logger
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
    _check_response_handlers_interval: float
    _check_response_handlers_thread: Thread = None
    _rh_lock: Lock

    _nid: Optional[str] = None
    _connection: IConnection

    _serializer: ISerializer
    _cmd_count: count

    _cmd_contexts: Dict[str, Any]
    _cmd_contexts_lock: Lock

    _closed_event: Event

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
        self._check_response_handlers_interval = check_response_handlers_interval
        self._cmd_contexts = {}
        self._cmd_contexts_lock = Lock()
        self._cmd_count = count()
        self._closed_event = Event()

        self._register_default_commands()

    def _register_default_commands(self) -> None:
        from .commands import default_commands
        self.register_cmd(CommandWrapper, self._serializer)
        self.register_cmd(AddResponse)

        for command_args in default_commands:
            self.register_cmd(*command_args)

    def connect(self, send_only=False) -> None:
        if self._nid is not None:
            # TODO reset when close
            logger.warning("connection already connected or closed")
            return
        self._nid = self._connection.connect()
        self.join(self._nid)
        if not send_only:
            self.join("all")
        self._check_response_handlers_thread = Thread(
            target=self._check_response_handlers, name="response_handler", daemon=True
        )
        self._check_response_handlers_thread.start()

    def close(self) -> None:
        if not self._closed_event.wait(0):
            self._closed_event.set()
            if self._check_response_handlers_thread is not None:
                self._check_response_handlers_thread.join(timeout=1)
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
        if 'all' in destinations and len(destinations) > 1:
            warnings.warn(f"Destinations {destinations} has been reduced to ['all']."
                          f" To eliminate duplicated command", category=RuntimeWarning)
            destinations = ['all']
        destinations = list(set(destinations))
        num_receivers = self._connection.send(self._serializer.serialize(cmd), destinations)
        logger.info(f"send command: {cmd}")
        return num_receivers

    @atomicmethod
    def _generate_cid(self) -> str:
        return str(next(self._cmd_count))

    def _get_response_handler_by_cid(self, cid: str, default=None) -> Optional[ResponseHandlerBase]:
        with self._rh_lock:
            return self._sent_cmd_response_handlers.get(cid, default)

    def _add_response_handler(self, cid: str, rh: ResponseHandlerBase) -> None:
        with self._rh_lock:
            self._sent_cmd_response_handlers[cid] = rh

    def _remove_response_handler(self, cid: str, silent=False) -> None:
        with self._rh_lock:
            try:
                del self._sent_cmd_response_handlers[cid]
            except KeyError:
                if not silent:
                    raise

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
        with self._cmd_contexts_lock:
            self._serializer.register_cmd(cmd_typ)
            if ctx is not NoContextSpecified:
                self._cmd_contexts[cmd_typ.__cmd_name__] = ctx

    def get_cmd_context(self, name: str) -> Any:
        with self._cmd_contexts_lock:
            return self._cmd_contexts[name]

    def add_response(self, cid: str, response: Any) -> None:
        rh = self._get_response_handler_by_cid(cid)
        if rh is not None:
            rh.add_response(response)
        else:
            response_str = f'{response}'
            if len(response_str) > 50:
                response_str = response_str[:50] + '...'
            logging.warning(f"Received response {response_str}, but command {cid} not found")

    def run_forever(self, sleep=0.5, num_workers=None, close_connection=True) -> None:
        try:
            if num_workers is None:
                num_workers = max(os.cpu_count(), 3)
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                while not self._closed_event.wait(0):
                    cmd = self.recv(block=True, timeout=sleep)
                    if cmd is not None:
                        logger.info(f"received command: {cmd}")
                        executor.submit(cmd.execute, self)
                logger.info("connection closed")
        finally:
            if close_connection:
                self._connection.close()

    def _check_response_handlers(self):
        while not self._closed_event.wait(0):
            with self._rh_lock:
                rhs = self._sent_cmd_response_handlers.copy()
            for cid, rh in rhs.items():
                if rh.finalize():
                    self._remove_response_handler(cid, silent=True)
            time.sleep(self._check_response_handlers_interval)

    @property
    def nid(self) -> str:
        return self._nid

    @property
    def num_response_handlers(self) -> int:
        with self._rh_lock:
            return len(self._sent_cmd_response_handlers)

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
        cmd = self.get_cmd(ctx)
        response = cmd.execute(ctx)
        if not isinstance(response, NoResponseType):
            if self.nid == ctx.nid:
                ctx.add_response(self.cid, response)
            else:
                ctx.send_no_response(AddResponse(cid=self.cid, response=response), self.nid)
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


class AddResponse(CommandBase[NoResponseType]):
    cid: str = cfg.Option(required=True, type=str)
    response: Any = cfg.Option(required=True)

    __cmd_name__ = '__cmd_add_response__'

    def execute(self, ctx: Node) -> NoResponseType:
        ctx.add_response(self.cid, self.response)
        return NoResponse
