import functools
import os
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, Future
from contextlib import ExitStack
from itertools import count
from threading import Lock, Thread, Event
from typing import TypeVar, Optional, Type, Any, Dict, Callable, Union, overload, Iterable
from weakref import WeakValueDictionary

from redis import Redis

from . import CommandBase, IConnection, ResponseHandlerBase, ISerializer, logger, exc, NoContextSpecified
from .commands import AddResponse, CommandWrapper
from .connections import RedisConnection
from . import response_handlers
from .serializers import JSONSerializer
from .utils import atomicmethod

R = TypeVar("R")


def error_logging(e: exc.CommandExecutionError) -> None:
    """
    Default error handler for :meth:`Node.recv_until_close`

    :param e: An Exception contains the failed command and the occurred exception.
    """
    logger.exception(
        f"An Exception raised in the command execution: {e.cmd}",
        exc_info=e.e
    )


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
        """
        Connect to the backend server, join ``own`` and ``all`` channels, then start to check response handlers.

        :param send_only: don't join the ``all`` channel.
        :raises ~exc.ConnectionFailedError: fail to connect.
        """
        if self._nid is not None:
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
        """
        Disconnect from the the backend server then terminate threads started by this node.

        .. warning:: A node cannot reuse after close
        """
        if not self._closed_event.wait(0):
            self._closed_event.set()
            self._connection.close()

    def join(self, name: str) -> None:
        """
        Join a channel with the given name.

        :param name: channel name
        :raises ~exc.NotConnectedError: the backend server is not connected.
        :raises ~exc.InvalidChannelError: channel is not available.
        :raises ~exc.ServerDisconnectedError: server disconnect during the invocation
        """
        self._connection.join(name)

    def leave(self, name: str) -> None:
        """
        Leave a channel with the given name.

        :param name: channel name
        :raises ~exc.NotConnectedError: the backend server is not connected.
        :raises ~exc.InvalidChannelError: channel is not available.
        :raises ~exc.ServerDisconnectedError: server disconnect during the invocation
        """
        self._connection.leave(name)

    def send(
            self,
            cmd: CommandBase[R],
            destinations: Union[str, Iterable[str]],
            response_handler: Optional[ResponseHandlerBase[R]] = None,
            response_timeout: float = None
    ) -> ResponseHandlerBase[R]:
        """
        Send command to the given destinations.

        .. note:: This method is intended to get responses from destination nodes.
           Any command sent by this method will be wrapped by :class:`CommandWrapper` to handle the response.
           If you aren't interest in responses, please use :meth:`Node.send_no_response`.

        :param cmd: command to be sent
        :param destinations: list of channel names represent `destinations`
        :param response_handler: a response handler
        :param response_timeout: automatically create a response handler with the given `response_timeout`
        :return: A :class:`.ResponseHandlerBase` to get results
        :raises ValueError: both `response_handler` and `response_timeout` are specified.
        :raises ~exc.InvalidChannelError: channel is not available.
        :raises ~exc.NotConnectedError: the backend server is not connected.
        :raises ~exc.ServerDisconnectedError: server disconnect during the invocation
        :raises ~exc.UnsupportedObjectError: unsupported objects appear in the command object
        """
        cid = self._generate_cid()
        wrapped_cmd = CommandWrapper(nid=self._nid, cid=cid, cmd=cmd)
        rh = self._create_response_handler(response_handler, response_timeout)
        num_receivers = self.send_no_response(wrapped_cmd, destinations)
        rh.set_num_receivers(num_receivers)
        self._add_response_handler(cid, rh)
        return rh

    def send_no_response(self, cmd: CommandBase, destinations: Union[str, Iterable[str]]) -> Optional[int]:
        """
        Send command to the given destinations without wrapping the command.

        :param cmd: command to be sent
        :param destinations: list of channel names represent destinations
        :return: numeral of received, None presents unknown
        :raises ~exc.InvalidChannelError: channel is not available.
        :raises ~exc.NotConnectedError: the backend server is not connected.
        :raises ~exc.ServerDisconnectedError: server disconnect during the invocation
        :raises ~exc.UnsupportedObjectError: unsupported objects appear in the command object
        """
        if isinstance(destinations, str):
            destinations = [destinations]
        if 'all' in destinations and len(destinations) > 1:
            warnings.warn(f"Destinations {destinations} has been reduced to ['all']."
                          f" To eliminate duplicated command", category=RuntimeWarning)
            destinations = ['all']
        destinations = set(destinations)
        num_receivers = self._connection.send(self._serializer.serialize(cmd), destinations)
        logger.info(f"send command: {cmd}")
        return num_receivers

    @atomicmethod
    def _generate_cid(self) -> str:
        return str(next(self._cmd_count))

    def _create_response_handler(
            self,
            response_handler: Optional[ResponseHandlerBase[R]],
            response_timeout: Optional[float]
    ) -> ResponseHandlerBase[R]:
        if response_handler is None:
            return response_handlers.BlockUntilAllReceived(timeout=response_timeout)
        elif response_timeout is not None:
            raise ValueError("cannot set both response_handler and response_timeout")
        return response_handler

    def _get_response_handler(self, cid: str, default=None) -> Optional[ResponseHandlerBase]:
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

    def recv(self, block=True, timeout=None) -> Optional[CommandBase]:
        """
        Receive a command from the backend server.

        :param block: is blocking or not
        :param timeout: timeout of this operation
        :return: :class:`.CommandBase` or ``None``, ``None`` present no command received
        :raises ~exc.NotConnectedError: the backend server is not connected.
        :raises ~exc.ServerDisconnectedError: server disconnect during the invocation
        :raises ~exc.InvalidFormatError: unrecognized raw data found while deserializing
        :raises ~exc.CommandNotRegisteredError: found a command that is not registered
        """
        raw_data = self._connection.recv(block, timeout)
        if raw_data is None:
            return None
        cmd = self._serializer.deserialize(raw_data)
        return cmd

    def register_cmd(self, cmd_typ: Type[CommandBase], ctx: Any = NoContextSpecified) -> None:
        """
        Register a command

        :param cmd_typ: a class based on :class:`.CommandBase`
        :param ctx: context for command execution. :data:`NoContextSpecified` presents no context specified.
        :raises ~exc.CommandHasRegisteredError: registering a command that has registered
        """
        with self._cmd_contexts_lock:
            self._serializer.register_cmd(cmd_typ)
            if ctx is not NoContextSpecified:
                self._cmd_contexts[cmd_typ.__cmd_name__] = ctx

    # @formatter:off
    @overload
    def get_cmd_context(self, cmd_typ: Type[CommandBase]) -> Any: ...
    @overload
    def get_cmd_context(self, name: str) -> Any: ...

    def get_cmd_context(self, key) -> Any:
        """
        Get command execution context by the command class or its name.

        :raises TypeError: wrong type argument was specified.
        :raises KeyError: context was not found in node
        """
        if isinstance(key, str):
            name = key
        elif isinstance(key, type) and issubclass(key, CommandBase):
            name = key.__cmd_name__
        else:
            raise TypeError(f"No signature matched to execute this method")
        with self._cmd_contexts_lock:
            return self._cmd_contexts[name]
    # @formatter:on

    def add_response(self, cid: str, response: Any) -> None:
        """
        Add a response to be handled

        :param cid: command id
        :param response: response contents
        """
        rh = self._get_response_handler(cid)
        if rh is not None:
            rh.add_response(response)
        else:
            response_str = f'{response}'
            logger.warning(f"Received response {response_str}, but command {cid} not found")

    def recv_until_close(
            self,
            sleep: float = 0.5,
            num_workers: int = None,
            close_when_exit: bool = True,
            reconnect_max_tries: int = 10,
            reconnect_interval: float = 10,
            error_handler: Callable[[exc.CommandExecutionError], None] = error_logging
    ) -> None:
        """
        Receive and execute commands until this node is closed.

        .. note:: The received command will be executed in a thread pool.

        :param sleep: max waiting time for a command
        :param num_workers: max number of workers for the internal
           :class:`~concurrent.futures.ThreadPoolExecutor`
        :param close_when_exit: close this node when this method has exited in
           any situation. For example, a :exc:`KeyboardInterrupt` has been
           raised.
        :param reconnect_max_tries: the max number of times to reconnect
        :param reconnect_interval: the interval between reconnects
        :param error_handler: a callable handles exceptions raised by commands
        """
        if num_workers is None:
            num_workers = max(os.cpu_count(), 3)
        with ExitStack() as stack:
            executor = stack.enter_context(ThreadPoolExecutor(max_workers=num_workers))
            if close_when_exit:
                stack.enter_context(self)

            while not self._closed_event.wait(0):
                try:
                    cmd = self.recv(block=True, timeout=sleep)
                    if cmd is not None:
                        logger.info(f"received command: {cmd}")
                        result = executor.submit(cmd.execute, self)
                        result.add_done_callback(
                            self.__create_result_error_handler(cmd, error_handler)
                        )
                except exc.CuriumConnectionError:
                    if self._closed_event.wait(0):  # connection closed while blocking
                        break
                    self.__reconnect_to_backend(reconnect_max_tries, reconnect_interval)
            logger.info("connection closed")

    def __create_result_error_handler(
            self,
            cmd: CommandBase,
            error_handler: Callable[[exc.CommandExecutionError], None]
    ):
        @functools.wraps(error_handler)
        def wrapped_error_handler(future: Future) -> None:
            exception = future.exception()
            if exception is not None:
                try:
                    raise exc.CommandExecutionError(cmd, exception)
                except exc.CommandExecutionError as e:
                    error_handler(e)

        return wrapped_error_handler

    def __reconnect_to_backend(self, reconnect_max_tries: int, reconnect_interval: float):
        last_err = None
        for i in range(reconnect_max_tries):
            try:
                logger.warning(f"Reconnecting to the backend server: {i}")
                self._connection.reconnect()
                logger.warning(f"Server reconnected")
                return
            except exc.ConnectionFailedError as e:
                last_err = e
                time.sleep(reconnect_interval)
        raise exc.ServerDisconnectedError(last_err)

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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self) -> None:
        self.close()
