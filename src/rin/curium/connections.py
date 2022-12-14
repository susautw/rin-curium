import time
import uuid
import warnings
from threading import Thread, Event, Lock
from typing import Optional, Iterable

from redis import Redis, exceptions
from redis.client import PubSub

from . import IConnection, logger, exc
from .utils import atomicmethod, add_error_handler


class RedisConnection(IConnection):
    _redis: Redis
    _pubsub: Optional[PubSub]
    _namespace: str
    _expire: int
    _uid_key: Optional[str] = None
    _uid: Optional[str] = None

    _refresh_thread: Optional[Thread] = None
    _refresh_thread_close: Event

    _connecting_operation_lock: Lock

    _send_timeout: Optional[float]
    _ping_while_sending: bool
    _send_ping_event: Event
    _ping_msg = b"curium-ping"

    def __init__(
            self,
            redis: Redis = None,
            namespace: str = "curium",
            expire: int = 120,
            send_timeout: float = None,
            ping_while_sending: bool = True
    ) -> None:
        self._redis = Redis() if redis is None else redis
        self._namespace = namespace
        self._expire = expire
        self._pubsub = None
        self._refresh_thread_close = Event()
        self._connecting_operation_lock = Lock()
        self._send_ping_event = Event()
        self._send_timeout = send_timeout
        self._ping_while_sending = ping_while_sending

    @add_error_handler(exceptions.ConnectionError, reraise_by=exc.ConnectionFailedError)
    def connect(self) -> str:
        with self._connecting_operation_lock:
            if self._pubsub is not None:
                warnings.warn(f"Already connected. uid: {self._uid}", category=RuntimeWarning, stacklevel=2)
                return self._uid
            self._redis.ping()  # check connected
            self._pubsub = self._redis.pubsub(ignore_subscribe_messages=True)
            while True:
                uid = str(uuid.uuid4())
                uid_key = f'{self._namespace}:{uid}'
                uid_code, _ = self._redis.pipeline().incr(uid_key).expire(uid_key, self._expire, nx=True).execute()
                if uid_code == 1:
                    break
            self._refresh_thread = Thread(target=self._refresh_uid, name="refresh_uid", daemon=True)
            self._refresh_thread_close.clear()
            self._uid_key = uid_key
            self._uid = uid
            self._refresh_thread.start()
            return uid

    @add_error_handler(exceptions.ConnectionError, reraise_by=exc.ConnectionFailedError)
    def reconnect(self) -> None:
        self._verify_connected()
        self._redis.set(self._uid_key, 1, get=True, ex=self._expire)

    def _refresh_uid(self) -> None:
        connected = True
        while not self._refresh_thread_close.wait(0):
            try:
                self._redis.setex(self._uid_key, self._expire, 1)
                if not connected:
                    connected = True
                    logger.warning("Server reconnected")
            except exceptions.ConnectionError:
                if connected:
                    logger.warning("Server disconnected")
                    connected = False

            time.sleep(1)

    @add_error_handler(exceptions.ConnectionError, suppress=True)
    def close(self) -> None:
        with self._connecting_operation_lock:
            if self._pubsub is not None:
                self._pubsub.close()
                self._pubsub = None
                self._refresh_thread_close.set()
                self._refresh_thread = None

                self._redis.delete(self._uid_key)

    @add_error_handler(exceptions.ConnectionError, reraise_by=exc.ServerDisconnectedError)
    def join(self, name: str) -> None:
        self._verify_name(name)
        self._verify_connected()
        self._pubsub.psubscribe(f"*|{name}|*")

    @add_error_handler(exceptions.ConnectionError, reraise_by=exc.ServerDisconnectedError)
    def leave(self, name: str) -> None:
        self._verify_name(name)
        self._verify_connected()
        self._pubsub.punsubscribe(f"*|{name}|*")

    @atomicmethod
    @add_error_handler(exceptions.ConnectionError, reraise_by=exc.ServerDisconnectedError)
    def send(self, data: bytes, destinations: Iterable[str]) -> Optional[int]:
        if not destinations:
            logger.warning("no channel specified, this operation is cancelled.")
            return 0
        for channel_name in destinations:
            self._verify_name(channel_name)
        self._verify_connected()
        if self._ping_while_sending:
            # ensure connected
            self._send_ping_event.clear()
            self._pubsub.ping(self._ping_msg)
            if not self._send_ping_event.wait(self._send_timeout):
                raise exc.ServerDisconnectedError()
        return self._redis.publish('|' + '|'.join(destinations) + '|', data)

    def _verify_name(self, name: str) -> None:
        if "|" in name:
            raise exc.InvalidChannelError(f"character '|' shouldn't appear in channel name: {name}")

    @add_error_handler(exceptions.ConnectionError, reraise_by=exc.ServerDisconnectedError)
    def recv(self, block=True, timeout: float = None) -> Optional[bytes]:
        """
        Receive data from the backend server.

        .. note:: Implementation detail:
            In Redis 4.3.4, :meth:`Pubsub.parse_response` cannot set both `block` and `timeout`.
            In fact, invoking the method with ``(block=False, timeout=...)`` will block with the timeout,
            which we expected.
            And we can implement non-blocking with ``(block=False, timeout=0)``.

        :param block: is blocking or not
        :param timeout: timeout of this operation in second, None presents forever.
           this option is ignored if ``block`` is ``False``.
        :return: received data.
                 None presents no data received
        :raises ~exc.NotConnectedError: the backend server is not connected.
        :raises ~exc.ServerDisconnectedError: server disconnect during the invocation
        """
        self._verify_connected()

        # See the note in the docstring
        if not block:
            timeout = 0
        elif timeout is not None:
            block = False

        while True:
            pubsub = self._pubsub
            if pubsub is None:
                raise exc.ServerDisconnectedError()
            message_pack = pubsub.handle_message(
                pubsub.parse_response(block, timeout)
            )

            if message_pack is None:
                return None
            else:
                if message_pack['type'] == 'pmessage':
                    break
                if message_pack['type'] == "pong" and message_pack['data'] == self._ping_msg:
                    self._send_ping_event.set()
        return message_pack['data']

    def _verify_connected(self) -> None:
        if self._pubsub is None:
            raise exc.NotConnectedError("operation before connect")

    def set_send_time(self, timeout: float = None) -> None:
        self._send_timeout = timeout

    def set_ping_while_sending(self, val: bool) -> None:
        self._ping_while_sending = val
