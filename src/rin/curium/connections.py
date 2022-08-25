import time
import uuid
import warnings
from threading import Thread, Event, Lock
from typing import Optional, List

from redis import Redis, exceptions
from redis.client import PubSub

from . import IConnection

INTERNAL_TIMEOUT = 1


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

    def __init__(self, redis: Redis = None, namespace: str = "curium", expire: int = 600) -> None:
        self._redis = Redis() if redis is None else redis
        self._namespace = namespace
        self._expire = expire
        self._pubsub = None
        self._refresh_thread_close = Event()
        self._connecting_operation_lock = Lock()

    def connect(self) -> str:
        with self._connecting_operation_lock:
            if self._pubsub is not None:
                warnings.warn(f"Already connected. uid: {self._uid}", category=RuntimeWarning)
                return self._uid
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

    def _refresh_uid(self) -> None:
        while not self._refresh_thread_close.wait(0):
            self._redis.setex(self._uid_key, self._expire, 1)
            time.sleep(1)

    def close(self) -> None:
        with self._connecting_operation_lock:
            self.verify_connected()
            self._pubsub.close()
            self._pubsub = None
            self._refresh_thread_close.set()
            self._refresh_thread.join(timeout=2)
            self._refresh_thread = None

            self._redis.delete(self._uid_key)

    def join(self, name: str) -> None:
        self._verify_name(name)
        self.verify_connected()
        self._pubsub.psubscribe(f"*|{name}|*")

    def leave(self, name: str) -> None:
        self._verify_name(name)
        self.verify_connected()
        self._pubsub.punsubscribe(f"*|{name}|*")

    def _verify_name(self, name: str) -> None:
        if "|" in name:
            raise ValueError("character '|' shouldn't appear in channel name")

    def send(self, data: bytes, destinations: List[str]) -> Optional[int]:
        self.verify_connected()
        return self._redis.publish('|' + '|'.join(destinations) + '|', data)

    def recv(self, block=True, timeout: float = None) -> Optional[bytes]:
        self.verify_connected()
        if not block:
            timeout = 0
        while True:
            try:
                message_pack = self._pubsub.handle_message(
                    self._pubsub.parse_response(False, timeout)
                )
            except exceptions.ConnectionError:  # connection may close while blocking
                return None
            if message_pack is None:
                if not block:
                    return None
            else:
                break
        return message_pack['data']

    def verify_connected(self) -> None:
        if self._pubsub is None:
            raise RuntimeError("operation before connect")
