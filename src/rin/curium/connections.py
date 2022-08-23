import time
import uuid
from threading import Thread, Event
from typing import Optional, List

from redis import Redis
from redis.client import PubSub

from . import IConnection

INTERNAL_TIMEOUT = 1


class RedisConnection(IConnection):
    _redis: Redis
    _pubsub: Optional[PubSub]
    _namespace: str
    _expire: int
    _uid_key: Optional[str] = None

    _refresh_thread: Optional[Thread] = None
    _refresh_thread_close: Event

    def __init__(self, redis: Redis = None, namespace: str = "curium", expire: int = 600) -> None:
        self._redis = Redis() if redis is None else redis
        self._namespace = namespace
        self._expire = expire
        self._pubsub = None
        self._refresh_thread_close = Event()

    def connect(self) -> str:
        self._pubsub = self._redis.pubsub(ignore_subscribe_messages=True)
        while True:
            uid = str(uuid.uuid4())
            uid_key = f'{self._namespace}:{uid}'
            uid_code, _ = self._redis.pipeline().incr(uid_key).expire(uid_key, self._expire, nx=True).execute()
            if uid_code == 1:
                break
        self._refresh_thread = Thread(target=self._refresh_uid, daemon=True)
        self._refresh_thread_close.clear()
        self._refresh_thread.start()
        self._uid_key = uid_key
        return uid

    def _refresh_uid(self) -> None:
        while not self._refresh_thread_close.wait(0):
            self._redis.setex(self._uid_key, self._expire, 1)
            time.sleep(1)

    def close(self) -> None:
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
        if 'all' in destinations and len(destinations) > 1:
            # TODO warn about duplicated destinations 'all'
            destinations = ['all']
        return self._redis.publish('|' + '|'.join(destinations) + '|', data)

    def recv(self, block=True, timeout: float = None) -> Optional[bytes]:
        self.verify_connected()
        if block:
            if timeout is None:
                internal_timeout = INTERNAL_TIMEOUT
            else:
                internal_timeout = timeout
        else:
            internal_timeout = 0
            timeout = 0

        while True:
            message_pack = self._pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=internal_timeout
            )
            if message_pack is None:
                if timeout is not None:
                    return None
            else:
                break
        # TODO estimate duplicated message
        return message_pack['data']

    def verify_connected(self) -> None:
        if self._pubsub is None:
            raise RuntimeError("operation before connect")
