import warnings
from abc import ABC, abstractmethod
from collections import deque
from threading import Event, Lock, Semaphore
from typing import TypeVar, List, final, Optional, Iterator

T = TypeVar("T")


class ResponseHandlerBase(Iterator[T], ABC):
    num_receivers: Optional[int] = None
    _num_received_results: int

    _results: deque
    _results_lock: Lock
    _finalized: Event
    _iter_semaphore: Semaphore
    _iter_timeout: float

    _is_next_executed: Event

    def __init__(self, iter_timeout=0.01):
        self._results = deque()
        self._num_received_results = 0
        self._iter_semaphore = Semaphore(value=0)
        self._iter_timeout = iter_timeout
        self._results_lock = Lock()
        self._finalized = Event()
        self._is_next_executed = Event()

    def add_response(self, response: T) -> None:
        with self._results_lock:
            self._results.append(response)
            self._iter_semaphore.release()
            self._num_received_results += 1

    @property
    def num_received_results(self) -> int:
        with self._results_lock:
            return self._num_received_results

    def set_num_receivers(self, num_receivers: Optional[int]) -> None:
        self.num_receivers = num_receivers

    @final
    def finalize(self) -> bool:
        if self.finalize_internal():
            self._finalized.set()
            return True
        return False

    @abstractmethod
    def finalize_internal(self) -> bool:
        ...

    @final
    def get(self, block=True, timeout=None) -> Optional[List[T]]:
        if self._is_next_executed.wait(0):
            self.__warn_may_get_unexpected_results()
        if not block:
            timeout = 0
        if not self._finalized.wait(timeout):
            return None
        with self._results_lock:
            return list(self._results)

    def __next__(self) -> T:
        self._is_next_executed.set()
        self.__should_stop_iteration()
        while not self._iter_semaphore.acquire(timeout=self._iter_timeout):
            self.__should_stop_iteration()
        with self._results_lock:
            return self._results.popleft()

    def __should_stop_iteration(self):
        with self._results_lock:
            num_results = len(self._results)
        if self.is_finalized and num_results == 0:
            raise StopIteration()

    @property
    def is_finalized(self) -> bool:
        return self._finalized.wait(0)

    def __warn_may_get_unexpected_results(self) -> None:
        warnings.warn("method get may get unexpected results because __next__ has been called. "
                      "method __next__ will remove the buffering results.", category=RuntimeWarning, stacklevel=3)
