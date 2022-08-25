from abc import ABC, abstractmethod
from threading import Event, Lock
from typing import Generic, TypeVar, List, final, Optional

T = TypeVar("T")


class ResponseHandlerBase(Generic[T], ABC):
    num_receivers: Optional[int] = None

    _results: List[T]
    _results_lock: Lock
    _finalized: Event

    def __init__(self):
        self._results = []
        self._results_lock = Lock()
        self._finalized = Event()

    def set_response(self, response: T) -> None:
        with self._results_lock:
            self._results.append(response)

    @property
    def num_received_results(self) -> int:
        with self._results_lock:
            return len(self._results)

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
    def get(self, block=True, timeout=None) -> List[T]:
        if block:
            self._finalized.wait(timeout)
        with self._results_lock:
            return self._results

    @property
    def is_finalized(self) -> bool:
        return self._finalized.wait(0)
