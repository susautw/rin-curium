from abc import ABC, abstractmethod
from threading import Event
from typing import Generic, TypeVar, List, Tuple, final, Optional

T = TypeVar("T")


class ResponseHandlerBase(Generic[T], ABC):
    num_receivers: Optional[int] = None

    _results: List[T]
    _finalized: Event

    def __init__(self):
        self._results = []
        self._finalized = Event()

    def set_response(self, response: T) -> None:
        self._results.append(response)

    def set_num_receivers(self, num_receivers: Optional[int]) -> None:
        self.num_receivers = num_receivers

    @final
    def finalize(self) -> bool:
        if self.finalize_internal():
            self._finalized.set()
            return True
        return False

    @abstractmethod
    def finalize_internal(self) -> bool: ...

    @final
    def get(self, block=True, timeout=None) -> Tuple[bool, T]:
        if block:
            ret = self._finalized.wait(timeout)
        else:
            ret = self._finalized.is_set()
        return ret, self._results
