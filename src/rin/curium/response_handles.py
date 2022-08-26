import time
import warnings
from typing import TypeVar

from . import ResponseHandlerBase

T = TypeVar("T")


class BlockUntilAllReceived(ResponseHandlerBase[T]):
    def __init__(self, timeout: float = None):
        if timeout is None:
            warnings.warn("No timeout specified may cause thread blocking forever", category=RuntimeWarning)
        super().__init__()
        self.timeout_at = timeout
        if timeout is not None:
            self.timeout_at += time.time()

    def finalize_internal(self) -> bool:
        if self.num_receivers is None and self.timeout_at is None:
            warnings.warn("This response handler has DROPPED:  "
                          "There is no number of received results or timeout provided. "
                          "The issue will cause the thread to block forever. ", category=RuntimeWarning)
            return True
        return self.num_received_results >= self.num_receivers or (
                self.timeout_at is not None and time.time() > self.timeout_at
        )


class UpdateTimeoutPerReceive(BlockUntilAllReceived[T]):

    def __init__(self, timeout: float):
        super().__init__(timeout)
        self.timeout = timeout

    def add_response(self, response: T) -> None:
        super().add_response(response)
        self.timeout_at = time.time() + self.timeout
