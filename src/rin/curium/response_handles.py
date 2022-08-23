import time
from typing import TypeVar, Callable, Any

from . import ResponseHandlerBase

T = TypeVar("T")


class BlockUntilAllReceived(ResponseHandlerBase[T]):
    def __init__(self, timeout: float = None):
        # TODO warnings.warn or logging.warn about No Timeout Specified
        super().__init__()
        self.timeout_at = timeout
        if timeout is not None:
            self.timeout_at += time.time()

    def finalize_internal(self) -> bool:
        if self.num_received_results is None and self.timeout_at is None:
            # TODO warn about run forever
            return True  # this will run forever, so should return True to stop this
        return self.num_received_results >= self.num_receivers or time.time() > self.timeout_at


class Callback(BlockUntilAllReceived[T]):
    def __init__(self, callback: Callable[[Any], None], timeout: float = None):
        super().__init__(timeout)
        self.callback = callback

    def set_response(self, response: T) -> None:
        super().set_response(response)
        self.callback(response)


class UpdateTimeoutPerReceive(BlockUntilAllReceived[T]):

    def __init__(self, timeout: float):
        super().__init__(timeout)
        self.timeout = timeout

    def set_response(self, response: T) -> None:
        super().set_response(response)
        self.timeout_at = time.time() + self.timeout
