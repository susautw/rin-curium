from abc import ABC, abstractmethod

from typing import List, Tuple, Optional


class IConnection(ABC):
    @abstractmethod
    def connect(self) -> str:
        """
        :return: node uid
        """

    @abstractmethod
    def close(self) -> None: ...

    @abstractmethod
    def join(self, name: str) -> None: ...

    @abstractmethod
    def leave(self, name: str) -> None: ...

    @abstractmethod
    def send(self, data: bytes, destinations: List[str]) -> Optional[int]:
        """

        :param data: data to be sent
        :param destinations: list of names present destinations
        :return: number of node that received, None presents unknown
        """

    @abstractmethod
    def recv(self, block, timeout: float = 0) -> Optional[bytes]:
        """
        :param block: is blocking or not
        :param timeout: timeout of this operation in second, 0 presents forever
        :return: received data.
                 None presents no data received
        """
