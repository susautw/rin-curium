from abc import ABC, abstractmethod

from typing import List, Optional


class IConnection(ABC):
    @abstractmethod
    def connect(self) -> str:
        """
        Connect to the backend server.

        :return: A unique id for node
        :raises exc.ConnectionFailedError: fail to connect.
        """

    @abstractmethod
    def reconnect(self) -> None:
        """
        Try to reconnect to the backend server using the unique id that was got previously.

        :raises exc.ConnectionFailedError: fail to reconnect
        :raises exc.NotConnectedError: no previous connection found.
        """

    @abstractmethod
    def close(self) -> None:
        """
        Disconnect from the backend server and clean up internal state.

        NOTE: No reaction when you invoke this method and the server was not connected.
        """

    @abstractmethod
    def join(self, name: str) -> None:
        """
        Join a channel with the given name.

        :param name: channel name
        :raises exc.NotConnectedError: the backend server is not connected.
        :raises exc.InvalidChannelError: channel is not available.
        :raises exc.ServerDisconnectedError: server disconnect during the invocation
        """

    @abstractmethod
    def leave(self, name: str) -> None:
        """
        Leave a channel with the given name.

        :param name: channel name
        :raises exc.NotConnectedError: the backend server is not connected.
        :raises exc.InvalidChannelError: channel is not available.
        :raises exc.ServerDisconnectedError: server disconnect during the invocation
        """

    @abstractmethod
    def send(self, data: bytes, destinations: List[str]) -> Optional[int]:
        """
        Send data to given destinations on the backend server.

        :param data: data to be sent
        :param destinations: list of channel names represent destinations
        :return: number of node that received, None presents unknown
        :raises exc.InvalidChannelError: channel is not available.
        :raises exc.NotConnectedError: the backend server is not connected.
        :raises exc.ServerDisconnectedError: server disconnect during the invocation
        """

    @abstractmethod
    def recv(self, block=True, timeout: float = None) -> Optional[bytes]:
        """
        Receive data from the backend server.

        :param block: is blocking or not
        :param timeout: timeout of this operation in second, None presents forever
        :return: received data.
                 None presents no data received
        :raises exc.NotConnectedError: the backend server is not connected.
        :raises exc.ServerDisconnectedError: server disconnect during the invocation
        """
