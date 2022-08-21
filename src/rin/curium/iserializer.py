from abc import ABC, abstractmethod
from typing import Type

from .icommand import ICommand


class ISerializer(ABC):
    @abstractmethod
    def serialize(self, cmd: ICommand) -> bytes: ...

    @abstractmethod
    def deserialize(self, data: bytes) -> ICommand: ...

    @abstractmethod
    def register_cmd(self, cmd_type: Type[ICommand]): ...
