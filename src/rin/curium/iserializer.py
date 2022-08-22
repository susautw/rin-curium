from abc import ABC, abstractmethod
from typing import Type, Union

from .command_base import CommandBase


class ISerializer(ABC):
    @abstractmethod
    def serialize(self, cmd: CommandBase) -> bytes: ...

    @abstractmethod
    def deserialize(self, raw_data: Union[bytes, dict]) -> CommandBase: ...

    @abstractmethod
    def register_cmd(self, cmd_type: Type[CommandBase]): ...
