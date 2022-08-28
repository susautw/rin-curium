from abc import ABC, abstractmethod
from typing import Type, Union

from .command_base import CommandBase


class ISerializer(ABC):
    @abstractmethod
    def serialize(self, cmd: CommandBase) -> bytes:
        """
        Serialize command into bytes.

        :param cmd: command to be serialized
        :return: raw bytes in bytes
        :raises exc.UnsupportedObjectError: unsupported objects appear in the command object
        """

    @abstractmethod
    def deserialize(self, raw_data: Union[bytes, dict]) -> CommandBase:
        """
        Deserialize raw data to command.

        :param raw_data: raw data to be deserialized
        :return: a command object
        :raises exc.InvalidFormatError: unrecognized raw data found while deserializing
        :raises exc.CommandNotRegisteredError: found a command that is not registered
        """

    @abstractmethod
    def register_cmd(self, cmd_type: Type[CommandBase]) -> None:
        """
        Register a command for deserialization.

        :param cmd_type: a command class
        :raises exc.CommandHasRegisteredError: registering a command that has registered
        """
