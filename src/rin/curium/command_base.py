from abc import ABC, abstractmethod
from typing import TypeVar, Generic, TYPE_CHECKING

from . import cfg

if TYPE_CHECKING:
    from . import Node

T = TypeVar("T")


class CommandBase(cfg.BaseConfig, Generic[T], ABC):

    __cmd_name__ = "__cmd_command_base__"
    __cmd_autoname__ = "module"

    def __init_subclass__(cls, **kwargs):
        if "__cmd_name__" not in vars(cls):
            if cls.__cmd_autoname__ == "class":
                cls.__cmd_name__ = cls.__name__
            elif cls.__cmd_autoname__ == "module":
                cls.__cmd_name__ = f'{cls.__module__}.{cls.__name__}'
            else:
                raise ValueError("__cmd_autoname__ should be 'class' or 'module'."
                                 " If you intend to specify a command name, you should use __cmd_name__")

        __cmd_name__ = getattr(cls, "__cmd_name__")
        __cmd_name_option__ = cfg.Lazy(lambda c: __cmd_name__, name="__cmd_name__")
        __cmd_name_option__.__set_name__(cls, "__cmd_name_option__")
        cls.__cmd_name_option__ = __cmd_name_option__

    @abstractmethod
    def execute(self, ctx: "Node") -> T:
        """
        Execute this command.

        :param ctx: A Node that received this command.
        :return: result of this execution. :py:data:`~.NoResponse`
         represents this execution doesn't have response.
        """
