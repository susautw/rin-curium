from abc import ABC, abstractmethod
from typing import TypeVar, Generic, TYPE_CHECKING

from fancy import config as cfg

if TYPE_CHECKING:
    from . import Node

T = TypeVar("T")


class CommandBase(cfg.BaseConfig, Generic[T], ABC):

    __cmd_name__ = "command_base"

    def __init_subclass__(cls, **kwargs):
        if "__cmd_name__" not in vars(cls):
            setattr(cls, "__cmd_name__", cls.__name__)

        __cmd_name__ = getattr(cls, "__cmd_name__")
        __cmd_name_option__ = cfg.Lazy(lambda c: __cmd_name__, name="__cmd_name__")
        __cmd_name_option__.__name__ = "__cmd_name_option__"
        cls.__cmd_name_option__ = __cmd_name_option__

    @abstractmethod
    def execute(self, ctx: "Node") -> T: ...
