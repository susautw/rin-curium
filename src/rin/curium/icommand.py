from abc import ABC, abstractmethod
from typing import TypeVar, Generic

T = TypeVar("T")


# TODO Serializable Command with fancy-config
class ICommand(Generic[T], ABC):
    @abstractmethod
    def execute(self, ctx) -> T: ...
