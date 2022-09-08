from typing import List, NoReturn

from rin.curium import CommandBase, Node, NoResponse, cfg


class MyCommand(CommandBase):
    __cmd_name__ = "my_command"
    x = cfg.Option()
    y: List[int] = cfg.Option(type=[int])
    z: float = cfg.Lazy(lambda c: c.x / 2)
    p: bool = cfg.PlaceHolder()

    def post_load(self):
        self.p = True

    def execute(self, ctx: "Node") -> None:
        pass


class AnotherCommand(CommandBase):
    __cmd_name__ = "my_command"

    def execute(self, ctx: "Node") -> None:
        pass


class ACommandRaisingError(CommandBase):

    def execute(self, ctx: Node) -> NoReturn:
        raise Exception("an Exception")


class ACommandDoNothing(CommandBase):
    def execute(self, ctx: "Node") -> NoResponse:
        return NoResponse
