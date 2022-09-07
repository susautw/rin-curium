import warnings
from functools import lru_cache
from typing import Callable, TYPE_CHECKING

from fancy import config as cfg

from rin.curium import CommandBase, NoResponseType, NoResponse, ISerializer
from rin.curium.utils import cmd_to_dict_filter
from .add_response import AddResponse

if TYPE_CHECKING:
    from .. import Node


def _to_cmd_dict(o) -> dict:
    if isinstance(o, CommandBase):
        return o.to_dict(prevent_circular=True, filter=cmd_to_dict_filter)
    elif isinstance(o, dict):
        return o
    raise TypeError(f"Type of {o} neither CommandBase nor dict")


class CommandWrapper(CommandBase[NoResponseType]):
    nid: str = cfg.Option(required=True, type=str)
    cid: str = cfg.Option(required=True, type=str)
    cmd: dict = cfg.Option(required=True, type=_to_cmd_dict)

    __cmd_name__ = "__cmd_wrapper__"

    def execute(self, ctx: "Node") -> NoResponseType:
        cmd = self.get_cmd(ctx)
        response = cmd.execute(ctx)
        if not isinstance(response, NoResponseType):
            if self.nid == ctx.nid:
                ctx.add_response(self.cid, response)
            else:
                ctx.send_no_response(AddResponse(cid=self.cid, response=response), self.nid)
        return NoResponse

    @lru_cache
    def get_cmd(self, node: "Node") -> CommandBase:
        s = node.get_cmd_context(self.__cmd_name__)
        assert isinstance(s, ISerializer)
        return s.deserialize(self.cmd)

    # noinspection PyShadowingBuiltins
    def to_dict(
            self,
            recursive=True,
            prevent_circular=True, *,
            load_lazies=None,
            filter: Callable[[cfg.PlaceHolder], bool] = None
    ) -> dict:
        """
        Convert this command wrapper to a :class:`dict`.
        """
        if not recursive or not prevent_circular:
            warnings.warn("param recursive and prevent_circular are always True in CommandWrapper. "
                          "set these param will not affect anything.", category=UserWarning)
        # turn off recursive because the attribute `cmd` has been converted before.
        return super().to_dict(recursive=False, prevent_circular=True, filter=filter)
