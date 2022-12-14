from typing import Any, TYPE_CHECKING

from .. import CommandBase, NoResponseType, NoResponse, cfg

if TYPE_CHECKING:
    from .. import Node


class AddResponse(CommandBase[NoResponseType]):
    cid: str = cfg.Option(required=True, type=str)
    response: Any = cfg.Option(required=True)

    __cmd_name__ = '__cmd_add_response__'

    def execute(self, ctx: "Node") -> NoResponseType:
        ctx.add_response(self.cid, self.response)
        return NoResponse
