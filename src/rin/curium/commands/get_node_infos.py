from typing import TYPE_CHECKING

from .. import CommandBase

if TYPE_CHECKING:
    from .. import Node


class GetNodeInfos(CommandBase):
    def execute(self, ctx: "Node"):
        return {'nid': ctx.nid, "num_response_handlers": ctx.num_response_handlers}
