from .. import CommandBase, Node


class GetNodeInfos(CommandBase):
    __cmd_name__ = "get_node_infos"

    def execute(self, ctx: Node):
        return {'nid': ctx.nid}
