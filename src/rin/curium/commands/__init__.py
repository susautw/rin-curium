from .get_node_infos import GetNodeInfos
from .add_response import AddResponse
from .command_wrapper import CommandWrapper

#: default_commands is a list of commands automatically registered into a :class:`.Node`.
#:  :class:`.CommandWrapper` and :class:`.AddResponse` are also considered
#:  default_commands. The Node will register the two classes when construction.
default_commands = [
    (GetNodeInfos, ),
]
