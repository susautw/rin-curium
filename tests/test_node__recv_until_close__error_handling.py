from unittest.mock import MagicMock

import pytest
from fakeredis import FakeRedis

from rin.curium import Node, CommandBase, RedisConnection
from rin.curium.node import error_logging


class ACommandRaisingError(CommandBase):

    def execute(self, ctx: Node) -> None:
        ctx.close()
        raise Exception("an Exception")


@pytest.mark.slow
def test_node__recv_until_close__error_handling():
    node = Node(RedisConnection(FakeRedis(), ping_while_sending=False))
    node.register_cmd(ACommandRaisingError)
    node.connect()
    cmd = ACommandRaisingError({})
    node.send_no_response(cmd, node.nid)
    mock = MagicMock()
    # noinspection PyTypeChecker
    node.recv_until_close(error_handler=mock)
    mock.assert_called_once()
    assert mock.call_args.args[0].to_dict() == cmd.to_dict()
    assert str(mock.call_args.args[1]) == "an Exception"


def test_node__error_logging(mocker):
    """
    Test the default error handler for :meth:`Node.recv_until_close`
    """
    from rin.curium import logger
    mock_exception = mocker.patch.object(logger, "exception")
    cmd = MagicMock(spec=CommandBase)
    exc_ = Exception()
    error_logging(cmd, exc_)
    mock_exception.assert_called_once_with(
        f"An Exception raised in the command execution for {cmd}",
        exc_info=exc_
    )
