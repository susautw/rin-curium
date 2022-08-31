from typing import NoReturn
from unittest.mock import MagicMock

from fakeredis import FakeRedis

from rin.curium import Node, CommandBase, RedisConnection
from rin.curium.node import error_logging, NoResponse


class ACommandRaisingError(CommandBase):

    def execute(self, ctx: Node) -> NoReturn:
        raise Exception("an Exception")


class ACommandDoNothing(CommandBase):
    def execute(self, ctx: "Node") -> NoResponse:
        return NoResponse


def test_node__recv_until_close__error_handling(mocker):
    node = Node(RedisConnection(FakeRedis()))
    node.connect()
    cmd_raises_error = ACommandRaisingError({})
    mocker.patch.object(node, "recv", side_effect=[
        cmd_raises_error,
        ACommandDoNothing({}),
        RuntimeError()  # escape the recv_until_close loop
    ])
    error_handler_mock = MagicMock()
    try:
        # noinspection PyTypeChecker
        node.recv_until_close(error_handler=error_handler_mock)
    except RuntimeError:
        pass
    error_handler_mock.assert_called_once()
    assert error_handler_mock.call_args.args[0] is cmd_raises_error
    assert str(error_handler_mock.call_args.args[1]) == "an Exception"


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
        f"An Exception raised in the command execution: {cmd}",
        exc_info=exc_
    )
