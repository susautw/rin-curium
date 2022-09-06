from unittest.mock import call, MagicMock

import pytest
from fakeredis import FakeRedis

from rin.curium import RedisConnection, Node, logger
from rin.curium.node import CommandWrapper
from units.fake_commands import MyCommand


@pytest.fixture
def connection():
    return RedisConnection(FakeRedis())


# noinspection PyProtectedMember
@pytest.fixture
def node(connection):
    return Node(connection)


@pytest.mark.parametrize("send_only", [False, True])
def test_connect(mocker, node, connection, send_only):
    mock_connect = mocker.patch.object(connection, "connect", side_effect=["UID"])
    mock_join = mocker.patch.object(node, "join")
    mock_start = mocker.patch("threading.Thread.start")
    node.connect(send_only)

    mock_connect.assert_called_once_with()
    expected_call_args_list = [call("UID")]
    if not send_only:
        expected_call_args_list.append(call("all"))
    assert mock_join.call_args_list == expected_call_args_list
    mock_start.assert_called_once_with()


@pytest.mark.parametrize("close_node", [False, True])
def test_connect__but_already_connected_or_closed(mocker, node, close_node):
    mock_warning = mocker.patch.object(logger, "warning")
    node.connect()
    if close_node:
        node.close()
    node.connect()
    mock_warning.assert_called_once_with("connection already connected or closed")


@pytest.mark.parametrize("closed", [False, True])
def test_close(mocker, node, connection, closed):
    mocker.patch.object(node._closed_event, "wait", side_effect=[closed])
    mock_set = mocker.patch.object(node._closed_event, "set")
    mock_close = mocker.patch.object(connection, "close")

    node.close()

    expected_call_count = 0 if closed else 1

    assert mock_set.call_count == expected_call_count
    assert mock_close.call_count == expected_call_count


@pytest.mark.parametrize("opname, args", [
    ("join", ("name",)),
    ("leave", ("name",)),
])
def test_delegate_to_connection(mocker, node, connection, opname, args):
    mock_op = mocker.patch.object(connection, opname)
    getattr(node, opname)(*args)

    mock_op.assert_called_once_with(*args)


def test_send(mocker, node):
    cmd = MyCommand(x=1, y=[1, 2, 3])
    destinations = MagicMock()
    response_handler = MagicMock()
    response_timeout = 10
    expected_num_receivers = 10
    expected_nid = "UID"
    expected_cid = '0'

    mock_rh = MagicMock()
    mock_create_response_handler = mocker.patch.object(node, "_create_response_handler", side_effect=[mock_rh])
    mock_send_no_response = mocker.patch.object(node, "send_no_response", side_effect=[expected_num_receivers])
    mock_add_response_handler = mocker.patch.object(node, "_add_response_handler")
    node._nid = expected_nid

    node.send(cmd, destinations, response_handler, response_timeout)

    mock_create_response_handler.assert_called_once_with(response_handler, response_timeout)
    mock_send_no_response.assert_called_once()
    assert mock_send_no_response.call_args_list[0].args[0].to_dict() == CommandWrapper(
        nid=expected_nid,
        cid=expected_cid,
        cmd=cmd
    ).to_dict()
    assert mock_send_no_response.call_args_list[0].args[1] is destinations
    mock_rh.set_num_receivers.assert_called_once_with(expected_num_receivers)
    mock_add_response_handler.assert_called_once_with(expected_cid, mock_rh)


def test_create_response_handler__pass_through(node):
    mock_rh = MagicMock()
    assert node._create_response_handler(response_handler=mock_rh, response_timeout=None) is mock_rh


@pytest.mark.parametrize("timeout", [None, 10])
def test_create_response_handler__create_default(mocker, node, timeout):
    mock_rh_default_cls = mocker.patch("rin.curium.response_handlers.BlockUntilAllReceived")
    node._create_response_handler(response_handler=None, response_timeout=timeout)
    mock_rh_default_cls.assert_called_once_with(timeout=timeout)


def test_create_response_handler__invalid_params(node):
    with pytest.raises(ValueError, match="cannot set both response_handler and response_timeout"):
        node._create_response_handler(..., ...)
