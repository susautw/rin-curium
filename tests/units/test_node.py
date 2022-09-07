from contextlib import ExitStack
from unittest.mock import call, MagicMock

import pytest
from fakeredis import FakeRedis

from rin.curium import RedisConnection, Node, logger, CommandBase
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


@pytest.mark.parametrize("destinations, expected_destinations, has_warning", [
    ("x", {"x"}, False),
    (["x", "y"], {"x", "y"}, False),
    (["x", "x", "y"], {"x", "y"}, False),  # should estimate duplications
    (["all", "x"], {"all"}, True)
])
def test_send_no_response(mocker, node, destinations, expected_destinations, has_warning):
    cmd = MagicMock(spec=CommandBase)
    expected_data = b'data'
    expected_num_receivers = 10
    mock_serialize = mocker.patch.object(node._serializer, "serialize", side_effect=[expected_data])
    mock_send = mocker.patch.object(node._connection, "send", side_effect=[expected_num_receivers])

    with ExitStack() as stack:
        if has_warning:
            stack.enter_context(pytest.warns(RuntimeWarning))
        assert node.send_no_response(cmd, destinations) == expected_num_receivers
    mock_serialize.assert_called_once_with(cmd)
    mock_send.assert_called_once_with(expected_data, expected_destinations)


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


@pytest.mark.parametrize("opname, opcall, expected_opname, expected_call", [
    ("_get_response_handler", call('0'), "get", [call('0', None)]),
    ("_get_response_handler", call('0', ...), "get", [call('0', ...)]),
    ("_add_response_handler", call('0', ...), "__setitem__", [call('0', ...)]),
    ("_remove_response_handler", call('0'), "__delitem__", [call('0')])
])
def test_access_response_handler(mocker, node, opname, opcall, expected_opname, expected_call):
    mock_rhs = mocker.patch.object(node, "_sent_cmd_response_handlers")
    getattr(node, opname)(*opcall.args, **opcall.kwargs)
    assert getattr(mock_rhs, expected_opname).call_args_list == expected_call


@pytest.mark.parametrize("silent", [True, False])
def test_remove_response_handler__silent(mocker, node, silent):
    mock_rhs = mocker.patch.object(node, "_sent_cmd_response_handlers")
    mock_rhs.__delitem__.side_effect = KeyError
    with ExitStack() as stack:
        if not silent:
            # noinspection PyTypeChecker
            #  type checker messed up
            stack.enter_context(pytest.raises(KeyError))
        node._remove_response_handler('0', silent)


@pytest.mark.parametrize("raw_data", [b'data', None])
def test_recv(mocker, node, raw_data):
    mock_recv = mocker.patch.object(node._connection, "recv", side_effect=[raw_data])
    expected_cmd = None if raw_data is None else object()
    mock_deserialize = mocker.patch.object(node._serializer, "deserialize", side_effect=[expected_cmd])
    assert node.recv(True, 10) == expected_cmd
    mock_recv.assert_called_once_with(True, 10)
    if raw_data is not None:
        mock_deserialize.assert_called_once_with(raw_data)


@pytest.mark.parametrize("opcall, expected_ctx, has_context", [
    (call(), None, False),
    (call(...), ..., True)
])
def test_register_cmd(mocker, node, opcall, expected_ctx, has_context):
    mock_register_cmd = mocker.patch.object(node._serializer, "register_cmd")
    mock_ctx = mocker.patch.object(node, "_cmd_contexts")

    node.register_cmd(MyCommand, *opcall.args, **opcall.kwargs)

    mock_register_cmd.assert_called_once_with(MyCommand)
    if has_context:
        mock_ctx.__setitem__.assert_called_once_with(MyCommand.__cmd_name__, expected_ctx)
    else:
        assert mock_ctx.__setitem__.call_count == 0


@pytest.mark.parametrize("key, expected_name", [
    ("x", "x"),
    (MyCommand, "my_command")
])
def test_get_cmd_context(mocker, node, key, expected_name):
    mock_ctx = mocker.patch.object(node, "_cmd_contexts")
    node.get_cmd_context(key)
    mock_ctx.__getitem__.assert_called_once_with(expected_name)


def test_get_cmd_context__with_wrong_signature(node):
    with pytest.raises(TypeError, match=f"No signature matched to execute this method"):
        # noinspection PyTypeChecker
        node.get_cmd_context(10)


def test_get_cmd_context__but_key_does_not_exist(mocker, node):
    mocker.patch.object(node, "_cmd_contexts")
    node._cmd_contexts.__getitem__.side_effect = KeyError
    with pytest.raises(KeyError):
        node.get_cmd_context("key")
