import re
from contextlib import ExitStack
from typing import List
from unittest.mock import call, MagicMock

import pytest
from fakeredis import FakeRedis

from rin.curium import RedisConnection, Node, logger, CommandBase, exc
from rin.curium.exc import CommandExecutionError
from rin.curium.node import CommandWrapper, error_logging
from units.fake_commands import MyCommand, ACommandRaisingError, ACommandDoNothing
from units.helper import keep_last_result


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
    mocker.patch("threading.Thread.start")
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


def test_add_response(mocker, node):
    mock_rh = MagicMock()
    expected_cid = "10"
    expected_response = MagicMock()
    mock_get_response_handler = mocker.patch.object(node, "_get_response_handler", side_effect=[mock_rh])
    node.add_response(expected_cid, expected_response)

    mock_get_response_handler.assert_called_once_with(expected_cid)
    mock_rh.add_response.assert_called_once_with(expected_response)


def test_add_response__but_response_handler_does_not_exist(mocker, node):
    mocker.patch.object(node, "_get_response_handler", side_effect=[None])
    mock_warning = mocker.patch.object(logger, "warning")

    cid = "10"
    response = "<response>"
    expected_msg = "Received response <response>, but command 10 not found"
    node.add_response(cid, response)
    mock_warning.assert_called_once_with(expected_msg)


def test_recv_until_close(mocker, node):
    mocker.patch.object(node._closed_event, "wait", side_effect=keep_last_result([
        False, True
    ]))
    cmd = MyCommand(x=1, y=[1, 2])
    mock_execute = mocker.patch.object(cmd, "execute")
    mock_recv = mocker.patch.object(node, "recv", side_effect=[cmd])
    excepted_sleep = 1
    node.recv_until_close(sleep=excepted_sleep)
    mock_execute.assert_called_once_with(node)
    mock_recv.assert_called_once_with(block=True, timeout=excepted_sleep)


@pytest.mark.parametrize("is_manually_closed", [True, False])
def test_recv_until_close__disconnected_when_recv(mocker, node, is_manually_closed):
    if is_manually_closed:
        wait_side_effect = [False, True]
    else:
        wait_side_effect = [False, False, True]

    mocker.patch.object(node._closed_event, "wait", side_effect=keep_last_result(wait_side_effect))
    mocker.patch.object(node, "recv", side_effect=exc.CuriumConnectionError)
    mock_reconnect_to_backend = mocker.patch.object(node, "_Node__reconnect_to_backend")

    node.recv_until_close()

    if not is_manually_closed:
        mock_reconnect_to_backend.assert_called_once()
    else:
        assert mock_reconnect_to_backend.call_count == 0


@pytest.mark.parametrize("failed_times, max_tries", [
    (0, 3),
    (1, 3),
    (2, 3),
    (3, 3),
    (4, 3)
])
def test_recv_until_close__reconnect(mocker, node, failed_times, max_tries):
    reconnect_side_effect: List = [exc.ConnectionFailedError] * failed_times
    reconnect_side_effect.append(None)
    mock_reconnect = mocker.patch.object(
        node._connection, "reconnect", side_effect=reconnect_side_effect
    )
    mock_warning = mocker.patch.object(logger, "warning")
    mocker.patch("time.sleep")
    expected_reconnect_count = min(failed_times + 1, max_tries)
    expected_msgs = [
        call(f"Reconnecting to the backend server: {i}")
        for i in range(expected_reconnect_count)
    ]
    reconnected = failed_times < max_tries

    if reconnected:
        expected_msgs.append(call(f"Server reconnected"))

    with ExitStack() as stack:
        if not reconnected:
            # noinspection PyTypeChecker
            stack.enter_context(pytest.raises(exc.ServerDisconnectedError))

        node._Node__reconnect_to_backend(max_tries, 1)

    assert mock_reconnect.call_count == expected_reconnect_count
    assert mock_warning.call_args_list == expected_msgs


@pytest.mark.parametrize("exception, expected_log", [
    (exc.InvalidFormatError("data"), "received command with invalid format: data"),
    (exc.CommandNotRegisteredError("cmd"), "received a unknown command: 'cmd'")
])
def test_recv_until_close__error_while_receiving(mocker, node, exception, expected_log):
    mocker.patch.object(node._closed_event, "wait", side_effect=keep_last_result(
        [False, True]
    ))
    mocker.patch.object(node, "recv", side_effect=exception)
    mock_exception = mocker.patch.object(logger, "exception")
    node.recv_until_close()

    mock_exception.assert_called_once_with(expected_log, exc_info=exception)


def test_recv_until_close__error_handling(mocker, node):
    cmd_raises_error = ACommandRaisingError({})
    following_cmd = ACommandDoNothing({})
    mocker.patch.object(node._closed_event, "wait", side_effect=keep_last_result([
        False, False, True
    ]))
    spy_execute = mocker.spy(following_cmd, "execute")
    mocker.patch.object(node, "recv", side_effect=[
        cmd_raises_error,
        following_cmd
    ])
    error_handler_mock = MagicMock()

    # noinspection PyTypeChecker
    node.recv_until_close(error_handler=error_handler_mock)

    error_handler_mock.assert_called_once()
    spy_execute.assert_called_once_with(node)
    e: Exception = error_handler_mock.call_args.args[0]
    assert e.args[0] is cmd_raises_error
    assert str(e.args[1]) == "an Exception"


@pytest.mark.parametrize("call_args, expected_recv_call, expected_thread_init_call", [
    (call(0.5, name="tn"), call(sleep=0.5), call(name="tn")),
    (call(0.5, None), call(sleep=0.5, num_workers=None), call()),
    (call(0.5, num_workers=3, name="tn"), call(sleep=0.5, num_workers=3), call(name="tn"))
])
def test_recv_until_close_in_thread(
        node,
        call_args,
        expected_recv_call,
        expected_thread_init_call
):
    mock_thread = MagicMock()
    mock_factory = MagicMock(return_value=mock_thread)
    assert node.recv_until_close_in_thread(
        *call_args.args, thread_factory=mock_factory, **call_args.kwargs
    ) is mock_thread
    mock_factory.assert_called_once_with(
        target=node.recv_until_close,
        kwargs=expected_recv_call.kwargs,
        **expected_thread_init_call.kwargs
    )


@pytest.mark.parametrize("call_args, kw_name", [
    (call(args=()), "args"),
    (call(target=1), "target"),
    (call(kwargs={}), "kwargs")
])
def test_recv_until_close_in_thread__with_invalid_parameters(node, call_args, kw_name):
    expected_error_msg = f"invalid parameter: {kw_name}({{val}})"
    mock_factory = MagicMock()
    with pytest.raises(ValueError, match=re.escape(expected_error_msg.format(val=call_args.kwargs[kw_name]))):
        node.recv_until_close_in_thread(*call_args.args, thread_factory=mock_factory, **call_args.kwargs)


def test_error_logging(mocker):
    """
    Test the default error handler for :meth:`Node.recv_until_close`
    """
    from rin.curium import logger
    mock_exception = mocker.patch.object(logger, "exception")
    cmd = MagicMock(spec=CommandBase)
    exc_ = Exception()
    error_logging(CommandExecutionError(cmd, exc_))
    mock_exception.assert_called_once_with(
        f"An Exception raised in the command execution: {cmd}",
        exc_info=exc_
    )


def test_check_response_handler(mocker, node):
    mocker.patch.object(node._closed_event, "wait", side_effect=[False, True])
    rh_not_finalized = MagicMock()
    rh_not_finalized.finalize.return_value = False

    rh_finalized = MagicMock()
    rh_finalized.finalize.return_value = True
    rhs = {
        "0": rh_not_finalized,
        "1": rh_finalized
    }

    mocker.patch.object(node._sent_cmd_response_handlers, "copy", return_value=rhs)
    mock_remove_rh = mocker.patch.object(node, "_remove_response_handler")
    mocker.patch("time.sleep")

    node._check_response_handlers()

    for rh in rhs.values():
        rh.finalize.assert_called_once_with()

    mock_remove_rh.assert_called_once_with("1", silent=True)
