from unittest.mock import call

import pytest
from fakeredis import FakeRedis
from redis import exceptions

from rin.curium import RedisConnection, exc, logger


def test_connect(mocker):
    mock_uuid4 = mocker.patch("uuid.uuid4", return_value="UID")
    r = FakeRedis()
    conn = RedisConnection(r, namespace="NS", expire=10)
    conn.connect()

    assert r.get("NS:UID") == b'1'
    assert r.ttl("NS:UID") == 10
    assert conn._pubsub is not None
    assert conn._refresh_thread.is_alive()
    mock_uuid4.assert_called_once()


def test_connect__with_dup_id(mocker):
    mock_uuid4 = mocker.patch("uuid.uuid4", side_effect=["UID1", "UID2"])
    r = FakeRedis()
    r.set("NS:UID1", 1)
    conn = RedisConnection(r, namespace="NS", expire=10)
    conn.connect()

    assert r.get("NS:UID1") == b'2'
    assert r.get("NS:UID2") == b'1'
    assert r.ttl("NS:UID1") == 10
    assert r.ttl("NS:UID2") == 10
    assert mock_uuid4.call_count == 2


def test_connect__ping_failed(mocker):
    r = FakeRedis()
    mocker.patch.object(r, "ping", side_effect=exceptions.ConnectionError)
    conn = RedisConnection(r)
    with pytest.raises(exc.ConnectionFailedError):
        conn.connect()


def test_connect__when_already_connected():
    conn = RedisConnection(FakeRedis())
    uid = conn.connect()
    with pytest.warns(RuntimeWarning, match=f"Already connected. uid: {uid}"):
        assert conn.connect() == uid


def test_refresh_uid(mocker):
    r = FakeRedis()
    conn = RedisConnection(r, namespace="NS", expire=10)
    conn._uid_key = "NS:UID"
    conn._expire = 10
    mocker.patch.object(conn._refresh_thread_close, "wait", side_effect=[False] * 5 + [True])
    mock_setex = mocker.patch.object(conn._redis, "setex", side_effect=[
        None,
        exceptions.ConnectionError(),  # expect only show one `Server disconnected` warning
        exceptions.ConnectionError(),
        None,
        None
    ])
    mocker_warning = mocker.patch.object(logger, "warning")
    mock_sleep = mocker.patch("time.sleep")
    conn._refresh_uid()

    assert mock_setex.call_args_list == [call("NS:UID", 10, 1)] * 5
    assert mocker_warning.call_args_list == [call("Server disconnected"), call("Server reconnected")]
    assert mock_sleep.call_count == 5


@pytest.mark.parametrize("name, args", [
    ("reconnect", ()),
    ("join", ("channel_name",)),
    ("leave", ("channel_name",)),
    ("send", (b'data', ["channel_name"],)),
    ("recv", (True,))
])
def test_operation_when_disconnected(name, args):
    conn = RedisConnection(FakeRedis())
    with pytest.raises(exc.NotConnectedError):
        getattr(conn, name)(*args)


def test_reconnect(mocker):
    mock_uuid4 = mocker.patch("uuid.uuid4", return_value="UID")
    r = FakeRedis()
    conn = RedisConnection(r, namespace="NS", expire=10)
    conn.connect()
    r.flushall()
    conn.reconnect()

    assert r.get("NS:UID") == b'1'
    assert r.ttl("NS:UID") == 10
    assert conn._pubsub is not None
    assert conn._refresh_thread.is_alive()
    mock_uuid4.assert_called_once()


def test_close(mocker):
    conn = RedisConnection(FakeRedis(), namespace="NS")
    spy_delete = mocker.spy(conn._redis, "delete")
    conn.close()
    assert spy_delete.call_count == 0
    uid = conn.connect()
    conn.close()
    conn.close()  # the second `close` shouldn't affect anything
    spy_delete.assert_called_once_with(f'NS:{uid}')


def test_close__disconnect_while_closing(mocker):
    conn = RedisConnection(FakeRedis())
    mocker.patch.object(conn._redis, "delete", side_effect=exceptions.ConnectionError)
    conn.close()  # expect no exceptions raised


@pytest.mark.parametrize(
    "opname, mock_getter, op_args, excepted_call_args", [
        ("join", lambda m, conn: m.patch.object(conn._pubsub, "psubscribe"), ("channel",), ("*|channel|*",)),
        ("leave", lambda m, conn: m.patch.object(conn._pubsub, "punsubscribe"), ("channel",), ("*|channel|*",)),
        (
                "send",
                lambda m, conn: m.patch.object(conn._redis, "publish"),
                (b'data', ["channel"],), ("|channel|", b'data')
        ),
    ]
)
def test_disconnect_during_operation(mocker, opname, mock_getter, op_args, excepted_call_args):
    conn = RedisConnection(FakeRedis(), ping_while_sending=False)
    conn.connect()
    mock = mock_getter(mocker, conn)
    getattr(conn, opname)(*op_args)
    mock.assert_called_once_with(*excepted_call_args)


@pytest.mark.parametrize(
    "name, args", [
        ("join", ("an|invalid|channel",)),
        ("leave", ("an|invalid|channel",)),
        ("send", (b'data', ["a valid channel", "an|invalid|channel"],)),
    ]
)
def test_operation_with_invalid_channel_name(name, args):
    conn = RedisConnection(FakeRedis())
    with pytest.raises(
            exc.InvalidChannelError,
            match="character '|' shouldn't appear in channel name: an|invalid|channel"
    ):
        getattr(conn, name)(*args)


def test_join(mocker):
    conn = RedisConnection(FakeRedis())
    conn.connect()
    mock_psubscribe = mocker.patch.object(conn._pubsub, "psubscribe")
    conn.join("channel")
    mock_psubscribe.assert_called_once_with("*|channel|*")


def test_leave(mocker):
    conn = RedisConnection(FakeRedis())
    conn.connect()
    mock_punsubscribe = mocker.patch.object(conn._pubsub, "punsubscribe")
    conn.leave("channel")
    mock_punsubscribe.assert_called_once_with("*|channel|*")


@pytest.mark.parametrize("channels, pattern", [
    (["a"], "|a|"),
    (["a", "bc"], "|a|bc|"),
    (["a", "bc", "def"], "|a|bc|def|"),
])
def test_send(mocker, channels, pattern):
    conn = RedisConnection(FakeRedis(), ping_while_sending=False)
    mock_publish = mocker.patch.object(conn._redis, "publish")
    conn.connect()
    conn.send(b'data', channels)
    mock_publish.assert_called_once_with(pattern, b'data')


def test_send__with_no_destination(mocker):
    conn = RedisConnection(FakeRedis())
    mock_warning = mocker.patch.object(logger, "warning")
    assert conn.send(b'data', []) == 0
    mock_warning.assert_called_once_with("no channel specified, this operation is cancelled.")


@pytest.mark.parametrize("success", [True, False])
def test_send__with_ping(mocker, success):
    send_timeout = 10
    conn = RedisConnection(FakeRedis(), send_timeout=send_timeout, ping_while_sending=True)
    conn.connect()
    mock_ping = mocker.patch.object(conn._pubsub, "ping")
    mock_wait = mocker.patch.object(conn._send_ping_event, "wait", side_effect=[success])
    mock_publish = mocker.patch.object(conn._redis, "publish")

    if not success:
        with pytest.raises(exc.ServerDisconnectedError):
            conn.send(b'data', ['destination'])
    else:
        conn.send(b'data', ['destination'])

    mock_ping.assert_called_once_with(conn._ping_msg)
    mock_wait.assert_called_once_with(send_timeout)
    if success:
        mock_publish.assert_called_once_with("|destination|", b'data')


def test_recv(mocker):
    conn = RedisConnection(FakeRedis())
    conn.connect()
    mocker.patch.object(conn._pubsub, "parse_response")
    mocker.patch.object(conn._pubsub, "handle_message", side_effect=[
        {"type": "pong", "data": conn._ping_msg},
        {"type": "pmessage", "data": b'data'}
    ])
    mock_set = mocker.patch.object(conn._send_ping_event, "set")
    assert conn.recv() == b'data'
    mock_set.assert_called_once_with()


@pytest.mark.parametrize(
    "block, timeout, expected_call", [
        (True, None, call(True, None)),
        (True, 10, call(False, 10)),
        (False, None, call(False, 0)),
        (False, ..., call(False, 0)),
    ]
)
def test_recv__invoke_parse_response(mocker, block, timeout, expected_call):
    conn = RedisConnection(FakeRedis())
    conn.connect()
    mock_parse_response = mocker.patch.object(conn._pubsub, "parse_response")
    mocker.patch.object(conn._pubsub, "handle_message", side_effect=[None])
    conn.recv(block, timeout)
    assert mock_parse_response.call_count == 1
    assert mock_parse_response.call_args_list[0] == expected_call


def test_recv__disconnect_during_receiving(mocker):
    conn = RedisConnection(FakeRedis())
    mocker.patch.object(conn, "_verify_connected")  # simulate that the first connection test is passed
    with pytest.raises(exc.ServerDisconnectedError):
        conn.recv()


def test_recv__timeout_or_non_blocking_with_no_message(mocker):
    conn = RedisConnection(FakeRedis())
    conn.connect()
    # assume parse_response end with timeout
    mocker.patch.object(conn._pubsub, "parse_response")
    mocker.patch.object(conn._pubsub, "handle_message", side_effect=[None])

    assert conn.recv() is None
