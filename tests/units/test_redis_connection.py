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
