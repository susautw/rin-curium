import pytest
from fakeredis import FakeRedis
from redis import exceptions

from rin.curium import RedisConnection, exc


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
    conn = RedisConnection(r, namespace="NS", expire=10)
    with pytest.raises(exc.ConnectionFailedError):
        conn.connect()


def test_connect__when_already_connected():
    conn = RedisConnection(FakeRedis(), namespace="NS", expire=10)
    uid = conn.connect()
    with pytest.warns(RuntimeWarning, match=f"Already connected. uid: {uid}"):
        assert conn.connect() == uid
