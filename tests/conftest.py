from math import inf

from fakeredis._commands import command, Key, Int
from fakeredis._fakesocket import FakeSocket
from fakeredis._helpers import SimpleError


# patch FakeSocket.expire to support nx, xx, gt, and lt.
#  until the [ISSUE](https://github.com/cunla/fakeredis-py/issues/45) is fixed.
@command((Key(), Int), (bytes,))
def expire(
        self,
        key,
        seconds,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False
):
    if nx and gt or nx and lt or gt and lt:
        raise SimpleError('ERR NX and XX, GT or LT options at the same time are not compatible')
    expireat = getattr(key, "expireat", None)
    new_expireat = self._db.time + seconds
    if (lt or gt) and expireat is None:
        expireat = inf
    if nx and expireat is not None:
        return 0
    if xx and expireat is None:
        return 0
    if gt and new_expireat <= expireat:
        return 0
    if lt and new_expireat >= expireat:
        return 0
    return self._expireat(key, self._db.time + seconds)

FakeSocket.expire = expire
