import functools
from weakref import WeakKeyDictionary

from . import cfg

from threading import Lock, RLock
from typing import Callable, Type

from rin.docutils import markers
from rin.docutils.flag import Flag


class Atomic:
    """
    A descriptor converts an attribute's accession and deletion to atomic operations
    """
    _lock_var_suffix = "_lock"
    _default_factory: Callable

    def __init__(self, default=None, default_factory=None):
        if default_factory is not None:
            if default is not None:
                raise ValueError("default and default_factory cannot use simultaneously.")
            self._default_factory = default_factory
        else:
            self._default_factory = self._default_wrapper(default)

    def __set_name__(self, owner, name):
        self._name = name

    def __get__(self, instance, owner):
        if instance is None:
            return self
        self._init_lock(instance)
        with getattr(instance, self._get_lock_name()):
            if self._name not in vars(instance):
                self._set_val(instance, self._default_factory())
            return vars(instance)[self._name]

    def __set__(self, instance, value):
        self._init_lock(instance)
        with getattr(instance, self._get_lock_name()):
            self._set_val(instance, value)

    def _set_val(self, instance, value):
        vars(instance)[self._name] = value

    def __delete__(self, instance):
        self._init_lock(instance)
        with getattr(instance, self._get_lock_name()):
            if self._name in vars(instance):
                del vars(instance)[self._name]

    def _init_lock(self, instance) -> None:
        if not hasattr(instance, self._get_lock_name()):
            setattr(instance, self._get_lock_name(), Lock())

    def _get_lock_name(self) -> str:
        return self._name + self._lock_var_suffix

    @staticmethod
    def _default_wrapper(value):
        def inner():
            return value

        return inner


@markers.decorator
def atomicfunction(fn):
    """ Convert a function to an atomic operation """
    lock = RLock()

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        with lock:
            return fn(*args, **kwargs)

    return wrapper


@markers.decorator
class atomicmethod:
    """ A descriptor converts a method to an atomic operation """
    MARK_AS_DELETED = Flag("MARK_AS_DELETED")

    def __init__(self, method):
        self._default_method = method
        self._instance_method_map = WeakKeyDictionary()
        self._instance_fn_map = WeakKeyDictionary()
        self._lock = Lock()

    def __set_name__(self, owner, name):
        self.__name__ = name

    def __get__(self, instance, owner):
        obj = owner if instance is None else instance
        with self._lock:
            method = self._instance_method_map.setdefault(obj, self._default_method)
        if method is self.MARK_AS_DELETED:
            raise AttributeError(f'{owner} object has no attribute {self.__name__}')
        if not (hasattr(method, "__get__") and hasattr(method, "__call__")):
            return method

        bound_method = method.__get__(instance, owner)

        with self._lock:
            if obj in self._instance_fn_map:
                return self._instance_fn_map[obj]

        lock = RLock()

        @functools.wraps(method)
        def wrapper(*args, **kwargs):
            with lock:
                return bound_method(*args, **kwargs)

        wrapper.__isabstractmethod__ = self.__isabstractmethod__
        with self._lock:
            self._instance_fn_map[obj] = wrapper
        return wrapper

    def __set__(self, instance, value):
        with self._lock:
            self._instance_method_map[instance] = value

    def __delete__(self, instance):
        with self._lock:
            self._instance_method_map[instance] = self.MARK_AS_DELETED

    @property
    def __isabstractmethod__(self):
        return getattr(self._default_method, "__isabstractmethod__", False)


def cmd_to_dict_filter(p: cfg.PlaceHolder) -> bool:
    """ A filter used in the command to dictionary conversion """
    return isinstance(p, cfg.Option) or p.name == "__cmd_name__"


@markers.decorator
def add_error_handler(
        error_typ, *,
        reraise_by: Type[Exception] = None,
        suppress: bool = None,
        custom: Callable[[Exception], None] = None
):
    """
    An decorator factory catches an exception and handle it by given handler.

    .. warning:: You can only specify one handler.
     If you specify multiple handlers, :exc:`ValueError` will be raised.

    :param error_typ: type of exception to be handled
    :param reraise_by: type of exception used to re-raise the caught exception
    :param suppress: is or not suppress the caught exception
    :param custom: a custom :data:`~typing.Callable` to handle the caught exception
    :return: a decorator
    """
    argn = 0
    for name, h in [("reraise_by", reraise_by), ("suppress", suppress), ("custom", custom)]:
        if h is not None:
            handler = name
            argn += 1
    if argn == 0:
        raise RuntimeError("No error handler specified")
    if argn > 1:
        raise RuntimeError("More one error handlers specified")

    def _decorator(fn):
        @functools.wraps(fn)
        def _wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except error_typ as e:
                if handler == "reraise_by":
                    raise reraise_by(e)
                elif handler == "suppress":
                    if not suppress:
                        raise
                else:
                    custom(e)

        return _wrapper

    return _decorator
