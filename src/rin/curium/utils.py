import functools
from weakref import WeakKeyDictionary

from fancy import config as cfg

from threading import Lock, RLock
from typing import Callable


class Atomic:
    """
    A descriptor makes applied variable atomic
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


def atomicfunction(fn):
    """ Make a function to be an atomic operation"""
    lock = RLock()

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        with lock:
            return fn(*args, **kwargs)

    return wrapper


class atomicmethod:
    def __init__(self, method):
        self._method = method
        self._instance_fn_map = WeakKeyDictionary()
        self._lock = Lock()

    def __get__(self, instance, owner):
        obj = owner if instance is None else instance
        with self._lock:
            if obj in self._instance_fn_map:
                return self._instance_fn_map[obj]

        lock = RLock()
        bound_method = self._method.__get__(instance, owner)

        @functools.wraps(self._method)
        def wrapper(*args, **kwargs):
            with lock:
                return bound_method(*args, **kwargs)

        wrapper.__isabstractmethod__ = self.__isabstractmethod__
        with self._lock:
            self._instance_fn_map[obj] = wrapper
        return wrapper

    def __set__(self, instance, value):
        raise RuntimeError("change an atomicmethod is not allowed")

    __delete__ = __set__

    @property
    def __isabstractmethod__(self):
        return getattr(self._method, "__isabstractmethod__", False)


def cmd_to_dict_filter(p: cfg.PlaceHolder) -> bool:
    return isinstance(p, cfg.Option) or p.name == "__cmd_name__"
