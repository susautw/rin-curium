from fancy import config as cfg


class Atomic: ...  # TODO Atomic descriptior


def option_only_filter(p: cfg.PlaceHolder) -> bool:
    return isinstance(p, cfg.Option)