import logging

logger = logging.getLogger("rin-curium")

logger.setLevel(logging.WARNING)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter(
    "{levelname[0]} [{threadName:^10s}][{asctime}]: {message}", style="{"
))
logger.addHandler(ch)
