import logging

logger = logging.getLogger("rin-curium")
logger.setLevel(logging.WARNING)
logger.addHandler(logging.StreamHandler())
