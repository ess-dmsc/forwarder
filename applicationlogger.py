import logging


def setup_logger(name: str, level: int = logging.DEBUG) -> logging.Logger:
    logging.basicConfig()
    logger = logging.getLogger(name)
    logger.setLevel(level)
    return logger


def get_logger() -> logging.Logger:
    return logging.getLogger('python-forwarder')
