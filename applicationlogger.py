import logging
import graypy


def setup_logger(name: str = "python-forwarder", level: int = logging.DEBUG) -> logging.Logger:
    logging.basicConfig()
    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = graypy.GELFUDPHandler('localhost', 12201)
    logger.addHandler(handler)
    return logger
