import logging
import graypy

application_name = "python-forwarder"


def setup_logger(
    name: str = application_name, level: int = logging.DEBUG
) -> logging.Logger:
    logging.basicConfig()
    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = graypy.GELFUDPHandler("localhost", 12201)
    logger.addHandler(handler)
    return logger


def get_logger():
    return logging.getLogger(application_name)
