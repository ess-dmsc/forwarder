import logging
import graypy
from typing import Optional

logger_name = "python-forwarder"


def setup_logger(
    level: int = logging.DEBUG, log_file_name: Optional[str] = None
) -> logging.Logger:
    if log_file_name is not None:
        logging.basicConfig(filename=log_file_name)
    else:
        logging.basicConfig()
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    handler = graypy.GELFTCPHandler("localhost", 12201)
    logger.addHandler(handler)
    return logger


def get_logger():
    return logging.getLogger(logger_name)
