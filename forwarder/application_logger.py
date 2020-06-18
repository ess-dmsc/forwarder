import logging
import graypy
from typing import Optional

logger_name = "python-forwarder"


def setup_logger(
    level: int = logging.DEBUG,
    log_file_name: Optional[str] = None,
    graylog_host: Optional[str] = None,
) -> logging.Logger:
    if log_file_name is not None:
        logging.basicConfig(filename=log_file_name)
    else:
        logging.basicConfig()
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    if graylog_host is not None:
        host, port = graylog_host.split(":")
        handler = graypy.GELFTCPHandler(host, int(port))
        logger.addHandler(handler)
    return logger


def get_logger():
    return logging.getLogger(logger_name)
