import logging
from typing import Optional

import graypy

logger_name = "python-forwarder"


def setup_logger(
    level: int = logging.DEBUG,
    log_file_name: Optional[str] = None,
    graylog_logger_address: Optional[str] = None,
) -> logging.Logger:
    if log_file_name is not None:
        logging.basicConfig(
            filename=log_file_name,
            format="%(asctime)s - %(filename)s - %(levelname)s - %(message)s",
        )
    else:
        logging.basicConfig()
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    if graylog_logger_address is not None:
        host, port = graylog_logger_address.split(":")
        handler = graypy.GELFTCPHandler(host, int(port))
        logger.addHandler(handler)
    return logger


def get_logger():
    return logging.getLogger(logger_name)
