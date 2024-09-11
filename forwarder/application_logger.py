import logging
from typing import Optional


logger_name = "python-forwarder"


def setup_logger(
    level: int = logging.DEBUG,
    log_file_name: Optional[str] = None,
    graylog_logger_address: Optional[str] = None,
) -> None:
    if log_file_name is not None:
        logging.basicConfig(
            filename=log_file_name,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
    else:
        logging.basicConfig()
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)


def get_logger():
    return logging.LoggerAdapter(
        logging.getLogger(logger_name), {"process_name": "forwarder"}
    )
