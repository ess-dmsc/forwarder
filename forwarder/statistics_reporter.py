from functools import wraps
from threading import Thread
import time
from typing import Dict

import graphyte

from forwarder.update_handlers.create_update_handler import UpdateHandler
from forwarder.parse_config_update import Channel


def run_in_thread(original):
    @wraps(original)
    def wrapper(*args, **kwargs):
        t = Thread(target=original, args=args, kwargs=kwargs, daemon=True)
        t.start()
        return t

    return wrapper


class StatisticsReporter:
    def __init__(
        self,
        graphyte_server: str,
        update_handlers: Dict[Channel, UpdateHandler],
        update_interval: float = 10.0,
    ):

        self._update_handlers = update_handlers
        self._update_interval = update_interval
        self._graphyte_server = graphyte_server

        self._sender = graphyte.Sender(self._graphyte_server, prefix="throughput")

        self._running = False

    @run_in_thread
    def start(self):
        while not self._running:
            self._sender.send("number_pvs", len(self._update_handlers.keys()))
            time.sleep(self._update_interval)
