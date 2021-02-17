from logging import Logger
import time
from typing import Dict

import graphyte  # type: ignore

from forwarder.parse_config_update import Channel
from forwarder.repeat_timer import RepeatTimer
from forwarder.update_handlers.create_update_handler import UpdateHandler
from forwarder.utils import Counter


class StatisticsReporter:
    def __init__(
        self,
        graphyte_server: str,
        update_handlers: Dict[Channel, UpdateHandler],
        update_msg_counter: Counter,
        logger: Logger,
        prefix: str = "throughput",
        update_interval_s: int = 10,
    ):
        self._graphyte_server = graphyte_server
        self._update_handlers = update_handlers
        self._update_msg_counter = update_msg_counter
        self._logger = logger

        self._sender = graphyte.Sender(self._graphyte_server, prefix=prefix)
        self._repeating_timer = RepeatTimer(update_interval_s, self.send_statistics)

    def start(self):
        self._repeating_timer.start()

    def send_statistics(self):
        counts = self._update_msg_counter.value
        print("Updates ", counts)
        timestamp = time.time()
        try:
            self._sender.send(
                "number_pvs", len(self._update_handlers.keys()), timestamp
            )
            self._sender.send("total_updates", counts, timestamp)
        except Exception as ex:
            self._logger.error(f"Could not send statistic: {ex}")

    def stop(self):
        if self._repeating_timer:
            self._repeating_timer.cancel()
