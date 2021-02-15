from collections import Counter
from logging import Logger
from queue import Queue, Empty
import time
from typing import Dict, List

import graphyte  # type: ignore

from forwarder.parse_config_update import Channel
from forwarder.repeat_timer import RepeatTimer
from forwarder.update_handlers.create_update_handler import UpdateHandler


class StatisticsReporter:
    def __init__(
        self,
        graphyte_server: str,
        update_handlers: Dict[Channel, UpdateHandler],
        update_msg_queue: Queue,
        logger: Logger,
        prefix: str = "throughput",
        update_interval_s: int = 1,
    ):
        self._graphyte_server = graphyte_server
        self._update_handlers = update_handlers
        self._update_msg_queue = update_msg_queue
        self._logger = logger

        self._total_updates: List[str] = []
        self._sender = graphyte.Sender(self._graphyte_server, prefix=prefix)
        self._repeating_timer = RepeatTimer(update_interval_s, self.send_statistics)

    def start(self):
        self._repeating_timer.start()

    def send_statistics(self):
        # Clear the _update_message_queue
        while not self._update_msg_queue.empty():
            try:
                self._total_updates.append(self._update_msg_queue.get_nowait())
            except Empty:
                continue
            self._update_msg_queue.task_done()

        timestamp = time.time()
        number_pvs = len(self._update_handlers.keys())
        try:
            self._sender.send("number_pvs", number_pvs, timestamp)
            for channel, counts in Counter(self._total_updates).items():
                print(channel, counts)
                self._sender.send(channel, counts, timestamp)

        except Exception as ex:
            self._logger.error(f"Could not send statistic: {ex}")

    def stop(self):
        self._total_updates.clear()
        self._repeating_timer.cancel()
