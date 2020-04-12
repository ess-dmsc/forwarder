from repeat_timer import RepeatTimer, milliseconds_to_seconds
from typing import Dict
import json


class StatusReporter:
    def __init__(self, update_handlers: Dict, interval_ms: int = 4000):
        self._repeating_timer = RepeatTimer(
            milliseconds_to_seconds(interval_ms), self.report_status
        )
        self._update_handlers = update_handlers

    def start(self):
        self._repeating_timer.start()

    def report_status(self):
        print(
            json.dumps(
                {
                    "streams": [
                        {"channel_name": channel_name}
                        for channel_name in self._update_handlers.keys()
                    ]
                }
            )
        )

    def stop(self):
        if self._repeating_timer is not None:
            self._repeating_timer.cancel()
