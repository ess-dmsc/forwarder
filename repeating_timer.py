from threading import Thread, Event
from typing import Callable


class RepeatingTimer(Thread):
    def __init__(self, stop_event: Event, callback: Callable, interval_s: float):
        Thread.__init__(self)
        self.stopped = stop_event
        self.interval_s = interval_s
        self.callback = callback

    def run(self):
        while not self.stopped.wait(self.interval_s):
            self.callback()
