from threading import Timer, Lock
from datetime import datetime, timedelta


def milliseconds_to_seconds(time_ms: int) -> float:
    return float(time_ms) / 1000


class RepeatTimer(Timer):
    def __init__(self, interval, function, args=None, kwargs=None):
        super(RepeatTimer, self).__init__(interval, function, args, kwargs)
        self._lock = Lock()
        self._interval_time = timedelta(seconds=interval)
        self._trigger_time = datetime.now() + self._interval_time

    def _calculate_new_wait_time(self, current_time: datetime) -> float:
        if current_time > self._trigger_time:
            self._trigger_time = self._trigger_time + self._interval_time * (
                (current_time - self._trigger_time) // self._interval_time + 1
            )
        return (self._trigger_time - datetime.now()).total_seconds()

    def run(self):
        with self._lock:
            wait_time = self._calculate_new_wait_time(datetime.now())
        while not self.finished.wait(wait_time):
            current_time = datetime.now()
            with self._lock:
                if current_time >= self._trigger_time:
                    self.function(*self.args, **self.kwargs)
                wait_time = self._calculate_new_wait_time(current_time)

    def reset(self):
        with self._lock:
            self._trigger_time += self._interval_time
