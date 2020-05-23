from threading import Timer


def milliseconds_to_seconds(time_ms: int) -> float:
    return float(time_ms) / 1000


class RepeatTimer(Timer):
    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)
