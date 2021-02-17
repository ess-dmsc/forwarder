from itertools import count
from threading import Lock


class Counter:
    """Thread safe Counter class without lock during write"""

    def __init__(self):
        self._num_read = 0
        self._counter = count()
        self._read_lock = Lock()

    def increment(self):
        """next is thread safe"""
        next(self._counter)

    @property
    def value(self):
        with self._read_lock:
            value = next(self._counter) - self._num_read
            self._num_read += 1
        return value
