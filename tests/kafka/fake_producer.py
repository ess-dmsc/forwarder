from typing import Callable, List, Optional


class FakeProducer:
    """
    Instead of publishing to Kafka when produce is called, this will store the payload so it can be checked in a test
    """

    def __init__(self, produce_callback: Optional[Callable[[bytes], None]] = None):
        self.messages_published = 0
        self.published_payloads: List[bytes] = []
        self._produce_callback = produce_callback

    def produce(
        self,
        topic: str,
        payload: bytes,
        timestamp_ms: int,
        key: Optional[str] = None,
    ):
        self.messages_published += 1
        self.published_payloads.append(payload)
        if self._produce_callback is not None:
            self._produce_callback(payload)

    def close(self):
        pass
