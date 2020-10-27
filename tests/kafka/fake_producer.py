from typing import Optional


class FakeProducer:
    """
    Instead of publishing to Kafka when produce is called, this will store the payload so it can be checked in a test
    """

    def __init__(self):
        self.messages_published = 0
        self.published_payload: Optional[bytes] = None

    def produce(
        self, topic: str, payload: bytes, timestamp_ms: int, key: Optional[str] = None,
    ):
        self.messages_published += 1
        self.published_payload = payload

    def close(self):
        pass
