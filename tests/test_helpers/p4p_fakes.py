from typing import Callable, Union

from p4p import Value


class FakeSubscription:
    def close(self):
        pass


class FakeContext:
    def __init__(self):
        def callback_undefined(*args, **kwargs):
            raise Exception(
                "Callback was called before it was set in FakeContext object"
            )

        self.callback: Callable = callback_undefined

    def call_monitor_callback_with_fake_pv_update(
        self, pv_update: Union[Value, Exception]
    ):
        self.callback(pv_update)

    def makeRequest(self, request_string: str) -> None:
        return None

    def monitor(
        self, pv_name: str, callback: Callable, request: None, notify_disconnect: bool
    ) -> FakeSubscription:
        self.callback = callback
        return FakeSubscription()
