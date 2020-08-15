from typing import List, Callable, Optional
from caproto import ReadNotifyResponse


class FakeSubscription:
    def __init__(self):
        def callback_undefined(*args, **kwargs):
            raise Exception(
                "Callback was called before it was set in FakeSubscription object"
            )

        self.callback: Callable = callback_undefined

    def add_callback(self, callback: Callable):
        self.callback = callback


class FakePV:
    def __init__(self, pv_name: str, subscription: FakeSubscription):
        self.name = pv_name
        self.subscription = subscription

    def subscribe(self, data_type: str) -> FakeSubscription:
        return self.subscription

    @staticmethod
    def unsubscribe_all():
        pass


class FakeContext:
    def __init__(self):
        self.subscription = FakeSubscription()
        self._connection_state_callback: Optional[Callable] = None

    def get_pvs(
        self, *pv_names: str, connection_state_callback: Callable
    ) -> List[FakePV]:
        self._connection_state_callback = connection_state_callback
        return [FakePV(pv_name, self.subscription) for pv_name in pv_names]

    def call_monitor_callback_with_fake_pv_update(self, pv_update: ReadNotifyResponse):
        self.subscription.callback(self.subscription, pv_update)

    def call_connection_state_callback_with_fake_state_change(self, state: str):
        if self._connection_state_callback is not None:
            self._connection_state_callback(None, state)
