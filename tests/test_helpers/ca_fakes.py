from typing import List, Callable
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

    def get_pvs(self, *pv_names: str) -> List[FakePV]:
        return [FakePV(pv_name, self.subscription) for pv_name in pv_names]

    def call_monitor_callback_with_fake_pv_update(self, pv_update: ReadNotifyResponse):
        self.subscription.callback(self.subscription, pv_update)
