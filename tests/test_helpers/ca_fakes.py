from typing import List, Callable


class FakeSubscription:
    @staticmethod
    def add_callback(callback: Callable):
        pass


class FakePV:
    @staticmethod
    def subscribe(data_type: str) -> FakeSubscription:
        return FakeSubscription()


class FakeContext:
    @staticmethod
    def get_pvs(*pv_names: str) -> List[FakePV]:
        return [FakePV() for _ in pv_names]
