from tests.kafka.fake_producer import FakeProducer
from tests.test_helpers.p4p_fakes import FakeContext
from forwarder.update_handlers.pva_update_handler import PVAUpdateHandler
from p4p import Value
from p4p.nt import NTScalar


def create_pv_update() -> Value:
    timestamp_seconds_since_epoch = 1.1
    return NTScalar("d").wrap(4.2, timestamp=timestamp_seconds_since_epoch)


def test_update_handler_publishes_update():
    producer = FakeProducer()
    context = FakeContext()
    pva_update_handler = PVAUpdateHandler(producer, context, "pv_name", "output_topic", "f142")  # type: ignore
    context.call_monitor_callback_with_fake_pv_update(create_pv_update())
    assert producer.published_payload is not None
    pva_update_handler.stop()
