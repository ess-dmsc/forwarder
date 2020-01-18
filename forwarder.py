from caproto.threading.client import Context
from kafka.kafkahelpers import create_producer, publish_f142_message
from time import sleep
from applicationlogger import setup_logger


def monitor_callback(response):
    logger.debug(f"EPICS update received with value {response.data[0]}")
    publish_f142_message(producer, "python_forwarder_topic", int(response.data[0]))


if __name__ == "__main__":
    logger = setup_logger(__name__)
    logger.info("Forwarder")

    # EPICS
    ctx = Context()
    x, = ctx.get_pvs('incrementing_ioc:x')
    sub = x.subscribe()

    # Kafka
    producer = create_producer()

    # Metrics
    # use https://github.com/zillow/aiographite ?
    # can modify https://github.com/claws/aioprometheus for graphite?
    # https://julien.danjou.info/atomic-lock-free-counters-in-python/

    token = sub.add_callback(monitor_callback)

    while True:
        sleep(2)
