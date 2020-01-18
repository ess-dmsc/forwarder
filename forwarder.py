from caproto.threading.client import Context
from caproto import ReadNotifyResponse
from kafka.kafkahelpers import create_producer, publish_f142_message
from time import sleep
from applicationlogger import setup_logger


def monitor_callback(response: ReadNotifyResponse):
    logger.debug(f"Received PV update {response.header}")
    publish_f142_message(producer, "python_forwarder_topic", response.data, response.data_count, response.data_type)


if __name__ == "__main__":
    logger = setup_logger()
    logger.info("Forwarder started")

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
