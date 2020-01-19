from caproto.threading.client import Context
from caproto import ReadNotifyResponse
from kafka.kafkahelpers import create_producer, create_consumer, publish_f142_message
from applicationlogger import setup_logger


def monitor_callback(response: ReadNotifyResponse):
    logger.debug(f"Received PV update {response.header}")
    publish_f142_message(
        producer,
        "forwarder-output",
        response.data,
        response.data_count,
        response.data_type,
    )


def subscribe_to_pv(name: str):
    if name in pvs_forwarding.keys():
        logger.warning("Forwarder asked to subscribe to PV it is already subscribed to")
        return
    (x_int,) = ctx.get_pvs(name)
    sub = x_int.subscribe()
    sub.add_callback(monitor_callback)
    pvs_forwarding[name] = x_int


def unsubscribe_from_pv(name: str):
    try:
        pvs_forwarding[name].unsubscribe_all()
        del pvs_forwarding[name]
    except KeyError:
        logger.warning(
            "Forwarder asked to unsubscribe from a PV it is not subscribed to"
        )


if __name__ == "__main__":
    logger = setup_logger()
    logger.info("Forwarder started")

    # EPICS
    ctx = Context()
    pvs_forwarding = dict()

    subscribe_to_pv("incrementing_ioc:x_int")

    # Kafka
    producer = create_producer()
    consumer = create_consumer()
    consumer.subscribe(["forwarder-config"])

    # Metrics
    # use https://github.com/zillow/aiographite ?
    # can modify https://github.com/claws/aioprometheus for graphite?
    # https://julien.danjou.info/atomic-lock-free-counters-in-python/

    try:
        while True:
            msg = consumer.poll(timeout=0.5)
            if msg is None:
                continue
            if msg.error():
                logger.error(msg.error())
            else:
                logger.info(f"Received config message:\n{msg.value()}")

    except KeyboardInterrupt:
        logger.info("%% Aborted by user")

    finally:
        consumer.close()
        producer.close()
