from caproto.threading.client import Context
from caproto import ReadNotifyResponse
from kafka.kafkahelpers import create_producer, create_consumer, publish_f142_message
from applicationlogger import setup_logger


def monitor_callback(response: ReadNotifyResponse):
    logger.debug(f"Received PV update {response.header}")
    publish_f142_message(
        producer,
        "python_forwarder_topic",
        response.data,
        response.data_count,
        response.data_type,
    )


if __name__ == "__main__":
    logger = setup_logger()
    logger.info("Forwarder started")

    # EPICS
    ctx = Context()
    (x_int,) = ctx.get_pvs("incrementing_ioc:x_int")
    sub = x_int.subscribe()

    # Kafka
    producer = create_producer()
    consumer = create_consumer()
    consumer.subscribe(["python-forwarder-config"])

    # Metrics
    # use https://github.com/zillow/aiographite ?
    # can modify https://github.com/claws/aioprometheus for graphite?
    # https://julien.danjou.info/atomic-lock-free-counters-in-python/

    token = sub.add_callback(monitor_callback)

    try:
        while True:
            msg = consumer.poll(timeout=0.5)
            if msg is None:
                continue
            if msg.error():
                logger.error(msg.error())
            else:
                # Proper message
                logger.info(f"Received config message:\n{msg.value()}")

    except KeyboardInterrupt:
        logger.info("%% Aborted by user")

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
