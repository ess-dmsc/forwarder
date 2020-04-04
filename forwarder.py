from caproto.threading.client import Context
from kafka.kafkahelpers import create_producer, create_consumer
from applicationlogger import setup_logger
from parseconfigupdate import parse_config_update, CommandTypes
from updatehandler import UpdateHandler


def subscribe_to_pv(name: str):
    if name in update_handlers.keys():
        logger.warning("Forwarder asked to subscribe to PV it is already subscribed to")
        return
    (pv,) = ctx.get_pvs(name)
    update_handlers[name] = UpdateHandler(producer, pv)
    logger.debug(f"Subscribed to PV {name}")


def unsubscribe_from_pv(name: str):
    try:
        update_handlers[name].pv.unsubscribe_all()
        del update_handlers[name]
    except KeyError:
        logger.warning(
            "Forwarder asked to unsubscribe from a PV it is not subscribed to"
        )
    logger.debug(f"Unsubscribed from PV {name}")


if __name__ == "__main__":
    logger = setup_logger()
    logger.info("Forwarder started")

    # EPICS
    ctx = Context()
    update_handlers = dict()

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
                logger.info(f"Received config message")
                config_change = parse_config_update(msg.value())
                for channel_name in config_change.channel_names:
                    if config_change.command_type == CommandTypes.ADD.value:
                        subscribe_to_pv(channel_name)
                    elif config_change.command_type == CommandTypes.REMOVE.value:
                        unsubscribe_from_pv(channel_name)

    except KeyboardInterrupt:
        logger.info("%% Aborted by user")

    finally:
        consumer.close()
        producer.close()
