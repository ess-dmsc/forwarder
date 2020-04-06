from caproto.threading.client import Context
from kafka.kafka_helpers import create_producer, create_consumer
from application_logger import setup_logger
from parse_config_update import parse_config_update, CommandTypes
from update_handler import UpdateHandler
import logging
import configargparse


def subscribe_to_pv(name: str):
    if name in update_handlers.keys():
        logger.warning("Forwarder asked to subscribe to PV it is already subscribed to")
        return
    (pv,) = ctx.get_pvs(name)
    update_handlers[name] = UpdateHandler(producer, pv)
    logger.info(f"Subscribed to PV {name}")


def unsubscribe_from_pv(name: str):
    try:
        update_handlers[name].stop()
        del update_handlers[name]
    except KeyError:
        logger.warning(
            "Forwarder asked to unsubscribe from a PV it is not subscribed to"
        )
    logger.info(f"Unsubscribed from PV {name}")


def parse_args():
    parser = configargparse.ArgumentParser(
        description="Writes NeXus files in a format specified with a json template.\n"
        "Writer modules can be used to populate the file from Kafka topics."
    )
    parser.add_argument(
        "--version", action="store_true", help="Print application version and exit"
    )
    parser.add_argument(
        "--config-topic",
        required=True,
        help="<host[:port][/topic]> Kafka broker/topic to listen for commands",
        type=str,
    )
    parser.add_argument(
        "--status-topic",
        required=True,
        help="<host[:port][/topic]> Kafka broker/topic to publish status updates on",
        type=str,
    )
    parser.add_argument(
        "--graylog-logger-address",
        required=False,
        help="<host:port> Log to Graylog",
        type=str,
    )
    parser.add_argument("--log-file", required=False, help="Log filename", type=str)
    parser.add_argument(
        "-c",
        "--config-file",
        required=False,
        is_config_file=True,
        help="Read configuration from an ini file",
    )
    log_choice_to_enum = {
        "Trace": logging.DEBUG,
        "Debug": logging.DEBUG,
        "Warning": logging.WARNING,
        "Error": logging.ERROR,
        "Critical": logging.CRITICAL,
    }
    parser.add_argument(
        "-v",
        "--verbosity",
        required=False,
        help="Set logging level",
        choices=log_choice_to_enum.keys(),
        default="Error",
    )
    args = parser.parse_args()
    args.verbosity = log_choice_to_enum[args.verbosity]
    return args


if __name__ == "__main__":
    args = parse_args()
    if args.version:
        raise NotImplementedError("Versioning not implemented yet")

    logger = setup_logger(level=args.verbosity, log_file_name=args.log_file)
    logger.info("Forwarder started")

    # EPICS
    ctx = Context()
    update_handlers = dict()

    # Kafka
    producer = create_producer()
    consumer = create_consumer()
    consumer.subscribe([args.config_topic])

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
        for _, handler in update_handlers.items():
            handler.stop()
        consumer.close()
        producer.close()
