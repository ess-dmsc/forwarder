from caproto.threading.client import Context as CaContext
from p4p.client.thread import Context as PvaContext
from kafka.kafka_helpers import (
    create_producer,
    create_consumer,
    get_broker_and_topic_from_uri,
)
from application_logger import setup_logger
from parse_config_update import parse_config_update, CommandType, Channel
from update_handlers.create_update_handler import create_update_handler
import logging
import configargparse
from typing import Optional
from status_reporter import StatusReporter


def subscribe_to_pv(
    new_channel: Channel, fake_pv_period: int, pv_update_period: Optional[int]
):
    if new_channel.name in update_handlers.keys():
        logger.warning("Forwarder asked to subscribe to PV it is already subscribed to")
        return

    update_handlers[new_channel.name] = create_update_handler(
        producer,
        ca_ctx,
        pva_ctx,
        new_channel,
        fake_pv_period,
        periodic_update_ms=pv_update_period,
    )
    logger.info(f"Subscribed to PV {new_channel.name}")


def unsubscribe_from_pv(name: str):
    try:
        update_handlers[name].stop()
        del update_handlers[name]
    except KeyError:
        logger.warning(
            "Forwarder asked to unsubscribe from a PV it is not subscribed to"
        )
    logger.info(f"Unsubscribed from PV {name}")


def unsubscribe_from_all():
    for _, update_handler in update_handlers.items():
        update_handler.stop()
    update_handlers.clear()
    logger.info("Unsubscribed from all PVs")


def parse_args():
    parser = configargparse.ArgumentParser(
        description="Writes NeXus files in a format specified with a json template.\n"
        "Writer modules can be used to populate the file from Kafka topics."
    )
    parser.add_argument(
        "--version",
        action="store_true",
        help="Print application version and exit",
        env_var="VERSION",
    )
    parser.add_argument(
        "--config-topic",
        required=True,
        help="<host[:port][/topic]> Kafka broker/topic to listen for commands",
        type=str,
        env_var="CONFIG_TOPIC",
    )
    parser.add_argument(
        "--status-topic",
        required=True,
        help="<host[:port][/topic]> Kafka broker/topic to publish status updates on",
        type=str,
        env_var="STATUS_TOPIC",
    )
    parser.add_argument(
        "--graylog-logger-address",
        required=False,
        help="<host:port> Log to Graylog",
        type=str,
        env_var="GRAYLOG_LOGGER_ADDRESS",
    )
    parser.add_argument(
        "--log-file", required=False, help="Log filename", type=str, env_var="LOG_FILE"
    )
    parser.add_argument(
        "-c",
        "--config-file",
        required=False,
        is_config_file=True,
        help="Read configuration from an ini file",
        env_var="CONFIG_FILE",
    )
    parser.add_argument(
        "--pv-update-period",
        required=False,
        help="If set then PV value will be sent with this interval even if unchanged (units=milliseconds)",
        env_var="PV_UPDATE_PERIOD",
        type=int,
    )
    parser.add_argument(
        "--fake-pv-period",
        required=False,
        help="Set period for random generated PV updates when channel_provider_type is specified as 'fake' (units=milliseconds)",
        env_var="FAKE_PV_PERIOD",
        type=int,
        default=1000,
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
        env_var="VERBOSITY",
    )
    optargs = parser.parse_args()
    optargs.verbosity = log_choice_to_enum[optargs.verbosity]
    return optargs


if __name__ == "__main__":
    args = parse_args()
    if args.version:
        raise NotImplementedError("Versioning not implemented yet")

    logger = setup_logger(level=args.verbosity, log_file_name=args.log_file)
    logger.info("Forwarder started")

    # EPICS
    ca_ctx = CaContext()
    pva_ctx = PvaContext("pva")
    update_handlers = dict()

    # Kafka
    producer = create_producer()
    config_broker, config_topic = get_broker_and_topic_from_uri(args.config_topic)
    consumer = create_consumer(config_broker)
    consumer.subscribe([config_topic])

    status_broker, status_topic = get_broker_and_topic_from_uri(args.status_topic)
    status_reporter = StatusReporter(update_handlers, status_broker, status_topic)
    status_reporter.start()

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
                logger.info("Received config message")
                config_change = parse_config_update(msg.value())
                if config_change.command_type == CommandType.REMOVE_ALL:
                    unsubscribe_from_all()
                    status_reporter.report_status()
                elif config_change.command_type == CommandType.EXIT:
                    logger.info("Exit command received")
                    break
                else:
                    for channel in config_change.channels:
                        if config_change.command_type == CommandType.ADD:
                            subscribe_to_pv(
                                channel, args.fake_pv_period, args.pv_update_period
                            )
                        elif config_change.command_type == CommandType.REMOVE:
                            unsubscribe_from_pv(channel.name)
                    status_reporter.report_status()

    except KeyboardInterrupt:
        logger.info("%% Aborted by user")

    finally:
        status_reporter.stop()
        for _, handler in update_handlers.items():
            handler.stop()
        consumer.close()
        producer.close()
