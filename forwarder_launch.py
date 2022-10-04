import os.path as osp
import sys
from socket import gethostname
from typing import Dict

from caproto.threading.client import Context as CaContext
from p4p.client.thread import Context as PvaContext

from forwarder.application_logger import get_logger, setup_logger
from forwarder.configuration_store import ConfigurationStore, NullConfigurationStore
from forwarder.handle_config_change import handle_configuration_change
from forwarder.kafka.kafka_helpers import (
    create_consumer,
    create_producer,
    get_broker_and_username_from_uri,
    get_broker_topic_and_username_from_uri,
)
from forwarder.parse_commandline_args import get_version, parse_args
from forwarder.parse_config_update import Channel, parse_config_update
from forwarder.statistics_reporter import StatisticsReporter
from forwarder.status_reporter import StatusReporter
from forwarder.update_handlers.create_update_handler import UpdateHandler
from forwarder.utils import Counter


def create_epics_producer(
    broker_uri, broker_sasl_password, update_message_counter, update_buffer_err_counter
):
    (
        output_broker,
        output_sasl_mechanism,
        output_username,
    ) = get_broker_and_username_from_uri(broker_uri)
    producer = create_producer(
        output_broker,
        output_sasl_mechanism,
        output_username,
        broker_sasl_password,
        counter=update_message_counter,
        buffer_err_counter=update_buffer_err_counter,
    )
    return producer


def create_config_consumer(broker_uri, broker_sasl_password):
    (
        config_broker,
        config_topic,
        config_sasl_mechanism,
        config_username,
    ) = get_broker_topic_and_username_from_uri(broker_uri)
    consumer = create_consumer(
        config_broker,
        config_sasl_mechanism,
        config_username,
        broker_sasl_password,
    )
    consumer.subscribe([config_topic])
    return consumer


def create_status_reporter(
    update_handlers, broker_uri, broker_sasl_password, service_id, version, logger
):
    (
        status_broker,
        status_topic,
        status_sasl_mechanism,
        status_username,
    ) = get_broker_topic_and_username_from_uri(broker_uri)
    status_reporter = StatusReporter(
        update_handlers,
        create_producer(
            status_broker,
            status_sasl_mechanism,
            status_username,
            broker_sasl_password,
        ),
        status_topic,
        service_id,
        version,
        logger,
    )
    return status_reporter


def create_configuration_store(storage_topic, storage_topic_sasl_password):
    (
        store_broker,
        store_topic,
        store_sasl_mechanism,
        store_username,
    ) = get_broker_topic_and_username_from_uri(storage_topic)
    configuration_store = ConfigurationStore(
        create_producer(
            store_broker,
            store_sasl_mechanism,
            store_username,
            storage_topic_sasl_password,
        ),
        create_consumer(
            store_broker,
            store_sasl_mechanism,
            store_username,
            storage_topic_sasl_password,
        ),
        store_topic,
    )
    return configuration_store


def create_statistics_reporter(
    service_id,
    grafana_carbon_address,
    update_handlers,
    update_message_counter,
    update_buffer_err_counter,
    logger,
    prefix,
    statistics_update_interval,
):
    metric_hostname = gethostname().replace(".", "_")
    prefix = f"Forwarder.{metric_hostname}.{service_id}".replace(" ", "").lower()
    statistics_reporter = StatisticsReporter(
        grafana_carbon_address,
        update_handlers,
        update_message_counter,  # type: ignore
        update_buffer_err_counter,  # type: ignore
        logger,
        prefix=f"{prefix}.throughput",
        update_interval_s=statistics_update_interval,
    )
    return statistics_reporter


if __name__ == "__main__":
    args = parse_args()

    if args.log_file:
        # Assumes log_file to be a str like /foo/bar/file_name.txt
        # or just the filename file_name.txt.
        # In the latter case will create in the directory where the
        # command was issued
        folder = osp.dirname(args.log_file)
        if folder and not osp.exists(folder):
            # Create logger with console, log error and exit
            setup_logger(
                level=args.verbosity, graylog_logger_address=args.graylog_logger_address
            )
            get_logger().error(
                f"Log folder '{folder}' does not exist. Please create it first!"
            )
            sys.exit()

    setup_logger(
        level=args.verbosity,
        log_file_name=args.log_file,
        graylog_logger_address=args.graylog_logger_address,
    )

    version = get_version()
    get_logger().info(f"Forwarder v{version} started, service Id: {args.service_id}")
    # EPICS
    ca_ctx = CaContext()
    pva_ctx = PvaContext("pva", nt=False)
    # Using dictionary with Channel as key to ensure we avoid having multiple
    # handlers active for identical configurations: serialising updates from
    # same pv with same schema and publishing to same topic
    update_handlers: Dict[Channel, UpdateHandler] = {}

    grafana_carbon_address = args.grafana_carbon_address
    update_message_counter = Counter() if grafana_carbon_address else None
    update_buffer_err_counter = Counter() if grafana_carbon_address else None

    # Kafka
    producer = create_epics_producer(
        args.output_broker,
        args.output_broker_sasl_password,
        update_message_counter,
        update_buffer_err_counter,
    )
    consumer = create_config_consumer(
        args.config_topic, args.config_topic_sasl_password
    )
    status_reporter = create_status_reporter(
        update_handlers,
        args.status_topic,
        args.status_topic_sasl_password,
        args.service_id,
        version,
        get_logger(),
    )
    status_reporter.start()

    statistic_reporter = None
    if grafana_carbon_address:
        statistics_reporter = create_statistics_reporter(
            args.service_id,
            grafana_carbon_address,
            update_handlers,
            update_message_counter,
            update_buffer_err_counter,
            get_logger(),
            args.prefix,
            args.statistics_update_interval,
        )
        statistic_reporter.start()  # type: ignore

    if args.storage_topic:
        configuration_store = create_configuration_store(
            args.storage_topic, args.storage_topic_sasl_password
        )
        if not args.skip_retrieval:
            try:
                restore_config_command = parse_config_update(
                    configuration_store.retrieve_configuration()
                )
                handle_configuration_change(
                    restore_config_command,
                    args.fake_pv_period,
                    args.pv_update_period,
                    update_handlers,
                    producer,
                    ca_ctx,
                    pva_ctx,
                    get_logger(),
                    status_reporter,
                    configuration_store,
                )
            except RuntimeError as error:
                get_logger().error(
                    "Could not retrieve stored configuration on start-up: " f"{error}"
                )
    else:
        configuration_store = NullConfigurationStore

    # Metrics
    # use https://github.com/Jetsetter/graphyte ?
    # https://julien.danjou.info/atomic-lock-free-counters-in-python/

    try:
        while True:
            msg = consumer.poll(timeout=0.5)
            if msg is None:
                continue
            if msg.error():
                get_logger().error(msg.error())
            else:
                get_logger().info("Received config message")
                config_change = parse_config_update(msg.value())
                handle_configuration_change(
                    config_change,
                    args.fake_pv_period,
                    args.pv_update_period,
                    update_handlers,
                    producer,
                    ca_ctx,
                    pva_ctx,
                    get_logger(),
                    status_reporter,
                    configuration_store,
                )

    except KeyboardInterrupt:
        get_logger().info("%% Aborted by user")
    except BaseException as e:
        get_logger().error(
            "Got an exception in the application main loop. The exception message was: {e}"
        )
        get_logger().exception(e)

    finally:
        status_reporter.stop()
        if statistic_reporter:
            statistic_reporter.stop()

        for _, handler in update_handlers.items():
            handler.stop()
        consumer.close()
        producer.close()
        try:
            if configuration_store is not None:
                configuration_store._producer.close()
        except AttributeError:
            pass
