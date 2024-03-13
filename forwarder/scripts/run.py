import os
import sys
from contextlib import ExitStack
from socket import gethostname
from typing import Dict, Optional

from caproto.threading.client import Context as CaContext
from p4p.client.thread import Context as PvaContext

from forwarder.application_logger import get_logger, setup_logger
from forwarder.common import Channel
from forwarder.configuration_store import ConfigurationStore, NullConfigurationStore
from forwarder.handle_config_change import handle_configuration_change
from forwarder.kafka.kafka_helpers import (
    create_consumer,
    create_producer,
    parse_kafka_uri,
)
from forwarder.metrics import Counter, Gauge
from forwarder.metrics.statistics_reporter import StatisticsReporter
from forwarder.parse_commandline_args import get_version, parse_args
from forwarder.parse_config_update import parse_config_update
from forwarder.status_reporter import StatusReporter
from forwarder.update_handlers.create_update_handler import UpdateHandler


def create_epics_producer(
    broker_uri,
    broker_sasl_password,
    broker_ssl_ca_file,
    statistics_reporter: Optional[StatisticsReporter],
):
    (
        broker,
        _,
        security_protocol,
        sasl_mechanism,
        username,
    ) = parse_kafka_uri(broker_uri)
    producer = create_producer(
        broker,
        security_protocol,
        sasl_mechanism,
        username,
        broker_sasl_password,
        broker_ssl_ca_file,
        statistics_reporter=statistics_reporter,
    )
    return producer


def create_config_consumer(broker_uri, broker_sasl_password, broker_ssl_ca_file):
    (
        broker,
        topic,
        security_protocol,
        sasl_mechanism,
        username,
    ) = parse_kafka_uri(broker_uri)

    if not topic:
        raise RuntimeError("Configuration consumer must have a config topic")

    consumer = create_consumer(
        broker,
        security_protocol,
        sasl_mechanism,
        username,
        broker_sasl_password,
        broker_ssl_ca_file,
    )
    consumer.subscribe([topic])
    return consumer


def create_status_reporter(
    update_handlers,
    broker_uri,
    broker_sasl_password,
    broker_ssl_ca_file,
    service_id,
    version,
    logger,
):
    (
        broker,
        topic,
        security_protocol,
        sasl_mechanism,
        username,
    ) = parse_kafka_uri(broker_uri)

    if not topic:
        raise RuntimeError("Status reporter must have a topic")

    status_reporter = StatusReporter(
        update_handlers,
        create_producer(
            broker,
            security_protocol,
            sasl_mechanism,
            username,
            broker_sasl_password,
            broker_ssl_ca_file,
        ),
        topic,
        service_id,
        version,
        logger,
    )
    return status_reporter


def create_configuration_store(
    storage_topic, storage_topic_sasl_password, broker_ssl_ca_file
):
    (
        broker,
        topic,
        security_protocol,
        sasl_mechanism,
        username,
    ) = parse_kafka_uri(storage_topic)

    if not topic:
        raise RuntimeError("Configuration store must have a storage topic")

    configuration_store = ConfigurationStore(
        create_producer(
            broker,
            security_protocol,
            sasl_mechanism,
            username,
            storage_topic_sasl_password,
            broker_ssl_ca_file,
        ),
        create_consumer(
            broker,
            security_protocol,
            sasl_mechanism,
            username,
            storage_topic_sasl_password,
            broker_ssl_ca_file,
        ),
        topic,
    )
    return configuration_store


def create_statistics_reporter(
    service_id,
    grafana_carbon_address,
    logger,
    statistics_update_interval,
):
    metric_hostname = gethostname().replace(".", "_")
    prefix = f"forwarder.{metric_hostname}.{service_id}".replace(" ", "").lower()
    statistics_reporter = StatisticsReporter(
        grafana_carbon_address,
        logger,
        prefix=prefix,
        update_interval_s=statistics_update_interval,
    )
    return statistics_reporter


def main():
    args = parse_args()

    if args.log_file:
        # Assumes log_file to be a str like /foo/bar/file_name.txt
        # or just the filename file_name.txt.
        # In the latter case will create in the directory where the
        # command was issued
        folder = os.path.dirname(args.log_file)
        if folder and not os.path.exists(folder):
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
    get_logger().info(
        f"Forwarder version '{version}' started, service Id: {args.service_id}"
    )
    # EPICS
    ca_ctx = CaContext()
    pva_ctx = PvaContext("pva", nt=False)
    # Using dictionary with Channel as key to ensure we avoid having multiple
    # handlers active for identical configurations: serialising updates from
    # same pv with same schema and publishing to same topic
    update_handlers: Dict[Channel, UpdateHandler] = {}

    grafana_carbon_address = args.grafana_carbon_address

    with ExitStack() as exit_stack:
        statistics_reporter = None
        pvs_subscribed_metric = None
        processing_errors_metric = None
        if grafana_carbon_address:
            statistics_reporter = create_statistics_reporter(
                args.service_id,
                grafana_carbon_address,
                get_logger(),
                args.statistics_update_interval,
            )
            exit_stack.callback(statistics_reporter.stop)
            statistics_reporter.start()
            pvs_subscribed_metric = Gauge("pvs_subscribed", "Number of PVs subscribed")
            statistics_reporter.register_metric(
                pvs_subscribed_metric.name, pvs_subscribed_metric
            )
            processing_errors_metric = Counter(
                "processing_errors", "Errors processing EPICS updates"
            )
            statistics_reporter.register_metric(
                processing_errors_metric.name, processing_errors_metric
            )

        # Kafka
        producer = create_epics_producer(
            args.output_broker,
            args.output_broker_sasl_password,
            args.ssl_ca_cert_file,
            statistics_reporter=statistics_reporter,
        )
        exit_stack.callback(producer.close)

        consumer = create_config_consumer(
            args.config_topic, args.config_topic_sasl_password, args.ssl_ca_cert_file
        )
        exit_stack.callback(consumer.close)

        status_reporter = create_status_reporter(
            update_handlers,
            args.status_topic,
            args.status_topic_sasl_password,
            args.ssl_ca_cert_file,
            args.service_id,
            version,
            get_logger(),
        )
        exit_stack.callback(status_reporter.stop)
        status_reporter.start()

        if args.storage_topic:
            configuration_store = create_configuration_store(
                args.storage_topic,
                args.storage_topic_sasl_password,
                args.ssl_ca_cert_file,
            )
            exit_stack.callback(configuration_store.stop)
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
                        statistics_reporter=statistics_reporter,
                        pvs_subscribed_metric=pvs_subscribed_metric,
                        processing_errors_metric=processing_errors_metric,
                    )
                except RuntimeError as error:
                    get_logger().error(
                        "Could not retrieve stored configuration on start-up: "
                        f"{error}"
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
                        statistics_reporter=statistics_reporter,
                        pvs_subscribed_metric=pvs_subscribed_metric,
                        processing_errors_metric=processing_errors_metric,
                    )

        except KeyboardInterrupt:
            get_logger().info("%% Aborted by user")
        except BaseException as e:
            get_logger().error(
                f"Got an exception in the application main loop. The exception message was: {e}"
            )
            get_logger().exception(e)

        finally:
            for handler in update_handlers.values():
                handler.stop()


if __name__ == "__main__":
    main()
