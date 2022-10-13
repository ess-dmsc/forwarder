import configparser
import logging
from os import getpid
from pathlib import Path

import configargparse


class VersionArgParser(configargparse.ArgumentParser):
    def error(self, message: str):
        """
        Override the default implementation so nothing gets printed to screen
        """
        raise RuntimeError("Did not ask for --version")

    def _print_message(self, message: str, file: None = None):
        """
        Override the default implementation so nothing gets printed to screen
        """
        raise RuntimeError("Did not ask for --version")


def get_version() -> str:
    """
    Gets the current version from the setup.cfg file
    """
    config = configparser.ConfigParser()
    path = Path(__file__).parent.parent / "setup.cfg"
    config.read(path)
    return str(config["metadata"]["version"])


def _print_version_if_requested():
    version_arg_parser = VersionArgParser()
    version_arg_parser.add_argument(
        "--version",
        required=True,
        action="store_true",
        help="Print application version and exit",
        env_var="VERSION",
    )
    try:
        version_arg_parser.parse_args()
        print(get_version())
        exit()
    except RuntimeError:
        pass


def parse_args():
    _print_version_if_requested()

    parser = configargparse.ArgumentParser(
        description="Forwards EPICS PVs to Apache Kafka. Part of the ESS data streaming pipeline."
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
        help="<[SASL_MECHANISM\\username@]host[:port][/topic]> Kafka broker/topic to listen for commands",
        type=str,
        env_var="CONFIG_TOPIC",
    )
    parser.add_argument(
        "--config-topic-sasl-password",
        required=False,
        help="Password for Kafka SASL authentication",
        type=str,
        env_var="CONFIG_TOPIC_SASL_PASSWORD",
    )
    parser.add_argument(
        "--status-topic",
        required=True,
        help="<[SASL_MECHANISM\\username@]host[:port][/topic]> Kafka broker/topic to publish status updates on",
        type=str,
        env_var="STATUS_TOPIC",
    )
    parser.add_argument(
        "--status-topic-sasl-password",
        required=False,
        help="Password for Kafka SASL authentication",
        type=str,
        env_var="STATUS_TOPIC_SASL_PASSWORD",
    )
    parser.add_argument(
        "--output-broker",
        required=True,
        help="<[SASL_MECHANISM\\username@]host[:port]> Kafka broker to forward data into",
        type=str,
        env_var="OUTPUT_BROKER",
    )
    parser.add_argument(
        "--output-broker-sasl-password",
        required=False,
        help="Password for Kafka SASL authentication",
        type=str,
        env_var="OUTPUT_BROKER_SASL_PASSWORD",
    )
    parser.add_argument(
        "--storage-topic",
        required=False,
        help="<[SASL_MECHANISM\\username@]host[:port][/topic]> Kafka broker/topic for storage of the "
        "last known forwarding details",
        type=str,
        env_var="STORAGE_TOPIC",
    )
    parser.add_argument(
        "--storage-topic-sasl-password",
        required=False,
        help="Password for Kafka SASL authentication",
        type=str,
        env_var="STORAGE_TOPIC_SASL_PASSWORD",
    )
    parser.add_argument(
        "-s",
        "--skip-retrieval",
        action="store_true",
        help="Ignore the stored configuration on startup",
    )
    parser.add_argument(
        "--graylog-logger-address",
        required=False,
        help="<host:port> Log to Graylog",
        type=str,
        env_var="GRAYLOG_LOGGER_ADDRESS",
    )
    parser.add_argument(
        "--grafana-carbon-address",
        required=False,
        help="<host:port> Address to the Grafana (Carbon) metrics server",
        type=str,
    )
    parser.add_argument(
        "--statistics-update-interval",
        required=False,
        help="Update interval (in seconds) to send statistics to Grafana metrics server",
        type=int,
        default=10,
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
        "--service-id",
        required=False,
        help='Identifier for this particular instance of the Forwarder, defaults to "<PID>',
        default=f"{getpid()}",
        env_var="SERVICE_ID",
        type=str,
    )
    parser.add_argument(
        "--fake-pv-period",
        required=False,
        help="Period for random generated PV updates when channel_provider_type is set to 'fake' (units=milliseconds)",
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
