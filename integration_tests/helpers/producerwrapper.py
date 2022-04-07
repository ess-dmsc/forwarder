import uuid
from typing import List

from confluent_kafka import Consumer, KafkaException, Producer
from streaming_data_types.fbschemas.forwarder_config_update_rf5k.Protocol import (
    Protocol,
)

from .forwarderconfig import ForwarderConfig


class ProducerWrapper:
    """
    A wrapper class for the kafka producer.
    """

    def __init__(
        self,
        server: str,
        config_topic: str,
        data_topic: str,
        epics_protocol: Protocol = Protocol.CA,
    ):
        self.topic = config_topic
        self.converter = ForwarderConfig(data_topic, epics_protocol)
        self._set_up_producer(server)

    def _set_up_producer(self, server: str):
        conf = {"bootstrap.servers": server}
        try:
            self.producer = Producer(**conf)

            if not self.topic_exists(self.topic, server):
                print(
                    "WARNING: topic {} does not exist. It will be created by default.".format(
                        self.topic
                    )
                )
        except KafkaException.args[0] == "_BROKER_NOT_AVAILABLE":
            print("No brokers found on server: " + server[0])
            quit()
        except KafkaException.args[0] == "_TIMED_OUT":
            print("No server found, connection error")
            quit()
        except KafkaException.args[0] == "_INVALID_ARG":
            print("Invalid configuration")
            quit()
        except KafkaException.args[0] == "_UNKNOWN_TOPIC":
            print(
                "Invalid topic, to enable auto creation of topics set"
                " auto.create.topics.enable to false in broker configuration"
            )
            quit()

    def add_config(self, pvs: List[str]):
        """
        Create a forwarder configuration to add more pvs to be monitored.

        :param pvs: A list of new PVs to add to the forwarder configuration.
        """
        message_buffer = self.converter.create_forwarder_configuration(pvs)
        self.producer.produce(self.topic, value=message_buffer)
        self.producer.flush()

    @staticmethod
    def topic_exists(topic_name: str, server: str) -> bool:
        conf = {"bootstrap.servers": server, "group.id": uuid.uuid4()}
        consumer = Consumer(**conf)
        try:
            consumer.subscribe([topic_name])
            consumer.close()
        except KafkaException as e:
            print("topic '{}' does not exist".format(topic_name))
            print(e)
            return False
        return True

    def remove_config(self, pvs: List[str]):
        """
        Create a forwarder configuration to remove pvs that are being monitored.

        :param pvs: A list of PVs to remove from the forwarder configuration.
        """
        message_buffer = self.converter.remove_forwarder_configuration(pvs)
        self.producer.produce(self.topic, value=message_buffer)
        self.producer.flush()

    def stop_all_pvs(self):
        """
        Sends a stop_all command to the forwarder to clear all configuration.
        """
        message_buffer = self.converter.remove_all_forwarder_configuration()
        self.producer.produce(self.topic, value=message_buffer)
        self.producer.flush()
