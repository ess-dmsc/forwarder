from .forwarderconfig import ForwarderConfig
from confluent_kafka import Producer, Consumer, KafkaException
import uuid
from typing import List


class ProducerWrapper:
    """
    A wrapper class for the kafka producer.
    """

    def __init__(self, server, config_topic, data_topic):
        self.topic = config_topic
        self.converter = ForwarderConfig(data_topic)
        self._set_up_producer(server)

    def _set_up_producer(self, server):
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
        :return: None
        """
        data = self.converter.create_forwarder_configuration(pvs)
        print("Sending data {}".format(data))
        self.producer.produce(self.topic, value=data)
        self.producer.flush()

    @staticmethod
    def topic_exists(topicname, server):
        conf = {"bootstrap.servers": server, "group.id": uuid.uuid4()}
        consumer = Consumer(**conf)
        try:
            consumer.subscribe([topicname])
            consumer.close()
        except KafkaException as e:
            print("topic '{}' does not exist".format(topicname))
            print(e)
            return False
        return True

    def remove_config(self, pvs: List[str]):
        """
        Create a forwarder configuration to remove pvs that are being monitored.
        
        :param pvs: A list of PVs to remove from the forwarder configuration.
        :return: None
        """
        data = self.converter.remove_forwarder_configuration(pvs)
        for pv in data:
            print("Sending data {}".format(pv))
            self.producer.produce(self.topic, value=pv)

    def stop_all_pvs(self):
        """
        Sends a stop_all command to the forwarder to clear all configuration.

        :return: None
        """
        self.producer.produce(self.topic, value='{"cmd": "stop_all"}')
