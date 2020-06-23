from forwarder.kafka.kafka_helpers import create_producer
import time

"""
"docker-compose up" first!
"""


if __name__ == "__main__":
    config_message = (
        "{"
        '  "cmd": "add",'
        '  "streams": ['
        "    {"
        '      "channel": "SIMPLE:ENUM",'
        '      "channel_provider_type": "ca",'
        '      "converter": {'
        '        "schema": "f142",'
        '        "topic": "localhost:9092/ca_data"'
        "      }"
        "    }"
        "  ]"
        "}"
    )

    print(config_message)

    producer = create_producer("localhost:9092")
    producer.produce(
        "forwarder_config", config_message.encode("utf8"), int(time.time() * 1000)
    )
    producer.close()
