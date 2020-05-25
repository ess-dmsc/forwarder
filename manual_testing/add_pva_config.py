from forwarder.kafka.kafka_helpers import create_producer

"""
"docker-compose up" first!
"""


if __name__ == "__main__":
    config_message = (
        "{"
        '  "cmd": "add",'
        '  "streams": ['
        "    {"
        '      "channel": "SIMPLE:DOUBLE3",'
        '      "channel_provider_type": "pva",'
        '      "converter": {'
        '        "schema": "f142",'
        '        "topic": "localhost:9092/pva_data"'
        "      }"
        "    }"
        "  ]"
        "}"
    )

    print(config_message)

    producer = create_producer("localhost:9092")
    producer.produce("forwarder_config", config_message.encode("utf8"))
    producer.close()
