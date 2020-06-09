from forwarder.kafka.kafka_helpers import create_producer
import time

if __name__ == "__main__":
    config_message = (
        "{"
        '  "cmd": "add",'
        '  "streams": ['
        "    {"
        '      "channel": "fake:int",'
        '      "channel_provider_type": "fake",'
        '      "converter": {'
        '        "schema": "f142",'
        '        "topic": "localhost:9092/fake_data"'
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
