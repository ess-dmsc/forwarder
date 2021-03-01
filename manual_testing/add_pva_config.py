import time

from streaming_data_types.fbschemas.forwarder_config_update_rf5k.Protocol import (
    Protocol,
)
from streaming_data_types.fbschemas.forwarder_config_update_rf5k.UpdateType import (
    UpdateType,
)
from streaming_data_types.forwarder_config_update_rf5k import StreamInfo, serialise_rf5k

from forwarder.kafka.kafka_helpers import create_producer

"""
"docker-compose up" first!
"""


if __name__ == "__main__":
    producer = create_producer("localhost:9092")

    producer.produce(
        "forwarder_config",
        serialise_rf5k(
            UpdateType.ADD,
            [
                StreamInfo(
                    "SIMPLE:DOUBLE3",
                    "f142",
                    "pva_data",
                    Protocol.PVA,
                )
            ],
        ),
        int(time.time() * 1000),
    )
    producer.close()
