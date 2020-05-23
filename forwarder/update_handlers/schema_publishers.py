from forwarder.kafka.kafka_helpers import publish_f142_message, publish_tdct_message
from typing import Dict, Callable


schema_publishers: Dict[str, Callable] = {
    "f142": publish_f142_message,
    "tdct": publish_tdct_message,
}
