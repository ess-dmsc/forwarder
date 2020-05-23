from kafka.kafka_helpers import publish_f142_message, publish_tdct_message


schema_publishers = {"f142": publish_f142_message, "tdct": publish_tdct_message}
