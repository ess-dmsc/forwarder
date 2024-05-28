import json
import random
import time

from caproto.sync.client import write
from confluent_kafka import Producer
from p4p.client.thread import Context
from streaming_data_types import (
    deserialise_f142,
    deserialise_fc00,
    deserialise_x5f2,
    serialise_fc00,
)
from streaming_data_types.fbschemas.forwarder_config_update_fc00.UpdateType import (
    UpdateType,
)
from streaming_data_types.forwarder_config_update_fc00 import Protocol, StreamInfo

from ..contract_tests.test_kafka_contract import assign_topic, create_consumer
from .prepare import CONFIG_TOPIC, DATA_TOPIC, KAFKA_HOST, STATUS_TOPIC, STORAGE_TOPIC


def _get_messages(consumer, timeout=10):
    messages = []
    start_time = time.monotonic()
    while True:
        if time.monotonic() > start_time + timeout:
            break
        msg = consumer.poll(timeout=0.5)
        if msg:
            messages.append(msg.value())
        time.sleep(0.1)
    return messages


def test_check_forwarder_works_as_expected():
    producer_config = {
        "bootstrap.servers": f"{KAFKA_HOST}:9092",
        "message.max.bytes": "20000000",
    }
    producer = Producer(producer_config)

    # Check it picks up stored configuration
    consumer = create_consumer(KAFKA_HOST)
    assign_topic(consumer, STATUS_TOPIC)
    messages = _get_messages(consumer)
    consumer.close()

    if not messages:
        assert False, "status timed out"

    msg = deserialise_x5f2(messages[~0])
    status = json.loads(msg.status_json)

    assert len(status["streams"]) == 1
    assert {
        "channel_name": "SIMPLE:DOUBLE",
        "protocol": "PVA",
        "output_topic": DATA_TOPIC,
        "schema": "f142",
    } in status["streams"]

    # Remove configuration
    producer.produce(CONFIG_TOPIC, serialise_fc00(UpdateType.REMOVEALL, []))
    producer.flush(timeout=5)

    consumer = create_consumer(KAFKA_HOST)
    assign_topic(consumer, STATUS_TOPIC)
    messages = _get_messages(consumer)
    consumer.close()

    if not messages:
        assert False, "status timed out"

    msg = deserialise_x5f2(messages[~0])
    status = json.loads(msg.status_json)

    assert len(status["streams"]) == 0

    # Configure PVs to forward
    streams = [
        StreamInfo("SIMPLE:DOUBLE", "f142", DATA_TOPIC, Protocol.Protocol.PVA, 1),
        StreamInfo("SIMPLE:DOUBLE2", "f142", DATA_TOPIC, Protocol.Protocol.CA, 1),
    ]

    storage_consumer = create_consumer(KAFKA_HOST)
    assign_topic(storage_consumer, STORAGE_TOPIC)

    producer.produce(CONFIG_TOPIC, serialise_fc00(UpdateType.ADD, streams))
    producer.flush(timeout=5)

    # Check forwarder status message contains configuration
    consumer = create_consumer(KAFKA_HOST)
    assign_topic(consumer, STATUS_TOPIC)
    messages = _get_messages(consumer)
    consumer.close()

    if not messages:
        assert False, "status timed out"

    msg = deserialise_x5f2(messages[~0])
    status = json.loads(msg.status_json)

    assert {
        "channel_name": "SIMPLE:DOUBLE2",
        "protocol": "CA",
        "output_topic": DATA_TOPIC,
        "schema": "f142",
    } in status["streams"]
    assert {
        "channel_name": "SIMPLE:DOUBLE",
        "protocol": "PVA",
        "output_topic": DATA_TOPIC,
        "schema": "f142",
    } in status["streams"]

    # Check storage updated
    messages = _get_messages(storage_consumer)
    storage_consumer.close()

    if not messages:
        assert False, "storage timed out"

    msg = deserialise_fc00(messages[~0])

    for s in streams:
        assert s in msg.streams

    # Check forwards PVA value change
    consumer = create_consumer(KAFKA_HOST)
    assign_topic(consumer, DATA_TOPIC)

    new_pva_value = random.randrange(1000)
    ctx = Context("pva", nt=False)
    ctx.put("SIMPLE:DOUBLE", new_pva_value, wait=True)

    messages = _get_messages(consumer)
    consumer.close()

    if not messages:
        assert False, "pva data timed out"

    found_value = False
    for msg in messages:
        if msg[4:8].decode("utf-8") == "f142":
            m = deserialise_f142(msg)
            if m.source_name == "SIMPLE:DOUBLE" and m.value == new_pva_value:
                found_value = True

    assert found_value, "didn't find value that was set via PVA"

    # Check forwards CA value change
    consumer = create_consumer(KAFKA_HOST)
    assign_topic(consumer, DATA_TOPIC)

    new_ca_value = random.randrange(1000)
    write("SIMPLE:DOUBLE2", new_ca_value, notify=True)

    messages = _get_messages(consumer)
    consumer.close()

    if not messages:
        assert False, "ca data timed out"

    found_value = False
    for msg in messages:
        if msg[4:8].decode("utf-8") == "f142":
            m = deserialise_f142(msg)
            if m.source_name == "SIMPLE:DOUBLE2" and m.value == new_ca_value:
                found_value = True

    assert found_value, "didn't find value that was set via CA"

    # Check for periodic updates
    consumer = create_consumer(KAFKA_HOST)
    assign_topic(consumer, DATA_TOPIC)
    messages = _get_messages(consumer, timeout=12)
    consumer.close()

    if not messages:
        assert False, "periodic data not found"

    found_pva_value = False
    found_ca_value = False
    for msg in messages:
        if msg[4:8].decode("utf-8") == "f142":
            m = deserialise_f142(msg)
            if m.source_name == "SIMPLE:DOUBLE" and m.value == new_pva_value:
                found_pva_value = True
            if m.source_name == "SIMPLE:DOUBLE2" and m.value == new_ca_value:
                found_ca_value = True

    assert found_pva_value, "didn't find periodic value for PVA"
    assert found_ca_value, "didn't find periodic value for CA"

    # Check that we can configure new PVs with REPLACE and without periodic updates
    streams = [
        StreamInfo("SIMPLE:DOUBLE", "f142", DATA_TOPIC, Protocol.Protocol.PVA, 0),
        StreamInfo("SIMPLE:DOUBLE2", "f142", DATA_TOPIC, Protocol.Protocol.CA, 0),
    ]
    producer.produce(CONFIG_TOPIC, serialise_fc00(UpdateType.REPLACE, streams))
    producer.flush(timeout=5)

    # Check forwarder status message contains new configuration
    consumer = create_consumer(KAFKA_HOST)
    assign_topic(consumer, STATUS_TOPIC)
    messages = _get_messages(consumer)
    consumer.close()

    if not messages:
        assert False, "status timed out"

    msg = deserialise_x5f2(messages[~0])
    status = json.loads(msg.status_json)

    assert {
        "channel_name": "SIMPLE:DOUBLE2",
        "protocol": "CA",
        "output_topic": DATA_TOPIC,
        "schema": "f142",
    } in status["streams"]
    assert {
        "channel_name": "SIMPLE:DOUBLE",
        "protocol": "PVA",
        "output_topic": DATA_TOPIC,
        "schema": "f142",
    } in status["streams"]

    # Ensure no periodic updates are received
    consumer = create_consumer(KAFKA_HOST)
    assign_topic(consumer, DATA_TOPIC)
    messages = _get_messages(consumer, timeout=12)
    consumer.close()

    found_periodic_pva_value = False
    found_periodic_ca_value = False
    for msg in messages:
        if msg[4:8].decode("utf-8") == "f142":
            m = deserialise_f142(msg)
            if m.source_name == "SIMPLE:DOUBLE":
                found_periodic_pva_value = True
            if m.source_name == "SIMPLE:DOUBLE2":
                found_periodic_ca_value = True

    assert (
        not found_periodic_pva_value
    ), "found periodic value for PVA when not expected"
    assert not found_periodic_ca_value, "found periodic value for CA when not expected"

    # Check forwards PVA value change after REPLACE
    consumer = create_consumer(KAFKA_HOST)
    assign_topic(consumer, DATA_TOPIC)

    new_pva_value = random.randrange(1000)
    ctx.put("SIMPLE:DOUBLE", new_pva_value, wait=True)

    messages = _get_messages(consumer)
    consumer.close()

    if not messages:
        assert False, "pva data timed out"

    found_value = False
    for msg in messages:
        if msg[4:8].decode("utf-8") == "f142":
            m = deserialise_f142(msg)
            if m.source_name == "SIMPLE:DOUBLE" and m.value == new_pva_value:
                found_value = True

    assert found_value, "didn't find value that was set via PVA after REPLACE"

    # Check forwards CA value change after REPLACE
    consumer = create_consumer(KAFKA_HOST)
    assign_topic(consumer, DATA_TOPIC)

    new_ca_value = random.randrange(1000)
    write("SIMPLE:DOUBLE2", new_ca_value, notify=True)

    messages = _get_messages(consumer)
    consumer.close()

    if not messages:
        assert False, "ca data timed out"

    found_value = False
    for msg in messages:
        if msg[4:8].decode("utf-8") == "f142":
            m = deserialise_f142(msg)
            if m.source_name == "SIMPLE:DOUBLE2" and m.value == new_ca_value:
                found_value = True

    assert found_value, "didn't find value that was set via CA after REPLACE"
