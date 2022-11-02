import json
import random
import time

from caproto.sync.client import write
from confluent_kafka import Producer
from p4p.client.thread import Context
from streaming_data_types import (
    deserialise_f142,
    deserialise_rf5k,
    deserialise_x5f2,
    serialise_rf5k,
)
from streaming_data_types.fbschemas.forwarder_config_update_rf5k.UpdateType import (
    UpdateType,
)
from streaming_data_types.forwarder_config_update_rf5k import Protocol, StreamInfo

from ..contract_tests.test_kafka_contract import assign_topic, create_consumer
from .create_topics import (
    CONFIG_TOPIC,
    DATA_TOPIC,
    KAFKA_HOST,
    STATUS_TOPIC,
    STORAGE_TOPIC,
)


def test_check_forwarder_works_as_expected():
    # Configure PVs to forward
    streams = [
        StreamInfo("SIMPLE:DOUBLE", "f142", "forwarder_data", Protocol.Protocol.PVA),
        StreamInfo("SIMPLE:DOUBLE2", "f142", "forwarder_data", Protocol.Protocol.CA),
    ]

    storage_consumer = create_consumer(KAFKA_HOST)
    assign_topic(storage_consumer, STORAGE_TOPIC)

    producer_config = {
        "bootstrap.servers": f"{KAFKA_HOST}:9092",
        "message.max.bytes": "20000000",
    }
    producer = Producer(producer_config)
    producer.produce(CONFIG_TOPIC, serialise_rf5k(UpdateType.ADD, streams))
    producer.flush(timeout=5)

    # Check forwarder status message contains configuration
    consumer = create_consumer(KAFKA_HOST)
    assign_topic(consumer, STATUS_TOPIC)

    latest_msg = None
    start_time = time.monotonic()
    while True:
        if time.monotonic() > start_time + 10:
            break
        msg = consumer.poll(timeout=0.5)
        if msg:
            latest_msg = msg
        time.sleep(0.1)

    consumer.close()

    if not latest_msg:
        assert False, "status timed out"

    msg = deserialise_x5f2(latest_msg.value())
    status = json.loads(msg.status_json)

    assert {
        "channel_name": "SIMPLE:DOUBLE2",
        "protocol": "CA",
        "output_topic": "forwarder_data",
        "schema": "f142",
    } in status["streams"]
    assert {
        "channel_name": "SIMPLE:DOUBLE",
        "protocol": "PVA",
        "output_topic": "forwarder_data",
        "schema": "f142",
    } in status["streams"]

    # Check storage updated
    latest_msg = None
    start_time = time.monotonic()
    while True:
        if time.monotonic() > start_time + 5:
            break
        msg = storage_consumer.poll(timeout=0.5)
        if msg:
            latest_msg = msg
        time.sleep(0.1)

    storage_consumer.close()

    if not latest_msg:
        assert False, "storage timed out"

    msg = deserialise_rf5k(latest_msg.value())

    for s in streams:
        assert s in msg.streams

    # Check forwards PVA value change
    consumer = create_consumer(KAFKA_HOST)
    assign_topic(consumer, DATA_TOPIC)

    new_pva_value = random.randrange(1000)
    ctx = Context("pva", nt=False)
    ctx.put("SIMPLE:DOUBLE", new_pva_value, wait=True)

    messages = []
    start_time = time.monotonic()
    while True:
        if time.monotonic() > start_time + 5:
            break
        msg = consumer.poll(timeout=0.5)
        if msg:
            messages.append(msg.value())
        time.sleep(0.1)

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

    messages = []
    start_time = time.monotonic()
    while True:
        if time.monotonic() > start_time + 5:
            break
        msg = consumer.poll(timeout=0.5)
        if msg:
            messages.append(msg.value())
        time.sleep(0.1)

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

    messages = []
    start_time = time.monotonic()
    while True:
        if time.monotonic() > start_time + 12:
            break
        msg = consumer.poll(timeout=0.5)
        if msg:
            messages.append(msg.value())
        time.sleep(0.1)

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
