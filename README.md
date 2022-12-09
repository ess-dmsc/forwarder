[![Build Status](https://jenkins.esss.dk/dm/job/ess-dmsc/job/forwarder/job/main/badge/icon)](https://jenkins.esss.dk/dm/job/ess-dmsc/job/forwarder/job/main/) [![codecov](https://codecov.io/gh/ess-dmsc/forwarder/branch/master/graph/badge.svg)](https://codecov.io/gh/ess-dmsc/forwarder)


# Forwarder
Forwards EPICS PVs to Apache Kafka. Part of the ESS data streaming pipeline.

## Installing dependencies

Python 3.8 or higher is required. https://www.python.org/downloads/

Runtime Python dependencies are listed in requirements.txt at the root of the
repository. They can be installed from a terminal by running
```
pip install -r requirements.txt
```

## Usage
To run with minimal settings:
```
forwarder_launch.py --config-topic some_server:9092/some_config_topic --status-topic some_server:9092/some_status_topic --output-broker some_other_server:9092
```

For help:
```
forwarder_launch.py --help
```

Required arguments:
 * config-topic - Kafka username/broker/topic (`sasl_mechanism\user@broker:port/topic`) to listen for commands relating to PVs to be forwarded on
 * status-topic - Kafka username/broker/topic (`sasl_mechanism\user@broker:port/topic`) to publish regular status updates on
 * output-broker - Kafka username/broker (`sasl_mechanism\user@broker:port`) to forward PV data into

Optional arguments:
 * config-topic-sasl-password - Password for SASL Kafka authentication. Note that the username is specified in the `config-topic` argument
 * status-topic-sasl-password - Password for SASL Kafka authentication. Note that the username is specified in the `status-topic` argument
 * output-broker-sasl-password - Password for SASL Kafka authentication. Note that the username is specified in the `output-broker` argument
 * storage-topic - Kafka username/broker/topic for storage of the current forwarding details; these will be reapplied when the forwarder is restarted
 * storage-topic-sasl-password - Password for SASL Kafka authentication. Note that the username is specified in the `storage-topic` argument
 * skip-retrieval - do not reapply stored forwarding details on start-up
 * graylog-logger-address - Graylog logger instance to log to
 * log-file - name of the file to log to
 * pv-update-period - period for forward PVs values even if the value hasn't changed (milliseconds)
 * service-id - identifier for this particular instance of the Forwarder
 * fake-pv-period - period for random generated PV updates when channel_provider_type is set to 'fake' (milliseconds)

Arguments can also be specified in a configuration file
```
forwarder_launch --config-file my_config.conf
```
The configuration file consists of 'key=value' pairs, e.g.
```
output-broker=localhost:9092
pv-update-period=1000
```

### Kafka authentication

Authentication is disabled by default. To enable it, you must provide the SASL mechanism, username and password as arguments.

The supported Kafka authentication SASL mechanisms are `SCRAM-SHA-256`, `SCRAM-SHA-512` and `PLAIN`.
Only non-TLS channels (`SASL_PLAINTEXT`) are currently supported.

The SASL mechanism can be specified as part of the username/broker string as follows: `sasl_mechanism\username@broker:port/topic`.
Example: `SCRAM-SHA-256\alice@10.123.123.1:9092/topic`.

## Configuring EPICS PVs to be forwarded

Adding or removing PVs to be forwarded is done by publishing configuration change messages to the configuration
topic specified in the command line arguments. Such messages must be serialised as FlatBuffers using
the rf5k schema which can be found [here](https://github.com/ess-dmsc/streaming-data-types/blob/master/schemas/rf5k_forwarder_config.fbs).
Support for serialising and deserialising these messages in Python in available in the
[ess-streaming-data-types](https://pypi.org/project/ess-streaming-data-types/) library.

The schema takes two parameters: the type of configuration change (UpdateType) and the corresponding streams.

There are three choices for the UpdateType of the configuration message:
 * ADD - add the specified streams to the existing set of streams
 * REMOVE - remove the specified streams from the set of streams
 * REMOVEALL - remove all streams

A stream contains:
 * The name of the PV to be forwarded.
 * The EPICS protocol for reading the PV.
 * The Kafka topic to write to.
 * The FlatBuffers schema to encode the PV value with. See the supported schemas [here](forwarder/update_handlers/schema_serialiser_factory.py#L24).
   * Note that additional messages with different schemas may be configured by the forwarder 
     automatically. In particular, every PV will also generate `ep01` messages, and PVs 
     configured for `f144` will forward alarm information as `al00` messages.


Note that when removing (using REMOVE) configured streams, not all fields in the `Stream` table of the schema need to be populated.
Missing or empty strings in the channel name, output topic and schema fields match all stream configurations.
At least one field must be populated though.
Single-character "?"" and multi-character "*" wildcards are allowed to be used in the channel name and output topic fields.
In conjunction with naming conventions for EPICS channel names and Kafka topics this can be used to carry out operations
such as clearing all configured streams for a particular instrument.

Empty PV updates are not forwarded and are not cached to send in periodic updates.
This addresses, for example, the case of empty chopper timestamp updates when a chopper is not spinning.

Chopper timestamps to be forwarded with `tdct` schema are assumed to be in nanoseconds and relative
to the EPICS update timestamp, they are converted to nanosecond-precision unix timestamps when forwarded.

### A Python example
To use for real, replace CONFIG_BROKER, CONFIG_TOPIC and STREAMS with values corresponding to the real system.

```python
from confluent_kafka import Producer
from streaming_data_types.forwarder_config_update_rf5k import (
    serialise_rf5k,
    StreamInfo,
    Protocol,
)
from streaming_data_types.fbschemas.forwarder_config_update_rf5k.UpdateType import (
    UpdateType,
)

CONFIG_BROKER = "some_kafka_broker:9092"

CONFIG_TOPIC = "TEST_forwarderConfig"

STREAMS = [
    StreamInfo("IOC:PV1", "f142", "some_topic", Protocol.Protocol.PVA),
    StreamInfo("IOC:PV2", "f142", "some_other_topic", Protocol.Protocol.CA),
    StreamInfo("IOC:PV3", "f142", "some_other_topic", Protocol.Protocol.PVA),
]

producer = Producer({"bootstrap.servers": CONFIG_BROKER})

# Add new streams
producer.produce(CONFIG_TOPIC, serialise_rf5k(UpdateType.ADD, STREAMS))

# Remove one stream (note that you need to pass the argument as a list)
# producer.produce(CONFIG_TOPIC, serialise_rf5k(UpdateType.REMOVE, [STREAMS[0]]))

# Remove all the streams at once
#   USE WITH CAUTION!!
# producer.produce(CONFIG_TOPIC, serialise_rf5k(UpdateType.REMOVEALL, []))

producer.flush()
```

# Developer information

## Development dependencies

Development dependencies (including all runtime dependencies) can be installed by using the following command

```
pip install -r requirements-dev.txt
```

`black`, `flake8` and `mypy` can be used as a pre-commit hook (installed by [pre-commit](https://pre-commit.com/)).
You need to run
```
pre-commit install
```
once to activate the pre-commit check.
To test the hooks run
```
pre-commit run --all-files
```
This command can also be used to run the hooks manually.

