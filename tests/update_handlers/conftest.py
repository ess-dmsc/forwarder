import logging
import random
import string
from typing import Union

import pytest

from forwarder.common import EpicsProtocol
from forwarder.metrics.statistics_reporter import StatisticsReporter
from forwarder.update_handlers.ca_update_handler import CAUpdateHandler
from forwarder.update_handlers.pva_update_handler import PVAUpdateHandler
from forwarder.update_handlers.serialiser_tracker import create_serialiser_list
from tests.kafka.fake_producer import FakeProducer
from tests.test_helpers.ca_fakes import FakeContext as CAFakeContext
from tests.test_helpers.p4p_fakes import FakeContext as PVAFakeContext


@pytest.fixture
def context(request, producer, pv_source_name):
    statistics_reporter = StatisticsReporter(
        "localhost", logging.getLogger("statistics_reporter")
    )
    context: Union[CAFakeContext, PVAFakeContext]
    schema = request.node.get_closest_marker("schema").args[0]
    epics_protocol = request.node.get_closest_marker("epics_protocol").args[0]
    update_period_ms = None
    if request.node.get_closest_marker("serialiser_update_period_ms"):
        update_period_ms = request.node.get_closest_marker(
            "serialiser_update_period_ms"
        ).args[0]

    if epics_protocol == EpicsProtocol.CA:
        context = CAFakeContext()
        update_handler = CAUpdateHandler(
            context,
            pv_source_name,
            create_serialiser_list(producer, pv_source_name, "output_topic", schema, EpicsProtocol.CA, update_period_ms),  # type: ignore
            statistics_reporter=statistics_reporter,
        )
    elif epics_protocol == EpicsProtocol.PVA:
        context = PVAFakeContext()
        update_handler = PVAUpdateHandler(
            context,
            pv_source_name,
            create_serialiser_list(producer, pv_source_name, "output_topic", schema, EpicsProtocol.PVA, update_period_ms),  # type: ignore
            statistics_reporter=statistics_reporter,
        )
    else:
        raise ValueError(
            f'Cannot create update handler for EPICS protocol "{epics_protocol}"'
        )
    yield context

    update_handler.stop()


@pytest.fixture
def producer():
    producer = FakeProducer()
    yield producer
    producer.close()


@pytest.fixture
def pv_source_name():
    name = "source_" + "".join(random.choices(string.ascii_lowercase, k=6))
    yield name
